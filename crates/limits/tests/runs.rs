use lenso_limits::{
    AcquireInput, Acquisition, ConsumeInput, CounterKind, Decision, FailurePolicy, Lease,
    LeaseCancellation, LeaseObservation, LeaseRunFailure, LeaseRunOptions, LeaseStopReason,
    LeaseToken, LimitError, LimitScope, LimitStore, Limits, LimitsPlugin, MemoryLimitStore,
};
use lenso_plugin_core::{App, Instance};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};

fn input() -> AcquireInput {
    AcquireInput {
        scope: LimitScope::new("reports", "one", "running").unwrap(),
        capacity: 1,
        quantity: 1,
        ttl: Duration::from_millis(300),
    }
}
fn renewing() -> LeaseRunOptions {
    LeaseRunOptions {
        renew_every: Some(Duration::from_millis(100)),
        cancellation: None,
    }
}
struct RecordingStore {
    memory: MemoryLimitStore,
    acquisitions: AtomicUsize,
    renewals: AtomicUsize,
    releases: AtomicUsize,
    fail_admission: AtomicBool,
    fail_renewal: AtomicBool,
    fail_release: AtomicBool,
    fail_observe: AtomicBool,
    renewal_delay: Duration,
}
impl RecordingStore {
    fn new() -> Self {
        let start = Instant::now();
        Self::from_memory(MemoryLimitStore::with_clock(move || {
            Instant::now().duration_since(start).as_millis() as u64
        }))
    }
    fn from_memory(memory: MemoryLimitStore) -> Self {
        Self {
            memory,
            acquisitions: AtomicUsize::new(0),
            renewals: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            fail_admission: AtomicBool::new(false),
            fail_renewal: AtomicBool::new(false),
            fail_release: AtomicBool::new(false),
            fail_observe: AtomicBool::new(false),
            renewal_delay: Duration::ZERO,
        }
    }
    fn available(&self) -> bool {
        let acquired = self
            .memory
            .acquire(&input(), LeaseToken::fresh().unwrap())
            .unwrap();
        if let Some(lease) = &acquired.lease {
            self.memory.release(&lease.scope, lease.token).unwrap();
        }
        acquired.decision.allowed
    }
}
impl LimitStore for RecordingStore {
    fn consume(&self, kind: CounterKind, input: &ConsumeInput) -> Result<Decision, LimitError> {
        self.memory.consume(kind, input)
    }
    fn acquire(&self, input: &AcquireInput, token: LeaseToken) -> Result<Acquisition, LimitError> {
        self.acquisitions.fetch_add(1, Ordering::SeqCst);
        if self.fail_admission.load(Ordering::SeqCst) {
            return Err(LimitError::BackendFailure);
        }
        self.memory.acquire(input, token)
    }
    fn renew(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
        ttl: Duration,
    ) -> Result<Option<Lease>, LimitError> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        if self.fail_renewal.load(Ordering::SeqCst) {
            return Err(LimitError::BackendFailure);
        }
        if !self.renewal_delay.is_zero() {
            std::thread::sleep(self.renewal_delay);
        }
        self.memory.renew(scope, token, ttl)
    }
    fn release(&self, scope: &LimitScope, token: LeaseToken) -> Result<(), LimitError> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(LimitError::BackendFailure);
        }
        self.memory.release(scope, token)
    }
    fn observe(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
    ) -> Result<Option<LeaseObservation>, LimitError> {
        if self.fail_observe.load(Ordering::SeqCst) {
            return Err(LimitError::BackendFailure);
        }
        self.memory.observe(scope, token)
    }
}

#[tokio::test(start_paused = true)]
async fn success_business_error_and_callback_panics_all_release() {
    let store = Arc::new(RecordingStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let value = limits
        .with_lease(input(), LeaseRunOptions::default(), |run| async move {
            assert!(run.lease_snapshot().is_some());
            Ok::<_, &'static str>(42)
        })
        .await
        .unwrap();
    assert_eq!(value, 42);
    let error = limits
        .with_lease(input(), LeaseRunOptions::default(), |_| async {
            Err::<(), _>("business")
        })
        .await
        .unwrap_err();
    assert_eq!(error.business_error, Some("business"));
    assert!(error.failures.is_empty());
    let construction = limits
        .with_lease(
            input(),
            LeaseRunOptions::default(),
            |_| -> std::future::Ready<Result<(), ()>> {
                panic!("callback construction");
            },
        )
        .await
        .unwrap_err();
    assert_eq!(construction.failures, [LeaseRunFailure::BusinessPanicked]);
    fn panic_on_poll() -> Result<(), ()> {
        panic!("callback polling");
    }
    let polling = limits
        .with_lease(input(), LeaseRunOptions::default(), |_| async {
            panic_on_poll()
        })
        .await
        .unwrap_err();
    assert_eq!(polling.failures, [LeaseRunFailure::BusinessPanicked]);
    assert_eq!(store.releases.load(Ordering::SeqCst), 4);
    assert!(store.available());
    limits.close_async().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn validation_precancellation_and_denial_never_execute_business() {
    let store = Arc::new(RecordingStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    for interval in [
        Duration::ZERO,
        Duration::from_nanos(1),
        input().ttl,
        input().ttl + Duration::from_millis(1),
    ] {
        let error = limits
            .with_lease(
                input(),
                LeaseRunOptions {
                    renew_every: Some(interval),
                    cancellation: None,
                },
                |_| async {
                    panic!("must not run");
                    #[allow(unreachable_code)]
                    Ok::<(), ()>(())
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.failures,
            [LeaseRunFailure::InvalidInput(LimitError::InvalidInput)]
        );
    }
    let cancellation = LeaseCancellation::new();
    cancellation.cancel();
    let error = limits
        .with_lease(
            input(),
            LeaseRunOptions {
                cancellation: Some(cancellation),
                ..LeaseRunOptions::default()
            },
            |_| async { Ok::<(), ()>(()) },
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Cancelled)]
    );
    assert_eq!(store.acquisitions.load(Ordering::SeqCst), 0);
    let lease = limits.acquire(&input()).unwrap().lease.unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    let error = limits
        .with_lease(input(), LeaseRunOptions::default(), move |_| async move {
            mark.store(true, Ordering::SeqCst);
            Ok::<(), ()>(())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error.failures.as_slice(),
        [LeaseRunFailure::Denied(_)]
    ));
    assert!(!called.load(Ordering::SeqCst));
    limits.release(&lease).unwrap();
    limits.close_async().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn explicit_renewal_extends_execution_and_stops_with_its_owner() {
    let store = Arc::new(RecordingStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let owner = limits.clone();
    let work = tokio::spawn(async move {
        owner
            .with_lease(input(), renewing(), move |run| async move {
                entered_tx
                    .send(run.clone())
                    .unwrap_or_else(|_| panic!("receiver dropped"));
                finish_rx.await.unwrap();
                Ok::<_, ()>(run.lease_snapshot().unwrap().expires_at_ms)
            })
            .await
    });
    let execution = entered_rx.await.unwrap();
    for _ in 0..5 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(store.renewals.load(Ordering::SeqCst), 5);
    assert_eq!(execution.lease_snapshot().unwrap().expires_at_ms, 800);
    assert!(!store.available());
    finish_tx.send(()).unwrap();
    assert_eq!(work.await.unwrap().unwrap(), 800);
    assert!(execution.lease_snapshot().is_none());
    let renewals = store.renewals.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), renewals);
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
    assert!(store.available());
    limits.close_async().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn expiry_and_executor_stalls_do_not_get_late_renewal_or_success() {
    for options in [LeaseRunOptions::default(), renewing()] {
        let store = Arc::new(RecordingStore::new());
        let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
        let (entered_tx, entered_rx) = oneshot::channel();
        let owner = limits.clone();
        let work = tokio::spawn(async move {
            owner
                .with_lease(input(), options, move |run| async move {
                    entered_tx.send(()).unwrap();
                    assert_eq!(run.cancelled().await, LeaseStopReason::Expired);
                    assert!(run.lease_snapshot().is_none());
                    Ok::<(), ()>(())
                })
                .await
        });
        entered_rx.await.unwrap();
        // One jump past both renewal and expiry deadlines simulates a stalled executor.
        tokio::time::advance(input().ttl).await;
        let error = work.await.unwrap().unwrap_err();
        assert_eq!(
            error.failures,
            [LeaseRunFailure::Stopped(LeaseStopReason::Expired)]
        );
        assert_eq!(store.renewals.load(Ordering::SeqCst), 0);
        assert!(store.available());
        limits.close_async().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn lost_renewal_cannot_release_a_replacement_and_backend_faults_never_fail_open() {
    for backend_fault in [false, true] {
        let now = Arc::new(AtomicU64::new(0));
        let clock = now.clone();
        let store = Arc::new(RecordingStore::from_memory(MemoryLimitStore::with_clock(
            move || clock.load(Ordering::SeqCst),
        )));
        let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Allow));
        let (entered_tx, entered_rx) = oneshot::channel();
        let owner = limits.clone();
        let work = tokio::spawn(async move {
            owner
                .with_lease(input(), renewing(), move |run| async move {
                    entered_tx.send(()).unwrap();
                    run.cancelled().await;
                    Err::<(), _>("business stopped")
                })
                .await
        });
        entered_rx.await.unwrap();
        let expected = if backend_fault {
            store.fail_renewal.store(true, Ordering::SeqCst);
            LeaseStopReason::RenewalFailed(LimitError::BackendFailure)
        } else {
            now.store(300, Ordering::SeqCst);
            assert!(
                store
                    .memory
                    .acquire(&input(), LeaseToken::fresh().unwrap())
                    .unwrap()
                    .decision
                    .allowed
            );
            LeaseStopReason::Lost
        };
        tokio::time::advance(Duration::from_millis(100)).await;
        let error = work.await.unwrap().unwrap_err();
        assert_eq!(error.failures, [LeaseRunFailure::Stopped(expected)]);
        assert_eq!(error.business_error, Some("business stopped"));
        assert_eq!(store.available(), backend_fault);
        limits.close_async().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn final_observation_catches_clock_jump_and_fail_open_is_visibly_unleased() {
    let now = Arc::new(AtomicU64::new(0));
    let clock = now.clone();
    let store = Arc::new(RecordingStore::from_memory(MemoryLimitStore::with_clock(
        move || clock.load(Ordering::SeqCst),
    )));
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Allow));
    let error = limits
        .with_lease(input(), LeaseRunOptions::default(), move |_| async move {
            // Business has already executed; the wrapper can report failure, not undo it.
            now.store(300, Ordering::SeqCst);
            Ok::<(), ()>(())
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Lost)]
    );
    store.fail_observe.store(true, Ordering::SeqCst);
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    let error = limits
        .with_lease(input(), renewing(), move |_| async move {
            mark.store(true, Ordering::SeqCst);
            Ok::<(), ()>(())
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::InspectionFailed(
            LimitError::BackendFailure
        ))]
    );
    assert!(!called.load(Ordering::SeqCst));
    store.fail_admission.store(true, Ordering::SeqCst);
    assert_eq!(
        limits
            .with_lease(input(), renewing(), |run| async move {
                assert!(run.lease_snapshot().is_none());
                Ok::<_, ()>(42)
            })
            .await
            .unwrap(),
        42
    );
    assert_eq!(store.renewals.load(Ordering::SeqCst), 0);
    limits.close_async().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn explicit_cancel_preserves_business_and_release_errors_and_close_retries() {
    let store = Arc::new(RecordingStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Allow));
    let cancellation = LeaseCancellation::new();
    let (entered_tx, entered_rx) = oneshot::channel();
    let options = LeaseRunOptions {
        cancellation: Some(cancellation.clone()),
        ..renewing()
    };
    let owner = limits.clone();
    let work = tokio::spawn(async move {
        owner
            .with_lease(input(), options, move |run| async move {
                entered_tx.send(()).unwrap();
                assert_eq!(run.cancelled().await, LeaseStopReason::Cancelled);
                Err::<(), _>("business")
            })
            .await
    });
    entered_rx.await.unwrap();
    store.fail_release.store(true, Ordering::SeqCst);
    cancellation.cancel();
    let error = work.await.unwrap().unwrap_err();
    assert_eq!(
        error.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Cancelled)]
    );
    assert_eq!(error.business_error, Some("business"));
    assert_eq!(error.release_error, Some(LimitError::BackendFailure));
    store.fail_release.store(false, Ordering::SeqCst);
    limits.close_async().await.unwrap();
    assert_eq!(store.releases.load(Ordering::SeqCst), 2);
    assert!(store.available());
}

#[tokio::test(start_paused = true)]
async fn dropping_caller_future_cancels_but_keeps_cleanup_owned() {
    let store = Arc::new(RecordingStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (stopped_tx, stopped_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let owner = limits.clone();
    let work = tokio::spawn(async move {
        owner
            .with_lease(input(), renewing(), move |run| async move {
                entered_tx.send(()).unwrap();
                stopped_tx.send(run.cancelled().await).unwrap();
                finish_rx.await.unwrap();
                Ok::<(), ()>(())
            })
            .await
    });
    entered_rx.await.unwrap();
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    assert_eq!(stopped_rx.await.unwrap(), LeaseStopReason::Cancelled);
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
    assert!(!store.available());
    finish_tx.send(()).unwrap();
    limits.close_async().await.unwrap();
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
    assert!(store.available());
}

#[tokio::test(start_paused = true)]
async fn synchronous_close_reports_active_runs_and_dropped_async_waiter_still_closes() {
    let store = Arc::new(RecordingStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let shared = AcquireInput {
        capacity: 2,
        ..input()
    };
    limits.acquire(&shared).unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (stopped_tx, stopped_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let owner = limits.clone();
    let work = tokio::spawn(async move {
        owner
            .with_lease(shared, renewing(), move |run| async move {
                entered_tx.send(()).unwrap();
                stopped_tx.send(run.cancelled().await).unwrap();
                finish_rx.await.unwrap();
                Ok::<(), ()>(())
            })
            .await
    });
    entered_rx.await.unwrap();
    assert_eq!(
        limits.close().unwrap_err().failures(),
        [LimitError::ActiveRuns]
    );
    assert_eq!(stopped_rx.await.unwrap(), LeaseStopReason::Closed);
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
    // Biased polling starts close_async, then drops its waiting future while it drains.
    tokio::select! {
        biased;
        _ = limits.close_async() => panic!("business has not drained"),
        _ = tokio::task::yield_now() => {}
    }
    finish_tx.send(()).unwrap();
    let error = work.await.unwrap().unwrap_err();
    assert_eq!(
        error.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Closed)]
    );
    for _ in 0..20 {
        if store.releases.load(Ordering::SeqCst) == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    // The detached close runner released the unrelated manual lease too.
    assert_eq!(store.releases.load(Ordering::SeqCst), 2);
    limits.close_async().await.unwrap();
}

#[tokio::test]
async fn plugin_cancels_before_work_gate_drain_and_waits_before_resources_cleanup() {
    let store = Arc::new(RecordingStore::new());
    let instance = Instance::new(
        "limits",
        LimitsPlugin::new(store.clone(), FailurePolicy::Error),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let limits = app.get(&handle).unwrap();
    let guard = app.work_gate().enter().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let work = tokio::spawn(async move {
        limits
            .with_lease(input(), renewing(), move |run| async move {
                let _guard = guard;
                entered_tx.send(()).unwrap();
                assert_eq!(run.cancelled().await, LeaseStopReason::Closed);
                Ok::<(), ()>(())
            })
            .await
    });
    entered_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), app.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        work.await.unwrap().unwrap_err().failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Closed)]
    );
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
    assert!(store.available());
}

#[tokio::test]
async fn renewal_returning_after_old_deadline_cannot_claim_continuous_ownership() {
    // A frozen provider clock reproduces the distinction between provider validity
    // and our conservative local deadline when a synchronous call is descheduled.
    let mut recording = RecordingStore::from_memory(MemoryLimitStore::with_clock(|| 0));
    recording.renewal_delay = Duration::from_millis(350);
    let store = Arc::new(recording);
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        limits.with_lease(input(), renewing(), |run| async move {
            assert_eq!(run.cancelled().await, LeaseStopReason::Expired);
            Ok::<(), ()>(())
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        result.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Expired)]
    );
    assert_eq!(store.renewals.load(Ordering::SeqCst), 1);
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
    assert!(store.available());
    limits.close_async().await.unwrap();
}

#[test]
fn runtime_without_time_driver_rejects_before_acquiring_or_starting_business() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let store = Arc::new(RecordingStore::from_memory(MemoryLimitStore::with_clock(
        || 0,
    )));
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    let result = runtime
        .block_on(
            limits.with_lease(input(), LeaseRunOptions::default(), move |_| async move {
                mark.store(true, Ordering::SeqCst);
                Ok::<(), ()>(())
            }),
        )
        .unwrap_err();
    assert_eq!(result.failures, [LeaseRunFailure::RuntimeUnavailable]);
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(store.acquisitions.load(Ordering::SeqCst), 0);
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
    assert!(store.available());
    limits.close().unwrap();
}
