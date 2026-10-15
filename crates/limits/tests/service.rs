use lenso_limits::{
    AcquireInput, Acquisition, ConsumeInput, CounterKind, Decision, DecisionReason, FailurePolicy,
    Lease, LeaseToken, LimitError, LimitScope, LimitStore, Limits, MemoryLimitStore,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Mutex,
    },
    thread,
    time::Duration,
};

fn input() -> AcquireInput {
    AcquireInput {
        scope: LimitScope::new("reports", "tenant-a", "running").unwrap(),
        capacity: 4,
        quantity: 2,
        ttl: Duration::from_secs(60),
    }
}
fn counter() -> ConsumeInput {
    ConsumeInput {
        scope: input().scope,
        capacity: 4,
        quantity: 1,
        period: Duration::from_secs(1),
    }
}

struct ObservedStore {
    memory: MemoryLimitStore,
    admission_error: Mutex<Option<LimitError>>,
    fail_renew: AtomicBool,
    fail_release: AtomicBool,
    release_calls: AtomicUsize,
}
impl ObservedStore {
    fn new() -> Self {
        Self {
            memory: MemoryLimitStore::with_clock(|| 0),
            admission_error: Mutex::new(None),
            fail_renew: AtomicBool::new(false),
            fail_release: AtomicBool::new(false),
            release_calls: AtomicUsize::new(0),
        }
    }
}
impl LimitStore for ObservedStore {
    fn consume(&self, kind: CounterKind, input: &ConsumeInput) -> Result<Decision, LimitError> {
        if let Some(error) = *self.admission_error.lock().unwrap() {
            return Err(error);
        }
        self.memory.consume(kind, input)
    }
    fn acquire(&self, input: &AcquireInput, token: LeaseToken) -> Result<Acquisition, LimitError> {
        if let Some(error) = *self.admission_error.lock().unwrap() {
            return Err(error);
        }
        self.memory.acquire(input, token)
    }
    fn renew(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
        ttl: Duration,
    ) -> Result<Option<Lease>, LimitError> {
        if self.fail_renew.load(Ordering::SeqCst) {
            return Err(LimitError::BackendFailure);
        }
        self.memory.renew(scope, token, ttl)
    }
    fn release(&self, scope: &LimitScope, token: LeaseToken) -> Result<(), LimitError> {
        self.release_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(LimitError::BackendFailure);
        }
        self.memory.release(scope, token)
    }
}

#[test]
fn only_backend_admission_faults_apply_the_explicit_failure_policy() {
    for policy in [
        FailurePolicy::Error,
        FailurePolicy::Deny,
        FailurePolicy::Allow,
    ] {
        let store = Arc::new(ObservedStore::new());
        *store.admission_error.lock().unwrap() = Some(LimitError::BackendFailure);
        let limits = Limits::new(store.clone(), policy);
        if policy == FailurePolicy::Error {
            assert_eq!(
                limits.consume_rate(&counter()),
                Err(LimitError::BackendFailure)
            );
            assert_eq!(
                limits.consume_quota(&counter()),
                Err(LimitError::BackendFailure)
            );
            assert_eq!(limits.acquire(&input()), Err(LimitError::BackendFailure));
        } else {
            let expected = Decision {
                allowed: policy == FailurePolicy::Allow,
                remaining: None,
                retry_after: None,
                reason: DecisionReason::BackendFailure,
            };
            assert_eq!(limits.consume_rate(&counter()).unwrap(), expected);
            assert_eq!(limits.consume_quota(&counter()).unwrap(), expected);
            let acquired = limits.acquire(&input()).unwrap();
            assert_eq!(acquired.decision, expected);
            assert!(acquired.lease.is_none());
        }
        for error in [
            LimitError::InvalidInput,
            LimitError::PolicyConflict,
            LimitError::Closed,
            LimitError::Internal,
        ] {
            *store.admission_error.lock().unwrap() = Some(error);
            assert_eq!(limits.consume_rate(&counter()), Err(error));
            assert_eq!(limits.acquire(&input()), Err(error));
        }
        limits.close().unwrap();
        assert_eq!(store.release_calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn renewal_and_release_errors_stay_visible_and_close_retries_failed_release() {
    let store = Arc::new(ObservedStore::new());
    let limits = Limits::new(store.clone(), FailurePolicy::Allow);
    let lease = limits.acquire(&input()).unwrap().lease.unwrap();
    store.fail_renew.store(true, Ordering::SeqCst);
    store.fail_release.store(true, Ordering::SeqCst);
    assert_eq!(
        limits.renew(&lease, Duration::from_secs(30)),
        Err(LimitError::BackendFailure)
    );
    assert_eq!(limits.release(&lease), Err(LimitError::BackendFailure));
    store.fail_release.store(false, Ordering::SeqCst);
    limits.close().unwrap();
    assert_eq!(store.release_calls.load(Ordering::SeqCst), 2);
    assert!(
        store
            .acquire(
                &AcquireInput {
                    quantity: 4,
                    ..input()
                },
                LeaseToken::fresh().unwrap()
            )
            .unwrap()
            .decision
            .allowed
    );
}

#[test]
fn close_aggregates_every_release_error_and_repeated_callers_share_failure() {
    let store = Arc::new(ObservedStore::new());
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Allow));
    limits.acquire(&input()).unwrap();
    limits.acquire(&input()).unwrap();
    store.fail_release.store(true, Ordering::SeqCst);
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let limits = limits.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                limits.close().unwrap_err()
            })
        })
        .collect();
    for handle in handles {
        assert_eq!(
            handle.join().unwrap().failures(),
            &[LimitError::BackendFailure; 2]
        );
    }
    assert!(limits.is_closed());
    assert_eq!(store.release_calls.load(Ordering::SeqCst), 2);
    assert_eq!(limits.acquire(&input()), Err(LimitError::Closed));
    assert_eq!(limits.consume_rate(&counter()), Err(LimitError::Closed));
    store.fail_release.store(false, Ordering::SeqCst);
    assert_eq!(
        limits.close().unwrap_err().failures(),
        &[LimitError::BackendFailure; 2]
    );
    assert_eq!(store.release_calls.load(Ordering::SeqCst), 2);
}

#[test]
fn closing_one_owner_preserves_other_leases_and_the_borrowed_store() {
    let store = Arc::new(ObservedStore::new());
    let first = Limits::new(store.clone(), FailurePolicy::Error);
    let second = Limits::new(store.clone(), FailurePolicy::Error);
    let first_lease = first.acquire(&input()).unwrap().lease.unwrap();
    let second_lease = second.acquire(&input()).unwrap().lease.unwrap();
    // Renewing another owner's lease does not transfer ownership.
    first
        .renew(&second_lease, Duration::from_secs(60))
        .unwrap()
        .unwrap();
    first.close().unwrap();
    assert!(first.is_closed());
    assert_eq!(
        first.renew(&first_lease, input().ttl),
        Err(LimitError::Closed)
    );
    assert_eq!(first.release(&first_lease), Err(LimitError::Closed));
    assert_eq!(store.release_calls.load(Ordering::SeqCst), 1);
    let replacement = second.acquire(&input()).unwrap().lease.unwrap();
    assert_ne!(replacement.token, first_lease.token);
    assert!(!second.acquire(&input()).unwrap().decision.allowed);
    assert!(second.renew(&second_lease, input().ttl).unwrap().is_some());
    second.close().unwrap();
    assert_eq!(store.release_calls.load(Ordering::SeqCst), 3);
    assert!(
        store
            .acquire(
                &AcquireInput {
                    quantity: 4,
                    ..input()
                },
                LeaseToken::fresh().unwrap()
            )
            .unwrap()
            .decision
            .allowed
    );
}

struct PausedAcquire {
    memory: MemoryLimitStore,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}
impl LimitStore for PausedAcquire {
    fn consume(&self, kind: CounterKind, input: &ConsumeInput) -> Result<Decision, LimitError> {
        self.memory.consume(kind, input)
    }
    fn acquire(&self, input: &AcquireInput, token: LeaseToken) -> Result<Acquisition, LimitError> {
        let result = self.memory.acquire(input, token)?;
        self.entered.send(()).unwrap();
        self.resume
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        Ok(result)
    }
    fn renew(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
        ttl: Duration,
    ) -> Result<Option<Lease>, LimitError> {
        self.memory.renew(scope, token, ttl)
    }
    fn release(&self, scope: &LimitScope, token: LeaseToken) -> Result<(), LimitError> {
        self.memory.release(scope, token)
    }
}

#[test]
fn close_seals_while_waiting_for_inflight_acquisition_then_releases_it() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(PausedAcquire {
        memory: MemoryLimitStore::with_clock(|| 0),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    });
    let limits = Arc::new(Limits::new(store.clone(), FailurePolicy::Error));
    let caller = limits.clone();
    let acquisition = thread::spawn(move || caller.acquire(&input()));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let closer = limits.clone();
    let (closed_tx, closed_rx) = mpsc::channel();
    let close = thread::spawn(move || {
        let result = closer.close();
        closed_tx.send(()).unwrap();
        result
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !limits.is_closed() && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(limits.is_closed());
    assert!(closed_rx.try_recv().is_err());
    assert_eq!(limits.acquire(&input()), Err(LimitError::Closed));
    assert_eq!(limits.consume_rate(&counter()), Err(LimitError::Closed));
    resume_tx.send(()).unwrap();
    assert!(acquisition.join().unwrap().unwrap().lease.is_some());
    close.join().unwrap().unwrap();
    assert!(
        store
            .memory
            .acquire(
                &AcquireInput {
                    quantity: 4,
                    ..input()
                },
                LeaseToken::fresh().unwrap()
            )
            .unwrap()
            .decision
            .allowed
    );
}
