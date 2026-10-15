use crate::{
    types::duration_ms, AcquireInput, Acquisition, CloseError, ConsumeInput, CounterKind, Decision,
    DecisionReason, FailurePolicy, Lease, LeaseCancellation, LeaseObservation, LeaseStopReason,
    LeaseToken, LimitError, LimitStore, MemoryLimitStore,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

#[derive(Default)]
struct State {
    owned: HashMap<LeaseToken, Lease>,
    closed: Option<Result<(), CloseError>>,
    runs: HashMap<LeaseToken, LeaseCancellation>,
    runs_stopping: bool,
}

/// Standalone service borrowing a store. Share the service explicitly via `Arc`.
///
/// Calls and close are serialized so close waits for in-flight acquisitions and
/// releases precisely this service's leases. Manual APIs need no task, timer or runtime;
/// `with_lease` and `close_async` add opt-in Tokio execution ownership.
/// Dropping a service does not close it; call `close` or use `LimitsPlugin`.
pub struct Limits<S: LimitStore = MemoryLimitStore> {
    store: Arc<S>,
    policy: FailurePolicy,
    closing: AtomicBool,
    state: Mutex<State>,
    run_count: tokio::sync::watch::Sender<usize>,
}

impl<S: LimitStore> Limits<S> {
    pub fn new(store: Arc<S>, policy: FailurePolicy) -> Self {
        Self {
            store,
            policy,
            closing: AtomicBool::new(false),
            state: Mutex::new(State::default()),
            run_count: tokio::sync::watch::channel(0).0,
        }
    }

    pub fn consume_rate(&self, input: &ConsumeInput) -> Result<Decision, LimitError> {
        self.consume(CounterKind::Rate, input)
    }

    pub fn consume_quota(&self, input: &ConsumeInput) -> Result<Decision, LimitError> {
        self.consume(CounterKind::Quota, input)
    }

    fn consume(&self, kind: CounterKind, input: &ConsumeInput) -> Result<Decision, LimitError> {
        input.validate()?;
        let _operation = self.open()?;
        self.store
            .consume(kind, input)
            .or_else(|error| self.failure(error))
    }

    pub fn acquire(&self, input: &AcquireInput) -> Result<Acquisition, LimitError> {
        input.validate()?;
        let mut state = self.open()?;
        let token = LeaseToken::fresh()?;
        match self.store.acquire(input, token) {
            Ok(acquisition) => {
                if let Some(lease) = &acquisition.lease {
                    state.owned.insert(lease.token, lease.clone());
                }
                Ok(acquisition)
            }
            Err(error) => Ok(Acquisition {
                decision: self.failure(error)?,
                lease: None,
            }),
        }
    }

    /// Renewal never applies fail-open/fail-closed admission policy.
    pub fn renew(&self, lease: &Lease, ttl: Duration) -> Result<Option<Lease>, LimitError> {
        duration_ms(ttl)?;
        let mut state = self.open()?;
        check_scope(&state, lease)?;
        let renewed = self.store.renew(&lease.scope, lease.token, ttl)?;
        if let Entry::Occupied(mut entry) = state.owned.entry(lease.token) {
            if let Some(renewed) = &renewed {
                entry.insert(renewed.clone());
            } else {
                entry.remove();
            }
        }
        Ok(renewed)
    }

    /// Idempotent; failures remain visible and retain ownership for close to retry.
    pub fn release(&self, lease: &Lease) -> Result<(), LimitError> {
        let mut state = self.open()?;
        check_scope(&state, lease)?;
        self.store.release(&lease.scope, lease.token)?;
        state.owned.remove(&lease.token);
        Ok(())
    }

    /// Stop admission and release owned leases, leaving the borrowed store usable.
    ///
    /// Concurrent/repeated callers share one result, including all release errors.
    /// With active wrappers, requests cancellation and returns `ActiveRuns` without
    /// releasing any leases; use `close_async` to drain and finish. This transitional
    /// result is not cached. A final release failure is not silently retried. Remaining leases
    /// expire by TTL or can be released directly through the borrowed store.
    pub fn close(&self) -> Result<(), CloseError> {
        // Seal immediately, even if a store call still owns the state lock.
        self.closing.store(true, Ordering::Release);
        let mut state = self.state.lock().map_err(|_| CloseError {
            failures: vec![LimitError::Internal],
        })?;
        if let Some(result) = &state.closed {
            return result.clone();
        }
        state.runs_stopping = true;
        for cancellation in state.runs.values() {
            cancellation.stop(LeaseStopReason::Closed);
        }
        if !state.runs.is_empty() {
            return Err(CloseError {
                failures: vec![LimitError::ActiveRuns],
            });
        }
        // No operation can enter while this guard is held.
        state.closed = Some(Ok(()));
        let mut failures = Vec::new();
        state.owned.retain(|_, lease| {
            if let Err(error) = self.store.release(&lease.scope, lease.token) {
                failures.push(error);
                true
            } else {
                false
            }
        });
        let result = if failures.is_empty() {
            Ok(())
        } else {
            Err(CloseError { failures })
        };
        state.closed = Some(result.clone());
        result
    }

    /// Cancel executions and drain them before releasing leases. Dropping this waiter
    /// does not cancel the owned close task; keep the Tokio runtime alive to finish.
    /// Call from outside the execution being drained to avoid waiting on yourself.
    pub async fn close_async(self: &Arc<Self>) -> Result<(), CloseError> {
        match self.close() {
            Err(error) if error.failures() == [LimitError::ActiveRuns] => {}
            result => return result,
        }
        let limits = self.clone();
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| CloseError {
            failures: vec![LimitError::Internal],
        })?;
        runtime
            .spawn(async move {
                let mut count = limits.run_count.subscribe();
                while *count.borrow_and_update() != 0 {
                    count.changed().await.map_err(|_| CloseError {
                        failures: vec![LimitError::Internal],
                    })?;
                }
                limits.close()
            })
            .await
            .map_err(|_| CloseError {
                failures: vec![LimitError::Internal],
            })?
    }

    /// Stop admitting wrappers and signal existing ones, without closing manual APIs.
    /// Used at the plugin's Entrances phase so WorkGate drain can make progress.
    pub fn stop_runs(&self) -> Result<(), LimitError> {
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        state.runs_stopping = true;
        for cancellation in state.runs.values() {
            cancellation.stop(LeaseStopReason::Closed);
        }
        Ok(())
    }

    pub(crate) fn register_run(
        &self,
        cancellation: LeaseCancellation,
    ) -> Result<LeaseToken, LimitError> {
        let mut state = self.open()?;
        if state.runs_stopping {
            return Err(LimitError::Closed);
        }
        let id = LeaseToken::fresh()?;
        state.runs.insert(id, cancellation);
        self.run_count.send_replace(state.runs.len());
        Ok(id)
    }

    pub(crate) fn finish_run(&self, id: LeaseToken) {
        if let Ok(mut state) = self.state.lock() {
            state.runs.remove(&id);
            self.run_count.send_replace(state.runs.len());
        }
    }

    pub(crate) fn observe_run(
        &self,
        lease: &Lease,
    ) -> Result<Option<LeaseObservation>, LimitError> {
        let _state = self.state.lock().map_err(|_| LimitError::Internal)?;
        self.store.observe(&lease.scope, lease.token)
    }

    pub(crate) fn release_run(&self, lease: &Lease) -> Result<(), LimitError> {
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        self.store.release(&lease.scope, lease.token)?;
        state.owned.remove(&lease.token);
        Ok(())
    }

    /// True as soon as close seals admission, including while releases are pending.
    pub fn is_closed(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    fn open(&self) -> Result<MutexGuard<'_, State>, LimitError> {
        if self.is_closed() {
            return Err(LimitError::Closed);
        }
        let state = self.state.lock().map_err(|_| LimitError::Internal)?;
        if self.is_closed() {
            return Err(LimitError::Closed);
        }
        Ok(state)
    }

    fn failure(&self, error: LimitError) -> Result<Decision, LimitError> {
        if error != LimitError::BackendFailure || self.policy == FailurePolicy::Error {
            return Err(error);
        }
        Ok(Decision {
            allowed: self.policy == FailurePolicy::Allow,
            remaining: None,
            retry_after: None,
            reason: DecisionReason::BackendFailure,
        })
    }
}

fn check_scope(state: &State, lease: &Lease) -> Result<(), LimitError> {
    if state
        .owned
        .get(&lease.token)
        .is_some_and(|previous| previous.scope != lease.scope)
    {
        Err(LimitError::InvalidInput)
    } else {
        Ok(())
    }
}
