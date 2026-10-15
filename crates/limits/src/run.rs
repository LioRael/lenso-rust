use crate::{
    types::duration_ms, AcquireInput, Decision, Lease, LeaseToken, LimitError, LimitStore, Limits,
};
use std::{error::Error, fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinError, time::Instant};

/// First observed reason an execution was asked to stop. Cancellation is cooperative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseStopReason {
    Cancelled,
    Closed,
    Expired,
    Lost,
    RenewalFailed(LimitError),
    InspectionFailed(LimitError),
}

/// Cloneable explicit cancellation signal; cancelling is idempotent and first-wins.
#[derive(Clone)]
pub struct LeaseCancellation {
    state: watch::Sender<Option<LeaseStopReason>>,
}
impl Default for LeaseCancellation {
    fn default() -> Self {
        Self::new()
    }
}
impl LeaseCancellation {
    pub fn new() -> Self {
        Self {
            state: watch::channel(None).0,
        }
    }
    pub fn cancel(&self) {
        self.stop(LeaseStopReason::Cancelled);
    }
    pub fn reason(&self) -> Option<LeaseStopReason> {
        *self.state.borrow()
    }
    pub fn check(&self) -> Result<(), LeaseStopReason> {
        self.reason().map_or(Ok(()), Err)
    }
    pub async fn cancelled(&self) -> LeaseStopReason {
        let mut receiver = self.state.subscribe();
        loop {
            if let Some(reason) = *receiver.borrow_and_update() {
                return reason;
            }
            // `self` retains a sender throughout this wait.
            if receiver.changed().await.is_err() {
                return LeaseStopReason::Closed;
            }
        }
    }
    pub(crate) fn stop(&self, reason: LeaseStopReason) {
        self.state.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(reason);
            true
        });
    }
}
impl fmt::Debug for LeaseCancellation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LeaseCancellation")
            .field(&self.reason())
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub struct LeaseRunOptions {
    /// None means no automatic renewal. Must be positive, whole milliseconds and < TTL.
    pub renew_every: Option<Duration>,
    pub cancellation: Option<LeaseCancellation>,
}

/// Execution context. A lease snapshot never promises future validity or fences effects.
#[derive(Clone)]
pub struct LeaseExecution {
    pub cancellation: LeaseCancellation,
    snapshot: watch::Receiver<Option<Lease>>,
}
impl LeaseExecution {
    /// Latest observed snapshot; None for fail-open admission or observed cancellation.
    /// Clocks can jump and cancellation can race this read. Do not treat this as a lock
    /// or an authorization to perform an irreversible effect.
    pub fn lease_snapshot(&self) -> Option<Lease> {
        if self.cancellation.reason().is_some() || self.snapshot.has_changed().is_err() {
            None
        } else {
            self.snapshot.borrow().clone()
        }
    }
    pub async fn cancelled(&self) -> LeaseStopReason {
        self.cancellation.cancelled().await
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseRunFailure {
    InvalidInput(LimitError),
    Admission(LimitError),
    Denied(Decision),
    Stopped(LeaseStopReason),
    BusinessPanicked,
    BusinessAborted,
    RuntimeUnavailable,
    RunnerAborted,
}

/// Preserves business and release failures alongside cancellation/lease failures.
/// Formatting intentionally does not print the generic business error or provider data.
pub struct LeaseRunError<E> {
    pub failures: Vec<LeaseRunFailure>,
    pub business_error: Option<E>,
    pub release_error: Option<LimitError>,
}
impl<E> fmt::Display for LeaseRunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Lease execution failed ({} lifecycle failures, business error: {}, release error: {})",
            self.failures.len(),
            self.business_error.is_some(),
            self.release_error.is_some()
        )
    }
}
impl<E> fmt::Debug for LeaseRunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl<E> Error for LeaseRunError<E> {}

fn failed<E>(failure: LeaseRunFailure) -> LeaseRunError<E> {
    LeaseRunError {
        failures: vec![failure],
        business_error: None,
        release_error: None,
    }
}

struct CancelOnDrop(Option<LeaseCancellation>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.0 {
            cancellation.cancel();
        }
    }
}
struct Registration<S: LimitStore> {
    limits: Arc<Limits<S>>,
    id: LeaseToken,
}
impl<S: LimitStore> Drop for Registration<S> {
    fn drop(&mut self) {
        self.limits.finish_run(self.id);
    }
}

impl<S: LimitStore> Limits<S> {
    /// Execute with a tracked lease and optional explicit renewal. Requires Tokio.
    ///
    /// The owned runner survives a dropped caller future, signals cancellation, drains
    /// cooperative business work and releases the lease. A callback which ignores the
    /// signal can delay close indefinitely. No callback is forcibly aborted; external
    /// effects are never rolled back. Do not await this service's close from its callback.
    pub async fn with_lease<F, Fut, T, E>(
        self: &Arc<Self>,
        input: AcquireInput,
        options: LeaseRunOptions,
        execute: F,
    ) -> Result<T, LeaseRunError<E>>
    where
        F: FnOnce(LeaseExecution) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        input
            .validate()
            .map_err(|error| failed(LeaseRunFailure::InvalidInput(error)))?;
        if let Some(every) = options.renew_every {
            duration_ms(every).map_err(|error| failed(LeaseRunFailure::InvalidInput(error)))?;
            if every >= input.ttl {
                return Err(failed(LeaseRunFailure::InvalidInput(
                    LimitError::InvalidInput,
                )));
            }
        }
        if let Some(reason) = options
            .cancellation
            .as_ref()
            .and_then(LeaseCancellation::reason)
        {
            return Err(failed(LeaseRunFailure::Stopped(reason)));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| failed(LeaseRunFailure::RuntimeUnavailable))?;
        // Tokio exposes no fallible timer constructor. Probe before admission so a
        // runtime built without `enable_time` cannot strand a lease or business task.
        if std::panic::catch_unwind(|| tokio::time::sleep(Duration::ZERO)).is_err() {
            return Err(failed(LeaseRunFailure::RuntimeUnavailable));
        }
        let cancellation = LeaseCancellation::new();
        let id = self
            .register_run(cancellation.clone())
            .map_err(|error| failed(LeaseRunFailure::Admission(error)))?;
        let registration = Registration {
            limits: self.clone(),
            id,
        };
        let limits = self.clone();
        let mut waiter = CancelOnDrop(Some(cancellation.clone()));
        let task = runtime.spawn(async move {
            let _registration = registration;
            let mut lease = None;
            let (outcome, failures) = limits
                .drive_run(input, options, execute, cancellation, &mut lease)
                .await;
            let release_error = lease
                .as_ref()
                .and_then(|lease| limits.release_run(lease).err());
            match outcome {
                Some(Ok(value)) if failures.is_empty() && release_error.is_none() => Ok(value),
                outcome => Err(LeaseRunError {
                    failures,
                    business_error: outcome.and_then(Result::err),
                    release_error,
                }),
            }
        });
        let result = task
            .await
            .map_err(|_| failed(LeaseRunFailure::RunnerAborted))?;
        waiter.0 = None;
        result
    }

    async fn drive_run<F, Fut, T, E>(
        &self,
        input: AcquireInput,
        options: LeaseRunOptions,
        execute: F,
        cancellation: LeaseCancellation,
        lease: &mut Option<Lease>,
    ) -> (Option<Result<T, E>>, Vec<LeaseRunFailure>)
    where
        F: FnOnce(LeaseExecution) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        if let Some(reason) = stopped(&cancellation, &options.cancellation) {
            return (None, vec![LeaseRunFailure::Stopped(reason)]);
        }
        let acquisition_start = Instant::now();
        let acquisition = match self.acquire(&input) {
            Ok(acquisition) => acquisition,
            Err(error) => return (None, vec![LeaseRunFailure::Admission(error)]),
        };
        *lease = acquisition.lease;
        if !acquisition.decision.allowed {
            return (None, vec![LeaseRunFailure::Denied(acquisition.decision)]);
        }
        let mut deadline = match self.observe_deadline(lease) {
            Ok(deadline) => deadline,
            Err(reason) => return (None, vec![LeaseRunFailure::Stopped(reason)]),
        };
        if let Some(observed) = deadline {
            deadline = Some(observed.min(acquisition_start + input.ttl));
        }
        if expired(deadline) {
            return (
                None,
                vec![LeaseRunFailure::Stopped(LeaseStopReason::Expired)],
            );
        }
        if let Some(reason) = stopped(&cancellation, &options.cancellation) {
            return (None, vec![LeaseRunFailure::Stopped(reason)]);
        }
        let (snapshots, snapshot) = watch::channel(lease.clone());
        let context = LeaseExecution {
            cancellation: cancellation.clone(),
            snapshot,
        };
        let future = match std::panic::catch_unwind(AssertUnwindSafe(|| execute(context))) {
            Ok(future) => future,
            Err(_) => return (None, vec![LeaseRunFailure::BusinessPanicked]),
        };
        if expired(deadline) {
            cancellation.stop(LeaseStopReason::Expired);
            return (
                None,
                vec![LeaseRunFailure::Stopped(LeaseStopReason::Expired)],
            );
        }
        let mut business = tokio::spawn(future);
        let mut next_renewal = renewal_time(options.renew_every, lease.is_some());
        loop {
            if let Some(reason) = stopped(&cancellation, &options.cancellation) {
                cancellation.stop(reason);
                return joined(business.await, Some(reason));
            }
            tokio::select! {
                biased;
                reason = cancellation.cancelled() => { cancellation.stop(reason); }
                reason = external_cancelled(&options.cancellation) => { cancellation.stop(reason); }
                _ = at(deadline) => { cancellation.stop(LeaseStopReason::Expired); }
                _ = at(next_renewal) => {
                    // If the executor was stalled, never use a late renewal to claim continuity.
                    if expired(deadline) {
                        cancellation.stop(LeaseStopReason::Expired);
                        continue;
                    }
                    if let Some(current) = lease.as_ref() {
                        let renewal_start = Instant::now();
                        match self.renew(current, input.ttl) {
                            Ok(Some(renewed)) => *lease = Some(renewed),
                            Ok(None) => { cancellation.stop(LeaseStopReason::Lost); continue; }
                            Err(LimitError::Closed) => { cancellation.stop(LeaseStopReason::Closed); continue; }
                            Err(error) => { cancellation.stop(LeaseStopReason::RenewalFailed(error)); continue; }
                        }
                        // A successful provider response cannot prove continuity across
                        // an already elapsed local deadline (including OS descheduling).
                        if expired(deadline) {
                            cancellation.stop(LeaseStopReason::Expired);
                            continue;
                        }
                        match self.observe_deadline(lease) {
                            Ok(observed) => {
                                if expired(deadline) {
                                    cancellation.stop(LeaseStopReason::Expired);
                                    continue;
                                }
                                deadline = observed.map(|observed| observed.min(renewal_start + input.ttl));
                            }
                            Err(reason) => { cancellation.stop(reason); continue; }
                        }
                        snapshots.send_replace(lease.clone());
                        next_renewal = renewal_time(options.renew_every, true);
                    }
                }
                result = &mut business => {
                    let mut reason = stopped(&cancellation, &options.cancellation);
                    if reason.is_none() && expired(deadline) {
                        reason = Some(LeaseStopReason::Expired);
                    }
                    // Final provider observation catches clock jumps/loss even if no timer fired.
                    if reason.is_none() {
                        if let Err(observed) = self.observe_deadline(lease) { reason = Some(observed); }
                    }
                    if reason.is_none() && expired(deadline) {
                        reason = Some(LeaseStopReason::Expired);
                    }
                    if let Some(reason) = reason { cancellation.stop(reason); }
                    return joined(result, reason);
                }
            }
        }
    }

    fn observe_deadline(
        &self,
        lease: &mut Option<Lease>,
    ) -> Result<Option<Instant>, LeaseStopReason> {
        let Some(current) = lease.as_ref() else {
            return Ok(None);
        };
        // Take the local baseline before the provider call for a conservative deadline.
        let before = Instant::now();
        let observed = self
            .observe_run(current)
            .map_err(LeaseStopReason::InspectionFailed)?
            .ok_or(LeaseStopReason::Lost)?;
        if observed.remaining.is_zero() {
            return Err(LeaseStopReason::Expired);
        }
        let deadline = before
            .checked_add(observed.remaining)
            .ok_or(LeaseStopReason::InspectionFailed(LimitError::InvalidInput))?;
        if Instant::now() >= deadline {
            return Err(LeaseStopReason::Expired);
        }
        *lease = Some(observed.lease);
        Ok(Some(deadline))
    }
}

fn expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

fn renewal_time(every: Option<Duration>, has_lease: bool) -> Option<Instant> {
    if has_lease {
        every.map(|every| Instant::now() + every)
    } else {
        None
    }
}
fn stopped(
    internal: &LeaseCancellation,
    external: &Option<LeaseCancellation>,
) -> Option<LeaseStopReason> {
    internal
        .reason()
        .or_else(|| external.as_ref().and_then(LeaseCancellation::reason))
}
async fn at(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
async fn external_cancelled(external: &Option<LeaseCancellation>) -> LeaseStopReason {
    match external {
        Some(external) => external.cancelled().await,
        None => std::future::pending().await,
    }
}
fn joined<T, E>(
    result: Result<Result<T, E>, JoinError>,
    reason: Option<LeaseStopReason>,
) -> (Option<Result<T, E>>, Vec<LeaseRunFailure>) {
    let mut failures: Vec<_> = reason.into_iter().map(LeaseRunFailure::Stopped).collect();
    let outcome = match result {
        Ok(outcome) => Some(outcome),
        Err(error) => {
            failures.push(if error.is_panic() {
                LeaseRunFailure::BusinessPanicked
            } else {
                LeaseRunFailure::BusinessAborted
            });
            None
        }
    };
    (outcome, failures)
}
