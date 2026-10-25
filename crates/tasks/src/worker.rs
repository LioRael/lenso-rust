use crate::{
    valid_json, Cancellation, Failure, JobStatus, StopReason, TaskContext, TaskError, Tasks,
};
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::sync::watch;

impl Tasks {
    /// Claim and await at most one due task. Host owns invocation/concurrency.
    /// The owned runner survives cancellation of its waiting caller. Shutdown
    /// drains it before releasing SQLite. A noncooperative handler can delay drain.
    ///
    /// # Errors
    /// Returns admission, provider, runtime or lost-claim errors. Handler failures
    /// are persisted in job status. An error is not proof that no business effect occurred.
    pub async fn run_one(self: &Arc<Self>) -> Result<Option<JobStatus>, TaskError> {
        self.spawn_one()?.await.map_err(|_| TaskError::Runtime)?
    }
    pub(crate) fn spawn_one(
        self: &Arc<Self>,
    ) -> Result<tokio::task::JoinHandle<Result<Option<JobStatus>, TaskError>>, TaskError> {
        let guard = self.gate.enter().map_err(|_| TaskError::Closed)?;
        let service = self.clone();
        Ok(tokio::spawn(async move {
            let _guard = guard;
            service.execute().await
        }))
    }
    async fn execute(&self) -> Result<Option<JobStatus>, TaskError> {
        let tasks: Vec<_> = self.tasks.keys().cloned().collect();
        let options = self.options.clone();
        let Some(claim) = self.database(move |db| db.claim(&tasks, &options)).await? else {
            return Ok(None);
        };
        let task = self
            .tasks
            .get(&claim.task)
            .ok_or(TaskError::UnknownTask)?
            .clone();
        let (signal, cancellation) = watch::channel(None);
        let context = TaskContext {
            job_id: claim.id,
            attempt: claim.attempt,
            cancellation: Cancellation(cancellation),
        };
        let input = claim.input.clone();
        // Poll cancellation/ownership once before invoking user code as well as during it.
        let first_claim = claim.clone();
        let lease_ms = self.options.lease_ms;
        match self
            .database(move |db| db.pulse(&first_claim, lease_ms))
            .await?
        {
            Some(false) => {}
            Some(true) => {
                let id = claim.id;
                self.database(move |db| db.settle(&claim, Err(Failure::Aborted)))
                    .await?;
                return self.database(move |db| db.get(id)).await;
            }
            None => return Err(TaskError::ClaimLost),
        }
        // Catch both a synchronous handler factory panic and a future polling panic.
        let business = AssertUnwindSafe(async move {
            if !valid_json(&input) {
                return Err(Failure::InvalidInput);
            }
            let result = (task.handler)(input, context).await?;
            if valid_json(&result) {
                Ok(result)
            } else {
                Err(Failure::InvalidResult)
            }
        })
        .catch_unwind();
        tokio::pin!(business);
        let interval = Duration::from_millis(u64::from(self.options.heartbeat_ms));
        let deadline = tokio::time::sleep(Duration::from_millis(u64::from(
            self.options.execution_timeout_ms,
        )));
        tokio::pin!(deadline);
        let mut failure = None;
        let outcome = loop {
            tokio::select! {
                result = &mut business => break result.unwrap_or(Err(Failure::HandlerFailed)),
                _ = &mut deadline, if signal.borrow().is_none() => {
                    signal.send_replace(Some(StopReason::Deadline));
                }
                _ = tokio::time::sleep(interval) => {
                    let current = claim.clone();
                    match self.database(move |db| db.pulse(&current, lease_ms)).await {
                        Ok(Some(true)) => { signal.send_if_modified(|s| { if s.is_none() { *s = Some(StopReason::Cancelled); true } else { false } }); }
                        Ok(Some(false)) => {},
                        Ok(None) => {
                            signal.send_replace(Some(StopReason::LeaseLost));
                            failure = Some(TaskError::ClaimLost);
                            break business.await.unwrap_or(Err(Failure::HandlerFailed));
                        }
                        Err(error) => {
                            signal.send_replace(Some(StopReason::ProviderUnavailable));
                            failure = Some(error);
                            break business.await.unwrap_or(Err(Failure::HandlerFailed));
                        }
                    }
                }
            }
        };
        if let Some(error) = failure {
            return Err(error);
        }
        let outcome = if signal.borrow().is_some() {
            Err(Failure::Aborted)
        } else {
            outcome
        };
        let id = claim.id;
        self.database(move |db| db.settle(&claim, outcome)).await?;
        self.database(move |db| db.get(id)).await
    }
}
