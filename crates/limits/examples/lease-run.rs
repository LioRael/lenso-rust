use lenso_limits::{
    AcquireInput, FailurePolicy, LeaseCancellation, LeaseRunFailure, LeaseRunOptions,
    LeaseStopReason, LimitScope, Limits, MemoryLimitStore,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = Arc::new(Limits::new(
        Arc::new(MemoryLimitStore::new()),
        FailurePolicy::Error,
    ));
    let cancellation = LeaseCancellation::new();
    let caller = cancellation.clone();
    let stop = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(350)).await;
        caller.cancel();
    });
    let completed_steps = Arc::new(AtomicUsize::new(0));
    let work_steps = completed_steps.clone();
    let result = limits
        .with_lease(
            AcquireInput {
                scope: LimitScope::new("reports", "tenant-a", "running")?,
                capacity: 1,
                quantity: 1,
                ttl: Duration::from_millis(250),
            },
            LeaseRunOptions {
                renew_every: Some(Duration::from_millis(100)),
                cancellation: Some(cancellation),
            },
            move |run| async move {
                loop {
                    tokio::select! {
                        biased;
                        reason = run.cancelled() => return Err::<(), _>(reason),
                        _ = tokio::time::sleep(Duration::from_millis(30)) => {
                            // Real business effects need their own idempotency/compensation.
                            work_steps.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            },
        )
        .await;
    stop.await?;
    let error = result.expect_err("explicit cancellation ends this example");
    assert_eq!(
        error.failures,
        [LeaseRunFailure::Stopped(LeaseStopReason::Cancelled)]
    );
    println!(
        "Cancelled after {} completed steps; earlier effects were not rolled back.",
        completed_steps.load(Ordering::SeqCst)
    );
    limits.close_async().await?;
    Ok(())
}
