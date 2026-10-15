use lenso_limits::{
    AcquireInput, ConsumeInput, FailurePolicy, LimitScope, Limits, MemoryLimitStore,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = Arc::new(AtomicU64::new(999));
    let clock = now.clone();
    let store = Arc::new(MemoryLimitStore::with_clock(move || {
        clock.load(Ordering::Relaxed)
    }));
    let limits = Limits::new(store, FailurePolicy::Error);
    let scope = LimitScope::new("reports", "tenant-a", "subject:123")?;
    let rate = ConsumeInput {
        scope: scope.clone(),
        capacity: 2,
        quantity: 2,
        period: Duration::from_secs(1),
    };
    assert!(limits.consume_rate(&rate)?.allowed);
    println!("rate exhausted: {:?}", limits.consume_rate(&rate)?);
    now.store(1000, Ordering::Relaxed);
    assert!(limits.consume_rate(&rate)?.allowed);
    assert!(limits.consume_quota(&rate)?.allowed);

    let work = AcquireInput {
        scope,
        capacity: 3,
        quantity: 2,
        ttl: Duration::from_secs(10),
    };
    let lease = limits.acquire(&work)?.lease.unwrap();
    let denied = limits.acquire(&work)?;
    assert!(!denied.decision.allowed);
    println!("weighted lease exhausted: {:?}", denied.decision);
    assert!(limits.renew(&lease, Duration::from_secs(20))?.is_some());
    limits.release(&lease)?;
    assert!(limits.acquire(&work)?.decision.allowed);
    limits.close()?;
    assert!(limits.is_closed());
    Ok(())
}
