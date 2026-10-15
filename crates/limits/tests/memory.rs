use lenso_limits::{
    AcquireInput, ConsumeInput, CounterKind, Decision, DecisionReason, FailurePolicy, LeaseToken,
    LimitError, LimitScope, LimitStore, Limits, MemoryLimitStore, MAX_DURATION_MS, MAX_QUANTITY,
    MAX_TIMESTAMP_MS,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier,
    },
    thread,
    time::Duration,
};

fn scope() -> LimitScope {
    LimitScope::new("reports", "tenant-a", "subject:123").unwrap()
}
fn consume() -> ConsumeInput {
    ConsumeInput {
        scope: scope(),
        capacity: 5,
        quantity: 1,
        period: Duration::from_millis(1000),
    }
}
fn acquire() -> AcquireInput {
    AcquireInput {
        scope: scope(),
        capacity: 5,
        quantity: 2,
        ttl: Duration::from_millis(1000),
    }
}
fn clocked(now: u64) -> (Arc<AtomicU64>, Arc<MemoryLimitStore>) {
    let clock = Arc::new(AtomicU64::new(now));
    let source = clock.clone();
    let store = Arc::new(MemoryLimitStore::with_clock(move || {
        source.load(Ordering::SeqCst)
    }));
    (clock, store)
}

#[test]
fn fixed_epoch_windows_denials_and_backward_clock() {
    let (clock, store) = clocked(999);
    let limits = Limits::new(store, FailurePolicy::Error);
    let full = ConsumeInput {
        quantity: 5,
        ..consume()
    };
    assert_eq!(
        limits.consume_rate(&full).unwrap(),
        Decision {
            allowed: true,
            remaining: Some(0),
            retry_after: Some(Duration::ZERO),
            reason: DecisionReason::Allowed,
        }
    );
    assert_eq!(
        limits.consume_rate(&consume()).unwrap(),
        Decision {
            allowed: false,
            remaining: Some(0),
            retry_after: Some(Duration::from_millis(1)),
            reason: DecisionReason::Exhausted,
        }
    );
    clock.store(1000, Ordering::SeqCst);
    assert_eq!(limits.consume_rate(&consume()).unwrap().remaining, Some(4));
    clock.store(999, Ordering::SeqCst);
    assert_eq!(limits.consume_rate(&consume()).unwrap().remaining, Some(3));
    clock.store(1500, Ordering::SeqCst);
    assert_eq!(
        limits
            .consume_rate(&ConsumeInput {
                quantity: 4,
                ..consume()
            })
            .unwrap(),
        Decision {
            allowed: false,
            remaining: Some(3),
            retry_after: Some(Duration::from_millis(500)),
            reason: DecisionReason::Exhausted,
        }
    );
    let too_large = limits
        .consume_rate(&ConsumeInput {
            quantity: MAX_QUANTITY,
            ..consume()
        })
        .unwrap();
    assert_eq!(too_large.remaining, Some(3));
    assert_eq!(too_large.reason, DecisionReason::TooLarge);
    assert_eq!(too_large.retry_after, None);
    clock.store(9000, Ordering::SeqCst);
    assert_eq!(limits.consume_rate(&consume()).unwrap().remaining, Some(4));
    limits.close().unwrap();
}

#[test]
fn counter_kinds_structured_scopes_and_store_objects_are_isolated() {
    let (_, store) = clocked(100);
    let limits = Limits::new(store.clone(), FailurePolicy::Deny);
    let same_store = Limits::new(store, FailurePolicy::Deny);
    let full = ConsumeInput {
        quantity: 5,
        ..consume()
    };
    limits.consume_rate(&full).unwrap();
    assert!(!same_store.consume_rate(&consume()).unwrap().allowed);
    assert_eq!(limits.consume_quota(&consume()).unwrap().remaining, Some(4));
    for other in [
        LimitScope::new("other", "tenant-a", "subject:123").unwrap(),
        LimitScope::new("reports", "tenant-b", "subject:123").unwrap(),
        LimitScope::new("reports", "tenant-a", "other").unwrap(),
    ] {
        assert!(
            limits
                .consume_rate(&ConsumeInput {
                    scope: other,
                    ..consume()
                })
                .unwrap()
                .allowed
        );
    }
    let a = LimitScope::new("a:b", "c", "d").unwrap();
    let b = LimitScope::new("a", "b:c", "d").unwrap();
    limits
        .consume_rate(&ConsumeInput { scope: a, ..full })
        .unwrap();
    assert!(
        limits
            .consume_rate(&ConsumeInput {
                scope: b,
                ..consume()
            })
            .unwrap()
            .allowed
    );
    let other_store = Limits::new(Arc::new(MemoryLimitStore::new()), FailurePolicy::Error);
    assert!(other_store.consume_rate(&consume()).unwrap().allowed);
    limits.close().unwrap();
    same_store.close().unwrap();
    other_store.close().unwrap();
}

#[test]
fn weighted_retry_waits_for_enough_units_not_just_the_first_expiry() {
    let (clock, store) = clocked(100);
    let limits = Limits::new(store, FailurePolicy::Error);
    let first = limits.acquire(&acquire()).unwrap().lease.unwrap();
    clock.store(500, Ordering::SeqCst);
    let second = limits.acquire(&acquire()).unwrap().lease.unwrap();
    assert_ne!(first.token, second.token);
    let blocked = limits
        .acquire(&AcquireInput {
            quantity: 4,
            ..acquire()
        })
        .unwrap();
    assert_eq!(
        blocked.decision,
        Decision {
            allowed: false,
            remaining: Some(1),
            retry_after: Some(Duration::from_millis(1000)),
            reason: DecisionReason::Exhausted,
        }
    );
    assert!(blocked.lease.is_none());
    let oversized = limits
        .acquire(&AcquireInput {
            quantity: 6,
            ..acquire()
        })
        .unwrap();
    assert_eq!(oversized.decision.reason, DecisionReason::TooLarge);
    assert_eq!(oversized.decision.retry_after, None);
    assert!(oversized.lease.is_none());
    clock.store(1100, Ordering::SeqCst);
    let third = limits.acquire(&acquire()).unwrap().lease.unwrap();
    assert!(limits
        .renew(&first, Duration::from_secs(1))
        .unwrap()
        .is_none());
    limits.release(&first).unwrap();
    limits.release(&first).unwrap();
    assert!(!limits.acquire(&acquire()).unwrap().decision.allowed);
    assert_eq!(
        limits
            .renew(&third, Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .token,
        third.token
    );
    clock.store(100, Ordering::SeqCst);
    assert_eq!(
        limits
            .renew(&third, Duration::from_millis(1))
            .unwrap()
            .unwrap()
            .expires_at_ms,
        third.expires_at_ms
    );
    limits.release(&second).unwrap();
    assert!(limits.acquire(&acquire()).unwrap().decision.allowed);
    limits.close().unwrap();
}

#[test]
fn renewal_extends_without_shortening_and_exact_expiry_loses_the_lease() {
    let (clock, store) = clocked(0);
    let limits = Limits::new(store, FailurePolicy::Error);
    let input = AcquireInput {
        capacity: 1,
        quantity: 1,
        ..acquire()
    };
    let first = limits.acquire(&input).unwrap().lease.unwrap();
    clock.store(500, Ordering::SeqCst);
    let renewed = limits
        .renew(&first, Duration::from_millis(1000))
        .unwrap()
        .unwrap();
    assert_eq!(renewed.expires_at_ms, 1500);
    // An old snapshot may renew/release the same token, but cannot shorten it.
    assert_eq!(
        limits
            .renew(&first, Duration::from_millis(1))
            .unwrap()
            .unwrap()
            .expires_at_ms,
        1500
    );
    clock.store(1500, Ordering::SeqCst);
    assert!(limits
        .renew(&renewed, Duration::from_secs(1))
        .unwrap()
        .is_none());
    let replacement = limits.acquire(&input).unwrap().lease.unwrap();
    limits.release(&first).unwrap();
    assert!(!limits.acquire(&input).unwrap().decision.allowed);
    assert_ne!(replacement.token, first.token);
    limits.close().unwrap();
}

#[test]
fn invalid_inputs_and_persistent_policy_conflicts_never_fail_open() {
    let (clock, store) = clocked(0);
    let limits = Limits::new(store.clone(), FailurePolicy::Allow);
    for value in [0, MAX_QUANTITY + 1, u32::MAX] {
        assert_eq!(
            limits.consume_rate(&ConsumeInput {
                capacity: value,
                ..consume()
            }),
            Err(LimitError::InvalidInput)
        );
        assert_eq!(
            limits.consume_rate(&ConsumeInput {
                quantity: value,
                ..consume()
            }),
            Err(LimitError::InvalidInput)
        );
        assert_eq!(
            limits.acquire(&AcquireInput {
                capacity: value,
                ..acquire()
            }),
            Err(LimitError::InvalidInput)
        );
        assert_eq!(
            limits.acquire(&AcquireInput {
                quantity: value,
                ..acquire()
            }),
            Err(LimitError::InvalidInput)
        );
    }
    for duration in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_micros(1500),
        Duration::from_millis(MAX_DURATION_MS + 1),
        Duration::MAX,
    ] {
        assert_eq!(
            limits.consume_rate(&ConsumeInput {
                period: duration,
                ..consume()
            }),
            Err(LimitError::InvalidInput)
        );
        assert_eq!(
            limits.acquire(&AcquireInput {
                ttl: duration,
                ..acquire()
            }),
            Err(LimitError::InvalidInput)
        );
    }
    // Even rejected admission establishes the scope policy.
    limits
        .consume_rate(&ConsumeInput {
            quantity: 6,
            ..consume()
        })
        .unwrap();
    limits
        .acquire(&AcquireInput {
            quantity: 6,
            ..acquire()
        })
        .unwrap();
    clock.store(100_000, Ordering::SeqCst);
    assert_eq!(
        limits.consume_rate(&ConsumeInput {
            capacity: 100,
            ..consume()
        }),
        Err(LimitError::PolicyConflict)
    );
    assert_eq!(
        limits.consume_rate(&ConsumeInput {
            period: Duration::from_secs(2),
            ..consume()
        }),
        Err(LimitError::PolicyConflict)
    );
    assert_eq!(
        limits.acquire(&AcquireInput {
            capacity: 100,
            ..acquire()
        }),
        Err(LimitError::PolicyConflict)
    );
    // Direct providers validate too, rather than relying on the service.
    assert_eq!(
        store.consume(
            CounterKind::Rate,
            &ConsumeInput {
                quantity: 0,
                ..consume()
            }
        ),
        Err(LimitError::InvalidInput)
    );
    assert_eq!(
        store.renew(&scope(), LeaseToken::fresh().unwrap(), Duration::ZERO),
        Err(LimitError::InvalidInput)
    );
    limits.close().unwrap();
}

#[test]
fn scope_lengths_and_clock_arithmetic_match_ts_boundaries() {
    for value in [String::new(), "a".repeat(513), "😀".repeat(257)] {
        assert_eq!(
            LimitScope::new(value, "tenant", "key"),
            Err(LimitError::InvalidInput)
        );
    }
    assert!(LimitScope::new("😀".repeat(256), "\0", "key").is_ok());
    let (clock, store) = clocked(MAX_TIMESTAMP_MS);
    let limits = Limits::new(store, FailurePolicy::Allow);
    let input = AcquireInput {
        ttl: Duration::from_millis(MAX_DURATION_MS),
        ..acquire()
    };
    let lease = limits.acquire(&input).unwrap().lease.unwrap();
    assert_eq!(lease.expires_at_ms, MAX_TIMESTAMP_MS + MAX_DURATION_MS);
    let counter = ConsumeInput {
        period: Duration::from_millis(MAX_DURATION_MS),
        quantity: 5,
        ..consume()
    };
    limits.consume_rate(&counter).unwrap();
    let denied = limits.consume_rate(&counter).unwrap();
    assert!(denied.retry_after.unwrap() <= counter.period);
    clock.store(MAX_TIMESTAMP_MS + 1, Ordering::SeqCst);
    assert_eq!(limits.consume_rate(&counter), Err(LimitError::InvalidInput));
    assert_eq!(limits.acquire(&input), Err(LimitError::InvalidInput));
    assert_eq!(
        limits.renew(&lease, input.ttl),
        Err(LimitError::InvalidInput)
    );
    clock.store(MAX_TIMESTAMP_MS, Ordering::SeqCst);
    limits.close().unwrap();
}

#[test]
fn immutable_store_snapshots_duplicate_tokens_and_wrong_scope() {
    let (_, store) = clocked(0);
    let limits = Limits::new(store.clone(), FailurePolicy::Error);
    let lease = limits.acquire(&acquire()).unwrap().lease.unwrap();
    assert_eq!(
        store.acquire(&acquire(), lease.token),
        Err(LimitError::InvalidInput)
    );
    let mut forged = lease.clone();
    forged.scope = LimitScope::new("reports", "wrong", "key").unwrap();
    assert_eq!(
        limits.renew(&forged, Duration::from_secs(1)),
        Err(LimitError::InvalidInput)
    );
    assert_eq!(limits.release(&forged), Err(LimitError::InvalidInput));
    forged = lease.clone();
    forged.quantity = 0;
    forged.expires_at_ms = 0;
    let current = limits
        .renew(&forged, Duration::from_millis(1))
        .unwrap()
        .unwrap();
    assert_eq!(current.quantity, 2);
    assert_eq!(current.expires_at_ms, 1000);
    assert_eq!(
        limits.acquire(&acquire()).unwrap().decision.remaining,
        Some(1)
    );
    limits.close().unwrap();
}

#[test]
fn real_threads_share_atomic_weighted_admission_across_service_instances() {
    let (_, store) = clocked(0);
    let barrier = Arc::new(Barrier::new(32));
    let handles: Vec<_> = (0..32)
        .map(|_| {
            let store = store.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let limits = Limits::new(store, FailurePolicy::Error);
                barrier.wait();
                let rate = limits
                    .consume_rate(&ConsumeInput {
                        capacity: 30,
                        quantity: 3,
                        ..consume()
                    })
                    .unwrap();
                let lease = limits
                    .acquire(&AcquireInput {
                        capacity: 10,
                        quantity: 2,
                        ..acquire()
                    })
                    .unwrap();
                // Keep every owner alive until the main thread measures the shared result.
                (limits, rate, lease)
            })
        })
        .collect();
    let results: Vec<_> = handles
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(
        results.iter().filter(|(_, rate, _)| rate.allowed).count(),
        10
    );
    assert_eq!(
        results
            .iter()
            .filter(|(_, _, lease)| lease.decision.allowed)
            .count(),
        5
    );
    let tokens: std::collections::HashSet<_> = results
        .iter()
        .filter_map(|(_, _, acquisition)| acquisition.lease.as_ref().map(|lease| lease.token))
        .collect();
    assert_eq!(tokens.len(), 5);
    for (limits, _, _) in results {
        limits.close().unwrap();
    }
    assert!(
        store
            .acquire(
                &AcquireInput {
                    capacity: 10,
                    quantity: 10,
                    ..acquire()
                },
                LeaseToken::fresh().unwrap()
            )
            .unwrap()
            .decision
            .allowed
    );
}
