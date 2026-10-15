mod common;
use common::poll_once;
use lenso_realtime::*;
use serde_json::json;
use std::{
    future::pending,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};

fn actor(subject: &str) -> Identity<()> {
    Identity {
        scope: "s".into(),
        subject: subject.into(),
        principal: (),
        expires_at: Instant::now() + Duration::from_secs(60),
    }
}
fn note(id: &str) -> Resource {
    Resource::new("s", "note", id).unwrap()
}
fn grant() -> Result<Option<Lease>, RealtimeError> {
    Ok(Some(Lease {
        valid_until: Instant::now() + Duration::from_secs(30),
    }))
}

#[tokio::test]
async fn pending_subscriptions_reserve_all_limits_and_drop_releases_reservations() {
    let rt = MemoryRealtime::new(
        RealtimeConfig {
            max_connections: 3,
            max_connections_per_subject: 1,
            max_subscriptions_per_connection: 1,
            max_subscriptions_per_subject: 1,
            max_subscribers_per_topic: 1,
            max_topics: 1,
            ..RealtimeConfig::default()
        },
        |_: Arc<Identity<()>>, _| pending(),
    )
    .unwrap();
    let a = rt.connect(actor("a")).unwrap();
    let b = rt.connect(actor("b")).unwrap();
    let c = rt.connect(actor("c")).unwrap();
    assert!(matches!(rt.connect(actor("a")), Err(RealtimeError::Limit)));
    assert!(matches!(rt.connect(actor("d")), Err(RealtimeError::Limit)));
    let mut first = Box::pin(a.subscribe(note("1")));
    assert!(poll_once(first.as_mut()).is_pending());
    assert_eq!(
        rt.stats(),
        Stats {
            connections: 3,
            subscriptions: 1,
            topics: 1,
            pending: 1
        }
    );
    assert!(matches!(
        a.subscribe(note("2")).await,
        Err(RealtimeError::Limit)
    ));
    assert!(matches!(
        b.subscribe(note("1")).await,
        Err(RealtimeError::Limit)
    ));
    assert!(matches!(
        c.subscribe(note("2")).await,
        Err(RealtimeError::Limit)
    ));
    drop(first);
    assert_eq!(
        rt.stats(),
        Stats {
            connections: 3,
            ..Stats::default()
        }
    );
    let mut next = Box::pin(b.subscribe(note("2")));
    assert!(poll_once(next.as_mut()).is_pending());
    assert_eq!(rt.stats().subscriptions, 1);
    drop(next);
    drop(c);
    assert!(rt.connect(actor("d")).is_ok());
}

#[tokio::test]
async fn per_subject_subscriptions_span_connections_but_not_scopes() {
    let rt = MemoryRealtime::new(
        RealtimeConfig {
            max_subscriptions_per_subject: 1,
            ..RealtimeConfig::default()
        },
        |_: Arc<Identity<()>>, _| async { grant() },
    )
    .unwrap();
    let a = rt.connect(actor("same")).unwrap();
    let b = rt.connect(actor("same")).unwrap();
    let mut other_actor = actor("same");
    other_actor.scope = "other".into();
    let other = rt.connect(other_actor).unwrap();
    let sub = a.subscribe(note("1")).await.unwrap();
    assert!(matches!(
        b.subscribe(note("2")).await,
        Err(RealtimeError::Limit)
    ));
    let _other_sub = other
        .subscribe(Resource::new("other", "note", "1").unwrap())
        .await
        .unwrap();
    drop(sub);
    assert!(b.subscribe(note("2")).await.is_ok());
}

#[tokio::test]
async fn global_pending_limit_is_independent_and_cancelled_future_is_dropped() {
    let alive = Arc::new(AtomicUsize::new(0));
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let rt = MemoryRealtime::new(
        RealtimeConfig {
            max_pending_operations: 1,
            ..RealtimeConfig::default()
        },
        {
            let alive = alive.clone();
            move |_: Arc<Identity<()>>, _| {
                let alive = alive.clone();
                async move {
                    alive.fetch_add(1, Ordering::SeqCst);
                    let _active = Active(alive);
                    pending().await
                }
            }
        },
    )
    .unwrap();
    let a = rt.connect(actor("a")).unwrap();
    let b = rt.connect(actor("b")).unwrap();
    let mut first = Box::pin(a.subscribe(note("1")));
    assert!(poll_once(first.as_mut()).is_pending());
    assert_eq!(alive.load(Ordering::SeqCst), 1);
    assert!(matches!(
        b.subscribe(note("2")).await,
        Err(RealtimeError::Limit)
    ));
    drop(first);
    assert_eq!(alive.load(Ordering::SeqCst), 0);
    assert_eq!(rt.stats().pending, 0);
    assert_eq!(rt.stats().subscriptions, 0);
}

#[tokio::test(start_paused = true)]
async fn timeout_and_lease_start_bound_cancel_slow_authorizers() {
    for (lease, timeout) in [(30, 2), (2, 30)] {
        let completed = Arc::new(AtomicBool::new(false));
        let rt = MemoryRealtime::new(
            RealtimeConfig {
                authorization_lease: Duration::from_secs(lease),
                authorization_timeout: Duration::from_secs(timeout),
                ..RealtimeConfig::default()
            },
            {
                let completed = completed.clone();
                move |_: Arc<Identity<()>>, _| {
                    let completed = completed.clone();
                    async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        completed.store(true, Ordering::SeqCst);
                        grant()
                    }
                }
            },
        )
        .unwrap();
        let c = rt.connect(actor("a")).unwrap();
        let started = Instant::now();
        assert!(matches!(
            c.subscribe(note("1")).await,
            Err(RealtimeError::Expired)
        ));
        assert_eq!(Instant::now() - started, Duration::from_secs(2));
        assert!(!completed.load(Ordering::SeqCst));
        assert_eq!(rt.stats().pending, 0);
        assert_eq!(rt.stats().subscriptions, 0);
    }
}

#[tokio::test]
async fn resource_subject_disconnect_and_shutdown_cancel_pending_authorization_without_resurrection(
) {
    for action in 0..4 {
        let gate = Arc::new(Semaphore::new(0));
        let rt = MemoryRealtime::new(RealtimeConfig::default(), {
            let gate = gate.clone();
            move |_: Arc<Identity<()>>, _| {
                let gate = gate.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    grant()
                }
            }
        })
        .unwrap();
        let c = rt.connect(actor("a")).unwrap();
        let mut first = Box::pin(c.subscribe(note("1")));
        assert!(poll_once(first.as_mut()).is_pending());
        match action {
            0 => rt.revoke_resource(&note("1")),
            1 => rt.revoke_subject("s", "a").unwrap(),
            2 => c.close(),
            _ => rt.close(),
        }
        // Make authorization and cancellation ready at once: cancellation wins.
        gate.add_permits(1);
        assert!(matches!(first.await, Err(RealtimeError::Closed)));
        assert_eq!(rt.stats().subscriptions, 0);
        assert_eq!(rt.stats().pending, 0);
        assert_eq!(gate.available_permits(), 1);
    }
}

#[tokio::test]
async fn denied_and_cancelled_renewal_purge_private_data_and_concurrent_renewal_is_bounded() {
    let allow = Arc::new(AtomicBool::new(true));
    let gate = Arc::new(Semaphore::new(1));
    let rt = MemoryRealtime::new(RealtimeConfig::default(), {
        let gate = gate.clone();
        let allow = allow.clone();
        move |_: Arc<Identity<()>>, _| {
            let gate = gate.clone();
            let allow = allow.clone();
            async move {
                gate.acquire().await.unwrap().forget();
                if allow.load(Ordering::SeqCst) {
                    grant()
                } else {
                    Ok(None)
                }
            }
        }
    })
    .unwrap();
    let mut c = rt.connect(actor("a")).unwrap();
    let sub = c.subscribe(note("1")).await.unwrap();
    c.recv().await;
    rt.publish(&note("1"), "updated", json!("private")).unwrap();
    let mut renewal = Box::pin(sub.renew());
    assert!(poll_once(renewal.as_mut()).is_pending());
    assert_eq!(sub.renew().await, Err(RealtimeError::Limit));
    allow.store(false, Ordering::SeqCst);
    gate.add_permits(1);
    assert_eq!(renewal.await, Err(RealtimeError::Denied));
    assert!(
        matches!(c.recv().await.unwrap().event, Event::Closed { subscription: Some(id), reason: CloseReason::Revoked } if id == sub.id())
    );
    assert_eq!(rt.stats().subscriptions, 0);

    allow.store(true, Ordering::SeqCst);
    gate.add_permits(1);
    let sub = c.subscribe(note("1")).await.unwrap();
    c.recv().await;
    let mut renewal = Box::pin(sub.renew());
    assert!(poll_once(renewal.as_mut()).is_pending());
    drop(renewal);
    assert_eq!(rt.stats().pending, 0);
    assert_eq!(rt.stats().subscriptions, 0);
    assert!(matches!(
        c.recv().await.unwrap().event,
        Event::Closed {
            reason: CloseReason::Revoked,
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn pending_renewal_cannot_bridge_an_expired_old_lease() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rt = MemoryRealtime::new(
        RealtimeConfig {
            authorization_lease: Duration::from_secs(2),
            ..RealtimeConfig::default()
        },
        {
            let calls = calls.clone();
            move |_: Arc<Identity<()>>, _| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call > 0 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    grant()
                }
            }
        },
    )
    .unwrap();
    let mut c = rt.connect(actor("a")).unwrap();
    let sub = c.subscribe(note("1")).await.unwrap();
    c.recv().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(sub.renew().await, Err(RealtimeError::Expired));
    assert!(matches!(
        c.recv().await.unwrap().event,
        Event::Closed {
            reason: CloseReason::Expired,
            ..
        }
    ));
    assert_eq!(rt.stats().subscriptions, 0);
}

#[tokio::test]
async fn callback_construction_and_poll_panics_deny_and_leave_no_reservations() {
    let construction = MemoryRealtime::new(
        RealtimeConfig::default(),
        |_: Arc<Identity<()>>, _| -> std::future::Ready<Result<Option<Lease>, RealtimeError>> {
            panic!("callback construction");
        },
    )
    .unwrap();
    let polling = MemoryRealtime::new(RealtimeConfig::default(), |_: Arc<Identity<()>>, _| async {
        panic!("callback polling");
    })
    .unwrap();
    for rt in [construction, polling] {
        let c = rt.connect(actor("a")).unwrap();
        assert!(matches!(
            c.subscribe(note("1")).await,
            Err(RealtimeError::Denied)
        ));
        assert_eq!(
            rt.stats(),
            Stats {
                connections: 1,
                ..Stats::default()
            }
        );
    }
}

#[test]
fn configuration_validates_before_work_and_identity_rejects_expired_or_invalid_parts() {
    for config in [
        RealtimeConfig {
            max_connections: 0,
            ..RealtimeConfig::default()
        },
        RealtimeConfig {
            max_pending_operations: 10_000_001,
            ..RealtimeConfig::default()
        },
        RealtimeConfig {
            authorization_lease: Duration::from_secs(31),
            ..RealtimeConfig::default()
        },
        RealtimeConfig {
            authorization_timeout: Duration::ZERO,
            ..RealtimeConfig::default()
        },
        RealtimeConfig {
            max_payload_bytes: 32769,
            ..RealtimeConfig::default()
        },
        RealtimeConfig {
            max_buffered_bytes: 512,
            ..RealtimeConfig::default()
        },
    ] {
        assert!(matches!(
            MemoryRealtime::new(config, |_: Arc<Identity<()>>, _| async { grant() }),
            Err(RealtimeError::InvalidInput)
        ));
    }
    let rt = MemoryRealtime::new(RealtimeConfig::default(), |_: Arc<Identity<()>>, _| async {
        grant()
    })
    .unwrap();
    let mut expired = actor("a");
    expired.expires_at = Instant::now();
    assert!(matches!(rt.connect(expired), Err(RealtimeError::Expired)));
    assert!(matches!(
        rt.connect(actor("")),
        Err(RealtimeError::InvalidInput)
    ));
}

#[tokio::test]
async fn initialization_has_no_replay_and_revocation_fences_a_pending_renewal() {
    let gate = Arc::new(Semaphore::new(0));
    let rt = MemoryRealtime::new(RealtimeConfig::default(), {
        let gate = gate.clone();
        move |_: Arc<Identity<()>>, _| {
            let gate = gate.clone();
            async move {
                gate.acquire().await.unwrap().forget();
                grant()
            }
        }
    })
    .unwrap();
    let mut c = rt.connect(actor("a")).unwrap();
    let mut pending = Box::pin(c.subscribe(note("1")));
    assert!(poll_once(pending.as_mut()).is_pending());
    assert_eq!(
        rt.publish(&note("1"), "updated", json!("before-ready"))
            .unwrap()
            .enqueued,
        0
    );
    gate.add_permits(1);
    let sub = pending.await.unwrap();
    assert!(matches!(c.recv().await.unwrap().event, Event::Ready { .. }));
    let mut renewal = Box::pin(sub.renew());
    assert!(poll_once(renewal.as_mut()).is_pending());
    rt.publish(&note("1"), "updated", json!("private")).unwrap();
    rt.revoke_resource(&note("1"));
    gate.add_permits(1);
    assert_eq!(renewal.await, Err(RealtimeError::Closed));
    assert!(matches!(
        c.recv().await.unwrap().event,
        Event::Closed {
            reason: CloseReason::Revoked,
            ..
        }
    ));
    assert_eq!(rt.stats().subscriptions, 0);
    assert_eq!(rt.stats().pending, 0);
}
