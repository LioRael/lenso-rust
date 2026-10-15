mod common;
use common::poll_once;
use lenso_realtime::*;
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

fn identity(scope: &str, subject: &str) -> Identity<bool> {
    Identity {
        scope: scope.into(),
        subject: subject.into(),
        principal: true,
        expires_at: Instant::now() + Duration::from_secs(60),
    }
}
fn resource(scope: &str, id: &str) -> Resource {
    Resource::new(scope, "note", id).unwrap()
}
fn service(config: RealtimeConfig) -> MemoryRealtime<bool> {
    MemoryRealtime::new(config, |identity: Arc<Identity<bool>>, _| async move {
        Ok(identity.principal.then_some(Lease {
            valid_until: Instant::now() + Duration::from_secs(30),
        }))
    })
    .unwrap()
}
fn closed(event: Option<Envelope>, id: Option<SubscriptionId>, reason: CloseReason) {
    assert!(
        matches!(event, Some(Envelope { event: Event::Closed { subscription, reason: actual }, .. }) if subscription == id && actual == reason)
    );
}

#[test]
fn topics_are_exact_utf8_encoded_tuples_and_parts_are_bounded() {
    assert_eq!(
        Resource::new("a/b", "n%o", "雪 !*'()~").unwrap().topic(),
        "resource/a%2Fb/n%25o/%E9%9B%AA%20!*'()~"
    );
    let a = Resource::new("a/b", "c", "d").unwrap();
    let b = Resource::new("a", "b/c", "d").unwrap();
    assert_ne!(a, b);
    assert_ne!(a.topic(), b.topic());
    assert!(Resource::new("", "note", "n").is_err());
    assert!(Resource::new("scope", "note", "雪".repeat(86)).is_err());
    assert!(Resource::new("scope", "note", "a".repeat(256)).is_ok());
}

#[tokio::test]
async fn authorization_scope_and_instance_isolation_with_exact_topic_fanout() {
    let realtime = service(RealtimeConfig::default());
    let independent = service(RealtimeConfig::default());
    let note = resource("s", "n1");
    let mut a = realtime.connect(identity("s", "a")).unwrap();
    let mut b = realtime.connect(identity("s", "b")).unwrap();
    let c = realtime.connect(identity("other", "a")).unwrap();
    let isolated = independent.connect(identity("s", "a")).unwrap();
    let mut denied = identity("s", "denied");
    denied.principal = false;
    let denied = realtime.connect(denied).unwrap();
    assert!(matches!(
        denied.subscribe(note.clone()).await,
        Err(RealtimeError::Denied)
    ));
    assert!(matches!(
        c.subscribe(note.clone()).await,
        Err(RealtimeError::Denied)
    ));
    let sa = a.subscribe(note.clone()).await.unwrap();
    let sb = b.subscribe(note.clone()).await.unwrap();
    let _isolated = isolated.subscribe(note.clone()).await.unwrap();
    assert!(
        matches!(a.recv().await.unwrap().event, Event::Ready { subscription } if subscription == sa.id())
    );
    b.recv().await.unwrap();
    assert_eq!(
        realtime
            .publish(&resource("s", "n*"), "updated", json!(0))
            .unwrap()
            .enqueued,
        0
    );
    assert_eq!(
        realtime
            .publish(&resource("other", "n1"), "updated", json!(0))
            .unwrap()
            .enqueued,
        0
    );
    assert_eq!(
        realtime
            .publish(&note, "note.updated", json!({"revision": 2}))
            .unwrap()
            .enqueued,
        2
    );
    for (connection, id) in [(&mut a, sa.id()), (&mut b, sb.id())] {
        let event = connection.recv().await.unwrap();
        assert!(
            matches!(event.event, Event::Update { subscription, ref event_type, ref data } if subscription == id && event_type == "note.updated" && **data == json!({"revision": 2}))
        );
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(wire["version"], 1);
        assert_eq!(wire["kind"], "update");
        assert!(wire.get("principal").is_none());
    }
    assert_eq!(independent.stats().subscriptions, 1);
    assert_eq!(realtime.stats().pending, 0);
}

#[tokio::test]
async fn unsubscribe_and_resource_revocation_purge_only_the_target_private_queue() {
    let rt = service(RealtimeConfig::default());
    let first = resource("s", "1");
    let second = resource("s", "2");
    let mut connection = rt.connect(identity("s", "a")).unwrap();
    let a = connection.subscribe(first.clone()).await.unwrap();
    let b = connection.subscribe(second.clone()).await.unwrap();
    connection.recv().await.unwrap();
    connection.recv().await.unwrap();
    rt.publish(&first, "updated", json!("private-first"))
        .unwrap();
    rt.publish(&second, "updated", json!("private-second"))
        .unwrap();
    a.unsubscribe();
    assert!(
        matches!(connection.recv().await.unwrap().event, Event::Update { subscription, .. } if subscription == b.id())
    );
    closed(
        connection.recv().await,
        Some(a.id()),
        CloseReason::Unsubscribed,
    );
    rt.publish(&second, "updated", json!("purged")).unwrap();
    rt.revoke_resource(&second);
    closed(connection.recv().await, Some(b.id()), CloseReason::Revoked);
    assert_eq!(rt.stats().subscriptions, 0);
    assert_eq!(rt.stats().topics, 0);
    assert_eq!(
        rt.publish(&second, "updated", json!(1)).unwrap().enqueued,
        0
    );
    let renewed = connection.subscribe(second.clone()).await.unwrap();
    rt.delete_resource(&second);
    closed(
        connection.recv().await,
        Some(renewed.id()),
        CloseReason::Deleted,
    );
}

#[tokio::test]
async fn subject_revocation_is_scoped_and_connection_drop_reclaims_everything() {
    let rt = service(RealtimeConfig::default());
    let mut a = rt.connect(identity("a", "same")).unwrap();
    let mut b = rt.connect(identity("b", "same")).unwrap();
    let sa = a.subscribe(resource("a", "1")).await.unwrap();
    let sb = b.subscribe(resource("b", "1")).await.unwrap();
    rt.revoke_subject("a", "same").unwrap();
    closed(a.recv().await, None, CloseReason::Revoked);
    assert!(a.recv().await.is_none());
    assert!(matches!(b.recv().await.unwrap().event, Event::Ready { .. }));
    drop(b);
    assert_eq!(rt.stats(), Stats::default());
    assert!(matches!(sb.renew().await, Err(RealtimeError::Closed)));
    drop(sa);
}

#[tokio::test]
async fn event_overflow_is_a_single_terminal_gap_and_report_excludes_purged_enqueues() {
    let rt = service(RealtimeConfig {
        max_buffered_events: 2,
        ..RealtimeConfig::default()
    });
    let note = resource("s", "1");
    let mut c = rt.connect(identity("s", "a")).unwrap();
    let _a = c.subscribe(note.clone()).await.unwrap();
    let _b = c.subscribe(note.clone()).await.unwrap();
    c.recv().await.unwrap();
    c.recv().await.unwrap();
    // One queued event remains, so the next publication can initially enqueue
    // one subscription then overflow on the second. Both must be counted as lost.
    let other = resource("s", "2");
    let _other = c.subscribe(other).await.unwrap();
    let report = rt.publish(&note, "updated", json!(1)).unwrap();
    assert_eq!(
        report,
        PublishReport {
            enqueued: 0,
            overflowed_connections: 1
        }
    );
    assert!(matches!(
        c.recv().await.unwrap().event,
        Event::Gap {
            reason: GapReason::Overflow
        }
    ));
    assert!(c.recv().await.is_none());
    assert_eq!(rt.stats(), Stats::default());
    assert!(matches!(
        c.subscribe(note).await,
        Err(RealtimeError::Closed)
    ));
}

#[tokio::test]
async fn exact_byte_budget_closes_slow_consumer_without_affecting_fast_consumer() {
    let rt = service(RealtimeConfig {
        max_payload_bytes: 256,
        max_buffered_bytes: 768,
        ..RealtimeConfig::default()
    });
    let note = resource("s", "1");
    let mut slow = rt.connect(identity("s", "slow")).unwrap();
    let mut fast = rt.connect(identity("s", "fast")).unwrap();
    let _slow = slow.subscribe(note.clone()).await.unwrap();
    let _fast = fast.subscribe(note.clone()).await.unwrap();
    fast.recv().await.unwrap();
    let mut overflows = 0;
    for _ in 0..6 {
        overflows += rt
            .publish(&note, "updated", json!("x".repeat(200)))
            .unwrap()
            .overflowed_connections;
        assert!(matches!(
            fast.recv().await.unwrap().event,
            Event::Update { .. }
        ));
    }
    assert_eq!(overflows, 1);
    assert!(matches!(
        slow.recv().await.unwrap().event,
        Event::Gap { .. }
    ));
    assert!(slow.recv().await.is_none());
    assert_eq!(rt.stats().connections, 1);
}

#[tokio::test]
async fn payload_byte_depth_and_node_limits_apply_once_before_fanout() {
    let rt = service(RealtimeConfig {
        max_payload_bytes: 32768,
        ..RealtimeConfig::default()
    });
    let note = resource("s", "1");
    let mut a = rt.connect(identity("s", "a")).unwrap();
    let mut b = rt.connect(identity("s", "b")).unwrap();
    let _sa = a.subscribe(note.clone()).await.unwrap();
    let _sb = b.subscribe(note.clone()).await.unwrap();
    a.recv().await;
    b.recv().await;
    let large = Value::Array(vec![json!(0); 9996]);
    assert_eq!(rt.publish(&note, "updated", large).unwrap().enqueued, 2);
    assert!(matches!(
        a.recv().await.unwrap().event,
        Event::Update { .. }
    ));
    assert!(matches!(
        b.recv().await.unwrap().event,
        Event::Update { .. }
    ));
    assert_eq!(
        rt.publish(&note, "updated", Value::Array(vec![json!(0); 9997])),
        Err(RealtimeError::Payload)
    );
    assert_eq!(
        rt.publish(&note, "updated", json!("雪".repeat(11000))),
        Err(RealtimeError::Payload)
    );
    let mut depth = Value::Null;
    for _ in 0..31 {
        depth = Value::Array(vec![depth]);
    }
    assert!(rt.publish(&note, "updated", depth.clone()).is_ok());
    assert_eq!(
        rt.publish(&note, "updated", Value::Array(vec![depth])),
        Err(RealtimeError::Payload)
    );
    for event_type in ["", "1bad", "with space", "évent", "a/b"] {
        assert_eq!(
            rt.publish(&note, event_type, Value::Null),
            Err(RealtimeError::InvalidInput)
        );
    }
    assert_eq!(rt.stats().connections, 2);
}

#[tokio::test(start_paused = true)]
async fn lease_expiry_purges_buffered_updates_and_does_not_reauthorize_per_event() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rt = MemoryRealtime::new(
        RealtimeConfig {
            authorization_lease: Duration::from_secs(2),
            ..RealtimeConfig::default()
        },
        {
            let calls = calls.clone();
            move |_: Arc<Identity<bool>>, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(Some(Lease {
                        valid_until: Instant::now() + Duration::from_secs(30),
                    }))
                }
            }
        },
    )
    .unwrap();
    let note = resource("s", "1");
    let mut c = rt.connect(identity("s", "a")).unwrap();
    let sub = c.subscribe(note.clone()).await.unwrap();
    c.recv().await;
    for n in 0..3 {
        rt.publish(&note, "updated", json!(n)).unwrap();
    }
    tokio::time::advance(Duration::from_secs(2)).await;
    closed(c.recv().await, Some(sub.id()), CloseReason::Expired);
    assert_eq!(rt.stats().subscriptions, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(sub.renew().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn waiting_receive_wakes_at_lease_then_identity_deadline_and_cancellation_is_safe() {
    let rt = service(RealtimeConfig {
        authorization_lease: Duration::from_secs(2),
        ..RealtimeConfig::default()
    });
    let mut actor = identity("s", "a");
    actor.expires_at = Instant::now() + Duration::from_secs(3);
    let mut c = rt.connect(actor).unwrap();
    let sub = c.subscribe(resource("s", "1")).await.unwrap();
    c.recv().await;
    {
        let mut waiting = Box::pin(c.recv());
        assert!(poll_once(waiting.as_mut()).is_pending());
    }
    closed(c.recv().await, Some(sub.id()), CloseReason::Expired);
    closed(c.recv().await, None, CloseReason::Expired);
    assert!(c.recv().await.is_none());
    assert_eq!(rt.stats(), Stats::default());
}

#[tokio::test(start_paused = true)]
async fn renewal_extends_lease_but_never_session_expiry() {
    let rt = service(RealtimeConfig {
        authorization_lease: Duration::from_secs(2),
        ..RealtimeConfig::default()
    });
    let mut actor = identity("s", "a");
    actor.expires_at = Instant::now() + Duration::from_secs(3);
    let mut c = rt.connect(actor).unwrap();
    let sub = c.subscribe(resource("s", "1")).await.unwrap();
    c.recv().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    sub.renew().await.unwrap();
    tokio::time::advance(Duration::from_millis(1500)).await;
    assert_eq!(rt.stats().subscriptions, 1);
    tokio::time::advance(Duration::from_millis(500)).await;
    closed(c.recv().await, None, CloseReason::Expired);
    assert!(sub.renew().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_have_identical_complete_order_for_every_subscriber() {
    let rt = service(RealtimeConfig {
        max_buffered_events: 1024,
        max_buffered_bytes: 262144,
        ..RealtimeConfig::default()
    });
    let note = resource("s", "1");
    let mut a = rt.connect(identity("s", "a")).unwrap();
    let mut b = rt.connect(identity("s", "b")).unwrap();
    let _sa = a.subscribe(note.clone()).await.unwrap();
    let _sb = b.subscribe(note.clone()).await.unwrap();
    a.recv().await;
    b.recv().await;
    let mut tasks = tokio::task::JoinSet::new();
    for publisher in 0..8 {
        let rt = rt.clone();
        let note = note.clone();
        tasks.spawn(async move {
            for index in 0..100 {
                assert_eq!(
                    rt.publish(&note, "updated", json!([publisher, index]))
                        .unwrap()
                        .enqueued,
                    2
                );
                tokio::task::yield_now().await;
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let mut seen = HashSet::new();
    let mut last = [None; 8];
    for _ in 0..800 {
        let Event::Update { data: first, .. } = a.recv().await.unwrap().event else {
            panic!("expected update");
        };
        let Event::Update { data: second, .. } = b.recv().await.unwrap().event else {
            panic!("expected update");
        };
        assert_eq!(first, second);
        let p = first[0].as_u64().unwrap() as usize;
        let n = first[1].as_u64().unwrap();
        assert_eq!(n, last[p].map_or(0, |v| v + 1));
        last[p] = Some(n);
        assert!(seen.insert((p, n)));
    }
    assert_eq!(seen.len(), 800);
    assert_eq!(rt.stats().subscriptions, 2);
}

#[tokio::test]
async fn close_wakes_waiting_receiver_and_is_idempotent_across_clones() {
    let rt = service(RealtimeConfig::default());
    let mut c = rt.connect(identity("s", "a")).unwrap();
    let mut receive = Box::pin(c.recv());
    assert!(poll_once(receive.as_mut()).is_pending());
    rt.clone().close();
    rt.close();
    closed(receive.await, None, CloseReason::Shutdown);
    assert!(c.recv().await.is_none());
    assert!(matches!(
        rt.connect(identity("s", "b")),
        Err(RealtimeError::Closed)
    ));
    assert_eq!(
        rt.publish(&resource("s", "1"), "updated", json!(0)),
        Err(RealtimeError::Closed)
    );
    assert_eq!(rt.stats(), Stats::default());
}

#[test]
fn principal_destructor_can_reenter_service_after_connection_drop() {
    struct Principal(Box<dyn Fn() + Send + Sync>);
    impl Drop for Principal {
        fn drop(&mut self) {
            (self.0)();
        }
    }
    let rt = Arc::new(
        MemoryRealtime::new(
            RealtimeConfig::default(),
            |_: Arc<Identity<Principal>>, _| async { Ok(None) },
        )
        .unwrap(),
    );
    let weak = Arc::downgrade(&rt);
    let (dropped, observed) = std::sync::mpsc::channel();
    let c = rt
        .connect(Identity {
            scope: "s".into(),
            subject: "a".into(),
            principal: Principal(Box::new(move || {
                assert_eq!(weak.upgrade().unwrap().stats().connections, 0);
                dropped.send(()).unwrap();
            })),
            expires_at: Instant::now() + Duration::from_secs(60),
        })
        .unwrap();
    // A destructor must never run while state removal holds the service lock.
    let worker = std::thread::spawn(move || drop(c));
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    worker.join().unwrap();
}

#[tokio::test(start_paused = true)]
async fn expiry_reclaims_admission_and_topic_capacity_without_background_sweeps() {
    let rt = service(RealtimeConfig {
        max_connections: 1,
        max_topics: 1,
        authorization_lease: Duration::from_millis(1),
        ..RealtimeConfig::default()
    });
    for index in 0..100 {
        let mut actor = identity("s", "a");
        actor.expires_at = Instant::now() + Duration::from_millis(2);
        let mut c = rt.connect(actor).unwrap();
        let first = c
            .subscribe(resource("s", &format!("first-{index}")))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        let second = c
            .subscribe(resource("s", &format!("second-{index}")))
            .await
            .unwrap();
        assert_eq!(rt.stats().topics, 1);
        assert_eq!(rt.stats().subscriptions, 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        closed(c.recv().await, None, CloseReason::Expired);
        assert_eq!(rt.stats(), Stats::default());
        drop((first, second));
    }
}
