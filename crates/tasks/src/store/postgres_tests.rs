use super::*;
use crate::RetryPolicy;
use serde_json::json;
use std::{
    thread,
    time::{Duration, Instant},
};
#[path = "../../tests/support/postgres.rs"]
mod support;

fn task(attempts: u32) -> Task {
    Task::new(
        "echo",
        RetryPolicy {
            max_attempts: attempts,
            delay_ms: 0,
            ..Default::default()
        },
        |v: Value, _| async { Ok(v) },
    )
}
#[test]
#[ignore = "requires local initdb/pg_ctl; creates its own temporary PostgreSQL cluster"]
fn postgres_lock_and_expiry_contract() {
    let cluster = support::Cluster::new();
    let mut config = cluster.config();
    provision(&config, "fence").unwrap();
    let mut observer = config.connect(NoTls).unwrap();
    let (mut next, _) = PostgresConnection::open(&config, "fence".into()).unwrap();
    config.application_name("lenso-fencing-waiter");
    let options = WorkerOptions {
        lease_ms: 500,
        heartbeat_ms: 30,
        execution_timeout_ms: 5000,
    };
    // Both heartbeat and acknowledgement must evaluate time AFTER waiting for a lock.
    for settle in [false, true] {
        let (mut old, _) = PostgresConnection::open(&config, "fence".into()).unwrap();
        let id = old
            .enqueue(&task(2), &json!(null), &Default::default())
            .unwrap();
        let claim = old.claim(&["echo".into()], &options).unwrap().unwrap();
        let mut blocker = cluster.config().connect(NoTls).unwrap();
        let mut lock = blocker.transaction().unwrap();
        lock.query_one(
            "SELECT id FROM lenso_task_job WHERE id=$1 FOR UPDATE",
            &[&id],
        )
        .unwrap();
        let stale = claim.clone();
        let waiter = thread::spawn(move || {
            if settle {
                assert!(matches!(
                    old.settle(&claim, Ok(json!("too late"))),
                    Err(TaskError::ClaimLost)
                ));
            } else {
                assert_eq!(old.pulse(&claim, 500).unwrap(), None);
            }
            old
        });
        let start = Instant::now();
        loop {
            let blocked: bool=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='lenso-fencing-waiter' AND wait_event_type='Lock')",&[]).unwrap().get(0);
            if blocked {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(3));
            thread::sleep(Duration::from_millis(5));
        }
        loop {
            let expired:bool=observer.query_one("SELECT lease_until <= floor(extract(epoch FROM clock_timestamp())*1000)::bigint FROM lenso_task_job WHERE id=$1",&[&id]).unwrap().get(0);
            if expired {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(3));
            thread::sleep(Duration::from_millis(5));
        }
        lock.commit().unwrap();
        let mut old = waiter.join().unwrap();
        let recovered = next.claim(&["echo".into()], &options).unwrap().unwrap();
        assert_eq!((recovered.id, recovered.attempt), (id, 2));
        assert_ne!(recovered.token, stale.token);
        next.settle(&recovered, Ok(json!("new"))).unwrap();
        assert_eq!(old.pulse(&stale, 500).unwrap(), None);
        assert!(matches!(
            old.settle(&stale, Ok(json!("stale"))),
            Err(TaskError::ClaimLost)
        ));
        assert_eq!(next.get(id).unwrap().unwrap().result, Some(json!("new")));
    }
    // A locked head-of-queue row cannot block another worker's due row.
    let first = next
        .enqueue(&task(1), &json!(1), &Default::default())
        .unwrap();
    let second = next
        .enqueue(&task(1), &json!(2), &Default::default())
        .unwrap();
    let mut blocker = cluster.config().connect(NoTls).unwrap();
    let mut lock = blocker.transaction().unwrap();
    lock.query_one(
        "SELECT id FROM lenso_task_job WHERE id=$1 FOR UPDATE",
        &[&first],
    )
    .unwrap();
    let claimed = next.claim(&["echo".into()], &options).unwrap().unwrap();
    assert_eq!(claimed.id, second);
    next.settle(&claimed, Ok(json!(2))).unwrap();
    lock.commit().unwrap();
    let claimed = next.claim(&["echo".into()], &options).unwrap().unwrap();
    assert_eq!(claimed.id, first);
    // Expired hard deadline also fences a still-live lease and exhausts the attempt.
    observer
        .execute(
            "UPDATE lenso_task_job SET expires_at=0 WHERE id=$1",
            &[&first],
        )
        .unwrap();
    assert_eq!(next.pulse(&claimed, 500).unwrap(), None);
    assert!(matches!(
        next.settle(&claimed, Ok(json!(1))),
        Err(TaskError::ClaimLost)
    ));
    assert!(next.claim(&["echo".into()], &options).unwrap().is_none());
    let failed = next.get(first).unwrap().unwrap();
    assert_eq!(
        (failed.state, failed.attempt, failed.last_error),
        (JobState::Failed, 1, Some(Failure::Aborted))
    );
    let cancelled = next
        .enqueue(&task(2), &json!(0), &Default::default())
        .unwrap();
    next.claim(&["echo".into()], &options).unwrap().unwrap();
    assert_eq!(next.cancel(cancelled).unwrap(), CancelResult::Requested);
    observer
        .execute(
            "UPDATE lenso_task_job SET lease_until=0 WHERE id=$1",
            &[&cancelled],
        )
        .unwrap();
    assert!(next.claim(&["echo".into()], &options).unwrap().is_none());
    assert_eq!(
        next.get(cancelled).unwrap().unwrap().state,
        JobState::Cancelled
    );
    assert!(!next.retry(cancelled).unwrap());
}
