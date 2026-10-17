use super::*;
use crate::store::postgres_test_support as support;
use crate::{
    store::postgres::provision, EnqueueOptions, JobState, RetryPolicy, Task, WorkerOptions,
};
use serde_json::{json, Value};
use std::{process::Command, thread};

fn echo() -> Task {
    Task::new(
        "echo",
        RetryPolicy {
            max_attempts: 2,
            delay_ms: 0,
            ..Default::default()
        },
        |v: Value, _| async { Ok(v) },
    )
}
fn pids(admin: &mut Client) -> Vec<i32> {
    admin.query("SELECT pid FROM pg_stat_activity WHERE application_name='lenso-pool-test' ORDER BY pid", &[])
        .unwrap().iter().map(|r| r.get(0)).collect()
}
fn disconnect(admin: &mut Client) {
    admin.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='lenso-pool-test'", &[]).unwrap();
}
fn wait_for_lock(admin: &mut Client) -> i32 {
    let start = Instant::now();
    loop {
        if let Some(row) = admin.query_opt("SELECT pid FROM pg_stat_activity WHERE application_name='lenso-pool-test' AND wait_event_type='Lock'", &[]).unwrap() {
            return row.get(0);
        }
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "writer did not reach row lock"
        );
        thread::sleep(Duration::from_millis(5));
    }
}
fn pg_ctl(cluster: &support::Cluster, args: &[&str]) {
    let output = Command::new("pg_ctl")
        .arg("-D")
        .arg(cluster.socket().join("data"))
        .arg("-l")
        .arg(cluster.socket().join("server.log"))
        .arg("-o")
        .arg(format!("-k {} -h '' -p 55431", cluster.socket().display()))
        .arg("-w")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "pg_ctl: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn invalid_pool_options_fail_without_connecting() {
    for options in [
        PostgresOptions::new(Config::new()).max_connections(0),
        PostgresOptions::new(Config::new()).max_connections(65),
        PostgresOptions::new(Config::new()).checkout_timeout(Duration::ZERO),
        PostgresOptions::new(Config::new()).checkout_timeout(Duration::MAX),
    ] {
        assert!(matches!(
            PostgresPool::open(options, "q".into()),
            Err(TaskError::InvalidOptions)
        ));
    }
}
#[test]
#[ignore = "requires local initdb/pg_ctl; owns and restarts a temporary socket-only cluster"]
fn postgres_pool_recovery_contract() {
    let cluster = support::Cluster::new();
    let mut admin = cluster.config().connect(NoTls).unwrap();
    let mut config = cluster.config();
    config.application_name("lenso-pool-test");
    let options = PostgresOptions::new(config)
        .max_connections(2)
        .checkout_timeout(Duration::from_millis(100));
    provision(&options, "pool").unwrap();
    // If a later eager connection fails, startup releases earlier sessions.
    let mut failing = options.clone();
    let opened = std::sync::atomic::AtomicBool::new(false);
    failing.connect = Arc::new(move |config| {
        let mut config = config.clone();
        if opened.swap(true, std::sync::atomic::Ordering::SeqCst) {
            config.dbname("lenso_nonexistent_database");
        }
        config.connect(NoTls)
    });
    assert!(PostgresPool::open(failing, "pool".into()).is_err());
    let start = Instant::now();
    while !pids(&mut admin).is_empty() {
        assert!(start.elapsed() < Duration::from_secs(3));
        thread::sleep(Duration::from_millis(5));
    }
    let pool = Arc::new(PostgresPool::open(options, "pool".into()).unwrap());
    let identity = pool.identity().to_owned();
    let original = pids(&mut admin);
    assert_eq!(original.len(), 2);
    let first = pool.checkout().unwrap();
    let second = pool.checkout().unwrap();
    let before = Instant::now();
    assert!(matches!(
        pool.with(|c| c.get(1)),
        Err(TaskError::PostgresPoolTimeout)
    ));
    assert!(before.elapsed() >= Duration::from_millis(100));
    drop(first);
    assert!(pool.with(|c| c.get(1)).unwrap().is_none());
    drop(second);
    assert_eq!(pids(&mut admin), original, "healthy slots are reused");

    // One pool admits concurrent producers and claims; no Store-wide serialization.
    let store = Arc::new(
        crate::store::Store::open_postgres(
            PostgresOptions::new(cluster.config()).max_connections(2),
            "pool".into(),
        )
        .unwrap(),
    );
    let mut producers = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        producers.push(thread::spawn(move || {
            store
                .with(|c| {
                    c.enqueue(
                        &echo(),
                        &json!(1),
                        &EnqueueOptions {
                            deduplication_key: Some("same".into()),
                            ..Default::default()
                        },
                    )
                })
                .unwrap()
        }));
    }
    let id = producers.remove(0).join().unwrap();
    for producer in producers {
        assert_eq!(producer.join().unwrap(), id);
    }
    let worker_options = WorkerOptions {
        lease_ms: 5000,
        heartbeat_ms: 100,
        execution_timeout_ms: 15000,
    };
    let mut workers = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let options = worker_options.clone();
        workers.push(thread::spawn(move || {
            store.with(|c| c.claim(&["echo".into()], &options)).unwrap()
        }));
    }
    let claims: Vec<_> = workers
        .into_iter()
        .filter_map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(claims.len(), 1);
    store.with(|c| c.settle(&claims[0], Ok(json!(1)))).unwrap();
    store.close().unwrap();

    let id = pool
        .with(|c| c.enqueue(&echo(), &json!(2), &Default::default()))
        .unwrap();
    let mut blocker = cluster.config().connect(NoTls).unwrap();
    let mut lock = blocker.transaction().unwrap();
    lock.query_one(
        "SELECT id FROM lenso_task_job WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .unwrap();
    let writer = thread::spawn({
        let pool = pool.clone();
        move || pool.with(|c| c.cancel(id))
    });
    let pid = wait_for_lock(&mut admin);
    // A blocked write must leave another connection available for reads.
    assert_eq!(
        pool.with(|c| c.get(id)).unwrap().unwrap().state,
        JobState::Pending
    );
    assert_eq!(pids(&mut admin).len(), 2);
    admin
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .unwrap();
    assert!(
        matches!(writer.join().unwrap(), Err(TaskError::Postgres(_))),
        "in-flight transaction must not be replayed"
    );
    lock.commit().unwrap();
    let status = pool.with(|c| c.get(id)).unwrap().unwrap();
    assert!(!status.cancel_requested);
    assert_eq!(status.state, JobState::Pending);

    let old = pool
        .with(|c| c.claim(&["echo".into()], &worker_options))
        .unwrap()
        .unwrap();
    // Expire and disconnect an old worker, then recover on the same live pool.
    admin
        .execute(
            "UPDATE lenso_task_job SET lease_until=0 WHERE id=$1",
            &[&id],
        )
        .unwrap();
    disconnect(&mut admin);
    let new = pool
        .with(|c| c.claim(&["echo".into()], &worker_options))
        .unwrap()
        .unwrap();
    assert_eq!((new.id, new.attempt), (id, 2));
    assert_ne!(new.token, old.token);
    assert_eq!(pool.with(|c| c.pulse(&old, 5000)).unwrap(), None);
    assert!(matches!(
        pool.with(|c| c.settle(&old, Ok(json!("stale")))),
        Err(TaskError::ClaimLost)
    ));
    pool.with(|c| c.settle(&new, Ok(json!("recovered"))))
        .unwrap();

    pg_ctl(&cluster, &["-m", "immediate", "stop"]);
    assert!(matches!(
        pool.with(|c| c.get(id)),
        Err(TaskError::Postgres(_))
    ));
    pg_ctl(&cluster, &["start"]);
    assert_eq!(pool.identity(), identity);
    assert_eq!(
        pool.with(|c| c.get(id)).unwrap().unwrap().result,
        Some(json!("recovered"))
    );
    admin = cluster.config().connect(NoTls).unwrap();
    admin
        .execute(
            "UPDATE lenso_task_queue SET identity=gen_random_uuid()::text WHERE name='pool'",
            &[],
        )
        .unwrap();
    disconnect(&mut admin);
    assert!(matches!(
        pool.with(|c| c.get(id)),
        Err(TaskError::QueueIdentityChanged)
    ));
    // Check all idle sessions are released, including slots broken by the restart.
    pool.close().unwrap();
    assert!(matches!(pool.with(|c| c.get(id)), Err(TaskError::Closed)));
    let start = Instant::now();
    while !pids(&mut admin).is_empty() {
        assert!(start.elapsed() < Duration::from_secs(3));
        thread::sleep(Duration::from_millis(5));
    }
}
