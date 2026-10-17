use lenso_plugin_core::{App, Instance, RunningApp};
use lenso_tasks::{
    provision, CancelResult, EnqueueOptions, Failure, JobState, RetryPolicy, StopReason, Task,
    TaskError, Tasks, TasksPlugin, WorkerOptions,
};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

fn policy(attempts: u32, delay_ms: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts: attempts,
        delay_ms,
        backoff: true,
        max_delay_ms: 5000,
    }
}
fn task() -> Task {
    Task::new(
        "echo",
        policy(2, 0),
        |input: Value, _| async move { Ok(input) },
    )
}
async fn start(path: &Path, queue: &str, tasks: Vec<Task>) -> (RunningApp, Arc<Tasks>) {
    let instance = Instance::new(
        "tasks",
        TasksPlugin::new(path, queue, tasks).worker_options(WorkerOptions {
            lease_ms: 2000,
            heartbeat_ms: 50,
            execution_timeout_ms: 10_000,
        }),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let service = app.get(&handle).unwrap();
    (app, service)
}
fn key(key: &str) -> EnqueueOptions {
    EnqueueOptions {
        deduplication_key: Some(key.into()),
        ..Default::default()
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn persistence_deduplication_identity_and_queue_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    provision(&path, "b").await.unwrap();
    let (app, queue) = start(&path, "a", vec![task()]).await;
    let identity = queue.identity().to_owned();
    let id = queue
        .enqueue("echo", json!({"a": 1, "b": 2}), key("unique"))
        .await
        .unwrap();
    assert_eq!(
        queue
            .enqueue("echo", json!({"b": 2, "a": 1}), key("unique"))
            .await
            .unwrap(),
        id
    );
    assert!(matches!(
        queue.enqueue("echo", json!({"a": 2}), key("unique")).await,
        Err(TaskError::DeduplicationConflict)
    ));
    app.shutdown().await.unwrap();
    assert!(matches!(queue.get(id).await, Err(TaskError::Closed)));
    let (restarted, queue) = start(&path, "a", vec![task()]).await;
    assert_eq!(queue.identity(), identity);
    assert_eq!(queue.lookup("unique").await.unwrap().unwrap().id, id);
    let completed = queue.run_one().await.unwrap().unwrap();
    assert_eq!(completed.state, JobState::Succeeded);
    assert_eq!(completed.result, Some(json!({"a":1, "b":2})));
    assert_eq!(
        queue
            .enqueue("echo", json!({"a": 1, "b": 2}), key("unique"))
            .await
            .unwrap(),
        id
    );
    assert!(queue.run_one().await.unwrap().is_none());
    let (other_app, other) = start(&path, "b", vec![task()]).await;
    assert_ne!(other.identity(), identity);
    assert!(other.get(id).await.unwrap().is_none());
    assert_eq!(other.cancel(id).await.unwrap(), CancelResult::Missing);
    assert!(!other.retry(id).await.unwrap());
    assert!(other.lookup("unique").await.unwrap().is_none());
    assert_ne!(
        other
            .enqueue("echo", json!(0), key("unique"))
            .await
            .unwrap(),
        id
    );
    restarted.shutdown().await.unwrap();
    other_app.shutdown().await.unwrap();
}

#[tokio::test]
async fn delayed_tasks_retry_visibility_exhaustion_and_manual_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    let failing = || {
        Task::new("fail", policy(2, 150), |_: Value, _| async {
            Err(Failure::HandlerFailed)
        })
    };
    let (app, queue) = start(&path, "a", vec![failing()]).await;
    let id = queue
        .enqueue(
            "fail",
            json!(null),
            EnqueueOptions {
                run_at: Some(now() + 200),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(queue.run_one().await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(220)).await;
    let first = queue.run_one().await.unwrap().unwrap();
    assert_eq!(
        (first.state, first.attempt, first.last_error),
        (JobState::Pending, 1, Some(Failure::HandlerFailed))
    );
    assert!(first.run_at > now());
    assert!(!queue.retry(id).await.unwrap());
    assert!(queue.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
    let (app, queue) = start(&path, "a", vec![failing()]).await;
    assert_eq!(queue.get(id).await.unwrap().unwrap(), first);
    tokio::time::sleep(Duration::from_millis(170)).await;
    let final_failure = queue.run_one().await.unwrap().unwrap();
    assert_eq!(
        (final_failure.state, final_failure.attempt),
        (JobState::Failed, 2)
    );
    assert!(queue.retry(id).await.unwrap());
    assert!(!queue.retry(id).await.unwrap());
    let replayed = queue.run_one().await.unwrap().unwrap();
    assert_eq!(
        (replayed.state, replayed.attempt, replayed.max_attempts),
        (JobState::Failed, 3, 3)
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn running_cancellation_stays_running_until_handler_settles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    let started = Arc::new(Notify::new());
    let observed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let business = Task::new("wait", policy(2, 0), {
        let (started, observed, release) = (started.clone(), observed.clone(), release.clone());
        move |_: Value, context| {
            let (started, observed, release) = (started.clone(), observed.clone(), release.clone());
            async move {
                started.notify_one();
                assert_eq!(
                    context.cancellation.cancelled().await,
                    StopReason::Cancelled
                );
                observed.notify_one();
                release.notified().await;
                Ok(json!("an ignored cancellation cannot report success"))
            }
        }
    });
    let (app, queue) = start(&path, "a", vec![business]).await;
    let id = queue
        .enqueue("wait", json!(null), Default::default())
        .await
        .unwrap();
    let runner = tokio::spawn({
        let queue = queue.clone();
        async move { queue.run_one().await }
    });
    started.notified().await;
    // Separate connection models cancellation from another producer process.
    let (manager_app, manager) = start(&path, "a", vec![task()]).await;
    assert_eq!(manager.cancel(id).await.unwrap(), CancelResult::Requested);
    tokio::time::timeout(Duration::from_secs(3), observed.notified())
        .await
        .unwrap();
    let status = manager.get(id).await.unwrap().unwrap();
    assert_eq!(status.state, JobState::Running);
    assert!(status.cancel_requested);
    assert!(!runner.is_finished());
    // Keep renewing while a cancelled handler still owns live work.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(manager.run_one().await.unwrap().is_none());
    assert_eq!(
        manager.get(id).await.unwrap().unwrap().state,
        JobState::Running
    );
    release.notify_one();
    let done = runner.await.unwrap().unwrap().unwrap();
    assert_eq!(
        (done.state, done.last_error, done.result),
        (JobState::Cancelled, Some(Failure::Aborted), None)
    );
    assert_eq!(manager.cancel(id).await.unwrap(), CancelResult::Terminal);
    assert!(!manager.retry(id).await.unwrap());
    manager_app.shutdown().await.unwrap();
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_waiter_keeps_work_admitted_and_shutdown_drains() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let business = Task::new("wait", policy(1, 0), {
        let (started, release) = (started.clone(), release.clone());
        move |_: Value, _| {
            let (started, release) = (started.clone(), release.clone());
            async move {
                started.notify_one();
                release.notified().await;
                Ok(json!(42))
            }
        }
    });
    let (app, queue) = start(&path, "a", vec![business]).await;
    let id = queue
        .enqueue("wait", json!(null), Default::default())
        .await
        .unwrap();
    let waiter = tokio::spawn({
        let queue = queue.clone();
        async move { queue.run_one().await }
    });
    started.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let app = Arc::new(app);
    let shutdown = tokio::spawn({
        let app = app.clone();
        async move { app.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!shutdown.is_finished());
    assert!(matches!(queue.run_one().await, Err(TaskError::Closed)));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (restarted, queue) = start(&path, "a", vec![task()]).await;
    assert_eq!(
        queue.get(id).await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn competing_connections_deduplicate_and_claim_only_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    let (a, qa) = start(&path, "a", vec![task()]).await;
    let (b, qb) = start(&path, "a", vec![task()]).await;
    let (left, right) = tokio::join!(
        qa.enqueue("echo", json!(1), key("race")),
        qb.enqueue("echo", json!(1), key("race"))
    );
    assert_eq!(left.unwrap(), right.unwrap());
    let (left, right) = tokio::join!(qa.run_one(), qb.run_one());
    let outcomes: Vec<_> = [left.unwrap(), right.unwrap()]
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        (outcomes[0].state, outcomes[0].attempt),
        (JobState::Succeeded, 1)
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_cancel_validation_bounded_reads_and_no_implicit_migration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    let instance = Instance::new("tasks", TasksPlugin::new(&path, "a", vec![task()]));
    assert!(App::new().with(instance).start().await.is_err());
    assert!(!path.exists());
    provision(&path, "a").await.unwrap();
    let integer = Task::new(
        "integer",
        policy(1, 0),
        |v: i64, _| async move { Ok(json!(v)) },
    );
    let (app, queue) = start(&path, "a", vec![integer]).await;
    assert!(matches!(
        queue
            .enqueue("integer", json!("invalid"), Default::default())
            .await,
        Err(TaskError::InvalidInput)
    ));
    let first = queue
        .enqueue("integer", json!(1), Default::default())
        .await
        .unwrap();
    let second = queue
        .enqueue("integer", json!(2), Default::default())
        .await
        .unwrap();
    assert_eq!(queue.cancel(first).await.unwrap(), CancelResult::Cancelled);
    assert_eq!(queue.cancel(first).await.unwrap(), CancelResult::Terminal);
    assert_eq!(queue.list("integer", 0, 1).await.unwrap()[0].id, first);
    assert_eq!(queue.list("integer", first, 1).await.unwrap()[0].id, second);
    assert!(matches!(
        queue.list("integer", 0, 101).await,
        Err(TaskError::InvalidOptions)
    ));
    assert_eq!(queue.run_one().await.unwrap().unwrap().id, second);
    assert!(queue.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn panics_and_invalid_results_are_fixed_visible_failures() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    let panics = Task::new("panic", policy(1, 0), |_: Value, _| async {
        panic!("private details");
    });
    let huge = Task::new("huge", policy(1, 0), |_: Value, _| async {
        Ok(json!("x".repeat(65_537)))
    });
    let (app, queue) = start(&path, "a", vec![panics, huge]).await;
    queue
        .enqueue("panic", json!(null), Default::default())
        .await
        .unwrap();
    queue
        .enqueue("huge", json!(null), Default::default())
        .await
        .unwrap();
    assert_eq!(
        queue.run_one().await.unwrap().unwrap().last_error,
        Some(Failure::HandlerFailed)
    );
    assert_eq!(
        queue.run_one().await.unwrap().unwrap().last_error,
        Some(Failure::InvalidResult)
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_attempt_cannot_overwrite_recovered_result() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    provision(&path, "a").await.unwrap();
    let started = Arc::new(Notify::new());
    let lost = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old = Task::new("echo", policy(2, 0), {
        let (started, lost, release) = (started.clone(), lost.clone(), release.clone());
        move |_: Value, context| {
            let (started, lost, release) = (started.clone(), lost.clone(), release.clone());
            async move {
                started.notify_one();
                assert_eq!(
                    context.cancellation.cancelled().await,
                    StopReason::LeaseLost
                );
                lost.notify_one();
                release.notified().await;
                Ok(json!("stale"))
            }
        }
    });
    let (a, qa) = start(&path, "a", vec![old]).await;
    let id = qa
        .enqueue("echo", json!("new"), Default::default())
        .await
        .unwrap();
    let runner = tokio::spawn({
        let qa = qa.clone();
        async move { qa.run_one().await }
    });
    started.notified().await;
    // Advance just this test claim across its expiry boundary without timing races.
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute(
        "UPDATE lenso_task_job SET lease_until = 0 WHERE id = ?1",
        [id],
    )
    .unwrap();
    let (b, qb) = start(&path, "a", vec![task()]).await;
    let done = qb.run_one().await.unwrap().unwrap();
    assert_eq!(
        (done.state, done.attempt, done.result.clone()),
        (JobState::Succeeded, 2, Some(json!("new")))
    );
    tokio::time::timeout(Duration::from_secs(3), lost.notified())
        .await
        .unwrap();
    release.notify_one();
    assert!(matches!(runner.await.unwrap(), Err(TaskError::ClaimLost)));
    assert_eq!(qb.get(id).await.unwrap().unwrap(), done);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

// This helper is executed by the ordinary parent test below, in a separate OS
// process that is deliberately killed after a committed fake business effect.
#[test]
#[ignore = "subprocess fixture, invoked by killed_process_recovers_with_business_idempotency"]
fn crash_worker_process() {
    let path =
        std::path::PathBuf::from(std::env::var_os("LENSO_TASK_FIXTURE_DB").expect("fixture path"));
    let marker = std::path::PathBuf::from(
        std::env::var_os("LENSO_TASK_FIXTURE_MARKER").expect("fixture marker"),
    );
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Task::new("effect", policy(2, 0), {
            let path = path.clone();
            move |input: Value, _| {
                let (path, marker) = (path.clone(), marker.clone());
                async move {
                    fake_effect(&path, input["business_key"].as_str().unwrap());
                    std::fs::write(marker, "committed before ack").unwrap();
                    std::future::pending::<Result<Value, Failure>>().await
                }
            }
        });
        let (_app, queue) = start(&path, "a", vec![handler]).await;
        queue.run_one().await.unwrap();
    });
}
fn fake_effect(path: &Path, key: &str) {
    let db = rusqlite::Connection::open(path).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    db.execute(
        "INSERT INTO fake_effects(business_key) VALUES (?1) ON CONFLICT DO NOTHING",
        [key],
    )
    .unwrap();
}
struct KillOnDrop(std::process::Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
async fn killed_process_recovers_with_business_idempotency() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    let marker = dir.path().join("started");
    provision(&path, "a").await.unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE fake_effects(business_key TEXT PRIMARY KEY)")
        .unwrap();
    let handler = Task::new("effect", policy(2, 0), {
        let path = path.clone();
        move |input: Value, _| {
            let path = path.clone();
            async move {
                fake_effect(&path, input["business_key"].as_str().unwrap());
                Ok(json!("committed"))
            }
        }
    });
    let (app, queue) = start(&path, "a", vec![handler]).await;
    let id = queue
        .enqueue(
            "effect",
            json!({"business_key":"invoice-fake-1"}),
            key("handoff-1"),
        )
        .await
        .unwrap();
    let mut child = KillOnDrop(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "crash_worker_process",
                "--ignored",
                "--nocapture",
            ])
            .env("LENSO_TASK_FIXTURE_DB", &path)
            .env("LENSO_TASK_FIXTURE_MARKER", &marker)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while !marker.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "fixture exited before its effect"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        queue.get(id).await.unwrap().unwrap().state,
        JobState::Running
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let done = queue.run_one().await.unwrap().unwrap();
    assert_eq!(
        (done.id, done.state, done.attempt),
        (id, JobState::Succeeded, 2)
    );
    let effects: i64 = db
        .query_row("SELECT count(*) FROM fake_effects", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        effects, 1,
        "business uniqueness survives redelivery after an unacknowledged effect"
    );
    assert_eq!(queue.lookup("handoff-1").await.unwrap().unwrap().id, id);
    app.shutdown().await.unwrap();
}
