#![cfg(feature = "postgres")]
#[path = "support/postgres.rs"]
mod support;
use lenso_plugin_core::{App, Instance, RunningApp};
use lenso_tasks::{
    provision_postgres, CancelResult, EnqueueOptions, Failure, JobState, PostgresConfig,
    RetryPolicy, StopReason, Task, TaskError, Tasks, TasksPlugin, WorkerOptions,
};
use postgres::{Client, NoTls};
use serde_json::{json, Value};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::Notify;

fn policy(attempts: u32, delay: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts: attempts,
        delay_ms: delay,
        max_delay_ms: 1000,
        backoff: true,
    }
}
fn echo() -> Task {
    Task::new("echo", policy(2, 0), |v: Value, _| async { Ok(v) })
}
fn key(s: &str) -> EnqueueOptions {
    EnqueueOptions {
        deduplication_key: Some(s.into()),
        ..Default::default()
    }
}
async fn start(config: &PostgresConfig, name: &str, tasks: Vec<Task>) -> (RunningApp, Arc<Tasks>) {
    let instance = Instance::new(
        "tasks",
        TasksPlugin::postgres(config.clone(), name, tasks).worker_options(WorkerOptions {
            lease_ms: 500,
            heartbeat_ms: 30,
            execution_timeout_ms: 15_000,
        }),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let queue = app.get(&handle).unwrap();
    (app, queue)
}
async fn sql<T: Send + 'static>(
    config: &PostgresConfig,
    f: impl FnOnce(&mut Client) -> T + Send + 'static,
) -> T {
    let config = config.clone();
    tokio::task::spawn_blocking(move || f(&mut config.connect(NoTls).unwrap()))
        .await
        .unwrap()
}

// Explicit opt-in: this starts a fresh temporary PG cluster, not a configured server.
#[test]
#[ignore = "requires local initdb/pg_ctl; starts and destroys its own socket-only cluster"]
fn postgres_contract() {
    let cluster = support::Cluster::new();
    let config = cluster.config();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let missing = Instance::new(
            "missing",
            TasksPlugin::postgres(config.clone(), "a", vec![echo()]),
        );
        assert!(App::new().with(missing).start().await.is_err());
        assert!(
            sql(&config, |c| c
                .query_one("SELECT to_regclass('lenso_task_job') IS NULL", &[])
                .unwrap()
                .get::<_, bool>(0))
            .await
        );
        let (a, b) = tokio::join!(
            provision_postgres(config.clone(), "a"),
            provision_postgres(config.clone(), "a")
        );
        a.unwrap();
        b.unwrap();
        concurrent_acceptance_and_workers(&config).await;
        retry_and_restart(&config).await;
        cancellation_and_stale_fencing(&config).await;
        killed_process_recovery(&config, cluster.socket()).await;
    });
}
async fn concurrent_acceptance_and_workers(config: &PostgresConfig) {
    let mut apps = Vec::new();
    let mut queues = Vec::new();
    for _ in 0..4 {
        let (app, queue) = start(config, "a", vec![echo()]).await;
        apps.push(app);
        queues.push(queue);
    }
    let identity = queues[0].identity().to_owned();
    let mut accepts = Vec::new();
    for i in 0..16 {
        let q = queues[i % 4].clone();
        accepts.push(tokio::spawn(async move {
            q.enqueue("echo", json!({"a":1,"b":2}), key("same"))
                .await
                .unwrap()
        }));
    }
    let id = accepts.remove(0).await.unwrap();
    for accept in accepts {
        assert_eq!(accept.await.unwrap(), id);
    }
    assert!(matches!(
        queues[1].enqueue("echo", json!(2), key("same")).await,
        Err(TaskError::DeduplicationConflict)
    ));
    for i in 0..24 {
        queues[i % 4]
            .enqueue("echo", json!(i), key(&format!("job-{i}")))
            .await
            .unwrap();
    }
    let mut runners = Vec::new();
    for queue in &queues {
        let queue = queue.clone();
        runners.push(tokio::spawn(async move {
            let mut ids = Vec::new();
            while let Some(done) = queue.run_one().await.unwrap() {
                assert_eq!((done.state, done.attempt), (JobState::Succeeded, 1));
                ids.push(done.id);
            }
            ids
        }));
    }
    let mut seen = HashSet::new();
    for runner in runners {
        for id in runner.await.unwrap() {
            assert!(seen.insert(id), "duplicate live claim");
        }
    }
    assert_eq!(seen.len(), 25);
    assert_eq!(
        queues[0]
            .enqueue("echo", json!({"b":2,"a":1}), key("same"))
            .await
            .unwrap(),
        id
    );
    assert_eq!(queues[0].list("echo", 0, 100).await.unwrap().len(), 25);
    for app in apps {
        app.shutdown().await.unwrap();
    }
    assert!(matches!(queues[0].get(id).await, Err(TaskError::Closed)));
    let (app, q) = start(config, "a", vec![echo()]).await;
    assert_eq!(q.identity(), identity);
    assert_eq!(q.lookup("same").await.unwrap().unwrap().id, id);
    provision_postgres(config.clone(), "other").await.unwrap();
    let (other_app, other) = start(config, "other", vec![echo()]).await;
    assert_ne!(other.identity(), identity);
    assert!(other.get(id).await.unwrap().is_none());
    assert!(other.lookup("same").await.unwrap().is_none());
    assert_eq!(other.cancel(id).await.unwrap(), CancelResult::Missing);
    assert!(!other.retry(id).await.unwrap());
    other_app.shutdown().await.unwrap();
    app.shutdown().await.unwrap();
}
async fn retry_and_restart(config: &PostgresConfig) {
    provision_postgres(config.clone(), "retry").await.unwrap();
    let fail = || {
        Task::new("fail", policy(2, 150), |_: Value, _| async {
            Err(Failure::HandlerFailed)
        })
    };
    let (app, q) = start(config, "retry", vec![fail()]).await;
    let now = sql(config, |c| {
        c.query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .unwrap()
        .get::<_, i64>(0)
    })
    .await;
    let id = q
        .enqueue(
            "fail",
            json!(null),
            EnqueueOptions {
                run_at: Some(now + 200),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(q.run_one().await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(220)).await;
    let first = q.run_one().await.unwrap().unwrap();
    assert_eq!(
        (first.state, first.attempt, first.last_error),
        (JobState::Pending, 1, Some(Failure::HandlerFailed))
    );
    assert!(!q.retry(id).await.unwrap());
    assert!(q.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
    let (app, q) = start(config, "retry", vec![fail()]).await;
    assert_eq!(q.get(id).await.unwrap().unwrap(), first);
    tokio::time::sleep(Duration::from_millis(180)).await;
    assert_eq!(q.run_one().await.unwrap().unwrap().state, JobState::Failed);
    assert!(q.retry(id).await.unwrap());
    let done = q.run_one().await.unwrap().unwrap();
    assert_eq!(
        (done.state, done.attempt, done.max_attempts),
        (JobState::Failed, 3, 3)
    );
    let pending = q
        .enqueue("fail", json!(null), Default::default())
        .await
        .unwrap();
    assert_eq!(q.cancel(pending).await.unwrap(), CancelResult::Cancelled);
    assert!(q.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
}
async fn cancellation_and_stale_fencing(config: &PostgresConfig) {
    provision_postgres(config.clone(), "cancel").await.unwrap();
    for (name, expected) in [
        ("cancel", StopReason::Cancelled),
        ("stale", StopReason::LeaseLost),
    ] {
        let started = Arc::new(Notify::new());
        let observed = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler = Task::new("echo", policy(2, 0), {
            let (started, observed, release) = (started.clone(), observed.clone(), release.clone());
            move |_: Value, ctx| {
                let (started, observed, release) =
                    (started.clone(), observed.clone(), release.clone());
                async move {
                    started.notify_one();
                    assert_eq!(ctx.cancellation.cancelled().await, expected);
                    observed.notify_one();
                    release.notified().await;
                    Ok(json!("stale success"))
                }
            }
        });
        let (app, q) = start(config, "cancel", vec![handler]).await;
        let (manager_app, manager) = start(config, "cancel", vec![echo()]).await;
        let id = q.enqueue("echo", json!("new"), key(name)).await.unwrap();
        let waiter = tokio::spawn({
            let q = q.clone();
            async move { q.run_one().await }
        });
        started.notified().await;
        if expected == StopReason::Cancelled {
            assert_eq!(manager.cancel(id).await.unwrap(), CancelResult::Requested);
            tokio::time::timeout(Duration::from_secs(3), observed.notified())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert_eq!(
                manager.get(id).await.unwrap().unwrap().state,
                JobState::Running
            );
            assert!(manager.run_one().await.unwrap().is_none());
            // Cancelling the waiter must retain admission until the handler settles.
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
            let shutdown = tokio::spawn(async move { app.shutdown().await });
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!shutdown.is_finished());
            assert!(matches!(q.get(id).await, Err(TaskError::Closed)));
            release.notify_one();
            shutdown.await.unwrap().unwrap();
            let done = manager.get(id).await.unwrap().unwrap();
            assert_eq!(
                (done.state, done.result, done.last_error),
                (JobState::Cancelled, None, Some(Failure::Aborted))
            );
        } else {
            sql(config, move |c| {
                c.execute(
                    "UPDATE lenso_task_job SET lease_until=0 WHERE id=$1",
                    &[&id],
                )
                .unwrap()
            })
            .await;
            let done = manager.run_one().await.unwrap().unwrap();
            assert_eq!(
                (done.state, done.attempt, done.result.clone()),
                (JobState::Succeeded, 2, Some(json!("new")))
            );
            tokio::time::timeout(Duration::from_secs(3), observed.notified())
                .await
                .unwrap();
            release.notify_one();
            assert!(matches!(waiter.await.unwrap(), Err(TaskError::ClaimLost)));
            assert_eq!(manager.get(id).await.unwrap().unwrap(), done);
            app.shutdown().await.unwrap();
        }
        manager_app.shutdown().await.unwrap();
    }
}
async fn effect(config: &PostgresConfig) {
    sql(config, |c| {
        c.execute(
            "INSERT INTO fake_effects(business_key) VALUES ('invoice-1') ON CONFLICT DO NOTHING",
            &[],
        )
        .unwrap()
    })
    .await;
}
struct KillOnDrop(std::process::Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn killed_process_recovery(config: &PostgresConfig, socket: std::path::PathBuf) {
    provision_postgres(config.clone(), "crash").await.unwrap();
    sql(config, |c| {
        c.batch_execute("CREATE TABLE fake_effects(business_key TEXT PRIMARY KEY)")
            .unwrap()
    })
    .await;
    let handler = Task::new("effect", policy(2, 0), {
        let config = config.clone();
        move |_: Value, _| {
            let config = config.clone();
            async move {
                effect(&config).await;
                Ok(json!("committed"))
            }
        }
    });
    let (app, q) = start(config, "crash", vec![handler]).await;
    let id = q
        .enqueue("effect", json!(null), key("effect"))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("started");
    let mut child = KillOnDrop(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "postgres_crash_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("LENSO_TASK_FIXTURE_SOCKET", socket)
            .env("LENSO_TASK_FIXTURE_MARKER", &marker)
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while !marker.exists() {
            assert!(child.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(q.get(id).await.unwrap().unwrap().state, JobState::Running);
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    app.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    let handler = Task::new("effect", policy(2, 0), {
        let config = config.clone();
        move |_: Value, _| {
            let config = config.clone();
            async move {
                effect(&config).await;
                Ok(json!("committed"))
            }
        }
    });
    let (app, q) = start(config, "crash", vec![handler]).await;
    let done = q.run_one().await.unwrap().unwrap();
    assert_eq!(
        (done.id, done.state, done.attempt),
        (id, JobState::Succeeded, 2)
    );
    assert_eq!(
        sql(config, |c| c
            .query_one("SELECT count(*) FROM fake_effects", &[])
            .unwrap()
            .get::<_, i64>(0))
        .await,
        1
    );
    assert_eq!(q.lookup("effect").await.unwrap().unwrap().id, id);
    app.shutdown().await.unwrap();
}
#[test]
#[ignore = "child fixture invoked only by postgres_contract"]
fn postgres_crash_fixture() {
    let config = support::config(
        std::env::var_os("LENSO_TASK_FIXTURE_SOCKET")
            .expect("fixture socket")
            .into(),
    );
    let marker = std::path::PathBuf::from(
        std::env::var_os("LENSO_TASK_FIXTURE_MARKER").expect("fixture marker"),
    );
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Task::new("effect", policy(2, 0), {
            let config = config.clone();
            move |_: Value, _| {
                let config = config.clone();
                let marker = marker.clone();
                async move {
                    effect(&config).await;
                    std::fs::write(marker, "committed").unwrap();
                    std::future::pending::<Result<Value, Failure>>().await
                }
            }
        });
        let (_app, q) = start(&config, "crash", vec![handler]).await;
        q.run_one().await.unwrap();
    });
}
