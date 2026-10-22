use lenso_plugin_core::{App, Instance};
use lenso_tasks::{
    provision, EnqueueOptions, JobState, RetryPolicy, Task, TasksPlugin, WorkerConfig, WorkerPlugin,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[tokio::test]
async fn bounded_stop_join_and_core_drain() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        provision(&path, "reports").await.unwrap();
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let task = Task::new("report", RetryPolicy::default(), {
            let entered = entered.clone();
            let release = release.clone();
            move |v: Value, _| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok(v)
                }
            }
        });
        let tasks = Instance::new("tasks", TasksPlugin::new(&path, "reports", vec![task]));
        let th = tasks.handle();
        let worker = Instance::new(
            "worker",
            WorkerPlugin::new(
                th.clone(),
                WorkerConfig {
                    concurrency: 2,
                    idle_wait: Duration::from_millis(10),
                },
            ),
        );
        let wh = worker.handle();
        let app = App::new().with(tasks).with(worker).start().await.unwrap();
        let queue = app.get(&th).unwrap();
        let worker = app.get(&wh).unwrap();
        let mut ids = vec![];
        for n in 0..3 {
            ids.push(
                queue
                    .enqueue("report", json!(n), EnqueueOptions::default())
                    .await
                    .unwrap(),
            );
        }
        entered.acquire_many(2).await.unwrap().forget();
        assert_eq!(entered.available_permits(), 0);
        assert_eq!(
            queue.get(ids[2]).await.unwrap().unwrap().state,
            JobState::Pending
        );
        worker.stop();
        // A cancelled join waiter does not abandon execution or the shared join result.
        let waiter = tokio::spawn({
            let worker = worker.clone();
            async move { worker.join().await }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        let shutdown = tokio::spawn(async move { app.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), worker.join())
                .await
                .is_err()
        );
        assert!(!shutdown.is_finished());
        release.add_permits(2);
        worker.join().await.unwrap();
        worker.join().await.unwrap();
        shutdown.await.unwrap().unwrap();
        assert!(matches!(
            queue.run_one().await,
            Err(lenso_tasks::TaskError::Closed)
        ));
        let reopened = Instance::new(
            "read",
            TasksPlugin::new(
                &path,
                "reports",
                vec![Task::new(
                    "report",
                    RetryPolicy::default(),
                    |v: Value, _| async { Ok(v) },
                )],
            ),
        );
        let h = reopened.handle();
        let app = App::new().with(reopened).start().await.unwrap();
        let queue = app.get(&h).unwrap();
        for id in &ids[..2] {
            assert_eq!(
                queue.get(*id).await.unwrap().unwrap().state,
                JobState::Succeeded
            );
        }
        assert_eq!(
            queue.get(ids[2]).await.unwrap().unwrap().state,
            JobState::Pending
        );
        app.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_worker_is_woken_by_core_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idle.sqlite");
    provision(&path, "idle").await.unwrap();
    let tasks = Instance::new(
        "tasks",
        TasksPlugin::new(
            &path,
            "idle",
            vec![Task::new(
                "echo",
                RetryPolicy::default(),
                |v: Value, _| async { Ok(v) },
            )],
        ),
    );
    let worker = Instance::new(
        "worker",
        WorkerPlugin::new(
            tasks.handle(),
            WorkerConfig {
                concurrency: 1,
                idle_wait: Duration::from_secs(3600),
            },
        ),
    );
    let app = App::new().with(tasks).with(worker).start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    tokio::time::timeout(Duration::from_secs(2), app.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn all_entrances_stop_before_worker_join() {
    use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
    struct WithEntrance(TasksPlugin, Arc<Semaphore>);
    impl Plugin for WithEntrance {
        type Service = lenso_tasks::Tasks;
        fn validate(&self) -> Result<(), BoxError> {
            self.0.validate()
        }
        async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
            let service = self.0.setup(setup).await?;
            setup.on_cleanup(CleanupPhase::Entrances, move || async move {
                self.1.add_permits(1);
                Ok(())
            });
            Ok(service)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("entrances.sqlite");
    provision(&path, "q").await.unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let task = Task::new("wait", RetryPolicy::default(), {
        let entered = entered.clone();
        let release = release.clone();
        move |v: Value, _| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(v)
            }
        }
    });
    let tasks = Instance::new(
        "tasks",
        WithEntrance(TasksPlugin::new(&path, "q", vec![task]), release),
    );
    let th = tasks.handle();
    let worker = Instance::new(
        "worker",
        WorkerPlugin::new(th.clone(), WorkerConfig::default()),
    );
    let app = App::new().with(tasks).with(worker).start().await.unwrap();
    app.get(&th)
        .unwrap()
        .enqueue("wait", json!(null), EnqueueOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    // The handler waits for the dependency's Entrance cleanup. Joining in the
    // worker Entrance would deadlock before Core can deliver that signal.
    tokio::time::timeout(Duration::from_secs(2), app.shutdown())
        .await
        .unwrap()
        .unwrap();
}
