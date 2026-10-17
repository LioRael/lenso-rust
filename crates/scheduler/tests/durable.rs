use chrono::DateTime;
use lenso_plugin_core::{App, Instance, RunningApp};
use lenso_scheduler::{
    provision, DeliveryError, MissedPolicy, Schedule, Scheduler, SchedulerError, SchedulerPlugin,
    TickOptions,
};
use lenso_tasks::{JobState, RetryPolicy, Task, Tasks, TasksPlugin};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};

fn at(value: &str) -> i64 {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
}
fn task(name: &str) -> Task {
    Task::new(name, RetryPolicy::default(), |value: Value, _| async move {
        Ok(value)
    })
}
struct Fixture {
    _dir: tempfile::TempDir,
    tasks: PathBuf,
    schedules: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let tasks = dir.path().join("tasks.sqlite");
        let schedules = dir.path().join("schedules.sqlite");
        lenso_tasks::provision(&tasks, "test").await.unwrap();
        let instance = Instance::new(
            "bootstrap",
            TasksPlugin::new(&tasks, "test", vec![task("echo")]),
        );
        let handle = instance.handle();
        let app = App::new().with(instance).start().await.unwrap();
        provision(&schedules, "test", app.get(&handle).unwrap().identity())
            .await
            .unwrap();
        app.shutdown().await.unwrap();
        Self {
            _dir: dir,
            tasks,
            schedules,
        }
    }
    async fn start(&self, tasks: Vec<Task>) -> (RunningApp, Arc<Tasks>, Arc<Scheduler>) {
        let instance = Instance::new("tasks", TasksPlugin::new(&self.tasks, "test", tasks));
        let tasks_handle = instance.handle();
        let scheduler = Instance::new(
            "scheduler",
            SchedulerPlugin::new(&self.schedules, "test", tasks_handle.clone()),
        );
        let handle = scheduler.handle();
        // Intentionally reversed installation order exercises exact dependency resolution.
        let app = App::new()
            .with(scheduler)
            .with(instance)
            .start()
            .await
            .unwrap();
        let tasks = app.get(&tasks_handle).unwrap();
        let scheduler = app.get(&handle).unwrap();
        (app, tasks, scheduler)
    }
    fn sql(&self, sql: &str) {
        Connection::open(&self.schedules)
            .unwrap()
            .execute_batch(sql)
            .unwrap();
    }
}
fn plan(key: &str, start: i64) -> Schedule {
    Schedule {
        key: key.into(),
        cron: "* * * * *".into(),
        timezone: "UTC".into(),
        task: "echo".into(),
        input: json!({"business": "test-only"}),
        start_after: start,
        missed: MissedPolicy::CatchUp,
    }
}
fn bounded(minutes: u32, deliveries: u32) -> TickOptions {
    TickOptions {
        max_schedules: 10,
        minutes_per_schedule: minutes,
        max_deliveries: deliveries,
    }
}

#[tokio::test]
async fn bounded_catchup_survives_restart_and_clock_rollback() {
    let f = Fixture::new().await;
    let (app, tasks, scheduler) = f.start(vec![task("echo")]).await;
    let start = at("2024-01-01T00:00:00Z");
    let schedule = plan("minute", start);
    let id = scheduler.create(schedule.clone()).await.unwrap().id;
    assert_eq!(scheduler.create(schedule.clone()).await.unwrap().id, id);
    let mut conflict = schedule;
    conflict.input = json!({"different": true});
    assert!(matches!(
        scheduler.create(conflict).await,
        Err(SchedulerError::ScheduleConflict)
    ));
    assert_eq!(
        scheduler
            .tick(start + 59_999, bounded(2, 1))
            .await
            .unwrap()
            .staged,
        0
    );
    let first = scheduler
        .tick(start + 180_000, bounded(2, 1))
        .await
        .unwrap();
    assert_eq!((first.staged, first.delivered, first.more), (2, 1, true));
    assert_eq!(tasks.list("echo", 0, 100).await.unwrap().len(), 1);
    app.shutdown().await.unwrap();
    assert!(matches!(
        scheduler.get("minute").await,
        Err(SchedulerError::Closed)
    ));
    let (app, tasks, scheduler) = f.start(vec![task("echo")]).await;
    let report = scheduler
        .tick(start + 180_000, bounded(2, 100))
        .await
        .unwrap();
    assert_eq!(
        (report.staged, report.delivered, report.more),
        (1, 2, false)
    );
    assert_eq!(
        scheduler
            .tick(start + 30_000, bounded(2, 100))
            .await
            .unwrap()
            .staged,
        0
    );
    assert_eq!(
        scheduler
            .tick(start + 180_000, bounded(2, 100))
            .await
            .unwrap()
            .delivered,
        0
    );
    let occurrences = scheduler.occurrences(id, -1, 100).await.unwrap();
    assert_eq!(
        occurrences.iter().map(|o| o.fire_at).collect::<Vec<_>>(),
        vec![start + 60_000, start + 120_000, start + 180_000]
    );
    assert_eq!(tasks.list("echo", 0, 100).await.unwrap().len(), 3);
    for _ in 0..3 {
        let job = tasks.run_one().await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Succeeded);
        assert_eq!(job.result, Some(json!({"business":"test-only"})));
    }
    assert!(tasks.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn skipped_misses_use_exact_grace_boundary_but_durable_outbox_is_not_expired() {
    let f = Fixture::new().await;
    let (app, tasks, scheduler) = f.start(vec![task("other")]).await;
    let start = at("2024-01-01T00:00:00Z");
    let mut schedule = plan("skip", start);
    schedule.missed = MissedPolicy::Skip { grace_ms: 60_000 };
    let id = scheduler.create(schedule).await.unwrap().id;
    let report = scheduler
        .tick(start + 180_000, bounded(60, 100))
        .await
        .unwrap();
    assert_eq!(report.staged, 2); // 00:02 and 00:03, including exact grace boundary
    assert_eq!(report.failures.len(), 2);
    assert_eq!(
        report.failures[0].last_error,
        Some(DeliveryError::UnknownTask)
    );
    assert!(tasks.list("other", 0, 100).await.unwrap().is_empty());
    app.shutdown().await.unwrap();
    let (app, tasks, scheduler) = f.start(vec![task("echo")]).await;
    let report = scheduler
        .tick(start + 600_001, bounded(60, 100))
        .await
        .unwrap();
    assert_eq!((report.staged, report.delivered), (1, 3)); // only 00:10 is newly eligible
    let occurrences = scheduler.occurrences(id, -1, 100).await.unwrap();
    assert_eq!(
        occurrences.iter().map(|o| o.fire_at).collect::<Vec<_>>(),
        vec![start + 120_000, start + 180_000, start + 600_000]
    );
    assert_eq!(tasks.list("echo", 0, 100).await.unwrap().len(), 3);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn outbox_commit_and_enqueue_ack_failures_recover_without_duplicate_job() {
    let f = Fixture::new().await;
    let (app, tasks, scheduler) = f.start(vec![task("echo")]).await;
    let start = at("2024-01-01T00:00:00Z");
    let id = scheduler.create(plan("crash", start)).await.unwrap().id;
    // Fail between insertion and cursor update: both roll back in the same transaction.
    f.sql("CREATE TRIGGER fail_cursor BEFORE UPDATE OF cursor ON lenso_schedule BEGIN SELECT RAISE(ABORT, 'test cursor failure'); END;");
    assert!(scheduler.tick(start + 60_000, bounded(1, 1)).await.is_err());
    assert!(scheduler.occurrences(id, -1, 100).await.unwrap().is_empty());
    assert_eq!(
        scheduler.get("crash").await.unwrap().unwrap().cursor,
        start + 60_000
    );
    assert!(tasks.list("echo", 0, 100).await.unwrap().is_empty());
    f.sql("DROP TRIGGER fail_cursor; CREATE TRIGGER fail_ack BEFORE UPDATE OF job_id ON lenso_schedule_fire BEGIN SELECT RAISE(ABORT, 'test ack failure'); END;");
    assert!(scheduler.tick(start + 60_000, bounded(1, 1)).await.is_err());
    let original = tasks.list("echo", 0, 100).await.unwrap();
    assert_eq!(original.len(), 1);
    assert_eq!(
        scheduler.occurrences(id, -1, 100).await.unwrap()[0].job_id,
        None
    );
    // Even a completed task must be reused when recovering the unacknowledged handoff.
    assert_eq!(
        tasks.run_one().await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    app.shutdown().await.unwrap();
    f.sql("DROP TRIGGER fail_ack;");
    let (app, tasks, scheduler) = f.start(vec![task("echo")]).await;
    let recovered = scheduler.tick(start + 60_000, bounded(1, 1)).await.unwrap();
    assert_eq!(
        (recovered.staged, recovered.delivered, recovered.more),
        (0, 1, false)
    );
    assert_eq!(
        scheduler.occurrences(id, -1, 100).await.unwrap()[0].job_id,
        Some(original[0].id)
    );
    assert_eq!(tasks.list("echo", 0, 100).await.unwrap().len(), 1);
    assert!(tasks.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_connections_deduplicate_the_same_schedule_and_trigger() {
    let f = Fixture::new().await;
    let (app_a, tasks_a, a) = f.start(vec![task("echo")]).await;
    let (app_b, tasks_b, b) = f.start(vec![task("echo")]).await;
    let start = at("2024-01-01T00:00:00Z");
    let (created_a, created_b) =
        tokio::join!(a.create(plan("same", start)), b.create(plan("same", start)));
    assert_eq!(created_a.unwrap().id, created_b.unwrap().id);
    let (result_a, result_b) = tokio::join!(
        a.tick(start + 600_000, bounded(60, 100)),
        b.tick(start + 600_000, bounded(60, 100))
    );
    assert!(result_a.unwrap().failures.is_empty());
    assert!(result_b.unwrap().failures.is_empty());
    let jobs = tasks_a.list("echo", 0, 100).await.unwrap();
    assert_eq!(jobs.len(), 10);
    assert_eq!(tasks_b.list("echo", 0, 100).await.unwrap(), jobs);
    let id = a.get("same").await.unwrap().unwrap().id;
    let history = a.occurrences(id, -1, 100).await.unwrap();
    assert_eq!(history.len(), 10);
    assert!(history
        .iter()
        .all(|o| o.job_id.is_some() && o.last_error.is_none()));
    app_a.shutdown().await.unwrap();
    app_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn iana_dst_gaps_folds_and_non_hour_offsets_are_explicit() {
    let f = Fixture::new().await;
    let (app, _, scheduler) = f.start(vec![task("echo")]).await;
    for (key, cron, timezone, start, end, expected) in [
        (
            "spring",
            "30 2 * * *",
            "America/New_York",
            "2024-03-10T05:00:00Z",
            "2024-03-10T09:00:00Z",
            vec![],
        ),
        (
            "fall",
            "30 1 * * *",
            "America/New_York",
            "2024-11-03T04:00:00Z",
            "2024-11-03T08:00:00Z",
            vec!["2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"],
        ),
        (
            "half-hour-fold",
            "45 1 * * *",
            "Australia/Lord_Howe",
            "2024-04-06T13:00:00Z",
            "2024-04-06T17:00:00Z",
            vec!["2024-04-06T14:45:00Z", "2024-04-06T15:15:00Z"],
        ),
        (
            "quarter-offset",
            "0 12 * * *",
            "Asia/Kathmandu",
            "2024-01-01T05:00:00Z",
            "2024-01-01T07:00:00Z",
            vec!["2024-01-01T06:15:00Z"],
        ),
    ] {
        let mut schedule = plan(key, at(start));
        schedule.cron = cron.into();
        schedule.timezone = timezone.into();
        let id = scheduler.create(schedule).await.unwrap().id;
        scheduler.tick(at(end), bounded(300, 100)).await.unwrap();
        let history = scheduler.occurrences(id, -1, 100).await.unwrap();
        assert_eq!(
            history.iter().map(|o| o.fire_at).collect::<Vec<_>>(),
            expected.iter().map(|s| at(s)).collect::<Vec<_>>(),
            "{key}"
        );
    }
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_deliveries_do_not_block_healthy_schedules_and_binding_cannot_change() {
    let f = Fixture::new().await;
    let (app, tasks, scheduler) = f.start(vec![task("echo")]).await;
    let start = at("2024-01-01T00:00:00Z");
    let mut bad = plan("bad", start);
    bad.task = "missing".into();
    let bad_id = scheduler.create(bad).await.unwrap().id;
    let good_id = scheduler.create(plan("good", start)).await.unwrap().id;
    let first = scheduler.tick(start + 60_000, bounded(1, 1)).await.unwrap();
    assert_eq!(first.failures.len(), 1);
    let second = scheduler.tick(start + 60_000, bounded(1, 1)).await.unwrap();
    assert_eq!(second.delivered, 1);
    assert_eq!(tasks.list("echo", 0, 100).await.unwrap().len(), 1);
    assert_eq!(
        scheduler.occurrences(bad_id, -1, 100).await.unwrap()[0].last_error,
        Some(DeliveryError::UnknownTask)
    );
    assert!(scheduler.occurrences(good_id, -1, 100).await.unwrap()[0]
        .job_id
        .is_some());
    assert!(matches!(
        provision(&f.schedules, "test", "different-queue").await,
        Err(SchedulerError::QueueMismatch)
    ));
    provision(&f.schedules, "test", tasks.identity())
        .await
        .unwrap();
    app.shutdown().await.unwrap();
    lenso_tasks::provision(&f.tasks, "other").await.unwrap();
    let queue = Instance::new(
        "wrong",
        TasksPlugin::new(&f.tasks, "other", vec![task("echo")]),
    );
    let scheduler = Instance::new(
        "scheduler",
        SchedulerPlugin::new(&f.schedules, "test", queue.handle()),
    );
    assert!(App::new()
        .with(queue)
        .with(scheduler)
        .start()
        .await
        .is_err());
}

#[tokio::test]
async fn invalid_configuration_is_rejected_and_startup_does_not_provision() {
    let f = Fixture::new().await;
    let (app, _, scheduler) = f.start(vec![task("echo")]).await;
    let start = at("2024-01-01T00:00:00Z");
    for (cron, timezone) in [
        ("61 * * * *", "UTC"),
        ("0 * * * * *", "UTC"),
        ("@daily", "UTC"),
        ("* * * * *", "Not/AZone"),
    ] {
        let mut schedule = plan("invalid", start);
        schedule.cron = cron.into();
        schedule.timezone = timezone.into();
        assert!(matches!(
            scheduler.create(schedule).await,
            Err(SchedulerError::InvalidOptions)
        ));
    }
    assert!(scheduler.get("invalid").await.unwrap().is_none());
    assert!(matches!(
        scheduler.tick(-1, TickOptions::default()).await,
        Err(SchedulerError::InvalidOptions)
    ));
    assert!(matches!(
        scheduler.tick(start, bounded(0, 1)).await,
        Err(SchedulerError::InvalidOptions)
    ));
    app.shutdown().await.unwrap();
    let missing = f._dir.path().join("absent.sqlite");
    let queue = Instance::new(
        "tasks",
        TasksPlugin::new(&f.tasks, "test", vec![task("echo")]),
    );
    let scheduler = Instance::new(
        "scheduler",
        SchedulerPlugin::new(&missing, "test", queue.handle()),
    );
    assert!(App::new()
        .with(queue)
        .with(scheduler)
        .start()
        .await
        .is_err());
    assert!(!missing.exists());
}

#[tokio::test]
async fn conflicting_queue_acceptance_and_invalid_input_are_never_acknowledged() {
    let f = Fixture::new().await;
    let typed = Task::new("number", RetryPolicy::default(), |n: u32, _| async move {
        Ok(json!(n))
    });
    let (app, tasks, scheduler) = f.start(vec![task("echo"), typed]).await;
    let start = at("2024-01-01T00:00:00Z");
    let conflict = scheduler.create(plan("conflict", start)).await.unwrap();
    let mut invalid = plan("invalid-input", start);
    invalid.task = "number".into();
    let invalid = scheduler.create(invalid).await.unwrap();
    let identity: String = Connection::open(&f.schedules)
        .unwrap()
        .query_row(
            "SELECT identity FROM lenso_scheduler WHERE name='test'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let reserved = format!(
        "lenso:scheduler:v1:{identity}:{}:{}",
        conflict.id,
        start + 60_000
    );
    tasks
        .enqueue(
            "echo",
            json!({"different":true}),
            lenso_tasks::EnqueueOptions {
                deduplication_key: Some(reserved),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let report = scheduler
        .tick(start + 60_000, bounded(1, 100))
        .await
        .unwrap();
    assert_eq!(
        (report.staged, report.delivered, report.failures.len()),
        (2, 0, 2)
    );
    assert_eq!(
        scheduler.occurrences(conflict.id, -1, 100).await.unwrap()[0].last_error,
        Some(DeliveryError::Conflict)
    );
    assert_eq!(
        scheduler.occurrences(invalid.id, -1, 100).await.unwrap()[0].last_error,
        Some(DeliveryError::InvalidInput)
    );
    assert!(report.failures.iter().all(|o| o.job_id.is_none()));
    app.shutdown().await.unwrap();
}
