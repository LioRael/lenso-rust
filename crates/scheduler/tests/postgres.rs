#![cfg(feature = "postgres-tests")]
#[path = "../../tasks/tests/support/postgres.rs"]
mod support;
use lenso_plugin_core::{App, Instance, RunningApp};
use lenso_scheduler::{provision, MissedPolicy, Schedule, Scheduler, SchedulerPlugin, TickOptions};
use lenso_tasks::{
    provision_postgres, JobState, PostgresConfig, RetryPolicy, Task, Tasks, TasksPlugin,
};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};

async fn start(config: &PostgresConfig, path: &Path) -> (RunningApp, Arc<Tasks>, Arc<Scheduler>) {
    let echo = Task::new("echo", RetryPolicy::default(), |v: Value, _| async {
        Ok(v)
    });
    let tasks = Instance::new(
        "pg-tasks",
        TasksPlugin::postgres(config.clone(), "handoff", vec![echo]),
    );
    let tasks_handle = tasks.handle();
    let scheduler = Instance::new(
        "scheduler",
        SchedulerPlugin::new(path, "handoff", tasks_handle.clone()),
    );
    let handle = scheduler.handle();
    let app = App::new()
        .with(scheduler)
        .with(tasks)
        .start()
        .await
        .unwrap();
    let tasks = app.get(&tasks_handle).unwrap();
    let scheduler = app.get(&handle).unwrap();
    (app, tasks, scheduler)
}

#[test]
#[ignore = "creates and destroys a temporary socket-only PostgreSQL cluster; needs initdb/pg_ctl"]
fn postgres_handoff_survives_ack_failure_restart_and_competing_schedulers() {
    let cluster = support::Cluster::new();
    let config = cluster.config();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scheduler.sqlite");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        provision_postgres(config.clone(), "handoff").await.unwrap();
        let echo = Task::new("echo", RetryPolicy::default(), |v: Value, _| async { Ok(v) });
        let tasks = Instance::new("bootstrap", TasksPlugin::postgres(config.clone(), "handoff", vec![echo]));
        let handle = tasks.handle();
        let bootstrap = App::new().with(tasks).start().await.unwrap();
        let identity = bootstrap.get(&handle).unwrap().identity().to_owned();
        provision(&path, "handoff", &identity).await.unwrap();
        bootstrap.shutdown().await.unwrap();
        let (app, tasks, scheduler) = start(&config, &path).await;
        let instant = 1_704_067_200_000;
        let schedule = scheduler.create(Schedule {
            key: "pg-handoff".into(), cron: "* * * * *".into(), timezone: "UTC".into(),
            task: "echo".into(), input: json!({"business_key":"test-only"}),
            start_after: instant, missed: MissedPolicy::CatchUp,
        }).await.unwrap();
        // PG acceptance commits, then the independent SQLite acknowledgement fails.
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        sqlite.execute_batch("CREATE TRIGGER fail_ack BEFORE UPDATE OF job_id ON lenso_schedule_fire BEGIN SELECT RAISE(ABORT, 'fixture ack failure'); END").unwrap();
        assert!(scheduler.tick(instant+60_000, TickOptions::default()).await.is_err());
        let accepted = tasks.list("echo",0,100).await.unwrap();
        assert_eq!(accepted.len(),1);
        assert_eq!(scheduler.occurrences(schedule.id,-1,100).await.unwrap()[0].job_id,None);
        app.shutdown().await.unwrap();
        sqlite.execute_batch("DROP TRIGGER fail_ack").unwrap();
        let (a,qa,sa) = start(&config,&path).await;
        let (b,qb,sb) = start(&config,&path).await;
        assert_eq!(qa.identity(),identity);
        let (left,right) = tokio::join!(sa.tick(instant+180_000,TickOptions::default()),sb.tick(instant+180_000,TickOptions::default()));
        assert!(left.unwrap().failures.is_empty()); assert!(right.unwrap().failures.is_empty());
        let jobs = qa.list("echo",0,100).await.unwrap();
        assert_eq!(jobs.len(),3);
        let occurrences=sa.occurrences(schedule.id,-1,100).await.unwrap();
        assert_eq!(occurrences.len(),3);
        assert_eq!(occurrences[0].job_id,Some(accepted[0].id));
        for (job,occurrence) in jobs.iter().zip(&occurrences) {
            assert_eq!(Some(job.id),occurrence.job_id);
            assert_eq!(job.run_at,occurrence.fire_at);
        }
        let (left,right) = tokio::join!(qa.run_one(),qb.run_one());
        let left=left.unwrap().unwrap(); let right=right.unwrap().unwrap();
        assert_ne!(left.id,right.id);
        assert_eq!((left.state,right.state),(JobState::Succeeded,JobState::Succeeded));
        assert_eq!(qa.run_one().await.unwrap().unwrap().state,JobState::Succeeded);
        assert!(qb.run_one().await.unwrap().is_none());
        assert_eq!(sa.tick(instant+180_000,TickOptions::default()).await.unwrap().delivered,0);
        a.shutdown().await.unwrap(); b.shutdown().await.unwrap();
    });
}
