//! Fake-only public API consumer. Requires a new, empty destination directory.
use lenso_plugin_core::{App, Instance};
use lenso_scheduler::{MissedPolicy, Schedule, SchedulerPlugin, TickOptions};
use lenso_tasks::{RetryPolicy, Task, TasksPlugin};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("provide a new test directory")?,
    );
    // create_dir refuses an existing directory; never opens user databases.
    std::fs::create_dir(&path)?;
    let tasks_path = path.join("tasks.sqlite");
    let scheduler_path = path.join("scheduler.sqlite");
    lenso_tasks::provision(&tasks_path, "demo").await?;
    let bootstrap = Instance::new(
        "bootstrap",
        TasksPlugin::new(&tasks_path, "demo", vec![task()]),
    );
    let handle = bootstrap.handle();
    let bootstrap = App::new().with(bootstrap).start().await?;
    let provision =
        lenso_scheduler::provision(&scheduler_path, "demo", bootstrap.get(&handle)?.identity())
            .await;
    let cleanup = bootstrap.shutdown().await;
    if provision.is_err() || cleanup.is_err() {
        return Err(format!("provision: {provision:?}; cleanup: {cleanup:?}").into());
    }
    let queue = Instance::new("queue", TasksPlugin::new(&tasks_path, "demo", vec![task()]));
    let queue_handle = queue.handle();
    let scheduler = Instance::new(
        "scheduler",
        SchedulerPlugin::new(&scheduler_path, "demo", queue_handle.clone()),
    );
    let handle = scheduler.handle();
    let app = App::new().with(queue).with(scheduler).start().await?;
    let queue = app.get(&queue_handle)?;
    let scheduler = app.get(&handle)?;
    let result = async {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        let minute = now / 60_000 * 60_000;
        let schedule = scheduler
            .create(Schedule {
                key: "demo".into(),
                cron: "* * * * *".into(),
                timezone: "Europe/London".into(),
                task: "echo".into(),
                input: json!({"fake":true}),
                start_after: minute - 60_000,
                missed: MissedPolicy::CatchUp,
            })
            .await?;
        let report = scheduler.tick(now, TickOptions::default()).await?;
        let job = queue.run_one().await?;
        println!("tick={report:?}; job={job:?}");
        println!(
            "occurrences={:?}",
            scheduler.occurrences(schedule.id, -1, 100).await?
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let cleanup = app.shutdown().await;
    if result.is_err() || cleanup.is_err() {
        return Err(format!("operation: {result:?}; cleanup: {cleanup:?}").into());
    }
    Ok(())
}
fn task() -> Task {
    Task::new(
        "echo",
        RetryPolicy::default(),
        |value: Value, _| async move { Ok(value) },
    )
}
