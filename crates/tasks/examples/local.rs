use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, EnqueueOptions, RetryPolicy, Task, TasksPlugin};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FakeReport {
    report_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("provide a new local SQLite path")?;
    // Reserve a new file exclusively; never migrate a pre-existing user database.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    provision(&path, "fake-reports").await?;
    let report = Task::new(
        "report",
        RetryPolicy::default(),
        |input: FakeReport, context| async move {
            Ok(json!({ "report_id": input.report_id, "attempt": context.attempt, "fake": true }))
        },
    );
    let instance = Instance::new(
        "tasks",
        TasksPlugin::new(&path, "fake-reports", vec![report]),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await?;
    let queue = app.get(&handle)?;
    let result = async {
        let id = queue
            .enqueue(
                "report",
                json!({"report_id":"demo-1"}),
                EnqueueOptions {
                    deduplication_key: Some("demo-1".into()),
                    ..Default::default()
                },
            )
            .await?;
        queue.run_one().await?;
        queue.get(id).await
    }
    .await;
    let shutdown = app.shutdown().await;
    println!("fake report: {result:?}; shutdown: {shutdown:?}");
    if result.is_err() || shutdown.is_err() {
        return Err("operation or shutdown failed (see both outcomes above)".into());
    }
    Ok(())
}
