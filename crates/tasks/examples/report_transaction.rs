#[path = "support/reports.rs"]
mod reports;
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, RetryPolicy, Task, TasksPlugin};
use reports::{ReportInput, Reports};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("provide a new local SQLite path")?;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    provision(&path, "reports").await?;
    // Explicit business schema provisioning, before service startup.
    rusqlite::Connection::open(&path)?.execute_batch(reports::SCHEMA)?;
    let task = Task::new(
        "report",
        RetryPolicy::default(),
        |input: ReportInput, _| async move {
            Ok(json!({"report_id": input.report_id, "heading": input.title.to_uppercase()}))
        },
    );
    let instance = Instance::new("tasks", TasksPlugin::new(&path, "reports", vec![task]));
    let handle = instance.handle();
    let app = App::new().with(instance).start().await?;
    let queue = app.get(&handle)?;
    let reports = Reports(queue.clone());
    let result = async {
        let job = reports
            .submit(
                "local-owner".into(),
                "report-1".into(),
                "Quarterly report".into(),
            )
            .await?
            .ok_or(lenso_tasks::TaskError::InvalidInput)?;
        assert_eq!(
            reports
                .submit(
                    "local-owner".into(),
                    "report-1".into(),
                    "Quarterly report".into()
                )
                .await?,
            Some(job)
        );
        assert_eq!(
            reports
                .receipt("other-owner".into(), "report-1".into())
                .await?,
            None
        );
        queue.run_one().await?;
        println!(
            "business receipt: {:?}; execution: {:?}",
            reports
                .receipt("local-owner".into(), "report-1".into())
                .await?,
            queue.get(job).await?
        );
        Ok::<_, lenso_tasks::TaskError>(())
    }
    .await;
    let shutdown = app.shutdown().await;
    println!("submit: {result:?}; shutdown: {shutdown:?}");
    if result.is_err() || shutdown.is_err() {
        return Err("operation or shutdown failed".into());
    }
    Ok(())
}
