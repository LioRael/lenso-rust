//! Separate processes: prepare NEW_DIRECTORY; approve DIRECTORY; approve DIRECTORY.
#[path = "support/file_review.rs"]
mod business;
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, TasksPlugin};
use lenso_workflow::{Outcome, Signal, WorkflowPlugin};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let phase = args
        .next()
        .ok_or("prepare|approve and directory required")?;
    let dir = std::path::PathBuf::from(args.next().ok_or("directory required")?);
    if !matches!(phase.as_str(), "prepare" | "approve") {
        return Err("unknown phase".into());
    }
    let db = dir.join("review.sqlite");
    let input = dir.join("document.txt");
    if phase == "prepare" {
        std::fs::create_dir(&dir)?;
        std::fs::write(&input, "This local document needs human approval.")?;
        provision(&db, "review").await?;
    }
    let tasks = Instance::new(
        "tasks",
        TasksPlugin::new(&db, "review", vec![business::processing_task()]),
    );
    let th = tasks.handle();
    let flow = Instance::new(
        "workflow",
        WorkflowPlugin::new(th.clone(), business::definition()),
    );
    let fh = flow.handle();
    let app = App::new().with(tasks).with(flow).start().await?;
    let result = async {
        let tasks = app.get(&th)?;
        let flow = app.get(&fh)?;
        if phase == "prepare" {
            business::provision_business(&tasks).await?;
            flow.provision().await?;
            flow.start(
                "document-1".into(),
                json!({"path":input.to_str().ok_or("non-UTF8 path")?}),
            )
            .await?;
            tasks.run_one().await?;
            flow.resume("document-1".into()).await?;
            println!(
                "checkpoint: {:?}",
                flow.checkpoint("document-1".into()).await?
            );
        } else {
            let signal = Signal {
                step: "approval".into(),
                version: 1,
                key: "human-fixture-1".into(),
                choice: true,
                data: json!({"actor":"local-fixture"}),
            };
            let outcome = flow.signal("document-1".into(), 1, signal).await?;
            if !matches!(outcome, Outcome::Applied | Outcome::Duplicate) {
                return Err(format!("signal: {outcome:?}").into());
            }
            // Approval is a durable checkpoint. A crash before this call resumes
            // the same transaction step; the confirmed result is never repeated.
            flow.resume("document-1".into()).await?;
            println!(
                "signal: {outcome:?}; business result: {}",
                flow.result("document-1".into())
                    .await?
                    .ok_or("missing result")?
            );
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let shutdown = app.shutdown().await;
    if result.is_err() || shutdown.is_err() {
        return Err(format!("operation: {result:?}; shutdown: {shutdown:?}").into());
    }
    Ok(())
}
