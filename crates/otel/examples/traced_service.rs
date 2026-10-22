//! No listener, collector or real database: a new temp SQLite queue and in-memory DB.
use lenso_log::{Logger, LoggerOptions};
use lenso_otel::{extract, inject, Boundary, Carrier, Telemetry, TelemetryPlugin};
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, EnqueueOptions, RetryPolicy, Task, TasksPlugin};
use opentelemetry::Context;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use serde::Deserialize;
use serde_json::json;
use std::{sync::Arc, time::Duration};
#[derive(Deserialize)]
struct Report {
    trace: Carrier,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = InMemorySpanExporter::default();
    let telemetry = Telemetry::owned(
        SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build(),
        Duration::from_secs(2),
    );
    let logger = Arc::new(Logger::new("reports", "demo", LoggerOptions::default()));
    let task_telemetry = telemetry.clone();
    let task_logger = logger.clone();
    let report = Task::new("report", RetryPolicy::default(), move |input: Report, _| {
        let t = task_telemetry.clone();
        let log = task_logger.clone();
        async move {
            t.instrument(Boundary::Task, &extract(&input.trace), &log, async {
                log.info(
                    json!({"report":"ready", "token":"never-export"}),
                    "report prepared",
                )
                .unwrap();
                Ok(json!({"ready":true}))
            })
            .await
            .map_err(|_| lenso_tasks::Failure::HandlerFailed)?
        }
    });
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("queue.db");
    provision(&path, "demo").await?;
    let queue = Instance::new("queue", TasksPlugin::new(&path, "demo", vec![report]));
    let queue_handle = queue.handle();
    let app = App::new()
        .with(Instance::new(
            "telemetry",
            TelemetryPlugin(telemetry.clone()),
        ))
        .with(queue)
        .start()
        .await?;
    let queue = app.get(&queue_handle)?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        telemetry.instrument(Boundary::Http,&Context::new(),&logger,async {
            telemetry.instrument(Boundary::Operation,&Context::current(),&logger,async {
                telemetry.instrument(Boundary::Database,&Context::current(),&logger,async {
                    let db = rusqlite::Connection::open_in_memory()?;
                    db.execute_batch("CREATE TABLE reports(id INTEGER); INSERT INTO reports VALUES(1);")?;
                    assert_eq!(db.query_row("SELECT count(*) FROM reports",[],|r| r.get::<_,i64>(0))?,1);
                    Ok::<_,rusqlite::Error>(())
                }).await??;
                queue.enqueue("report",json!({"trace":inject(&Context::current())}),EnqueueOptions::default()).await?;
                Ok::<_,Box<dyn std::error::Error>>(())
            }).await??;
            Ok::<_,Box<dyn std::error::Error>>(())
        }).await??;
        queue.run_one().await?;
        Ok(())
    }.await;
    // Read before shutdown because the SDK recording exporter clears on shutdown.
    let spans = exporter.get_finished_spans()?;
    let shutdown = app.shutdown().await;
    if result.is_err() || shutdown.is_err() {
        eprintln!("business result: {result:?}; shutdown result: {shutdown:?}");
        return Err("business operation or shutdown failed".into());
    }
    assert_eq!(spans.len(), 4);
    let trace = spans[0].span_context.trace_id();
    assert!(spans.iter().all(|s| s.span_context.trace_id() == trace));
    println!(
        "HTTP → operation → SQLite + persisted Tasks: {} correlated spans",
        spans.len()
    );
    Ok(())
}
