use lenso_log_plugins::{AggregateConfig, LogAggregationPlugin, LogAggregator, MemoryLogSource};
use lenso_plugin_core::{App, BoxError, Instance};
use lenso_plugin_runtime::{start_runtime_app, RuntimeExport, RuntimePlugin};
use runtime_sdk::{BusinessError, HandlerContext, HostOptions, RuntimeError, StreamSender};
use std::{sync::Arc, time::Duration};
pub mod logs {
    include!("../../generated/logs.rs");
}
use logs::*;

async fn aggregate_native(
    input: AggregateInput,
    context: HandlerContext,
    service: Arc<LogAggregator<MemoryLogSource>>,
) -> Result<AggregateOutput, BusinessError<AggregateError>> {
    let records: Vec<_> = input
        .logs
        .into_iter()
        .map(|log| lenso_log_plugins::LogRecord {
            level: log.level,
            message: log.message,
            duration_ms: log.duration_ms,
        })
        .collect();
    let timing = std::env::var("LENSO_PERF_TIMING").as_deref() == Ok("1");
    let business_started = std::time::Instant::now();
    let result = service
        .aggregate_records_with_cancellation(&records, || context.cancellation.is_cancelled())
        .map_err(|error| {
            BusinessError(AggregateError {
                reason: error.reason,
            })
        })?;
    if timing {
        eprintln!(
            "{}",
            serde_json::json!({
                "event":"aggregate_completed",
                "business_ms":business_started.elapsed().as_secs_f64() * 1000.0
            })
        );
    }
    Ok(AggregateOutput {
        count: result.count,
        error_count: result.error_count,
        total_duration_ms: result.total_duration_ms,
    })
}

#[cfg(test)]
async fn aggregate(
    input: AggregateInput,
    context: HandlerContext,
) -> Result<AggregateOutput, BusinessError<AggregateError>> {
    let service = Arc::new(LogAggregator::new(
        Arc::new(MemoryLogSource::default()),
        AggregateConfig::default(),
    ));
    aggregate_native(input, context, service).await
}

async fn progress(
    input: ProgressInput,
    context: HandlerContext,
    output: StreamSender<ProgressOutput>,
) -> Result<(), RuntimeError> {
    if input.logs.is_empty() {
        return Err(RuntimeError::business(ProgressError {
            reason: "At least one log is required".into(),
        }));
    }
    let total = input.logs.len() as i64;
    for (index, _) in input.logs.iter().enumerate() {
        let delay = input.delay_ms.unwrap_or(0);
        if delay > 0 {
            tokio::select! {
                _ = context.cancellation.cancelled() => return Err(RuntimeError::new("cancelled")),
                _ = tokio::time::sleep(Duration::from_millis(delay as u64)) => (),
            }
        }
        output
            .send(ProgressOutput {
                processed: index as i64 + 1,
                total,
            })
            .await?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let initialization_started = std::time::Instant::now();
    let port = std::env::var("LENSO_RUNTIME_PORT")
        .unwrap_or_else(|_| "7331".into())
        .parse()?;
    let instance = std::env::var("LENSO_RUNTIME_INSTANCE")
        .or_else(|_| std::env::var("LENSO_WORKSPACE_INSTANCE"))
        .unwrap_or_else(|_| "rust-one".into());
    let service = std::env::var("LENSO_RUNTIME_SERVICE").unwrap_or_else(|_| "batch".into());
    let diagnostics = std::env::var_os("LENSO_RUNTIME_DIAGNOSTICS").is_some();
    let mut initialization_count = 0;
    let source = Instance::new("logs-source", MemoryLogSource::default());
    initialization_count += 1;
    let aggregation = Instance::new(
        "aggregation",
        LogAggregationPlugin::new(source.handle(), AggregateConfig::default()),
    );
    initialization_count += 1;
    let runtime = Instance::new(
        "runtime",
        RuntimePlugin::new(
            HostOptions {
                namespace: "demo".into(),
                service: service.clone(),
                instance: instance.clone(),
                port,
                contract: serde_json::from_str(include_str!("../../contracts/logs.json"))?,
                max_concurrency: 8,
            },
            5000,
        ),
    );
    initialization_count += 1;
    let aggregation_handle = aggregation.handle();
    let export = RuntimeExport::new(runtime.handle(), move |running| {
        let service = running.get(&aggregation_handle)?;
        Ok(LogsServer::new()
            .aggregate(move |input, context| aggregate_native(input, context, service.clone()))
            .progress(progress)
            .build()?)
    });
    let app = App::new().with(source).with(aggregation).with(runtime);
    let running = start_runtime_app(app, vec![export]).await?;
    let initialization_elapsed_ms = initialization_started.elapsed().as_millis();
    eprintln!(
        "{}",
        serde_json::json!({
            "event":"service_ready",
            "namespace":"demo",
            "service":service,
            "instance":instance,
            "version":contract().get("version").and_then(serde_json::Value::as_str),
            "port":port,
            "initialization_elapsed_ms":initialization_elapsed_ms,
            "plugin_instance_count":initialization_count
        })
    );
    if diagnostics {
        eprintln!(
            "{}",
            serde_json::json!({"event":"initialization_complete","plugin_instance_count":initialization_count,"elapsed_ms":initialization_elapsed_ms})
        );
    }
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => (),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    running.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_server_infers_closure_types_and_rejects_incomplete_or_duplicate_bindings() {
        assert_eq!(
            LogsServer::new().build().err().unwrap().code,
            "contract_mismatch"
        );
        let server = LogsServer::new()
            .aggregate(|input, _context| async move {
                Ok(AggregateOutput {
                    count: input.logs.len() as i64,
                    error_count: 0,
                    total_duration_ms: 0,
                })
            })
            .progress(|input, _context, output| async move {
                output
                    .send(ProgressOutput {
                        processed: 0,
                        total: input.logs.len() as i64,
                    })
                    .await
            });
        assert!(server.build().is_ok());
        assert_eq!(
            LogsServer::new()
                .aggregate(aggregate)
                .aggregate(aggregate)
                .progress(progress)
                .build()
                .err()
                .unwrap()
                .code,
            "contract_mismatch"
        );
    }
    #[test]
    fn generated_optional_integer_rejects_null_but_accepts_absence() {
        assert!(serde_json::from_str::<ProgressInput>(r#"{"logs":[]}"#)
            .unwrap()
            .delay_ms
            .is_none());
        assert!(serde_json::from_str::<ProgressInput>(r#"{"logs":[],"delay_ms":null}"#).is_err());
        assert!(serde_json::from_str::<ProgressInput>(r#"{}"#).is_err());
    }
    #[tokio::test]
    async fn aggregate_counts_structured_logs_and_returns_typed_business_error() {
        let context = HandlerContext {
            cancellation: runtime_sdk::Cancellation::new(),
            call_id: "one".into(),
            trace_id: "trace".into(),
            parent_id: None,
            caller: serde_json::Value::Null,
        };
        let input = serde_json::from_str(r#"{"logs":[{"level":"error","message":"failed","duration_ms":12},{"level":"info","message":"done","duration_ms":8}]}"#).unwrap();
        let output = aggregate(input, context.clone()).await.unwrap();
        assert_eq!(
            (output.count, output.error_count, output.total_duration_ms),
            (2, 1, 20)
        );
        assert_eq!(
            aggregate(AggregateInput { logs: vec![] }, context)
                .await
                .unwrap_err()
                .0
                .reason,
            "At least one log is required"
        );
    }
}
