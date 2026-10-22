use lenso_log::{Logger, LoggerOptions};
use lenso_otel::{extract, inject, Boundary, Carrier, Telemetry};
use opentelemetry::{
    trace::{Span, TraceContextExt, Tracer, TracerProvider},
    Context,
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
fn logger() -> Logger {
    Logger::borrowed(
        "test",
        "test",
        &tracing::Dispatch::new(tracing_subscriber::registry()),
        LoggerOptions::default(),
    )
}
#[tokio::test]
async fn concurrent_trees_and_safe_errors() {
    let exporter = InMemorySpanExporter::default();
    let t = Telemetry::borrowed(
        SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build(),
    );
    let log = logger();
    let run = || async {
        t.instrument(Boundary::Http, &Context::new(), &log, async {
            tokio::task::yield_now().await;
            t.instrument(Boundary::Database, &Context::current(), &log, async {
                Err::<(), _>("SECRET raw SQL password")
            })
            .await
            .unwrap()
            .unwrap_err();
            Ok::<_, ()>(())
        })
        .await
        .unwrap()
        .unwrap();
    };
    tokio::join!(run(), run());
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 4);
    let roots: Vec<_> = spans.iter().filter(|s| s.name == "http.request").collect();
    assert_ne!(
        roots[0].span_context.trace_id(),
        roots[1].span_context.trace_id()
    );
    for child in spans.iter().filter(|s| s.name == "db.operation") {
        assert!(roots
            .iter()
            .any(|r| r.span_context.span_id() == child.parent_span_id
                && r.span_context.trace_id() == child.span_context.trace_id()));
        assert!(!format!("{child:?}").contains("SECRET"));
    }
    t.close().await.unwrap();
    assert!(t
        .instrument(Boundary::Http, &Context::new(), &log, async {
            Ok::<_, ()>(())
        })
        .await
        .is_err());
}
#[test]
fn propagation_excludes_baggage_and_invalid_context() {
    let input: Carrier = [
        (
            "traceparent".into(),
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".into(),
        ),
        ("baggage".into(), "password=SECRET".into()),
    ]
    .into();
    let output = inject(&extract(&input));
    assert!(!output.contains_key("baggage"));
    assert_eq!(output["traceparent"], input["traceparent"]);
    let bad: Carrier = [("traceparent".into(), "x".repeat(1000))].into();
    assert!(!extract(&bad).span().span_context().is_valid());
}
#[tokio::test]
async fn borrowed_provider_remains_usable() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    Telemetry::borrowed(provider.clone()).close().await.unwrap();
    provider.tracer("host").start("still alive").end();
    assert_eq!(exporter.get_finished_spans().unwrap().len(), 1);
}
#[derive(Debug, Clone, Default)]
struct Capture(Arc<Mutex<Vec<http::Request<bytes::Bytes>>>>);
#[async_trait::async_trait]
impl opentelemetry_http::HttpClient for Capture {
    async fn send_bytes(
        &self,
        request: http::Request<bytes::Bytes>,
    ) -> Result<http::Response<bytes::Bytes>, opentelemetry_http::HttpError> {
        self.0.lock().unwrap().push(request);
        Ok(http::Response::new(bytes::Bytes::new()))
    }
}
#[tokio::test]
async fn official_otlp_encoding_is_flushed_to_memory_transport() {
    use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
    use prost::Message;
    let capture = Capture::default();
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint("http://fixture.invalid/v1/traces")
        .with_http_client(capture.clone())
        .build()
        .unwrap();
    let t = Telemetry::batched(exporter, Duration::from_secs(2));
    t.instrument(Boundary::Operation, &Context::new(), &logger(), async {
        Ok::<_, ()>(())
    })
    .await
    .unwrap()
    .unwrap();
    t.close().await.unwrap();
    let requests = capture.0.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].headers()["content-type"],
        "application/x-protobuf"
    );
    let decoded =
        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(
            requests[0].body().clone(),
        )
        .unwrap();
    assert_eq!(
        decoded.resource_spans[0].scope_spans[0].spans[0].name,
        "business.operation"
    );
}

#[derive(Debug)]
struct SlowShutdown {
    started: Arc<tokio::sync::Notify>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    done: Arc<std::sync::atomic::AtomicBool>,
}
impl opentelemetry_sdk::trace::SpanExporter for SlowShutdown {
    async fn export(
        &self,
        _: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.started.notify_one();
        self.release.lock().unwrap().recv_timeout(timeout).unwrap();
        self.done.store(true, std::sync::atomic::Ordering::SeqCst);
        Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(
            "SECRET provider failure".into(),
        ))
    }
}
#[tokio::test]
async fn cancelled_close_waiter_preserves_owned_cleanup_and_safe_failure() {
    let started = Arc::new(tokio::sync::Notify::new());
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (release, receiver) = std::sync::mpsc::channel();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(SlowShutdown {
            started: started.clone(),
            release: Mutex::new(receiver),
            done: done.clone(),
        })
        .build();
    let t = Telemetry::owned(provider, Duration::from_secs(2));
    let waiter = tokio::spawn({
        let t = t.clone();
        async move { t.close().await }
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    waiter.abort();
    release.send(()).unwrap();
    let error = t.close().await.unwrap_err();
    assert_eq!(error.to_string(), "telemetry_shutdown_failed");
    assert!(done.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn bounded_metrics_export_actual_counts() {
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let t = Telemetry::owned_with_metrics(
        SdkTracerProvider::builder().build(),
        provider.clone(),
        Duration::from_secs(2),
    );
    for _ in 0..3 {
        t.instrument(Boundary::Operation, &Context::new(), &logger(), async {
            Ok::<_, ()>(())
        })
        .await
        .unwrap()
        .unwrap();
    }
    provider.force_flush().unwrap();
    let data = exporter.get_finished_metrics().unwrap();
    let rendered = format!("{data:?}");
    assert!(rendered.contains("lenso.operations"));
    // Typed data assertions below keep counts independent of Debug formatting.
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    let counter = data
        .iter()
        .flat_map(|r| r.scope_metrics())
        .flat_map(|s| s.metrics())
        .find(|m| m.name() == "lenso.operations")
        .unwrap();
    match counter.data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
            assert_eq!(sum.data_points().map(|p| p.value()).sum::<u64>(), 3)
        }
        _ => panic!("expected u64 counter"),
    }
    t.close().await.unwrap();
}

#[derive(Clone)]
struct LogBytes(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn structured_log_ids_match_exported_span_and_secrets_are_redacted() {
    let bytes = LogBytes(Arc::default());
    let writer = bytes.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .event_format(lenso_log::StructuredJson)
        .with_writer(move || writer.clone())
        .finish();
    let logger = Logger::borrowed(
        "test",
        "test",
        &tracing::Dispatch::new(subscriber),
        LoggerOptions::default(),
    );
    let exporter = InMemorySpanExporter::default();
    let t = Telemetry::borrowed(
        SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build(),
    );
    t.instrument(Boundary::Operation,&Context::new(),&logger,async {
        logger.info(serde_json::json!({"authorization":"SECRET", "body":{"password":"SECRET"}, "traceId":"forged"}),"safe operation").unwrap();
        Ok::<_,()>(())
    }).await.unwrap().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let text = String::from_utf8(bytes.0.lock().unwrap().clone()).unwrap();
    assert!(!text.contains("SECRET"));
    assert!(!text.contains("forged"));
    let record: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    assert_eq!(
        record["traceId"],
        spans[0].span_context.trace_id().to_string()
    );
    assert_eq!(
        record["spanId"],
        spans[0].span_context.span_id().to_string()
    );
}

#[tokio::test]
async fn plugin_waits_for_host_admission_before_provider_shutdown() {
    use lenso_otel::TelemetryPlugin;
    use lenso_plugin_core::{App, Instance};
    let exporter = InMemorySpanExporter::default();
    let t = Telemetry::owned(
        SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build(),
        Duration::from_secs(2),
    );
    let app = Arc::new(
        App::new()
            .with(Instance::new("telemetry", TelemetryPlugin(t.clone())))
            .start()
            .await
            .unwrap(),
    );
    let guard = app.work_gate().enter().unwrap();
    let shutdown = tokio::spawn({
        let app = app.clone();
        async move { app.shutdown().await }
    });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    t.instrument(Boundary::Database, &Context::new(), &logger(), async {
        Ok::<_, ()>(())
    })
    .await
    .unwrap()
    .unwrap();
    drop(guard);
    shutdown.await.unwrap().unwrap();
    assert!(t
        .instrument(Boundary::Http, &Context::new(), &logger(), async {
            Ok::<_, ()>(())
        })
        .await
        .is_err());
}
