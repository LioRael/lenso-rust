//! Explicit OpenTelemetry adapters; context is correlation, never authorization.
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use opentelemetry::{
    metrics::MeterProvider,
    propagation::TextMapPropagator,
    trace::{FutureExt, SpanKind, Status, TraceContextExt, Tracer, TracerProvider},
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider},
};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
/// Application-owned transport envelope. No baggage is propagated.
pub type Carrier = HashMap<String, String>;
pub fn extract(carrier: &Carrier) -> Context {
    let safe: Carrier = [("traceparent", 55), ("tracestate", 512)]
        .into_iter()
        .filter_map(|(k, n)| {
            carrier
                .get(k)
                .filter(|v| v.len() <= n && v.is_ascii())
                .map(|v| (k.into(), v.clone()))
        })
        .collect();
    TraceContextPropagator::new().extract_with_context(&Context::new(), &safe)
}
pub fn inject(context: &Context) -> Carrier {
    let mut carrier = Carrier::new();
    TraceContextPropagator::new().inject_context(context, &mut carrier);
    carrier
}
/// Fixed labels cannot accidentally contain URLs, SQL or secrets.
#[derive(Clone, Copy, Debug)]
pub enum Boundary {
    Http,
    Operation,
    Database,
    Task,
}
impl Boundary {
    fn label(self) -> &'static str {
        match self {
            Self::Http => "http.request",
            Self::Operation => "business.operation",
            Self::Database => "db.operation",
            Self::Task => "task.process",
        }
    }
    fn kind(self) -> SpanKind {
        match self {
            Self::Http => SpanKind::Server,
            Self::Database => SpanKind::Client,
            Self::Task => SpanKind::Consumer,
            Self::Operation => SpanKind::Internal,
        }
    }
}
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("telemetry_closed")]
    Closed,
    #[error("telemetry_shutdown_failed")]
    Shutdown,
    #[error("telemetry_exporter_configuration_failed")]
    Configuration,
}
struct Inner {
    provider: SdkTracerProvider,
    owned: bool,
    timeout: Duration,
    metrics: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    closed: Mutex<bool>,
    completion: tokio::sync::watch::Sender<Option<Result<(), TelemetryError>>>,
}
#[derive(Clone)]
pub struct Telemetry {
    inner: Arc<Inner>,
}
impl Telemetry {
    pub fn borrowed(provider: SdkTracerProvider) -> Self {
        Self::new(provider, false, Duration::from_secs(5), None)
    }
    pub fn owned(provider: SdkTracerProvider, timeout: Duration) -> Self {
        Self::new(provider, true, timeout, None)
    }
    /// Own trace and metric providers together; both flush during shutdown.
    pub fn owned_with_metrics(
        provider: SdkTracerProvider,
        metrics: opentelemetry_sdk::metrics::SdkMeterProvider,
        timeout: Duration,
    ) -> Self {
        Self::new(provider, true, timeout, Some(metrics))
    }
    fn new(
        provider: SdkTracerProvider,
        owned: bool,
        timeout: Duration,
        metrics: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    ) -> Self {
        let (completion, _) = tokio::sync::watch::channel(None);
        Self {
            inner: Arc::new(Inner {
                provider,
                owned,
                timeout,
                metrics,
                closed: Mutex::new(false),
                completion,
            }),
        }
    }
    /// Official OTLP HTTP/protobuf exporter, bounded batch queue. Explicit host endpoint.
    pub fn collector(endpoint: String, timeout: Duration) -> Result<Self, TelemetryError> {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_timeout(timeout)
            .build()
            .map_err(|_| TelemetryError::Configuration)?;
        Ok(Self::batched(exporter, timeout))
    }
    /// Batch an official or host-supplied exporter with the same bounded queue.
    pub fn batched(
        exporter: impl opentelemetry_sdk::trace::SpanExporter + 'static,
        timeout: Duration,
    ) -> Self {
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(1024)
                    .with_max_export_batch_size(128)
                    .build(),
            )
            .build();
        Self::owned(
            SdkTracerProvider::builder()
                .with_span_processor(processor)
                .build(),
            timeout,
        )
    }
    /// Host retains its outer WorkGuard. Explicit parent is entered on every poll.
    /// Spawned work must receive a carrier explicitly; raw errors are never inspected.
    pub async fn instrument<T, E, F: Future<Output = Result<T, E>>>(
        &self,
        boundary: Boundary,
        parent: &Context,
        logger: &lenso_log::Logger,
        future: F,
    ) -> Result<Result<T, E>, TelemetryError> {
        let context = {
            let closed = self.inner.closed.lock().unwrap_or_else(|p| p.into_inner());
            if *closed {
                return Err(TelemetryError::Closed);
            }
            let tracer = self.inner.provider.tracer("lenso-otel");
            parent.with_span(
                tracer
                    .span_builder(boundary.label())
                    .with_kind(boundary.kind())
                    .start_with_context(&tracer, parent),
            )
        };
        struct End(Context);
        impl Drop for End {
            fn drop(&mut self) {
                self.0.span().end();
            }
        }
        let _end = End(context.clone());
        let sc = context.span().span_context().clone();
        let ids =
            lenso_log::TraceContext::new(sc.trace_id().to_string(), sc.span_id().to_string()).ok();
        let start = std::time::Instant::now();
        let result = logger
            .scope(boundary.label(), ids, future.with_context(context.clone()))
            .await;
        if result.is_err() {
            context
                .span()
                .set_attribute(KeyValue::new("error.type", "operation_failed"));
            context.span().set_status(Status::error("operation_failed"));
        }
        if let Some(provider) = &self.inner.metrics {
            let meter = provider.meter("lenso-otel");
            let labels = [
                KeyValue::new("boundary", boundary.label()),
                KeyValue::new("outcome", if result.is_ok() { "ok" } else { "error" }),
            ];
            meter
                .u64_counter("lenso.operations")
                .build()
                .add(1, &labels);
            meter
                .f64_histogram("lenso.duration")
                .with_unit("s")
                .build()
                .record(start.elapsed().as_secs_f64(), &labels);
        }
        Ok(result)
    }
    /// Call after host work drain. Owned SDK shutdown flushes; cancelled waiters
    /// cannot cancel cleanup. Custom processors must honor the SDK timeout contract.
    pub async fn close(&self) -> Result<(), TelemetryError> {
        {
            let mut closed = self.inner.closed.lock().unwrap_or_else(|p| p.into_inner());
            if !*closed {
                *closed = true;
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    let result = if inner.owned {
                        let provider = inner.provider.clone();
                        let timeout = inner.timeout;
                        let metrics = inner.metrics.clone();
                        match tokio::task::spawn_blocking(move || {
                            let traces = provider.shutdown_with_timeout(timeout);
                            let metrics = metrics
                                .map(|m| m.shutdown_with_timeout(timeout))
                                .transpose();
                            traces.and(metrics.map(|_| ()))
                        })
                        .await
                        {
                            Ok(Ok(())) => Ok(()),
                            _ => Err(TelemetryError::Shutdown),
                        }
                    } else {
                        Ok(())
                    };
                    inner.completion.send_replace(Some(result));
                });
            }
        }
        let mut receiver = self.inner.completion.subscribe();
        loop {
            if let Some(result) = *receiver.borrow_and_update() {
                return result;
            }
            receiver
                .changed()
                .await
                .map_err(|_| TelemetryError::Shutdown)?;
        }
    }
}
pub struct TelemetryPlugin(pub Telemetry);
impl Plugin for TelemetryPlugin {
    type Service = Telemetry;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Telemetry>, BoxError> {
        let service = Arc::new(self.0);
        let cleanup = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().await.map_err(|e| Box::new(e) as BoxError)
        });
        Ok(service)
    }
}
