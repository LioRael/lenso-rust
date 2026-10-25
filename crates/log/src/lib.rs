//! Scoped structured logging without a global subscriber or Runtime dependency.
#![doc = include_str!("../README.md")]

use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use serde_json::{json, Map, Value};
use std::{
    cell::Cell,
    error::Error,
    fmt,
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{watch, Notify};
use tracing::{instrument::WithSubscriber, Dispatch, Instrument, Span};

/// Structured public fields. Messages and all non-sensitive values must be public.
pub type Fields = Map<String, Value>;

/// Severity ordered from least to most important.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Fine-grained diagnostics.
    Trace,
    /// Development diagnostics.
    Debug,
    /// Normal operation.
    Info,
    /// Recoverable failure.
    Warn,
    /// Failed operation.
    Error,
    /// Fatal failure, emitted at tracing ERROR with severity `fatal`.
    Fatal,
}
impl Level {
    fn label(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fatal => "fatal",
        }
    }
}

/// Only application-approved labels may be returned by a classifier.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrorClassification {
    /// Public stable error code, not a raw error property.
    pub code: Option<String>,
    /// Public execution phase.
    pub phase: Option<String>,
}

/// A classifier never receives permission to serialize the error.
pub type ErrorClassifier = Arc<dyn Fn(&(dyn Error + 'static)) -> ErrorClassification + Send + Sync>;

/// Shared policy for owned and borrowed subscribers.
#[derive(Clone)]
pub struct LoggerOptions {
    /// Minimum emitted severity (the subscriber may filter further).
    pub minimum_level: Level,
    /// Optional public classification. Panics omit classification.
    pub classify_error: Option<ErrorClassifier>,
}
impl Default for LoggerOptions {
    fn default() -> Self {
        Self {
            minimum_level: Level::Info,
            classify_error: None,
        }
    }
}

/// Public failures contain no underlying sink or callback diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogError {
    /// This logger and all its children have closed.
    Closed,
    /// Owned flush returned failure.
    FlushFailed,
    /// Owned flush exceeded its deadline.
    FlushTimeout,
    /// Owned flush panicked or its task failed.
    FlushPanicked,
    /// Trace/span identifiers must be nonzero hexadecimal IDs of standard length.
    InvalidTraceContext,
    /// Convenience methods require a JSON object, not an array or primitive.
    InvalidFields,
}
impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Closed => "logger_closed",
            Self::FlushFailed => "logger_flush_failed",
            Self::FlushTimeout => "logger_flush_timeout",
            Self::FlushPanicked => "logger_flush_panicked",
            Self::InvalidTraceContext => "invalid_trace_context",
            Self::InvalidFields => "invalid_log_fields",
        })
    }
}
impl Error for LogError {}

/// Explicit trace association, independent of an OTel provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: String,
    span_id: String,
}
impl TraceContext {
    /// Validate standard 32/16-character hexadecimal trace/span identifiers.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidTraceContext`] for wrong lengths, non-hexadecimal
    /// characters, or all-zero identifiers.
    pub fn new(trace_id: impl Into<String>, span_id: impl Into<String>) -> Result<Self, LogError> {
        let trace_id = trace_id.into();
        let span_id = span_id.into();
        let valid = |id: &str, length| {
            id.len() == length
                && id.bytes().all(|byte| byte.is_ascii_hexdigit())
                && id.bytes().any(|byte| byte != b'0')
        };
        if !valid(&trace_id, 32) || !valid(&span_id, 16) {
            return Err(LogError::InvalidTraceContext);
        }
        Ok(Self {
            trace_id: trace_id.to_ascii_lowercase(),
            span_id: span_id.to_ascii_lowercase(),
        })
    }
}

type FlushFuture = Pin<Box<dyn Future<Output = Result<(), LogError>> + Send>>;
type Flush = Box<dyn FnOnce() -> FlushFuture + Send>;

struct Inner {
    dispatch: Dispatch,
    options: LoggerOptions,
    writes: Mutex<Writes>,
    drained: Notify,
    completion: watch::Sender<Option<Result<(), LogError>>>,
}

struct Writes {
    closed: bool,
    active: usize,
    flush: Option<(Flush, Duration)>,
}

struct WritePermit(Arc<Inner>);
impl Drop for WritePermit {
    fn drop(&mut self) {
        let drained = {
            let mut writes = self.0.writes.lock().expect("logger admission poisoned");
            writes.active -= 1;
            writes.closed && writes.active == 0
        };
        if drained {
            // Only the owned close task waits; retain a permit if it has not polled yet.
            self.0.drained.notify_one();
        }
    }
}

thread_local! {
    static DISPATCHING: Cell<bool> = const { Cell::new(false) };
}

struct DispatchScope;
impl DispatchScope {
    fn enter() -> Option<Self> {
        DISPATCHING.with(|active| {
            if active.replace(true) {
                None
            } else {
                Some(Self)
            }
        })
    }
}
impl Drop for DispatchScope {
    fn drop(&mut self) {
        DISPATCHING.with(|active| active.set(false));
    }
}

struct Scope {
    owner: Arc<Inner>,
    span: Span,
    trace: Option<TraceContext>,
}
tokio::task_local! {
    static ACTIVE_SCOPE: Scope;
}

/// Cloneable scoped service. Children share policy, subscriber and local lifecycle.
#[derive(Clone)]
pub struct Logger {
    inner: Arc<Inner>,
    plugin_id: Arc<str>,
    instance_name: Arc<str>,
    fields: Fields,
}

impl Logger {
    /// Create an owned synchronous JSON stderr subscriber, never installing globally.
    pub fn new(
        plugin_id: impl Into<String>,
        instance_name: impl Into<String>,
        options: LoggerOptions,
    ) -> Self {
        let subscriber = tracing_subscriber::fmt()
            .json()
            .event_format(StructuredJson)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::stderr)
            .finish();
        Self::from_dispatch(
            plugin_id,
            instance_name,
            Dispatch::new(subscriber),
            options,
            None,
        )
    }

    /// Borrow the host's subscriber via a cloned Dispatch; close never flushes it.
    pub fn borrowed(
        plugin_id: impl Into<String>,
        instance_name: impl Into<String>,
        dispatch: &Dispatch,
        options: LoggerOptions,
    ) -> Self {
        Self::from_dispatch(plugin_id, instance_name, dispatch.clone(), options, None)
    }

    /// Own an explicit flush callback for this logger's subscriber.
    ///
    /// Callback failure is reduced to `FlushFailed`, panic to `FlushPanicked`.
    /// Timeout aborts the callback task. Callbacks must be cooperative async work:
    /// blocking operations cannot be preempted by Tokio.
    pub fn owned_with_flush<F, Fut>(
        plugin_id: impl Into<String>,
        instance_name: impl Into<String>,
        dispatch: Dispatch,
        options: LoggerOptions,
        timeout: Duration,
        flush: F,
    ) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), LogError>> + Send + 'static,
    {
        Self::from_dispatch(
            plugin_id,
            instance_name,
            dispatch,
            options,
            Some((Box::new(move || Box::pin(flush())), timeout)),
        )
    }

    fn from_dispatch(
        plugin_id: impl Into<String>,
        instance_name: impl Into<String>,
        dispatch: Dispatch,
        options: LoggerOptions,
        flush: Option<(Flush, Duration)>,
    ) -> Self {
        let (completion, _) = watch::channel(None);
        Self {
            inner: Arc::new(Inner {
                dispatch,
                options,
                writes: Mutex::new(Writes {
                    closed: false,
                    active: 0,
                    flush,
                }),
                drained: Notify::new(),
                completion,
            }),
            plugin_id: Arc::from(plugin_id.into()),
            instance_name: Arc::from(instance_name.into()),
            fields: Fields::new(),
        }
    }

    /// Add recursively sanitized bindings without changing identity or ownership.
    #[must_use]
    pub fn child(&self, fields: Fields) -> Self {
        let mut child = self.clone();
        child.fields.extend(sanitize_fields(fields));
        child
    }

    /// Run a future with an explicit tracing span and optional trace identifiers.
    ///
    /// Context is entered on each poll, not held across await. Spawned tasks do not
    /// inherit it: scope each spawned future explicitly. Scope is logger-local,
    /// including its children; another logger cannot inherit these IDs or span.
    pub async fn scope<F: Future>(
        &self,
        operation: &str,
        trace: Option<TraceContext>,
        future: F,
    ) -> F::Output {
        let span = tracing::dispatcher::with_default(
            &self.inner.dispatch,
            || tracing::info_span!(parent: None, "lenso_scope", operation),
        );
        ACTIVE_SCOPE
            .scope(
                Scope {
                    owner: self.inner.clone(),
                    span: span.clone(),
                    trace,
                },
                future.instrument(span),
            )
            .with_subscriber(self.inner.dispatch.clone())
            .await
    }

    /// Emit public fields at a severity. Reserved metadata cannot be overridden.
    /// Logging does not re-enter the app work gate: admitted work and cleanup can
    /// continue to log until Resource-phase close.
    ///
    /// # Errors
    /// Returns [`LogError::Closed`] after local write admission is sealed.
    pub fn log(&self, level: Level, message: &str, fields: Fields) -> Result<(), LogError> {
        self.emit(level, message, sanitize_fields(fields))
    }

    /// Emit trace-level object fields.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidFields`] for non-object fields or [`LogError::Closed`].
    pub fn trace(&self, fields: Value, message: &str) -> Result<(), LogError> {
        self.log_value(Level::Trace, fields, message)
    }
    /// Emit debug-level object fields.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidFields`] for non-object fields or [`LogError::Closed`].
    pub fn debug(&self, fields: Value, message: &str) -> Result<(), LogError> {
        self.log_value(Level::Debug, fields, message)
    }
    /// Emit info-level object fields.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidFields`] for non-object fields or [`LogError::Closed`].
    pub fn info(&self, fields: Value, message: &str) -> Result<(), LogError> {
        self.log_value(Level::Info, fields, message)
    }
    /// Emit warning-level object fields.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidFields`] for non-object fields or [`LogError::Closed`].
    pub fn warn(&self, fields: Value, message: &str) -> Result<(), LogError> {
        self.log_value(Level::Warn, fields, message)
    }
    /// Emit error-level object fields. Use `log_error` for native errors.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidFields`] for non-object fields or [`LogError::Closed`].
    pub fn error(&self, fields: Value, message: &str) -> Result<(), LogError> {
        self.log_value(Level::Error, fields, message)
    }
    /// Emit fatal-level object fields.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidFields`] for non-object fields or [`LogError::Closed`].
    pub fn fatal(&self, fields: Value, message: &str) -> Result<(), LogError> {
        self.log_value(Level::Fatal, fields, message)
    }
    fn log_value(&self, level: Level, fields: Value, message: &str) -> Result<(), LogError> {
        match fields {
            Value::Object(fields) => self.log(level, message, fields),
            _ => Err(LogError::InvalidFields),
        }
    }

    /// Emit an opaque error plus only classifier-approved code/phase.
    /// No Display, Debug, source, stack or arbitrary error properties are read.
    ///
    /// # Errors
    /// Returns [`LogError::Closed`] after write admission is sealed. Classifier
    /// panics omit classification rather than failing the log operation.
    pub fn log_error(
        &self,
        message: &str,
        error: &(dyn Error + 'static),
        fields: Fields,
    ) -> Result<(), LogError> {
        let classified = self
            .inner
            .options
            .classify_error
            .as_ref()
            .and_then(|classify| catch_unwind(AssertUnwindSafe(|| classify(error))).ok())
            .unwrap_or_default();
        let mut safe = json!({"type": "Error"});
        if let Some(code) = classified.code {
            safe["code"] = Value::String(code);
        }
        if let Some(phase) = classified.phase {
            safe["phase"] = Value::String(phase);
        }
        let mut fields = sanitize_fields(fields);
        fields.insert("err".into(), safe);
        self.emit(Level::Error, message, fields)
    }

    fn emit(&self, level: Level, message: &str, fields: Fields) -> Result<(), LogError> {
        let _write = {
            let mut writes = self.inner.writes.lock().expect("logger admission poisoned");
            if writes.closed {
                return Err(LogError::Closed);
            }
            writes.active += 1;
            WritePermit(self.inner.clone())
        };
        if level < self.inner.options.minimum_level {
            return Ok(());
        }
        // Tracing cannot replace its thread-local Dispatch from inside a subscriber.
        // Suppress recursive sink logging, including through another Logger.
        let Some(_dispatch) = DispatchScope::enter() else {
            return Ok(());
        };
        let mut record = self.fields.clone();
        record.extend(fields);
        // These identifiers describe the actual scope, not caller-supplied bindings.
        record.remove("traceId");
        record.remove("spanId");
        let span = ACTIVE_SCOPE
            .try_with(|scope| {
                if !Arc::ptr_eq(&scope.owner, &self.inner) {
                    return None;
                }
                if let Some(trace) = &scope.trace {
                    record.insert("traceId".into(), json!(trace.trace_id));
                    record.insert("spanId".into(), json!(trace.span_id));
                }
                Some(scope.span.clone())
            })
            .ok()
            .flatten();
        record.insert("pluginId".into(), json!(&*self.plugin_id));
        record.insert("instanceName".into(), json!(&*self.instance_name));
        record.insert("severity".into(), json!(level.label()));
        let record = Value::Object(record);
        tracing::dispatcher::with_default(&self.inner.dispatch, || {
            macro_rules! event {
                ($level:expr) => {
                    tracing::event!(parent: span.as_ref().and_then(Span::id), $level,
                        message, fields = %record)
                };
            }
            // serde_json is emitted as JSON text inside the tracing fields envelope.
            match level {
                Level::Trace => event!(tracing::Level::TRACE),
                Level::Debug => event!(tracing::Level::DEBUG),
                Level::Info => event!(tracing::Level::INFO),
                Level::Warn => event!(tracing::Level::WARN),
                Level::Error | Level::Fatal => event!(tracing::Level::ERROR),
            }
        });
        Ok(())
    }

    /// Reject further writes atomically, then flush owned resources once.
    ///
    /// Concurrent closes share the same result. Cancellation of a close waiter
    /// does not cancel flushing. Borrowed Dispatch is never flushed or shut down.
    /// Async close requires a Tokio runtime.
    ///
    /// # Errors
    /// Returns [`LogError::FlushFailed`], [`LogError::FlushPanicked`], or
    /// [`LogError::FlushTimeout`] for the owned flush callback. The logger remains
    /// closed and later close calls return the same result without retrying.
    pub async fn close(&self) -> Result<(), LogError> {
        {
            let mut writes = self.inner.writes.lock().expect("logger admission poisoned");
            if !writes.closed {
                writes.closed = true;
                let flush = writes.flush.take();
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    loop {
                        if inner
                            .writes
                            .lock()
                            .expect("logger admission poisoned")
                            .active
                            == 0
                        {
                            break;
                        }
                        inner.drained.notified().await;
                    }
                    let result = if let Some((flush, timeout)) = flush {
                        let mut task = tokio::spawn(async move { flush().await });
                        match tokio::time::timeout(timeout, &mut task).await {
                            Ok(Ok(Ok(()))) => Ok(()),
                            Ok(Ok(Err(_))) => Err(LogError::FlushFailed),
                            Ok(Err(_)) => Err(LogError::FlushPanicked),
                            Err(_) => {
                                task.abort();
                                Err(LogError::FlushTimeout)
                            }
                        }
                    } else {
                        Ok(())
                    };
                    inner.completion.send_replace(Some(result));
                });
            }
        }
        let mut completion = self.inner.completion.subscribe();
        loop {
            if let Some(result) = *completion.borrow_and_update() {
                return result;
            }
            if completion.changed().await.is_err() {
                return Err(LogError::FlushFailed);
            }
        }
    }
}

/// JSON formatter for Lenso events, preserving nested JSON field types.
///
/// Host subscribers receive the same structured record encoded in the tracing
/// `fields` string field; they can use this formatter or their own event visitor.
pub struct StructuredJson;

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for StructuredJson
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        visitor.fields.remove("spans");
        if let Some(scope) = ctx.event_scope() {
            let spans: Vec<Value> = scope
                .from_root()
                .map(|span| json!({"name": span.name(), "id": span.id().into_u64()}))
                .collect();
            visitor.fields.insert("spans".into(), json!(spans));
        }
        visitor.fields.insert("msg".into(), json!(visitor.message));
        visitor
            .fields
            .insert("level".into(), json!(event.metadata().level().as_str()));
        writeln!(writer, "{}", Value::Object(visitor.fields))
    }
}

#[derive(Default)]
struct JsonVisitor {
    fields: Fields,
    message: String,
}
impl tracing::field::Visit for JsonVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message" => self.message = value.to_owned(),
            "fields" => {
                if let Ok(fields) = serde_json::from_str(value) {
                    self.fields = fields;
                }
            }
            _ => {}
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

fn sanitize_fields(fields: Fields) -> Fields {
    fields
        .into_iter()
        .map(|(key, value)| {
            let value = sanitize(&key, value);
            (key, value)
        })
        .collect()
}

fn sanitize(key: &str, value: Value) -> Value {
    let normalized: String = key
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    if matches!(
        normalized.as_str(),
        "authorization"
            | "cookie"
            | "password"
            | "token"
            | "secret"
            | "session"
            | "credential"
            | "credentials"
            | "signature"
            | "connectionstring"
            | "sessioncredential"
            | "sessiontoken"
            | "signedurl"
            | "databaseurl"
            | "body"
            | "payload"
    ) {
        return json!("[REDACTED]");
    }
    if matches!(normalized.as_str(), "err" | "error") {
        return json!({"type": "Error"});
    }
    match value {
        Value::Object(fields) => Value::Object(sanitize_fields(fields)),
        Value::Array(values) => Value::Array(values.into_iter().map(|v| sanitize("", v)).collect()),
        value => value,
    }
}

/// Inject an ordinary Logger service and close it during Resource cleanup.
pub struct LoggerPlugin {
    plugin_id: String,
    options: LoggerOptions,
    dispatch: Option<Dispatch>,
    flush: Option<(Flush, Duration)>,
}
impl LoggerPlugin {
    /// Use an owned default JSON stderr subscriber.
    pub fn new(plugin_id: impl Into<String>, options: LoggerOptions) -> Self {
        Self {
            plugin_id: plugin_id.into(),
            options,
            dispatch: None,
            flush: None,
        }
    }

    /// Use a host-owned subscriber, without flushing or closing it.
    pub fn borrowed(
        plugin_id: impl Into<String>,
        dispatch: &Dispatch,
        options: LoggerOptions,
    ) -> Self {
        Self {
            plugin_id: plugin_id.into(),
            options,
            dispatch: Some(dispatch.clone()),
            flush: None,
        }
    }

    /// Inject a subscriber with an explicit logger-owned bounded flush callback.
    pub fn owned_with_flush<F, Fut>(
        plugin_id: impl Into<String>,
        dispatch: Dispatch,
        options: LoggerOptions,
        timeout: Duration,
        flush: F,
    ) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), LogError>> + Send + 'static,
    {
        Self {
            plugin_id: plugin_id.into(),
            options,
            dispatch: Some(dispatch),
            flush: Some((Box::new(move || Box::pin(flush())), timeout)),
        }
    }
}
impl Plugin for LoggerPlugin {
    type Service = Logger;

    async fn setup(self, setup: &mut Setup) -> Result<Arc<Logger>, BoxError> {
        let logger = Arc::new(match self.dispatch {
            Some(dispatch) => Logger::from_dispatch(
                self.plugin_id,
                setup.instance_name(),
                dispatch,
                self.options,
                self.flush,
            ),
            None => Logger::new(self.plugin_id, setup.instance_name(), self.options),
        });
        let cleanup = logger.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup
                .close()
                .await
                .map_err(|error| Box::new(error) as BoxError)
        });
        Ok(logger)
    }
}
