use lenso_log::{
    ErrorClassification, Fields, Level, LogError, Logger, LoggerOptions, LoggerPlugin,
    StructuredJson, TraceContext,
};
use lenso_plugin_core::{App, BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use serde_json::{json, Value};
use std::{
    fmt,
    io::{self, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tracing::Dispatch;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);
impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Capture {
    fn records(&self) -> Vec<Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}
fn dispatch() -> (Dispatch, Capture) {
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .event_format(StructuredJson)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    (Dispatch::new(subscriber), capture)
}
fn logger() -> (Logger, Capture) {
    let (dispatch, capture) = dispatch();
    (
        Logger::borrowed("demo", "one", &dispatch, LoggerOptions::default()),
        capture,
    )
}
fn fields(value: Value) -> Fields {
    value.as_object().unwrap().clone()
}

#[test]
fn recursive_redaction_covers_arrays_children_and_opaque_errors() {
    let (log, capture) = logger();
    let child = log.child(fields(json!({
        "binding": {"PASSWORD": "private-child"},
        "pluginId": "forged"
    })));
    child
        .info(
            json!({
                "request": {"headers": {"Authorization": "private-header"}},
                "deep": [{"nested": [{"session_token": "private-token"}]}],
                "payload": {"ok": "private-payload"},
                "body": "private-body",
                "err": {"message": "private-error", "code": "private-code"},
                "nested": {"error": "private-primitive"},
                "count": 42, "valid": true
            }),
            "ready",
        )
        .unwrap();
    let record = &capture.records()[0];
    assert_eq!(record["pluginId"], "demo");
    assert_eq!(record["instanceName"], "one");
    assert_eq!(record["count"], 42);
    assert_eq!(record["valid"], true);
    assert_eq!(record["binding"]["PASSWORD"], "[REDACTED]");
    assert_eq!(
        record["deep"][0]["nested"][0]["session_token"],
        "[REDACTED]"
    );
    assert_eq!(record["err"], json!({"type": "Error"}));
    assert_eq!(record["nested"]["error"], json!({"type": "Error"}));
    assert!(!record.to_string().contains("private-"));
}

struct PrivateError;
impl fmt::Debug for PrivateError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("Debug must never be called")
    }
}
impl fmt::Display for PrivateError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("Display must never be called")
    }
}
impl std::error::Error for PrivateError {}

#[test]
fn native_error_only_exposes_approved_classifier_labels() {
    let (dispatch, capture) = dispatch();
    let log = Logger::borrowed(
        "demo",
        "one",
        &dispatch,
        LoggerOptions {
            classify_error: Some(Arc::new(|_| ErrorClassification {
                code: Some("PUBLIC_FAILURE".into()),
                phase: Some("handler".into()),
            })),
            ..Default::default()
        },
    );
    log.log_error("failed", &PrivateError, Fields::new())
        .unwrap();
    assert_eq!(
        capture.records()[0]["err"],
        json!({"type":"Error", "code":"PUBLIC_FAILURE", "phase":"handler"})
    );
}

#[test]
fn classifier_panic_is_optional_and_omits_raw_error() {
    let (dispatch, capture) = dispatch();
    let log = Logger::borrowed(
        "demo",
        "one",
        &dispatch,
        LoggerOptions {
            classify_error: Some(Arc::new(|_| panic!("classifier failed"))),
            ..Default::default()
        },
    );
    log.log_error("failed", &PrivateError, Fields::new())
        .unwrap();
    assert_eq!(capture.records()[0]["err"], json!({"type":"Error"}));
}

#[tokio::test]
async fn borrowed_dispatch_remains_usable_after_local_close() {
    let (dispatch, capture) = dispatch();
    let first = Logger::borrowed("demo", "first", &dispatch, LoggerOptions::default());
    let child = first.child(Fields::new());
    let second = Logger::borrowed("demo", "second", &dispatch, LoggerOptions::default());
    first.info(json!({}), "first").unwrap();
    first.close().await.unwrap();
    assert_eq!(child.info(json!({}), "rejected"), Err(LogError::Closed));
    second.info(json!({}), "still usable").unwrap();
    assert_eq!(capture.records().len(), 2);
}

#[test]
fn minimum_level_and_json_validation_are_explicit() {
    let (log, capture) = logger();
    log.debug(json!({}), "filtered").unwrap();
    log.info(json!({}), "info").unwrap();
    log.warn(json!({}), "warn").unwrap();
    log.error(json!({}), "error").unwrap();
    log.fatal(json!({}), "fatal").unwrap();
    assert_eq!(log.info(json!([]), "invalid"), Err(LogError::InvalidFields));
    let records = capture.records();
    assert_eq!(records.len(), 4);
    assert_eq!(records[3]["severity"], "fatal");
    assert_eq!(records[3]["level"], "ERROR");
}

fn trace(id: char) -> TraceContext {
    TraceContext::new(id.to_string().repeat(32), id.to_string().repeat(16)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_async_scopes_do_not_bleed_and_children_inherit() {
    let (log, capture) = logger();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for id in ['a', 'b'] {
        let log = log.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            log.scope("request", Some(trace(id)), async {
                barrier.wait().await;
                tokio::task::yield_now().await;
                log.child(Fields::new())
                    .info(
                        json!({"request": id.to_string(), "traceId": "forged"}),
                        "scoped",
                    )
                    .unwrap();
            })
            .await;
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    log.info(json!({"traceId":"forged", "spanId":"forged"}), "outside")
        .unwrap();
    let records = capture.records();
    for record in &records[..2] {
        let id = record["request"].as_str().unwrap();
        assert_eq!(record["traceId"], id.repeat(32));
        assert_eq!(record["spanId"], id.repeat(16));
        assert_eq!(record["spans"][0]["name"], "lenso_scope");
    }
    assert_ne!(records[0]["spans"][0]["id"], records[1]["spans"][0]["id"]);
    assert!(records[2].get("traceId").is_none());
    assert!(records[2].get("spanId").is_none());
    assert!(records[2].get("spans").is_none());
}

#[tokio::test]
async fn nested_scopes_restore_and_unrelated_loggers_do_not_inherit() {
    let (log, capture) = logger();
    let (other, other_capture) = logger();
    log.scope("outer", Some(trace('a')), async {
        log.scope("inner", Some(trace('b')), async {
            log.info(json!({}), "inner").unwrap();
            other.info(json!({}), "unrelated").unwrap();
        })
        .await;
        log.info(json!({}), "outer").unwrap();
    })
    .await;
    assert_eq!(capture.records()[0]["traceId"], "b".repeat(32));
    assert_eq!(capture.records()[1]["traceId"], "a".repeat(32));
    assert!(other_capture.records()[0].get("traceId").is_none());
    assert!(other_capture.records()[0].get("spans").is_none());
}

#[test]
fn trace_context_rejects_zero_invalid_length_and_nonhex() {
    assert_eq!(
        TraceContext::new("0".repeat(32), "1".repeat(16)),
        Err(LogError::InvalidTraceContext)
    );
    assert_eq!(
        TraceContext::new("1".repeat(31), "1".repeat(16)),
        Err(LogError::InvalidTraceContext)
    );
    assert_eq!(
        TraceContext::new("g".repeat(32), "1".repeat(16)),
        Err(LogError::InvalidTraceContext)
    );
}

#[tokio::test]
async fn owned_flush_runs_once_and_concurrent_close_shares_failure() {
    let (dispatch, _) = dispatch();
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let log = Logger::owned_with_flush(
        "demo",
        "one",
        dispatch,
        LoggerOptions::default(),
        Duration::from_secs(1),
        move || async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Err(LogError::InvalidFields)
        },
    );
    let (a, b) = tokio::join!(log.close(), log.close());
    assert_eq!(a, Err(LogError::FlushFailed));
    assert_eq!(b, a);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(log.info(json!({}), "closed"), Err(LogError::Closed));
}

#[tokio::test]
async fn owned_flush_timeout_and_panic_have_fixed_public_errors() {
    let (dispatch, _) = dispatch();
    let timed = Logger::owned_with_flush(
        "demo",
        "one",
        dispatch.clone(),
        LoggerOptions::default(),
        Duration::from_millis(10),
        || async { std::future::pending().await },
    );
    assert_eq!(timed.close().await, Err(LogError::FlushTimeout));
    let panicked = Logger::owned_with_flush(
        "demo",
        "one",
        dispatch,
        LoggerOptions::default(),
        Duration::from_secs(1),
        || async { panic!("private panic detail") },
    );
    assert_eq!(panicked.close().await, Err(LogError::FlushPanicked));
}

#[tokio::test]
async fn cancelling_close_waiter_does_not_lose_owned_flush() {
    let (dispatch, _) = dispatch();
    let release = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let signal = started.clone();
    let wait = release.clone();
    let log = Logger::owned_with_flush(
        "demo",
        "one",
        dispatch,
        LoggerOptions::default(),
        Duration::from_secs(1),
        move || async move {
            signal.notify_one();
            wait.notified().await;
            Ok(())
        },
    );
    let waiter_log = log.clone();
    let waiter = tokio::spawn(async move { waiter_log.close().await });
    started.notified().await;
    waiter.abort();
    release.notify_one();
    assert_eq!(log.close().await, Ok(()));
}

struct Dependent(Handle<Logger>);
impl Plugin for Dependent {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.0.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        let log = setup.get(&self.0)?;
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            log.log(Level::Info, "dependent cleanup", Fields::new())?;
            Ok(())
        });
        Ok(Arc::new(()))
    }
}

#[tokio::test]
async fn plugin_uses_core_instance_name_and_closes_after_dependent_cleanup() {
    let (dispatch, capture) = dispatch();
    let mut app = App::new();
    let handle = app.install(
        "exact-instance",
        LoggerPlugin::borrowed("demo", &dispatch, LoggerOptions::default()),
    );
    app.install("dependent", Dependent(handle.clone()));
    let running = app.start().await.unwrap();
    let log = running.get(&handle).unwrap();
    running.shutdown().await.unwrap();
    assert_eq!(capture.records()[0]["instanceName"], "exact-instance");
    assert_eq!(capture.records()[0]["msg"], "dependent cleanup");
    assert_eq!(log.info(json!({}), "closed"), Err(LogError::Closed));
}

#[tokio::test]
async fn plugin_owned_flush_failure_survives_shutdown_and_logger_stays_closed() {
    let (dispatch, _) = dispatch();
    let mut app = App::new();
    let handle = app.install(
        "log",
        LoggerPlugin::owned_with_flush(
            "demo",
            dispatch,
            LoggerOptions::default(),
            Duration::from_secs(1),
            || async { Err(LogError::FlushFailed) },
        ),
    );
    let running = app.start().await.unwrap();
    let log = running.get(&handle).unwrap();
    assert!(running.shutdown().await.is_err());
    assert_eq!(log.close().await, Err(LogError::FlushFailed));
}
