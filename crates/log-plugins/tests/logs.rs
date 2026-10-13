use lenso_log_plugins::{
    aggregate, aggregate_with_cancellation, AggregateConfig, JsonLinesConfig, JsonLinesError,
    JsonLinesPlugin, JsonLinesSource, LogAggregationError, LogAggregationPlugin, LogAggregator,
    LogRecord, LogSource, MemoryLogSource, Summary,
};
use lenso_plugin_core::{App, Instance, Plugin};
use std::{convert::Infallible, path::PathBuf, sync::Arc};

fn record(level: &str, duration_ms: i64) -> LogRecord {
    LogRecord {
        level: level.into(),
        message: "event".into(),
        duration_ms,
    }
}

fn path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn native_aggregation_preserves_batch_semantics() {
    assert_eq!(
        aggregate(&[record("error", 12), record("info", 8)]).unwrap(),
        Summary {
            count: 2,
            error_count: 1,
            total_duration_ms: 20,
        }
    );
    assert_eq!(
        aggregate(&[record("ERROR", -3)]).unwrap(),
        Summary {
            count: 1,
            error_count: 0,
            total_duration_ms: -3,
        }
    );
    assert_eq!(
        aggregate_with_cancellation(&[], || panic!("empty input must be checked first"))
            .unwrap_err()
            .reason,
        "At least one log is required"
    );
    let mut calls = 0;
    assert_eq!(
        aggregate_with_cancellation(&[record("info", 1), record("info", 1)], || {
            calls += 1;
            calls == 2
        })
        .unwrap_err()
        .reason,
        "Aggregation cancelled"
    );
    assert_eq!(calls, 2);
}

#[test]
fn native_aggregation_reports_overflow() {
    assert_eq!(
        aggregate(&[record("info", i64::MAX), record("info", 1)])
            .unwrap_err()
            .reason,
        "Aggregation overflow"
    );
}

#[test]
fn file_source_parses_structured_records_and_reports_physical_lines() {
    let source = JsonLinesSource::new(JsonLinesConfig::new(path("examples/data/api.jsonl")));
    let logs = source.read_logs().unwrap();
    assert_eq!(logs[0].message, "request failed");
    assert_eq!(aggregate(&logs).unwrap().total_duration_ms, 20);
    assert_eq!(source.read_logs().unwrap(), logs);

    let malformed = JsonLinesSource::new(JsonLinesConfig::new(path("tests/data/malformed.jsonl")));
    assert!(matches!(
        malformed.read_logs(),
        Err(JsonLinesError::Parse { line: 3, .. })
    ));
    let missing =
        JsonLinesSource::new(JsonLinesConfig::new(path("tests/data/missing-field.jsonl")));
    assert!(matches!(
        missing.read_logs(),
        Err(JsonLinesError::Parse { line: 1, .. })
    ));
    let mut strict = source.config().clone();
    strict.skip_blank_lines = false;
    assert!(matches!(
        JsonLinesSource::new(strict).read_logs(),
        Err(JsonLinesError::Parse { line: 2, .. })
    ));
    let absent = JsonLinesSource::new(JsonLinesConfig::new(path("tests/data/absent.jsonl")));
    assert!(matches!(
        absent.read_logs(),
        Err(JsonLinesError::Io { line: None, .. })
    ));
    let aggregator = LogAggregator::new(Arc::new(absent), AggregateConfig::default());
    assert!(matches!(
        aggregator.summarize(),
        Err(LogAggregationError::Source(_))
    ));
    assert_eq!(
        aggregator
            .aggregate_records(&[record("error", 4)])
            .unwrap()
            .total_duration_ms,
        4
    );
    assert_eq!(
        aggregator
            .aggregate_records_with_cancellation(&[record("info", 1)], || true)
            .unwrap_err()
            .reason,
        "Aggregation cancelled"
    );
}

#[test]
fn source_validation_requires_an_explicit_path_but_does_not_open_files() {
    assert!(JsonLinesPlugin::new(JsonLinesConfig::new(""))
        .validate()
        .is_err());
    assert!(
        JsonLinesPlugin::new(JsonLinesConfig::new(path("tests/data/absent.jsonl")))
            .validate()
            .is_ok()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn configured_plugin_instances_keep_sources_and_classification_separate() {
    let api = Instance::new(
        "api",
        JsonLinesPlugin::new(JsonLinesConfig::new(path("examples/data/api.jsonl"))),
    );
    let jobs = Instance::new(
        "jobs",
        JsonLinesPlugin::new(JsonLinesConfig::new(path("examples/data/jobs.jsonl"))),
    );
    let api_summary = Instance::new(
        "api-summary",
        LogAggregationPlugin::new(api.handle(), AggregateConfig::default()),
    );
    let jobs_summary = Instance::new(
        "jobs-summary",
        LogAggregationPlugin::new(
            jobs.handle(),
            AggregateConfig {
                error_level: "warning".into(),
            },
        ),
    );
    let api_handle = api_summary.handle();
    let jobs_handle = jobs_summary.handle();
    let app = App::new()
        .with(api_summary)
        .with(jobs_summary)
        .with(jobs)
        .with(api);
    let running = app.start().await.unwrap();
    assert_eq!(
        running.get(&api_handle).unwrap().summarize().unwrap(),
        Summary {
            count: 2,
            error_count: 1,
            total_duration_ms: 20,
        }
    );
    assert_eq!(
        running.get(&jobs_handle).unwrap().summarize().unwrap(),
        Summary {
            count: 2,
            error_count: 1,
            total_duration_ms: 8,
        }
    );
    let supplied = [record("warning", 1)];
    assert_eq!(
        running
            .get(&api_handle)
            .unwrap()
            .aggregate_records(&supplied)
            .unwrap()
            .error_count,
        0
    );
    assert_eq!(
        running
            .get(&jobs_handle)
            .unwrap()
            .aggregate_records(&supplied)
            .unwrap()
            .error_count,
        1
    );
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn memory_source_is_installable_and_clones_do_not_share_state() {
    let mut first = MemoryLogSource::new(vec![record("error", 7)]);
    let second = first.clone();
    first.replace(vec![record("info", 2)]);
    assert_eq!(first.read_logs().unwrap()[0].duration_ms, 2);
    let mut app = App::new();
    let source = app.install("memory", second);
    let summary = app.install(
        "summary",
        LogAggregationPlugin::new(source, AggregateConfig::default()),
    );
    let running = app.start().await.unwrap();
    assert_eq!(
        running
            .get(&summary)
            .unwrap()
            .summarize()
            .unwrap()
            .total_duration_ms,
        7
    );
    running.shutdown().await.unwrap();
}

#[test]
fn alternate_source_uses_the_same_ordinary_aggregation_service() {
    struct CustomSource;
    impl LogSource for CustomSource {
        type Error = Infallible;

        fn read_logs(&self) -> Result<Vec<LogRecord>, Self::Error> {
            Ok(vec![record("fatal", 9)])
        }
    }
    let aggregator = LogAggregator::new(
        Arc::new(CustomSource),
        AggregateConfig {
            error_level: "fatal".into(),
        },
    );
    assert_eq!(
        aggregator.summarize().unwrap(),
        Summary {
            count: 1,
            error_count: 1,
            total_duration_ms: 9,
        }
    );
}
