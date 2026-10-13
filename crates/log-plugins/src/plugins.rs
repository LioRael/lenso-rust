use crate::{
    aggregate::aggregate_configured, source::read_json_lines, AggregateConfig, AggregateError,
    JsonLinesConfig, JsonLinesError, LogRecord, LogSource, MemoryLogSource, Summary,
};
use lenso_plugin_core::{BoxError, Handle, InstanceKey, Plugin, Setup};
use std::{error::Error, fmt, sync::Arc};

/// Reads the explicitly configured JSON-lines file, without keeping a cursor.
#[derive(Clone, Debug)]
pub struct JsonLinesSource {
    config: JsonLinesConfig,
}

impl JsonLinesSource {
    pub fn new(config: JsonLinesConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &JsonLinesConfig {
        &self.config
    }
}

impl LogSource for JsonLinesSource {
    type Error = JsonLinesError;

    fn read_logs(&self) -> Result<Vec<LogRecord>, Self::Error> {
        read_json_lines(&self.config)
    }
}

/// Installs a file source; construction and setup do not open the file.
pub struct JsonLinesPlugin {
    pub config: JsonLinesConfig,
}

impl JsonLinesPlugin {
    pub fn new(config: JsonLinesConfig) -> Self {
        Self { config }
    }
}

impl Plugin for JsonLinesPlugin {
    type Service = JsonLinesSource;

    fn validate(&self) -> Result<(), BoxError> {
        if self.config.path.as_os_str().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "JSON-lines source path must not be empty",
            )
            .into());
        }
        Ok(())
    }

    async fn setup(self, _setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        Ok(Arc::new(JsonLinesSource::new(self.config)))
    }
}

impl Plugin for MemoryLogSource {
    type Service = Self;

    async fn setup(self, _setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        Ok(Arc::new(self))
    }
}

/// Keeps source failures separate from aggregation business failures.
#[derive(Debug)]
pub enum LogAggregationError<E> {
    Source(E),
    Aggregate(AggregateError),
}

impl<E: fmt::Display> fmt::Display for LogAggregationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(f, "log source: {error}"),
            Self::Aggregate(error) => error.fmt(f),
        }
    }
}

impl<E: Error + 'static> Error for LogAggregationError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Aggregate(error) => Some(error),
        }
    }
}

/// The ordinary service can also be constructed directly with any log source.
pub struct LogAggregator<S> {
    source: Arc<S>,
    config: AggregateConfig,
}

impl<S: LogSource> LogAggregator<S> {
    pub fn new(source: Arc<S>, config: AggregateConfig) -> Self {
        Self { source, config }
    }

    pub fn summarize(&self) -> Result<Summary, LogAggregationError<S::Error>> {
        let logs = self
            .source
            .read_logs()
            .map_err(LogAggregationError::Source)?;
        self.aggregate_records(&logs)
            .map_err(LogAggregationError::Aggregate)
    }

    /// Applies this instance's classification to supplied records without reading its source.
    pub fn aggregate_records(&self, logs: &[LogRecord]) -> Result<Summary, AggregateError> {
        self.aggregate_records_with_cancellation(logs, || false)
    }

    /// Uses the same native loop as source-backed aggregation and transport-free calls.
    pub fn aggregate_records_with_cancellation(
        &self,
        logs: &[LogRecord],
        cancelled: impl FnMut() -> bool,
    ) -> Result<Summary, AggregateError> {
        aggregate_configured(logs, &self.config, cancelled)
    }
}

/// Declares an exact typed source dependency, not a source type or global name.
pub struct LogAggregationPlugin<S: Send + Sync + 'static> {
    pub source: Handle<S>,
    pub config: AggregateConfig,
}

impl<S: Send + Sync + 'static> LogAggregationPlugin<S> {
    pub fn new(source: Handle<S>, config: AggregateConfig) -> Self {
        Self { source, config }
    }
}

impl<S: LogSource + Send + Sync + 'static> Plugin for LogAggregationPlugin<S> {
    type Service = LogAggregator<S>;

    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.source.key()]
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let source = setup.get(&self.source)?;
        Ok(Arc::new(LogAggregator::new(source, self.config)))
    }
}
