//! Durable, at-least-once native tasks.
#![doc = include_str!("../README.md")]
mod owned_worker;
pub use owned_worker::{Worker, WorkerConfig, WorkerPlugin};

mod transaction;
pub use transaction::SqliteTaskTransaction;
mod plugin;
mod store;
mod worker;

use lenso_plugin_core::WorkGate;
pub use plugin::TasksPlugin;
#[cfg(feature = "postgres")]
pub use postgres::Config as PostgresConfig;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, path::Path, pin::Pin, sync::Arc};
#[cfg(feature = "postgres")]
pub use store::PostgresOptions;
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("invalid task configuration or options")]
    InvalidOptions,
    #[error("unknown task")]
    UnknownTask,
    #[error("invalid task input")]
    InvalidInput,
    #[error("deduplication key conflicts with existing task/input")]
    DeduplicationConflict,
    #[error("task queue is closing")]
    Closed,
    #[error("task provider failed")]
    Provider(#[source] rusqlite::Error),
    #[cfg(feature = "postgres")]
    #[error("task PostgreSQL provider failed")]
    Postgres(#[from] postgres::Error),
    #[cfg(feature = "postgres")]
    #[error("task provider contains invalid data")]
    InvalidStoredData,
    #[cfg(feature = "postgres")]
    #[error("task PostgreSQL pool checkout timed out")]
    PostgresPoolTimeout,
    #[cfg(feature = "postgres")]
    #[error("task PostgreSQL queue identity changed")]
    QueueIdentityChanged,
    #[error("this operation requires the owned SQLite provider")]
    SqliteRequired,
    #[error("transaction scope failed; no commit attempted")]
    TransactionAborted,
    #[error("SQLite commit was not confirmed; inspect the original business key before replay")]
    CommitUnconfirmed(#[source] rusqlite::Error),
    #[error("transaction rollback failed after {source}")]
    RollbackFailed {
        source: Box<TaskError>,
        rollback: rusqlite::Error,
    },
    #[error("task runtime failed")]
    Runtime,
    #[error("task claim is no longer owned")]
    ClaimLost,
}
impl From<rusqlite::Error> for TaskError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Provider(error)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Failure {
    HandlerFailed,
    InvalidInput,
    InvalidResult,
    Aborted,
}
impl Failure {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::HandlerFailed => "handler-failed",
            Self::InvalidInput => "invalid-input",
            Self::InvalidResult => "invalid-result",
            Self::Aborted => "aborted",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobStatus {
    pub id: i64,
    pub task: String,
    pub state: JobState,
    pub attempt: u32,
    pub max_attempts: u32,
    pub cancel_requested: bool,
    /// Next eligible execution time, Unix milliseconds. Not a promise of execution.
    pub run_at: i64,
    /// Retained during a pending retry so failures are visible before exhaustion.
    pub last_error: Option<Failure>,
    pub result: Option<Value>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelResult {
    Cancelled,
    Requested,
    Terminal,
    Missing,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    Cancelled,
    LeaseLost,
    Deadline,
    ProviderUnavailable,
}
#[derive(Clone)]
pub struct Cancellation(watch::Receiver<Option<StopReason>>);
impl Cancellation {
    pub fn reason(&self) -> Option<StopReason> {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) -> StopReason {
        let mut receiver = self.0.clone();
        loop {
            if let Some(reason) = *receiver.borrow_and_update() {
                return reason;
            }
            if receiver.changed().await.is_err() {
                return StopReason::LeaseLost;
            }
        }
    }
}
#[derive(Clone)]
pub struct TaskContext {
    pub job_id: i64,
    pub attempt: u32,
    pub cancellation: Cancellation,
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay_ms: u32,
    pub backoff: bool,
    pub max_delay_ms: u32,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            delay_ms: 1000,
            backoff: false,
            max_delay_ms: 60_000,
        }
    }
}
impl RetryPolicy {
    fn validate(&self) -> Result<(), TaskError> {
        if self.max_attempts == 0
            || self.max_attempts > 1_000_000
            || self.max_delay_ms < self.delay_ms
        {
            return Err(TaskError::InvalidOptions);
        }
        Ok(())
    }
}
type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, Failure>> + Send>>;
type Handler = dyn Fn(Value, TaskContext) -> HandlerFuture + Send + Sync;
#[derive(Clone)]
pub struct Task {
    pub(crate) name: String,
    pub(crate) retry: RetryPolicy,
    pub(crate) validate: Arc<dyn Fn(Value) -> bool + Send + Sync>,
    pub(crate) handler: Arc<Handler>,
}
impl Task {
    /// Deserialize at enqueue and again on execution; the original JSON is persisted.
    pub fn new<I, F, Fut>(name: impl Into<String>, retry: RetryPolicy, handler: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        F: Fn(I, TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, Failure>> + Send + 'static,
    {
        Self {
            name: name.into(),
            retry,
            validate: Arc::new(|input| serde_json::from_value::<I>(input).is_ok()),
            handler: Arc::new(move |input, context| {
                let input = match serde_json::from_value::<I>(input) {
                    Ok(input) => input,
                    Err(_) => return Box::pin(async { Err(Failure::InvalidInput) }),
                };
                Box::pin(handler(input, context))
            }),
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct EnqueueOptions {
    pub run_at: Option<i64>,
    pub deduplication_key: Option<String>,
}
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub lease_ms: u32,
    pub heartbeat_ms: u32,
    pub execution_timeout_ms: u32,
}
impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            lease_ms: 30_000,
            heartbeat_ms: 1000,
            execution_timeout_ms: 900_000,
        }
    }
}
impl WorkerOptions {
    pub(crate) fn validate(&self) -> Result<(), TaskError> {
        if self.heartbeat_ms == 0
            || self.heartbeat_ms >= self.lease_ms
            || self.execution_timeout_ms < self.lease_ms
        {
            return Err(TaskError::InvalidOptions);
        }
        Ok(())
    }
}

/// Explicit provisioning only. Startup and service methods never create a schema.
/// Use a dedicated local SQLite file; this does not touch Lenso Storage.
pub async fn provision(path: impl AsRef<Path>, queue: &str) -> Result<(), TaskError> {
    validate_name(queue)?;
    let path = path.as_ref().to_owned();
    let queue = queue.to_owned();
    tokio::task::spawn_blocking(move || store::provision(&path, &queue))
        .await
        .map_err(|_| TaskError::Runtime)?
}

/// Explicit PostgreSQL provisioning; requires PostgreSQL 13+ and a dedicated
/// schema/database. Uses NoTls; use `provision_postgres_with_options` for TLS.
#[cfg(feature = "postgres")]
pub async fn provision_postgres(config: PostgresConfig, queue: &str) -> Result<(), TaskError> {
    provision_postgres_with_options(PostgresOptions::new(config), queue).await
}

/// Explicit provisioning with a caller-selected TLS connector. Opens one connection.
#[cfg(feature = "postgres")]
pub async fn provision_postgres_with_options(
    config: PostgresOptions,
    queue: &str,
) -> Result<(), TaskError> {
    validate_name(queue)?;
    config.validate()?;
    let queue = queue.to_owned();
    tokio::task::spawn_blocking(move || store::provision_postgres(&config, &queue))
        .await
        .map_err(|_| TaskError::Runtime)?
}

pub struct Tasks {
    pub(crate) store: Arc<store::Store>,
    pub(crate) tasks: Arc<BTreeMap<String, Task>>,
    pub(crate) options: WorkerOptions,
    pub(crate) gate: WorkGate,
}
impl Tasks {
    /// Stable identity persisted when the queue is provisioned, suitable for host bindings.
    pub fn identity(&self) -> &str {
        &self.store.identity
    }
    pub(crate) async fn database<T, F>(&self, operation: F) -> Result<T, TaskError>
    where
        T: Send + 'static,
        F: FnOnce(&mut dyn store::StoreConnection) -> Result<T, TaskError> + Send + 'static,
    {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.with(operation))
            .await
            .map_err(|_| TaskError::Runtime)?
    }
    // The admitted guard moves into blocking work: cancelling an await never releases
    // a resource while its database operation is still running.
    async fn admitted<T, F>(&self, operation: F) -> Result<T, TaskError>
    where
        T: Send + 'static,
        F: FnOnce(&mut dyn store::StoreConnection) -> Result<T, TaskError> + Send + 'static,
    {
        let guard = self.gate.enter().map_err(|_| TaskError::Closed)?;
        self.database(move |connection| {
            let _guard = guard;
            operation(connection)
        })
        .await
    }
    pub async fn enqueue(
        &self,
        task: &str,
        input: Value,
        options: EnqueueOptions,
    ) -> Result<i64, TaskError> {
        let definition = prepare_enqueue(&self.tasks, task, &input, &options)?;
        self.admitted(move |db| db.enqueue(&definition, &input, &options))
            .await
    }
    pub async fn get(&self, id: i64) -> Result<Option<JobStatus>, TaskError> {
        self.admitted(move |db| db.get(id)).await
    }
    pub async fn lookup(&self, key: &str) -> Result<Option<JobStatus>, TaskError> {
        let key = key.to_owned();
        self.admitted(move |db| db.lookup(&key)).await
    }
    /// One bounded page, scoped to an explicitly chosen registered task; excludes input.
    pub async fn list(
        &self,
        task: &str,
        after: i64,
        limit: u32,
    ) -> Result<Vec<JobStatus>, TaskError> {
        if !self.tasks.contains_key(task) {
            return Err(TaskError::UnknownTask);
        }
        if limit == 0 || limit > 100 || after < 0 {
            return Err(TaskError::InvalidOptions);
        }
        let task = task.to_owned();
        self.admitted(move |db| db.list(&task, after, limit)).await
    }
    pub async fn cancel(&self, id: i64) -> Result<CancelResult, TaskError> {
        self.admitted(move |db| db.cancel(id)).await
    }
    /// Only failed jobs qualify. Adds one attempt without resetting history.
    /// The trusted host must first decide whether replay is safe for this business task.
    pub async fn retry(&self, id: i64) -> Result<bool, TaskError> {
        self.admitted(move |db| db.retry(id)).await
    }
}
fn validate_name(name: &str) -> Result<(), TaskError> {
    if name.is_empty() || name.len() > 256 {
        Err(TaskError::InvalidOptions)
    } else {
        Ok(())
    }
}
pub(crate) fn valid_json(value: &Value) -> bool {
    fn depth(value: &Value, remaining: usize) -> bool {
        if remaining == 0 {
            return false;
        }
        match value {
            Value::Array(items) => items.iter().all(|v| depth(v, remaining - 1)),
            Value::Object(items) => items.values().all(|v| depth(v, remaining - 1)),
            _ => true,
        }
    }
    depth(value, 33) && serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 65_536)
}

pub(crate) fn prepare_enqueue(
    tasks: &BTreeMap<String, Task>,
    task: &str,
    input: &Value,
    options: &EnqueueOptions,
) -> Result<Task, TaskError> {
    let definition = tasks.get(task).ok_or(TaskError::UnknownTask)?.clone();
    if !valid_json(input) || !(definition.validate)(input.clone()) {
        return Err(TaskError::InvalidInput);
    }
    if options.run_at.is_some_and(|at| at < 0)
        || options
            .deduplication_key
            .as_ref()
            .is_some_and(|key| key.is_empty() || key.len() > 256)
    {
        return Err(TaskError::InvalidOptions);
    }
    Ok(definition)
}
