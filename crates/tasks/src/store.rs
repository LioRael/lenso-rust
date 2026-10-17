use crate::{CancelResult, EnqueueOptions, Failure, JobStatus, Task, TaskError, WorkerOptions};
use serde_json::Value;
use std::{path::Path, sync::Mutex};
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
mod postgres_pool;
#[cfg(all(test, feature = "postgres"))]
#[path = "../tests/support/postgres.rs"]
mod postgres_test_support;
#[cfg(feature = "postgres")]
pub use postgres_pool::PostgresOptions;
mod sqlite;
#[cfg(feature = "postgres")]
pub(crate) use postgres::provision as provision_postgres;
pub(crate) use sqlite::provision;

// Synchronous provider operations run only on the blocking pool. SQLite serializes
// local operations; PostgreSQL leases pool slots. Both fence across connections.
pub(crate) trait StoreConnection: Send {
    fn close(self: Box<Self>) -> Result<(), TaskError>;
    fn enqueue(
        &mut self,
        task: &Task,
        input: &Value,
        options: &EnqueueOptions,
    ) -> Result<i64, TaskError>;
    fn get(&mut self, id: i64) -> Result<Option<JobStatus>, TaskError>;
    fn lookup(&mut self, key: &str) -> Result<Option<JobStatus>, TaskError>;
    fn list(&mut self, task: &str, after: i64, limit: u32) -> Result<Vec<JobStatus>, TaskError>;
    fn cancel(&mut self, id: i64) -> Result<CancelResult, TaskError>;
    fn retry(&mut self, id: i64) -> Result<bool, TaskError>;
    fn claim(
        &mut self,
        tasks: &[String],
        options: &WorkerOptions,
    ) -> Result<Option<Claim>, TaskError>;
    fn pulse(&mut self, claim: &Claim, lease_ms: u32) -> Result<Option<bool>, TaskError>;
    fn settle(&mut self, claim: &Claim, outcome: Result<Value, Failure>) -> Result<(), TaskError>;
}
#[derive(Clone)]
pub(crate) struct Claim {
    pub id: i64,
    pub task: String,
    pub input: Value,
    pub attempt: u32,
    pub token: String,
}

enum StoreInner {
    Sqlite(Mutex<Option<Box<dyn StoreConnection>>>),
    #[cfg(feature = "postgres")]
    Postgres(Box<postgres_pool::PostgresPool>),
}

pub(crate) struct Store {
    inner: StoreInner,
    pub identity: String,
}
impl Store {
    #[cfg(feature = "postgres")]
    pub fn open_postgres(config: PostgresOptions, queue: String) -> Result<Self, TaskError> {
        let pool = postgres_pool::PostgresPool::open(config, queue)?;
        let identity = pool.identity().to_owned();
        Ok(Self {
            inner: StoreInner::Postgres(Box::new(pool)),
            identity,
        })
    }

    pub fn open(path: &Path, queue: String) -> Result<Self, TaskError> {
        let (connection, identity) = sqlite::SqliteConnection::open(path, queue)?;
        Ok(Self {
            inner: StoreInner::Sqlite(Mutex::new(Some(Box::new(connection)))),
            identity,
        })
    }
    pub fn with<T>(
        &self,
        operation: impl FnOnce(&mut dyn StoreConnection) -> Result<T, TaskError>,
    ) -> Result<T, TaskError> {
        match &self.inner {
            StoreInner::Sqlite(inner) => {
                let mut inner = inner.lock().map_err(|_| TaskError::Runtime)?;
                operation(inner.as_mut().ok_or(TaskError::Closed)?.as_mut())
            }
            #[cfg(feature = "postgres")]
            StoreInner::Postgres(pool) => pool.with(operation),
        }
    }
    pub fn close(&self) -> Result<(), TaskError> {
        match &self.inner {
            StoreInner::Sqlite(inner) => {
                if let Some(connection) = inner.lock().map_err(|_| TaskError::Runtime)?.take() {
                    connection.close()?;
                }
                Ok(())
            }
            #[cfg(feature = "postgres")]
            StoreInner::Postgres(pool) => pool.close(),
        }
    }
}
