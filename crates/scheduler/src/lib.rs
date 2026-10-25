//! Durable cron schedules with a recoverable Tasks handoff.
#![doc = include_str!("../README.md")]
mod calendar;
mod plugin;
mod store;

use lenso_plugin_core::WorkGate;
use lenso_tasks::{EnqueueOptions, TaskError, Tasks};
pub use plugin::SchedulerPlugin;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::Path, sync::Arc};

/// Bundled IANA database used for all timezone calculations; independent of the host OS.
pub const TIMEZONE_DATABASE_VERSION: &str = chrono_tz::IANA_TZDB_VERSION;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("invalid scheduler configuration, cron, timezone or options")]
    InvalidOptions,
    #[error("schedule key already belongs to a different definition")]
    ScheduleConflict,
    #[error("scheduler is bound to a different task queue")]
    QueueMismatch,
    #[error("scheduler is closing")]
    Closed,
    #[error("scheduler provider failed")]
    Provider(#[from] rusqlite::Error),
    #[error("scheduler runtime failed")]
    Runtime,
}

/// Policy applies only to occurrences not yet committed to the outbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MissedPolicy {
    /// Retain all occurrences, advancing through the backlog in bounded ticks.
    CatchUp,
    /// Discard instants strictly older than now - grace_ms. Exact boundary is included.
    Skip { grace_ms: u32 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    /// Immutable, caller-chosen key scoped to a scheduler identity.
    pub key: String,
    /// Five fields: minute, hour, day of month, month, day of week.
    pub cron: String,
    pub timezone: String,
    pub task: String,
    pub input: Value,
    /// Exclusive UTC Unix millisecond instant. No implicit backfill before this instant.
    pub start_after: i64,
    pub missed: MissedPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleStatus {
    pub id: i64,
    pub key: String,
    /// Next unexamined UTC minute, not necessarily a matching occurrence.
    pub cursor: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct TickOptions {
    pub max_schedules: u32,
    pub minutes_per_schedule: u32,
    pub max_deliveries: u32,
}
impl Default for TickOptions {
    fn default() -> Self {
        Self {
            max_schedules: 10,
            minutes_per_schedule: 60,
            max_deliveries: 100,
        }
    }
}
impl TickOptions {
    fn validate(self) -> Result<(), SchedulerError> {
        if !(1..=100).contains(&self.max_schedules)
            || !(1..=1440).contains(&self.minutes_per_schedule)
            || !(1..=100).contains(&self.max_deliveries)
        {
            return Err(SchedulerError::InvalidOptions);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryError {
    UnknownTask,
    InvalidInput,
    Conflict,
    QueueUnavailable,
}
impl DeliveryError {
    fn from_task(error: TaskError) -> Self {
        match error {
            TaskError::UnknownTask => Self::UnknownTask,
            TaskError::InvalidInput | TaskError::InvalidOptions => Self::InvalidInput,
            TaskError::DeduplicationConflict => Self::Conflict,
            _ => Self::QueueUnavailable,
        }
    }
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::UnknownTask => "unknown-task",
            Self::InvalidInput => "invalid-input",
            Self::Conflict => "conflict",
            Self::QueueUnavailable => "queue-unavailable",
        }
    }
    pub(crate) fn parse(value: &str) -> Result<Self, SchedulerError> {
        match value {
            "unknown-task" => Ok(Self::UnknownTask),
            "invalid-input" => Ok(Self::InvalidInput),
            "conflict" => Ok(Self::Conflict),
            "queue-unavailable" => Ok(Self::QueueUnavailable),
            _ => Err(SchedulerError::Runtime),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Occurrence {
    pub schedule_id: i64,
    /// UTC instant identifying this occurrence, including each side of a DST fold.
    pub fire_at: i64,
    pub job_id: Option<i64>,
    pub last_error: Option<DeliveryError>,
}
#[derive(Clone, Debug, Default)]
pub struct TickReport {
    pub staged: u32,
    pub delivered: u32,
    pub failures: Vec<Occurrence>,
    /// More scanning or pending delivery remains as of the final read.
    pub more: bool,
}

/// Explicitly create scheduler tables and bind a durable identity to one Tasks queue.
/// Opening the plugin never creates or migrates a database.
/// # Errors
/// Returns invalid identifiers, queue binding conflicts, SQLite or worker failures.
pub async fn provision(
    path: impl AsRef<Path>,
    name: &str,
    queue_identity: &str,
) -> Result<(), SchedulerError> {
    validate_name(name)?;
    validate_name(queue_identity)?;
    let path = path.as_ref().to_owned();
    let name = name.to_owned();
    let queue = queue_identity.to_owned();
    tokio::task::spawn_blocking(move || store::provision(&path, &name, &queue))
        .await
        .map_err(|_| SchedulerError::Runtime)?
}

pub struct Scheduler {
    store: Arc<store::Store>,
    tasks: Arc<Tasks>,
    gate: WorkGate,
}
impl Scheduler {
    /// SQLite phases each own admission; the Tasks phase uses Tasks' own WorkGate.
    /// No nested gate admission is needed. A sealed next phase leaves a durable retry.
    async fn database<T, F>(&self, operation: F) -> Result<T, SchedulerError>
    where
        T: Send + 'static,
        F: FnOnce(&mut store::Database) -> Result<T, SchedulerError> + Send + 'static,
    {
        let guard = self.gate.enter().map_err(|_| SchedulerError::Closed)?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            store.with(operation)
        })
        .await
        .map_err(|_| SchedulerError::Runtime)?
    }

    /// Idempotent for the identical definition. Definitions cannot be overwritten.
    /// # Errors
    /// Returns invalid calendar/schedule, definition conflict, closed admission,
    /// SQLite or worker failures. Reconcile uncertain creation using the same key.
    pub async fn create(&self, schedule: Schedule) -> Result<ScheduleStatus, SchedulerError> {
        calendar::validate(&schedule)?;
        self.database(move |db| db.create(schedule)).await
    }
    /// # Errors
    /// Returns invalid key, closed admission, SQLite or worker failures.
    pub async fn get(&self, key: &str) -> Result<Option<ScheduleStatus>, SchedulerError> {
        let key = key.to_owned();
        self.database(move |db| db.get(&key)).await
    }
    /// Bounded history projection; inputs and provider error text are never returned.
    /// # Errors
    /// Returns invalid pagination arguments, closed admission, SQLite or worker failures.
    pub async fn occurrences(
        &self,
        schedule_id: i64,
        after: i64,
        limit: u32,
    ) -> Result<Vec<Occurrence>, SchedulerError> {
        if schedule_id <= 0 || after < -1 || !(1..=100).contains(&limit) {
            return Err(SchedulerError::InvalidOptions);
        }
        self.database(move |db| db.occurrences(schedule_id, after, limit))
            .await
    }
    /// Finite host-driven invocation. now is a trusted host UTC clock, Unix milliseconds.
    /// An error/cancelled waiter can follow committed progress; retry is safe.
    /// # Errors
    /// Returns invalid time/options, closed admission, SQLite or worker failures.
    /// Earlier phases may already have committed; pending outbox rows remain
    /// recoverable. Individual delivery refusals are recorded in the tick report.
    pub async fn tick(&self, now: i64, options: TickOptions) -> Result<TickReport, SchedulerError> {
        calendar::validate_instant(now)?;
        options.validate()?;
        let staged = self.database(move |db| db.stage(now, options)).await?;
        let pending = self
            .database(move |db| db.pending(options.max_deliveries))
            .await?;
        let mut report = TickReport {
            staged,
            ..TickReport::default()
        };
        for delivery in pending {
            let result = self
                .tasks
                .enqueue(
                    &delivery.task,
                    delivery.input,
                    EnqueueOptions {
                        run_at: Some(delivery.fire_at),
                        deduplication_key: Some(delivery.key),
                    },
                )
                .await
                .map_err(DeliveryError::from_task);
            let schedule_id = delivery.schedule_id;
            let fire_at = delivery.fire_at;
            self.database(move |db| db.acknowledge(schedule_id, fire_at, result))
                .await?;
            match result {
                Ok(_) => report.delivered += 1,
                Err(error) => report.failures.push(Occurrence {
                    schedule_id,
                    fire_at,
                    job_id: None,
                    last_error: Some(error),
                }),
            }
        }
        report.more = self.database(move |db| db.more(now)).await?;
        Ok(report)
    }
}
fn validate_name(name: &str) -> Result<(), SchedulerError> {
    if name.is_empty() || name.len() > 256 {
        Err(SchedulerError::InvalidOptions)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use lenso_plugin_core::{App, Instance};
    use lenso_tasks::{RetryPolicy, Task, TasksPlugin};
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_database_waiter_keeps_work_admitted_until_commit_and_drain() {
        let dir = tempfile::tempdir().unwrap();
        let queue_path = dir.path().join("queue.sqlite");
        let scheduler_path = dir.path().join("scheduler.sqlite");
        lenso_tasks::provision(&queue_path, "test").await.unwrap();
        let definition = Task::new(
            "echo",
            RetryPolicy::default(),
            |input: Value, _| async move { Ok(input) },
        );
        let queue = Instance::new(
            "queue",
            TasksPlugin::new(&queue_path, "test", vec![definition.clone()]),
        );
        let queue_handle = queue.handle();
        let app = App::new().with(queue).start().await.unwrap();
        let tasks = app.get(&queue_handle).unwrap();
        provision(&scheduler_path, "test", tasks.identity())
            .await
            .unwrap();
        let identity = tasks.identity().to_owned();
        app.shutdown().await.unwrap();
        let queue = Instance::new(
            "queue",
            TasksPlugin::new(&queue_path, "test", vec![definition]),
        );
        let instance = Instance::new(
            "scheduler",
            SchedulerPlugin::new(&scheduler_path, "test", queue.handle()),
        );
        let handle = instance.handle();
        let app = App::new().with(queue).with(instance).start().await.unwrap();
        let scheduler = app.get(&handle).unwrap();
        let plan = Schedule {
            key: "cancelled-waiter".into(),
            cron: "* * * * *".into(),
            timezone: "UTC".into(),
            task: "echo".into(),
            input: json!(null),
            start_after: 1_704_067_200_000,
            missed: MissedPolicy::CatchUp,
        };
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let caller = scheduler.clone();
        let waiter = tokio::spawn(async move {
            caller
                .database(move |db| {
                    started.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(5))
                        .map_err(|_| SchedulerError::Runtime)?;
                    db.create(plan)
                })
                .await
        });
        ready.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let mut shutdown = tokio::spawn(async move { app.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        shutdown.await.unwrap().unwrap();
        assert!(matches!(
            scheduler.get("cancelled-waiter").await,
            Err(SchedulerError::Closed)
        ));
        assert!(matches!(
            scheduler.store.with(|_| Ok(())),
            Err(SchedulerError::Closed)
        ));
        let store = store::Store::open(&scheduler_path, "test".into(), &identity).unwrap();
        assert!(store
            .with(|db| db.get("cancelled-waiter"))
            .unwrap()
            .is_some());
        store.close().unwrap();
    }
}
