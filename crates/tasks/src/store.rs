use crate::{
    CancelResult, EnqueueOptions, Failure, JobState, JobStatus, Task, TaskError, WorkerOptions,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use std::{path::Path, sync::Mutex, time::Duration};

// Use SQLite's wall clock for coordination across independent connections/processes.
// Clock adjustments still affect leases; fenced queue writes cannot fence business effects.
const NOW: &str = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
const COLUMNS: &str =
    "id, task, state, attempt, max_attempts, cancel_requested, run_at, last_error, result";
const DELAY: &str = "MIN(retry_max_ms, retry_delay_ms * (CASE WHEN retry_backoff = 1 THEN (1 << MIN(MAX(attempt - 1, 0), 30)) ELSE 1 END))";

pub(crate) struct Store {
    inner: Mutex<Option<StoreConnection>>,
    pub identity: String,
}
pub(crate) struct StoreConnection {
    connection: Connection,
    queue: String,
}
#[derive(Clone)]
pub(crate) struct Claim {
    pub id: i64,
    pub task: String,
    pub input: Value,
    pub attempt: u32,
    pub token: String,
}
impl Store {
    pub fn open(path: &Path, queue: String) -> Result<Self, TaskError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        configure(&connection)?;
        let identity = connection.query_row(
            "SELECT identity FROM lenso_task_queue WHERE name = ?1",
            [&queue],
            |row| row.get(0),
        )?;
        connection.prepare(&format!("SELECT {COLUMNS}, input, dedup_key, retry_delay_ms, retry_max_ms, retry_backoff, token, lease_until, expires_at FROM lenso_task_job LIMIT 0"))?;
        Ok(Self {
            inner: Mutex::new(Some(StoreConnection { connection, queue })),
            identity,
        })
    }
    pub fn with<T>(
        &self,
        operation: impl FnOnce(&mut StoreConnection) -> Result<T, TaskError>,
    ) -> Result<T, TaskError> {
        let mut inner = self.inner.lock().map_err(|_| TaskError::Runtime)?;
        operation(inner.as_mut().ok_or(TaskError::Closed)?)
    }
    pub fn close(&self) -> Result<(), TaskError> {
        let mut inner = self.inner.lock().map_err(|_| TaskError::Runtime)?;
        if let Some(db) = inner.take() {
            if let Err((connection, error)) = db.connection.close() {
                *inner = Some(StoreConnection {
                    connection,
                    queue: db.queue,
                });
                return Err(error.into());
            }
        }
        Ok(())
    }
}
fn configure(connection: &Connection) -> Result<(), TaskError> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(())
}
pub(crate) fn provision(path: &Path, queue: &str) -> Result<(), TaskError> {
    let mut connection = Connection::open(path)?;
    configure(&connection)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(include_str!("../migrations/0001_tasks.sql"))?;
    transaction.execute(
        "INSERT INTO lenso_task_queue(name) VALUES (?1) ON CONFLICT(name) DO NOTHING",
        [queue],
    )?;
    transaction.commit()?;
    Ok(())
}
fn status(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobStatus> {
    let state: String = row.get(2)?;
    let state = match state.as_str() {
        "pending" => JobState::Pending,
        "running" => JobState::Running,
        "succeeded" => JobState::Succeeded,
        "failed" => JobState::Failed,
        "cancelled" => JobState::Cancelled,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    let error: Option<String> = row.get(7)?;
    let last_error = match error.as_deref() {
        None => None,
        Some("handler-failed") => Some(Failure::HandlerFailed),
        Some("invalid-input") => Some(Failure::InvalidInput),
        Some("invalid-result") => Some(Failure::InvalidResult),
        Some("aborted") => Some(Failure::Aborted),
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    let result: Option<String> = row.get(8)?;
    let result = result
        .map(|raw| {
            serde_json::from_str::<Value>(&raw)
                .ok()
                .filter(crate::valid_json)
                .ok_or(rusqlite::Error::InvalidQuery)
        })
        .transpose()?;
    Ok(JobStatus {
        id: row.get(0)?,
        task: row.get(1)?,
        state,
        attempt: row.get(3)?,
        max_attempts: row.get(4)?,
        cancel_requested: row.get(5)?,
        run_at: row.get(6)?,
        last_error,
        result,
    })
}
impl StoreConnection {
    pub fn enqueue(
        &mut self,
        task: &Task,
        input: &Value,
        options: &EnqueueOptions,
    ) -> Result<i64, TaskError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(key) = &options.deduplication_key {
            let existing: Option<(i64, String, String)> = transaction.query_row("SELECT id, task, input FROM lenso_task_job WHERE queue = ?1 AND dedup_key = ?2", params![self.queue, key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
            if let Some((id, name, raw)) = existing {
                if name != task.name
                    || serde_json::from_str::<Value>(&raw).ok().as_ref() != Some(input)
                {
                    return Err(TaskError::DeduplicationConflict);
                }
                return Ok(id);
            }
        }
        transaction.execute(&format!("INSERT INTO lenso_task_job(queue, task, input, dedup_key, max_attempts, retry_delay_ms, retry_max_ms, retry_backoff, run_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, COALESCE(?9, {NOW}))"), params![self.queue, task.name, input.to_string(), options.deduplication_key, task.retry.max_attempts, task.retry.delay_ms, task.retry.max_delay_ms, task.retry.backoff, options.run_at])?;
        let id = transaction.last_insert_rowid();
        transaction.commit()?;
        Ok(id)
    }
    pub fn get(&self, id: i64) -> Result<Option<JobStatus>, TaskError> {
        Ok(self
            .connection
            .query_row(
                &format!("SELECT {COLUMNS} FROM lenso_task_job WHERE queue = ?1 AND id = ?2"),
                params![self.queue, id],
                status,
            )
            .optional()?)
    }
    pub fn lookup(&self, key: &str) -> Result<Option<JobStatus>, TaskError> {
        Ok(self
            .connection
            .query_row(
                &format!(
                    "SELECT {COLUMNS} FROM lenso_task_job WHERE queue = ?1 AND dedup_key = ?2"
                ),
                params![self.queue, key],
                status,
            )
            .optional()?)
    }
    pub fn list(&self, task: &str, after: i64, limit: u32) -> Result<Vec<JobStatus>, TaskError> {
        let mut statement = self.connection.prepare(&format!("SELECT {COLUMNS} FROM lenso_task_job WHERE queue = ?1 AND task = ?2 AND id > ?3 ORDER BY id LIMIT ?4"))?;
        let rows = statement.query_map(params![self.queue, task, after, limit], status)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn cancel(&mut self, id: i64) -> Result<CancelResult, TaskError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: Option<String> = transaction.query_row("UPDATE lenso_task_job SET cancel_requested = 1, state = CASE WHEN state = 'pending' THEN 'cancelled' ELSE state END, last_error = CASE WHEN state = 'pending' THEN 'aborted' ELSE last_error END WHERE queue = ?1 AND id = ?2 AND state IN ('pending', 'running') RETURNING state", params![self.queue, id], |row| row.get(0)).optional()?;
        let result = match state.as_deref() {
            Some("running") => CancelResult::Requested,
            Some("cancelled") => CancelResult::Cancelled,
            _ => {
                let exists: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM lenso_task_job WHERE queue = ?1 AND id = ?2)",
                    params![self.queue, id],
                    |r| r.get(0),
                )?;
                if exists {
                    CancelResult::Terminal
                } else {
                    CancelResult::Missing
                }
            }
        };
        transaction.commit()?;
        Ok(result)
    }
    pub fn retry(&self, id: i64) -> Result<bool, TaskError> {
        Ok(self.connection.execute(&format!("UPDATE lenso_task_job SET state = 'pending', max_attempts = attempt + 1, run_at = {NOW}, result = NULL WHERE queue = ?1 AND id = ?2 AND state = 'failed' AND cancel_requested = 0 AND attempt < 4294967295"), params![self.queue, id])? == 1)
    }
    pub fn claim(
        &mut self,
        tasks: &[String],
        options: &WorkerOptions,
    ) -> Result<Option<Claim>, TaskError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now: i64 = transaction.query_row(&format!("SELECT {NOW}"), [], |r| r.get(0))?;
        transaction.execute(&format!("UPDATE lenso_task_job SET state = CASE WHEN cancel_requested = 1 THEN 'cancelled' WHEN attempt >= max_attempts THEN 'failed' ELSE 'pending' END, run_at = ?1 + {DELAY}, last_error = 'aborted', token = NULL, lease_until = NULL, expires_at = NULL, result = NULL WHERE queue = ?2 AND state = 'running' AND (lease_until <= ?1 OR expires_at <= ?1)"), params![now, self.queue])?;
        // Workers only claim their registered task catalog; another deployment may
        // legitimately own other tasks in the same durable queue.
        let task_json = serde_json::to_string(tasks).map_err(|_| TaskError::Runtime)?;
        let claimed = transaction.query_row("UPDATE lenso_task_job SET state = 'running', attempt = attempt + 1, token = lower(hex(randomblob(16))), lease_until = ?1, expires_at = ?2 WHERE id = (SELECT id FROM lenso_task_job WHERE queue = ?3 AND state = 'pending' AND cancel_requested = 0 AND run_at <= ?4 AND attempt < max_attempts AND task IN (SELECT value FROM json_each(?5)) ORDER BY run_at, id LIMIT 1) RETURNING id, task, input, attempt, token", params![now + i64::from(options.lease_ms), now + i64::from(options.execution_timeout_ms), self.queue, now, task_json], |row| {
            let input: String = row.get(2)?;
            Ok(Claim { id: row.get(0)?, task: row.get(1)?, input: serde_json::from_str(&input).map_err(|_| rusqlite::Error::InvalidQuery)?, attempt: row.get(3)?, token: row.get(4)? })
        }).optional()?;
        transaction.commit()?;
        Ok(claimed)
    }
    pub fn pulse(&self, claim: &Claim, lease_ms: u32) -> Result<Option<bool>, TaskError> {
        Ok(self.connection.query_row(&format!("UPDATE lenso_task_job SET lease_until = MIN({NOW} + ?1, expires_at) WHERE queue = ?2 AND id = ?3 AND token = ?4 AND state = 'running' AND lease_until > {NOW} AND expires_at > {NOW} RETURNING cancel_requested"), params![lease_ms, self.queue, claim.id, claim.token], |r| r.get(0)).optional()?)
    }
    pub fn settle(&self, claim: &Claim, outcome: Result<Value, Failure>) -> Result<(), TaskError> {
        let (success, result, error) = match outcome {
            Ok(value) => (true, Some(value.to_string()), None),
            Err(error) => (false, None, Some(error.code())),
        };
        let changed = self.connection.execute(&format!("UPDATE lenso_task_job SET state = CASE WHEN cancel_requested = 1 THEN 'cancelled' WHEN ?1 THEN 'succeeded' WHEN attempt < max_attempts THEN 'pending' ELSE 'failed' END, run_at = CASE WHEN NOT ?1 AND cancel_requested = 0 AND attempt < max_attempts THEN {NOW} + {DELAY} ELSE run_at END, result = CASE WHEN cancel_requested = 0 THEN ?2 ELSE NULL END, last_error = CASE WHEN cancel_requested = 1 THEN 'aborted' ELSE ?3 END, token = NULL, lease_until = NULL, expires_at = NULL WHERE queue = ?4 AND id = ?5 AND token = ?6 AND state = 'running' AND lease_until > {NOW} AND expires_at > {NOW}"), params![success, result, error, self.queue, claim.id, claim.token])?;
        if changed == 1 {
            Ok(())
        } else {
            Err(TaskError::ClaimLost)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RetryPolicy, Task};
    use serde_json::json;

    #[test]
    fn expired_exhausted_and_cancelled_claims_do_not_execute_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.sqlite");
        provision(&path, "test").unwrap();
        let store = Store::open(&path, "test".into()).unwrap();
        store
            .with(|db| {
                let task = Task::new(
                    "fake",
                    RetryPolicy {
                        max_attempts: 1,
                        ..Default::default()
                    },
                    |_: Value, _| async { Ok(json!(null)) },
                );
                let id = db.enqueue(&task, &json!(null), &EnqueueOptions::default())?;
                let claim = db
                    .claim(&["fake".into()], &WorkerOptions::default())?
                    .unwrap();
                db.connection.execute(
                    "UPDATE lenso_task_job SET lease_until = 0 WHERE id = ?1",
                    [id],
                )?;
                assert!(db
                    .claim(&["fake".into()], &WorkerOptions::default())?
                    .is_none());
                let status = db.get(id)?.unwrap();
                assert_eq!(
                    (status.state, status.attempt, status.last_error),
                    (JobState::Failed, 1, Some(Failure::Aborted))
                );
                assert!(matches!(
                    db.settle(&claim, Ok(json!("late"))),
                    Err(TaskError::ClaimLost)
                ));
                let id = db.enqueue(&task, &json!(null), &EnqueueOptions::default())?;
                db.claim(&["fake".into()], &WorkerOptions::default())?
                    .unwrap();
                assert_eq!(db.cancel(id)?, CancelResult::Requested);
                db.connection.execute(
                    "UPDATE lenso_task_job SET lease_until = 0 WHERE id = ?1",
                    [id],
                )?;
                assert!(db
                    .claim(&["fake".into()], &WorkerOptions::default())?
                    .is_none());
                assert_eq!(db.get(id)?.unwrap().state, JobState::Cancelled);
                assert!(!db.retry(id)?);
                Ok(())
            })
            .unwrap();
    }
}
