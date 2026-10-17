use super::{Claim, StoreConnection};
use crate::{
    CancelResult, EnqueueOptions, Failure, JobState, JobStatus, Task, TaskError, WorkerOptions,
};
use ::postgres::{Client, Config, IsolationLevel, NoTls, Row, Transaction};
use serde_json::Value;

// Each write starts after acquiring any blocking row locks. statement_timestamp
// then gives one server-clock instant for all ownership checks in that write.
const NOW: &str = "floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint";
const COLUMNS: &str =
    "id, task, state, attempt, max_attempts, cancel_requested, run_at, last_error, result";
const DELAY: &str = "LEAST(retry_max_ms, retry_delay_ms * (CASE WHEN retry_backoff THEN (1::bigint << LEAST(GREATEST(attempt - 1, 0), 30)::integer) ELSE 1 END))";

pub(crate) struct PostgresConnection {
    connection: Client,
    queue: String,
}
impl PostgresConnection {
    pub fn open(config: &Config, queue: String) -> Result<(Self, String), TaskError> {
        let mut connection = config.connect(NoTls)?;
        let identity = connection
            .query_one(
                "SELECT identity FROM lenso_task_queue WHERE name = $1",
                &[&queue],
            )?
            .try_get(0)?;
        connection.prepare(&format!("SELECT {COLUMNS}, input, dedup_key, retry_delay_ms, retry_max_ms, retry_backoff, token, lease_until, expires_at FROM lenso_task_job LIMIT 0"))?;
        Ok((Self { connection, queue }, identity))
    }
}
pub(crate) fn provision(config: &Config, queue: &str) -> Result<(), TaskError> {
    let mut client = config.connect(NoTls)?;
    let mut tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()?;
    // Serialize explicit provisioning within this database, including first DDL.
    tx.query_one("SELECT pg_advisory_xact_lock(74201931761018)", &[])?;
    tx.batch_execute(include_str!("../../migrations/0001_tasks_postgres.sql"))?;
    tx.execute(
        "INSERT INTO lenso_task_queue(name) VALUES ($1) ON CONFLICT(name) DO NOTHING",
        &[&queue],
    )?;
    tx.commit()?;
    Ok(())
}
fn status(row: Row) -> Result<JobStatus, TaskError> {
    let state: String = row.try_get(2)?;
    let state = match state.as_str() {
        "pending" => JobState::Pending,
        "running" => JobState::Running,
        "succeeded" => JobState::Succeeded,
        "failed" => JobState::Failed,
        "cancelled" => JobState::Cancelled,
        _ => return Err(TaskError::InvalidStoredData),
    };
    let error: Option<String> = row.try_get(7)?;
    let last_error = match error.as_deref() {
        None => None,
        Some("handler-failed") => Some(Failure::HandlerFailed),
        Some("invalid-input") => Some(Failure::InvalidInput),
        Some("invalid-result") => Some(Failure::InvalidResult),
        Some("aborted") => Some(Failure::Aborted),
        _ => return Err(TaskError::InvalidStoredData),
    };
    let raw: Option<String> = row.try_get(8)?;
    let result = raw
        .map(|s| {
            serde_json::from_str(&s)
                .ok()
                .filter(crate::valid_json)
                .ok_or(TaskError::InvalidStoredData)
        })
        .transpose()?;
    Ok(JobStatus {
        id: row.try_get(0)?,
        task: row.try_get(1)?,
        state,
        attempt: u32::try_from(row.try_get::<_, i64>(3)?)
            .map_err(|_| TaskError::InvalidStoredData)?,
        max_attempts: u32::try_from(row.try_get::<_, i64>(4)?)
            .map_err(|_| TaskError::InvalidStoredData)?,
        cancel_requested: row.try_get(5)?,
        run_at: row.try_get(6)?,
        last_error,
        result,
    })
}
fn lock_claim(tx: &mut Transaction<'_>, queue: &str, claim: &Claim) -> Result<(), TaskError> {
    // Token/time checks happen in the subsequent statement, after any lock wait.
    tx.query_opt(
        "SELECT id FROM lenso_task_job WHERE queue = $1 AND id = $2 FOR UPDATE",
        &[&queue, &claim.id],
    )?;
    Ok(())
}
impl StoreConnection for PostgresConnection {
    fn close(self: Box<Self>) -> Result<(), TaskError> {
        self.connection.close()?;
        Ok(())
    }
    fn enqueue(
        &mut self,
        task: &Task,
        input: &Value,
        options: &EnqueueOptions,
    ) -> Result<i64, TaskError> {
        let mut tx = self
            .connection
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()?;
        let inserted = tx.query_opt(&format!("INSERT INTO lenso_task_job(queue, task, input, dedup_key, max_attempts, retry_delay_ms, retry_max_ms, retry_backoff, run_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9,{NOW})) ON CONFLICT(queue, dedup_key) DO NOTHING RETURNING id"), &[&self.queue, &task.name, &input.to_string(), &options.deduplication_key, &i64::from(task.retry.max_attempts), &i64::from(task.retry.delay_ms), &i64::from(task.retry.max_delay_ms), &task.retry.backoff, &options.run_at])?;
        let id = if let Some(row) = inserted {
            row.try_get(0)?
        } else {
            // New READ COMMITTED snapshot sees the winning concurrent insertion.
            let row = tx.query_one(
                "SELECT id, task, input FROM lenso_task_job WHERE queue=$1 AND dedup_key=$2",
                &[&self.queue, &options.deduplication_key],
            )?;
            let name: String = row.try_get(1)?;
            let raw: String = row.try_get(2)?;
            if name != task.name || serde_json::from_str::<Value>(&raw).ok().as_ref() != Some(input)
            {
                return Err(TaskError::DeduplicationConflict);
            }
            row.try_get(0)?
        };
        tx.commit()?;
        Ok(id)
    }
    fn get(&mut self, id: i64) -> Result<Option<JobStatus>, TaskError> {
        self.connection
            .query_opt(
                &format!("SELECT {COLUMNS} FROM lenso_task_job WHERE queue=$1 AND id=$2"),
                &[&self.queue, &id],
            )?
            .map(status)
            .transpose()
    }
    fn lookup(&mut self, key: &str) -> Result<Option<JobStatus>, TaskError> {
        self.connection
            .query_opt(
                &format!("SELECT {COLUMNS} FROM lenso_task_job WHERE queue=$1 AND dedup_key=$2"),
                &[&self.queue, &key],
            )?
            .map(status)
            .transpose()
    }
    fn list(&mut self, task: &str, after: i64, limit: u32) -> Result<Vec<JobStatus>, TaskError> {
        self.connection.query(&format!("SELECT {COLUMNS} FROM lenso_task_job WHERE queue=$1 AND task=$2 AND id>$3 ORDER BY id LIMIT $4"), &[&self.queue, &task, &after, &i64::from(limit)])?.into_iter().map(status).collect()
    }
    fn cancel(&mut self, id: i64) -> Result<CancelResult, TaskError> {
        let mut tx = self
            .connection
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()?;
        let row = tx.query_opt("UPDATE lenso_task_job SET cancel_requested=true, state=CASE WHEN state='pending' THEN 'cancelled' ELSE state END, last_error=CASE WHEN state='pending' THEN 'aborted' ELSE last_error END WHERE queue=$1 AND id=$2 AND state IN ('pending','running') RETURNING state", &[&self.queue, &id])?;
        let result = match row.map(|r| r.get::<_, String>(0)).as_deref() {
            Some("running") => CancelResult::Requested,
            Some("cancelled") => CancelResult::Cancelled,
            _ => {
                if tx
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM lenso_task_job WHERE queue=$1 AND id=$2)",
                        &[&self.queue, &id],
                    )?
                    .get(0)
                {
                    CancelResult::Terminal
                } else {
                    CancelResult::Missing
                }
            }
        };
        tx.commit()?;
        Ok(result)
    }
    fn retry(&mut self, id: i64) -> Result<bool, TaskError> {
        Ok(self.connection.execute(&format!("UPDATE lenso_task_job SET state='pending', max_attempts=attempt+1, run_at={NOW}, result=NULL WHERE queue=$1 AND id=$2 AND state='failed' AND NOT cancel_requested AND attempt<4294967295"), &[&self.queue, &id])? == 1)
    }
    fn claim(
        &mut self,
        tasks: &[String],
        options: &WorkerOptions,
    ) -> Result<Option<Claim>, TaskError> {
        let mut tx = self
            .connection
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()?;
        // Skip rows with active writers; recover at most 100 abandoned attempts per call.
        tx.execute(&format!("WITH expired AS (SELECT id FROM lenso_task_job WHERE queue=$1 AND state='running' AND (lease_until<={NOW} OR expires_at<={NOW}) ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED) UPDATE lenso_task_job SET state=CASE WHEN cancel_requested THEN 'cancelled' WHEN attempt>=max_attempts THEN 'failed' ELSE 'pending' END, run_at={NOW}+{DELAY}, last_error='aborted', token=NULL, lease_until=NULL, expires_at=NULL, result=NULL WHERE id IN (SELECT id FROM expired)"), &[&self.queue])?;
        let candidate = tx.query_opt(&format!("SELECT id FROM lenso_task_job WHERE queue=$1 AND state='pending' AND NOT cancel_requested AND run_at<={NOW} AND attempt<max_attempts AND task=ANY($2) ORDER BY run_at,id LIMIT 1 FOR UPDATE SKIP LOCKED"), &[&self.queue, &tasks])?;
        let claimed = if let Some(row) = candidate {
            let id: i64 = row.try_get(0)?;
            let row = tx.query_one(&format!("UPDATE lenso_task_job SET state='running', attempt=attempt+1, token=gen_random_uuid()::text, lease_until={NOW}+$1, expires_at={NOW}+$2 WHERE id=$3 RETURNING id,task,input,attempt,token"), &[&i64::from(options.lease_ms), &i64::from(options.execution_timeout_ms), &id])?;
            let raw: String = row.try_get(2)?;
            Some(Claim {
                id: row.try_get(0)?,
                task: row.try_get(1)?,
                input: serde_json::from_str(&raw).map_err(|_| TaskError::InvalidStoredData)?,
                attempt: u32::try_from(row.try_get::<_, i64>(3)?)
                    .map_err(|_| TaskError::InvalidStoredData)?,
                token: row.try_get(4)?,
            })
        } else {
            None
        };
        tx.commit()?;
        Ok(claimed)
    }
    fn pulse(&mut self, claim: &Claim, lease_ms: u32) -> Result<Option<bool>, TaskError> {
        let mut tx = self
            .connection
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()?;
        lock_claim(&mut tx, &self.queue, claim)?;
        let row = tx.query_opt(&format!("UPDATE lenso_task_job SET lease_until=LEAST({NOW}+$1,expires_at) WHERE queue=$2 AND id=$3 AND token=$4 AND state='running' AND lease_until>{NOW} AND expires_at>{NOW} RETURNING cancel_requested"), &[&i64::from(lease_ms), &self.queue, &claim.id, &claim.token])?;
        tx.commit()?;
        Ok(row.map(|r| r.get(0)))
    }
    fn settle(&mut self, claim: &Claim, outcome: Result<Value, Failure>) -> Result<(), TaskError> {
        let (success, result, error) = match outcome {
            Ok(value) => (true, Some(value.to_string()), None),
            Err(error) => (false, None, Some(error.code())),
        };
        let mut tx = self
            .connection
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()?;
        lock_claim(&mut tx, &self.queue, claim)?;
        let changed = tx.execute(&format!("UPDATE lenso_task_job SET state=CASE WHEN cancel_requested THEN 'cancelled' WHEN $1 THEN 'succeeded' WHEN attempt<max_attempts THEN 'pending' ELSE 'failed' END, run_at=CASE WHEN NOT $1 AND NOT cancel_requested AND attempt<max_attempts THEN {NOW}+{DELAY} ELSE run_at END, result=CASE WHEN NOT cancel_requested THEN $2 ELSE NULL END, last_error=CASE WHEN cancel_requested THEN 'aborted' ELSE $3 END, token=NULL, lease_until=NULL, expires_at=NULL WHERE queue=$4 AND id=$5 AND token=$6 AND state='running' AND lease_until>{NOW} AND expires_at>{NOW}"), &[&success, &result, &error, &self.queue, &claim.id, &claim.token])?;
        if changed != 1 {
            return Err(TaskError::ClaimLost);
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
