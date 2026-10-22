//! A local report submission business service. The caller supplies an already
//! verified owner identity; this module does not authenticate network requests.
use lenso_tasks::{EnqueueOptions, SqliteTaskTransaction, TaskError, Tasks};
use rusqlite::params;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub const SCHEMA: &str = "CREATE TABLE report_request (
    report_id TEXT PRIMARY KEY NOT NULL,
    owner TEXT NOT NULL,
    queue_identity TEXT NOT NULL,
    title TEXT NOT NULL,
    job_id INTEGER UNIQUE REFERENCES lenso_task_job(id)
)";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportInput {
    pub report_id: String,
    pub title: String,
}
#[derive(Clone)]
pub struct Reports(pub Arc<Tasks>);
impl Reports {
    /// None is a changed request or owner denial. Successful retries return the
    /// original acceptance; neither path submits a second job.
    pub async fn submit(
        &self,
        owner: String,
        report_id: String,
        title: String,
    ) -> Result<Option<i64>, TaskError> {
        if owner.is_empty()
            || report_id.is_empty()
            || report_id.len() > 128
            || title.is_empty()
            || title.len() > 1024
        {
            return Err(TaskError::InvalidInput);
        }
        let identity = self.0.identity().to_owned();
        self.0
            .sqlite_transaction(move |tx| submit(tx, &identity, &owner, &report_id, &title))
            .await
    }
    /// Read the original stable business key after an unconfirmed COMMIT. None
    /// is a snapshot, not proof that an earlier writer can never commit.
    pub async fn receipt(
        &self,
        owner: String,
        report_id: String,
    ) -> Result<Option<i64>, TaskError> {
        let identity = self.0.identity().to_owned();
        self.0
            .sqlite_transaction(move |tx| {
                tx.query_row(
            "SELECT (SELECT job_id FROM report_request WHERE report_id = ?1 AND owner = ?2 AND queue_identity = ?3)",
            params![report_id, owner, identity], |row| row.get(0),
        )
            })
            .await
    }
}
fn submit(
    tx: &mut SqliteTaskTransaction<'_>,
    identity: &str,
    owner: &str,
    id: &str,
    title: &str,
) -> Result<Option<i64>, TaskError> {
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM report_request WHERE report_id = ?1)",
        [id],
        |r| r.get(0),
    )?;
    if exists {
        return tx.query_row("SELECT CASE WHEN owner = ?2 AND title = ?3 AND queue_identity = ?4 THEN job_id ELSE NULL END FROM report_request WHERE report_id = ?1", params![id, owner, title, identity], |r| r.get(0));
    }
    tx.execute(
        "INSERT INTO report_request(report_id, owner, title, queue_identity) VALUES (?1, ?2, ?3, ?4)",
        params![id, owner, title, identity],
    )?;
    let job = tx.enqueue(
        "report",
        json!({"report_id": id, "title": title}),
        EnqueueOptions {
            deduplication_key: Some(format!("report:{id}")),
            ..Default::default()
        },
    )?;
    tx.execute(
        "UPDATE report_request SET job_id = ?2 WHERE report_id = ?1",
        params![id, job],
    )?;
    Ok(Some(job))
}
