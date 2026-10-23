use crate::*;
use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Dedicated append-only SQLite store. Open once at startup. Each operation runs
/// on Tokio's blocking pool; cancellation does not cancel an accepted DB write.
/// FULL synchronous commits are used, but deployment durability is not attested:
/// `durable_intents()` remains false. Use append/reconciliation, not strict prepare.
#[derive(Clone)]
pub struct SqliteAuditRepository {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteAuditRepository {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, BoxError> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = Connection::open(path)?;
            connection.busy_timeout(Duration::from_secs(5))?;
            connection.execute_batch(
                "PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS lenso_audit_v1 (
                    scope TEXT NOT NULL, id TEXT NOT NULL, recorded_at INTEGER NOT NULL,
                    payload TEXT NOT NULL, PRIMARY KEY(scope, id));
                CREATE INDEX IF NOT EXISTS lenso_audit_v1_page
                    ON lenso_audit_v1(scope, recorded_at DESC, id DESC);
                CREATE TRIGGER IF NOT EXISTS lenso_audit_v1_no_update
                    BEFORE UPDATE ON lenso_audit_v1 BEGIN SELECT RAISE(ABORT, 'append-only'); END;
                CREATE TRIGGER IF NOT EXISTS lenso_audit_v1_no_delete
                    BEFORE DELETE ON lenso_audit_v1 BEGIN SELECT RAISE(ABORT, 'append-only'); END;",
            )?;
            Ok(Self {
                connection: Arc::new(Mutex::new(connection)),
            })
        })
        .await
        .map_err(|_| AuditError::StorageFailed)?
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, BoxError> + Send + 'static,
    ) -> Result<T, BoxError> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let connection = connection.lock().map_err(|_| AuditError::StorageFailed)?;
            operation(&connection)
        })
        .await
        .map_err(|_| AuditError::StorageFailed)?
    }
}

impl AuditRepository for SqliteAuditRepository {
    async fn insert(&self, event: AuditEvent) -> Result<InsertStatus, BoxError> {
        self.run(move |connection| {
            let scope = serde_json::to_string(&event.input.scope)?;
            let payload = serde_json::to_string(&event)?;
            let inserted = connection.execute(
                "INSERT INTO lenso_audit_v1(scope,id,recorded_at,payload) VALUES (?1,?2,?3,?4) ON CONFLICT(scope,id) DO NOTHING",
                params![scope, event.input.id, i64::try_from(event.recorded_at)?, payload],
            )?;
            if inserted == 1 { return Ok(InsertStatus::Inserted); }
            let previous: String = connection.query_row(
                "SELECT payload FROM lenso_audit_v1 WHERE scope=?1 AND id=?2",
                params![scope, event.input.id], |row| row.get(0))?;
            let previous: AuditEvent = serde_json::from_str(&previous)?;
            Ok(if previous.same_content(&event) { InsertStatus::Duplicate } else { InsertStatus::Conflict })
        }).await
    }

    async fn get(&self, scope: &AuditScope, id: &str) -> Result<Option<AuditEvent>, BoxError> {
        let scope = serde_json::to_string(scope)?;
        let id = id.to_owned();
        self.run(move |connection| {
            let payload: Option<String> = connection
                .query_row(
                    "SELECT payload FROM lenso_audit_v1 WHERE scope=?1 AND id=?2",
                    params![scope, id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(payload
                .map(|value| serde_json::from_str(&value))
                .transpose()?)
        })
        .await
    }

    async fn list(&self, query: &AuditQuery, limit: usize) -> Result<Vec<AuditEvent>, BoxError> {
        let query = query.clone();
        self.run(move |connection| {
            let scope = serde_json::to_string(&query.scope)?;
            let mut sql = String::from("SELECT payload FROM lenso_audit_v1 WHERE scope=?");
            let mut parameters = vec![Value::Text(scope)];
            if let Some(from) = query.recorded_from {
                sql.push_str(" AND recorded_at>=?");
                parameters.push(Value::Integer(i64::try_from(from)?));
            }
            if let Some(to) = query.recorded_to {
                sql.push_str(" AND recorded_at<=?");
                parameters.push(Value::Integer(i64::try_from(to)?));
            }
            if let Some(cursor) = &query.cursor {
                sql.push_str(" AND (recorded_at,id)<(?,?)");
                parameters.push(Value::Integer(i64::try_from(cursor.recorded_at)?));
                parameters.push(Value::Text(cursor.id.clone()));
            }
            sql.push_str(" ORDER BY recorded_at DESC,id DESC");
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(params_from_iter(parameters))?;
            let mut events = Vec::new();
            // The page limit applies after residual payload filters, not before them.
            while events.len() < limit {
                let Some(row) = rows.next()? else { break };
                let payload: String = row.get(0)?;
                let event: AuditEvent = serde_json::from_str(&payload)?;
                if crate::validation::matches_query(&event, &query) {
                    events.push(event);
                }
            }
            Ok(events)
        })
        .await
    }
}
