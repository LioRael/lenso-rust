#![doc = include_str!("../README.md")]

use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid input: {0}")]
    Invalid(&'static str),
    #[error("operation id already has a different command")]
    Conflict,
    #[error("quota exceeded")]
    Exhausted,
    #[error("quota or reservation not found")]
    Missing,
    #[error("reservation is terminal or expired")]
    Terminal,
    #[error("service closed")]
    Closed,
    #[error("service mutex poisoned")]
    Poisoned,
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("record encoding error: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("commit outcome unknown for operation {operation}; query before any further action")]
    CommitUnknown { operation: String },
}

/// Scope is an opaque host-authorized tenant/resource/window identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Command {
    Create {
        scope: String,
        limit: i64,
    },
    Reserve {
        scope: String,
        reservation: String,
        amount: i64,
        expires_at: i64,
    },
    Consume {
        reservation: String,
        amount: i64,
    },
    Release {
        reservation: String,
    },
    /// Explicit durable expiration: call on startup and periodically, with fresh IDs.
    Expire {
        scope: String,
    },
    /// A real fixture row and its quota consumption commit in this same transaction.
    ApiCall {
        scope: String,
        request: String,
        units: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    pub operation: String,
    pub at: i64,
    pub command: Command,
    /// Units created, reserved, consumed, released, or expired by this operation.
    pub units: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Balance {
    pub limit: i64,
    pub used: i64,
    pub reserved: i64,
}

/// Absent is not proof that an earlier remote/in-flight writer cannot commit.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Committed(Record),
    NotObserved,
}

pub struct UsageQuota {
    connection: Mutex<Option<Connection>>,
    gate: Option<WorkGate>,
}

impl UsageQuota {
    /// Open once at startup. Separate instances/processes share SQLite write locking.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
        )?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(include_str!("schema.sql"))?;
        tx.commit()?;
        Ok(Self {
            connection: Mutex::new(Some(connection)),
            gate: None,
        })
    }

    /// Synchronous bounded DB operation; async hosts should run it on a blocking worker.
    /// Retry only the exact same command/ID after resolving ambiguous ownership/outcome.
    pub fn execute(&self, operation: &str, at: i64, command: Command) -> Result<Record, Error> {
        let _work = self
            .gate
            .as_ref()
            .map(|g| g.enter().map_err(|_| Error::Closed))
            .transpose()?;
        identifier(operation)?;
        if at < 0 {
            return Err(Error::Invalid("negative timestamp"));
        }
        let mut lock = self.connection.lock().map_err(|_| Error::Poisoned)?;
        let connection = lock.as_mut().ok_or(Error::Closed)?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(record) = read_record(&tx, operation)? {
            if record.command != command {
                return Err(Error::Conflict);
            }
            return Ok(record);
        }
        let last: i64 =
            tx.query_row("SELECT last_at FROM uq_clock WHERE id=1", [], |r| r.get(0))?;
        if at < last {
            return Err(Error::Invalid("clock moved backwards"));
        }
        let units = apply(&tx, at, &command)?;
        let record = Record {
            operation: operation.into(),
            at,
            command,
            units,
        };
        tx.execute(
            "INSERT INTO uq_operations(operation, record) VALUES(?1, ?2)",
            params![operation, serde_json::to_string(&record)?],
        )?;
        tx.execute("UPDATE uq_clock SET last_at=?1 WHERE id=1", [at])?;
        tx.commit().map_err(|_| Error::CommitUnknown {
            operation: operation.into(),
        })?;
        Ok(record)
    }

    pub fn outcome(&self, operation: &str) -> Result<Outcome, Error> {
        self.read(|c| {
            Ok(match read_record(c, operation)? {
                Some(r) => Outcome::Committed(r),
                None => Outcome::NotObserved,
            })
        })
    }

    pub fn balance(&self, scope: &str) -> Result<Balance, Error> {
        self.read(|c| {
            c.query_row(
                "SELECT capacity, used, held FROM uq_quotas WHERE scope=?1",
                [scope],
                |r| {
                    Ok(Balance {
                        limit: r.get(0)?,
                        used: r.get(1)?,
                        reserved: r.get(2)?,
                    })
                },
            )
            .optional()?
            .ok_or(Error::Missing)
        })
    }

    pub fn records(&self) -> Result<Vec<Record>, Error> {
        self.read(|c| {
            let mut statement = c.prepare("SELECT record FROM uq_operations ORDER BY seq")?;
            let values = statement.query_map([], |r| r.get::<_, String>(0))?;
            values.map(|v| Ok(serde_json::from_str(&v?)?)).collect()
        })
    }

    pub fn api_calls(&self, scope: &str) -> Result<i64, Error> {
        self.read(|c| {
            Ok(c.query_row(
                "SELECT COUNT(*) FROM uq_api_calls WHERE scope=?1",
                [scope],
                |r| r.get(0),
            )?)
        })
    }

    fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T, Error>) -> Result<T, Error> {
        let _work = self
            .gate
            .as_ref()
            .map(|g| g.enter().map_err(|_| Error::Closed))
            .transpose()?;
        let lock = self.connection.lock().map_err(|_| Error::Poisoned)?;
        f(lock.as_ref().ok_or(Error::Closed)?)
    }

    pub fn close(&self) -> Result<(), Error> {
        let mut lock = self.connection.lock().map_err(|_| Error::Poisoned)?;
        if let Some(connection) = lock.take() {
            if let Err((connection, error)) = connection.close() {
                *lock = Some(connection);
                return Err(Error::Database(error));
            }
        }
        Ok(())
    }
}

fn identifier(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 256 {
        return Err(Error::Invalid("identifier length must be 1..=256 bytes"));
    }
    Ok(())
}
fn positive(value: i64) -> Result<(), Error> {
    if value <= 0 {
        return Err(Error::Invalid("units must be positive"));
    }
    Ok(())
}
fn read_record(connection: &Connection, operation: &str) -> Result<Option<Record>, Error> {
    let value: Option<String> = connection
        .query_row(
            "SELECT record FROM uq_operations WHERE operation=?1",
            [operation],
            |r| r.get(0),
        )
        .optional()?;
    value
        .map(|v| serde_json::from_str(&v).map_err(Error::from))
        .transpose()
}
fn debit(tx: &Transaction<'_>, scope: &str, amount: i64, reserve: bool) -> Result<(), Error> {
    identifier(scope)?;
    positive(amount)?;
    let sql = if reserve {
        "UPDATE uq_quotas SET held=held+?2 WHERE scope=?1 AND ?2 <= capacity-used-held"
    } else {
        "UPDATE uq_quotas SET used=used+?2 WHERE scope=?1 AND ?2 <= capacity-used-held"
    };
    if tx.execute(sql, params![scope, amount])? != 1 {
        return Err(Error::Exhausted);
    }
    Ok(())
}
fn apply(tx: &Transaction<'_>, at: i64, command: &Command) -> Result<i64, Error> {
    match command {
        Command::Create { scope, limit } => {
            identifier(scope)?;
            if *limit < 0 {
                return Err(Error::Invalid("negative capacity"));
            }
            if tx.execute(
                "INSERT OR IGNORE INTO uq_quotas(scope, capacity) VALUES(?1, ?2)",
                params![scope, limit],
            )? != 1
            {
                return Err(Error::Conflict);
            }
            Ok(*limit)
        }
        Command::Reserve {
            scope,
            reservation,
            amount,
            expires_at,
        } => {
            identifier(reservation)?;
            if *expires_at <= at {
                return Err(Error::Invalid("expiry must be in the future"));
            }
            debit(tx, scope, *amount, true)?;
            if tx.execute("INSERT OR IGNORE INTO uq_reservations(id, scope, remaining, expires_at) VALUES(?1, ?2, ?3, ?4)", params![reservation, scope, amount, expires_at])? != 1 { return Err(Error::Conflict); }
            Ok(*amount)
        }
        Command::Consume {
            reservation,
            amount,
        } => {
            positive(*amount)?;
            let (scope, remaining, expires_at) = reservation_row(tx, reservation)?;
            if remaining == 0 || at >= expires_at {
                return Err(Error::Terminal);
            }
            if *amount > remaining {
                return Err(Error::Exhausted);
            }
            tx.execute(
                "UPDATE uq_reservations SET remaining=remaining-?2 WHERE id=?1",
                params![reservation, amount],
            )?;
            tx.execute(
                "UPDATE uq_quotas SET held=held-?2, used=used+?2 WHERE scope=?1",
                params![scope, amount],
            )?;
            Ok(*amount)
        }
        Command::Release { reservation } => {
            let (scope, remaining, expires_at) = reservation_row(tx, reservation)?;
            if remaining == 0 || at >= expires_at {
                return Err(Error::Terminal);
            }
            tx.execute(
                "UPDATE uq_reservations SET remaining=0 WHERE id=?1",
                [reservation],
            )?;
            tx.execute(
                "UPDATE uq_quotas SET held=held-?2 WHERE scope=?1",
                params![scope, remaining],
            )?;
            Ok(remaining)
        }
        Command::Expire { scope } => {
            identifier(scope)?;
            // Checked aggregation stays within this scope's capacity.
            let mut statement = tx.prepare("SELECT id, scope, remaining FROM uq_reservations WHERE expires_at<=?1 AND scope=?2 AND remaining>0 ORDER BY id")?;
            let rows = statement.query_map(params![at, scope], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?;
            let rows = rows.collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            let mut total = 0i64;
            for (id, scope, remaining) in rows {
                total = total
                    .checked_add(remaining)
                    .ok_or(Error::Invalid("expiration batch overflow"))?;
                tx.execute(
                    "UPDATE uq_quotas SET held=held-?2 WHERE scope=?1",
                    params![scope, remaining],
                )?;
                tx.execute("UPDATE uq_reservations SET remaining=0 WHERE id=?1", [&id])?;
                tx.execute(
                    "INSERT INTO uq_expirations(reservation, expired_at, units) VALUES(?1, ?2, ?3)",
                    params![id, at, remaining],
                )?;
            }
            Ok(total)
        }
        Command::ApiCall {
            scope,
            request,
            units,
        } => {
            identifier(request)?;
            debit(tx, scope, *units, false)?;
            if tx.execute(
                "INSERT OR IGNORE INTO uq_api_calls(scope, request, units) VALUES(?1, ?2, ?3)",
                params![scope, request, units],
            )? != 1
            {
                return Err(Error::Conflict);
            }
            Ok(*units)
        }
    }
}
fn reservation_row(tx: &Transaction<'_>, id: &str) -> Result<(String, i64, i64), Error> {
    identifier(id)?;
    tx.query_row(
        "SELECT scope, remaining, expires_at FROM uq_reservations WHERE id=?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .optional()?
    .ok_or(Error::Missing)
}

/// Owns its SQLite connection. Startup opens it, shutdown drains work then closes it.
pub struct UsageQuotaPlugin {
    path: PathBuf,
}
impl UsageQuotaPlugin {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
impl Plugin for UsageQuotaPlugin {
    type Service = UsageQuota;
    fn validate(&self) -> Result<(), BoxError> {
        if self.path.as_os_str().is_empty() {
            return Err(Error::Invalid("empty database path").into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<UsageQuota>, BoxError> {
        let mut service = UsageQuota::open(self.path)?;
        service.gate = Some(setup.work_gate());
        let service = Arc::new(service);
        let closing = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            closing.close().map_err(Into::into)
        });
        Ok(service)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_commit_is_unknown_and_does_not_trigger_retry_or_refund() {
        let directory = tempfile::tempdir().unwrap();
        let service = UsageQuota::open(directory.path().join("db")).unwrap();
        service
            .execute(
                "create",
                1,
                Command::Create {
                    scope: "api".into(),
                    limit: 5,
                },
            )
            .unwrap();
        service
            .connection
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .commit_hook(Some(|| true))
            .unwrap();
        assert!(
            matches!(service.execute("ambiguous", 2, Command::ApiCall { scope: "api".into(), request: "r".into(), units: 3 }), Err(Error::CommitUnknown { operation }) if operation == "ambiguous")
        );
        // The public API deliberately does not classify arbitrary commit errors as
        // proof of rollback. This injected SQLite veto is known to roll back.
        assert_eq!(service.outcome("ambiguous").unwrap(), Outcome::NotObserved);
        assert_eq!(service.balance("api").unwrap().used, 0);
        assert_eq!(service.api_calls("api").unwrap(), 0);
        assert_eq!(service.records().unwrap().len(), 1);
    }
}
