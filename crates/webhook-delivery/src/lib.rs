#![doc = include_str!("../README.md")]
mod http;
pub use http::Endpoint;
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use lenso_tasks::{Cancellation, Failure, RetryPolicy, Task};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid webhook configuration or input")]
    Invalid,
    #[error("delivery identity conflicts with immutable stored request")]
    Conflict,
    #[error("delivery not found")]
    Missing,
    #[error("webhook service closed")]
    Closed,
    #[error("database operation failed; inspect event ID before retrying writes")]
    Database(#[from] rusqlite::Error),
    #[error("webhook runtime failed")]
    Runtime,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    Pending,
    Retryable,
    Delivered,
    Paused,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Receipt {
    pub event_id: String,
    pub state: State,
    pub attempts: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    pub number: u32,
    pub outcome: String,
    pub http_status: Option<u16>,
}
/// Acknowledges that the previous attempt may already have caused a remote effect.
pub enum ReplayConsent {
    AcceptPossibleDuplicate,
}

pub(crate) fn identifier(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 200
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
    {
        Err(Error::Invalid)
    } else {
        Ok(())
    }
}
/// Explicit provisioning only; use a dedicated local database, never Storage tables.
pub async fn provision(path: impl AsRef<Path>) -> Result<(), Error> {
    let path = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || -> Result<(), Error> {
        let db = Connection::open(path)?;
        db.execute_batch("PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS webhook_meta(version INTEGER NOT NULL);
          INSERT INTO webhook_meta SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM webhook_meta);
          CREATE TABLE IF NOT EXISTS deliveries(event TEXT PRIMARY KEY, endpoint TEXT NOT NULL,
            payload BLOB NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL, budget INTEGER NOT NULL, initial_budget INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS attempts(event TEXT NOT NULL, number INTEGER NOT NULL,
            outcome TEXT NOT NULL, http_status INTEGER, PRIMARY KEY(event,number));")?;
        Ok(())
    }).await.map_err(|_| Error::Runtime)?
}
#[derive(Clone)]
pub struct Delivery {
    db: Arc<Mutex<Option<Connection>>>,
    endpoint: Arc<Endpoint>,
    gate: WorkGate,
    stopping: Arc<AtomicBool>,
}
impl Delivery {
    async fn open(path: PathBuf, endpoint: Arc<Endpoint>, gate: WorkGate) -> Result<Self, Error> {
        let db = tokio::task::spawn_blocking(move || -> Result<_, Error> {
            let mut db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            db.busy_timeout(Duration::from_secs(35))?;
            db.execute_batch("PRAGMA synchronous=FULL;")?;
            if db.query_row("SELECT version FROM webhook_meta", [], |r| {
                r.get::<_, i64>(0)
            })? != 1
            {
                return Err(Error::Invalid);
            }
            // An immediate transaction fences the bounded network transaction of any
            // other process. A crash or abandoned pre-send claim becomes unknown.
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "UPDATE attempts SET outcome='unknown' WHERE outcome='sending'",
                [],
            )?;
            tx.execute(
                "UPDATE deliveries SET state='unknown' WHERE state='sending'",
                [],
            )?;
            tx.commit()?;
            Ok(db)
        })
        .await
        .map_err(|_| Error::Runtime)??;
        Ok(Self {
            db: Arc::new(Mutex::new(Some(db))),
            endpoint,
            gate,
            stopping: Arc::new(AtomicBool::new(false)),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection, &Delivery) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let guard = self.gate.enter().map_err(|_| Error::Closed)?;
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut db = this.db.lock().map_err(|_| Error::Runtime)?;
            f(db.as_mut().ok_or(Error::Closed)?, &this)
        })
        .await
        .map_err(|_| Error::Runtime)?
    }
    /// The authenticated host supplies a globally unique stable business event ID.
    /// Persist exact JSON bytes once; retries never deserialize/reserialize them.
    pub async fn create(
        &self,
        event_id: String,
        raw_json: Vec<u8>,
        max_attempts: u32,
    ) -> Result<Receipt, Error> {
        identifier(&event_id)?;
        if raw_json.len() > 65_536
            || max_attempts == 0
            || max_attempts > 100
            || serde_json::from_slice::<serde_json::Value>(&raw_json).is_err()
        {
            return Err(Error::Invalid);
        }
        self.run(move |db, this| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing: Option<(String, Vec<u8>, u32)> = tx
                .query_row(
                    "SELECT endpoint,payload,initial_budget FROM deliveries WHERE event=?1",
                    [&event_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((endpoint, payload, budget)) = existing {
                if endpoint != this.endpoint.identity
                    || payload != raw_json
                    || budget != max_attempts
                {
                    return Err(Error::Conflict);
                }
            } else {
                tx.execute(
                    "INSERT INTO deliveries VALUES(?1,?2,?3,'pending',0,?4,?4)",
                    params![event_id, this.endpoint.identity, raw_json, max_attempts],
                )?;
            }
            let receipt = read(&tx, &event_id, &this.endpoint.identity)?;
            tx.commit()?;
            Ok(receipt)
        })
        .await
    }
    pub async fn status(&self, event_id: String) -> Result<Receipt, Error> {
        self.run(move |db, this| read(db, &event_id, &this.endpoint.identity))
            .await
    }
    pub async fn attempts(&self, event_id: String) -> Result<Vec<Attempt>, Error> {
        self.run(move |db, this| {
            read(db, &event_id, &this.endpoint.identity)?;
            let mut stmt = db.prepare(
                "SELECT number,outcome,http_status FROM attempts WHERE event=?1 ORDER BY number",
            )?;
            let rows = stmt.query_map([event_id], |r| {
                Ok(Attempt {
                    number: r.get(0)?,
                    outcome: r.get(1)?,
                    http_status: r.get(2)?,
                })
            })?;
            Ok(rows.collect::<Result<_, _>>()?)
        })
        .await
    }
    /// Pause prevents subsequent attempts; it cannot undo an already accepted remote effect.
    pub async fn pause(&self, event_id: String) -> Result<Receipt, Error> {
        self.run(move |db,this| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            read(&tx,&event_id,&this.endpoint.identity)?;
            tx.execute("UPDATE deliveries SET state='paused' WHERE event=?1 AND state IN ('pending','retryable')", [&event_id])?;
            let receipt=read(&tx,&event_id,&this.endpoint.identity)?; tx.commit()?; Ok(receipt)
        }).await
    }
    /// Reopens paused/unknown only, adding exactly one permitted attempt without
    /// changing event ID, endpoint, payload or history. Host must schedule a new Tasks
    /// job with a new dedupe key; a previously successful Tasks job is not reopened.
    pub async fn replay(&self, event_id: String, _: ReplayConsent) -> Result<Receipt, Error> {
        self.run(move |db,this| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            read(&tx,&event_id,&this.endpoint.identity)?;
            tx.execute("UPDATE deliveries SET state='pending',budget=attempts+1 WHERE event=?1 AND state IN ('paused','unknown')", [&event_id])?;
            let receipt=read(&tx,&event_id,&this.endpoint.identity)?; tx.commit()?; Ok(receipt)
        }).await
    }
    /// One attempt only; no sleeping, polling, timer or internal queue. Prefer `task`.
    pub async fn deliver(
        &self,
        event_id: String,
        cancellation: Option<Cancellation>,
    ) -> Result<Receipt, Error> {
        self.run(move |db, this| {
            let stopped = || {
                this.stopping.load(Ordering::Acquire)
                    || cancellation.as_ref().is_some_and(|c| c.reason().is_some())
            };
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let receipt = read(&tx, &event_id, &this.endpoint.identity)?;
            if stopped() || !matches!(receipt.state, State::Pending | State::Retryable) {
                return Ok(receipt);
            }
            let (payload, budget): (Vec<u8>, u32) = tx.query_row(
                "SELECT payload,budget FROM deliveries WHERE event=?1",
                [&event_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if receipt.attempts >= budget {
                return Ok(receipt);
            }
            let number = receipt.attempts + 1;
            tx.execute(
                "UPDATE deliveries SET state='sending',attempts=?2 WHERE event=?1",
                params![event_id, number],
            )?;
            tx.execute(
                "INSERT INTO attempts VALUES(?1,?2,'sending',NULL)",
                params![event_id, number],
            )?;
            // Durable uncertainty BEFORE any external effect.
            tx.commit()?;
            // Hold a bounded writer fence over I/O and settlement. Another instance
            // cannot recover/replay this claim while its request is still executing.
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let state: String = tx.query_row(
                "SELECT state FROM deliveries WHERE event=?1 AND attempts=?2",
                params![event_id, number],
                |r| r.get(0),
            )?;
            if state != "sending" {
                return read(&tx, &event_id, &this.endpoint.identity);
            }
            let status = if stopped() {
                None
            } else {
                this.endpoint.send(&event_id, &payload)
            };
            let state = if stopped() {
                "unknown"
            } else {
                match status {
                    Some(200..=299) => "delivered",
                    Some(429 | 500..=599) if number < budget => "retryable",
                    Some(_) => "paused",
                    None => "unknown",
                }
            };
            tx.execute(
                "UPDATE attempts SET outcome=?3,http_status=?4 WHERE event=?1 AND number=?2",
                params![event_id, number, state, status],
            )?;
            tx.execute(
                "UPDATE deliveries SET state=?2 WHERE event=?1",
                params![event_id, state],
            )?;
            let receipt = read(&tx, &event_id, &this.endpoint.identity)?;
            tx.commit()?;
            Ok(receipt)
        })
        .await
    }
    /// Public Tasks adapter: retry only a stored retryable response. Unknown,
    /// paused and delivered are terminal successful inspection results for Tasks.
    /// Install Tasks in a host-owned lifetime; shut it down before this plugin.
    pub fn task(&self, name: &str, retry: RetryPolicy) -> Task {
        let sender = self.clone();
        Task::new(name, retry, move |event: String, context| {
            let sender = sender.clone();
            async move {
                let receipt = sender
                    .deliver(event, Some(context.cancellation))
                    .await
                    .map_err(|_| Failure::HandlerFailed)?;
                if receipt.state == State::Retryable {
                    Err(Failure::HandlerFailed)
                } else {
                    serde_json::to_value(receipt).map_err(|_| Failure::InvalidResult)
                }
            }
        })
    }
}
fn read(db: &Connection, event: &str, endpoint: &str) -> Result<Receipt, Error> {
    let (stored, state, attempts): (String, String, u32) = db
        .query_row(
            "SELECT endpoint,state,attempts FROM deliveries WHERE event=?1",
            [event],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?
        .ok_or(Error::Missing)?;
    if stored != endpoint {
        return Err(Error::Conflict);
    }
    let state = match state.as_str() {
        "pending" => State::Pending,
        "retryable" => State::Retryable,
        "delivered" => State::Delivered,
        "paused" => State::Paused,
        "unknown" | "sending" => State::Unknown,
        _ => return Err(Error::Invalid),
    };
    Ok(Receipt {
        event_id: event.into(),
        state,
        attempts,
    })
}
pub struct DeliveryPlugin {
    path: PathBuf,
    endpoint: Arc<Endpoint>,
}
impl DeliveryPlugin {
    pub fn new(path: impl Into<PathBuf>, endpoint: Endpoint) -> Self {
        Self {
            path: path.into(),
            endpoint: Arc::new(endpoint),
        }
    }
}
impl Plugin for DeliveryPlugin {
    type Service = Delivery;
    fn validate(&self) -> Result<(), BoxError> {
        if self.path.as_os_str().is_empty() {
            Err(Error::Invalid.into())
        } else {
            Ok(())
        }
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Delivery>, BoxError> {
        let service = Arc::new(Delivery::open(self.path, self.endpoint, setup.work_gate()).await?);
        let close = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            tokio::task::spawn_blocking(move || -> Result<(), Error> {
                let mut slot = close.db.lock().map_err(|_| Error::Runtime)?;
                if let Some(db) = slot.take() {
                    db.close().map_err(|(_, e)| Error::Database(e))?;
                }
                Ok(())
            })
            .await??;
            Ok(())
        });
        let stop = service.stopping.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            stop.store(true, Ordering::Release);
            Ok(())
        });
        Ok(service)
    }
}
