//! Durable plain-text notifications. See README for trust and retry boundaries.
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
mod resend;
pub use resend::Resend;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid notification input or configuration")]
    Invalid,
    #[error("business key conflicts with immutable notification")]
    Conflict,
    #[error("notification service closed")]
    Closed,
    #[error("notification persistence failed")]
    Storage,
    #[error("notification runtime failed")]
    Runtime,
    #[error("notification not found in scope")]
    Missing,
    #[error("provider identity differs from persisted notification")]
    ProviderChanged,
}
impl From<rusqlite::Error> for Error {
    fn from(_: rusqlite::Error) -> Self {
        Self::Storage
    }
}
/// Host-derived tenant and actor/resource scope. Neither value is an authorization credential.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Scope {
    pub tenant: String,
    pub owner: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Template {
    pub name: String,
    pub version: u32,
    pub category: String,
    pub required: bool,
    pub subject: String,
    pub text: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request {
    pub scope: Scope,
    pub business_key: String,
    pub to: String,
    pub template: Template,
    pub variables: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub to: String,
    pub subject: String,
    pub text: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum State {
    Pending,
    Suppressed,
    Accepted,
    Rejected,
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attempt {
    pub number: u32,
    pub state: State,
    pub provider_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub id: i64,
    pub state: State,
    pub attempts: Vec<Attempt>,
}
/// Implementations must bound I/O and must not retry internally. Unknown includes panic/crash.
pub trait Transport: Send + Sync + 'static {
    /// Stable account + endpoint + sender identity, never a credential.
    fn identity(&self) -> &str;
    fn send(&self, idempotency_key: &str, message: &Message) -> Outcome;
}
#[derive(Clone, Debug)]
pub enum Outcome {
    Accepted(String),
    Rejected,
    Unknown,
}
fn name(s: &str) -> Result<(), Error> {
    if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
        Err(Error::Invalid)
    } else {
        Ok(())
    }
}
fn scope(s: &Scope) -> Result<(), Error> {
    name(&s.tenant)?;
    name(&s.owner)
}
fn render(s: &str, vars: &BTreeMap<String, String>) -> Result<String, Error> {
    let mut out = String::new();
    let mut rest = s;
    while let Some((head, tail)) = rest.split_once("{{") {
        out.push_str(head);
        let (key, tail) = tail.split_once("}}").ok_or(Error::Invalid)?;
        out.push_str(vars.get(key).ok_or(Error::Invalid)?);
        rest = tail;
        if out.len() > 65536 {
            return Err(Error::Invalid);
        }
    }
    out.push_str(rest);
    if out.len() > 65536 {
        return Err(Error::Invalid);
    }
    Ok(out)
}
fn encode<T: Serialize>(v: &T) -> Result<String, Error> {
    serde_json::to_string(v).map_err(|_| Error::Invalid)
}
fn decode<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, Error> {
    serde_json::from_str(s).map_err(|_| Error::Storage)
}
/// Explicit schema provisioning in a dedicated database. Never called by startup.
pub async fn provision(path: impl AsRef<Path>) -> Result<(), Error> {
    let path = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || {
        let db = Connection::open(path)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS notification_meta(version INTEGER NOT NULL);
        INSERT INTO notification_meta SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM notification_meta);
        CREATE TABLE IF NOT EXISTS notifications(
          id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, owner TEXT NOT NULL, business_key TEXT NOT NULL,
          request TEXT NOT NULL, message TEXT NOT NULL, provider TEXT NOT NULL,
          token TEXT NOT NULL DEFAULT (lower(hex(randomblob(24)))), receipt TEXT NOT NULL,
          UNIQUE(tenant,owner,business_key));
        CREATE TABLE IF NOT EXISTS preferences(tenant TEXT NOT NULL, owner TEXT NOT NULL, category TEXT NOT NULL,
          enabled INTEGER NOT NULL, PRIMARY KEY(tenant,owner,category));")?;
        Ok(())
    }).await.map_err(|_| Error::Runtime)?
}
/// One connection serializes operations, including bounded transport I/O. No worker or queue.
#[derive(Clone)]
pub struct Notifications {
    db: Arc<Mutex<Option<Connection>>>,
    transport: Arc<dyn Transport>,
    gate: WorkGate,
}
impl Notifications {
    pub async fn open(
        path: impl AsRef<Path>,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, Error> {
        Self::open_with_gate(path.as_ref().to_owned(), transport, WorkGate::default()).await
    }
    async fn open_with_gate(
        path: PathBuf,
        transport: Arc<dyn Transport>,
        gate: WorkGate,
    ) -> Result<Self, Error> {
        name(transport.identity())?;
        let db = tokio::task::spawn_blocking(move || -> Result<_, Error> {
            let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            db.busy_timeout(Duration::from_secs(2))?;
            db.execute_batch("PRAGMA synchronous=FULL;")?;
            let v: i64 = db.query_row("SELECT version FROM notification_meta", [], |r| r.get(0))?;
            if v != 1 {
                return Err(Error::Invalid);
            }
            Ok(db)
        })
        .await
        .map_err(|_| Error::Runtime)??;
        Ok(Self {
            db: Arc::new(Mutex::new(Some(db))),
            transport,
            gate,
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection, &dyn Transport) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let guard = self.gate.enter().map_err(|_| Error::Closed)?;
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut db = this.db.lock().map_err(|_| Error::Runtime)?;
            f(db.as_mut().ok_or(Error::Closed)?, this.transport.as_ref())
        })
        .await
        .map_err(|_| Error::Runtime)?
    }
    /// Optional category preference is evaluated once at create; required templates bypass it.
    pub async fn set_preference(
        &self,
        s: Scope,
        category: String,
        enabled: bool,
    ) -> Result<(), Error> {
        scope(&s)?;
        name(&category)?;
        self.run(move |db,_| { db.execute("INSERT INTO preferences VALUES(?1,?2,?3,?4) ON CONFLICT(tenant,owner,category) DO UPDATE SET enabled=excluded.enabled", params![s.tenant,s.owner,category,enabled])?; Ok(()) }).await
    }
    pub async fn create(&self, request: Request) -> Result<Receipt, Error> {
        scope(&request.scope)?;
        name(&request.business_key)?;
        name(&request.to)?;
        name(&request.template.name)?;
        name(&request.template.category)?;
        if request.template.version == 0 || encode(&request)?.len() > 65536 {
            return Err(Error::Invalid);
        }
        let message = Message {
            to: request.to.clone(),
            subject: render(&request.template.subject, &request.variables)?,
            text: render(&request.template.text, &request.variables)?,
        };
        name(&message.subject)?;
        self.run(move |db,transport| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let original: Option<(String,String)> = tx.query_row("SELECT request,receipt FROM notifications WHERE tenant=?1 AND owner=?2 AND business_key=?3", params![request.scope.tenant,request.scope.owner,request.business_key], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
            let encoded = encode(&request)?;
            if let Some((old,receipt)) = original { return if old == encoded { decode(&receipt) } else { Err(Error::Conflict) }; }
            let enabled: bool = tx.query_row("SELECT enabled FROM preferences WHERE tenant=?1 AND owner=?2 AND category=?3", params![request.scope.tenant,request.scope.owner,request.template.category], |r| r.get(0)).optional()?.unwrap_or(true);
            tx.execute("INSERT INTO notifications(tenant,owner,business_key,request,message,provider,receipt) VALUES(?1,?2,?3,?4,?5,?6,'')",params![request.scope.tenant,request.scope.owner,request.business_key,encoded,encode(&message)?,transport.identity()])?;
            let receipt = Receipt { id: tx.last_insert_rowid(), state: if enabled || request.template.required {State::Pending} else {State::Suppressed}, attempts: vec![] };
            save(&tx,&receipt)?; tx.commit()?; Ok(receipt)
        }).await
    }
    pub async fn status(&self, s: Scope, id: i64) -> Result<Receipt, Error> {
        scope(&s)?;
        self.run(move |db, _| load(db, &s, id)).await
    }
    /// Sends only Pending or definitively Rejected records, at most ten attempts.
    /// Unknown is terminal for automated delivery, even within a provider dedupe window.
    pub async fn deliver(&self, s: Scope, id: i64) -> Result<Receipt, Error> {
        scope(&s)?;
        self.run(move |db, transport| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut receipt = load(&tx, &s, id)?;
            if !matches!(receipt.state, State::Pending | State::Rejected)
                || receipt.attempts.len() >= 10
            {
                return Ok(receipt);
            }
            let (provider, token, message): (String, String, String) = tx.query_row(
                "SELECT provider,token,message FROM notifications WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if provider != transport.identity() {
                return Err(Error::ProviderChanged);
            }
            let message: Message = decode(&message)?;
            receipt.state = State::Unknown;
            receipt.attempts.push(Attempt {
                number: receipt.attempts.len() as u32 + 1,
                state: State::Unknown,
                provider_id: None,
            });
            save(&tx, &receipt)?;
            tx.commit()?; // commit uncertainty BEFORE any external effect
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                transport.send(&token, &message)
            }))
            .unwrap_or(Outcome::Unknown);
            let (state, provider_id) = match outcome {
                Outcome::Accepted(id) => (State::Accepted, Some(id)),
                Outcome::Rejected => (State::Rejected, None),
                Outcome::Unknown => (State::Unknown, None),
            };
            receipt.state = state.clone();
            let last = receipt.attempts.last_mut().ok_or(Error::Storage)?;
            last.state = state;
            last.provider_id = provider_id;
            save(db, &receipt)?;
            Ok(receipt)
        })
        .await
    }
    /// Bounded recovery inventory; never resends or resets uncertain attempts.
    pub async fn recover(&self, s: Scope, after: i64, limit: u32) -> Result<Vec<Receipt>, Error> {
        scope(&s)?;
        if after < 0 || limit == 0 || limit > 100 {
            return Err(Error::Invalid);
        }
        self.run(move |db,_| {
            let mut stmt = db.prepare("SELECT receipt FROM notifications WHERE tenant=?1 AND owner=?2 AND id>?3 ORDER BY id LIMIT ?4")?;
            let rows = stmt.query_map(params![s.tenant,s.owner,after,limit],|r| r.get::<_,String>(0))?;
            rows.map(|row| decode(&row?)).collect()
        }).await
    }
    /// Waits for serialized admitted operations and closes the connection; cancellation safe.
    pub async fn close(&self) -> Result<(), Error> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.lock().map_err(|_| Error::Runtime)?.take();
            Ok(())
        })
        .await
        .map_err(|_| Error::Runtime)?
    }
}
fn load(db: &Connection, s: &Scope, id: i64) -> Result<Receipt, Error> {
    let raw: String = db
        .query_row(
            "SELECT receipt FROM notifications WHERE tenant=?1 AND owner=?2 AND id=?3",
            params![s.tenant, s.owner, id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or(Error::Missing)?;
    decode(&raw)
}
fn save(db: &Connection, receipt: &Receipt) -> Result<(), Error> {
    db.execute(
        "UPDATE notifications SET receipt=?1 WHERE id=?2",
        params![encode(receipt)?, receipt.id],
    )?;
    Ok(())
}
pub struct NotificationsPlugin {
    path: PathBuf,
    transport: Arc<dyn Transport>,
}
impl NotificationsPlugin {
    pub fn new(path: impl Into<PathBuf>, transport: Arc<dyn Transport>) -> Self {
        Self {
            path: path.into(),
            transport,
        }
    }
}
impl Plugin for NotificationsPlugin {
    type Service = Notifications;
    fn validate(&self) -> Result<(), BoxError> {
        name(self.transport.identity())?;
        if self.path.as_os_str().is_empty() {
            return Err(Error::Invalid.into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Notifications>, BoxError> {
        let service = Arc::new(
            Notifications::open_with_gate(self.path, self.transport, setup.work_gate()).await?,
        );
        let cleanup = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().await.map_err(Into::into)
        });
        Ok(service)
    }
}
