//! Lenso v1 signed receiver with a same-database transactional inbox.
use hmac::{Hmac, KeyInit, Mac};
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    Connection, OpenFlags, Params,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncRead, AsyncReadExt};
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid receiver configuration")]
    Config,
    #[error("webhook authentication failed")]
    Unauthorized,
    #[error("webhook payload invalid")]
    Invalid,
    #[error("webhook source or type forbidden")]
    Forbidden,
    #[error("webhook body too large")]
    TooLarge,
    #[error("webhook body timed out")]
    Timeout,
    #[error("webhook event conflict")]
    Conflict,
    #[error("webhook receiver unavailable")]
    Unavailable,
    #[error("webhook business apply failed")]
    Apply,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    Duplicate,
}
/// Host-selected key. Intentionally no Debug implementation.
#[derive(Clone)]
pub struct SigningKey {
    pub id: String,
    pub secret: Vec<u8>,
}
#[derive(Clone)]
pub struct Config {
    pub receiver_id: String,
    pub source: String,
    pub event_types: Vec<String>,
    pub keys: Vec<SigningKey>,
    pub max_body_bytes: usize,
    pub body_timeout: Duration,
    pub tolerance_seconds: u64,
}
fn bounded(s: &str, n: usize) -> bool {
    !s.is_empty() && s.len() <= n
}
fn identifier(s: &str) -> bool {
    bounded(s, 128)
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
impl Config {
    /// # Errors
    /// Returns `Config` for invalid receiver/source/type/key configuration or body limits.
    pub fn validate(&self) -> Result<(), Error> {
        if !bounded(&self.receiver_id, 256)
            || !bounded(&self.source, 256)
            || self.event_types.is_empty()
            || self.event_types.len() > 1000
            || self.event_types.iter().any(|v| !bounded(v, 128))
            || self.keys.is_empty()
            || self.keys.len() > 64
            || self
                .keys
                .iter()
                .any(|k| !identifier(&k.id) || k.secret.is_empty())
            || self.max_body_bytes == 0
            || self.max_body_bytes > 16 * 1024 * 1024
            || self.body_timeout.is_zero()
            || self.body_timeout > Duration::from_secs(3600)
        {
            return Err(Error::Config);
        }
        Ok(())
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub version: u8,
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub source: String,
    #[serde(rename = "occurredAt")]
    pub occurred_at: String,
    #[serde(deserialize_with = "required_data")]
    pub data: serde_json::Value,
}
fn required_data<'de, D: serde::Deserializer<'de>>(d: D) -> Result<serde_json::Value, D::Error> {
    serde_json::Value::deserialize(d)
}
/// Host must reject duplicate security headers before constructing this value.
pub struct Headers {
    pub event_id: String,
    pub timestamp: String,
    pub signature: String,
}
/// Restricted SQL capability. No connection, transaction control, schema changes,
/// inbox access, SQL functions or attached databases are exposed.
pub struct BusinessTransaction<'a> {
    connection: &'a Connection,
    failed: std::cell::Cell<bool>,
}
impl BusinessTransaction<'_> {
    /// # Errors
    /// Returns `Apply` for forbidden/failed SQL or an already-aborted transaction.
    /// A swallowed statement error still prevents inbox and business commit.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize, Error> {
        if self.failed.get() || self.connection.is_autocommit() {
            self.failed.set(true);
            return Err(Error::Apply);
        }
        match self.connection.execute(sql, params) {
            Ok(n) => Ok(n),
            Err(_) => {
                self.failed.set(true);
                Err(Error::Apply)
            }
        }
    }
}
fn business_auth(ctx: AuthContext<'_>) -> Authorization {
    match ctx.action {
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Read { table_name, .. }
            if ctx.database_name == Some("main")
                && !table_name.eq_ignore_ascii_case("lenso_webhook_inbox")
                && !table_name.to_ascii_lowercase().starts_with("sqlite_") =>
        {
            Authorization::Allow
        }
        AuthAction::Select => Authorization::Allow,
        _ => Authorization::Deny,
    }
}
/// Explicit migration; the host separately provisions business tables in this database.
///
/// # Errors
/// Returns `Unavailable` if opening or provisioning the inbox database fails.
pub fn provision(path: impl AsRef<Path>) -> Result<(), Error> {
    let c = Connection::open(path).map_err(|_| Error::Unavailable)?;
    c.execute_batch("CREATE TABLE IF NOT EXISTS lenso_webhook_inbox(receiver_id TEXT NOT NULL,event_id TEXT NOT NULL,body_digest BLOB NOT NULL,received_at INTEGER NOT NULL,PRIMARY KEY(receiver_id,event_id));").map_err(|_|Error::Unavailable)
}
pub struct Receiver {
    config: Config,
    connection: Mutex<Option<Connection>>,
    gate: WorkGate,
}
impl Receiver {
    /// # Errors
    /// Returns `Config` for invalid settings or `Unavailable` for database/schema failures.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Arc<Self>, Error> {
        Self::open_with_gate(path, config, WorkGate::default())
    }
    fn open_with_gate(
        path: impl AsRef<Path>,
        config: Config,
        gate: WorkGate,
    ) -> Result<Arc<Self>, Error> {
        config.validate()?;
        let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|_| Error::Unavailable)?;
        c.busy_timeout(Duration::from_secs(5))
            .map_err(|_| Error::Unavailable)?;
        c.prepare(
            "SELECT receiver_id,event_id,body_digest,received_at FROM lenso_webhook_inbox LIMIT 0",
        )
        .map_err(|_| Error::Unavailable)?;
        Ok(Arc::new(Self {
            config,
            connection: Mutex::new(Some(c)),
            gate,
        }))
    }
    /// Waits for active SQL work, closes the resource, and rejects later transactions.
    ///
    /// # Errors
    /// Returns `Unavailable` if the connection mutex is poisoned.
    pub fn close(&self) -> Result<(), Error> {
        self.connection
            .lock()
            .map_err(|_| Error::Unavailable)?
            .take();
        Ok(())
    }
    /// Host-owned request adapter. Host checks POST, JSON content type and identity
    /// content encoding. Reader supplies exact raw bytes, before any JSON decoding.
    ///
    /// # Errors
    /// Returns authentication, payload, policy, size/timeout, event-conflict,
    /// business-apply or persistence errors. An unconfirmed commit may have applied;
    /// redelivery must retain the same event ID and exact body for deduplication.
    pub async fn receive<R, F>(
        self: &Arc<Self>,
        headers: Headers,
        mut body: R,
        apply: F,
    ) -> Result<Outcome, Error>
    where
        R: AsyncRead + Unpin,
        F: FnOnce(&BusinessTransaction<'_>, &Event) -> Result<(), Error> + Send + 'static,
    {
        let guard = self.gate.enter().map_err(|_| Error::Unavailable)?;
        let maximum = self.config.max_body_bytes;
        let bytes = tokio::time::timeout(self.config.body_timeout, async {
            let mut bytes = Vec::new();
            let mut chunk = [0; 8192];
            loop {
                let n = body.read(&mut chunk).await.map_err(|_| Error::Invalid)?;
                if n == 0 {
                    break;
                }
                if bytes.len() + n > maximum {
                    return Err(Error::TooLarge);
                }
                bytes.extend_from_slice(&chunk[..n]);
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| Error::Timeout)??;
        let receiver = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            receiver.commit(headers, bytes, apply)
        })
        .await
        .map_err(|_| Error::Unavailable)?
    }
    fn commit<F>(&self, headers: Headers, bytes: Vec<u8>, apply: F) -> Result<Outcome, Error>
    where
        F: FnOnce(&BusinessTransaction<'_>, &Event) -> Result<(), Error>,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_secs();
        verify(&self.config, &headers, &bytes, now)?;
        let event: Event = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        if event.version != 1
            || event.id != headers.event_id
            || !bounded(&event.event_type, 128)
            || !bounded(&event.source, 256)
            || !bounded(&event.occurred_at, 64)
            || chrono::DateTime::parse_from_rfc3339(&event.occurred_at).is_err()
        {
            return Err(Error::Invalid);
        }
        fn check(v: &serde_json::Value, depth: usize, n: &mut usize) -> bool {
            *n += 1;
            if depth > 32 || *n > 50_000 {
                return false;
            }
            match v {
                serde_json::Value::Array(a) => a.iter().all(|v| check(v, depth + 1, n)),
                serde_json::Value::Object(o) => o.values().all(|v| check(v, depth + 1, n)),
                _ => true,
            }
        }
        if !check(&event.data, 0, &mut 0) {
            return Err(Error::Invalid);
        }
        if event.source != self.config.source
            || !self.config.event_types.contains(&event.event_type)
        {
            return Err(Error::Forbidden);
        }
        let digest = Sha256::digest(&bytes).to_vec();
        let mut lock = self.connection.lock().map_err(|_| Error::Unavailable)?;
        let c = lock.as_mut().ok_or(Error::Unavailable)?;
        let tx = c
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|_| Error::Unavailable)?;
        use rusqlite::OptionalExtension;
        let prior: Option<Vec<u8>> = tx
            .query_row(
                "SELECT body_digest FROM lenso_webhook_inbox WHERE receiver_id=?1 AND event_id=?2",
                (&self.config.receiver_id, &event.id),
                |r| r.get(0),
            )
            .optional()
            .map_err(|_| Error::Unavailable)?;
        if let Some(prior) = prior {
            return if prior == digest {
                Ok(Outcome::Duplicate)
            } else {
                Err(Error::Conflict)
            };
        }
        tx.execute(
            "INSERT INTO lenso_webhook_inbox VALUES (?1,?2,?3,?4)",
            (
                &self.config.receiver_id,
                &event.id,
                &digest,
                i64::try_from(now).map_err(|_| Error::Unavailable)?,
            ),
        )
        .map_err(|_| Error::Unavailable)?;
        tx.authorizer(Some(business_auth))
            .map_err(|_| Error::Unavailable)?;
        let business = BusinessTransaction {
            connection: &tx,
            failed: std::cell::Cell::new(false),
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| apply(&business, &event)));
        tx.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .map_err(|_| Error::Unavailable)?;
        if business.failed.get() || tx.is_autocommit() {
            return Err(Error::Apply);
        }
        result.map_err(|_| Error::Apply)??;
        tx.commit().map_err(|_| Error::Unavailable)?;
        Ok(Outcome::Accepted)
    }
}
fn verify(config: &Config, h: &Headers, body: &[u8], now: u64) -> Result<(), Error> {
    let bad = Error::Unauthorized;
    if !identifier(&h.event_id)
        || h.timestamp.is_empty()
        || h.timestamp.len() > 16
        || !h.timestamp.bytes().all(|b| b.is_ascii_digit())
        || (h.timestamp.starts_with('0') && h.timestamp.len() > 1)
        || h.signature.len() > 220
    {
        return Err(bad);
    }
    let timestamp = h.timestamp.parse::<u64>().map_err(|_| bad)?;
    if timestamp > 9_007_199_254_740_991 || now.abs_diff(timestamp) > config.tolerance_seconds {
        return Err(bad);
    }
    let (kid, mac) = h
        .signature
        .strip_prefix("v1;kid=")
        .and_then(|s| s.split_once(";mac="))
        .ok_or(bad)?;
    if !identifier(kid)
        || mac.len() != 64
        || !mac
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(bad);
    }
    let mut received = [0u8; 32];
    for (i, v) in received.iter_mut().enumerate() {
        *v = u8::from_str_radix(&mac[i * 2..i * 2 + 2], 16).map_err(|_| bad)?;
    }
    for key in &config.keys {
        if key.id == kid {
            let mut m = Hmac::<Sha256>::new_from_slice(&key.secret).map_err(|_| bad)?;
            m.update(h.timestamp.as_bytes());
            m.update(b"\n");
            m.update(h.event_id.as_bytes());
            m.update(b"\n");
            m.update(body);
            if m.verify_slice(&received).is_ok() {
                return Ok(());
            }
        }
    }
    Err(bad)
}
pub struct ReceiverPlugin {
    pub path: PathBuf,
    pub config: Config,
}
impl Plugin for ReceiverPlugin {
    type Service = Receiver;
    fn validate(&self) -> Result<(), BoxError> {
        self.config.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Receiver>, BoxError> {
        let receiver = Receiver::open_with_gate(self.path, self.config, setup.work_gate())?;
        let owned = Arc::clone(&receiver);
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            owned.close().map_err(Into::into)
        });
        Ok(receiver)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_v1_golden_vector() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixture.json")).unwrap();
        let cfg = Config {
            receiver_id: "fixture".into(),
            source: "partner".into(),
            event_types: vec!["order.created".into()],
            keys: vec![SigningKey {
                id: "fixture".into(),
                secret: b"PUBLIC-TEST-FIXTURE-NOT-A-SECRET".to_vec(),
            }],
            max_body_bytes: 4096,
            body_timeout: Duration::from_secs(1),
            tolerance_seconds: 300,
        };
        let h = Headers {
            event_id: "evt_1".into(),
            timestamp: fixture["timestamp"].as_str().unwrap().into(),
            signature: fixture["signature"].as_str().unwrap().into(),
        };
        let body = fixture["body"].as_str().unwrap().as_bytes();
        assert_eq!(verify(&cfg, &h, body, 1792627200), Ok(()));
        let mut changed = body.to_vec();
        changed.push(b' ');
        assert_eq!(
            verify(&cfg, &h, &changed, 1792627200),
            Err(Error::Unauthorized)
        );
    }
}
