//! Bounded imports from Storage, committed through the Tasks SQLite scope.
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup, WorkGate};
use lenso_storage::{CancellationToken, LocalStorage, ObjectStorage, ReadOptions, ReadRange};
use lenso_tasks::{SqliteTaskTransaction, TaskError, Tasks};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use tokio::io::BufReader;
mod parser;

/// Explicitly provision alongside application tables before starting Tasks.
pub const SCHEMA: &str = "CREATE TABLE lenso_import_sessions(queue TEXT NOT NULL, id TEXT NOT NULL, binding TEXT NOT NULL, offset INTEGER NOT NULL, row INTEGER NOT NULL, PRIMARY KEY(queue,id));";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Format {
    Csv,
    Jsonl,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Kind {
    Text,
    Integer,
    Boolean,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Field {
    /// CSV: zero-based column index as decimal text. JSONL: object property.
    pub source: String,
    pub target: String,
    pub kind: Kind,
    pub required: bool,
    pub max_chars: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InvalidRows {
    Stop,
    Skip,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    /// Change when business validation or sink behavior changes.
    pub version: String,
    pub format: Format,
    /// CSV has no implicit header; import data rows only.
    pub fields: Vec<Field>,
    pub invalid_rows: InvalidRows,
    pub max_bytes: u64,
    pub max_record_bytes: usize,
    pub batch_rows: usize,
}
impl Plan {
    fn validate(&self) -> Result<(), Error> {
        let mut names = HashSet::new();
        if self.version.is_empty()
            || self.fields.is_empty()
            || self.fields.len() > 256
            || self.max_bytes > i64::MAX as u64
            || self.max_record_bytes == 0
            || self.max_record_bytes > 16 * 1024 * 1024
            || self.batch_rows == 0
            || self.batch_rows > 1000
            || self.fields.iter().any(|f| {
                f.target.is_empty()
                    || !names.insert(&f.target)
                    || (matches!(self.format, Format::Csv) && f.source.parse::<usize>().is_err())
            })
        {
            return Err(Error::InvalidPlan);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cursor {
    pub offset: u64,
    pub row: u64,
}
#[derive(Clone, Debug)]
pub struct RowError {
    pub row: u64,
    pub offset: u64,
    pub field: Option<String>,
    pub code: &'static str,
}
#[derive(Clone, Debug)]
pub struct Record {
    pub row: u64,
    pub offset: u64,
    pub values: Map<String, Value>,
}
#[derive(Debug)]
pub struct Batch {
    pub cursor: Cursor,
    pub records: Vec<Record>,
    pub errors: Vec<RowError>,
    pub done: bool,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid import plan")]
    InvalidPlan,
    #[error("source exceeds limit")]
    Limit,
    #[error("session source or plan changed")]
    BindingChanged,
    #[error("another importer advanced the cursor; reload before retrying")]
    StaleCursor,
    #[error("import cancelled or service closed; inspect checkpoint before retrying")]
    Cancelled,
    #[error("invalid row: {0:?}")]
    Row(RowError),
    #[error("malformed CSV framing")]
    Framing,
    #[error("storage error")]
    Storage(#[from] lenso_storage::StorageError),
    #[error("input read failed")]
    Io(#[from] std::io::Error),
    #[error("transaction failed; reconcile checkpoint on unconfirmed commit")]
    Tasks(#[from] TaskError),
}
/// Trusted synchronous business code. Only writes through `tx` share checkpoint
/// atomicity. No network/external effects; never retain or recursively call Tasks.
pub trait Sink: Send + Sync + 'static {
    fn validate(&self, _record: &Record) -> Result<(), &'static str> {
        Ok(())
    }
    fn write(&self, tx: &mut SqliteTaskTransaction<'_>, rows: &[Record]) -> Result<(), TaskError>;
}
pub struct ImportPlugin {
    pub storage: Handle<LocalStorage>,
    pub tasks: Handle<Tasks>,
    pub plan: Plan,
    pub sink: Arc<dyn Sink>,
}
impl Plugin for ImportPlugin {
    type Service = Importer;
    fn validate(&self) -> Result<(), BoxError> {
        self.plan.validate().map_err(Into::into)
    }
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.storage.key(), self.tasks.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Importer>, BoxError> {
        let stop = CancellationToken::new();
        let closing = stop.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            closing.cancel();
            Ok(())
        });
        Ok(Arc::new(Importer {
            storage: setup.get(&self.storage)?,
            tasks: setup.get(&self.tasks)?,
            plan: self.plan,
            sink: self.sink,
            gate: setup.work_gate(),
            stop,
        }))
    }
}
pub struct Importer {
    storage: Arc<dyn ObjectStorage>,
    tasks: Arc<Tasks>,
    plan: Plan,
    sink: Arc<dyn Sink>,
    gate: WorkGate,
    stop: CancellationToken,
}
impl Importer {
    /// Seal admission and interrupt reads. Already dispatched SQLite transactions
    /// settle independently; Core shutdown drains their accepted work.
    pub fn close(&self) {
        self.stop.cancel();
    }
    fn check(&self, cancel: &CancellationToken) -> Result<(), Error> {
        if self.stop.is_cancelled() || cancel.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
    /// Read one bounded preview from the beginning without creating a session.
    pub async fn preview(&self, key: &str, cancel: CancellationToken) -> Result<Batch, Error> {
        let _guard = self.gate.enter().map_err(|_| Error::Cancelled)?;
        self.check(&cancel)?;
        let meta = self
            .storage
            .head(key, Default::default())
            .await?
            .ok_or(Error::BindingChanged)?;
        self.read(key, &meta.etag, meta.size, Cursor::default(), &cancel)
            .await
    }
    /// One batch per call. Repeat the SAME session id to resume/reconcile, including
    /// after a dropped waiter. A new id intentionally represents a new import.
    pub async fn next(
        &self,
        id: &str,
        key: &str,
        cancel: CancellationToken,
    ) -> Result<Batch, Error> {
        let _guard = self.gate.enter().map_err(|_| Error::Cancelled)?;
        self.check(&cancel)?;
        if id.is_empty() || id.len() > 256 {
            return Err(Error::InvalidPlan);
        }
        let meta = self
            .storage
            .head(key, Default::default())
            .await?
            .ok_or(Error::BindingChanged)?;
        if meta.size > self.plan.max_bytes {
            return Err(Error::Limit);
        }
        let binding = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(key, &meta.etag, meta.size, &self.plan))
                    .map_err(|_| Error::InvalidPlan)?
            )
        );
        let queue = self.tasks.identity().to_owned();
        let session = id.to_owned();
        let (q, s, b) = (queue.clone(), session.clone(), binding.clone());
        let state = self
            .tasks
            .sqlite_transaction(move |tx| {
                tx.execute(
                    "INSERT OR IGNORE INTO lenso_import_sessions VALUES (?1,?2,?3,0,0)",
                    (&q, &s, &b),
                )?;
                tx.query_row(
                    "SELECT binding,offset,row FROM lenso_import_sessions WHERE queue=?1 AND id=?2",
                    (&q, &s),
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, i64>(2)?,
                        ))
                    },
                )
            })
            .await?;
        if state.0 != binding {
            return Err(Error::BindingChanged);
        }
        if state.1 < 0 || state.2 < 0 {
            return Err(Error::BindingChanged);
        }
        let start = Cursor {
            offset: state.1 as u64,
            row: state.2 as u64,
        };
        let batch = self
            .read(key, &meta.etag, meta.size, start.clone(), &cancel)
            .await?;
        self.check(&cancel)?;
        if batch.cursor == start {
            return Ok(batch);
        }
        let rows = batch.records.clone();
        let end = batch.cursor.clone();
        let sink = self.sink.clone();
        let committed = self.tasks.sqlite_transaction(move |tx| {
            let changed = tx.execute("UPDATE lenso_import_sessions SET offset=?1,row=?2 WHERE queue=?3 AND id=?4 AND binding=?5 AND offset=?6 AND row=?7", (end.offset as i64,end.row as i64,&queue,&session,&binding,start.offset as i64,start.row as i64))?;
            if changed == 0 { return Ok(false); }
            sink.write(tx, &rows)?;
            Ok(true)
        }).await?;
        if !committed {
            return Err(Error::StaleCursor);
        }
        Ok(batch)
    }
    async fn read(
        &self,
        key: &str,
        etag: &str,
        size: u64,
        mut cursor: Cursor,
        cancel: &CancellationToken,
    ) -> Result<Batch, Error> {
        if size > self.plan.max_bytes {
            return Err(Error::Limit);
        }
        if cursor.offset > size {
            return Err(Error::BindingChanged);
        }
        let mut batch = Batch {
            cursor: cursor.clone(),
            records: vec![],
            errors: vec![],
            done: cursor.offset == size,
        };
        if batch.done {
            return Ok(batch);
        }
        let download = self
            .storage
            .get(
                key,
                ReadOptions {
                    if_match: Some(etag.into()),
                    range: Some(ReadRange {
                        offset: cursor.offset,
                        length: None,
                    }),
                    ..Default::default()
                },
            )
            .await?;
        let mut reader = BufReader::with_capacity(8192, download.body);
        for _ in 0..self.plan.batch_rows {
            self.check(cancel)?;
            let raw = tokio::select! {
                _ = self.stop.cancelled() => return Err(Error::Cancelled),
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                result = parser::record(&mut reader, &self.plan.format, self.plan.max_record_bytes) => result.map_err(|e| match e {
                    Error::Framing => Error::Row(RowError { row: cursor.row + 1, offset: cursor.offset, field: None, code: "csv-framing" }),
                    Error::Limit => Error::Row(RowError { row: cursor.row + 1, offset: cursor.offset, field: None, code: "record-limit" }),
                    other => other,
                })?,
            };
            let Some((bytes, consumed)) = raw else {
                return Err(Error::BindingChanged);
            };
            let row = cursor.row + 1;
            let error = |code, field| RowError {
                row,
                offset: cursor.offset,
                field,
                code,
            };
            let mapped =
                parser::map(&bytes, &self.plan).map_err(|(code, field)| error(code, field));
            let mapped = mapped.and_then(|values| {
                let record = Record {
                    row,
                    offset: cursor.offset,
                    values,
                };
                self.sink
                    .validate(&record)
                    .map_err(|code| error(code, None))?;
                Ok(record)
            });
            match mapped {
                Ok(record) => batch.records.push(record),
                Err(e) => match self.plan.invalid_rows {
                    InvalidRows::Stop => return Err(Error::Row(e)),
                    InvalidRows::Skip => batch.errors.push(e),
                },
            }
            cursor.offset += consumed;
            cursor.row = row;
            if cursor.offset == size {
                break;
            }
        }
        batch.done = cursor.offset == size;
        batch.cursor = cursor;
        Ok(batch)
    }
}
