#![doc = include_str!("../README.md")]

use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Sanitized failures; never include SQL, paths or indexed content.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SearchError {
    #[error("invalid search input")]
    InvalidInput,
    #[error("invalid or stale cursor")]
    InvalidCursor,
    #[error("search service closed")]
    Closed,
    #[error("SQLite FTS5 unavailable")]
    Fts5Unavailable,
    #[error("search database must be provisioned with schema version 1")]
    NotProvisioned,
    #[error("search storage failure")]
    Storage,
}
type Result<T> = std::result::Result<T, SearchError>;
fn db<T>(r: rusqlite::Result<T>) -> Result<T> {
    r.map_err(|_| SearchError::Storage)
}

/// Exact partition. The host must derive this from current authorization on EVERY call.
/// This value and cursors are not credentials.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Scope {
    pub namespace: String,
    pub tenant: String,
    pub owner: String,
}
impl Scope {
    pub fn new(namespace: &str, tenant: &str, owner: &str) -> Self {
        Self {
            namespace: namespace.into(),
            tenant: tenant.into(),
            owner: owner.into(),
        }
    }
    fn validate(&self) -> Result<()> {
        name(&self.namespace)?;
        name(&self.tenant)?;
        name(&self.owner)
    }
}
fn name(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
        Err(SearchError::InvalidInput)
    } else {
        Ok(())
    }
}
/// A projection of an authoritative note, keyed within its exact scope.
pub struct Note<'a> {
    pub id: &'a str,
    pub title: &'a str,
    pub body: &'a str,
}
#[derive(Debug)]
pub struct Hit {
    pub id: String,
    pub title: String,
    pub summary: String,
}
#[derive(Debug)]
pub struct Page {
    pub hits: Vec<Hit>,
    pub next_cursor: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    scope: Scope,
    query: String,
    revision: i64,
    offset: usize,
    limit: usize,
}

fn fts(conn: &Connection) -> Result<()> {
    let enabled: i64 = db(conn.query_row(
        "SELECT sqlite_compileoption_used('ENABLE_FTS5')",
        [],
        |r| r.get(0),
    ))?;
    if enabled == 0 {
        return Err(SearchError::Fts5Unavailable);
    }
    Ok(())
}
fn configure(conn: &Connection) -> Result<()> {
    db(conn.busy_timeout(Duration::from_secs(2)))?;
    fts(conn)
}

/// Explicit DDL step on a host-selected application database. Never called by startup.
pub fn provision(path: impl AsRef<Path>) -> Result<()> {
    let mut conn = db(Connection::open(path))?;
    configure(&conn)?;
    let tx = db(conn.transaction())?;
    db(tx.execute_batch("CREATE TABLE IF NOT EXISTS lenso_search_meta(version INTEGER NOT NULL, revision INTEGER NOT NULL);
      INSERT INTO lenso_search_meta SELECT 1,0 WHERE NOT EXISTS(SELECT 1 FROM lenso_search_meta);
      CREATE TABLE IF NOT EXISTS lenso_search_notes(rowid INTEGER PRIMARY KEY, namespace TEXT NOT NULL, tenant TEXT NOT NULL, owner TEXT NOT NULL, id TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, UNIQUE(namespace,tenant,owner,id));
      CREATE VIRTUAL TABLE IF NOT EXISTS lenso_search_fts USING fts5(title,body,content='lenso_search_notes',content_rowid='rowid',tokenize='unicode61');
      CREATE TRIGGER IF NOT EXISTS lenso_search_ai AFTER INSERT ON lenso_search_notes BEGIN
        INSERT INTO lenso_search_fts(rowid,title,body) VALUES(new.rowid,new.title,new.body);
        UPDATE lenso_search_meta SET revision=revision+1; END;
      CREATE TRIGGER IF NOT EXISTS lenso_search_ad AFTER DELETE ON lenso_search_notes BEGIN
        INSERT INTO lenso_search_fts(lenso_search_fts,rowid,title,body) VALUES('delete',old.rowid,old.title,old.body);
        UPDATE lenso_search_meta SET revision=revision+1; END;
      CREATE TRIGGER IF NOT EXISTS lenso_search_au AFTER UPDATE ON lenso_search_notes BEGIN
        INSERT INTO lenso_search_fts(lenso_search_fts,rowid,title,body) VALUES('delete',old.rowid,old.title,old.body);
        INSERT INTO lenso_search_fts(rowid,title,body) VALUES(new.rowid,new.title,new.body);
        UPDATE lenso_search_meta SET revision=revision+1; END;"))?;
    db(tx.commit())
}

/// Synchronous, serialized SQLite service. Use on a blocking thread for large workloads.
/// Operations hold admission and the connection until all I/O settles.
pub struct Search {
    connection: Mutex<Option<Connection>>,
    gate: WorkGate,
}
impl Search {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_gate(path.as_ref(), WorkGate::default())
    }
    fn open_with_gate(path: &Path, gate: WorkGate) -> Result<Self> {
        let conn = db(Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        ))?;
        configure(&conn)?;
        let version: i64 = conn
            .query_row("SELECT version FROM lenso_search_meta", [], |r| r.get(0))
            .map_err(|_| SearchError::NotProvisioned)?;
        if version != 1 {
            return Err(SearchError::NotProvisioned);
        }
        conn.prepare("SELECT rowid FROM lenso_search_fts LIMIT 0")
            .map_err(|_| SearchError::NotProvisioned)?;
        Ok(Self {
            connection: Mutex::new(Some(conn)),
            gate,
        })
    }
    fn with<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let _work = self.gate.enter().map_err(|_| SearchError::Closed)?;
        let mut lock = self.connection.lock().map_err(|_| SearchError::Storage)?;
        f(lock.as_mut().ok_or(SearchError::Closed)?)
    }
    /// Writes only this partition. On ownership changes the host must delete the OLD scope too.
    pub fn upsert(&self, scope: &Scope, note: Note<'_>) -> Result<()> {
        self.with(|conn| {
            scope.validate()?; name(note.id)?;
            if note.title.len() > 4096 || note.body.len() > 1_048_576 || note.title.contains('\0') || note.body.contains('\0') { return Err(SearchError::InvalidInput); }
            db(conn.execute("INSERT INTO lenso_search_notes(namespace,tenant,owner,id,title,body) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(namespace,tenant,owner,id) DO UPDATE SET title=excluded.title,body=excluded.body", params![scope.namespace,scope.tenant,scope.owner,note.id,note.title,note.body]))?;
            Ok(())
        })
    }
    pub fn delete(&self, scope: &Scope, id: &str) -> Result<bool> {
        self.with(|conn| {
            scope.validate()?; name(id)?;
            Ok(db(conn.execute("DELETE FROM lenso_search_notes WHERE namespace=?1 AND tenant=?2 AND owner=?3 AND id=?4", params![scope.namespace,scope.tenant,scope.owner,id]))? > 0)
        })
    }
    /// All whitespace-separated terms must match (literal FTS phrases; no query operators).
    /// Title substring matches sort first, then binary note ID; no cross-scope BM25 statistics.
    /// Pagination is capped at 10,000 results and invalidated by ANY database mutation.
    pub fn query(
        &self,
        scope: &Scope,
        query: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page> {
        self.with(|conn| {
            scope.validate()?;
            let query = query.trim();
            if query.is_empty() || query.len() > 1024 || query.chars().any(char::is_control) || !(1..=100).contains(&limit) { return Err(SearchError::InvalidInput); }
            let terms: Vec<_> = query.split_whitespace().collect();
            if terms.len() > 32 || terms.iter().any(|s| !s.chars().any(char::is_alphanumeric)) { return Err(SearchError::InvalidInput); }
            let expression = terms.iter().map(|s| format!("\"{}\"",s.replace('"', "\"\""))).collect::<Vec<_>>().join(" AND ");
            let tx = db(conn.transaction())?;
            let revision: i64 = db(tx.query_row("SELECT revision FROM lenso_search_meta", [], |r| r.get(0)))?;
            let offset = if let Some(raw) = cursor {
                if raw.len() > 8192 { return Err(SearchError::InvalidCursor); }
                let c: Cursor = serde_json::from_str(raw).map_err(|_| SearchError::InvalidCursor)?;
                if c.version != 1 || c.scope != *scope || c.query != query || c.revision != revision || c.limit != limit || c.offset == 0 || c.offset >= 10000 { return Err(SearchError::InvalidCursor); }
                c.offset
            } else { 0 };
            let take = limit.min(10000 - offset);
            let mut stmt = db(tx.prepare("SELECT n.id,n.title,snippet(lenso_search_fts,1,'','',' … ',24) FROM lenso_search_fts JOIN lenso_search_notes n ON n.rowid=lenso_search_fts.rowid WHERE lenso_search_fts MATCH ?1 AND n.namespace=?2 AND n.tenant=?3 AND n.owner=?4 ORDER BY (instr(lower(n.title),lower(?5))>0) DESC,n.id COLLATE BINARY ASC LIMIT ?6 OFFSET ?7"))?;
            let rows = db(stmt.query_map(params![expression,scope.namespace,scope.tenant,scope.owner,query,(take+1) as i64,offset as i64], |r| Ok(Hit { id:r.get(0)?,title:r.get(1)?,summary:r.get::<_,String>(2)?.chars().take(512).collect() })))?;
            let mut hits = db(rows.collect::<rusqlite::Result<Vec<_>>>())?;
            let more = hits.len() > take && offset + take < 10000;
            hits.truncate(take);
            let next_cursor = if more { Some(serde_json::to_string(&Cursor { version:1,scope:scope.clone(),query:query.into(),revision,offset:offset+take,limit }).map_err(|_| SearchError::Storage)?) } else { None };
            drop(stmt); db(tx.commit())?;
            Ok(Page { hits,next_cursor })
        })
    }
    /// Waits for synchronous operations, then closes; retained services reject further work.
    pub fn close(&self) -> Result<()> {
        let mut lock = self.connection.lock().map_err(|_| SearchError::Storage)?;
        if let Some(conn) = lock.take() {
            conn.close().map_err(|_| SearchError::Storage)?;
        }
        Ok(())
    }
}
pub struct SearchPlugin {
    path: std::path::PathBuf,
}
impl SearchPlugin {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
impl Plugin for SearchPlugin {
    type Service = Search;
    fn validate(&self) -> std::result::Result<(), BoxError> {
        if self.path.as_os_str().is_empty() {
            return Err(SearchError::InvalidInput.into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> std::result::Result<Arc<Search>, BoxError> {
        let search = Arc::new(Search::open_with_gate(&self.path, setup.work_gate())?);
        let cleanup = search.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().map_err(Into::into)
        });
        Ok(search)
    }
}
