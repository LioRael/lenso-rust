use crate::{Derived, Error, Result};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::{Arc, Mutex},
};
struct State {
    db: Connection,
    _lock: File,
}
#[derive(Clone)]
pub(crate) struct Journal(Arc<Mutex<Option<State>>>);
impl Journal {
    pub async fn open(path: PathBuf, storage: String) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            let mut lock_path = path.as_os_str().to_owned(); lock_path.push(".lock");
            let lock = OpenOptions::new().create(true).truncate(false).read(true).write(true).open(lock_path).map_err(|_| Error::Journal)?;
            lock.try_lock_exclusive().map_err(|_| Error::Busy)?;
            let db = Connection::open(path).map_err(|_| Error::Journal)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS media_owner(storage TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS media_artifacts(identity TEXT PRIMARY KEY, object_key TEXT NOT NULL UNIQUE, result TEXT);").map_err(|_| Error::Journal)?;
            let owner:Option<String> = db.query_row("SELECT storage FROM media_owner",[],|r|r.get(0)).optional().map_err(|_| Error::Journal)?;
            match owner { Some(owner) if owner != storage => return Err(Error::Invalid), None => { db.execute("INSERT INTO media_owner VALUES (?)",[storage]).map_err(|_| Error::Journal)?; }, _ => {} }
            Ok(Self(Arc::new(Mutex::new(Some(State { db,_lock:lock })))))
        }).await.map_err(|_| Error::Worker)?
    }
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = this.0.lock().map_err(|_| Error::Journal)?;
            f(&state.as_ref().ok_or(Error::Busy)?.db)
        })
        .await
        .map_err(|_| Error::Worker)?
    }
    pub async fn ready(&self, id: String) -> Result<Option<Derived>> {
        self.run(move |db| {
            let json: Option<String> = db
                .query_row(
                    "SELECT result FROM media_artifacts WHERE identity=? AND result IS NOT NULL",
                    [id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|_| Error::Journal)?;
            json.map(|s| serde_json::from_str(&s).map_err(|_| Error::Journal))
                .transpose()
        })
        .await
    }
    pub async fn begin(&self, id: String) -> Result<String> {
        self.run(move |db| {
            let key: String = db
                .query_row(
                    "SELECT 'media-derived/' || lower(hex(randomblob(24)))",
                    [],
                    |r| r.get(0),
                )
                .map_err(|_| Error::Journal)?;
            db.execute(
                "INSERT INTO media_artifacts VALUES (?,?,NULL)",
                params![id, key],
            )
            .map_err(|_| Error::Journal)?;
            Ok(key)
        })
        .await
    }
    pub async fn publish(&self, d: Derived) -> Result<()> {
        self.run(move |db| {
            let json=serde_json::to_string(&d).map_err(|_| Error::Journal)?;
            let changed=db.execute("UPDATE media_artifacts SET result=? WHERE identity=? AND object_key=? AND result IS NULL",params![json,d.identity,d.metadata.key]).map_err(|_| Error::Journal)?;
            if changed!=1 { return Err(Error::Journal); } Ok(())
        }).await
    }
    pub async fn pending(&self) -> Result<Vec<(String, String)>> {
        self.run(|db| {
            let mut stmt = db
                .prepare(
                    "SELECT identity,object_key FROM media_artifacts WHERE result IS NULL LIMIT 1",
                )
                .map_err(|_| Error::Journal)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(|_| Error::Journal)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| Error::Journal)
        })
        .await
    }
    pub async fn remove(&self, id: String) -> Result<()> {
        self.run(move |db| {
            db.execute(
                "DELETE FROM media_artifacts WHERE identity=? AND result IS NULL",
                [id],
            )
            .map_err(|_| Error::Journal)?;
            Ok(())
        })
        .await
    }
    pub async fn close(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = this.0.lock().map_err(|_| Error::Journal)?.take();
            if let Some(state) = state {
                state.db.close().map_err(|_| Error::Journal)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Worker)?
    }
}
