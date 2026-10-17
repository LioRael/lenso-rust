use crate::EventError;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboxRecord {
    /// Stable within this outbox directory; destinations must deduplicate retries.
    pub id: u64,
    pub payload: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    next: u64,
    pending: BTreeMap<u64, OutboxRecord>,
}
struct State {
    lock: Option<File>,
    snapshot: Snapshot,
    failed: bool,
}
/// A bounded, exclusive single-process outbox in a trusted dedicated directory.
/// Methods perform synchronous disk I/O. Host blocking workers should own admission.
/// No automatic dispatcher, transaction with business state, or delivery guarantee.
pub struct LocalOutbox {
    root: PathBuf,
    capacity: usize,
    max_event_bytes: usize,
    state: Mutex<State>,
}
impl LocalOutbox {
    pub fn open(
        root: impl AsRef<Path>,
        capacity: usize,
        max_event_bytes: usize,
    ) -> Result<Self, EventError> {
        if capacity == 0 || max_event_bytes == 0 {
            return Err(EventError::Invalid);
        }
        // Caller provisions and owns this directory; do not create arbitrary ancestry.
        let root = fs::canonicalize(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("outbox.lock"))?;
        lock.try_lock_exclusive()?;
        let snapshot = match fs::read(root.join("outbox.json")) {
            Ok(bytes) => serde_json::from_slice::<Snapshot>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Snapshot {
                version: 1,
                next: 1,
                pending: BTreeMap::new(),
            },
            Err(error) => return Err(error.into()),
        };
        if snapshot.version != 1
            || snapshot.next == 0
            || snapshot.pending.len() > capacity
            || snapshot.pending.iter().any(|(id, record)| {
                *id == 0
                    || *id != record.id
                    || *id >= snapshot.next
                    || record.payload.len() > max_event_bytes
            })
        {
            return Err(EventError::Invalid);
        }
        Ok(Self {
            root,
            capacity,
            max_event_bytes,
            state: Mutex::new(State {
                lock: Some(lock),
                snapshot,
                failed: false,
            }),
        })
    }
    fn check(state: &State) -> Result<(), EventError> {
        if state.lock.is_none() || state.failed {
            Err(EventError::Closed)
        } else {
            Ok(())
        }
    }
    fn persist(&self, state: &mut State, next: Snapshot) -> Result<(), EventError> {
        let result = (|| -> Result<(), EventError> {
            let bytes = serde_json::to_vec(&next)?;
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(self.root.join("outbox.tmp"))?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(self.root.join("outbox.tmp"), self.root.join("outbox.json"))?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            // Rename may already have committed. Never overwrite uncertain disk state.
            state.failed = true;
            return result;
        }
        state.snapshot = next;
        Ok(())
    }
    /// Success is returned only after snapshot and directory fsync.
    pub fn enqueue(&self, payload: Vec<u8>) -> Result<u64, EventError> {
        if payload.len() > self.max_event_bytes {
            return Err(EventError::Invalid);
        }
        let mut state = self.state.lock().unwrap();
        Self::check(&state)?;
        if state.snapshot.pending.len() >= self.capacity {
            return Err(EventError::Full);
        }
        let mut next = state.snapshot.clone();
        let id = next.next;
        next.next = id.checked_add(1).ok_or(EventError::Full)?;
        next.pending.insert(id, OutboxRecord { id, payload });
        self.persist(&mut state, next)?;
        Ok(id)
    }
    /// Read without consuming. Failed or interrupted delivery leaves records pending.
    pub fn pending(&self, limit: usize) -> Result<Vec<OutboxRecord>, EventError> {
        let state = self.state.lock().unwrap();
        Self::check(&state)?;
        Ok(state
            .snapshot
            .pending
            .values()
            .take(limit)
            .cloned()
            .collect())
    }
    /// Call only after destination confirmation. Duplicate acknowledgements return false.
    pub fn acknowledge(&self, id: u64) -> Result<bool, EventError> {
        let mut state = self.state.lock().unwrap();
        Self::check(&state)?;
        let mut next = state.snapshot.clone();
        if next.pending.remove(&id).is_none() {
            return Ok(false);
        }
        self.persist(&mut state, next)?;
        Ok(true)
    }
    pub fn close(&self) {
        self.state.lock().unwrap().lock.take();
    }
}

/// Owns a local outbox; acquisition occurs only during plugin setup.
pub struct OutboxPlugin {
    pub root: PathBuf,
    pub capacity: usize,
    pub max_event_bytes: usize,
}
impl lenso_plugin_core::Plugin for OutboxPlugin {
    type Service = LocalOutbox;
    fn validate(&self) -> Result<(), lenso_plugin_core::BoxError> {
        if self.capacity == 0 || self.max_event_bytes == 0 || self.root.as_os_str().is_empty() {
            return Err(EventError::Invalid.into());
        }
        Ok(())
    }
    async fn setup(
        self,
        setup: &mut lenso_plugin_core::Setup,
    ) -> Result<std::sync::Arc<LocalOutbox>, lenso_plugin_core::BoxError> {
        let outbox = std::sync::Arc::new(LocalOutbox::open(
            self.root,
            self.capacity,
            self.max_event_bytes,
        )?);
        let cleanup = outbox.clone();
        setup.on_cleanup(
            lenso_plugin_core::CleanupPhase::Resources,
            move || async move {
                cleanup.close();
                Ok(())
            },
        );
        Ok(outbox)
    }
}
