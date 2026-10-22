//! Bounded typed process-local cache with owned, coalesced Tokio loaders.
#![doc = include_str!("../README.md")]

use futures_util::FutureExt;
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    future::Future,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::watch,
    task::{AbortHandle, JoinHandle},
    time::Instant,
};

/// Finite storage and task limits. Namespace is not authorization.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Root path label; separate constructors still have separate storage.
    pub namespace: String,
    /// Applied when a method receives no TTL; zero disables storage.
    pub default_ttl: Duration,
    /// Highest accepted TTL, at most one day.
    pub max_ttl: Duration,
    /// Entry count across all scopes in the family.
    pub max_entries: usize,
    /// Maximum JSON bytes emitted for one value.
    pub max_value_bytes: usize,
    /// Total stored JSON, key and path UTF-8 bytes.
    pub max_bytes: usize,
    /// Maximum unfinished loader tasks, including fenced tasks.
    pub max_in_flight: usize,
    /// Total join budget after aborting all loaders.
    pub close_timeout: Duration,
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            namespace: "cache".into(),
            default_ttl: Duration::from_secs(60),
            max_ttl: Duration::from_secs(86400),
            max_entries: 1000,
            max_value_bytes: 65536,
            max_bytes: 8 * 1024 * 1024,
            max_in_flight: 128,
            close_timeout: Duration::from_secs(5),
        }
    }
}
impl CacheConfig {
    /// Validate without acquiring resources.
    pub fn validate(&self) -> Result<(), CacheError> {
        name(&self.namespace)?;
        if self.max_ttl.is_zero()
            || self.max_ttl > Duration::from_secs(86400)
            || self.default_ttl > self.max_ttl
            || self.max_entries == 0
            || self.max_value_bytes == 0
            || self.max_bytes == 0
            || self.max_in_flight == 0
            || self.close_timeout.is_zero()
        {
            return Err(CacheError::InvalidInput);
        }
        Ok(())
    }
}
/// Sanitized public errors. Loader causes require explicit access.
#[derive(Clone)]
pub enum CacheError {
    /// Invalid names, limits, batch size or TTL.
    InvalidInput,
    /// JSON conversion failed or exceeded byte limits.
    Serialization,
    /// No loader task capacity remains.
    Busy,
    /// The family no longer admits calls.
    Closed,
    /// The loader completion channel became unavailable.
    Aborted,
    /// Original loader error; formatting and automatic source traversal omit it.
    Loader(Arc<dyn Error + Send + Sync>),
    /// The loader or its serialization panicked.
    LoaderPanicked,
    /// Some aborted tasks did not finish in the configured join budget.
    CloseTimeout,
    /// Internal synchronization became unavailable.
    Internal,
}
impl CacheError {
    /// Not exposed through Debug, Display, or Error::source.
    pub fn loader_cause(&self) -> Option<&(dyn Error + Send + Sync)> {
        match self {
            Self::Loader(cause) => Some(cause.as_ref()),
            _ => None,
        }
    }
    fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid-input",
            Self::Serialization => "serialization",
            Self::Busy => "busy",
            Self::Closed => "closed",
            Self::Aborted => "aborted",
            Self::Loader(_) => "loader",
            Self::LoaderPanicked => "loader-panicked",
            Self::CloseTimeout => "close-timeout",
            Self::Internal => "internal",
        }
    }
}
impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cache operation failed ({})", self.code())
    }
}
impl fmt::Debug for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl Error for CacheError {}
/// Whether explicit storage was enabled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The serialized value was stored.
    Stored,
    /// Zero TTL removed any old entry.
    Skipped,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct Address {
    path: Vec<String>,
    key: String,
}
struct Entry {
    bytes: Arc<Vec<u8>>,
    expires: Instant,
    size: usize,
}
type LoadResult = Result<Arc<Vec<u8>>, CacheError>;
struct Flight {
    writable: AtomicBool,
    waiters: AtomicUsize,
    result: watch::Sender<Option<LoadResult>>,
    abort: AbortHandle,
}
struct OwnedTask {
    flight: Arc<Flight>,
    handle: JoinHandle<()>,
}
struct State {
    closed: bool,
    entries: HashMap<Address, Entry>,
    fifo: VecDeque<Address>,
    bytes: usize,
    flights: HashMap<Address, Arc<Flight>>,
    tasks: Vec<OwnedTask>,
}
impl State {
    fn remove(&mut self, address: &Address) {
        if let Some(entry) = self.entries.remove(address) {
            self.bytes -= entry.size;
        }
        self.fifo.retain(|a| a != address);
    }
    fn prune(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| {
            if entry.expires <= now {
                self.bytes -= entry.size;
                false
            } else {
                true
            }
        });
        self.fifo.retain(|a| self.entries.contains_key(a));
        self.tasks.retain(|t| !t.handle.is_finished());
        self.flights
            .retain(|_, f| f.writable.load(Ordering::SeqCst));
    }
    fn fence(&mut self, address: &Address) {
        if let Some(f) = self.flights.remove(address) {
            f.writable.store(false, Ordering::SeqCst);
        }
    }
    fn store(
        &mut self,
        address: Address,
        bytes: Arc<Vec<u8>>,
        expires: Instant,
        config: &CacheConfig,
    ) {
        self.remove(&address);
        let size =
            bytes.len() + address.key.len() + address.path.iter().map(String::len).sum::<usize>();
        while self.entries.len() >= config.max_entries || self.bytes + size > config.max_bytes {
            if let Some(oldest) = self.fifo.pop_front() {
                self.remove(&oldest);
            } else {
                break;
            }
        }
        self.bytes += size;
        self.fifo.push_back(address.clone());
        self.entries.insert(
            address,
            Entry {
                bytes,
                expires,
                size,
            },
        );
    }
}
struct Inner {
    config: CacheConfig,
    state: Mutex<State>,
    close: watch::Sender<Option<Result<(), CacheError>>>,
}
/// Cloneable view of an isolated family. Scope paths are structured.
pub struct MemoryCache<V> {
    inner: Arc<Inner>,
    path: Vec<String>,
    marker: PhantomData<fn() -> V>,
}
impl<V> Clone for MemoryCache<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            path: self.path.clone(),
            marker: PhantomData,
        }
    }
}
fn name(value: &str) -> Result<(), CacheError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(|c| c.is_control()) {
        Err(CacheError::InvalidInput)
    } else {
        Ok(())
    }
}
struct Waiter {
    flight: Arc<Flight>,
    inner: Arc<Inner>,
}
impl Drop for Waiter {
    fn drop(&mut self) {
        // Admission and last-waiter fencing must have the same linearization lock.
        let _state = self.inner.state.lock();
        if self.flight.waiters.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.flight.writable.store(false, Ordering::SeqCst);
            self.flight.abort.abort();
        }
    }
}
impl<V: Serialize + DeserializeOwned + Send + Sync + 'static> MemoryCache<V> {
    /// Creates an isolated backend. No work is spawned until loading or closing.
    pub fn new(config: CacheConfig) -> Result<Self, CacheError> {
        config.validate()?;
        let path = vec![config.namespace.clone()];
        let (close, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State {
                    closed: false,
                    entries: HashMap::new(),
                    fifo: VecDeque::new(),
                    bytes: 0,
                    flights: HashMap::new(),
                    tasks: Vec::new(),
                }),
                close,
            }),
            path,
            marker: PhantomData,
        })
    }
    fn address(&self, key: &str) -> Result<Address, CacheError> {
        name(key)?;
        Ok(Address {
            path: self.path.clone(),
            key: key.into(),
        })
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>, CacheError> {
        let mut state = self.inner.state.lock().map_err(|_| CacheError::Internal)?;
        if state.closed {
            return Err(CacheError::Closed);
        }
        state.prune();
        Ok(state)
    }
    fn ttl(&self, ttl: Option<Duration>) -> Result<Duration, CacheError> {
        let ttl = ttl.unwrap_or(self.inner.config.default_ttl);
        if ttl > self.inner.config.max_ttl {
            return Err(CacheError::InvalidInput);
        }
        Ok(ttl)
    }
    fn encode(&self, address: &Address, value: &V) -> Result<Arc<Vec<u8>>, CacheError> {
        // Bound the writer, not just the final allocation after serialization.
        let mut writer = LimitedWriter {
            bytes: Vec::new(),
            limit: self.inner.config.max_value_bytes,
        };
        serde_json::to_writer(&mut writer, value).map_err(|_| CacheError::Serialization)?;
        let size = writer.bytes.len()
            + address.key.len()
            + address.path.iter().map(String::len).sum::<usize>();
        if size > self.inner.config.max_bytes {
            return Err(CacheError::Serialization);
        }
        Ok(Arc::new(writer.bytes))
    }
    /// Hit/miss is separate from V; cached Option::None is Some(None).
    pub fn get(&self, key: &str) -> Result<Option<V>, CacheError> {
        let address = self.address(key)?;
        let bytes = self.state()?.entries.get(&address).map(|e| e.bytes.clone());
        bytes
            .map(|b| serde_json::from_slice(&b).map_err(|_| CacheError::Serialization))
            .transpose()
    }
    /// Ordered per-key reads, duplicates preserved, at most 100 keys.
    pub fn get_many(&self, keys: &[&str]) -> Result<Vec<Option<V>>, CacheError> {
        if keys.len() > 100 {
            return Err(CacheError::InvalidInput);
        }
        drop(self.state()?);
        keys.iter().map(|key| self.get(key)).collect()
    }
    /// Stores a serialized copy; zero TTL deletes and fences pending writeback.
    pub fn set(
        &self,
        key: &str,
        value: V,
        ttl: Option<Duration>,
    ) -> Result<WriteOutcome, CacheError> {
        let address = self.address(key)?;
        let ttl = self.ttl(ttl)?;
        if ttl.is_zero() {
            let mut state = self.state()?;
            state.fence(&address);
            state.remove(&address);
            return Ok(WriteOutcome::Skipped);
        }
        drop(self.state()?);
        let bytes = self.encode(&address, &value)?;
        let mut state = self.state()?;
        state.fence(&address);
        state.store(address, bytes, Instant::now() + ttl, &self.inner.config);
        Ok(WriteOutcome::Stored)
    }
    /// Deletes only this key and fences old loaders.
    pub fn delete(&self, key: &str) -> Result<(), CacheError> {
        let address = self.address(key)?;
        let mut state = self.state()?;
        state.fence(&address);
        state.remove(&address);
        Ok(())
    }
    /// Invalidates this exact scope, not its ancestors or descendants.
    pub fn invalidate(&self) -> Result<(), CacheError> {
        let mut state = self.state()?;
        let addresses: Vec<_> = state
            .entries
            .keys()
            .chain(state.flights.keys())
            .filter(|a| a.path == self.path)
            .cloned()
            .collect();
        for a in addresses {
            state.fence(&a);
            state.remove(&a);
        }
        Ok(())
    }
    /// Adds a validated segment, with at most 16 total segments.
    pub fn scope(&self, segment: &str) -> Result<Self, CacheError> {
        name(segment)?;
        drop(self.state()?);
        if self.path.len() >= 16 {
            return Err(CacheError::InvalidInput);
        }
        let mut scoped = self.clone();
        scoped.path.push(segment.into());
        Ok(scoped)
    }
    /// First caller chooses loader/TTL. Dropping a waiter cannot cancel peers.
    /// Last waiter cancellation aborts the loader at its next yield point.
    pub async fn get_or_set<F, Fut, E>(
        &self,
        key: &str,
        loader: F,
        ttl: Option<Duration>,
    ) -> Result<V, CacheError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        let address = self.address(key)?;
        let ttl = self.ttl(ttl)?;
        let (flight, mut result) = {
            let mut state = self.state()?;
            if let Some(flight) = state.flights.get(&address) {
                flight.waiters.fetch_add(1, Ordering::SeqCst);
                (flight.clone(), flight.result.subscribe())
            } else {
                if !ttl.is_zero() {
                    if let Some(entry) = state.entries.get(&address) {
                        let bytes = entry.bytes.clone();
                        drop(state);
                        return serde_json::from_slice(&bytes)
                            .map_err(|_| CacheError::Serialization);
                    }
                }
                if state.tasks.len() >= self.inner.config.max_in_flight {
                    return Err(CacheError::Busy);
                }
                if ttl.is_zero() {
                    state.remove(&address);
                }
                let (sender, receiver) = watch::channel(None);
                let (start, started) = tokio::sync::oneshot::channel::<Arc<Flight>>();
                let cache = self.clone();
                let task_address = address.clone();
                let task = tokio::spawn(async move {
                    let Ok(flight) = started.await else { return };
                    let outcome = AssertUnwindSafe(async {
                        let value = loader()
                            .await
                            .map_err(|e| CacheError::Loader(Arc::new(e)))?;
                        cache.encode(&task_address, &value)
                    })
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(CacheError::LoaderPanicked));
                    if let Ok(mut state) = cache.inner.state.lock() {
                        if !state.closed && flight.writable.load(Ordering::SeqCst) {
                            if let Ok(bytes) = &outcome {
                                if !ttl.is_zero() {
                                    state.prune();
                                    state.store(
                                        task_address.clone(),
                                        bytes.clone(),
                                        Instant::now() + ttl,
                                        &cache.inner.config,
                                    );
                                }
                            }
                        }
                        if state
                            .flights
                            .get(&task_address)
                            .is_some_and(|f| Arc::ptr_eq(f, &flight))
                        {
                            state.flights.remove(&task_address);
                        }
                        if !state.closed {
                            flight.result.send_replace(Some(outcome));
                        }
                    } else {
                        flight.result.send_replace(Some(Err(CacheError::Internal)));
                    }
                });
                let flight = Arc::new(Flight {
                    writable: AtomicBool::new(true),
                    waiters: AtomicUsize::new(1),
                    result: sender,
                    abort: task.abort_handle(),
                });
                state.flights.insert(address, flight.clone());
                state.tasks.push(OwnedTask {
                    flight: flight.clone(),
                    handle: task,
                });
                let _ = start.send(flight.clone());
                (flight, receiver)
            }
        };
        let _waiter = Waiter {
            flight,
            inner: self.inner.clone(),
        };
        loop {
            // User deserialization may reenter the cache; never retain a watch read lock.
            let outcome = result.borrow_and_update().clone();
            if let Some(outcome) = outcome {
                return serde_json::from_slice(&outcome?).map_err(|_| CacheError::Serialization);
            }
            result.changed().await.map_err(|_| CacheError::Aborted)?;
        }
    }
    /// Seals all views, aborts loaders, then joins within one total timeout.
    /// Once started, close continues even if this close waiter is dropped.
    pub async fn close(&self) -> Result<(), CacheError> {
        let mut completion = self.inner.close.subscribe();
        {
            let mut state = self.inner.state.lock().map_err(|_| CacheError::Internal)?;
            if !state.closed {
                state.closed = true;
                for task in &state.tasks {
                    let flight = &task.flight;
                    flight.writable.store(false, Ordering::SeqCst);
                    flight.result.send_replace(Some(Err(CacheError::Closed)));
                }
                state.flights.clear();
                state.entries.clear();
                state.fifo.clear();
                state.bytes = 0;
                let tasks = std::mem::take(&mut state.tasks);
                for task in &tasks {
                    task.handle.abort();
                }
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    let joined = tokio::time::timeout(inner.config.close_timeout, async move {
                        for task in tasks {
                            let _ = task.handle.await;
                        }
                    })
                    .await;
                    inner
                        .close
                        .send_replace(Some(joined.map_err(|_| CacheError::CloseTimeout)));
                });
            }
        }
        loop {
            if let Some(outcome) = completion.borrow_and_update().clone() {
                return outcome;
            }
            completion
                .changed()
                .await
                .map_err(|_| CacheError::Internal)?;
        }
    }
}
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl std::io::Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("cache serialization limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Owned memory cache plugin. Resources cleanup follows global work drain.
pub struct CachePlugin<V> {
    config: CacheConfig,
    marker: PhantomData<fn() -> V>,
}
impl<V> CachePlugin<V> {
    /// Declare an owned cache instance without acquiring resources.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            marker: PhantomData,
        }
    }
}
impl<V: Serialize + DeserializeOwned + Send + Sync + 'static> Plugin for CachePlugin<V> {
    type Service = MemoryCache<V>;
    fn validate(&self) -> Result<(), BoxError> {
        self.config.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let cache = Arc::new(MemoryCache::new(self.config)?);
        let cleanup = cache.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().await.map_err(Into::into)
        });
        Ok(cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn churn_prunes_expired_entries_and_retains_no_scope_or_generation_tombstones() {
        let cache = MemoryCache::<String>::new(CacheConfig {
            max_entries: 3,
            ..CacheConfig::default()
        })
        .unwrap();
        for index in 0..5000 {
            let view = cache.scope(&format!("scope-{index}")).unwrap();
            view.set("k", "v".into(), Some(Duration::from_secs(1)))
                .unwrap();
            if index % 2 == 0 {
                view.invalidate().unwrap();
            }
            let state = cache.inner.state.lock().unwrap();
            assert!(state.entries.len() <= 3);
            assert_eq!(state.entries.len(), state.fifo.len());
            assert!(state.flights.is_empty());
            assert!(state.tasks.is_empty());
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        cache.get("unrelated").unwrap();
        let state = cache.inner.state.lock().unwrap();
        assert!(state.entries.is_empty());
        assert!(state.fifo.is_empty());
        assert_eq!(state.bytes, 0);
    }
}
