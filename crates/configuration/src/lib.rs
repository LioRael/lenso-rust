//! Explicit bounded JSON reloads through Core configuration, with request snapshots.
use lenso_plugin_core::{
    BoxError, CleanupPhase, Config, ConfigError, ConfigFieldSource, Plugin, Setup, WorkGate,
    WorkGuard,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    fs::OpenOptions,
    io::Read,
    ops::Deref,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Errors never retain file contents, file paths, or business validation text.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration service is closed")]
    Closed,
    #[error("configuration version conflict")]
    Conflict,
    #[error("configuration file unavailable or not a regular file")]
    File,
    #[error("configuration file exceeds byte limit")]
    TooLarge,
    #[error("configuration file is not valid JSON")]
    Json,
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("configuration failed business validation")]
    Validation,
    #[error("configuration worker failed")]
    Worker,
    #[error("configuration options are invalid")]
    Options,
}

/// One immutable resolved version. T should be plain immutable configuration data;
/// interior mutable fields would defeat the application's snapshot contract.
pub struct Snapshot<T> {
    version: u64,
    value: T,
    sources: Vec<ConfigFieldSource>,
}
impl<T> Snapshot<T> {
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn value(&self) -> &T {
        &self.value
    }
    /// File values are Core's Explicit source; environment/default sources are unchanged.
    pub fn sources(&self) -> &[ConfigFieldSource] {
        &self.sources
    }
}

/// Retain for the complete outer business request. Shutdown drains these permits.
/// Do not acquire another request permit in dependency-local calls.
pub struct RequestConfig<T> {
    snapshot: Arc<Snapshot<T>>,
    _work: WorkGuard,
}
impl<T> Deref for RequestConfig<T> {
    type Target = Snapshot<T>;
    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

type Resolver<T> = dyn Fn(Value) -> Config<T> + Send + Sync;
type Validator<T> = dyn Fn(&T) -> bool + Send + Sync;

/// Exactly one host-selected absolute path. No watching, traversal or remote source.
/// Writers should atomically replace complete files in a trusted directory.
pub struct ConfigurationPlugin<T> {
    path: PathBuf,
    max_bytes: usize,
    resolve: Arc<Resolver<T>>,
    validate: Arc<Validator<T>>,
}
impl<T: DeserializeOwned> ConfigurationPlugin<T> {
    /// The resolver must pass the supplied file JSON to Core's explicit_json layer.
    /// Configure only approved environment variables and diagnostic/sensitive paths.
    pub fn new(
        path: PathBuf,
        max_bytes: usize,
        resolve: impl Fn(Value) -> Config<T> + Send + Sync + 'static,
        validate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            path,
            max_bytes,
            resolve: Arc::new(resolve),
            validate: Arc::new(validate),
        }
    }
    fn load(&self, version: u64) -> Result<Snapshot<T>, Error> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&self.path).map_err(|_| Error::File)?;
        if !file.metadata().map_err(|_| Error::File)?.is_file() {
            return Err(Error::File);
        }
        let mut bytes = Vec::new();
        file.take(self.max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::File)?;
        if bytes.len() > self.max_bytes {
            return Err(Error::TooLarge);
        }
        let json = serde_json::from_slice(&bytes).map_err(|_| Error::Json)?;
        let (value, sources) = (self.resolve)(json).resolve_with_sources()?;
        if !(self.validate)(&value) {
            return Err(Error::Validation);
        }
        Ok(Snapshot {
            version,
            value,
            sources,
        })
    }
}
struct State<T> {
    current: Arc<Snapshot<T>>,
    closed: bool,
}

pub struct Configuration<T> {
    source: ConfigurationPlugin<T>,
    state: Mutex<State<T>>,
    gate: WorkGate,
}
impl<T: DeserializeOwned + Send + Sync + 'static> Configuration<T> {
    /// # Errors
    /// Returns `Closed` after admission is sealed or `Worker` for a poisoned state mutex.
    pub fn request(&self) -> Result<RequestConfig<T>, Error> {
        let work = self.gate.enter().map_err(|_| Error::Closed)?;
        let state = self.state.lock().map_err(|_| Error::Worker)?;
        if state.closed {
            return Err(Error::Closed);
        }
        Ok(RequestConfig {
            snapshot: state.current.clone(),
            _work: work,
        })
    }
    /// Compare-and-swap against the caller's observed version. Concurrent reloads
    /// from that version have at most one winner. Cancellation of this waiter does
    /// not cancel accepted work; obtain a fresh request to learn the current version.
    ///
    /// # Errors
    /// Returns revision conflict, file/JSON/Core configuration, business validation,
    /// lifecycle or worker errors. Failed validation never replaces the old snapshot.
    pub async fn reload(self: &Arc<Self>, expected: u64) -> Result<u64, Error> {
        let work = self.gate.enter().map_err(|_| Error::Closed)?;
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let _work = work;
            {
                let state = service.state.lock().map_err(|_| Error::Worker)?;
                if state.closed {
                    return Err(Error::Closed);
                }
                if state.current.version != expected {
                    return Err(Error::Conflict);
                }
            }
            let version = expected.checked_add(1).ok_or(Error::Conflict)?;
            let next = Arc::new(service.source.load(version)?);
            let mut state = service.state.lock().map_err(|_| Error::Worker)?;
            if state.closed {
                return Err(Error::Closed);
            }
            if state.current.version != expected {
                return Err(Error::Conflict);
            }
            state.current = next;
            Ok(version)
        })
        .await
        .map_err(|_| Error::Worker)?
    }
}
impl<T: DeserializeOwned + Send + Sync + 'static> Plugin for ConfigurationPlugin<T> {
    type Service = Configuration<T>;
    fn validate(&self) -> Result<(), BoxError> {
        if !self.path.is_absolute() || self.max_bytes == 0 || self.max_bytes > 16 * 1024 * 1024 {
            return Err(Error::Options.into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let (source, current) = tokio::task::spawn_blocking(move || {
            let current = self.load(1)?;
            Ok::<_, Error>((self, Arc::new(current)))
        })
        .await
        .map_err(|_| Error::Worker)??;
        let service = Arc::new(Configuration {
            source,
            state: Mutex::new(State {
                current,
                closed: false,
            }),
            gate: setup.work_gate(),
        });
        let closing = service.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            closing.state.lock().map_err(|_| Error::Worker)?.closed = true;
            Ok(())
        });
        Ok(service)
    }
}
