use crate::{body::DownloadBody, error::Result, types::validate_content_type, *};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File as StdFile, Metadata, OpenOptions},
    future::Future,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tokio_util::task::TaskTracker;

const METADATA_LIMIT: usize = 64 * 1024;
const CHUNK: usize = 64 * 1024;

/// A trusted, dedicated Unix directory. Missing ancestry is created without
/// following symlinks. Configure a global upload limit here when inputs are untrusted.
#[derive(Clone, Debug)]
pub struct LocalStorageConfig {
    pub root: PathBuf,
    pub max_upload_bytes: Option<u64>,
}
impl LocalStorageConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_upload_bytes: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if !cfg!(unix) {
            return Err(StorageError::new(ErrorCode::Unsupported));
        }
        if self.root.as_os_str().is_empty()
            || self
                .root
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        {
            return Err(StorageError::new(ErrorCode::InvalidInput));
        }
        Ok(())
    }
}

struct Inner {
    config: LocalStorageConfig,
    identity: Metadata,
    lifetime: CancellationToken,
    tasks: TaskTracker,
    closing: Mutex<bool>,
    cleanup_failed: AtomicBool,
}

/// Cloneable standalone service; clones share close state, not object namespaces.
/// Call `close().await` while the Tokio runtime is alive. Drop alone is not shutdown.
#[derive(Clone)]
pub struct LocalStorage {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub(crate) struct Signals {
    lifetime: CancellationToken,
    caller: CancellationToken,
    waiter: CancellationToken,
}
impl Signals {
    pub(crate) fn check(&self) -> Result<()> {
        if self.lifetime.is_cancelled() || self.caller.is_cancelled() || self.waiter.is_cancelled()
        {
            Err(StorageError::new(ErrorCode::Aborted))
        } else {
            Ok(())
        }
    }
    pub(crate) async fn cancelled(&self) {
        tokio::select! {
            _ = self.lifetime.cancelled() => (),
            _ = self.caller.cancelled() => (),
            _ = self.waiter.cancelled() => (),
        }
    }
}

async fn blocking<T: Send + 'static>(
    action: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(action)
        .await
        .map_err(|error| StorageError::caused(ErrorCode::Provider, error))?
}

impl LocalStorage {
    pub async fn open(mut config: LocalStorageConfig) -> Result<Self> {
        config.validate()?;
        if config.root.is_relative() {
            config.root = std::env::current_dir()
                .map_err(StorageError::io)?
                .join(&config.root);
        }
        let root = config.root.clone();
        let identity = blocking(move || {
            create_root(&root)?;
            directory(&root)
        })
        .await?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                identity,
                lifetime: CancellationToken::new(),
                tasks: TaskTracker::new(),
                closing: Mutex::new(false),
                cleanup_failed: AtomicBool::new(false),
            }),
        })
    }

    pub fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            range_read: true,
            conditional_read: true,
            signed_upload: false,
            signed_download: false,
            upload_requires_size: false,
            max_upload_bytes: self.inner.config.max_upload_bytes,
        }
    }

    /// Seals this service, cancels owned operations/downloads, then waits for cleanup.
    /// Concurrent/repeated close calls are supported; cancelling a waiter does not
    /// cancel cleanup. Committed objects and other instances' temporary files remain.
    pub async fn close(&self) -> Result<()> {
        {
            let mut closing = self.inner.closing.lock().expect("storage lifecycle mutex");
            *closing = true;
            self.inner.lifetime.cancel();
            self.inner.tasks.close();
        }
        self.inner.tasks.wait().await;
        if self.inner.cleanup_failed.load(Ordering::Acquire) {
            Err(StorageError::new(ErrorCode::Provider))
        } else {
            Ok(())
        }
    }

    async fn run<T, F, Fut>(&self, cancellation: Option<CancellationToken>, action: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Inner>, Signals) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let waiter = CancellationToken::new();
        let guard = waiter.clone().drop_guard();
        let signals = Signals {
            lifetime: self.inner.lifetime.clone(),
            caller: cancellation.unwrap_or_default(),
            waiter,
        };
        let task = {
            let closing = self.inner.closing.lock().expect("storage lifecycle mutex");
            if *closing {
                return Err(StorageError::new(ErrorCode::Aborted));
            }
            signals.check()?;
            let inner = self.inner.clone();
            self.inner
                .tasks
                .spawn(async move { action(inner, signals).await })
        };
        let result = task
            .await
            .map_err(|error| StorageError::caused(ErrorCode::Provider, error))?;
        guard.disarm();
        result
    }

    pub async fn put(&self, input: PutObject) -> Result<ObjectMetadata> {
        self.run(
            input.cancellation.clone(),
            move |inner, signals| async move { put(inner, input, signals).await },
        )
        .await
    }

    pub async fn get(&self, key: &str, options: ReadOptions) -> Result<ObjectDownload> {
        let key = key.to_owned();
        self.run(
            options.cancellation.clone(),
            move |inner, signals| async move {
                validate_key(&key)?;
                if options.range.is_some_and(|range| range.length == Some(0)) {
                    return Err(StorageError::new(ErrorCode::InvalidInput));
                }
                let lock = lock_root(&inner, &signals).await?;
                let root = inner.config.root.clone();
                let (metadata, payload) = blocking(move || {
                    let _lock = lock;
                    load(&root, &object_name(&key), Some(&key))
                })
                .await?;
                if options
                    .if_match
                    .as_ref()
                    .is_some_and(|etag| etag != &metadata.etag)
                {
                    return Err(StorageError::new(ErrorCode::Conflict));
                }
                let offset = options.range.map_or(0, |range| range.offset);
                if options.range.is_some() && offset >= metadata.size {
                    return Err(StorageError::new(ErrorCode::InvalidInput));
                }
                let length = options
                    .range
                    .and_then(|range| range.length)
                    .unwrap_or(metadata.size - offset)
                    .min(metadata.size - offset);
                let mut payload = File::from_std(payload);
                payload
                    .seek(io::SeekFrom::Start(offset))
                    .await
                    .map_err(StorageError::io)?;
                signals.check()?;
                let body = DownloadBody::new(payload, length, signals, &inner.tasks);
                Ok(ObjectDownload {
                    metadata,
                    body,
                    range: options.range.map(|_| ResolvedRange { offset, length }),
                })
            },
        )
        .await
    }

    pub async fn head(
        &self,
        key: &str,
        options: OperationOptions,
    ) -> Result<Option<ObjectMetadata>> {
        let key = key.to_owned();
        self.run(options.cancellation, move |inner, signals| async move {
            validate_key(&key)?;
            let lock = lock_root(&inner, &signals).await?;
            let root = inner.config.root.clone();
            blocking(move || {
                let _lock = lock;
                signals.check()?;
                match load(&root, &object_name(&key), Some(&key)) {
                    Ok((metadata, _)) => Ok(Some(metadata)),
                    Err(error) if error.code() == ErrorCode::NotFound => Ok(None),
                    Err(error) => Err(error),
                }
            })
            .await
        })
        .await
    }

    pub async fn delete(&self, key: &str, options: OperationOptions) -> Result<DeleteOutcome> {
        let key = key.to_owned();
        self.run(options.cancellation, move |inner, signals| async move {
            validate_key(&key)?;
            let lock = lock_root(&inner, &signals).await?;
            let root = inner.config.root.clone();
            blocking(move || {
                let _lock = lock;
                let name = object_name(&key);
                let tomb = root.join(format!(".delete-{name}"));
                let recovered = remove_tomb(&tomb, &signals)?;
                match load(&root, &name, Some(&key)) {
                    Ok(_) => (),
                    Err(error) if error.code() == ErrorCode::NotFound => {
                        return Ok(if recovered {
                            DeleteOutcome::Deleted
                        } else {
                            DeleteOutcome::NotFound
                        })
                    }
                    Err(error) => return Err(error),
                }
                signals.check()?;
                fs::rename(root.join(name), &tomb).map_err(StorageError::io)?;
                remove_tomb(&tomb, &signals)?;
                Ok(DeleteOutcome::Deleted)
            })
            .await
        })
        .await
    }

    pub async fn list(&self, options: ListOptions) -> Result<ObjectPage> {
        self.run(
            options.cancellation.clone(),
            move |inner, signals| async move {
                if !options.prefix.is_empty() {
                    validate_key(options.prefix.strip_suffix('/').unwrap_or(&options.prefix))?;
                }
                if let Some(cursor) = &options.cursor {
                    validate_key(cursor)?;
                }
                if !(1..=1000).contains(&options.limit) {
                    return Err(StorageError::new(ErrorCode::InvalidInput));
                }
                let lock = lock_root(&inner, &signals).await?;
                let root = inner.config.root.clone();
                blocking(move || {
                    let _lock = lock;
                    // Keep at most limit + 1 metadata records, even for a large directory.
                    let mut objects = BTreeMap::new();
                    for entry in fs::read_dir(&root).map_err(StorageError::io)? {
                        signals.check()?;
                        let entry = entry.map_err(StorageError::io)?;
                        let name = entry
                            .file_name()
                            .into_string()
                            .map_err(|_| StorageError::new(ErrorCode::Forbidden))?;
                        if temporary(&name) {
                            // The uploader can clean its own staging directory without
                            // the publication lock. A disappeared staging entry is benign.
                            match directory(&entry.path()) {
                                Ok(_) => continue,
                                Err(error) if error.code() == ErrorCode::NotFound => continue,
                                Err(error) => return Err(error),
                            }
                        }
                        directory(&entry.path())?;
                        if name.strip_prefix(".delete-").is_some_and(hash_name) {
                            continue;
                        }
                        if !hash_name(&name) {
                            return Err(StorageError::new(ErrorCode::Forbidden));
                        }
                        let (metadata, _) = load(&root, &name, None)?;
                        if metadata.key.starts_with(&options.prefix)
                            && options
                                .cursor
                                .as_ref()
                                .is_none_or(|cursor| metadata.key > *cursor)
                        {
                            objects.insert(metadata.key.clone(), metadata);
                            if objects.len() > options.limit + 1 {
                                objects.pop_last();
                            }
                        }
                    }
                    let has_more = objects.len() > options.limit;
                    if has_more {
                        objects.pop_last();
                    }
                    let cursor = if has_more {
                        objects.last_key_value().map(|(key, _)| key.clone())
                    } else {
                        None
                    };
                    Ok(ObjectPage {
                        objects: objects.into_values().collect(),
                        cursor,
                    })
                })
                .await
            },
        )
        .await
    }
}

impl ObjectStorage for LocalStorage {
    fn capabilities(&self) -> StorageCapabilities {
        self.capabilities()
    }
    fn put(&self, input: PutObject) -> StorageFuture<'_, ObjectMetadata> {
        Box::pin(self.put(input))
    }
    fn get(&self, key: &str, options: ReadOptions) -> StorageFuture<'_, ObjectDownload> {
        let key = key.to_owned();
        Box::pin(async move { self.get(&key, options).await })
    }
    fn head(
        &self,
        key: &str,
        options: OperationOptions,
    ) -> StorageFuture<'_, Option<ObjectMetadata>> {
        let key = key.to_owned();
        Box::pin(async move { self.head(&key, options).await })
    }
    fn delete(&self, key: &str, options: OperationOptions) -> StorageFuture<'_, DeleteOutcome> {
        let key = key.to_owned();
        Box::pin(async move { self.delete(&key, options).await })
    }
    fn list(&self, options: ListOptions) -> StorageFuture<'_, ObjectPage> {
        Box::pin(self.list(options))
    }
}

async fn put(inner: Arc<Inner>, mut input: PutObject, signals: Signals) -> Result<ObjectMetadata> {
    validate_key(&input.key)?;
    validate_content_type(&input.content_type)?;
    let max_bytes = match (inner.config.max_upload_bytes, input.max_bytes) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    if input
        .size
        .zip(max_bytes)
        .is_some_and(|(size, limit)| size > limit)
    {
        return Err(StorageError::new(ErrorCode::TooLarge));
    }
    // Reject oversized metadata before polling the body or creating staging files.
    let preliminary = ObjectMetadata {
        key: input.key.clone(),
        size: u64::MAX,
        content_type: input.content_type.clone(),
        etag: format!("\"{}\"", "0".repeat(64)),
        last_modified: SystemTime::now(),
        custom_metadata: input.custom_metadata.clone(),
    };
    serialize_metadata(&preliminary)?;
    signals.check()?;
    let lock = lock_root(&inner, &signals).await?;
    let root = inner.config.root.clone();
    let temp = blocking(move || {
        let _lock = lock;
        tempfile::Builder::new()
            .prefix(".tmp-")
            .tempdir_in(root)
            .map_err(StorageError::io)
    })
    .await?;
    let result = async {
        let path = temp.path().join("payload");
        let payload = blocking(move || create_file(&path)).await?;
        let mut payload = File::from_std(payload);
        let mut hash = Sha256::new();
        let mut size = 0u64;
        let mut buffer = vec![0; CHUNK];
        loop {
            let count = tokio::select! {
                biased;
                _ = signals.cancelled() => return Err(StorageError::new(ErrorCode::Aborted)),
                result = input.body.read(&mut buffer) => result.map_err(StorageError::upload_source)?,
            };
            if count == 0 { break; }
            size = size.checked_add(count as u64).ok_or_else(|| StorageError::new(ErrorCode::TooLarge))?;
            if max_bytes.is_some_and(|limit| size > limit) { return Err(StorageError::new(ErrorCode::TooLarge)); }
            if input.size.is_some_and(|declared| size > declared) { return Err(StorageError::new(ErrorCode::InvalidInput)); }
            signals.check()?;
            payload.write_all(&buffer[..count]).await.map_err(StorageError::io)?;
            hash.update(&buffer[..count]);
        }
        if input.size.is_some_and(|declared| size != declared) { return Err(StorageError::new(ErrorCode::InvalidInput)); }
        // Flush before sync: Tokio file writes may still have a blocking write in flight.
        payload.flush().await.map_err(StorageError::io)?;
        payload.sync_all().await.map_err(StorageError::io)?;
        drop(payload);
        let metadata = ObjectMetadata { key: input.key, size, content_type: input.content_type,
            etag: format!("\"{:x}\"", hash.finalize()), last_modified: SystemTime::now(), custom_metadata: input.custom_metadata };
        let bytes = serialize_metadata(&metadata)?;
        let path = temp.path().join("metadata.json");
        let metadata_file = blocking(move || create_file(&path)).await?;
        let mut metadata_file = File::from_std(metadata_file);
        metadata_file.write_all(&bytes).await.map_err(StorageError::io)?;
        metadata_file.flush().await.map_err(StorageError::io)?;
        metadata_file.sync_all().await.map_err(StorageError::io)?;
        drop(metadata_file);
        let lock = lock_root(&inner, &signals).await?;
        let root = inner.config.root.clone();
        let source = temp.path().to_owned();
        let destination = root.join(object_name(&metadata.key));
        let publish_signals = signals.clone();
        blocking(move || {
            let _lock = lock;
            publish_signals.check()?;
            match fs::symlink_metadata(&destination) {
                Ok(_) => { directory(&destination)?; return Err(StorageError::new(ErrorCode::Conflict)); }
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(StorageError::io(error)),
            }
            // All cooperative writers hold the same root lock. Committed objects
            // are nonempty directories and Unix rename cannot overwrite them.
            fs::rename(source, destination).map_err(StorageError::io)
        }).await?;
        Ok(metadata)
    }.await;
    // The owned task reaches this even when the API future is dropped/aborted.
    let cleanup = if result.is_ok() {
        // Publication moved this directory. Disarm TempDir instead of trying to
        // delete the old name (which could now belong to a different upload).
        let _ = temp.keep();
        Ok(())
    } else {
        blocking(move || temp.close().map_err(StorageError::io)).await
    };
    match (result, cleanup) {
        (result, Ok(())) => result,
        (result, Err(cleanup)) => {
            inner.cleanup_failed.store(true, Ordering::Release);
            Err(StorageError::caused(
                result
                    .as_ref()
                    .err()
                    .map_or(ErrorCode::Provider, StorageError::code),
                CleanupError {
                    primary: result.err(),
                    cleanup,
                },
            ))
        }
    }
}

#[derive(Debug)]
struct CleanupError {
    primary: Option<StorageError>,
    cleanup: StorageError,
}
impl std::fmt::Display for CleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Upload cleanup failed (primary: {:?}, cleanup: {:?})",
            self.primary, self.cleanup
        )
    }
}
impl std::error::Error for CleanupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cleanup)
    }
}

fn serialize_metadata(metadata: &ObjectMetadata) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(metadata)
        .map_err(|error| StorageError::caused(ErrorCode::InvalidInput, error))?;
    if bytes.len() > METADATA_LIMIT {
        return Err(StorageError::new(ErrorCode::InvalidInput));
    }
    Ok(bytes)
}

fn object_name(key: &str) -> String {
    format!("{:x}", Sha256::digest(key.as_bytes()))
}
fn hash_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn temporary(name: &str) -> bool {
    name.strip_prefix(".tmp-").is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_alphanumeric())
    })
}

fn directory(path: &Path) -> Result<Metadata> {
    let metadata = fs::symlink_metadata(path).map_err(StorageError::io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(StorageError::new(ErrorCode::Forbidden));
    }
    Ok(metadata)
}
fn ancestry(root: &Path) -> Result<()> {
    for path in root.ancestors() {
        directory(path)?;
    }
    Ok(())
}
fn create_root(root: &Path) -> Result<()> {
    let mut path = PathBuf::new();
    for component in root.components() {
        path.push(component);
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&path) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(StorageError::io(error)),
        }
        directory(&path)?;
    }
    Ok(())
}
fn same_identity(a: &Metadata, b: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    }
}
fn regular(metadata: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.is_file() && metadata.nlink() == 1
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        false
    }
}
fn safe_open(path: &Path) -> Result<StdFile> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            StorageError::caused(ErrorCode::Provider, error)
        } else {
            StorageError::io(error)
        }
    })?;
    if !regular(&file.metadata().map_err(StorageError::io)?) {
        return Err(StorageError::new(ErrorCode::Forbidden));
    }
    Ok(file)
}
fn create_file(path: &Path) -> Result<StdFile> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    }
    options.open(path).map_err(StorageError::io)
}

async fn lock_root(inner: &Arc<Inner>, signals: &Signals) -> Result<StdFile> {
    loop {
        signals.check()?;
        let inner = inner.clone();
        let lock = blocking(move || {
            ancestry(&inner.config.root)?;
            let mut options = OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY);
            }
            let file = options.open(&inner.config.root).map_err(StorageError::io)?;
            if !same_identity(&inner.identity, &file.metadata().map_err(StorageError::io)?) {
                return Err(StorageError::new(ErrorCode::Conflict));
            }
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => {
                    ancestry(&inner.config.root)?;
                    Ok(Some(file))
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
                Err(error) => Err(StorageError::io(error)),
            }
        })
        .await?;
        if let Some(lock) = lock {
            signals.check()?;
            return Ok(lock);
        }
        tokio::select! { biased; _ = signals.cancelled() => return Err(StorageError::new(ErrorCode::Aborted)), _ = tokio::time::sleep(Duration::from_millis(5)) => () }
    }
}

fn load(root: &Path, name: &str, expected_key: Option<&str>) -> Result<(ObjectMetadata, StdFile)> {
    let path = root.join(name);
    let identity = directory(&path)?;
    let entries = fs::read_dir(&path)
        .map_err(StorageError::io)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<Vec<_>>>()
        .map_err(StorageError::io)?;
    if entries.len() != 2
        || !entries.iter().any(|name| name == "payload")
        || !entries.iter().any(|name| name == "metadata.json")
    {
        return Err(StorageError::new(ErrorCode::Forbidden));
    }
    let metadata_file = safe_open(&path.join("metadata.json"))?;
    let mut bytes = Vec::new();
    metadata_file
        .take(METADATA_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(StorageError::io)?;
    if bytes.len() > METADATA_LIMIT {
        return Err(StorageError::new(ErrorCode::Provider));
    }
    let metadata: ObjectMetadata = serde_json::from_slice(&bytes)
        .map_err(|error| StorageError::caused(ErrorCode::Provider, error))?;
    if validate_key(&metadata.key).is_err()
        || validate_content_type(&metadata.content_type).is_err()
        || object_name(&metadata.key) != name
        || expected_key.is_some_and(|key| key != metadata.key)
        || metadata.etag.len() != 66
        || !metadata.etag.starts_with('"')
        || !metadata.etag.ends_with('"')
        || !hash_name(&metadata.etag[1..65])
    {
        return Err(StorageError::new(ErrorCode::Provider));
    }
    let payload = safe_open(&path.join("payload"))?;
    if payload.metadata().map_err(StorageError::io)?.len() != metadata.size {
        return Err(StorageError::new(ErrorCode::Provider));
    }
    if !same_identity(&identity, &directory(&path)?) {
        return Err(StorageError::new(ErrorCode::Conflict));
    }
    Ok((metadata, payload))
}

fn remove_tomb(tomb: &Path, signals: &Signals) -> Result<bool> {
    match directory(tomb) {
        Ok(_) => (),
        Err(error) if error.code() == ErrorCode::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    let entries = fs::read_dir(tomb)
        .map_err(StorageError::io)?
        .collect::<io::Result<Vec<_>>>()
        .map_err(StorageError::io)?;
    for entry in &entries {
        if entry.file_name() != "payload" && entry.file_name() != "metadata.json" {
            return Err(StorageError::new(ErrorCode::Forbidden));
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(StorageError::io)?;
        if !regular(&metadata) || metadata.file_type().is_symlink() {
            return Err(StorageError::new(ErrorCode::Forbidden));
        }
    }
    for entry in entries {
        signals.check()?;
        fs::remove_file(entry.path()).map_err(StorageError::io)?;
    }
    signals.check()?;
    fs::remove_dir(tomb).map_err(StorageError::io)?;
    Ok(true)
}
