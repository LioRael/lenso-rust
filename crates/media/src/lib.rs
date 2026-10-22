#![doc = include_str!("../README.md")]
mod codec;
mod journal;
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup, WorkGate};
use lenso_storage::{ObjectMetadata, ObjectStorage, PutObject, ReadOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Cursor, path::PathBuf, sync::Arc};
use tokio::{io::AsyncReadExt, sync::Semaphore};
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid media configuration or transform")]
    Invalid,
    #[error("media resource limit exceeded")]
    Limit,
    #[error("unsupported or invalid image")]
    Image,
    #[error("media access denied")]
    Forbidden,
    #[error("source version changed or absent")]
    Stale,
    #[error("media worker busy or closing")]
    Busy,
    #[error("media storage failed")]
    Storage,
    #[error("media journal failed")]
    Journal,
    #[error("media worker failed")]
    Worker,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Source {
    pub key: String,
    pub etag: String,
    pub scope: String,
}
/// Host policy is consulted anew for source inspection, publication and every read.
/// Scope is a resource selector, not proof of identity. The policy owns verified identity.
pub trait Authority: Send + Sync + 'static {
    fn authorize(&self, source: &Source) -> bool;
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Format {
    Png,
    Jpeg,
}
impl Format {
    fn image(self) -> image::ImageFormat {
        match self {
            Self::Png => image::ImageFormat::Png,
            Self::Jpeg => image::ImageFormat::Jpeg,
        }
    }
    fn mime(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Preset {
    /// Host-controlled version; change when business intent changes.
    pub version: String,
    /// Aspect-preserving fit inside this box.
    pub width: u32,
    pub height: u32,
    /// Optional pixel rectangle [x,y,width,height] applied before resize.
    pub crop: Option<[u32; 4]>,
    pub format: Format,
}
#[derive(Clone, Debug)]
pub struct Limits {
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub max_pixels: u64,
    pub max_dimension: u32,
    pub memory_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 8 * 1024 * 1024,
            output_bytes: 8 * 1024 * 1024,
            max_pixels: 4_000_000,
            max_dimension: 4096,
            memory_bytes: 160 * 1024 * 1024,
        }
    }
}
impl Limits {
    fn check_dimensions(&self, w: u32, h: u32) -> Result<()> {
        if w == 0
            || h == 0
            || w > self.max_dimension
            || h > self.max_dimension
            || u64::from(w) * u64::from(h) > self.max_pixels
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
}
pub struct Config {
    /// Stable host identity for this exact Storage resource; never reuse across roots.
    pub storage_id: String,
    /// Exclusive owned SQLite manifest. Parent directory must already exist.
    pub journal: PathBuf,
    pub presets: BTreeMap<String, Preset>,
    pub limits: Limits,
}
impl Config {
    fn validate(&self) -> Result<()> {
        let l = &self.limits;
        let budget = l
            .max_pixels
            .checked_mul(32)
            .and_then(|n| n.checked_add(l.input_bytes))
            .and_then(|n| n.checked_add(l.output_bytes));
        if self.storage_id.is_empty()
            || self.presets.is_empty()
            || l.input_bytes == 0
            || l.output_bytes == 0
            || l.max_pixels == 0
            || l.input_bytes == u64::MAX
            || l.output_bytes == u64::MAX
            || budget.is_none_or(|n| n > l.memory_bytes)
        {
            return Err(Error::Invalid);
        }
        for (name, p) in &self.presets {
            if name.is_empty() || p.version.is_empty() {
                return Err(Error::Invalid);
            }
            l.check_dimensions(p.width, p.height)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Derived {
    source: Source,
    identity: String,
    metadata: ObjectMetadata,
    pub width: u32,
    pub height: u32,
}
impl Derived {
    pub fn key(&self) -> &str {
        &self.metadata.key
    }
}
pub struct Media {
    storage: Arc<dyn ObjectStorage>,
    authority: Arc<dyn Authority>,
    config: Config,
    journal: journal::Journal,
    gate: WorkGate,
    slot: Arc<Semaphore>,
}
impl Media {
    fn authorize(&self, source: &Source) -> Result<()> {
        if self.authority.authorize(source) {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    async fn current(&self, source: &Source) -> Result<()> {
        self.authorize(source)?;
        let meta = self
            .storage
            .head(&source.key, Default::default())
            .await
            .map_err(|_| Error::Storage)?;
        if meta.is_none_or(|m| m.etag != source.etag) {
            return Err(Error::Stale);
        }
        Ok(())
    }
    async fn source_bytes(&self, source: &Source) -> Result<Vec<u8>> {
        self.current(source).await?;
        let object = self
            .storage
            .get(
                &source.key,
                ReadOptions {
                    if_match: Some(source.etag.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| Error::Stale)?;
        if object.metadata.size > self.config.limits.input_bytes {
            return Err(Error::Limit);
        }
        let mut bytes = Vec::new();
        object
            .body
            .take(self.config.limits.input_bytes + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| Error::Storage)?;
        if bytes.len() as u64 > self.config.limits.input_bytes {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    /// Non-queued admission: one job per instance bounds aggregate decode memory.
    /// The owned task continues if its caller drops the future; shutdown drains it.
    pub async fn derive(self: &Arc<Self>, source: Source, preset: String) -> Result<Derived> {
        let guard = self.gate.enter().map_err(|_| Error::Busy)?;
        let slot = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let this = self.clone();
        tokio::spawn(async move {
            let (_guard, _slot) = (guard, slot);
            this.derive_inner(source, preset).await
        })
        .await
        .map_err(|_| Error::Worker)?
    }
    async fn derive_inner(&self, source: Source, name: String) -> Result<Derived> {
        self.current(&source).await?;
        let preset = self
            .config
            .presets
            .get(&name)
            .ok_or(Error::Invalid)?
            .clone();
        let identity = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "image-0.25.10/media-v1",
                    &self.config.storage_id,
                    &source,
                    &name,
                    &preset
                ))
                .map_err(|_| Error::Invalid)?
            )
        );
        if let Some(derived) = self.journal.ready(identity.clone()).await? {
            self.read_inner(&derived).await?;
            return Ok(derived);
        }
        // Recover this identity's previous failed attempt before retrying.
        self.recover().await?;
        let bytes = self.source_bytes(&source).await?;
        let limits = self.config.limits.clone();
        let format = preset.format;
        let (width, height, bytes) =
            tokio::task::spawn_blocking(move || codec::process(bytes, Some(preset), limits))
                .await
                .map_err(|_| Error::Worker)??;
        self.current(&source).await?;
        let key = self.journal.begin(identity.clone()).await?;
        let result = async {
            let mut put = PutObject::new(&key, Cursor::new(bytes));
            put.content_type = format.mime().into();
            put.max_bytes = Some(self.config.limits.output_bytes);
            let metadata = match self.storage.put(put).await {
                Ok(metadata) => metadata,
                Err(error) if error.code() == lenso_storage::ErrorCode::Conflict => {
                    // A create-only collision is not our object to clean up.
                    self.journal.remove(identity.clone()).await?;
                    return Err(Error::Storage);
                }
                Err(_) => return Err(Error::Storage),
            };
            self.current(&source).await?;
            let derived = Derived {
                source,
                identity: identity.clone(),
                metadata,
                width,
                height,
            };
            self.journal.publish(derived.clone()).await?;
            Ok(derived)
        }
        .await;
        if result.is_err() {
            self.recover().await?;
        }
        result
    }
    /// Returns bounded bytes after fresh host authorization and source version check.
    pub async fn read(self: &Arc<Self>, derived: &Derived) -> Result<Vec<u8>> {
        let guard = self.gate.enter().map_err(|_| Error::Busy)?;
        let slot = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let this = self.clone();
        let derived = derived.clone();
        tokio::spawn(async move {
            let (_guard, _slot) = (guard, slot);
            this.read_inner(&derived).await
        })
        .await
        .map_err(|_| Error::Worker)?
    }
    async fn read_inner(&self, derived: &Derived) -> Result<Vec<u8>> {
        self.current(&derived.source).await?;
        // Only this manifest's published records are readable, including deserialized references.
        let stored = self
            .journal
            .ready(derived.identity.clone())
            .await?
            .ok_or(Error::Stale)?;
        if serde_json::to_vec(&stored).map_err(|_| Error::Journal)?
            != serde_json::to_vec(derived).map_err(|_| Error::Journal)?
        {
            return Err(Error::Stale);
        }
        let object = self
            .storage
            .get(
                &stored.metadata.key,
                ReadOptions {
                    if_match: Some(stored.metadata.etag),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| Error::Storage)?;
        if object.metadata.size > self.config.limits.output_bytes {
            return Err(Error::Limit);
        }
        let mut bytes = Vec::new();
        object
            .body
            .take(self.config.limits.output_bytes + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| Error::Storage)?;
        if bytes.len() as u64 > self.config.limits.output_bytes {
            return Err(Error::Limit);
        }
        self.current(&derived.source).await?;
        Ok(bytes)
    }
    pub async fn inspect(self: &Arc<Self>, source: Source) -> Result<(u32, u32)> {
        let guard = self.gate.enter().map_err(|_| Error::Busy)?;
        let slot = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let this = self.clone();
        tokio::spawn(async move {
            let (_guard, _slot) = (guard, slot);
            let bytes = this.source_bytes(&source).await?;
            let limits = this.config.limits.clone();
            let (w, h, _) =
                tokio::task::spawn_blocking(move || codec::process(bytes, None, limits))
                    .await
                    .map_err(|_| Error::Worker)??;
            this.current(&source).await?;
            Ok((w, h))
        })
        .await
        .map_err(|_| Error::Worker)?
    }
    async fn recover(&self) -> Result<()> {
        for (id, key) in self.journal.pending().await? {
            self.storage
                .delete(&key, Default::default())
                .await
                .map_err(|_| Error::Storage)?;
            self.journal.remove(id).await?;
        }
        Ok(())
    }
}
pub struct MediaPlugin<S: ObjectStorage + 'static> {
    storage: Handle<S>,
    config: Config,
    authority: Arc<dyn Authority>,
}
impl<S: ObjectStorage + 'static> MediaPlugin<S> {
    pub fn new(storage: Handle<S>, config: Config, authority: Arc<dyn Authority>) -> Self {
        Self {
            storage,
            config,
            authority,
        }
    }
}
impl<S: ObjectStorage + 'static> Plugin for MediaPlugin<S> {
    type Service = Media;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.storage.key()]
    }
    fn validate(&self) -> std::result::Result<(), BoxError> {
        self.config.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> std::result::Result<Arc<Media>, BoxError> {
        let storage = setup.get(&self.storage)?;
        if !storage.capabilities().conditional_read {
            return Err(Error::Invalid.into());
        }
        let journal =
            journal::Journal::open(self.config.journal.clone(), self.config.storage_id.clone())
                .await?;
        let cleanup = journal.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().await.map_err(Into::into)
        });
        let service = Arc::new(Media {
            storage,
            authority: self.authority,
            config: self.config,
            journal,
            gate: setup.work_gate(),
            slot: Arc::new(Semaphore::new(1)),
        });
        service.recover().await?;
        Ok(service)
    }
}
