use crate::{CancellationToken, DownloadBody, ErrorCode, StorageError};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, pin::Pin, time::SystemTime};
use tokio::io::AsyncRead;

pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;
pub type UploadBody = Pin<Box<dyn AsyncRead + Send + 'static>>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub content_type: String,
    /// Quoted lowercase SHA-256 of the uploaded bytes.
    pub etag: String,
    pub last_modified: SystemTime,
    pub custom_metadata: BTreeMap<String, String>,
}

/// A create-only upload. Ownership lets cancellation drop a stalled source.
pub struct PutObject {
    pub key: String,
    pub body: UploadBody,
    pub content_type: String,
    pub size: Option<u64>,
    pub max_bytes: Option<u64>,
    pub custom_metadata: BTreeMap<String, String>,
    pub cancellation: Option<CancellationToken>,
}
impl PutObject {
    pub fn new(key: impl Into<String>, body: impl AsyncRead + Send + 'static) -> Self {
        Self {
            key: key.into(),
            body: Box::pin(body),
            content_type: "application/octet-stream".into(),
            size: None,
            max_bytes: None,
            custom_metadata: BTreeMap::new(),
            cancellation: None,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct OperationOptions {
    pub cancellation: Option<CancellationToken>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRange {
    pub offset: u64,
    pub length: Option<u64>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRange {
    pub offset: u64,
    pub length: u64,
}
#[derive(Clone, Default, Debug)]
pub struct ReadOptions {
    pub range: Option<ReadRange>,
    pub if_match: Option<String>,
    pub cancellation: Option<CancellationToken>,
}
pub struct ObjectDownload {
    /// Metadata always describes the entire object.
    pub metadata: ObjectMetadata,
    pub body: DownloadBody,
    pub range: Option<ResolvedRange>,
}
#[derive(Clone, Debug)]
pub struct ListOptions {
    pub prefix: String,
    /// Exclusive key cursor, using Rust's Unicode scalar/UTF-8 lexical order.
    pub cursor: Option<String>,
    pub limit: usize,
    pub cancellation: Option<CancellationToken>,
}
impl Default for ListOptions {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            cursor: None,
            limit: 100,
            cancellation: None,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPage {
    pub objects: Vec<ObjectMetadata>,
    pub cursor: Option<String>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageCapabilities {
    pub range_read: bool,
    pub conditional_read: bool,
    pub signed_upload: bool,
    pub signed_download: bool,
    pub upload_requires_size: bool,
    pub max_upload_bytes: Option<u64>,
}

/// Minimal object-safe boundary for business services; no DI or permissions container.
/// Providers own their lifecycle separately from this interface.
pub trait ObjectStorage: Send + Sync {
    fn capabilities(&self) -> StorageCapabilities;
    fn put(&self, input: PutObject) -> StorageFuture<'_, ObjectMetadata>;
    fn get(&self, key: &str, options: ReadOptions) -> StorageFuture<'_, ObjectDownload>;
    fn head(
        &self,
        key: &str,
        options: OperationOptions,
    ) -> StorageFuture<'_, Option<ObjectMetadata>>;
    fn delete(&self, key: &str, options: OperationOptions) -> StorageFuture<'_, DeleteOutcome>;
    fn list(&self, options: ListOptions) -> StorageFuture<'_, ObjectPage>;
}

/// # Errors
/// Returns `InvalidInput` for empty/oversized keys, absolute paths, traversal,
/// empty components, control characters, backslashes or drive-letter prefixes.
pub fn validate_key(key: &str) -> Result<(), StorageError> {
    let bytes = key.as_bytes();
    if key.is_empty()
        || bytes.len() > 1024
        || bytes.iter().any(|b| *b < 32 || *b == 127 || *b == b'\\')
        || key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || (bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
    {
        return Err(StorageError::new(ErrorCode::InvalidKey));
    }
    Ok(())
}
pub(crate) fn validate_content_type(value: &str) -> Result<(), StorageError> {
    if value.is_empty() || value.len() > 1024 || value.bytes().any(|b| b < 32 || b == 127) {
        return Err(StorageError::new(ErrorCode::InvalidInput));
    }
    Ok(())
}
