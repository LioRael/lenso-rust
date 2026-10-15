//! Streaming object storage with an independently usable local service.
//!
//! See the crate README for lifecycle, filesystem trust and parity boundaries.
#![doc = include_str!("../README.md")]

mod body;
mod error;
mod local;
mod plugin;
mod types;

pub use body::DownloadBody;
pub use error::{ErrorCode, StorageError};
pub use local::{LocalStorage, LocalStorageConfig};
pub use plugin::LocalStoragePlugin;
pub use tokio_util::sync::CancellationToken;
pub use types::*;
