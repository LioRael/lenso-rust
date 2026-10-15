use crate::{LocalStorage, LocalStorageConfig};
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use std::sync::Arc;

/// Thin ownership adapter. Dependents declare this exact instance's handle.
pub struct LocalStoragePlugin {
    config: LocalStorageConfig,
}
impl LocalStoragePlugin {
    pub fn new(config: LocalStorageConfig) -> Self {
        Self { config }
    }
}
impl Plugin for LocalStoragePlugin {
    type Service = LocalStorage;
    fn validate(&self) -> Result<(), BoxError> {
        self.config.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<LocalStorage>, BoxError> {
        let service = Arc::new(LocalStorage::open(self.config).await?);
        let cleanup = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().await.map_err(Into::into)
        });
        Ok(service)
    }
}
