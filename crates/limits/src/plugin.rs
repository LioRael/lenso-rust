use crate::{FailurePolicy, LimitStore, Limits, MemoryLimitStore};
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use std::sync::Arc;

enum StoreSource<S> {
    Borrowed(Arc<S>),
    Instance(Handle<S>),
}

/// Thin lifecycle adapter. No global lookup, new container, or store ownership.
pub struct LimitsPlugin<S: LimitStore = MemoryLimitStore> {
    source: StoreSource<S>,
    policy: FailurePolicy,
}
impl<S: LimitStore> LimitsPlugin<S> {
    /// Borrow an application-owned store directly.
    pub fn new(store: Arc<S>, policy: FailurePolicy) -> Self {
        Self {
            source: StoreSource::Borrowed(store),
            policy,
        }
    }

    /// Borrow precisely this plugin instance's store and declare its dependency.
    pub fn from_handle(store: Handle<S>, policy: FailurePolicy) -> Self {
        Self {
            source: StoreSource::Instance(store),
            policy,
        }
    }
}
impl<S: LimitStore> Plugin for LimitsPlugin<S> {
    type Service = Limits<S>;

    fn dependencies(&self) -> Vec<InstanceKey> {
        match &self.source {
            StoreSource::Borrowed(_) => Vec::new(),
            StoreSource::Instance(handle) => vec![handle.key()],
        }
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let store = match self.source {
            StoreSource::Borrowed(store) => store,
            StoreSource::Instance(handle) => setup.get(&handle)?,
        };
        let limits = Arc::new(Limits::new(store, self.policy));
        let entrance = limits.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            entrance.stop_runs().map_err(Into::into)
        });
        let cleanup = limits.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close_async().await.map_err(Into::into)
        });
        Ok(limits)
    }
}
