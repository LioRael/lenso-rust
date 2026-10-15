use crate::{
    authorizer, Authorizer, Identity, Lease, MemoryRealtime, RealtimeConfig, RealtimeError,
    Resource,
};
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use std::{future::Future, sync::Arc};

type Factory<P> = Box<dyn FnOnce(&Setup) -> Result<Authorizer<P>, BoxError> + Send>;

/// Thin composition adapter. The service itself requires no plugin container.
pub struct RealtimePlugin<P> {
    config: RealtimeConfig,
    dependencies: Vec<InstanceKey>,
    factory: Factory<P>,
}
impl<P: Send + Sync + 'static> RealtimePlugin<P> {
    /// Inject an ordinary host callback; construction does not call it.
    pub fn new<F, Fut>(config: RealtimeConfig, authorize: F) -> Self
    where
        F: Fn(Arc<Identity<P>>, Resource) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Lease>, RealtimeError>> + Send + 'static,
    {
        Self {
            config,
            dependencies: Vec::new(),
            factory: Box::new(move |_| Ok(authorizer(authorize))),
        }
    }

    /// Bind exactly one existing policy/business service instance. The callback
    /// is the small authorization port; there is no Auth crate dependency.
    pub fn with_dependency<D, F, Fut>(
        config: RealtimeConfig,
        dependency: Handle<D>,
        authorize: F,
    ) -> Self
    where
        D: Send + Sync + 'static,
        F: Fn(Arc<D>, Arc<Identity<P>>, Resource) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Lease>, RealtimeError>> + Send + 'static,
    {
        Self {
            config,
            dependencies: vec![dependency.key()],
            factory: Box::new(move |setup| {
                let service = setup.get(&dependency)?;
                Ok(authorizer(move |identity, resource| {
                    authorize(service.clone(), identity, resource)
                }))
            }),
        }
    }
}
impl<P: Send + Sync + 'static> Plugin for RealtimePlugin<P> {
    type Service = MemoryRealtime<P>;
    fn validate(&self) -> Result<(), BoxError> {
        self.config.validate().map_err(Into::into)
    }
    fn dependencies(&self) -> Vec<InstanceKey> {
        self.dependencies.clone()
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let realtime = Arc::new(MemoryRealtime::with_authorizer(
            self.config,
            (self.factory)(setup)?,
        )?);
        let cleanup = realtime.clone();
        // Wake streams and pending authorization before host WorkGuards drain.
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            cleanup.close();
            Ok(())
        });
        Ok(realtime)
    }
}
