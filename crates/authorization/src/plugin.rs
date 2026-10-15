use crate::{Authorization, AuthorizationOptions, ConfigurationError};
use lenso_plugin_core::{BoxError, InstanceKey, Plugin, Setup};
use std::{future::Future, pin::Pin, sync::Arc};

/// A setup closure may borrow Core's existing context through its asynchronous work.
pub type AuthorizationSetupFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Arc<Authorization>, BoxError>> + Send + 'a>>;
type Factory = Box<dyn for<'a> FnOnce(&'a mut Setup) -> AuthorizationSetupFuture<'a> + Send>;

/// Thin, optional Core lifecycle wiring. No resource acquisition at declaration,
/// implicit dependency lookup, extra container, work gate, or borrowed-resource cleanup.
pub struct AuthorizationPlugin {
    requires: Vec<InstanceKey>,
    factory: Factory,
}
impl AuthorizationPlugin {
    /// Prevalidate ordinary options before installing a resource-free service.
    pub fn new(options: AuthorizationOptions) -> Result<Self, ConfigurationError> {
        let service = Arc::new(Authorization::new(options)?);
        Ok(Self::setup(vec![], move |_| {
            Box::pin(async move { Ok(service) })
        }))
    }
    /// Resolve exact declared handles with Setup::get in the supplied closure.
    /// Resource owners remain responsible for their own Core cleanup registration.
    pub fn setup<F>(requires: Vec<InstanceKey>, factory: F) -> Self
    where
        F: for<'a> FnOnce(&'a mut Setup) -> AuthorizationSetupFuture<'a> + Send + 'static,
    {
        Self {
            requires,
            factory: Box::new(factory),
        }
    }
}
impl Plugin for AuthorizationPlugin {
    type Service = Authorization;
    fn dependencies(&self) -> Vec<InstanceKey> {
        self.requires.clone()
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Authorization>, BoxError> {
        (self.factory)(setup).await
    }
}
