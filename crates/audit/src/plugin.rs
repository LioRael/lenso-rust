use crate::*;
use lenso_plugin_core::{CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use std::sync::Arc;

/// Thin instance-specific wiring. Providers retain ownership of their resources.
/// Hosts admit external work through RunningApp::work_gate and keep the guard
/// through dependency calls; Audit never reenters that gate during app drain.
pub struct AuditPlugin<R, A, D = NoReporter> {
    repository: Handle<R>,
    authority: Handle<A>,
    reporter: Option<Handle<D>>,
    config: AuditConfig,
}

impl<R: AuditRepository, A: AuditAuthority> AuditPlugin<R, A> {
    pub fn new(repository: Handle<R>, authority: Handle<A>, config: AuditConfig) -> Self {
        Self {
            repository,
            authority,
            reporter: None,
            config,
        }
    }
}

impl<R, A, D> AuditPlugin<R, A, D> {
    pub fn with_reporter<T: AuditReporter>(self, reporter: Handle<T>) -> AuditPlugin<R, A, T> {
        AuditPlugin {
            repository: self.repository,
            authority: self.authority,
            reporter: Some(reporter),
            config: self.config,
        }
    }
}

impl<R: AuditRepository, A: AuditAuthority, D: AuditReporter> Plugin for AuditPlugin<R, A, D> {
    type Service = AuditService<R, A>;

    fn validate(&self) -> Result<(), BoxError> {
        Ok(self.config.validate()?)
    }

    fn dependencies(&self) -> Vec<InstanceKey> {
        let mut keys = vec![self.repository.key(), self.authority.key()];
        if let Some(reporter) = &self.reporter {
            keys.push(reporter.key());
        }
        keys
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let mut service = AuditService::new(
            setup.get(&self.repository)?,
            setup.get(&self.authority)?,
            self.config,
        )?;
        if let Some(reporter) = self.reporter {
            service = service.with_reporter(setup.get(&reporter)?);
        }
        let service = Arc::new(service);
        let closing = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            closing.close();
            Ok(())
        });
        Ok(service)
    }
}
