use lenso_audit::*;
use lenso_plugin_core::{App, Instance, Plugin, Setup};
use std::sync::Arc;

struct RepositoryProvider;
impl Plugin for RepositoryProvider {
    type Service = MemoryAuditRepository;
    async fn setup(self, _: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        // This memory adapter owns no external resource. A real repository plugin
        // registers its own pool cleanup immediately after acquisition.
        Ok(Arc::new(MemoryAuditRepository::new(100)?))
    }
}

struct Authority;
impl AuditAuthority for Authority {
    type Principal = ();
    async fn resolve(
        &self,
        _: &(),
        scope: &AuditScope,
        _: AuditOperation,
    ) -> Result<AuditActor, BoxError> {
        if scope.tenant_id.is_some() || scope.scope_id != "maintenance" {
            return Err(AuditError::Unauthorized.into());
        }
        Ok(AuditActor::System {
            system_id: "example".into(),
        })
    }
}
impl Plugin for Authority {
    type Service = Self;
    async fn setup(self, _: &mut Setup) -> Result<Arc<Self>, BoxError> {
        Ok(Arc::new(self))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let repository = Instance::new("audit-memory", RepositoryProvider);
    let authority = Instance::new("maintenance-authority", Authority);
    let audit = Instance::new(
        "audit",
        AuditPlugin::new(
            repository.handle(),
            authority.handle(),
            AuditConfig::default(),
        ),
    );
    let handle = audit.handle();
    // Insertion order does not decide dependency order; handles do.
    let running = App::new()
        .with(audit)
        .with(authority)
        .with(repository)
        .start()
        .await?;
    let guard = running.work_gate().enter()?;
    let service = running.get(&handle)?;
    let query = AuditQuery::new(AuditScope {
        tenant_id: None,
        scope_id: "maintenance".into(),
    });
    assert!(service.query(query.clone(), &()).await?.events.is_empty());
    drop(guard);
    running.shutdown().await?;
    assert_eq!(
        service.query(query, &()).await.unwrap_err(),
        AuditError::Closed
    );
    println!("Exact dependency wiring and retained-service shutdown verified");
    Ok(())
}
