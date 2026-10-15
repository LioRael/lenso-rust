use lenso_audit::*;
use std::sync::Arc;

// Application-owned policy. A real adapter must authenticate/revalidate evidence
// (including audience and revocation) before returning this identity snapshot.
struct MaintenanceAuthority;
struct MaintenancePermit;
impl AuditAuthority for MaintenanceAuthority {
    type Principal = MaintenancePermit;

    async fn resolve(
        &self,
        _: &MaintenancePermit,
        scope: &AuditScope,
        _: AuditOperation,
    ) -> Result<AuditActor, BoxError> {
        if scope.tenant_id.is_some() || scope.scope_id != "maintenance" {
            return Err(AuditError::Unauthorized.into());
        }
        Ok(AuditActor::System {
            system_id: "maintenance".into(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let repository = Arc::new(MemoryAuditRepository::new(100)?);
    let audit = AuditService::new(
        repository,
        Arc::new(MaintenanceAuthority),
        AuditConfig::default(),
    )?;
    let scope = AuditScope {
        tenant_id: None,
        scope_id: "maintenance".into(),
    };
    let input = AuditInput {
        id: "00000000-0000-0000-0000-000000000001".into(),
        occurred_at: 1,
        scope: scope.clone(),
        action: "maintenance.inspect".into(),
        target: AuditTarget {
            kind: "resource".into(),
            id: "resource-1".into(),
        },
        result: AuditResult::Success,
        reason_code: "inspected".into(),
        correlation_id: None,
        summary: Summary::new(),
        relation: None,
    };
    let permit = MaintenancePermit;
    assert_eq!(
        audit.append(input.clone(), &permit).await?.status,
        AppendStatus::Inserted
    );
    assert_eq!(
        audit.append(input, &permit).await?.status,
        AppendStatus::Duplicate
    );
    let page = audit.query(AuditQuery::new(scope), &permit).await?;
    println!(
        "{} scoped event; retry returned Duplicate",
        page.events.len()
    );
    Ok(())
}
