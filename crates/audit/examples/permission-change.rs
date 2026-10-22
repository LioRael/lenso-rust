//! Application-owned fixture: no real identities, credentials or user database.
use lenso_audit::*;
use std::{collections::BTreeMap, sync::Arc};

struct FixtureAuthority;
impl AuditAuthority for FixtureAuthority {
    type Principal = str;
    async fn resolve(
        &self,
        principal: &str,
        scope: &AuditScope,
        _: AuditOperation,
    ) -> Result<AuditActor, BoxError> {
        if principal != "test-admin" || scope != &fixture_scope() {
            return Err(AuditError::Unauthorized.into());
        }
        Ok(AuditActor::Subject {
            kind: SubjectKind::User,
            realm_id: "test-realm".into(),
            subject_id: principal.into(),
        })
    }
}
fn fixture_scope() -> AuditScope {
    AuditScope {
        tenant_id: Some("test-tenant".into()),
        scope_id: "test-project".into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fixture.sqlite");
    let repository = Arc::new(SqliteAuditRepository::open(&path).await?);
    let config = AuditConfig {
        summary_policy: BTreeMap::from([(
            "permission.change".into(),
            BTreeMap::from([(
                "role".into(),
                SummaryRule::Enum(vec!["reader".into(), "editor".into()]),
            )]),
        )]),
        ..Default::default()
    };
    let audit = AuditService::new(repository, Arc::new(FixtureAuthority), config.clone())?;
    // Authenticate/authorize at the business boundary independently of audit access.
    let principal = "test-admin";
    if principal != "test-admin" {
        return Err(AuditError::Unauthorized.into());
    }
    let mut intent = AuditInput {
        id: "00000000-0000-0000-0000-000000000001".into(),
        occurred_at: 1,
        scope: fixture_scope(),
        action: "permission.change".into(),
        target: AuditTarget {
            kind: "membership".into(),
            id: "test-member".into(),
        },
        result: AuditResult::Intent,
        reason_code: "requested".into(),
        correlation_id: Some("test-request-1".into()),
        summary: BTreeMap::from([("role".into(), SummaryValue::Enum("editor".into()))]),
        relation: None,
    };
    // Stable operation ID: duplicate intent requires reconciliation, never replay.
    if audit.append(intent.clone(), principal).await?.status == AppendStatus::Inserted {
        let business_path = path.clone();
        tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
            let mut db = rusqlite::Connection::open(business_path)?;
            db.execute_batch(
                "CREATE TABLE fixture_members (id TEXT PRIMARY KEY, role TEXT NOT NULL);
                INSERT INTO fixture_members VALUES ('test-member','reader');",
            )?;
            let tx = db.transaction()?;
            let changed = tx.execute(
                "UPDATE fixture_members SET role='editor' WHERE id='test-member' AND role='reader'",
                [],
            )?;
            assert_eq!(changed, 1);
            tx.commit()?;
            let role: String = db.query_row(
                "SELECT role FROM fixture_members WHERE id='test-member'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(role, "editor");
            Ok(())
        })
        .await??;
        // Separate commits: failure here means effect may have happened; reconcile.
        intent.relation = Some(AuditRelation {
            kind: RelationKind::Outcome,
            event_id: intent.id.clone(),
        });
        intent.id = "00000000-0000-0000-0000-000000000002".into();
        intent.result = AuditResult::Success;
        intent.reason_code = "changed".into();
        audit.append(intent, principal).await?;
    }
    drop(audit);
    let audit = AuditService::new(
        Arc::new(SqliteAuditRepository::open(&path).await?),
        Arc::new(FixtureAuthority),
        config,
    )?;
    let page = audit
        .query(AuditQuery::new(fixture_scope()), principal)
        .await?;
    assert_eq!(page.events.len(), 2);
    println!("reader -> editor; reopened SQLite contains intent + success with trusted actor and request context");
    Ok(())
}
