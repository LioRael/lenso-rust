#![cfg(feature = "sqlite")]
use lenso_audit::*;
use std::sync::Arc;

fn scope(tenant: Option<&str>) -> AuditScope {
    AuditScope {
        tenant_id: tenant.map(str::to_owned),
        scope_id: "project".into(),
    }
}
fn event(n: u64, scope: AuditScope) -> AuditEvent {
    AuditEvent {
        input: AuditInput {
            id: format!("00000000-0000-0000-0000-{n:012}"),
            occurred_at: 1,
            scope,
            action: "permission.change".into(),
            target: AuditTarget {
                kind: "member".into(),
                id: "test-member".into(),
            },
            result: AuditResult::Success,
            reason_code: "changed".into(),
            correlation_id: Some("test-request".into()),
            summary: Summary::new(),
            relation: None,
        },
        recorded_at: 100,
        actor: AuditActor::System {
            system_id: "test-system".into(),
        },
    }
}
struct Authority;
impl AuditAuthority for Authority {
    type Principal = ();
    async fn resolve(
        &self,
        _: &(),
        _: &AuditScope,
        _: AuditOperation,
    ) -> Result<AuditActor, BoxError> {
        Ok(AuditActor::System {
            system_id: "test-system".into(),
        })
    }
}
#[tokio::test]
async fn restart_scope_filters_pagination_and_append_only() -> Result<(), BoxError> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("audit.sqlite");
    let repo = SqliteAuditRepository::open(&path).await?;
    assert!(!repo.durable_intents());
    for n in 1..=3 {
        repo.insert(event(n, scope(None))).await?;
    }
    for (n, recorded_at) in [(5, 99), (6, 101)] {
        let mut outside = event(n, scope(None));
        outside.recorded_at = recorded_at;
        repo.insert(outside).await?;
    }
    repo.insert(event(4, scope(Some("null")))).await?;
    drop(repo);
    let repo = Arc::new(SqliteAuditRepository::open(&path).await?);
    let service = AuditService::new(repo.clone(), Arc::new(Authority), AuditConfig::default())?;
    let mut query = AuditQuery::new(scope(None));
    query.limit = Some(2);
    query.action = Some("permission.change".into());
    query.target = Some(event(1, scope(None)).input.target);
    query.result = Some(AuditResult::Success);
    query.correlation_id = Some("test-request".into());
    query.recorded_from = Some(100);
    query.recorded_to = Some(100);
    let first = service.query(query.clone(), &()).await?;
    assert_eq!(
        first
            .events
            .iter()
            .map(|e| e.input.id.clone())
            .collect::<Vec<_>>(),
        vec![
            event(3, scope(None)).input.id,
            event(2, scope(None)).input.id
        ]
    );
    query.cursor = first.next_cursor;
    let second = service.query(query.clone(), &()).await?;
    assert_eq!(second.events, vec![event(1, scope(None))]);
    assert!(second.next_cursor.is_none());
    query.action = Some("other.action".into());
    assert!(service.query(query, &()).await?.events.is_empty());
    assert!(repo
        .get(&scope(Some("null")), &event(1, scope(None)).input.id)
        .await?
        .is_none());
    let db = rusqlite::Connection::open(path)?;
    assert!(db
        .execute("UPDATE lenso_audit_v1 SET payload='{}'", [])
        .is_err());
    assert!(db.execute("DELETE FROM lenso_audit_v1", []).is_err());
    Ok(())
}

#[tokio::test]
async fn sql_bounds_skip_decoding_and_keep_residual_page_matches() -> Result<(), BoxError> {
    let dir = tempfile::tempdir()?;
    let queries = [
        (
            "lower",
            AuditQuery {
                recorded_from: Some(100),
                ..AuditQuery::new(scope(None))
            },
            90,
        ),
        (
            "upper",
            AuditQuery {
                recorded_to: Some(100),
                ..AuditQuery::new(scope(None))
            },
            110,
        ),
        (
            "cursor",
            AuditQuery {
                cursor: Some(AuditCursor {
                    recorded_at: 100,
                    id: event(5, scope(None)).input.id,
                }),
                ..AuditQuery::new(scope(None))
            },
            100,
        ),
    ];
    for (name, mut query, excluded_time) in queries {
        let path = dir.path().join(format!("{name}.sqlite"));
        let repo = SqliteAuditRepository::open(&path).await?;
        let memory = MemoryAuditRepository::new(100)?;
        for n in 1..=10 {
            let mut row = event(n, scope(None));
            row.recorded_at = match n {
                9 => 90,
                10 => 110,
                _ => 100,
            };
            if n >= 6 || n == 3 {
                row.input.action = "other.action".into();
            }
            repo.insert(row.clone()).await?;
            memory.insert(row).await?;
        }
        query.action = Some("permission.change".into());

        // An excluded payload must not be decoded merely to apply an indexed bound.
        // Only this owned fixture bypasses the append API to insert malformed JSON.
        let db = rusqlite::Connection::open(&path)?;
        db.execute(
            "INSERT INTO lenso_audit_v1(scope,id,recorded_at,payload) VALUES (?1,?2,?3,?4)",
            rusqlite::params![
                serde_json::to_string(&query.scope)?,
                event(999, scope(None)).input.id,
                excluded_time,
                "not-json",
            ],
        )?;
        let expected = memory.list(&query, 100).await?;
        assert!(expected.len() > 2);
        assert_eq!(repo.list(&query, 100).await?, expected);
        assert_eq!(repo.list(&query, 2).await?, expected[..2]);
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_connections_classify_and_preserve_original() -> Result<(), BoxError> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("audit.sqlite");
    let a = SqliteAuditRepository::open(&path).await?;
    let b = SqliteAuditRepository::open(&path).await?;
    let original = event(1, scope(Some("test-tenant")));
    let (left, right) = tokio::join!(a.insert(original.clone()), b.insert(original.clone()));
    let statuses = [left?, right?];
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == InsertStatus::Inserted)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == InsertStatus::Duplicate)
            .count(),
        1
    );
    let mut retry = original.clone();
    retry.recorded_at += 1;
    assert_eq!(b.insert(retry.clone()).await?, InsertStatus::Duplicate);
    retry.actor = AuditActor::System {
        system_id: "different".into(),
    };
    assert_eq!(b.insert(retry).await?, InsertStatus::Conflict);
    assert_eq!(
        a.get(&original.input.scope, &original.input.id).await?,
        Some(original)
    );
    Ok(())
}
#[tokio::test]
async fn sensitive_input_never_reaches_file() -> Result<(), BoxError> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("audit.sqlite");
    let repo = Arc::new(SqliteAuditRepository::open(&path).await?);
    let service = AuditService::new(repo.clone(), Arc::new(Authority), AuditConfig::default())?;
    let mut input = event(1, scope(None)).input;
    input.summary.insert(
        "password".into(),
        SummaryValue::Enum("TEST-ONLY-SECRET-MARKER".into()),
    );
    assert_eq!(
        service.append(input.clone(), &()).await.unwrap_err(),
        AuditError::InvalidInput
    );
    assert!(repo.get(&input.scope, &input.id).await?.is_none());
    let bytes = std::fs::read(path)?;
    assert!(!bytes
        .windows(b"TEST-ONLY-SECRET-MARKER".len())
        .any(|v| v == b"TEST-ONLY-SECRET-MARKER"));
    Ok(())
}
