mod support;
use lenso_audit::*;
use std::sync::Arc;
use support::*;

fn event(n: u64, recorded_at: u64) -> AuditEvent {
    AuditEvent {
        input: input(n),
        recorded_at,
        actor: AuditActor::System {
            system_id: "worker".into(),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_and_conflicts_are_atomic_and_capacity_never_evicts() {
    let repository = Arc::new(MemoryAuditRepository::new(1).unwrap());
    assert!(!repository.durable_intents());
    let barrier = Arc::new(tokio::sync::Barrier::new(32));
    let mut tasks = Vec::new();
    for n in 0..32 {
        let repository = repository.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let mut candidate = event(1, n);
            if n % 2 == 0 {
                candidate.input.reason_code = "changed".into();
            }
            barrier.wait().await;
            repository.insert(candidate).await.unwrap()
        }));
    }
    let mut counts = [0; 3];
    for task in tasks {
        counts[match task.await.unwrap() {
            InsertStatus::Inserted => 0,
            InsertStatus::Duplicate => 1,
            InsertStatus::Conflict => 2,
        }] += 1;
    }
    assert_eq!(counts, [1, 15, 16]);
    let original = repository.get(&scope(), &id(1)).await.unwrap().unwrap();
    assert!(repository.insert(event(2, 0)).await.is_err());
    let mut returned = original.clone();
    returned.input.reason_code = "mutated-copy".into();
    assert_eq!(
        repository.get(&scope(), &id(1)).await.unwrap(),
        Some(original)
    );
}

#[tokio::test]
async fn exact_scope_keys_distinguish_absent_tenant_literal_null_and_delimiter_boundaries() {
    let repository = MemoryAuditRepository::new(20).unwrap();
    let scopes = [
        AuditScope {
            tenant_id: None,
            scope_id: "scope".into(),
        },
        AuditScope {
            tenant_id: Some("null".into()),
            scope_id: "scope".into(),
        },
        AuditScope {
            tenant_id: Some("a:b".into()),
            scope_id: "c".into(),
        },
        AuditScope {
            tenant_id: Some("a".into()),
            scope_id: "b:c".into(),
        },
        AuditScope {
            tenant_id: None,
            scope_id: "other".into(),
        },
    ];
    for scope in scopes {
        let mut candidate = event(1, 10);
        candidate.input.scope = scope.clone();
        assert_eq!(
            repository.insert(candidate.clone()).await.unwrap(),
            InsertStatus::Inserted
        );
        assert_eq!(
            repository.get(&scope, &id(1)).await.unwrap(),
            Some(candidate.clone())
        );
        assert_eq!(
            repository.list(&AuditQuery::new(scope), 10).await.unwrap(),
            vec![candidate]
        );
    }
    assert!(repository.get(&scope(), &id(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn pages_have_exclusive_tie_breaking_and_inclusive_time_filters() {
    let f = Fixture::new(false);
    let early = AuditService::new(f.repository.clone(), f.authority.clone(), config())
        .unwrap()
        .with_clock(|| 10);
    let late = AuditService::new(f.repository.clone(), f.authority.clone(), config())
        .unwrap()
        .with_clock(|| 30);
    early.append(input(1), "alice").await.unwrap();
    late.append(input(2), "alice").await.unwrap();
    late.append(input(3), "alice").await.unwrap();
    let mut query = AuditQuery::new(scope());
    query.limit = Some(2);
    let page = f.service.query(query.clone(), "alice").await.unwrap();
    assert_eq!(
        page.events
            .iter()
            .map(|e| e.input.id.clone())
            .collect::<Vec<_>>(),
        vec![id(3), id(2)]
    );
    assert_eq!(
        page.next_cursor,
        Some(AuditCursor {
            recorded_at: 30,
            id: id(2)
        })
    );
    query.cursor = page.next_cursor;
    let next = f.service.query(query, "alice").await.unwrap();
    assert_eq!(
        next.events
            .iter()
            .map(|e| e.input.id.clone())
            .collect::<Vec<_>>(),
        vec![id(1)]
    );
    assert_eq!(next.next_cursor, None);
    let mut query = AuditQuery::new(scope());
    query.recorded_from = Some(30);
    query.recorded_to = Some(30);
    query.action = Some("note.remove".into());
    query.target = Some(input(2).target);
    query.result = Some(AuditResult::Success);
    query.correlation_id = Some("request-1".into());
    assert_eq!(
        f.service
            .query(query.clone(), "alice")
            .await
            .unwrap()
            .events
            .len(),
        2
    );
    let mut other_action = query.clone();
    other_action.action = Some("note.create".into());
    assert!(f
        .service
        .query(other_action, "alice")
        .await
        .unwrap()
        .events
        .is_empty());
    let mut other_target = query.clone();
    other_target.target.as_mut().unwrap().id = "note-2".into();
    assert!(f
        .service
        .query(other_target, "alice")
        .await
        .unwrap()
        .events
        .is_empty());
    let mut other_correlation = query.clone();
    other_correlation.correlation_id = Some("request-2".into());
    assert!(f
        .service
        .query(other_correlation, "alice")
        .await
        .unwrap()
        .events
        .is_empty());
    query.result = Some(AuditResult::Denied);
    assert!(f
        .service
        .query(query, "alice")
        .await
        .unwrap()
        .events
        .is_empty());
}

struct BrokenRepository {
    returned: AuditEvent,
    duplicate: bool,
}
impl AuditRepository for BrokenRepository {
    async fn insert(&self, _: AuditEvent) -> Result<InsertStatus, BoxError> {
        Ok(if self.duplicate {
            InsertStatus::Duplicate
        } else {
            InsertStatus::Inserted
        })
    }
    async fn get(&self, _: &AuditScope, _: &str) -> Result<Option<AuditEvent>, BoxError> {
        Ok(Some(self.returned.clone()))
    }
    async fn list(&self, _: &AuditQuery, _: usize) -> Result<Vec<AuditEvent>, BoxError> {
        Ok(vec![self.returned.clone()])
    }
}

#[tokio::test]
async fn service_rejects_cross_scope_wrong_id_and_misclassified_duplicate_provider_results() {
    for mutation in 0..3 {
        let mut returned = event(1, 10);
        returned.actor = AuditActor::Subject {
            kind: SubjectKind::User,
            realm_id: "https://issuer.example".into(),
            subject_id: "auth0|alice".into(),
        };
        match mutation {
            0 => returned.input.scope.tenant_id = Some("other".into()),
            1 => returned.input.id = id(99),
            _ => returned.input.reason_code = "mismatch".into(),
        }
        let service = AuditService::new(
            Arc::new(BrokenRepository {
                returned,
                duplicate: true,
            }),
            Arc::new(Authority::default()),
            config(),
        )
        .unwrap();
        assert_eq!(
            service.append(input(1), "alice").await.unwrap_err(),
            AuditError::StorageFailed
        );
        if mutation < 2 {
            assert_eq!(
                service.get(&scope(), &id(1), "alice").await.unwrap_err(),
                AuditError::StorageFailed
            );
        }
        if mutation == 0 {
            assert_eq!(
                service
                    .query(AuditQuery::new(scope()), "alice")
                    .await
                    .unwrap_err(),
                AuditError::StorageFailed
            );
        }
        let mut correction = input(2);
        correction.relation = Some(AuditRelation {
            kind: RelationKind::Correction,
            event_id: id(1),
        });
        if mutation < 2 {
            assert_eq!(
                service.append(correction, "alice").await.unwrap_err(),
                AuditError::StorageFailed
            );
        }
    }
}
