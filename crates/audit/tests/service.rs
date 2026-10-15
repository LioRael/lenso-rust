mod support;
use lenso_audit::*;
use std::{
    error::Error,
    sync::{atomic::Ordering, Arc},
};
use support::*;

#[tokio::test]
async fn authorization_and_validation_precede_storage_and_revalidate_each_call() {
    let f = Fixture::new(false);
    let event = f.service.append(input(1), "alice").await.unwrap().event;
    assert_eq!(
        event.actor,
        AuditActor::Subject {
            kind: SubjectKind::User,
            realm_id: "https://issuer.example".into(),
            subject_id: "auth0|alice".into()
        }
    );
    let before = f.repository.calls.load(Ordering::SeqCst);
    for principal in ["bob", "forged", ""] {
        assert_eq!(
            f.service.append(input(2), principal).await.unwrap_err(),
            AuditError::Unauthorized
        );
        assert_eq!(
            f.service
                .get(&scope(), &id(1), principal)
                .await
                .unwrap_err(),
            AuditError::Unauthorized
        );
        assert_eq!(
            f.service
                .get(&scope(), &id(99), principal)
                .await
                .unwrap_err(),
            AuditError::Unauthorized
        );
        assert_eq!(
            f.service
                .query(AuditQuery::new(scope()), principal)
                .await
                .unwrap_err(),
            AuditError::Unauthorized
        );
    }
    let mut foreign = AuditQuery::new(scope());
    foreign.scope.tenant_id = Some("tenant-b".into());
    assert_eq!(
        f.service.query(foreign, "alice").await.unwrap_err(),
        AuditError::Unauthorized
    );
    f.authority.revoked.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service
            .query(AuditQuery::new(scope()), "alice")
            .await
            .unwrap_err(),
        AuditError::Unauthorized
    );
    assert_eq!(f.repository.calls.load(Ordering::SeqCst), before);
    assert!(f
        .authority
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(_, op)| *op == AuditOperation::Query));
}

#[tokio::test]
async fn input_and_policy_boundaries_fail_without_provider_calls() {
    let f = Fixture::new(false);
    let mut cases = Vec::new();
    for (key, value) in [
        ("token", SummaryValue::Enum("secret".into())),
        ("state", SummaryValue::Enum("Bearer-secret".into())),
        ("count", SummaryValue::Integer(11)),
        ("removed", SummaryValue::Enum("yes".into())),
    ] {
        let mut value_input = input(1);
        value_input.summary.insert(key.into(), value);
        cases.push(value_input);
    }
    cases.extend([
        AuditInput {
            occurred_at: u64::MAX,
            ..input(1)
        },
        AuditInput {
            id: "ABCDEF00-0000-0000-0000-000000000001".into(),
            ..input(1)
        },
        AuditInput {
            reason_code: "a".repeat(65),
            ..input(1)
        },
        AuditInput {
            correlation_id: Some("https://user:password@example.test".into()),
            ..input(1)
        },
        AuditInput {
            target: AuditTarget {
                kind: "note".into(),
                id: "a".repeat(257),
            },
            ..input(1)
        },
        AuditInput {
            relation: Some(AuditRelation {
                kind: RelationKind::Correction,
                event_id: id(1),
            }),
            ..input(1)
        },
    ]);
    for bad in cases {
        assert_eq!(
            f.service.append(bad, "alice").await.unwrap_err(),
            AuditError::InvalidInput
        );
    }
    assert_eq!(f.repository.calls.load(Ordering::SeqCst), 0);
    assert!(f.authority.calls.lock().unwrap().is_empty());
    let mut bad_config = config();
    bad_config
        .summary_policy
        .get_mut("note.remove")
        .unwrap()
        .insert("credentialDigest".into(), SummaryRule::Boolean);
    assert_eq!(bad_config.validate(), Err(AuditError::InvalidInput));
    bad_config = config();
    bad_config
        .summary_policy
        .get_mut("note.remove")
        .unwrap()
        .insert(
            "count".into(),
            SummaryRule::Integer {
                min: 0,
                max: i64::MAX,
            },
        );
    assert_eq!(bad_config.validate(), Err(AuditError::InvalidInput));
    let mut query = AuditQuery::new(scope());
    query.recorded_from = Some(20);
    query.recorded_to = Some(19);
    assert_eq!(
        f.service.query(query, "alice").await.unwrap_err(),
        AuditError::InvalidInput
    );
    let mut query = AuditQuery::new(scope());
    query.limit = Some(501);
    assert_eq!(
        f.service.query(query, "alice").await.unwrap_err(),
        AuditError::InvalidInput
    );
}

#[tokio::test]
async fn duplicate_ignores_recorded_time_and_corrections_do_not_mutate_original() {
    let f = Fixture::new(false);
    let original = f.service.append(input(1), "alice").await.unwrap();
    let later = AuditService::new(f.repository.clone(), f.authority.clone(), config())
        .unwrap()
        .with_clock(|| 999);
    let retry = later.append(input(1), "alice").await.unwrap();
    assert_eq!(retry.status, AppendStatus::Duplicate);
    assert_eq!(retry.event, original.event);
    let mut changed = input(1);
    changed.reason_code = "changed".into();
    assert_eq!(
        later.append(changed, "alice").await.unwrap_err(),
        AuditError::DuplicateConflict
    );
    let mut correction = input(2);
    correction.relation = Some(AuditRelation {
        kind: RelationKind::Correction,
        event_id: id(1),
    });
    correction.result = AuditResult::Failure;
    f.service.append(correction, "alice").await.unwrap();
    assert_eq!(
        f.service.get(&scope(), &id(1), "alice").await.unwrap(),
        Some(original.event)
    );
    let mut missing = input(3);
    missing.relation = Some(AuditRelation {
        kind: RelationKind::Correction,
        event_id: id(99),
    });
    assert_eq!(
        f.service.append(missing, "alice").await.unwrap_err(),
        AuditError::RelationMissing
    );
    let mut invalid_outcome = input(4);
    invalid_outcome.relation = Some(AuditRelation {
        kind: RelationKind::Outcome,
        event_id: id(1),
    });
    assert_eq!(
        f.service
            .append(invalid_outcome, "alice")
            .await
            .unwrap_err(),
        AuditError::InvalidInput
    );
    f.service.append(intent(5), "alice").await.unwrap();
    let mut wrong_target = input(6);
    wrong_target.target.id = "other-note".into();
    wrong_target.relation = Some(AuditRelation {
        kind: RelationKind::Outcome,
        event_id: id(5),
    });
    assert_eq!(
        f.service.append(wrong_target, "alice").await.unwrap_err(),
        AuditError::InvalidInput
    );
}

#[tokio::test]
async fn best_effort_only_downgrades_storage_and_sanitizes_diagnostics() {
    let f = Fixture::new(false);
    let silent = AuditService::new(f.repository.clone(), f.authority.clone(), config()).unwrap();
    assert_eq!(
        silent
            .append_best_effort(input(1), "alice")
            .await
            .unwrap_err(),
        AuditError::DiagnosticsRequired
    );
    f.repository.fail_writes.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service
            .append_best_effort(input(1), "alice")
            .await
            .unwrap(),
        BestEffortResult::Unconfirmed
    );
    assert_eq!(
        f.service
            .append_best_effort(input(1), "bob")
            .await
            .unwrap_err(),
        AuditError::Unauthorized
    );
    assert_eq!(
        f.service
            .append_best_effort(
                AuditInput {
                    reason_code: "bad value".into(),
                    ..input(1)
                },
                "alice"
            )
            .await
            .unwrap_err(),
        AuditError::InvalidInput
    );
    assert_eq!(
        *f.reporter.entries.lock().unwrap(),
        vec![AuditDiagnostic {
            mode: AuditMode::BestEffort,
            stage: AuditStage::Append
        }]
    );
    f.reporter.fail.store(true, Ordering::SeqCst);
    let error = f
        .service
        .append_best_effort(input(1), "alice")
        .await
        .unwrap_err();
    assert_eq!(error, AuditError::DiagnosticsFailed);
    assert!(error.source().is_none());
    assert!(!format!("{error:?} {error}").contains("must-not-escape"));
    f.reporter.fail.store(false, Ordering::SeqCst);
    f.repository.fail_writes.store(false, Ordering::SeqCst);
    f.repository.fail_after_commit.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service
            .append_best_effort(input(1), "alice")
            .await
            .unwrap(),
        BestEffortResult::Unconfirmed
    );
    f.repository
        .fail_after_commit
        .store(false, Ordering::SeqCst);
    assert!(matches!(
        f.service
            .append_best_effort(input(1), "alice")
            .await
            .unwrap(),
        BestEffortResult::Confirmed(result) if result.status == AppendStatus::Duplicate
    ));
    let changed = AuditInput {
        reason_code: "different".into(),
        ..input(1)
    };
    assert_eq!(
        f.service
            .append_best_effort(changed, "alice")
            .await
            .unwrap_err(),
        AuditError::DuplicateConflict
    );
}

#[tokio::test]
async fn strict_requires_attestation_and_preserves_unknown_outcomes_for_reconciliation() {
    let unavailable = Fixture::new(false);
    assert_eq!(
        unavailable
            .service
            .prepare(intent(1), "alice")
            .await
            .unwrap_err(),
        AuditError::StrictUnavailable
    );
    assert_eq!(unavailable.repository.calls.load(Ordering::SeqCst), 0);
    // This fixture attests only to exercise protocol paths; it proves no durability.
    let f = Fixture::new(true);
    f.repository.fail_writes.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service.prepare(intent(1), "alice").await.unwrap_err(),
        AuditError::StorageFailed
    );
    f.repository.fail_writes.store(false, Ordering::SeqCst);
    let PrepareResult::Ready(receipt) = f.service.prepare(intent(1), "alice").await.unwrap() else {
        panic!("expected ready")
    };
    assert!(
        matches!(f.service.prepare(intent(1), "alice").await.unwrap(), PrepareResult::AlreadyRecorded { intent_id } if intent_id == id(1))
    );
    let other = AuditService::new(f.repository.clone(), f.authority.clone(), config()).unwrap();
    assert_eq!(
        other.complete(&receipt, outcome(2)).await.unwrap_err(),
        AuditError::InvalidReceipt
    );
    f.authority.revoked.store(true, Ordering::SeqCst);
    f.repository.fail_writes.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service.complete(&receipt, outcome(2)).await.unwrap_err(),
        AuditError::OutcomeUnknown {
            intent_id: id(1),
            diagnostics_failed: false
        }
    );
    f.reporter.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service.complete(&receipt, outcome(2)).await.unwrap_err(),
        AuditError::OutcomeUnknown {
            intent_id: id(1),
            diagnostics_failed: true
        }
    );
    f.reporter.fail.store(false, Ordering::SeqCst);
    f.repository.fail_writes.store(false, Ordering::SeqCst);
    let completed = f
        .service
        .complete(&receipt.clone(), outcome(2))
        .await
        .unwrap();
    assert_eq!(
        f.service.complete(&receipt, outcome(2)).await.unwrap(),
        completed
    );
    assert_eq!(
        completed.actor,
        f.repository
            .memory
            .get(&scope(), &id(1))
            .await
            .unwrap()
            .unwrap()
            .actor
    );
    assert_eq!(
        completed.input.relation,
        Some(AuditRelation {
            kind: RelationKind::Outcome,
            event_id: id(1)
        })
    );
    let mut bad_outcome = outcome(3);
    bad_outcome
        .summary
        .insert("password".into(), SummaryValue::Enum("secret".into()));
    assert!(matches!(
        f.service.complete(&receipt, bad_outcome).await,
        Err(AuditError::OutcomeUnknown { .. })
    ));
    assert_eq!(
        f.repository
            .memory
            .list(&AuditQuery::new(scope()), 10)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_prepare_has_one_ready_and_lost_ack_does_not_readmit_effect() {
    let f = Fixture::new(true);
    let barrier = Arc::new(tokio::sync::Barrier::new(32));
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let audit = f.service.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            audit.prepare(intent(1), "alice").await.unwrap()
        }));
    }
    let mut ready = 0;
    for task in tasks {
        if matches!(task.await.unwrap(), PrepareResult::Ready(_)) {
            ready += 1;
        }
    }
    assert_eq!(ready, 1);
    f.repository.fail_after_commit.store(true, Ordering::SeqCst);
    assert_eq!(
        f.service.prepare(intent(2), "alice").await.unwrap_err(),
        AuditError::StorageFailed
    );
    f.repository
        .fail_after_commit
        .store(false, Ordering::SeqCst);
    assert!(matches!(
        f.service.prepare(intent(2), "alice").await.unwrap(),
        PrepareResult::AlreadyRecorded { .. }
    ));
}
