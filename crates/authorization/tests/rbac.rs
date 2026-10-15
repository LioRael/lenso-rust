mod support;
use lenso_authorization::*;
use std::sync::Arc;
use support::*;

#[tokio::test]
async fn inheritance_is_exactly_scoped_and_identity_and_expiry_are_checked() {
    let rbac = Rbac::new(graph(), ["read", "write"]).unwrap();
    assert!(rbac.permits(&request(), &evaluation()).await.unwrap());
    let mut variants = vec![];
    for scope in [
        Scope::new("organization", "home"),
        Scope::new("personal", "home/child"),
        Scope::new("personal", "other"),
    ] {
        let mut facts = request();
        facts.resource.scope = scope;
        variants.push(facts);
    }
    let mut facts = request();
    facts.principal.as_mut().unwrap().realm_id = "another-realm".into();
    variants.push(facts);
    let mut facts = request();
    facts.principal.as_mut().unwrap().kind = "service".into();
    variants.push(facts);
    let mut facts = request();
    facts.principal.as_mut().unwrap().subject_id = "bob".into();
    variants.push(facts);
    let mut facts = request();
    facts.principal = None;
    variants.push(facts);
    let mut facts = request();
    facts.action = "write".into();
    variants.push(facts);
    for facts in variants {
        assert!(!rbac.permits(&facts, &evaluation()).await.unwrap());
    }
    let mut expired = evaluation();
    expired.now = 200;
    assert!(!rbac.permits(&request(), &expired).await.unwrap());
    let mut specific = graph();
    specific.roles[0].permissions[0].resource_id = Some("one".into());
    let rbac = Rbac::new(specific, ["read"]).unwrap();
    assert!(rbac.permits(&request(), &evaluation()).await.unwrap());
    let mut facts = request();
    facts.resource.id = "two".into();
    assert!(!rbac.permits(&facts, &evaluation()).await.unwrap());
}

#[tokio::test]
async fn rbac_predicate_composes_with_constraints_instead_of_granting_independent_or() {
    let mut config = options();
    config.rules.push(rule(
        Outcome::Allow,
        Condition::All(vec![
            Condition::Predicate(Arc::new(Rbac::new(graph(), ["read"]).unwrap())),
            Condition::Predicate(predicate(|facts, _| {
                Ok(facts
                    .resource
                    .attributes
                    .get("state")
                    .and_then(|v| v.as_str())
                    == Some("closed"))
            })),
        ]),
    ));
    assert!(!Authorization::new(config).unwrap().can(request()).await);
}

#[test]
fn rejects_bad_graphs_without_partial_grants() {
    let mut invalid = vec![];
    let mut candidate = graph();
    candidate.roles.push(candidate.roles[0].clone());
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.bindings.push(candidate.bindings[0].clone());
    invalid.push(candidate);
    let mut candidate = graph();
    let duplicate = candidate.roles[0].permissions[0].clone();
    candidate.roles[0].permissions.push(duplicate);
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[0].permissions[0].action = "unknown".into();
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[0].permissions[0].scope.id = "other".into();
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[0].inherits.push("member".into());
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[0].inherits.push("reader".into());
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[1].inherits.push("reader".into());
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[0].scope.id = "other".into();
    candidate.roles[0].permissions.clear();
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.roles[1].inherits = vec!["missing".into()];
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.bindings[0].role_id = "missing".into();
    invalid.push(candidate);
    let mut candidate = graph();
    candidate.bindings[0].principal.realm_id.clear();
    invalid.push(candidate);
    let candidate = RoleGraph {
        roles: (0..513)
            .map(|i| Role {
                id: i.to_string(),
                scope: scope(),
                permissions: vec![],
                inherits: vec![],
            })
            .collect(),
        bindings: vec![],
    };
    invalid.push(candidate);
    for candidate in invalid {
        assert!(Rbac::new(candidate, ["read", "write"]).is_err());
    }
    assert!(Rbac::new(graph(), ["read", "read"]).is_err());
}

#[tokio::test]
async fn fresh_read_observes_revocation_while_old_snapshot_remains_immutable() {
    let original = graph();
    let store = Arc::new(MemoryRoleStore::new(original.clone(), ["read"]).unwrap());
    let retained = store.snapshot().unwrap();
    let mut config = options();
    config.policies.push(Arc::new(
        Rbac::from_source(["read"], store.clone()).unwrap(),
    ));
    let engine = Authorization::new(config).unwrap();
    assert!(engine.can(request()).await);
    let mut next = original;
    next.bindings.clear();
    assert!(store
        .compare_and_swap(retained.revision, next, &evaluation())
        .unwrap());
    assert!(!engine.can(request()).await);
    assert_eq!(retained.graph.bindings.len(), 1);
    assert_eq!(retained.revision, 0);
    assert!(!store
        .compare_and_swap(retained.revision, graph(), &evaluation())
        .unwrap());
    assert_eq!(store.snapshot().unwrap().revision, 1);
    let cancelled = evaluation();
    cancelled.cancellation.cancel();
    assert!(!store.compare_and_swap(1, graph(), &cancelled).unwrap());
    let mut invalid = graph();
    invalid.roles.clear();
    assert!(store.compare_and_swap(1, invalid, &evaluation()).is_err());
    assert_eq!(store.snapshot().unwrap().revision, 1);
    // Re-grant gets a new monotonic revision; the original revision never becomes valid.
    assert!(store.compare_and_swap(1, graph(), &evaluation()).unwrap());
    assert!(!store
        .compare_and_swap(0, RoleGraph::default(), &evaluation())
        .unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_cas_has_exactly_one_winner() {
    let store = Arc::new(MemoryRoleStore::new(graph(), ["read"]).unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = vec![];
    for _ in 0..2 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .compare_and_swap(0, RoleGraph::default(), &evaluation())
                .unwrap()
        }));
    }
    barrier.wait().await;
    let mut wins = 0;
    for task in tasks {
        wins += usize::from(task.await.unwrap());
    }
    assert_eq!(wins, 1);
    assert_eq!(store.snapshot().unwrap().revision, 1);
}

struct InvalidSource(bool);
impl RoleSource for InvalidSource {
    fn read<'a>(&'a self, _: &'a Evaluation) -> EvaluationFuture<'a, Arc<RoleSnapshot>> {
        Box::pin(async move {
            if self.0 {
                return Err(EvaluationError);
            }
            let mut graph = graph();
            graph.roles[0].permissions[0].scope.id = "other".into();
            Ok(Arc::new(RoleSnapshot { revision: 9, graph }))
        })
    }
}
#[tokio::test]
async fn source_error_and_invalid_snapshot_override_an_independent_allow() {
    for fails in [false, true] {
        let mut config = allow_options();
        config.policies.push(Arc::new(
            Rbac::from_source(["read"], Arc::new(InvalidSource(fails))).unwrap(),
        ));
        assert_eq!(
            Authorization::new(config)
                .unwrap()
                .check(request())
                .await
                .code,
            ReasonCode::EvaluationFailed
        );
    }
}

#[test]
fn permission_ceiling_never_widens_resource_specific_grants() {
    let broad = Permission::new("read", "note", scope());
    let mut narrow = broad.clone();
    narrow.resource_id = Some("one".into());
    assert!(broad.covers(&narrow));
    assert!(!narrow.covers(&broad));
    let mut other = narrow.clone();
    other.resource_id = Some("two".into());
    assert!(!narrow.covers(&other));
}

#[tokio::test]
async fn inflight_snapshot_is_stable_and_revocation_is_seen_by_next_read() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Semaphore;
    struct PausedSource {
        store: Arc<MemoryRoleStore>,
        first: AtomicBool,
        entered: Semaphore,
        release: Semaphore,
    }
    impl RoleSource for PausedSource {
        fn read<'a>(&'a self, _: &'a Evaluation) -> EvaluationFuture<'a, Arc<RoleSnapshot>> {
            Box::pin(async move {
                let snapshot = self.store.snapshot().map_err(|_| EvaluationError)?;
                if self.first.swap(false, Ordering::SeqCst) {
                    self.entered.add_permits(1);
                    self.release.acquire().await.unwrap().forget();
                }
                Ok(snapshot)
            })
        }
    }
    let store = Arc::new(MemoryRoleStore::new(graph(), ["read"]).unwrap());
    let source = Arc::new(PausedSource {
        store: store.clone(),
        first: AtomicBool::new(true),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut config = options();
    config.policies.push(Arc::new(
        Rbac::from_source(["read"], source.clone()).unwrap(),
    ));
    let engine = Authorization::new(config).unwrap();
    let pending = tokio::spawn({
        let engine = engine.clone();
        async move { engine.check(request()).await }
    });
    source.entered.acquire().await.unwrap().forget();
    assert!(store
        .compare_and_swap(0, RoleGraph::default(), &evaluation())
        .unwrap());
    source.release.add_permits(1);
    assert!(pending.await.unwrap().is_allowed());
    assert_eq!(engine.check(request()).await.code, ReasonCode::DefaultDeny);
}
