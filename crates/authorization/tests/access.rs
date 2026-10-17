mod support;
use lenso_authorization::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use support::*;

// Host-owned evidence: the adapter has no parser, token store or login flow.
struct Evidence;
#[derive(Clone)]
struct Session {
    principal: Principal,
    audience: String,
    expiry: u64,
    credential: Option<CredentialLimit>,
}
struct Host(Mutex<Option<Session>>);
impl AccessVerifier<Evidence> for Host {
    fn verify<'a>(
        &'a self,
        _: &'a Evidence,
        _: &'a Resource,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, Option<AccessIdentity>> {
        Box::pin(async move {
            Ok(self
                .0
                .lock()
                .unwrap()
                .as_ref()
                .map(|session| AccessIdentity {
                    principal: session.principal.clone(),
                    audience: session.audience.clone(),
                    session_expires_at: Some(session.expiry),
                    credential: session.credential.clone(),
                }))
        })
    }
}
fn host() -> Arc<Host> {
    Arc::new(Host(Mutex::new(Some(Session {
        principal: principal(),
        audience: "console".into(),
        expiry: 200,
        credential: None,
    }))))
}
fn access(
    host: Arc<dyn AccessVerifier<Evidence>>,
    options: AuthorizationOptions,
) -> AuthorizedAccess<Evidence> {
    AuthorizedAccess::new(
        "notes",
        "console",
        host,
        Authorization::new(options).unwrap(),
    )
    .unwrap()
}
async fn check(access: &AuthorizedAccess<Evidence>) -> Decision {
    access
        .check(Evidence, "read", request().resource, Attributes::new())
        .await
}

#[tokio::test]
async fn exact_realm_and_audience_are_required_before_any_allow_policy() {
    let host = host();
    let access = access(host.clone(), allow_options());
    assert!(check(&access).await.is_allowed());
    for (realm, audience, allowed) in [
        ("other", "console", false),
        ("notes", "api", false),
        ("notes", "console-extra", false),
        ("notes", "console", true),
    ] {
        {
            let mut session = host.0.lock().unwrap();
            let session = session.as_mut().unwrap();
            session.principal.realm_id = realm.into();
            session.audience = audience.into();
        }
        assert_eq!(
            check(&access).await.code,
            if allowed {
                ReasonCode::Allowed
            } else {
                ReasonCode::BoundaryDenied
            }
        );
    }
}

#[tokio::test]
async fn each_call_reads_renewal_and_revocation_without_extending_an_old_snapshot() {
    let host = host();
    let now = Arc::new(AtomicU64::new(100));
    let clock = now.clone();
    let mut config = allow_options();
    config.clock = Arc::new(move || Ok(clock.load(Ordering::SeqCst)));
    let access = access(host.clone(), config);
    assert!(check(&access).await.is_allowed());
    now.store(200, Ordering::SeqCst);
    assert_eq!(check(&access).await.code, ReasonCode::BoundaryDenied);
    // The existing host renews; the authorization adapter only observes it.
    host.0.lock().unwrap().as_mut().unwrap().expiry = 300;
    assert!(check(&access).await.is_allowed());
    *host.0.lock().unwrap() = None;
    assert_eq!(check(&access).await.code, ReasonCode::BoundaryDenied);
    assert_eq!(
        access
            .enforce(Evidence, "read", request().resource, Attributes::new())
            .await,
        Err(AuthorizationError)
    );
}

#[tokio::test]
async fn verified_credential_remains_a_ceiling_after_authoritative_resource_resolution() {
    struct Moved;
    impl ResourceResolver for Moved {
        fn resolve<'a>(
            &'a self,
            resource: &'a Resource,
            _: &'a Evaluation,
        ) -> EvaluationFuture<'a, Resource> {
            Box::pin(async move {
                let mut resource = resource.clone();
                resource.scope.id = "other".into();
                Ok(resource)
            })
        }
    }
    let host = host();
    host.0.lock().unwrap().as_mut().unwrap().credential = Some(CredentialLimit {
        permissions: vec![Permission::new("read", "note", scope())],
        expires_at: Some(180),
    });
    let access = access(host.clone(), allow_options());
    assert!(check(&access).await.is_allowed());
    assert!(
        !access
            .can(Evidence, "write", request().resource, Attributes::new())
            .await
    );
    let mut config = allow_options();
    config.resolve_resource = Some(Arc::new(Moved));
    let moved = AuthorizedAccess::new(
        "notes",
        "console",
        host,
        Authorization::new(config).unwrap(),
    )
    .unwrap();
    assert_eq!(check(&moved).await.code, ReasonCode::BoundaryDenied);
}

#[tokio::test]
async fn expiry_and_clock_rollback_refuse_a_late_allow() {
    for through_adapter in [false, true] {
        for finished_at in [179, 180, 99] {
            let now = Arc::new(AtomicU64::new(100));
            let clock = now.clone();
            let mut config = options();
            config.clock = Arc::new(move || Ok(clock.load(Ordering::SeqCst)));
            config.policies.push(policy(move |_, _| {
                now.store(finished_at, Ordering::SeqCst);
                Ok(Effect::Allow)
            }));
            let decision = if through_adapter {
                let host = host();
                host.0.lock().unwrap().as_mut().unwrap().expiry = 180;
                check(&access(host, config)).await
            } else {
                let mut req = request();
                req.credential = Some(CredentialLimit {
                    permissions: vec![Permission::new("read", "note", scope())],
                    expires_at: Some(180),
                });
                Authorization::new(config).unwrap().check(req).await
            };
            assert_eq!(
                decision.code,
                match finished_at {
                    179 => ReasonCode::Allowed,
                    180 => ReasonCode::BoundaryDenied,
                    _ => ReasonCode::EvaluationFailed,
                }
            );
        }
    }
}

#[tokio::test]
async fn renewal_during_an_inflight_check_cannot_extend_its_verified_snapshot() {
    let host = host();
    let renewing_host = host.clone();
    let now = Arc::new(AtomicU64::new(100));
    let clock = now.clone();
    let mut config = options();
    config.clock = Arc::new(move || Ok(clock.load(Ordering::SeqCst)));
    config.policies.push(policy(move |_, _| {
        renewing_host.0.lock().unwrap().as_mut().unwrap().expiry = 300;
        now.store(200, Ordering::SeqCst);
        Ok(Effect::Allow)
    }));
    let service = access(host, config);
    assert_eq!(check(&service).await.code, ReasonCode::BoundaryDenied);
    assert!(check(&service).await.is_allowed());
}

#[tokio::test]
async fn expiry_during_async_observation_refuses_the_final_result() {
    struct ObserverClock(Arc<AtomicU64>);
    impl Observer for ObserverClock {
        fn observe(&self, _: Decision) -> EvaluationFuture<'_, ()> {
            Box::pin(async {
                tokio::task::yield_now().await;
                self.0.store(200, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    let now = Arc::new(AtomicU64::new(100));
    let clock = now.clone();
    let mut config = allow_options();
    config.clock = Arc::new(move || Ok(clock.load(Ordering::SeqCst)));
    config.observe = Some(Arc::new(ObserverClock(now)));
    assert_eq!(
        check(&access(host(), config)).await.code,
        ReasonCode::BoundaryDenied
    );
}

struct BrokenHost;
impl AccessVerifier<Evidence> for BrokenHost {
    fn verify<'a>(
        &'a self,
        _: &'a Evidence,
        _: &'a Resource,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, Option<AccessIdentity>> {
        Box::pin(async { panic!("private provider detail") })
    }
}
struct PendingHost(Arc<Mutex<Option<Evaluation>>>);
impl AccessVerifier<Evidence> for PendingHost {
    fn verify<'a>(
        &'a self,
        _: &'a Evidence,
        _: &'a Resource,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Option<AccessIdentity>> {
        Box::pin(async move {
            *self.0.lock().unwrap() = Some(evaluation.clone());
            std::future::pending().await
        })
    }
}
#[tokio::test(start_paused = true)]
async fn provider_is_inside_the_same_fail_closed_deadline_and_cancellation_boundary() {
    let broken = access(Arc::new(BrokenHost), allow_options());
    assert_eq!(
        broken
            .check(Evidence, "unknown", request().resource, Attributes::new())
            .await
            .code,
        ReasonCode::UnknownAction
    );
    assert_eq!(
        check(&access(Arc::new(BrokenHost), allow_options()))
            .await
            .code,
        ReasonCode::EvaluationFailed
    );
    let captured = Arc::new(Mutex::new(None));
    let service = access(Arc::new(PendingHost(captured.clone())), allow_options());
    assert_eq!(check(&service).await.code, ReasonCode::Timeout);
    assert!(captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .cancellation
        .is_cancelled());
    let cancel = Cancellation::default();
    cancel.cancel();
    assert_eq!(
        service
            .check_with_cancellation(
                Evidence,
                "read",
                request().resource,
                Attributes::new(),
                cancel
            )
            .await
            .code,
        ReasonCode::Cancelled
    );
}
