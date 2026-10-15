mod support;
use lenso_authorization::*;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use support::*;

#[tokio::test]
async fn conflicts_are_order_independent_and_default_deny() {
    for left in [Effect::Allow, Effect::Deny, Effect::Abstain] {
        for right in [Effect::Allow, Effect::Deny, Effect::Abstain] {
            let mut options = support::options();
            options.policies = vec![policy(move |_, _| Ok(left)), policy(move |_, _| Ok(right))];
            let expected = if [left, right].contains(&Effect::Deny) {
                ReasonCode::ExplicitDeny
            } else if [left, right].contains(&Effect::Allow) {
                ReasonCode::Allowed
            } else {
                ReasonCode::DefaultDeny
            };
            assert_eq!(
                Authorization::new(options)
                    .unwrap()
                    .check(request())
                    .await
                    .code,
                expected
            );
        }
    }
    assert_eq!(
        Authorization::new(options())
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::DefaultDeny
    );
    let mut options = allow_options();
    options.rules.push(rule(Outcome::Deny, Condition::always()));
    let engine = Authorization::new(options).unwrap();
    assert_eq!(engine.check(request()).await.code, ReasonCode::ExplicitDeny);
    assert_eq!(
        engine.enforce(request()).await.unwrap_err().to_string(),
        "Access denied."
    );
}

#[tokio::test]
async fn only_applicable_rules_match_and_scope_and_star_are_literal() {
    let mut options = support::options();
    let mut allow = rule(Outcome::Allow, Condition::always());
    allow.scope = Some(scope());
    allow.resource_id = Some("one".into());
    options.rules.push(allow);
    let mut deny = rule(Outcome::Deny, Condition::always());
    deny.resource_id = Some("other".into());
    options.rules.push(deny);
    let engine = Authorization::new(options).unwrap();
    assert!(engine.can(request()).await);
    let mut variants = vec![];
    for scope in [
        Scope::new("personal", "home/child"),
        Scope::new("organization", "home"),
        Scope::new("personal", "*"),
    ] {
        let mut facts = request();
        facts.resource.scope = scope;
        variants.push(facts);
    }
    let mut facts = request();
    facts.resource.id = "*".into();
    variants.push(facts);
    let mut facts = request();
    facts.resource.kind = "notes".into();
    variants.push(facts);
    let mut facts = request();
    facts.action = "write".into();
    variants.push(facts);
    for facts in variants {
        assert!(!engine.can(facts).await);
    }
    let mut facts = request();
    facts.action = "unknown".into();
    assert_eq!(engine.check(facts).await.code, ReasonCode::UnknownAction);
}

#[tokio::test]
async fn identity_and_credential_limits_precede_any_allow_extension() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut options = support::options();
    options.identity = IdentityRequirements {
        required: true,
        realms: Some(vec!["notes".into()]),
        audiences: Some(vec!["notes-read".into()]),
        credential_required: true,
    };
    let counter = calls.clone();
    options.policies.push(policy(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(Effect::Allow)
    }));
    let engine = Authorization::new(options).unwrap();
    let mut valid = request();
    valid.audience = Some("notes-read".into());
    valid.credential = Some(CredentialLimit {
        permissions: vec![Permission::new("read", "note", scope())],
        expires_at: Some(200),
    });
    let mut variants = vec![];
    let mut facts = valid.clone();
    facts.principal = None;
    variants.push(facts);
    let mut facts = valid.clone();
    facts.principal.as_mut().unwrap().realm_id = "other".into();
    variants.push(facts);
    let mut facts = valid.clone();
    facts.audience = Some("other".into());
    variants.push(facts);
    let mut facts = valid.clone();
    facts.credential = None;
    variants.push(facts);
    let mut facts = valid.clone();
    facts.credential.as_mut().unwrap().expires_at = Some(100);
    variants.push(facts);
    let mut facts = valid.clone();
    facts.credential.as_mut().unwrap().permissions.clear();
    variants.push(facts);
    let mut facts = valid.clone();
    facts
        .credential
        .as_mut()
        .unwrap()
        .permissions
        .push(Permission::new("unknown", "note", scope()));
    variants.push(facts);
    let mut facts = valid.clone();
    facts.action = "write".into();
    variants.push(facts);
    let mut facts = valid.clone();
    facts.resource.scope.id = "home/child".into();
    variants.push(facts);
    let mut facts = valid.clone();
    facts.credential.as_mut().unwrap().permissions[0].resource_id = Some("other".into());
    variants.push(facts);
    for facts in variants {
        assert_eq!(engine.check(facts).await.code, ReasonCode::BoundaryDenied);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(engine.can(valid).await);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn conditions_do_not_mask_errors_and_missing_is_not_null() {
    let resource_state = Condition::Attribute {
        source: AttributeSource::Resource,
        key: "state".into(),
        comparison: Comparison::Equals(json!("closed")),
    };
    for (condition, expected) in [
        (
            Condition::All(vec![Condition::always(), resource_state.clone()]),
            ReasonCode::DefaultDeny,
        ),
        (
            Condition::Any(vec![Condition::always(), resource_state]),
            ReasonCode::Allowed,
        ),
        (
            Condition::Any(vec![
                Condition::always(),
                Condition::Predicate(predicate(|_, _| Err(EvaluationError))),
            ]),
            ReasonCode::EvaluationFailed,
        ),
        (
            Condition::All(vec![
                Condition::Predicate(predicate(|_, _| Ok(false))),
                Condition::Predicate(predicate(|_, _| Err(EvaluationError))),
            ]),
            ReasonCode::EvaluationFailed,
        ),
        (
            Condition::Attribute {
                source: AttributeSource::Resource,
                key: "missing".into(),
                comparison: Comparison::Equals(json!(null)),
            },
            ReasonCode::DefaultDeny,
        ),
        (
            Condition::Attribute {
                source: AttributeSource::Resource,
                key: "state".into(),
                comparison: Comparison::In(vec![json!("open"), json!("closed")]),
            },
            ReasonCode::Allowed,
        ),
    ] {
        let mut options = support::options();
        options.rules.push(rule(Outcome::Allow, condition));
        assert_eq!(
            Authorization::new(options)
                .unwrap()
                .check(request())
                .await
                .code,
            expected
        );
    }
    // Explicit anonymous grants still cannot bypass configured identity requirements.
    let mut facts = request();
    facts.principal = None;
    assert!(
        Authorization::new(allow_options())
            .unwrap()
            .can(facts.clone())
            .await
    );
    let mut options = allow_options();
    options.identity.required = true;
    assert_eq!(
        Authorization::new(options).unwrap().check(facts).await.code,
        ReasonCode::BoundaryDenied
    );
}

struct AuthoritativeResource {
    substitute_id: bool,
}
impl ResourceResolver for AuthoritativeResource {
    fn resolve<'a>(
        &'a self,
        resource: &'a Resource,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, Resource> {
        Box::pin(async move {
            let mut resource = resource.clone();
            resource.scope.id = "authoritative".into();
            if self.substitute_id {
                resource.id = "another".into();
            }
            Ok(resource)
        })
    }
}
struct AttributesProvider(Arc<AtomicUsize>);
impl AttributeResolver for AttributesProvider {
    fn resolve<'a>(
        &'a self,
        _: &'a Request,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, ResolvedAttributes> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ResolvedAttributes {
                principal: Some(Attributes::from_iter([("approved".into(), json!(true))])),
                resource: Some(Attributes::from_iter([("reviewed".into(), json!(true))])),
                context: None,
            })
        })
    }
}
#[tokio::test]
async fn authoritative_scope_precedes_ceiling_attributes_replace_and_boundary_precedes_policy() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut options = allow_options();
    options.resolve_resource = Some(Arc::new(AuthoritativeResource {
        substitute_id: false,
    }));
    options.attributes = Some(Arc::new(AttributesProvider(calls.clone())));
    options.boundaries.push(predicate(|facts, _| {
        Ok(
            facts.principal.as_ref().unwrap().attributes.get("approved") == Some(&json!(true))
                && !facts.resource.attributes.contains_key("state"),
        )
    }));
    let engine = Authorization::new(options).unwrap();
    let mut facts = request();
    facts.credential = Some(CredentialLimit {
        permissions: vec![Permission::new("read", "note", scope())],
        expires_at: None,
    });
    assert_eq!(
        engine.check(facts.clone()).await.code,
        ReasonCode::BoundaryDenied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    facts.credential.as_mut().unwrap().permissions[0].scope.id = "authoritative".into();
    assert!(engine.can(facts).await);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut options = allow_options();
    options.resolve_resource = Some(Arc::new(AuthoritativeResource {
        substitute_id: true,
    }));
    assert_eq!(
        Authorization::new(options)
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::BoundaryDenied
    );
    let mut options = allow_options();
    options.attributes = Some(Arc::new(AttributesProvider(calls)));
    let mut facts = request();
    facts.principal = None;
    assert_eq!(
        Authorization::new(options).unwrap().check(facts).await.code,
        ReasonCode::EvaluationFailed
    );
    let mut options = allow_options();
    options.boundaries.push(predicate(|_, _| Ok(false)));
    assert_eq!(
        Authorization::new(options)
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::BoundaryDenied
    );
}

struct Relations;
impl RelationResolver for Relations {
    fn check<'a>(
        &'a self,
        principal: &'a Principal,
        relation: &'a str,
        resource: &'a Resource,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, bool> {
        Box::pin(async move {
            Ok(principal.subject_id == "alice" && relation == "member" && resource.id == "team")
        })
    }
}
#[tokio::test]
async fn relations_are_explicit_and_missing_resolver_fails_closed() {
    let mut options = support::options();
    options.rules.push(rule(
        Outcome::Allow,
        Condition::Relation {
            relation: "member".into(),
            target: Some(Resource::new("organization", "team", scope())),
        },
    ));
    assert_eq!(
        Authorization::new(options)
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::EvaluationFailed
    );
    let mut options = support::options();
    options.relations = Some(Arc::new(Relations));
    options.rules.push(rule(
        Outcome::Allow,
        Condition::Relation {
            relation: "member".into(),
            target: Some(Resource::new("organization", "team", scope())),
        },
    ));
    let engine = Authorization::new(options).unwrap();
    assert!(engine.can(request()).await);
    let mut facts = request();
    facts.principal = None;
    assert!(!engine.can(facts).await);
}

#[test]
fn rejects_invalid_configuration_at_construction() {
    let mut invalid = vec![
        AuthorizationOptions::new(Vec::<String>::new()),
        AuthorizationOptions::new(["read", "read"]),
    ];
    let mut config = support::options();
    config.timeout = std::time::Duration::ZERO;
    invalid.push(config);
    let mut config = support::options();
    config.identity.realms = Some(vec![]);
    invalid.push(config);
    for when in [
        Condition::All(vec![]),
        Condition::Any(vec![]),
        Condition::Attribute {
            source: AttributeSource::Context,
            key: "key".into(),
            comparison: Comparison::Equals(json!({"nested":true})),
        },
    ] {
        let mut config = support::options();
        config.rules.push(rule(Outcome::Allow, when));
        invalid.push(config);
    }
    let mut deep = Condition::always();
    for _ in 0..34 {
        deep = Condition::All(vec![deep]);
    }
    let mut config = support::options();
    config.rules.push(rule(Outcome::Allow, deep));
    invalid.push(config);
    let mut config = support::options();
    config.rules.push(rule(
        Outcome::Allow,
        Condition::All(vec![Condition::always(); 1024]),
    ));
    invalid.push(config);
    let mut config = support::options();
    let mut unknown = rule(Outcome::Allow, Condition::always());
    unknown.actions = vec!["unknown".into()];
    config.rules.push(unknown);
    invalid.push(config);
    let mut config = support::options();
    config.rules = vec![rule(Outcome::Allow, Condition::always()); 2];
    invalid.push(config);
    for config in invalid {
        assert!(Authorization::new(config).is_err());
    }
}

#[tokio::test]
async fn invalid_and_oversized_facts_fail_before_extensions() {
    let engine = Authorization::new(allow_options()).unwrap();
    let mut facts = request();
    facts.resource.id.clear();
    assert_eq!(engine.check(facts).await.code, ReasonCode::EvaluationFailed);
    let mut facts = request();
    facts.context.insert("large".into(), json!(vec![0; 10_000]));
    assert_eq!(engine.check(facts).await.code, ReasonCode::EvaluationFailed);
    let mut deep = json!(true);
    for _ in 0..65 {
        deep = json!([deep]);
    }
    let mut facts = request();
    facts.context.insert("deep".into(), deep);
    assert_eq!(engine.check(facts).await.code, ReasonCode::EvaluationFailed);
}

struct ObserverFailure;
impl Observer for ObserverFailure {
    fn observe(&self, _: Decision) -> EvaluationFuture<'_, ()> {
        Box::pin(async { Err(EvaluationError) })
    }
}
#[tokio::test]
async fn observer_failure_and_unwind_cannot_allow_and_explanation_is_gated() {
    let mut config = allow_options();
    config.observe = Some(Arc::new(ObserverFailure));
    assert_eq!(
        Authorization::new(config)
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::EvaluationFailed
    );
    let mut config = support::options();
    config
        .policies
        .push(policy(|_, _| panic!("private details")));
    assert_eq!(
        Authorization::new(config)
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::EvaluationFailed
    );
    let mut config = support::options();
    config.rules.push(rule(Outcome::Allow, Condition::always()));
    config.explain = Some(ExplainGate {
        action: "write".into(),
        resource: Resource::new("authorization", "instance", scope()),
    });
    config.rules.push(Rule::new(
        "private-admin-rule",
        Outcome::Allow,
        ["write"],
        "authorization",
        Condition::Predicate(predicate(|facts, _| {
            Ok(facts
                .principal
                .as_ref()
                .is_some_and(|p| p.subject_id == "admin"))
        })),
    ));
    let engine = Authorization::new(config).unwrap();
    assert!(engine
        .explain(request(), request(), Cancellation::default())
        .await
        .is_err());
    let mut manager = request();
    manager.action = "write".into();
    manager.resource = Resource::new("authorization", "instance", scope());
    assert!(engine
        .explain(manager.clone(), request(), Cancellation::default())
        .await
        .is_err());
    manager.principal.as_mut().unwrap().subject_id = "admin".into();
    let explanation = engine
        .explain(manager, request(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(explanation.paths, vec!["rules/0/allow"]);
    assert!(!format!("{explanation:?}").contains("private"));
}

#[tokio::test]
async fn integer_attribute_comparisons_preserve_precision_without_losing_numeric_equivalence() {
    for (actual, expected, equal) in [
        (
            json!(9_007_199_254_740_993_u64),
            json!(9_007_199_254_740_992_u64),
            false,
        ),
        (
            json!(9_007_199_254_740_993_u64),
            json!(9_007_199_254_740_992.0_f64),
            false,
        ),
        (
            json!(u64::MAX),
            json!(18_446_744_073_709_551_616.0_f64),
            false,
        ),
        (
            json!(i64::MAX),
            json!(9_223_372_036_854_775_808.0_f64),
            false,
        ),
        (json!(1), json!(1.0), true),
        (json!(-1), json!(-1.0), true),
        (
            json!(i64::MIN),
            json!(-9_223_372_036_854_775_808.0_f64),
            true,
        ),
        (json!(1), json!(1.5), false),
        (json!(1), json!("1"), false),
    ] {
        for (actual, expected) in [(actual.clone(), expected.clone()), (expected, actual)] {
            for comparison in [
                Comparison::Equals(expected.clone()),
                Comparison::In(vec![expected.clone()]),
            ] {
                let mut config = options();
                config.rules.push(rule(
                    Outcome::Allow,
                    Condition::Attribute {
                        source: AttributeSource::Context,
                        key: "number".into(),
                        comparison,
                    },
                ));
                let mut facts = request();
                facts.context.insert("number".into(), actual.clone());
                assert_eq!(
                    Authorization::new(config).unwrap().can(facts).await,
                    equal,
                    "{actual} vs {expected}"
                );
            }
        }
    }
}
