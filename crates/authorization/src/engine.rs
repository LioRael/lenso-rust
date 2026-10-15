use crate::*;
use futures_util::FutureExt;
use serde_json::Value;
use std::{
    collections::HashSet,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub enum AttributeSource {
    Principal,
    Resource,
    Context,
}
#[derive(Clone, Debug)]
pub enum Comparison {
    Equals(Value),
    In(Vec<Value>),
}
#[derive(Clone)]
pub enum Condition {
    All(Vec<Condition>),
    Any(Vec<Condition>),
    Predicate(Arc<dyn Predicate>),
    Attribute {
        source: AttributeSource,
        key: String,
        comparison: Comparison,
    },
    Relation {
        relation: String,
        target: Option<Resource>,
    },
}
impl Condition {
    pub fn always() -> Self {
        Self::Predicate(predicate(|_, _| Ok(true)))
    }
    fn valid(&self) -> bool {
        let mut pending = vec![(self, 0)];
        let mut nodes = 0;
        while let Some((condition, depth)) = pending.pop() {
            nodes += 1;
            if depth > 32 || nodes > 1024 {
                return false;
            }
            match condition {
                Self::All(children) | Self::Any(children) => {
                    if children.is_empty() || children.len() > 1024 {
                        return false;
                    }
                    pending.extend(children.iter().map(|child| (child, depth + 1)));
                }
                Self::Predicate(_) => (),
                Self::Attribute {
                    key, comparison, ..
                } => {
                    let primitive = |value: &Value| !value.is_array() && !value.is_object();
                    if !identifier(key)
                        || !match comparison {
                            Comparison::Equals(value) => primitive(value),
                            Comparison::In(values) => {
                                !values.is_empty() && values.iter().all(primitive)
                            }
                        }
                    {
                        return false;
                    }
                }
                Self::Relation { relation, target } => {
                    if !identifier(relation)
                        || target.as_ref().is_some_and(|r| {
                            !r.valid() || !attributes_valid(&[Some(&r.attributes)], 10_000, 64)
                        })
                    {
                        return false;
                    }
                }
            }
        }
        true
    }
}

#[derive(Clone)]
pub struct Rule {
    /// Local validation identifier; never included in decision/explanation output.
    pub id: String,
    pub effect: Outcome,
    pub actions: Vec<String>,
    pub resource_type: String,
    /// None deliberately matches all scopes. No wildcard or prefix interpretation.
    pub scope: Option<Scope>,
    pub resource_id: Option<String>,
    pub when: Condition,
}
impl Rule {
    pub fn new(
        id: impl Into<String>,
        effect: Outcome,
        actions: impl IntoIterator<Item = impl Into<String>>,
        resource_type: impl Into<String>,
        when: Condition,
    ) -> Self {
        Self {
            id: id.into(),
            effect,
            actions: actions.into_iter().map(Into::into).collect(),
            resource_type: resource_type.into(),
            scope: None,
            resource_id: None,
            when,
        }
    }
}
#[derive(Clone, Default)]
pub struct IdentityRequirements {
    pub required: bool,
    pub realms: Option<Vec<String>>,
    pub audiences: Option<Vec<String>>,
    pub credential_required: bool,
}
#[derive(Clone)]
pub struct ExplainGate {
    pub action: String,
    pub resource: Resource,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Explanation {
    pub decision: Decision,
    pub paths: Vec<String>,
}

/// Construction consumes configuration; later changes to caller-owned copies cannot affect it.
/// Extension objects are trusted application code and remain responsible for their own state.
pub struct AuthorizationOptions {
    pub actions: Vec<String>,
    pub rules: Vec<Rule>,
    pub policies: Vec<Arc<dyn Policy>>,
    pub boundaries: Vec<Arc<dyn Predicate>>,
    pub identity: IdentityRequirements,
    pub relations: Option<Arc<dyn RelationResolver>>,
    pub attributes: Option<Arc<dyn AttributeResolver>>,
    pub resolve_resource: Option<Arc<dyn ResourceResolver>>,
    pub timeout: Duration,
    /// Unix milliseconds. Invoked once per check, inside the failure boundary.
    pub clock: Arc<dyn Fn() -> Result<u64, EvaluationError> + Send + Sync>,
    pub observe: Option<Arc<dyn Observer>>,
    pub explain: Option<ExplainGate>,
}
impl AuthorizationOptions {
    pub fn new(actions: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            actions: actions.into_iter().map(Into::into).collect(),
            rules: vec![],
            policies: vec![],
            boundaries: vec![],
            identity: IdentityRequirements::default(),
            relations: None,
            attributes: None,
            resolve_resource: None,
            timeout: Duration::from_secs(1),
            clock: Arc::new(|| {
                u64::try_from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| EvaluationError)?
                        .as_millis(),
                )
                .map_err(|_| EvaluationError)
            }),
            observe: None,
            explain: None,
        }
    }
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        let actions: HashSet<_> = self.actions.iter().collect();
        if actions.is_empty()
            || actions.len() != self.actions.len()
            || self.actions.iter().any(|a| !identifier(a))
            || self.timeout < Duration::from_millis(1)
            || self.timeout > Duration::from_secs(60)
        {
            return Err(ConfigurationError);
        }
        let mut ids = HashSet::new();
        for rule in &self.rules {
            if !identifier(&rule.id)
                || !ids.insert(&rule.id)
                || !identifier(&rule.resource_type)
                || rule.actions.is_empty()
                || rule.actions.iter().any(|a| !actions.contains(a))
                || rule.scope.as_ref().is_some_and(|s| !s.valid())
                || rule.resource_id.as_ref().is_some_and(|id| !identifier(id))
                || !rule.when.valid()
            {
                return Err(ConfigurationError);
            }
        }
        for values in [&self.identity.realms, &self.identity.audiences]
            .into_iter()
            .flatten()
        {
            if values.is_empty() || values.iter().any(|s| !identifier(s)) {
                return Err(ConfigurationError);
            }
        }
        if self.explain.as_ref().is_some_and(|gate| {
            !actions.contains(&gate.action)
                || !gate.resource.valid()
                || !attributes_valid(&[Some(&gate.resource.attributes)], 10_000, 64)
        }) {
            return Err(ConfigurationError);
        }
        Ok(())
    }
}

/// Ordinary cloneable service. Owns no resources, workers, global registry or permission cache.
#[derive(Clone)]
pub struct Authorization(Arc<AuthorizationOptions>);
impl Authorization {
    pub fn new(options: AuthorizationOptions) -> Result<Self, ConfigurationError> {
        options.validate()?;
        Ok(Self(Arc::new(options)))
    }
    pub async fn check(&self, request: Request) -> Decision {
        self.check_with_cancellation(request, Cancellation::default())
            .await
    }
    pub async fn check_with_cancellation(
        &self,
        request: Request,
        cancellation: Cancellation,
    ) -> Decision {
        self.evaluate(request, cancellation).await.decision
    }
    pub async fn can(&self, request: Request) -> bool {
        self.check(request).await.is_allowed()
    }
    pub async fn enforce(&self, request: Request) -> Result<(), AuthorizationError> {
        if self.can(request).await {
            Ok(())
        } else {
            Err(AuthorizationError)
        }
    }
    pub async fn enforce_with_cancellation(
        &self,
        request: Request,
        cancellation: Cancellation,
    ) -> Result<(), AuthorizationError> {
        if self
            .check_with_cancellation(request, cancellation)
            .await
            .is_allowed()
        {
            Ok(())
        } else {
            Err(AuthorizationError)
        }
    }
    pub async fn explain(
        &self,
        manager: Request,
        target: Request,
        cancellation: Cancellation,
    ) -> Result<Explanation, AuthorizationError> {
        let gate = self.0.explain.as_ref().ok_or(AuthorizationError)?;
        if manager.action != gate.action
            || manager.resource.kind != gate.resource.kind
            || manager.resource.id != gate.resource.id
            || manager.resource.scope != gate.resource.scope
            || !self
                .check_with_cancellation(manager, cancellation.clone())
                .await
                .is_allowed()
        {
            return Err(AuthorizationError);
        }
        Ok(self.evaluate(target, cancellation).await)
    }
    async fn evaluate(&self, request: Request, caller: Cancellation) -> Explanation {
        let started = Instant::now();
        let cancellation = Cancellation::default();
        let _finish = CancelOnDrop(cancellation.clone());
        let failure = |code| Explanation {
            decision: Decision::new(code),
            paths: vec![],
        };
        let work = AssertUnwindSafe(async {
            if !request.valid() {
                return Ok(failure(ReasonCode::EvaluationFailed));
            }
            let now = (self.0.clock)()?;
            if !self.0.actions.contains(&request.action) {
                return Ok(failure(ReasonCode::UnknownAction));
            }
            if caller.is_cancelled() {
                return Ok(failure(ReasonCode::Cancelled));
            }
            let evaluation = Evaluation {
                now,
                cancellation: cancellation.clone(),
            };
            self.work(request, &evaluation).await
        })
        .catch_unwind();
        tokio::pin!(work);
        let result = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(started + self.0.timeout) => failure(ReasonCode::Timeout),
            _ = caller.cancelled() => failure(ReasonCode::Cancelled),
            result = &mut work => match result {
                Ok(Ok(result)) => result,
                _ => failure(ReasonCode::EvaluationFailed),
            },
        };
        // A callback can block one poll, or cancel and return allow in the same poll.
        if started.elapsed() >= self.0.timeout {
            failure(ReasonCode::Timeout)
        } else if caller.is_cancelled() {
            failure(ReasonCode::Cancelled)
        } else {
            result
        }
    }
    async fn work(
        &self,
        mut facts: Request,
        evaluation: &Evaluation,
    ) -> Result<Explanation, EvaluationError> {
        let options = &self.0;
        let result = |code| Explanation {
            decision: Decision::new(code),
            paths: vec![],
        };
        let identity = &options.identity;
        if (identity.required && facts.principal.is_none())
            || identity.realms.as_ref().is_some_and(|realms| {
                facts
                    .principal
                    .as_ref()
                    .map_or(true, |p| !realms.contains(&p.realm_id))
            })
            || identity.audiences.as_ref().is_some_and(|audiences| {
                facts
                    .audience
                    .as_ref()
                    .map_or(true, |a| !audiences.contains(a))
            })
            || (identity.credential_required && facts.credential.is_none())
        {
            return Ok(result(ReasonCode::BoundaryDenied));
        }
        if let Some(resolver) = &options.resolve_resource {
            let resource = resolver.resolve(&facts.resource, evaluation).await?;
            if !resource.valid()
                || resource.kind != facts.resource.kind
                || resource.id != facts.resource.id
            {
                return Ok(result(ReasonCode::BoundaryDenied));
            }
            facts.resource = resource;
            if !facts.valid() {
                return Err(EvaluationError);
            }
        }
        evaluation.ensure_active()?;
        if let Some(credential) = &facts.credential {
            if credential
                .expires_at
                .is_some_and(|expiry| expiry <= evaluation.now)
                || credential
                    .permissions
                    .iter()
                    .any(|p| !p.valid() || !options.actions.contains(&p.action))
                || !credential.permissions.iter().any(|p| p.matches(&facts))
            {
                return Ok(result(ReasonCode::BoundaryDenied));
            }
        }
        if let Some(resolver) = &options.attributes {
            let resolved = resolver.resolve(&facts, evaluation).await?;
            if let Some(attributes) = resolved.principal {
                facts.principal.as_mut().ok_or(EvaluationError)?.attributes = attributes;
            }
            if let Some(attributes) = resolved.resource {
                facts.resource.attributes = attributes;
            }
            if let Some(context) = resolved.context {
                facts.context = context;
            }
            if !facts.valid() {
                return Err(EvaluationError);
            }
        }
        for boundary in &options.boundaries {
            evaluation.ensure_active()?;
            if !boundary.test(&facts, evaluation).await? {
                return Ok(result(ReasonCode::BoundaryDenied));
            }
        }
        let mut allowed = false;
        let mut denied = false;
        let mut paths = vec![];
        for (index, rule) in options.rules.iter().enumerate() {
            if !rule.actions.contains(&facts.action)
                || rule.resource_type != facts.resource.kind
                || rule
                    .scope
                    .as_ref()
                    .is_some_and(|s| s != &facts.resource.scope)
                || rule
                    .resource_id
                    .as_ref()
                    .is_some_and(|id| id != &facts.resource.id)
            {
                continue;
            }
            if self
                .condition_matches(&rule.when, &facts, evaluation)
                .await?
            {
                let effect = match rule.effect {
                    Outcome::Allow => {
                        allowed = true;
                        "allow"
                    }
                    Outcome::Deny => {
                        denied = true;
                        "deny"
                    }
                };
                paths.push(format!("rules/{index}/{effect}"));
            }
        }
        for (index, policy) in options.policies.iter().enumerate() {
            evaluation.ensure_active()?;
            match policy.evaluate(&facts, evaluation).await? {
                Effect::Allow => {
                    allowed = true;
                    paths.push(format!("policies/{index}/allow"));
                }
                Effect::Deny => {
                    denied = true;
                    paths.push(format!("policies/{index}/deny"));
                }
                Effect::Abstain => (),
            }
        }
        let decision = Decision::new(if denied {
            ReasonCode::ExplicitDeny
        } else if allowed {
            ReasonCode::Allowed
        } else {
            ReasonCode::DefaultDeny
        });
        if let Some(observer) = &options.observe {
            observer.observe(decision).await?;
        }
        evaluation.ensure_active()?;
        Ok(Explanation { decision, paths })
    }
    fn condition_matches<'a>(
        &'a self,
        condition: &'a Condition,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, bool> {
        Box::pin(async move {
            evaluation.ensure_active()?;
            match condition {
                Condition::All(children) | Condition::Any(children) => {
                    let all = matches!(condition, Condition::All(_));
                    let mut matched = all;
                    // Never short-circuit: errors in another branch must not be hidden.
                    for child in children {
                        let result = self.condition_matches(child, request, evaluation).await?;
                        if all {
                            matched &= result;
                        } else {
                            matched |= result;
                        }
                    }
                    Ok(matched)
                }
                Condition::Predicate(predicate) => predicate.test(request, evaluation).await,
                Condition::Attribute {
                    source,
                    key,
                    comparison,
                } => {
                    let bag = match source {
                        AttributeSource::Principal => {
                            request.principal.as_ref().map(|p| &p.attributes)
                        }
                        AttributeSource::Resource => Some(&request.resource.attributes),
                        AttributeSource::Context => Some(&request.context),
                    };
                    let Some(value) = bag.and_then(|bag| bag.get(key)) else {
                        return Ok(false);
                    };
                    Ok(match comparison {
                        Comparison::Equals(expected) => primitive_eq(value, expected),
                        Comparison::In(values) => {
                            values.iter().any(|expected| primitive_eq(value, expected))
                        }
                    })
                }
                Condition::Relation { relation, target } => {
                    let Some(principal) = &request.principal else {
                        return Ok(false);
                    };
                    self.0
                        .relations
                        .as_ref()
                        .ok_or(EvaluationError)?
                        .check(
                            principal,
                            relation,
                            target.as_ref().unwrap_or(&request.resource),
                            evaluation,
                        )
                        .await
                }
            }
        })
    }
}
fn primitive_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => match (left.is_f64(), right.is_f64()) {
            (false, false) => left == right,
            (true, true) => left.as_f64() == right.as_f64(),
            (true, false) => float_matches_integer(left.as_f64().unwrap(), right),
            (false, true) => float_matches_integer(right.as_f64().unwrap(), left),
        },
        _ => left == right,
    }
}
fn float_matches_integer(float: f64, integer: &serde_json::Number) -> bool {
    if float.fract() != 0.0 {
        return false;
    }
    if let Some(value) = integer.as_i64() {
        // Exclusive upper bounds prevent saturating casts from widening a match.
        (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&float)
            && float as i64 == value
    } else if let Some(value) = integer.as_u64() {
        (0.0..18_446_744_073_709_551_616.0).contains(&float) && float as u64 == value
    } else {
        false
    }
}
struct CancelOnDrop(Cancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
