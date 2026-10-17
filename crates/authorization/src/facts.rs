use serde_json::{Map, Value};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::watch;

/// Owned JSON facts: no shared mutable containers, accessors, cycles or handles.
pub type Attributes = Map<String, Value>;
pub type EvaluationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, EvaluationError>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scope {
    pub kind: String,
    pub id: String,
}
impl Scope {
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
        }
    }
    pub(crate) fn valid(&self) -> bool {
        identifier(&self.kind) && identifier(&self.id)
    }
}

/// Trusted identity-source namespace, subject and kind; never an authenticated DTO.
#[derive(Clone, Debug, PartialEq)]
pub struct Principal {
    pub realm_id: String,
    pub subject_id: String,
    pub kind: String,
    pub attributes: Attributes,
}
impl Principal {
    pub fn new(
        realm: impl Into<String>,
        subject: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            realm_id: realm.into(),
            subject_id: subject.into(),
            kind: kind.into(),
            attributes: Attributes::new(),
        }
    }
    /// Attributes are deliberately not part of identity equality.
    pub fn same_identity(&self, other: &Self) -> bool {
        self.realm_id == other.realm_id
            && self.subject_id == other.subject_id
            && self.kind == other.kind
    }
    pub(crate) fn valid(&self) -> bool {
        identifier(&self.realm_id) && identifier(&self.subject_id) && identifier(&self.kind)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Resource {
    pub kind: String,
    pub id: String,
    pub scope: Scope,
    pub attributes: Attributes,
}
impl Resource {
    pub fn new(kind: impl Into<String>, id: impl Into<String>, scope: Scope) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            scope,
            attributes: Attributes::new(),
        }
    }
    pub(crate) fn valid(&self) -> bool {
        identifier(&self.kind) && identifier(&self.id) && self.scope.valid()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Permission {
    pub action: String,
    pub resource_type: String,
    pub scope: Scope,
    /// None covers every resource of the exact type in the exact scope.
    pub resource_id: Option<String>,
}
impl Permission {
    pub fn new(action: impl Into<String>, resource_type: impl Into<String>, scope: Scope) -> Self {
        Self {
            action: action.into(),
            resource_type: resource_type.into(),
            scope,
            resource_id: None,
        }
    }
    pub fn matches(&self, request: &Request) -> bool {
        self.action == request.action
            && self.resource_type == request.resource.kind
            && self.scope == request.resource.scope
            && self
                .resource_id
                .as_ref()
                .map_or(true, |id| id == &request.resource.id)
    }
    pub fn covers(&self, other: &Self) -> bool {
        self.action == other.action
            && self.resource_type == other.resource_type
            && self.scope == other.scope
            && self
                .resource_id
                .as_ref()
                .map_or(true, |id| other.resource_id.as_ref() == Some(id))
    }
    pub(crate) fn valid(&self) -> bool {
        identifier(&self.action)
            && identifier(&self.resource_type)
            && self.scope.valid()
            && self.resource_id.as_ref().map_or(true, |id| identifier(id))
    }
}

#[derive(Clone, Debug)]
pub struct CredentialLimit {
    pub permissions: Vec<Permission>,
    /// Unix milliseconds, checked at entry and again before returning an allow.
    pub expires_at: Option<u64>,
}

/// All fields must come from trusted application code after authentication/loading.
/// Passing ownership fixes these facts even before the returned future is polled.
#[derive(Clone, Debug)]
pub struct Request {
    pub principal: Option<Principal>,
    pub action: String,
    pub resource: Resource,
    pub context: Attributes,
    pub audience: Option<String>,
    pub credential: Option<CredentialLimit>,
}
impl Request {
    pub fn new(
        principal: Option<Principal>,
        action: impl Into<String>,
        resource: Resource,
    ) -> Self {
        Self {
            principal,
            action: action.into(),
            resource,
            context: Attributes::new(),
            audience: None,
            credential: None,
        }
    }
    pub(crate) fn valid(&self) -> bool {
        self.resource.valid()
            && self.principal.as_ref().map_or(true, Principal::valid)
            && attributes_valid(
                &[
                    self.principal.as_ref().map(|p| &p.attributes),
                    Some(&self.resource.attributes),
                    Some(&self.context),
                ],
                10_000,
                64,
            )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
    Abstain,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Allow,
    Deny,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReasonCode {
    Allowed,
    DefaultDeny,
    ExplicitDeny,
    BoundaryDenied,
    UnknownAction,
    EvaluationFailed,
    Timeout,
    Cancelled,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    pub effect: Outcome,
    pub code: ReasonCode,
}
impl Decision {
    pub(crate) fn new(code: ReasonCode) -> Self {
        Self {
            effect: if code == ReasonCode::Allowed {
                Outcome::Allow
            } else {
                Outcome::Deny
            },
            code,
        }
    }
    pub fn is_allowed(self) -> bool {
        self.effect == Outcome::Allow
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Invalid authorization configuration")]
pub struct ConfigurationError;
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Authorization evaluation failed")]
pub struct EvaluationError;
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Access denied.")]
pub struct AuthorizationError;

/// Reusable caller cancellation signal. Cancellation is sticky and wakes all waiters.
#[derive(Clone, Debug)]
pub struct Cancellation(watch::Sender<bool>);
impl Default for Cancellation {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}
impl Cancellation {
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

/// A single trusted clock reading and cooperative per-check cancellation signal.
#[derive(Clone, Debug)]
pub struct Evaluation {
    pub now: u64,
    pub cancellation: Cancellation,
}
impl Evaluation {
    pub fn ensure_active(&self) -> Result<(), EvaluationError> {
        if self.cancellation.is_cancelled() {
            Err(EvaluationError)
        } else {
            Ok(())
        }
    }
}

pub trait Predicate: Send + Sync {
    fn test<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, bool>;
}
pub trait Policy: Send + Sync {
    fn evaluate<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Effect>;
}
pub trait RelationResolver: Send + Sync {
    fn check<'a>(
        &'a self,
        principal: &'a Principal,
        relation: &'a str,
        resource: &'a Resource,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, bool>;
}
pub trait ResourceResolver: Send + Sync {
    /// May replace scope and attributes, but must preserve resource type and ID.
    fn resolve<'a>(
        &'a self,
        resource: &'a Resource,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Resource>;
}
#[derive(Clone, Debug, Default)]
pub struct ResolvedAttributes {
    pub principal: Option<Attributes>,
    pub resource: Option<Attributes>,
    pub context: Option<Attributes>,
}
pub trait AttributeResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, ResolvedAttributes>;
}
/// Receives only completed policy decisions. Early boundary/error outcomes are omitted.
pub trait Observer: Send + Sync {
    fn observe(&self, decision: Decision) -> EvaluationFuture<'_, ()>;
}

struct SyncPredicate<F>(F);
impl<F: Fn(&Request, &Evaluation) -> Result<bool, EvaluationError> + Send + Sync> Predicate
    for SyncPredicate<F>
{
    fn test<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, bool> {
        Box::pin(async move { (self.0)(request, evaluation) })
    }
}
pub fn predicate<F>(test: F) -> Arc<dyn Predicate>
where
    F: Fn(&Request, &Evaluation) -> Result<bool, EvaluationError> + Send + Sync + 'static,
{
    Arc::new(SyncPredicate(test))
}
struct SyncPolicy<F>(F);
impl<F: Fn(&Request, &Evaluation) -> Result<Effect, EvaluationError> + Send + Sync> Policy
    for SyncPolicy<F>
{
    fn evaluate<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Effect> {
        Box::pin(async move { (self.0)(request, evaluation) })
    }
}
pub fn policy<F>(evaluate: F) -> Arc<dyn Policy>
where
    F: Fn(&Request, &Evaluation) -> Result<Effect, EvaluationError> + Send + Sync + 'static,
{
    Arc::new(SyncPolicy(evaluate))
}

pub(crate) fn identifier(value: &str) -> bool {
    !value.is_empty() && value.encode_utf16().take(257).count() <= 256
}

/// Iterative bounded validation, before callbacks; JSON values are already owned and finite.
pub(crate) fn attributes_valid(
    bags: &[Option<&Attributes>],
    max_nodes: usize,
    max_depth: usize,
) -> bool {
    let mut pending = Vec::new();
    let mut nodes = 0usize;
    for bag in bags.iter().flatten() {
        nodes = nodes.saturating_add(1 + bag.len());
        if nodes > max_nodes {
            return false;
        }
        pending.extend(bag.values().map(|value| (value, 1)));
    }
    while let Some((value, depth)) = pending.pop() {
        if depth > max_depth {
            return false;
        }
        let children: Box<dyn Iterator<Item = &Value>> = match value {
            Value::Array(items) => Box::new(items.iter()),
            Value::Object(items) => Box::new(items.values()),
            _ => continue,
        };
        for child in children {
            nodes += 1;
            if nodes > max_nodes {
                return false;
            }
            pending.push((child, depth + 1));
        }
    }
    true
}
