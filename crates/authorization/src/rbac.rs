use crate::*;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

#[derive(Clone, Debug)]
pub struct Role {
    pub id: String,
    pub scope: Scope,
    pub permissions: Vec<Permission>,
    /// Parent IDs resolve only within this role's exact scope.
    pub inherits: Vec<String>,
}
#[derive(Clone, Debug)]
pub struct Binding {
    pub id: String,
    pub principal: Principal,
    pub role_id: String,
    pub scope: Scope,
    pub expires_at: Option<u64>,
}
#[derive(Clone, Debug, Default)]
pub struct RoleGraph {
    pub roles: Vec<Role>,
    pub bindings: Vec<Binding>,
}
#[derive(Clone, Debug)]
pub struct RoleSnapshot {
    /// Adapter-local revision; it is not a permission ticket or cross-store fence.
    pub revision: u64,
    pub graph: RoleGraph,
}
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Invalid role graph")]
pub struct RoleGraphError;

/// Trusted internal adapter. Return one consistent, owned snapshot per read.
/// Errors must be mapped to EvaluationError, without exposing backend details.
pub trait RoleSource: Send + Sync {
    fn read<'a>(&'a self, evaluation: &'a Evaluation) -> EvaluationFuture<'a, Arc<RoleSnapshot>>;
}

pub fn validate_role_graph(graph: &RoleGraph, actions: &[String]) -> Result<(), RoleGraphError> {
    let known: HashSet<_> = actions.iter().collect();
    if actions.is_empty()
        || actions.len() > 512
        || known.len() != actions.len()
        || actions.iter().any(|a| !identifier(a))
        || graph.roles.len() > 512
        || graph.bindings.len() > 4096
    {
        return Err(RoleGraphError);
    }
    let mut roles = HashMap::new();
    for (index, role) in graph.roles.iter().enumerate() {
        if !identifier(&role.id)
            || !role.scope.valid()
            || role.permissions.len() > 512
            || role.inherits.len() > 512
            || roles.insert((&role.scope, &role.id), index).is_some()
        {
            return Err(RoleGraphError);
        }
        let mut permissions = HashSet::new();
        for permission in &role.permissions {
            if !permission.valid()
                || !known.contains(&permission.action)
                || permission.scope != role.scope
                || !permissions.insert(permission)
            {
                return Err(RoleGraphError);
            }
        }
        let mut parents = HashSet::new();
        if role
            .inherits
            .iter()
            .any(|parent| !identifier(parent) || !parents.insert(parent))
        {
            return Err(RoleGraphError);
        }
    }
    // Topological removal validates missing/cross-scope parents and cycles without recursion.
    let mut child_counts = vec![0usize; graph.roles.len()];
    let mut edges = vec![Vec::new(); graph.roles.len()];
    for (index, role) in graph.roles.iter().enumerate() {
        for parent in &role.inherits {
            let parent = *roles.get(&(&role.scope, parent)).ok_or(RoleGraphError)?;
            child_counts[index] += 1;
            edges[parent].push(index);
        }
    }
    let mut pending: Vec<_> = child_counts
        .iter()
        .enumerate()
        .filter_map(|(index, count)| (*count == 0).then_some(index))
        .collect();
    let mut visited = 0;
    while let Some(parent) = pending.pop() {
        visited += 1;
        for child in &edges[parent] {
            child_counts[*child] -= 1;
            if child_counts[*child] == 0 {
                pending.push(*child);
            }
        }
    }
    if visited != graph.roles.len() {
        return Err(RoleGraphError);
    }
    let mut bindings = HashSet::new();
    for binding in &graph.bindings {
        if !identifier(&binding.id)
            || !bindings.insert(&binding.id)
            || !binding.principal.valid()
            || !binding.scope.valid()
            || !identifier(&binding.role_id)
            || !roles.contains_key(&(&binding.scope, &binding.role_id))
        {
            return Err(RoleGraphError);
        }
    }
    let bags: Vec<_> = graph
        .bindings
        .iter()
        .map(|b| Some(&b.principal.attributes))
        .collect();
    if !attributes_valid(&bags, 4_000_000, 32) {
        return Err(RoleGraphError);
    }
    Ok(())
}

/// In-process trusted store. No authorization is performed on administrative writes.
/// Compare-and-swap is atomic; store-issued monotonic revisions prevent revision reuse (ABA).
pub struct MemoryRoleStore {
    actions: Vec<String>,
    current: RwLock<Arc<RoleSnapshot>>,
}
impl MemoryRoleStore {
    pub fn new(
        graph: RoleGraph,
        actions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, RoleGraphError> {
        let actions: Vec<_> = actions.into_iter().map(Into::into).collect();
        validate_role_graph(&graph, &actions)?;
        Ok(Self {
            actions,
            current: RwLock::new(Arc::new(RoleSnapshot { revision: 0, graph })),
        })
    }
    pub fn snapshot(&self) -> Result<Arc<RoleSnapshot>, RoleGraphError> {
        Ok(self.current.read().map_err(|_| RoleGraphError)?.clone())
    }
    /// Call only behind the application's own administrative authorization boundary.
    /// Cancellation checked at commit; cancellation after commit cannot undo it.
    pub fn compare_and_swap(
        &self,
        expected_revision: u64,
        next: RoleGraph,
        evaluation: &Evaluation,
    ) -> Result<bool, RoleGraphError> {
        if evaluation.cancellation.is_cancelled() {
            return Ok(false);
        }
        // No callbacks or awaits while the lock is held.
        let mut current = self.current.write().map_err(|_| RoleGraphError)?;
        if current.revision != expected_revision {
            return Ok(false);
        }
        validate_role_graph(&next, &self.actions)?;
        let revision = current.revision.checked_add(1).ok_or(RoleGraphError)?;
        if evaluation.cancellation.is_cancelled() {
            return Ok(false);
        }
        *current = Arc::new(RoleSnapshot {
            revision,
            graph: next,
        });
        Ok(true)
    }
}
impl RoleSource for MemoryRoleStore {
    fn read<'a>(&'a self, evaluation: &'a Evaluation) -> EvaluationFuture<'a, Arc<RoleSnapshot>> {
        Box::pin(async move {
            evaluation.ensure_active()?;
            self.snapshot().map_err(|_| EvaluationError)
        })
    }
}

struct FixedRoleSource(Arc<RoleSnapshot>);
impl RoleSource for FixedRoleSource {
    fn read<'a>(&'a self, evaluation: &'a Evaluation) -> EvaluationFuture<'a, Arc<RoleSnapshot>> {
        Box::pin(async move {
            evaluation.ensure_active()?;
            Ok(self.0.clone())
        })
    }
}

/// RBAC can act as an independent OR policy or a predicate inside an AND condition.
/// Every evaluation reads and validates one snapshot; there is no positive cache.
pub struct Rbac {
    actions: Vec<String>,
    source: Arc<dyn RoleSource>,
}
impl Rbac {
    pub fn new(
        graph: RoleGraph,
        actions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, RoleGraphError> {
        let actions: Vec<_> = actions.into_iter().map(Into::into).collect();
        validate_role_graph(&graph, &actions)?;
        Ok(Self {
            actions,
            source: Arc::new(FixedRoleSource(Arc::new(RoleSnapshot {
                revision: 0,
                graph,
            }))),
        })
    }
    pub fn from_source(
        actions: impl IntoIterator<Item = impl Into<String>>,
        source: Arc<dyn RoleSource>,
    ) -> Result<Self, RoleGraphError> {
        let actions: Vec<_> = actions.into_iter().map(Into::into).collect();
        validate_role_graph(&RoleGraph::default(), &actions)?;
        Ok(Self { actions, source })
    }
    pub async fn permits(
        &self,
        request: &Request,
        evaluation: &Evaluation,
    ) -> Result<bool, EvaluationError> {
        evaluation.ensure_active()?;
        let Some(principal) = &request.principal else {
            return Ok(false);
        };
        if !self.actions.contains(&request.action) {
            return Ok(false);
        }
        let snapshot = self.source.read(evaluation).await?;
        evaluation.ensure_active()?;
        validate_role_graph(&snapshot.graph, &self.actions).map_err(|_| EvaluationError)?;
        let graph = &snapshot.graph;
        let mut pending: Vec<_> = graph
            .bindings
            .iter()
            .filter(|binding| {
                binding.principal.same_identity(principal)
                    && binding.scope == request.resource.scope
                    && binding
                        .expires_at
                        .map_or(true, |expiry| expiry > evaluation.now)
            })
            .map(|binding| &binding.role_id)
            .collect();
        let mut visited = HashSet::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            let role = graph
                .roles
                .iter()
                .find(|role| &role.id == id && role.scope == request.resource.scope)
                .ok_or(EvaluationError)?;
            if role
                .permissions
                .iter()
                .any(|permission| permission.matches(request))
            {
                return Ok(true);
            }
            pending.extend(&role.inherits);
        }
        Ok(false)
    }
}
impl Predicate for Rbac {
    fn test<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, bool> {
        Box::pin(self.permits(request, evaluation))
    }
}
impl Policy for Rbac {
    fn evaluate<'a>(
        &'a self,
        request: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Effect> {
        Box::pin(async move {
            Ok(if self.permits(request, evaluation).await? {
                Effect::Allow
            } else {
                Effect::Abstain
            })
        })
    }
}
