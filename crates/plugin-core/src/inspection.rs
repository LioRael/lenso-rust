use super::*;
use serde::Serialize;

/// Stable machine-readable preflight failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DiagnosticCode {
    /// An instance name is used more than once.
    DuplicateName,
    /// An instance identity is installed more than once.
    DuplicateIdentity,
    /// A reservation has no plugin.
    UnboundInstance,
    /// A binding was supplied more than once or has the wrong service type.
    InvalidBinding,
    /// A binding selects a handle outside this app.
    ForeignBinding,
    /// Plugin configuration validation failed.
    InvalidConfig,
    /// Plugin validation unwound.
    ValidationPanicked,
    /// Dependency declaration unwound.
    DependenciesPanicked,
    /// A dependency selects an instance outside this app.
    MissingDependency,
    /// A closed dependency path exists.
    DependencyCycle,
}

/// A safe diagnostic: no plugin error causes, panic payloads, or opaque key addresses.
/// Instance names are caller-supplied metadata and must not contain secrets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Diagnostic {
    /// Machine-readable classification; consumers should tolerate new codes.
    pub code: DiagnosticCode,
    /// Index into `Inspection::instances`, when an owning instance exists.
    pub instance: Option<usize>,
    /// Declaration index of an invalid dependency, when applicable.
    pub dependency_index: Option<usize>,
    /// Closed cycle of instance indices, including the repeated first node.
    pub path: Vec<usize>,
    /// Fixed safe text, never an arbitrary plugin error or panic message.
    pub message: String,
}
impl Diagnostic {
    pub(crate) fn new(code: DiagnosticCode, instance: Option<usize>, message: &str) -> Self {
        Self {
            code,
            instance,
            dependency_index: None,
            path: Vec::new(),
            message: message.into(),
        }
    }
}

/// One dependency declaration; unresolved external identities have no target index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DependencyInspection {
    /// Index in the plugin's dependency declarations.
    pub dependency_index: usize,
    /// Target index into `Inspection::instances`, or `None` for a foreign handle.
    pub target: Option<usize>,
}

/// Installed instance metadata, with no plugin configuration or service values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InstanceInspection {
    /// App-local identity, stable for the lifetime of this builder.
    pub index: usize,
    /// Caller-supplied instance name.
    pub name: String,
    /// Whether the reservation has a bound plugin.
    pub bound: bool,
    /// Ordered dependency declarations, including duplicates.
    pub dependencies: Vec<DependencyInspection>,
}

/// Whether a check actually ran; static success is not runtime readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CheckStatus {
    /// The check ran and passed.
    Passed,
    /// The check ran and found errors.
    Failed,
    /// The operation was deliberately not run by static inspection.
    NotRun,
    /// This native API does not implement the operation.
    Unsupported,
}

/// Explicit scope of a static inspection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InspectionCheck {
    /// Stable check name.
    pub name: &'static str,
    /// Actual execution status.
    pub status: CheckStatus,
}

/// Serializable static snapshot. This is not a running app or readiness proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Inspection {
    /// Version of this serialized shape (currently 1).
    pub schema_version: u32,
    /// Executed preflight and explicit non-executed runtime checks.
    pub checks: Vec<InspectionCheck>,
    /// Installed instances in insertion order.
    pub instances: Vec<InstanceInspection>,
    /// Complete dependency-first order on success; empty when diagnostics exist.
    pub startup_order: Vec<usize>,
    /// All discovered static errors, in deterministic traversal order.
    pub diagnostics: Vec<Diagnostic>,
}
impl Inspection {
    /// Whether static checks passed. Setup may still fail.
    pub fn is_valid(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

impl App {
    pub(crate) fn inspect_plan(&self) -> (Inspection, Vec<Vec<InstanceKey>>) {
        let mut diagnostics = self.binding_errors.clone();
        let mut names = HashSet::new();
        let mut keys = HashMap::new();
        for (index, instance) in self.instances.iter().enumerate() {
            if !names.insert(&instance.name) {
                diagnostics.push(Diagnostic::new(
                    DiagnosticCode::DuplicateName,
                    Some(index),
                    "duplicate instance name",
                ));
            }
            if keys.insert(instance.key.clone(), index).is_some() {
                diagnostics.push(Diagnostic::new(
                    DiagnosticCode::DuplicateIdentity,
                    Some(index),
                    "duplicate instance identity",
                ));
            }
        }
        let mut dependencies = Vec::new();
        let mut instances = Vec::new();
        for (index, instance) in self.instances.iter().enumerate() {
            let deps = match &instance.plugin {
                Some(plugin) => {
                    match catch_unwind(AssertUnwindSafe(|| plugin.validate())) {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => diagnostics.push(Diagnostic::new(
                            DiagnosticCode::InvalidConfig,
                            Some(index),
                            "invalid config: plugin configuration rejected",
                        )),
                        Err(_) => diagnostics.push(Diagnostic::new(
                            DiagnosticCode::ValidationPanicked,
                            Some(index),
                            "validation panicked",
                        )),
                    }
                    match catch_unwind(AssertUnwindSafe(|| plugin.dependencies())) {
                        Ok(deps) => deps,
                        Err(_) => {
                            diagnostics.push(Diagnostic::new(
                                DiagnosticCode::DependenciesPanicked,
                                Some(index),
                                "dependencies panicked",
                            ));
                            Vec::new()
                        }
                    }
                }
                None => {
                    diagnostics.push(Diagnostic::new(
                        DiagnosticCode::UnboundInstance,
                        Some(index),
                        "unbound instance",
                    ));
                    Vec::new()
                }
            };
            let declarations = deps
                .iter()
                .enumerate()
                .map(|(dependency_index, dep)| {
                    let target = keys.get(dep).copied();
                    if target.is_none() {
                        let mut diagnostic = Diagnostic::new(
                            DiagnosticCode::MissingDependency,
                            Some(index),
                            "missing or foreign dependency",
                        );
                        diagnostic.dependency_index = Some(dependency_index);
                        diagnostics.push(diagnostic);
                    }
                    DependencyInspection {
                        dependency_index,
                        target,
                    }
                })
                .collect();
            dependencies.push(deps);
            instances.push(InstanceInspection {
                index,
                name: instance.name.clone(),
                bound: instance.plugin.is_some(),
                dependencies: declarations,
            });
        }
        // Iterative DFS avoids overflowing the call stack on long graphs. Visiting
        // only known edges still finds cycles when another component has a missing edge.
        let mut colors = vec![0u8; instances.len()];
        for root in 0..instances.len() {
            if colors[root] != 0 {
                continue;
            }
            colors[root] = 1;
            let mut stack = vec![(root, 0)];
            while let Some((node, next)) = stack.last_mut() {
                if *next == instances[*node].dependencies.len() {
                    colors[*node] = 2;
                    stack.pop();
                    continue;
                }
                let dep = &instances[*node].dependencies[*next];
                *next += 1;
                let Some(target) = dep.target else {
                    continue;
                };
                match colors[target] {
                    0 => {
                        colors[target] = 1;
                        stack.push((target, 0));
                    }
                    1 => {
                        let mut diagnostic = Diagnostic::new(
                            DiagnosticCode::DependencyCycle,
                            Some(*node),
                            "dependency cycle",
                        );
                        diagnostic.dependency_index = Some(dep.dependency_index);
                        let start = stack
                            .iter()
                            .position(|(index, _)| *index == target)
                            .unwrap();
                        diagnostic.path = stack[start..]
                            .iter()
                            .map(|(index, _)| *index)
                            .chain(std::iter::once(target))
                            .collect();
                        diagnostics.push(diagnostic);
                    }
                    _ => {}
                }
            }
        }
        // Preserve the existing startup tie-breaking order for independent nodes.
        let mut order = Vec::new();
        if diagnostics.is_empty() {
            let mut visited = HashSet::new();
            loop {
                let before = order.len();
                for (index, instance) in self.instances.iter().enumerate() {
                    if !visited.contains(&instance.key)
                        && dependencies[index].iter().all(|dep| visited.contains(dep))
                    {
                        visited.insert(instance.key.clone());
                        order.push(index);
                    }
                }
                if order.len() == before {
                    break;
                }
            }
        }
        let checks = vec![
            InspectionCheck {
                name: "preflight",
                status: if diagnostics.is_empty() {
                    CheckStatus::Passed
                } else {
                    CheckStatus::Failed
                },
            },
            InspectionCheck {
                name: "setup",
                status: CheckStatus::NotRun,
            },
            InspectionCheck {
                name: "runtime-connection",
                status: CheckStatus::NotRun,
            },
            InspectionCheck {
                name: "session-status",
                status: CheckStatus::Unsupported,
            },
        ];
        (
            Inspection {
                schema_version: 1,
                checks,
                instances,
                startup_order: order,
                diagnostics,
            },
            dependencies,
        )
    }
}
