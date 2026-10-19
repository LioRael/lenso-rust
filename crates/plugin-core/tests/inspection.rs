use lenso_plugin_core::{
    App, BoxError, CheckStatus, DiagnosticCode, Handle, Instance, InstanceKey, Plugin, Setup,
    StartError,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Node {
    deps: Vec<InstanceKey>,
    setups: Arc<AtomicUsize>,
}
impl Plugin for Node {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        self.deps.clone()
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        assert!(setup.instance_name().starts_with("node"));
        self.setups.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(()))
    }
}
fn node(deps: Vec<InstanceKey>, setups: &Arc<AtomicUsize>) -> Node {
    Node {
        deps,
        setups: setups.clone(),
    }
}

#[tokio::test]
async fn public_snapshot_is_serializable_and_borrows_without_setup_then_starts() {
    let setups = Arc::new(AtomicUsize::new(0));
    let dependency = Instance::new("node-dependency", node(vec![], &setups));
    let dependent = Instance::new(
        "node-dependent",
        node(vec![dependency.handle().key(); 2], &setups),
    );
    let app = App::new().with(dependent).with(dependency);
    let snapshot = app.inspect();
    assert!(snapshot.is_valid());
    assert_eq!(snapshot.startup_order, [1, 0]);
    assert_eq!(snapshot.instances[0].dependencies.len(), 2);
    assert_eq!(snapshot.instances[0].dependencies[1].target, Some(1));
    assert_eq!(snapshot, app.inspect());
    assert_eq!(setups.load(Ordering::SeqCst), 0);
    let json = serde_json::to_value(snapshot).unwrap();
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["checks"][0]["status"], "passed");
    assert_eq!(json["checks"][1]["status"], "not-run");
    assert_eq!(json["checks"][2]["name"], "runtime-connection");
    assert_eq!(json["checks"][3]["status"], "unsupported");
    let running = app.start().await.unwrap();
    assert_eq!(setups.load(Ordering::SeqCst), 2);
    running.shutdown().await.unwrap();
}

#[test]
fn inspect_needs_no_tokio_runtime_and_reports_missing_edge_and_closed_cycle_together() {
    let setups = Arc::new(AtomicUsize::new(0));
    let foreign = Instance::new("not-installed", node(vec![], &setups));
    let mut app = App::new();
    let a: Handle<()> = app.reserve("node-a");
    let b: Handle<()> = app.reserve("node-b");
    app.bind(&a, node(vec![b.key()], &setups));
    app.bind(&b, node(vec![a.key(), foreign.handle().key()], &setups));
    let report = app.inspect();
    assert!(!report.is_valid());
    assert_eq!(report.checks[0].status, CheckStatus::Failed);
    assert!(report.startup_order.is_empty());
    let missing = report
        .diagnostics
        .iter()
        .find(|d| d.code == DiagnosticCode::MissingDependency)
        .unwrap();
    assert_eq!(
        (missing.instance, missing.dependency_index),
        (Some(1), Some(1))
    );
    assert_eq!(report.instances[1].dependencies[1].target, None);
    let cycle = report
        .diagnostics
        .iter()
        .find(|d| d.code == DiagnosticCode::DependencyCycle)
        .unwrap();
    assert_eq!(cycle.path, [0, 1, 0]);
    assert_eq!(setups.load(Ordering::SeqCst), 0);
    assert!(!format!("{report:?}").contains("0x"));
}

#[test]
fn self_cycle_and_disconnected_cycles_have_actual_closed_paths() {
    let setups = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let a: Handle<()> = app.reserve("node-a");
    let b: Handle<()> = app.reserve("node-b");
    app.bind(&a, node(vec![a.key()], &setups));
    app.bind(&b, node(vec![b.key()], &setups));
    let report = app.inspect();
    let paths: Vec<_> = report.diagnostics.iter().map(|d| d.path.clone()).collect();
    assert_eq!(paths, [vec![0, 0], vec![1, 1]]);
}

#[test]
fn binding_and_reservation_failures_are_structured() {
    let setups = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let a: Handle<()> = app.reserve("node-a");
    app.bind(&a, node(vec![], &setups));
    app.bind(&a, node(vec![], &setups));
    app.reserve::<()>("node-a");
    let foreign = Instance::new("foreign", node(vec![], &setups)).handle();
    app.bind(&foreign, node(vec![], &setups));
    let report = app.inspect();
    for code in [
        DiagnosticCode::InvalidBinding,
        DiagnosticCode::ForeignBinding,
        DiagnosticCode::DuplicateName,
        DiagnosticCode::UnboundInstance,
    ] {
        assert!(report.diagnostics.iter().any(|d| d.code == code));
    }
}

struct Invalid {
    panic_validation: bool,
}
impl Plugin for Invalid {
    type Service = ();
    fn validate(&self) -> Result<(), BoxError> {
        if self.panic_validation {
            panic!("secret-panic-value");
        }
        Err("secret-config-value".into())
    }
    fn dependencies(&self) -> Vec<InstanceKey> {
        panic!("secret-dependency-value")
    }
    async fn setup(self, _: &mut Setup) -> Result<Arc<()>, BoxError> {
        panic!("setup must not run")
    }
}

#[tokio::test]
async fn errors_and_panic_payloads_never_enter_public_diagnostics_or_start_errors() {
    let mut app = App::new();
    app.install(
        "invalid",
        Invalid {
            panic_validation: false,
        },
    );
    app.install(
        "panic",
        Invalid {
            panic_validation: true,
        },
    );
    let report = app.inspect();
    for code in [
        DiagnosticCode::InvalidConfig,
        DiagnosticCode::ValidationPanicked,
        DiagnosticCode::DependenciesPanicked,
    ] {
        assert!(report.diagnostics.iter().any(|d| d.code == code));
    }
    let text = format!("{report:?} {}", serde_json::to_string(&report).unwrap());
    assert!(!text.contains("secret-"));
    let Err(StartError::Validation(errors)) = app.start().await else {
        panic!("expected validation failure")
    };
    assert_eq!(errors.len(), report.diagnostics.len());
    assert!(!format!("{errors:?}").contains("secret-"));
}
