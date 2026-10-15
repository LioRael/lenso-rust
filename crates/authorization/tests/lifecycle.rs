#![cfg(feature = "plugin")]
mod support;
use lenso_authorization::*;
use lenso_plugin_core::{App, BoxError, CleanupPhase, Handle, Plugin, Setup};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use support::*;

struct Source {
    snapshot: Arc<RoleSnapshot>,
    closed: Arc<AtomicBool>,
}
impl RoleSource for Source {
    fn read<'a>(&'a self, evaluation: &'a Evaluation) -> EvaluationFuture<'a, Arc<RoleSnapshot>> {
        Box::pin(async move {
            evaluation.ensure_active()?;
            if self.closed.load(Ordering::SeqCst) {
                Err(EvaluationError)
            } else {
                Ok(self.snapshot.clone())
            }
        })
    }
}
struct Provider {
    graph: RoleGraph,
    setups: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}
impl Plugin for Provider {
    type Service = Source;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Source>, BoxError> {
        self.setups.fetch_add(1, Ordering::SeqCst);
        let closed = Arc::new(AtomicBool::new(false));
        let cleanup_closed = closed.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup_closed.store(true, Ordering::SeqCst);
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        Ok(Arc::new(Source {
            snapshot: Arc::new(RoleSnapshot {
                revision: 0,
                graph: self.graph,
            }),
            closed,
        }))
    }
}
fn authorization(source: Handle<Source>) -> AuthorizationPlugin {
    AuthorizationPlugin::setup(vec![source.key()], move |setup| {
        Box::pin(async move {
            let source = setup.get(&source)?;
            let mut config = options();
            config
                .policies
                .push(Arc::new(Rbac::from_source(["read"], source)?));
            Ok(Arc::new(Authorization::new(config)?))
        })
    })
}
fn provider(graph: RoleGraph, setups: &Arc<AtomicUsize>, closes: &Arc<AtomicUsize>) -> Provider {
    Provider {
        graph,
        setups: setups.clone(),
        closes: closes.clone(),
    }
}

#[tokio::test]
async fn exact_dependency_instances_and_owner_cleanup_are_preserved() {
    let setups = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let grants = app.install("grants", provider(graph(), &setups, &closes));
    let revoked = app.install("revoked", provider(RoleGraph::default(), &setups, &closes));
    let granted_auth = app.install("granted-auth", authorization(grants));
    let denied_auth = app.install("denied-auth", authorization(revoked));
    assert_eq!(setups.load(Ordering::SeqCst), 0);
    let running = app.start().await.unwrap();
    assert_eq!(setups.load(Ordering::SeqCst), 2);
    assert!(running.get(&granted_auth).unwrap().can(request()).await);
    assert!(!running.get(&denied_auth).unwrap().can(request()).await);
    running.shutdown().await.unwrap();
    running.shutdown().await.unwrap();
    assert_eq!(closes.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn same_name_impostor_does_not_satisfy_dependency_and_undeclared_get_fails() {
    let setups = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let mut original = App::new();
    let handle = original.install("roles", provider(graph(), &setups, &closes));
    let mut app = App::new();
    app.install("roles", provider(graph(), &setups, &closes));
    app.install("auth", authorization(handle));
    assert!(app.start().await.is_err());
    assert_eq!(setups.load(Ordering::SeqCst), 0);
    let mut app = App::new();
    let handle = app.install("roles", provider(graph(), &setups, &closes));
    app.install(
        "auth",
        AuthorizationPlugin::setup(vec![], move |setup| {
            Box::pin(async move {
                setup.get(&handle)?;
                Ok(Arc::new(Authorization::new(options())?))
            })
        }),
    );
    assert!(app.start().await.is_err());
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn setup_failure_rolls_back_owner_resources_once() {
    let setups = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let source = app.install("roles", provider(graph(), &setups, &closes));
    app.install(
        "auth",
        AuthorizationPlugin::setup(vec![source.key()], move |setup| {
            Box::pin(async move {
                let _source = setup.get(&source)?;
                Err("application setup failed".into())
            })
        }),
    );
    assert!(app.start().await.is_err());
    assert_eq!(setups.load(Ordering::SeqCst), 1);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn outer_work_guard_keeps_dependencies_alive_during_shutdown() {
    let setups = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let source = app.install("roles", provider(graph(), &setups, &closes));
    let auth = app.install("auth", authorization(source));
    let running = app.start().await.unwrap();
    let auth = running.get(&auth).unwrap();
    let gate = running.work_gate();
    let admitted = gate.enter().unwrap();
    let stopping = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    // Wait until Core has sealed new business admission.
    while gate.enter().is_ok() {
        tokio::task::yield_now().await;
    }
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    assert!(auth.can(request()).await); // No nested admission that would reject accepted work.
    drop(admitted);
    stopping.await.unwrap().unwrap();
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert!(gate.enter().is_err());
    assert_eq!(
        auth.check(request()).await.code,
        ReasonCode::EvaluationFailed
    );
}

#[tokio::test]
async fn resource_free_service_works_standalone_and_plugin_does_not_invent_a_lifetime() {
    assert!(AuthorizationPlugin::new(AuthorizationOptions::new(["read", "read"])).is_err());
    let direct = Authorization::new(allow_options()).unwrap();
    assert!(direct.can(request()).await);
    let mut app = App::new();
    let handle = app.install("auth", AuthorizationPlugin::new(allow_options()).unwrap());
    let running = app.start().await.unwrap();
    let retained = running.get(&handle).unwrap();
    running.shutdown().await.unwrap();
    assert!(retained.can(request()).await); // Pure dependency; outer business entry owns admission.
}
