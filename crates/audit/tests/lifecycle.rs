#![cfg(feature = "plugin")]
mod support;
use lenso_audit::*;
use lenso_plugin_core::{App, CleanupPhase, Handle, Instance, InstanceKey, Plugin, Setup};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use support::*;
use tokio::sync::Semaphore;

struct Provider<T> {
    value: Arc<T>,
    acquired: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
}
impl<T: Send + Sync + 'static> Plugin for Provider<T> {
    type Service = T;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<T>, BoxError> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        Ok(self.value)
    }
}
fn provider<T>(
    value: Arc<T>,
    acquired: &Arc<AtomicUsize>,
    closed: &Arc<AtomicUsize>,
) -> Provider<T> {
    Provider {
        value,
        acquired: acquired.clone(),
        closed: closed.clone(),
    }
}

struct EntranceSignal(Arc<Semaphore>);
impl Plugin for EntranceSignal {
    type Service = ();
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            self.0.add_permits(1);
            Ok(())
        });
        Ok(Arc::new(()))
    }
}

#[tokio::test]
async fn admitted_work_finishes_during_drain_and_closed_complete_never_touches_reporter() {
    let f = Fixture::new(true);
    let acquired = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let sealed = Arc::new(Semaphore::new(0));
    let mut app = App::new();
    let repo = app.install(
        "repository",
        provider(f.repository.clone(), &acquired, &closed),
    );
    let auth = app.install(
        "authority",
        provider(f.authority.clone(), &acquired, &closed),
    );
    let reporter = app.install("reporter", provider(f.reporter.clone(), &acquired, &closed));
    let audit = app.install(
        "audit",
        AuditPlugin::new(repo, auth, config()).with_reporter(reporter),
    );
    app.install("entry", EntranceSignal(sealed.clone()));
    let running = app.start().await.unwrap();
    let guard = running.work_gate().enter().unwrap();
    let service = running.get(&audit).unwrap();
    let PrepareResult::Ready(receipt) = service.prepare(intent(1), "alice").await.unwrap() else {
        panic!("expected ready")
    };
    let shutdown = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    sealed.acquire().await.unwrap().forget();
    assert!(running.work_gate().enter().is_err());
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    // Dependency-local Audit calls do not reenter the sealed app gate.
    service.complete(&receipt, outcome(2)).await.unwrap();
    service.append(input(3), "alice").await.unwrap();
    assert!(!shutdown.is_finished());
    drop(guard);
    shutdown.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    assert_eq!(acquired.load(Ordering::SeqCst), 3);
    assert_eq!(closed.load(Ordering::SeqCst), 3);
    let before_storage = f.repository.calls.load(Ordering::SeqCst);
    let before_report = f.reporter.entries.lock().unwrap().len();
    assert_eq!(
        service.complete(&receipt, outcome(2)).await.unwrap_err(),
        AuditError::Closed
    );
    assert_eq!(
        service.append(input(4), "alice").await.unwrap_err(),
        AuditError::Closed
    );
    assert_eq!(
        service
            .query(AuditQuery::new(scope()), "alice")
            .await
            .unwrap_err(),
        AuditError::Closed
    );
    assert_eq!(f.repository.calls.load(Ordering::SeqCst), before_storage);
    assert_eq!(f.reporter.entries.lock().unwrap().len(), before_report);
}

#[tokio::test]
async fn config_preflight_and_exact_instance_dependencies_fail_before_acquisition() {
    for scenario in 0..3 {
        let acquired = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let repo = Instance::new(
            "repository",
            provider(Arc::new(TestRepository::new(false)), &acquired, &closed),
        );
        let auth = Instance::new(
            "authority",
            provider(Arc::new(Authority::default()), &acquired, &closed),
        );
        let mut config = config();
        if scenario == 0 {
            config.max_page_size = 0;
        }
        if scenario == 1 {
            config
                .summary_policy
                .get_mut("note.remove")
                .unwrap()
                .insert("password".into(), SummaryRule::Boolean);
        }
        let audit = Instance::new(
            "audit",
            AuditPlugin::new(repo.handle(), auth.handle(), config),
        );
        let app = App::new().with(audit).with(repo);
        let app = if scenario == 2 {
            // A same-name/same-type lookalike does not satisfy the supplied handle.
            app.with(Instance::new(
                "authority",
                provider(Arc::new(Authority::default()), &acquired, &closed),
            ))
        } else {
            app.with(auth)
        };
        assert!(app.start().await.is_err());
        assert_eq!(acquired.load(Ordering::SeqCst), 0);
        assert_eq!(closed.load(Ordering::SeqCst), 0);
    }
}

type Service = AuditService<TestRepository, Authority>;
struct FailingDependent {
    audit: Handle<Service>,
    retained: Arc<Mutex<Option<Arc<Service>>>>,
}
impl Plugin for FailingDependent {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.audit.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        *self.retained.lock().unwrap() = Some(setup.get(&self.audit)?);
        Err("intentional downstream setup failure".into())
    }
}

#[tokio::test]
async fn rollback_closes_service_and_owners_once_and_separate_instances_remain_isolated() {
    let acquired = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let retained = Arc::new(Mutex::new(None));
    let mut app = App::new();
    let repo = app.install(
        "repo",
        provider(Arc::new(TestRepository::new(false)), &acquired, &closed),
    );
    let auth = app.install(
        "auth",
        provider(Arc::new(Authority::default()), &acquired, &closed),
    );
    let audit = app.install("audit", AuditPlugin::new(repo, auth, config()));
    app.install(
        "fail",
        FailingDependent {
            audit,
            retained: retained.clone(),
        },
    );
    assert!(app.start().await.is_err());
    assert_eq!(closed.load(Ordering::SeqCst), 2);
    let service = retained.lock().unwrap().take().unwrap();
    assert_eq!(
        service.append(input(1), "alice").await.unwrap_err(),
        AuditError::Closed
    );

    let mut app = App::new();
    let auth = app.install(
        "auth",
        provider(Arc::new(Authority::default()), &acquired, &closed),
    );
    let repo_a = app.install(
        "repo-a",
        provider(Arc::new(TestRepository::new(false)), &acquired, &closed),
    );
    let repo_b = app.install(
        "repo-b",
        provider(Arc::new(TestRepository::new(false)), &acquired, &closed),
    );
    let a = app.install("audit-a", AuditPlugin::new(repo_a, auth.clone(), config()));
    let b = app.install("audit-b", AuditPlugin::new(repo_b, auth, config()));
    let running = app.start().await.unwrap();
    let guard = running.work_gate().enter().unwrap();
    running
        .get(&a)
        .unwrap()
        .append(input(1), "alice")
        .await
        .unwrap();
    assert!(running
        .get(&b)
        .unwrap()
        .get(&scope(), &id(1), "alice")
        .await
        .unwrap()
        .is_none());
    drop(guard);
    running.shutdown().await.unwrap();
    assert_eq!(
        closed.load(Ordering::SeqCst),
        acquired.load(Ordering::SeqCst)
    );
}
