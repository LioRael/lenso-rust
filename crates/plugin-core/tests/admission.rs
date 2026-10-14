use lenso_plugin_core::{
    AccessError, App, BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup, WorkGate,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Semaphore;

struct Resource(Arc<AtomicBool>);
impl Resource {
    fn local_call(&self) {
        assert!(!self.0.load(Ordering::SeqCst));
    }
}

struct Provider(Arc<AtomicBool>);
impl Plugin for Provider {
    type Service = Resource;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Resource>, BoxError> {
        let closed = self.0.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            closed.store(true, Ordering::SeqCst);
            Ok(())
        });
        Ok(Arc::new(Resource(self.0)))
    }
}

struct Business {
    gate: WorkGate,
    resource: Arc<Resource>,
}
impl Business {
    async fn call(&self, entered: &Semaphore, release: &Semaphore) -> Result<(), AccessError> {
        let _guard = self.gate.enter()?;
        entered.add_permits(1);
        release.acquire().await.unwrap().forget();
        self.resource.local_call();
        Ok(())
    }
}
struct Dependent {
    resource: Handle<Resource>,
    entrances: Arc<Semaphore>,
}
impl Plugin for Dependent {
    type Service = Business;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.resource.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Business>, BoxError> {
        assert_eq!(setup.instance_name(), "business");
        let gate = setup.work_gate();
        let resource = setup.get(&self.resource)?;
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            self.entrances.add_permits(1);
            Err("entrance failure must not skip drain".into())
        });
        Ok(Arc::new(Business { gate, resource }))
    }
}

#[tokio::test]
async fn seal_precedes_entrances_and_resource_cleanup_waits_for_dependent_business_work() {
    let closed = Arc::new(AtomicBool::new(false));
    let entrances = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut app = App::new();
    let provider = app.install("resource", Provider(closed.clone()));
    let handle = app.install(
        "business",
        Dependent {
            resource: provider,
            entrances: entrances.clone(),
        },
    );
    let running = app.start().await.unwrap();
    let retained = running.get(&handle).unwrap();
    let business = tokio::spawn({
        let retained = retained.clone();
        let entered = entered.clone();
        let release = release.clone();
        async move { retained.call(&entered, &release).await }
    });
    entered.acquire().await.unwrap().forget();
    let shutdown = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    entrances.acquire().await.unwrap().forget();
    assert!(matches!(
        running.work_gate().enter(),
        Err(AccessError::Closing)
    ));
    assert!(matches!(running.get(&handle), Err(AccessError::Closing)));
    assert!(matches!(
        retained.call(&entered, &release).await,
        Err(AccessError::Closing)
    ));
    assert!(!closed.load(Ordering::SeqCst));
    assert!(!shutdown.is_finished());
    release.add_permits(1);
    business.await.unwrap().unwrap();
    assert_eq!(shutdown.await.unwrap().unwrap_err().failures.len(), 1);
    assert!(closed.load(Ordering::SeqCst));
    assert_eq!(running.shutdown().await.unwrap_err().failures.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admission_and_shutdown_never_use_a_closed_resource() {
    let closed = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let handle = app.install("resource", Provider(closed.clone()));
    let running = app.start().await.unwrap();
    let resource = running.get(&handle).unwrap();
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let gate = running.work_gate();
        let resource = resource.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                match gate.enter() {
                    Ok(_guard) => {
                        tokio::task::yield_now().await;
                        resource.local_call();
                    }
                    Err(AccessError::Closing) => break,
                    Err(e) => panic!("{e}"),
                }
            }
        }));
    }
    running.shutdown().await.unwrap();
    for task in tasks {
        task.await.unwrap();
    }
    assert!(closed.load(Ordering::SeqCst));
}

struct FailingDependent {
    resource: Handle<Resource>,
    release: Arc<Semaphore>,
    entrances: Arc<Semaphore>,
}
impl Plugin for FailingDependent {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.resource.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        let resource = setup.get(&self.resource)?;
        let guard = setup.work_gate().enter()?;
        tokio::spawn(async move {
            let _guard = guard;
            self.release.acquire().await.unwrap().forget();
            resource.local_call();
        });
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            self.entrances.add_permits(1);
            Ok(())
        });
        Err("dependent setup failed".into())
    }
}

#[tokio::test]
async fn rollback_seals_and_drains_work_before_closing_prior_dependencies() {
    let closed = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Semaphore::new(0));
    let entrances = Arc::new(Semaphore::new(0));
    let mut app = App::new();
    let resource = app.install("resource", Provider(closed.clone()));
    app.install(
        "failing",
        FailingDependent {
            resource,
            release: release.clone(),
            entrances: entrances.clone(),
        },
    );
    let startup = tokio::spawn(app.start());
    entrances.acquire().await.unwrap().forget();
    assert!(!startup.is_finished());
    assert!(!closed.load(Ordering::SeqCst));
    release.add_permits(1);
    assert!(matches!(
        startup.await.unwrap(),
        Err(lenso_plugin_core::StartError::Setup { .. })
    ));
    assert!(closed.load(Ordering::SeqCst));
}
