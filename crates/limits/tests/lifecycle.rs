use lenso_limits::{
    AcquireInput, FailurePolicy, LeaseToken, LimitError, LimitScope, LimitStore, Limits,
    LimitsPlugin, MemoryLimitStore,
};
use lenso_plugin_core::{
    App, BoxError, CleanupPhase, Handle, Instance, InstanceKey, Plugin, Setup,
};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::oneshot;

fn input() -> AcquireInput {
    AcquireInput {
        scope: LimitScope::new("reports", "one", "running").unwrap(),
        capacity: 1,
        quantity: 1,
        ttl: Duration::from_secs(60),
    }
}

struct StorePlugin {
    store: Arc<MemoryLimitStore>,
    cleaned: Arc<AtomicUsize>,
}
impl Plugin for StorePlugin {
    type Service = MemoryLimitStore;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let store = self.store.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            // The dependent limits must release before its exact provider cleans up.
            let lease = store
                .acquire(&input(), LeaseToken::fresh()?)?
                .lease
                .ok_or_else(|| io::Error::other("dependent retained a lease"))?;
            store.release(&lease.scope, lease.token)?;
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        Ok(self.store)
    }
}

#[tokio::test]
async fn exact_instances_release_before_provider_cleanup_and_keep_borrowed_stores_usable() {
    let a = Arc::new(MemoryLimitStore::with_clock(|| 0));
    let b = Arc::new(MemoryLimitStore::with_clock(|| 0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let store_a = Instance::new(
        "store-a",
        StorePlugin {
            store: a.clone(),
            cleaned: cleaned.clone(),
        },
    );
    let store_b = Instance::new(
        "store-b",
        StorePlugin {
            store: b.clone(),
            cleaned: cleaned.clone(),
        },
    );
    let limits_a = Instance::new(
        "limits-a",
        LimitsPlugin::from_handle(store_a.handle(), FailurePolicy::Error),
    );
    let limits_b = Instance::new(
        "limits-b",
        LimitsPlugin::from_handle(store_b.handle(), FailurePolicy::Error),
    );
    let handle_a = limits_a.handle();
    let handle_b = limits_b.handle();
    let app = App::new()
        .with(limits_b)
        .with(store_a)
        .with(limits_a)
        .with(store_b)
        .start()
        .await
        .unwrap();
    let retained_a = app.get(&handle_a).unwrap();
    let retained_b = app.get(&handle_b).unwrap();
    assert!(retained_a.acquire(&input()).unwrap().decision.allowed);
    assert!(retained_b.acquire(&input()).unwrap().decision.allowed);
    assert!(!retained_a.acquire(&input()).unwrap().decision.allowed);
    let (left, right) = tokio::join!(app.shutdown(), app.shutdown());
    left.unwrap();
    right.unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 2);
    assert_eq!(retained_a.acquire(&input()), Err(LimitError::Closed));
    assert_eq!(retained_b.acquire(&input()), Err(LimitError::Closed));
    assert!(app.get(&handle_a).is_err());
    for store in [a, b] {
        assert!(
            store
                .acquire(&input(), LeaseToken::fresh().unwrap())
                .unwrap()
                .decision
                .allowed
        );
    }
}

struct Failing {
    limits: Handle<Limits>,
}
impl Plugin for Failing {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.limits.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        assert!(setup.get(&self.limits)?.acquire(&input())?.decision.allowed);
        Err(io::Error::other("later setup failed").into())
    }
}

#[tokio::test]
async fn startup_rollback_releases_held_lease_and_foreign_handle_is_rejected() {
    let store = Arc::new(MemoryLimitStore::with_clock(|| 0));
    let limits = Instance::new(
        "limits",
        LimitsPlugin::new(store.clone(), FailurePolicy::Error),
    );
    let failing = Instance::new(
        "failure",
        Failing {
            limits: limits.handle(),
        },
    );
    assert!(App::new().with(failing).with(limits).start().await.is_err());
    let lease = store
        .acquire(&input(), LeaseToken::fresh().unwrap())
        .unwrap()
        .lease
        .unwrap();
    store.release(&lease.scope, lease.token).unwrap();

    let external = Instance::new(
        "foreign",
        StorePlugin {
            store,
            cleaned: Arc::new(AtomicUsize::new(0)),
        },
    );
    let wrong = Instance::new(
        "wrong",
        LimitsPlugin::from_handle(external.handle(), FailurePolicy::Error),
    );
    assert!(App::new().with(wrong).start().await.is_err());
}

struct EntranceSignal(oneshot::Sender<()>);
impl Plugin for EntranceSignal {
    type Service = ();
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            let _ = self.0.send(());
            Ok(())
        });
        Ok(Arc::new(()))
    }
}

#[tokio::test]
async fn admitted_work_can_use_limits_after_gate_seal_before_resources_close() {
    let store = Arc::new(MemoryLimitStore::with_clock(|| 0));
    let limits = Instance::new(
        "limits",
        LimitsPlugin::new(store.clone(), FailurePolicy::Error),
    );
    let handle = limits.handle();
    let (sealed_tx, sealed_rx) = oneshot::channel();
    let app = App::new()
        .with(limits)
        .with(Instance::new("entrance", EntranceSignal(sealed_tx)))
        .start()
        .await
        .unwrap();
    let service = app.get(&handle).unwrap();
    let gate = app.work_gate();
    let guard = gate.enter().unwrap();
    let lease = service.acquire(&input()).unwrap().lease.unwrap();
    let shutdown_app = app.clone();
    let shutdown = tokio::spawn(async move { shutdown_app.shutdown().await });
    sealed_rx.await.unwrap();
    assert!(gate.enter().is_err());
    assert!(!service.is_closed());
    assert!(service.renew(&lease, input().ttl).unwrap().is_some());
    service.release(&lease).unwrap();
    drop(guard);
    shutdown.await.unwrap().unwrap();
    assert!(service.is_closed());
    assert!(
        store
            .acquire(&input(), LeaseToken::fresh().unwrap())
            .unwrap()
            .decision
            .allowed
    );
}
