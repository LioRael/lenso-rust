use lenso_plugin_core::{App, BoxError, CleanupHandle, CleanupPhase, Plugin, Setup, StartError};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Semaphore;

struct Early {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
    fail_setup: bool,
}
impl Plugin for Early {
    type Service = CleanupHandle;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<CleanupHandle>, BoxError> {
        let handle = setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            Err("early disposal failed".into())
        });
        if self.fail_setup {
            assert!(handle.dispose().await.is_err());
            Err("subsequent setup failed".into())
        } else {
            Ok(Arc::new(handle))
        }
    }
}

#[tokio::test]
async fn early_disposal_cancelled_waiter_shutdown_and_repeat_join_one_failure() {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let handle = app.install(
        "resource",
        Early {
            entered: entered.clone(),
            release: release.clone(),
            calls: calls.clone(),
            fail_setup: false,
        },
    );
    let running = app.start().await.unwrap();
    let cleanup = running.get(&handle).unwrap();
    let early = tokio::spawn({
        let cleanup = cleanup.clone();
        async move { cleanup.dispose().await }
    });
    entered.acquire().await.unwrap().forget();
    early.abort();
    assert!(early.await.unwrap_err().is_cancelled());
    let shutdown = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    release.add_permits(1);
    let shutdown = shutdown.await.unwrap().unwrap_err();
    let repeated = cleanup.dispose().await.unwrap_err();
    assert!(Arc::ptr_eq(&shutdown.failures[0].source, &repeated.source));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_early_disposal_is_retained_by_setup_rollback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.install(
        "resource",
        Early {
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(1)),
            calls: calls.clone(),
            fail_setup: true,
        },
    );
    let Err(StartError::Setup {
        cleanup: Some(cleanup),
        ..
    }) = app.start().await
    else {
        panic!("expected retained early disposal failure");
    };
    assert_eq!(cleanup.failures.len(), 1);
    assert_eq!(
        cleanup.failures[0].source.to_string(),
        "early disposal failed"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct PanicCleanup(Arc<AtomicUsize>);
impl Plugin for PanicCleanup {
    type Service = ();
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        setup.on_cleanup(CleanupPhase::Resources, || async {
            tokio::task::yield_now().await;
            panic!("cleanup poll panic");
        });
        setup.on_cleanup(CleanupPhase::Entrances, || {
            panic!("cleanup construction panic");
            #[allow(unreachable_code)]
            async {
                Ok(())
            }
        });
        Ok(Arc::new(()))
    }
}

#[tokio::test]
async fn cleanup_construction_and_poll_panics_do_not_skip_later_callbacks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.install("resource", PanicCleanup(calls.clone()));
    let running = app.start().await.unwrap();
    assert_eq!(running.shutdown().await.unwrap_err().failures.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
