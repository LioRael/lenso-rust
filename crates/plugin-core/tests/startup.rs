use lenso_plugin_core::{App, BoxError, CleanupPhase, InstanceKey, Plugin, Setup, StartError};
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};
use tokio::sync::Semaphore;

struct Startup {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    cleaned: Arc<Semaphore>,
    fail: bool,
}
impl Plugin for Startup {
    type Service = ();
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.cleaned.add_permits(1);
            Ok(())
        });
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if self.fail {
            Err("setup failed".into())
        } else {
            Ok(Arc::new(()))
        }
    }
}

#[tokio::test]
async fn cancellation_during_setup_finishes_and_cleans_undelivered_app_or_rolls_back_error() {
    for fail in [false, true] {
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let cleaned = Arc::new(Semaphore::new(0));
        let mut app = App::new();
        app.install(
            "startup",
            Startup {
                entered: entered.clone(),
                release: release.clone(),
                cleaned: cleaned.clone(),
                fail,
            },
        );
        let waiter = tokio::spawn(app.start());
        entered.acquire().await.unwrap().forget();
        waiter.abort();
        assert!(matches!(waiter.await, Err(e) if e.is_cancelled()));
        release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(2), cleaned.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert_eq!(cleaned.available_permits(), 0);
    }
}

#[tokio::test]
async fn cancellation_after_delivery_is_queued_still_shuts_down_unacknowledged_app() {
    let entered = Arc::new(Semaphore::new(0));
    let cleaned = Arc::new(Semaphore::new(0));
    let mut app = App::new();
    app.install(
        "startup",
        Startup {
            entered: entered.clone(),
            release: Arc::new(Semaphore::new(1)),
            cleaned: cleaned.clone(),
            fail: false,
        },
    );
    let mut start = Box::pin(app.start());
    poll_fn(|cx| {
        assert!(start.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    entered.acquire().await.unwrap().forget();
    // On this single-threaded runtime the owned task has now returned from setup
    // and queued delivery, but the start future has never polled that delivery.
    drop(start);
    tokio::time::timeout(std::time::Duration::from_secs(2), cleaned.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[derive(Clone, Copy)]
enum PanicAt {
    Validate,
    Dependencies,
    Construction,
    Poll,
}
struct Panicker {
    at: PanicAt,
    cleaned: Arc<AtomicUsize>,
}
impl Plugin for Panicker {
    type Service = ();
    fn validate(&self) -> Result<(), BoxError> {
        if matches!(self.at, PanicAt::Validate) {
            panic!("sensitive validation panic");
        }
        Ok(())
    }
    fn dependencies(&self) -> Vec<InstanceKey> {
        if matches!(self.at, PanicAt::Dependencies) {
            panic!("sensitive dependency panic");
        }
        vec![]
    }
    fn setup(self, setup: &mut Setup) -> impl Future<Output = Result<Arc<()>, BoxError>> + Send {
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            Err("rollback failure".into())
        });
        if matches!(self.at, PanicAt::Construction) {
            panic!("sensitive construction panic");
        }
        async move {
            if matches!(self.at, PanicAt::Poll) {
                tokio::task::yield_now().await;
                panic!("sensitive poll panic");
            }
            Ok(Arc::new(()))
        }
    }
}

#[tokio::test]
async fn validation_and_dependency_unwinds_prevent_all_setup() {
    for at in [PanicAt::Validate, PanicAt::Dependencies] {
        let cleaned = Arc::new(AtomicUsize::new(0));
        let mut app = App::new();
        app.install(
            "panic",
            Panicker {
                at,
                cleaned: cleaned.clone(),
            },
        );
        let Err(StartError::Validation(errors)) = app.start().await else {
            panic!("expected validation error");
        };
        assert!(errors.iter().any(|e| e.contains("panicked for panic")));
        assert!(!format!("{errors:?}").contains("sensitive"));
        assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn construction_and_poll_unwinds_preserve_partial_setup_cleanup_and_failures() {
    for at in [PanicAt::Construction, PanicAt::Poll] {
        let cleaned = Arc::new(AtomicUsize::new(0));
        let mut app = App::new();
        app.install(
            "panic",
            Panicker {
                at,
                cleaned: cleaned.clone(),
            },
        );
        let Err(StartError::Setup {
            source,
            cleanup: Some(cleanup),
            ..
        }) = app.start().await
        else {
            panic!("expected setup error and rollback failure");
        };
        assert_eq!(source.to_string(), "plugin setup panicked");
        assert_eq!(cleanup.failures.len(), 1);
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }
}
