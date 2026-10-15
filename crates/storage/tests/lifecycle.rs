#![cfg(unix)]

use lenso_plugin_core::{App, BoxError, Handle, InstanceKey, Plugin, Setup, WorkGate};
use lenso_storage::*;
use std::{
    future::{poll_fn, Future},
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::Semaphore,
};

struct ConsumerPlugin {
    storage: Handle<LocalStorage>,
}
struct Consumer {
    storage: Arc<LocalStorage>,
    gate: WorkGate,
}
impl Plugin for ConsumerPlugin {
    type Service = Consumer;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.storage.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Consumer>, BoxError> {
        Ok(Arc::new(Consumer {
            storage: setup.get(&self.storage)?,
            gate: setup.work_gate(),
        }))
    }
}

#[tokio::test]
async fn exact_instance_injection_and_resources_close_after_admitted_work_drains() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let mut app = App::new();
    let first = app.install(
        "first",
        LocalStoragePlugin::new(LocalStorageConfig::new(base.join("first"))),
    );
    let second = app.install(
        "second",
        LocalStoragePlugin::new(LocalStorageConfig::new(base.join("second"))),
    );
    let consumer = app.install(
        "consumer",
        ConsumerPlugin {
            storage: second.clone(),
        },
    );
    let app = app.start().await.unwrap();
    let first = app.get(&first).unwrap();
    let second = app.get(&second).unwrap();
    let consumer = app.get(&consumer).unwrap();
    assert!(Arc::ptr_eq(&second, &consumer.storage));
    assert!(!Arc::ptr_eq(&first, &consumer.storage));
    let admitted = consumer.gate.enter().unwrap();
    let mut shutdown = Box::pin(app.shutdown());
    poll_fn(|cx| {
        assert!(shutdown.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(consumer.gate.enter().is_err());
    // A dependency-local operation must not reenter the application's sealed gate.
    consumer
        .storage
        .put(PutObject::new("work", io::Cursor::new(b"done")))
        .await
        .unwrap();
    assert!(first
        .head("work", OperationOptions::default())
        .await
        .unwrap()
        .is_none());
    drop(admitted);
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second
            .head("work", OperationOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Aborted
    );
    let reopened = LocalStorage::open(LocalStorageConfig::new(base.join("second")))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .head("work", OperationOptions::default())
            .await
            .unwrap()
            .unwrap()
            .size,
        4
    );
    reopened.close().await.unwrap();
}

struct Stalled(Arc<Semaphore>);
impl AsyncRead for Stalled {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.add_permits(1);
        Poll::Pending
    }
}
struct FailingPlugin {
    storage: Handle<LocalStorage>,
    cancelled: Arc<Semaphore>,
}
impl Plugin for FailingPlugin {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.storage.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        let storage = setup.get(&self.storage)?;
        let entered = Arc::new(Semaphore::new(0));
        let ready = entered.clone();
        tokio::spawn(async move {
            let error = storage
                .put(PutObject::new("pending", Stalled(entered)))
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::Aborted);
            self.cancelled.add_permits(1);
        });
        ready.acquire().await.unwrap().forget();
        Err("intentional dependent setup failure".into())
    }
}

#[tokio::test]
async fn startup_rollback_cancels_owned_work_and_preserves_committed_data() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("objects");
    let service = LocalStorage::open(LocalStorageConfig::new(&root))
        .await
        .unwrap();
    service
        .put(PutObject::new("keep", io::Cursor::new(b"value")))
        .await
        .unwrap();
    service.close().await.unwrap();
    let mut app = App::new();
    let cancelled = Arc::new(Semaphore::new(0));
    let storage = app.install(
        "storage",
        LocalStoragePlugin::new(LocalStorageConfig::new(&root)),
    );
    app.install(
        "fail",
        FailingPlugin {
            storage,
            cancelled: cancelled.clone(),
        },
    );
    assert!(tokio::time::timeout(Duration::from_secs(5), app.start())
        .await
        .unwrap()
        .is_err());
    tokio::time::timeout(Duration::from_secs(5), cancelled.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    let reopened = LocalStorage::open(LocalStorageConfig::new(root))
        .await
        .unwrap();
    assert!(reopened
        .head("keep", OperationOptions::default())
        .await
        .unwrap()
        .is_some());
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_close_waiter_does_not_cancel_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("objects");
    let storage = LocalStorage::open(LocalStorageConfig::new(&root))
        .await
        .unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let ready = entered.clone();
    let worker = storage.clone();
    let upload = tokio::spawn(async move {
        worker
            .put(PutObject::new("pending", Stalled(entered)))
            .await
    });
    ready.acquire().await.unwrap().forget();
    let mut close = Box::pin(storage.close());
    poll_fn(|cx| {
        let _ = close.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
    drop(close);
    tokio::time::timeout(Duration::from_secs(5), storage.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        upload.await.unwrap().unwrap_err().code(),
        ErrorCode::Aborted
    );
    assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
}
