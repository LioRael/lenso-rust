use lenso_log::{LogError, Logger, LoggerOptions};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex, OnceLock, Weak,
    },
    time::Duration,
};
use tracing::Dispatch;

#[test]
fn host_writer_can_log_through_the_same_logger() {
    let (send, receive) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let slot = Arc::new(OnceLock::<Weak<Logger>>::new());
        let captured = slot.clone();
        let entered = Arc::new(AtomicUsize::new(0));
        let observed = entered.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                if let Some(logger) = captured.get().and_then(Weak::upgrade) {
                    logger.info(json!({}), "nested").unwrap();
                }
                std::io::sink()
            })
            .finish();
        let logger = Arc::new(Logger::borrowed(
            "test",
            "reentrant",
            &Dispatch::new(subscriber),
            LoggerOptions::default(),
        ));
        slot.set(Arc::downgrade(&logger)).unwrap();
        logger.info(json!({}), "outer").unwrap();
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        send.send(()).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(5))
        .expect("logging deadlocked or panicked");
    worker.join().unwrap();
}

#[tokio::test]
async fn close_drains_accepted_writes_even_after_its_waiter_is_cancelled() {
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || {
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            std::io::sink()
        })
        .finish();
    let flushed = Arc::new(AtomicUsize::new(0));
    let observed = flushed.clone();
    let logger = Logger::owned_with_flush(
        "test",
        "drain",
        Dispatch::new(subscriber),
        LoggerOptions::default(),
        Duration::from_secs(1),
        move || async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );
    let writer = logger.clone();
    let thread = std::thread::spawn(move || writer.info(json!({}), "accepted"));
    started.await.unwrap();
    let closing = logger.clone();
    let mut close = tokio::spawn(async move { closing.close().await });
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut close)
        .await
        .is_err());
    assert_eq!(flushed.load(Ordering::SeqCst), 0);
    assert_eq!(logger.info(json!({}), "late"), Err(LogError::Closed));
    close.abort();
    assert!(close.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    thread.join().unwrap().unwrap();
    logger.close().await.unwrap();
    assert_eq!(flushed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn panicking_subscriber_releases_write_admission_without_poisoning_it() {
    let panic_once = AtomicBool::new(true);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || {
            assert!(
                !panic_once.swap(false, Ordering::SeqCst),
                "fixture writer panic"
            );
            std::io::sink()
        })
        .finish();
    let logger = Logger::borrowed(
        "test",
        "panic",
        &Dispatch::new(subscriber),
        LoggerOptions::default(),
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        logger.info(json!({}), "first")
    }))
    .is_err());
    logger.info(json!({}), "second").unwrap();
    tokio::time::timeout(Duration::from_secs(1), logger.close())
        .await
        .unwrap()
        .unwrap();
}
