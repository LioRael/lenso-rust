use lenso_events::{EventConfig, EventError, Events, EventsPlugin, LocalOutbox, OutboxPlugin};
use lenso_plugin_core::{App, Instance};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "lenso-events-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn backpressure_is_atomic_drop_frees_capacity_and_close_drains() {
    let events = Events::new(EventConfig {
        subscriber_capacity: 1,
        max_subscribers: 2,
        max_event_bytes: 8,
    })
    .unwrap();
    let mut fast = events.subscribe().unwrap();
    let slow = events.subscribe().unwrap();
    assert!(matches!(events.subscribe(), Err(EventError::Full)));
    assert_eq!(events.publish(vec![1]).unwrap(), 2);
    assert_eq!(&*fast.recv().await.unwrap(), &[1]);
    assert!(matches!(events.publish(vec![2]), Err(EventError::Full)));
    // Cancellation must not consume, and a failed fan-out must not queue to fast.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), fast.recv())
            .await
            .is_err()
    );
    drop(slow);
    assert_eq!(events.publish(vec![3]).unwrap(), 1);
    events.close();
    assert_eq!(&*fast.recv().await.unwrap(), &[3]);
    assert!(fast.recv().await.is_none());
    assert!(matches!(events.publish(vec![4]), Err(EventError::Closed)));
    assert!(matches!(events.subscribe(), Err(EventError::Closed)));
}

#[test]
fn durable_reopen_ack_capacity_and_exclusive_owner() {
    let dir = Dir::new();
    let outbox = LocalOutbox::open(&dir.0, 1, 8).unwrap();
    let id = outbox.enqueue(vec![1, 2]).unwrap();
    assert!(LocalOutbox::open(&dir.0, 1, 8).is_err());
    assert!(matches!(outbox.enqueue(vec![3]), Err(EventError::Full)));
    drop(outbox); // simulated interrupted dispatcher, no ack
    let outbox = LocalOutbox::open(&dir.0, 1, 8).unwrap();
    assert_eq!(outbox.pending(10).unwrap()[0].payload, vec![1, 2]);
    assert_eq!(outbox.pending(10).unwrap()[0].id, id);
    assert!(outbox.acknowledge(id).unwrap());
    assert!(!outbox.acknowledge(id).unwrap());
    outbox.close();
    assert!(matches!(outbox.pending(1), Err(EventError::Closed)));
    let outbox = LocalOutbox::open(&dir.0, 1, 8).unwrap();
    assert!(outbox.pending(10).unwrap().is_empty());
    assert!(outbox.enqueue(vec![3]).unwrap() > id);
    assert!(matches!(
        outbox.enqueue(vec![0; 9]),
        Err(EventError::Invalid)
    ));
}

#[test]
fn failed_persistence_seals_instance_and_keeps_previous_snapshot() {
    let dir = Dir::new();
    let outbox = LocalOutbox::open(&dir.0, 2, 8).unwrap();
    let id = outbox.enqueue(vec![1]).unwrap();
    std::fs::create_dir(dir.0.join("outbox.tmp")).unwrap();
    assert!(matches!(outbox.acknowledge(id), Err(EventError::Io(_))));
    assert!(matches!(outbox.enqueue(vec![2]), Err(EventError::Closed)));
    drop(outbox);
    let reopened = LocalOutbox::open(&dir.0, 2, 8).unwrap();
    assert_eq!(reopened.pending(10).unwrap()[0].id, id);
}

#[test]
fn corrupt_snapshot_is_not_silently_replaced() {
    let dir = Dir::new();
    std::fs::write(dir.0.join("outbox.json"), b"broken").unwrap();
    assert!(matches!(
        LocalOutbox::open(&dir.0, 2, 8),
        Err(EventError::Corrupt(_))
    ));
    assert_eq!(std::fs::read(dir.0.join("outbox.json")).unwrap(), b"broken");
}

#[tokio::test]
async fn plugin_shutdown_wakes_subscribers_and_releases_outbox_lock() {
    let dir = Dir::new();
    let instance = Instance::new("events", EventsPlugin(EventConfig::default()));
    let handle = instance.handle();
    let disk = Instance::new(
        "outbox",
        OutboxPlugin {
            root: dir.0.clone(),
            capacity: 10,
            max_event_bytes: 64,
        },
    );
    let disk_handle = disk.handle();
    let app = App::new().with(instance).with(disk).start().await.unwrap();
    let events = app.get(&handle).unwrap();
    let outbox = app.get(&disk_handle).unwrap();
    let mut subscription = events.subscribe().unwrap();
    outbox.enqueue(vec![1]).unwrap();
    app.shutdown().await.unwrap();
    assert!(subscription.recv().await.is_none());
    assert!(matches!(outbox.enqueue(vec![2]), Err(EventError::Closed)));
    assert_eq!(
        LocalOutbox::open(&dir.0, 10, 64)
            .unwrap()
            .pending(10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn durable_survives_process_exit_without_cleanup() {
    const CHILD_DIR: &str = "LENSO_EVENTS_TEST_CHILD_DIR";
    if let Some(path) = std::env::var_os(CHILD_DIR) {
        let outbox = LocalOutbox::open(path, 2, 8).unwrap();
        outbox.enqueue(vec![7, 8]).unwrap();
        // Bypass Rust destructors and any explicit service cleanup.
        std::process::exit(0);
    }
    let dir = Dir::new();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "durable_survives_process_exit_without_cleanup"])
        .env(CHILD_DIR, &dir.0)
        .status()
        .unwrap();
    assert!(status.success());
    let outbox = LocalOutbox::open(&dir.0, 2, 8).unwrap();
    assert_eq!(outbox.pending(1).unwrap()[0].payload, vec![7, 8]);
}
