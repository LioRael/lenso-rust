use lenso_configuration::{ConfigurationPlugin, Error, RequestConfig};
use lenso_plugin_core::{App, Config, ConfigSource, Instance};
use serde::Deserialize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};

#[derive(Deserialize)]
struct Pricing {
    shipping_cents: u64,
    #[serde(default = "currency")]
    currency: String,
    credential: String,
}
fn currency() -> String {
    "USD".into()
}
// The checkout retains a single admitted snapshot through pricing and payment.
struct Checkout {
    config: RequestConfig<Pricing>,
}
impl Checkout {
    fn quote(&self, subtotal: u64) -> (u64, &str) {
        (
            subtotal + self.config.value().shipping_cents,
            &self.config.value().currency,
        )
    }
}
fn resolve(json: serde_json::Value) -> Config<Pricing> {
    Config::new("checkout")
        .explicit_json(json)
        .diagnostic_field("/shipping_cents")
        .diagnostic_field("/currency")
        .diagnostic_field("/credential")
        .sensitive("/credential")
}
fn write(path: &std::path::Path, shipping: u64) {
    std::fs::write(
        path,
        format!(r#"{{"shipping_cents":{shipping},"credential":"synthetic-private-marker"}}"#),
    )
    .unwrap();
}
#[tokio::test]
async fn checkout_retains_price_and_failed_reloads_retain_version_without_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pricing.json");
    write(&path, 500);
    let instance = Instance::new(
        "pricing",
        ConfigurationPlugin::new(path.clone(), 512, resolve, |p| {
            p.shipping_cents <= 2000 && !p.credential.is_empty()
        }),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let config = app.get(&handle).unwrap();
    let old = Checkout {
        config: config.request().unwrap(),
    };
    assert_eq!(old.quote(1000), (1500, "USD"));
    assert_eq!(old.config.sources()[0].source, ConfigSource::Explicit);
    assert_eq!(old.config.sources()[1].source, ConfigSource::Defaults);
    write(&path, 700);
    assert_eq!(config.reload(1).await.unwrap(), 2);
    let new = Checkout {
        config: config.request().unwrap(),
    };
    assert_eq!(new.quote(1000), (1700, "USD"));
    assert_eq!(old.quote(1000), (1500, "USD"));
    for raw in [
        r#"{"shipping_cents":3000,"credential":"synthetic-private-marker"}"#,
        r#"{"shipping_cents":700,"credential":{"synthetic-private-marker":1}}"#,
        "synthetic-private-marker",
    ] {
        std::fs::write(&path, raw).unwrap();
        let error = config.reload(2).await.unwrap_err();
        assert!(!format!("{error:?} {error}").contains("synthetic-private-marker"));
        assert_eq!(config.request().unwrap().version(), 2);
    }
    std::fs::write(&path, "x".repeat(513)).unwrap();
    assert!(matches!(config.reload(2).await, Err(Error::TooLarge)));
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(config.reload(2).await, Err(Error::File)));
    drop(old);
    drop(new);
    app.shutdown().await.unwrap();
    assert!(matches!(config.request(), Err(Error::Closed)));
    assert!(matches!(config.reload(2).await, Err(Error::Closed)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_reloads_publish_exactly_one_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pricing.json");
    write(&path, 500);
    let block = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let b = block.clone();
    let instance = Instance::new(
        "pricing",
        ConfigurationPlugin::new(path.clone(), 512, resolve, move |_| {
            if b.load(Ordering::SeqCst) {
                barrier.wait();
            }
            true
        }),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let config = app.get(&handle).unwrap();
    write(&path, 700);
    block.store(true, Ordering::SeqCst);
    let (a, b) = tokio::join!(config.reload(1), config.reload(1));
    assert!(matches!(
        (&a, &b),
        (Ok(2), Err(Error::Conflict)) | (Err(Error::Conflict), Ok(2))
    ));
    assert_eq!(config.request().unwrap().version(), 2);
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_cancelled_reload_and_old_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pricing.json");
    write(&path, 500);
    let block = Arc::new(AtomicBool::new(false));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let b = block.clone();
    let r = release.clone();
    let instance = Instance::new(
        "pricing",
        ConfigurationPlugin::new(path, 512, resolve, move |_| {
            if b.load(Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                let (lock, wake) = &*r;
                let guard = lock.lock().unwrap();
                let (guard, timeout) = wake
                    .wait_timeout_while(guard, std::time::Duration::from_secs(10), |done| !*done)
                    .unwrap();
                assert!(!timeout.timed_out() && *guard);
            }
            true
        }),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let config = app.get(&handle).unwrap();
    let old = config.request().unwrap();
    block.store(true, Ordering::SeqCst);
    let c = config.clone();
    let reload = tokio::spawn(async move { c.reload(1).await });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    reload.abort();
    let closing = tokio::spawn(async move { app.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while config.request().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!closing.is_finished());
    assert_eq!(old.version(), 1);
    drop(old);
    assert!(!closing.is_finished());
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    tokio::time::timeout(std::time::Duration::from_secs(5), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(config.request(), Err(Error::Closed)));
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlink_file_source() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real.json");
    write(&real, 500);
    let link = dir.path().join("link.json");
    std::os::unix::fs::symlink(real, &link).unwrap();
    let instance = Instance::new(
        "pricing",
        ConfigurationPlugin::new(link, 512, resolve, |_| true),
    );
    assert!(App::new().with(instance).start().await.is_err());
}
