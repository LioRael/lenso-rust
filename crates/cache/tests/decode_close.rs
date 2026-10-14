use lenso_cache::{CacheConfig, CacheError, MemoryCache};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
    time::{Duration, Instant},
};

static CACHE: OnceLock<MemoryCache<Reentrant>> = OnceLock::new();
static DECODING: AtomicBool = AtomicBool::new(false);
static CLOSING: AtomicBool = AtomicBool::new(false);

#[derive(Serialize)]
struct Reentrant(u64);

impl<'de> Deserialize<'de> for Reentrant {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        DECODING.store(true, Ordering::SeqCst);
        while !CLOSING.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(30));
        assert!(matches!(
            CACHE.get().unwrap().delete("other"),
            Err(CacheError::Closed)
        ));
        Ok(Self(value))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deserialization_reentry_during_close_does_not_retain_a_watch_lock() {
    if std::env::var_os("LENSO_CACHE_DECODE_PROBE").is_none() {
        // A lock-cycle regression can block Tokio itself, so bound an owned test process.
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "deserialization_reentry_during_close_does_not_retain_a_watch_lock",
            ])
            .env("LENSO_CACHE_DECODE_PROBE", "1")
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("cache decode/close lock cycle");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let cache = MemoryCache::<Reentrant>::new(CacheConfig {
        close_timeout: Duration::from_millis(10),
        ..CacheConfig::default()
    })
    .unwrap();
    assert!(CACHE.set(cache.clone()).is_ok());
    let runtime = tokio::runtime::Handle::current();
    let closing = std::thread::spawn(move || {
        while !DECODING.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        CLOSING.store(true, Ordering::SeqCst);
        runtime.block_on(cache.close()).unwrap();
    });
    let result = CACHE
        .get()
        .unwrap()
        .get_or_set(
            "key",
            || async { Ok::<_, std::io::Error>(Reentrant(1)) },
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.0, 1);
    closing.join().unwrap();
}
