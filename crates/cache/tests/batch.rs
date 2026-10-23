use lenso_cache::{CacheConfig, CacheError, MemoryCache};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    cell::RefCell,
    sync::Arc,
    time::{Duration, Instant},
};

thread_local! {
    static ON_DECODE: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

#[derive(Debug, PartialEq, Serialize)]
struct Reentrant(u64);

impl<'de> Deserialize<'de> for Reentrant {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        let action = ON_DECODE.with(|slot| slot.borrow_mut().take());
        if let Some(action) = action {
            action();
        }
        Ok(Self(value))
    }
}

fn on_decode(action: impl FnOnce() + 'static) {
    ON_DECODE.with(|slot| {
        assert!(slot.borrow_mut().replace(Box::new(action)).is_none());
    });
}

#[test]
fn batch_reads_preserve_per_key_reentry_expiry_and_close() {
    if std::env::var_os("LENSO_CACHE_BATCH_PROBE").is_none() {
        // Retaining a state lock through user deserialization would deadlock.
        // Bound an owned process, as in the existing decode/close regression.
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "batch_reads_preserve_per_key_reentry_expiry_and_close",
            ])
            .env("LENSO_CACHE_BATCH_PROBE", "1")
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
                panic!("batch deserialization retained a cache lock");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap(),
    );
    let _entered = runtime.enter();
    runtime.block_on(async { tokio::time::pause() });
    let cache = MemoryCache::<Reentrant>::new(CacheConfig::default()).unwrap();
    cache.set("first", Reentrant(1), None).unwrap();
    cache.set("next", Reentrant(2), None).unwrap();

    let writer = cache.clone();
    on_decode(move || {
        writer.set("next", Reentrant(9), None).unwrap();
    });
    assert_eq!(
        cache
            .get_many(&["first", "next", "missing", "next"])
            .unwrap(),
        vec![
            Some(Reentrant(1)),
            Some(Reentrant(9)),
            None,
            Some(Reentrant(9)),
        ]
    );

    cache
        .set("expires", Reentrant(3), Some(Duration::from_secs(10)))
        .unwrap();
    let clock = runtime.clone();
    on_decode(move || {
        clock.block_on(tokio::time::advance(Duration::from_secs(10)));
    });
    assert_eq!(
        cache.get_many(&["first", "expires"]).unwrap(),
        vec![Some(Reentrant(1)), None]
    );

    // Rejecting a later invalid key must not skip earlier per-key deserialization.
    let writer = cache.clone();
    on_decode(move || {
        writer.set("next", Reentrant(10), None).unwrap();
    });
    assert!(matches!(
        cache.get_many(&["first", ""]),
        Err(CacheError::InvalidInput)
    ));
    assert_eq!(cache.get("next").unwrap(), Some(Reentrant(10)));
    assert!(cache.get_many(&[]).unwrap().is_empty());
    assert_eq!(cache.get_many(&["next"; 100]).unwrap().len(), 100);

    let closer = cache.clone();
    let closing_runtime = runtime.clone();
    on_decode(move || closing_runtime.block_on(closer.close()).unwrap());
    assert!(matches!(
        cache.get_many(&["first", "next"]),
        Err(CacheError::Closed)
    ));
    assert!(matches!(cache.get_many(&[]), Err(CacheError::Closed)));
    assert!(matches!(cache.get_many(&[""]), Err(CacheError::Closed)));
    assert!(matches!(
        cache.get_many(&["next"; 101]),
        Err(CacheError::InvalidInput)
    ));
}
