use lenso_cache::{CacheConfig, CacheError, CachePlugin, MemoryCache, WriteOutcome};
use lenso_plugin_core::App;
use std::{
    future::{poll_fn, Future},
    io,
    pin::Pin,
    task::Poll,
    time::Duration,
};
use tokio::sync::oneshot;

async fn admit<T>(future: Pin<&mut impl Future<Output = T>>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
fn cache() -> MemoryCache<String> {
    MemoryCache::new(CacheConfig::default()).unwrap()
}
async fn value(s: &'static str) -> Result<String, io::Error> {
    Ok(s.into())
}

#[tokio::test(start_paused = true)]
async fn ttl_expires_at_boundary_and_overwrite_refreshes_expiry() {
    let cache = cache();
    cache
        .set("k", "old".into(), Some(Duration::from_secs(10)))
        .unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    cache
        .set("k", "new".into(), Some(Duration::from_secs(20)))
        .unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(cache.get("k").unwrap(), Some("new".into()));
    tokio::time::advance(Duration::from_secs(15)).await;
    assert_eq!(cache.get("k").unwrap(), None);
}

#[test]
fn null_and_ordered_duplicate_batch_hits_are_not_misses() {
    let cache = MemoryCache::<Option<String>>::new(CacheConfig::default()).unwrap();
    cache.set("null", None, None).unwrap();
    assert_eq!(
        cache.get_many(&["null", "missing", "null"]).unwrap(),
        vec![Some(None), None, Some(None)]
    );
}

#[test]
fn exact_scopes_and_separate_instances_do_not_collide() {
    let root = cache();
    let child = root.scope("a:b").unwrap();
    let nested = root.scope("a").unwrap().scope("b").unwrap();
    let other = cache();
    for view in [&root, &child, &nested, &other] {
        view.set("k", "present".into(), None).unwrap();
    }
    root.invalidate().unwrap();
    child.delete("k").unwrap();
    assert_eq!(root.get("k").unwrap(), None);
    assert_eq!(child.get("k").unwrap(), None);
    assert_eq!(nested.get("k").unwrap(), Some("present".into()));
    assert_eq!(other.get("k").unwrap(), Some("present".into()));
}

#[test]
fn fifo_reads_do_not_refresh_and_overwrites_become_newest() {
    let cache = MemoryCache::<String>::new(CacheConfig {
        max_entries: 2,
        ..CacheConfig::default()
    })
    .unwrap();
    cache.set("a", "a".into(), None).unwrap();
    cache.set("b", "b".into(), None).unwrap();
    cache.get("a").unwrap();
    cache.set("c", "c".into(), None).unwrap();
    assert_eq!(cache.get("a").unwrap(), None);
    cache.set("b", "B".into(), None).unwrap();
    cache.set("d", "d".into(), None).unwrap();
    assert_eq!(cache.get("c").unwrap(), None);
    assert_eq!(cache.get("b").unwrap(), Some("B".into()));
}

#[test]
fn bytes_include_serialized_value_key_and_scope_and_reject_large_values() {
    let cache = MemoryCache::<String>::new(CacheConfig {
        max_bytes: 18,
        max_value_bytes: 4,
        ..CacheConfig::default()
    })
    .unwrap();
    // Each entry: namespace 5, key 1, JSON string 3 = 9 bytes.
    cache.set("a", "a".into(), None).unwrap();
    cache.set("b", "b".into(), None).unwrap();
    cache.set("c", "c".into(), None).unwrap();
    assert_eq!(cache.get("a").unwrap(), None);
    assert!(matches!(
        cache.set("x", "large".into(), None),
        Err(CacheError::Serialization)
    ));
}

#[tokio::test]
async fn coalesces_same_key_and_cancellation_of_one_waiter_keeps_the_other() {
    let cache = cache();
    let (finish, wait) = oneshot::channel();
    let mut first = Box::pin(cache.get_or_set(
        "k",
        || async move {
            wait.await.unwrap();
            value("first").await
        },
        None,
    ));
    admit(first.as_mut()).await;
    let mut second = Box::pin(cache.get_or_set("k", || value("second"), None));
    admit(second.as_mut()).await;
    drop(first);
    finish.send(()).unwrap();
    assert_eq!(second.await.unwrap(), "first");
    assert_eq!(cache.get("k").unwrap(), Some("first".into()));
    cache.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_last_waiter_aborts_loader_and_allows_retry() {
    let cache = cache();
    let (started, start) = oneshot::channel();
    let (finish, wait) = oneshot::channel::<()>();
    let mut load = Box::pin(cache.get_or_set(
        "k",
        || async move {
            started.send(()).unwrap();
            wait.await.unwrap();
            value("old").await
        },
        None,
    ));
    admit(load.as_mut()).await;
    start.await.unwrap();
    drop(load);
    tokio::task::yield_now().await;
    assert!(finish.is_closed());
    assert_eq!(
        cache
            .get_or_set("k", || value("fresh"), None)
            .await
            .unwrap(),
        "fresh"
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn set_delete_and_invalidate_fence_pending_loader_writeback() {
    for operation in ["set", "delete", "invalidate"] {
        let cache = cache();
        let (finish, wait) = oneshot::channel();
        let mut load = Box::pin(cache.get_or_set(
            "k",
            || async move {
                wait.await.unwrap();
                value("old").await
            },
            None,
        ));
        admit(load.as_mut()).await;
        match operation {
            "set" => {
                cache.set("k", "new".into(), None).unwrap();
            }
            "delete" => cache.delete("k").unwrap(),
            _ => cache.invalidate().unwrap(),
        }
        finish.send(()).unwrap();
        assert_eq!(load.await.unwrap(), "old");
        assert_eq!(
            cache.get("k").unwrap(),
            if operation == "set" {
                Some("new".into())
            } else {
                None
            }
        );
        cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn zero_ttl_deletes_hits_and_first_coalesced_loader_ttl_wins() {
    let cache = cache();
    cache.set("k", "old".into(), None).unwrap();
    let (finish, wait) = oneshot::channel();
    let mut first = Box::pin(cache.get_or_set(
        "k",
        || async move {
            wait.await.unwrap();
            value("fresh").await
        },
        Some(Duration::ZERO),
    ));
    admit(first.as_mut()).await;
    let mut second = Box::pin(cache.get_or_set("k", || value("wrong"), None));
    admit(second.as_mut()).await;
    finish.send(()).unwrap();
    assert_eq!(first.await.unwrap(), "fresh");
    assert_eq!(second.await.unwrap(), "fresh");
    assert_eq!(cache.get("k").unwrap(), None);
    assert_eq!(
        cache
            .set("k", "ignored".into(), Some(Duration::ZERO))
            .unwrap(),
        WriteOutcome::Skipped
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn fenced_tasks_still_consume_capacity_and_close_settles_their_waiters() {
    let cache = MemoryCache::<String>::new(CacheConfig {
        max_in_flight: 1,
        ..CacheConfig::default()
    })
    .unwrap();
    let mut load = Box::pin(cache.get_or_set(
        "k",
        || async { std::future::pending::<Result<String, io::Error>>().await },
        None,
    ));
    admit(load.as_mut()).await;
    cache.invalidate().unwrap();
    assert!(matches!(
        cache.get_or_set("other", || value("no"), None).await,
        Err(CacheError::Busy)
    ));
    cache.close().await.unwrap();
    assert!(matches!(load.await, Err(CacheError::Closed)));
    assert!(matches!(cache.get("k"), Err(CacheError::Closed)));
    cache.close().await.unwrap();
}

#[tokio::test]
async fn loader_errors_are_sanitized_with_explicit_cause_and_panics_settle() {
    let cache = cache();
    let error = cache
        .get_or_set(
            "k",
            || async { Err::<String, _>(io::Error::other("private data")) },
            None,
        )
        .await
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("private"));
    assert!(error.source().is_none());
    assert_eq!(error.loader_cause().unwrap().to_string(), "private data");
    let error = cache
        .get_or_set(
            "k",
            || async {
                panic!("private panic payload");
                #[allow(unreachable_code)]
                value("no").await
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CacheError::LoaderPanicked));
    assert_eq!(
        cache
            .get_or_set("k", || value("retry"), None)
            .await
            .unwrap(),
        "retry"
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn owned_plugin_instances_are_isolated_and_retained_arcs_are_sealed() {
    let mut app = App::new();
    let a = app.install("a", CachePlugin::<String>::new(CacheConfig::default()));
    let b = app.install("b", CachePlugin::<String>::new(CacheConfig::default()));
    let app = app.start().await.unwrap();
    let a = app.get(&a).unwrap();
    let b = app.get(&b).unwrap();
    a.set("k", "a".into(), None).unwrap();
    assert_eq!(b.get("k").unwrap(), None);
    app.shutdown().await.unwrap();
    assert!(matches!(a.get("k"), Err(CacheError::Closed)));
    assert!(matches!(b.scope("child"), Err(CacheError::Closed)));
}

#[test]
fn invalid_configuration_and_names_fail_without_secret_values() {
    assert!(matches!(
        MemoryCache::<String>::new(CacheConfig {
            max_entries: 0,
            ..CacheConfig::default()
        }),
        Err(CacheError::InvalidInput)
    ));
    let cache = cache();
    for key in ["", "\n", "\u{85}"] {
        assert!(matches!(cache.get(key), Err(CacheError::InvalidInput)));
    }
    assert!(matches!(
        cache.set("k", "v".into(), Some(Duration::from_secs(86401))),
        Err(CacheError::InvalidInput)
    ));
}

#[tokio::test]
async fn cancelled_close_waiter_does_not_cancel_owned_cleanup() {
    let cache = cache();
    let mut load =
        Box::pin(cache.get_or_set("k", std::future::pending::<Result<String, io::Error>>, None));
    admit(load.as_mut()).await;
    let mut close = Box::pin(cache.close());
    admit(close.as_mut()).await;
    drop(close);
    assert!(matches!(cache.get("k"), Err(CacheError::Closed)));
    cache.close().await.unwrap();
    assert!(matches!(load.await, Err(CacheError::Closed)));
}

#[tokio::test]
async fn panic_settles_all_coalesced_waiters() {
    let cache = cache();
    let child = cache.scope("child").unwrap();
    let (finish, wait) = oneshot::channel();
    let mut first = Box::pin(child.get_or_set(
        "k",
        || async move {
            wait.await.unwrap();
            panic!("loader panic");
            #[expect(unreachable_code)]
            value("no").await
        },
        None,
    ));
    admit(first.as_mut()).await;
    let mut second = Box::pin(child.get_or_set("k", || value("wrong"), None));
    admit(second.as_mut()).await;
    cache.invalidate().unwrap();
    finish.send(()).unwrap();
    assert!(matches!(first.await, Err(CacheError::LoaderPanicked)));
    assert!(matches!(second.await, Err(CacheError::LoaderPanicked)));
    cache.close().await.unwrap();
}

#[tokio::test]
async fn root_invalidation_does_not_fence_child_loader() {
    let cache = cache();
    let child = cache.scope("child").unwrap();
    let (finish, wait) = oneshot::channel();
    let mut load = Box::pin(child.get_or_set(
        "k",
        || async move {
            wait.await.unwrap();
            value("child").await
        },
        None,
    ));
    admit(load.as_mut()).await;
    cache.invalidate().unwrap();
    finish.send(()).unwrap();
    assert_eq!(load.await.unwrap(), "child");
    assert_eq!(child.get("k").unwrap(), Some("child".into()));
    cache.close().await.unwrap();
}

#[tokio::test]
async fn close_joins_cooperative_loader_before_returning() {
    struct Dropped(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let cache = cache();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let witness = Dropped(dropped.clone());
    let (started, start) = oneshot::channel();
    let mut load = Box::pin(cache.get_or_set(
        "k",
        || async move {
            let _witness = witness;
            started.send(()).unwrap();
            std::future::pending::<Result<String, io::Error>>().await
        },
        None,
    ));
    admit(load.as_mut()).await;
    start.await.unwrap();
    cache.close().await.unwrap();
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    assert!(matches!(load.await, Err(CacheError::Closed)));
}

use std::error::Error;
