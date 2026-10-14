# lenso-cache

Native typed, bounded process-local caching. This is subset alignment with
TypeScript `packages/cache`, not a remote adapter port.

Run the standalone example with `cargo run -p lenso-cache --example cache-native`.

`V: Serialize + DeserializeOwned + Send + Sync + 'static`. Storage is JSON,
deserialized independently for each read/waiter. `Option<V>` separates misses
from cached null (`V = Option<X>` gives `Some(None)`). Errors have sanitized
Display/Debug and no automatic source chain; `loader_cause()` explicitly grants
access to the original cause (which may contain private data).

```rust
use lenso_cache::{CacheConfig, MemoryCache};
use std::time::Duration;
# fn main() -> Result<(), lenso_cache::CacheError> {
let cache = MemoryCache::<Option<String>>::new(CacheConfig::default())?;
cache.set("nullable", None, Some(Duration::from_secs(1)))?;
assert_eq!(cache.get("nullable")?, Some(None));
let child = cache.scope("tenant:a")?;
child.invalidate()?; // exact scope only
# Ok(())
# }
```

```rust
use lenso_cache::{CacheConfig, CachePlugin};
use lenso_plugin_core::App;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut app = App::new();
let handle = app.install("summaries", CachePlugin::<String>::new(CacheConfig::default()));
let running = app.start().await?;
let guard = running.work_gate().enter()?;
let cache = running.get(&handle)?;
// Business services already admitted by their host call cache methods directly.
let value = cache.get_or_set("key", || async {
    Ok::<_, std::io::Error>("summary".to_owned())
}, None).await?;
assert_eq!(value, "summary");
drop(guard); // release external admission before awaiting drain
running.shutdown().await?;
assert!(cache.get("key").is_err());
# Ok(())
# }
```

## Rules and limits

- Explicit `Duration` TTL, default 60 seconds, maximum one day. `expires <= now`
  is expired. Zero set TTL deletes; zero loader TTL removes old hits and skips
  result storage. First coalesced caller chooses TTL and loader.
- Deterministic FIFO across the whole family: reads do not refresh ordering,
  overwrites become newest. Entry count and serialized value bytes plus key and
  scope UTF-8 bytes are bounded. Allocator overhead is not counted. Pruning scans
  at most `max_entries` entries on each admitted operation.
- Namespace/scope paths use vector equality, not concatenation. Invalidation
  affects one exact path. Independent constructors/plugin instances are isolated
  even with identical configured namespaces.
- `max_in_flight` bounds owned loader tasks, including fenced/cancelled tasks
  until Tokio finishes them. Busy errors do not queue. No generation tombstones
  or historical keys are retained. Batch reads preserve duplicates, max 100.
- Set/delete/invalidation fence old loaders; existing waiters may receive their
  result. Dropping one waiter cannot cancel another; dropping the last aborts its
  loader. Panic becomes `LoaderPanicked`; the process panic hook may still log
  the panic payload.
- `close()` seals the family once polled, settles waiters, aborts tasks and joins
  within one `close_timeout` budget. Concurrent close callers share completion;
  cancelling a close waiter does not cancel cleanup. Loaders must cooperate with
  Tokio (yield; never block its executor). Timeout reports incomplete joins;
  arbitrary blocking code cannot be forcibly stopped. Keep the runtime alive
  through close. Drop promises no asynchronous cleanup.
- The plugin owns its backend and closes in `Resources`, after app drain. No
  borrowed view or remote adapter is offered. Dependent calls do not reenter the
  global work gate so accepted business can finish during drain. Hosts must admit
  external calls with `RunningApp::work_gate()` before using services. Namespace
  is not a permission boundary.
- If a loader uses other app resources, it must own the accepted work guard for
  the duration of that background work, for example capture an `Arc<WorkGuard>`
  in its `move` closure. Keeping the guard only in a caller is insufficient:
  cancellation of that caller could otherwise release admission while a peer
  waiter still depends on the loader. The cache does not silently acquire a
  second global admission.

Not implemented: Redis/DB/Runtime, JSON envelope interoperability, failure-open
backend policy, runtime validators, remote generations, or permissions.
