# lenso-limits

Fixed epoch rate/quota windows and weighted concurrent leases. Manual business
APIs work without an app or Tokio runtime. Optional async execution uses Tokio;
neither mode needs a DB or Auth plugin.

```rust
use lenso_limits::{AcquireInput, ConsumeInput, FailurePolicy, LimitScope, Limits, MemoryLimitStore};
use std::{sync::Arc, time::Duration};

let store = Arc::new(MemoryLimitStore::new());
let limits = Limits::new(store, FailurePolicy::Deny);
let scope = LimitScope::new("reports", "tenant-a", "subject:123")?;
let rate = limits.consume_rate(&ConsumeInput {
    scope: scope.clone(), capacity: 5, quantity: 2,
    period: Duration::from_secs(60),
})?;
assert_eq!(rate.remaining, Some(3));
let acquired = limits.acquire(&AcquireInput {
    scope, capacity: 4, quantity: 2, ttl: Duration::from_secs(30),
})?;
if let Some(lease) = acquired.lease {
    let renewed = limits.renew(&lease, Duration::from_secs(60))?;
    assert!(renewed.is_some());
    limits.release(&lease)?;
}
limits.close()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

See `examples/limits-native.rs` for standalone use and `examples/injected.rs`
for exact plugin instance composition. `PARITY.md` records the TypeScript
reference, supported behaviors and intentionally deferred features.

## Owned async execution

`with_lease` acquires, executes a closure, and releases on success, business error,
callback panic or cooperative cancellation. The service is an `Arc` because an
owned runner must finish cleanup even when the caller drops its waiting future.
The closure and its future/result/error must be `Send + 'static`.

```rust
use lenso_limits::{AcquireInput, FailurePolicy, LeaseCancellation, LeaseRunOptions,
    LimitScope, Limits, MemoryLimitStore};
use std::{sync::Arc, time::Duration};
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let limits = Arc::new(Limits::new(Arc::new(MemoryLimitStore::new()), FailurePolicy::Error));
let stop = LeaseCancellation::new(); // The caller can keep a clone and call cancel().
let value = limits.with_lease(
    AcquireInput {
        scope: LimitScope::new("reports", "tenant-a", "running")?,
        capacity: 2, quantity: 1, ttl: Duration::from_secs(1),
    },
    LeaseRunOptions {
        renew_every: Some(Duration::from_millis(200)),
        cancellation: Some(stop),
    },
    |run| async move {
        tokio::select! {
            biased;
            reason = run.cancelled() => Err(reason),
            _ = tokio::time::sleep(Duration::from_millis(10)) => Ok(42),
        }
    },
).await?;
assert_eq!(value, 42);
limits.close_async().await?;
# Ok(())
# }
```

Run `cargo run -p lenso-limits --example lease-run` for renewal across the original
TTL followed by explicit cancellation. Default options perform **no renewal**.
An interval must be positive whole milliseconds and less than TTL. The injected
provider clock is used for liveness observations, while a conservative Tokio
deadline detects local expiry or executor stalls. Crossing the old deadline
before or during renewal stops the execution even if a late renewal succeeds.
Executions without automatic renewal are also cancelled at their deadline.

The runner checks `LimitStore::observe` before business, after renewal and before
reporting success. Observation never renews. `None` means lost; a backend failure
in observation or renewal stops execution under every admission policy. The
default `observe` implementation fails safely, so custom stores must implement it
to support leased execution. Fail-open admission remains visibly unleased:
`lease_snapshot()` is `None` and no renewal is attempted.

Business must cooperate with `run.cancelled()` or `run.cancellation.check()`.
Cancellation signals the callback, stops renewal, waits for it to finish, and
then releases. A callback ignoring cancellation can delay closing indefinitely.
Dropping the waiting future requests cancellation but the runner retains cleanup
ownership. Keep the Tokio runtime (with time enabled) alive until close completes.
`close_async()` also retains its close task when its waiting future is dropped.
Calling it from inside the callback being drained would wait on itself; signal
cancellation and return from that callback instead.

`LeaseRunError` retains lifecycle failures, the business error and a release error
independently. It does not silently erase one failure with another. Failed release
stays owned for close to retry. Callback construction/poll panics are reported and
released; store/clock panics and runtime destruction remain outside the provider
contract and cleanup guarantee.

**A lease is not fencing and business effects cannot be automatically rolled
back.** A clock jump, scheduling delay, process pause or concurrent release can
invalidate a snapshot before the callback observes cancellation. `lease_snapshot`
returns the latest observation, not a safety assertion; it becomes unavailable
on observed cancellation or after the runner finishes. Check cancellation between
work steps and design effect idempotency/compensation in the business layer. An
error after business finishes reports loss; it cannot undo work already performed.

## Semantics

- Rate and quota are separate counters keyed by the structured
  `(instance, tenant, key)` scope. Fixed windows start at `floor(epoch / period) * period`.
- Capacity and period cannot change for an existing counter; lease capacity cannot
  change for an existing lease scope, even after expiry. Use a new key for a new policy.
- Only allowed consumption changes usage. Quantities count units, not calls.
  Oversized requests have no finite retry estimate; lease retry estimates wait for
  enough total weight to expire, assuming holders do not renew.
- Expiry is exclusive: a lease expiring at `now` is already lost. Renew cannot
  revive it or shorten a live lease. Release is idempotent and token-specific.
- Clock regressions are clamped per counter/scope. Use
  `MemoryLimitStore::with_clock(|| epoch_milliseconds)` for deterministic clocks.
- Counts are positive `u32`, capped at `i32::MAX`. Durations must be whole, positive
  milliseconds, at most 366 days. Scopes contain 1–512 UTF-16 units per field.
  Invalid data and policy conflicts are errors under every failure policy.

## Failure and ownership

Choose `FailurePolicy` explicitly. `Error` propagates backend failures; `Deny`
and `Allow` return a decision with `BackendFailure`, unknown remaining/retry, and
no lease. **Fail-open admission is not a concurrency guarantee.** Renewal and
release always propagate failure. There is no implicit logging or raw provider
error output; callers can observe the typed outcome.

`Limits` borrows its store. It owns only leases it acquires; renewing a lease
borrowed from another service does not transfer ownership. Manual calls are synchronous
and serialized with close, so a close waits for an in-flight call before releasing
its successful acquisition. It attempts every owned release and caches the result
for concurrent/repeated callers. A failed explicit release stays owned and is
retried by close. Close seals new admission immediately, before waiting for the
in-flight call. With active async executions, `close()` signals cancellation and
returns `CloseError` containing `ActiveRuns` without releasing their leases. Use
`close_async().await` to drain them and finish close; this transitional result is
not cached. A final release failure is cached; remaining leases expire by TTL or can
be released via the store. Dropping the service does not call close.

`LimitsPlugin` registers Entrances and Resources cleanup immediately and may borrow either an
`Arc<S>` or a specific `Handle<S>`. Dependency cleanup order keeps the store alive
until limits close. The service is a dependency-local API: it does not reenter the
application WorkGate. Entrances calls `stop_runs` to stop admitting wrappers and
cancel current ones before global work drain; manual methods remain available
to already-admitted work. Resources calls `close_async` and waits for all wrapper
cleanup before the store owner's cleanup. Application business entry points should hold their
`WorkGuard` through their work; this lets admitted work finish during shutdown.
Retained service references reject calls after resource cleanup closes them.

`LimitStore` is a small synchronous, atomic, non-reentrant interface for this
process-local implementation. It must not block on network/DB I/O. Each service
holds its own lock during store calls, and the memory store locks its own state.
Manual methods start no renewal timers or tasks. The injected clock is a trusted,
quick, non-reentrant callback. Panics are not converted into admission success;
poisoned locks yield `Internal`.

Store state is shared only through the same explicit store object. Distinct store
objects remain isolated. Process-unique tokens are opaque identifiers, not
credentials and not suitable as distributed tokens. Scope strings are trusted
application labels, never permission grants. The memory store retains policy and
clock history for each used scope; callers must bound cardinality. The service
also retains ownership records for expired leases until explicit release,
renewal returns `None`, or close; long-lived callers should release leases rather
than relying on expiry alone to reclaim local tracking. It does not
provide persistence, eviction, cross-process coordination, DB or Auth integration.
