# lenso-realtime

Process-local, bounded resource subscriptions with host-injected authorization.
`MemoryRealtime<P>` is an ordinary Rust service. `RealtimePlugin<P>` only validates,
resolves an optional exact instance dependency, and registers shutdown cleanup.
No Redis, Auth, HTTP, Runtime SDK, permission container, or global registry is used.
Run async subscription/receive methods in a Tokio runtime with time enabled.

```rust
use lenso_realtime::{Identity, Lease, MemoryRealtime, RealtimeConfig, Resource};
use serde_json::json;
use std::time::Duration;
use tokio::time::Instant;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let realtime = MemoryRealtime::new(RealtimeConfig::default(),
    |identity: std::sync::Arc<Identity<bool>>, _resource| async move {
        Ok(identity.principal.then_some(Lease {
            valid_until: Instant::now() + Duration::from_secs(10),
        }))
    })?;
let resource = Resource::new("workspace-1", "note", "n1")?;
let mut connection = realtime.connect(Identity {
    scope: "workspace-1".into(), subject: "reader".into(), principal: true,
    expires_at: Instant::now() + Duration::from_secs(60),
})?;
let subscription = connection.subscribe(resource.clone()).await?;
let ready = connection.recv().await.unwrap();
let report = realtime.publish(&resource, "note.updated", json!({"revision": 2}))?;
assert_eq!(report.enqueued, 1);
let update = connection.recv().await.unwrap();
subscription.renew().await?;
realtime.revoke_resource(&resource);
realtime.close();
# Ok(())
# }
```

Run `cargo run -p lenso-realtime --example memory` for the standalone example,
or `--example injected` for exact instance composition with plugin-core.

## Delivery and authorization contract

- Resource keys are structured `(scope, type, id)` values. `topic()` matches TS
  `encodeURIComponent` escaping and is never interpreted as a pattern. Each part
  must be 1–256 UTF-8 bytes. Connections cannot subscribe outside their scope.
- The host verifies identities and controls access to publishing/revocation.
  Connections expose no publishing API. Principals are not serialized into events.
- The authorizer is invoked only by subscribe/renew, outside the state lock.
  `Ok(None)`, callback errors and unwind panics deny access. Grant deadlines are
  clamped to the call's start plus the configured lease (maximum 30 seconds), and
  identity expiry. Time uses `tokio::time::Instant`, not wall time.
- All admission limits include pending subscriptions. Pending callbacks have a
  global limit and timeout; their futures are owned by the caller and dropped on
  timeout/cancellation. Host callbacks must be cooperative async work: detached
  tasks or blocking work created by the callback remain the host's responsibility.
- Renewal must finish before the previous lease expires. A failed or cancelled
  admitted renewal removes its subscription. A concurrent renewal returns `Limit`.
  Revoke, unsubscribe, connection close and service close prevent a late grant
  from reviving a subscription. Revocation is not a persistent denial list; update
  the host policy before revoking if future subscriptions should also be denied.
- Each connection has a bounded event count and exact serialized byte budget.
  Overflow purges its entire queue, returns one terminal `Gap(Overflow)`, then EOF.
  `PublishReport` counts this call's surviving enqueues and overflowed connections;
  it is not an acknowledgement of consumer processing or durability.
- Unsubscribe, revoke, deletion and expiry purge queued data for that subscription.
  Subject revoke/connection close/shutdown purge the full connection queue. Data
  already returned by `recv` belongs to the consumer and cannot be recalled.
- `recv(&mut self)` has one consumer and is cancel-safe. It checks expiry before
  returning queued data and wakes on the nearest lease/session deadline. No timer
  task is spawned. Idle entries are physically reclaimed on the next operation or
  `stats()`; deadlines are always enforced on reads, delivery and admission.
- Keep each `Subscription` alive: dropping it unsubscribes. Dropping `Connection`
  closes it. `close()` is synchronous and idempotent; it signals pending callbacks,
  which are dropped when their caller next polls or drops its future. No hidden
  detached task is created by this crate.
- The plugin closes streams in `CleanupPhase::Entrances`, before host work drain,
  so waiting receivers cannot keep shutdown stuck. Retained service handles reject
  connect/subscribe/publish afterward. Hosts own WorkGate admission at their outer
  business/transport boundary; the standalone service does not reenter that gate.

## Explicit first-wave boundary

This is an ephemeral memory stream. There is **no snapshot API, cursor, watermark,
reconnect replay, durable delivery, SSE response, heartbeat, Redis provider, or
cross-process transport**. `Ready` only means subscription authorization completed;
it does not certify an initial snapshot. Updates published during authorization
are not delivered. Obtain application state using the host's own consistency
protocol, and resynchronize after every overflow/reconnect. This crate does not
claim to make a separately read snapshot race-free.

See [PARITY.md](PARITY.md) for the pinned TS reference and validation matrix.
