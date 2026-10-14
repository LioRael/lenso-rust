# First native plugins: shared behavior, not shared internals

This candidate adds only structured Log and process-local bounded Cache.
`lenso-log-plugins` remains the existing log reader/aggregator, not a logger.
Core owns instance identity, dependency preflight, startup, work admission and
cleanup. Each plugin owns its typed configuration and resources. Runtime owns
transport, contracts, generation, readiness, promotion and remote drain.
No linked/portable modes, global service registry or automatic RPC are added.

## Small shared behavior contract

| Case | Required invariant |
| --- | --- |
| Dependency identity | A handle/object refers to the exact installed instance, not another instance of the same type/name. |
| Invalid graph/config | No plugin resource setup begins. Errors attribute the instance; configuration errors omit rejected values. |
| Partial setup failure | Already registered owned resources, including the failing instance's resources, are attempted once in rollback. |
| Accepted work during shutdown | Stop new entry work, retain dependencies until the work actually settles, then release resources. Cancellation response alone is not settlement. |
| Early disposal | Explicit disposal and later shutdown join one completion; an earlier failure remains visible to shutdown/rollback. Early disposal is owner-directed, not permission to release a resource still used by dependents. |
| Repeated/concurrent stop | Callers share one completion and failure result; one cancelled waiter does not cancel cleanup. |
| Cleanup failure | Attempt subsequent finalizers and retain original startup plus cleanup failures. |
| Borrowed resources | Never close a host-owned sink/client/backend merely because an App stops. |

TS Core keeps global LIFO cleanup. Rust keeps global Entrances and Resources
phases, with accepted work draining between them. They are not identical APIs.
TS Core does not automatically instrument arbitrary business methods; existing
Runtime/Engine entry owners drain real invocations. Rust's `WorkGate` makes the
native application admission rule explicit.

### Retained services and detached work

Rust business entry methods must call `WorkGate::enter` and retain `WorkGuard`
until the operation settles. `RunningApp::get` refusing access does not revoke
an `Arc` already handed out. The batch service checks the gate on every ordinary
call, including calls on a retained service. A detached cache loader shares an
`Arc<WorkGuard>` with its caller, so cancellation of a waiter cannot release
dependencies while the loader still uses them.

Log and Cache are dependency APIs, not implicit app entry adapters. Already
accepted work may keep using them while the app drains; they seal their local
APIs during owned resource cleanup. If a host directly exposes a cache/logger
handle as a new app entry, that host must acquire the app gate too. Arbitrary
third-party service methods cannot be automatically intercepted by Rust Core.

`App::start` owns setup independently of its delivery waiter. A cancelled waiter
does not abandon partial setup: setup finishes or fails, then an undelivered App
is shut down. This is not a timeout for uncooperative setup. Unwinding validation,
dependency and setup panics are handled; `panic=abort`, process termination and
stopping Tokio cannot be recovered. The host's panic hook remains host policy.
No asynchronous Drop guarantee is made. A guard leaked forever stalls safe
drain rather than allowing dependency destruction under active work.

## Configuration

Use ordinary typed options, or Core's small `Config<T>` helper with serde.
Only explicitly named env bindings are read; values use JSON encoding.
Precedence is present env > explicit fields > serde defaults. Missing env
bindings leave the explicit/default value intact; invalid present env fails,
not falls back. Diagnostics retain source IDs and field paths, not raw values
or raw deserializer error strings. Sensitive source metadata can be masked.
Resource acquisition still belongs in `Plugin::setup`, after validation.

The serving batch binds `LENSO_BATCH_CACHE_TTL_MS` to `cache_ttl_ms`, default
60000 milliseconds, valid range 0..=86400000. Zero disables storage; it does
not turn a missing value into a permanent entry. There is no config center,
file watcher or generic schema DSL.

## Log subset

`lenso-log` uses tracing dispatch/subscribers without installing a process-global
subscriber per App. It supports structured fields and levels, plugin/instance
bindings, explicit async scope and optional valid trace/span identifiers.
Error records contain only approved public labels, never raw error text/stack.
Known sensitive structured fields are redacted; arbitrary message text is not
a secret detector. A borrowed dispatch/sink stays with its host. Owned flush
is explicit, timed and reports safe failure. No OTel exporter is required.
See `crates/log/README.md` for standalone and injected usage.

This is not a Pino API port. TS custom serializers and borrowed logger policy
remain TS owner extension points; Rust's logger maintains its own safe boundary.

## Cache subset and differences

`lenso-cache` supports typed JSON-roundtrippable values in bounded local memory,
finite TTL, ordered reads, exact-key deletion, exact-scope invalidation, scoped
namespaces and same-key shared loads. `Option<V>` represents a miss; a stored
`V = Option<T>` containing `None` is a legitimate hit. Serialized value and total
byte budgets accompany entry and in-flight loader limits. Expiry is `now >=
expires_at`. Namespace/key fencing prevents old loads from overwriting newer
writes/invalidation; cancelled waiters do not cancel other waiters.

Explicit differences: Rust uses `Duration` and a monotonic clock rather than JS
integer milliseconds/wall time; storage uses FIFO rather than TS memory's LRU.
Rust loader errors are safe cache errors rather than arbitrary thrown values.
Rust misses do not carry TS's absent/expired/corrupt/backend reason tags; batch
reads return one `Result`, not TS per-key backend error statuses.
Only the owned local memory backend is offered: cloning shares that family's
lifetime, and is not a borrowed backend ownership transfer. Generic borrowed
adapter plugins, Redis, fail-open remote driver policy, distributed generation
tokens and persistence are not claimed. Ordinary Rust applications need no
Runtime to use it. See `crates/cache/README.md` for the exact API and bounds.

The cache does not store authentication/authorization decisions. Scopes and
Runtime service identifiers are not permission boundaries.

## One real batch, two ways to call it

`examples/batch/src/native.rs` is the ordinary Rust business service. It injects
the existing native aggregator, Log and Cache. Only generated `aggregate` and
`progress` exports go through Runtime. No logger write or cache read is an RPC.
The existing JSON `logs` contract and TS generated client remain the boundary;
input/output/business errors are validated there.

```sh
# Pure native batch call, no listener or Runtime process:
printf '%s\n' '{"logs":[{"level":"error","message":"synthetic","duration_ms":12}]}' \
  | target/debug/logs-batch --native

# Standalone TS App, same local cache/aggregation path:
bun run --cwd examples/runtime offline
```

The Rust serving App completes all native setup before activating exports.
Runtime cancellation is cooperative, not transaction rollback. The batch caller
cancels its own cache waiter; shared work belongs to the cache and accepted
native lifetime. Progress uses the existing bounded stream/cancellation path.
Ready/promotion/drain stay with the existing SDK and TS development workspace.

## Candidate and verification ownership

Formal released/pinned dependency paths remain unchanged. Explicit local
candidate preparation/building selects source hashes rather than claiming the
old immutable pin. Runtime binaries, SDK, canonical contract and generator must
match the selected source candidate. Machine-specific paths belong only in local
ignored build/provenance artifacts, never formal package dependencies.

The existing smoke, `verify:lenso` and performance entry are extended/reused,
not replaced with another supervisor. Performance separates native TS, native
Rust, Runtime→TS and Runtime→Rust, with the same bounded input. TTL zero keeps
aggregation work present on all measured paths. Cold/warm measurements, native
versus transported timing boundaries, serialization bytes, concurrency and
errors are reported explicitly; builds are not included in request latency.
These measurements do not promise Rust wins.

## Public compatibility changes

- `Setup::on_cleanup` now returns `CleanupHandle` rather than `()`. Existing
  registration statements still work; functions returning its old unit result
  as their tail expression must discard the new handle explicitly.
- `AccessError::Closing` is a new enum variant; exhaustive downstream matches
  must handle it. `RunningApp::get` now rejects once shutdown starts.
- `App::start` keeps its signature but owns startup independently of cancellation.
  Unknown validation error text is intentionally omitted from public diagnostics.
- The example's TS `batch.aggregate` now returns a Promise. Its callers have been
  updated to await it; the generated Runtime JSON contract is unchanged.

Native typed calls do not automatically acquire transport deadlines or reproduce
the whole JSON wire validator. The `--native` demonstration bounds stdin bytes
and deserializes generated input types; generated Runtime boundaries additionally
enforce schema constraints. Native callers own their cancellation/timeout future.
The shared native service still applies domain errors and app work admission.

See [the executed verification record](VERIFICATION.md) for candidate fingerprints,
commands, performance timing boundaries and unexecuted checks. Native CI covers
locked tests, formatting and Clippy. Its manual cross-repository job requires
explicit companion refs, then runs the existing smoke and development verification.
No automatic sibling discovery or hosted publication is added.

No main merge, publication, deployment, TLS/HA/Wasm platform or trusted
cross-language user identity is part of this change. A TS actor in JSON is still
untrusted input at the Rust resource owner.
