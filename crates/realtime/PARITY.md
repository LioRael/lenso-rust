# Realtime first-wave parity

Rust base: `LioRael/lenso-rust` commit
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

TS reference supplied for this task: `LioRael/lenso` commit
`c6d2820513878a8b24d40c015da613b9918b5d6b`,
[`packages/realtime/src`](https://github.com/LioRael/lenso/tree/c6d2820513878a8b24d40c015da613b9918b5d6b/packages/realtime/src)
and [`test`](https://github.com/LioRael/lenso/tree/c6d2820513878a8b24d40c015da613b9918b5d6b/packages/realtime/test).
The nine source/test files used were checked byte-for-byte against the supplied
`ts.tar.gz` archive rooted at `LioRael-lenso-c6d2820`; the older local TS repository
does not contain that commit. No older local main files were used as substitutes.

| TS behavior | Rust status and deliberate differences | Evidence |
| --- | --- | --- |
| Exact resource topics and scope isolation (`wire.ts`) | Implemented: typed tuple equality, identical percent encoding, 256 UTF-8 byte part limit; no wildcard routing | `topics_are_exact_utf8_encoded_tuples_and_parts_are_bounded`, `authorization_scope_and_instance_isolation_with_exact_topic_fanout` |
| `connect`, `subscribe`, `publish`, fan-out | Implemented in one isolated `MemoryRealtime<P>`; callbacks run only for subscribe/renew; publish reports enqueues rather than a cursor | fan-out test; `concurrent_publishers_have_identical_complete_order_for_every_subscriber` (8 publishers × 100 updates × 2 subscribers) |
| Updates during initial authorization | Skipped, as in TS; `Ready` explicitly has no snapshot/cursor claim | `initialization_has_no_replay_and_revocation_fences_a_pending_renewal` |
| Lease authorization, identity/session expiry | Implemented; grant clamped to start-time lease and identity expiry. Uses Tokio monotonic deadlines rather than wall-clock milliseconds | `lease_expiry_purges_buffered_updates_and_does_not_reauthorize_per_event`, `renewal_extends_lease_but_never_session_expiry`, timeout test |
| Explicit renewal / denial | Implemented. Old lease must remain valid until renewal completes. Rejected/cancelled admitted renewal purges data and removes its subscription | denied/cancelled renewal and old-lease boundary tests |
| Revoke resource, delete resource, revoke scoped subject | Implemented, including pending grants. These invalidate current leases, not future host policy decisions | resource/subject purge tests; pending authorization/renewal cancellation tests |
| Unsubscribe and disconnect | Implemented plus Rust RAII: dropping `Subscription` unsubscribes, dropping `Connection` disconnects | queue purge and connection drop tests |
| Count and byte buffer limits | Implemented with exact serialized envelope sizes; one reserved constant-size terminal slot outside the queue. Overflow clears all queued data, emits `Gap(Overflow)`, closes connection | event overflow and byte budget tests |
| JSON validation and payload bounds | Implemented: `serde_json::Value`, byte limit, depth 32/node 10,000 budget including provider wrapper. JS accessors, prototypes, sparse arrays, cycles and undefined are unrepresentable; Rust JSON integer representation is preserved | `payload_byte_depth_and_node_limits_apply_once_before_fanout` |
| Connection, subject, subscription, topic and pending-operation limits | Implemented; slots reserved before callback; pending subscriptions count toward every applicable limit | admission, subject-scope and global pending tests |
| Bounded operations after timeout | Rust cancellation drops the owned callback future and releases its slot; unlike JS promises, no abandoned operation is kept running by this crate. Host-spawned detached work is outside ownership | pending drop/timeout tests |
| Periodic lease sweep | No background task. Reads, delivery, stats and admission prune expired state; a waiting receiver wakes at its nearest expiry. Idle physical cleanup is lazy, authorization enforcement is not | waiting-receive deadline and expiry churn tests |
| Memory provider isolation | Implemented via independent constructors and shared clones. A separate pluggable provider interface is not introduced in this wave | instance isolation test |
| Shutdown and plugin setup rollback | Implemented: synchronous idempotent close signals in-flight callbacks and purges streams. Thin plugin registers `Entrances` cleanup before work drain | four lifecycle tests |
| Exact policy service dependency | Implemented with `RealtimePlugin::with_dependency(Handle<D>, callback)`; no Auth dependency or new permission/DI container | same-type allow/deny instances lifecycle test; `examples/injected.rs` |
| Snapshot consistency / before-after cursor check | **Not implemented**. No snapshot API or consistency guarantee | explicit README boundary |
| Cursor tokens, sequence/generation, deduplication, out-of-order detection, cursor expiry | **Not implemented**. No cursor is exposed; no history or watermark map is maintained | explicit README boundary |
| Reconnect replay / reliability | **Not implemented**. All delivery is ephemeral. TS itself does not replay on reconnect; this Rust wave also omits TS reconnect gap/cursor protocol | explicit README boundary; initialization and overflow tests |
| SSE, retry, heartbeat, Fetch/oRPC/Workers transports | **Not implemented**. Consume `recv(&mut self)` from a host-owned transport | explicit README boundary |
| Redis provider and provider failure gaps | **Not implemented**. No Redis dependency or cross-process publication | memory-only API |
| TS config schema binding and diagnostics callback | **Not implemented**. Typed `RealtimeConfig::validate`, fixed Rust errors and explicit publish reports are provided | config regression; compile-time typed fields |

## API and lifetime choices

- `Identity<P>` is a trusted host input, not an authentication operation. Both
  publishing and revocation are trusted business capabilities on the service.
- The consumer cannot mutate provider state through an event payload: queued
  `Value`s are shared through `Arc`, and Rust ownership prevents shared mutation.
- Subscription IDs are opaque, process-local integers, not TS UUIDs. Handles carry
  their actual service instance and cannot be fabricated from an ID.
- A callback returns `Result<Option<Lease>, RealtimeError>`; `None` denies, and
  callback errors/unwind panics become `Denied`. The host owns panic-hook policy.
- `close()` does not poll a caller-owned future. It signals cancellation; that
  future is dropped on its next poll/drop. No detached task is spawned internally.
  A transport should hold plugin-core's WorkGuard at its outer entry so work drain
  waits for its pending operation to unwind before dependency resource cleanup.
- Once revoked or expired state is removed, subsequent handle operations report
  `Closed`; an operation whose own authorization deadline expires reports `Expired`.
- One service mutex establishes publication order and serializes revocation with
  reads. Public callbacks and principal destructors run outside that mutex.

## Integration diff outside this crate

Only these root edits are required for parent-task integration:

1. `Cargo.toml`: add `"crates/realtime"` to workspace members (one line).
2. `Cargo.lock`: add the `lenso-realtime 0.1.0` package with existing dependencies
   `futures-util`, `lenso-plugin-core`, `serde`, `serde_json`, `thiserror`, `tokio`
   (12 lines). No dependency versions or Git revisions change.

All service code, examples, tests and documentation live in `crates/realtime/`.
No Log, Cache, Core, Runtime, shared original repository or sibling task was edited.
