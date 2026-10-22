# Cache completion evidence

Base: `c31a47b52522302c3365855eaff47d970ac985e7`.
Branch: `feat/cache-consumer`, isolated checkout in task-43.

The original supplied task-8 checkout was clean at `126b778`, and did not have
this base locally. A local task-40 clone contained the requested commit; the
independent checkout selects that exact commit, not task-40's branch tip.
Existing Cache implementation was unchanged between those two bases.

## Changes

Reuse the existing bounded process-local CachePlugin/MemoryCache. Correct zero
TTL set so deletion and loader fencing do not depend on serializing a discarded
value. Add an oversized-value regression for both a stored hit and a pending
loader; its old result may reach an existing waiter but cannot repopulate cache.
Strengthen the existing loader-error test with miss/retry/success assertions.

Replace the standalone toy example with an exact-handle Core business consumer
that parses a real local fixture. Two concurrent requests plus a warm request
perform one file read; explicit invalidation makes the fourth request perform a
second read. All return count 3 / total 16. The loader owns a clone of the
accepted work guard, and Core cleanup seals retained service/cache access.
Only the Cache crate is changed; Tokio filesystem support is dev-only.

## Executed successfully

- `cargo test -p lenso-cache --all-targets --locked --offline`: 21 tests passed,
  including the executable business consumer test.
- `cargo run -p lenso-cache --example cache-native --locked --offline`:
  `4 business requests, 2 actual file reads; cold/coalesced + warm + invalidated; total=16`.
- `cargo test --workspace --locked --offline`: passed after normal escalation
  for local Unix sockets. The initial sandbox run failed six Runtime lifecycle
  tests at socket binding with `Operation not permitted`; no bypass was used.
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings`: passed.
- `cargo fmt --all --check` and `git diff --check`: passed.

Used installed Rust executables via `/Users/leosouthey/.cargo/bin` because the
mise command shim could not initialize within the sandbox. No dependency or
Runtime pin update, downloads, push, main mutation, deployment or publication.

## Limits / unverified

No real Redis, external database or remote cache was used. Existing ignored
PostgreSQL integration fixtures remain ignored. No distributed consistency,
performance benchmark, OS cold-disk state, or end-to-end latency claim is made.
The read counts measure actual filesystem API invocations (OS caching may apply),
not build/test time. Existing tests cover TTL boundaries, namespaces, finite
entries/bytes/tasks, coalescing, invalidation, cancellation, panic/error behavior
and owned cleanup; runtime termination and non-cooperative loaders remain outside
the cleanup guarantee described in README.
