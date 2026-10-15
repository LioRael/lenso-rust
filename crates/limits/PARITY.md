# TypeScript parity and scope

Rust base: `LioRael/lenso-rust` main
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

TypeScript reference: `LioRael/lenso` main
`c6d2820513878a8b24d40c015da613b9918b5d6b`, from the supplied read-only
`/tmp/lenso-plugin-inventory-20261015/ts/packages/limits/` snapshot. The shared
original checkout is older and was not used as the semantic baseline.

| Behavior | Rust status / evidence |
| --- | --- |
| Epoch-aligned fixed windows, separate rate/quota | Implemented; `tests/memory.rs` fixed-window and isolation regressions |
| Weighted consumption; denial does not debit usage | Implemented; remaining/retry/too-large assertions |
| `(instance, tenant, key)` structured keys and separate stores | Implemented; separator ambiguity, namespace and store-instance regressions |
| Positive integer counts and millisecond durations | Implemented; Rust unsigned types reject negative/fractional counts structurally; explicit zero, max and duration precision validation |
| Scope length 1–512 JS string units | Implemented with UTF-16 length; empty, astral Unicode and limit regressions |
| Fixed capacity/period per counter; fixed lease capacity | Implemented, including conflicts after expiry and after rejected first admission |
| Per-scope backward clock clamp; safe timestamp arithmetic | Implemented; exact boundary, regression and maximum timestamp tests |
| Weighted lease acquire and quantity-aware retry | Implemented; retry waits for sufficient cumulative weight, not the first expiry |
| Renew retains token/weight, never shortens, cannot revive expiry | Implemented; stale snapshot, backward clock and exact expiry regressions |
| Idempotent token-specific release | Implemented; expired owner cannot release a replacement |
| Fresh UUID per service acquisition | Adapted: opaque process-unique `LeaseToken`; fresh tokens checked for exhaustion, no distributed/cryptographic promise |
| Snapshot ownership and wrong-scope rejection | Implemented with owned Rust values; mutations cannot change store state; owned token with altered scope rejected |
| Explicit throw/deny/allow policy | Implemented as `Error`/`Deny`/`Allow`; only `BackendFailure` admission errors are converted |
| Fail-open does not fabricate lease/remaining/retry | Implemented; `tests/service.rs` fault injection checks all three policies |
| Renew/release failure remains visible | Implemented; failed explicit release retains ownership for close retry |
| Close seals admission, waits for entered calls, releases own leases only | Implemented synchronously, with an immediate atomic seal and serialized store calls; actual thread race regression |
| Repeated/concurrent close shares aggregate result | Implemented for final close; all final releases are attempted once and waiters see the cached result. `ActiveRuns` is transitional and not cached; async drain finishes close |
| Borrowed store stays usable after close | Implemented; independent owners and direct-provider checks |
| Exact plugin dependency and rollback cleanup | Implemented via existing `Handle<S>`/`Resources`; foreign handle, startup rollback, shutdown ordering and WorkGate drain regressions |
| No hidden renewal for manual leases | Implemented: no timers, tasks or implicit renewal |
| `withLease`, cooperative abort, automatic renewal ownership | Implemented as `Arc<Limits>::with_lease`, explicit `LeaseCancellation` and opt-in `renew_every`; owned runner drains and releases even when the caller future is dropped |
| Business and cleanup errors both survive | Implemented; `LeaseRunError` independently retains lifecycle, business and release failures; callback construction/poll panics release too |
| Wrapper expiry / late renewal continuity | Strengthened: local deadline stops expired work even without renewal; a renewal crossing the old deadline cannot claim continuity |
| Provider liveness at successful completion | Strengthened: `LimitStore::observe` checks current liveness without renewing, including injected clock jumps; unavailable observation never becomes fail-open protection |
| Async wrapper shutdown and abandoned close waiters | Implemented: Entrances signals cancellation before WorkGate drain, Resources awaits `close_async`; sync close returns transitional `ActiveRuns`; owned close task survives waiter cancellation |
| Async external store/provider operations | Deferred. The current small `LimitStore` trait is synchronous and for bounded process-local work only |
| Optional backend warning logger / raw cause retention | Deferred. Typed sanitized failures and visibly degraded decisions are returned; no implicit logging or log plugin dependency |
| TS Config sources/schema | Adapted: required typed `FailurePolicy` constructor argument; no new config/DI container |
| SQLite, Postgres, D1, migrations, Auth integration | Out of scope for this memory-first crate |

The Rust regressions are ports of the relevant observable cases in TS
`test/memory.test.ts`, `test/lifecycle.test.ts` and `test/integration.test.ts`.
No claim is made that the complete TS test suite or its DB/Auth integrations run
against this synchronous Rust interface.

## Deliberate limits

The store retains policies and clock history per scope. The service retains
lease ownership records until release, lost renewal or close, including expired
leases. Callers must bound scope cardinality and explicitly release completed
leases. This matches TS memory behavior; eviction would discard policy/history.

Each service serializes store calls with close. The memory backend uses one mutex
for its state, so operations are atomic across distinct services sharing it. This
is an initial correctness-oriented in-process backend, not a high-throughput
sharded or distributed limiter. Providers and clocks must not reenter the service
or perform blocking external I/O. Panics poison locks and are not translated into
fail-open admission. Final close results, including release errors, are cached and
not retried by repeated close. `ActiveRuns` only reports pending wrapper drain;
it is not cached. `close_async` drains wrappers and completes final close.

## Root integration diff (for parent task integration)

- `Cargo.toml`: one additional workspace member, `"crates/limits"`.
- `Cargo.lock`: one new `lenso-limits` package entry referencing the already
  resolved `lenso-plugin-core` and `tokio` packages. The async follow-up promotes
  Tokio from dev-only to a normal dependency without changing that lock entry.
- No root workspace dependency alias, dependency version changes or changes to
  Core, Runtime, Log, Cache, other plugin directories or shared original checkouts.

All remaining implementation, examples, regression tests and documentation are
contained in `crates/limits/`.
