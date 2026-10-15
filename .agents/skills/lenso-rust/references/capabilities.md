# Available source and deliberate gaps

Baseline: `e5dfafb` in lenso-rust, checked 2026-10-15. These are current source APIs, not a claim of registry releases. Read the crate's public exports and focused README before selecting a capability.

| Task | Existing source/example | Boundary |
| --- | --- | --- |
| Native DI/lifecycle/config | `crates/plugin-core` | No HTTP/CLI host, authentication, persistence or kernel required |
| Structured logging | `crates/log`, example `injected` | `LoggerPlugin`, `Logger`, `LoggerOptions`; structured stderr; borrowed tracing dispatch remains host-owned |
| Bounded in-memory cache | `crates/cache`, example `cache-native` | `CachePlugin<V>`, `MemoryCache<V>`, `CacheConfig`; process-local JSON storage; not a durable database |
| Two independently configured sources | `crates/log-plugins/examples/offline.rs` | JSON-lines/memory source examples, not a generic Storage implementation |
| Explicit Runtime exports | `crates/plugin-runtime`, `examples/batch` | Optional adapter over the real pinned SDK |

```sh
cargo run --locked -p lenso-log --example injected
cargo run --locked -p lenso-cache --example cache-native
```

Log writes and Cache dependency calls do not reacquire global admission. Their outer business owner must hold its work guard. Error redaction is structured-field/classification based; arbitrary free text and original error causes can still be private.

Cache uses bounded entries, serialized bytes and loader tasks; namespaces isolate keys, not permissions. `Option<V>` distinguishes miss from cached null. TTL is `Duration`, not JS milliseconds. Zero TTL skips storage/deletes old entries; concurrent loading, fencing and cancellation have explicit semantics in `crates/cache/README.md`. Independent constructors remain isolated. There is no Redis/DB/Runtime cache adapter or TS envelope interoperability in this baseline.

Limits, Authorization, Audit, Storage and Realtime are independent implementation tasks and **not present in this baseline**. Do not import proposed crates, manufacture parity with TS, or present branch examples as current APIs. Before using a later delivery, verify its integrated commit, exported types, manifest/lock provenance and executable example. An external DB or HTTP library can remain a plain native dependency owned by a plugin; absence of a Lenso adapter does not justify reimplementing a framework.
