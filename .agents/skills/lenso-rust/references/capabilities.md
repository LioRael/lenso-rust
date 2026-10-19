# Available source and deliberate gaps

Integrated baseline: `c4c9a0b` in lenso-rust, checked 2026-10-19. Always query installed manifests/exports first; branch-only additions require a compiling consumer. These are current source APIs, not a claim of registry releases. Read the crate's public exports and focused README before selecting a capability.

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

The installed workspace now also includes the following native implementations.
Read each crate README/PARITY and exported types before using it; names alone do
not prove a provider, transport, or operational feature exists.

| Capability | Installed source | Boundary |
| --- | --- | --- |
| Authorization | `crates/authorization` | Trusted application facts, RBAC, existing host access verifier; no authentication issuer |
| Audit | `crates/audit` | Append/query with injected repository and authority; no owned DB client |
| Storage | `crates/storage` | Streaming local objects with lifecycle cleanup; no remote object provider |
| Realtime | `crates/realtime` | Bounded process-local subscriptions with host authorization |
| Events/outbox | `crates/events` | Ephemeral local fan-out plus explicit local durable outbox |
| Limits | `crates/limits` | Rate/quota windows and weighted leases; pluggable native store |
| Tasks | `crates/tasks` | Provisioned SQLite queue, optional PostgreSQL feature; host drives workers |
| Scheduler | `crates/scheduler` | Provisioned SQLite schedules/outbox; host drives tick and Tasks workers |
| MCP | `crates/mcp` | Explicit native operation projection; default stdio server via pinned rmcp |

No generic native long-lived `dev` session, remote `status` or remote `call`
control API is provided. The additive `App::inspect` preflight reports this scope
explicitly and runs no setup or Runtime connection. An external DB or HTTP
library can remain a plain native dependency owned by a plugin; absence of a
Lenso adapter does not justify reimplementing a framework.
