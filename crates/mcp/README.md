# Native MCP tools

`lenso-mcp` projects an explicit selection of ordinary native service
callbacks into serializable MCP `ToolPage` / `CallToolResult` values. It borrows
trusted host identity and never starts listeners, loads credentials or owns the
application. The default `stdio` feature adds a real server using official `rmcp` 3.5.0
(exactly pinned, Rust 1.88+). The SDK owns initialize negotiation, messages
and cancellation. `--no-default-features` retains the native projection alone.

See `examples/mock.rs` for a local JSON protocol projection with no network.

Three delivered boundaries:

* Stable names match TS `operationToolName`: SHA-256 of the exact JSON
  `[pluginId, method]` tuple, encoded base64url without padding. Reordering or
  filtering the allowlist preserves names. Duplicate tuples reject construction.
  `list_tools` rechecks visibility every page; cursors expire, are single-use,
  capped, and bound to the same trusted identity **Arc reference** and visible
  tool sequence. Restart after policy changes. Cursor counters are opaque adapter
  state, not bearer credentials. Returned metadata is independently owned.
* Successful results include JSON text and `structuredContent`; scalars, arrays
  and null use `{ "result": value }`. Both encodings obey the output byte budget.
  Tool failures contain fixed safe code/phase/message diagnostics; unknown tool
  and stale cursor use protocol errors (`code()`). Host must never put secrets
  in metadata or successful values: this crate does not redact arbitrary JSON.
* Calls and discovery share a bounded no-queue admission slot and Core WorkGuard.
  Cancellation, deadline and close signal the callback and return a finite result;
  work runs independently until settlement, retaining the slot/guard even after
  response futures are dropped. `close` waits for actual work; `stop` only seals
  and signals. The plugin registers `stop` in Entrances, then Core drains guards
  before resources are released. Hanging/noncooperative work can prolong drain.
  No rollback, retry or safe cancellation of external effects is promised.

`Policies::new(can_list, authorize)` requires both trusted callbacks and checks
both before invocation. `RequestContext.identity` must come from the host's
existing verified identity interface, not business input. Owning services still
apply current object/tenant/permission rules. This crate supplies no OAuth,
credentials, authentication cache or authorization engine.

`Operation::new` requires an object input schema and explicit handler. The host's
shared business validator must validate the input's schema constraints before
calling its service and return `ToolError::InvalidInput` on rejection. Only the
outer JSON object shape and serialized byte size are checked here; there is no
Rust Engine/schema interpreter. Output is `serde_json::Value` (finite JSON), not
arbitrary objects. Error constructors accept classifications, never private text.
Panic unwind becomes a safe tool failure; the host panic hook remains host policy.

Default budgets match the TS borrowed adapter: input/catalog 256 KiB, output
1 MiB, concurrency 4, deadline 30 seconds, page size 100, cursor TTL 5 minutes,
1024 cursors. Budgets cover serialized values, not allocations inside trusted
callbacks. The host must bound incoming frames before parsing and total outgoing
frames including envelopes. Tokio must stay alive through drain.

HTTP, tasks, prompts, resources, progress, durable cancellation and Engine
conversion remain outside this plugin. For
exact-instance native injection, construct an adapter from a plugin's `setup`,
resolve dependencies through `Setup::get`, pass `setup.work_gate()`, then register
`adapter.stop()` immediately in Entrances. `McpPlugin` is the simpler convenience
for callbacks borrowing already host-owned services.

## Real stdio entry

```sh
cargo build -p lenso-mcp --bin lenso-mcp-demo --locked
# Configure an MCP client to launch the resulting absolute executable path:
# /absolute/checkout/target/debug/lenso-mcp-demo
```

The demo is an actual standalone stdio server exposing synthetic echo/wait tools;
it performs no login, credential creation, network request or model invocation.
For application use, inject your own adapter and trusted local identity:

```rust,ignore
let server = lenso_mcp::stdio::McpServer::new(adapter.clone(), trusted_identity);
server.run_stdio(shutdown_token).await?;
```

Initialize negotiates the SDK-supported legacy revisions 2025-11-25,
2025-03-26 or 2024-11-05. Only tools/list_changed capability is advertised.
Incoming frames use the SDK codec with a fixed 1 MiB limit; invalid frames close
the entry. Initialization times out after 30 seconds. EOF, Ctrl-C (demo), explicit
shutdown or runner drop seals adapter admission and signals active work. Normal
runner completion drains actual work; dropping a runner cannot await cleanup, so
its application owner must still await app shutdown. Keep Tokio alive through drain.
The SDK may suppress responses to cancelled requests; cancellation completion is
not evidence of rollback. The native adapter still retains accepted work guards.

Each McpServer owns one session and one trusted identity. Use a fresh server per
entry. All application logs must use stderr; stdout belongs to SDK frames.
The bridge does not intercept process-global logging or sandbox trusted callbacks.

`McpServer::replace_operations(new_selection).await` is a trusted host lifecycle
operation, never an MCP tool. Resolve the exact initialized service instances in
host setup before publishing callbacks. Replacement validates the whole catalog,
invalidates all prior cursors (including same-name schema changes), and sends the
SDK list-change notification after initialization. Already running callbacks keep
their original closure and WorkGuard until they settle; replacement does not free
borrowed services. If notification delivery fails, the valid new catalog remains
installed and the host should restart that session. The lower-level adapter method
performs atomic replacement without notifications for hosts owning their own entry.

Acceptance: `cargo test -p lenso-mcp --test stdio --locked` launches the actual
binary through pipes with the official SDK client. A second SDK session exercises
host-driven replacement and cooperative cancellation/drain over local duplex pipes.
No protocol parsing or dispatch code is duplicated in the plugin.
