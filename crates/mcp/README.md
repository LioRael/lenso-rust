# Native MCP tools

`lenso-mcp` projects an explicit, immutable selection of ordinary native service
callbacks into serializable MCP `ToolPage` / `CallToolResult` values. It borrows
trusted host identity and never starts listeners, loads credentials or owns the
application. This is an adapter for a host's MCP implementation, not a complete
MCP server or a replacement for SDK negotiation/framing.

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

Dynamic tool registration, list-change notifications, HTTP/stdio framing,
initialize/negotiation, tasks, prompts, resources, progress, durable cancellation,
and Engine conversion are deliberately outside this native projection. Selection
changes require a new adapter, as with the current TS borrowed selection. For
exact-instance native injection, construct an adapter from a plugin's `setup`,
resolve dependencies through `Setup::get`, pass `setup.work_gate()`, then register
`adapter.stop()` immediately in Entrances. `McpPlugin` is the simpler convenience
for callbacks borrowing already host-owned services.
