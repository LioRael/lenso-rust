# Audit parity and integration boundary

Implementation base: `LioRael/lenso-rust` main
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

Semantic reference: `LioRael/lenso` main
`c6d2820513878a8b24d40c015da613b9918b5d6b`, `packages/audit/src` and its
`service`, `repository`, `integrations` and `consumer` tests. The supplied
read-only snapshot `/tmp/lenso-plugin-inventory-20261015/ts` was used, not the
possibly older local TS checkout. No TS source or other Rust crate was modified.

| TS surface / behavior | Rust status | Evidence / boundary |
| --- | --- | --- |
| Injected `AuditRepository`, `AuditAuthority<P>` | Implemented | Native async traits, associated principal type; no Auth/DI container |
| Standalone business service | Implemented | `--no-default-features` has no library dependencies; standalone example |
| `append`, `get`, `query` | Implemented | `tests/service.rs`, `tests/repository.rs` |
| Scoped idempotency and conflicts | Implemented | Exact structured keys, actor/content comparison, recorded time excluded, atomic memory insertion |
| Scope isolation and reauthorization | Implemented at boundary | Exact-scope authority call before storage; revocation fixture, tenant/delimiter collision tests; actual authentication belongs to injected authority |
| Stable descending cursor, filters, time bounds | Implemented | Tie-break pagination, positive/negative filter cases, inclusive bounds |
| Append-only corrections and outcomes | Implemented | Same-scope link checks, intent/action/target validation, original event preserved |
| Summary whitelist and bounded input | Implemented | Boolean/integer/enum types; forbidden names and policy/input validation |
| `appendBestEffort` | Implemented | Explicit reporter required; only storage failure becomes Unconfirmed; reporter errors visible |
| Strict `prepare` / `complete` | Implemented protocol | Attestation required, opaque instance receipt, duplicate never Ready, snapshot identity after revocation, unknown-outcome diagnostics |
| Lost acknowledgement / concurrent prepare | Implemented and tested | 32 contenders: one Ready; commit-then-error and commit-then-cancel retries do not readmit effect |
| Exact plugin dependencies / preflight | Implemented | `AuditPlugin` over existing plugin-core handles, optional exact reporter, lookalike rejected before acquisition |
| Shutdown / rollback | Native adaptation | Host work guard through effect; no dependency gate reentry; own service sealed after drain, providers remain owner-managed |
| Memory validation adapter | Added | Explicit event capacity, no eviction, always nondurable; not a TS persistent adapter replacement |
| SQLite persistence | Implemented behind `sqlite` | Versioned append-only table, reopen recovery, concurrent uniqueness; no deployment-durability attestation |
| Postgres / D1 | Not implemented | No provider or deployment-durability claim |
| Auth adapter / audience verification | Not implemented | Application implements `AuditAuthority`; tests use a scoped/revocable fixture |
| OTel counter and Log reporter adapter | Not implemented | Fixed `AuditDiagnostic` / `AuditReporter` seam is provided; no implicit Log/OTel dependency |
| Tasks reconciliation adapter | Not implemented | Application may query intents and append stable linked outcomes; no queue, outbox or effect replay |
| Manage/scoped Manage/CLI/MCP/browser operations | Not implemented | Native service API only; no agent-facing write or global listing surface |
| Notes/Auth/DB/Manage TS consumer | Not ported | Two Rust examples exercise standalone and plugin assembly |
| TS JSON wire compatibility / Zod schemas | Not implemented | Native Rust fields/enums; no JSON decoder/serializer compatibility claim |
| Real crash durability / external effect atomicity | Not implemented | Strict fixtures only validate protocol; no distributed transaction or exactly-once claim |

## Deliberate native differences

- `AuditEvent` contains `input`, `actor`, `recorded_at`; target uses `kind`.
  Summary and policy are owned `BTreeMap`s, removing key-order/prototype hazards.
  Typed structs/enums prohibit unknown event fields, injected actors, NaN,
  fractional times and invalid result/relation discriminants at construction.
- A separate `OutcomeResult` cannot represent Intent. Receipt fields are private;
  a Rust clone preserves the same capability rather than fabricating a JS object.
- Provider reads additionally reject a wrong ID, duplicate content mismatch,
  unsorted/oversized query pages or filter violations. These are fixed
  `StorageFailed` errors; no foreign records are returned.
- Policy validation is part of plugin preflight, including whitelist validation.
  The TS plugin only preflights page size and checks policy during setup.
- Post-shutdown access returns `Closed`; `complete` does not try an already closed
  reporter. Rust futures are cancelable; the service creates no background tasks.
- Actor length limits count UTF-16 code units for TS compatibility. Whitespace
  checks use Rust's Unicode whitespace definition rather than JS object parsing.

## Root files for parent integration

Only these root changes are needed; review them separately when combining the
five plugin branches:

```diff
--- Cargo.toml
+++ Cargo.toml
@@ workspace.members
+    "crates/audit",

--- Cargo.lock
+++ Cargo.lock
@@ before lenso-cache package
+[[package]]
+name = "lenso-audit"
+version = "0.1.0"
+dependencies = [
+ "lenso-plugin-core",
+ "tokio",
+]
+
```

`Cargo.lock` adds one 8-line `lenso-audit` package entry with dependencies
`lenso-plugin-core` and `tokio` (Tokio is dev-only in the Audit crate). Existing
package versions/resolutions are unchanged. There is no new registry or Git
dependency and no workspace-level `lenso-audit` alias. All other changes are
under `crates/audit/`, including examples and this documentation.

SQLite follow-up base: verified upstream main `c31a47b52522302c3365855eaff47d970ac985e7`. Only Audit files and its Cargo.lock dependency list change; existing dependency versions and Core remain unchanged.
