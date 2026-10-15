# Audit verification

Executed locally on 2026-10-15 on macOS arm64, using Rust/Cargo 1.99.0.
Commands ran in the isolated `audit-rust` checkout based on
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

The shell's mise shim was unavailable in the sandbox. Checks used the existing
toolchain directly, without installation or environment/configuration changes:

```sh
PATH=$HOME/.rustup/toolchains/1.99.0-aarch64-apple-darwin/bin:$PATH
```

| Command | Result |
| --- | --- |
| `cargo test -p lenso-audit --locked --offline` | Passed: 14 integration tests + 2 documentation tests |
| `cargo test -p lenso-audit --no-default-features --locked --offline` | Passed: 11 integration tests + 2 documentation tests; 3 plugin lifecycle tests excluded by feature |
| `cargo clippy -p lenso-audit --all-targets --locked --offline -- -D warnings` | Passed |
| `cargo clippy -p lenso-audit --all-targets --no-default-features --locked --offline -- -D warnings` | Passed |
| `cargo fmt --all --check` | Passed |
| `git diff --check` | Passed |
| `cargo run -p lenso-audit --example audit-native --no-default-features --locked --offline` | Passed; `1 scoped event; retry returned Duplicate` |
| `cargo run -p lenso-audit --example audit-plugin --locked --offline` | Passed; `Exact dependency wiring and retained-service shutdown verified` |
| `cargo tree -p lenso-audit --no-default-features --edges normal --locked --offline` | Only `lenso-audit`; no normal dependencies |

No package versions or existing dependency resolutions changed. The full
workspace test suite was not rerun: implementation is isolated to Audit and
uses existing plugin-core through its public API. The workspace formatting
check includes all crates. No network/provider credentials were required.

## Regression evidence

- `tests/service.rs`: authority revalidation before storage, denial/nonexistent-ID
  equivalence, opaque issuer/subject snapshots, input/whitelist bounds, recorded
  time ignored for idempotency, conflicts, immutable correction links, invalid
  outcomes, best-effort error classification/sanitization, lost responses,
  reporter failures, strict attestation, instance-bound receipts, revocation
  after intent, outcome retry, 32 concurrent prepare calls with exactly one Ready.
- `tests/repository.rs`: 32 simultaneous inserts produce exactly one Inserted,
  15 Duplicates and 16 Conflicts; full-capacity rejection preserves old events;
  structural tenant/scope isolation including `None` vs `"null"` and delimiter
  collisions; cursor ties, inclusive time bounds, positive/negative filters,
  defensive rejection of cross-scope/wrong-ID/misclassified duplicate results.
- `tests/cancellation.rs`: cancels prepare after the fixture committed but before
  acknowledgement; retry returns AlreadyRecorded and never a new Ready receipt.
- `tests/lifecycle.rs`: exact-handle lookalikes and invalid config fail before
  resource acquisition; accepted work completes after the app gate seals;
  rollback closes retained service; owner resources close once; separate
  instances stay isolated; retained complete after shutdown touches neither
  storage nor diagnostics.

## Independent review

A separate GPT-6 Astra reviewer performed read-only source/TS comparison and
ran the Audit tests and clippy. The first review found one P2 lifecycle issue:
`complete` could report through a provider after `Closed`. The implementation
now returns `Closed` immediately without diagnostics, and the dedicated
lifecycle regression checks both provider call counters. The reviewer
rechecked the fix, scope/idempotency/strict fixtures, cancellation ordering,
and negative query filters. Final review: no unresolved findings in scope;
14 integration tests and all-targets clippy passed.

## Limits of this evidence

The in-memory durable-attestation wrappers are **test-only fault injection**.
They verify protocol sequencing and failure handling, not disk/replication
durability or external-effect atomicity. The public memory repository always
reports nondurable. Host-owned work guards are required for resource safety.
Actual Auth, Postgres, SQLite, D1, Tasks, Manage, Log and OTel integrations were
not implemented or exercised. No production/publishing/deployment action,
push, or change to the shared source checkout was performed.
