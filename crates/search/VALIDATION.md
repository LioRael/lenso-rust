# Candidate verification — 2026-10-22

Base: `126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2`.
Runtime SDK revision remains `dd0eb084e490e5e4bda05ce0dbb503f3aae50590`.
The source checkout was clean and was cloned without hard links into task-34.
Only this candidate's Search crate and minimal workspace/lock registration changed.

## Executed

- `git diff --check`: passed.
- System SQLite smoke, using DDL and SELECT extracted directly from
  `src/lib.rs`, on a disposable `/tmp/task34-search-smoke.sqlite` database:
  title match ID 2 sorted before body-only ID 1; Bob's secret did not appear.
  Separate SQLite processes reopened the same database. Updating Alice ID 2
  to Python and deleting Alice ID 1 gave:
  `remaining alice rust|0`, `bob preserved|1`, `update indexed|1`, `revision|5`.
  The SELECT used SQLite `.parameter` bindings for all seven parameters.
  This verifies SQL behavior with the system SQLite, not Rust or bundled SQLite.

## Blocked / unverified

First attempted compilation:
`cargo test --offline -p lenso-search --no-run`
was rejected by the local mise launcher with `Operation not permitted (os error 1)`.
No alternate compiler path, escalation, dependency installation, or network fetch
was attempted. This was a local execution failure, not an auto-review rejection.
The initial scripted Python workspace edit hit the same launcher failure; the
registration was subsequently written using the normal file patch tool.

The lockfile addition was written manually using only already locked dependencies;
Cargo has not validated it. Rust compilation, formatting, Clippy, all Rust tests,
and the public example remain unverified. This candidate requires these checks
in an authorized toolchain environment before integration:

```
cargo fmt --all
cargo test --locked -p lenso-search
cargo run --locked -p lenso-search --example notes_search
cargo test --locked -p lenso-plugin-core --test admission
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all --check
```

`tests/search.rs` covers real FTS hits/order, namespace/tenant/owner isolation,
cross-scope/query/page-size and stale cursor rejection, parameterized hostile
identifiers and literal query operators, empty/oversized inputs, pagination,
update/delete, reopen persistence, absent-file startup, and retained-service
shutdown rejection. No actual Rust test result is claimed.

Missing-FTS5 failure is implemented via the SQLite compile-option check but was
not exercised against a build without FTS5. Concurrent shutdown/drain stress,
10,000-hit cap and maximum document/summary sizes are not execution-verified.
No performance benchmark, Chinese segmentation, vector search, source database
synchronization, publication or deployment is claimed.
