# Candidate verification — 2026-10-22

Base: `126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2`.
Runtime SDK remains `dd0eb084e490e5e4bda05ce0dbb503f3aae50590`.
Initial frozen candidate: `ff2f746f47303d62d7d922069ec3365bb6463039`.
The source checkout was clean and cloned without hard links into task-34.
Only this candidate's Search crate and minimal workspace/lock registration changed.

## Toolchain failure and authorized resolution

Initial `cargo test --offline -p lenso-search --no-run` failed in the mise
launcher with `Operation not permitted (os error 1)`, before Rust compilation.
PATH resolves Cargo to `~/.local/share/mise/command-wrappers/bin/cargo`, a symlink
to `/opt/homebrew/bin/mise`. This was a sandbox execution failure, not an
explicit approval rejection. The exact prohibited syscall/path was not exposed.

After parent clarification, the same command was submitted through the normal
`require_escalated` approval mechanism and permitted. No toolchain path,
environment, cache location or dependency version was changed. Offline Cargo
then compiled and reported E0277: `usize` does not implement SQLite `ToSql`.
The follow-up fixes the bounded pagination parameters to use `i64` and applies
Rust formatting. The frozen candidate is retained in Git history, not amended.

## Executed successfully

- `cargo test --offline --locked -p lenso-search`: 3 integration tests passed.
  Covers actual bundled FTS5 hits/order, namespace/tenant/owner isolation,
  cross-scope/query/page-size and stale cursor rejection, hostile identifiers,
  literal query operators, input limits, pagination, update/delete, reopening,
  absent-file startup and retained-service rejection after shutdown.
- `cargo run --offline --locked -p lenso-search --example notes_search`: passed;
  printed Alice's hit and `Reopened: Alice update/delete and Bob isolation verified`.
  Uses public APIs, App lifecycle and only a temporary application database.
- `cargo fmt --all`: succeeded and changed only Search files.
- Initial system SQLite smoke used DDL and SELECT extracted from `src/lib.rs`:
  title match ID 2 sorted before body-only ID 1; Bob's secret did not appear.
  Separate processes reopened the disposable database; updating/deleting Alice
  yielded `remaining alice rust|0`, `bob preserved|1`, `update indexed|1`.

## Remaining limitations

Missing-FTS5 failure is implemented but was not exercised against a build without
FTS5. Concurrent Search shutdown/drain stress, the 10,000-hit cap and maximum
summary/document sizes are not execution-verified. No performance benchmark,
Chinese segmentation, vector search, automatic source database synchronization,
publication or deployment is claimed.

## Final repository checks

- `cargo test --offline --workspace --locked`: exit 0, including Core admission
  regressions and Search integration tests. Existing same-name example output
  collision warnings were emitted; pre-existing ignored fixtures/docs remained
  ignored. Optional PostgreSQL integration suites were not enabled by this command.
- `cargo clippy --offline --workspace --all-targets --all-features --locked -- -D warnings`:
  exit 0. This compiles optional feature targets, not their external-service tests.
- `cargo fmt --all --check`: exit 0.
- `git diff --check`: passed before the follow-up commit.

All Cargo invocations used the existing mise/Cargo path through normal approval.
No pending or denied approval remains. Dependencies were available offline;
Cargo accepted the locked workspace registration without changing Cargo.lock.
