# Verification — 2026-10-22

Base: `c31a47b52522302c3365855eaff47d970ac985e7`.
Fetched that exact object from the local task-37 checkout; no later Audit changes
were imported. Independent checkout and branch: task-44/lenso-rust,
`feat/usage-quota`. No source checkout edits, push, merge, publish or deployment.

Commands ran offline, with the installed Rust/Cargo 1.94.0 toolchain directory
prepended to PATH and CARGO_TARGET_DIR set to task-44/target. No dependencies were
upgraded; Cargo.lock adds only this workspace package. Runtime SDK remains pinned
to the existing workspace revision `dd0eb084e490e5e4bda05ce0dbb503f3aae50590`.

- `cargo test --offline --locked -p lenso-usage-quota`: PASS, 1 unit test and
  9 integration tests. The subprocess recovery helper exits without destructors.
- `cargo run --offline --locked -p lenso-usage-quota --example api-quota`: PASS.
  Three distinct requests with capacity two produce two real fixture rows;
  replaying the first request adds no usage; the third distinct request exhausts.
- `cargo fmt --all --check`: PASS.
- `cargo clippy --offline --workspace --all-targets --all-features --locked -- -D warnings`:
  PASS after fixing a README rustdoc formatting warning.
- `cargo test --offline --workspace --locked`: NOT ALL PASS. Existing
  `crates/plugin-runtime/tests/lifecycle.rs` has 6 failures with OS permission
  denied when binding localhost TCP listeners (e.g. line 111). No permission
  escalation or network bypass attempted. Cargo stopped there, so remaining
  workspace test binaries were not certified by that run.
- `git diff --check`: PASS.

Tests cover persisted replay/conflicts; partial consume/release; inclusive expiry;
scoped expiry at i64 capacity limit; validation and backwards-clock rejection;
API insertion failure/debit rollback; 16 independent connections contending for
7 units; racing identical reservations; consume-versus-release serialization;
plugin shutdown rejection; SQLite commit-hook failure classified as unknown with
no retry/refund; and reopening after child process exit without cleanup.

Not verified: actual power-loss or filesystem faults, multi-host/network filesystem
locking, remote provider outcomes, PostgreSQL, real customer billing, throughput,
or production transport/auth policy. Cross-process crash recovery was exercised;
concurrent writer contention used independent connections in threads, not a
multi-process load test. No runtime performance claim is made from build times.

Expiry is explicitly host-scheduled per scope. Unknown outcome lookup returning
NotObserved is not a safe automatic retry/refund decision. Operational records
are retained but are not a complete accounting ledger or tamper-evident audit log.
