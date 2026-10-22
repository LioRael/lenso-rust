# Notifications candidate validation — 2026-10-22

Source: `/Users/leosouthey/Documents/Codex/2026-10-20/task-8/lenso-rust`.
Baseline: `126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2`.
Independent clone: `/Users/leosouthey/Documents/Codex/2026-10-22/task-35/notifications-checkout`.
Branch: `feat/persistent-notifications`.

Read AGENTS.md and `.agents/skills/lenso-rust/SKILL.md` plus native lifecycle reference.
Source HEAD matched baseline and source status was clean before and after work.
Only this candidate's `crates/notifications/**`, workspace member registration and
required lockfile additions changed. Existing dependency versions and Runtime SDK
`dd0eb084e490e5e4bda05ce0dbb503f3aae50590` are preserved. No task-23 work was imported
and no Core/Tasks/other owner source was edited. All dependency resolution was offline
using the existing Cargo cache; nothing was installed.

## Passed

- `cargo test -p lenso-notifications --locked --offline`: **6 passed**, two HTTP
  listener tests explicitly ignored. Coverage: persistence/reopen, same-key reuse
  and conflict, tenant partition, optional suppression and required bypass, bounded
  recovery, frozen retry payload/key, Unknown no replay, retry cap, invalid inputs,
  atomic concurrent claim across connections, provider identity mismatch, durable
  Unknown visible from another connection before transport, panic handling, cancelled
  waiter, Core shutdown drain and retained-service rejection.
- `cargo run -p lenso-notifications --example order_confirmation --locked --offline`:
  existing Tasks `run_one` succeeded, one capture, Accepted with provider ID
  `capture-only-42`, one attempt; reopen matched receipt with no second send.
- `cargo test -p lenso-tasks --test durable --locked --offline`: **9 passed**, one
  pre-existing ignored helper. No Tasks source changes.
- `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings`:
  passed. Also reran crate-only Clippy after the last test assertion improvement.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

The installed mise command shim returned `Operation not permitted`; commands used
existing rustup binaries with the installed toolchain on PATH. This changed no
permissions or installations. One exec-server disconnection happened before a file
write; inspected target files to confirm no execution before continuing.

## Blocked / not verified

Initial `cargo test -p lenso-notifications --locked --offline` reached the real HTTP
fixture tests, but both failed at `TcpListener::bind("127.0.0.1:0")` with OS code 1,
`PermissionDenied: Operation not permitted`. No request was sent. Status: **denied**,
not pending or uncertain. Did not change addresses, escalate, or retry the listener.
Those tests are now explicitly ignored by default, with the permitted-environment
command documented in README. Their actual HTTP encoding, response/error mapping
and timeout assertions are compiled but **not runtime-verified**. The adapter is
real ureq HTTPS code, separate from the local capture demonstration.

`cargo test --workspace --locked --offline` was attempted and failed in the existing
`lenso-plugin-runtime --test lifecycle` suite: six listener-dependent tests returned
the same PermissionDenied at `tests/lifecycle.rs` lines 111, 162, 179, 218, 264 and
280. Two tests in that suite passed. Earlier suites ran successfully, but later
workspace suites were not reached. This is **not a green full-workspace result**;
no denied Runtime listener was retried. Notifications' own listener tests were
already ignored on this workspace run.

No real email, real credentials, production endpoint, TLS network handshake,
provider account behavior, DNS resolution, paid service, push, main merge, release
or deployment was exercised. No process-kill fault injection was performed for this
crate; durable pre-send commit, panic, cancellation, reopen and concurrent-connection
behavior were tested. Live provider reconciliation/delivery tracking is outside scope.

## Integration

Implementation and minimal workspace registration are delivered as separate commits.
Integrators should combine the crate commit with their own serialized workspace
registration, or take both commits from this branch. HTTP fixture execution remains
an acceptance blocker in a separately permitted environment; do not equate ignored
fixtures with verified HTTP transport.
