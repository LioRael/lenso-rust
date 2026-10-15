# Realtime verification

Verified locally on 2026-10-15, macOS arm64, Rust/Cargo 1.99 toolchain, using the
existing cached dependency versions with `--offline --locked`.

| Command | Result |
| --- | --- |
| `cargo test -p lenso-realtime --offline --locked` | Passed: 28 integration regressions (10 authorization, 4 plugin lifecycle, 14 delivery/boundary), 1 doctest |
| `cargo clippy -p lenso-realtime --all-targets --offline --locked -- -D warnings` | Passed, zero warnings |
| `cargo fmt -p lenso-realtime -- --check` | Passed |
| `git diff --check` | Passed |
| `cargo run -p lenso-realtime --example memory --offline --locked` | Passed: ready, update, renew, revoke and close |
| `cargo run -p lenso-realtime --example injected --offline --locked` | Passed: exact policy dependency, ready, shutdown terminal, EOF |

The shell's mise shim was unavailable in this sandbox. The checks used the
installed toolchain binary directory directly and a task-owned `CARGO_HOME` with
read-only access to the existing caches. No package installation or network fetch
was needed. The original repositories were only read; this work is a standalone
local clone on `feat/realtime-memory` based exactly on the requested Rust SHA.

## Separate review pass

The implementation agent performed a second code-reading pass after the initial
tests, inspecting the state machine and TS parity separately from writing code.
This is **not an independent second-reviewer sign-off**; parent-task review remains
the independent integration gate.

The review checked lock ordering, wakeup/cancellation races, reservation rollback,
resource-topic removal, exact queue byte accounting, overflow during multi-subscription
fan-out, renewable lease boundaries, shutdown before host work drain and retained
service handles. It found and fixed a host-destructor reentrancy issue: the state
registry could release the last `Identity<P>` under its mutex. The connection now
retains an identity reference, so a user principal's `Drop` runs after unlocking.
`principal_destructor_can_reenter_service_after_connection_drop` verifies it.

The reviewed regression suite includes:

- Cancellation and an authorization grant becoming ready together; cancellation
  wins, and the removed subscription cannot be revived.
- Subscribe/renew cancellation, callback construction/poll panic, old lease expiry
  during renewal, denied renewal, pending operation capacity and slot reclamation.
- Slow authorization cannot reset the start of its lease after it resolves.
- Overflow partway through delivery to multiple subscriptions on one connection;
  the report excludes earlier enqueues purged by that same overflow.
- Revocation/expiry remove queued private updates before the next read.
- Eight concurrent publishers, 800 total messages, two complete identical ordered
  subscriber streams, with each publisher's local order preserved.
- One hundred expiry/admission cycles reclaim topic and connection capacity
  without a sweep task or retained historical topics.
- Two same-type policy instances, missing dependency/config rejection, startup
  rollback and shutdown of a stream holding the host's WorkGuard.

## Remaining boundaries

No TS tests were executed; the pinned TS source/tests were used as a behavioral
reference. Verification is scoped to this crate and its examples, not the entire
workspace, and it does not certify the five-plugin parent integration. There is
no Redis/HTTP/SSE/snapshot/cursor integration in this wave; see `PARITY.md`.
