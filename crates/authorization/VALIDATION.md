# Authorization delivery validation

Date: 2026-10-15. Rust base:
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.
Branch: `feat/authorization-plugin`, in the isolated `authorization-work` clone.
All authored source, examples, tests and docs are under `crates/authorization`.
The shared original checkout and existing Log/Cache/Core/Runtime sources were not
modified. No push, deployment, publishing or production access was performed.

## Executed checks

Toolchain: `rustc 1.94.0 (4a4ef493e 2026-03-02)`;
`cargo 1.94.0 (85eff7c80 2026-01-15)`. The shell's mise shim is blocked in this
sandbox, so checks used the installed toolchain directly:

```sh
export PATH=$HOME/.rustup/toolchains/1.94.0-aarch64-apple-darwin/bin:$PATH
```

| Command | Result |
| --- | --- |
| `cargo test -p lenso-authorization --all-features --offline --locked` | Passed: 30 integration tests and 1 executable documentation test |
| `cargo test -p lenso-authorization --no-default-features --offline --locked` | Passed: 25 service tests and 1 documentation test; 5 Core lifecycle tests correctly feature-gated out |
| `cargo clippy -p lenso-authorization --all-targets --all-features --offline --locked -- -D warnings` | Passed, no warnings |
| `cargo fmt -p lenso-authorization -- --check` | Passed |
| `cargo run -p lenso-authorization --no-default-features --example standalone --offline --locked` | Passed; prior allow followed by changed owner/version refuses conditional write |
| `cargo run -p lenso-authorization --example injected --offline --locked` | Passed; exact injected role instance grants read; clean shutdown |
| `cargo tree -p lenso-authorization --no-default-features --offline --locked -e normal` | Confirmed no plugin-core dependency in service-only build |
| `git diff --check` | Passed |

No network access or new registry version resolution was needed. Package checks
resolve the formal existing workspace lock, without a local Runtime source override.
The entire unrelated workspace suite and Rust 1.80 execution were not rerun;
source MSRV is declared 1.80 and selected direct/transitive dependency metadata
requires no newer compiler. Parent integration remains responsible for the combined
five-plugin workspace check.

## Regression evidence

- `tests/decisions.rs` (10 tests): full effect conflict matrix, rule applicability,
  exact scope and literal wildcard text, identity/audience/credential ceilings,
  condition failure propagation, authoritative resource/attribute ordering,
  relation resolution, validation limits, gated explanations, observer/panic
  failure and precise integer comparisons.
- `tests/async_boundaries.rs` (7): ownership before first poll and through awaits,
  one clock snapshot, deadline and late-result rejection, cancellation before/
  during/same poll, abandoned futures, concurrent request isolation, business
  owner/version fence and synchronous over-deadline callback rejection.
- `tests/rbac.rs` (8): exact identity/scope/expiry, constrained RBAC grants,
  invalid graph rejection, fresh-read revocation and immutable retained snapshots,
  two-writer CAS, invalid-source fail-closed behavior, resource permission ceilings,
  in-flight old-snapshot versus next-read revocation semantics.
- `tests/lifecycle.rs` (5): repeated provider types remain exact instances,
  same-name foreign handles and undeclared reads fail, setup rollback closes only
  owner resources once, outer Core admission drains before borrowed-provider
  cleanup, pure service has no invented shutdown state.

## Independent review

A separate read-only reviewer inspected the pure engine, fact types, RBAC and Core
wrapper against the supplied TS source. It reproduced an integer attribute bug:
converting both values to `f64` could let `9007199254740993_u64` match
`9007199254740992_u64`. The fix retains exact integer equality and performs bounded,
integral float/integer comparison. Bidirectional `Equals` and `In` regressions
include >2^53 values, i64/u64 upper boundaries, i64 minimum and `1 == 1.0`.

The reviewer independently ran the then-current 23 tests successfully and reported
no remaining concrete authorization bypass. Its requested lifecycle, in-flight
revocation and synchronous-deadline regressions were subsequently added; the final
30-test run above passed. This is a focused code review, not a formal security proof.

## Root integration diff, isolated from crate ownership

Only these two root changes are required. Existing dependency versions, checksums
and Runtime Git pin remain unchanged:

```diff
--- a/Cargo.toml
+++ b/Cargo.toml
@@ workspace members
     "crates/cache",
+    "crates/authorization",
     "crates/plugin-runtime",
```

```diff
--- a/Cargo.lock
+++ b/Cargo.lock
@@ before lenso-cache
+[[package]]
+name = "lenso-authorization"
+version = "0.1.0"
+dependencies = [
+ "futures-util",
+ "lenso-plugin-core",
+ "serde_json",
+ "thiserror",
+ "tokio",
+]
+
 [[package]]
 name = "lenso-cache"
```

The parent can add the one member and merge this one local package record while
integrating the other plugins. No root `workspace.dependencies` alias is necessary
for this crate; applications may add one at integration time if desired.

## Remaining scope boundaries

No delivery blocker remains. Auth adapters, persistent stores/migrations, role
management/delegation, list filtering, transport registration and SQL translation
are explicitly unimplemented; see [PARITY.md](PARITY.md). A prior allow never fences
a future database or external side effect. Custom sources own their consistency
and cancellation behavior; callback-spawned work is not owned by this engine.
