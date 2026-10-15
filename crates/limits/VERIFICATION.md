# Limits verification

Initial implementation validated on 2026-10-15 with Rust/Cargo 1.99.0, on the
pinned Rust base
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

| Command | Result |
| --- | --- |
| `cargo test --locked --offline -p lenso-limits -p lenso-plugin-core` | Passed: Limits 16 integration tests + 1 doctest; existing Core 28 integration tests + 3 doctests |
| `cargo clippy --offline -p lenso-limits --all-targets -- -D warnings` | Passed, no warnings |
| `cargo fmt --all -- --check` | Passed for the complete workspace |
| `cargo run --offline -p lenso-limits --example limits-native` | Passed; epoch reset and weighted lease denial/renew/release demonstrated |
| `cargo run --offline -p lenso-limits --example injected` | Passed; exact instance composition and shutdown demonstrated |
| `git diff --check` | Passed |
| `cargo tree --offline -p lenso-limits -e normal --depth 1` | Only direct normal dependency: existing `lenso-plugin-core` |

The host's mise shim could not run inside the sandbox, so commands used the
installed toolchain's `bin` directory explicitly. An isolated task-local
`CARGO_HOME` reads the existing cached dependencies; no network download or
shared original repository changes were needed.

## Initial implementation review

An independent reviewer performed two read-only passes against the supplied TS
snapshot, existing plugin-core cleanup implementation, final Rust source and
tests. No blocking correctness or lifecycle issue was found. The second pass
independently ran `cargo test --offline -p lenso-limits`; all 16 integration
tests and the doctest passed.

The first pass identified an ownership-tracking documentation gap: expired
leases remain recorded by the service until release, lost renewal or close.
README and PARITY now explicitly describe this behavior and the need for
long-lived callers to release completed leases.

The final implementation also seals admission before waiting for an in-flight
store operation. A controlled thread regression verifies that newly arriving
calls reject while close waits, the in-flight acquisition completes, and close
then releases that lease.

## Integration and limits

The implementation, examples, tests and docs are confined to `crates/limits`.
Root wiring is separately identified: one `Cargo.toml` workspace-member line
and one eight-line `Cargo.lock` package entry, with no existing dependency update.
No Core, Runtime, Log or Cache implementation changed. The initial implementation
is preserved in commit `64b571b`; root wiring is preserved separately in `265aae4`. The async follow-up is a separate commit.

See [PARITY.md](PARITY.md) for the full supported/deferred matrix. The entire
Runtime workspace and TS DB/Auth test suites were not run; this validation targets
the new in-memory crate and its existing Core lifecycle dependency. There are no
remaining execution blockers. Async external providers, optional logging and
DB/Auth integrations remain deferred; owned async execution is covered below.

## Owned async execution follow-up

The follow-up adds `with_lease`, opt-in automatic renewal, cooperative
cancellation, `close_async`, non-renewing liveness observation and plugin drain
integration. Tokio is now a normal dependency; the existing lockfile resolution
is unchanged. Manual methods still need no runtime and start no renewal tasks.
The new `lease-run` example demonstrates renewal across the original TTL and
explicit cancellation; it does not claim to roll back business effects.

The paused task's saved verification reported these successful checks before
this resumption (they were not repeated during the final documentation pass):

| Command | Saved result |
| --- | --- |
| `cargo test --locked --offline -p lenso-limits -p lenso-plugin-core` | Limits: 28 integration tests + 2 doctests; Core: 28 integration tests + 3 doctests |
| `cargo clippy --offline -p lenso-limits --all-targets -- -D warnings` | Passed |

Final checks run on 2026-10-15 with Rust/Cargo 1.99.0 and
`CARGO_HOME=<isolated-cargo-home>`:

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed for the complete workspace, including the new example |
| `cargo clippy --locked --offline -p lenso-limits --all-targets -- -D warnings` | Passed, including all examples and regression targets |
| `cargo run --locked --offline -p lenso-limits --example lease-run` | Passed; cancelled after 10 completed steps and `close_async` completed |
| `git diff --check` | Passed |

The 12 added integration regressions cover success/business failure/callback
panics, validation and pre-cancellation, owned renewal, expiry/executor stalls,
lost leases and backend failures, final liveness observation and visible
fail-open admission, combined business/release failures, dropped callers,
transitional synchronous close and dropped async close waiters, plugin WorkGate
drain, renewal crossing its old deadline, and a runtime without a time driver.

The handoff records two review findings as fixed. The final read-only review
checked the saved runner, service, memory observation, plugin cleanup and tests;
it found no further blocking issue. In particular, a renewal returning after the
old local deadline cannot claim continuous ownership, and missing Tokio time
support is rejected before lease acquisition or business execution.

`ActiveRuns` is an uncached transitional result: synchronous close seals
admission and requests cancellation without releasing leases while wrappers are
active. `close_async` drains them, then final close attempts remaining releases
and caches that final result. Cancellation is cooperative; unresponsive callbacks
can delay drain indefinitely. Provider/clock panics and runtime destruction are
outside the cleanup guarantee. A lease snapshot is not fencing and cannot undo
business effects. No broader workspace/TS/DB/Auth verification was added, and no
push, main merge, publication or deployment was performed.
