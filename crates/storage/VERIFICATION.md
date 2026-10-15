# Storage delivery verification

Verified 2026-10-15 on macOS arm64 with Rust/Cargo 1.99.0. All tests use disposable
temporary directories with canonicalized trusted ancestry. No production paths,
credentials, network providers, shared source repositories or other plugin task
directories were modified.

## Checks

| Command (Rust 1.99.0) | Result |
| --- | --- |
| `cargo test -p lenso-storage -p lenso-plugin-core --offline --locked` | Passed: Storage 15 integration tests + 1 executable doc test; existing Core 28 integration tests + 3 doc tests |
| `cargo clippy -p lenso-storage --all-targets --offline --locked -- -D warnings` | Passed without warnings |
| `cargo fmt -p lenso-storage -- --check` | Passed |
| `cargo run -p lenso-storage --example local --offline --locked` | Passed; `notes/hello.txt: 13 bytes, selected text: storage` |
| `cargo run -p lenso-storage --example injected --offline --locked` | Passed; exact-instance dependency completed and shut down |
| `git diff --check` | Passed |

The host's default `mise` shim could not run inside the sandbox. Checks used the
existing `~/.cargo/bin/cargo +1.99.0`; fmt/clippy used the installed 1.99.0
toolchain's bin directory on PATH. No toolchain installation or network access
was required.

The regression set covers concurrent create-only writers (24 independent
instances), delete/recreate/read snapshots, list racing owned staging cleanup,
byte limits/size mismatch/source failure, cancellation before and during blocked
upload, dropped caller futures, unconsumed downloads, terminal EOF, range/ETag
boundaries, empty objects, pagination, partial deletion tombs, forged/corrupt
metadata, unsafe ancestry, replaced roots, symlinks/hardlinks/FIFOs and unexpected
entries. Lifecycle tests verify exact-handle injection, use of dependencies by
already admitted work during shutdown, resources cleanup after drain, startup
rollback and cancellation of a close waiter.

## Independent review

A separate GPT-6 Astra reviewer performed a read-only code and TS-semantic review.
It found one P2 issue: an upload could remove its staging directory after `list`
enumerated it, causing `list` to return `NotFound`. The fix ignores disappearance
only for a valid staging directory name; symlinks and other invalid entries
remain errors. The reviewer reproduced the issue in round 0 before the fix and
verified 100 rounds with 100 cancelled uploads each (10,000 cancellations) after
the fix without another list failure. A bounded version of this regression is
included in the crate tests. The review reported no other confirmed blocking
issue within the trusted-root contract.

During implementation checks, successful publication also exposed an incorrect
attempt to close the staging directory at its former path. Publication now
disarms the temporary-directory guard after a successful rename; failed uploads
still explicitly remove their own staging directory and propagate cleanup errors.

Not exercised: Linux execution, Windows support, injected disk/sync/cleanup
failures, network filesystem behavior, hostile concurrent ancestor replacement,
process-crash recovery and power-loss durability. These are documented limits,
not claims covered by passing tests.

## Integration boundary

All implementation, tests, examples and docs are under `crates/storage/`.
No Core, Runtime, Log or Cache source files changed.

The root build wiring is deliberately separate for parent-task integration:

- `Cargo.toml`: add only `"crates/storage"` to workspace members.
- `Cargo.lock`: add `lenso-storage` and its dependency closure/feature resolution;
  no existing locked package version is upgraded.

Direct dependencies are the existing Core, Tokio, serde and serde_json workspace
dependencies plus sha2, tempfile, fs2, tokio-util and Unix libc. The plugin has no
cross-plugin business dependency, database provider or new DI/permission container.
The parent can integrate the crate independently and regenerate its combined
workspace lockfile while merging the other plugin tasks.
