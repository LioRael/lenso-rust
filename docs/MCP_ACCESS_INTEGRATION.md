# Local Rust MCP and trusted-access integration

Local integration owner receipt, 2026-10-17 UTC. No publication or landing.

## Sources and commits

* Rust base: fetched `origin/main` at
  `768c2830c0bbb6d44f3ac0443165428d512e8b5d`.
* MCP native projection: `1bd6bcd3fd0c9b1556cc3a1306e343794beb8420`.
* Real official-SDK stdio and catalog lifecycle follow-up: `8afbb39`.
* Access candidate from `task-11/rust-auth`:
  `ed9792644b7a0c9db5501f7b0697e663010b4d3e`, based on `2640a27`.
  That base is an ancestor of fetched main, and
  `git diff 2640a27 origin/main -- crates/authorization` was empty.
  Cherry-picked without conflicts as `063b3fde5d61af96764b24170489bcbabf771040`,
  preserving authorship. Its source checkout was left clean and unchanged.
* TS reference: `12b5e2ec3aa722cb22ffcab8173480e5858e882c`; no TS mutations.

The final integration adds one dev-only dependency from MCP to Authorization,
one joint protocol test, and explicit lenso-mcp server identification. Production
MCP has no Auth/Authorization dependency and consumes only host policy callbacks.
No changes to the imported authorization implementation were needed.

## Actual client/server acceptance

`crates/mcp/tests/stdio.rs` launches `lenso-mcp-demo` as an independent process
with piped stdin/stdout. An official rmcp 3.5.0 client completes initialize,
negotiates 2025-11-25, discovers echo/wait tools, receives structured echo results,
observes safe input and unknown-tool errors, cancels a request through the SDK,
and successfully calls echo again. Closing the client leads to a successful child
exit. No arbitrary JSON output or test harness is mixed into server stdout.

A second SDK session uses local duplex pipes to inspect actual cancellation
arrival, retain a blocked callback through cancellation and catalog replacement,
receive tools/list_changed, discover only the replacement, reject stale cursors
and removed names, and verify close waits for actual callback settlement.

`crates/mcp/tests/authorization.rs` connects the same bounded SDK bridge to
`AuthorizedAccess<Arc<Evidence>>`. The synthetic host verifier requires the exact
original identity Arc. Through actual SDK tool calls, a discovered tool:

1. succeeds with current host evidence;
2. refuses at effective session expiry;
3. succeeds after the host extends its current session;
4. refuses a different audience;
5. refuses after host revocation, with a fixed forbidden-operation diagnostic.

MCP caches neither the resulting allow nor a session snapshot. AccessVerifier is
called by trusted policy on every invocation. Only the host changes session state;
there is no OAuth issuer, credential creation, login or renewal API in this work.

## Final checks

Toolchain: Rust/Cargo 1.99.0, macOS arm64. Explicit toolchain bin PATH bypasses
restricted mise shims. Isolated CARGO_HOME only reads existing cached registry/git
sources and contains no copied credentials. All checks use the formal Runtime
Git pin `dd0eb084e490e5e4bda05ce0dbb503f3aae50590` unchanged.

```sh
cargo test --workspace --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test -p lenso-mcp -p lenso-authorization --no-default-features --locked --offline
cargo fmt --all --check
git diff --check
```

Results: **226 workspace tests passed**, strict workspace Clippy and formatting
passed; **39 no-default-feature tests passed**. One explanatory README snippet
is intentionally ignored by rustdoc. MCP has 9 focused integration tests (6 native,
2 SDK stdio/lifecycle, 1 SDK/access). Existing Runtime tests require reviewed
permissions for synthetic ephemeral loopback peers; all new MCP tests use only
pipes/subprocesses. No real service or model was contacted.

Final Cargo.lock SHA-256:
`6f914beb3b8e106e558e2bf210ea559e7b8d7dc21da2a8279f2efe9a46a8cf13`.
Lockfile additions are the MCP/official-SDK dependency graph and dev-only access
integration dependency. No previously locked package or Runtime pin was upgraded.

## Deliverable boundaries

Changed: `crates/mcp/**`, imported `crates/authorization/**`, workspace member and
lock entries, root README, this integration receipt. Source workspaces, other
plugins, TS/Agent/Runtime implementations, environment files, databases and
credentials were not modified. No push, merge to main, release, deployment or
browser launch occurred.

Usable entry: build `cargo build -p lenso-mcp --bin lenso-mcp-demo --locked` and
configure an MCP stdio client with the absolute `target/debug/lenso-mcp-demo`
path. This demonstration exposes synthetic tools. Real hosts provide their own
explicit services/validators, identity, policies and stderr logging.

Only stdio tools and host-driven list-change notifications are shipped. HTTP,
OAuth issuance, task/resource/prompt surfaces, durable cancellation and newer
stateless protocol behavior are outside scope. Native results are finite JSON
but are not automatically secret-redacted. Noncooperative work can prolong drain.
These are documented limits, not unverified claims of production auth readiness.
