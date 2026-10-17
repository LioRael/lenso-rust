# MCP native projection: executed evidence

Checked 2026-10-17 UTC with Rust 1.99.0 on macOS arm64.

## Source reconciliation

* Isolated checkout/branch: `task-13/lenso-rust`, `feat/mcp-native-parity`.
* Fetched Rust `origin/main`: `768c2830c0bbb6d44f3ac0443165428d512e8b5d`.
* Fetched TS main into a separate tracked-files-only reference:
  `12b5e2ec3aa722cb22ffcab8173480e5858e882c`.
* Historical `task-12/lenso-rust` branch:
  `04a4cf63031424155fb842adaeaf59a2dd86a028`. Its Limits code is already
  integrated; `git diff history/limits origin/main -- crates/limits` changes
  only two lines of VERIFICATION.md. Source checkouts were clean and untouched.
* Current Rust main contained no MCP crate. Current TS `packages/mcp/src/adapter.ts`
  supplied the exact stable-name algorithm, pagination/structured result and
  cancellation/drain reference; `protocol.ts` supplied protocol error separation.
  TS uses immutable selected operations; dynamic registration is not implemented
  in this Rust increment either. This is not a claim of TS Engine API parity.

## Delivered gaps and observable improvements

1. A Rust host can expose an explicit native callback catalog with names that
   remain stable across allowlist reorder. Pagination cannot transfer an offset
   to a different identity Arc, changed visible sequence, replayed/expired cursor
   or different adapter. Construction rejects duplicate names and unbounded
   catalog metadata. The hash test vector is independently calculated with
   Python hashlib over compact UTF-8 JSON, matching the inspected TS algorithm.
2. Calls produce serializable MCP JSON text plus object structuredContent,
   including wrapping array/scalar/null values. Fixed safe error classifications
   omit private input/error strings. Input/output byte bounds and protocol
   InvalidParams differ from tool business errors. Validators remain host-owned.
3. A canceled/timed-out/dropped response cannot prematurely release its native
   app WorkGuard or adapter concurrency slot. Real callback settlement gates
   close and native shutdown. Policies receive the cooperative cancellation token;
   owning services retain their own authorization responsibility.

## Executed commands

Use a real toolchain bin directory at the front of PATH in this host environment:
its default Cargo/rustdoc/formatter shims invoke mise and are sandbox-restricted.
An isolated CARGO_HOME used read-through symlinks to the existing registry/git
cache, without copying credentials or config. Runtime's immutable Git pin is
unchanged. Lockfile additions are only lenso-mcp and base64 0.22.1.

```sh
cargo test -p lenso-mcp --locked --offline
cargo run -p lenso-mcp --example mock --locked --offline
cargo test --workspace --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo fmt --all --check
git diff --check
```

Results: 6 focused integration tests passed; 216 workspace tests including
rustdoc tests passed; strict workspace Clippy, formatting and whitespace checks
passed. The first workspace attempt failed only on pre-existing Runtime tests'
loopback socket binds denied by the sandbox. After inspecting their synthetic
127.0.0.1 peer implementation, the same locked offline suite passed under
reviewed local socket permissions. No real Runtime server was contacted.

The mock example started one native App, listed the echo tool with stable name
`operation__uEUnFF9ufYiFVT9imNLqrmXg5mIhA_a8Y7QG_v-9JI`, emitted JSON-RPC
result envelopes for IDs `0` and `""`, called it with synthetic local text,
returned `structuredContent: {"echo":"local mock"}`, closed the adapter and
shut down the App. These are local protocol projections, not a negotiated SDK
client/server session.

## Boundaries and unrun checks

Only `crates/mcp/**`, workspace member/dependency lock entries and root README
change. No Auth/Authorization/OAuth module, Runtime/Agent/TS API, credential,
environment file or DB changes. No source workspace mutations, push, main merge,
publish, deployment, model request, real account binding or browser launch.

This increment does not ship HTTP/stdio framing, official SDK interoperability,
initialize negotiation, dynamic list-change notifications or schema conversion.
Those require a separate host transport integration. No full MCP server or
production authentication readiness is claimed. See README for validator,
identity Arc, output redaction and noncooperative drain limits.
