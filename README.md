# Lenso Rust

Native Rust plugin composition and applications. This repository owns:

- `crates/plugin-core` (`lenso-plugin-core`): typed instance handles, dependency
  planning, validation, setup and explicit asynchronous cleanup.
- `crates/log-plugins` (`lenso-log-plugins`): replaceable JSON-lines and memory
  sources, ordinary aggregation services and a transport-free example.
- `crates/log` (`lenso-log`): structured tracing-backed logging, scoped
  subscribers, redaction and explicit owned flush.
- `crates/cache` (`lenso-cache`): typed bounded memory, finite TTL, exact
  invalidation and cancellation-safe shared loads.
- `crates/mcp` (`lenso-mcp`): borrowed native tool projection with stable names,
  scoped pagination, structured results, cancellation/drain and official-SDK
  stdio transport. See its
  [scope and host integration boundary](crates/mcp/README.md).
- `crates/plugin-runtime` (`lenso-plugin-runtime`): optional Runtime connection
  injection and explicit post-setup exports.
- `examples/batch`: the native serving App (`logs-batch`) and generated-client
  caller App (`logs-client`).

[Lenso Runtime](https://github.com/LioRael/lenso-runtime) owns the execution
kernel, protocol, Rust/TypeScript SDKs, transport, contract generator and shared
cross-language smoke. None of those implementations is copied here. TypeScript
Lenso, its CLI and Console remain separately owned.

The [first-wave contract](FIRST_WAVE.md) defines shared behavior, ownership,
supported TS alignment and deliberate differences. Log and Cache stay local;
only selected business exports use Runtime.

## Build and offline usage

Use Rust/Cargo. The generated batch bindings use `std::sync::LazyLock`, requiring
Rust 1.80 or newer for the batch bindings; MCP's default stdio feature requires
Rust 1.88 or newer. Core declares a Rust 1.77 minimum,
matching the locked `thiserror` dependency.

```sh
cargo build --workspace --locked
cargo test --workspace --locked
cargo run --locked -p lenso-log-plugins --example offline
cargo run --locked -p lenso-log --example log-native
cargo run --locked -p lenso-cache --example cache-native
```

The offline example needs no kernel or port. It installs two separately
configured file sources and aggregators and prints counts and durations. Cargo
may still fetch dependencies when its cache is empty; “offline” describes the
application, not Cargo network resolution.

Minimal assembly with an ordinary typed service:

```rust
use lenso_log_plugins::{AggregateConfig, LogAggregationPlugin, LogRecord, MemoryLogSource};
use lenso_plugin_core::{App, BoxError, Instance};

async fn example() -> Result<(), BoxError> {
    let source = Instance::new("source", MemoryLogSource::new(vec![LogRecord {
        level: "error".into(),
        message: "example request failed".into(),
        duration_ms: 12,
    }]));
    let summary = Instance::new(
        "summary",
        LogAggregationPlugin::new(source.handle(), AggregateConfig::default()),
    );
    let handle = summary.handle();
    let app = App::new().with(summary).with(source);
    let running = app.start().await?;
    let result = running.get(&handle)?.summarize();
    running.shutdown().await?;
    println!("{:?}", result?);
    Ok(())
}
```

Use `MemoryLogSource::new(records)`, `JsonLinesPlugin` or your own `LogSource`
without changing the aggregation plugin. See the
[core README](crates/plugin-core/README.md) for a complete plugin implementation,
resource registration and lifecycle rules.

## Runtime integration and smoke

The adapter and batch share one `runtime-sdk` dependency in `Cargo.toml`, pinned
to Git revision `dd0eb084e490e5e4bda05ce0dbb503f3aae50590` at
`https://github.com/LioRael/lenso-runtime`. `Cargo.lock` records that immutable
source. There is no default sibling checkout dependency or committed local path.
This is source integration, not a claim of crates.io publication.

Verify the formal pin with the locked commands above. For cross-source smoke,
build both repositories against one explicitly selected Runtime candidate
(replace the placeholder paths):

```sh
cd /path/to/lenso-runtime
bun install --frozen-lockfile
bun run build
bun scripts/local.ts debug

cd /path/to/lenso-rust
sh scripts/build-runtime.sh --runtime-root /path/to/lenso-runtime
sh scripts/build-runtime.sh --runtime-root /path/to/lenso-runtime \
  --bin logs-client --out target/debug/logs-client

cd /path/to/lenso-runtime
LENSO_RUST_ROOT=/path/to/lenso-rust bun run smoke
```

The Runtime checkout must include its candidate-proof helpers in
`scripts/local.ts` and its `LENSO_RUST_ROOT` smoke mode. That mode selects this
repository's `target/debug/logs-batch` and
`target/debug/logs-client`, using Runtime's real kernel, generated TypeScript
client and existing tests for calls, streaming, cancellation, deadlines and lost
connections. It starts and stops its own processes on an available loopback port.
No TypeScript SDK or runner belongs in this repository.

`examples/contracts/logs.json` and `examples/generated/logs.rs` are the retained
example contract and generated bindings. Keep them in sync using Runtime's
generator when the example contract changes; the generator remains Runtime-owned.

For deliberate SDK candidate testing only, provide an explicit Cargo override:

```sh
RUNTIME_ROOT=/absolute/path/to/trusted/lenso-runtime
cargo test --workspace \
  --config "patch.\"https://github.com/LioRael/lenso-runtime\".runtime-sdk.path=\"$RUNTIME_ROOT/crates/sdk\""
```

This selects local candidate bytes, including uncommitted edits; it does not
verify the formal Git pin. Do not commit the override or a candidate-resolved
lockfile. Run `cargo update -p runtime-sdk` without the override and repeat the
locked build/tests to return to the formal pinned SDK.

For repeatable local **development builds**, use the staged builder instead of
altering the checkout's lockfile:

```sh
sh scripts/build-runtime.sh --runtime-root "$LENSO_RUNTIME_ROOT" --out target/debug/logs-batch
sh scripts/build-runtime.sh --runtime-root "$LENSO_RUNTIME_ROOT" --out target/debug/logs-client --bin logs-client
```

It requires Bun, Cargo and POSIX shell. Only the owned
`target/.lenso/runtime-rust-stage` receives the local Git-source override and
Runtime-generated contract/bindings. Unchanged source timestamps and target
artifacts remain reusable; deleted source files are removed from that stage.
Staging compares file contents, not size/second-resolution mtimes, so rapid or
same-size source changes cannot silently reuse an old Cargo build. Source trees
must contain regular files/directories; this example builder rejects symlinks.
The repository's manifest, formal dependency pin and `Cargo.lock` stay unchanged.
Successful builds atomically copy the executable to `--out`; failed builds leave
the prior executable intact. `--target-dir` selects the Cargo artifact cache;
`--release --out target/release/logs-batch` builds the measured optimized binary.
The binary sidecar records Runtime/native source hashes, actual Cargo profile,
binary SHA-256 and build wall time. Current cross-source checks reject missing
or stale proofs, rather than silently accepting a formal-pin or older build as
the selected local candidate. This consistency check is not signed artifact
attestation and does not replace the separate formal-pin verification.
No npm/crates release is involved.

The serving App accepts `LENSO_RUNTIME_INSTANCE` or `LENSO_WORKSPACE_INSTANCE`
for exact candidate readiness, and `LENSO_RUNTIME_SERVICE` (default `batch`).
SIGTERM awaits native shutdown, including Runtime drain. Initialization emits
actual instance/version/elapsed metadata; `LENSO_PERF_TIMING=1` enables bounded
benchmark collection of per-request business compute time through structured
stderr (no input payloads). The TS Engine workspace owns build/watch/cutover;
this App retains ownership of native resources. Windows staging/signal behavior
has not been verified.

## Lifecycle and trust boundaries

Native plugins are trusted, statically linked Rust code with the process's
privileges, not sandboxed modules. JSON-lines sources read explicitly configured
paths. Do not treat plugin validation, typed handles or contract validation as
authorization or isolation. Runtime's local protocol is not a public-network
security boundary; use a trusted loopback environment and consult Runtime's
protocol/security documentation before exposing it.

Runtime caller identity describes a local process, not authenticated user
evidence. Serialized TS Auth actors are not trusted Rust identities. This version
adds no cross-service user authorization protocol; real user/resource permissions
remain enforced by the existing resource owner.

`Instance::new` acquires no resources. `App::start` validates and plans the whole
graph before setup. Register cleanup immediately after acquiring a resource,
before another fallible operation or await. Returned setup errors and unwinding
setup panics roll back registered resources. Preflight panics fail before setup.

Core's default `App::start` owns startup independently of its waiter and shuts
down an App it cannot deliver. Cancelling a waiter is not a bounded way to stop
uncooperative setup. The serving adapter also owns post-setup export activation;
only explicit exports become ready. Process termination, `panic=abort` and a
stopped Tokio runtime cannot be recovered.

Shutdown seals `WorkGate`, stops all entrances, drains accepted `WorkGuard`s,
then releases resources in reverse dependency order. Entry methods must check
their captured gate even on retained `Arc` references; dependency Log/Cache calls
remain available to already accepted work until resource cleanup. Explicit early
disposal and shutdown share one cleanup completion. Concurrent shutdown waiters
share results; cancelling a waiter does not abandon cleanup. Keep Tokio alive
until shutdown completes. Dropping an App or retained service does not perform
asynchronous cleanup; arbitrary service methods are not intercepted automatically.
