# Lenso Rust

Native Rust plugin composition and applications. This repository owns:

- `crates/plugin-core` (`lenso-plugin-core`): typed instance handles, dependency
  planning, validation, setup and explicit asynchronous cleanup.
- `crates/log-plugins` (`lenso-log-plugins`): replaceable JSON-lines and memory
  sources, ordinary aggregation services and a transport-free example.
- `crates/plugin-runtime` (`lenso-plugin-runtime`): optional Runtime connection
  injection and explicit post-setup exports.
- `examples/batch`: the native serving App (`logs-batch`) and generated-client
  caller App (`logs-client`).

[Lenso Runtime](https://github.com/LioRael/lenso-runtime) owns the execution
kernel, protocol, Rust/TypeScript SDKs, transport, contract generator and shared
cross-language smoke. None of those implementations is copied here. TypeScript
Lenso, its CLI and Console remain separately owned.

## Build and offline usage

Use Rust/Cargo. The generated batch bindings use `std::sync::LazyLock`, requiring
Rust 1.80 or newer for the full workspace. Core declares a Rust 1.77 minimum,
matching the locked `thiserror` dependency.

```sh
cargo build --workspace --locked
cargo test --workspace --locked
cargo run --locked -p lenso-log-plugins --example offline
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

Build Runtime first, then Rust, and run the existing Runtime smoke with an
explicit Rust root (replace the placeholder paths):

```sh
cd /path/to/lenso-runtime
bun install --frozen-lockfile
bun run build

cd /path/to/lenso-rust
cargo build --workspace --locked

cd /path/to/lenso-runtime
LENSO_RUST_ROOT=/path/to/lenso-rust bun run smoke
```

The Runtime checkout must include its optional `LENSO_RUST_ROOT` smoke mode. That
mode selects this repository's `target/debug/logs-batch` and
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
before another fallible operation or await. Returned setup errors roll back
registered resources; validation/setup panics are not returned setup errors.

Core startup is **not cancellation-safe**: do not drop its future once polled.
The serving adapter's `start_runtime_app` owns startup in a separate Tokio task
and shuts down an App it cannot deliver to a cancelled caller. Export factories
run after native setup; only explicit exports become ready. This is not a
guarantee against process termination, a stopped Tokio runtime or plugin panics.

Cleanup stops all entrances before releasing resources, in reverse dependency
order within each phase. Concurrent shutdown waiters share completion; cancelling
a waiter does not abandon cleanup once it has started. Keep Tokio alive until
shutdown completes. Dropping an App handle or retained service does not perform
asynchronous cleanup; services define their own behavior after shutdown.
