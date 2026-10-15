# Optional native Runtime adapter

`lenso-plugin-runtime` owns native App composition; `runtime-sdk` owns the wire connection. Source: `crates/plugin-runtime/src/lib.rs` and `examples/batch/src/main.rs`. The workspace pins SDK Git revision `dd0eb084e490e5e4bda05ce0dbb503f3aae50590`; do not assume it has every API of a newer Runtime checkout.

1. Construct `RuntimePlugin::new(HostOptions, drain_timeout_ms)` as one exact native `Instance`; capture its handle. It connects registered/unready during setup and registers entrance drain/close.
2. Inject its `Handle<RuntimeHost>` normally into plugins that need generated outbound clients.
3. Build `RuntimeExport::new(connection_handle, move |running| ...)` with generated server bindings over already initialized native services. It binds that exact connection, not a name-matched service.
4. Await `start_runtime_app(app, exports)`. It runs native setup first, validates explicit factories, then activates; failures preserve rollback. Keep unselected methods local. Use the example rather than inventing a `Plugin::export` API.
5. Await `running.shutdown()`. Entrances drain before native resources release. Retained native business methods still need WorkGate admission.

At this baseline the **native RuntimePlugin constructor has no transport selector**. The newer standalone Runtime SDK supports TCP, stdio and WebSocket; that does not make `RuntimePlugin::with_transport` exist. Use the adapter's supported configuration, or explicitly scope a separately reviewed adapter update. Do not silently substitute SDK-only composition for a requested native App.

For the formal dependency run `cargo test --workspace --locked`. For an explicit local SDK candidate use the repository's staged builder, which preserves the root lockfile:

```sh
sh scripts/build-runtime.sh --runtime-root /absolute/trusted/lenso-runtime
sh scripts/build-runtime.sh --runtime-root /absolute/trusted/lenso-runtime --bin logs-client --out target/debug/logs-client
```

These paths are placeholders supplied by the task, not auto-discovered siblings. The staged builder records source/binary proofs. Cross-repo smoke selects the explicit Rust root from the Runtime repo after matching builds. A candidate proof establishes local source consistency, not publication or signed attestation.

Serialized actors, service names and contract validation do not authenticate an end user. Authorization belongs at the resource owner; native values, transactions and clients do not cross JSON. Cancellation is cooperative, not rollback or a retry guarantee. Read the Runtime skill/reference for protocol constraints when changing exported boundaries.
