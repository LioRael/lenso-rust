# Native composition and lifecycle

Evidence: `crates/plugin-core/src/lib.rs`, `src/config.rs`, and the crate README at integrated `c4c9a0b` (verify installed exports first). Prefer those local files over old portable/linked/process examples. The shortest complete plugin is the README's Prefix/Greeting example; it is compiled by `cargo test -p lenso-plugin-core --doc --locked`. The two-source composition sample is `crates/log-plugins/examples/offline.rs`. Its unguarded synchronous aggregator is not a template for retained-service admission, and its final shutdown `?` does not retain simultaneous business/cleanup errors:

```sh
cargo run --locked -p lenso-log-plugins --example offline
```

## Authoring decisions

| Need | Actual API and consequence |
| --- | --- |
| Define a plugin | `impl Plugin`, `type Service: Send + Sync + 'static`, ordinary `async fn setup(self, &mut Setup) -> Result<Arc<Service>, BoxError>` |
| Create/install | `Instance::new(name, plugin)` → `.handle()` → `App::new().with(instance)`; or `App::install` |
| Forward dependency | `App::reserve::<Service>(name)`, then `App::bind(&handle, plugin)`; unbound/foreign/duplicate bindings fail before setup |
| Exact injection | Store `Handle<Service>`, return its `.key()` from `dependencies`, then `setup.get(&handle)?`; cloning a handle preserves identity |
| Config | Plain typed plugin data by default; `Plugin::validate` for synchronous checks. Optional `Config<T>` resolves serde data; it does not own resources |
| Resource release | `setup.on_cleanup(CleanupPhase::Resources, closure)` returns `CleanupHandle`; `.dispose().await` shares the same cleanup result |
| Listener/task admission | `CleanupPhase::Entrances` stops incoming work; an outer service stores `setup.work_gate()` and holds `let _work = gate.enter()?` for the complete operation |

Do not hold a synchronous mutex guard across await. Long-lived tasks need an owned stop signal, a join path, and registered cleanup. Moving a service into an `Arc` supplies shared ownership, not automatic synchronization or async destruction.

## Current shutdown and startup semantics

Shutdown seals new admission → stops **all** entrances → waits for admitted `WorkGuard`s → releases resources. Each cleanup phase reverses startup/registration order. Release your outer guard before awaiting shutdown; waiting while holding it deadlocks drain. Get dependency services during setup, then call them within admitted work; a fresh `RunningApp::get` after sealing is rejected even if the caller already holds a guard.

Retained service methods must participate in this admission discipline. Core cannot instrument arbitrary Rust calls. If background work outlives its caller, transfer/retain the accepted guard until that work and its resource use finish. Do not admit it a second time after shutdown has sealed the gate. A blocked setup, guard, cleanup or non-yielding task has no automatic universal timeout.

At integrated `c4c9a0b`, Core owns startup in a Tokio task. Cancellation of the waiting future does not abandon setup: it finishes and shuts down an app that was not acknowledged. Returned setup failures **and unwind panics** roll back callbacks, including partial setup. Validation/dependency unwind failures happen before setup. This differs from the old `f10badf` startup, which was not cancellation-safe. Runtime termination and abort-on-panic remain outside the guarantee.

Shutdown and early disposal share owned completion across concurrent/cancelled waiters. Dropping an acknowledged `RunningApp` still does not run async cleanup. Keep Tokio alive until completion. Early disposal does not seal the gate; coordinate dependents first. Preserve `StartError::Setup { source, cleanup, .. }` and `ShutdownError.failures`; only expose approved safe error classifications at external entries.

## Optional config

`Config::<Settings>::new(instance).explicit_json(value).env_json("/field", "ENV_KEY").resolve()` is the actual builder. Values selected from environment are JSON, so a string value needs JSON quotes. Precedence is selected environment > explicit JSON > serde defaults. Overlapping env selections apply in declaration order; nested pointers traverse objects only. This is not TS top-level source composition and has no implicit dotenv/file/remote provider.

Use `.diagnostic_field("/field")` for safe paths and `.sensitive("/credentials")` for sensitive subtrees. Keep original values and serde error messages out of diagnostics. Do not copy TS `bindConfig`, Standard Schema, or Engine APIs into Rust.

Focused evidence exists in `tests/startup.rs`, `admission.rs`, `early_cleanup.rs`, `config.rs` and `validation.rs`; select the test relevant to the change.

Static preflight: when installed, `App::inspect(&self)` returns structured diagnostics
and check execution scope without setup. It invokes trusted validation/dependency
hooks and cannot certify resource availability or a running development session.
