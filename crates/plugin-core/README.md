# lenso-plugin-core

Native Rust plugin composition, independent of Lenso Runtime and its SDK.
Each installed instance has a typed handle. The same plugin and service types
may be installed repeatedly under different names.

```rust
use lenso_plugin_core::{AccessError, App, BoxError, Handle, Instance, InstanceKey, Plugin, Setup, WorkGate};
use std::sync::Arc;

struct Prefix(String);
impl Plugin for Prefix {
    type Service = String;
    async fn setup(self, _: &mut Setup) -> Result<Arc<String>, BoxError> {
        Ok(Arc::new(self.0))
    }
}

struct GreetingPlugin { prefix: Handle<String> }
struct Greeting { prefix: Arc<String>, gate: WorkGate }
impl Greeting {
    async fn greet(&self, name: &str) -> Result<String, AccessError> {
        let _work = self.gate.enter()?;
        Ok(format!("{} {name}", self.prefix))
    }
}
impl Plugin for GreetingPlugin {
    type Service = Greeting;
    fn dependencies(&self) -> Vec<InstanceKey> { vec![self.prefix.key()] }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Greeting>, BoxError> {
        Ok(Arc::new(Greeting { prefix: setup.get(&self.prefix)?, gate: setup.work_gate() }))
    }
}

# async fn example() -> Result<(), BoxError> {
let prefix_instance = Instance::new("english", Prefix("Hello".into()));
let prefix = prefix_instance.handle();
let greeting_instance = Instance::new("greeting", GreetingPlugin { prefix });
let greeting = greeting_instance.handle();
let app = App::new().with(greeting_instance).with(prefix_instance);
let running = app.start().await?;
assert_eq!(running.get(&greeting)?.greet("Ada").await?, "Hello Ada");
running.shutdown().await?;
# Ok(())
# }
# tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(example()).unwrap();
```

## Configuration and planning

Configuration is ordinary plugin-owned Rust data. Override `Plugin::validate`
to check it synchronously without acquiring resources. `App::start` checks all
configuration, instance names/identities, bindings, missing/foreign dependencies,
and cycles before invoking **any** setup. Validation errors are collected.
Dependencies are instance keys, not names or service types. Duplicate edges are
harmless; self-dependency is a cycle. `Setup::get` rejects undeclared access.

`Instance::new` owns the plugin and creates a stable handle without validating
or acquiring resources. Instances are moved into `App::with`; they are not
cloneable, so each lifecycle has one owner. Handles can be captured by dependent
plugins before composition, and dependency planning—not insertion order—decides
startup order. `App::install`, `App::reserve`, and `App::bind` remain available
for compatibility and forward declarations. Every reservation must be bound;
foreign handles and duplicate bindings are pre-start errors.

`App::inspect(&self)` exposes the same synchronous preflight used by `start`,
without consuming the builder, requiring Tokio, invoking setup, connecting to
Runtime, or acquiring plugin resources. Plugin `validate` and `dependencies`
hooks remain trusted code and must be free of resource acquisition. `start`
checks again; a snapshot does not freeze plugin interior state or environment.

```rust
use lenso_plugin_core::{App, CheckStatus};
let app = App::new();
let report = app.inspect();
assert!(report.is_valid());
assert_eq!(report.checks[0].status, CheckStatus::Passed);
let json = serde_json::to_value(&report)?;
assert_eq!(json["checks"][1]["status"], "not-run");
# Ok::<(), serde_json::Error>(())
```

Run the public static consumer with
`cargo run --locked -p lenso-plugin-core --example inspect`; its setup panics if
invoked and the process needs no Tokio runtime.

The serializable `Inspection` shape has `schema_version: 1`, `instances`,
`startup_order`, `diagnostics`, and `checks`. Check names/statuses explicitly
separate `preflight` (`passed`/`failed`) from `setup` and `runtime-connection`
(`not-run`) and `session-status` (`unsupported`). No native `dev`, session status,
or remote `call` API is supplied. A host wanting a persistent development session
must own startup, identity, transport, admission, readiness, and shutdown; static
inspection cannot prove any of those runtime properties.

Diagnostics have a stable `snake_case` `code`, optional owning `instance` index,
optional declaration `dependency_index`, closed cycle `path`, and fixed `message`.
Indices resolve against `instances` in insertion order; missing/foreign handles
have `target: null` because Core cannot safely invent their names. Cycle paths
include the repeated first node and are discovered even if another edge is
missing. `startup_order` is empty on any error. Duplicate edges remain visible
and harmless. Consumers should tolerate future diagnostic codes. No key addresses,
plugin error causes, configuration values, or panic payloads enter the report.
Caller-supplied instance names are metadata and must not contain secrets. Host
panic hooks may independently log panic payloads; Core does not replace them.
`StartError::Validation(Vec<String>)` remains compatible for existing consumers;
use `inspect` for structured details.

Optional `Config<T>` resolves a serde `Deserialize` type without introducing a
configuration language. Serde attributes own defaults. Explicit JSON must be an
object; specifically selected environment values are JSON, so strings require
quotes. Selected environment > explicit JSON > serde defaults; absent variables
do not override anything. Nested overrides use object-only JSON pointers.

```rust
use lenso_plugin_core::Config;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Settings { #[serde(default = "default_limit")] limit: usize }
fn default_limit() -> usize { 10 }

let settings = Config::<Settings>::new("worker")
    .explicit_json(json!({"limit": 20}))
    .resolve()?;
assert_eq!(settings.limit, 20);
# Ok::<(), lenso_plugin_core::ConfigError>(())
```

`.env_json("/limit", "WORKER_LIMIT")` selects an environment override. Selection
order resolves overlapping environment paths, with the last selection winning.
Errors contain the instance, a declared field JSON pointer (otherwise redacted),
a fixed reason and redacted provenance, never raw JSON, environment values or
underlying serde error messages. Use `.diagnostic_field("/path")` for useful
trusted paths; `.env_json` implicitly declares its selected path. Unknown map keys
are always redacted. `.sensitive("/credentials")` redacts that subtree's paths.
`resolve_with_sources()` returns the resolved value and non-value field source
metadata; do not debug-print the resolved value when it contains secrets.
`Config` debug output also redacts explicit values. Plugin-owned semantic
validation remains `Plugin::validate`, and should avoid exposing secrets in errors.

## Resource ownership and cleanup

After acquiring each resource, register a closure synchronously, before the next
fallible operation or await:

```rust
# use lenso_plugin_core::{BoxError, CleanupPhase, Setup};
# use std::sync::Arc;
# struct Connection;
# impl Connection { async fn close(&self) -> Result<(), BoxError> { Ok(()) } }
# fn register(setup: &mut Setup, connection: Arc<Connection>) {
let cleanup_connection = connection.clone();
setup.on_cleanup(CleanupPhase::Resources, move || async move {
    cleanup_connection.close().await
});
# }
```

Returned setup errors and unwind panics roll back callbacks from the failing plugin's partial
setup and every previously started instance. Cleanup has two global phases:
seal admission, all `Entrances` callbacks, drain admitted work, then all `Resources`
callbacks. Each phase runs
in reverse dependency/startup order and reverse registration order per instance.
Every callback is awaited; errors and callback panics are collected without
skipping later callbacks. Startup preserves both its original error and rollback
errors.

`on_cleanup` returns a `CleanupHandle`; using it for early disposal is optional. Its `dispose().await` supports
explicit early disposal and joins the same callback completion as rollback and
shutdown. Failed early disposal remains visible in rollback/shutdown; cancelling
a disposer waiter does not cancel its owned cleanup task. Plugins must coordinate
early resource release with their active dependents; early disposal itself does
not seal the application's gate.

`RunningApp` is cloneable. Concurrent and repeated `shutdown` calls share one
completion and aggregate result. Once shutdown begins, cancelling or dropping a
waiter does not cancel cleanup: Tokio owns its task. Keep the Tokio runtime alive
until another waiter confirms completion.

Cleanup is explicit. Dropping handles, services, or an acknowledged `RunningApp`
does not run asynchronous cleanup. `get` rejects access once shutdown seals the
gate. A service's retained `Arc` must use `Setup::work_gate()` at its outer
business entry, keeping its `WorkGuard` through every await and dependency call.
Dependency-local logging/cache methods used within that work must **not** reenter
the gate: admitted work is allowed to finish after sealing.

Startup is owned by a Tokio task. Cancelling a startup waiter lets setup finish;
errors roll back and a successfully started app not acknowledged by its caller
is shut down. Even cancellation after delivery is queued cannot lose cleanup.
This is not a timeout: a blocked plugin setup must finish cooperatively before
cleanup can run. Keep the runtime alive. Unwind panics in validation/dependencies
become validation failures before any setup; setup construction/poll unwinds
preserve the current `Setup` and run rollback. Abort-on-panic, runtime termination
and plugin resources acquired without immediately registering cleanup are outside
these guarantees.
