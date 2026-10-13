# lenso-plugin-core

Native Rust plugin composition, independent of Lenso Runtime, its SDK, and JSON.
Each installed instance has a typed handle. The same plugin and service types
may be installed repeatedly under different names.

```rust
use lenso_plugin_core::{App, BoxError, Handle, Instance, InstanceKey, Plugin, Setup};
use std::sync::Arc;

struct Prefix(String);
impl Plugin for Prefix {
    type Service = String;
    async fn setup(self, _: &mut Setup) -> Result<Arc<String>, BoxError> {
        Ok(Arc::new(self.0))
    }
}

struct GreetingPlugin { prefix: Handle<String> }
struct Greeting { prefix: Arc<String> }
impl Greeting {
    async fn greet(&self, name: &str) -> String {
        format!("{} {name}", self.prefix)
    }
}
impl Plugin for GreetingPlugin {
    type Service = Greeting;
    fn dependencies(&self) -> Vec<InstanceKey> { vec![self.prefix.key()] }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Greeting>, BoxError> {
        Ok(Arc::new(Greeting { prefix: setup.get(&self.prefix)? }))
    }
}

# async fn example() -> Result<(), BoxError> {
let prefix_instance = Instance::new("english", Prefix("Hello".into()));
let prefix = prefix_instance.handle();
let greeting_instance = Instance::new("greeting", GreetingPlugin { prefix });
let greeting = greeting_instance.handle();
let app = App::new().with(greeting_instance).with(prefix_instance);
let running = app.start().await?;
assert_eq!(running.get(&greeting)?.greet("Ada").await, "Hello Ada");
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

Returned setup errors roll back callbacks from the failing plugin's partial
setup and every previously started instance. Cleanup has two global phases:
all `Entrances` callbacks first, then all `Resources` callbacks. Each phase runs
in reverse dependency/startup order and reverse registration order per instance.
Every callback is awaited; errors and callback panics are collected without
skipping later callbacks. Startup preserves both its original error and rollback
errors.

`RunningApp` is cloneable. Concurrent and repeated `shutdown` calls share one
completion and aggregate result. Once shutdown begins, cancelling or dropping a
waiter does not cancel cleanup: Tokio owns its task. Keep the Tokio runtime alive
until another waiter confirms completion.

Cleanup is explicit. Dropping handles, services, or `RunningApp` does not run
asynchronous cleanup. Services may be retained after shutdown; plugins must
define how their ordinary methods behave after their underlying resources close.
Startup is **not cancellation-safe**; do not drop its future once polled.
Panics in plugin validation/setup are not treated as returned setup errors.
An adapter requiring cancellable startup must own startup in a separate task
and explicitly shut down an app that cannot be delivered to its caller.
