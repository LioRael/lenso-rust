# lenso-log

Native structured logging built on `tracing`, usable without Runtime or an app.
It implements the semantic subset of TypeScript `@lenso/log`: levels, child
bindings, public error classification, sensitive-field redaction, trace
association and borrowed logger ownership. It does not reproduce Pino options,
custom serializers/transports, pretty output, or install an OTel exporter.

Run `cargo run -p lenso-log --example log-native` or `--example injected` for complete
programs. Both produce JSON on stderr and leave stdout empty.

## Native function

```rust
use lenso_log::{Logger, LoggerOptions};
use serde_json::json;

fn greet(log: &Logger, name: &str) -> Result<String, lenso_log::LogError> {
    log.info(json!({"event": "greet", "name": name}), "greeting")?;
    Ok(format!("Hello, {name}!"))
}

let log = Logger::new("greeting", "local", LoggerOptions::default());
assert_eq!(greet(&log, "Ada")?, "Hello, Ada!");
# Ok::<(), lenso_log::LogError>(())
```

Default output is synchronous JSON on **stderr**, not protocol stdout. Nested
objects, arrays and scalar types remain JSON, not debug text. A host-provided
`tracing::Dispatch` can be borrowed; it is never globally installed, flushed,
or shut down. Tracing events carry a `message` and a serialized JSON `fields`
record. Hosts can use `StructuredJson` with their `fmt` subscriber to decode it,
or consume that record in their own visitor. Host filtering still applies.
Recursive logging from a subscriber or writer on the same thread is suppressed,
including calls through another Logger, and returns success without emitting.
Concurrent writes on different threads may enter the subscriber concurrently.
The formatter includes `spans` with subscriber-local numeric IDs when an explicit
scope is active. These are tracing span IDs, not distributed OTel span IDs.

## Injected plugin

```rust
use lenso_log::{LoggerPlugin, LoggerOptions};
use lenso_plugin_core::App;
use serde_json::json;

# async fn example() -> Result<(), lenso_plugin_core::BoxError> {
let mut app = App::new();
let handle = app.install("application-log", LoggerPlugin::new("greeting", LoggerOptions::default()));
let running = app.start().await?;
let log = running.get(&handle)?;
log.info(json!({"event": "started"}), "ready")?;
running.shutdown().await?;
assert!(log.info(json!({}), "too late").is_err());
# Ok(())
# }
```

Ordinary plugins declare the logger handle in `dependencies`, then use
`setup.get(&handle)` to inject `Arc<Logger>` into their service. Logger identity
includes the plugin ID passed to `LoggerPlugin` and Core's exact instance name.
The logger closes in the Resources phase, after dependent cleanup in reverse
dependency order. Writes do **not** re-enter the global work gate. New top-level
work must be admitted by the owning service; already-admitted dependency work
and cleanup may continue logging until the logger itself closes.

## Async association

```rust
use lenso_log::{Logger, LoggerOptions, TraceContext};
use serde_json::json;

# async fn example() -> Result<(), lenso_log::LogError> {
let log = Logger::new("greeting", "local", LoggerOptions::default());
let trace = TraceContext::new("0123456789abcdef0123456789abcdef", "0123456789abcdef")?;
log.scope("greet", Some(trace), async {
    tokio::task::yield_now().await;
    log.info(json!({"traceId": "ignored"}), "associated")
}).await?;
log.close().await?;
# Ok(())
# }
```

Scopes enter spans per poll and restore them on suspension, cancellation and
completion. Trace IDs are validated nonzero hexadecimal identifiers and override
user bindings at write time. IDs cannot be forged by fields outside a scope.
Children share scope; unrelated loggers do not. Nested scopes restore their
outer context. Spawned tasks do not implicitly inherit context: wrap each spawned
future explicitly with `scope`. Scope operation names are public text.

## Privacy and close boundaries

Redaction applies recursively to objects and arrays, including child bindings.
Keys are compared case-insensitively with punctuation ignored. Sensitive keys:
authorization, cookie, password, token, secret, session, credential(s),
signature, connectionString, sessionCredential, sessionToken, signedUrl,
databaseUrl, body and payload. Their entire values become `[REDACTED]`.
`err` and `error` fields become `{"type":"Error"}` at any depth.
Native errors go through `log_error`; no Display, Debug, source, raw code or
stack is accessed. `LoggerOptions::classify_error` may return **only approved
public code/phase**, and classifier panic omits classification.

This is field redaction, not secret detection. Messages, operation names,
plugin/instance names, public classification labels and values under other keys
must already be safe. Rust's process panic hook still runs when a classifier or
flush callback panics; this crate does not replace that global hook.

`close().await` atomically rejects writes on every child, waits for synchronous
in-flight writes, and runs an explicit owned flush callback at most once. Repeated
or concurrent close calls return the same result. Dropping a close waiter does
not cancel the owned flush. A subscriber must not synchronously wait for this logger's close:
close drains the subscriber's own active write.
`owned_with_flush` accepts a cooperative async callback and deadline:
failure, panic and timeout become fixed public `LogError`
variants. On timeout its task is aborted; blocking callback code cannot be
preempted. Borrowed constructors have no flush callback by design.
Logger does not automatically close when a clone is dropped. Use explicit close
for standalone ownership or Resource cleanup via `LoggerPlugin`.
