# lenso-otel

Explicit native OpenTelemetry adapters for HTTP → operation → database → queued
Tasks, reusing `lenso-log` for structured logs. No global subscriber or global
provider is installed. Only this crate and workspace registration are changed.

`Telemetry::instrument(Boundary, &Context, &Logger, future)` scopes each poll with
an explicit parent and supplies matching trace/span IDs to the existing logger.
The outer `Result` reports a closed telemetry wrapper; the inner result preserves
the business result. Export failures never rewrite a completed business result.
Fixed boundary names and error classifications prevent raw URLs, SQL statements,
payloads and error strings from entering spans. Logger's existing recursive
redaction still applies. Applications must keep log messages/public fields safe.

Use `extract` on a lowercase HTTP header map, `Context::current()` inside a scope,
and `inject` into an **application-owned** task envelope. Only bounded W3C
traceparent/tracestate are accepted; no baggage is extracted. Trace context is
untrusted correlation, never authentication. Hosts may choose `Context::new()`
instead of trusting a remote parent. A task handler calls `extract` explicitly;
spawned futures do not inherit scopes. DB calls use the Database boundary around
ordinary repository futures. There is no SQL or Tasks monkeypatching.

```rust,ignore
telemetry.instrument(Boundary::Http, &extract(&headers), &logger, async {
    telemetry.instrument(Boundary::Operation, &Context::current(), &logger, async {
        // Call repository under Boundary::Database, then enqueue a payload with
        // inject(&Context::current()). Restore that carrier in the task handler.
        Ok::<_, BusinessError>(())
    }).await?
}).await?
```

`Telemetry::collector(endpoint, timeout)` builds the official OTLP HTTP/protobuf
trace exporter with queue capacity 1024 and batch size 128. It is intended for an
explicit trusted local HTTP collector. This build does not enable TLS. For host
transport/auth/TLS policy, construct an official exporter and pass it to
`Telemetry::batched`, or supply a configured SDK provider. Nothing here hand-writes
OTLP. Never construct a collector on a request path. The SDK may use its standard
OTEL environment configuration; the host owns that configuration.

`owned_with_metrics` accepts a host-created official metric provider and records
`lenso.operations` and `lenso.duration` (seconds), labelled only by four boundaries
and two outcomes. Counts/durations cover completed futures; cancelled scopes end
their span but are not counted as success or failure. Configure its official
metric exporter/reader at startup. The collector convenience constructor exports
traces only.

Install `TelemetryPlugin(telemetry)` to register Resource-phase cleanup. Core
first seals admission and drains accepted work. HTTP/worker owners retain their
WorkGuard through all work; the adapter does not admit again. Exact plugin handles
can be declared/resolved by business plugins through existing Core APIs. Register
telemetry before dependent services so it is released after them. Standalone users
must drain their work before `close()`. `borrowed` never shuts down the host
provider; `owned` and `owned_with_metrics` explicitly own shutdown. Cancelled close
waiters do not abandon cleanup. SDK shutdown flushes and failures return a fixed
safe error. SDK/custom processors must honor their timeout contract; a blocking
third-party implementation cannot be forcibly killed. Trace and metric shutdown
receive the timeout separately. Keep Tokio alive until shutdown completes.

## Executable consumer and checks

```sh
cargo run --locked -p lenso-otel --example traced_service
cargo test --locked -p lenso-otel --all-targets
cargo clippy --locked -p lenso-otel --all-targets --all-features -- -D warnings
```

The example uses an explicit HTTP boundary fixture, an in-memory SQLite business
table and the existing Tasks plugin with an exclusively created temporary queue.
It verifies four correlated spans and prints a redacted structured task log. It
opens no listener and contacts no telemetry service. The OTLP test substitutes an
in-memory HTTP client under the **official** exporter and decodes the resulting
request with official protobuf types. This proves encoding and shutdown flush,
not network collector interoperability, TLS, retries, or production throughput.
