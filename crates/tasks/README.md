# Native durable tasks

`lenso-tasks` provides ordinary typed Rust handlers, a local SQLite queue and a
thin Lenso plugin. `provision(path, queue)` explicitly creates the schema and
stable queue identity. Plugin startup only opens an existing provisioned queue;
no request creates a schema or connection. SQLite work runs on Tokio's blocking
pool. The plugin owns its connection and closes it after admitted work drains.

```rust,no_run
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, EnqueueOptions, RetryPolicy, Task, TasksPlugin};
use serde_json::{json, Value};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// Explicit provisioning step, normally a separate command.
provision("local-tasks.sqlite", "reports").await?;
let report = Task::new("report", RetryPolicy { max_attempts: 3, ..Default::default() },
    |input: Value, context| async move {
        if context.cancellation.reason().is_some() {
            return Err(lenso_tasks::Failure::Aborted);
        }
        // Call an ordinary service. Use its stable business key for any effects.
        Ok(json!({"job": context.job_id, "input": input}))
    });
let instance = Instance::new("reports", TasksPlugin::new("local-tasks.sqlite", "reports", vec![report]));
let handle = instance.handle();
let app = App::new().with(instance).start().await?;
let queue = app.get(&handle)?;
let result = async {
    let id = queue.enqueue("report", json!({"reportId": "example-1"}), EnqueueOptions {
        deduplication_key: Some("example-1".into()), ..Default::default()
    }).await?;
    queue.run_one().await?; // finite, awaited host invocation; no implicit worker
    queue.get(id).await
}.await;
let shutdown = app.shutdown().await;
// Preserve both errors in application diagnostics if both operations fail.
println!("operation: {result:?}; shutdown: {shutdown:?}");
# Ok(()) }
```

A producer only calls `enqueue`. A worker explicitly awaits `run_one()`;
independent processes can open the same file/queue. The host owns polling,
concurrency and signal handling. During shutdown the Core WorkGate rejects new
calls, waits for accepted operations/handlers, then releases the connection.
Dropping a `run_one` waiter does not abandon accepted work. A handler ignoring
cooperative cancellation can delay shutdown indefinitely. No forced abort API.

## Durable semantics

- `run_at` is an absolute Unix millisecond instant. Pending jobs survive restart
  and cannot be claimed early. This is one-time task scheduling; recurring cron,
  timezones, schedule editing and a Scheduler outbox are not implemented.
- Claims and expired-claim recovery share an immediate SQLite transaction. Each
  attempt has a random token, an expiring renewable lease and a hard lifetime.
  Heartbeats and acknowledgement check live ownership. Old attempts cannot
  overwrite recovered attempts. SQLite wall-clock jumps can affect timing.
- Failure stores a fixed `last_error`, cumulative `attempt`, attempt budget and
  next `run_at`. Pending retries retain their latest failure. Backoff is capped
  by `max_delay_ms`. Expired claims consume their already-started attempt and
  recover as aborted, pending/failed/cancelled according to budget and intent.
- `retry(id)` only reopens a final failed job and adds one attempt, preserving
  history. The trusted application must decide whether business replay is safe.
- `cancel(id)` returns `Cancelled` for pending work, `Requested` for running work,
  `Terminal` for completed work, or `Missing`. Running state stays visible until
  the handler settles (or its abandoned lease is recovered). Cancellation is
  durable, polled across connections and wins over a handler's successful result.
- Queue + deduplication key identifies retained acceptance. Matching task/input
  returns the original ID even after completion; different input/task conflicts.
  JSON object key order is irrelevant. `lookup(key)` is read-only, supporting a
  future scheduler handoff retry without a second enqueue. Rows are not pruned.
- Delivery is **at-least-once**. Queue fencing does not fence external effects.
  A process may die after an effect and before acknowledgement; use a business
  unique key/transaction or the external provider's idempotency facility.

Task input is deserialized at enqueue and execution. Payload/result limits are
64 KiB and depth 32. `get`, `lookup` and bounded `list(task, after_id, limit)` omit
input; results are application-defined projections, never raw handler errors.
These are trusted native APIs, not authorization boundaries. The application
must check current ownership and authority before exposing reads, retry or
cancel. Do not register raw queue operations as untrusted HTTP/MCP tools.

Run the fake-only consumer with an explicit new test path:
`cargo run --locked -p lenso-tasks --example local -- /tmp/my-task-demo.sqlite`.
It refuses an existing file. No network or paid services are used.
