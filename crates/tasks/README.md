# Native durable tasks

`lenso-tasks` provides ordinary typed Rust handlers, a local SQLite queue and a
thin Lenso plugin, with an optional PostgreSQL provider described below.
`provision(path, queue)` explicitly creates the schema and
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

## PostgreSQL provider (optional)

Enable `lenso-tasks` feature `postgres`. The existing `Tasks` API, typed handlers,
worker, deduplication, retry budgets and cooperative cancellation are shared with
SQLite. `TasksPlugin::new` remains the SQLite constructor. PostgreSQL does not
import or modify an existing SQLite queue; provision a dedicated PostgreSQL
schema/database explicitly, then bind producers and workers to that queue.

```rust,no_run
# #[cfg(feature = "postgres")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision_postgres, PostgresConfig, RetryPolicy, Task, TasksPlugin};
use serde_json::Value;
let mut config = PostgresConfig::new();
config.host_path("/tmp/my-owned-postgres-socket")
    .user("my_test_user").dbname("postgres")
    .connect_timeout(std::time::Duration::from_secs(3))
    .options("-c statement_timeout=5000");
provision_postgres(config.clone(), "reports").await?;
let echo = Task::new("echo", RetryPolicy::default(), |v: Value, _| async { Ok(v) });
let plugin = TasksPlugin::postgres(config, "reports", vec![echo]);
let app = App::new().with(Instance::new("reports", plugin)).start().await?;
app.shutdown().await?;
# Ok(()) }
```

- Requires PostgreSQL 13+ SQL facilities (`gen_random_uuid`, `SKIP LOCKED`); tested
  locally with PostgreSQL 18.6. This implementation uses `NoTls`: use a Unix
  socket or an independently secured local tunnel. TLS connector integration,
  pooling, automatic reconnect and network failover are not implemented.
- One synchronous connection per plugin, acquired only at startup. All database
  operations and close run on Tokio's blocking pool. Independent plugin instances
  or processes provide concurrent workers. Set connection and statement timeouts
  in `PostgresConfig`; reopen the plugin after a broken connection. No implicit
  schema migration, worker loop, cron timer or schema repair occurs at startup.
- Explicit provisioning serializes its DDL with a database advisory transaction
  lock. Queue identities are stable across repeated provisioning and restarts.
  Use a dedicated schema/search path consistently across all producers/workers.
- Claims use row locks with `SKIP LOCKED`. Each invocation recovers at most 100
  expired attempts, then claims one eligible registered task. Continue invoking
  `run_one` to recover larger abandoned backlogs. Locked rows are retried later;
  strict FIFO fairness is not promised.
- Writes use the database server's wall clock. Heartbeat and settlement obtain
  the row lock *before* checking the current statement time, token and deadline;
  time spent waiting for a lock cannot revive an expired attempt. Transactions
  explicitly use READ COMMITTED. Wall-clock changes can still affect timing.
- A lost connection during commit can leave acceptance/settlement uncertain.
  Retrying enqueue with the same queue/key and matching task/input returns the
  retained job; `lookup` can inspect it. External business effects still require
  their own idempotency, and delivery remains at-least-once.

Real PostgreSQL checks are opt-in and create/destroy their own temporary,
socket-only clusters under `/tmp`. They do not read a DSN, `.env` or user DB.
Install PostgreSQL `initdb`, `pg_ctl` and server binaries on `PATH`, then run:

```sh
cargo test --locked -p lenso-tasks --features postgres --test postgres postgres_contract -- --ignored --nocapture
cargo test --locked -p lenso-tasks --features postgres --lib postgres_lock_and_expiry_contract -- --ignored --nocapture
```

The first exercises concurrent provisioning, 16 racing duplicate producers, four
workers claiming 25 jobs, retained acceptance, queue isolation, delayed retries,
restart, cancellation/drain, stale fencing and an OS-killed worker after an
idempotent fake business effect. The second verifies that heartbeat/settlement
blocked on a row lock cannot revive an expired claim, a locked queue head is
skipped, hard deadlines fence writes, and exhausted/cancelled claims stay final.
Ordinary workspace tests compile these fixtures but do not start PostgreSQL.
