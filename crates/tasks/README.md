# Native durable tasks

`lenso-tasks` provides ordinary typed Rust handlers, a local SQLite queue and a
thin Lenso plugin, with an optional PostgreSQL provider described below.
`provision(path, queue)` explicitly creates the schema and
stable queue identity. Plugin startup only opens an existing provisioned queue;
no request creates a schema. PostgreSQL can replace broken connections in its
startup-owned pool. SQLite work runs on Tokio's blocking
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
  locally with PostgreSQL 18.6. Existing constructors retain `NoTls` for Unix
  sockets or independently secured local tunnels. `PostgresOptions::with_tls`
  accepts a cloneable, thread-safe `postgres::tls::MakeTlsConnect` connector;
  certificate roots, client certificates and hostname verification belong to that
  connector. Set `PostgresConfig::ssl_mode(Require)` to prevent plaintext fallback.
- Each plugin eagerly opens a fixed pool at startup (default 4 connections,
  configurable 1–64). All database operations, reconnect and close run on Tokio's
  blocking pool. Different slots allow concurrent operations on one `Tasks`
  instance. A slot stays exclusive through the complete transaction. Pool checkout
  waits at most 5 seconds by default, returning `TaskError::PostgresPoolTimeout`.
  It does not bound connection/SQL time or the number of callers waiting in Tokio.
- `PostgresOptions::max_connections` and `checkout_timeout` configure these bounds.
  Missing connection timeout defaults to 5 seconds (driver timeout per host, not
  a total failover budget). Set session `statement_timeout` in `PostgresConfig`
  according to application needs; no new SQL timeout is imposed. Keep pool and
  SQL contention below the worker lease/heartbeat budget.
- Checkout probes a reused connection with one empty-query round trip. A failed
  probe or known closed session is replaced once before caller SQL starts. A
  failed reconnect returns an error; a later invocation can try again. Replacement
  checks the provisioned schema and original queue identity; a changed identity
  returns `TaskError::QueueIdentityChanged`. No implicit schema repair, migration,
  background reconnection loop, custom network failover or transaction replay.
- An error from caller SQL is returned unchanged, and that session is discarded.
  No read, write or transaction is automatically replayed, including uncertain
  commit outcomes. Other pool slots remain usable. After admitted work drains,
  shutdown closes all idle sessions, including already disconnected ones.
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

TLS example (add compatible `postgres-native-tls` and `native-tls` dependencies
in the consuming application; the provider does not mandate a TLS backend):

```rust,no_run
# #[cfg(feature = "postgres")]
# async fn tls_example() -> Result<(), Box<dyn std::error::Error>> {
use lenso_tasks::{provision_postgres_with_options, PostgresConfig, PostgresOptions, TasksPlugin};
use postgres::config::SslMode;
use postgres_native_tls::MakeTlsConnector;
let mut config = PostgresConfig::new();
config.host("db.example.invalid").user("tasks").dbname("tasks")
    .ssl_mode(SslMode::Require).options("-c statement_timeout=5000");
// Default connector verifies the server certificate and hostname. For a private
// CA, add its certificate to the builder without disabling verification.
let tls = MakeTlsConnector::new(native_tls::TlsConnector::builder().build()?);
let options = PostgresOptions::with_tls(config, tls)
    .max_connections(4).checkout_timeout(std::time::Duration::from_secs(2));
provision_postgres_with_options(options.clone(), "reports").await?;
let plugin = TasksPlugin::postgres_with_options(options, "reports", vec![/* registered tasks */]);
# let _ = plugin;
# Ok(()) }
```

Real PostgreSQL checks are opt-in and create/destroy their own temporary clusters
under `/tmp`. They do not read a DSN, `.env` or user DB. Pool/recovery tests use
Unix sockets; TLS tests use a temporary certificate and an ephemeral IPv4 loopback
port. Install PostgreSQL `initdb`, `pg_ctl`, server binaries and `openssl` on
`PATH`, then run:

```sh
cargo test --locked -p lenso-tasks --features postgres --test postgres postgres_contract -- --ignored --nocapture
cargo test --locked -p lenso-tasks --features postgres --lib postgres_lock_and_expiry_contract -- --ignored --nocapture
cargo test --locked -p lenso-tasks --features postgres --lib postgres_pool_recovery_contract -- --ignored --nocapture
cargo test --locked -p lenso-tasks --features postgres --test postgres_tls -- --ignored --nocapture
```

The existing contracts exercise concurrent provisioning, deduplication, workers,
queue isolation, retry, cancellation/drain, killed-process recovery and row-lock
expiry fencing. The pool contract verifies reuse and capacity, checkout timeout,
concurrent producers/claims, an available read during a blocked write, a killed
in-flight transaction without replay, reconnect/lease fencing, full server outage
and restart, queue identity mismatch, and session cleanup. TLS checks require
verified encryption, reject untrusted certificates and hostname mismatch, and
exercise reconnect and shutdown through the public plugin API. Ordinary workspace
tests compile these fixtures but do not start PostgreSQL.

## Explicit owned worker

For an owned polling lifetime, install `WorkerPlugin::new(tasks_handle,
WorkerConfig { concurrency: 4, idle_wait: Duration::from_millis(250) })` as
another `Instance` in the same App. It depends on that exact Tasks handle and
reuses `run_one`'s admitted execution path. No worker starts unless installed.
The concurrency limit (1–64) bounds this worker's simultaneous accepted runs;
other workers and manual `run_one` callers have independent limits. Each empty
claim waits for `idle_wait` (nonzero), so delayed jobs and other processes are
observed by polling. No local queue or enqueue notification channel is added.

Retain the worker handle before startup and obtain `Arc<Worker>` via `app.get`.
`worker.stop()` synchronously closes its claim admission and wakes idle lanes.
`worker.join().await` drains accepted runs, is repeatable and survives dropped
waiters. Stop does not cancel handlers or producers; pending jobs remain durable.
A provider/runtime error stops further worker admission, joins other lanes and
is returned by join and Core cleanup; handler failures retain ordinary task retry
semantics. Core shutdown stops worker admission in Entrances, stops all other entrances,
then drains WorkGate. Resources cleanup joins the worker before closing Tasks. Await shutdown while Tokio is
alive. A noncooperative handler can delay both join and shutdown indefinitely.

Runnable public consumer (new local file only, no external services):
`cargo run --locked -p lenso-tasks --example owned -- /tmp/my-owned-worker.sqlite`.

## SQLite business write and task acceptance

`Tasks::sqlite_transaction` borrows the existing Tasks-owned SQLite connection
exclusively and starts one IMMEDIATE transaction on Tokio's blocking pool. Its
synchronous callback receives `&mut SqliteTaskTransaction<'_>` with parameterized
`execute`, `query_row` and typed `enqueue`. The enqueue path shares task/input,
options, deduplication and retry validation and the existing queue INSERT; it
does not open another connection or start an inner transaction. Only the outer
successful return confirms the staged IDs. PostgreSQL returns `SqliteRequired`;
no generic transaction token or cross-provider atomicity is supplied.

Provision business tables explicitly before startup, in the same SQLite file.
The connection belongs to Tasks from startup through Core drain; business code
only borrows the scoped interface. No raw connection, transaction or statement
escapes. SQLite's authorizer rejects transaction/savepoint commands, ATTACH,
DETACH and PRAGMA within the callback. SQL remains trusted application code,
including responsibility for business authorization and schema integrity.
Do not call Tasks recursively, block on asynchronous Tasks methods or perform
external effects inside the callback. Unrelated clients and databases cannot
participate in this transaction.

Any SQL/enqueue error poisons the scope, rejects later statements and prevents
commit even if caught by the callback. Callback errors and unwind panics roll
back; rollback errors preserve the original error. COMMIT errors return
`CommitUnconfirmed`, never automatically replay. Read the original business key
before deciding whether a retry is safe. A dropped async waiter still leaves the
admitted transaction running; Core drains it before closing the connection.
This does not promise external effect idempotency or process survival.

The runnable `report_transaction` consumer uses `examples/support/reports.rs`:
`Reports::submit(owner, report_id, title)` checks the existing owner and immutable
request, inserts a report request, enqueues its task and writes the job receipt
in one transaction. Same-key retries return the original ID; a different owner
or title, or another queue identity, returns no receipt and does not enqueue. `receipt` reads the original
report key with an owner predicate. The owner must already be authenticated by
the application. A real business row/mapping failure rolls back task acceptance,
so no orphan-job compensation is required for this same-file write. Report
execution remains at-least-once; the sample renderer only uppercases a heading.
This is a local native business slice, not integration with a deployed report app.

`cargo run --locked -p lenso-tasks --example report_transaction -- /tmp/new-report.sqlite`
reserves a new file, provisions both schemas, submits a report through the
service, repeats submission, verifies owner isolation and executes the task.
It uses no user database, server, model or paid service.
