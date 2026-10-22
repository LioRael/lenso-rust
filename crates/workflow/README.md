# Native durable workflow

`WorkflowPlugin::new(Handle<Tasks>, Definition)` borrows the exact Tasks instance.
The engine contains no file processing, approval policy, inventory logic or
business result table. Consumers define business schemas, Tasks and transaction
callbacks; the engine persists only `lenso_workflow_run_v1` checkpoints.

Public composition API:

- `Definition::new(name, version, Vec<Step>)`: 1–16 named/versioned steps.
- `Step::task(name, version, registered_task_name, next)`: run a read-only/pure
  Tasks handler; its output becomes the next payload.
- `Step::signal(name, version, yes, no)`: wait for an explicitly addressed signal.
- `Step::transaction(name, version, next, callback)`: execute business SQL using
  `SqliteTaskTransaction` and `StepContext`, then commit the returned payload and
  checkpoint together. Context includes workflow/step identity, payload, signal
  receipts and a stable idempotency key suitable for a business receipt column.
- `Target::{Step(index), Complete, Reject}`: explicit dependencies and branches.
  Edges must point forward; cycles, duplicate names and out-of-range targets fail
  validation before startup. No DSL, dynamic graph or second scheduler exists.

Call `provision()` explicitly before accepting work. `start(id, input)` persists
acceptance and enqueues the first task atomically if applicable. IDs are scoped to
queue identity + definition name. Changed input conflicts. Initial input, step
payload and each signal data value are bounded to 16 KiB and 32 levels.

The host drives existing Tasks workers and calls `resume(id)` to reconcile one
Tasks receipt or execute one transaction step. Successful steps are recorded with
versions/job IDs; they do not rerun after restart. Tasks owns attempts, leases,
retry timing and drain. `retry(id)` only retries failed Tasks; a transaction error
leaves the checkpoint at that step, so the host can explicitly resume it again.

`signal(id, flow_version, Signal { step, version, key, choice, data })` stores an
immutable per-step receipt and selects a branch. Premature/out-of-order signals
return `NotReady` without buffering. Exact duplicates return `Duplicate`; changed
keys/decisions/data conflict. Approval is not completion: the following transaction
step remains independently resumable. `checkpoint` and `result` expose receipts.
Cancellation persists first, then requests Tasks cancellation; repeating it repairs
a crash between those operations. A concurrent retry can leave a harmless read
queued, but a cancelled checkpoint cannot execute a business transaction.

Versions **and topology** are persisted and compared before mutation. Hosts must
bump step versions when changing callback semantics. No automatic migration is
provided. This revision replaces the earlier file-specific candidate; it does not
migrate that unpublished prototype's `lenso_workflow` table.

SQLite only: public Tasks has no corresponding PostgreSQL transaction contract.
Callbacks must perform only same-database SQL, never external side effects or
nested Tasks calls. Tasks used here must be read-only/pure. No exactly-once external
execution or universal compensation is claimed. An external-effect extension
would need durable intent, explicit idempotency keys, receipt queries and an
`Unknown` state. `TaskError::CommitUnconfirmed` remains unaltered and is never
replayed automatically; inspect the original checkpoint/receipt after reconciliation.
A missing snapshot does not prove that an outstanding commit cannot finish.

The host authenticates/authorizes every caller. Each storage operation uses Tasks
admission; multi-call operations may stop with `Closed` during shutdown and can
resume from durable state. Await app shutdown while Tokio remains alive.

Two independently composed consumers (business logic is entirely in `examples/`):

```sh
# Three separate processes; prepare requires a NEW disposable directory.
cargo run --offline --locked -p lenso-workflow --example file_review -- prepare /tmp/new-workflow-fixture
cargo run --offline --locked -p lenso-workflow --example file_review -- approve /tmp/new-workflow-fixture
cargo run --offline --locked -p lenso-workflow --example file_review -- approve /tmp/new-workflow-fixture
# Shipment counting -> dock signal -> stock increment OR quarantine receipt.
cargo run --offline --locked -p lenso-workflow --example stock_receipt
```

The file consumer returns the same result on repeated confirmation. The stock
consumer checks that duplicate signals and a rejected shipment leave inventory at
7 units. It uses a fresh temporary SQLite database, no external service.

Focused checks: `cargo test --offline --locked -p lenso-workflow` and
`cargo clippy --offline --locked -p lenso-workflow --all-targets -- -D warnings`.
