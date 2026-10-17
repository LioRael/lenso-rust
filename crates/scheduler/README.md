# Native durable scheduler

`lenso-scheduler` stores cron schedules, scan cursors and an outbox in its own
SQLite tables. It submits ordinary jobs through the existing `Tasks::enqueue`
API. It does not execute handlers, change the worker or require a TaskStore API
extension. The scheduler database can be separate from the Tasks database.

## Public lifecycle

1. Explicitly provision the Tasks queue and obtain its stable `Tasks::identity()`.
2. Call `lenso_scheduler::provision(path, name, queue_identity)` as a provisioning
   step. Reprovisioning is idempotent but rejects a different queue identity.
3. Construct `SchedulerPlugin::new(path, name, tasks_handle)`, using the exact
   `Handle<Tasks>` from the selected instance. Startup checks the binding and
   existing tables; it never creates a file/schema or starts timers.
4. Call `Scheduler::create(Schedule { ... })`. The key is scoped to this scheduler.
   Repeating an identical definition returns the same schedule; a changed
   definition returns `ScheduleConflict`. JSON object ordering is irrelevant.
5. The trusted host supplies UTC Unix milliseconds to `tick(now, TickOptions)`.
   It owns polling, concurrency, backoff, clock accuracy and stopping polling
   before shutdown. The Tasks worker is driven separately with `run_one()`.
6. Await `app.shutdown()` with Tokio alive, preserving operation and cleanup
   errors. Retained scheduler references reject subsequent database operations.

The compiling [local example](examples/local.rs) provisions two new local files,
submits one fake job and runs its in-memory echo handler. It refuses an existing
destination directory:

```sh
cargo run --locked -p lenso-scheduler --example local -- /tmp/new-scheduler-demo
```

## Calendar and missed occurrences

- Cron has exactly **five** fields: minute, hour, day of month, month, day of week.
  Croner 4.0.1 handles ranges, lists, steps, names and its five-field extensions.
  Sunday is 0 or 7. Restricted day-of-month and day-of-week fields use OR by
  default (`+MON` explicitly selects AND). Seconds/year fields and `@` macros
  are rejected. Resolution is one minute; unsupported input returns an error.
- `timezone` must be an IANA name recognized by pinned `chrono-tz` 0.10.4,
  including `UTC`. The bundled database is **2025b**, exposed as
  `TIMEZONE_DATABASE_VERSION`; the OS timezone is never consulted.
- Supported start/tick instants are 2000-01-01 UTC inclusive through 2100-01-01
  UTC exclusive. This excludes historical sub-minute UTC offsets. `start_after`
  is exclusive: a plan starts at the next whole UTC minute, even when created
  exactly on a minute boundary.
- The engine scans UTC minutes and matches their local calendar fields. A
  spring-forward local time that does not exist is skipped. An autumn fold
  matches **both** UTC instants; they are two distinct occurrences. Half-hour
  folds and quarter-hour timezone offsets use the same rule. No local timestamp
  is used as a deduplication key.
- `MissedPolicy::CatchUp` preserves every unexamined matching occurrence since
  `start_after`, with backlog spread across bounded ticks and restarts.
- `MissedPolicy::Skip { grace_ms }` discards unexamined instants strictly before
  `now - grace_ms`; the exact boundary remains eligible. For example, at
  00:03:00 with 60 seconds of grace, 00:02 and 00:03 are eligible. At 00:03:00.001,
  00:02 is too late. Zero grace requires an exact minute instant. The skip is
  durable cursor progress, not a row for every discarded occurrence.
- Once committed to the outbox, an occurrence remains deliverable regardless
  of later lateness or a policy's grace window. A backward clock does not rewind
  cursors or re-create old occurrences. Tasks still enforces its `run_at` time.
  A wrong forward host clock can advance a skip cursor irreversibly.
- A timezone database upgrade may change unscanned occurrences. Keep the pinned
  version consistent across processes; already committed UTC occurrences are
  unchanged. No automatic tzdata updates are performed.

## Atomic handoff and restart

An immediate SQLite transaction reads due plans, inserts occurrences and advances
cursors. `(schedule_id, fire_at)` is unique. Other processes opening the same
scheduler see those committed cursors. Submission happens **after** commit,
through `Tasks::enqueue` with `run_at = fire_at` and a reserved key:

`lenso:scheduler:v1:<persisted scheduler identity>:<schedule id>:<UTC fire_at>`

The immutable definition supplies the original task and input on every retry.
After enqueue succeeds, a separate SQLite update records its returned job ID.
A crash before the first commit rolls back the cursor and outbox together. A
crash after that commit leaves pending work. A crash after enqueue but before
acknowledgement reuses the exact key/input, and Tasks returns the original job ID,
even after that job has completed. A concurrent failed submission cannot erase
another process's acknowledgement. A conflicting queue record is reported as a
failure and never silently treated as an accepted occurrence.

**This deduplicates retained queue acceptance, not external effects.** The Tasks
worker remains at-least-once. Handlers must use business transactions or external
idempotency keys for their effects; `TaskContext::job_id` is stable across retries.
Keep scheduler identity, outbox and Tasks deduplication records durable. Deleting
or independently restoring these records is outside the guarantee. Do not use
the reserved scheduler key prefix for other producers.

## Bounds, failures and admission

`TickOptions` defaults to 10 due schedules, 60 scanned minutes per schedule and
100 submissions. Hard maxima are 100, 1440 and 100 respectively. Due plans are
selected by oldest cursor, then ID. This is a bounded scan rather than an
unbounded search for the next cron match; impossible calendar combinations do
not hang a tick. A long `CatchUp` backlog requires repeated invocations. Storage
and database lock waits still contribute to latency (SQLite busy timeout: 5s).

`TickReport.more` is a final snapshot of outstanding scanning or pending delivery.
`staged` counts this tick's new outbox rows; `delivered` counts successful handoff
attempts, including deduplicated retries. Concurrent reports must not be added as
unique job counts. `get(key)` returns the durable scan cursor, and
`occurrences(schedule_id, after_fire_at, limit)` returns a bounded history page
with UTC instant, job ID and safe error classification, excluding task input.

Unknown task, invalid typed input, deduplication conflict and queue unavailability
remain pending with a fixed error classification. Task-specific input validation
happens at enqueue, because Tasks has no public validation-only interface.
Failures do not stop the other deliveries in that tick; the next page prefers
fewer prior attempts so one bad row does not block healthy rows in a fixed backlog.
Hosts should inspect failures and back off, not spin on `more = true`. Definitions
are immutable in this version; there is no edit, pause/delete or automatic pruning
API. Rows are retained, and a permanently invalid definition requires operator
intervention rather than silently losing its occurrences.

Each SQLite phase enters WorkGate once and transfers its guard into blocking
work. The enqueue phase uses Tasks' existing admission. The multi-phase tick is
not one atomically admitted operation: shutdown may seal the next phase, and an
error or dropped waiter can follow committed progress. In-flight database work
retains its guard until completion; unacknowledged outbox work recovers on a later
start. No outer guard is held while reentering Tasks' shared gate. This preserves
safe drain without adding an internal enqueue bypass to the public Tasks API.

These are trusted native APIs, not tenant authorization boundaries. The host must
validate ownership/authority before exposing schedule mutations or reads. SQLite
is the verified scheduler backend. PG Tasks compatibility depends on the same
queue identity and retained enqueue-deduplication contract; this crate does not
implement or claim a PostgreSQL scheduler store.

## PostgreSQL Tasks integration check

The scheduler can bind to `TasksPlugin::postgres` with the same exact
`Handle<Tasks>`; its own schedule/outbox store remains SQLite. The opt-in
`postgres-tests` feature enables the Tasks provider for the cross-provider test:

```sh
cargo test --locked -p lenso-scheduler --features postgres-tests --test postgres -- --ignored --nocapture
```

This test creates and destroys its own temporary socket-only PostgreSQL cluster
(`initdb`/`pg_ctl` on PATH), plus a separate temporary SQLite scheduler. It injects
failure after PG acceptance but before SQLite acknowledgement, reopens two
competing schedulers, verifies the retained original job ID and three distinct
UTC occurrences, then executes all jobs with two PG workers. No configured user
DSN, external service or production database is used.
