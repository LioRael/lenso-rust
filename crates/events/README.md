# Events and local Outbox

`EventsPlugin(EventConfig)` exports `Events`; `OutboxPlugin { root, capacity,
max_event_bytes }` exports `LocalOutbox`. Install each using `Instance::new`, keep
its exact handle, and obtain the service from `RunningApp::get`. They can also be
used directly without the container. See `tests/events.rs` for a compiling public
API consumer and shutdown integration.

`Events::subscribe()` returns an owned subscription. Drop unregisters it;
`recv()` is cancellation safe. Publish reserves all current subscribers' bounded
queues before sending, returning `Full` without partial fan-out when any is full.
The caller chooses whether to retry or discard. Successful publish reports queue
acceptance count, including zero when there are no subscribers. This is ephemeral:
there are no durable delivery, callback execution, replay, or acknowledgement
claims. Close rejects new calls and lets queued messages drain before EOF.

`LocalOutbox::open` requires an existing, trusted, dedicated local directory on a
filesystem supporting exclusive file locks, atomic rename and file/directory
fsync (validated locally on macOS). Do not replace/delete its lock file or mutate
the directory while open. A second owner fails instead of racing the first.
Capacity bounds pending records and individual payload bytes. Every enqueue and
ack rewrites the bounded snapshot and syncs both data and directory. This simple
implementation is for small local queues, not high-throughput or distributed use.

Delivery integration is explicit: `enqueue(payload)` persists a stable ID;
`pending(limit)` reads in ID order without consuming; the host sends to its
idempotent destination, then calls `acknowledge(id)` **after confirmation**. Failed
or interrupted sends remain pending across reopen. IDs are scoped to this outbox
directory and never reused, including after ack. Use a host-assigned outbox
identity together with the ID as the destination's deduplication key. Do not share
one outbox between independent consumers: there are no claims or leases. A failed
record may head-of-line block a host's small batch; retry/dead-letter policy and
worker scheduling belong to the host.

An I/O error can mean an uncertain commit. The instance then rejects further
operations; close/reopen and inspect pending records before retrying. Corrupt or
incompatible snapshots fail closed. Uncommitted `outbox.tmp` is ignored on open.
No transaction spans this queue and business storage, and no exactly-once delivery
or autonomous eventual delivery is provided. Do not acknowledge a durable record
merely because `Events::publish` accepted it: ephemeral queueing is not destination
confirmation.

Disk calls are synchronous. Hosts should run them on blocking workers and retain
their outer WorkGate guard through actual completion. Plugin cleanup closes event
entrances before drain and releases Outbox ownership after admitted host work
settles. Direct callers must coordinate their own shutdown. No Tasks, Scheduler,
Realtime or Storage driver is changed.

Reference: Rust `b906eed`; TS `12b5e2e`. TS Realtime explicitly excludes general
event bus/queue guarantees; Scheduler owns an occurrence outbox coupled to its own
store transaction and Notifications owns a persisted logical-notification outbox.
This crate is a separate native local primitive, not parity with those business
transactions or their Postgres/D1 drivers.
