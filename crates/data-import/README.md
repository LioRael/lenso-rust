# lenso-data-import

Native bounded CSV/JSONL imports from the exact LocalStorage instance into the
exact Tasks SQLite instance. `tests/consumer.rs` is a public API consumer: it
provisions an application `contacts` table, implements `Sink`, installs
`ImportPlugin`, previews, imports 3507 rows, shuts down at 1016 rows, reopens the
same database and finishes with no missing or duplicate IDs.

Provision `SCHEMA` explicitly in the business/Tasks SQLite file before startup.
There is no migration runner. The plugin borrows dependencies and starts no worker.
Call `preview(key, cancellation)` for a bounded first-batch dry run with no session
writes. Call `next(session_id, key, cancellation)` until `Batch.done`. Keep the same
session ID after an interruption or lost response. Applications must authenticate
and scope session IDs/object keys to their own tenant and logical destination;
this API is trusted native code, not an authorization boundary.

`Plan` defines format, mapping, types, required fields, text length, byte/record
limits, batch size (1–1000), and Stop/Skip policy. CSV inputs contain data rows,
without an implicit header; mappings use zero-based column numbers. JSONL inputs
are one object per physical line. Text must be UTF-8. CSV supports commas, doubled
quotes, CRLF and quoted multiline fields. Invalid CSV framing and oversized records
always stop: guessing a subsequent record boundary would be unsafe. Other invalid
rows either abort the entire current batch or advance past the rejected row under
Skip. Returned errors locate the logical row (one-based), byte offset (zero-based)
and mapped field, without echoing source values. Skip diagnostics are returned
per batch, not a durable audit log. Preview obeys the same policy and validation.

The session binds storage key, content SHA-256 ETag, size, complete plan and its
explicit validation/sink version. Change the version when `Sink` behavior or its
logical destination changes. Changed content/plan cannot reuse an old offset.
`Sink::validate` is pure synchronous validation; `Sink::write` receives only a
scoped Tasks transaction. Business rows and cursor commit in that same transaction.
A stale concurrent batch cannot write; retry reads the committed cursor. Retrying
a completed session writes nothing. A new session ID is a new import and may
conflict with application uniqueness constraints. This is not arbitrary callback
or cross-database exactly-once: no external effects or recursive Tasks calls are
permitted inside the callback. Application SQL is trusted.

Cancellation before dispatch leaves the current batch uncommitted. Once a SQLite
transaction is dispatched, it settles even if the waiter is dropped: reconcile by
calling `next` with the same session. `TaskError::CommitUnconfirmed` is preserved;
never switch to a new session as an automatic retry. `close()` seals this importer
and interrupts reads; Core shutdown cancels imports at Entrances and drains
accepted transactions before releasing dependencies. A shutdown race can return
Tasks Closed before transaction dispatch. `close()` alone is a signal, not a drain
future. Owned Storage/Tasks cleanup remains with their plugins.

Memory is bounded by the configured record size and batch size, plus an 8 KiB
reader; the entire source is never collected. Limits apply to source metadata and
to actual record bytes. Imports use storage conditional range reads at committed
record boundaries. No real user files, credentials, remote services or paid APIs
are needed for the fixture.

Validation: `cargo test --offline --locked -p lenso-data-import`.
