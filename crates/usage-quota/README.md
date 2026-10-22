# Usage & Quota

Native durable usage/quota service with SQLite IMMEDIATE transactions, WAL and
FULL synchronization. Not a payment integration or a complete accounting ledger.
No real customer balances are accessed. `lenso-limits` remains the existing
in-memory rate/lease limiter; this crate owns separate `uq_*` tables and changes
no Core APIs.

Create an immutable capacity per host-authorized scope (for example a tenant, API and
billing window combination). Reserve units, consume partially, release the remaining units,
or expire them explicitly. Consumed units cannot be refunded. Used + reserved
never exceeds capacity. Reservation IDs are globally unique within this database;
request IDs are unique per scope; operation IDs are globally unique and must be
retained as long as retries are possible. Identical successful commands return the
original record; changed payloads conflict. Timestamp is trusted execution context,
not part of command identity, so retries return the original timestamp. Errors
before commit roll back and are not recorded as committed operations.

Pass trusted nonnegative Unix timestamps in one consistent unit. Successful new
operations reject backwards clock movement across the database. Expiry is inclusive
(`now >= expires_at`); consume/release reject expired reservations even before the
sweeper runs. Run `Expire { scope }` with a fresh operation ID for each scope at startup and periodically.
Until swept, expired reservations conservatively retain capacity. Restart reopens
persisted reservations, usage and records. No background worker or external clock
is created. Never accept scope, timestamps or reservation IDs directly from an
unauthorized client: this library assumes a trusted host authorization boundary.

`ApiCall` is an executable local API fixture: it inserts a request row and consumes
actual quota atomically. Duplicate request IDs with another operation ID conflict
and roll back the debit. The atomic boundary is this SQLite domain only. A remote
API, provider debit, payment or other database is not atomic with it; callers need
their own reconciliation protocol. No automatic retries or compensating refunds
are provided. `CommitUnknown` includes the operation ID; query `outcome` on a
healthy connection/restart. `Committed` is durable evidence, while `NotObserved`
is only absence in that snapshot, never permission to retry/refund an unresolved
in-flight or external operation. Query failure also leaves outcome unresolved.

`records()` provides ordered successful commands and quantities for reconciliation;
expiration detail is retained in `uq_expirations`. These are local operational
records, not tamper-proof audit evidence. The DB owner can edit them. No retention,
quota top-up, window rollover, remote store, payment adapter or HTTP server is
included. Application policy should create a new scope for a new window.

Open once at startup, close explicitly. `UsageQuotaPlugin` owns the connection,
admits service operations with WorkGate and closes after drain. Standalone calls
and close serialize through a mutex; independent connections/processes serialize
writers through SQLite. Busy timeout is 5 seconds; surfaced failures are not
retried. Calls are synchronous; async hosts should use a blocking worker. One
sweep/records read can scale with retained data, so this is a bounded fixture/local
service, not a claim of high-volume production throughput. Database files must be
on a filesystem with correct SQLite locking and durability behavior.

Run `cargo run --locked -p lenso-usage-quota --example api-quota` and
`cargo test --locked -p lenso-usage-quota`.
