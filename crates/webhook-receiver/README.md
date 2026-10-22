# lenso-webhook-receiver

Native receiver for **Lenso v1** (`timestamp + "\n" + event ID + "\n" + raw body`, HMAC-SHA256). This is not a Stripe/GitHub verifier.

`provision(path)` explicitly creates the SQLite inbox. Provision the application's business tables in the **same database**, then call `Receiver::open` or install `ReceiverPlugin` in a Core App. Startup opens an existing database; request handling never opens or migrates it. Keys, receiver ID, source and event types are trusted host configuration. No environment credentials are read.

`Receiver::receive(Headers, AsyncRead, apply)` is the host-owned request body adapter. The host owns the listener and must require POST, JSON content type, identity content encoding, and exactly one of each `x-lenso-event-id`, `x-lenso-timestamp`, `x-lenso-signature` header; reject malformed/oversized headers and oversized Content-Length before calling it. Do not deserialize, reserialize or decompress before passing bytes. The adapter bounds actual streamed bytes (even if Content-Length lies), times out body reads, authenticates raw bytes and checks the source/type allowlist. Signed envelopes reject unknown/missing fields, invalid RFC3339 dates, mismatched IDs and data exceeding depth 32 / 50,000 nodes. RFC3339 dates are intentionally narrower than JavaScript Date.parse. Timestamp checks use whole epoch seconds with symmetric tolerance.

`apply` receives `BusinessTransaction::execute` and an immutable event. It must use parameterized SQL and derive tenant/authorization from the host's selected receiver, never treat body claims as credentials. SQL can read/write application tables in main only. The capability has no connection getter, commit, savepoint, batch executor or function access. SQLite authorizer also rejects inbox/schema access, ATTACH, PRAGMA and transaction control, including through triggers. Any SQL error poisons the apply attempt even if the callback swallows it; rollback conflict actions cannot escape into autocommit writes. No raw driver error, body or key appears in public errors.

An IMMEDIATE transaction checks unique `(receiver_id,event_id)` and the raw-body SHA256 digest, inserts its receipt, runs apply and commits both. Identical retries return Duplicate without apply; a different signed body with the same ID returns Conflict. Keep receipt rows for the entire desired deduplication lifetime: there is no automatic pruning. Re-sign old events with a fresh timestamp for retries outside the signature window. Uncertain commit failures return Unavailable; retry the same event/body to resolve them through the inbox.

This is atomic local SQLite processing, **not cross-database exactly-once**. A callback is trusted native application code: do not perform external I/O, open another database, or spawn effects from it. SQL capability restrictions cannot sandbox arbitrary Rust. The host owns business schema/triggers and must not independently mutate the inbox. Business execution is synchronous on Tokio's blocking pool; only body reads and SQLite lock contention have deadlines (configured body timeout; 5s busy timeout). There is no forced timeout for arbitrary application callbacks. Cancelling a waiter after admission to SQL does not cancel the transaction. Core shutdown drains accepted work before closing; a retained plugin service then rejects. Ordinary services need explicit `close()`; close waits active SQL, while pending body reads can finish only to reject against the closed connection.

## Public consumer and checks

```sh
cargo run --offline --locked -p lenso-webhook-receiver --example partner_order
cargo test --offline --locked -p lenso-webhook-receiver
node crates/webhook-receiver/tests/verify-fixture.mjs
```

`partner_order` uses only exported APIs, a temporary provisioned SQLite file and the explicitly public `PUBLIC-TEST-FIXTURE-NOT-A-SECRET` key. It applies one order, submits the same event twice, shuts down the App, reopens and confirms one durable order. Its request timestamp is current; `tests/fixture.json` additionally fixes the exact timestamp/body/MAC for reproducible Node crypto and Rust verification. No live callback, real secret or user database is used.
