# lenso-audit

Scoped, append-only audit as an ordinary Rust async service. Supply an
`Arc<R: AuditRepository>` and `Arc<A: AuditAuthority>`; the authority's associated
`Principal` type belongs to the application. Audit does not create authentication,
a permissions container, a transport, a queue, or a database client.

```rust
use lenso_audit::*;

async fn record<R: AuditRepository, A: AuditAuthority>(
    audit: &AuditService<R, A>,
    input: AuditInput,
    principal: &A::Principal,
) -> Result<AppendResult, AuditError> {
    audit.append(input, principal).await
}
```

`AuditService::new(repository, authority, config)?` validates configuration.
`.with_reporter(Arc<dyn AuditReporter>)` installs explicit failure diagnostics;
`.with_clock(...)` supplies a trusted Unix-millisecond clock. Neither constructor
performs I/O. The `plugin` feature is enabled by default and can be disabled for
a service with **no library dependencies**.

Runnable examples:

```sh
cargo run -p lenso-audit --example audit-native --no-default-features
cargo run -p lenso-audit --example audit-plugin
```

[audit-native.rs](examples/audit-native.rs) demonstrates standalone append,
idempotent retry and scoped query.
[audit-plugin.rs](examples/audit-plugin.rs) demonstrates exact dependency
handles, external work admission and retained-service shutdown behavior.
The example authorities are application-owned policy fixtures, not authentication
implementations. Production authorities must verify/revalidate evidence,
audience and revocation, and authorize the exact requested scope and operation.

## Event and query contract

- `AuditInput` cannot contain caller-supplied actor or recorded time. The service
  validates input, resolves identity for each append/get/query, then accesses
  storage. `complete` deliberately uses the intent's earlier identity snapshot.
- Event IDs are lowercase UUID-shaped strings. Times are nonnegative integer
  Unix milliseconds up to `8_640_000_000_000_000`. Identifiers have bounded ASCII
  syntax and reject URLs; trusted subject IDs remain opaque, and realm IDs may
  be issuer URLs. These snapshots are audit data; do not indiscriminately log them.
- Summary fields require an explicit action/field whitelist: booleans, bounded
  safe integers, or enumerated literals. There are no arbitrary strings, nested
  payloads, request bodies or credential fields. Policy is owned Rust data, with
  at most 100 actions, 16 fields per action, 32 values per enum. Forbidden field
  substrings match the TS Audit policy. An empty policy accepts only empty summaries.
- Identity is `(tenant_id: Option<String>, scope_id, event_id)`. `None`, the
  literal tenant `"null"`, and delimiter-containing identifiers remain distinct.
  There is no global lookup, count, update, or delete API.
- A repeated ID with identical content is `Duplicate`; recorded time is excluded
  from that comparison and the original stored event is returned. Any other
  content, including a different actor, is a conflict. Corrections append a new
  event linked within the exact scope and do not mutate the original.
- Outcome relations require an existing intent with the same action and target.
  Self-relations and missing relations fail. The repository must remain append-only
  so a checked relation cannot subsequently disappear.
- Queries default to 50 events, capped by `max_page_size` (default 100; range
  1–500). Sorting is descending `(recorded_at, id)` with an exclusive cursor;
  time bounds are inclusive. Scope plus optional action, target, result,
  correlation and recorded-time filters all apply. One extra row determines
  `next_cursor`; pages have no global count or snapshot isolation promise.
- Provider error details and source chains are discarded. Read/duplicate results
  are checked for exact scope and ID; query results are checked for ordering,
  filters and requested bounds before being returned.

## Failure modes and strict intent protocol

`append` propagates fixed errors. `append_best_effort` requires a reporter and
turns **only storage failures** into `BestEffortResult::Unconfirmed`, after
reporting. Input/auth failures, conflicts and invalid relations are errors.
Reporter failure is `DiagnosticsFailed`, never silent success. Diagnostics
contain only mode, stage and the fixed `storage-failed` code, without event,
identity, scope or provider details. A write may already have committed when an
error/timeout arrives; retry with the same stable ID and content or reconcile.

`prepare` first requires `repository.durable_intents() == true`. This is the
repository owner's deployment attestation, **not a capability Audit verifies**.
Never claim it for memory, an uncommitted transaction, or an unverified
acknowledgement path. Only a newly acknowledged intent returns `Ready(receipt)`.
`AlreadyRecorded` does not authorize repeating the business effect.

```rust
use lenso_audit::*;

async fn run_once<R, A, F, Fut>(
    audit: &AuditService<R, A>,
    principal: &A::Principal,
    intent: AuditInput,
    effect: F,
) -> Result<(), AuditError>
where
    R: AuditRepository,
    A: AuditAuthority,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = AuditOutcome>,
{
    match audit.prepare(intent, principal).await? {
        PrepareResult::Ready(receipt) => {
            // The application still authorizes the real effect at its boundary.
            // A timed-out effect may need result Unknown, not an invented rollback.
            let outcome = effect().await;
            audit.complete(&receipt, outcome).await?;
        }
        PrepareResult::AlreadyRecorded { intent_id: _ } => {
            // Read the existing outcome/reconcile; do not run effect again.
        }
    }
    Ok(())
}
```

Receipts have private fields, bind to one service instance and cannot be
serialized. Cloning a receipt preserves its capability; moving a service does
not invalidate it. A receipt retains its own intent without an unbounded
service-side receipt registry. Completion uses that trusted identity even after
principal revocation. Retrying the same outcome is idempotent. As in TS, a
different outcome ID may append an additional outcome; the receipt is not a
single-use business authorization.

Completion failures become `OutcomeUnknown { intent_id, diagnostics_failed }`:
the business effect may have happened and cannot be undone by Audit. There is
no automatic retry or reconciliation worker. A wrong-instance receipt returns
`InvalidReceipt`; a plugin service already shut down returns `Closed` without
touching providers or diagnostics.

All async methods are caller-owned futures. Cancellation can occur after a
provider committed but before the caller observed success. A cancelled/lost
`prepare` grants no permission for the effect, and its duplicate retry cannot
return Ready. Dropped `complete` futures may leave an unknown outcome; retain
stable IDs and reconcile. Audit spawns no hidden work and guarantees no
diagnostic delivery on cancellation. Adapter panics are programming failures,
not sanitized provider `Err` values.

## Ownership and plugin lifecycle

`AuditPlugin::new(repository_handle, authority_handle, config)` declares those
exact instances. `.with_reporter(reporter_handle)` adds that exact dependency.
All policy/config validation runs during preflight before any setup. No string
lookup, service-type singleton or dependency acquisition outside `Setup` is used.

The application/resource provider owns its repository, authority and reporter
lifecycle. Audit only closes its own service in the `Resources` phase. It does
not close borrowed providers. Retained service references reject calls after
cleanup; the same applies to setup rollback.

Hosts must admit external work through `RunningApp::work_gate()` and retain the
guard throughout the business operation and every dependency call. Audit is a
dependency-local service and does not reenter the app gate; already admitted work
must finish during drain. Retaining an `Arc` alone is not admission. Direct calls
racing resource shutdown without a host guard are outside this contract.

## Adapter boundary and limits

`AuditRepository` and `AuditAuthority` use native `impl Future + Send` traits and
static dispatch; callers can implement them with ordinary `async fn`. Reporter
uses a boxed future to support an optional type-erased diagnostics binding.
Storage inserts must atomically classify duplicate/conflict and return original
content on reads. The service cannot compensate for a dishonest durability
attestation or an adapter that overwrites records.

`MemoryAuditRepository::new(max_events)` is a bounded, process-local validation
adapter. A mutex linearizes insert classification. It never advertises durable
intents, never evicts existing records, and fails new inserts at capacity while
still classifying duplicates/conflicts. Reads return independent owned copies.
Queries scan the bounded store. This is not a production persistence, crash
recovery, tamper-evidence, or multi-process guarantee. Direct repository methods
are trusted adapter APIs, not a substitute for service validation/authorization.

See [PARITY.md](PARITY.md) for TS alignment and omitted integrations, and
[CHECKS.md](CHECKS.md) for actual verification and review evidence.

## Optional SQLite persistence

Enable `sqlite` for `SqliteAuditRepository::open(path).await`. Open at host startup,
then inject its `Arc` into the existing `AuditService` or `AuditPlugin` repository
handle. No Core or authorization policy change is required. All database work
runs on Tokio's blocking pool, behind one connection mutex per repository. Clones
share that connection; separate opens coordinate uniqueness through SQLite.
Dropping the last clone closes the connection after accepted operations settle.
Hosts must retain their work guard and await calls before shutdown. Cancelling a
waiter cannot undo a write already submitted to the blocking pool.

The versioned `lenso_audit_v1` table stores validated event snapshots as JSON.
A structured serialized scope key distinguishes absent tenants, literal `null`
and delimiter-containing IDs. The primary key atomically classifies concurrent
inserts; update/delete triggers reject accidental mutation. These triggers are
not tamper evidence against a database owner. Existing databases are trusted
host resources; this adapter does not repair incompatible schemas.

Scope selection, inclusive time bounds and the exclusive `(recorded_at, id)`
cursor use indexed columns before payload decoding. Remaining filters reuse the
native predicate while streaming rows; the page limit applies after those filters.
Memory is bounded by the requested page, but a selective residual filter can still
scan the selected range. No global list is exposed. Payload serialization is an
internal v1 storage format, not a promised TS wire representation.

Commits use `synchronous=FULL`. Reopening a file recovers committed audit rows.
This alone does not verify filesystem/power-loss durability, so `durable_intents`
remains false and strict `prepare` returns `StrictUnavailable`. The adapter does
not claim business/audit atomicity, exactly-once effects, encryption, or a
reconciliation worker. Provider errors are sanitized by `AuditService`; startup
and direct trusted repository calls return provider errors to the resource owner.

Continue to send events through `AuditService`: its approved Boolean/integer/enum
summary projection excludes sensitive fields before storage. Rejected inputs are
not persisted; there is no raw-payload logging or post-storage redaction layer.
Direct repository methods remain trusted APIs and bypass that policy.

```sh
cargo run --locked -p lenso-audit --features sqlite --example permission-change
```

The example updates an actual temporary SQLite membership row from reader to
editor and records actor, tenant/project scope, target, action, request correlation,
and intent/outcome. It uses only obvious test identities. Its business and audit
commits are separate: after an interrupted effect/outcome, reconcile the stable
operation ID rather than replaying the change. It is not a production permission
policy or strict intent consumer.
