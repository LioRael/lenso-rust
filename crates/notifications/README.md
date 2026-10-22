# Native persistent Notifications

A dedicated, explicitly provisioned SQLite database stores logical notifications,
immutable rendered plain text, provider identity, a random provider idempotency key,
and every attempt. Ordinary `Notifications` methods work without Runtime; the
`NotificationsPlugin` registers owned cleanup and uses Core admission/drain.
There is no queue, worker, scheduler, automatic recovery send, or schema migration
on startup. The public example uses the existing Tasks host-driven `run_one` API.

```sh
cargo run --locked --offline -p lenso-notifications --example order_confirmation
cargo test --locked --offline -p lenso-notifications
# Only in an environment allowing local TCP listeners:
cargo test --locked --offline -p lenso-notifications --test http -- --ignored
```

## Host and persistence contract

The host must authorize each operation and derive `Scope { tenant, owner }` from
trusted identity. Scope is a database partition, not proof of authorization. The
host also controls templates, required categories, recipients, preference changes,
and transport configuration. Never deserialize an untrusted client directly into
these privileged service arguments. Template name/version, category, required flag,
subject and body are persisted with the request; variables use literal `{{name}}`
substitution (missing values fail). There is no HTML execution or template registry.
Hosts must assign a new version when changing a template definition.

`create` deduplicates `(tenant, owner, business_key)` transactionally. Identical
requests return the original receipt, including a previous suppression decision;
different requests conflict. A disabled optional category yields Suppressed and
never calls the transport. Required categories bypass preferences. Preferences
are sampled at creation; changing them does not revoke pending messages. Exact
scope is required for status, delivery and recovery. Input is bounded to 64 KiB;
recovery uses ascending IDs, a caller-supplied cursor and at most 100 records.
It returns inventory, including terminal records, without performing sends.

Use a host-owned, dedicated SQLite file; protect its permissions/backups because
it contains recipient and message data. Provision explicitly before opening.
SQLite FULL synchronization and a transaction commit precede each external effect.
The attempt begins as Unknown. A crash before transmission can conservatively
leave Unknown even though nothing was sent. A crash after provider acceptance
but before recording the receipt also leaves Unknown. Never delete/recreate such
records or invent a new business key merely to force delivery.

## Delivery and retry boundary

- Pending can be delivered. Accepted and Suppressed are never sent again.
- Accepted means provider acceptance, **not inbox delivery**. No delivery webhook,
  bounce tracking, reconciliation or operator resolution API is implemented.
- Unknown includes timeout, transport error, ambiguous HTTP response, malformed
  success body, panic, crash and interrupted persistence. `deliver` returns the
  stored Unknown receipt without resending. `recover` never changes that state.
- Only definitive Rejected can be explicitly retried, with a lifetime cap of ten
  attempts. No automatic retries/backoff occur. Messages, provider account label,
  endpoint, sender and idempotency key remain fixed. Changed provider identity is
  rejected before transmission. Credentials may rotate only within the same actual
  account; the caller must keep the stable account label truthful.
- Resend documents a **24-hour** idempotency window, matching key and payload:
  <https://resend.com/docs/dashboard/emails/idempotency-keys> (checked 2026-10-22).
  This service does not rely on that window to replay Unknown, even before expiry.
  A host must investigate unknown effects out of band before any new business action.
- Tasks retries may repeat `deliver`; that never replays Unknown/Accepted. The
  example returns the receipt as a successful task result even for Unknown, so a
  successful task must not be interpreted as a delivered email. Creating a
  notification and enqueuing a Task are separate transactions; use bounded recovery
  inventory to discover pending records after a crash between those operations.

`Resend` implements HTTPS POST `/emails`, bearer authorization, `Idempotency-Key`,
and the provider's from/to/subject/text JSON body. Only HTTP 400/401/403/404/422 are
classified as definitive rejection. Other errors (including 408/409/429/5xx),
redirects and invalid success bodies are Unknown. HTTP success requires a nonempty
provider ID. The adapter uses a 10-second total timeout and an 8 KiB response limit;
redirects, environment proxies and connection reuse are disabled (the latter avoids
implicit pooled-connection retries). TLS uses public WebPKI roots and validates
`api.resend.com`. The host supplies a trusted pre-resolved IP at construction,
so delivery never performs potentially unbounded system DNS. Refresh addresses via
host startup configuration; an IP change does not change the provider identity.
No credentials are read from environment or disk and no production call is made
by tests or examples. `local_fixture` uses loopback and hard-coded dummy credentials.

Custom `Transport` implementations are trusted and must themselves bound I/O,
avoid internal retries, accurately classify effects and maintain stable identity.
One connection serializes each service's operations across transport I/O. Multiple
service connections claim atomically in SQLite, so only one can send a pending
record. This is deliberately low-throughput. Blocking work retains Core's admitted
guard after a caller drops its await. App shutdown drains accepted work before
resource cleanup; retained services reject calls afterwards. Ordinary-service hosts
should stop producing work before `close`, which waits for active serialized work
and closes the connection; queued operations may observe Closed. Keep Tokio alive
through shutdown. Runtime termination cannot guarantee completion and leaves the
already committed Unknown attempt for inspection.

## Validation limits

See [VALIDATION.md](VALIDATION.md). The HTTP fixture tests require an explicitly
permitted loopback listener and are ignored by default; ignored is not passed.
The capture example is only a local demonstration, not a real email sender.
