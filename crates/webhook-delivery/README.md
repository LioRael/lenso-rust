# Durable outbound webhooks

`DeliveryPlugin` owns a dedicated explicitly provisioned SQLite database and a
fixed `Endpoint`. A trusted order host calls `create("order-42-paid", raw_json,
3)`, then enqueues that event ID through `Delivery::task` in existing Tasks.
The public-API HTTP fixture in `tests/delivery.rs` is the executable consumer:
it submits an order event, drives Tasks, verifies the receiving server's raw
body signature, and reopens the plugin to inspect durable receipts.

Signature protocol: `Authorization: LensoWebhook v1=<lowercase HMAC-SHA256 hex>` over
`b"v1\n" + Webhook-Event-Id + b"\n" + exact HTTP body bytes`. JSON is validated
at acceptance but never normalized or reserialized: these stored bytes are the
canonical payload. `Webhook-Key-Id` selects the receiver's configured key. Keys
are passed explicitly in memory, never stored in SQLite or included in Debug/logs.
The signature uses Authorization so ureq redacts it even under debug logging;
do not place secrets in URL paths or event IDs, which transport logs may include.
The receiver must authenticate the signature and durably deduplicate the stable
event ID; there is no remote exactly-once guarantee or timestamp freshness claim.

Only 2xx confirms delivery. 429/5xx permit Tasks-policy retry within the persisted
attempt budget. Other HTTP responses pause. Transport failure, timeout, shutdown,
and a recovered interrupted attempt are unknown, not proof of non-delivery.
Unknown is not automatically resent. Manual replay of paused/unknown requires
`ReplayConsent::AcceptPossibleDuplicate`, adds one attempt and preserves ID/raw
bytes/history. Pause affects future attempts and cannot recall in-flight effects.

The host owns Tasks provisioning, enqueue reconciliation, worker execution,
retry policy and shutdown order (Tasks first, then Delivery). There is no second
queue, poller, or cross-database transaction. A crash between create and enqueue
requires the host to re-read its stable event ID and repeat Tasks enqueue with
the same dedupe key. For manual replay, enqueue a new job/dedupe key after replay;
reconcile that handoff similarly. Keep the Tasks retry budget aligned with create's
budget. Exhausted Tasks jobs may leave a retryable delivery awaiting host action.
Authorization, tenancy, endpoint selection and business outbox atomicity belong
to the host; these native APIs must not be exposed directly to remote callers.

SQLite persists a sending attempt before HTTP, then holds an IMMEDIATE writer
transaction throughout the bounded HTTP call and settlement. This deliberately
serializes all deliveries per database, including across processes. Startup
recovery takes the same fence and marks abandoned sending attempts unknown;
it can conservatively suppress an attempt in the pre-send gap, never resend it.
Admission guards stay inside blocking work after a caller drops its future.
Shutdown signals stop and waits for bounded in-flight I/O; it does not forcibly
interrupt sockets. An observed cancellation makes the outcome unknown even if
a response arrives. Database commit errors are ambiguous: inspect the same event
ID before deciding on replay. No pruning, automatic recovery resend or migrations.

Production accepts only HTTPS, an explicit public pinned IP and a verified TLS
hostname. Private/loopback/link-local/metadata, special/reserved ranges, URL
credentials/query/fragment, proxies and redirects are rejected or disabled.
DNS is resolved by the trusted host before construction; the adapter never
re-resolves, so DNS rebinding cannot change the connection destination. The host
must refresh pins deliberately and ensure its resolver/configuration is trusted.
This cannot prevent a public destination acting as an application-level proxy or
network routing changes. `local_fixture` is an explicit loopback HTTP test adapter,
never an option to enable private targets in production configuration.
