# Scope and boundaries

Implemented: immutable scoped quota capacities; durable operation idempotency and
payload conflicts; partial reservation consumption; remaining-unit release;
scoped inclusive expiry; successful ordered operation records and expiration
rows; SQLite cross-connection writer serialization; restart recovery; owned native
plugin lifecycle; API request fixture consumption in the same SQLite transaction.

The implementation is intentionally independent of existing `lenso-limits` and
other agents' new modules. It does not claim TypeScript plugin API parity. No Core,
Runtime, payment or external accounting integration is changed. The workspace
manifest and lockfile only register this crate with already locked dependencies.

Not implemented: HTTP/authentication transport, external provider reconciliation,
refunds, pricing, invoices, mutable capacity/top-ups, pagination/retention,
PostgreSQL, background expiry scheduling, cross-database atomicity, tamper-evident
audit storage. Hosts must authorize scopes and operation/reservation identifiers,
provide trusted timestamps, and schedule expiry at startup and during operation.
Unknown commit outcomes must be queried; absence or query failure never authorizes
blind retry or compensation. Failure injection verifies classification, not all
physical disk/power-loss failure modes.
