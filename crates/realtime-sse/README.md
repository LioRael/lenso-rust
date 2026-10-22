# lenso-realtime-sse

Demand-driven SSE adapter for an existing `lenso_realtime::Connection`.

```sh
cargo run --locked --offline -p lenso-realtime-sse --example notes_dashboard
cargo test --locked --offline -p lenso-realtime-sse
```

`Sse::new(connection, heartbeat, last_event_id)` consumes a connection. Keep its
Subscription handles alive until streaming finishes; renew them explicitly using
the existing service API. `next_frame` is cancel-safe while awaiting data.
`into_stream()` supplies `Result<Bytes, Error>` suitable for Axum
`Body::from_stream`; `write_to` writes a raw SSE body to an established writer.
Dropping either owned transport releases the connection. Writing errors also drop
it. Hosts own headers, HTTP framing, verified identity and admission. Keep the
original accepted-work guard through body EOF/drop; do not admit again.
Subscription-level `closed` does not end other subscriptions; connection-level
closure reaches EOF.

Use `CONTENT_TYPE` and `CACHE_CONTROL` headers. Invalid heartbeat or nonempty
Last-Event-ID is rejected before success headers. Heartbeats are comments;
ready/update/closed/gap use fixed event names and compact JSON envelopes. No `id`
or `retry` is emitted. Obtain fresh application state after every reconnect/gap;
ready does not guarantee an initial snapshot or race-free snapshot acquisition.
No replay, WebSocket, listener, authentication or auto-renewal is added.

No adapter queue or spawned producer exists. Each downstream poll reads at most
one envelope; each writer iteration finishes its write before receiving again.
Memory consists of the existing bounded realtime mailbox plus at most one encoded
frame per in-progress writer/body and the host's transport buffers. Downstream
code must not collect an infinite stream or add an unbounded queue. Already
returned bytes cannot be recalled by later revocation. Stalled consumers do not
accumulate missed heartbeats. Tokio time must be enabled. Heartbeat is bounded to
1 second–1 hour; choose it below the host idle timeout.

The notes/dashboard example uses an in-memory HTTP exchange and fixture identity.
It consumes ready, publishes a committed-note-shaped update, renders title and
revision, and observes subject revocation and EOF. Its parser is only a fixture.
The standard byte stream fits task40 web-axum's public Body boundary (reviewed at
`b9267cd7f391c0fde9740449a1579459da4ebd38`). That separate crate is not imported or
modified; combined Axum integration is not claimed here.
