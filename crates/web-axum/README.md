# lenso-web-axum

Explicit native Axum routes for ordinary Lenso services. `WebPlugin` declares an
exact service handle and builds routes in setup. It does not discover operations
or introduce a second business engine. Core, Tasks, and Runtime are unchanged.

```sh
cargo run --locked -p lenso-web-axum --example notes_http
cargo test --locked -p lenso-web-axum --all-targets
```

The example creates a real in-memory Notes service, validates a typed request,
checks a clearly marked test credential, writes a note and consumes the JSON
response through the public router. Replace its fixture credential check with
host identity verification; a host can insert a typed Axum `Extension` once.
Each business service must still authorize the current resource/action. The
adapter never trusts client role/identity headers as verified identity.
`json_input<T: DeserializeOwned + Validate>` maps decode/validation failures to
safe errors. `HttpError` exposes only fixed codes; map private service errors to
these classes explicitly. Custom host routes retain control of their responses.

`Limits` bounds buffered request bytes and total read time. Upload streaming is
not supported in this first adapter. Response bodies remain streaming and are
polled only on downstream demand; EOF, error and drop release admission. An idle
timeout is enforced when the body is polled (an externally retained, unpolled body
must be consumed or dropped by its host). No handler execution deadline is imposed:
an accepted write must settle before resources can close. Client cancellation
stops waiting but does not abort the accepted handler. Application-spawned work
must retain a clone of the request's `AcceptedWork` extension until it settles.
Panic maps to a fixed internal error; arbitrary detached work without that permit
is outside this lifecycle guarantee.

`listen: None` performs no bind: the external host owns listener shutdown/join and
must stop its listener as well as shut down the app. `listen: Some(address)` binds
during setup, registers the stop signal immediately, then registers a server join.
App shutdown seals admission, signals graceful server stop, drains work including
response bodies, joins the server, and releases the earlier dependency. Constructing
or inspecting the plugin never binds. Keep Tokio alive and await app shutdown.

## Evidence / limits

Base: `c31a47b52522302c3365855eaff47d970ac985e7`, copied into an independent
checkout from the local publication checkout's Git objects. No publication-line
working files or later Search/Notifications/Webhook changes were copied.
Runtime Git pin is unchanged. Only this crate plus workspace membership and the
required Axum dependency lock additions are changed. Network listener verification
is intentionally unrun because inherited socket restrictions prohibit retries;
owned server binding/join code is compiled, not claimed as network-end-to-end proof.
Workspace-wide integration/CI remains the publication owner's responsibility.

Verified locally (2026-10-22, offline dependencies, normal approved Cargo execution):

- `cargo test --offline --locked -p lenso-web-axum --all-targets`: five tests pass
  (four lifecycle tests plus business route tests).
- `cargo run --offline --locked -p lenso-web-axum --example notes_http`: emitted
  `{"id":1,"text":"First real note"}`.
- `cargo clippy --offline --locked -p lenso-web-axum --all-targets -- -D warnings`:
  passed; this also type-checks all affected targets.
- `cargo fmt -p lenso-web-axum --check` and `git diff --check`: passed.

Tests cover handler-waiter cancellation, shutdown waiting for response EOF, stream
drop, closing admission, stalled upload and polled response timeout, authentication
rejection, typed validation, malformed JSON, body limit and missing routes.
First compilation populated a cold crate target; subsequent checks reused outputs.
No latency/performance, real network, production authentication, or full-workspace
claim is made. The local router mode requires no socket or external service.
