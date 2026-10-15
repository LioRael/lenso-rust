# lenso-authorization

A decision service over trusted application facts, with exact-scope RBAC and an
optional thin adapter to `lenso-plugin-core`. It owns no connections, workers,
global registry, transport, authentication method or database. Ordinary business
services can hold `Authorization` or `Arc<Authorization>` directly.

## Minimal service API

```rust
use lenso_authorization::*;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut options = AuthorizationOptions::new(["read"]);
options.identity.required = true;
let mut owner_rule = Rule::new(
    "owner-read", Outcome::Allow, ["read"], "note",
    Condition::Predicate(predicate(|facts, _| {
        Ok(facts.principal.as_ref().is_some_and(|principal| {
            facts.resource.attributes.get("owner").and_then(|v| v.as_str())
                == Some(principal.subject_id.as_str())
        }))
    })),
);
owner_rule.scope = Some(Scope::new("personal", "home"));
options.rules.push(owner_rule);
let authorization = Authorization::new(options)?;

let mut resource = Resource::new("note", "one", Scope::new("personal", "home"));
resource.attributes.insert("owner".into(), "alice".into());
let request = Request::new(
    Some(Principal::new("notes", "alice", "user")), "read", resource,
);
authorization.enforce(request.clone()).await?;
assert!(authorization.can(request.clone()).await);
assert_eq!(authorization.check(request).await.code, ReasonCode::Allowed);
# Ok(())
# }
```

`check` returns a safe `Decision`; `can` exposes a boolean; `enforce` returns only
`AuthorizationError("Access denied.")` on refusal. Decisions never contain
principal/resource identifiers, rule identifiers or extension error details.
Treat reason codes as internal diagnostics, not a public resource-existence oracle.

`Request`, `Principal`, `Resource`, attribute bags, rules and role graphs are owned
Rust data. Passing a request by value fixes its contents before the future's first
poll. Extensions receive shared references to that snapshot. JSON attributes have
no handles, getters, cycles or interior mutability. An application may retain and
change its own clone without changing an in-flight decision. Construction consumes
configuration too; installed extension objects themselves remain trusted code.

Do not build facts from an unverified public principal/owner/scope DTO. The caller
must authenticate, load authoritative resources and establish credential ceilings
before invoking this service. This crate does not verify passwords, tokens or
Runtime peer identity. It provides no serialization-based authentication contract.

## Evaluation contract

The engine evaluates in this order:

1. Validate resource/principal identifiers and attribute bounds; read the trusted
   clock once; reject unknown actions and caller cancellation.
2. Enforce configured identity presence, exact realms, operation audiences and
   credential presence.
3. Resolve an optional authoritative resource. Type and ID cannot change; its
   authoritative scope may change. Validate the resulting owned facts.
4. Intersect credential permissions/expiry with that target. Resolve optional
   attributes, replacing selected bags, then enforce application boundaries.
   An attribute resolver cannot mint an anonymous principal or change identity,
   action, scope or credential ceilings.
5. Evaluate matching rules and all policies. Applicable deny wins; otherwise any
   allow grants; no match/all abstain denies. Any evaluated extension error or
   unwinding panic refuses access, even if an earlier rule allowed.

Scopes compare both `kind` (TS `type`) and `id`. There is no prefix, parent,
tenant inference or wildcard; `*` is an ordinary string. Omitting a permission's
`resource_id` covers every resource of its exact type in its exact scope.
Omitting a rule's scope deliberately permits it to apply across scopes.
Principal identity is the exact `(realm_id, subject_id, kind)` tuple; attributes
and matching emails do not link identities.

Independent allow rules/policies combine with OR. Use `Condition::All` to combine
an RBAC grant with a restriction. `All` and `Any` evaluate every child so an error
in a later branch cannot be masked. Empty groups, depth above 32 and more than
1,024 condition nodes are rejected. Conditions include trusted predicates, direct
relation resolution, and primitive attribute `Equals`/`In`; a missing key never
matches null. Integer comparisons preserve full `i64`/`u64` precision, including
values above JavaScript's safe-integer range. Numerically equivalent integer and
float values such as `1` and `1.0` match, without rounding integers to floats.

Identifier limits match 1–256 UTF-16 code units. Request attribute bags have a
combined 10,000-node/64-depth bound. Conditions and role graphs have separate
bounds. These are application fact constraints, not a native-code sandbox.

## Async extensions, cancellation and deadlines

Implement `Policy`, `Predicate`, `ResourceResolver`, `AttributeResolver`,
`RelationResolver` or `RoleSource` directly. Each has a small boxed, `Send` future
API supporting borrowed request facts. The `policy` and `predicate` helpers wrap
ordinary synchronous fallible closures. No framework macro or DI container is
needed. Extension errors use the opaque `EvaluationError`; record private errors
only through the application's own trusted diagnostics policy.

`check_with_cancellation` and `enforce_with_cancellation` accept a cloneable,
sticky `Cancellation`. `Evaluation` carries one Unix-millisecond `now` and a
per-check cancellation signal. The signal is cancelled when a check completes,
times out, is cancelled, or its polled future is dropped. Pending extension
futures are dropped on timeout/cancellation; the engine spawns no background work.

The default deadline is one second; accepted durations are 1 ms through 60 s.
A final elapsed-time check prevents a blocking callback from returning a late
allow. Tokio must have time enabled. A deadline cannot preempt synchronous native
code or undo external effects. Extensions should be read-only, yield promptly and
honor cancellation; work they spawn themselves remains theirs to manage. Unwinding
panics are caught, but the process panic hook still runs. Abort panics, process
termination and a stopped runtime cannot be recovered.

`Observer` receives only completed policy decisions, as in TS; early validation,
boundary, timeout and error outcomes are not an audit stream. Observer error or
timeout denies. `explain` is disabled unless an exact action/resource gate is
configured and the manager request is allowed. Explanations expose only ordinal
paths such as `rules/0/allow`, never rule IDs. Manager facts are still trusted
application inputs; this method does not authenticate the manager.

## RBAC and snapshot boundaries

`Rbac::new(graph, actions)` holds a static owned graph. `Rbac::from_source` borrows
an explicit `Arc<dyn RoleSource>` and reads one consistent snapshot on every
check. It implements both `Policy` (allow/abstain) and `Predicate` (for conjunctive
rules). No positive cross-request cache is installed.

Roles and inheritance are confined to exact scopes. Validation rejects unknown
actions, duplicate role/permission/binding keys, missing parents, cycles,
cross-scope permissions/inheritance and unknown bound roles. Limits are 512 roles,
4,096 bindings and 512 permissions/parents per role. Bindings match exact identity
and expire when `expires_at <= evaluation.now`. All timestamps are Rust `u64`
Unix milliseconds.

`MemoryRoleStore` provides immutable `Arc<RoleSnapshot>` reads and atomic
`compare_and_swap(expected_revision, next_graph, evaluation)`. Revisions are
store-issued monotonically increasing integers, preventing reuse of an earlier
revision. Invalid updates leave the current snapshot untouched; stale or
cancelled writes return false. This is a **trusted internal store**, not an
administrative authorization service. Applications must authorize any mutation
before calling it. CAS fences only this store, not another identity/resource
source or an external approval.

Revocation affects the next read that sees the new snapshot. In-flight old
snapshots and past decisions remain unchanged. A static graph stays static.
Custom adapters must state their consistency and revocation visibility; replica
lag or their own caches can delay fresh reads. Multiple resolvers are not one
atomic snapshot.

An allow is not a capability for a later write. Resource owner, status and version
can change after a check. The business write must atomically recheck the relevant
owner/version or use an appropriate transaction. See the executable
[standalone example](examples/standalone.rs) and the concurrent regression in
[async_boundaries.rs](tests/async_boundaries.rs). They use application-owned memory
to demonstrate the fence; no database atomicity is claimed.

## Optional Core wiring and lifetime

The default `plugin` feature adds `AuthorizationPlugin` using existing
`Plugin`, `InstanceKey`, `Setup` and handles. Disable it for a service with no
plugin-core dependency. `AuthorizationPlugin::new(options)` validates a pure
configuration. `AuthorizationPlugin::setup(requires, factory)` runs a factory at
Core setup and obtains exact dependencies through `setup.get(&handle)`; no
provider is selected by name or service type. Configuration requiring providers
is constructed and validated in that factory.

The wrapper registers no cleanup and closes no borrowed provider. Resource owners
register cleanup through existing Core lifecycle. `Authorization` is a dependency
service; the outer business entry holds the existing Core `WorkGuard` through
both checking and protected use. Authorization does not reenter admission, so
already admitted work can finish during shutdown. A retained pure service has no
artificial closed state; using borrowed providers after shutdown is outside their
lifetime. The guarded entry must reject new work. The [injected example](examples/injected.rs)
and [lifecycle regressions](tests/lifecycle.rs) demonstrate this boundary.

## Running and scope

```sh
cargo test -p lenso-authorization --locked
cargo test -p lenso-authorization --no-default-features --locked
cargo clippy -p lenso-authorization --all-targets --all-features --locked -- -D warnings
cargo fmt -p lenso-authorization -- --check
cargo run -p lenso-authorization --no-default-features --example standalone --locked
cargo run -p lenso-authorization --example injected --locked
```

The crate source targets Rust 1.80; this delivery was checked with Rust 1.94.0.
See [PARITY.md](PARITY.md) for TS alignment and explicit omissions, and
[VALIDATION.md](VALIDATION.md) for actual results and integration notes.
