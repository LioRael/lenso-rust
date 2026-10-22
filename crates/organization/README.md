# lenso-organization

Native SQLite organization service using the existing Lenso `AccessVerifier<E>`
and `Authorization` interfaces. Authentication, session issuance and notification
transport remain host-owned. This crate opens only its configured SQLite database;
it never sends invitations or changes an external identity provider.

Install `OrganizationPlugin::new(path, realm, audience, authorization_handle,
verifier)` in an `App`. The authorization handle is an exact declared dependency.
The verifier is a borrowed host service; its resource owner must outlive this
plugin. Provide fresh verified identity, revocation and session facts on every
call, just as for `AuthorizedAccess`. No principal or membership role is accepted
from public command input. See the working `team_workspace` example.

```sh
cargo run -p lenso-organization --example team_workspace --locked --offline
cargo test -p lenso-organization --locked --offline
```

`Arc<Organization<E>>::execute(evidence, organization_id, Command)` is the public
business API. Commands cover create/read/rename, add/change role/remove/leave,
explicit owner grant/transfer, targeted invite/accept/revoke/expire. `Read` returns
all members and the exact version; `snapshot.members.len()` is the count under
that same scoped authorization. Invitation metadata is included only for owners.
Unknown and inaccessible organizations return the same `Error::Denied`.

Every operation additionally runs the host's action/resource policy. Registered
actions are in `ACTIONS`. Resources have kind and scope kind `organization`, with
the requested organization ID, and trusted `realm`, `version`, `membership_role`
attributes. Context includes targets, requested roles and invitation metadata
where relevant, never token or digest. Hosts may restrict this further. The
built-in minimum boundary allows authenticated creation, member reads/leave,
targeted invitation acceptance, and owner-only relationship management. `Admin`
is a stored application role, not a bypass of owner management restrictions.
There is no platform administrator privilege or implicit cross-organization grant.

A plugin serves one identity realm and audience; targets must belong to that
realm. Subject identity includes realm, subject ID and kind. SQLite keys include
realm and organization ID. Ordinary role updates and invitations cannot grant
Owner. Explicit grants require an existing member, and transfer atomically
promotes the target and demotes the acting owner. Removal, demotion and exit
cannot eliminate the final owner.

Invitations have a random 256-bit token, fixed target/role and expiry (maximum 30
days). Only a SHA-256 digest is persisted. The raw token is returned once in
`Outcome::Invited` and has no Debug implementation. Deliver it using an application
notification component only after successful commit; the example uses a local
mock. Acceptance is exact-target, scoped and single-use. Expiry is enforced at
acceptance and transaction commit; `Expire` persists terminal expired status.
Revocation and acceptance cannot race into two successful outcomes. Neither
returned tokens nor commands should be logged.

Storage uses a versioned `lenso_organizations_v1` table containing bounded JSON
relationship snapshots. SQLite `BEGIN IMMEDIATE` plus version comparison protects
each commit across independent connections. After a version conflict, the entire
read/identity verification/policy check is repeated (up to 8 attempts). Reads also
validate the version before returning. Changed outcomes expose the committed
version for application invalidation, without an event bus. Busy/storage errors
are opaque `Storage`; the caller must not assume success or blindly replay an
uncertain mutation. There is no idempotency key API in this slice.

The plugin acquires its connection only at startup, registers owned cleanup before
schema initialization, and performs SQLite work on Tokio blocking workers. Public
calls enter Core's WorkGate once; an owned task retains admission until accepted
work settles even if its caller cancels. Shutdown drains before closing SQLite.
A caller cancelling an invite may lose its one-time raw token; owners can revoke
that pending invitation and issue another. Keep Tokio alive through shutdown.

Limits: 1,000 members and 1,000 total invitations per organization, including
terminal invitations, with no pruning API yet; identifiers/names up to 256 bytes.
This small-organization slice returns complete lists, without pagination,
cross-organization discovery, organization deletion, HTTP routes, production
session storage or a durable notification/outbox guarantee. Schema startup is
additive; future schema upgrades require explicit versioned migration. No claim
is made about power-loss durability, distributed databases or tamper resistance.
