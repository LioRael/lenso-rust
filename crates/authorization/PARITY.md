# TypeScript parity and deliberate scope

Reference: `LioRael/lenso` main
`c6d2820513878a8b24d40c015da613b9918b5d6b`, supplied read-only snapshot at
`/tmp/lenso-plugin-inventory-20261015/ts/packages/authorization`.
Rust base: `LioRael/lenso-rust` main
`e5dfafbf9e7e96ec7593c66ae108b227e6629d74`.

This is semantic alignment of the pure decision/RBAC subset, not an assertion
that every TS package entry point has been ported.

Access/session follow-up reference: TS `5f91b9e20f7733fdd7e1c0db0ffcef9f7ec1a98c`
(`packages/auth/src/core.ts`, `sessions.ts`, `packages/authorization/src/auth.ts`),
Rust baseline `2640a273750333e982d07684a3ecfa31d0e38811`. Host authentication remains
external; the new adapter does not claim to port TS's actor proof mechanism or
managed session store.

| TS surface / observable behavior | Rust status | Evidence / boundary |
| --- | --- | --- |
| `core.ts`: default deny, applicable deny over allow, policy abstain | Implemented | `decisions::conflicts_are_order_independent_and_default_deny` |
| Exact actions, resource types/IDs, scope type+ID; literal `*` | Implemented | `only_applicable_rules_match_and_scope_and_star_are_literal`; `Permission::matches/covers` |
| Trusted principal tuple; anonymous only through explicit grants | Implemented | Required realm/audience/identity/credential checks before extensions; no authentication |
| Credential permissions and inclusive expiry boundary | Implemented | `identity_and_credential_limits_precede_any_allow_extension`; malformed or unknown-action permissions refuse |
| `snapshot.ts`: detached facts before async work | Rust ownership equivalent | Request is consumed; callbacks borrow immutable owned data; tested before first poll and across awaits |
| Authoritative resource resolution before credential checks | Implemented | Type/ID substitution denied; authoritative scope allowed; `authoritative_scope_precedes_ceiling_attributes_replace_and_boundary_precedes_policy` |
| Structured attribute resolution | Implemented | Replacement bags; cannot mint principal or modify identity/scope/ceiling; bounded owned JSON |
| `conditions.ts`: all, any, predicate, primitive equals/in, relation | Implemented | All evaluated branches checked; missing is not null; optional direct resolver fails closed |
| Empty/deep/oversized conditions, duplicate rule IDs | Implemented | `rejects_invalid_configuration_at_construction`; Rust enum/owned tree excludes malformed tags and structural cycles |
| Numeric primitive equality | Deliberate safer Rust behavior | Exact integer precision retained; `1 == 1.0`; unequal integers above 2^53 never collapse into an allow |
| Fail-closed extension errors, timeouts, abort, late allow | Implemented with Rust cancellation | Per-check cancellation, future drop and final elapsed check; no spawned evaluation work; synchronous code cannot be preempted |
| `check`, `can`, `enforce` | Implemented | Generic deny error; enum reason codes; cancellation variants for check/enforce |
| Explain gate and ordinal paths | Implemented | Exact gate and manager decision required; private rule IDs omitted |
| Optional observation | Implemented | Completed policy decisions only; observer failure denies; no durable auditing claim |
| `rbac.ts`: exact identity/scope, inheritance, binding expiry | Implemented | `inheritance_is_exactly_scoped_and_identity_and_expiry_are_checked` |
| Role graph limits, cycles, duplicates, missing roles/parents | Implemented | `rejects_bad_graphs_without_partial_grants`; every custom source snapshot revalidated |
| Static graph and fresh store-backed reads | Implemented | `Rbac::new` / `Rbac::from_source`; no positive cross-request cache |
| Immutable memory snapshots and atomic CAS | Implemented with revision adaptation | Store-issued monotonic `u64` revision prevents ABA; exactly one concurrent CAS winner; invalid/cancelled writes do not mutate |
| RBAC as policy and as predicate within AND | Implemented | `rbac_predicate_composes_with_constraints_instead_of_granting_independent_or` |
| In-flight revocation boundary | Implemented/documented | Old snapshot may allow; next fresh read denies; `inflight_snapshot_is_stable_and_revocation_is_seen_by_next_read` |
| `plugin.ts`: exact requires, setup-only wiring, borrowed ownership | Implemented using existing Core | Five lifecycle tests: exact instances, impostor/undeclared rejection, rollback, drain, pure retained service |
| `toctou.test.ts`: old allow cannot authorize changed state | Equivalent business-boundary example | Concurrent in-memory owner/version fence; actual DB transaction integration not included |
| Generic TS action/resource/context type parameters | Idiomatic narrower data API | String actions, explicit resource struct, owned JSON attribute/context bags; use trusted predicates for domain logic |
| Dynamic JS invalid values / whole-object snapshot budget | Adapted | Rust types exclude undefined/nonfinite/functions/cycles; identifiers and attribute/condition/graph bounds validated; no JS prototype/getter emulation |
| `effectiveRolePermissions` / `effectivePermissions` public helpers | Not exported | Internal traversal supports RBAC; avoid an unchecked public graph-reading API |
| `RoleStore.compareAndSwap` trait | Narrowed | Read-only `RoleSource` is the extension point; memory CAS is concrete; persistent writes belong to an application adapter |
| `auth.ts` verified Auth chain adapter | Native host integration seam | `AuthorizedAccess<E>` invokes an explicit trusted `AccessVerifier<E>` per call, pins exact realm/audience, then uses the existing decision engine; no Rust Auth/token implementation |
| Session renewal/revocation visibility | Host-owned, read afresh per access call | `access::each_call_reads_renewal_and_revocation_without_extending_an_old_snapshot`; no renewal side effect or cross-call cache |
| Auth post-policy session expiry / credential ceiling | Checked before returning allow | Minimum session/credential expiry, inclusive boundary and backwards-clock refusal; existing authoritative resource scope and permission intersection retained |
| `management.ts`, `manage.ts`: authorized role editing/delegation and inspection | Not implemented | Memory CAS is trusted infrastructure, not an authorized management endpoint |
| `list.ts`: bounded list filtering and pagination | Not implemented | Single-object decisions do not authorize a query, total, page or aggregate |
| `drizzle/*`, schemas, migrations, SQLite/PG/D1 stores | Not implemented | No database dependency or schema; integrate through explicit `RoleSource` if needed |
| Web/CLI/MCP/Runtime exports, durable audit, SQL policy compilation | Not implemented | No transport wiring, permission container, production resources or core modifications |

Timestamp adaptation: `u64` Unix milliseconds exclude negative, fractional and
nonfinite values by type. Values are not artificially limited to JS safe integers.
Identifiers retain the TS UTF-16 length rule. Context is an attribute object, not
an arbitrary Rust generic payload. Rust source targets 1.80, but this delivery's
executed checks use 1.94.0; minimum-toolchain execution remains unverified.
