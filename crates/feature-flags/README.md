# lenso-feature-flags

Versioned native boolean feature flags for fixture-driven applications. Run
`cargo run --locked -p lenso-feature-flags --example checkout_rollout`.

Install FeatureFlagsPlugin and capture one snapshot per admitted request from
verified host user/organization identities. Each snapshot pins both configuration
and context. Every evaluate call requires an explicit failure default; missing
flags, invalid context, closed/unavailable service and missing rollout identity
return that default with a reason. Invalid updates keep the current valid config.
User rules precede organization rules, then rollout, then configured default.
Rollout members receive true; nonmembers receive configured default (normally false).

Publish uses a CAS revision starting at 1. Rollback restores one of the last 16
revisions as a new revision, preventing ABA. Version strings are descriptive labels.
Existing snapshots keep the old view. Restart uses the supplied fixture; history is
memory only. No persistence, remote updates, polling, TTL or freshness guarantee.
No Cache dependency or persistent resource is acquired.

Stable buckets: FNV-1a 64 over length-prefixed UTF-8 fields with u64 LE lengths:
lenso-flags-v1, key, salt, cohort name (user/organization), realm, subject. Modulo
10000 gives basis points. Versions are excluded so increasing percentages nests
cohorts. This is not a cryptographic algorithm and cannot prevent identity gaming.
Flags are NEVER authorization; hosts still perform permission/revocation checks.

Limits: 256 flags, 256 rules per identity kind, 256 bytes per name, JSON up to 1 MiB.
Caller-held snapshots retain their config until dropped. Plugin cleanup seals new
snapshots/updates in Resources after Core drain; old snapshots remain immutable
usable data, not admission permits. Hosts retain one WorkGate guard over all request
work. No background workers, connections or request-path I/O.
