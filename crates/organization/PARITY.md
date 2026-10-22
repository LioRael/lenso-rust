# Implementation and verification record

2026-10-22, branch `codex/organization`, independent checkout in task-39.

Source repository `/Users/leosouthey/Documents/Codex/2026-10-20/task-8/lenso-rust`
was clean on main at `126b778b8a4e8c18ca7055dbe358d8b0f5afbfd2` before cloning.
Reviewed root AGENTS, `.agents/skills/lenso-rust/SKILL.md` and native lifecycle
reference, Cargo manifests, Authorization access/facts/plugin APIs and Tasks
resource ownership pattern. Organization search found only Authorization scope
examples, no existing relationship/invitation service. Also read task-33
PLAN.zh-CN.md R6 and SOURCES.zh-CN.md S6, and the TS organization service as a
semantic reference. This is a native implementation, not a TS API parity claim.

The requested newer main `c31a47b52522302c3365855eaff47d970ac985e7` could not be
obtained: `git fetch origin main` against the configured GitHub remote failed
because the existing proxy at 127.0.0.1:7890 was unreachable. No proxy, credential,
permission or network configuration was changed. The source checkout was not
modified. Integration onto newer main remains the sole integration owner's work.

Changes are restricted to `crates/organization/**`, one workspace member line and
one Cargo.lock package stanza. Runtime Git pin and all dependency versions remain
unchanged. No Core, Authorization, Search, Notifications or Webhook files changed.
No push, publication, deployment, real mail or user database operation occurred.

Executed targeted validation (offline, existing dependencies):

- `cargo test -p lenso-organization --offline --locked`: business integration tests
  exercise target/token/org/realm isolation; digest-only persistence; one-time
  concurrent acceptance; reopening SQLite; immediate removal denial; owner
  demotion/exit and two-connection last-owner race; explicit transfer; invite
  revocation/expiry; host session revocation; stale read CAS reauthorization;
  cancellation and shutdown drain; rejected SQLite write rollback/version.
- `cargo run -p lenso-organization --example team_workspace --offline --locked`:
  actual temporary SQLite SaaS fixture creates a team, mock-delivers/accepts an
  invitation, reads two members, removes one and observes access refusal.
- `cargo clippy -p lenso-organization --all-targets --all-features --offline --locked -- -D warnings`.
- `cargo fmt -p lenso-organization -- --check`.

The initial restricted build launcher failed with mise `Operation not permitted`;
the same regular Cargo build was then approved through the standard escalation
flow. No alternate path/cache/address workaround was used. The first compile
caught rusqlite's signed integer mapping and was fixed before the passing run.

Not executed: full workspace CI, newer-main combination tests, production Auth
provider or mail integration, disk/power-loss recovery and distributed database
semantics. The supplied sessions and policy are explicitly fixture-only.
