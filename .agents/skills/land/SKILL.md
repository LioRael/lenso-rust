---
name: land
description: Verify and land requested Lenso Rust changes to the remote destination branch, preserving native application lifecycles and pinned Runtime dependencies.
user-invocable: true
disable-model-invocation: true
metadata:
  delta-action: land
  internal: true
---

# Land Lenso Rust changes

Complete an explicit landing request for this repository, normally onto remote
`main`. Creating or editing this skill supplies no landing authorization.
Crates/npm publication, tags, releases, binary distribution and deployment
require separate authorization.

## 1. Pin scope and destination

Work in the attached Lenso Rust checkout. Read applicable `AGENTS.md`/`AGENT.md`,
repository policies, [README](../../../README.md), manifests, toolchain files
and current CI workflows. Inspect status, staged changes, branches, recent
commits and fetch/push URLs. Confirm the publishing repository and remote;
the expected repository is `LioRael/lenso-rust`. The `local` backlink is not a
publishing remote.

Identify exactly which commits/files the user requested. Preserve unrelated
changes and authorship. Creating this skill does not include the checkout's
existing implementation changes in a future landing. Ask when source scope,
destination or publication permission is ambiguous.

Fetch the destination, record its full base SHA and query current protection,
rulesets, required CI, reviews, signatures and allowed merge methods. A policy
query failure blocks landing. Read destination instructions if they differ.
Creating a missing destination branch requires explicit authorization.

Commit only in-scope files on a recoverable source branch, retaining configured
signing. Stop if signing authentication is unavailable. Use `GIT_EDITOR=true`
for Git commands that may open an editor. Preserve unrelated staged/unstaged
work without stashing or discarding it.

For retries, explicitly pull requested parent-thread fixes before pinning the
source again; a message about a fix does not update this checkout.

**Done:** source SHA, scope, repository URL, publishing remote, destination,
base SHA and direct-push or PR route are recorded and authorized.

## 2. Form and review one candidate

Create a unique local integration branch from the fetched destination and
integrate the pinned source, respecting linear-history rules. Leave the source
and destination refs unchanged. Resolve conflicts only when combined intent is
clear; otherwise stop for a decision.

Review the complete diff before executing changed scripts or workflows with
credentials. For affected code, verify typed instance dependencies, validation
before acquisition, initialization rollback, cleanup ownership and awaited
shutdown. Runtime exports remain explicit, readiness follows native setup,
drain precedes resource release, and cancellation does not imply rollback.
Keep the kernel, protocol, SDK and generator Runtime-owned; Rust applications
and native plugins remain Lenso Rust-owned.

Preserve the formal Runtime Git revision and `Cargo.lock` unless updating them
is explicitly in scope. Local candidate overrides belong in owned staging
directories; they do not certify the formal dependency pin. Review generated
bindings against their declared contract source and matching generator.

If the change is already present, verify remote contents and report a no-op
without claiming a new landing.

**Done:** a clean, reviewed candidate contains the requested source plus current
destination work, excludes unrelated edits, and has a pinned candidate SHA.

## 3. Verify that exact revision

Read current manifests and verification entrypoints rather than assuming tool
versions or script names. Record `rustc --version` and `cargo --version`, and
`bun --version` when staging or cross-language checks require Bun. Keep global
tool configuration unchanged; disclose environment workarounds.

For native implementation, adapter or example changes, verify the formal
dependency resolution:

```sh
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all --check
cargo run --locked -p lenso-log-plugins --example offline
```

`cargo test` resolves locked dependencies and compiles the workspace, so do not
prepend a separate fetch/build unless diagnosing resolution or producing an
artifact that tests do not build. Start with the affected crate's tests during
iteration; run the workspace checks above for the final native candidate.

Reuse successful evidence only for the same full candidate SHA, unchanged tree,
lockfile, toolchain/features/profile, commands and relevant companion revisions.
Record the run URL/attempt or local receipt. A different integration tree, dirty
source or changed Runtime/TS ref requires affected verification again. Reusing
local evidence never substitutes for required hosting checks.

Record lockfile hashes before and after checks. If dependency updates are in
scope, synchronize only their necessary resolutions on the candidate, review
and commit them, then repeat locked verification in a clean source copy.
Unexplained lockfile changes block promotion.

For staging/generation changes, inspect the current builder and its regression
tests. Run applicable tests with an explicitly selected, attached Runtime
checkout, including generation failure/recovery, changed-content invalidation
and prior-artifact preservation. The current test entry is:

```sh
LENSO_RUNTIME_ROOT=<selected-runtime-checkout> bun test scripts/build-runtime.test.ts
```

When cross-language behavior changes, follow the Runtime integration section
of the README and the selected Runtime checkout's actual smoke entry. Build
both native caller and server before selecting `LENSO_RUST_ROOT`; use owned
ports/processes and confirm their cleanup. A local SDK override must be confined
to disposable staging and reported separately from formal-pin verification.
Reading another repository does not authorize changing or landing it. If
required integration needs such edits, obtain that scope first.

Documentation/skill-only changes need frontmatter, local-link, Markdown and
`git diff --check` validation, not unrelated application builds. Performance
checks apply when performance changes or claims are in scope; use the existing
Runtime benchmark rather than inventing a parallel platform.

Inspect external/generated changes, commit intended output and repeat affected
checks after any candidate change. Missing required tools, unavailable required
integration or failed checks block landing; local skips cannot replace CI.

**Done:** applicable checks pass on the unchanged candidate, formal dependency
resolution is reproducible, and output/resource ownership is verified.

## 4. Publish and confirm landing

Recheck destination policy. Publish the final candidate to a unique ref when
required for CI or a PR, matching the repository's actual workflow triggers.
Native checks run for pull requests and main pushes; a source-branch push alone does not start them.
The manual cross-repository job also needs exact trusted Runtime/TS refs.
Verify required CI using the full SHA, ref, workflow, event, run attempt and
successful job conclusions. Satisfy required reviews and signatures on that
revision; infer no check names from the TypeScript or Runtime repositories.

Fetch the destination immediately before promotion. If it advanced, retain the
old candidate and integrate, review and verify a new one. For an allowed direct
route, use an explicit fast-forward refspec:
`git push <remote> <verified-sha>:refs/heads/<destination>`.
For a required PR route, use an allowed hosting-service merge method with a
pinned expected PR head. Shared-branch force updates and protection bypasses
are outside this workflow.

Read back the remote ref and merge record. Verify the destination equals or
contains the tested candidate, or verify the resulting diff and actual SHA for
squash/rebase. A candidate push, pending PR or scheduled auto-merge is not a
completed landing.

Retain recovery branches and candidate refs by default. Report repository URL,
destination, source/base/candidate/result SHAs, PR link when applicable, exact
checks, limitations and retained refs. Distinguish pre-merge evidence from
post-merge destination CI, and wait for the latter when policy requires it.

**Done:** remote/content evidence confirms landing with all prerequisites met,
or a specific blocker is reported with recoverable source/candidate refs.

For a Delta Land action, follow the attached landing-outcome instructions and
emit the structured result only for successful landing or a terminal failure.
Skill creation, preparation and already-landed no-ops are not landing outcomes.
