# Lenso Rust

- Start at `Cargo.toml`, the affected crate's README/PARITY, and
  `.agents/skills/lenso-rust/SKILL.md`. Runtime owns the kernel, wire contract,
  SDK and generator; preserve the formal Git revision and lockfile unless the
  dependency update is in scope.
- Keep business behavior native. Validate config/dependency graphs before
  acquiring resources; initialize resources at startup, never on a request path.
  Register owned cleanup immediately. Exact instance handles select dependencies;
  borrowed resources retain their host owner.
- Admit external work once with WorkGate and retain its guard until actual work
  settles. Shutdown drains accepted work before freeing dependencies; cancelling
  a waiter does not roll back effects or cancel shared cleanup.
- Reuse verified request identity within a request; resource/action authorization
  and revocation checks still belong to the owning service. Never cache an allow
  as proof for a later changed row or principal.
- Iterate with the affected crate's existing tests. Final native checks are locked
  workspace tests, workspace Clippy (all targets/features), and formatting, as in
  `.github/workflows/check.yml`. Tests already compile; avoid redundant fetch/build.
  Staging/generator changes additionally require the selected Runtime builder
  regressions and applicable smoke/development lifecycle checks.
- Preserve authorization/revocation, audit idempotency, storage consistency,
  cancellation/drain and protocol regression coverage. Prefer observable outcomes
  over mock call counts; merge duplicate cases only after reading their assertions.
- Measure actual I/O, serial round trips and request latency separately from build
  and test time. Label cold/warm caches, exact source/companion SHAs and unrun checks.
  Use the built-in browser or isolated headless browser if needed; never launch
  the user's Chrome. Report local, merged, published and deployed evidence separately.
