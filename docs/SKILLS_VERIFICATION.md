# Rust skill evidence

Date: 2026-10-15 UTC. Local documentation/skill delivery only. No core/SDK/generator source or root lockfiles changed; no global skill installation, push, merge, publication or deployment. Skill entrypoints use progressive references and passed skill-creator quick_validate; relative links resolve.

Source baseline: e5dfafbf9e7e96ec7593c66ae108b227e6629d74. Skill: `.agents/skills/lenso-rust`; trigger: native plugin/application implementation, lifecycle and optional adapter wiring. The only other edit clarifies that Core `on_cleanup` returns CleanupHandle (using it is optional) in `crates/plugin-core/README.md`.

Actual commands used installed Cargo with `CARGO_TARGET_DIR=/tmp/lenso-skills-cargo`, cached dependencies and `--offline --locked`:

- `cargo test -p lenso-plugin-core --doc`: 3 passed.
- `cargo run -p lenso-cache --example cache-native`: `Summary { count: 3, total: 16 }`.
- `cargo run -p lenso-log --example injected`: structured stderr greeting for Ada; exit 0.
- skill-creator quick_validate and relative reference checks passed.

A separate agent used this skill to implement a native consumer in `/tmp/lenso-rust-skill-acceptance.f6Cldw`: two same-type catalogs, exact handles, setup-owned resources, WorkGate at outer query entry, shutdown and retained-Arc refusal. First public-API compilation passed; output distinguished North Ada/South Grace, both retained services returned AccessError::Closing after shutdown and resource read count stayed 4. Its existing focused lifecycle test passed (1 test). No framework code/lock was changed. Full local report: `ACCEPTANCE.md` in that temporary directory.

That acceptance found the old offline aggregation example is useful for composition but not a complete gate/error-retention template; references now say so. The new TS/Rust paired fixture (owned by the TS migration skill) separately passed normal/rollback checks against this Core. Its evidence is recorded in the TS repo's `docs/SKILLS_VERIFICATION.md`.

No network/native Runtime integration was rerun. The five new native plugins are not part of this baseline and are not advertised as available. Older f10badf cancellation behavior is explicitly distinguished from current Core's owned startup.
