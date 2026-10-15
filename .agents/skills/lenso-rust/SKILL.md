---
name: lenso-rust
description: Build or modify native lenso-rust plugins and applications with typed instance dependencies, configuration, resource cleanup, Log/Cache, and optional Runtime exports. Use for Rust Lenso implementation or lifecycle fixes; use lenso-migrate for TS/Rust translation and lenso-runtime for protocol/SDK work.
---

# Native Rust Lenso

Keep business behavior in ordinary Rust functions and services. Core composes trusted native code; neither JSON contracts nor a kernel are required for local calls.

1. Read the target Cargo manifest, selected source revision, one nearby plugin and its assembly. Verify the installed crate exports before copying an API. This skill's baseline is source revision `e5dfafb`, not a crates.io release; older `f10badf` has materially different startup and admission behavior.
2. Model each dependency with the **exact** `Handle<Service>` obtained from the installed `Instance`. Declare `handle.key()` in `Plugin::dependencies`; resolve with `Setup::get`. Names and service types do not select instances. Capture handles before moving instances into `App::with`.
3. Let setup acquire resources and immediately register owned cleanup before the next fallible operation or await. Borrowed resources retain their original owner. Keep config/validation free of resource acquisition.
4. Admit external work once at its outer service/host entry with `WorkGate`; retain the guard through awaits and dependency work. Internal Log/Cache calls must not reenter that gate. A retained `Arc` is not an admission permit.
5. Start once, call ordinary methods, explicitly await shutdown while Tokio is alive. Preserve business/setup errors together with cleanup failures; do not let a trailing `?` discard the original failure.

Read only the relevant reference:

- New plugin, configuration, failure, cancellation, or drain: [Native lifecycle](references/native.md).
- Choosing shipped examples or native Log/Cache: [Existing capabilities](references/capabilities.md).
- Crossing a process/language boundary: [Optional Runtime adapter](references/runtime.md).

Validate the changed boundary with a compiling public-API consumer and an existing focused test. Exercise rollback/admission only when changing those semantics. Do not add a benchmark or an entire transport matrix for a native service edit. Record the selected revision and exact commands; source availability and successful local compilation do not establish publication.
