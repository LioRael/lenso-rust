# First-wave verification record

This is an unpublished working-tree candidate, not a release. The checked
baseline heads were:

| Repository | Baseline HEAD |
| --- | --- |
| lenso-rust | `2abf4ae5af18d8e1699816d4c31e779685a88197` |
| lenso | `d86dc30e0cfceab692c551411844dc8a41afb086` |
| lenso-runtime | `cc4a5af938e534b34371e4a9771190cade601a51` |

The uncommitted candidate bytes, not these baseline commits alone, produced the
verified binaries. The optimized native batch sidecar recorded:

- Runtime build-input inventory SHA-256:
  `036016db3fceb24912eb80c2def46b81f20fb0925e214d3b5ea6d1c075f243c4`
- Native build-input inventory SHA-256:
  `2275df3c955e51c53ddd5feb6b6f287c0360d75ebaa31a6ecc5b76ea5a6f2d60`
- Native release binary SHA-256:
  `8f4b780765adbbe4e6d5a1856422131ae8109c38fc01436a038eb8c4e693dab9`

Inventory digests hash `JSON.stringify(proof.files)` and
`JSON.stringify(proof.native_files)`. Sidecars also record actual Cargo profile
and build wall time. Runtime source, native source, SDK output, installed SDK
manifest/entry, canonical contract and generator were checked without fallback.
Formal Rust Git pins and TS pinned preparation remain available and unchanged.
Selected local paths exist only in ignored candidate artifacts and command env.

## Executed checks

On macOS/Darwin 27.0.0 arm64, Apple M2 Pro, Bun 1.4.3,
Rust 1.101.0-nightly:

- `cargo test --workspace --locked` in lenso-rust: **79 tests + 8 doctests passed**.
- `cargo clippy --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo fmt --all --check` and `git diff --check`: passed.
- Both Log examples and the native Cache example ran successfully.
- Staged debug `logs-batch`/`logs-client` and optimized `logs-batch` built against
  the explicitly selected current Runtime. The native JSON call produced
  `{"count":1,"error_count":1,"total_duration_ms":12}` with no Runtime process.
- `bun test scripts/build-runtime.test.ts`: **2 tests, 22 assertions passed**,
  including changed source/binary rejection, profile mismatch, omitted-input
  regression, generation/build failure preserving prior binaries, and lock safety.
- TS frozen install, affected framework/dependency builds, example generation
  and `examples/runtime` typecheck passed. Relevant Cache/Log/Core/example tests:
  **67 passed**. Preparation tests: **8 passed**. Changed TS files passed
  `oxfmt --check`, `oxlint --deny-warnings`; runtime-script typecheck passed.
- Runtime SDK build, generator, typecheck and SDK/generator tests:
  **17 tests passed**. Runtime's locked Rust workspace tests: **39 passed**.
- Actual cross-source `bun run smoke`: passed, including generated clients in
  both directions, typed business error, invalid contract/data, stream credit,
  deadline, abort, disconnect and drained admission.
- Actual `bun run verify:lenso`: passed using the real CLI workspace. Setup
  precedes ready; TS reload keeps Rust; failed Rust build retains the callable
  old instance; compatible cutover retains an already assigned stream;
  incompatible promotion leaves routing unchanged; final owned port was released.

The deliberate build failure occurs only inside the verifier's owned fixture,
not in developer business source. The TS source watcher trigger changes a
timestamp and restores it if source bytes are unchanged. No unknown listener
or process was stopped. `TMPDIR=/tmp` worked around the local mbx wrapper's Unix
socket path-length limit; it is not an application requirement.

Review found and regression checks now cover startup delivery acknowledgement,
configuration map-key/error-value leakage, watch-lock reentry during cache close,
compiled README/root tsconfig proof inputs, and build-profile provenance.

## Four-path sample

Executed from Runtime after all delegated builds finished:

```sh
PERF_REQUESTS=1000 PERF_RECORDS=1000 PERF_CONCURRENCY=8 bun run perf
```

`LENSO_ROOT` and `LENSO_RUST_ROOT` selected the prepared source workspaces.
All four paths used the same 1000-record input, 1000 measured requests, concurrency
8 and 100 excluded warmup calls. Payload sizes were **60915 input bytes / 59 output
bytes**, excluding framing. Cache TTL was zero: no stored-value hits, but
overlapping same-key calls can still coalesce. This measures equal public request
workloads, not a fixed loader execution count.

| Path | First operation ms | Warm p50 / p95 / p99 ms | Requests/s | Errors |
| --- | ---: | ---: | ---: | ---: |
| Native TS | 1.111 | 0.470 / 0.824 / 0.875 | 15627 | 0/1000 |
| Native Rust | 0.291 | 0.277 / 0.557 / 0.662 | 20523 | 0/1000 |
| Runtime → TS | 7.914 | 8.823 / 9.458 / 9.947 | 899 | 0/1000 |
| Runtime → Rust | 3.873 | 9.008 / 9.517 / 9.788 | 883 | 0/1000 |

Every output was checked for equality. Both remote business timing collections
were complete. First-operation timings are single calls after App/transport
startup, not cold compilation or process-start latency. Native Rust per-operation
timing excludes preparation/cloning of its owned input; native TS passes its
existing input object. All paths still serialize local cache keys/values.
Remote timing additionally includes conversion, framing, schema validation,
queueing, transport and decoding. These timing boundaries are not interchangeable.

Recorded kernel build wall time was **222 ms** and native optimized App build
wall time **6846 ms**, with existing compiler caches. Native staging/generation
is included in its build wall time; neither is included in request latency.
TS build wall time was not measured. This was one non-isolated machine sample,
not a general Rust performance claim. The cheap aggregation is dominated by
transport on remote paths; Runtime→Rust did not outperform Runtime→TS here.

Development verifier feedback, also one cached sample: startup 3920 ms,
TS invalidation 316 ms, failed Rust build notification 414 ms, compatible native
rebuild/readiness/cutover 1817 ms. These are feedback observations, not promises.

## Reproduce the candidate

Set explicit trusted source roots rather than relying on sibling discovery:

```sh
# Runtime:
bun install --frozen-lockfile --ignore-scripts
bun run build:sdk
bun run generate
bun scripts/local.ts debug
bun scripts/local.ts release

# TypeScript Lenso:
bun scripts/prepare-runtime.ts --runtime-root "$LENSO_RUNTIME_ROOT"
bun install --frozen-lockfile --ignore-scripts
bun x --no-install turbo run build --filter=@lenso/cli... --filter=@lenso/runtime... \
  --filter=@lenso/cache... --filter=@lenso/log...
bun run --cwd examples/runtime generate

# lenso-rust:
sh scripts/build-runtime.sh --runtime-root "$LENSO_RUNTIME_ROOT"
sh scripts/build-runtime.sh --runtime-root "$LENSO_RUNTIME_ROOT" \
  --bin logs-client --out target/debug/logs-client
sh scripts/build-runtime.sh --runtime-root "$LENSO_RUNTIME_ROOT" \
  --release --out target/release/logs-batch

# Runtime, with LENSO_ROOT and LENSO_RUST_ROOT also set:
bun run smoke
bun run verify:lenso
PERF_REQUESTS=1000 PERF_RECORDS=1000 PERF_CONCURRENCY=8 bun run perf
```

Hosted GitHub CI was added but not executed. Windows, Linux, declared minimum
Rust versions, production deployment and cross-machine performance were not
validated here. No main merge, push, npm/crates publication or credentials
were created.
