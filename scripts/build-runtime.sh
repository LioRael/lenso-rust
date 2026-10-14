#!/bin/sh
set -eu

usage() {
  echo "usage: $0 --runtime-root PATH [--out PATH] [--target-dir PATH] [--bin NAME] [--release]" >&2
  exit 2
}

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
caller_dir=$(pwd)
runtime_root=
out="$repo_root/target/debug/logs-batch"
target_dir="$repo_root/target"
bin=logs-batch
profile=debug

while [ "$#" -gt 0 ]; do
  case "$1" in
    --runtime-root) [ "$#" -ge 2 ] || usage; runtime_root=$2; shift 2 ;;
    --out) [ "$#" -ge 2 ] || usage; out=$2; shift 2 ;;
    --target-dir) [ "$#" -ge 2 ] || usage; target_dir=$2; shift 2 ;;
    --bin) [ "$#" -ge 2 ] || usage; bin=$2; shift 2 ;;
    --release) profile=release; shift ;;
    *) usage ;;
  esac
done

[ -n "$runtime_root" ] || usage
runtime_root=$(CDPATH= cd -- "$runtime_root" && pwd)
[ "${out#/}" != "$out" ] || out="$caller_dir/$out"
[ "${target_dir#/}" != "$target_dir" ] || target_dir="$caller_dir/$target_dir"
[ -f "$runtime_root/tools/generate.ts" ] || { echo "missing Runtime generator: $runtime_root/tools/generate.ts" >&2; exit 1; }
case "$bin" in logs-batch|logs-client) ;; *) echo "unsupported bin: $bin" >&2; exit 2 ;; esac

child_pid=
interrupt() {
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  exit 1
}
trap interrupt HUP INT TERM
stage="$repo_root/target/.lenso/runtime-rust-stage"
mkdir -p "$stage"
if ! cmp -s "$repo_root/Cargo.lock" "$stage/.source-Cargo.lock"; then
  cp -p "$repo_root/Cargo.lock" "$stage/Cargo.lock"
  cp -p "$repo_root/Cargo.lock" "$stage/.source-Cargo.lock"
fi
mkdir -p "$stage/crates" "$stage/examples/batch" "$stage/examples/contracts" "$stage/examples/generated"
# Preserve unchanged timestamps; changed content gets full-resolution write times.
bun "$repo_root/scripts/stage-native.ts" "$repo_root" "$stage" &
child_pid=$!
wait "$child_pid"
child_pid=

# The stage alone receives candidate Runtime-generated source and canonical contract.
if [ ! -f "$stage/examples/generated/logs.rs" ] ||
   ! cmp -s "$runtime_root/examples/contracts/logs.json" "$stage/.source-contract.json" ||
   ! cmp -s "$runtime_root/examples/contracts/logs.json" "$stage/examples/contracts/logs.json" ||
   ! cmp -s "$runtime_root/tools/generate.ts" "$stage/.generator.ts"; then
  (cd "$runtime_root" && exec bun tools/generate.ts examples/contracts/logs.json "$stage/examples/generated" logs) &
  child_pid=$!
  wait "$child_pid"
  child_pid=
  cp -p "$runtime_root/examples/contracts/logs.json" "$stage/examples/contracts/logs.json"
  cp -p "$runtime_root/tools/generate.ts" "$stage/.generator.ts"
  cp -p "$runtime_root/examples/contracts/logs.json" "$stage/.source-contract.json"
fi

mkdir -p "$(dirname -- "$out")" "$target_dir"
tmp_out="${out}.tmp.$$"
trap 'rm -f "$tmp_out"' EXIT
if [ "$profile" = release ]; then set -- --release; else set --; fi
(
  cd "$stage"
  exec cargo build -p logs-batch --bin "$bin" "$@" \
    --target-dir "$target_dir" \
    --config "patch.\"https://github.com/LioRael/lenso-runtime\".runtime-sdk.path=\"$runtime_root/crates/sdk\""
) &
child_pid=$!
wait "$child_pid"
child_pid=
trap - HUP INT TERM
cp "$target_dir/$profile/$bin" "$tmp_out"
chmod +x "$tmp_out"
mv -f "$tmp_out" "$out"
trap - EXIT HUP INT TERM
printf '%s\n' "$out"
