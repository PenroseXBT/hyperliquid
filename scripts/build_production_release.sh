#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

# Build assurance ends here. None of these reports or hashes are runtime
# authority for the deployed daemon.
mkdir -p target/production-release
cargo fmt --all -- --check
cargo check --locked --workspace
cargo test --locked --workspace 2>&1 | tee target/production-release/test-report.txt
bash scripts/check_engine_boundary.sh 2>&1 \
  | tee target/production-release/isolation-report.txt
git diff --check
release_target="${ENGINE_RELEASE_TARGET:-x86_64-unknown-linux-musl}"

if [[ "$release_target" == "x86_64-unknown-linux-musl" ]] \
  && [[ -z "${CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER:-}" ]] \
  && command -v x86_64-linux-musl-gcc >/dev/null 2>&1; then
  export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc
fi

cargo build --locked --release -p engine --target "$release_target"
release_binary="target/$release_target/release/engine"
file "$release_binary" | grep -q 'ELF 64-bit.*x86-64'
shasum -a 256 "$release_binary" \
  > target/production-release/engine.sha256
