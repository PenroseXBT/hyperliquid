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
cargo build --locked --release -p engine
shasum -a 256 target/release/engine \
  > target/production-release/engine.sha256
