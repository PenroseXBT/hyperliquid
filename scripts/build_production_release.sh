#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

topology_policy="deploy/topology/production-host.json"
mkdir -p target/production-release
cp THIRD_PARTY_NOTICES.md target/production-release/THIRD_PARTY_NOTICES.md
cargo fmt --all -- --check
cargo check --locked --workspace
cargo test --locked --workspace 2>&1 | tee target/production-release/test-report.txt
git diff --check
cargo build --locked --release -p copytrade-observer -p copytrade-signer
{
  shasum -a 256 deploy/systemd/hype-signer.socket
  shasum -a 256 deploy/systemd/hype-signer.service
  shasum -a 256 deploy/systemd/hype-observer.service
} > target/production-release/systemd-units.sha256

find Cargo.toml Cargo.lock README.md THIRD_PARTY_NOTICES.md common_ticker.rs src crates config policy scripts fixtures -type f -print \
  | LC_ALL=C sort \
  | while IFS= read -r file; do shasum -a 256 "$file"; done \
  > target/production-release/source-files.sha256
source_tree_sha256="$(shasum -a 256 target/production-release/source-files.sha256 | awk '{print $1}')"
{
  shasum -a 256 config/copytrade.json
  shasum -a 256 config/read-api-policy.json
  shasum -a 256 config/public-mainnet-transport.json
  shasum -a 256 policy/production-market-rules-v1.json
  shasum -a 256 policy/production-dynamic-floor-v1.json
  shasum -a 256 policy/production-ioc-pricing-v1.json
  shasum -a 256 policy/production-ipc-v1.json
} > target/production-release/runtime-configuration.sha256
git_tree_state="clean"
if [[ -n "$(git status --porcelain)" ]]; then
  git_tree_state="dirty"
fi

manifest_arguments=(
  config/copytrade.json
  target/release/copytrade-observer
  target/release/copytrade-signer
  target/production-release/test-report.txt
  policy/production-market-rules-v1.json
  policy/production-dynamic-floor-v1.json
  policy/production-ioc-pricing-v1.json
  policy/production-ipc-v1.json
  target/production-release/systemd-units.sha256
  "$topology_policy"
  "$(git rev-parse HEAD)"
  "$git_tree_state"
  "$source_tree_sha256"
  target/production-release/source-files.sha256
  target/production-release/runtime-configuration.sha256
  1
  1
  1
  1
)
cargo run --locked --quiet -p copytrade-core --example create_production_manifest -- \
  "${manifest_arguments[@]}" > target/production-release/release-manifest.json
cargo run --locked --quiet -p copytrade-core --example create_production_manifest -- \
  "${manifest_arguments[@]}" > target/production-release/release-manifest.replay.json
cmp target/production-release/release-manifest.json \
  target/production-release/release-manifest.replay.json
find Cargo.toml Cargo.lock README.md THIRD_PARTY_NOTICES.md common_ticker.rs src crates config policy scripts fixtures -type f -print \
  | LC_ALL=C sort \
  | while IFS= read -r file; do shasum -a 256 "$file"; done \
  > target/production-release/source-files.post-build.sha256
cmp target/production-release/source-files.sha256 \
  target/production-release/source-files.post-build.sha256
shasum -a 256 target/production-release/release-manifest.json \
  > target/production-release/release-manifest.sha256
shasum -a 256 target/release/copytrade-observer target/release/copytrade-signer
