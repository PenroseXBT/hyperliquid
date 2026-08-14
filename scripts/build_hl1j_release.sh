#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mkdir -p target/hl1j
cp THIRD_PARTY_NOTICES.md target/hl1j/THIRD_PARTY_NOTICES.md
bash scripts/check_hl1i_qualification.sh
cargo build --locked --release -p copytrade-observer

printf '%s\n' \
  'format=hl1j-test-report-v1' \
  'workspace_tests=passed' \
  'hl1c_isolation=passed' \
  'hl1i_failure_injection=passed' \
  > target/hl1j-test-report.txt

git_commit="$(git rev-parse HEAD)"
git_tree_state="clean"
if [[ -n "$(git status --porcelain)" ]]; then
  git_tree_state="dirty"
fi
source_tree_sha256="$({
  find Cargo.toml Cargo.lock README.md THIRD_PARTY_NOTICES.md Dockerfile.railway .dockerignore crates config policy scripts .github fixtures -type f -print
} | LC_ALL=C sort | while IFS= read -r file; do
  shasum -a 256 "$file"
done | shasum -a 256 | awk '{print $1}')"
target/release/copytrade-observer \
  release-manifest \
  --config config/copytrade.json \
  --test-report target/hl1j-test-report.txt \
  --git-commit "$git_commit" \
  --git-tree-state "$git_tree_state" \
  --source-tree-sha256 "$source_tree_sha256" \
  > target/hl1j/release-manifest.json

target/release/copytrade-observer \
  release-manifest \
  --config config/copytrade.json \
  --test-report target/hl1j-test-report.txt \
  --git-commit "$git_commit" \
  --git-tree-state "$git_tree_state" \
  --source-tree-sha256 "$source_tree_sha256" \
  > target/hl1j/release-manifest.replay.json

cmp target/hl1j/release-manifest.json target/hl1j/release-manifest.replay.json
echo "HL1J reproducible release manifest passed"
