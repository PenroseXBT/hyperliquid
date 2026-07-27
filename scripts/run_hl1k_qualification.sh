#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mkdir -p target/hl1c target/hl1k
bash scripts/build_hl1j_release.sh
bash scripts/check_hl1c_isolation.sh | tee target/hl1c/isolation-report.txt

before_file="$(mktemp)"
find target/hl1k -mindepth 1 -maxdepth 1 -type d -print | sort > "$before_file"

target/release/copytrade-observer qualify-mainnet \
  --config config/copytrade.json \
  --request-policy config/read-api-policy.json \
  --transport-policy config/public-mainnet-transport.json \
  --release-manifest target/hl1j/release-manifest.json \
  --isolation-report target/hl1c/isolation-report.txt \
  --duration 24h \
  --output target/hl1k

post_report="target/hl1c/isolation-report-post.txt"
bash scripts/check_hl1c_isolation.sh | tee "$post_report"

bundle="$(find target/hl1k -mindepth 1 -maxdepth 1 -type d -print | sort | comm -13 "$before_file" - | tail -1)"
if [[ -z "$bundle" ]]; then
  echo "HL1K evidence bundle was not created" >&2
  exit 1
fi
target/release/copytrade-observer finalize-qualification \
  --bundle "$bundle" \
  --isolation-report "$post_report"

echo "HL1K qualification passed: $bundle"
