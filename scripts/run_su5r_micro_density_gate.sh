#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mkdir -p target/hl1c target/su5r-micro-density
bash scripts/build_hl1j_release.sh

pre_report="target/hl1c/su5r-isolation-pre.txt"
bash scripts/check_hl1c_isolation.sh | tee "$pre_report"

before_file="$(mktemp)"
find target/su5r-micro-density -mindepth 1 -maxdepth 1 -type d -print | sort > "$before_file"

set +e
target/release/copytrade-observer qualify-micro-density \
  --config config/copytrade.json \
  --request-policy config/read-api-policy.json \
  --transport-policy config/public-mainnet-transport.json \
  --release-manifest target/hl1j/release-manifest.json \
  --isolation-report "$pre_report" \
  --duration 10m \
  --output target/su5r-micro-density
run_status=$?
set -e

bundle="$(find target/su5r-micro-density -mindepth 1 -maxdepth 1 -type d -print | sort | comm -13 "$before_file" - | tail -1)"
if [[ -z "$bundle" ]]; then
  echo "micro-density evidence bundle was not created" >&2
  exit 1
fi
printf '%s\n' "$bundle" > target/su5r-micro-density/current-run.txt

post_report="target/hl1c/su5r-isolation-post.txt"
bash scripts/check_hl1c_isolation.sh | tee "$post_report"
target/release/copytrade-observer finalize-qualification \
  --bundle "$bundle" \
  --isolation-report "$post_report"

if [[ "$run_status" -ne 0 ]]; then
  echo "micro-density gate failed; verified diagnostic bundle retained: $bundle" >&2
  exit "$run_status"
fi

echo "SU5R unsigned micro-density gate passed: $bundle"
