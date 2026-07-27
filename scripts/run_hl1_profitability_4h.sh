#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mkdir -p target/hl1c target/hl1-profitability
bash scripts/build_hl1j_release.sh
pre_report="target/hl1c/profitability-isolation-pre.txt"
bash scripts/check_hl1c_isolation.sh | tee "$pre_report"

before_file="$(mktemp)"
find target/hl1-profitability -mindepth 1 -maxdepth 1 -type d -print | sort > "$before_file"

set +e
target/release/copytrade-observer qualify-profitability \
  --config config/copytrade.json \
  --request-policy config/read-api-policy.json \
  --transport-policy config/public-mainnet-transport.json \
  --release-manifest target/hl1j/release-manifest.json \
  --isolation-report "$pre_report" \
  --duration 4h \
  --output target/hl1-profitability &
observer_pid=$!
trap 'kill -TERM "$observer_pid" 2>/dev/null || true' INT TERM EXIT
bundle=""
for _ in {1..300}; do
  bundle="$(find target/hl1-profitability -mindepth 1 -maxdepth 1 -type d -print | sort | comm -13 "$before_file" - | tail -1)"
  if [[ -n "$bundle" ]]; then
    printf '%s\n' "$bundle" > target/hl1-profitability/current-run.txt
    printf '%s\n' "$observer_pid" > "$bundle/observer.pid"
    break
  fi
  sleep 0.1
done
wait "$observer_pid"
run_status=$?
trap - INT TERM EXIT
set -e

post_report="target/hl1c/profitability-isolation-post.txt"
bash scripts/check_hl1c_isolation.sh | tee "$post_report"

if [[ -z "$bundle" ]]; then
  bundle="$(find target/hl1-profitability -mindepth 1 -maxdepth 1 -type d -print | sort | comm -13 "$before_file" - | tail -1)"
fi
if [[ -z "$bundle" ]]; then
  echo "profitability evidence bundle was not created" >&2
  exit 1
fi
target/release/copytrade-observer finalize-qualification \
  --bundle "$bundle" \
  --isolation-report "$post_report"

if [[ "$run_status" -ne 0 ]]; then
  echo "four-hour profitability gate failed; evidence verified and diagnostic bundle retained: $bundle" >&2
  exit "$run_status"
fi

echo "four-hour unsigned profitability measurement passed: $bundle"
