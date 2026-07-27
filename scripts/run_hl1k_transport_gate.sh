#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mkdir -p target/hl1c target/hl1k-transport
bash scripts/build_hl1j_release.sh
bash scripts/check_hl1c_isolation.sh | tee target/hl1c/transport-gate-isolation-pre.txt

set +e
target/release/copytrade-observer qualify-transport \
  --config config/copytrade.json \
  --request-policy config/read-api-policy.json \
  --transport-policy config/public-mainnet-transport.json \
  --release-manifest target/hl1j/release-manifest.json \
  --isolation-report target/hl1c/transport-gate-isolation-pre.txt \
  --duration 10m \
  --output target/hl1k-transport
gate_status=$?
set -e

bash scripts/check_hl1c_isolation.sh | tee target/hl1c/transport-gate-isolation-post.txt
if [[ "$gate_status" -ne 0 ]]; then
  echo "HL1K transport gate failed; diagnostic bundle retained" >&2
  exit "$gate_status"
fi
echo "HL1K transport gate passed"
