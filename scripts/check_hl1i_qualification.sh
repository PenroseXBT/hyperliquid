#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

cargo fmt --all -- --check
cargo test --workspace
cargo check --workspace
bash scripts/check_hl1c_isolation.sh

if rg -n \
  'HYPERLIQUID_(MASTER_)?PRIVATE_KEY[[:space:]]*[:=]' \
  config crates --glob '!compile_fail_proofs.rs'; then
  echo "private-key material or assignment found in HL1 files" >&2
  exit 1
fi

if target/release/copytrade-observer --approve-agent >/dev/null 2>&1; then
  echo "observer unexpectedly accepted an approval command" >&2
  exit 1
fi

if target/release/copytrade-observer --submit-order >/dev/null 2>&1; then
  echo "observer unexpectedly accepted a submission command" >&2
  exit 1
fi

echo "HL1I failure-injection and isolation qualification passed"
