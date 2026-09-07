#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

cargo test --locked --workspace
cargo check --locked --workspace --all-targets

echo "engine authority and MFCE dependency boundary passed"
