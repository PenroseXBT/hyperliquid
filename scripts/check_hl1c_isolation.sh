#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mkdir -p target/hl1c

cargo build --locked --release -p copytrade-core -p copytrade-observer
cargo test --locked -p copytrade-core -p copytrade-observer
cargo test --locked --doc -p copytrade-observer

cargo tree --locked -p copytrade-core --edges normal,build \
  > target/hl1c/core-dependencies.txt
cargo tree --locked -p copytrade-observer --edges normal,build \
  > target/hl1c/observer-dependencies.txt
cargo tree --locked -p copytrade-signer --edges normal,build \
  > target/hl1c/signer-dependencies.txt

while IFS= read -r package; do
  [[ -z "$package" || "$package" == \#* ]] && continue
  if sed -E 's/^[^[:alnum:]_]+//' \
    target/hl1c/core-dependencies.txt target/hl1c/observer-dependencies.txt \
    | awk '{print $1}' | grep -F -x "$package" >/dev/null; then
    echo "forbidden package present in HL1 dependency closure: $package" >&2
    exit 1
  fi

  reverse_report="target/hl1c/reverse-${package}.txt"
  cargo tree --locked -p copytrade-observer -i "$package" >"$reverse_report" 2>&1 || true
  if grep -F "copytrade-observer v" "$reverse_report" >/dev/null; then
    echo "forbidden reverse dependency reaches observer: $package" >&2
    exit 1
  fi
done < policy/hl1c-forbidden-packages.txt

# MFCE owns native model execution in the observer only. The signer depends on
# core, so this closure check also prevents LightGBM from entering core and
# crossing the signing boundary transitively.
for package in copytrade-mfce lightgbm3 lightgbm3-sys; do
  if sed -E 's/^[^[:alnum:]_]+//' target/hl1c/signer-dependencies.txt \
    | awk '{print $1}' | grep -F -x "$package" >/dev/null; then
    echo "observer-only model package present in signer dependency closure: $package" >&2
    exit 1
  fi
done

while IFS= read -r symbol; do
  [[ -z "$symbol" || "$symbol" == \#* ]] && continue
  source_pattern="(use .*\\b${symbol}\\b|::${symbol}\\b|\\b${symbol}[[:space:]]*[<(]|struct[[:space:]]+${symbol}\\b|trait[[:space:]]+${symbol}\\b|fn[[:space:]]+${symbol}\\b)"
  if rg -n "$source_pattern" crates/copytrade-core/src crates/copytrade-observer/src \
    --glob '!compile_fail_proofs.rs' > target/hl1c/forbidden-source-hit.txt; then
    echo "forbidden mutation symbol present in HL1 source: $symbol" >&2
    cat target/hl1c/forbidden-source-hit.txt >&2
    exit 1
  fi
done < policy/hl1c-forbidden-source-symbols.txt

nm target/release/copytrade-observer > target/hl1c/observer-symbols.txt
strings target/release/copytrade-observer > target/hl1c/observer-strings.txt

while IFS= read -r pattern; do
  [[ -z "$pattern" || "$pattern" == \#* ]] && continue
  if grep -F "$pattern" target/hl1c/observer-symbols.txt \
    target/hl1c/observer-strings.txt >/dev/null; then
    echo "forbidden mutation pattern present in observer artifact: $pattern" >&2
    exit 1
  fi
done < policy/hl1c-forbidden-artifact-patterns.txt

echo "HL1C isolation policy passed"
