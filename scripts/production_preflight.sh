#!/usr/bin/env bash
set -euo pipefail

role="$1"
manifest="$2"
manifest_digest="$3"
observer="$4"
signer="$5"
ipc_policy="$6"
topology="$7"
config="$8"

for file in "$manifest" "$manifest_digest" "$observer" "$signer" "$ipc_policy" "$topology" "$config"; do
  [[ -f "$file" ]] || { echo "missing production artifact: $file" >&2; exit 1; }
done

expected_manifest="$(awk '{print $1}' "$manifest_digest")"
actual_manifest="$(sha256sum "$manifest" | awk '{print $1}')"
[[ "$expected_manifest" == "$actual_manifest" ]] || { echo "manifest digest mismatch" >&2; exit 1; }

[[ "$(jq -r .git_tree_state "$manifest")" == clean ]] || { echo "manifest tree is not clean" >&2; exit 1; }
[[ "$(jq -r .qualification_stage "$manifest")" == PRODUCTION_RELEASE ]] || { echo "not a production manifest" >&2; exit 1; }
[[ "$(jq -r .global_risk_scale "$manifest")" == 0.1 ]] || { echo "unexpected production GRS" >&2; exit 1; }
[[ "$(sha256sum "$observer" | awk '{print $1}')" == "$(jq -r .observer_binary_sha256 "$manifest")" ]] || { echo "observer binary mismatch" >&2; exit 1; }
[[ "$(sha256sum "$signer" | awk '{print $1}')" == "$(jq -r .signer_binary_sha256 "$manifest")" ]] || { echo "signer binary mismatch" >&2; exit 1; }
[[ "$(sha256sum "$ipc_policy" | awk '{print $1}')" == "$(jq -r .ipc_policy_sha256 "$manifest")" ]] || { echo "IPC policy mismatch" >&2; exit 1; }
[[ "$(sha256sum "$topology" | awk '{print $1}')" == "$(jq -r .linux_topology_policy_sha256 "$manifest")" ]] || { echo "topology policy mismatch" >&2; exit 1; }
[[ "$(jq -r .resolved "$topology")" == true ]] || { echo "topology policy unresolved" >&2; exit 1; }
! grep -q REPLACE_WITH_REVIEWED_PHYSICAL_CPUS "$topology" || { echo "CPU affinity unresolved" >&2; exit 1; }

config_fingerprint="$($observer --config "$config" --print-build-manifest | sed -n 's/.* config=//p')"
[[ "$config_fingerprint" == "$(jq -r .configuration_sha256 "$manifest")" ]] || { echo "candidate configuration mismatch" >&2; exit 1; }

case "$role" in
  observer|signer) ;;
  *) echo "invalid preflight role" >&2; exit 1 ;;
esac
