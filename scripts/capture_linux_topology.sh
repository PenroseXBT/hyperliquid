#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Linux host topology capture is required" >&2
  exit 1
fi

output="${1:-deploy/topology/production-host.json}"
nic="${2:-eth0}"
mkdir -p "$(dirname "$output")"
nic_node="$(cat "/sys/class/net/$nic/device/numa_node")"
if [[ "$nic_node" == "-1" ]]; then
  echo "NIC NUMA node is unresolved" >&2
  exit 1
fi

cpu_rows="$(lscpu -p=CPU,CORE,SOCKET,NODE,ONLINE | grep -v '^#')"
if [[ -z "$cpu_rows" ]]; then
  echo "CPU topology is empty" >&2
  exit 1
fi

host="$(hostname)"
kernel="$(uname -r)"
cpu_sha="$(printf '%s\n' "$cpu_rows" | sha256sum | awk '{print $1}')"
irq_sha="$(grep -E "$nic|mlx|ixgbe|i40e|ena|virtio" /proc/interrupts | sha256sum | awk '{print $1}')"
cat > "$output" <<EOF
{
  "schema_version": 1,
  "resolved": true,
  "host": "$host",
  "kernel": "$kernel",
  "nic": "$nic",
  "nic_numa_node": $nic_node,
  "cpu_topology_sha256": "$cpu_sha",
  "nic_irq_map_sha256": "$irq_sha",
  "observer_cpu_affinity": "REPLACE_WITH_REVIEWED_PHYSICAL_CPUS",
  "signer_cpu_affinity": "REPLACE_WITH_REVIEWED_PHYSICAL_CPUS"
}
EOF

if grep -q REPLACE_WITH_REVIEWED_PHYSICAL_CPUS "$output"; then
  echo "captured topology; operator must select and review physical CPU affinity: $output" >&2
  exit 2
fi
