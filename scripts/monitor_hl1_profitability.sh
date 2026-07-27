#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

interval="${1:-10}"
while true; do
  run_dir="$(cat target/hl1-profitability/current-run.txt)"
  checkpoint="$(tail -n 1 "$run_dir/periodic-checkpoints.jsonl")"
  clear
  printf 'HL1 unsigned profitability run\n'
  printf 'bundle: %s\n' "$run_dir"
  observer_pid="$(cat "$run_dir/observer.pid")"
  printf 'observer PID: %s\n\n' "$observer_pid"
  jq -r '
    .payload |
    "elapsed: \(.monotonic_elapsed_ms / 1000)s / 14400s\n" +
    "fresh candidates: \(.active_source_count)\n" +
    "decisions/executions: \(.decision_count)/\(.shadow_execution_count)\n" +
    "open/closed episodes: \(.open_episode_count)/\(.closed_episode_count)\n" +
    "risk violations: \(.projection_violations)\n" +
    "queue pending/in flight: \(.pending_queue)/\(.in_flight)\n" +
    "HTTP 429s/retries: \(.rate_limited_responses)/\(.retry_count)\n" +
    "protected reserve used: \(.reserved_weight_consumed)"
  ' <<< "$checkpoint"
  printf '\naccounting to latest checkpoint:\n'
  jq -r '
    def field_sum(field): map(.[field] | tonumber) | add // 0;
    "actions: \(length)\n" +
    "fees: \(field_sum("fees"))\n" +
    "funding: \(field_sum("funding"))\n" +
    "slippage: \(field_sum("execution_slippage"))\n" +
    "action net deltas: \(field_sum("net_pnl_delta"))"
  ' "$run_dir/shadow-actions.json"
  printf 'five-minute buckets: '
  jq -r 'length' "$run_dir/five-minute-return-buckets.json"
  if ! kill -0 "$observer_pid" 2>/dev/null && [[ ! -f "$run_dir/terminal-summary.json" ]]; then
    printf '\nobserver is not alive and no terminal summary exists\n'
    printf 'this run is interrupted/failed; the displayed checkpoint is stale\n'
    printf 'last checkpoint mtime: '
    stat -f '%Sm' -t '%Y-%m-%dT%H:%M:%S%z' "$run_dir/periodic-checkpoints.jsonl"
    exit 1
  fi
  if [[ -f "$run_dir/terminal-summary.json" ]]; then
    verified="$(jq -r '.evidence_verified // false' "$run_dir/terminal-summary.json")"
    if [[ "$verified" != "true" ]]; then
      printf '\nobserver stopped; external evidence finalization is still in progress\n'
      sleep "$interval"
      continue
    fi
    printf '\nterminal summary finalized\n'
    jq '.' "$run_dir/terminal-summary.json"
    exit 0
  fi
  sleep "$interval"
done
