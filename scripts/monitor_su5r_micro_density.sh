#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

interval="${1:-10}"
while true; do
  if [[ -s target/su5r-micro-density/current-run.txt ]]; then
    run_dir="$(cat target/su5r-micro-density/current-run.txt)"
  else
    run_dir="$(find target/su5r-micro-density -mindepth 1 -maxdepth 1 -type d -print | sort | tail -1)"
  fi
  [[ -n "$run_dir" ]] || { sleep "$interval"; continue; }
  if [[ -s "$run_dir/periodic-checkpoints.jsonl" ]]; then
    checkpoint="$(tail -n 1 "$run_dir/periodic-checkpoints.jsonl")"
    if [[ -t 1 && -n "${TERM:-}" ]]; then clear || true; fi
    printf 'SU5R unsigned micro-density gate\n'
    printf 'bundle: %s\n\n' "$run_dir"
    jq -r '.payload |
      "elapsed: \(.monotonic_elapsed_ms / 1000)s / 600s\n" +
      "fresh candidates: \(.active_source_count)\n" +
      "decisions/executions: \(.decision_count)/\(.shadow_execution_count)\n" +
      "open/closed episodes: \(.open_episode_count)/\(.closed_episode_count)\n" +
      "risk violations: \(.projection_violations)\n" +
      "queue pending/in flight: \(.pending_queue)/\(.in_flight)\n" +
      "HTTP 429s/retries: \(.rate_limited_responses)/\(.retry_count)\n" +
      "protected reserve used: \(.reserved_weight_consumed)"' <<< "$checkpoint"
  fi
  if [[ -f "$run_dir/terminal-summary.json" ]] \
      && [[ "$(jq -r '.evidence_verified // false' "$run_dir/terminal-summary.json")" == "true" ]]; then
    printf '\nterminal summary finalized\n'
    jq . "$run_dir/terminal-summary.json"
    exit 0
  fi
  sleep "$interval"
done
