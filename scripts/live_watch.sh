#!/usr/bin/env bash
# live_watch.sh — SU6 Railway session terminal interface.
#
# Only two commands. No extra monitoring stack.
#
#   bash scripts/live_watch.sh watch   # live filtered tail (default)
#   bash scripts/live_watch.sh fail    # failure dump from persisted volume artifacts
#   bash scripts/live_watch.sh logs    # unfiltered tail
#
# Or source for shell functions:
#   source scripts/live_watch.sh
#   hlwatch
#   hlfail
#
# Canonical production generation SYSTEM_V1. Override only for local testing:
#   SU6_DATA_ROOT=/data/system-v1 bash scripts/live_watch.sh fail

set -euo pipefail

SU6_DATA_ROOT="${SU6_DATA_ROOT:-/data/system-v1}"
WATCH_PATTERN='execution_state=|unexpected_exit|RISK_ONLY|ERROR|WARN|failed=true|fatal|panic|IdentityMismatch|InvalidExchangeState|source_.*failed|submission|fill|order|position|MFCE|HIP.?3'

hlwatch() {
    # Filtered live view. Railway retains the full logs; this only focuses the terminal.
    if command -v rg >/dev/null 2>&1; then
        railway logs --deployment latest \
            | rg --line-buffered "${1:-$WATCH_PATTERN}"
    else
        railway logs --deployment latest \
            | grep --line-buffered -E "${1:-$WATCH_PATTERN}"
    fi
}

hlfail() {
    # Persisted diagnostics maintained by scripts/run_su6_railway.sh.
    # Safe to run when the service is stopped: unexpected exits stay stopped
    # (restartPolicyType=NEVER) precisely so you can inspect these first.
    local root="${1:-$SU6_DATA_ROOT}"
    echo "=== CURRENT RUN ==="
    railway run cat "$root/failure/current-run.meta"
    echo
    echo "=== LAST EXIT ==="
    railway run cat "$root/failure/last-exit.meta" 2>/dev/null || true
    echo
    echo "=== LAST STDERR ==="
    railway run tail -c 20000 "$root/failure/last-exit.stderr" 2>/dev/null || true
}

hllogs() {
    railway logs --deployment latest
}

if [[ "${BASH_SOURCE[0]:-}" == "$0" ]]; then
    cmd="${1:-watch}"
    case "$cmd" in
        watch) hlwatch "${2:-$WATCH_PATTERN}" ;;
        fail) hlfail "${2:-$SU6_DATA_ROOT}" ;;
        logs) hllogs ;;
        *) printf 'usage: %s [watch|fail|logs]\n' "$0" >&2; exit 1 ;;
    esac
fi
