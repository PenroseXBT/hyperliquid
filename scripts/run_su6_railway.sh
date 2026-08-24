#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly OBSERVER="/app/bin/copytrade-observer"
readonly CONFIG="/app/config/copytrade.json"
readonly VERY_PROFITABLE_LAYER="/app/config/very-profitable-layer.json"
readonly READ_POLICY="/app/policy/read-api-policy.json"
readonly TRANSPORT_POLICY="/app/policy/public-mainnet-transport.json"

readonly VOLUME_ROOT="${RAILWAY_VOLUME_MOUNT_PATH:?RAILWAY_VOLUME_MOUNT_PATH is required}"
readonly DATA_ROOT_NAME="${SU6_DATA_ROOT_NAME:?SU6_DATA_ROOT_NAME is required}"
[[ "$DATA_ROOT_NAME" =~ ^[a-zA-Z0-9._-]+$ ]] || {
    printf 'invalid SU6_DATA_ROOT_NAME\n' >&2
    exit 1
}
readonly DATA_ROOT="${VOLUME_ROOT}/${DATA_ROOT_NAME}"
readonly STATE_ROOT="${DATA_ROOT}/state"
readonly RUNTIME_ROOT="${DATA_ROOT}/runtime"
readonly FAILURE_ROOT="${DATA_ROOT}/failure"
readonly PROCESS_STDERR="${FAILURE_ROOT}/process.stderr"
readonly EXECUTION_MODE="${SU6_EXECUTION_MODE:-shadow}"

[[ "$EXECUTION_MODE" == "shadow" || "$EXECUTION_MODE" == "live" ]] || {
    printf 'SU6_EXECUTION_MODE must be shadow or live\n' >&2
    exit 1
}

OBSERVER_PID=""
STOP_REQUESTED=0

log() {
    printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1"
}

stop_observer() {
    STOP_REQUESTED=1
    if [[ -n "$OBSERVER_PID" ]] && kill -0 "$OBSERVER_PID" 2>/dev/null; then
        kill -INT "$OBSERVER_PID" 2>/dev/null || true
    fi
}

trap stop_observer INT TERM

mkdir -p "$STATE_ROOT" "$RUNTIME_ROOT" "$FAILURE_ROOT"

: > "$PROCESS_STDERR"
log "continuous_observer_start=true data_root=${DATA_ROOT} execution_mode=${EXECUTION_MODE}"

set +e
"$OBSERVER" continuous \
    --config "$CONFIG" \
    --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
    --request-policy "$READ_POLICY" \
    --transport-policy "$TRANSPORT_POLICY" \
    --output "$RUNTIME_ROOT" \
    --state-root "$STATE_ROOT" \
    2>"$PROCESS_STDERR" &
OBSERVER_PID=$!
wait "$OBSERVER_PID"
observer_exit=$?
OBSERVER_PID=""
set -e

if (( STOP_REQUESTED == 1 )); then
    log "continuous_observer_stopped_by_operator=true exit=${observer_exit}"
    exit 0
fi

if (( observer_exit == 0 )); then
    log "continuous_observer_exited_without_operator_request=true"
    observer_exit=1
fi

# Preserve one bounded diagnostic and return control to Railway's existing
# bounded service restart policy. Exchange recovery remains observer-owned.
tail -c 1048576 "$PROCESS_STDERR" > "${FAILURE_ROOT}/last-exit.stderr.tmp"
mv "${FAILURE_ROOT}/last-exit.stderr.tmp" "${FAILURE_ROOT}/last-exit.stderr"
rm -f "$PROCESS_STDERR"
log "continuous_observer_unexpected_exit=true exit=${observer_exit} railway_restart=true"
exit "$observer_exit"
