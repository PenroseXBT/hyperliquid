#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly OBSERVER="/app/bin/copytrade-observer"
readonly CONFIG="/app/config/copytrade.json"
readonly VERY_PROFITABLE_LAYER="/app/config/very-profitable-layer.json"
readonly READ_POLICY="/app/policy/read-api-policy.json"
readonly TRANSPORT_POLICY="/app/policy/public-mainnet-transport.json"
readonly MANIFEST="/app/frozen/release-manifest.json"
readonly MANIFEST_DIGEST="/app/frozen/release-manifest.sha256"
readonly EMBEDDED_IDENTITY="/app/frozen/frozen-identity.json"
readonly ISOLATION_REPORT="/app/frozen/isolation-report.txt"

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
readonly VOLUME_IDENTITY="${DATA_ROOT}/frozen-identity.json"
readonly FATAL_STOP_MARKER="${STATE_ROOT}/fatal-stop.json"
readonly PROCESS_STDERR="${FAILURE_ROOT}/process.stderr"

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

(
    cd "$(dirname "$MANIFEST")"
    sha256sum -c "$(basename "$MANIFEST_DIGEST")"
) >/dev/null || {
    log "fatal=frozen_manifest_checksum_mismatch"
    exit 1
}

if [[ -e "$VOLUME_IDENTITY" ]]; then
    cmp -s "$EMBEDDED_IDENTITY" "$VOLUME_IDENTITY" || {
        log "fatal=state_root_identity_mismatch"
        exit 1
    }
else
    temporary="${VOLUME_IDENTITY}.tmp.$$"
    cp "$EMBEDDED_IDENTITY" "$temporary"
    sync -f "$temporary"
    mv "$temporary" "$VOLUME_IDENTITY"
    sync -f "$DATA_ROOT"
fi

if [[ -e "$FATAL_STOP_MARKER" ]]; then
    log "fatal_stop_present=true state=quiescent marker=${FATAL_STOP_MARKER}"
    while true; do sleep 3600; done
fi

: > "$PROCESS_STDERR"
log "continuous_observer_start=true data_root=${DATA_ROOT} unsigned=true planned_only=true submission=false"

set +e
"$OBSERVER" continuous \
    --config "$CONFIG" \
    --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
    --request-policy "$READ_POLICY" \
    --transport-policy "$TRANSPORT_POLICY" \
    --release-manifest "$MANIFEST" \
    --isolation-report "$ISOLATION_REPORT" \
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
    log "fatal=continuous_observer_exited_without_operator_request"
    observer_exit=1
fi

# A healthy daemon has no routine exit. Preserve exactly one bounded failure
# diagnostic and fail closed on any unexpected termination.
tail -c 1048576 "$PROCESS_STDERR" > "${FAILURE_ROOT}/fatal.stderr.tmp"
mv "${FAILURE_ROOT}/fatal.stderr.tmp" "${FAILURE_ROOT}/fatal.stderr"
rm -f "$PROCESS_STDERR"
cat > "${FATAL_STOP_MARKER}.tmp" <<EOF
{
  "schema_version": 1,
  "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "classification": "continuous_runtime_failure",
  "observer_exit": ${observer_exit},
  "diagnostic": "${FAILURE_ROOT}/fatal.stderr"
}
EOF
sync -f "${FATAL_STOP_MARKER}.tmp"
mv "${FATAL_STOP_MARKER}.tmp" "$FATAL_STOP_MARKER"
sync -f "$STATE_ROOT"
log "fatal_stop_created=true exit=${observer_exit}"
exit "$observer_exit"
