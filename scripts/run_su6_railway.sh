#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly ENGINE="/app/bin/engine"
readonly CONFIG="/app/config/copytrade.json"
readonly VERY_PROFITABLE_LAYER="/app/config/very-profitable-layer.json"
readonly READ_POLICY="/app/policy/read-api-policy.json"
readonly TRANSPORT_POLICY="/app/policy/public-mainnet-transport.json"

# Canonical production generation SYSTEM_V1. Single permanent state root.
# Fresh roots start with zero durable baselines by default. Set
# SU6_SOURCE_BACKFILL_PATH once to perform a validated one-time import of an
# old root's source-state.sqlite (e.g. /data/system-v1/source-state.sqlite)
# into the new DATA_ROOT. The import runs only when the destination database
# does not exist yet; every later restart keeps using the destination file and
# never re-copies (which would wipe fills accrued since the migration).
readonly DATA_ROOT="${SU6_DATA_ROOT:-/data/system-v1}"
[[ "$DATA_ROOT" = /* && "$DATA_ROOT" != "/" ]] || {
    printf 'invalid SU6_DATA_ROOT\n' >&2
    exit 1
}
readonly STATE_ROOT="${DATA_ROOT}/state"
readonly RUNTIME_ROOT="${DATA_ROOT}/runtime"
readonly FAILURE_ROOT="${DATA_ROOT}/failure"
readonly PROCESS_STDERR="${FAILURE_ROOT}/process.stderr"
readonly SOURCE_DB="${DATA_ROOT}/source-state.sqlite"
readonly BACKFILL_SRC="${SU6_SOURCE_BACKFILL_PATH:-}"

ENGINE_PID=""
STOP_REQUESTED=0

log() {
    printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1"
}

stop_engine() {
    STOP_REQUESTED=1
    if [[ -n "$ENGINE_PID" ]] && kill -0 "$ENGINE_PID" 2>/dev/null; then
        kill -INT "$ENGINE_PID" 2>/dev/null || true
    fi
}

trap stop_engine INT TERM

readonly BINARY_SHA="$(sha256sum "$ENGINE" | cut -d ' ' -f 1)"
log "engine_binary_sha256=${BINARY_SHA}"
# One-time backfill gate: validate before the engine starts so a misconfigured
# path fails closed instead of silently running with zero baselines.
if [[ -n "$BACKFILL_SRC" ]]; then
    [[ "$BACKFILL_SRC" = /* && "$BACKFILL_SRC" != "/" ]] || {
        log "invalid_source_backfill_path=${BACKFILL_SRC}"
        exit 1
    }
    [[ "$BACKFILL_SRC" == /data/* ]] || {
        log "invalid_source_backfill_path=${BACKFILL_SRC}"
        exit 1
    }
    [[ ! -L "$BACKFILL_SRC" && -f "$BACKFILL_SRC" ]] || {
        log "invalid_source_backfill_path=${BACKFILL_SRC}"
        exit 1
    }
    [[ "$BACKFILL_SRC" != "$SOURCE_DB" ]] || {
        log "invalid_source_backfill_path=${BACKFILL_SRC}"
        exit 1
    }
    log "source_backfill_path=${BACKFILL_SRC}"
fi
# Wrapper-owned directories and diagnostics are an explicit schema, separate
# from the engine's state/live schema. Never redirect through a symlink.
for directory in "$DATA_ROOT" "$STATE_ROOT" "$RUNTIME_ROOT" "$FAILURE_ROOT"; do
    [[ ! -L "$directory" && ( ! -e "$directory" || -d "$directory" ) ]] || {
        log "invalid_runtime_directory=${directory}"
        exit 1
    }
done
mkdir -p "$STATE_ROOT" "$RUNTIME_ROOT" "$FAILURE_ROOT"
shopt -s nullglob dotglob
for artifact in "$FAILURE_ROOT"/*; do
    case "${artifact##*/}" in
        process.stderr|last-exit.stderr|last-exit.stderr.tmp|last-exit.meta|last-exit.meta.tmp|current-run.meta|current-run.meta.tmp)
            [[ -f "$artifact" && ! -L "$artifact" ]] && continue ;;
    esac
    log "invalid_failure_artifact=${artifact}"
    exit 1
done
shopt -u nullglob dotglob

readonly STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
readonly RUN_ID="${STARTED_AT}-$$"
PREVIOUS_RUN_ID=none
if [[ -f "$FAILURE_ROOT/current-run.meta" ]]; then
    PREVIOUS_RUN_ID="$(sed -n 's/^run_id=//p' "$FAILURE_ROOT/current-run.meta")"
fi
if [[ -f "$FAILURE_ROOT/last-exit.meta" ]]; then
    log "predecessor_exit_metadata_begin=true"
    head -c 4096 "$FAILURE_ROOT/last-exit.meta"
fi
if [[ -f "$FAILURE_ROOT/last-exit.stderr" ]]; then
    log "predecessor_exit_stderr_begin=true"
    tail -c 1048576 "$FAILURE_ROOT/last-exit.stderr"
    log "predecessor_exit_stderr_end=true"
fi

write_run_metadata() {
    printf 'run_id=%s\nprevious_run_id=%s\nstarted_at=%s\nwrapper_pid=%s\nengine_pid=%s\nbinary_sha256=%s\n' \
        "$RUN_ID" "$PREVIOUS_RUN_ID" "$STARTED_AT" "$$" "$ENGINE_PID" "$BINARY_SHA"
}

persist_exit() {
    tail -c 1048576 "$PROCESS_STDERR" > "$FAILURE_ROOT/last-exit.stderr.tmp" &&
    {
        write_run_metadata
        printf 'exited_at=%s\nexit_code=%s\nstderr_sha256=%s\n' \
            "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$engine_exit" \
            "$(sha256sum "$FAILURE_ROOT/last-exit.stderr.tmp" | cut -d ' ' -f 1)"
    } > "$FAILURE_ROOT/last-exit.meta.tmp" &&
    sync -f "$FAILURE_ROOT" &&
    mv "$FAILURE_ROOT/last-exit.stderr.tmp" "$FAILURE_ROOT/last-exit.stderr" &&
    mv "$FAILURE_ROOT/last-exit.meta.tmp" "$FAILURE_ROOT/last-exit.meta" &&
    sync -f "$FAILURE_ROOT"
}

: > "$PROCESS_STDERR"
log "continuous_engine_start=true run_id=${RUN_ID} previous_run_id=${PREVIOUS_RUN_ID} data_root=${DATA_ROOT} execution=authenticated_live"

set +e
# Branch instead of an empty `"${BACKFILL_ARGS[@]}"` expansion: that form fails
# under `set -u` on bash 3.2 when no backfill flag is set.
if [[ -n "$BACKFILL_SRC" ]]; then
    "$ENGINE" continuous \
        --config "$CONFIG" \
        --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
        --request-policy "$READ_POLICY" \
        --transport-policy "$TRANSPORT_POLICY" \
        --output "$RUNTIME_ROOT" \
        --state-root "$STATE_ROOT" \
        --source-backfill-from "$BACKFILL_SRC" \
        2>"$PROCESS_STDERR" &
else
    "$ENGINE" continuous \
        --config "$CONFIG" \
        --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
        --request-policy "$READ_POLICY" \
        --transport-policy "$TRANSPORT_POLICY" \
        --output "$RUNTIME_ROOT" \
        --state-root "$STATE_ROOT" \
        2>"$PROCESS_STDERR" &
fi
ENGINE_PID=$!
write_run_metadata > "$FAILURE_ROOT/current-run.meta.tmp" &&
    mv "$FAILURE_ROOT/current-run.meta.tmp" "$FAILURE_ROOT/current-run.meta" &&
    sync -f "$FAILURE_ROOT"
metadata_exit=$?
if (( metadata_exit != 0 )); then
    log "current_run_metadata_persistence_failed=true"
    kill -INT "$ENGINE_PID" 2>/dev/null || true
fi
wait "$ENGINE_PID"
engine_exit=$?
set -e

if (( STOP_REQUESTED == 1 )); then
    log "continuous_engine_stopped_by_operator=true exit=${engine_exit}"
    exit 0
fi

# Commit the actual child status before normalizing an unexpected clean exit.
if ! persist_exit; then
    log "predecessor_diagnostic_persistence_failed=true exit=${engine_exit}"
fi
if (( engine_exit == 0 )); then
    log "continuous_engine_exited_without_operator_request=true"
    engine_exit=1
fi

# No in-process or platform respawn. A fatal exit is persisted and remains
# stopped until an operator diagnoses it and explicitly starts a deployment.
log "continuous_engine_unexpected_exit=true exit=${engine_exit} operator_restart_required=true"
exit "$engine_exit"
