#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly OBSERVER="/app/bin/copytrade-observer"
readonly WRAPPER="/app/bin/run_su6_railway.sh"
readonly CONFIG="/app/config/copytrade.json"
readonly VERY_PROFITABLE_LAYER="/app/config/very-profitable-layer.json"
readonly READ_POLICY="/app/policy/read-api-policy.json"
readonly TRANSPORT_POLICY="/app/policy/public-mainnet-transport.json"
readonly CANDIDATE_CONFIG="/app/policy/candidate-configuration.json"
readonly STRATEGY_POLICY="/app/policy/strategy-policy.json"
readonly RISK_POLICY="/app/policy/risk-policy.json"
readonly MANIFEST="/app/frozen/release-manifest.json"
readonly MANIFEST_DIGEST="/app/frozen/release-manifest.sha256"
readonly EMBEDDED_IDENTITY="/app/frozen/frozen-identity.json"
readonly ISOLATION_REPORT="/app/frozen/isolation-report.txt"

readonly VOLUME_ROOT="${RAILWAY_VOLUME_MOUNT_PATH:?RAILWAY_VOLUME_MOUNT_PATH is required}"
# Never reuse the failed R8 root or any earlier state with this corrected
# wrapper identity. Corrected R8 starts from a fresh root.
readonly DATA_ROOT="${VOLUME_ROOT}/su6r1-375-forward-r8-corrected-1"
readonly STATE_ROOT="${DATA_ROOT}/state"
readonly EVIDENCE_ROOT="${DATA_ROOT}/evidence"
readonly ARCHIVE_ROOT="${EVIDENCE_ROOT}/archives"
readonly DIAGNOSTIC_ROOT="${EVIDENCE_ROOT}/diagnostic-journals"
readonly AGGREGATE_ROOT="${DATA_ROOT}/aggregate"
readonly LOG_ROOT="${DATA_ROOT}/logs"
readonly VOLUME_IDENTITY="${DATA_ROOT}/frozen-identity.json"
readonly WINDOW_INDEX="${AGGREGATE_ROOT}/windows.tsv"
readonly RECORD_METADATA="${AGGREGATE_ROOT}/record-metadata.json"
readonly AGGREGATE_SUMMARY="${AGGREGATE_ROOT}/summary.json"
readonly ACTIVE_WINDOW="${AGGREGATE_ROOT}/active-window.json"
readonly COMPLETE_MARKER="${AGGREGATE_ROOT}/record-complete.json"
readonly FATAL_STOP_MARKER="${STATE_ROOT}/fatal-stop.json"

readonly MINIMUM_DAYS="${SU6_RUN_MINIMUM_DAYS:?SU6_RUN_MINIMUM_DAYS is required}"
readonly MINIMUM_CLOSED="${SU6_MIN_CLOSED_EPISODES:?SU6_MIN_CLOSED_EPISODES is required}"
readonly WINDOW_SECONDS=14400
readonly MAX_TRANSIENT_RETRIES=3
readonly TRANSIENT_RETRY_BASE_SECONDS=5
readonly SUCCESSFUL_RAW_JOURNALS_TO_RETAIN=0
readonly FAILED_RAW_JOURNALS_TO_RETAIN=2
readonly MAX_COMPACT_ARCHIVE_BYTES=25000000
readonly MAX_DAILY_ARCHIVE_BYTES=150000000
readonly MAX_WEEKLY_ARCHIVE_BYTES=1000000000
readonly VOLUME_HIGH_WATER_BYTES=2000000000
readonly VOLUME_MANDATORY_STOP_BYTES=3000000000
readonly WINDOW_INDEX_HEADER=$'sequence\tstart_wall_ms\trecorded_at_epoch_seconds\tstatus\tobserver_exit\tfinalizer_exit\taggregate_exit\telapsed_seconds\tcumulative_closed\tclosed_delta\ttotal_net_pnl\tprofit_factor\tshadow_executions\tbundle_id\tterminal_sha256\tprofitability_sha256\tfrozen_identity_sha256\tarchive'

# Railway's Docker/image start command replaces the image ENTRYPOINT. When the
# configured wrapper is therefore launched directly as PID 1, re-exec it under
# the embedded minimal init so signals are forwarded and children are reaped.
if (( $$ == 1 )); then
    exec /usr/bin/tini -- "$0" "$@"
fi

STOP_REQUESTED=0
OBSERVER_PID=""
RECORDED_WINDOW_STATUS=""
WINDOW_FAILURE_CLASSIFICATION=""

log() {
    local message="$1"
    local line
    line="$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${message}"
    printf '%s\n' "$line"
    if [[ -d "$LOG_ROOT" ]]; then
        printf '%s\n' "$line" >> "${LOG_ROOT}/railway-worker.log"
    fi
}

fail() {
    log "fatal=$1"
    exit 1
}

json_scalar() {
    local path="$1"
    local key="$2"
    local line
    line="$(grep -m 1 -E "^[[:space:]]*\"${key}\"[[:space:]]*:" "$path" || true)"
    if [[ -z "$line" ]]; then
        # State envelopes are allowed to be compact JSON. The values consumed
        # here are scalars without embedded commas, so split compact top-level
        # members into the same one-member-per-line form used below.
        line="$(tr '{},' '\n\n\n' < "$path" \
            | grep -m 1 -E "^[[:space:]]*\"${key}\"[[:space:]]*:" \
            || true)"
    fi
    [[ -n "$line" ]] || return 1
    printf '%s\n' "$line" | sed -E \
        -e "s/^[[:space:]]*\"${key}\"[[:space:]]*:[[:space:]]*//" \
        -e 's/,[[:space:]]*$//' \
        -e 's/^"//' \
        -e 's/"$//'
}

sha256_file() {
    sha256sum "$1" | awk '{print $1}'
}

normalize_hash() {
    local value="$1"
    printf '%s\n' "${value#0x}" | tr '[:upper:]' '[:lower:]'
}

require_hash_match() {
    local expected="$1"
    local path="$2"
    local label="$3"
    local actual
    expected="$(normalize_hash "$expected")"
    [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || fail "${label}_expected_hash_invalid"
    actual="$(sha256_file "$path")"
    [[ "$actual" == "$expected" ]] || fail "${label}_hash_mismatch"
}

atomic_write() {
    local path="$1"
    local contents="$2"
    local directory temporary
    directory="$(dirname "$path")"
    temporary="${path}.tmp.$$"
    printf '%s\n' "$contents" > "$temporary"
    sync -f "$temporary"
    mv -f "$temporary" "$path"
    sync -f "$directory"
}

classify_observer_failure() {
    local bundle="$1"
    local observer_exit="$2"
    local process_stderr="${3:-}"
    local stderr="${bundle}/observer.stderr.log"
    local terminal="${bundle}/terminal-summary.json"
    local evidence=""
    [[ -f "$stderr" ]] && evidence+="$(tr '\n' ' ' < "$stderr")"
    [[ -f "$terminal" ]] && evidence+=" $(tr '\n' ' ' < "$terminal")"
    [[ -n "$process_stderr" && -f "$process_stderr" ]] \
        && evidence+=" $(tr '\n' ' ' < "$process_stderr")"

    if [[ "$evidence" =~ ConflictingCandle ]]; then
        printf 'conflicting_candle\n'
    elif [[ "$evidence" =~ NonMonotonicCandle ]]; then
        printf 'non_monotonic_candle\n'
    elif [[ "$evidence" =~ [Cc]hecksum ]]; then
        printf 'snapshot_checksum_failure\n'
    elif [[ "$evidence" =~ [Ii]dentity.*[Mm]ismatch ]]; then
        printf 'state_identity_mismatch\n'
    elif [[ "$evidence" =~ [Aa]ccounting.*([Ii]nvariant|[Rr]econcil) ]]; then
        printf 'accounting_invariant_failure\n'
    elif [[ "$evidence" =~ ([Cc]orrupt|[Tt]runcated|[Ii]nvalid\ unsigned).*[Ss]napshot ]]; then
        printf 'corrupt_snapshot\n'
    elif [[ "$evidence" =~ [Uu]nsupported.*[Ss]chema ]]; then
        printf 'unsupported_schema\n'
    elif [[ "$evidence" =~ ([Dd][Nn][Ss]|429|[Rr]ate.?limit|[Tt]imed.?out|[Cc]onnection|[Tt]ransport) ]]; then
        printf 'transient_transport\n'
    elif [[ "$evidence" =~ ([Pp]rofitability|[Qq]ualification).*gate.*failed ]]; then
        printf 'qualification_gate\n'
    elif [[ "$observer_exit" == "0" ]]; then
        printf 'none\n'
    else
        printf 'unknown_observer_failure\n'
    fi
}

normalize_recorded_failure() {
    local indexed_status="$1"
    local failure_classification="$2"

    if [[ "$indexed_status" == "evidence_invalid" \
        && ( "$failure_classification" == "none" \
            || "$failure_classification" == "qualification_gate" \
            || "$failure_classification" == "unknown_observer_failure" ) ]]; then
        printf 'evidence_finalization_failure\n'
    elif [[ "$indexed_status" == "finalization_failed" ]]; then
        printf 'finalization_invariant_failure\n'
    elif [[ "$indexed_status" == "complete" \
        && ( "$failure_classification" == "none" \
            || "$failure_classification" == "qualification_gate" \
            || "$failure_classification" == "unknown_observer_failure" ) ]]; then
        # A fully finalized 14,400-second bundle is the authoritative signal
        # that the observer reached its expected profitability-gate exit.
        # Railway does not redirect the process-level gate message into the
        # bundle's observer.stderr.log, so an exit status of 1 can otherwise
        # appear as an unknown failure here.
        printf 'none\n'
    elif [[ "$indexed_status" != "complete" \
        && ( "$failure_classification" == "none" \
            || "$failure_classification" == "qualification_gate" ) ]]; then
        printf 'unexpected_incomplete_window\n'
    else
        printf '%s\n' "$failure_classification"
    fi
}

write_fatal_stop_marker() {
    local classification="$1"
    local sequence="$2"
    local bundle_id="$3"
    local error_evidence_hash="$4"
    local generation="null"
    local initialized="${STATE_ROOT}/initialized.json"
    if [[ -f "$initialized" ]]; then
        generation="$(json_scalar "$initialized" generation || printf 'null')"
        [[ "$generation" =~ ^[0-9]+$ ]] || generation="null"
    fi
    atomic_write "$FATAL_STOP_MARKER" "$(
        printf '{\n'
        printf '  "schema_version": 1,\n'
        printf '  "failure_classification": "%s",\n' "$classification"
        printf '  "timestamp": "%s",\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        printf '  "active_window_sequence": %s,\n' "$sequence"
        printf '  "observer_identity": "%s",\n' "$(sha256_file "$VOLUME_IDENTITY")"
        printf '  "last_valid_snapshot_generation": %s,\n' "$generation"
        printf '  "error_evidence_hash": "%s",\n' "$error_evidence_hash"
        printf '  "evidence_bundle_id": "%s"\n' "$bundle_id"
        printf '}'
    )"
    log "fatal_stop_created=true classification=${classification} sequence=${sequence} evidence_sha256=${error_evidence_hash}"
}

append_index_row() {
    local row="$1"
    local temporary="${WINDOW_INDEX}.tmp.$$"
    cp "$WINDOW_INDEX" "$temporary"
    printf '%s\n' "$row" >> "$temporary"
    sync -f "$temporary"
    mv -f "$temporary" "$WINDOW_INDEX"
    sync -f "$AGGREGATE_ROOT"
}

validate_window_index() {
    local identity_hash="$1"
    local header
    IFS= read -r header < "$WINDOW_INDEX" || fail "window_index_unreadable"
    [[ "$header" == "$WINDOW_INDEX_HEADER" ]] || fail "window_index_header_mismatch"
    awk -F '\t' -v identity="$identity_hash" '
        BEGIN { previous_closed = 0 }
        NR == 1 { next }
        NF != 18 { exit 1 }
        $1 != NR - 1 { exit 1 }
        $4 !~ /^(complete|incomplete|evidence_invalid|finalization_failed)$/ { exit 1 }
        $9 !~ /^[0-9]+$/ || $10 !~ /^[0-9]+$/ { exit 1 }
        $9 != previous_closed + $10 { exit 1 }
        $14 !~ /^[0-9]{10,}-[0-9]+-[0-9a-f]{12,64}$/ { exit 1 }
        $17 != identity { exit 1 }
        $18 != "archives/" $14 ".tar.gz" { exit 1 }
        { previous_closed = $9 }
    ' "$WINDOW_INDEX" || fail "window_index_invalid"
}

on_shutdown() {
    STOP_REQUESTED=1
    log "shutdown_requested=true"
    if [[ -n "$OBSERVER_PID" ]] && kill -0 "$OBSERVER_PID" 2>/dev/null; then
        # The observer's SIGINT path stops planning ticks, drains in-flight
        # reads, persists state, and emits an incomplete terminal summary.
        kill -INT "$OBSERVER_PID" 2>/dev/null || true
    fi
}

trap on_shutdown TERM INT

validate_environment() {
    [[ "$VOLUME_ROOT" == "/data" ]] || fail "railway_volume_mount_must_be_/data"
    [[ "${TZ:-}" == "UTC" ]] || fail "TZ_must_equal_UTC"
    [[ "${RUST_LOG:-}" == "info" ]] || fail "RUST_LOG_must_equal_info"
    [[ "${SU6_UNSIGNED:-}" == "true" ]] || fail "SU6_UNSIGNED_must_equal_true"
    [[ "${SU6_PLANNED_ONLY:-}" == "true" ]] || fail "SU6_PLANNED_ONLY_must_equal_true"
    [[ "${SU6_SUBMISSION_CAPABLE:-}" == "false" ]] \
        || fail "SU6_SUBMISSION_CAPABLE_must_equal_false"
    [[ "$MINIMUM_DAYS" =~ ^[0-9]+$ ]] || fail "minimum_days_not_an_integer"
    [[ "$MINIMUM_CLOSED" =~ ^[0-9]+$ ]] || fail "minimum_closed_not_an_integer"
    (( MINIMUM_DAYS >= 7 )) || fail "minimum_days_below_seven"
    (( MINIMUM_CLOSED >= 100 )) || fail "minimum_closed_below_one_hundred"

    local unknown_su6
    unknown_su6="$(
        env | sed -n 's/^\(SU6_[A-Za-z0-9_]*\)=.*/\1/p' \
            | grep -Ev '^(SU6_UNSIGNED|SU6_PLANNED_ONLY|SU6_SUBMISSION_CAPABLE|SU6_RUN_MINIMUM_DAYS|SU6_MIN_CLOSED_EPISODES)$' \
            || true
    )"
    [[ -z "$unknown_su6" ]] || fail "unrecognized_SU6_environment_variable"

    local credential_names
    credential_names="$(
        env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' \
            | grep -Ei '(PRIVATE_KEY|MNEMONIC|API_WALLET|FOLLOWER_CREDENTIAL|SIGNER_SECRET|AUTH_TOKEN)' \
            || true
    )"
    [[ -z "$credential_names" ]] || fail "credential_environment_present"
}

validate_embedded_release() {
    local required
    for required in \
        "$OBSERVER" \
        "$WRAPPER" \
        "$CONFIG" \
        "$VERY_PROFITABLE_LAYER" \
        "$READ_POLICY" \
        "$TRANSPORT_POLICY" \
        "$CANDIDATE_CONFIG" \
        "$STRATEGY_POLICY" \
        "$RISK_POLICY" \
        "$MANIFEST" \
        "$MANIFEST_DIGEST" \
        "$EMBEDDED_IDENTITY" \
        "$ISOLATION_REPORT"
    do
        [[ -f "$required" ]] || fail "missing_embedded_file_$(basename "$required")"
    done

    [[ ! -e /app/bin/copytrade-signer ]] || fail "signer_binary_present"
    [[ ! -e /app/config/ledger.json ]] || fail "follower_ledger_present"
    if find /app -type f \( \
        -name '*.key' -o \
        -name '*.pem' -o \
        -name '.env' -o \
        -iname '*credential*' -o \
        -iname '*secret*' \
    \) -print -quit | grep -q .; then
        fail "key_or_credential_file_present"
    fi

    local digest_file_hash manifest_hash identity_manifest_hash
    local manifest_binary_hash identity_binary_hash actual_binary_hash
    digest_file_hash="$(awk 'NR == 1 {print $1}' "$MANIFEST_DIGEST")"
    manifest_hash="$(sha256_file "$MANIFEST")"
    identity_manifest_hash="$(json_scalar "$EMBEDDED_IDENTITY" manifest_sha256)" \
        || fail "identity_manifest_hash_missing"
    [[ "$manifest_hash" == "$(normalize_hash "$digest_file_hash")" ]] \
        || fail "manifest_digest_sidecar_mismatch"
    [[ "$manifest_hash" == "$(normalize_hash "$identity_manifest_hash")" ]] \
        || fail "identity_manifest_hash_mismatch"

    manifest_binary_hash="$(json_scalar "$MANIFEST" binary_sha256)" \
        || fail "manifest_binary_hash_missing"
    identity_binary_hash="$(json_scalar "$EMBEDDED_IDENTITY" observer_binary_sha256)" \
        || fail "identity_binary_hash_missing"
    actual_binary_hash="$(sha256_file "$OBSERVER")"
    [[ "$actual_binary_hash" == "$(normalize_hash "$manifest_binary_hash")" ]] \
        || fail "observer_manifest_hash_mismatch"
    [[ "$actual_binary_hash" == "$(normalize_hash "$identity_binary_hash")" ]] \
        || fail "observer_identity_hash_mismatch"

    local manifest_source_hash manifest_config_hash manifest_risk_hash
    local identity_source_hash identity_config_hash identity_strategy_hash
    local identity_candidate_hash identity_risk_hash
    manifest_source_hash="$(json_scalar "$MANIFEST" source_tree_sha256)" \
        || fail "manifest_source_tree_hash_missing"
    manifest_config_hash="$(json_scalar "$MANIFEST" configuration_sha256)" \
        || fail "manifest_configuration_hash_missing"
    manifest_risk_hash="$(json_scalar "$MANIFEST" risk_policy_sha256)" \
        || fail "manifest_risk_hash_missing"
    identity_source_hash="$(json_scalar "$EMBEDDED_IDENTITY" source_tree_sha256)" \
        || fail "identity_source_tree_hash_missing"
    identity_config_hash="$(json_scalar "$EMBEDDED_IDENTITY" configuration_sha256)" \
        || fail "identity_configuration_hash_missing"
    identity_strategy_hash="$(json_scalar "$EMBEDDED_IDENTITY" strategy_configuration_sha256)" \
        || fail "identity_strategy_hash_missing"
    identity_candidate_hash="$(json_scalar "$EMBEDDED_IDENTITY" candidate_configuration_sha256)" \
        || fail "identity_candidate_hash_missing"
    identity_risk_hash="$(json_scalar "$EMBEDDED_IDENTITY" risk_policy_sha256)" \
        || fail "identity_risk_hash_missing"
    [[ "$(normalize_hash "$manifest_source_hash")" == "$(normalize_hash "$identity_source_hash")" ]] \
        || fail "source_tree_identity_mismatch"
    [[ "$(normalize_hash "$manifest_config_hash")" == "$(normalize_hash "$identity_config_hash")" ]] \
        || fail "configuration_identity_mismatch"
    [[ "$(normalize_hash "$manifest_risk_hash")" == "$(normalize_hash "$identity_risk_hash")" ]] \
        || fail "risk_policy_identity_mismatch"
    require_hash_match "$identity_strategy_hash" "$STRATEGY_POLICY" \
        "strategy_configuration"
    require_hash_match "$identity_candidate_hash" "$CANDIDATE_CONFIG" \
        "candidate_configuration"

    local identity_only_hash
    for identity_only_hash in \
        persistence_schema_sha256 \
        railway_dockerfile_sha256 \
        railway_toml_sha256
    do
        [[ "$(normalize_hash "$(json_scalar "$EMBEDDED_IDENTITY" "$identity_only_hash")")" =~ ^[0-9a-f]{64}$ ]] \
            || fail "identity_${identity_only_hash}_missing_or_invalid"
    done
    local binary_persistence_hash identity_persistence_hash
    binary_persistence_hash="$(
        "$OBSERVER" --config "$CONFIG" \
            --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
            --print-persistence-schema-hash
    )"
    identity_persistence_hash="$(json_scalar "$EMBEDDED_IDENTITY" persistence_schema_sha256)" \
        || fail "identity_persistence_schema_hash_missing"
    [[ "$(normalize_hash "$binary_persistence_hash")" == "$(normalize_hash "$identity_persistence_hash")" ]] \
        || fail "persistence_schema_identity_mismatch"

    local file_hash_field file_path expected
    while IFS='|' read -r file_hash_field file_path; do
        expected="$(json_scalar "$EMBEDDED_IDENTITY" "$file_hash_field")" \
            || fail "identity_${file_hash_field}_missing"
        require_hash_match "$expected" "$file_path" "$file_hash_field"
    done <<EOF
unsigned_configuration_file_sha256|${CONFIG}
candidate_configuration_file_sha256|${CANDIDATE_CONFIG}
strategy_policy_file_sha256|${STRATEGY_POLICY}
risk_policy_file_sha256|${RISK_POLICY}
read_policy_file_sha256|${READ_POLICY}
transport_policy_file_sha256|${TRANSPORT_POLICY}
very_profitable_layer_file_sha256|${VERY_PROFITABLE_LAYER}
railway_wrapper_sha256|${WRAPPER}
EOF

    local config_output
    config_output="$("$OBSERVER" --config "$CONFIG" \
        --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
        --validate-config)"
    [[ "$config_output" == *"candidates=375"* ]] || fail "candidate_count_not_375"
    [[ "$(json_scalar "$CONFIG" source_budget_fraction)" == "0.35" ]] \
        || fail "source_budget_not_35_percent"
    [[ "$(json_scalar "$CONFIG" technical_budget_fraction)" == "0.65" ]] \
        || fail "technical_budget_not_65_percent"
    [[ "$(json_scalar "$CONFIG" follower_address)" == "null" ]] \
        || fail "follower_address_present"
    grep -Fxq 'HL1C isolation policy passed' "$ISOLATION_REPORT" \
        || fail "embedded_isolation_report_invalid"
}

initialize_data_root() {
    mkdir -p \
        "$DATA_ROOT" \
        "$STATE_ROOT" \
        "$EVIDENCE_ROOT" \
        "$ARCHIVE_ROOT" \
        "$DIAGNOSTIC_ROOT" \
        "$AGGREGATE_ROOT" \
        "$LOG_ROOT"

    if [[ -e "$VOLUME_IDENTITY" ]]; then
        cmp -s "$EMBEDDED_IDENTITY" "$VOLUME_IDENTITY" \
            || fail "volume_frozen_identity_mismatch"
    else
        local temporary="${VOLUME_IDENTITY}.tmp.$$"
        cp "$EMBEDDED_IDENTITY" "$temporary"
        sync -f "$temporary"
        mv "$temporary" "$VOLUME_IDENTITY"
        sync -f "$DATA_ROOT"
    fi

    local identity_hash
    identity_hash="$(sha256_file "$VOLUME_IDENTITY")"
    if [[ ! -e "$WINDOW_INDEX" ]]; then
        atomic_write "$WINDOW_INDEX" "$WINDOW_INDEX_HEADER"
    fi
    validate_window_index "$identity_hash"
    if [[ ! -e "$RECORD_METADATA" ]]; then
        local started
        started="$(date +%s)"
        atomic_write "$RECORD_METADATA" "$(
            printf '{\n'
            printf '  "schema_version": 1,\n'
            printf '  "started_at_epoch_seconds": %s,\n' "$started"
            printf '  "window_seconds": %s,\n' "$WINDOW_SECONDS"
            printf '  "minimum_days": %s,\n' "$MINIMUM_DAYS"
            printf '  "minimum_closed_episodes": %s,\n' "$MINIMUM_CLOSED"
            printf '  "frozen_identity_sha256": "%s"\n' "$identity_hash"
            printf '}'
        )"
    else
        [[ "$(json_scalar "$RECORD_METADATA" frozen_identity_sha256)" == "$identity_hash" ]] \
            || fail "aggregate_identity_mismatch"
        [[ "$(json_scalar "$RECORD_METADATA" window_seconds)" == "$WINDOW_SECONDS" ]] \
            || fail "aggregate_window_duration_mismatch"
        [[ "$(json_scalar "$RECORD_METADATA" minimum_days)" == "$MINIMUM_DAYS" ]] \
            || fail "aggregate_minimum_days_mismatch"
        [[ "$(json_scalar "$RECORD_METADATA" minimum_closed_episodes)" == "$MINIMUM_CLOSED" ]] \
            || fail "aggregate_minimum_closed_mismatch"
    fi
}

bundle_id_is_safe() {
    local bundle_id="$1"
    [[ "$bundle_id" =~ ^[0-9]{10,}-[0-9]+-[0-9a-f]{12,64}$ ]]
}

bundle_is_indexed() {
    local bundle_id="$1"
    awk -F '\t' -v id="$bundle_id" 'NR > 1 && $14 == id {found=1} END {exit !found}' \
        "$WINDOW_INDEX"
}

next_sequence() {
    awk -F '\t' 'NR > 1 {last=$1} END {print (last ? last + 1 : 1)}' "$WINDOW_INDEX"
}

last_cumulative_closed() {
    awk -F '\t' 'NR > 1 {last=$9} END {print (last ? last : 0)}' "$WINDOW_INDEX"
}

write_replay_journal_index() {
    local bundle="$1"
    local journal="${bundle}/replay-events.jsonl"
    [[ -f "$journal" ]] || return 0
    local events first_sequence last_sequence first_hash last_hash raw_hash chain_head
    events="$(wc -l < "$journal" | tr -d '[:space:]')"
    first_sequence="$(awk 'match($0,/"sequence":[0-9]+/){v=substr($0,RSTART+11,RLENGTH-11); print v; exit}' "$journal")"
    last_sequence="$(tail -n 1 "$journal" | awk 'match($0,/"sequence":[0-9]+/){print substr($0,RSTART+11,RLENGTH-11)}')"
    first_hash="$(head -n 1 "$journal" | sed -nE 's/.*"event_hash":"([0-9a-f]{64})".*/\1/p')"
    last_hash="$(tail -n 1 "$journal" | sed -nE 's/.*"event_hash":"([0-9a-f]{64})".*/\1/p')"
    raw_hash="$(sha256_file "$journal")"
    chain_head="$last_hash"
    if [[ -f "${bundle}/replay-event-chain-head.txt" ]]; then
        chain_head="$(tr -d '[:space:]' < "${bundle}/replay-event-chain-head.txt")"
    fi
    [[ "$events" =~ ^[0-9]+$ && "$first_sequence" =~ ^[0-9]+$ \
        && "$last_sequence" =~ ^[0-9]+$ \
        && "$first_hash" =~ ^[0-9a-f]{64}$ \
        && "$last_hash" =~ ^[0-9a-f]{64}$ \
        && "$chain_head" =~ ^[0-9a-f]{64}$ ]] \
        || fail "replay_journal_index_invalid_$(basename "$bundle")"
    atomic_write "${bundle}/replay-journal-index.json" "$(
        printf '{\n'
        printf '  "schema_version": 1,\n'
        printf '  "events": %s,\n' "$events"
        printf '  "first_sequence": %s,\n' "$first_sequence"
        printf '  "last_sequence": %s,\n' "$last_sequence"
        printf '  "first_event_hash": "%s",\n' "$first_hash"
        printf '  "last_event_hash": "%s",\n' "$last_hash"
        printf '  "recorded_chain_head": "%s",\n' "$chain_head"
        printf '  "raw_journal_sha256": "%s"\n' "$raw_hash"
        printf '}'
    )"
}

retain_raw_journal() {
    local bundle="$1"
    local status="$2"
    local journal="${bundle}/replay-events.jsonl"
    [[ -f "$journal" ]] || return 0
    local category limit bundle_id retained temporary sidecar
    bundle_id="$(basename "$bundle")"
    if [[ "$status" == "complete" ]]; then
        category="successful"
        limit="$SUCCESSFUL_RAW_JOURNALS_TO_RETAIN"
    else
        category="failed"
        limit="$FAILED_RAW_JOURNALS_TO_RETAIN"
    fi
    if (( limit == 0 )); then
        log "raw_journal_disposition=discard_after_verified_compaction category=${category} limit=0 bundle=${bundle_id}"
        return 0
    fi
    retained="${DIAGNOSTIC_ROOT}/${category}-${bundle_id}.replay-events.jsonl.gz"
    temporary="${retained}.tmp.$$"
    gzip -n -c "$journal" > "$temporary"
    sync -f "$temporary"
    gzip -t "$temporary" || fail "retained_journal_gzip_invalid_${bundle_id}"
    mv "$temporary" "$retained"
    sidecar="${retained}.sha256"
    atomic_write "$sidecar" "$(sha256_file "$retained")  $(basename "$retained")"
    sync -f "$DIAGNOSTIC_ROOT"

    local -a retained_journals
    retained_journals=()
    while IFS= read -r retained_journal; do
        [[ -n "$retained_journal" ]] && retained_journals+=("$retained_journal")
    done < <(
        find "$DIAGNOSTIC_ROOT" -maxdepth 1 -type f \
            -name "${category}-*.replay-events.jsonl.gz" -print \
            | LC_ALL=C sort
    )
    while (( ${#retained_journals[@]} > limit )); do
        local expired="${retained_journals[0]}"
        [[ "$(dirname "$expired")" == "$DIAGNOSTIC_ROOT" ]] \
            || fail "unsafe_retained_journal_path"
        rm -f -- "$expired" "${expired}.sha256"
        retained_journals=("${retained_journals[@]:1}")
        log "retained_raw_journal_expired=$(basename "$expired") recoverable=false"
    done
    sync -f "$DIAGNOSTIC_ROOT"
    log "raw_journal_retained=$(basename "$retained") category=${category} limit=${limit}"
}

archive_bytes_for_latest_windows() {
    local limit="$1"
    local total=0 archive
    while IFS= read -r archive; do
        [[ -n "$archive" ]] || continue
        total=$((total + $(wc -c < "$archive" | tr -d '[:space:]')))
    done < <(
        find "$ARCHIVE_ROOT" -maxdepth 1 -type f -name '*.tar.gz' -print \
            | LC_ALL=C sort | tail -n "$limit"
    )
    printf '%s\n' "$total"
}

enforce_storage_policy() {
    local bundle_id="$1"
    local archive="$2"
    local status="$3"
    local archive_bytes daily_bytes weekly_bytes volume_bytes sequence evidence_hash
    local successful_archive_within_limit=1
    archive_bytes="$(wc -c < "$archive" | tr -d '[:space:]')"
    daily_bytes="$(archive_bytes_for_latest_windows 6)"
    weekly_bytes="$(archive_bytes_for_latest_windows 42)"
    volume_bytes="$(( $(du -sk "$DATA_ROOT" | awk '{print $1}') * 1024 ))"
    log "storage_policy bundle=${bundle_id} compact_archive_bytes=${archive_bytes} daily_archive_bytes=${daily_bytes} weekly_archive_bytes=${weekly_bytes} volume_bytes=${volume_bytes}"

    if (( volume_bytes >= VOLUME_HIGH_WATER_BYTES )); then
        log "alert=volume_high_water bytes=${volume_bytes} threshold=${VOLUME_HIGH_WATER_BYTES}"
    fi
    if [[ "$status" == "complete" ]] && (( archive_bytes > MAX_COMPACT_ARCHIVE_BYTES )); then
        successful_archive_within_limit=0
    fi
    if (( successful_archive_within_limit == 1 \
        && daily_bytes <= MAX_DAILY_ARCHIVE_BYTES \
        && weekly_bytes <= MAX_WEEKLY_ARCHIVE_BYTES \
        && volume_bytes < VOLUME_MANDATORY_STOP_BYTES )); then
        return 0
    fi

    sequence="$(awk -F '\t' 'END {print $1}' "$WINDOW_INDEX")"
    [[ "$sequence" =~ ^[0-9]+$ ]] || sequence=0
    evidence_hash="$(sha256_file "$archive")"
    write_fatal_stop_marker \
        "storage_policy_violation" \
        "$sequence" \
        "$bundle_id" \
        "$evidence_hash"
    log "mandatory_storage_stop=true compact_limit=${MAX_COMPACT_ARCHIVE_BYTES} daily_limit=${MAX_DAILY_ARCHIVE_BYTES} weekly_limit=${MAX_WEEKLY_ARCHIVE_BYTES} volume_stop=${VOLUME_MANDATORY_STOP_BYTES}"
    exit 70
}

archive_bundle() {
    local bundle="$1"
    local status="$2"
    local bundle_id archive archive_sidecar contents_sidecar
    local archive_temporary sidecar_temporary contents_temporary verify_root
    bundle_id="$(basename "$bundle")"
    bundle_id_is_safe "$bundle_id" || fail "unsafe_bundle_id_for_archive"
    [[ "$(dirname "$bundle")" == "$EVIDENCE_ROOT" ]] || fail "unsafe_bundle_parent"
    [[ -d "$bundle" && ! -L "$bundle" ]] || fail "bundle_not_plain_directory"
    if find "$bundle" -mindepth 1 ! -type f ! -type d -print -quit | grep -q .; then
        fail "bundle_contains_nonregular_entry_${bundle_id}"
    fi
    write_replay_journal_index "$bundle"
    retain_raw_journal "$bundle" "$status"

    archive="${ARCHIVE_ROOT}/${bundle_id}.tar.gz"
    archive_sidecar="${archive}.sha256"
    contents_sidecar="${ARCHIVE_ROOT}/${bundle_id}.contents.sha256"

    if [[ -e "$archive" || -e "$archive_sidecar" || -e "$contents_sidecar" ]]; then
        if [[ ! -f "$archive" || ! -f "$archive_sidecar" || ! -f "$contents_sidecar" ]]; then
            # A crash may interrupt publication of the three-file archive set.
            # The still-present raw bundle is authoritative, so discard only
            # the exact partial outputs and rebuild them.
            rm -f -- "$archive" "$archive_sidecar" "$contents_sidecar"
            sync -f "$ARCHIVE_ROOT"
        fi
    fi

    if [[ -f "$archive" && -f "$archive_sidecar" && -f "$contents_sidecar" ]]; then
        (
            cd "$ARCHIVE_ROOT"
            sha256sum -c "$(basename "$archive_sidecar")"
        ) || fail "existing_archive_hash_invalid_${bundle_id}"
        gzip -t "$archive" || fail "existing_archive_gzip_invalid_${bundle_id}"
        contents_temporary="${contents_sidecar}.compare.$$"
        (
            cd "$EVIDENCE_ROOT"
            find "$bundle_id" -type f \
                ! -name 'replay-events.jsonl' \
                ! -name 'observer-release-binary' -print0 \
                | LC_ALL=C sort -z \
                | xargs -0 -r sha256sum
        ) > "$contents_temporary"
        cmp -s "$contents_sidecar" "$contents_temporary" \
            || fail "existing_archive_contents_mismatch_${bundle_id}"
        rm -f -- "$contents_temporary"
    else
        archive_temporary="${archive}.tmp.$$"
        contents_temporary="${contents_sidecar}.tmp.$$"
        (
            cd "$EVIDENCE_ROOT"
            find "$bundle_id" -type f \
                ! -name 'replay-events.jsonl' \
                ! -name 'observer-release-binary' -print0 \
                | LC_ALL=C sort -z \
                | xargs -0 -r sha256sum
        ) > "$contents_temporary"
        sync -f "$contents_temporary"

        tar \
            --format=posix \
            --sort=name \
            --numeric-owner \
            --owner=0 \
            --group=0 \
            --mtime='@0' \
            --pax-option=delete=atime,delete=ctime \
            --exclude="${bundle_id}/replay-events.jsonl" \
            --exclude="${bundle_id}/observer-release-binary" \
            -C "$EVIDENCE_ROOT" \
            -czf "$archive_temporary" \
            "$bundle_id"
        sync -f "$archive_temporary"
        gzip -t "$archive_temporary" || fail "archive_gzip_verification_failed_${bundle_id}"

        verify_root="$(mktemp -d)"
        tar -xzf "$archive_temporary" -C "$verify_root"
        (
            cd "$verify_root"
            find "$bundle_id" -type f \
                ! -name 'replay-events.jsonl' \
                ! -name 'observer-release-binary' -print0 \
                | LC_ALL=C sort -z \
                | xargs -0 -r sha256sum
        ) | cmp -s "$contents_temporary" - \
            || fail "archive_content_verification_failed_${bundle_id}"
        rm -rf -- "$verify_root"

        mv "$archive_temporary" "$archive"
        mv "$contents_temporary" "$contents_sidecar"
        sync -f "$ARCHIVE_ROOT"

        sidecar_temporary="${archive_sidecar}.tmp.$$"
        (
            cd "$ARCHIVE_ROOT"
            sha256sum "$(basename "$archive")"
        ) > "$sidecar_temporary"
        sync -f "$sidecar_temporary"
        mv "$sidecar_temporary" "$archive_sidecar"
        sync -f "$ARCHIVE_ROOT"
        (
            cd "$ARCHIVE_ROOT"
            sha256sum -c "$(basename "$archive_sidecar")"
        ) || fail "archive_sidecar_verification_failed_${bundle_id}"
    fi

    # The aggregate row is durable before this function is called. Remove only
    # this validated, direct child bundle after its archive and both integrity
    # sidecars are durable and verified.
    rm -rf -- "$bundle"
    sync -f "$EVIDENCE_ROOT"
    enforce_storage_policy "$bundle_id" "$archive" "$status"
    log "bundle_archived=${bundle_id} archive_kind=compact status=${status} archive_sha256=$(sha256_file "$archive")"
}

record_bundle() {
    local bundle="$1"
    local observer_exit="$2"
    local bundle_id terminal profitability run_header
    local finalizer_exit=1 aggregate_exit="-" status elapsed closed previous_closed closed_delta
    local observed_elapsed
    local total_net_pnl profit_factor shadow_executions start_wall_ms
    local terminal_hash profitability_hash identity_hash sequence recorded_at row

    bundle_id="$(basename "$bundle")"
    bundle_id_is_safe "$bundle_id" || fail "unsafe_bundle_id_for_aggregate"
    [[ "$(dirname "$bundle")" == "$EVIDENCE_ROOT" ]] || fail "unsafe_aggregate_bundle_parent"
    if bundle_is_indexed "$bundle_id"; then
        local indexed_status
        indexed_status="$(awk -F '\t' -v id="$bundle_id" 'NR > 1 && $14 == id {print $4; exit}' "$WINDOW_INDEX")"
        archive_bundle "$bundle" "$indexed_status"
        return 0
    fi

    terminal="${bundle}/terminal-summary.json"
    profitability="${bundle}/profitability-summary.json"
    run_header="${bundle}/run-header.json"
    if [[ -f "$terminal" ]]; then
        set +e
        "$OBSERVER" --config "$CONFIG" \
            --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
            finalize-qualification \
            --bundle "$bundle" \
            --isolation-report "$ISOLATION_REPORT" \
            2> "${bundle}/finalizer.stderr.log"
        finalizer_exit=$?
        set -e
        if (( finalizer_exit != 0 )); then
            log "alert=evidence_finalization_failed bundle=${bundle_id} diagnostic_sha256=$(sha256_file "${bundle}/finalizer.stderr.log")"
        fi
    fi

    observed_elapsed="-"
    [[ -f "$terminal" ]] \
        && observed_elapsed="$(json_scalar "$terminal" monotonic_elapsed_seconds || printf '-')"
    identity_hash="$(sha256_file "$VOLUME_IDENTITY")"
    if [[ -f "$run_header" ]]; then
        set +e
        "$OBSERVER" --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
            aggregate-qualification \
            --config "$CONFIG" \
            --bundle "$bundle" \
            --output "$AGGREGATE_ROOT" \
            --frozen-identity-sha256 "$identity_hash" \
            --minimum-observation-seconds "$((MINIMUM_DAYS * 86400))" \
            --minimum-closed-episodes "$MINIMUM_CLOSED"
        aggregate_exit=$?
        set -e
    fi

    elapsed="$observed_elapsed"
    closed="$(last_cumulative_closed)"
    start_wall_ms="-"
    [[ -f "$terminal" ]] && closed="$(json_scalar "$terminal" closed_shadow_episodes || printf '%s' "$closed")"
    [[ -f "$run_header" ]] && start_wall_ms="$(json_scalar "$run_header" start_wall_clock_utc_ms || printf '-')"

    total_net_pnl="-"
    profit_factor="-"
    shadow_executions="-"
    if [[ -f "$profitability" ]]; then
        total_net_pnl="$(json_scalar "$profitability" total_net_pnl || printf '-')"
        profit_factor="$(json_scalar "$profitability" profit_factor || printf '-')"
        shadow_executions="$(json_scalar "$profitability" shadow_executions || printf '-')"
    fi

    if [[ "$elapsed" == "$WINDOW_SECONDS" \
        && "$finalizer_exit" -eq 0 \
        && "$aggregate_exit" == "0" \
        && -f "$profitability" ]]; then
        status="complete"
    elif [[ "$elapsed" == "$WINDOW_SECONDS" ]]; then
        status="evidence_invalid"
    else
        status="incomplete"
    fi

    [[ "$closed" =~ ^[0-9]+$ ]] || fail "invalid_closed_episode_count_${bundle_id}"
    previous_closed="$(last_cumulative_closed)"
    (( closed >= previous_closed )) || fail "closed_episode_count_regressed_${bundle_id}"
    closed_delta=$((closed - previous_closed))

    terminal_hash="-"
    profitability_hash="-"
    [[ -f "$terminal" ]] && terminal_hash="$(sha256_file "$terminal")"
    [[ -f "$profitability" ]] && profitability_hash="$(sha256_file "$profitability")"
    sequence="$(next_sequence)"
    recorded_at="$(date +%s)"
    row="$(
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
            "$sequence" \
            "$start_wall_ms" \
            "$recorded_at" \
            "$status" \
            "$observer_exit" \
            "$finalizer_exit" \
            "$aggregate_exit" \
            "$elapsed" \
            "$closed" \
            "$closed_delta" \
            "$total_net_pnl" \
            "$profit_factor" \
            "$shadow_executions" \
            "$bundle_id" \
            "$terminal_hash" \
            "$profitability_hash" \
            "$identity_hash" \
            "archives/${bundle_id}.tar.gz"
    )"
    append_index_row "$row"
    RECORDED_WINDOW_STATUS="$status"
    log "window_indexed=${bundle_id} status=${status} observer_exit=${observer_exit} aggregate_exit=${aggregate_exit} closed_delta=${closed_delta}"

    archive_bundle "$bundle" "$status"
}

reconcile_existing_bundles() {
    local bundle
    while IFS= read -r bundle; do
        [[ -n "$bundle" ]] || continue
        record_bundle "$bundle" "-1"
    done < <(
        find "$EVIDENCE_ROOT" \
            -mindepth 1 \
            -maxdepth 1 \
            -type d \
            -name '[0-9]*-[0-9]*-[0-9a-f]*' \
            -print \
            | LC_ALL=C sort
    )

    local archive bundle_id
    while IFS= read -r archive; do
        [[ -n "$archive" ]] || continue
        bundle_id="$(basename "$archive" .tar.gz)"
        bundle_is_indexed "$bundle_id" || fail "archived_bundle_missing_from_aggregate_${bundle_id}"
        [[ -f "${archive}.sha256" ]] \
            || fail "archived_bundle_sidecar_missing_${bundle_id}"
        [[ -f "${ARCHIVE_ROOT}/${bundle_id}.contents.sha256" ]] \
            || fail "archived_bundle_contents_sidecar_missing_${bundle_id}"
        gzip -t "$archive" || fail "archived_bundle_gzip_invalid_${bundle_id}"
        (
            cd "$ARCHIVE_ROOT"
            sha256sum -c "$(basename "${archive}.sha256")"
        ) || fail "archived_bundle_failed_startup_check_${bundle_id}"
    done < <(
        find "$ARCHIVE_ROOT" -maxdepth 1 -type f -name '*.tar.gz' -print | LC_ALL=C sort
    )

    local aggregate_exit
    while IFS=$'\t' read -r bundle_id aggregate_exit; do
        [[ -n "$bundle_id" ]] || continue
        [[ -f "${ARCHIVE_ROOT}/${bundle_id}.tar.gz" ]] \
            || fail "indexed_bundle_archive_missing_${bundle_id}"
        [[ -f "${ARCHIVE_ROOT}/${bundle_id}.tar.gz.sha256" ]] \
            || fail "indexed_bundle_archive_sidecar_missing_${bundle_id}"
        [[ -f "${ARCHIVE_ROOT}/${bundle_id}.contents.sha256" ]] \
            || fail "indexed_bundle_contents_sidecar_missing_${bundle_id}"
        if [[ "$aggregate_exit" == "0" ]]; then
            [[ -f "${AGGREGATE_ROOT}/windows/${bundle_id}.json" ]] \
                || fail "indexed_bundle_canonical_record_missing_${bundle_id}"
        fi
    done < <(awk -F '\t' 'NR > 1 {print $14 "\t" $7}' "$WINDOW_INDEX")

    local aggregate_record
    if [[ -d "${AGGREGATE_ROOT}/windows" ]]; then
        while IFS= read -r aggregate_record; do
            [[ -n "$aggregate_record" ]] || continue
            bundle_id="$(basename "$aggregate_record" .json)"
            bundle_id_is_safe "$bundle_id" \
                || fail "unsafe_canonical_aggregate_record_name"
            bundle_is_indexed "$bundle_id" \
                || fail "canonical_record_missing_from_window_index_${bundle_id}"
        done < <(
            find "${AGGREGATE_ROOT}/windows" \
                -maxdepth 1 \
                -type f \
                -name '*.json' \
                -print \
                | LC_ALL=C sort
        )
    fi

    if awk -F '\t' 'NR > 1 && $4 == "finalization_failed" {found=1} END {exit !found}' \
        "$WINDOW_INDEX"; then
        fail "aggregate_contains_unfinalized_complete_window"
    fi
}

record_is_complete() {
    [[ -f "$AGGREGATE_SUMMARY" ]] || return 1
    [[ "$(json_scalar "$AGGREGATE_SUMMARY" record_complete)" == "true" ]] || return 1
    # Every audit-index row must also have reached the canonical aggregate.
    ! awk -F '\t' 'NR > 1 && $7 != "0" {missing=1} END {exit !missing}' \
        "$WINDOW_INDEX"
}

mark_complete_and_idle() {
    local now elapsed completed cumulative
    now="$(date +%s)"
    elapsed="$(json_scalar "$AGGREGATE_SUMMARY" completed_observation_seconds)"
    completed="$(json_scalar "$AGGREGATE_SUMMARY" completed_four_hour_windows)"
    cumulative="$(json_scalar "$AGGREGATE_SUMMARY" independent_closed_episodes)"
    atomic_write "$COMPLETE_MARKER" "$(
        printf '{\n'
        printf '  "schema_version": 1,\n'
        printf '  "completed_at_epoch_seconds": %s,\n' "$now"
        printf '  "elapsed_seconds": %s,\n' "$elapsed"
        printf '  "completed_four_hour_windows": %s,\n' "$completed"
        printf '  "cumulative_closed_episodes": %s,\n' "$cumulative"
        printf '  "frozen_identity_sha256": "%s"\n' "$(sha256_file "$VOLUME_IDENTITY")"
        printf '}'
    )"
    log "authoritative_record_complete=true completed_windows=${completed} cumulative_closed=${cumulative}"
    while (( STOP_REQUESTED == 0 )); do
        sleep 60 &
        wait $! || true
    done
    exit 0
}

hold_fatal_stop() {
    log "fatal_stop_present=true marker=${FATAL_STOP_MARKER} worker_quiescent=true"
    while (( STOP_REQUESTED == 0 )); do
        sleep 60 &
        wait $! || true
    done
    log "fatal_stop_quiescent_exit=true"
    exit 0
}

run_window() {
    local before_file after_file bundle new_count run_status=0
    local requested_sequence started_at failure_classification error_evidence error_evidence_hash
    local observer_process_stderr
    before_file="$(mktemp)"
    after_file="$(mktemp)"
    find "$EVIDENCE_ROOT" \
        -mindepth 1 \
        -maxdepth 1 \
        -type d \
        -name '[0-9]*-[0-9]*-[0-9a-f]*' \
        -printf '%f\n' \
        | LC_ALL=C sort > "$before_file"

    requested_sequence="$(next_sequence)"
    started_at="$(date +%s)"
    atomic_write "$ACTIVE_WINDOW" "$(
        printf '{\n'
        printf '  "sequence": %s,\n' "$requested_sequence"
        printf '  "started_at_epoch_seconds": %s,\n' "$started_at"
        printf '  "status": "running",\n'
        printf '  "duration_seconds": %s\n' "$WINDOW_SECONDS"
        printf '}'
    )"
    log "window_started_sequence=${requested_sequence}"
    observer_process_stderr="${LOG_ROOT}/observer-window-${requested_sequence}.stderr.log"

    "$OBSERVER" qualify-profitability \
        --config "$CONFIG" \
        --very-profitable-layer "$VERY_PROFITABLE_LAYER" \
        --request-policy "$READ_POLICY" \
        --transport-policy "$TRANSPORT_POLICY" \
        --release-manifest "$MANIFEST" \
        --isolation-report "$ISOLATION_REPORT" \
        --state-root "$STATE_ROOT" \
        --duration 4h \
        --output "$EVIDENCE_ROOT" \
        2> "$observer_process_stderr" &
    OBSERVER_PID=$!

    set +e
    while true; do
        wait "$OBSERVER_PID"
        run_status=$?
        if ! kill -0 "$OBSERVER_PID" 2>/dev/null; then
            break
        fi
    done
    set -e
    OBSERVER_PID=""
    sync -f "$observer_process_stderr"
    if [[ -s "$observer_process_stderr" ]]; then
        tail -n 200 "$observer_process_stderr" >&2
    fi

    find "$EVIDENCE_ROOT" \
        -mindepth 1 \
        -maxdepth 1 \
        -type d \
        -name '[0-9]*-[0-9]*-[0-9a-f]*' \
        -printf '%f\n' \
        | LC_ALL=C sort > "$after_file"
    bundle="$(comm -13 "$before_file" "$after_file" || true)"
    rm -f -- "$before_file" "$after_file"
    new_count="$(printf '%s\n' "$bundle" | sed '/^$/d' | wc -l | tr -d '[:space:]')"
    if [[ "$new_count" == "0" && "$STOP_REQUESTED" -eq 1 ]]; then
        atomic_write "$ACTIVE_WINDOW" "$(
            printf '{\n'
            printf '  "sequence": %s,\n' "$requested_sequence"
            printf '  "started_at_epoch_seconds": %s,\n' "$started_at"
            printf '  "ended_at_epoch_seconds": %s,\n' "$(date +%s)"
            printf '  "status": "incomplete_no_evidence_bundle"\n'
            printf '}'
        )"
        log "window_incomplete_without_bundle_sequence=${requested_sequence}"
        WINDOW_FAILURE_CLASSIFICATION="shutdown"
        return 0
    fi
    [[ "$new_count" == "1" ]] || fail "observer_created_${new_count}_evidence_bundles"
    bundle="${EVIDENCE_ROOT}/${bundle}"

    failure_classification="$(
        classify_observer_failure "$bundle" "$run_status" "$observer_process_stderr"
    )"
    if (( STOP_REQUESTED == 1 )); then
        failure_classification="shutdown"
    fi
    error_evidence="$observer_process_stderr"
    [[ -s "$error_evidence" ]] || error_evidence="${bundle}/observer.stderr.log"
    [[ -f "$error_evidence" ]] || error_evidence="${bundle}/terminal-summary.json"
    if [[ -f "$error_evidence" ]]; then
        error_evidence_hash="$(sha256_file "$error_evidence")"
    else
        error_evidence_hash="$(printf '%s' "${failure_classification}:${run_status}" | sha256sum | awk '{print $1}')"
    fi
    record_bundle "$bundle" "$run_status"
    local indexed_status="$RECORDED_WINDOW_STATUS"
    failure_classification="$(
        normalize_recorded_failure "$indexed_status" "$failure_classification"
    )"
    atomic_write "$ACTIVE_WINDOW" "$(
        printf '{\n'
        printf '  "sequence": %s,\n' "$requested_sequence"
        printf '  "started_at_epoch_seconds": %s,\n' "$started_at"
        printf '  "ended_at_epoch_seconds": %s,\n' "$(date +%s)"
        printf '  "status": "%s",\n' "$indexed_status"
        printf '  "bundle_id": "%s"\n' "$(basename "$bundle")"
        printf '}'
    )"
    WINDOW_FAILURE_CLASSIFICATION="$failure_classification"
    if [[ "$failure_classification" != "none" \
        && "$failure_classification" != "evidence_finalization_failure" \
        && "$failure_classification" != "transient_transport" \
        && "$failure_classification" != "shutdown" ]]; then
        write_fatal_stop_marker \
            "$failure_classification" \
            "$requested_sequence" \
            "$(basename "$bundle")" \
            "$error_evidence_hash"
    fi
}

main() {
    if [[ -e "$FATAL_STOP_MARKER" ]]; then
        hold_fatal_stop
    fi
    validate_environment
    validate_embedded_release
    initialize_data_root
    log "unsigned=true planned_only=true submission_capable=false frozen_identity_verified=true"
    reconcile_existing_bundles

    if [[ -e "$COMPLETE_MARKER" ]]; then
        record_is_complete || fail "record_complete_marker_inconsistent"
        mark_complete_and_idle
    fi

    local transient_retries=0
    while (( STOP_REQUESTED == 0 )); do
        if record_is_complete; then
            mark_complete_and_idle
        fi
        run_window
        case "$WINDOW_FAILURE_CLASSIFICATION" in
            none|evidence_finalization_failure)
                transient_retries=0
                ;;
            transient_transport)
                transient_retries=$((transient_retries + 1))
                if (( transient_retries > MAX_TRANSIENT_RETRIES )); then
                    write_fatal_stop_marker \
                        "transient_retry_exhausted" \
                        "$(json_scalar "$ACTIVE_WINDOW" sequence)" \
                        "$(json_scalar "$ACTIVE_WINDOW" bundle_id)" \
                        "$(sha256_file "$ACTIVE_WINDOW")"
                    exit 70
                fi
                local backoff=$((TRANSIENT_RETRY_BASE_SECONDS * (1 << (transient_retries - 1))))
                log "transient_retry=${transient_retries}/${MAX_TRANSIENT_RETRIES} backoff_seconds=${backoff}"
                sleep "$backoff" &
                wait $! || true
                ;;
            *)
                exit 70
                ;;
        esac
    done

    log "worker_exit_after_shutdown=true"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi

