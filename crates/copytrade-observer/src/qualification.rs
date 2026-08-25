#[cfg(feature = "research-cli")]
use crate::cohort_layer::VeryProfitableLayerArtifact;
use crate::execution::{
    recoverable_reconciliation_error, ExecutionMode, LiveExecutionRuntime, LiveExecutionSettings,
    LiveExecutionUpdate,
};
use crate::live_shadow::{
    EconomicAttribution, LiveShadowEngine, ProductionIntentIdentity, UnresolvedRootStatus,
    UnsignedShadowStateIdentity,
};
#[cfg(feature = "research-cli")]
use crate::profitability::summarize_profitability;
#[cfg(feature = "research-cli")]
use crate::profitability::ProfitabilitySummary;
use crate::public_mainnet::{
    HyperliquidPublicTransport, PublicTransportPolicy, SourceFill, SourceFillRangeResponse,
    SourceStateResponse,
};
use crate::qualification_evidence::{
    load_release_manifest, sha256_file, CheckpointPayload, LatencyEvidence, RunHeader,
};
#[cfg(feature = "research-cli")]
use crate::qualification_evidence::{verify_checkpoint_chain, EvidenceBundle, TerminalSummary};
#[cfg(feature = "research-cli")]
use crate::replay::verify_replay_event_chain;
use crate::replay::ReplayPayload;
use crate::state_root::{StateRootStartup, UnsignedStateRoot};
use crate::streaming::{
    parse_asset_contexts, valid_market, MarketDirectory, StreamingEvent, StreamingHandle,
    StreamingSourceBook, MAX_HOT_BOOKS,
};
use crate::ObserverCoreState;
use copytrade_core::decision::{derive_config_hash, derive_risk_policy_hash};
use copytrade_core::scheduler::{
    BudgetClass, Clock, ExecutionOutcome, MonotonicClock, ReadOnlyDataSource,
    ReadOnlySchedulerConfig, ReadRequestKind, RequestKey, RequestPriority, RequestScheduler,
    RequestSubject, ScheduleOutcome, ScheduledReadRequest, SchedulerHealth, SourceTier, Timestamp,
};
use copytrade_core::CopyTradeConfig;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

pub const MINIMUM_QUALIFICATION_SECONDS: u64 = 86_400;
const MAX_ACTIVE_SOURCE_CANDIDATES: usize = 100;
const MAX_RECOVERED_SOURCE_FILLS: usize = 32_768;

#[derive(Default)]
struct HistoryRecovery {
    snapshots: BTreeMap<String, SourceStateResponse>,
    ranges: BTreeMap<String, VecDeque<(u64, u64)>>,
    fills: Vec<SourceFill>,
    fill_counts: BTreeMap<String, usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FillRangeProgress {
    Next,
    WalletComplete,
    Restart,
    Stale,
}

impl HistoryRecovery {
    fn record_snapshot(&mut self, start: u64, state: SourceStateResponse) -> bool {
        if state.source_time_ms < start {
            return false;
        }
        let wallet = state.candidate_id.clone();
        self.ranges.insert(
            wallet.clone(),
            VecDeque::from([(start.saturating_sub(1), state.source_time_ms)]),
        );
        self.fill_counts.insert(wallet.clone(), 0);
        self.snapshots.insert(wallet, state);
        true
    }

    fn next_range(&self, wallet: &str) -> Option<(u64, u64)> {
        self.ranges.get(wallet)?.front().copied()
    }

    fn record_range(&mut self, range: &mut SourceFillRangeResponse) -> FillRangeProgress {
        let Some(ranges) = self.ranges.get_mut(&range.wallet) else {
            return FillRangeProgress::Stale;
        };
        if ranges.front().copied() != Some((range.start_time_ms, range.end_time_ms)) {
            return FillRangeProgress::Stale;
        }
        let total = self.fill_counts.entry(range.wallet.clone()).or_default();
        if range.raw_count == 2_000 {
            if range.start_time_ms == range.end_time_ms
                || total.saturating_add(range.raw_count) >= 10_000
            {
                return FillRangeProgress::Restart;
            }
            ranges.pop_front();
            let midpoint = range.start_time_ms + (range.end_time_ms - range.start_time_ms) / 2;
            ranges.push_front((midpoint + 1, range.end_time_ms));
            ranges.push_front((range.start_time_ms, midpoint));
            return FillRangeProgress::Next;
        }
        ranges.pop_front();
        if self.fills.len().saturating_add(range.fills.len()) > MAX_RECOVERED_SOURCE_FILLS {
            return FillRangeProgress::Restart;
        }
        *total = total.saturating_add(range.raw_count);
        self.fills.append(&mut range.fills);
        if *total >= 10_000 {
            FillRangeProgress::Restart
        } else if ranges.is_empty() {
            FillRangeProgress::WalletComplete
        } else {
            FillRangeProgress::Next
        }
    }

    fn cutoff(&self) -> Option<u64> {
        self.snapshots
            .values()
            .map(|state| state.source_time_ms)
            .min()
    }

    fn completed_wallet(&self, wallet: &str) -> Option<(SourceStateResponse, Vec<SourceFill>)> {
        let ending = self.snapshots.get(wallet)?.clone();
        let fills = self
            .fills
            .iter()
            .filter(|fill| fill.wallet == wallet)
            .cloned()
            .collect();
        Some((ending, fills))
    }
}

fn select_hot_books(
    urgent: &BTreeSet<String>,
    required: &BTreeSet<String>,
    last_required: &BTreeMap<String, Timestamp>,
    subscribed: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut selected = BTreeSet::new();
    let mut add_tier = |mut assets: Vec<String>| {
        assets.sort_by_key(|asset| (!subscribed.contains(asset), asset.clone()));
        for asset in assets {
            if selected.len() == MAX_HOT_BOOKS {
                break;
            }
            if valid_market(&asset) {
                selected.insert(asset);
            }
        }
    };

    add_tier(urgent.iter().cloned().collect());
    add_tier(required.difference(urgent).cloned().collect::<Vec<_>>());

    let mut grace = last_required
        .iter()
        .filter(|(asset, _)| !required.contains(*asset))
        .map(|(asset, timestamp)| (asset.clone(), *timestamp))
        .collect::<Vec<_>>();
    grace.sort_by(|(left_asset, left_time), (right_asset, right_time)| {
        right_time
            .cmp(left_time)
            .then_with(|| {
                (!subscribed.contains(left_asset)).cmp(&!subscribed.contains(right_asset))
            })
            .then_with(|| left_asset.cmp(right_asset))
    });
    add_tier(grace.into_iter().map(|(asset, _)| asset).collect());
    selected
}

/// Research-only polling capacity used by the legacy qualification commands.
/// The production daemon uses stream continuity plus bounded REST baseline and
/// reconciliation work; it does not use polling age as source freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct SourceDispatchPlan {
    effective_candidate_count: usize,
    active_candidate_count: usize,
    inactive_candidate_count: usize,
    active_cadence_ms: u64,
    inactive_cadence_ms: u64,
    source_call_capacity_per_window: u64,
    active_dispatches_per_window: u64,
    inactive_dispatches_per_window: u64,
    combined_dispatches_per_window: u64,
}

pub struct QualificationOptions {
    pub config_path: PathBuf,
    pub very_profitable_layer_path: Option<PathBuf>,
    pub request_policy_path: PathBuf,
    pub transport_policy_path: PathBuf,
    pub release_manifest_path: PathBuf,
    pub isolation_report_path: PathBuf,
    pub output: PathBuf,
    pub state_root: Option<PathBuf>,
    pub duration_seconds: u64,
    pub transport_gate: bool,
    pub profitability_gate: bool,
    pub micro_density_gate: bool,
    /// Run the hardened observer as a continuous daemon. Qualification
    /// evidence, deadlines, and profitability exit gates are disabled; only
    /// safety/integrity errors terminate the process.
    pub continuous: bool,
}

struct RuntimeEvidence {
    #[cfg(feature = "research-cli")]
    bundle: Option<EvidenceBundle>,
}

impl RuntimeEvidence {
    #[cfg(feature = "research-cli")]
    fn qualification(bundle: EvidenceBundle) -> Self {
        Self {
            bundle: Some(bundle),
        }
    }

    fn continuous() -> Self {
        Self {
            #[cfg(feature = "research-cli")]
            bundle: None,
        }
    }

    fn enabled(&self) -> bool {
        #[cfg(feature = "research-cli")]
        {
            self.bundle.is_some()
        }
        #[cfg(not(feature = "research-cli"))]
        {
            false
        }
    }

    fn append_replay_event(
        &mut self,
        observed_at_mono: Timestamp,
        payload: ReplayPayload,
    ) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "research-cli")]
        if let Some(bundle) = &mut self.bundle {
            bundle.append_replay_event(observed_at_mono, payload)?;
        }
        #[cfg(not(feature = "research-cli"))]
        let _ = (observed_at_mono, payload);
        Ok(())
    }

    fn append_log(&mut self, stderr: bool, line: &str) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "research-cli")]
        if let Some(bundle) = &mut self.bundle {
            bundle.append_log(stderr, line)?;
        }
        #[cfg(not(feature = "research-cli"))]
        let _ = (stderr, line);
        Ok(())
    }

    fn append_checkpoint(&mut self, payload: CheckpointPayload) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "research-cli")]
        if let Some(bundle) = &mut self.bundle {
            bundle.append_checkpoint(payload)?;
        }
        #[cfg(not(feature = "research-cli"))]
        let _ = payload;
        Ok(())
    }

    fn write_summary<T: Serialize + ?Sized>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "research-cli")]
        if let Some(bundle) = &self.bundle {
            bundle.write_summary(name, value)?;
        }
        #[cfg(not(feature = "research-cli"))]
        let _ = (name, value);
        Ok(())
    }

    #[cfg(feature = "research-cli")]
    fn write_terminal(&self, summary: &TerminalSummary) -> Result<(), Box<dyn Error>> {
        if let Some(bundle) = &self.bundle {
            bundle.write_terminal(summary)?;
        }
        Ok(())
    }

    fn flush_replay_events(&mut self) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "research-cli")]
        if let Some(bundle) = &mut self.bundle {
            bundle.flush_replay_events()?;
        }
        Ok(())
    }
}

fn persist_unsigned_state(
    engine: &mut LiveShadowEngine,
    state_root: &mut Option<UnsignedStateRoot>,
) -> Result<(), Box<dyn Error>> {
    if let Some(state_root) = state_root {
        state_root.persist(engine)?;
    }
    Ok(())
}

fn append_new_cohort_records(
    evidence: &mut RuntimeEvidence,
    engine: &LiveShadowEngine,
    cursor: &mut usize,
    observed_at_mono: Timestamp,
) -> Result<(), Box<dyn Error>> {
    if !evidence.enabled() {
        *cursor = engine.cohort_indicator_records().len();
        return Ok(());
    }
    let records = engine.cohort_indicator_records();
    for indicator in records.get(*cursor..).unwrap_or_default() {
        evidence.append_replay_event(
            observed_at_mono,
            ReplayPayload::CohortIndicatorSnapshot {
                indicator: indicator.clone(),
            },
        )?;
    }
    *cursor = records.len();
    Ok(())
}

fn append_new_technical_records(
    evidence: &mut RuntimeEvidence,
    engine: &LiveShadowEngine,
    cursor: &mut usize,
) -> Result<(), Box<dyn Error>> {
    if !evidence.enabled() {
        *cursor = engine.technical_decision_records().len();
        return Ok(());
    }
    let records = engine.technical_decision_records();
    for decision in records.get(*cursor..).unwrap_or_default() {
        evidence.append_replay_event(
            decision.observed_at_mono,
            ReplayPayload::TechnicalDecisionSnapshot {
                decision: decision.clone(),
            },
        )?;
    }
    *cursor = records.len();
    Ok(())
}

#[derive(Debug, Default, Serialize)]
struct Counters {
    schedule_rejections: u64,
    replaceable_schedule_shed: u64,
    completed_fresh: u64,
    completed_stale: u64,
    retries: u64,
    retry_exhausted: u64,
    invalid_responses: u64,
    permanent_failures: u64,
    total_weight: u64,
    reserved_weight: u64,
    maximum_pending: usize,
    maximum_successors: usize,
    maximum_in_flight: usize,
    response_rejected_as_stale: u64,
    accepted_count_by_tier: BTreeMap<SourceTier, u64>,
    queue_wait_ms_by_tier: BTreeMap<SourceTier, LatencyEvidence>,
    transport_latency_ms_by_kind: BTreeMap<ReadRequestKind, LatencyEvidence>,
}

#[derive(Serialize)]
struct RollingEconomicStatus {
    horizon: &'static str,
    cutoff_mono: Timestamp,
    archived_episode_totals_included: bool,
    attribution_scope: &'static str,
    closures: usize,
    executions: usize,
    turnover: Decimal,
    gross_pnl: Decimal,
    fees: Decimal,
    slippage: Decimal,
    funding: Decimal,
    net_pnl: Decimal,
    net_pnl_per_turnover: Decimal,
    starting_settled_equity: Decimal,
    settled_equity_return: Decimal,
    profit_factor_state: &'static str,
    profit_factor: Option<Decimal>,
    maximum_drawdown: Decimal,
    annualized_sharpe_5m: f64,
    net_pnl_by_asset: BTreeMap<String, Decimal>,
    net_pnl_by_source: BTreeMap<String, Decimal>,
    execution_net_pnl_by_context: BTreeMap<EconomicAttribution, Decimal>,
}

#[derive(Serialize)]
struct ContinuousStatus<'a> {
    mode: &'static str,
    run_id: &'a str,
    started_at_mono: Timestamp,
    observed_at_mono: Timestamp,
    elapsed_seconds: u64,
    healthy: bool,
    fatal_stop: bool,
    snapshot_generation: Option<u64>,
    candidate_count: usize,
    projection_violations: u64,
    persistence_failures: u64,
    unresolved_roots: usize,
    unresolved_root_details: Vec<UnresolvedRootStatus>,
    open_episodes: usize,
    closed_episodes_lifetime: u64,
    scheduler_pending: usize,
    scheduler_in_flight: usize,
    rest_weight_consumed_process: u64,
    rest_reserved_weight_consumed_process: u64,
    rate_limited_responses: u64,
    streaming: Option<StreamingRuntimeStatus>,
    unique_source_transitions: u64,
    modeled_executions_retained: usize,
    mfce_completed_transition_labels: usize,
    mfce_active_transitions: usize,
    mfce_awaiting_live_books: usize,
    mfce_book_ready_transitions: usize,
    mfce_model_epoch: Option<u64>,
    mfce_policy_exploit: u64,
    mfce_policy_explore: u64,
    mfce_policy_reject: u64,
    source_risk_increases_allocated: u64,
    source_risk_increases_rejected_by_mfce: u64,
    marked_equity: Decimal,
    settled_equity: Decimal,
    open_marked_equity_contribution: Decimal,
    mfce: crate::mfce::MfceReport,
    economics: Vec<RollingEconomicStatus>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct StreamingRuntimeStatus {
    source_healthy: bool,
    hydrated_source_count: usize,
    discovered_markets: usize,
    subscribed_trade_markets: usize,
    coverage_ready_markets: usize,
    coverage_stale_markets: usize,
    coverage_reconciliation_wallets_pending: usize,
    coverage_reconciliations: u64,
    subscribed_hot_books: usize,
    connections: u64,
    gaps: u64,
    public_trades_seen: u64,
    tracked_trade_updates: u64,
    book_updates: u64,
    invalid_messages: u64,
}

pub async fn run_continuous_daemon(
    mut options: QualificationOptions,
) -> Result<PathBuf, Box<dyn Error>> {
    options.continuous = true;
    run_qualification_impl::<true>(options).await
}

#[cfg(feature = "research-cli")]
pub async fn run_qualification(options: QualificationOptions) -> Result<PathBuf, Box<dyn Error>> {
    if options.continuous {
        run_qualification_impl::<true>(options).await
    } else {
        run_qualification_impl::<false>(options).await
    }
}

async fn run_qualification_impl<const CONTINUOUS: bool>(
    options: QualificationOptions,
) -> Result<PathBuf, Box<dyn Error>> {
    let execution_mode = if CONTINUOUS {
        ExecutionMode::from_environment()?
    } else {
        ExecutionMode::Shadow
    };
    let live_settings = LiveExecutionSettings::load_if_live(execution_mode)?;
    if CONTINUOUS
        && (options.transport_gate || options.profitability_gate || options.micro_density_gate)
    {
        return Err("continuous runtime cannot enable qualification gates".into());
    }
    if CONTINUOUS && options.state_root.is_none() {
        return Err("continuous runtime requires --state-root".into());
    }
    if options.state_root.is_some() && !options.profitability_gate && !CONTINUOUS {
        return Err(
            "--state-root is supported only by unsigned profitability or continuous runs".into(),
        );
    }
    if CONTINUOUS {
        // No elapsed-time or profitability lifecycle controls the daemon.
    } else if options.micro_density_gate {
        if !(600..=1_800).contains(&options.duration_seconds) {
            return Err("micro-density gate duration must be 10–30 minutes".into());
        }
    } else if options.transport_gate {
        if !(600..=900).contains(&options.duration_seconds) {
            return Err("transport gate duration must be 10–15 minutes".into());
        }
    } else if options.profitability_gate {
        if options.duration_seconds != 14_400 {
            return Err("profitability gate requires exactly 14400 monotonic seconds".into());
        }
    } else if options.duration_seconds < MINIMUM_QUALIFICATION_SECONDS {
        return Err("HL1K requires at least 86400 monotonic seconds".into());
    }
    let observer_state = ObserverCoreState::load_with_very_profitable_layer(
        &options.config_path,
        options.very_profitable_layer_path.as_ref(),
    )?;
    let config = observer_state.config().clone();
    let very_profitable_layer = observer_state.very_profitable_layer().cloned();
    let scheduler_config = ReadOnlySchedulerConfig::from_path(&options.request_policy_path)?;
    let transport_policy: PublicTransportPolicy =
        serde_json::from_slice(&std::fs::read(&options.transport_policy_path)?)?;
    transport_policy
        .validate()
        .map_err(|error| format!("invalid transport policy: {error:?}"))?;
    let streaming_enabled = transport_policy.streaming.enabled;
    if CONTINUOUS && !streaming_enabled {
        return Err("continuous production requires the HD1 streaming data plane".into());
    }
    let expanded_source_candidates = if streaming_enabled {
        config
            .candidates
            .iter()
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect()
    } else {
        very_profitable_layer
            .as_ref()
            .map(|layer| layer.qualified_members.clone())
            .unwrap_or_default()
    };
    let active_freshness = transport_policy
        .source_deadline_ms(SourceTier::Active)
        .ok_or("active freshness overflow")?;
    let inactive_freshness = transport_policy
        .source_deadline_ms(SourceTier::Inactive)
        .ok_or("inactive freshness overflow")?;
    if !streaming_enabled
        && (config.global_risk.source_snapshot_max_age_ms != inactive_freshness
            || scheduler_config.freshness.configured_max_age_ms != inactive_freshness)
    {
        return Err("configured freshness must equal the maximum derived tier deadline".into());
    }
    let source_dispatch_plan = if streaming_enabled {
        streaming_source_dispatch_plan(
            config.candidates.len(),
            &scheduler_config,
            &transport_policy,
        )?
    } else {
        derive_source_dispatch_plan_with_expansion(
            config.candidates.len(),
            expanded_source_candidates.len(),
            transport_policy.perp_dexes.len(),
            &scheduler_config,
            &transport_policy,
        )?
    };
    // Build assurance is deliberately not runtime authority for the continuous
    // unsigned daemon. CI verifies isolation, source identity, and release
    // reproducibility before deployment; production startup validates only the
    // runtime configuration, durable state, and hard trading invariants.
    let manifest = if CONTINUOUS {
        None
    } else {
        let manifest = load_release_manifest(&options.release_manifest_path)?;
        verify_isolation_report(&options.isolation_report_path)?;
        let production_manifest = manifest.qualification_stage == "PRODUCTION_RELEASE";
        let stage_matches = manifest_stage_matches(false, &manifest.qualification_stage);
        let binary_matches = if production_manifest {
            manifest.observer_binary_sha256.as_deref()
                == Some(&sha256_file(std::env::current_exe()?)?)
        } else {
            manifest.binary_sha256 == sha256_file(std::env::current_exe()?)?
        };
        if !stage_matches
            || manifest.configuration_sha256 != derive_config_hash(&config)?.to_hex()
            || manifest.risk_policy_sha256 != derive_risk_policy_hash(&config.global_risk)?.to_hex()
            || !binary_matches
        {
            return Err("release manifest mismatch".into());
        }
        Some(manifest)
    };
    let binary_sha256 = sha256_file(std::env::current_exe()?)?;
    let configuration_sha256 = derive_config_hash(&config)?.to_hex();
    let risk_policy_sha256 = derive_risk_policy_hash(&config.global_risk)?.to_hex();
    let wall_ms: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let clock = MonotonicClock::default();
    let start = clock.now_ms();
    let run_id = format!(
        "{}-{}-{}",
        wall_ms,
        std::process::id(),
        &binary_sha256[..12]
    );
    let output = if CONTINUOUS {
        options.output.clone()
    } else {
        options.output.join(&run_id)
    };
    let read_policy_value = serde_json::to_value(&scheduler_config)?;
    let transport_policy_value = serde_json::to_value(&transport_policy)?;
    let header = RunHeader {
        stage: {
            #[cfg(not(feature = "research-cli"))]
            {
                "SU6-CONTINUOUS".into()
            }
            #[cfg(feature = "research-cli")]
            {
                if CONTINUOUS {
                    "SU6-CONTINUOUS".into()
                } else if options.micro_density_gate {
                    "SU5R-MICRO-DENSITY".into()
                } else if options.transport_gate {
                    "HL1K-TRANSPORT-GATE".into()
                } else if options.profitability_gate {
                    "HL1-PROFITABILITY-4H".into()
                } else {
                    "HL1K".into()
                }
            }
        },
        run_id,
        binary_sha256,
        source_tree_sha256: manifest
            .as_ref()
            .map(|manifest| manifest.source_tree_sha256.clone())
            .unwrap_or_else(|| "build-time-only".into()),
        git_commit: manifest
            .as_ref()
            .map(|manifest| manifest.git_commit.clone())
            .unwrap_or_else(|| "build-time-only".into()),
        git_tree_state: manifest
            .as_ref()
            .map(|manifest| manifest.git_tree_state.clone())
            .unwrap_or_else(|| "build-time-only".into()),
        configuration_sha256,
        risk_policy_sha256,
        read_policy_sha256: sha256_json(&read_policy_value)?,
        transport_policy_sha256: sha256_json(&transport_policy_value)?,
        start_wall_clock_utc_ms: wall_ms,
        start_monotonic_ms: start,
        expected_minimum_duration_seconds: if CONTINUOUS {
            0
        } else {
            options.duration_seconds
        },
        candidate_count: config.candidates.len(),
        process_id: std::process::id(),
        host_identity: std::env::var("HOSTNAME").unwrap_or_else(|_| "unavailable".into()),
        unsigned: execution_mode == ExecutionMode::Shadow,
        planned_only: execution_mode == ExecutionMode::Shadow,
        submission_capable: execution_mode == ExecutionMode::Live,
        api_wallet_present: execution_mode == ExecutionMode::Live,
    };
    #[cfg(feature = "research-cli")]
    let qualification_config = serde_json::json!({"copytrade":config,"read_policy":read_policy_value,"transport_policy":transport_policy_value});
    let mut evidence = if CONTINUOUS {
        std::fs::create_dir_all(&output)?;
        RuntimeEvidence::continuous()
    } else {
        #[cfg(feature = "research-cli")]
        {
            RuntimeEvidence::qualification(EvidenceBundle::create(
                &output,
                &options.release_manifest_path,
                &qualification_config,
                &header,
                &options.isolation_report_path,
            )?)
        }
        #[cfg(not(feature = "research-cli"))]
        unreachable!("qualification evidence is excluded from the production build")
    };
    if !CONTINUOUS {
        if let Some(layer_path) = &options.very_profitable_layer_path {
            std::fs::copy(layer_path, output.join("very-profitable-layer.json"))?;
        }
    }
    if options.very_profitable_layer_path.is_some() {
        evidence.append_log(
            false,
            "very_profitable_layer=installed hyperdash_role=discovery_and_context hyperliquid_role=authoritative_state",
        )?;
    }
    evidence.append_replay_event(
        start,
        ReplayPayload::RunContext {
            replay_chain_version: crate::replay::REPLAY_CHAIN_VERSION,
            canonicalization: crate::replay::REPLAY_CANONICALIZATION.into(),
            binary_sha256: header.binary_sha256.clone(),
            configuration_sha256: header.configuration_sha256.clone(),
            risk_policy_sha256: header.risk_policy_sha256.clone(),
            read_policy_sha256: header.read_policy_sha256.clone(),
            transport_policy_sha256: header.transport_policy_sha256.clone(),
        },
    )?;
    if !CONTINUOUS {
        std::fs::copy(
            std::env::current_exe()?,
            output.join("observer-release-binary"),
        )?;
    }
    evidence.append_log(
        false,
        if execution_mode == ExecutionMode::Live {
            "execution_mode=live signer=in_process submission_capable=true"
        } else {
            "unsigned=true planned_only=true submission_capable=false api_wallet_present=false"
        },
    )?;
    let scheduler = RequestScheduler::new(
        clock.clone(),
        scheduler_config.api.clone(),
        scheduler_config.retry.clone(),
    )?;
    let transport = HyperliquidPublicTransport::new(clock.clone(), transport_policy.clone())
        .map_err(|error| format!("transport: {error:?}"))?;
    transport
        .set_fee_user(config.follower_address.as_deref())
        .map_err(|error| format!("live fee subject: {error:?}"))?;
    transport
        .set_expanded_source_candidates(expanded_source_candidates.iter().cloned())
        .map_err(|error| format!("expanded source cohort: {error:?}"))?;
    evidence.write_summary("source-dispatch-plan.json", &source_dispatch_plan)?;
    evidence.append_log(
        false,
        &format!(
            "source_dispatch_plan=derived effective_candidates={} active={} inactive={} active_per_window={} inactive_per_window={} combined_per_window={} capacity_per_window={}",
            source_dispatch_plan.effective_candidate_count,
            source_dispatch_plan.active_candidate_count,
            source_dispatch_plan.inactive_candidate_count,
            source_dispatch_plan.active_dispatches_per_window,
            source_dispatch_plan.inactive_dispatches_per_window,
            source_dispatch_plan.combined_dispatches_per_window,
            source_dispatch_plan.source_call_capacity_per_window,
        ),
    )?;
    let mut engine = LiveShadowEngine::new(
        config.clone(),
        header.binary_sha256.as_bytes(),
        header.run_id.clone(),
        active_freshness,
        inactive_freshness,
    )
    .map_err(|error| error.to_string())?;
    if streaming_enabled {
        engine.enable_source_stream_mode();
    }
    if let Some(layer) = very_profitable_layer {
        engine
            .install_very_profitable_layer(layer)
            .map_err(|error| error.to_string())?;
    }
    let mut cohort_record_cursor = 0usize;
    let mut technical_record_cursor = 0usize;
    let state_identity = UnsignedShadowStateIdentity {
        // Retain the snapshot identity shape while removing build artifacts
        // from restore authority. Runtime compatibility is bound to the
        // validated configuration/risk contract and persistence schema.
        source_tree_sha256: "build-time-only".into(),
        observer_binary_sha256: "build-time-only".into(),
        configuration_sha256: header.configuration_sha256.clone(),
        risk_policy_sha256: header.risk_policy_sha256.clone(),
    };
    let mut state_root = options
        .state_root
        .as_ref()
        .map(|root| UnsignedStateRoot::acquire(root, &state_identity))
        .transpose()?;
    let mut restored_state = false;
    if let Some(state_root) = state_root.as_mut() {
        match state_root.startup() {
            StateRootStartup::Restore => {
                let generation = state_root.restore(&mut engine)?;
                restored_state = true;
                evidence.append_log(
                    false,
                    &format!(
                        "unsigned_shadow_state=restored identity_verified=true generation={generation}"
                    ),
                )?;
            }
            StateRootStartup::Initialize => {
                let generation = state_root.initialize(&mut engine)?;
                evidence.append_log(
                    false,
                    &format!(
                        "unsigned_shadow_state=initialized identity_verified=true generation={generation}"
                    ),
                )?;
            }
        }
    }
    if CONTINUOUS {
        engine.rebase_runtime_time(start, wall_ms)?;
    }
    let direct_live = if execution_mode == ExecutionMode::Live {
        let settings = live_settings.ok_or("live execution settings are unavailable")?;
        let live_root = options
            .state_root
            .as_ref()
            .ok_or("live execution requires --state-root")?
            .join("live");
        let (mut runtime, startup_update) = LiveExecutionRuntime::initialize(
            settings,
            &live_root,
            derive_risk_policy_hash(&config.global_risk)?,
            derive_config_hash(&config)?,
            wall_ms,
        )
        .await?;
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [0; 32],
            signer_release_hash: [0; 32],
            release_manifest_hash: [0; 32],
            market_rules_hash: [0; 32],
            dynamic_floor_policy_hash: [0; 32],
            ioc_policy_hash: [0; 32],
            expires_after_ms: 20_000,
        });
        engine.enter_live_recovery_only();
        if let Some(update) = startup_update {
            match apply_authenticated_reconciliation(&mut engine, &mut runtime, update, wall_ms)? {
                AuthenticatedReconciliation::Applied(_) => {
                    persist_unsigned_state(&mut engine, &mut state_root)?;
                }
                AuthenticatedReconciliation::RecoveryPending => {}
            }
        } else {
            engine.enter_live_recovery_only();
            runtime.defer_recovery(wall_ms);
        }
        Some(std::sync::Arc::new(tokio::sync::Mutex::new(runtime)))
    } else {
        None
    };
    let mut streaming = streaming_enabled
        .then(|| {
            StreamingHandle::start(
                transport_policy.streaming.clone(),
                config
                    .candidates
                    .iter()
                    .map(|candidate| candidate.address.to_ascii_lowercase()),
            )
        })
        .transpose()
        .map_err(|error| format!("streaming transport: {error:?}"))?;
    let mut streaming_sources = streaming_enabled
        .then(|| {
            StreamingSourceBook::new(
                config
                    .candidates
                    .iter()
                    .map(|candidate| candidate.address.to_ascii_lowercase()),
            )
        })
        .transpose()
        .map_err(|error| format!("streaming source book: {error:?}"))?;
    let mut market_directory: Option<MarketDirectory> = None;
    let mut stream_live_taker_fee_bps = None;
    let mut stream_connected = false;
    let mut reconciliation_epoch_active = streaming_enabled;
    let mut reconciliation_started_at = start.saturating_sub(1);
    let mut reconciliation_remaining = if streaming_enabled {
        config
            .candidates
            .iter()
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let mut history_recovery =
        (streaming_enabled && restored_state && engine.history_complete_through_ms().is_some())
            .then(HistoryRecovery::default);
    let mut bootstrap_history_cutoff = None::<u64>;
    if history_recovery.is_some() {
        streaming_sources
            .as_mut()
            .ok_or("streaming source book unavailable")?
            .begin_recovery();
    }
    let mut next_reconciliation_due = start
        .checked_add(transport_policy.streaming.reconciliation_interval_ms)
        .ok_or("streaming reconciliation deadline overflow")?;
    let mut hot_book_last_required = BTreeMap::<String, Timestamp>::new();
    let mut subscribed_hot_books = BTreeSet::<String>::new();
    let mut subscribed_trade_markets = BTreeSet::<String>::new();
    let mut coverage_ready_markets = BTreeSet::<String>::new();
    let mut coverage_pending_markets = BTreeSet::<String>::new();
    let mut coverage_reconciliation_remaining = BTreeSet::<String>::new();
    let mut coverage_gap_started_at = None::<Timestamp>;
    let mut coverage_reconciliations = 0_u64;
    let mut trade_market_rotation_cursor = 0_usize;
    let mut trade_market_rotation_due = start;
    let mut candidate_tiers = config
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            (
                candidate.address.to_ascii_lowercase(),
                if index < source_dispatch_plan.active_candidate_count {
                    SourceTier::Active
                } else {
                    SourceTier::Inactive
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut source_due = phase_staggered_source_due(&config, start, source_dispatch_plan)?;
    for (candidate, tier) in &candidate_tiers {
        engine.set_source_tier(candidate, *tier);
        evidence.append_replay_event(
            start,
            ReplayPayload::TierSet {
                candidate_id: candidate.clone(),
                tier: *tier,
            },
        )?;
    }
    // Rebalance only on a fixed cadence. Validated last-known position state is
    // a polling hint, never a substitute for fresh consensus input.
    let mut tier_rebalance_due = start + inactive_freshness;
    let mut book_enqueue_due = 0;
    let mut book_cursor = 0_usize;
    let mut scheduled = BTreeSet::new();
    let mut market_due = 0;
    let mut metadata_due = 0;
    let mut decision_due = 0;
    const EQUITY_BUCKET_INTERVAL_MS: u64 = 300_000;
    let first_bucket_delay = if CONTINUOUS {
        let elapsed_in_wall_bucket = wall_ms % EQUITY_BUCKET_INTERVAL_MS;
        EQUITY_BUCKET_INTERVAL_MS
            .checked_sub(elapsed_in_wall_bucket)
            .ok_or("bucket overflow")?
    } else {
        EQUITY_BUCKET_INTERVAL_MS
    };
    let mut profitability_bucket_due = start
        .checked_add(first_bucket_delay)
        .ok_or("bucket overflow")?;
    let mut checkpoint_due = 0;
    let mut counters = Counters::default();
    let mut tasks = JoinSet::new();
    let deadline = (!CONTINUOUS)
        .then(|| {
            start
                .checked_add(
                    options
                        .duration_seconds
                        .checked_mul(1000)
                        .ok_or("duration overflow")?,
                )
                .ok_or("deadline overflow")
        })
        .transpose()?;
    let mut interrupted = false;
    if options.profitability_gate || CONTINUOUS {
        let restored_inside_current_wall_bucket = CONTINUOUS
            && restored_state
            && engine.last_equity_boundary().is_some_and(|last| {
                last / EQUITY_BUCKET_INTERVAL_MS == wall_ms / EQUITY_BUCKET_INTERVAL_MS
            });
        if !restored_inside_current_wall_bucket {
            engine.record_equity_boundary(start)?;
            evidence.append_replay_event(start, ReplayPayload::EquityBoundary)?;
            persist_unsigned_state(&mut engine, &mut state_root)?;
        }
    }
    while deadline.is_none_or(|deadline| clock.now_ms() < deadline) {
        let now = clock.now_ms();
        if !streaming_enabled && now >= tier_rebalance_due {
            let ordered_candidates = config
                .candidates
                .iter()
                .map(|candidate| candidate.address.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let candidates_with_positions = ordered_candidates
                .iter()
                .filter(|candidate| engine.latest_source_has_position(candidate))
                .cloned()
                .collect::<BTreeSet<_>>();
            let next_tiers = assign_source_tiers(
                &ordered_candidates,
                &candidates_with_positions,
                source_dispatch_plan.active_candidate_count,
            );
            let promoted = next_tiers
                .iter()
                .filter(|(candidate, tier)| {
                    **tier == SourceTier::Active
                        && candidate_tiers.get(*candidate) != Some(&SourceTier::Active)
                })
                .map(|(candidate, _)| candidate.clone())
                .collect::<Vec<_>>();
            for (ordinal, candidate) in promoted.iter().enumerate() {
                source_due.insert(
                    candidate.clone(),
                    phase_deadline(
                        now,
                        source_dispatch_plan.active_cadence_ms,
                        ordinal,
                        promoted.len(),
                    )?,
                );
            }
            for (candidate, tier) in &next_tiers {
                engine.set_source_tier(candidate, *tier);
                if candidate_tiers.get(candidate) != Some(tier) {
                    evidence.append_replay_event(
                        now,
                        ReplayPayload::TierSet {
                            candidate_id: candidate.clone(),
                            tier: *tier,
                        },
                    )?;
                }
            }
            candidate_tiers = next_tiers;
            tier_rebalance_due = next_cadence_deadline(tier_rebalance_due, now, 60_000)?;
        }
        if streaming_enabled
            && now >= next_reconciliation_due
            && !reconciliation_epoch_active
            && coverage_reconciliation_remaining.is_empty()
        {
            source_due = phase_staggered_source_due(&config, now, source_dispatch_plan)?;
            next_reconciliation_due = next_cadence_deadline(
                next_reconciliation_due,
                now,
                transport_policy.streaming.reconciliation_interval_ms,
            )?;
        }
        let scheduler_health = scheduler.health();
        rearm_idle_stream_reconciliation(
            &config,
            source_dispatch_plan,
            now,
            stream_connected
                && tasks.is_empty()
                && scheduler_health.pending == 0
                && scheduler_health.successors == 0
                && scheduler_health.in_flight == 0,
            &reconciliation_remaining,
            &coverage_reconciliation_remaining,
            &mut source_due,
        )?;
        for candidate in &config.candidates {
            let id = candidate.address.to_ascii_lowercase();
            if source_due[&id] <= now
                && (!streaming_enabled || market_directory.is_some())
                && (history_recovery.is_none() || stream_connected)
            {
                let tier = candidate_tiers[&id];
                let interval = match tier {
                    SourceTier::Active => source_dispatch_plan.active_cadence_ms,
                    SourceTier::Inactive => source_dispatch_plan.inactive_cadence_ms,
                };
                let recovery_range = history_recovery
                    .as_ref()
                    .and_then(|recovery| recovery.next_range(&id));
                let (source_subject, source_kind) =
                    if let Some((start_time_ms, end_time_ms)) = recovery_range {
                        (
                            RequestSubject::CandidateFillRange {
                                candidate: id.clone(),
                                start_time_ms,
                                end_time_ms,
                            },
                            ReadRequestKind::SourceFills,
                        )
                    } else {
                        (
                            RequestSubject::Candidate(id.clone()),
                            if streaming_enabled || expanded_source_candidates.contains(&id) {
                                ReadRequestKind::ExpandedSourceState
                            } else {
                                ReadRequestKind::SourceState
                            },
                        )
                    };
                enqueue(
                    &scheduler,
                    &scheduler_config,
                    source_subject,
                    source_kind,
                    // Source cadence is the qualification invariant. Public order-book
                    // enrichment may use only the normal-budget capacity left after it.
                    if reconciliation_epoch_active || !coverage_reconciliation_remaining.is_empty()
                    {
                        RequestPriority::Critical
                    } else {
                        RequestPriority::High
                    },
                    Some(tier),
                    now,
                    if streaming_enabled {
                        transport_policy.streaming.reconciliation_spread_ms
                    } else {
                        config.global_risk.source_snapshot_max_age_ms
                    },
                    &mut counters,
                )?;
                scheduled.insert(id.clone());
                if streaming_enabled {
                    source_due.insert(id, u64::MAX);
                } else {
                    let previous_due = source_due[&id];
                    source_due.insert(id, next_cadence_deadline(previous_due, now, interval)?);
                }
            }
        }
        if !streaming_enabled && market_due <= now {
            enqueue(
                &scheduler,
                &scheduler_config,
                RequestSubject::Market,
                ReadRequestKind::MarketMids,
                RequestPriority::High,
                None,
                now,
                40_000,
                &mut counters,
            )?;
            market_due = now + 20_000;
        }
        if metadata_due <= now {
            enqueue(
                &scheduler,
                &scheduler_config,
                RequestSubject::Market,
                ReadRequestKind::ExchangeMetadata,
                RequestPriority::High,
                None,
                now,
                120_000,
                &mut counters,
            )?;
            // Funding context is admission-critical and the accepted response
            // is valid for 120 seconds. Refresh at half-life so a normal
            // delayed poll cannot leave MFCE pending for most of the hour.
            metadata_due = if streaming_enabled {
                now + 6 * 60 * 60_000
            } else {
                now + 60_000
            };
        }
        if !streaming_enabled && now >= book_enqueue_due {
            let urgent_assets = engine.urgent_book_assets().into_iter().collect::<Vec<_>>();
            let assets = if urgent_assets.is_empty() {
                engine
                    .assets_requiring_books(now)
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                urgent_assets
            };
            if !assets.is_empty() {
                let asset = &assets[book_cursor % assets.len()];
                book_cursor = book_cursor.wrapping_add(1);
                enqueue(
                    &scheduler,
                    &scheduler_config,
                    RequestSubject::Asset(asset.clone()),
                    ReadRequestKind::OrderBook,
                    if engine.urgent_book_assets().contains(asset) {
                        RequestPriority::High
                    } else {
                        RequestPriority::Normal
                    },
                    None,
                    now,
                    40_000,
                    &mut counters,
                )?;
            }
            // 30 books/minute = 60 weight. Together with allMids (6)
            // and the combined metadata + live-fee refresh (22), enrichment
            // stays within the 90-weight non-source allowance.
            book_enqueue_due = next_cadence_deadline(book_enqueue_due, now, 2_000)?;
        }
        if let Some(stream) = streaming.as_ref() {
            let urgent = engine.urgent_book_assets();
            let required = engine.assets_requiring_books(now);
            for asset in required.iter().chain(&urgent) {
                if !valid_market(asset) {
                    continue;
                }
                hot_book_last_required.insert(asset.clone(), now);
            }
            hot_book_last_required.retain(|_, last_required| {
                now.saturating_sub(*last_required) <= transport_policy.streaming.hot_book_grace_ms
            });
            let next_hot_books = select_hot_books(
                &urgent,
                &required,
                &hot_book_last_required,
                &subscribed_hot_books,
            );
            if next_hot_books != subscribed_hot_books {
                stream
                    .replace_hot_books(next_hot_books.clone())
                    .await
                    .map_err(|error| format!("hot book subscriptions: {error:?}"))?;
                subscribed_hot_books = next_hot_books;
            }
            if now >= trade_market_rotation_due
                && !reconciliation_epoch_active
                && coverage_reconciliation_remaining.is_empty()
            {
                if let Some(directory) = market_directory.as_ref() {
                    let mut priority = streaming_sources
                        .as_ref()
                        .map_or_else(BTreeSet::new, StreamingSourceBook::active_assets);
                    priority.extend(subscribed_hot_books.iter().cloned());
                    let (next_markets, next_cursor) = directory
                        .rotated_markets(&priority, trade_market_rotation_cursor)
                        .map_err(|error| format!("trade subscription rotation: {error:?}"))?;
                    if next_markets != subscribed_trade_markets {
                        coverage_ready_markets = coverage_ready_markets
                            .intersection(&next_markets)
                            .cloned()
                            .collect();
                        coverage_pending_markets = next_markets
                            .difference(&coverage_ready_markets)
                            .cloned()
                            .collect();
                        engine
                            .replace_source_market_coverage(coverage_ready_markets.clone(), now)
                            .map_err(|error| error.to_string())?;
                        stream
                            .replace_markets(next_markets.clone())
                            .await
                            .map_err(|error| format!("trade subscriptions: {error:?}"))?;
                        subscribed_trade_markets = next_markets;
                        if !coverage_pending_markets.is_empty() {
                            coverage_gap_started_at = Some(now);
                            coverage_reconciliation_remaining = config
                                .candidates
                                .iter()
                                .map(|candidate| candidate.address.to_ascii_lowercase())
                                .collect();
                            source_due = phase_staggered_source_due(
                                &config,
                                now.checked_add(1)
                                    .ok_or("coverage reconciliation start overflow")?,
                                source_dispatch_plan,
                            )?;
                            coverage_reconciliations = coverage_reconciliations.saturating_add(1);
                        }
                    }
                    trade_market_rotation_cursor = next_cursor;
                }
                trade_market_rotation_due = now
                    .checked_add(60_000)
                    .ok_or("trade subscription rotation overflow")?;
            }
        }
        while tasks.len() < scheduler_config.api.max_concurrency {
            let Some(dispatch) = scheduler.begin_next() else {
                break;
            };
            let request = dispatch.request().clone();
            if let Some(tier) = request.source_tier {
                record_latency(
                    counters.queue_wait_ms_by_tier.entry(tier).or_default(),
                    now.saturating_sub(request.created_at),
                );
            }
            counters.total_weight += u64::from(request.weight);
            if request.budget_class == BudgetClass::Critical {
                counters.reserved_weight += u64::from(request.weight)
            }
            let source = transport.clone();
            let started_at = now;
            let kind = request.kind;
            tasks.spawn(async move {
                let result = source.execute(request).await;
                (dispatch, result, started_at, kind)
            });
        }
        let health = scheduler.health();
        counters.maximum_pending = counters.maximum_pending.max(health.pending);
        counters.maximum_successors = counters.maximum_successors.max(health.successors);
        counters.maximum_in_flight = counters.maximum_in_flight.max(health.in_flight);
        tokio::select! {
            result = tasks.join_next(), if !tasks.is_empty() => {
                let (dispatch, result, started_at, kind) = result.ok_or("task set ended")??;
                let completed_request = dispatch.request().clone();
                record_latency(
                    counters.transport_latency_ms_by_kind.entry(kind).or_default(),
                    clock.now_ms().saturating_sub(started_at),
                );
                if let Ok(response) = &result {
                    counters.total_weight = counters.total_weight.saturating_sub(
                        u64::from(completed_request.weight.saturating_sub(response.actual_weight)),
                    );
                }
                let outcome = scheduler.finish(dispatch, result);
                append_freshness_decision(
                    &mut evidence,
                    &completed_request,
                    outcome,
                    clock.now_ms(),
                )?;
                match outcome {
                    ExecutionOutcome::CompletedFresh => counters.completed_fresh += 1,
                    ExecutionOutcome::CompletedButStale => {
                        counters.completed_stale += 1;
                        counters.response_rejected_as_stale += 1;
                    }
                    ExecutionOutcome::Superseded => counters.completed_stale += 1,
                    ExecutionOutcome::RetryScheduled => counters.retries += 1,
                    ExecutionOutcome::RetryExhausted => counters.retry_exhausted += 1,
                    ExecutionOutcome::InvalidResponse => counters.invalid_responses += 1,
                    ExecutionOutcome::PermanentFailure => counters.permanent_failures += 1,
                }
                if outcome == ExecutionOutcome::CompletedFresh {
                    if let Some(mut response) = transport.take_accepted(&completed_request) {
                        let mut completes_stream_reconciliation = false;
                        let mut completes_coverage_reconciliation = false;
                        let mut stream_history_cutoff = None;
                        let mut ingest_response = true;
                        let mut recovered_states = None;
                        if streaming_enabled {
                            match &mut response.payload {
                                crate::public_mainnet::PublicPayload::SourceState(state) => {
                                    bootstrap_history_cutoff = Some(
                                        bootstrap_history_cutoff.map_or(
                                            state.source_time_ms,
                                            |cutoff| cutoff.min(state.source_time_ms),
                                        ),
                                    );
                                    if reconciliation_epoch_active && history_recovery.is_some() {
                                        ingest_response = false;
                                        let history_start = engine
                                            .history_complete_through_ms()
                                            .ok_or("history recovery has no durable watermark")?;
                                        if response.requested_at_mono <= reconciliation_started_at
                                            || !history_recovery
                                                .as_mut()
                                                .ok_or("history recovery unavailable")?
                                                .record_snapshot(history_start, state.clone())
                                        {
                                            source_due.insert(
                                                state.candidate_id.clone(),
                                                clock.now_ms().checked_add(
                                                    scheduler_config.retry.maximum_backoff_ms,
                                                ).ok_or("source history retry overflow")?,
                                            );
                                        } else {
                                            source_due.insert(
                                                state.candidate_id.clone(),
                                                clock.now_ms().checked_add(1)
                                                    .ok_or("source-fill recovery due overflow")?,
                                            );
                                        }
                                    } else {
                                        let installed = streaming_sources
                                            .as_mut()
                                            .ok_or("streaming source book unavailable")?
                                            .install_baseline(state.clone())
                                            .map_err(|error| format!("source baseline: {error:?}"))?;
                                        *state = installed;
                                        if response.requested_at_mono > reconciliation_started_at {
                                            reconciliation_remaining.remove(&state.candidate_id);
                                        }
                                    }
                                    if coverage_gap_started_at.is_some_and(|gap_started_at| {
                                        response.requested_at_mono > gap_started_at
                                    }) {
                                        coverage_reconciliation_remaining
                                            .remove(&state.candidate_id);
                                    }
                                    if reconciliation_epoch_active
                                        && stream_connected
                                        && reconciliation_remaining.is_empty()
                                    {
                                        reconciliation_epoch_active = false;
                                        completes_stream_reconciliation = true;
                                        stream_history_cutoff = bootstrap_history_cutoff;
                                    }
                                    if stream_connected
                                        && !coverage_pending_markets.is_empty()
                                        && coverage_reconciliation_remaining.is_empty()
                                    {
                                        completes_coverage_reconciliation = true;
                                    }
                                }
                                crate::public_mainnet::PublicPayload::SourceFills(range) => {
                                    ingest_response = false;
                                    let wallet = range.wallet.clone();
                                    let progress = history_recovery
                                        .as_mut()
                                        .map_or(FillRangeProgress::Stale, |recovery| {
                                            recovery.record_range(range)
                                        });
                                    match progress {
                                        FillRangeProgress::Next => {
                                            source_due.insert(
                                                wallet,
                                                clock.now_ms().checked_add(1)
                                                    .ok_or("next fill-range due overflow")?,
                                            );
                                        }
                                        FillRangeProgress::WalletComplete => {
                                            let history_start = engine
                                                .history_complete_through_ms()
                                                .ok_or("history recovery has no durable watermark")?;
                                            let (ending, fills) = history_recovery
                                                .as_ref()
                                                .and_then(|recovery| {
                                                    recovery.completed_wallet(&wallet)
                                                })
                                                .ok_or("completed wallet recovery is unavailable")?;
                                            match streaming_sources
                                                .as_mut()
                                                .ok_or("streaming source book unavailable")?
                                                .recover_wallet_and_install(
                                                    history_start,
                                                    ending,
                                                    fills,
                                                )
                                            {
                                                Ok(state) => {
                                                    recovered_states = Some(vec![state]);
                                                    reconciliation_remaining.remove(&wallet);
                                                }
                                                Err(_) => restart_history_recovery(
                                                    &mut history_recovery,
                                                    &config,
                                                    source_dispatch_plan,
                                                    clock.now_ms().checked_add(
                                                        scheduler_config
                                                            .retry
                                                            .maximum_backoff_ms,
                                                    ).ok_or(
                                                        "history reconciliation retry overflow",
                                                    )?,
                                                    &mut reconciliation_remaining,
                                                    &mut source_due,
                                                )?,
                                            }
                                        }
                                        FillRangeProgress::Restart => restart_history_recovery(
                                            &mut history_recovery,
                                            &config,
                                            source_dispatch_plan,
                                            clock.now_ms().checked_add(
                                                scheduler_config.retry.maximum_backoff_ms,
                                            ).ok_or("history recovery retry overflow")?,
                                            &mut reconciliation_remaining,
                                            &mut source_due,
                                        )?,
                                        FillRangeProgress::Stale => {
                                            if history_recovery.is_some()
                                                && reconciliation_remaining.contains(&wallet)
                                            {
                                                source_due.insert(
                                                    wallet,
                                                    clock.now_ms().checked_add(1).ok_or(
                                                        "stale fill-range recovery due overflow",
                                                    )?,
                                                );
                                            }
                                        }
                                    }
                                    if !matches!(
                                        progress,
                                        FillRangeProgress::Restart | FillRangeProgress::Stale
                                    )
                                        && stream_connected
                                        && reconciliation_remaining.is_empty()
                                    {
                                        let cutoff = history_recovery
                                            .as_ref()
                                            .ok_or("history recovery unavailable")?
                                            .cutoff()
                                            .ok_or("history recovery has no ending snapshots")?;
                                        if streaming_sources
                                            .as_mut()
                                            .ok_or("streaming source book unavailable")?
                                            .recovery_complete()
                                        {
                                            history_recovery = None;
                                            reconciliation_epoch_active = false;
                                            completes_stream_reconciliation = true;
                                            stream_history_cutoff = Some(cutoff);
                                        }
                                    }
                                }
                                crate::public_mainnet::PublicPayload::MarketMetadata(metadata) => {
                                    let directory = MarketDirectory::from_metadata(metadata)
                                        .map_err(|error| format!("stream market directory: {error:?}"))?;
                                    let mut priority = streaming_sources
                                        .as_ref()
                                        .map_or_else(BTreeSet::new, StreamingSourceBook::active_assets);
                                    priority.extend(subscribed_hot_books.iter().cloned());
                                    let (markets, next_cursor) = directory
                                        .rotated_markets(&priority, trade_market_rotation_cursor)
                                        .map_err(|error| format!("trade subscription selection: {error:?}"))?;
                                    if subscribed_trade_markets.is_empty() {
                                        streaming
                                            .as_ref()
                                            .ok_or("streaming handle unavailable")?
                                            .replace_markets(markets.clone())
                                            .await
                                            .map_err(|error| {
                                                format!("trade subscriptions: {error:?}")
                                            })?;
                                        subscribed_trade_markets = markets;
                                        coverage_ready_markets.clear();
                                        coverage_pending_markets.clear();
                                        coverage_reconciliation_remaining.clear();
                                        coverage_gap_started_at = None;
                                        trade_market_rotation_cursor = next_cursor;
                                        trade_market_rotation_due = clock
                                            .now_ms()
                                            .checked_add(60_000)
                                            .ok_or("trade subscription rotation overflow")?;
                                    }
                                    stream_live_taker_fee_bps = metadata.live_taker_fee_bps;
                                    market_directory = Some(directory);
                                }
                                _ => {}
                            }
                        }
                        if let Some(tier) = response.source_tier {
                            *counters.accepted_count_by_tier.entry(tier).or_default() += 1;
                        }
                        let accepted_at = clock.now_ms();
                        if !CONTINUOUS {
                            evidence.append_replay_event(
                                accepted_at,
                                ReplayPayload::AcceptedResponse {
                                    response: response.clone(),
                                },
                            )?;
                        }
                        let executions_before = engine.metrics().shadow_executions;
                        if ingest_response {
                            engine.ingest(response, accepted_at).map_err(|e| e.to_string())?;
                        }
                        if let Some(states) = recovered_states {
                            for state in states {
                                let subject = state.candidate_id.clone();
                                let source_time_ms = state.source_time_ms;
                                let tier = candidate_tiers
                                    .get(&subject)
                                    .copied()
                                    .unwrap_or(SourceTier::Inactive);
                                engine
                                    .ingest(
                                        accepted_stream_response(
                                            crate::public_mainnet::PublicPayload::SourceState(state),
                                            subject.clone(),
                                            ReadRequestKind::ExpandedSourceState,
                                            Some(tier),
                                            accepted_at,
                                            config.global_risk.source_snapshot_max_age_ms,
                                        )?,
                                        accepted_at,
                                    )
                                    .map_err(|error| error.to_string())?;
                                engine
                                    .admit_reconciled_source_wallet(
                                        &subject,
                                        source_time_ms,
                                        accepted_at,
                                    )
                                    .map_err(|error| error.to_string())?;
                            }
                        }
                        if completes_stream_reconciliation {
                            engine
                                .complete_source_stream_reconciliation(
                                    subscribed_trade_markets.clone(),
                                    stream_history_cutoff
                                        .ok_or("stream reconciliation has no history cutoff")?,
                                    accepted_at,
                                )
                                .map_err(|e| e.to_string())?;
                            coverage_ready_markets = subscribed_trade_markets.clone();
                        }
                        if completes_coverage_reconciliation {
                            coverage_ready_markets
                                .append(&mut coverage_pending_markets);
                            coverage_gap_started_at = None;
                            engine
                                .complete_source_market_reconciliation(
                                    coverage_ready_markets.clone(),
                                    accepted_at,
                                )
                                .map_err(|e| e.to_string())?;
                            next_reconciliation_due = accepted_at
                                .checked_add(
                                    transport_policy.streaming.reconciliation_interval_ms,
                                )
                                .ok_or("streaming reconciliation deadline overflow")?;
                            trade_market_rotation_due = accepted_at
                                .checked_add(60_000)
                                .ok_or("trade subscription rotation overflow")?;
                        }
                        dispatch_production_intents(
                            &mut engine,
                            direct_live.as_ref(),
                        )
                        .await?;
                        append_new_cohort_records(
                            &mut evidence,
                            &engine,
                            &mut cohort_record_cursor,
                            accepted_at,
                        )?;
                        append_new_technical_records(
                            &mut evidence,
                            &engine,
                            &mut technical_record_cursor,
                        )?;
                        if !CONTINUOUS
                            || engine.metrics().shadow_executions != executions_before
                        {
                            persist_unsigned_state(&mut engine, &mut state_root)?;
                        }
                    }
                } else {
                    let _ = transport.take_accepted(&completed_request);
                }
                rearm_unresolved_source_request(
                    &completed_request,
                    outcome,
                    clock.now_ms(),
                    scheduler_config.retry.maximum_backoff_ms,
                    &reconciliation_remaining,
                    &coverage_reconciliation_remaining,
                    &mut source_due,
                )?;
            }
            stream_event = async {
                match streaming.as_mut() {
                    Some(stream) => stream.recv().await,
                    None => std::future::pending().await,
                }
            }, if streaming_enabled => {
                let event = stream_event.ok_or("public streaming task stopped")?;
                let observed_at = clock.now_ms();
                if engine.source_stream_healthy()
                    && matches!(
                        &event,
                        StreamingEvent::Trades(_)
                            | StreamingEvent::AssetContexts(_)
                            | StreamingEvent::OrderBook(_)
                    )
                {
                    let wall_now = u64::try_from(
                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
                    )?;
                    engine.note_continuous_source_history(
                        wall_now.saturating_sub(transport_policy.streaming.idle_timeout_ms),
                    );
                }
                match event {
                    StreamingEvent::Connected { .. } => {
                        stream_connected = true;
                        // A reconnected public market stream is current even
                        // while tracked-wallet history is still repairing.
                        // Wallet readiness, not global history completion,
                        // controls which source contributions are actionable.
                        coverage_ready_markets = subscribed_trade_markets.clone();
                        engine
                            .replace_source_market_coverage(
                                coverage_ready_markets.clone(),
                                observed_at,
                            )
                            .map_err(|error| error.to_string())?;
                        if reconciliation_epoch_active
                            && reconciliation_remaining.iter().all(|wallet| {
                                source_due.get(wallet).is_some_and(|due| *due == u64::MAX)
                            })
                        {
                            source_due = phase_staggered_source_due(
                                &config,
                                observed_at.checked_add(1)
                                    .ok_or("stream recovery start overflow")?,
                                source_dispatch_plan,
                            )?;
                        }
                        if reconciliation_epoch_active
                            && history_recovery.is_none()
                            && reconciliation_remaining.is_empty()
                            && streaming_sources
                                .as_ref()
                                .is_some_and(StreamingSourceBook::all_hydrated)
                        {
                            reconciliation_epoch_active = false;
                            engine
                                .complete_source_stream_reconciliation(
                                    subscribed_trade_markets.clone(),
                                    bootstrap_history_cutoff
                                        .ok_or("stream bootstrap has no history cutoff")?,
                                    observed_at,
                                )
                                .map_err(|e| e.to_string())?;
                            coverage_ready_markets = subscribed_trade_markets.clone();
                            dispatch_production_intents(
                                &mut engine,
                                direct_live.as_ref(),
                            )
                            .await?;
                        }
                    }
                    StreamingEvent::Gap {
                        affected_markets,
                        ..
                    } => {
                        stream_connected = false;
                        reconciliation_epoch_active = true;
                        reconciliation_started_at = observed_at;
                        history_recovery = engine
                            .history_complete_through_ms()
                            .map(|_| HistoryRecovery::default());
                        if history_recovery.is_some() {
                            streaming_sources
                                .as_mut()
                                .ok_or("streaming source book unavailable")?
                                .begin_recovery();
                        }
                        let affected_markets = if affected_markets.is_empty() {
                            subscribed_trade_markets.clone()
                        } else {
                            affected_markets
                        };
                        coverage_ready_markets
                            .retain(|market| !affected_markets.contains(market));
                        engine
                            .mark_source_stream_gap(&affected_markets, observed_at)
                            .map_err(|error| error.to_string())?;
                        coverage_pending_markets.clear();
                        coverage_reconciliation_remaining.clear();
                        coverage_gap_started_at = None;
                        reconciliation_remaining = config
                            .candidates
                            .iter()
                            .map(|candidate| candidate.address.to_ascii_lowercase())
                            .collect();
                        source_due.values_mut().for_each(|due| *due = u64::MAX);
                    }
                    StreamingEvent::Trades(trades) => {
                        let executions_before = engine.metrics().shadow_executions;
                        for trade in trades {
                            let states = streaming_sources
                                .as_mut()
                                .ok_or("streaming source book unavailable")?
                                .apply_trade(trade)
                                .map_err(|error| format!("stream trade: {error:?}"))?;
                            for state in states {
                                let subject = state.candidate_id.clone();
                                let tier = candidate_tiers
                                    .get(&subject)
                                    .copied()
                                    .unwrap_or(SourceTier::Inactive);
                                let response = accepted_stream_response(
                                    crate::public_mainnet::PublicPayload::SourceState(state),
                                    subject,
                                    ReadRequestKind::ExpandedSourceState,
                                    Some(tier),
                                    observed_at,
                                    config.global_risk.source_snapshot_max_age_ms,
                                )?;
                                engine.ingest(response, observed_at).map_err(|e| e.to_string())?;
                            }
                        }
                        dispatch_production_intents(
                            &mut engine,
                            direct_live.as_ref(),
                        )
                        .await?;
                        if engine.metrics().shadow_executions != executions_before {
                            persist_unsigned_state(&mut engine, &mut state_root)?;
                        }
                    }
                    StreamingEvent::AssetContexts(data) => {
                        if let Some(directory) = market_directory.as_ref() {
                            let Ok((mids, metadata)) = parse_asset_contexts(
                                &data,
                                directory,
                                stream_live_taker_fee_bps,
                            ) else {
                                // Replaceable market context is allowed to go
                                // explicitly missing. Its existing validity
                                // expires naturally; malformed context cannot
                                // terminate an otherwise continuous source
                                // and reconciliation daemon.
                                if let Some(stream) = streaming.as_ref() {
                                    stream.note_invalid_payload();
                                }
                                continue;
                            };
                            engine.ingest(
                                accepted_stream_response(
                                    crate::public_mainnet::PublicPayload::MarketSnapshot(mids),
                                    "market".into(),
                                    ReadRequestKind::MarketMids,
                                    None,
                                    observed_at,
                                    transport_policy.market_validity_ms,
                                )?,
                                observed_at,
                            )?;
                            engine.ingest(
                                accepted_stream_response(
                                    crate::public_mainnet::PublicPayload::MarketMetadata(metadata),
                                    "market".into(),
                                    ReadRequestKind::ExchangeMetadata,
                                    None,
                                    observed_at,
                                    transport_policy.market_validity_ms,
                                )?,
                                observed_at,
                            )?;
                        }
                    }
                    StreamingEvent::OrderBook(book) => {
                        let asset = book.asset.clone();
                        let executions_before = engine.metrics().shadow_executions;
                        engine.ingest(
                            accepted_stream_response(
                                crate::public_mainnet::PublicPayload::OrderBook(book),
                                asset,
                                ReadRequestKind::OrderBook,
                                None,
                                observed_at,
                                transport_policy.market_validity_ms,
                            )?,
                            observed_at,
                        )?;
                        dispatch_production_intents(
                            &mut engine,
                            direct_live.as_ref(),
                        )
                        .await?;
                        if engine.metrics().shadow_executions != executions_before {
                            persist_unsigned_state(&mut engine, &mut state_root)?;
                        }
                    }
                }
                append_new_cohort_records(
                    &mut evidence,
                    &engine,
                    &mut cohort_record_cursor,
                    observed_at,
                )?;
                append_new_technical_records(
                    &mut evidence,
                    &engine,
                    &mut technical_record_cursor,
                )?;
            }
            _ = tokio::signal::ctrl_c() => {
                interrupted = true;
                break;
            }
            _ = sleep(Duration::from_millis(25)) => {}
        }
        if engine.service_mfce_training() {
            persist_unsigned_state(&mut engine, &mut state_root)?;
        }
        let now = clock.now_ms();
        if now >= decision_due
            && deadline.is_none_or(|deadline| now.saturating_add(40_000) < deadline)
        {
            evidence.append_replay_event(now, ReplayPayload::DecisionTick)?;
            let _ = engine
                .construct_next_decision(now)
                .map_err(|e| e.to_string())?;
            dispatch_production_intents(&mut engine, direct_live.as_ref()).await?;
            append_new_cohort_records(&mut evidence, &engine, &mut cohort_record_cursor, now)?;
            append_new_technical_records(&mut evidence, &engine, &mut technical_record_cursor)?;
            persist_unsigned_state(&mut engine, &mut state_root)?;
            decision_due = now + 20_000;
        }
        if (options.profitability_gate || options.micro_density_gate || CONTINUOUS)
            && now >= profitability_bucket_due
        {
            engine.record_equity_boundary(profitability_bucket_due)?;
            evidence
                .append_replay_event(profitability_bucket_due, ReplayPayload::EquityBoundary)?;
            persist_unsigned_state(&mut engine, &mut state_root)?;
            profitability_bucket_due = profitability_bucket_due
                .checked_add(EQUITY_BUCKET_INTERVAL_MS)
                .ok_or("bucket deadline overflow")?;
        }
        if now >= checkpoint_due {
            let h = scheduler.health();
            let t = transport.metrics();
            let m = engine.metrics();
            if !CONTINUOUS {
                let (fresh_by_tier, expired_by_age, never_refreshed) =
                    coverage_evidence(&config, &candidate_tiers, &engine, now);
                evidence.append_checkpoint(CheckpointPayload {
                    sequence: 0,
                    monotonic_elapsed_ms: now - start,
                    accepted_source_snapshots: m.accepted_source_snapshots,
                    stale_source_snapshots: m.stale_source_snapshots,
                    rejected_source_snapshots: m.rejected_source_snapshots,
                    active_source_count: engine.active_source_count(now),
                    total_weight_consumed: counters.total_weight,
                    reserved_weight_consumed: counters.reserved_weight,
                    pending_queue: h.pending,
                    successor_queue: h.successors,
                    in_flight: h.in_flight,
                    rate_limited_responses: t.rate_limited.load(Ordering::SeqCst),
                    retry_count: counters.retries,
                    decision_count: m.decisions,
                    projection_violations: m.projection_violations,
                    shadow_execution_count: m.shadow_executions,
                    open_episode_count: engine.ledger().portfolio_open_count() as u64,
                    closed_episode_count: engine.ledger().portfolio_closed_count(),
                    persistence_healthy: m.persistence_failures == 0,
                    response_rejected_as_stale: counters.response_rejected_as_stale,
                    accepted_snapshot_expired_by_age: expired_by_age,
                    candidate_never_refreshed: never_refreshed,
                    request_expired_in_queue: h.expired_in_queue_by_tier.clone(),
                    request_superseded_pending: h.superseded_pending_by_tier.clone(),
                    request_superseded_in_flight: h.superseded_in_flight_by_tier.clone(),
                    dispatch_count_by_tier: h.dispatch_count_by_tier.clone(),
                    accepted_count_by_tier: counters.accepted_count_by_tier.clone(),
                    queue_wait_ms_by_tier: counters.queue_wait_ms_by_tier.clone(),
                    transport_latency_ms_by_kind: counters.transport_latency_ms_by_kind.clone(),
                    fresh_candidates_by_tier: fresh_by_tier,
                    oldest_pending_age_by_tier: h.oldest_pending_age_ms_by_tier.clone(),
                })?;
            }
            if CONTINUOUS {
                let durable_now = engine.durable_timestamp(now)?;
                engine.compact_runtime_history(
                    durable_now.saturating_sub(30 * 24 * 60 * 60 * 1_000),
                )?;
                cohort_record_cursor = engine.cohort_indicator_records().len();
                technical_record_cursor = engine.technical_decision_records().len();
                persist_unsigned_state(&mut engine, &mut state_root)?;
                if let Err(error) = write_continuous_status(
                    &output,
                    &header.run_id,
                    start,
                    now,
                    execution_mode,
                    &config,
                    &engine,
                    &counters,
                    &h,
                    t.rate_limited.load(Ordering::SeqCst),
                    streaming_runtime_status(
                        streaming.as_ref(),
                        streaming_sources.as_ref(),
                        &engine,
                        market_directory.as_ref(),
                        &subscribed_trade_markets,
                        &coverage_ready_markets,
                        reconciliation_pending_count(
                            &reconciliation_remaining,
                            &coverage_reconciliation_remaining,
                        ),
                        coverage_reconciliations,
                        &subscribed_hot_books,
                    ),
                ) {
                    println!("rolling_status_update_failed=true nonfatal=true error={error}");
                }
            } else {
                engine
                    .persist(output.join("shadow-ledger.json"))
                    .map_err(|e| e.to_string())?;
                engine
                    .persist_target_state(output.join("virtual-target-ledger.json"))
                    .map_err(|e| e.to_string())?;
                evidence.write_summary("shadow-actions.json", engine.executions())?;
                evidence.write_summary(
                    "decision-plan-summary.json",
                    &engine.compact_decision_plan_summary(),
                )?;
                evidence.write_summary(
                    "cohort-indicator-records.json",
                    &engine.compact_cohort_decision_records(),
                )?;
                evidence.write_summary(
                    "cohort-decision-summary.json",
                    &engine.compact_cohort_decision_summary(),
                )?;
                evidence.write_summary(
                    "technical-decision-records.json",
                    engine.technical_decision_records(),
                )?;
                evidence.write_summary(
                    "book-evaluation-events.json",
                    engine.book_evaluation_events(),
                )?;
                evidence.write_summary(
                    "action-lifecycle-events.json",
                    engine.action_lifecycle_events(),
                )?;
                evidence.write_summary(
                    "executable-density-summary.json",
                    &engine.executable_density_summary()?,
                )?;
                evidence
                    .write_summary("five-minute-return-buckets.json", engine.equity_buckets())?;
                evidence.write_summary(
                    "portfolio-episodes.json",
                    engine.ledger().portfolio_closed(),
                )?;
                evidence
                    .write_summary("source-episodes.json", &engine.ledger().all_source_closed())?;
                evidence.write_summary(
                    "candidate-ingestion-audit.json",
                    &transport.candidate_audit_snapshot(),
                )?;
            }
            checkpoint_due = now + 60_000;
        }
    }
    if let Some(stream) = streaming.as_ref() {
        stream.shutdown().await;
    }
    while let Some(result) = tasks.join_next().await {
        let (dispatch, result, started_at, kind) = result?;
        let completed_request = dispatch.request().clone();
        record_latency(
            counters
                .transport_latency_ms_by_kind
                .entry(kind)
                .or_default(),
            clock.now_ms().saturating_sub(started_at),
        );
        let outcome = scheduler.finish(dispatch, result);
        append_freshness_decision(&mut evidence, &completed_request, outcome, clock.now_ms())?;
        if outcome == ExecutionOutcome::CompletedFresh {
            if let Some(mut response) = transport.take_accepted(&completed_request) {
                if streaming_enabled {
                    if let crate::public_mainnet::PublicPayload::SourceState(state) =
                        &mut response.payload
                    {
                        *state = streaming_sources
                            .as_mut()
                            .ok_or("streaming source book unavailable")?
                            .install_baseline(state.clone())
                            .map_err(|error| format!("source baseline: {error:?}"))?;
                    }
                }
                let accepted_at = clock.now_ms();
                if !CONTINUOUS {
                    evidence.append_replay_event(
                        accepted_at,
                        ReplayPayload::AcceptedResponse {
                            response: response.clone(),
                        },
                    )?;
                }
                let executions_before = engine.metrics().shadow_executions;
                engine
                    .ingest(response, accepted_at)
                    .map_err(|e| e.to_string())?;
                dispatch_production_intents(&mut engine, direct_live.as_ref()).await?;
                append_new_cohort_records(
                    &mut evidence,
                    &engine,
                    &mut cohort_record_cursor,
                    accepted_at,
                )?;
                append_new_technical_records(&mut evidence, &engine, &mut technical_record_cursor)?;
                if !CONTINUOUS || engine.metrics().shadow_executions != executions_before {
                    persist_unsigned_state(&mut engine, &mut state_root)?;
                }
            }
        } else {
            let _ = transport.take_accepted(&completed_request);
        }
    }
    if (options.profitability_gate || options.micro_density_gate) && !interrupted {
        let deadline = deadline.ok_or("qualification deadline missing")?;
        match engine.last_equity_boundary() {
            Some(last) if last == deadline => {}
            Some(last) if last < deadline => {
                engine.record_equity_boundary(deadline)?;
                evidence.append_replay_event(deadline, ReplayPayload::EquityBoundary)?;
                persist_unsigned_state(&mut engine, &mut state_root)?;
            }
            Some(_) => return Err("final equity boundary exceeds deadline".into()),
            None => return Err("initial equity boundary missing".into()),
        }
    }
    evidence.flush_replay_events()?;
    if CONTINUOUS {
        persist_unsigned_state(&mut engine, &mut state_root)?;
        let now = clock.now_ms();
        let health = scheduler.health();
        if let Err(error) = write_continuous_status(
            &output,
            &header.run_id,
            start,
            now,
            execution_mode,
            &config,
            &engine,
            &counters,
            &health,
            transport.metrics().rate_limited.load(Ordering::SeqCst),
            streaming_runtime_status(
                streaming.as_ref(),
                streaming_sources.as_ref(),
                &engine,
                market_directory.as_ref(),
                &subscribed_trade_markets,
                &coverage_ready_markets,
                reconciliation_pending_count(
                    &reconciliation_remaining,
                    &coverage_reconciliation_remaining,
                ),
                coverage_reconciliations,
                &subscribed_hot_books,
            ),
        ) {
            println!("rolling_status_update_failed=true nonfatal=true error={error}");
        }
        println!(
            "continuous_observer_stopped_by_operator=true healthy=true output={}",
            output.display()
        );
        return Ok(output);
    }
    #[cfg(feature = "research-cli")]
    {
        engine
            .persist(output.join("shadow-ledger.json"))
            .map_err(|e| e.to_string())?;
        engine
            .persist_target_state(output.join("virtual-target-ledger.json"))
            .map_err(|e| e.to_string())?;
        persist_unsigned_state(&mut engine, &mut state_root)?;
        let elapsed = (clock.now_ms() - start) / 1000;
        let chain = verify_checkpoint_chain(output.join("periodic-checkpoints.jsonl"))?;
        if std::fs::read_to_string(output.join("event-chain-head.txt"))?.trim() != chain {
            return Err("checkpoint chain mismatch".into());
        }
        let t = transport.metrics();
        let m = engine.metrics();
        let summary = TerminalSummary {
            stage: if options.micro_density_gate {
                "SU5R-MICRO-DENSITY".into()
            } else if options.transport_gate {
                "HL1K-TRANSPORT-GATE".into()
            } else if options.profitability_gate {
                "HL1-PROFITABILITY-4H".into()
            } else {
                "HL1K".into()
            },
            passed: false,
            failure_reason: Some(if interrupted {
                "operator_stopped_diagnostic_run".into()
            } else {
                "awaiting_external_finalization".into()
            }),
            secondary_reason: None,
            unsigned: true,
            planned_only: true,
            submission_capable: false,
            api_wallet_present: false,
            candidate_count: config.candidates.len(),
            monotonic_elapsed_seconds: elapsed,
            process_restarts: 0,
            risk_invariant_violations: m.projection_violations,
            mutation_requests: t.mutation_requests.load(Ordering::SeqCst),
            key_file_opens: t.key_file_opens.load(Ordering::SeqCst),
            closed_shadow_episodes: engine.ledger().portfolio_closed().len() as u64,
            closed_source_episodes: engine.ledger().total_source_closed() as u64,
            event_chain_verified: true,
            all_candidates_scheduled: scheduled.len() == config.candidates.len(),
            unreconciled_shadow_intents: m.unreconciled_shadow_intents,
            isolation_pre_run_passed: true,
            isolation_post_run_passed: false,
            evidence_verified: false,
            profitability_gate_passed: false,
            operational_gate_passed: false,
            accounting_gate_passed: false,
            profitability_signal_positive: false,
            sharpe_target_proven: false,
        };
        evidence.write_summary("scheduler-summary.json", &counters)?;
        evidence.write_summary("freshness-summary.json", m)?;
        evidence.write_summary("shadow-accounting-summary.json",&serde_json::json!({"executions":m.shadow_executions,"unreconciled":m.unreconciled_shadow_intents}))?;
        evidence.write_summary("shadow-actions.json", engine.executions())?;
        evidence.write_summary(
            "decision-plan-summary.json",
            &engine.compact_decision_plan_summary(),
        )?;
        evidence.write_summary(
            "cohort-indicator-records.json",
            &engine.compact_cohort_decision_records(),
        )?;
        evidence.write_summary(
            "cohort-decision-summary.json",
            &engine.compact_cohort_decision_summary(),
        )?;
        evidence.write_summary(
            "technical-decision-records.json",
            engine.technical_decision_records(),
        )?;
        evidence.write_summary(
            "book-evaluation-events.json",
            engine.book_evaluation_events(),
        )?;
        evidence.write_summary(
            "action-lifecycle-events.json",
            engine.action_lifecycle_events(),
        )?;
        let executable_density = engine.executable_density_summary()?;
        evidence.write_summary("executable-density-summary.json", &executable_density)?;
        evidence.write_summary("five-minute-return-buckets.json", engine.equity_buckets())?;
        evidence.write_summary(
            "portfolio-episodes.json",
            engine.ledger().portfolio_closed(),
        )?;
        evidence.write_summary("source-episodes.json", &engine.ledger().all_source_closed())?;
        evidence.write_summary(
            "candidate-ingestion-audit.json",
            &transport.candidate_audit_snapshot(),
        )?;
        evidence.write_summary(
        "episode-summary.json",
        &serde_json::json!({"closed_portfolio_episodes":engine.ledger().portfolio_closed().len(),"closed_source_episodes":engine.ledger().total_source_closed()}),
    )?;
        evidence.write_terminal(&summary)?;
        if options.transport_gate {
            let gate = evaluate_transport_gate(
                &output,
                &config,
                source_dispatch_plan,
                &candidate_tiers,
                &engine,
                &scheduler.health(),
                &counters,
                transport.metrics().rate_limited.load(Ordering::SeqCst),
            )?;
            evidence.write_summary("transport-gate-summary.json", &gate)?;
            if !gate.passed {
                return Err(
                    "short transport qualification failed; diagnostic bundle preserved".into(),
                );
            }
        } else if options.profitability_gate || options.micro_density_gate {
            let starting_equity =
                Decimal::from_f64(config.starting_equity_usd).ok_or("invalid starting equity")?;
            let profitability = summarize_profitability(
                starting_equity,
                engine.executions(),
                engine.equity_buckets(),
                engine.ledger(),
            )
            .map_err(|error| format!("profitability summary: {error}"))?;
            evidence.write_summary("profitability-summary.json", &profitability)?;
            let gate = evaluate_profitability_gate(
                &profitability,
                &executable_density,
                elapsed,
                scheduled.len() == config.candidates.len(),
                engine.active_source_count(clock.now_ms()) == config.candidates.len(),
                m.projection_violations,
                t.mutation_requests.load(Ordering::SeqCst),
                t.key_file_opens.load(Ordering::SeqCst),
                counters.reserved_weight,
                transport.metrics().rate_limited.load(Ordering::SeqCst),
            );
            evidence.write_summary("profitability-gate-summary.json", &gate)?;
            if options.micro_density_gate {
                let micro_gate = evaluate_micro_density_gate(
                    &executable_density,
                    elapsed,
                    scheduled.len() == config.candidates.len(),
                    engine.active_source_count(clock.now_ms()) == config.candidates.len(),
                    m.projection_violations,
                    t.mutation_requests.load(Ordering::SeqCst),
                    t.key_file_opens.load(Ordering::SeqCst),
                    counters.reserved_weight,
                    transport.metrics().rate_limited.load(Ordering::SeqCst),
                );
                evidence.write_summary("micro-density-gate-summary.json", &micro_gate)?;
                if !micro_gate.passed {
                    return Err(
                        "short micro-density gate failed; diagnostic bundle preserved".into(),
                    );
                }
            } else if !gate.passed {
                if interrupted {
                    return Err(
                        "four-hour profitability window interrupted; partial evidence retained"
                            .into(),
                    );
                }
                return Err(
                    "four-hour profitability measurement gate failed; bundle retained".into(),
                );
            }
        }
        if interrupted {
            return Err("HL1K interrupted; qualification clock reset".into());
        }
        println!(
            "hl1k_observation_complete=true provisional=true output={}",
            output.display()
        );
        Ok(output)
    }
    #[cfg(not(feature = "research-cli"))]
    unreachable!("the continuous daemon returns from its shutdown path")
}

fn write_continuous_status(
    output: &Path,
    run_id: &str,
    start: Timestamp,
    now: Timestamp,
    execution_mode: ExecutionMode,
    config: &CopyTradeConfig,
    engine: &LiveShadowEngine,
    counters: &Counters,
    scheduler: &SchedulerHealth,
    rate_limited_responses: u64,
    streaming: Option<StreamingRuntimeStatus>,
) -> Result<(), Box<dyn Error>> {
    let mfce = engine.mfce_report();
    let unresolved_roots = engine.unresolved_actionable_root_count();
    let durable_start = engine.durable_timestamp(start)?;
    let durable_now = engine.durable_timestamp(now)?;
    let economics = [
        ("lifetime", 0),
        ("since_process_start", durable_start),
        ("last_24h", durable_now.saturating_sub(24 * 60 * 60 * 1_000)),
        (
            "last_7d",
            durable_now.saturating_sub(7 * 24 * 60 * 60 * 1_000),
        ),
        (
            "last_30d",
            durable_now.saturating_sub(30 * 24 * 60 * 60 * 1_000),
        ),
    ]
    .into_iter()
    .map(|(horizon, cutoff)| runtime_economic_status(horizon, cutoff, config, engine))
    .collect::<Result<Vec<_>, _>>()?;
    let metrics = engine.metrics();
    let deployment_equity = engine.deployment_equity()?;
    let status = ContinuousStatus {
        mode: match execution_mode {
            ExecutionMode::Shadow => "continuous_shadow",
            ExecutionMode::Live => "continuous_live",
        },
        run_id,
        started_at_mono: start,
        observed_at_mono: now,
        elapsed_seconds: now.saturating_sub(start) / 1_000,
        healthy: metrics.projection_violations == 0
            && metrics.persistence_failures == 0
            && unresolved_roots == 0
            && engine.source_stream_healthy(),
        fatal_stop: false,
        snapshot_generation: engine.snapshot_generation(),
        candidate_count: config.candidates.len(),
        projection_violations: metrics.projection_violations,
        persistence_failures: metrics.persistence_failures,
        unresolved_roots,
        unresolved_root_details: engine.unresolved_actionable_roots(),
        open_episodes: engine.ledger().portfolio_open_count(),
        closed_episodes_lifetime: engine.ledger().portfolio_closed_count(),
        scheduler_pending: scheduler.pending,
        scheduler_in_flight: scheduler.in_flight,
        rest_weight_consumed_process: counters.total_weight,
        rest_reserved_weight_consumed_process: counters.reserved_weight,
        rate_limited_responses,
        streaming,
        unique_source_transitions: mfce.observed_transitions,
        modeled_executions_retained: engine.executions().len(),
        mfce_completed_transition_labels: mfce.completed_samples,
        mfce_active_transitions: mfce.active_transitions,
        mfce_awaiting_live_books: mfce.awaiting_live_books,
        mfce_book_ready_transitions: mfce
            .active_transitions
            .saturating_sub(mfce.awaiting_live_books),
        mfce_model_epoch: mfce.incumbent_epoch,
        mfce_policy_exploit: mfce.decision_counts.exploit,
        mfce_policy_explore: mfce.decision_counts.explore,
        mfce_policy_reject: mfce.decision_counts.reject,
        source_risk_increases_allocated: mfce.decision_counts.allocated,
        source_risk_increases_rejected_by_mfce: mfce.decision_counts.reject,
        marked_equity: deployment_equity.current_equity,
        settled_equity: deployment_equity.settled_equity,
        open_marked_equity_contribution: deployment_equity.unrealized_net_pnl,
        mfce,
        economics,
    };
    let path = output.join("rolling-status.json");
    let temporary = output.join("rolling-status.tmp");
    let mut bytes = serde_json::to_vec_pretty(&status)?;
    bytes.push(b'\n');
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    let execution_start = engine.executions().len().saturating_sub(1_000);
    let journal = serde_json::json!({
        "schema_version": 1,
        "scope": "last_1000_modeled_executions_and_lifecycle_events",
        "executions": &engine.executions()[execution_start..],
        "lifecycle": engine.action_lifecycle_events(),
    });
    let journal_path = output.join("rolling-execution-journal.json");
    let journal_temporary = output.join("rolling-execution-journal.tmp");
    let mut journal_bytes = serde_json::to_vec(&journal)?;
    journal_bytes.push(b'\n');
    std::fs::write(&journal_temporary, journal_bytes)?;
    std::fs::rename(journal_temporary, journal_path)?;
    Ok(())
}

fn streaming_runtime_status(
    stream: Option<&StreamingHandle>,
    sources: Option<&StreamingSourceBook>,
    engine: &LiveShadowEngine,
    directory: Option<&MarketDirectory>,
    subscribed_trade_markets: &BTreeSet<String>,
    coverage_ready_markets: &BTreeSet<String>,
    coverage_reconciliation_wallets_pending: usize,
    coverage_reconciliations: u64,
    subscribed_hot_books: &BTreeSet<String>,
) -> Option<StreamingRuntimeStatus> {
    let metrics = stream?.metrics();
    let discovered_markets = directory.map_or(0, |directory| directory.markets().len());
    Some(StreamingRuntimeStatus {
        source_healthy: engine.source_stream_healthy(),
        hydrated_source_count: sources.map_or(0, StreamingSourceBook::hydrated_count),
        discovered_markets,
        subscribed_trade_markets: subscribed_trade_markets.len(),
        coverage_ready_markets: coverage_ready_markets.len(),
        coverage_stale_markets: discovered_markets.saturating_sub(coverage_ready_markets.len()),
        coverage_reconciliation_wallets_pending,
        coverage_reconciliations,
        subscribed_hot_books: subscribed_hot_books.len(),
        connections: metrics.connections.load(Ordering::SeqCst),
        gaps: metrics.gaps.load(Ordering::SeqCst),
        public_trades_seen: metrics.trades.load(Ordering::SeqCst),
        tracked_trade_updates: metrics.tracked_trade_updates.load(Ordering::SeqCst),
        book_updates: metrics.book_updates.load(Ordering::SeqCst),
        invalid_messages: metrics.invalid_messages.load(Ordering::SeqCst),
    })
}

fn runtime_economic_status(
    horizon: &'static str,
    cutoff: Timestamp,
    config: &CopyTradeConfig,
    engine: &LiveShadowEngine,
) -> Result<RollingEconomicStatus, Box<dyn Error>> {
    let episodes = engine
        .ledger()
        .portfolio_closed()
        .iter()
        .filter(|episode| episode.closed_at >= cutoff)
        .collect::<Vec<_>>();
    let executions = engine
        .executions()
        .iter()
        .filter(|execution| execution.evaluation_timestamp_mono >= cutoff)
        .collect::<Vec<_>>();
    let buckets = engine
        .equity_buckets()
        .iter()
        .filter(|bucket| bucket.closed_at_mono >= cutoff)
        .collect::<Vec<_>>();
    let sum_decimal = |values: Vec<Decimal>| -> Result<Decimal, Box<dyn Error>> {
        values.into_iter().try_fold(Decimal::ZERO, |sum, value| {
            sum.checked_add(value)
                .ok_or_else(|| "rolling economic sum overflow".into())
        })
    };
    let include_archived = horizon == "lifetime";
    let mut turnover = sum_decimal(executions.iter().map(|item| item.filled_notional).collect())?;
    let mut gross_pnl = sum_decimal(episodes.iter().map(|item| item.realized_pnl).collect())?;
    let mut fees = sum_decimal(episodes.iter().map(|item| item.fees).collect())?;
    let mut slippage = sum_decimal(episodes.iter().map(|item| item.slippage).collect())?;
    let mut funding = sum_decimal(episodes.iter().map(|item| item.funding).collect())?;
    let mut net_pnl = sum_decimal(episodes.iter().map(|item| item.net_pnl).collect())?;
    let mut gains = sum_decimal(
        episodes
            .iter()
            .filter(|item| item.net_pnl > Decimal::ZERO)
            .map(|item| item.net_pnl)
            .collect(),
    )?;
    let mut losses = sum_decimal(
        episodes
            .iter()
            .filter(|item| item.net_pnl < Decimal::ZERO)
            .map(|item| item.net_pnl.abs())
            .collect(),
    )?;
    let configured_starting_equity = Decimal::from_f64(config.starting_equity_usd)
        .ok_or("invalid configured starting equity")?;
    let archived_net_pnl = engine
        .ledger()
        .portfolio_archived_totals()
        .values()
        .try_fold(Decimal::ZERO, |sum, totals| sum.checked_add(totals.net_pnl))
        .ok_or("archived settled pnl overflow")?;
    let starting_settled_equity = if include_archived {
        configured_starting_equity
    } else {
        engine
            .ledger()
            .portfolio_closed()
            .iter()
            .filter(|episode| episode.closed_at < cutoff)
            .try_fold(
                configured_starting_equity
                    .checked_add(archived_net_pnl)
                    .ok_or("starting settled equity overflow")?,
                |equity, episode| equity.checked_add(episode.net_pnl),
            )
            .ok_or("starting settled equity overflow")?
    };
    let mut net_pnl_by_asset = BTreeMap::new();
    for episode in &episodes {
        let value = net_pnl_by_asset.entry(episode.asset.clone()).or_default();
        *value =
            Decimal::checked_add(*value, episode.net_pnl).ok_or("rolling asset pnl overflow")?;
    }
    let mut archived_closures = 0usize;
    if include_archived {
        for (asset, totals) in engine.ledger().portfolio_archived_totals() {
            archived_closures = archived_closures.saturating_add(totals.closed_count as usize);
            turnover = turnover
                .checked_add(totals.entry_notional)
                .and_then(|value| value.checked_add(totals.exit_notional))
                .ok_or("lifetime turnover overflow")?;
            gross_pnl = gross_pnl
                .checked_add(totals.gross_pnl)
                .ok_or("lifetime gross pnl overflow")?;
            fees = fees
                .checked_add(totals.fees)
                .ok_or("lifetime fees overflow")?;
            slippage = slippage
                .checked_add(totals.slippage)
                .ok_or("lifetime slippage overflow")?;
            funding = funding
                .checked_add(totals.funding)
                .ok_or("lifetime funding overflow")?;
            net_pnl = net_pnl
                .checked_add(totals.net_pnl)
                .ok_or("lifetime net pnl overflow")?;
            gains = gains
                .checked_add(totals.gains)
                .ok_or("lifetime gains overflow")?;
            losses = losses
                .checked_add(totals.losses)
                .ok_or("lifetime losses overflow")?;
            let value = net_pnl_by_asset.entry(asset.clone()).or_default();
            *value = value
                .checked_add(totals.net_pnl)
                .ok_or("lifetime asset pnl overflow")?;
        }
    }
    let mut execution_net_pnl_by_context = BTreeMap::new();
    for execution in &executions {
        let value = execution_net_pnl_by_context
            .entry(execution.economic_attribution)
            .or_default();
        *value = Decimal::checked_add(*value, execution.net_pnl_delta)
            .ok_or("rolling context pnl overflow")?;
    }
    let mut net_pnl_by_source = BTreeMap::new();
    for episode in engine
        .ledger()
        .all_source_closed()
        .into_iter()
        .filter(|episode| episode.closed_at >= cutoff)
    {
        let value = net_pnl_by_source.entry(episode.candidate_id).or_default();
        *value = Decimal::checked_add(*value, episode.modeled_net_pnl)
            .ok_or("rolling source pnl overflow")?;
    }
    let returns = buckets
        .iter()
        .filter_map(|bucket| bucket.return_fraction.to_f64())
        .collect::<Vec<_>>();
    let annualized_sharpe_5m = if returns.len() < 2 {
        0.0
    } else {
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (returns.len() - 1) as f64;
        if variance <= 0.0 {
            0.0
        } else {
            mean / variance.sqrt() * (365.0_f64 * 24.0 * 12.0).sqrt()
        }
    };
    let mut peak = buckets
        .first()
        .map_or(starting_settled_equity, |bucket| bucket.starting_equity);
    let mut maximum_drawdown = Decimal::ZERO;
    for bucket in buckets {
        peak = peak.max(bucket.ending_equity);
        if !peak.is_zero() {
            let drawdown = peak
                .checked_sub(bucket.ending_equity)
                .and_then(|value| value.checked_div(peak))
                .ok_or("rolling drawdown overflow")?;
            maximum_drawdown = maximum_drawdown.max(drawdown);
        }
    }
    Ok(RollingEconomicStatus {
        horizon,
        cutoff_mono: cutoff,
        archived_episode_totals_included: include_archived,
        attribution_scope: "exact_retained_30_day_episode_and_execution_records",
        closures: episodes.len().saturating_add(archived_closures),
        executions: executions.len(),
        turnover,
        gross_pnl,
        fees,
        slippage,
        funding,
        net_pnl,
        net_pnl_per_turnover: if turnover.is_zero() {
            Decimal::ZERO
        } else {
            net_pnl
                .checked_div(turnover)
                .ok_or("rolling efficiency overflow")?
        },
        starting_settled_equity,
        settled_equity_return: if starting_settled_equity.is_zero() {
            Decimal::ZERO
        } else {
            net_pnl
                .checked_div(starting_settled_equity)
                .ok_or("rolling return overflow")?
        },
        profit_factor_state: if episodes.len().saturating_add(archived_closures) == 0 {
            "unavailable_no_closures"
        } else if losses.is_zero() && gains > Decimal::ZERO {
            "infinite_no_losses"
        } else if losses.is_zero() {
            "unavailable_no_wins_or_losses"
        } else {
            "finite"
        },
        profit_factor: (!losses.is_zero())
            .then(|| gains.checked_div(losses).ok_or("rolling PF overflow"))
            .transpose()?,
        maximum_drawdown,
        annualized_sharpe_5m,
        net_pnl_by_asset,
        net_pnl_by_source,
        execution_net_pnl_by_context,
    })
}

fn manifest_stage_matches(production_enabled: bool, stage: &str) -> bool {
    if production_enabled {
        stage == "PRODUCTION_RELEASE"
    } else {
        matches!(stage, "HL1J" | "PRODUCTION_RELEASE")
    }
}

async fn dispatch_production_intents(
    engine: &mut LiveShadowEngine,
    direct_live: Option<
        &std::sync::Arc<
            tokio::sync::Mutex<
                LiveExecutionRuntime<copytrade_signer::transport::HyperliquidMainnetTransport>,
            >,
        >,
    >,
) -> Result<(), Box<dyn Error>> {
    let Some(direct_live) = direct_live else {
        return Ok(());
    };
    let mut live = direct_live.lock().await;
    let recovering = live.recovery_only();
    let mut intents = engine.take_prepared_authorized_intents().into_iter();
    if recovering {
        for intent in intents.by_ref() {
            engine.release_unaccepted_production_intent(intent.planned_cloid);
        }
    }
    let mut latest_exchange_timestamp = None;
    if !recovering {
        while let Some(intent) = intents.next() {
            let unix_ms: u64 = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis()
                .try_into()?;
            let cloid = intent.planned_cloid;
            let (_, update) = match live.submit(intent, unix_ms).await {
                Ok(result) => result,
                Err(error) if recoverable_reconciliation_error(&error) => {
                    for unsubmitted in intents {
                        engine.release_unaccepted_production_intent(unsubmitted.planned_cloid);
                    }
                    engine.enter_live_recovery_only();
                    live.defer_recovery(unix_ms);
                    // The durable submission registry and deterministic CLOID now
                    // decide whether the action reached the exchange. Do not emit
                    // another action until authenticated reconciliation resolves it.
                    return Ok(());
                }
                Err(error) => {
                    engine.release_unaccepted_production_intent(cloid);
                    return Err(error.into());
                }
            };
            match apply_authenticated_reconciliation(engine, &mut live, update, unix_ms)? {
                AuthenticatedReconciliation::Applied(timestamp) => {
                    latest_exchange_timestamp = latest_exchange_timestamp.max(timestamp);
                }
                AuthenticatedReconciliation::RecoveryPending => {
                    for unsubmitted in intents {
                        engine.release_unaccepted_production_intent(unsubmitted.planned_cloid);
                    }
                    return Ok(());
                }
            }
        }
    }
    let unix_ms: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    match live.reconcile_if_due(unix_ms).await {
        Ok(Some(update)) => {
            match apply_authenticated_reconciliation(engine, &mut live, update, unix_ms)? {
                AuthenticatedReconciliation::Applied(timestamp) => {
                    latest_exchange_timestamp = latest_exchange_timestamp.max(timestamp);
                }
                AuthenticatedReconciliation::RecoveryPending => return Ok(()),
            }
        }
        Ok(None) => {}
        Err(error) if recoverable_reconciliation_error(&error) => {
            engine.enter_live_recovery_only();
            live.defer_recovery(unix_ms);
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    }
    if let Some(timestamp) = latest_exchange_timestamp {
        engine.recompute_after_production_updates(timestamp)?;
    }
    Ok(())
}

enum AuthenticatedReconciliation {
    Applied(Option<u64>),
    RecoveryPending,
}

fn apply_authenticated_reconciliation(
    engine: &mut LiveShadowEngine,
    live: &mut LiveExecutionRuntime<copytrade_signer::transport::HyperliquidMainnetTransport>,
    update: LiveExecutionUpdate,
    observed_at: u64,
) -> Result<AuthenticatedReconciliation, Box<dyn Error>> {
    let checkpoint = engine.live_execution_checkpoint();
    let result = apply_live_update_inner(engine, update, observed_at);
    match result {
        Ok(timestamp) if engine.live_positions_match(live.state()) => {
            engine.synchronize_live_state(live.state());
            live.leave_recovery_only();
            Ok(AuthenticatedReconciliation::Applied(timestamp))
        }
        Ok(_) => {
            engine.restore_live_execution_checkpoint(checkpoint);
            engine.enter_live_recovery_only();
            live.defer_recovery(observed_at);
            if live.note_application_recovery_failure() >= 3 {
                return Err(
                    "authoritative exchange base positions still disagree with observer state after three bounded reconciliation attempts"
                        .into(),
                );
            }
            Ok(AuthenticatedReconciliation::RecoveryPending)
        }
        Err(error) => {
            engine.restore_live_execution_checkpoint(checkpoint);
            engine.enter_live_recovery_only();
            live.defer_recovery(observed_at);
            if live.note_application_recovery_failure() >= 3 {
                return Err(format!(
                    "authoritative exchange activity could not be applied after three bounded reconciliation attempts: {error}"
                )
                .into());
            }
            Ok(AuthenticatedReconciliation::RecoveryPending)
        }
    }
}

fn apply_live_update_inner(
    engine: &mut LiveShadowEngine,
    update: LiveExecutionUpdate,
    observed_at: u64,
) -> Result<Option<u64>, Box<dyn Error>> {
    enum ExchangeEvent<'a> {
        Fill(&'a copytrade_core::live_trading::VerifiedExchangeFill),
        Funding(&'a copytrade_core::live_trading::VerifiedFundingEvent),
    }
    impl ExchangeEvent<'_> {
        fn occurred_at(&self) -> u64 {
            match self {
                Self::Fill(fill) => fill.occurred_at,
                Self::Funding(event) => event.occurred_at,
            }
        }
    }
    let mut events = update
        .applied
        .fills
        .iter()
        .map(ExchangeEvent::Fill)
        .chain(update.applied.funding.iter().map(ExchangeEvent::Funding))
        .collect::<Vec<_>>();
    events.sort_by_key(ExchangeEvent::occurred_at);
    let latest_exchange_timestamp = events.last().map(ExchangeEvent::occurred_at);
    for event in events {
        match event {
            ExchangeEvent::Fill(fill) => engine.apply_live_execution_fill(fill)?,
            ExchangeEvent::Funding(event) => engine.apply_live_funding(event)?,
        }
    }
    for terminal in update.terminal {
        engine.resolve_live_execution_terminal(
            terminal.cloid,
            terminal.original_quantity,
            terminal.filled_quantity,
            observed_at,
            terminal.rejected,
        )?;
    }
    Ok(latest_exchange_timestamp)
}

#[cfg(test)]
fn derive_source_dispatch_plan(
    candidate_count: usize,
    scheduler: &ReadOnlySchedulerConfig,
    transport: &PublicTransportPolicy,
) -> Result<SourceDispatchPlan, Box<dyn Error>> {
    derive_source_dispatch_plan_with_expansion(candidate_count, 0, 0, scheduler, transport)
}

fn streaming_source_dispatch_plan(
    candidate_count: usize,
    scheduler: &ReadOnlySchedulerConfig,
    transport: &PublicTransportPolicy,
) -> Result<SourceDispatchPlan, Box<dyn Error>> {
    if candidate_count == 0 || !transport.streaming.enabled {
        return Err("streaming source universe must be nonempty and enabled".into());
    }
    let expanded_weight = scheduler
        .api
        .endpoint_weights
        .get(&ReadRequestKind::ExpandedSourceState)
        .copied()
        .ok_or("expanded source-state endpoint weight missing")?;
    // One default DEX plus at most twenty discovered builder DEXes, each at
    // the official clearinghouseState weight of two.
    if expanded_weight < 42 {
        return Err("streaming reconciliation must reserve 42 REST weight per wallet".into());
    }
    let source_call_capacity_per_window =
        u64::from(scheduler.api.source_polling_weight_per_window / expanded_weight);
    if source_call_capacity_per_window == 0 {
        return Err("streaming reconciliation has no REST budget".into());
    }
    let cadence = transport.streaming.reconciliation_spread_ms;
    Ok(SourceDispatchPlan {
        effective_candidate_count: candidate_count,
        active_candidate_count: 0,
        inactive_candidate_count: candidate_count,
        active_cadence_ms: cadence,
        inactive_cadence_ms: cadence,
        source_call_capacity_per_window,
        active_dispatches_per_window: 0,
        inactive_dispatches_per_window: source_call_capacity_per_window,
        combined_dispatches_per_window: source_call_capacity_per_window,
    })
}

fn derive_source_dispatch_plan_with_expansion(
    candidate_count: usize,
    expanded_candidate_count: usize,
    additional_perp_dex_count: usize,
    scheduler: &ReadOnlySchedulerConfig,
    transport: &PublicTransportPolicy,
) -> Result<SourceDispatchPlan, Box<dyn Error>> {
    if candidate_count == 0 {
        return Err("effective source candidate universe must be nonempty".into());
    }
    if expanded_candidate_count > candidate_count {
        return Err("expanded source cohort exceeds candidate universe".into());
    }
    let window_ms = scheduler.api.window_ms;
    let active_cadence_ms = transport.active_source_interval_ms;
    let inactive_cadence_ms = transport.inactive_source_interval_ms;
    if window_ms % active_cadence_ms != 0 || window_ms % inactive_cadence_ms != 0 {
        return Err("source cadences must divide the scheduler accounting window exactly".into());
    }
    let active_dispatches_per_candidate = window_ms / active_cadence_ms;
    let inactive_dispatches_per_candidate = window_ms / inactive_cadence_ms;
    if active_dispatches_per_candidate <= inactive_dispatches_per_candidate {
        return Err("active source cadence must dispatch more often than inactive cadence".into());
    }
    let source_state_weight = scheduler
        .api
        .endpoint_weights
        .get(&ReadRequestKind::SourceState)
        .copied()
        .ok_or("source-state endpoint weight missing")?;
    if source_state_weight == 0 {
        return Err("source-state endpoint weight must be positive".into());
    }
    let expanded_source_state_weight = scheduler
        .api
        .endpoint_weights
        .get(&ReadRequestKind::ExpandedSourceState)
        .copied()
        .ok_or("expanded source-state endpoint weight missing")?;
    if expanded_candidate_count > 0
        && expanded_source_state_weight
            != source_state_weight
                .checked_mul(u32::try_from(additional_perp_dex_count + 1)?)
                .ok_or("expanded source-state weight overflow")?
    {
        return Err("expanded source-state weight does not match physical DEX calls".into());
    }
    let source_call_capacity_per_window =
        u64::from(scheduler.api.source_polling_weight_per_window / source_state_weight);
    let active_dispatch_target = u64::from(
        scheduler
            .api
            .source_tier_dispatch_targets_per_window
            .get(&SourceTier::Active)
            .copied()
            .ok_or("active source dispatch target missing")?,
    );
    let policy_active_capacity = active_dispatch_target
        .checked_div(active_dispatches_per_candidate)
        .ok_or("active source cadence must be positive")?;
    let base_inactive_dispatches = u64::try_from(candidate_count)?
        .checked_mul(inactive_dispatches_per_candidate)
        .ok_or("inactive source demand overflow")?;
    let expansion_calls = u64::try_from(expanded_candidate_count)?
        .checked_mul(u64::try_from(additional_perp_dex_count)?)
        .and_then(|value| value.checked_mul(inactive_dispatches_per_candidate))
        .ok_or("expanded source demand overflow")?;
    let standard_dispatches = u64::try_from(candidate_count - expanded_candidate_count)?
        .checked_mul(inactive_dispatches_per_candidate)
        .ok_or("standard source demand overflow")?;
    let expanded_dispatches = u64::try_from(expanded_candidate_count)?
        .checked_mul(inactive_dispatches_per_candidate)
        .ok_or("expanded source demand overflow")?;
    let base_weight = standard_dispatches
        .checked_mul(u64::from(source_state_weight))
        .and_then(|weight| {
            expanded_dispatches
                .checked_mul(u64::from(expanded_source_state_weight))?
                .checked_add(weight)
        })
        .ok_or("expanded source weight overflow")?;
    if base_weight > u64::from(scheduler.api.source_polling_weight_per_window) {
        return Err(format!(
            "effective candidate count {candidate_count} with {expanded_candidate_count} expanded sources cannot remain fresh: source weight {base_weight} exceeds {} per window",
            scheduler.api.source_polling_weight_per_window
        )
        .into());
    }
    let incremental_active_dispatches = active_dispatches_per_candidate
        .checked_sub(inactive_dispatches_per_candidate)
        .ok_or("active source dispatch increment underflow")?;
    // Multi-DEX source snapshots already consume the bounded source reserve.
    // Keep every source at the existing safe inactive cadence instead of
    // silently under-accounting an expanded active source's extra API calls.
    let budget_active_capacity = if expansion_calls == 0 {
        source_call_capacity_per_window
            .checked_sub(base_inactive_dispatches)
            .ok_or("source capacity underflow")?
            .checked_div(incremental_active_dispatches)
            .ok_or("active source dispatch increment must be positive")?
    } else {
        0
    };
    let active_candidate_count =
        candidate_count
            .min(MAX_ACTIVE_SOURCE_CANDIDATES)
            .min(usize::try_from(
                policy_active_capacity.min(budget_active_capacity),
            )?);
    let inactive_candidate_count = candidate_count.saturating_sub(active_candidate_count);
    let active_dispatches_per_window = u64::try_from(active_candidate_count)?
        .checked_mul(active_dispatches_per_candidate)
        .ok_or("active source demand overflow")?;
    let inactive_dispatches_per_window = u64::try_from(inactive_candidate_count)?
        .checked_mul(inactive_dispatches_per_candidate)
        .ok_or("inactive source demand overflow")?;
    let combined_dispatches_per_window = active_dispatches_per_window
        .checked_add(inactive_dispatches_per_window)
        .ok_or("combined source demand overflow")?;
    if combined_dispatches_per_window > source_call_capacity_per_window {
        return Err("derived source dispatch plan exceeds unchanged source budget".into());
    }
    Ok(SourceDispatchPlan {
        effective_candidate_count: candidate_count,
        active_candidate_count,
        inactive_candidate_count,
        active_cadence_ms,
        inactive_cadence_ms,
        source_call_capacity_per_window,
        active_dispatches_per_window,
        inactive_dispatches_per_window,
        combined_dispatches_per_window,
    })
}

fn assign_source_tiers(
    ordered_candidates: &[String],
    candidates_with_positions: &BTreeSet<String>,
    active_capacity: usize,
) -> BTreeMap<String, SourceTier> {
    let mut active = BTreeSet::new();
    for candidate in ordered_candidates {
        if candidates_with_positions.contains(candidate) && active.len() < active_capacity {
            active.insert(candidate.clone());
        }
    }
    for candidate in ordered_candidates {
        if active.len() >= active_capacity {
            break;
        }
        active.insert(candidate.clone());
    }
    ordered_candidates
        .iter()
        .map(|candidate| {
            (
                candidate.clone(),
                if active.contains(candidate) {
                    SourceTier::Active
                } else {
                    SourceTier::Inactive
                },
            )
        })
        .collect()
}

fn phase_staggered_source_due(
    config: &CopyTradeConfig,
    start: u64,
    plan: SourceDispatchPlan,
) -> Result<BTreeMap<String, u64>, Box<dyn Error>> {
    if config.candidates.len() != plan.effective_candidate_count {
        return Err("source dispatch plan candidate cardinality mismatch".into());
    }
    let active_count = plan.active_candidate_count;
    let inactive_count = plan.inactive_candidate_count;
    config
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            let (interval, ordinal, count) = if index < active_count {
                (plan.active_cadence_ms, index, active_count)
            } else {
                (
                    plan.inactive_cadence_ms,
                    index - active_count,
                    inactive_count,
                )
            };
            Ok((
                candidate.address.to_ascii_lowercase(),
                phase_deadline(start, interval, ordinal, count)?,
            ))
        })
        .collect()
}

fn restart_history_recovery(
    recovery: &mut Option<HistoryRecovery>,
    config: &CopyTradeConfig,
    plan: SourceDispatchPlan,
    start: Timestamp,
    remaining: &mut BTreeSet<String>,
    source_due: &mut BTreeMap<String, Timestamp>,
) -> Result<(), Box<dyn Error>> {
    *recovery = Some(HistoryRecovery::default());
    *remaining = config
        .candidates
        .iter()
        .map(|candidate| candidate.address.to_ascii_lowercase())
        .collect();
    *source_due = phase_staggered_source_due(config, start, plan)?;
    Ok(())
}

fn reconciliation_pending_count(
    stream_reconciliation: &BTreeSet<String>,
    coverage_reconciliation: &BTreeSet<String>,
) -> usize {
    stream_reconciliation.union(coverage_reconciliation).count()
}

fn rearm_unresolved_source_request(
    request: &ScheduledReadRequest,
    outcome: ExecutionOutcome,
    now: Timestamp,
    retry_delay_ms: u64,
    stream_reconciliation: &BTreeSet<String>,
    coverage_reconciliation: &BTreeSet<String>,
    source_due: &mut BTreeMap<String, Timestamp>,
) -> Result<bool, Box<dyn Error>> {
    if !request.kind.uses_source_polling_budget()
        || matches!(
            outcome,
            ExecutionOutcome::CompletedFresh
                | ExecutionOutcome::RetryScheduled
                | ExecutionOutcome::Superseded
        )
    {
        return Ok(false);
    }
    let Some(candidate) = request.candidate_id.as_ref() else {
        return Ok(false);
    };
    if !stream_reconciliation.contains(candidate) && !coverage_reconciliation.contains(candidate) {
        return Ok(false);
    }
    source_due.insert(
        candidate.clone(),
        now.checked_add(retry_delay_ms)
            .ok_or("source reconciliation retry overflow")?,
    );
    Ok(true)
}

fn rearm_idle_stream_reconciliation(
    config: &CopyTradeConfig,
    plan: SourceDispatchPlan,
    now: Timestamp,
    scheduler_idle: bool,
    stream_reconciliation: &BTreeSet<String>,
    coverage_reconciliation: &BTreeSet<String>,
    source_due: &mut BTreeMap<String, Timestamp>,
) -> Result<bool, Box<dyn Error>> {
    if !scheduler_idle
        || (stream_reconciliation.is_empty() && coverage_reconciliation.is_empty())
        || stream_reconciliation
            .union(coverage_reconciliation)
            .any(|candidate| {
                source_due
                    .get(candidate)
                    .is_some_and(|due| *due != u64::MAX)
            })
    {
        return Ok(false);
    }
    *source_due = phase_staggered_source_due(config, now, plan)?;
    Ok(true)
}

fn phase_deadline(
    start: u64,
    interval: u64,
    ordinal: usize,
    count: usize,
) -> Result<u64, Box<dyn Error>> {
    if count == 0 || ordinal >= count {
        return Err("invalid phase cardinality".into());
    }
    let offset = u128::from(interval)
        .checked_mul(ordinal as u128)
        .and_then(|value| value.checked_div(count as u128))
        .ok_or("phase arithmetic overflow")?;
    start
        .checked_add(offset.try_into()?)
        .ok_or_else(|| "phase deadline overflow".into())
}

fn next_cadence_deadline(
    previous_due: u64,
    now: u64,
    interval: u64,
) -> Result<u64, Box<dyn Error>> {
    let elapsed_intervals = now
        .saturating_sub(previous_due)
        .checked_div(interval)
        .ok_or("zero cadence interval")?;
    previous_due
        .checked_add(
            elapsed_intervals
                .checked_add(1)
                .and_then(|count| count.checked_mul(interval))
                .ok_or("cadence overflow")?,
        )
        .ok_or_else(|| "cadence deadline overflow".into())
}

#[cfg(any(feature = "research-cli", test))]
fn required_dispatch_count(
    candidate_count: usize,
    elapsed_ms: u64,
    cadence_ms: u64,
) -> Result<u64, Box<dyn Error>> {
    if cadence_ms == 0 {
        return Err("source cadence must be positive".into());
    }
    let count = u128::from(u64::try_from(candidate_count)?)
        .checked_mul(u128::from(elapsed_ms))
        .and_then(|value| value.checked_div(u128::from(cadence_ms)))
        .ok_or("required source dispatch arithmetic overflow")?;
    Ok(count.try_into()?)
}

#[cfg(feature = "research-cli")]
pub fn finalize_qualification(
    bundle: &Path,
    post_report: &Path,
) -> Result<TerminalSummary, Box<dyn Error>> {
    verify_isolation_report(post_report)?;
    let mut s: TerminalSummary =
        serde_json::from_slice(&std::fs::read(bundle.join("terminal-summary.json"))?)?;
    let h: RunHeader = serde_json::from_slice(&std::fs::read(bundle.join("run-header.json"))?)?;
    if sha256_file(bundle.join("observer-release-binary"))? != h.binary_sha256 {
        return Err("observed release binary hash mismatch".into());
    }
    let qualification_config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(bundle.join("qualification-config.json"))?)?;
    let recorded_config: CopyTradeConfig = serde_json::from_value(
        qualification_config
            .get("copytrade")
            .cloned()
            .ok_or("recorded copytrade configuration missing")?,
    )?;
    if derive_config_hash(&recorded_config)?.to_hex() != h.configuration_sha256
        || derive_risk_policy_hash(&recorded_config.global_risk)?.to_hex() != h.risk_policy_sha256
        || sha256_json(
            qualification_config
                .get("read_policy")
                .ok_or("recorded read policy missing")?,
        )? != h.read_policy_sha256
        || sha256_json(
            qualification_config
                .get("transport_policy")
                .ok_or("recorded transport policy missing")?,
        )? != h.transport_policy_sha256
    {
        return Err("recorded configuration or policy hash mismatch".into());
    }
    if let Some(identity) = &recorded_config.very_profitable_layer {
        let artifact_path = bundle.join("very-profitable-layer.json");
        let artifact = VeryProfitableLayerArtifact::from_path(&artifact_path)
            .map_err(|error| format!("invalid retained cohort artifact: {error}"))?;
        if artifact.canonical_sha256()? != identity.artifact_sha256
            || artifact.membership.observed_at_ms != identity.cohort_snapshot_timestamp_ms
        {
            return Err("retained cohort artifact does not match the frozen identity".into());
        }
        let indicators: Vec<copytrade_core::cohort::CohortIndicatorRecord> =
            serde_json::from_slice(&std::fs::read(
                bundle.join("cohort-indicator-records.json"),
            )?)?;
        if indicators.iter().any(|indicator| {
            indicator.cohort_snapshot_timestamp_ms != identity.cohort_snapshot_timestamp_ms
                || indicator.membership_set_hash != identity.membership_set_hash
        }) {
            return Err("cohort indicator evidence does not match the frozen membership".into());
        }
    } else if bundle.join("very-profitable-layer.json").exists() {
        return Err("unbound cohort artifact is present in the qualification bundle".into());
    }
    let chain = verify_checkpoint_chain(bundle.join("periodic-checkpoints.jsonl"))?;
    if std::fs::read_to_string(bundle.join("event-chain-head.txt"))?.trim() != chain {
        return Err("event chain mismatch".into());
    }
    let replay_chain = verify_replay_event_chain(bundle.join("replay-events.jsonl"))?;
    if std::fs::read_to_string(bundle.join("replay-event-chain-head.txt"))?.trim() != replay_chain {
        return Err("replay event chain mismatch".into());
    }
    s.isolation_post_run_passed = true;
    s.evidence_verified = true;
    s.secondary_reason = None;
    let mut incomplete_window = s.monotonic_elapsed_seconds < h.expected_minimum_duration_seconds;
    s.passed = s.unsigned
        && s.planned_only
        && !s.submission_capable
        && !s.api_wallet_present
        && s.candidate_count == h.candidate_count
        && s.monotonic_elapsed_seconds >= h.expected_minimum_duration_seconds
        && s.process_restarts == 0
        && s.risk_invariant_violations == 0
        && s.mutation_requests == 0
        && s.key_file_opens == 0
        && s.closed_shadow_episodes >= 1
        && s.closed_source_episodes >= 1
        && s.event_chain_verified
        && s.all_candidates_scheduled
        && (h.stage == "HL1-PROFITABILITY-4H" || s.unreconciled_shadow_intents == 0)
        && s.isolation_pre_run_passed
        && s.isolation_post_run_passed
        && s.evidence_verified;
    if h.stage == "HL1-PROFITABILITY-4H" {
        let gate: serde_json::Value = serde_json::from_slice(&std::fs::read(
            bundle.join("profitability-gate-summary.json"),
        )?)?;
        s.profitability_gate_passed =
            gate.get("passed").and_then(serde_json::Value::as_bool) == Some(true);
        s.operational_gate_passed = gate
            .get("operational_gate_passed")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        s.accounting_gate_passed = gate
            .get("accounting_gate_passed")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        s.profitability_signal_positive = gate
            .get("profitability_signal_positive")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        s.sharpe_target_proven = gate
            .get("sharpe_target_proven")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        incomplete_window |= gate
            .get("five_minute_bucket_count")
            .and_then(serde_json::Value::as_u64)
            != Some(48);
        s.passed = s.passed
            && s.profitability_gate_passed
            && gate
                .get("engineering_measurement_complete")
                .and_then(serde_json::Value::as_bool)
                == Some(true);
    } else if h.stage == "SU5R-MICRO-DENSITY" {
        let gate: serde_json::Value = serde_json::from_slice(&std::fs::read(
            bundle.join("micro-density-gate-summary.json"),
        )?)?;
        let micro_passed = gate.get("passed").and_then(serde_json::Value::as_bool) == Some(true);
        s.operational_gate_passed = micro_passed;
        s.accounting_gate_passed = gate
            .get("closed_portfolio_episodes")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count >= 2);
        s.profitability_gate_passed = false;
        s.profitability_signal_positive = false;
        s.sharpe_target_proven = false;
        s.passed = s.passed && micro_passed;
    }
    s.failure_reason = if incomplete_window {
        Some("incomplete_interrupted_window".into())
    } else if s.passed {
        None
    } else if h.stage == "SU5R-MICRO-DENSITY" {
        Some("micro_density_gate_failed".into())
    } else if !s.profitability_gate_passed && h.stage == "HL1-PROFITABILITY-4H" {
        Some("profitability_gate_failed".into())
    } else {
        Some("qualification_gate_failed".into())
    };
    let mut bytes = serde_json::to_vec_pretty(&s)?;
    bytes.push(b'\n');
    std::fs::write(bundle.join("terminal-summary.json"), bytes)?;
    std::fs::copy(post_report, bundle.join("isolation-check-report-post"))?;
    Ok(s)
}

fn enqueue<C: Clock>(
    scheduler: &RequestScheduler<C>,
    config: &ReadOnlySchedulerConfig,
    subject: RequestSubject,
    kind: ReadRequestKind,
    priority: RequestPriority,
    source_tier: Option<SourceTier>,
    now: u64,
    lifetime: u64,
    counters: &mut Counters,
) -> Result<(), Box<dyn Error>> {
    let request = config.api.request(
        RequestKey { subject, kind },
        priority,
        if kind.uses_source_polling_budget() {
            BudgetClass::SourcePolling
        } else {
            BudgetClass::Normal
        },
        source_tier,
        now,
        now,
        now.checked_add(lifetime).ok_or("expiry overflow")?,
        0,
    )?;
    match scheduler.schedule(request) {
        ScheduleOutcome::Enqueued
        | ScheduleOutcome::ReplacedOlder
        | ScheduleOutcome::SuccessorRecorded
        | ScheduleOutcome::RejectedOlder => Ok(()),
        ScheduleOutcome::ShedReplaceable => {
            counters.replaceable_schedule_shed += 1;
            Ok(())
        }
        other => {
            counters.schedule_rejections += 1;
            Err(format!("scheduler rejection: {other:?}").into())
        }
    }
}

fn accepted_stream_response(
    payload: crate::public_mainnet::PublicPayload,
    subject: String,
    request_kind: ReadRequestKind,
    source_tier: Option<SourceTier>,
    now: Timestamp,
    validity_ms: u64,
) -> Result<crate::public_mainnet::AcceptedPublicResponse, Box<dyn Error>> {
    Ok(crate::public_mainnet::AcceptedPublicResponse {
        request_kind,
        source_tier,
        subject,
        requested_at_mono: now,
        received_at_mono: now,
        valid_until_mono: now
            .checked_add(validity_ms)
            .ok_or("stream validity overflow")?,
        payload,
    })
}

fn append_freshness_decision(
    evidence: &mut RuntimeEvidence,
    request: &ScheduledReadRequest,
    outcome: ExecutionOutcome,
    observed_at_mono: u64,
) -> Result<(), Box<dyn Error>> {
    evidence.append_replay_event(
        observed_at_mono,
        ReplayPayload::FreshnessDecision {
            request_kind: request.kind,
            subject: format!("{:?}", request.key.subject),
            requested_at_mono: request.created_at,
            expires_at_mono: request.expires_at,
            attempt: request.attempt,
            outcome: format!("{outcome:?}"),
        },
    )?;
    Ok(())
}

fn verify_isolation_report(path: &Path) -> Result<(), Box<dyn Error>> {
    if !std::fs::read_to_string(path)?
        .lines()
        .any(|line| line.trim() == "HL1C isolation policy passed")
    {
        return Err("valid HL1C isolation report required".into());
    }
    Ok(())
}

fn record_latency(evidence: &mut LatencyEvidence, value_ms: u64) {
    evidence.samples = evidence.samples.saturating_add(1);
    evidence.total_ms = evidence.total_ms.saturating_add(value_ms);
    evidence.maximum_ms = evidence.maximum_ms.max(value_ms);
}

fn sha256_json(value: &impl Serialize) -> Result<String, Box<dyn Error>> {
    let digest = Sha256::digest(serde_json::to_vec(value)?);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn coverage_evidence(
    config: &CopyTradeConfig,
    tiers: &BTreeMap<String, SourceTier>,
    engine: &LiveShadowEngine,
    now: u64,
) -> (
    BTreeMap<SourceTier, u64>,
    BTreeMap<SourceTier, u64>,
    BTreeMap<SourceTier, u64>,
) {
    let mut fresh = BTreeMap::new();
    let mut expired = BTreeMap::new();
    let mut never = BTreeMap::new();
    for candidate in &config.candidates {
        let id = candidate.address.to_ascii_lowercase();
        let tier = tiers.get(&id).copied().unwrap_or(SourceTier::Inactive);
        if engine.source_is_fresh(&id, now) {
            *fresh.entry(tier).or_default() += 1;
        } else if engine.source_ever_accepted(&id) {
            *expired.entry(tier).or_default() += 1;
        } else {
            *never.entry(tier).or_default() += 1;
        }
    }
    (fresh, expired, never)
}
pub fn parse_duration_seconds(value: &str) -> Result<u64, Box<dyn Error>> {
    let (n, m) = if let Some(v) = value.strip_suffix('h') {
        (v, 3600)
    } else if let Some(v) = value.strip_suffix('s') {
        (v, 1)
    } else if let Some(v) = value.strip_suffix('m') {
        (v, 60)
    } else {
        return Err("duration requires h or s suffix".into());
    };
    n.parse::<u64>()?
        .checked_mul(m)
        .ok_or_else(|| "duration overflow".into())
}

#[cfg(feature = "research-cli")]
#[derive(Debug, Serialize)]
struct TransportGateSummary {
    passed: bool,
    all_candidates_accepted: bool,
    effective_candidate_count: usize,
    active_candidate_count: usize,
    inactive_candidate_count: usize,
    active_cadence_ms: u64,
    inactive_cadence_ms: u64,
    source_call_capacity_per_window: u64,
    active_dispatch_per_minute: f64,
    inactive_dispatch_per_minute: f64,
    combined_dispatch_per_minute: f64,
    active_dispatch_count: u64,
    inactive_dispatch_count: u64,
    required_active_dispatch_count: u64,
    required_inactive_dispatch_count: u64,
    fresh_candidates: usize,
    pending_queue: usize,
    maximum_in_flight: usize,
    reserve_consumed: u64,
    http_429s: u64,
    request_expired_in_queue: u64,
    oldest_pending_age_ms: u64,
}

#[cfg(feature = "research-cli")]
#[derive(Debug, Serialize)]
struct ProfitabilityGateSummary {
    passed: bool,
    operational_gate_passed: bool,
    accounting_gate_passed: bool,
    engineering_measurement_complete: bool,
    profitability_signal_positive: bool,
    sharpe_target_proven: bool,
    monotonic_elapsed_seconds: u64,
    all_candidates_scheduled: bool,
    all_candidates_fresh_at_end: bool,
    risk_invariant_violations: u64,
    mutation_requests: u64,
    key_file_opens: u64,
    protected_reserve_consumed: u64,
    http_429s: u64,
    shadow_executions: usize,
    closed_portfolio_episodes: usize,
    closed_source_episodes: usize,
    five_minute_bucket_count: usize,
    root_actionable_targets: usize,
    root_conservation_verified: bool,
    unresolved_actionable_targets: usize,
    minimum_execution_attempts_met: bool,
    terminal_equity_matches_final_bucket: bool,
}

#[cfg(feature = "research-cli")]
#[derive(Debug, Serialize)]
struct MicroDensityGateSummary {
    passed: bool,
    monotonic_elapsed_seconds: u64,
    all_candidates_scheduled: bool,
    all_candidates_fresh_at_end: bool,
    risk_invariant_violations: u64,
    mutation_requests: u64,
    key_file_opens: u64,
    protected_reserve_consumed: u64,
    http_429s: u64,
    raw_nonzero_target_changes: u64,
    target_changes_above_dynamic_minimum: u64,
    admitted_new_positions: u64,
    exits: u64,
    rotations: u64,
    maximum_admitted_micro_slots: usize,
    independent_root_actions: usize,
    closed_portfolio_episodes: usize,
    root_conservation_verified: bool,
    unresolved_actionable_targets: usize,
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "research-cli")]
fn evaluate_micro_density_gate(
    density: &crate::live_shadow::ExecutableDensitySummary,
    elapsed_seconds: u64,
    all_candidates_scheduled: bool,
    all_candidates_fresh_at_end: bool,
    risk_invariant_violations: u64,
    mutation_requests: u64,
    key_file_opens: u64,
    protected_reserve_consumed: u64,
    http_429s: u64,
) -> MicroDensityGateSummary {
    let passed = elapsed_seconds >= 600
        && all_candidates_scheduled
        && all_candidates_fresh_at_end
        && risk_invariant_violations == 0
        && mutation_requests == 0
        && key_file_opens == 0
        && protected_reserve_consumed == 0
        && http_429s == 0
        && density.maximum_admitted_micro_slots <= 7
        && density.admitted_new_positions >= 2
        && density.exits >= 2
        && density.root_actionable_targets >= 4
        && density.closed_portfolio_episodes >= 2
        && density.root_conservation_verified
        && density.unresolved_actionable_targets == 0;
    MicroDensityGateSummary {
        passed,
        monotonic_elapsed_seconds: elapsed_seconds,
        all_candidates_scheduled,
        all_candidates_fresh_at_end,
        risk_invariant_violations,
        mutation_requests,
        key_file_opens,
        protected_reserve_consumed,
        http_429s,
        raw_nonzero_target_changes: density.raw_nonzero_target_changes,
        target_changes_above_dynamic_minimum: density.target_changes_above_dynamic_minimum,
        admitted_new_positions: density.admitted_new_positions,
        exits: density.exits,
        rotations: density.rotations,
        maximum_admitted_micro_slots: density.maximum_admitted_micro_slots,
        independent_root_actions: density.root_actionable_targets,
        closed_portfolio_episodes: density.closed_portfolio_episodes,
        root_conservation_verified: density.root_conservation_verified,
        unresolved_actionable_targets: density.unresolved_actionable_targets,
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "research-cli")]
fn evaluate_profitability_gate(
    profitability: &ProfitabilitySummary,
    density: &crate::live_shadow::ExecutableDensitySummary,
    elapsed_seconds: u64,
    all_candidates_scheduled: bool,
    all_candidates_fresh_at_end: bool,
    risk_invariant_violations: u64,
    mutation_requests: u64,
    key_file_opens: u64,
    protected_reserve_consumed: u64,
    http_429s: u64,
) -> ProfitabilityGateSummary {
    let minimum_execution_attempts_met = profitability.shadow_executions >= 3;
    let terminal_equity_matches_final_bucket =
        profitability.equity_change_including_open_positions == density.net_pnl;
    let operational_gate_passed = elapsed_seconds >= 14_400
        && all_candidates_scheduled
        && all_candidates_fresh_at_end
        && risk_invariant_violations == 0
        && mutation_requests == 0
        && key_file_opens == 0
        && protected_reserve_consumed == 0
        && http_429s == 0
        && density.root_conservation_verified
        && minimum_execution_attempts_met;
    let accounting_gate_passed = profitability.engineering_measurement_complete
        && profitability.five_minute_bucket_count == 48
        && terminal_equity_matches_final_bucket;
    let passed = operational_gate_passed && accounting_gate_passed;
    ProfitabilityGateSummary {
        passed,
        operational_gate_passed,
        accounting_gate_passed,
        engineering_measurement_complete: profitability.engineering_measurement_complete,
        profitability_signal_positive: profitability.positive_edge_signal,
        sharpe_target_proven: false,
        monotonic_elapsed_seconds: elapsed_seconds,
        all_candidates_scheduled,
        all_candidates_fresh_at_end,
        risk_invariant_violations,
        mutation_requests,
        key_file_opens,
        protected_reserve_consumed,
        http_429s,
        shadow_executions: profitability.shadow_executions,
        closed_portfolio_episodes: profitability.closed_portfolio_episodes,
        closed_source_episodes: profitability.closed_source_episodes,
        five_minute_bucket_count: profitability.five_minute_bucket_count,
        root_actionable_targets: density.root_actionable_targets,
        root_conservation_verified: density.root_conservation_verified,
        unresolved_actionable_targets: density.unresolved_actionable_targets,
        minimum_execution_attempts_met,
        terminal_equity_matches_final_bucket,
    }
}

#[cfg(feature = "research-cli")]
fn evaluate_transport_gate(
    bundle: &Path,
    config: &CopyTradeConfig,
    source_dispatch_plan: SourceDispatchPlan,
    tiers: &BTreeMap<String, SourceTier>,
    engine: &LiveShadowEngine,
    health: &copytrade_core::scheduler::SchedulerHealth,
    counters: &Counters,
    rate_limited: u64,
) -> Result<TransportGateSummary, Box<dyn Error>> {
    if config.candidates.len() != source_dispatch_plan.effective_candidate_count {
        return Err("transport gate source dispatch plan cardinality mismatch".into());
    }
    let active_tier_count = tiers
        .values()
        .filter(|tier| **tier == SourceTier::Active)
        .count();
    let inactive_tier_count = tiers
        .values()
        .filter(|tier| **tier == SourceTier::Inactive)
        .count();
    let tier_cardinality_matches = active_tier_count == source_dispatch_plan.active_candidate_count
        && inactive_tier_count == source_dispatch_plan.inactive_candidate_count;
    let lines = std::fs::read_to_string(bundle.join("periodic-checkpoints.jsonl"))?;
    let checkpoints = lines
        .lines()
        .map(|line| serde_json::from_str::<crate::qualification_evidence::ChainedCheckpoint>(line))
        .collect::<Result<Vec<_>, _>>()?;
    let first = checkpoints
        .iter()
        .find(|checkpoint| checkpoint.payload.monotonic_elapsed_ms >= 120_000)
        .ok_or("missing steady-state checkpoint")?;
    let last = checkpoints.last().ok_or("missing terminal checkpoint")?;
    let minutes = (last
        .payload
        .monotonic_elapsed_ms
        .saturating_sub(first.payload.monotonic_elapsed_ms) as f64
        / 60_000.0)
        .max(1.0 / 60.0);
    let count = |map: &BTreeMap<SourceTier, u64>, tier| map.get(&tier).copied().unwrap_or(0);
    let active_count = count(&last.payload.dispatch_count_by_tier, SourceTier::Active)
        - count(&first.payload.dispatch_count_by_tier, SourceTier::Active);
    let inactive_count = count(&last.payload.dispatch_count_by_tier, SourceTier::Inactive)
        - count(&first.payload.dispatch_count_by_tier, SourceTier::Inactive);
    let active = active_count as f64 / minutes;
    let inactive = inactive_count as f64 / minutes;
    // Cadence is a discrete event invariant. Flooring the fractional event at
    // an arbitrary checkpoint boundary avoids treating sub-second checkpoint
    // jitter as a missing request while never crediting an event not observed.
    let elapsed_ms = last
        .payload
        .monotonic_elapsed_ms
        .saturating_sub(first.payload.monotonic_elapsed_ms);
    let required_active = required_dispatch_count(
        source_dispatch_plan.active_candidate_count,
        elapsed_ms,
        source_dispatch_plan.active_cadence_ms,
    )?;
    let required_inactive = required_dispatch_count(
        source_dispatch_plan.inactive_candidate_count,
        elapsed_ms,
        source_dispatch_plan.inactive_cadence_ms,
    )?;
    let required_combined = required_active.saturating_add(required_inactive);
    let all = config
        .candidates
        .iter()
        .all(|candidate| engine.source_ever_accepted(&candidate.address));
    let fresh = config
        .candidates
        .iter()
        .filter(|candidate| {
            engine.source_is_fresh(&candidate.address, last.payload.monotonic_elapsed_ms)
        })
        .count();
    let expired = health.expired_in_queue_by_tier.values().sum();
    let oldest = health
        .oldest_pending_age_ms_by_tier
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let passed = tier_cardinality_matches
        && all
        && active_count >= required_active
        && inactive_count >= required_inactive
        && active_count.saturating_add(inactive_count) >= required_combined
        && fresh == config.candidates.len()
        && health.pending <= health.queue_capacity.min(32)
        && health.in_flight <= counters.maximum_in_flight
        && counters.reserved_weight == 0
        && rate_limited == 0
        && expired == 0
        && oldest < 5_000;
    Ok(TransportGateSummary {
        passed,
        all_candidates_accepted: all,
        effective_candidate_count: source_dispatch_plan.effective_candidate_count,
        active_candidate_count: source_dispatch_plan.active_candidate_count,
        inactive_candidate_count: source_dispatch_plan.inactive_candidate_count,
        active_cadence_ms: source_dispatch_plan.active_cadence_ms,
        inactive_cadence_ms: source_dispatch_plan.inactive_cadence_ms,
        source_call_capacity_per_window: source_dispatch_plan.source_call_capacity_per_window,
        active_dispatch_per_minute: active,
        inactive_dispatch_per_minute: inactive,
        combined_dispatch_per_minute: active + inactive,
        active_dispatch_count: active_count,
        inactive_dispatch_count: inactive_count,
        required_active_dispatch_count: required_active,
        required_inactive_dispatch_count: required_inactive,
        fresh_candidates: fresh,
        pending_queue: health.pending,
        maximum_in_flight: counters.maximum_in_flight,
        reserve_consumed: counters.reserved_weight,
        http_429s: rate_limited,
        request_expired_in_queue: expired,
        oldest_pending_age_ms: oldest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::scheduler::ManualClock;

    fn markets(prefix: &str, count: usize) -> BTreeSet<String> {
        (0..count)
            .map(|index| format!("{prefix}{index:03}"))
            .collect()
    }

    #[test]
    fn history_recovery_splits_saturated_ranges_before_wallet_completion() {
        let wallet = "0x1111111111111111111111111111111111111111".to_string();
        let mut recovery = HistoryRecovery::default();
        assert!(recovery.record_snapshot(
            100,
            SourceStateResponse {
                candidate_id: wallet.clone(),
                account_value: Decimal::ONE,
                source_time_ms: 200,
                positions: BTreeMap::new(),
                closed_candles: Vec::new(),
            },
        ));
        let mut range = SourceFillRangeResponse {
            wallet: wallet.clone(),
            start_time_ms: 99,
            end_time_ms: 200,
            raw_count: 2_000,
            fills: Vec::new(),
        };
        assert!(matches!(
            recovery.record_range(&mut range),
            FillRangeProgress::Next
        ));
        for expected in [FillRangeProgress::Next, FillRangeProgress::WalletComplete] {
            let (start_time_ms, end_time_ms) = recovery.next_range(&wallet).unwrap();
            let mut leaf = SourceFillRangeResponse {
                wallet: wallet.clone(),
                start_time_ms,
                end_time_ms,
                raw_count: 0,
                fills: Vec::new(),
            };
            assert!(recovery.record_range(&mut leaf) == expected);
        }
        assert_eq!(recovery.cutoff(), Some(200));
    }

    #[test]
    fn stale_or_superseded_fill_ranges_do_not_mutate_current_recovery() {
        let wallet = "0x1111111111111111111111111111111111111111".to_string();
        let mut recovery = HistoryRecovery::default();
        let state = |source_time_ms| SourceStateResponse {
            candidate_id: wallet.clone(),
            account_value: Decimal::ONE,
            source_time_ms,
            positions: BTreeMap::new(),
            closed_candles: Vec::new(),
        };
        assert!(recovery.record_snapshot(100, state(200)));
        let mut unknown = SourceFillRangeResponse {
            wallet: "0x2222222222222222222222222222222222222222".into(),
            start_time_ms: 99,
            end_time_ms: 200,
            raw_count: 0,
            fills: Vec::new(),
        };
        assert_eq!(
            recovery.record_range(&mut unknown),
            FillRangeProgress::Stale
        );
        let mut superseded = SourceFillRangeResponse {
            wallet: wallet.clone(),
            start_time_ms: 99,
            end_time_ms: 200,
            raw_count: 0,
            fills: Vec::new(),
        };

        assert!(recovery.record_snapshot(150, state(250)));
        assert_eq!(
            recovery.record_range(&mut superseded),
            FillRangeProgress::Stale
        );
        assert_eq!(recovery.next_range(&wallet), Some((149, 250)));

        let mut current = SourceFillRangeResponse {
            wallet: wallet.clone(),
            start_time_ms: 149,
            end_time_ms: 250,
            raw_count: 0,
            fills: Vec::new(),
        };
        assert_eq!(
            recovery.record_range(&mut current),
            FillRangeProgress::WalletComplete
        );
        assert_eq!(
            recovery.record_range(&mut current),
            FillRangeProgress::Stale
        );
    }

    #[test]
    fn hot_book_selection_bounds_capacity_and_omits_malformed_markets() {
        let mut required = markets("M", MAX_HOT_BOOKS + 1);
        required.insert("bad market".into());
        let history = required.iter().cloned().map(|asset| (asset, 10)).collect();

        let selected = select_hot_books(&BTreeSet::new(), &required, &history, &BTreeSet::new());

        assert_eq!(selected.len(), MAX_HOT_BOOKS);
        assert!(!selected.contains("bad market"));
        assert!(!selected.contains("M128"));
    }

    #[test]
    fn urgent_book_displaces_lower_priority_demand_at_capacity() {
        let required = markets("M", MAX_HOT_BOOKS);
        let subscribed = required.clone();
        let history = required.iter().cloned().map(|asset| (asset, 10)).collect();
        let urgent = BTreeSet::from(["URGENT".to_string()]);

        let selected = select_hot_books(&urgent, &required, &history, &subscribed);

        assert_eq!(selected.len(), MAX_HOT_BOOKS);
        assert!(selected.contains("URGENT"));
        assert_eq!(selected.intersection(&required).count(), MAX_HOT_BOOKS - 1);
    }

    #[test]
    fn pending_book_receives_slot_after_demand_and_grace_expire() {
        let required = markets("M", MAX_HOT_BOOKS + 1);
        let mut history = required
            .iter()
            .cloned()
            .map(|asset| (asset, 10))
            .collect::<BTreeMap<_, _>>();
        let first = select_hot_books(&BTreeSet::new(), &required, &history, &BTreeSet::new());
        assert!(!first.contains("M128"));

        let mut reduced = required;
        reduced.remove("M000");
        history.remove("M000");
        let second = select_hot_books(&BTreeSet::new(), &reduced, &history, &first);

        assert_eq!(second.len(), MAX_HOT_BOOKS);
        assert!(!second.contains("M000"));
        assert!(second.contains("M128"));
    }

    fn source_policies() -> (ReadOnlySchedulerConfig, PublicTransportPolicy) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut scheduler =
            ReadOnlySchedulerConfig::from_path(root.join("config/read-api-policy.json")).unwrap();
        scheduler.api.total_weight_per_window = 1_200;
        scheduler.api.reserved_weight_per_window = 360;
        scheduler.api.source_polling_weight_per_window = 750;
        scheduler
            .api
            .endpoint_weights
            .insert(ReadRequestKind::ExpandedSourceState, 4);
        let mut transport: PublicTransportPolicy = serde_json::from_slice(
            &std::fs::read(root.join("config/public-mainnet-transport.json")).unwrap(),
        )
        .unwrap();
        transport.perp_dexes.clear();
        transport.streaming.enabled = false;
        (scheduler, transport)
    }

    #[test]
    fn duration_is_exact() {
        assert_eq!(parse_duration_seconds("24h").unwrap(), 86_400);
        assert!(parse_duration_seconds("24").is_err());
    }

    #[test]
    fn unsigned_runner_accepts_the_bound_production_manifest_stage() {
        assert!(manifest_stage_matches(false, "HL1J"));
        assert!(manifest_stage_matches(false, "PRODUCTION_RELEASE"));
        assert!(manifest_stage_matches(true, "PRODUCTION_RELEASE"));
        assert!(!manifest_stage_matches(true, "HL1J"));
    }

    #[test]
    fn replaceable_refresh_queue_pressure_is_nonfatal_and_counted() {
        let (mut scheduler_config, _) = source_policies();
        scheduler_config.api.queue_capacity = 1;
        let scheduler = RequestScheduler::new(
            copytrade_core::scheduler::ManualClock::new(10),
            scheduler_config.api.clone(),
            scheduler_config.retry.clone(),
        )
        .unwrap();
        let mut counters = Counters::default();

        enqueue(
            &scheduler,
            &scheduler_config,
            RequestSubject::Candidate("candidate-a".to_string()),
            ReadRequestKind::SourceState,
            RequestPriority::Normal,
            Some(SourceTier::Active),
            10,
            40_000,
            &mut counters,
        )
        .unwrap();
        enqueue(
            &scheduler,
            &scheduler_config,
            RequestSubject::Candidate("candidate-b".to_string()),
            ReadRequestKind::SourceState,
            RequestPriority::Low,
            Some(SourceTier::Active),
            11,
            40_000,
            &mut counters,
        )
        .unwrap();

        assert_eq!(counters.schedule_rejections, 0);
        assert_eq!(counters.replaceable_schedule_shed, 1);
        assert_eq!(scheduler.health().pending, 1);
    }

    #[test]
    fn tier_assignment_is_bounded_and_promotions_are_swaps() {
        let ordered = (0..169)
            .map(|n| format!("candidate-{n:03}"))
            .collect::<Vec<_>>();
        let every_candidate_has_a_position = ordered.iter().cloned().collect::<BTreeSet<_>>();
        let tiers = assign_source_tiers(&ordered, &every_candidate_has_a_position, 100);
        assert_eq!(
            tiers
                .values()
                .filter(|tier| **tier == SourceTier::Active)
                .count(),
            100
        );
        assert_eq!(
            tiers
                .values()
                .filter(|tier| **tier == SourceTier::Inactive)
                .count(),
            69
        );

        let late_positions = ordered[100..].iter().cloned().collect::<BTreeSet<_>>();
        let promoted = assign_source_tiers(&ordered, &late_positions, 100);
        assert_eq!(
            promoted
                .values()
                .filter(|tier| **tier == SourceTier::Active)
                .count(),
            100
        );
        assert!(ordered[100..]
            .iter()
            .all(|id| promoted[id] == SourceTier::Active));
        assert_eq!(promoted[&ordered[0]], SourceTier::Active);
        assert_eq!(promoted[&ordered[31]], SourceTier::Inactive);
    }

    #[test]
    fn source_dispatch_plan_derives_gate_counts_from_effective_candidates() {
        let (scheduler, transport) = source_policies();
        let original = derive_source_dispatch_plan(169, &scheduler, &transport).unwrap();
        assert_eq!(original.active_candidate_count, 100);
        assert_eq!(original.inactive_candidate_count, 69);
        assert_eq!(original.active_dispatches_per_window, 300);
        assert_eq!(original.inactive_dispatches_per_window, 69);
        assert_eq!(original.combined_dispatches_per_window, 369);
        assert_eq!(original.source_call_capacity_per_window, 375);

        let smaller = derive_source_dispatch_plan(150, &scheduler, &transport).unwrap();
        assert_eq!(smaller.active_candidate_count, 100);
        assert_eq!(smaller.inactive_candidate_count, 50);
        assert_eq!(smaller.active_dispatches_per_window, 300);
        assert_eq!(smaller.inactive_dispatches_per_window, 50);
        assert_eq!(smaller.combined_dispatches_per_window, 350);
    }

    #[test]
    fn source_dispatch_plan_trades_active_slots_for_expanded_fresh_coverage() {
        let (scheduler, transport) = source_policies();
        let full_active = derive_source_dispatch_plan(175, &scheduler, &transport).unwrap();
        assert_eq!(full_active.active_candidate_count, 100);
        assert_eq!(full_active.inactive_candidate_count, 75);
        assert_eq!(full_active.combined_dispatches_per_window, 375);

        let expanded = derive_source_dispatch_plan(200, &scheduler, &transport).unwrap();
        assert_eq!(expanded.active_candidate_count, 87);
        assert_eq!(expanded.inactive_candidate_count, 113);
        assert_eq!(expanded.active_dispatches_per_window, 261);
        assert_eq!(expanded.inactive_dispatches_per_window, 113);
        assert_eq!(expanded.combined_dispatches_per_window, 374);
        assert_eq!(expanded.active_cadence_ms, 20_000);
        assert_eq!(expanded.inactive_cadence_ms, 60_000);
    }

    #[test]
    fn source_dispatch_plan_fails_closed_beyond_unchanged_source_budget() {
        let (scheduler, transport) = source_policies();
        let boundary = derive_source_dispatch_plan(375, &scheduler, &transport).unwrap();
        assert_eq!(boundary.active_candidate_count, 0);
        assert_eq!(boundary.inactive_candidate_count, 375);
        assert_eq!(boundary.combined_dispatches_per_window, 375);
        assert!(derive_source_dispatch_plan(376, &scheduler, &transport).is_err());
    }

    #[test]
    fn expanded_xyz_sources_are_fully_weighted_and_remain_at_safe_cadence() {
        let (mut scheduler, mut transport) = source_policies();
        scheduler.api.reserved_weight_per_window = 120;
        scheduler.api.source_polling_weight_per_window = 920;
        transport.perp_dexes = vec!["xyz".into()];
        let plan = derive_source_dispatch_plan_with_expansion(
            375,
            80,
            transport.perp_dexes.len(),
            &scheduler,
            &transport,
        )
        .unwrap();
        assert_eq!(plan.active_candidate_count, 0);
        assert_eq!(plan.inactive_candidate_count, 375);
        assert_eq!(plan.combined_dispatches_per_window, 375);
        let request = scheduler
            .api
            .request(
                RequestKey {
                    subject: RequestSubject::Candidate("expanded".into()),
                    kind: ReadRequestKind::ExpandedSourceState,
                },
                RequestPriority::High,
                BudgetClass::SourcePolling,
                Some(SourceTier::Inactive),
                0,
                0,
                75_000,
                0,
            )
            .unwrap();
        assert_eq!(request.weight, 4);
        let request_scheduler = RequestScheduler::new(
            ManualClock::new(0),
            scheduler.api.clone(),
            scheduler.retry.clone(),
        )
        .unwrap();
        assert_eq!(
            request_scheduler.schedule(request),
            ScheduleOutcome::Enqueued
        );
        assert!(derive_source_dispatch_plan_with_expansion(
            375,
            86,
            transport.perp_dexes.len(),
            &scheduler,
            &transport,
        )
        .is_err());
    }

    #[test]
    fn streaming_reconciliation_is_weighted_below_the_public_rest_ceiling() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let scheduler =
            ReadOnlySchedulerConfig::from_path(root.join("config/read-api-policy.json")).unwrap();
        let transport: PublicTransportPolicy = serde_json::from_slice(
            &std::fs::read(root.join("config/public-mainnet-transport.json")).unwrap(),
        )
        .unwrap();
        let plan = streaming_source_dispatch_plan(375, &scheduler, &transport).unwrap();
        assert_eq!(scheduler.api.total_weight_per_window, 900);
        assert_eq!(
            scheduler.api.endpoint_weights[&ReadRequestKind::ExchangeMetadata],
            60
        );
        assert_eq!(
            scheduler.api.endpoint_weights[&ReadRequestKind::ExpandedSourceState],
            42
        );
        assert_eq!(plan.source_call_capacity_per_window, 16);
        assert_eq!(plan.active_candidate_count, 0);
        assert_eq!(plan.inactive_candidate_count, 375);
        let minimum_sweep_ms = 375_u64
            .div_ceil(plan.source_call_capacity_per_window)
            .checked_mul(scheduler.api.window_ms)
            .unwrap();
        assert!(minimum_sweep_ms <= transport.streaming.reconciliation_spread_ms);
    }

    #[test]
    fn exhausted_or_stale_source_recovery_is_rearmed_while_uncertainty_remains() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let scheduler =
            ReadOnlySchedulerConfig::from_path(root.join("config/read-api-policy.json")).unwrap();
        let candidate = "0x0000000000000000000000000000000000000001".to_string();
        let request = scheduler
            .api
            .request(
                RequestKey {
                    subject: RequestSubject::Candidate(candidate.clone()),
                    kind: ReadRequestKind::ExpandedSourceState,
                },
                RequestPriority::Critical,
                BudgetClass::SourcePolling,
                Some(SourceTier::Inactive),
                10,
                10,
                10_000,
                0,
            )
            .unwrap();
        let remaining = BTreeSet::from([candidate.clone()]);
        let mut due = BTreeMap::from([(candidate.clone(), u64::MAX)]);

        assert!(rearm_unresolved_source_request(
            &request,
            ExecutionOutcome::RetryExhausted,
            100,
            30_000,
            &remaining,
            &BTreeSet::new(),
            &mut due,
        )
        .unwrap());
        assert_eq!(due[&candidate], 30_100);

        due.insert(candidate.clone(), u64::MAX);
        assert!(rearm_unresolved_source_request(
            &request,
            ExecutionOutcome::CompletedButStale,
            200,
            30_000,
            &remaining,
            &BTreeSet::new(),
            &mut due,
        )
        .unwrap());
        assert_eq!(due[&candidate], 30_200);

        due.insert(candidate.clone(), u64::MAX);
        assert!(!rearm_unresolved_source_request(
            &request,
            ExecutionOutcome::RetryScheduled,
            300,
            30_000,
            &remaining,
            &BTreeSet::new(),
            &mut due,
        )
        .unwrap());
        assert_eq!(due[&candidate], u64::MAX);

        assert!(!rearm_unresolved_source_request(
            &request,
            ExecutionOutcome::CompletedFresh,
            400,
            30_000,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &mut due,
        )
        .unwrap());
    }

    #[test]
    fn idle_stale_reconciliation_rearms_a_bounded_sweep() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let scheduler =
            ReadOnlySchedulerConfig::from_path(root.join("config/read-api-policy.json")).unwrap();
        let transport: PublicTransportPolicy = serde_json::from_slice(
            &std::fs::read(root.join("config/public-mainnet-transport.json")).unwrap(),
        )
        .unwrap();
        let plan = streaming_source_dispatch_plan(config.candidates.len(), &scheduler, &transport)
            .unwrap();
        let unresolved = config
            .candidates
            .iter()
            .take(2)
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let mut due = config
            .candidates
            .iter()
            .map(|candidate| (candidate.address.to_ascii_lowercase(), u64::MAX))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(reconciliation_pending_count(&unresolved, &unresolved), 2);
        assert!(rearm_idle_stream_reconciliation(
            &config,
            plan,
            50_000,
            true,
            &unresolved,
            &BTreeSet::new(),
            &mut due,
        )
        .unwrap());
        assert!(unresolved
            .iter()
            .all(|candidate| due[candidate] != u64::MAX));
        assert!(!rearm_idle_stream_reconciliation(
            &config,
            plan,
            60_000,
            true,
            &unresolved,
            &BTreeSet::new(),
            &mut due,
        )
        .unwrap());
    }

    #[test]
    fn source_dispatch_plan_uses_policy_weight_capacity() {
        let (mut scheduler, transport) = source_policies();
        scheduler
            .api
            .endpoint_weights
            .insert(ReadRequestKind::SourceState, 3);
        let plan = derive_source_dispatch_plan(169, &scheduler, &transport).unwrap();
        assert_eq!(plan.source_call_capacity_per_window, 250);
        assert_eq!(plan.active_candidate_count, 40);
        assert_eq!(plan.inactive_candidate_count, 129);
        assert_eq!(plan.combined_dispatches_per_window, 249);
    }

    #[test]
    fn transport_gate_dispatch_requirements_follow_the_derived_plan() {
        let (scheduler, transport) = source_policies();
        let elapsed_ms = 8 * 60_000;
        let original = derive_source_dispatch_plan(169, &scheduler, &transport).unwrap();
        assert_eq!(
            required_dispatch_count(
                original.active_candidate_count,
                elapsed_ms,
                original.active_cadence_ms,
            )
            .unwrap(),
            2_400
        );
        assert_eq!(
            required_dispatch_count(
                original.inactive_candidate_count,
                elapsed_ms,
                original.inactive_cadence_ms,
            )
            .unwrap(),
            552
        );

        let expanded = derive_source_dispatch_plan(200, &scheduler, &transport).unwrap();
        let required_active = required_dispatch_count(
            expanded.active_candidate_count,
            elapsed_ms,
            expanded.active_cadence_ms,
        )
        .unwrap();
        let required_inactive = required_dispatch_count(
            expanded.inactive_candidate_count,
            elapsed_ms,
            expanded.inactive_cadence_ms,
        )
        .unwrap();
        assert_eq!(required_active, 2_088);
        assert_eq!(required_inactive, 904);
        assert_eq!(required_active + required_inactive, 2_992);
    }

    #[test]
    fn cadence_deadlines_do_not_drift_with_loop_latency() {
        assert_eq!(next_cadence_deadline(0, 37, 20_000).unwrap(), 20_000);
        assert_eq!(
            next_cadence_deadline(20_000, 20_037, 20_000).unwrap(),
            40_000
        );
        assert_eq!(
            next_cadence_deadline(20_000, 65_000, 20_000).unwrap(),
            80_000
        );
    }

    #[test]
    fn phase_staggering_spreads_work_across_the_interval() {
        assert_eq!(phase_deadline(1_000, 20_000, 0, 100).unwrap(), 1_000);
        assert_eq!(phase_deadline(1_000, 20_000, 50, 100).unwrap(), 11_000);
        assert_eq!(phase_deadline(1_000, 20_000, 99, 100).unwrap(), 20_800);
        assert!(phase_deadline(0, 20_000, 0, 0).is_err());
    }
}
