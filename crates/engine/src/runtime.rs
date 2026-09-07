use crate::decision::{
    DecisionEngine, EconomicAttribution, Hip3PipelineStatus, ProductionIntentIdentity,
    StateIdentity, UnresolvedRootStatus,
};
use crate::domain::decision::{derive_config_hash, derive_risk_policy_hash};
use crate::domain::scheduler::{
    BudgetClass, Clock, ExecutionOutcome, MonotonicClock, ReadOnlyDataSource,
    ReadOnlySchedulerConfig, ReadRequestKind, RequestKey, RequestPriority, RequestScheduler,
    RequestSubject, ScheduleOutcome, ScheduledReadRequest, SchedulerHealth, SourceTier, Timestamp,
};
use crate::domain::CopyTradeConfig;
use crate::execution::{
    recoverable_reconciliation_error, LiveExecutionRuntime, LiveExecutionSettings,
    LiveExecutionUpdate,
};
use crate::public_mainnet::{
    HyperliquidPublicTransport, PublicTransportPolicy, SourceStateResponse,
};
use crate::source_state::{
    import_source_backfill, SourceBackfillOutcome, SourceContinuityStatus, SourceStateStore,
};
use crate::state_root::{StateRootStartup, UnsignedStateRoot};
use crate::streaming::{
    parse_asset_contexts, valid_market, MarketDirectory, StreamingEvent, StreamingHandle,
    StreamingSourceBook, MAX_HOT_BOOKS,
};
use crate::EngineState;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
const MAX_ACTIVE_SOURCE_CANDIDATES: usize = 100;
fn select_hot_books(
    urgent: &BTreeSet<String>,
    required: &BTreeSet<String>,
    last_required: &BTreeMap<String, Timestamp>,
    subscribed: &BTreeSet<String>,
) -> BTreeSet<String> {
    let assets = urgent
        .iter()
        .chain(required)
        .chain(last_required.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut ranked = assets
        .into_iter()
        .filter(|asset| valid_market(asset))
        .collect::<Vec<_>>();
    ranked.sort_by_key(|asset| {
        let tier = if urgent.contains(asset) {
            0
        } else if required.contains(asset) {
            1
        } else {
            2
        };
        (
            tier,
            std::cmp::Reverse(if tier == 2 {
                last_required.get(asset).copied().unwrap_or_default()
            } else {
                0
            }),
            !subscribed.contains(asset),
            asset.clone(),
        )
    });
    ranked.into_iter().take(MAX_HOT_BOOKS).collect()
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
pub struct RuntimeOptions {
    pub config_path: PathBuf,
    pub very_profitable_layer_path: Option<PathBuf>,
    pub request_policy_path: PathBuf,
    pub transport_policy_path: PathBuf,
    pub output: PathBuf,
    pub state_root: Option<PathBuf>,
    pub source_backfill_from: Option<PathBuf>,
}
#[derive(Debug, Clone, Default, Serialize)]
struct LatencySummary {
    samples: u64,
    total_ms: u64,
    maximum_ms: u64,
}
fn sha256_file(path: impl AsRef<Path>) -> Result<String, Box<dyn Error>> {
    Ok(Sha256::digest(std::fs::read(path)?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
fn persist_unsigned_state(
    engine: &mut DecisionEngine,
    state_root: &mut Option<UnsignedStateRoot>,
) -> Result<(), Box<dyn Error>> {
    if let Some(state_root) = state_root {
        state_root.persist(engine)?;
    }
    Ok(())
}
fn persist_source_state(
    store: &mut Option<SourceStateStore>,
    state: &SourceStateResponse,
    live_confirmed: bool,
    stream_gap: bool,
) -> bool {
    let Some(current) = store.as_mut() else {
        return false;
    };
    if let Err(error) = current.persist(state, live_confirmed, stream_gap) {
        eprintln!("source_persistence_failed=true nonfatal=true error={error}");
        return false;
    }
    true
}
#[derive(Debug, Default, Serialize)]
struct Counters {
    source_history_pages: u64,
    source_persistence_failures: u64,
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
    queue_wait_ms_by_tier: BTreeMap<SourceTier, LatencySummary>,
    transport_latency_ms_by_kind: BTreeMap<ReadRequestKind, LatencySummary>,
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
    execution_state: &'static str,
    strategy_target_execution_enabled: bool,
    exchange_risk_positions: Option<&'a BTreeMap<String, Decimal>>,
    snapshot_generation: Option<u64>,
    candidate_count: usize,
    projection_violations: u64,
    persistence_failures: u64,
    source_persistence_failures: u64,
    source_history_pages: u64,
    unresolved_roots: usize,
    unresolved_root_details: Vec<UnresolvedRootStatus>,
    rules_pending_markets: BTreeSet<String>,
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
    #[serde(flatten)]
    hip3: Hip3PipelineStatus,
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
    durable_baselines: usize,
    live_state_confirmed: usize,
    live_state_recovering: usize,
    source_history_contiguous: usize,
    source_history_catching_up: usize,
    source_history_gapped: usize,
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
pub async fn run_continuous_daemon(options: RuntimeOptions) -> Result<PathBuf, Box<dyn Error>> {
    let live_settings = Some(LiveExecutionSettings::load()?);
    if options.state_root.is_none() {
        return Err("continuous runtime requires --state-root".into());
    }
    let engine_state = EngineState::load_with_very_profitable_layer(
        &options.config_path,
        options.very_profitable_layer_path.as_ref(),
    )?;
    let config = engine_state.config().clone();
    let very_profitable_layer = engine_state.very_profitable_layer().cloned();
    let scheduler_config = ReadOnlySchedulerConfig::from_path(&options.request_policy_path)?;
    let transport_policy: PublicTransportPolicy =
        serde_json::from_slice(&std::fs::read(&options.transport_policy_path)?)?;
    transport_policy
        .validate()
        .map_err(|error| format!("invalid transport policy: {error:?}"))?;
    let streaming_enabled = transport_policy.streaming.enabled;
    if !streaming_enabled {
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
    let output = options.output.clone();
    std::fs::create_dir_all(&output)?;
    let scheduler = RequestScheduler::new(
        clock.clone(),
        scheduler_config.api.clone(),
        scheduler_config.retry.clone(),
    )?;
    let transport = HyperliquidPublicTransport::new(clock.clone(), transport_policy.clone())
        .map_err(|error| format!("transport: {error:?}"))?;
    transport
        .set_fee_user(
            live_settings
                .as_ref()
                .map(|settings| settings.execution_account.as_str())
                .or(config.follower_address.as_deref()),
        )
        .map_err(|error| format!("live fee subject: {error:?}"))?;
    transport
        .set_expanded_source_candidates(expanded_source_candidates.iter().cloned())
        .map_err(|error| format!("expanded source cohort: {error:?}"))?;
    let mut engine = DecisionEngine::new(
        config.clone(),
        binary_sha256.as_bytes(),
        run_id.clone(),
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
    let state_identity = StateIdentity {
        source_tree_sha256: "build-time-only".into(),
        observer_binary_sha256: "build-time-only".into(),
        configuration_sha256: configuration_sha256.clone(),
        risk_policy_sha256: risk_policy_sha256.clone(),
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
                state_root.restore(&mut engine)?;
                restored_state = true;
            }
            StateRootStartup::Initialize => {
                state_root.initialize(&mut engine)?;
            }
        }
    }
    {
        engine.rebase_runtime_time(start, wall_ms)?;
    }
    let direct_live = {
        let settings = live_settings.ok_or("production execution settings are unavailable")?;
        let live_root = options
            .state_root
            .as_ref()
            .ok_or("live execution requires --state-root")?
            .join("live");
        let runtime = initialize_continuous_execution(
            &mut engine,
            LiveExecutionRuntime::initialize(
                settings,
                &live_root,
                derive_risk_policy_hash(&config.global_risk)?,
                derive_config_hash(&config)?,
                wall_ms,
            ),
            wall_ms,
        )
        .await?;
        if !runtime.recovery_only() {
            persist_unsigned_state(&mut engine, &mut state_root)?;
        }
        Some(std::sync::Arc::new(tokio::sync::Mutex::new(runtime)))
    };
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
    let tracked_source_wallets = config
        .candidates
        .iter()
        .map(|candidate| candidate.address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut durable_sources = None;
    let mut restored_source_baselines = 0_usize;
    if streaming_enabled {
        let source_database_path = options
            .state_root
            .as_ref()
            .and_then(|root| root.parent())
            .ok_or("continuous source-state database has no data root")?
            .join("source-state.sqlite");
        if let Some(source) = options.source_backfill_from.as_ref() {
            match import_source_backfill(source, &source_database_path)
                .map_err(|error| format!("source backfill import failed closed: {error}"))?
            {
                SourceBackfillOutcome::Imported(report) => {
                    eprintln!(
                        "source_backfill_imported=true wallets={} durable_baselines={} fill_events={} history_cursors={}",
                        report.wallets,
                        report.durable_baselines,
                        report.fill_events,
                        report.history_cursors,
                    );
                }
                SourceBackfillOutcome::SkippedDestinationExists => {
                    eprintln!("source_backfill_skipped=true reason=destination_exists");
                }
            }
        }
        match SourceStateStore::open(
            source_database_path,
            &format!("{}-wallet", tracked_source_wallets.len()),
            tracked_source_wallets.clone(),
        ) {
            Ok(store) => {
                match store.load_baselines() {
                    Ok(baselines) => {
                        for baseline in baselines {
                            restored_source_baselines += 1;
                            streaming_sources
                                .as_mut()
                                .ok_or("streaming source book unavailable")?
                                .restore_durable_baseline(baseline)
                                .map_err(|error| {
                                    format!("restore durable source baseline: {error:?}")
                                })?;
                        }
                    }
                    Err(error) => {
                        eprintln!("source_baseline_restore_failed=true nonfatal=true error={error}")
                    }
                }
                durable_sources = Some(store);
            }
            Err(error) => eprintln!("source_database_open_failed=true nonfatal=true error={error}"),
        }
    }
    let mut streaming = streaming_enabled
        .then(|| {
            StreamingHandle::start(
                transport_policy.streaming.clone(),
                tracked_source_wallets.into_iter(),
            )
        })
        .transpose()
        .map_err(|error| format!("streaming transport: {error:?}"))?;
    println!("source_cohort_loaded={} restored_source_baselines={} source_sqlite_available={} live_confirmations_reset=true", config.candidates.len(), restored_source_baselines, durable_sources.is_some());
    let mut market_directory: Option<MarketDirectory> = None;
    let mut stream_live_taker_fee_bps = None;
    let mut stream_connected = false;
    let mut reconciliation_started_at = start.saturating_sub(1);
    if restored_source_baselines != 0 {
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
    }
    let mut tier_rebalance_due = start + inactive_freshness;
    let mut book_enqueue_due = 0;
    let mut book_cursor = 0_usize;
    let mut scheduled = BTreeSet::new();
    let mut market_due = 0;
    let mut metadata_due = 0;
    let mut rules_hydration_due = 0;
    let mut rules_hydration_cursor = 0usize;
    let mut decision_due = 0;
    const EQUITY_BUCKET_INTERVAL_MS: u64 = 300_000;
    let first_bucket_delay = {
        let elapsed_in_wall_bucket = wall_ms % EQUITY_BUCKET_INTERVAL_MS;
        EQUITY_BUCKET_INTERVAL_MS
            .checked_sub(elapsed_in_wall_bucket)
            .ok_or("bucket overflow")?
    };
    let mut profitability_bucket_due = start
        .checked_add(first_bucket_delay)
        .ok_or("bucket overflow")?;
    let mut checkpoint_due = 0;
    let mut counters = Counters::default();
    let mut tasks = JoinSet::new();
    let mut source_history_due = start;
    let mut source_history_wallet = 0usize;
    {
        let restored_inside_current_wall_bucket = restored_state
            && engine.last_equity_boundary().is_some_and(|last| {
                last / EQUITY_BUCKET_INTERVAL_MS == wall_ms / EQUITY_BUCKET_INTERVAL_MS
            });
        if !restored_inside_current_wall_bucket {
            engine.record_equity_boundary(start)?;
            persist_unsigned_state(&mut engine, &mut state_root)?;
        }
    }
    loop {
        let now = clock.now_ms();
        // One bounded page at a time enters the existing low-priority source
        // budget. Wallet rotation is independent of live confirmation; retries
        // retain their immutable interval and never gate another wallet.
        if now >= source_history_due {
            let wallet = config.candidates[source_history_wallet % config.candidates.len()]
                .address
                .to_ascii_lowercase();
            source_history_wallet = source_history_wallet.wrapping_add(1);
            source_history_due = now.saturating_add(2_000);
            if let Some(store) = durable_sources.as_ref() {
                match store
                    .history_request(&wallet, wall_ms.saturating_add(now.saturating_sub(start)))
                {
                    Ok(Some(subject)) => {
                        let request = scheduler_config.api.request(
                            RequestKey {
                                subject,
                                kind: ReadRequestKind::SourceFills,
                            },
                            RequestPriority::Low,
                            BudgetClass::SourcePolling,
                            Some(SourceTier::Inactive),
                            now,
                            now,
                            now.saturating_add(60_000),
                            0,
                        )?;
                        if scheduler.schedule(request) != ScheduleOutcome::Enqueued {
                            counters.schedule_rejections += 1;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        counters.source_persistence_failures += 1;
                        eprintln!("source_history_read_failed=true nonfatal=true error={error}");
                    }
                }
            }
        }
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
                if candidate_tiers.get(candidate) != Some(tier) {}
            }
            candidate_tiers = next_tiers;
            tier_rebalance_due = next_cadence_deadline(tier_rebalance_due, now, 60_000)?;
        }
        if streaming_enabled && now >= next_reconciliation_due {
            source_due = phase_staggered_source_due(&config, now, source_dispatch_plan)?;
            next_reconciliation_due = next_cadence_deadline(
                next_reconciliation_due,
                now,
                transport_policy.streaming.reconciliation_interval_ms,
            )?;
        }
        let pending_source_wallets = engine.pending_source_wallets();
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
            &pending_source_wallets,
            &mut source_due,
        )?;
        for candidate in &config.candidates {
            let id = candidate.address.to_ascii_lowercase();
            if source_due[&id] <= now
                && (!streaming_enabled || market_directory.is_some())
                && (!streaming_enabled || stream_connected)
            {
                let tier = candidate_tiers[&id];
                let interval = match tier {
                    SourceTier::Active => source_dispatch_plan.active_cadence_ms,
                    SourceTier::Inactive => source_dispatch_plan.inactive_cadence_ms,
                };
                let source_subject = RequestSubject::Candidate(id.clone());
                let source_kind = if streaming_enabled || expanded_source_candidates.contains(&id) {
                    ReadRequestKind::ExpandedSourceState
                } else {
                    ReadRequestKind::SourceState
                };
                enqueue(
                    &scheduler,
                    &scheduler_config,
                    source_subject,
                    source_kind,
                    if streaming_enabled && pending_source_wallets.contains(&id) {
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
        if rules_hydration_due <= now {
            let dexes = engine
                .waiting_market_rules()
                .iter()
                .filter_map(|asset| asset.split_once(':').map(|(dex, _)| dex.to_owned()))
                .collect::<BTreeSet<_>>();
            if !dexes.is_empty() {
                let dex = dexes
                    .iter()
                    .nth(rules_hydration_cursor % dexes.len())
                    .unwrap();
                enqueue(
                    &scheduler,
                    &scheduler_config,
                    RequestSubject::Asset(dex.clone()),
                    ReadRequestKind::ExchangeMetadata,
                    RequestPriority::High,
                    None,
                    now,
                    120_000,
                    &mut counters,
                )?;
                rules_hydration_cursor = rules_hydration_cursor.wrapping_add(1);
                rules_hydration_due = now.saturating_add(30_000);
            }
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
            book_enqueue_due = next_cadence_deadline(book_enqueue_due, now, 2_000)?;
        }
        if let Some(stream) = streaming.as_ref() {
            let urgent = engine.urgent_book_assets();
            let live_required = engine.assets_requiring_books(now);
            let mut required = live_required.clone();
            required.extend(engine.learning_book_assets());
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
                &live_required.union(&urgent).cloned().collect(),
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
            if stream_connected && now >= trade_market_rotation_due {
                if let Some(directory) = market_directory.as_ref() {
                    let mut priority = streaming_sources
                        .as_ref()
                        .map_or_else(BTreeSet::new, StreamingSourceBook::active_assets);
                    priority.extend(subscribed_hot_books.iter().cloned());
                    let (next_markets, next_cursor) = directory
                        .rotated_markets(&priority, trade_market_rotation_cursor)
                        .map_err(|error| format!("trade subscription rotation: {error:?}"))?;
                    if next_markets != subscribed_trade_markets {
                        let resumed_market = !next_markets.is_subset(&subscribed_trade_markets);
                        engine
                            .replace_source_market_coverage(next_markets.clone(), now)
                            .map_err(|error| error.to_string())?;
                        stream
                            .replace_markets(next_markets.clone())
                            .await
                            .map_err(|error| format!("trade subscriptions: {error:?}"))?;
                        subscribed_trade_markets = next_markets;
                        if resumed_market {
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
                    .checked_add(transport_policy.streaming.reconciliation_spread_ms)
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
                counters.reserved_weight += u64::from(request.weight);
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
                        if response.request_kind.is_source_state()
                            && response.requested_at_mono <= reconciliation_started_at {
                            // A pre-gap REST result cannot remove the wallet's
                            // recovery barrier, even when it arrives after reconnect.
                            source_due.insert(response.subject.clone(),clock.now_ms().saturating_add(1));
                            continue;
                        }
                        let mut confirmed_baseline = None::<SourceStateResponse>;
                        if streaming_enabled {
                            match &mut response.payload {
                                crate::public_mainnet::PublicPayload::SourceFills(page) => {
                                    if let Some(store) = durable_sources.as_mut() {
                                        match store.persist_history(page, wall_ms.saturating_add(clock.now_ms().saturating_sub(start))) {
                                            Ok(()) => counters.source_history_pages += 1,
                                            Err(error) => {
                                                counters.source_persistence_failures += 1;
                                                eprintln!("source_history_commit_failed=true nonfatal=true error={error}");
                                            }
                                        }
                                    }
                                }
                                crate::public_mainnet::PublicPayload::SourceState(state) => {
                                    let installed = streaming_sources
                                        .as_mut()
                                        .ok_or("streaming source book unavailable")?
                                        .install_baseline(state.clone())
                                        .map_err(|error| format!("source baseline: {error:?}"))?;
                                    *state = installed;
                                    if response.requested_at_mono > reconciliation_started_at {
                                        confirmed_baseline = Some(state.clone());
                                    }
                                }
                                crate::public_mainnet::PublicPayload::MarketMetadata(metadata) => {
                                    if response.subject != "market" {
                                        let Some(merged) = engine.merge_hydrated_metadata(metadata.clone(), &response.subject) else {
                                            eprintln!("market_rules_unavailable=true dex={} reason=inconsistent_metadata", response.subject);
                                            continue;
                                        };
                                        *metadata = merged;
                                        eprintln!("market_rules_hydrated=true dex={}", response.subject);
                                    }
                                    let execution_dex_order = transport.discovered_perp_dex_order();
                                    engine.install_execution_dex_order(execution_dex_order.clone());
                                    if let Some(live) = &direct_live {
                                        live.lock()
                                            .await
                                            .install_perp_dex_order(execution_dex_order)
                                            .map_err(|error| {
                                                format!("execution DEX order: {error}")
                                            })?;
                                    }
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
                                        engine
                                            .replace_source_market_coverage(
                                                subscribed_trade_markets.clone(),
                                                clock.now_ms(),
                                            )
                                            .map_err(|error| error.to_string())?;
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
                        let executions_before = engine.metrics().executions;
                        engine.ingest(response, accepted_at).map_err(|e| e.to_string())?;
                        if let Some(state) = confirmed_baseline {
                            let admitted = engine
                                .admit_reconciled_source_wallet(
                                    &state.candidate_id,
                                    state.source_time_ms,
                                    accepted_at,
                                )
                                .map_err(|error| error.to_string())?;
                            if !persist_source_state(
                                    &mut durable_sources,
                                &state,
                                admitted,
                                !admitted,
                            ) { counters.source_persistence_failures += 1; }
                            if admitted {
                            } else {
                                source_due.insert(
                                    state.candidate_id,
                                    accepted_at
                                        .checked_add(scheduler_config.retry.maximum_backoff_ms)
                                        .ok_or("source baseline rearm overflow")?,
                                );
                            }
                        }
                        dispatch_runtime_intents(
                            &mut engine,
                            direct_live.as_ref(),
                        )
                        .await?;
                        if engine.metrics().executions != executions_before
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
                    &engine.pending_source_wallets(),
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
                match event {
                    StreamingEvent::Connected { .. } => {
                        stream_connected = true;
                        // A reconnected public market stream is current even
                        // while tracked-wallet history is still repairing.
                        // Wallet readiness, not global history completion,
                        // controls which source contributions are actionable.
                        engine
                            .replace_source_market_coverage(
                                subscribed_trade_markets.clone(),
                                observed_at,
                            )
                            .map_err(|error| error.to_string())?;
                        let pending_wallets = engine.pending_source_wallets();
                        if !pending_wallets.is_empty()
                            && pending_wallets.iter().all(|wallet| {
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
                    }
                    StreamingEvent::Gap {
                        affected_markets,
                        ..
                    } => {
                        engine.learning_stream_gap();
                        stream_connected = false;
                        reconciliation_started_at = observed_at;
                        streaming_sources
                            .as_mut()
                            .ok_or("streaming source book unavailable")?
                            .begin_recovery();
                        if let Some(store) = durable_sources.as_mut() {
                            if let Err(error) = store.mark_stream_gap() {
                                counters.source_persistence_failures += 1;
                                eprintln!("source_gap_commit_failed=true nonfatal=true error={error}");
                            }
                        }
                        let affected_markets = if affected_markets.is_empty() {
                            subscribed_trade_markets.clone()
                        } else {
                            affected_markets
                        };
                        engine
                            .mark_source_stream_gap(&affected_markets, observed_at)
                            .map_err(|error| error.to_string())?;
                        source_due.values_mut().for_each(|due| *due = u64::MAX);
                    }
                    StreamingEvent::MarketFlow { asset, flow } => {
                        engine.ingest_market_flow(asset, flow, observed_at);
                    }
                    StreamingEvent::Trades(trades) => {
                        let executions_before = engine.metrics().executions;
                        for trade in trades {
                            if let Some(store) = durable_sources.as_mut() {
                                if let Err(error) = store.persist_trade(&trade, wall_ms.saturating_add(observed_at.saturating_sub(start))) {
                                    counters.source_persistence_failures += 1;
                                    eprintln!("source_fill_commit_failed=true nonfatal=true error={error}");
                                }
                            }
                            let states = streaming_sources
                                .as_mut()
                                .ok_or("streaming source book unavailable")?
                                .apply_trade(trade)
                                .map_err(|error| format!("stream trade: {error:?}"))?;
                            for update in states {
                                let state = update.state;
                                let subject = state.candidate_id.clone();
                                let confirmed = engine.source_is_fresh(&subject, observed_at);
                                if !persist_source_state(
                                            &mut durable_sources,
                                    &state,
                                    confirmed,
                                    !confirmed,
                                ) { counters.source_persistence_failures += 1; }
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
                        dispatch_runtime_intents(
                            &mut engine,
                            direct_live.as_ref(),
                        )
                        .await?;
                        if engine.metrics().executions != executions_before {
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
                        let executions_before = engine.metrics().executions;
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
                        dispatch_runtime_intents(
                            &mut engine,
                            direct_live.as_ref(),
                        )
                        .await?;
                        if engine.metrics().executions != executions_before {
                            persist_unsigned_state(&mut engine, &mut state_root)?;
                        }
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
            _ = sleep(Duration::from_millis(25)) => {}
        }
        engine.service_delayed_learning(clock.now_ms());
        if engine.service_mfce_training() {
            persist_unsigned_state(&mut engine, &mut state_root)?;
        }
        let now = clock.now_ms();
        if now >= decision_due {
            let _ = engine
                .construct_next_decision(now)
                .map_err(|e| e.to_string())?;
            dispatch_runtime_intents(&mut engine, direct_live.as_ref()).await?;
            persist_unsigned_state(&mut engine, &mut state_root)?;
            decision_due = now + 20_000;
        }
        if now >= profitability_bucket_due {
            engine.record_equity_boundary(profitability_bucket_due)?;
            persist_unsigned_state(&mut engine, &mut state_root)?;
            profitability_bucket_due = profitability_bucket_due
                .checked_add(EQUITY_BUCKET_INTERVAL_MS)
                .ok_or("bucket deadline overflow")?;
        }
        if now >= checkpoint_due {
            let h = scheduler.health();
            let t = transport.metrics();
            {
                let durable_now = engine.durable_timestamp(now)?;
                engine.compact_runtime_history(
                    durable_now.saturating_sub(30 * 24 * 60 * 60 * 1_000),
                )?;

                persist_unsigned_state(&mut engine, &mut state_root)?;
                if let Err(error) = write_continuous_status(
                    &output,
                    &run_id,
                    start,
                    now,
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
                        coverage_reconciliations,
                        &subscribed_hot_books,
                        durable_sources
                            .as_ref()
                            .and_then(|store| store.continuity_status().ok()),
                    ),
                    durable_sources
                        .as_ref()
                        .and_then(|store| store.hip3_source_activity().ok()),
                ) {
                    println!("rolling_status_update_failed=true nonfatal=true error={error}");
                }
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
                let executions_before = engine.metrics().executions;
                engine
                    .ingest(response, accepted_at)
                    .map_err(|e| e.to_string())?;
                dispatch_runtime_intents(&mut engine, direct_live.as_ref()).await?;
                if engine.metrics().executions != executions_before {
                    persist_unsigned_state(&mut engine, &mut state_root)?;
                }
            }
        } else {
            let _ = transport.take_accepted(&completed_request);
        }
    }
    {
        persist_unsigned_state(&mut engine, &mut state_root)?;
        let now = clock.now_ms();
        let health = scheduler.health();
        if let Err(error) = write_continuous_status(
            &output,
            &run_id,
            start,
            now,
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
                coverage_reconciliations,
                &subscribed_hot_books,
                durable_sources
                    .as_ref()
                    .and_then(|store| store.continuity_status().ok()),
            ),
            durable_sources
                .as_ref()
                .and_then(|store| store.hip3_source_activity().ok()),
        ) {
            println!("rolling_status_update_failed=true nonfatal=true error={error}");
        }
        println!(
            "continuous_engine_stopped_by_operator=true healthy=true output={}",
            output.display()
        );
        return Ok(output);
    }
}
fn write_continuous_status(
    output: &Path,
    run_id: &str,
    start: Timestamp,
    now: Timestamp,
    config: &CopyTradeConfig,
    engine: &DecisionEngine,
    counters: &Counters,
    scheduler: &SchedulerHealth,
    rate_limited_responses: u64,
    streaming: Option<StreamingRuntimeStatus>,
    hip3_source_activity: Option<crate::source_state::Hip3SourceActivity>,
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
        strategy_target_execution_enabled: engine.strategy_target_execution_enabled(),
        mode: "continuous_live",
        run_id,
        started_at_mono: start,
        observed_at_mono: now,
        elapsed_seconds: now.saturating_sub(start) / 1_000,
        healthy: metrics.projection_violations == 0
            && metrics.persistence_failures == 0
            && unresolved_roots == 0
            && engine.source_stream_healthy(),
        fatal_stop: false,
        execution_state: if engine.execution_recovery_only() {
            "RISK_ONLY"
        } else {
            "NORMAL"
        },
        exchange_risk_positions: engine.exchange_risk_positions(),
        snapshot_generation: engine.snapshot_generation(),
        candidate_count: config.candidates.len(),
        projection_violations: metrics.projection_violations,
        persistence_failures: metrics.persistence_failures,
        source_persistence_failures: counters.source_persistence_failures,
        source_history_pages: counters.source_history_pages,
        unresolved_roots,
        unresolved_root_details: engine.unresolved_actionable_roots(),
        rules_pending_markets: engine.waiting_market_rules().clone(),
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
        hip3: engine.hip3_pipeline_status(hip3_source_activity.as_ref()),
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
    let journal = serde_json::json!(
        { "schema_version" : 1, "scope" :
        "last_1000_modeled_executions_and_lifecycle_events", "executions" : & engine
        .executions() [execution_start..], "lifecycle" : engine
        .action_lifecycle_events(), }
    );
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
    engine: &DecisionEngine,
    directory: Option<&MarketDirectory>,
    subscribed_trade_markets: &BTreeSet<String>,
    coverage_reconciliations: u64,
    subscribed_hot_books: &BTreeSet<String>,
    continuity: Option<SourceContinuityStatus>,
) -> Option<StreamingRuntimeStatus> {
    let metrics = stream?.metrics();
    let discovered_markets = directory.map_or(0, |directory| directory.markets().len());
    let continuity = continuity.unwrap_or_default();
    Some(StreamingRuntimeStatus {
        source_healthy: engine.source_stream_healthy(),
        hydrated_source_count: sources.map_or(0, StreamingSourceBook::hydrated_count),
        durable_baselines: continuity.durable_baselines,
        live_state_confirmed: continuity.live_state_confirmed,
        live_state_recovering: continuity.live_state_recovering,
        source_history_contiguous: continuity.history_contiguous,
        source_history_catching_up: continuity.history_catching_up,
        source_history_gapped: continuity.history_gapped,
        discovered_markets,
        subscribed_trade_markets: subscribed_trade_markets.len(),
        coverage_ready_markets: engine.source_market_coverage_count(),
        coverage_stale_markets: discovered_markets
            .saturating_sub(engine.source_market_coverage_count()),
        coverage_reconciliation_wallets_pending: engine.pending_source_wallets().len(),
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
    engine: &DecisionEngine,
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
    let (recovered_count, recovered_turnover) =
        engine.ledger().recovered_execution_stats(cutoff)?;
    turnover = turnover
        .checked_add(recovered_turnover)
        .ok_or("recovered turnover overflow")?;
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
        executions: executions.len() + recovered_count,
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
#[cfg(test)]
fn manifest_stage_matches(production_enabled: bool, stage: &str) -> bool {
    if production_enabled {
        stage == "PRODUCTION_RELEASE"
    } else {
        matches!(stage, "HL1J" | "PRODUCTION_RELEASE")
    }
}
async fn initialize_continuous_execution<
    T: crate::signing::transport::AuthenticatedExchangeTransport + Clone,
>(
    engine: &mut DecisionEngine,
    startup: impl std::future::Future<
        Output = Result<
            (LiveExecutionRuntime<T>, Option<LiveExecutionUpdate>),
            crate::signing::SignerError,
        >,
    >,
    wall_ms: u64,
) -> Result<LiveExecutionRuntime<T>, Box<dyn Error>> {
    let (mut runtime, startup_update) = startup.await?;
    engine.enable_production_intents(ProductionIntentIdentity {
        observer_release_hash: [0; 32],
        signer_release_hash: [0; 32],
        release_manifest_hash: [0; 32],
        market_rules_hash: [0; 32],
        dynamic_floor_policy_hash: [0; 32],
        ioc_policy_hash: [0; 32],
        expires_after_ms: 20_000,
    });
    engine.observe_recovery_risk(runtime.exchange_snapshot(), runtime.state());
    let registered = runtime.registered_cloids().await?;
    engine.retire_unregistered_pending_actions(&registered, wall_ms);
    if let Some(update) = startup_update {
        apply_authenticated_reconciliation(engine, &mut runtime, update, wall_ms)?;
    } else {
        runtime.defer_recovery(wall_ms);
    }
    Ok(runtime)
}
async fn dispatch_runtime_intents(
    engine: &mut DecisionEngine,
    direct_live: Option<
        &std::sync::Arc<
            tokio::sync::Mutex<
                LiveExecutionRuntime<crate::signing::transport::HyperliquidMainnetTransport>,
            >,
        >,
    >,
) -> Result<(), Box<dyn Error>> {
    let direct_live = direct_live.ok_or("authenticated production runtime unavailable")?;
    let mut live = direct_live.lock().await;
    let recovering = live.recovery_only();
    let mut intents = engine.take_prepared_authorized_intents();
    if recovering {
        intents.retain(|intent| {
            if intent.reduce_only {
                true
            } else {
                engine.release_unaccepted_production_intent(intent.planned_cloid);
                false
            }
        });
    }
    let mut intents = intents.into_iter();
    let mut latest_exchange_timestamp = None;
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
                engine.observe_recovery_risk(live.exchange_snapshot(), live.state());
                live.defer_recovery(unix_ms);
                return Ok(());
            }
            Err(crate::signing::SignerError::Authorization(_)) if recovering => {
                engine.release_unaccepted_production_intent(cloid);
                continue;
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
            engine.observe_recovery_risk(live.exchange_snapshot(), live.state());
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
fn apply_authenticated_reconciliation<
    T: crate::signing::transport::AuthenticatedExchangeTransport + Clone,
>(
    engine: &mut DecisionEngine,
    live: &mut LiveExecutionRuntime<T>,
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
            engine.observe_recovery_risk(live.exchange_snapshot(), live.state());
            live.defer_recovery(observed_at);
            Ok(AuthenticatedReconciliation::RecoveryPending)
        }
        Err(_) => {
            engine.restore_live_execution_checkpoint(checkpoint);
            engine.observe_recovery_risk(live.exchange_snapshot(), live.state());
            live.defer_recovery(observed_at);
            Ok(AuthenticatedReconciliation::RecoveryPending)
        }
    }
}
fn apply_live_update_inner(
    engine: &mut DecisionEngine,
    update: LiveExecutionUpdate,
    observed_at: u64,
) -> Result<Option<u64>, Box<dyn Error>> {
    enum ExchangeEvent<'a> {
        Fill(&'a crate::domain::live_trading::VerifiedExchangeFill),
        Funding(&'a crate::domain::live_trading::VerifiedFundingEvent),
        External(&'a crate::domain::live_trading::ExternalFillAccounting),
    }
    impl ExchangeEvent<'_> {
        fn occurred_at(&self) -> u64 {
            match self {
                Self::Fill(fill) => fill.occurred_at,
                Self::Funding(event) => event.occurred_at,
                Self::External(event) => event.fill.occurred_at,
            }
        }
    }
    let mut events = update
        .applied
        .fills
        .iter()
        .map(ExchangeEvent::Fill)
        .chain(update.applied.funding.iter().map(ExchangeEvent::Funding))
        .chain(update.applied.external.iter().map(ExchangeEvent::External))
        .collect::<Vec<_>>();
    events.sort_by_key(ExchangeEvent::occurred_at);
    let latest_exchange_timestamp = events.last().map(ExchangeEvent::occurred_at);
    for event in events {
        match event {
            ExchangeEvent::Fill(fill) => engine.apply_live_execution_fill(fill)?,
            ExchangeEvent::Funding(event) => engine.apply_live_funding(event)?,
            ExchangeEvent::External(event) => engine.apply_external_fill(event)?,
        }
    }
    for terminal in update.terminal {
        if terminal.no_action {
            engine.retire_no_action(terminal.cloid, observed_at);
            continue;
        }
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
        return Err(
            format!(
                "effective candidate count {candidate_count} with {expanded_candidate_count} expanded sources cannot remain fresh: source weight {base_weight} exceeds {} per window",
                scheduler.api.source_polling_weight_per_window
            )
                .into(),
        );
    }
    let incremental_active_dispatches = active_dispatches_per_candidate
        .checked_sub(inactive_dispatches_per_candidate)
        .ok_or("active source dispatch increment underflow")?;
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
fn rearm_unresolved_source_request(
    request: &ScheduledReadRequest,
    outcome: ExecutionOutcome,
    now: Timestamp,
    retry_delay_ms: u64,
    stream_reconciliation: &BTreeSet<String>,
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
    if !stream_reconciliation.contains(candidate) {
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
    source_due: &mut BTreeMap<String, Timestamp>,
) -> Result<bool, Box<dyn Error>> {
    if !scheduler_idle
        || stream_reconciliation.is_empty()
        || stream_reconciliation.iter().any(|candidate| {
            source_due
                .get(candidate)
                .is_some_and(|due| *due != u64::MAX)
        })
    {
        return Ok(false);
    }
    for (wallet, due) in phase_staggered_source_due(config, now, plan)? {
        if stream_reconciliation.contains(&wallet) {
            source_due.insert(wallet, due);
        }
    }
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
#[cfg(test)]
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
fn record_latency(summary: &mut LatencySummary, value_ms: u64) {
    summary.samples = summary.samples.saturating_add(1);
    summary.total_ms = summary.total_ms.saturating_add(value_ms);
    summary.maximum_ms = summary.maximum_ms.max(value_ms);
}
#[cfg(test)]
fn parse_duration_seconds(value: &str) -> Result<u64, Box<dyn Error>> {
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::SnapshotIdentity;
    use crate::domain::decision::{ConfigHash, PayloadHash, PlannedCloid, RiskPolicyHash};
    use crate::domain::live_trading::{LiveTradingState, VerifiedExchangeFill};
    use crate::domain::scheduler::ManualClock;
    use crate::signing::transport::*;
    use crate::signing::{
        ApiWalletSecret, ApprovedExecutionIntent, SubmissionRegistry, SubmissionState,
    };
    #[derive(Clone)]
    struct RecoveryAccount(std::sync::Arc<std::sync::Mutex<RecoveryTruth>>);
    struct RecoveryTruth {
        positions: BTreeMap<String, Decimal>,
        fills: Vec<UserFill>,
        funding: Vec<FundingRecord>,
        submissions: usize,
    }
    #[async_trait::async_trait]
    impl AuthenticatedExchangeTransport for RecoveryAccount {
        async fn submit_ioc(
            &self,
            _: SignedIocRequest,
        ) -> Result<SubmissionResponse, SubmissionTransportError> {
            self.0.lock().unwrap().submissions += 1;
            panic!("recovery must never submit a trade")
        }
        async fn lookup_order(
            &self,
            _: PlannedCloid,
        ) -> Result<OrderObservation, ReconciliationError> {
            panic!("fixture has only terminal orders")
        }
        async fn read_positions(&self) -> Result<ExchangePositionSnapshot, ReconciliationError> {
            Ok(ExchangePositionSnapshot {
                positions: self.0.lock().unwrap().positions.clone(),
                account_equity: Decimal::from(101),
                observed_at_ms: 100,
                source_hash: PayloadHash([0; 32]),
            })
        }
        async fn read_open_orders(
            &self,
        ) -> Result<ExchangeOpenOrdersSnapshot, ReconciliationError> {
            Ok(ExchangeOpenOrdersSnapshot {
                orders: vec![],
                source_hash: PayloadHash([0; 32]),
            })
        }
        async fn read_fills(
            &self,
            cursor: FillCursor,
        ) -> Result<UserFillBatch, ReconciliationError> {
            Ok(UserFillBatch {
                fills: self
                    .0
                    .lock()
                    .unwrap()
                    .fills
                    .iter()
                    .filter(|fill| {
                        fill.occurred_at_ms >= cursor.start_time_ms
                            && fill.occurred_at_ms <= cursor.end_time_ms.unwrap()
                    })
                    .cloned()
                    .collect(),
                next_cursor: FillCursor {
                    start_time_ms: cursor.end_time_ms.unwrap() + 1,
                    ..cursor
                },
                source_hash: PayloadHash([0; 32]),
            })
        }
        async fn read_funding(
            &self,
            cursor: FundingCursor,
        ) -> Result<FundingBatch, ReconciliationError> {
            Ok(FundingBatch {
                events: self
                    .0
                    .lock()
                    .unwrap()
                    .funding
                    .iter()
                    .filter(|event| {
                        event.occurred_at_ms >= cursor.start_time_ms
                            && event.occurred_at_ms <= cursor.end_time_ms.unwrap()
                    })
                    .cloned()
                    .collect(),
                next_cursor: FundingCursor {
                    start_time_ms: cursor.end_time_ms.unwrap() + 1,
                    ..cursor
                },
                source_hash: PayloadHash([0; 32]),
            })
        }
    }
    fn recovered_order(root: &Path, fill: &VerifiedExchangeFill) -> UserFill {
        let mut registry = SubmissionRegistry::default();
        let cloid = fill.identity.cloid.to_hex();
        registry
            .register(ApprovedExecutionIntent {
                decision_id: "01".repeat(32),
                target_version: 1,
                root_cloid: cloid.clone(),
                cloid: cloid.clone(),
                parent_cloid: None,
                continuation_generation: 0,
                asset: fill.asset.clone(),
                asset_index: 0,
                is_buy: true,
                reduce_only: false,
                limit_price: fill.submitted_limit_price,
                quantity: Decimal::ONE,
                risk_projection_hash: "00".repeat(32),
                risk_policy_hash: "00".repeat(32),
                configuration_hash: "00".repeat(32),
                release_manifest_hash: "00".repeat(32),
                decision_reference_price: fill.decision_reference_price,
                decision_timestamp_ms: fill.decision_timestamp,
                expires_at_mono: 200,
                canonical_intent_hash: "00".repeat(32),
            })
            .unwrap();
        for state in [
            SubmissionState::NonceAllocated { nonce: 1 },
            SubmissionState::Signed { nonce: 1 },
            SubmissionState::SubmissionStarted { nonce: 1 },
            SubmissionState::Acknowledged {
                order_id: fill.identity.exchange_order_id.0.clone(),
            },
            SubmissionState::Cancelled {
                order_id: Some(fill.identity.exchange_order_id.0.clone()),
                filled: fill.filled_quantity,
            },
        ] {
            registry.transition(&cloid, state).unwrap();
        }
        registry
            .persist(&root.join("live-submission-registry.json"))
            .unwrap();
        UserFill {
            position_before: Some(Decimal::ZERO),
            cloid: Some(fill.identity.cloid),
            exchange_order_id: fill.identity.exchange_order_id.0.clone(),
            trade_id: fill.identity.trade_id.0.clone(),
            asset: fill.asset.clone(),
            side: "B".into(),
            price: fill.average_fill_price,
            quantity: fill.filled_quantity,
            closed_pnl: fill.exchange_closed_pnl,
            fee: fill.fee_amount,
            fee_token: fill.fee_asset.clone(),
            crossed: true,
            occurred_at_ms: fill.occurred_at,
            source_hash: fill.source_hash,
        }
    }
    #[tokio::test]
    async fn continuous_startup_preserves_hype_and_reconciles_in_place_without_a_trade() {
        let root = tempfile::tempdir().unwrap();
        let (mut engine, mut fill) = crate::decision::tests::live_fill_engine("HYPE");
        fill.average_fill_price = Decimal::new(81525, 3);
        let recovered_fill = recovered_order(root.path(), &fill);
        LiveTradingState::new(Decimal::from(100), 0)
            .unwrap()
            .save_atomic(root.path().join("live-trading-state.json"))
            .unwrap();
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::from([("HYPE".into(), Decimal::new(25, 2))]),
                fills: vec![],
                funding: vec![FundingRecord {
                    event_hash: "funding-1".into(),
                    asset: "HYPE".into(),
                    signed_usdc_delta: Decimal::new(-1, 3),
                    occurred_at_ms: 11,
                    source_hash: PayloadHash([4; 32]),
                }],
                submissions: 0,
            })));
        let mut runtime = initialize_continuous_execution(
            &mut engine,
            LiveExecutionRuntime::initialize_with_transport(
                transport.clone(),
                ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
                root.path(),
                RiskPolicyHash([0; 32]),
                ConfigHash([0; 32]),
                100,
            ),
            100,
        )
        .await
        .expect("continuous startup must survive external PositionDivergence");
        let targets = engine.target_ledger().clone();
        for now in [101, 1_101] {
            crate::decision::tests::assert_recovery_ingestion_progress(&mut engine, now);
            assert!(runtime.recovery_only());
            assert!(engine.execution_recovery_only());
            assert_eq!(
                engine.exchange_risk_positions().unwrap()["HYPE"],
                Decimal::new(25, 2)
            );
            assert!(engine.construct_next_decision(now).unwrap().is_none());
            assert_eq!(engine.target_ledger(), &targets);
            assert!(engine.take_prepared_authorized_intents().is_empty());
            assert_eq!(transport.0.lock().unwrap().submissions, 0);
            assert_eq!(runtime.state().position("HYPE"), Decimal::ZERO);
            assert_eq!(
                runtime.state().fill_cursor_ms(),
                0,
                "unresolved replay must retain catch-up cursor"
            );
        }
        assert!(matches!(
            runtime.reconcile_if_due(1_101).await,
            Err(crate::signing::SignerError::ExchangeTruthMismatch(_))
        ));
        transport.0.lock().unwrap().fills.push(recovered_fill);
        let update = runtime.reconcile_if_due(2_201).await.unwrap().unwrap();
        assert!(matches!(
            apply_authenticated_reconciliation(&mut engine, &mut runtime, update, 2_201).unwrap(),
            AuthenticatedReconciliation::Applied(_)
        ));
        assert!(!runtime.recovery_only());
        assert!(!engine.execution_recovery_only());
        assert_eq!(
            engine.exchange_risk_positions().unwrap()["HYPE"],
            Decimal::new(25, 2)
        );
        assert_eq!(runtime.state().position("HYPE"), Decimal::new(25, 2));
        assert_eq!(
            engine.ledger().portfolio_position("HYPE"),
            Decimal::new(25, 2)
        );
        assert_eq!(
            runtime.state().verified_fills()[0].occurred_at,
            fill.occurred_at
        );
        assert_eq!(
            runtime.state().verified_fills()[0].average_fill_price,
            fill.average_fill_price
        );
        assert!(
            engine.ledger().portfolio_closed().is_empty(),
            "positive unrealized PnL is not a completed label"
        );
        assert_eq!(transport.0.lock().unwrap().submissions, 0);
        assert_eq!(
            transport.0.lock().unwrap().positions["HYPE"],
            Decimal::new(25, 2)
        );
    }

    #[tokio::test]
    async fn startup_retires_unregistered_action_when_initial_reconciliation_is_unavailable() {
        let root = tempfile::tempdir().unwrap();
        let (mut engine, _) = crate::decision::tests::live_fill_engine("GAS");
        LiveTradingState::new(Decimal::from(100), 0)
            .unwrap()
            .save_atomic(root.path().join("live-trading-state.json"))
            .unwrap();
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::new(),
                fills: vec![],
                funding: vec![],
                submissions: 0,
            })));
        let (runtime, _) = LiveExecutionRuntime::initialize_with_transport(
            transport.clone(),
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            root.path(),
            RiskPolicyHash([0; 32]),
            ConfigHash([0; 32]),
            100,
        )
        .await
        .unwrap();

        let runtime = initialize_continuous_execution(
            &mut engine,
            async move { Ok::<_, crate::signing::SignerError>((runtime, None)) },
            100,
        )
        .await
        .unwrap();

        assert!(runtime.recovery_only());
        assert!(engine.execution_recovery_only());
        assert_eq!(engine.unresolved_actionable_root_count(), 0);
        assert_eq!(transport.0.lock().unwrap().submissions, 0);
    }
    #[tokio::test]
    async fn zero_hash_funding_records_are_distinct_and_legacy_replay_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let (_, fill) = crate::decision::tests::live_fill_engine("HYPE");
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        crate::domain::live_trading::apply_exchange_fill(&mut state, fill).unwrap();
        let legacy = crate::domain::live_trading::VerifiedFundingEvent {
            event_id: crate::domain::live_trading::FundingEventId([7; 32]),
            asset: "HYPE".into(),
            amount: Decimal::new(-254, 6),
            occurred_at: 11,
            source_hash: PayloadHash([8; 32]),
        };
        crate::domain::live_trading::apply_funding_event(&mut state, legacy.clone()).unwrap();
        state
            .save_atomic(root.path().join("live-trading-state.json"))
            .unwrap();
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::from([("HYPE".into(), Decimal::new(25, 2))]),
                fills: vec![],
                submissions: 0,
                funding: [254, 255, 257, 259, 257, 256, 257, 259]
                    .into_iter()
                    .enumerate()
                    .map(|(i, amount)| FundingRecord {
                        event_hash: format!("0x{}", "00".repeat(32)),
                        asset: "HYPE".into(),
                        signed_usdc_delta: Decimal::new(-amount, 6),
                        occurred_at_ms: 11 + i as u64,
                        source_hash: PayloadHash([0; 32]),
                    })
                    .collect(),
            })));
        let (runtime, update) = LiveExecutionRuntime::initialize_with_transport(
            transport,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            root.path(),
            RiskPolicyHash([0; 32]),
            ConfigHash([0; 32]),
            100,
        )
        .await
        .unwrap();
        assert!(update.is_some());
        let funding = runtime.state().verified_funding();
        assert_eq!(funding.len(), 8);
        assert_eq!(
            funding[0], legacy,
            "never rewrite a durable predecessor event"
        );
        assert_eq!(
            funding
                .iter()
                .map(|event| event.event_id)
                .collect::<BTreeSet<_>>()
                .len(),
            8
        );
        assert_eq!(
            funding.iter().map(|event| event.amount).sum::<Decimal>(),
            Decimal::new(-2054, 6)
        );
    }

    #[tokio::test]
    async fn continuous_startup_corrupt_durable_position_is_still_fatal() {
        let root = tempfile::tempdir().unwrap();
        let state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        let mut corrupt = serde_json::to_value(state).unwrap();
        corrupt["ledger"]["portfolio"]["positions"]["HYPE"] = serde_json::json!("0.25");
        std::fs::write(
            root.path().join("live-trading-state.json"),
            serde_json::to_vec(&corrupt).unwrap(),
        )
        .unwrap();
        let (mut engine, _) = crate::decision::tests::live_fill_engine("HYPE");
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::new(),
                fills: vec![],
                funding: vec![],
                submissions: 0,
            })));
        let startup = LiveExecutionRuntime::initialize_with_transport(
            transport,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            root.path(),
            RiskPolicyHash([0; 32]),
            ConfigHash([0; 32]),
            100,
        );
        let result = initialize_continuous_execution(&mut engine, startup, 100).await;
        assert!(
            matches!(result, Err(error) if error.downcast_ref::<crate::signing::SignerError>() == Some(&crate::signing::SignerError::Ledger("PositionDivergence".into())))
        );
    }

    #[tokio::test]
    async fn retained_hype_targeted_recovery_closes_manual_episode_and_restarts_exactly_once() {
        let root = tempfile::tempdir().unwrap();
        let template = tempfile::tempdir().unwrap();
        let retained = std::env::var_os("RETAINED_QUALIFICATION_INPUT").map(PathBuf::from);
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let loaded = retained.as_ref().map(|_| {
            EngineState::load_with_very_profitable_layer(
                workspace.join("railway-frozen/copytrade.json"),
                Some(workspace.join("railway-frozen/very-profitable-layer.json")),
            )
            .unwrap()
        });
        let config = || {
            if let Some(loaded) = &loaded {
                return loaded.config().clone();
            }
            CopyTradeConfig::from_path(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json"),
            )
            .unwrap()
        };
        let mut engine =
            DecisionEngine::new(config(), b"retained-hype", "first", 40_000, 80_000).unwrap();
        let prepare = |engine: &mut DecisionEngine| {
            if let Some(loaded) = &loaded {
                engine.enable_source_stream_mode();
                engine
                    .install_very_profitable_layer(loaded.very_profitable_layer().unwrap().clone())
                    .unwrap();
            }
        };
        prepare(&mut engine);
        let (_, mut fill) = crate::decision::tests::live_fill_engine("HYPE");
        fill.identity.cloid = PlannedCloid([
            0x44, 0xdd, 0x33, 0xe8, 0x95, 0xff, 0x2c, 0x32, 0x2f, 0x50, 0x43, 0x91, 0x1e, 0xbd,
            0x3f, 0x63,
        ]);
        fill.identity.exchange_order_id.0 = "528085972382".into();
        fill.identity.trade_id.0 = "291442235899840".into();
        fill.occurred_at = 1_787_810_429_913;
        fill.decision_timestamp = 2_645; // monotonic, never a REST time boundary
        fill.average_fill_price = Decimal::new(81525, 3);
        fill.decision_reference_price = Decimal::new(8154750, 5);
        fill.submitted_limit_price = Decimal::new(81559, 3);
        fill.fee_amount = Decimal::new(8804, 6);
        let opening = recovered_order(template.path(), &fill);
        let cloid = fill.identity.cloid.to_string();
        let original =
            SubmissionRegistry::restore(&template.path().join("live-submission-registry.json"))
                .unwrap();
        let mut intent = original.intent(&cloid).unwrap().clone();
        intent.quantity = Decimal::new(25, 2);
        intent.decision_id =
            "e88e9e81ffb2e1decafccba0334678c8eb8b7d1d9f169a5698ff03eec85789bd".into();
        let mut registry = SubmissionRegistry::default();
        registry.register(intent.clone()).unwrap();
        let nonce = 1_787_810_428_545;
        for status in [
            SubmissionState::NonceAllocated { nonce },
            SubmissionState::Signed { nonce },
            SubmissionState::SubmissionStarted { nonce },
            SubmissionState::Filled {
                order_id: "528085972382".into(),
                filled: Decimal::new(25, 2),
            },
        ] {
            registry.transition_at(&cloid, status, nonce).unwrap();
        }
        if retained.is_none() {
            registry
                .persist(&root.path().join("live-submission-registry.json"))
                .unwrap();
        }
        let mut closing = opening.clone();
        closing.cloid = None;
        closing.position_before = Some(Decimal::new(25, 2));
        closing.exchange_order_id = "528429565565".into();
        closing.trade_id = "838440153201631".into();
        closing.side = "A".into();
        closing.price = Decimal::new(84163, 3);
        closing.closed_pnl = Decimal::new(6595, 4);
        closing.fee = Decimal::new(9089, 6);
        closing.occurred_at_ms = 1_787_841_431_286;
        let cursor = 1_787_810_522_372;
        let ledger_path = root.path().join("live-trading-state.json");
        LiveTradingState::new(Decimal::new(1003, 1), cursor)
            .unwrap()
            .save_atomic(&ledger_path)
            .unwrap();
        let mut retained_root = retained.as_ref().map(|input| {
            let state = root.path().join("retained-state");
            std::fs::create_dir(&state).unwrap();
            for name in ["initialized.json", "unsigned-observer-state.msgpack"] {
                std::fs::copy(input.join("state").join(name), state.join(name)).unwrap();
            }
            for name in ["live-trading-state.json", "live-submission-registry.json"] {
                std::fs::copy(input.join("state/live").join(name), root.path().join(name)).unwrap();
            }
            let marker: serde_json::Value =
                serde_json::from_slice(&std::fs::read(state.join("initialized.json")).unwrap())
                    .unwrap();
            let identity: SnapshotIdentity =
                serde_json::from_value(marker["identity"].clone()).unwrap();
            assert_eq!(
                identity.configuration_sha256,
                derive_config_hash(&config()).unwrap().to_hex()
            );
            assert_eq!(
                identity.risk_policy_sha256,
                derive_risk_policy_hash(&config().global_risk)
                    .unwrap()
                    .to_hex()
            );
            let mut state_root = UnsignedStateRoot::acquire(&state, &identity).unwrap();
            state_root.restore(&mut engine).unwrap();
            assert_eq!(engine.live_confirmed_source_count(), 0);
            assert_eq!(engine.pending_source_wallets().len(), 375);
            state_root
        });
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::new(),
                fills: vec![opening, closing],
                submissions: 0,
                funding: [254, 255, 257, 259, 257, 256, 257, 259]
                    .into_iter()
                    .enumerate()
                    .map(|(i, cost)| FundingRecord {
                        event_hash: format!("0x{}", "00".repeat(32)),
                        asset: "HYPE".into(),
                        signed_usdc_delta: Decimal::new(-cost, 6),
                        occurred_at_ms: [
                            1787814000047,
                            1787817600119,
                            1787821200047,
                            1787824800056,
                            1787828400074,
                            1787832000043,
                            1787835600066,
                            1787839200035,
                        ][i],
                        source_hash: PayloadHash([0; 32]),
                    })
                    .collect(),
            })));
        let now = 1_787_920_000_000;
        if let Some(input) = &retained {
            let fills: serde_json::Value =
                serde_json::from_slice(&std::fs::read(input.join("account-fills.json")).unwrap())
                    .unwrap();
            let funding: serde_json::Value =
                serde_json::from_slice(&std::fs::read(input.join("account-funding.json")).unwrap())
                    .unwrap();
            let current: serde_json::Value =
                serde_json::from_slice(&std::fs::read(input.join("account-current.json")).unwrap())
                    .unwrap();
            assert!(current["assetPositions"].as_array().unwrap().is_empty());
            assert_eq!(fills.as_array().unwrap().len(), 2);
            let truth = transport.0.lock().unwrap();
            for (actual, expected) in fills.as_array().unwrap().iter().zip(&truth.fills) {
                assert_eq!(actual["time"].as_u64(), Some(expected.occurred_at_ms));
                assert_eq!(
                    actual["oid"].as_u64().unwrap().to_string(),
                    expected.exchange_order_id
                );
                assert_eq!(
                    actual["tid"].as_u64().unwrap().to_string(),
                    expected.trade_id
                );
                for (key, value) in [
                    ("px", expected.price),
                    ("sz", expected.quantity),
                    ("fee", expected.fee),
                    ("closedPnl", expected.closed_pnl),
                ] {
                    assert_eq!(
                        actual[key].as_str().unwrap().parse::<Decimal>().unwrap(),
                        value
                    );
                }
            }
            assert_eq!(funding.as_array().unwrap().len(), 8);
            for (actual, expected) in funding.as_array().unwrap().iter().zip(&truth.funding) {
                assert_eq!(actual["time"].as_u64(), Some(expected.occurred_at_ms));
                assert_eq!(
                    actual["delta"]["usdc"]
                        .as_str()
                        .unwrap()
                        .parse::<Decimal>()
                        .unwrap(),
                    expected.signed_usdc_delta
                );
            }
        }
        let initialize = |now| {
            LiveExecutionRuntime::initialize_with_transport(
                transport.clone(),
                ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
                root.path(),
                RiskPolicyHash([0; 32]),
                ConfigHash([0; 32]),
                now,
            )
        };
        let mut runtime = initialize_continuous_execution(&mut engine, initialize(now), now)
            .await
            .unwrap();
        assert!(!runtime.recovery_only(), "{:?}", runtime.state());
        assert!(!engine.execution_recovery_only());
        assert_eq!(runtime.state().position("HYPE"), Decimal::ZERO);
        assert_eq!(engine.ledger().portfolio_position("HYPE"), Decimal::ZERO);
        assert_eq!(runtime.state().verified_fills().len(), 1);
        assert_eq!(runtime.state().external_fills().len(), 1);
        assert_eq!(runtime.state().verified_funding().len(), 8);
        assert_eq!(runtime.state().settled_equity(), Decimal::new(100939553, 6));
        let episode = engine.ledger().portfolio_closed()[0].clone();
        assert_eq!(episode.opened_at, fill.occurred_at);
        assert_eq!(episode.closed_at, 1_787_841_431_286);
        assert_eq!(episode.net_pnl, Decimal::new(639553, 6));
        let (_, mfce_time) = engine.mfce_persistence_snapshot();
        let mfce = engine.mfce_persistence_snapshot().0;
        assert!(
            mfce.samples.is_empty(),
            "missing features must not become supervised labels"
        );
        let provenance = mfce.delayed.provenance.clone();
        let evidence = provenance.episodes.values().next().unwrap();
        assert!(
            evidence.transitions.is_empty(),
            "unknown alpha must not become Source"
        );
        assert!(evidence.settled.as_ref().unwrap().external_manual_exit);
        assert_eq!(
            evidence.settled.as_ref().unwrap().outcome.net_pnl,
            episode.net_pnl
        );
        assert!(mfce_time >= episode.closed_at);
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "engine".into(),
            configuration_sha256: "config".into(),
            risk_policy_sha256: "risk".into(),
        };
        let snapshot = root.path().join("unsigned-observer-state.msgpack");
        let cursor_before = runtime.state().fill_cursor_ms();
        let verified_before = runtime.state().verified_fills().to_vec();
        let external_before = runtime.state().external_fills().to_vec();
        let update = runtime
            .reconcile_if_due(now + 2_000)
            .await
            .unwrap()
            .unwrap();
        apply_authenticated_reconciliation(&mut engine, &mut runtime, update, now + 2_000).unwrap();
        assert!(!runtime.recovery_only());
        let semantic_a = engine.persistence_semantic_fingerprint();
        if let Some(state_root) = retained_root.as_mut() {
            state_root.persist(&mut engine).unwrap();
            let marker: serde_json::Value = serde_json::from_slice(
                &std::fs::read(root.path().join("retained-state/initialized.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                marker["snapshot_schema_version"].as_u64(),
                Some(crate::decision::UNSIGNED_SNAPSHOT_SCHEMA_VERSION.into())
            );
        } else {
            engine.persist_unsigned_state(&snapshot, &identity).unwrap();
        }
        drop(runtime);
        let mut restarted =
            DecisionEngine::new(config(), b"retained-hype", "second", 40_000, 80_000).unwrap();
        prepare(&mut restarted);
        if let Some(state_root) = retained_root.as_mut() {
            state_root.restore(&mut restarted).unwrap();
        } else {
            restarted
                .restore_unsigned_state(&snapshot, &identity)
                .unwrap();
        }
        assert_eq!(
            restarted.persistence_semantic_fingerprint(),
            semantic_a,
            "legacy restore -> current persist -> current restore changed economic state"
        );
        let runtime =
            initialize_continuous_execution(&mut restarted, initialize(now + 4_000), now + 4_000)
                .await
                .unwrap();
        assert_eq!(
            restarted.persistence_semantic_fingerprint(),
            semantic_a,
            "startup reconciliation mutated an already reconciled economic state"
        );
        assert!(!runtime.recovery_only());
        assert_eq!(runtime.state().verified_fills(), verified_before);
        assert_eq!(runtime.state().external_fills(), external_before);
        assert_eq!(restarted.ledger().portfolio_closed(), &[episode]);
        assert_eq!(
            restarted.mfce_persistence_snapshot().0.delayed.provenance,
            provenance
        );
        assert!(runtime.state().fill_cursor_ms() >= cursor_before && cursor_before >= cursor);
        assert_eq!(transport.0.lock().unwrap().submissions, 0);
        if retained.is_some() {
            println!("retained_production_root=PASS restarts=2 mode=NORMAL orders=0 position_HYPE=0 fills=2 funding=8 cursor_monotonic=true episode_net=0.639553 confirmations=0/375");
        }
    }

    #[tokio::test]
    async fn retained_cursor_gap_stays_alive_but_does_not_claim_trading_readiness() {
        let root = tempfile::tempdir().unwrap();
        let config = CopyTradeConfig::from_path(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json"),
        )
        .unwrap();
        // The retained production snapshot has neither pending actions nor
        // accounted fills. Do not seed the missing attribution in this test.
        let mut engine =
            DecisionEngine::new(config, b"retained-gap", "run", 40_000, 80_000).unwrap();
        let (_, mut entry) = crate::decision::tests::live_fill_engine("HYPE");
        entry.occurred_at = 1_787_810_429_913;
        entry.average_fill_price = Decimal::new(81525, 3);
        let opening = recovered_order(root.path(), &entry);
        let mut closing = opening.clone();
        closing.cloid = None;
        closing.exchange_order_id = "528429565565".into();
        closing.trade_id = "838440153201631".into();
        closing.side = "A".into();
        closing.price = Decimal::new(84163, 3);
        closing.closed_pnl = Decimal::new(6595, 4);
        closing.occurred_at_ms = 1_787_841_431_286;
        let cursor = 1_787_810_522_372;
        let ledger_path = root.path().join("live-trading-state.json");
        let mut state = LiveTradingState::new(Decimal::new(1003, 1), cursor).unwrap();
        state.reconcile_equity(Decimal::new(100285446, 6)).unwrap();
        state.save_atomic(&ledger_path).unwrap();
        let before = std::fs::read(&ledger_path).unwrap();
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::new(),
                fills: vec![opening, closing],
                funding: vec![FundingRecord {
                    event_hash: format!("0x{}", "00".repeat(32)),
                    asset: "HYPE".into(),
                    signed_usdc_delta: Decimal::new(-254, 6),
                    occurred_at_ms: cursor + 3_600_000,
                    source_hash: PayloadHash([0; 32]),
                }],
                submissions: 0,
            })));
        let now = 1_787_920_000_000;
        let startup = LiveExecutionRuntime::initialize_with_transport(
            transport.clone(),
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            root.path(),
            RiskPolicyHash([0; 32]),
            ConfigHash([0; 32]),
            now,
        );
        let mut runtime = initialize_continuous_execution(&mut engine, startup, now)
            .await
            .unwrap();
        for offset in [1_101, 2_202, 3_303] {
            assert!(runtime.recovery_only());
            assert!(engine.exchange_risk_positions().unwrap().is_empty());
            crate::decision::tests::assert_recovery_ingestion_progress(&mut engine, offset);
            assert!(matches!(
                runtime.reconcile_if_due(now + offset).await,
                Err(crate::signing::SignerError::ExchangeTruthMismatch(_))
            ));
        }
        assert_eq!(std::fs::read(&ledger_path).unwrap(), before);
        assert_eq!(runtime.state().fill_cursor_ms(), cursor);
        assert!(runtime.state().verified_fills().is_empty());
        assert!(engine.ledger().portfolio_closed().is_empty());
        assert_eq!(transport.0.lock().unwrap().submissions, 0);
        // This is deliberately NOT a convergence/trading PASS: the opening
        // precedes the retained cursor and still requires history repair.
    }

    #[tokio::test]
    async fn continuous_startup_external_close_is_risk_only_not_a_synthetic_mfce_label() {
        let root = tempfile::tempdir().unwrap();
        let (mut engine, fill) = crate::decision::tests::live_fill_engine("HYPE");
        engine.apply_live_execution_fill(&fill).unwrap();
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        crate::domain::live_trading::apply_exchange_fill(&mut state, fill.clone()).unwrap();
        let ledger_path = root.path().join("live-trading-state.json");
        state.save_atomic(&ledger_path).unwrap();
        let mut close = recovered_order(root.path(), &fill);
        close.cloid = None;
        close.position_before = None; // insufficient external linkage stays RISK_ONLY
        close.side = "A".into();
        close.price = Decimal::new(84163, 3);
        close.closed_pnl = Decimal::new(6595, 4);
        close.occurred_at_ms = 20;
        let transport =
            RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                positions: BTreeMap::new(),
                fills: vec![close],
                funding: vec![],
                submissions: 0,
            })));
        let before = std::fs::read(&ledger_path).unwrap();
        let startup = LiveExecutionRuntime::initialize_with_transport(
            transport.clone(),
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            root.path(),
            RiskPolicyHash([0; 32]),
            ConfigHash([0; 32]),
            100,
        );
        let mut runtime = initialize_continuous_execution(&mut engine, startup, 100)
            .await
            .unwrap();
        assert!(runtime.recovery_only());
        assert!(engine.execution_recovery_only());
        assert!(engine.exchange_risk_positions().unwrap().is_empty());
        assert!(engine.construct_next_decision(101).unwrap().is_none());
        assert!(engine.take_prepared_authorized_intents().is_empty());
        assert_eq!(runtime.state().position("HYPE"), Decimal::new(25, 2));
        assert!(engine.ledger().portfolio_closed().is_empty());
        assert!(
            matches!(runtime.reconcile_if_due(1_101).await, Err(crate::signing::SignerError::ExchangeTruthMismatch(reason)) if reason.contains("UNATTRIBUTED_EXTERNAL_FILL"))
        );
        assert_eq!(std::fs::read(&ledger_path).unwrap(), before);
        assert_eq!(transport.0.lock().unwrap().submissions, 0);
    }

    #[tokio::test]
    async fn continuous_startup_equal_positions_is_normal_and_exchange_flat_keeps_local_evidence() {
        for local_position in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (mut engine, fill) = crate::decision::tests::live_fill_engine("HYPE");
            let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
            if local_position {
                crate::domain::live_trading::apply_exchange_fill(&mut state, fill.clone()).unwrap();
                engine.apply_live_execution_fill(&fill).unwrap();
            }
            state
                .save_atomic(root.path().join("live-trading-state.json"))
                .unwrap();
            let transport =
                RecoveryAccount(std::sync::Arc::new(std::sync::Mutex::new(RecoveryTruth {
                    positions: BTreeMap::new(),
                    fills: vec![],
                    funding: vec![],
                    submissions: 0,
                })));
            let runtime = initialize_continuous_execution(
                &mut engine,
                LiveExecutionRuntime::initialize_with_transport(
                    transport.clone(),
                    ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
                    root.path(),
                    RiskPolicyHash([0; 32]),
                    ConfigHash([0; 32]),
                    100,
                ),
                100,
            )
            .await
            .unwrap();
            assert_eq!(runtime.recovery_only(), local_position);
            assert_eq!(engine.execution_recovery_only(), local_position);
            assert!(engine.exchange_risk_positions().unwrap().is_empty());
            assert_eq!(
                runtime.state().position("HYPE"),
                if local_position {
                    Decimal::new(25, 2)
                } else {
                    Decimal::ZERO
                }
            );
            assert_eq!(transport.0.lock().unwrap().submissions, 0);
        }
    }
    fn markets(prefix: &str, count: usize) -> BTreeSet<String> {
        (0..count)
            .map(|index| format!("{prefix}{index:03}"))
            .collect()
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
            crate::domain::scheduler::ManualClock::new(10),
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
        assert!(rearm_idle_stream_reconciliation(
            &config,
            plan,
            50_000,
            true,
            &unresolved,
            &mut due,
        )
        .unwrap());
        assert!(unresolved
            .iter()
            .all(|candidate| due[candidate] != u64::MAX));
        assert!(due
            .iter()
            .all(|(candidate, at)| unresolved.contains(candidate) || *at == u64::MAX));
        assert!(!rearm_idle_stream_reconciliation(
            &config,
            plan,
            60_000,
            true,
            &unresolved,
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
