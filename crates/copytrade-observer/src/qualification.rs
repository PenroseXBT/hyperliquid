use crate::ipc_client::{
    IntentDispatchState, ProductionDispatchHandle, ProductionIntentDispatcher,
};
use crate::live_shadow::{LiveShadowEngine, ProductionIntentIdentity, UnsignedShadowStateIdentity};
use crate::profitability::{summarize_profitability, ProfitabilitySummary};
use crate::public_mainnet::{HyperliquidPublicTransport, PublicTransportPolicy};
use crate::qualification_evidence::{
    load_release_manifest, sha256_file, verify_checkpoint_chain, CheckpointPayload, EvidenceBundle,
    LatencyEvidence, RunHeader, TerminalSummary,
};
use crate::replay::{verify_replay_event_chain, ReplayPayload};
use copytrade_core::decision::{derive_config_hash, derive_risk_policy_hash};
use copytrade_core::scheduler::{
    BudgetClass, Clock, ExecutionOutcome, MonotonicClock, ReadOnlyDataSource,
    ReadOnlySchedulerConfig, ReadRequestKind, RequestKey, RequestPriority, RequestScheduler,
    RequestSubject, ScheduleOutcome, ScheduledReadRequest, SourceTier,
};
use copytrade_core::CopyTradeConfig;
use rust_decimal::prelude::FromPrimitive;
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

pub const MINIMUM_QUALIFICATION_SECONDS: u64 = 86_400;

pub struct QualificationOptions {
    pub config_path: PathBuf,
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
    pub production: Option<ProductionObserverRuntime>,
}

fn persist_unsigned_state(
    engine: &mut LiveShadowEngine,
    state_path: Option<&Path>,
    identity: &UnsignedShadowStateIdentity,
) -> Result<(), Box<dyn Error>> {
    if let Some(path) = state_path {
        engine
            .persist_unsigned_state(path, identity)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct ProductionObserverRuntime {
    pub dispatcher: std::sync::Arc<ProductionIntentDispatcher>,
    pub dispatch_handle: ProductionDispatchHandle,
    pub identity: ProductionIntentIdentity,
    pub state: std::sync::Arc<tokio::sync::Mutex<crate::production_state::ProductionTradingState>>,
    pub state_path: PathBuf,
}

#[derive(Debug, Default, Serialize)]
struct Counters {
    schedule_rejections: u64,
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

pub async fn run_qualification(options: QualificationOptions) -> Result<PathBuf, Box<dyn Error>> {
    if options.production.is_some() && options.state_root.is_some() {
        return Err("unsigned state restoration is forbidden in production mode".into());
    }
    if options.state_root.is_some() && !options.profitability_gate {
        return Err("--state-root is supported only by unsigned profitability runs".into());
    }
    if options.production.is_some() {
        if options.duration_seconds < 60 {
            return Err("production observer duration must be at least 60 seconds".into());
        }
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
    let config = CopyTradeConfig::from_path(&options.config_path)?;
    let scheduler_config = ReadOnlySchedulerConfig::from_path(&options.request_policy_path)?;
    let transport_policy: PublicTransportPolicy =
        serde_json::from_slice(&std::fs::read(&options.transport_policy_path)?)?;
    transport_policy
        .validate()
        .map_err(|error| format!("invalid transport policy: {error:?}"))?;
    let manifest = load_release_manifest(&options.release_manifest_path)?;
    verify_isolation_report(&options.isolation_report_path)?;
    let production_manifest = manifest.qualification_stage == "PRODUCTION_RELEASE";
    let stage_matches =
        manifest_stage_matches(options.production.is_some(), &manifest.qualification_stage);
    let binary_matches = if production_manifest {
        manifest.observer_binary_sha256.as_deref() == Some(&sha256_file(std::env::current_exe()?)?)
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
        &manifest.binary_sha256[..12]
    );
    let output = options.output.join(&run_id);
    let read_policy_value = serde_json::to_value(&scheduler_config)?;
    let transport_policy_value = serde_json::to_value(&transport_policy)?;
    let header = RunHeader {
        stage: if options.micro_density_gate {
            "SU5R-MICRO-DENSITY".into()
        } else if options.transport_gate {
            "HL1K-TRANSPORT-GATE".into()
        } else if options.profitability_gate {
            "HL1-PROFITABILITY-4H".into()
        } else {
            "HL1K".into()
        },
        run_id,
        binary_sha256: manifest.binary_sha256.clone(),
        source_tree_sha256: manifest.source_tree_sha256.clone(),
        git_commit: manifest.git_commit.clone(),
        git_tree_state: manifest.git_tree_state.clone(),
        configuration_sha256: manifest.configuration_sha256.clone(),
        risk_policy_sha256: manifest.risk_policy_sha256.clone(),
        read_policy_sha256: sha256_json(&read_policy_value)?,
        transport_policy_sha256: sha256_json(&transport_policy_value)?,
        start_wall_clock_utc_ms: wall_ms,
        start_monotonic_ms: start,
        expected_minimum_duration_seconds: options.duration_seconds,
        candidate_count: config.candidates.len(),
        process_id: std::process::id(),
        host_identity: std::env::var("HOSTNAME").unwrap_or_else(|_| "unavailable".into()),
        unsigned: true,
        planned_only: true,
        submission_capable: false,
        api_wallet_present: false,
    };
    let qualification_config = serde_json::json!({"copytrade":config,"read_policy":read_policy_value,"transport_policy":transport_policy_value});
    let mut evidence = EvidenceBundle::create(
        &output,
        &options.release_manifest_path,
        &qualification_config,
        &header,
        &options.isolation_report_path,
    )?;
    evidence.append_replay_event(
        start,
        ReplayPayload::RunContext {
            binary_sha256: header.binary_sha256.clone(),
            configuration_sha256: header.configuration_sha256.clone(),
            risk_policy_sha256: header.risk_policy_sha256.clone(),
            read_policy_sha256: header.read_policy_sha256.clone(),
            transport_policy_sha256: header.transport_policy_sha256.clone(),
        },
    )?;
    std::fs::copy(
        std::env::current_exe()?,
        output.join("observer-release-binary"),
    )?;
    evidence.append_log(false, if options.production.is_some() {
        "observer_key_access=false signer_handoff_enabled=true observer_submission_capable=false"
    } else {
        "unsigned=true planned_only=true submission_capable=false api_wallet_present=false"
    })?;
    let scheduler = RequestScheduler::new(
        clock.clone(),
        scheduler_config.api.clone(),
        scheduler_config.retry.clone(),
    )?;
    let transport = HyperliquidPublicTransport::new(clock.clone(), transport_policy.clone())
        .map_err(|error| format!("transport: {error:?}"))?;
    let active_freshness = transport_policy
        .source_deadline_ms(SourceTier::Active)
        .ok_or("active freshness overflow")?;
    let inactive_freshness = transport_policy
        .source_deadline_ms(SourceTier::Inactive)
        .ok_or("inactive freshness overflow")?;
    if config.global_risk.source_snapshot_max_age_ms != inactive_freshness
        || scheduler_config.freshness.configured_max_age_ms != inactive_freshness
    {
        return Err("configured freshness must equal the maximum derived tier deadline".into());
    }
    let mut engine = LiveShadowEngine::new(
        config.clone(),
        manifest.binary_sha256.as_bytes(),
        header.run_id.clone(),
        active_freshness,
        inactive_freshness,
    )
    .map_err(|error| error.to_string())?;
    let state_identity = UnsignedShadowStateIdentity {
        source_tree_sha256: manifest.source_tree_sha256.clone(),
        observer_binary_sha256: sha256_file(std::env::current_exe()?)?,
        configuration_sha256: manifest.configuration_sha256.clone(),
        risk_policy_sha256: manifest.risk_policy_sha256.clone(),
    };
    let state_path = options
        .state_root
        .as_ref()
        .map(|root| root.join("unsigned-shadow-state.json"));
    if let Some(path) = state_path.as_deref() {
        if path.exists() {
            engine
                .restore_unsigned_state(path, &state_identity)
                .map_err(|error| error.to_string())?;
            evidence.append_log(
                false,
                "unsigned_shadow_state=restored identity_verified=true",
            )?;
        } else {
            persist_unsigned_state(&mut engine, Some(path), &state_identity)?;
            evidence.append_log(
                false,
                "unsigned_shadow_state=initialized identity_verified=true",
            )?;
        }
    }
    if let Some(production) = &options.production {
        engine.enable_production_intents(production.identity.clone());
        let mut state = production.state.lock().await;
        production
            .dispatcher
            .reconcile_into(&mut state, &production.state_path)
            .await?;
        engine.synchronize_production_state(&state);
    }
    let mut candidate_tiers = config
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            (
                candidate.address.to_ascii_lowercase(),
                if index < 100 {
                    SourceTier::Active
                } else {
                    SourceTier::Inactive
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut source_due = phase_staggered_source_due(&config, start, 100)?;
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
    let mut profitability_bucket_due = start.checked_add(300_000).ok_or("bucket overflow")?;
    let mut checkpoint_due = 0;
    let mut counters = Counters::default();
    let mut tasks = JoinSet::new();
    let deadline = start
        .checked_add(
            options
                .duration_seconds
                .checked_mul(1000)
                .ok_or("duration overflow")?,
        )
        .ok_or("deadline overflow")?;
    let mut interrupted = false;
    if options.profitability_gate {
        engine.record_equity_boundary(start)?;
        evidence.append_replay_event(start, ReplayPayload::EquityBoundary)?;
        persist_unsigned_state(&mut engine, state_path.as_deref(), &state_identity)?;
    }
    while clock.now_ms() < deadline {
        let now = clock.now_ms();
        if now >= tier_rebalance_due {
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
            let next_tiers =
                assign_source_tiers(&ordered_candidates, &candidates_with_positions, 100);
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
                    phase_deadline(now, 20_000, ordinal, promoted.len())?,
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
        for candidate in &config.candidates {
            let id = candidate.address.to_ascii_lowercase();
            if source_due[&id] <= now {
                let tier = candidate_tiers[&id];
                let interval = match tier {
                    SourceTier::Active => 20_000,
                    SourceTier::Inactive => 60_000,
                };
                enqueue(
                    &scheduler,
                    &scheduler_config,
                    RequestSubject::Candidate(id.clone()),
                    ReadRequestKind::SourceState,
                    // Source cadence is the qualification invariant. Public order-book
                    // enrichment may use only the normal-budget capacity left after it.
                    RequestPriority::High,
                    Some(tier),
                    now,
                    config.global_risk.source_snapshot_max_age_ms,
                    &mut counters,
                )?;
                scheduled.insert(id.clone());
                let previous_due = source_due[&id];
                source_due.insert(id, next_cadence_deadline(previous_due, now, interval)?);
            }
        }
        if market_due <= now {
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
            metadata_due = now + 3_600_000;
        }
        if now >= book_enqueue_due {
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
            // and the hourly metadata burst (20), enrichment stays within 90.
            book_enqueue_due = next_cadence_deadline(book_enqueue_due, now, 2_000)?;
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
                    if let Some(response) = transport.take_accepted(&completed_request) {
                        if let Some(tier) = response.source_tier {
                            *counters.accepted_count_by_tier.entry(tier).or_default() += 1;
                        }
                        let accepted_at = clock.now_ms();
                        evidence.append_replay_event(
                            accepted_at,
                            ReplayPayload::AcceptedResponse {
                                response: response.clone(),
                            },
                        )?;
                        let persist_after_ingest = match &response.payload {
                            crate::public_mainnet::PublicPayload::SourceState(state) => {
                                !state.closed_candles.is_empty()
                            }
                            _ => true,
                        };
                        engine.ingest(response, accepted_at).map_err(|e| e.to_string())?;
                        dispatch_production_intents(
                            &mut engine,
                            options.production.as_ref(),
                        )
                        .await?;
                        if persist_after_ingest {
                            persist_unsigned_state(
                                &mut engine,
                                state_path.as_deref(),
                                &state_identity,
                            )?;
                        }
                    }
                } else {
                    let _ = transport.take_accepted(&completed_request);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                interrupted = true;
                break;
            }
            _ = sleep(Duration::from_millis(25)) => {}
        }
        let now = clock.now_ms();
        if now >= decision_due && now.saturating_add(40_000) < deadline {
            evidence.append_replay_event(now, ReplayPayload::DecisionTick)?;
            let _ = engine
                .construct_next_decision(now)
                .map_err(|e| e.to_string())?;
            dispatch_production_intents(&mut engine, options.production.as_ref()).await?;
            persist_unsigned_state(&mut engine, state_path.as_deref(), &state_identity)?;
            decision_due = now + 20_000;
        }
        if (options.profitability_gate || options.micro_density_gate)
            && now >= profitability_bucket_due
        {
            engine.record_equity_boundary(profitability_bucket_due)?;
            evidence
                .append_replay_event(profitability_bucket_due, ReplayPayload::EquityBoundary)?;
            persist_unsigned_state(&mut engine, state_path.as_deref(), &state_identity)?;
            profitability_bucket_due = profitability_bucket_due
                .checked_add(300_000)
                .ok_or("bucket deadline overflow")?;
        }
        if now >= checkpoint_due {
            let h = scheduler.health();
            let t = transport.metrics();
            let m = engine.metrics();
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
                closed_episode_count: engine.ledger().portfolio_closed().len() as u64,
                persistence_healthy: m.persistence_failures == 0,
                response_rejected_as_stale: counters.response_rejected_as_stale,
                accepted_snapshot_expired_by_age: expired_by_age,
                candidate_never_refreshed: never_refreshed,
                request_expired_in_queue: h.expired_in_queue_by_tier,
                request_superseded_pending: h.superseded_pending_by_tier,
                request_superseded_in_flight: h.superseded_in_flight_by_tier,
                dispatch_count_by_tier: h.dispatch_count_by_tier,
                accepted_count_by_tier: counters.accepted_count_by_tier.clone(),
                queue_wait_ms_by_tier: counters.queue_wait_ms_by_tier.clone(),
                transport_latency_ms_by_kind: counters.transport_latency_ms_by_kind.clone(),
                fresh_candidates_by_tier: fresh_by_tier,
                oldest_pending_age_by_tier: h.oldest_pending_age_ms_by_tier,
            })?;
            engine
                .persist(output.join("shadow-ledger.json"))
                .map_err(|e| e.to_string())?;
            engine
                .persist_target_state(output.join("virtual-target-ledger.json"))
                .map_err(|e| e.to_string())?;
            evidence.write_summary("shadow-actions.json", engine.executions())?;
            evidence.write_summary("decision-plans.json", engine.plans())?;
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
            checkpoint_due = now + 60_000;
        }
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
            if let Some(response) = transport.take_accepted(&completed_request) {
                let accepted_at = clock.now_ms();
                evidence.append_replay_event(
                    accepted_at,
                    ReplayPayload::AcceptedResponse {
                        response: response.clone(),
                    },
                )?;
                engine
                    .ingest(response, accepted_at)
                    .map_err(|e| e.to_string())?;
                dispatch_production_intents(&mut engine, options.production.as_ref()).await?;
                persist_unsigned_state(&mut engine, state_path.as_deref(), &state_identity)?;
            }
        } else {
            let _ = transport.take_accepted(&completed_request);
        }
    }
    if (options.profitability_gate || options.micro_density_gate) && !interrupted {
        match engine.last_equity_boundary() {
            Some(last) if last == deadline => {}
            Some(last) if last < deadline => {
                engine.record_equity_boundary(deadline)?;
                evidence.append_replay_event(deadline, ReplayPayload::EquityBoundary)?;
                persist_unsigned_state(&mut engine, state_path.as_deref(), &state_identity)?;
            }
            Some(_) => return Err("final equity boundary exceeds deadline".into()),
            None => return Err("initial equity boundary missing".into()),
        }
    }
    evidence.flush_replay_events()?;
    engine
        .persist(output.join("shadow-ledger.json"))
        .map_err(|e| e.to_string())?;
    engine
        .persist_target_state(output.join("virtual-target-ledger.json"))
        .map_err(|e| e.to_string())?;
    persist_unsigned_state(&mut engine, state_path.as_deref(), &state_identity)?;
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
    evidence.write_summary("decision-plans.json", engine.plans())?;
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
    if interrupted {
        return Err("HL1K interrupted; qualification clock reset".into());
    }
    if options.transport_gate {
        let gate = evaluate_transport_gate(
            &output,
            &config,
            &candidate_tiers,
            &engine,
            &scheduler.health(),
            &counters,
            transport.metrics().rate_limited.load(Ordering::SeqCst),
        )?;
        evidence.write_summary("transport-gate-summary.json", &gate)?;
        if !gate.passed {
            return Err("short transport qualification failed; diagnostic bundle preserved".into());
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
                return Err("short micro-density gate failed; diagnostic bundle preserved".into());
            }
        } else if !gate.passed {
            return Err("four-hour profitability measurement gate failed; bundle retained".into());
        }
    }
    println!(
        "hl1k_observation_complete=true provisional=true output={}",
        output.display()
    );
    Ok(output)
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
    production: Option<&ProductionObserverRuntime>,
) -> Result<(), Box<dyn Error>> {
    let Some(production) = production else {
        return Ok(());
    };
    for intent in engine.take_prepared_authorized_intents() {
        let cloid = intent.planned_cloid;
        let outcome = production.dispatch_handle.persist_and_queue(intent).await?;
        if outcome == IntentDispatchState::CapacityUnavailableTargetRetained {
            engine.release_unaccepted_production_intent(cloid);
        }
    }
    let mut state = production.state.lock().await;
    production
        .dispatcher
        .reconcile_into(&mut state, &production.state_path)
        .await?;
    let requires_recompute = !state.assets_requiring_replan.is_empty();
    let reconciliation_timestamp = state.live.latest_exchange_event_timestamp();
    engine.synchronize_production_state(&state);
    if requires_recompute {
        engine.recompute_after_production_updates(
            reconciliation_timestamp.ok_or("production replan has no exchange event timestamp")?,
        )?;
        state.assets_requiring_replan.clear();
        state.save_atomic(&production.state_path)?;
    }
    Ok(())
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
    active_capacity: usize,
) -> Result<BTreeMap<String, u64>, Box<dyn Error>> {
    let active_count = active_capacity.min(config.candidates.len());
    let inactive_count = config.candidates.len().saturating_sub(active_count);
    config
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            let (interval, ordinal, count) = if index < active_count {
                (20_000, index, active_count)
            } else {
                (60_000, index - active_count, inactive_count)
            };
            Ok((
                candidate.address.to_ascii_lowercase(),
                phase_deadline(start, interval, ordinal, count)?,
            ))
        })
        .collect()
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
    s.failure_reason = if s.passed {
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
        if kind == ReadRequestKind::SourceState {
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
        other => {
            counters.schedule_rejections += 1;
            Err(format!("scheduler rejection: {other:?}").into())
        }
    }
}

fn append_freshness_decision(
    evidence: &mut EvidenceBundle,
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

#[derive(Debug, Serialize)]
struct TransportGateSummary {
    passed: bool,
    all_candidates_accepted: bool,
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

fn evaluate_transport_gate(
    bundle: &Path,
    config: &CopyTradeConfig,
    tiers: &BTreeMap<String, SourceTier>,
    engine: &LiveShadowEngine,
    health: &copytrade_core::scheduler::SchedulerHealth,
    counters: &Counters,
    rate_limited: u64,
) -> Result<TransportGateSummary, Box<dyn Error>> {
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
    let required_count = |per_minute: u64| -> u64 {
        (u128::from(per_minute) * u128::from(elapsed_ms) / 60_000_u128) as u64
    };
    let required_active = required_count(300);
    let required_inactive = required_count(69);
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
    let passed = all
        && active_count >= required_active
        && inactive_count >= required_inactive
        && active_count.saturating_add(inactive_count) >= required_count(369)
        && fresh == config.candidates.len()
        && health.pending <= health.queue_capacity.min(32)
        && health.in_flight <= counters.maximum_in_flight
        && counters.reserved_weight == 0
        && rate_limited == 0
        && expired == 0
        && oldest < 5_000;
    let _ = tiers;
    Ok(TransportGateSummary {
        passed,
        all_candidates_accepted: all,
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
