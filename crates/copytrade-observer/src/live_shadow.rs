use crate::public_mainnet::{
    AcceptedPublicResponse, CandleResponse, MarketMetadataResponse, MarketSnapshotResponse,
    OrderBookResponse, PublicPayload, SourceStateResponse,
};
use copytrade_core::authorized_intent::{
    AuthorizedExecutionIntent, PreSigningContext, TimeInForce, AUTHORIZED_INTENT_SCHEMA_VERSION,
};
use copytrade_core::configuration::CopyTradeConfig;
use copytrade_core::consensus::{ConsensusInput, SourceExposureBook, SourceExposureState};
use copytrade_core::decision::{
    canonical_target_hash, construct_decision, construct_planned_actions, derive_config_hash,
    derive_planned_cloid, derive_projection_hash, derive_risk_policy_hash,
    hash_market_snapshot_bytes, hash_payload_bytes, DecisionConstructionInput, DecisionRecord,
    EngineInstanceId, ExclusionReason, PlannedCloidInput, PreviousTargetState, Side,
    SnapshotSetMember, SourceEligibilitySummary,
};
use copytrade_core::deployment_equity::{calculate_deployment_equity, DeploymentEquity};
use copytrade_core::execution_floor::{validates_rounded_order, ExecutionFloorPolicy};
use copytrade_core::exit_planning::{plan_risk_reducing_ioc, ExitPlanningBlock, ExitPlanningInput};
use copytrade_core::ledger::DualLedger;
use copytrade_core::portfolio_risk::project_and_validate_portfolio;
use copytrade_core::portfolio_risk::{MarketRules, PortfolioProjectionInput};
use copytrade_core::scheduler::SourceTier;
use copytrade_core::scheduler::{SnapshotAcceptance, SnapshotStore, SourceSnapshot, Timestamp};
use copytrade_core::shadow::{
    execute_shadow_ioc, plan_marketable_ioc, DepthLevel, LatencyScenario, MarketableIocPricingMode,
    ShadowExecution, ShadowExecutionInput, ShadowMarketSnapshot,
};
use copytrade_core::target_state::VirtualTargetLedger;
use copytrade_core::technical::{CandleAcceptance, CostEstimate, TechnicalEngine, TechnicalTarget};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct LiveShadowMetrics {
    pub accepted_source_snapshots: u64,
    pub stale_source_snapshots: u64,
    pub rejected_source_snapshots: u64,
    pub decisions: u64,
    pub projection_violations: u64,
    pub shadow_executions: u64,
    pub unreconciled_shadow_intents: u64,
    pub persistence_failures: u64,
}

fn normalized_directional_weights(
    contributions: Option<&BTreeMap<String, Decimal>>,
    target: Decimal,
) -> Result<BTreeMap<String, Decimal>, LiveShadowError> {
    if target.is_zero() {
        return Ok(BTreeMap::new());
    }
    let selected = contributions
        .into_iter()
        .flat_map(|values| values.iter())
        .filter(|(_, contribution)| {
            !contribution.is_zero() && contribution.is_sign_positive() == target.is_sign_positive()
        })
        .map(|(candidate, contribution)| (candidate.clone(), contribution.abs()))
        .collect::<BTreeMap<_, _>>();
    let total = selected
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(LiveShadowError::Arithmetic)?;
    if total.is_zero() {
        return Ok(BTreeMap::new());
    }
    selected
        .into_iter()
        .map(|(candidate, value)| {
            Ok((
                candidate,
                value
                    .checked_div(total)
                    .ok_or(LiveShadowError::Arithmetic)?,
            ))
        })
        .collect()
}

fn include_held_assets_in_target_universe(
    consensus_inputs: &mut BTreeMap<String, Vec<ConsensusInput>>,
    held_assets: impl IntoIterator<Item = String>,
) {
    for asset in held_assets {
        consensus_inputs.entry(asset).or_default();
    }
}

fn allocate_source_fill(
    current_positions: &BTreeMap<String, Decimal>,
    side: Side,
    filled: Decimal,
    opening_weights: &BTreeMap<String, Decimal>,
) -> Result<BTreeMap<String, Decimal>, LiveShadowError> {
    if filled.is_zero() {
        return Ok(BTreeMap::new());
    }
    let closes = current_positions
        .iter()
        .filter(|(_, position)| match side {
            Side::Buy => position.is_sign_negative(),
            Side::Sell => position.is_sign_positive(),
        })
        .map(|(candidate, position)| (candidate.clone(), position.abs()))
        .collect::<BTreeMap<_, _>>();
    let close_capacity = closes
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(LiveShadowError::Arithmetic)?;
    let closing = filled.min(close_capacity);
    let mut allocations = proportional_allocations(closing, &closes)?;
    let residual = filled
        .checked_sub(closing)
        .ok_or(LiveShadowError::Arithmetic)?;
    if !residual.is_zero() {
        if opening_weights.is_empty() {
            return Err(LiveShadowError::Core(
                "residual source fill has no directional attribution".into(),
            ));
        }
        for (candidate, quantity) in proportional_allocations(residual, opening_weights)? {
            let entry = allocations.entry(candidate).or_default();
            *entry = entry
                .checked_add(quantity)
                .ok_or(LiveShadowError::Arithmetic)?;
        }
    }
    canonicalize_allocation_total(&mut allocations, filled)?;
    let total = allocations
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(LiveShadowError::Arithmetic)?;
    if total != filled {
        return Err(LiveShadowError::Core(
            "source fill quantities do not reconcile".into(),
        ));
    }
    Ok(allocations)
}

fn canonicalize_allocation_total(
    allocations: &mut BTreeMap<String, Decimal>,
    expected_total: Decimal,
) -> Result<(), LiveShadowError> {
    let anchor = allocations
        .keys()
        .next_back()
        .cloned()
        .ok_or_else(|| LiveShadowError::Core("source fill has no allocation anchor".into()))?;
    let prefix = allocations
        .iter()
        .filter(|(candidate, _)| candidate.as_str() != anchor.as_str())
        .try_fold(Decimal::ZERO, |sum, (_, value)| sum.checked_add(*value))
        .ok_or(LiveShadowError::Arithmetic)?;
    let anchor_allocation = expected_total
        .checked_sub(prefix)
        .ok_or(LiveShadowError::Arithmetic)?;
    if anchor_allocation < Decimal::ZERO {
        return Err(LiveShadowError::Core(
            "source allocation residual is negative".into(),
        ));
    }
    allocations.insert(anchor, anchor_allocation);
    Ok(())
}

fn proportional_allocations(
    total: Decimal,
    weights: &BTreeMap<String, Decimal>,
) -> Result<BTreeMap<String, Decimal>, LiveShadowError> {
    if total.is_zero() {
        return Ok(BTreeMap::new());
    }
    let weight_sum = weights
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(LiveShadowError::Arithmetic)?;
    if weight_sum <= Decimal::ZERO {
        return Err(LiveShadowError::Core("invalid attribution weights".into()));
    }
    let mut remaining = total;
    let mut output = BTreeMap::new();
    let len = weights.len();
    for (index, (candidate, weight)) in weights.iter().enumerate() {
        let allocation = if index + 1 == len {
            remaining
        } else {
            total
                .checked_mul(*weight)
                .and_then(|value| value.checked_div(weight_sum))
                .ok_or(LiveShadowError::Arithmetic)?
                .min(remaining)
        };
        remaining = remaining
            .checked_sub(allocation)
            .ok_or(LiveShadowError::Arithmetic)?;
        output.insert(candidate.clone(), allocation);
    }
    Ok(output)
}

fn scaled_source_execution(
    execution: &ShadowExecution,
    quantity: Decimal,
    position_before: Decimal,
) -> Result<ShadowExecution, LiveShadowError> {
    let fraction = quantity
        .checked_div(execution.modeled_filled_quantity)
        .ok_or(LiveShadowError::Arithmetic)?;
    let scale = |value: Decimal| {
        value
            .checked_mul(fraction)
            .ok_or(LiveShadowError::Arithmetic)
    };
    let signed = match execution.action.side {
        Side::Buy => quantity,
        Side::Sell => -quantity,
    };
    let mut source = execution.clone();
    source.action.rounded_notional = scale(execution.action.rounded_notional)?;
    source.rounded_quantity = scale(execution.rounded_quantity)?;
    source.modeled_filled_quantity = quantity;
    source.unfilled_ioc_remainder = source
        .rounded_quantity
        .checked_sub(quantity)
        .ok_or(LiveShadowError::Arithmetic)?;
    source.modeled_filled_notional = scale(execution.modeled_filled_notional)?;
    source.fees = scale(execution.fees)?;
    source.funding = scale(execution.funding)?;
    source.slippage = scale(execution.slippage)?;
    source.position_before = position_before;
    source.position_after = position_before
        .checked_add(signed)
        .ok_or(LiveShadowError::Arithmetic)?;
    Ok(source)
}

fn validate_source_reconciliation(
    ledger: &DualLedger,
    asset: &str,
    portfolio_position: Decimal,
    size_step: Decimal,
) -> Result<(), LiveShadowError> {
    let attributed = ledger
        .source_positions_for_asset(asset)
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(LiveShadowError::Arithmetic)?;
    // Source attribution divides one exchange-valid fill across independent
    // books. Decimal division can leave representation dust far below the
    // asset's exchange quantity precision even though every allocation is
    // conserved. Reconcile at an asset-specific sub-lot quantum rather than
    // treating 1e-28 bookkeeping dust as an economic position mismatch.
    let (difference, tolerance) =
        reconciliation_difference(attributed, portfolio_position, size_step)?;
    if difference > tolerance {
        return Err(LiveShadowError::Core(format!(
            "source positions do not reconcile for {asset}: {attributed} != {portfolio_position} (difference {difference}, tolerance {tolerance})"
        )));
    }
    Ok(())
}

fn reconciliation_difference(
    attributed: Decimal,
    portfolio_position: Decimal,
    size_step: Decimal,
) -> Result<(Decimal, Decimal), LiveShadowError> {
    if size_step <= Decimal::ZERO {
        return Err(LiveShadowError::Core(
            "source reconciliation requires a positive size step".into(),
        ));
    }
    let tolerance = size_step
        .checked_mul(Decimal::new(1, 12))
        .ok_or(LiveShadowError::Arithmetic)?;
    let difference = attributed
        .checked_sub(portfolio_position)
        .ok_or(LiveShadowError::Arithmetic)?
        .abs();
    Ok((difference, tolerance))
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowActionAccounting {
    pub shadow_execution_id: String,
    pub decision_id: String,
    pub asset: String,
    pub side: String,
    pub execution_mode: String,
    pub root_planned_cloid: String,
    pub parent_planned_cloid: Option<String>,
    pub retry_generation: u32,
    pub decision_timestamp_mono: u64,
    pub evaluation_timestamp_mono: u64,
    pub decision_midpoint: Decimal,
    pub modeled_ioc_limit: Decimal,
    pub worst_required_depth_price: Option<Decimal>,
    pub visible_executable_quantity: Decimal,
    pub requested_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub unfilled_quantity: Decimal,
    pub average_fill_price: Option<Decimal>,
    pub filled_notional: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    pub execution_slippage: Decimal,
    pub gross_pnl_delta: Decimal,
    pub net_pnl_delta: Decimal,
    pub portfolio_equity_return: Decimal,
    pub current_equity: Decimal,
    pub settled_equity: Decimal,
    pub deployment_equity: Decimal,
    pub source_attributed_returns: BTreeMap<String, Decimal>,
    pub position_before: Decimal,
    pub position_after: Decimal,
    pub source_filled_quantities: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EquityReturnBucket {
    pub run_id: String,
    pub bucket_id: String,
    pub bucket_index: u64,
    pub opened_at_mono: u64,
    pub closed_at_mono: u64,
    pub starting_equity: Decimal,
    pub ending_equity: Decimal,
    pub return_fraction: Decimal,
    pub source_returns: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecisionPlanningAccounting {
    pub decision_id: String,
    pub created_at_mono: u64,
    pub raw_desired_targets: BTreeMap<String, Decimal>,
    pub constrained_targets: BTreeMap<String, Decimal>,
    pub retained_below_minimum_targets: BTreeMap<String, Decimal>,
    pub proposed_action_notionals: BTreeMap<String, Decimal>,
    pub executable_action_notionals: BTreeMap<String, Decimal>,
    pub below_minimum_action_count: usize,
    pub current_equity: Decimal,
    pub settled_equity: Decimal,
    pub deployment_equity: Decimal,
    pub micro_slots: BTreeMap<String, copytrade_core::decision::MicroPositionSlot>,
}

#[derive(Debug)]
pub enum LiveShadowError {
    Core(String),
    InvalidMarket(String),
    Arithmetic,
    DuplicateBucket,
    InvalidBucketInterval,
}

impl Display for LiveShadowError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl Error for LiveShadowError {}

#[derive(Clone)]
struct PendingShadow {
    action: copytrade_core::decision::PlannedAction,
    root_planned_cloid: String,
    parent_planned_cloid: Option<String>,
    decision_book: ShadowMarketSnapshot,
    source_weights: BTreeMap<String, Decimal>,
    price_tick: Decimal,
    size_step: Decimal,
    projection_input: PortfolioProjectionInput,
    risk_policy_hash: copytrade_core::decision::RiskPolicyHash,
    configuration_hash: copytrade_core::decision::ConfigHash,
}

#[derive(Clone)]
struct PendingBookIntent {
    original_decision_id: String,
    original_planned_cloid: String,
    first_seen_at_mono: Timestamp,
}

#[derive(Clone)]
struct ContinuationIntent {
    root_planned_cloid: String,
    parent_planned_cloid: String,
    next_retry_generation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookActionResolution {
    AwaitBook,
    RecomputeWithFreshBook,
    InvalidateAfterRecompute,
    NoIntent,
}

fn resolve_book_action(
    waiting: bool,
    fresh_book: bool,
    executable_now: bool,
) -> BookActionResolution {
    match (waiting, fresh_book, executable_now) {
        (_, false, true) => BookActionResolution::AwaitBook,
        (true, true, true) => BookActionResolution::RecomputeWithFreshBook,
        (true, true, false) => BookActionResolution::InvalidateAfterRecompute,
        _ => BookActionResolution::NoIntent,
    }
}

fn retain_pending_book_intent(
    pending: &mut BTreeMap<String, PendingBookIntent>,
    asset: String,
    decision_id: String,
    planned_cloid: String,
    now: Timestamp,
) -> bool {
    if pending.contains_key(&asset) {
        return false;
    }
    pending.insert(
        asset,
        PendingBookIntent {
            original_decision_id: decision_id,
            original_planned_cloid: planned_cloid,
            first_seen_at_mono: now,
        },
    );
    true
}

fn reidentify_continuation_action(
    engine_instance_id: EngineInstanceId,
    action: &mut copytrade_core::decision::PlannedAction,
    retry_generation: u32,
) -> Result<(), LiveShadowError> {
    action.retry_generation = retry_generation;
    action.planned_cloid = derive_planned_cloid(&PlannedCloidInput {
        engine_instance_id,
        decision_id: action.decision_id,
        target_version: action.target_version,
        asset: action.asset.clone(),
        side: action.side,
        reduce_only: action.reduce_only,
        action_ordinal: action.action_ordinal,
        retry_generation,
    })
    .map_err(core)?;
    Ok(())
}

fn parse_planned_cloid(
    value: &str,
) -> Result<copytrade_core::decision::PlannedCloid, LiveShadowError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 32 {
        return Err(LiveShadowError::Core(
            "invalid planned CLOID encoding".into(),
        ));
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| LiveShadowError::Core("invalid planned CLOID encoding".into()))?;
    }
    Ok(copytrade_core::decision::PlannedCloid(bytes))
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookEvaluationOutcome {
    PendingBookEvaluation,
    RecomputedAction,
    NoLongerRequired,
    BelowExchangeMinimumAfterCurrentRecompute,
}

#[derive(Debug, Clone, Serialize)]
pub struct BookEvaluationEvent {
    pub asset: String,
    pub original_decision_id: String,
    pub original_planned_cloid: String,
    pub recomputed_decision_id: Option<String>,
    pub observed_at_mono: Timestamp,
    pub waited_ms: u64,
    pub recomputed_notional: Option<Decimal>,
    pub outcome: BookEvaluationOutcome,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionAttemptOutcome {
    ExecutedFully,
    ExecutedPartiallyRemainderReplanned,
    SupersededByNewTarget,
    NoLongerRequired,
    BlockedByCurrentRisk,
    BelowExchangeMinimumAfterCurrentRecompute,
    ExchangeRejected,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActionLifecycleEvent {
    pub asset: String,
    pub decision_id: String,
    pub planned_cloid: String,
    pub root_planned_cloid: String,
    pub parent_planned_cloid: Option<String>,
    pub retry_generation: u32,
    pub observed_at_mono: Timestamp,
    pub requested_notional: Decimal,
    pub filled_quantity: Option<Decimal>,
    pub unfilled_quantity: Option<Decimal>,
    pub outcome: ActionAttemptOutcome,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutableDensitySummary {
    pub global_risk_scale: f64,
    pub root_actionable_targets: usize,
    pub initial_ioc_attempts: usize,
    pub depth_priced_attempts: usize,
    pub fallback_attempts: usize,
    pub full_fills: usize,
    pub partial_fills: usize,
    pub zero_fills: usize,
    pub continuation_attempts: usize,
    pub maximum_continuation_generation: u32,
    pub terminally_satisfied_targets: usize,
    pub no_longer_required_targets: usize,
    pub below_minimum_current_residuals: usize,
    pub hard_blocked_or_rejected_targets: usize,
    pub unresolved_actionable_targets: usize,
    pub opened_portfolio_episodes: usize,
    pub closed_portfolio_episodes: usize,
    pub fees: Decimal,
    pub funding: Decimal,
    pub slippage: Decimal,
    pub net_pnl: Decimal,
    pub root_conservation_verified: bool,
    pub raw_nonzero_target_changes: u64,
    pub target_changes_above_dynamic_minimum: u64,
    pub admitted_new_positions: u64,
    pub exits: u64,
    pub rotations: u64,
    pub maximum_admitted_micro_slots: usize,
    pub gross_turnover: Decimal,
    pub realized_net_pnl: Decimal,
    pub net_pnl_per_dollar_traded: Decimal,
    pub closed_episodes_per_hour: Decimal,
}

#[derive(Debug, Default)]
struct MicroDensityCounters {
    previous_raw: BTreeMap<String, Decimal>,
    previous_admitted: BTreeMap<String, Decimal>,
    raw_nonzero_target_changes: u64,
    target_changes_above_dynamic_minimum: u64,
    admitted_new_positions: u64,
    exits: u64,
    rotations: u64,
    maximum_admitted_micro_slots: usize,
}

pub struct LiveShadowEngine {
    config: CopyTradeConfig,
    run_id: String,
    engine_instance: EngineInstanceId,
    source_store: SnapshotStore<SourceStateResponse>,
    source_exposure_book: SourceExposureBook,
    target_ledger: VirtualTargetLedger,
    source_sequences: BTreeMap<String, u64>,
    source_tiers: BTreeMap<String, SourceTier>,
    active_freshness_ms: u64,
    inactive_freshness_ms: u64,
    mids: Option<(MarketSnapshotResponse, Timestamp)>,
    metadata: Option<MarketMetadataResponse>,
    books: BTreeMap<String, (OrderBookResponse, Timestamp)>,
    previous_target: Option<PreviousTargetState>,
    decision_sequence: u64,
    pending: BTreeMap<String, PendingShadow>,
    pending_book: BTreeMap<String, PendingBookIntent>,
    continuations: BTreeMap<String, ContinuationIntent>,
    desired_books: BTreeSet<String>,
    ledger: DualLedger,
    metrics: LiveShadowMetrics,
    executions: Vec<ShadowActionAccounting>,
    equity_buckets: Vec<EquityReturnBucket>,
    plans: Vec<DecisionPlanningAccounting>,
    book_evaluation_events: Vec<BookEvaluationEvent>,
    action_lifecycle_events: Vec<ActionLifecycleEvent>,
    last_bucket: Option<(Timestamp, Decimal, BTreeMap<String, Decimal>)>,
    last_funding_accrual: Option<Timestamp>,
    accrued_funding: BTreeMap<String, Decimal>,
    micro_density: MicroDensityCounters,
    prepared_authorized_intents: Vec<AuthorizedExecutionIntent>,
    emitted_production_cloids: BTreeSet<copytrade_core::decision::PlannedCloid>,
    production_identity: Option<ProductionIntentIdentity>,
    production_positions: Option<BTreeMap<String, Decimal>>,
    production_equities: Option<(Decimal, Decimal, Decimal)>,
    production_exposure_blocked: bool,
    technical_engine: TechnicalEngine,
    technical_targets: BTreeMap<String, TechnicalTarget>,
}

#[derive(Debug, Clone)]
pub struct ProductionIntentIdentity {
    pub observer_release_hash: [u8; 32],
    pub signer_release_hash: [u8; 32],
    pub release_manifest_hash: [u8; 32],
    pub market_rules_hash: [u8; 32],
    pub dynamic_floor_policy_hash: [u8; 32],
    pub ioc_policy_hash: [u8; 32],
    pub expires_after_ms: u64,
}

impl LiveShadowEngine {
    pub fn new(
        config: CopyTradeConfig,
        instance_seed: &[u8],
        run_id: impl Into<String>,
        active_freshness_ms: u64,
        inactive_freshness_ms: u64,
    ) -> Result<Self, LiveShadowError> {
        let digest = Sha256::digest(instance_seed);
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        let source_store = SnapshotStore::new(copytrade_core::scheduler::FreshnessPolicy {
            configured_max_age_ms: config.global_risk.source_snapshot_max_age_ms,
            maximum_future_skew_ms: 2_000,
        })
        .map_err(|error| LiveShadowError::Core(error.to_string()))?;
        let technical_engine = TechnicalEngine::new(config.technical.clone()).map_err(|error| {
            LiveShadowError::Core(format!("invalid technical configuration: {error:?}"))
        })?;
        Ok(Self {
            config,
            run_id: run_id.into(),
            engine_instance: EngineInstanceId(id),
            source_store,
            source_exposure_book: SourceExposureBook::default(),
            target_ledger: VirtualTargetLedger::default(),
            source_sequences: BTreeMap::new(),
            source_tiers: BTreeMap::new(),
            active_freshness_ms,
            inactive_freshness_ms,
            mids: None,
            metadata: None,
            books: BTreeMap::new(),
            previous_target: None,
            decision_sequence: 0,
            pending: BTreeMap::new(),
            pending_book: BTreeMap::new(),
            continuations: BTreeMap::new(),
            desired_books: BTreeSet::new(),
            ledger: DualLedger::default(),
            metrics: LiveShadowMetrics::default(),
            executions: Vec::new(),
            equity_buckets: Vec::new(),
            plans: Vec::new(),
            book_evaluation_events: Vec::new(),
            action_lifecycle_events: Vec::new(),
            last_bucket: None,
            last_funding_accrual: None,
            accrued_funding: BTreeMap::new(),
            micro_density: MicroDensityCounters::default(),
            prepared_authorized_intents: Vec::new(),
            emitted_production_cloids: BTreeSet::new(),
            production_identity: None,
            production_positions: None,
            production_equities: None,
            production_exposure_blocked: false,
            technical_engine,
            technical_targets: BTreeMap::new(),
        })
    }

    /// Enables production intent emission. Pricing, floor, allocation and risk
    /// remain the same authoritative calculations used by the observer.
    pub fn enable_production_intents(&mut self, identity: ProductionIntentIdentity) {
        self.production_identity = Some(identity);
    }

    pub fn take_prepared_authorized_intents(&mut self) -> Vec<AuthorizedExecutionIntent> {
        std::mem::take(&mut self.prepared_authorized_intents)
    }

    pub fn release_unaccepted_production_intent(
        &mut self,
        cloid: copytrade_core::decision::PlannedCloid,
    ) {
        self.emitted_production_cloids.remove(&cloid);
    }

    pub fn synchronize_production_state(
        &mut self,
        state: &crate::production_state::ProductionTradingState,
    ) {
        self.production_positions = Some(state.live.positions());
        self.production_equities = Some((
            state.live.current_equity(),
            state.live.settled_equity(),
            state.live.deployment_equity(),
        ));
        self.production_exposure_blocked =
            !state.position_mismatches.is_empty() || state.equity_mismatch.is_some();
    }

    pub fn recompute_after_production_updates(
        &mut self,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        if self.production_identity.is_some() {
            self.construct_next_decision(now)?;
        }
        Ok(())
    }

    fn authoritative_position(&self, asset: &str) -> Decimal {
        self.production_positions
            .as_ref()
            .and_then(|positions| positions.get(asset).copied())
            .unwrap_or_else(|| self.ledger.portfolio_position(asset))
    }

    fn authoritative_assets(&self) -> Vec<String> {
        self.production_positions
            .as_ref()
            .map(|positions| positions.keys().cloned().collect())
            .unwrap_or_else(|| self.ledger.portfolio_assets())
    }

    pub fn ingest(
        &mut self,
        response: AcceptedPublicResponse,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        match response.payload {
            PublicPayload::SourceState(state) => {
                let candidate_id = state.candidate_id.clone();
                let sequence = self
                    .source_sequences
                    .entry(candidate_id.clone())
                    .or_insert(0);
                *sequence = sequence.checked_add(1).ok_or(LiveShadowError::Arithmetic)?;
                let payload = serde_json::to_vec(&state)
                    .map_err(|error| LiveShadowError::Core(error.to_string()))?;
                let payload_hash = hash_payload_bytes(&payload);
                let exposures = state
                    .positions
                    .iter()
                    .map(|(asset, position)| {
                        let exposure = position
                            .signed_notional
                            .checked_div(state.account_value)
                            .ok_or(LiveShadowError::Arithmetic)?;
                        Ok((asset.clone(), exposure))
                    })
                    .collect::<Result<BTreeMap<_, _>, LiveShadowError>>()?;
                let exposure_state = SourceExposureState {
                    accepted_sequence: *sequence,
                    payload_hash,
                    exposures,
                };
                let snapshot = SourceSnapshot {
                    candidate_id: state.candidate_id.clone(),
                    payload: state,
                    requested_at: response.requested_at_mono,
                    received_at: response.received_at_mono,
                    source_observed_at: None,
                    valid_until: response.valid_until_mono,
                    sequence: *sequence,
                };
                match self.source_store.accept(snapshot, now, true) {
                    SnapshotAcceptance::Accepted => {
                        self.source_exposure_book
                            .accept(candidate_id, exposure_state);
                        self.metrics.accepted_source_snapshots += 1;
                    }
                    SnapshotAcceptance::Expired => self.metrics.stale_source_snapshots += 1,
                    _ => self.metrics.rejected_source_snapshots += 1,
                }
            }
            PublicPayload::MarketSnapshot(mids) => {
                self.mids = Some((mids, response.received_at_mono))
            }
            PublicPayload::MarketMetadata(metadata) => {
                self.accrue_funding(response.received_at_mono)?;
                self.metadata = Some(metadata);
                self.last_funding_accrual = Some(response.received_at_mono);
            }
            PublicPayload::OrderBook(book) => {
                let asset = book.asset.clone();
                let triggers_recompute = self.pending_book.contains_key(&asset);
                self.books
                    .insert(asset.clone(), (book.clone(), response.received_at_mono));
                let remainder_requires_recompute =
                    self.try_execute_pending(&book, response.received_at_mono)?;
                if triggers_recompute || remainder_requires_recompute {
                    self.construct_next_decision(response.received_at_mono)?;
                }
            }
            PublicPayload::Candle(CandleResponse { candle }) => {
                if self
                    .technical_engine
                    .accept_closed_candle(candle)
                    .map_err(|error| {
                        LiveShadowError::Core(format!("invalid closed candle: {error:?}"))
                    })?
                    == CandleAcceptance::Accepted
                {
                    self.construct_next_decision(response.received_at_mono)?;
                }
            }
        }
        Ok(())
    }

    pub fn active_source_count(&self, now: Timestamp) -> usize {
        self.config
            .candidates
            .iter()
            .filter(|candidate| {
                self.fresh_source(&candidate.address.to_ascii_lowercase(), now)
                    .is_some()
            })
            .count()
    }

    pub fn assets_requiring_books(&self, now: Timestamp) -> BTreeSet<String> {
        let _ = now;
        self.desired_books.clone()
    }

    pub fn urgent_book_assets(&self) -> BTreeSet<String> {
        self.pending_book
            .keys()
            .chain(self.continuations.keys())
            .cloned()
            .collect()
    }

    pub fn latest_source_has_position(&self, candidate: &str) -> bool {
        self.source_store
            .latest(&candidate.to_ascii_lowercase())
            .is_some_and(|snapshot| !snapshot.payload.positions.is_empty())
    }

    pub fn source_is_fresh(&self, candidate: &str, now: Timestamp) -> bool {
        self.fresh_source(&candidate.to_ascii_lowercase(), now)
            .is_some()
    }

    pub fn set_source_tier(&mut self, candidate: &str, tier: SourceTier) {
        self.source_tiers
            .insert(candidate.to_ascii_lowercase(), tier);
    }

    fn fresh_source(
        &self,
        candidate: &str,
        now: Timestamp,
    ) -> Option<&SourceSnapshot<SourceStateResponse>> {
        let snapshot = self.source_store.fresh(candidate, now)?;
        let deadline = match self
            .source_tiers
            .get(candidate)
            .copied()
            .unwrap_or(SourceTier::Inactive)
        {
            SourceTier::Active => self.active_freshness_ms,
            SourceTier::Inactive => self.inactive_freshness_ms,
        };
        (now.saturating_sub(snapshot.received_at) <= deadline).then_some(snapshot)
    }

    pub fn source_ever_accepted(&self, candidate: &str) -> bool {
        self.source_sequences
            .contains_key(&candidate.to_ascii_lowercase())
    }

    pub fn construct_next_decision(
        &mut self,
        now: Timestamp,
    ) -> Result<Option<DecisionRecord>, LiveShadowError> {
        self.accrue_funding(now)?;
        let Some((mids, mids_received)) = &self.mids else {
            return Ok(None);
        };
        if now.saturating_sub(*mids_received) > self.config.global_risk.source_snapshot_max_age_ms {
            return Ok(None);
        }
        let Some(metadata) = &self.metadata else {
            return Ok(None);
        };
        let mut eligibility = SourceEligibilitySummary::default();
        let mut members = Vec::new();
        let mut consensus_inputs: BTreeMap<String, Vec<ConsensusInput>> = BTreeMap::new();
        let mut source_contributions: BTreeMap<String, BTreeMap<String, Decimal>> = BTreeMap::new();
        let source_budget = self.config.technical.source_budget_fraction;
        for candidate in &self.config.candidates {
            let id = candidate.address.to_ascii_lowercase();
            if !candidate.enabled {
                eligibility.excluded.insert(id, ExclusionReason::Disabled);
                continue;
            }
            let Some(snapshot) = self.fresh_source(&id, now) else {
                eligibility.excluded.insert(id, ExclusionReason::Stale);
                continue;
            };
            let exposure_state = self.source_exposure_book.get(&id).ok_or_else(|| {
                LiveShadowError::Core(format!("missing absolute exposure state for {id}"))
            })?;
            if exposure_state.accepted_sequence != snapshot.sequence {
                return Err(LiveShadowError::Core(format!(
                    "exposure state sequence mismatch for {id}"
                )));
            }
            eligibility.active_ids.insert(id.clone());
            members.push(SnapshotSetMember {
                candidate_id: id.clone(),
                accepted_sequence: snapshot.sequence,
                payload_hash: exposure_state.payload_hash,
                received_at_mono: snapshot.received_at,
                valid_until_mono: snapshot.valid_until,
            });
            for (asset, absolute_exposure) in &exposure_state.exposures {
                let exposure = absolute_exposure
                    .to_f64()
                    .ok_or(LiveShadowError::Arithmetic)?;
                consensus_inputs
                    .entry(asset.clone())
                    .or_default()
                    .push(ConsensusInput {
                        candidate_id: id.clone(),
                        allocation_weight: candidate.allocation_weight * source_budget,
                        confidence_modifier: candidate
                            .confidence_modifier
                            .ok_or_else(|| LiveShadowError::Core("missing confidence".into()))?,
                        source_exposure: exposure,
                        enabled: true,
                        quarantined: false,
                        snapshot_age_ms: now.saturating_sub(snapshot.received_at),
                    });
                let confidence = candidate
                    .confidence_modifier
                    .ok_or_else(|| LiveShadowError::Core("missing confidence".into()))?;
                let bounded = exposure.clamp(
                    -self.config.global_risk.max_source_exposure,
                    self.config.global_risk.max_source_exposure,
                );
                let contribution = Decimal::from_f64(
                    candidate.allocation_weight * source_budget * confidence * bounded,
                )
                .ok_or(LiveShadowError::Arithmetic)?;
                source_contributions
                    .entry(asset.clone())
                    .or_default()
                    .insert(id.clone(), contribution);
            }
        }
        for asset in self.technical_engine.tracked_assets() {
            consensus_inputs.entry(asset.clone()).or_default();
        }
        if eligibility.active_ids.is_empty() {
            return Ok(None);
        }
        // Assets already held by the follower remain in the target universe
        // even after every source goes flat. An empty consensus input produces
        // target zero and therefore an explicit flattening delta.
        include_held_assets_in_target_universe(&mut consensus_inputs, self.authoritative_assets());
        let market_rules = build_market_rules(mids, metadata, consensus_inputs.keys())?;
        let execution_rules = market_rules.clone();
        let filled_positions: BTreeMap<String, Decimal> = market_rules
            .iter()
            .filter_map(|(asset, rules)| {
                let quantity = self.authoritative_position(asset);
                (!quantity.is_zero())
                    .then(|| {
                        quantity
                            .checked_mul(rules.mark_price)
                            .map(|notional| (asset.clone(), notional))
                    })
                    .flatten()
            })
            .collect();
        let deployment = self.deployment_equity()?;
        let equity = deployment.deployment_equity;
        let equity_f64 = equity.to_f64().ok_or(LiveShadowError::Arithmetic)?;
        let leverage = Decimal::from_f64(curve_leverage(&self.config, equity_f64)?)
            .ok_or(LiveShadowError::Arithmetic)?;
        let available_gross = equity
            .checked_mul(leverage)
            .and_then(|value| {
                value.checked_mul(Decimal::from_f64(
                    self.config.global_risk.global_risk_scale,
                )?)
            })
            .ok_or(LiveShadowError::Arithmetic)?;
        let technical_assets = self
            .technical_engine
            .tracked_assets()
            .cloned()
            .collect::<Vec<_>>();
        self.technical_targets.clear();
        if self.config.technical.enabled {
            for asset in technical_assets {
                let hourly_funding = metadata
                    .contexts
                    .get(&asset)
                    .and_then(|context| context.funding_rate_hourly.abs().to_f64())
                    .unwrap_or_default();
                let fee = self.config.taker_fee_bps / 10_000.0;
                let slippage = self.config.execution.max_slippage_bps / 10_000.0;
                let costs = CostEstimate {
                    entry_fee_fraction: fee,
                    exit_fee_fraction: fee,
                    entry_slippage_fraction: slippage,
                    exit_slippage_fraction: slippage,
                    hourly_funding_fraction: hourly_funding,
                };
                if let Some(target) = self
                    .technical_engine
                    .evaluate(&asset, available_gross, costs)
                    .map_err(|error| {
                        LiveShadowError::Core(format!(
                            "technical evaluation failed for {asset}: {error:?}"
                        ))
                    })?
                {
                    let score = target.score.to_f64().ok_or(LiveShadowError::Arithmetic)?;
                    let component_id =
                        format!("technical:{:?}", target.archetype).to_ascii_lowercase();
                    consensus_inputs
                        .entry(asset.clone())
                        .or_default()
                        .push(ConsensusInput {
                            candidate_id: component_id.clone(),
                            allocation_weight: self.config.technical.technical_budget_fraction,
                            confidence_modifier: 1.0,
                            source_exposure: score,
                            enabled: true,
                            quarantined: false,
                            snapshot_age_ms: 0,
                        });
                    source_contributions
                        .entry(asset.clone())
                        .or_default()
                        .insert(
                            component_id,
                            Decimal::from_f64(
                                self.config.technical.technical_budget_fraction * score,
                            )
                            .ok_or(LiveShadowError::Arithmetic)?,
                        );
                    self.technical_targets.insert(asset, target);
                }
            }
        }
        let projection_filled_positions = filled_positions.clone();
        let market_bytes =
            serde_json::to_vec(mids).map_err(|error| LiveShadowError::Core(error.to_string()))?;
        let projection_input = PortfolioProjectionInput {
            current_equity: equity,
            curve_leverage: leverage,
            global_risk_scale: Decimal::from_f64(self.config.global_risk.global_risk_scale)
                .ok_or(LiveShadowError::Arithmetic)?,
            max_single_asset_equity_pct: Decimal::from_f64(
                self.config.global_risk.max_single_asset_equity_pct,
            )
            .ok_or(LiveShadowError::Arithmetic)?,
            max_net_equity_pct: Decimal::from_f64(self.config.global_risk.max_net_equity_pct)
                .ok_or(LiveShadowError::Arithmetic)?,
            filled_positions,
            filled_position_state_complete: true,
            acknowledged_open_orders: vec![],
            open_order_state_complete: true,
            unconstrained_targets: BTreeMap::new(),
            market_rules,
        };
        let decision = construct_decision(DecisionConstructionInput {
            engine_instance_id: self.engine_instance,
            decision_sequence: self.decision_sequence,
            config_hash: derive_config_hash(&self.config).map_err(core)?,
            risk_policy_hash: derive_risk_policy_hash(&self.config.global_risk).map_err(core)?,
            snapshot_members: members,
            eligibility,
            market_snapshot_id: hash_market_snapshot_bytes(&market_bytes),
            consensus_inputs,
            maximum_source_exposure: self.config.global_risk.max_source_exposure,
            source_snapshot_max_age_ms: self.config.global_risk.source_snapshot_max_age_ms,
            execution_floor_policy: ExecutionFloorPolicy {
                exchange_minimum_notional: Decimal::from_f64(
                    self.config.global_risk.min_order_notional_usd,
                )
                .ok_or(LiveShadowError::Arithmetic)?,
                rounding_buffer: Decimal::from_f64(
                    self.config.global_risk.order_rounding_buffer_usd,
                )
                .ok_or(LiveShadowError::Arithmetic)?,
                closeability_margin: Decimal::from_f64(
                    self.config.global_risk.closeability_margin_usd,
                )
                .ok_or(LiveShadowError::Arithmetic)?,
                maximum_slippage_fraction: Decimal::from_f64(
                    self.config.execution.max_slippage_bps / 10_000.0,
                )
                .ok_or(LiveShadowError::Arithmetic)?,
            },
            slot_rank_hysteresis: Decimal::from_f64(
                self.config.global_risk.slot_rank_hysteresis,
            )
            .ok_or(LiveShadowError::Arithmetic)?,
            projection_input: projection_input.clone(),
            previous_target: self.previous_target.clone(),
            created_at_mono: now,
        })
        .map_err(|error| {
            self.metrics.projection_violations += 1;
            LiveShadowError::Core(format!(
                "{error}; equity={equity}; leverage={leverage}; filled_positions={projection_filled_positions:?}"
            ))
        })?;
        self.target_ledger
            .replace_absolute_targets(
                &decision.unconstrained_targets,
                &decision.projection.constrained_targets,
                &projection_filled_positions,
                &BTreeMap::new(),
                &BTreeMap::new(),
                decision.target_version,
                decision.snapshot_set_id,
            )
            .map_err(core)?;
        self.previous_target = Some(PreviousTargetState {
            version: decision.target_version,
            target_hash: decision.target_hash,
        });
        self.decision_sequence = self
            .decision_sequence
            .checked_add(1)
            .ok_or(LiveShadowError::Arithmetic)?;
        self.metrics.decisions += 1;
        self.record_micro_density(&decision)?;
        let reduce_only_by_asset = decision
            .projection
            .rounded_deltas
            .iter()
            .filter_map(|(asset, delta)| {
                let filled = projection_filled_positions
                    .get(asset)
                    .copied()
                    .unwrap_or_default();
                requires_close_first(filled, *delta).then(|| (asset.clone(), true))
            })
            .collect::<BTreeMap<_, _>>();
        let mut actions =
            construct_planned_actions(self.engine_instance, &decision, &reduce_only_by_asset)
                .map_err(core)?;
        for action in &mut actions {
            let Some(continuation) = self.continuations.get(&action.asset) else {
                continue;
            };
            reidentify_continuation_action(
                self.engine_instance,
                action,
                continuation.next_retry_generation,
            )?;
        }
        let proposed_action_notionals = actions
            .iter()
            .map(|action| (action.asset.clone(), action.rounded_notional))
            .collect::<BTreeMap<_, _>>();
        let executable_actions = actions
            .into_iter()
            .filter(|action| !self.production_exposure_blocked || action.reduce_only)
            .filter(|action| {
                decision.micro_slots.get(&action.asset).is_some_and(|slot| {
                    let required = if slot.filled_notional.is_zero() {
                        slot.execution_floor.minimum_opening_notional
                    } else {
                        slot.execution_floor.minimum_order_notional
                    };
                    action.rounded_notional >= required
                })
            })
            .collect::<Vec<_>>();
        let executable_action_notionals = executable_actions
            .iter()
            .map(|action| (action.asset.clone(), action.rounded_notional))
            .collect::<BTreeMap<_, _>>();
        self.plans.push(DecisionPlanningAccounting {
            decision_id: decision.decision_id.to_string(),
            created_at_mono: now,
            raw_desired_targets: decision.unconstrained_targets.clone(),
            constrained_targets: decision.projection.constrained_targets.clone(),
            retained_below_minimum_targets: decision.retained_below_minimum.clone(),
            below_minimum_action_count: proposed_action_notionals
                .len()
                .saturating_sub(executable_action_notionals.len()),
            current_equity: deployment.current_equity,
            settled_equity: deployment.settled_equity,
            deployment_equity: deployment.deployment_equity,
            micro_slots: decision.micro_slots.clone(),
            proposed_action_notionals: proposed_action_notionals.clone(),
            executable_action_notionals,
        });
        let executable_by_asset = executable_actions
            .into_iter()
            .map(|action| (action.asset.clone(), action))
            .collect::<BTreeMap<_, _>>();
        let waiting_assets = self.pending_book.keys().cloned().collect::<Vec<_>>();
        for asset in waiting_assets {
            let fresh_book = self
                .books
                .get(&asset)
                .is_some_and(|(_, received_at)| now.saturating_sub(*received_at) <= 40_000);
            if resolve_book_action(true, fresh_book, executable_by_asset.contains_key(&asset))
                == BookActionResolution::InvalidateAfterRecompute
            {
                let intent = self
                    .pending_book
                    .remove(&asset)
                    .ok_or_else(|| LiveShadowError::Core("missing pending book intent".into()))?;
                self.pending.remove(&asset);
                let recomputed = proposed_action_notionals.get(&asset).copied();
                let outcome = if recomputed.is_some_and(|value| !value.is_zero()) {
                    BookEvaluationOutcome::BelowExchangeMinimumAfterCurrentRecompute
                } else {
                    BookEvaluationOutcome::NoLongerRequired
                };
                self.book_evaluation_events.push(BookEvaluationEvent {
                    asset: asset.clone(),
                    original_decision_id: intent.original_decision_id,
                    original_planned_cloid: intent.original_planned_cloid,
                    recomputed_decision_id: Some(decision.decision_id.to_string()),
                    observed_at_mono: now,
                    waited_ms: now.saturating_sub(intent.first_seen_at_mono),
                    recomputed_notional: recomputed,
                    outcome,
                });
            }
        }
        let completed_continuations = self
            .continuations
            .keys()
            .filter(|asset| !executable_by_asset.contains_key(*asset))
            .cloned()
            .collect::<Vec<_>>();
        for asset in completed_continuations {
            let continuation = self
                .continuations
                .remove(&asset)
                .ok_or_else(|| LiveShadowError::Core("missing continuation".into()))?;
            let proposed = proposed_action_notionals.get(&asset).copied();
            self.action_lifecycle_events.push(ActionLifecycleEvent {
                asset: asset.clone(),
                decision_id: decision.decision_id.to_string(),
                planned_cloid: continuation.parent_planned_cloid.clone(),
                root_planned_cloid: continuation.root_planned_cloid,
                parent_planned_cloid: Some(continuation.parent_planned_cloid),
                retry_generation: continuation.next_retry_generation,
                observed_at_mono: now,
                requested_notional: proposed.unwrap_or_default(),
                filled_quantity: None,
                unfilled_quantity: None,
                outcome: if proposed.is_some_and(|value| !value.is_zero()) {
                    ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute
                } else {
                    ActionAttemptOutcome::NoLongerRequired
                },
            });
        }
        for (asset, action) in executable_by_asset {
            let fresh_book = self
                .books
                .get(&asset)
                .filter(|(_, received_at)| now.saturating_sub(*received_at) <= 40_000);
            if fresh_book.is_none() {
                if retain_pending_book_intent(
                    &mut self.pending_book,
                    asset.clone(),
                    action.decision_id.to_string(),
                    action.planned_cloid.to_string(),
                    now,
                ) {
                    let intent = self
                        .pending_book
                        .get(&asset)
                        .ok_or_else(|| LiveShadowError::Core("missing retained intent".into()))?;
                    self.book_evaluation_events.push(BookEvaluationEvent {
                        asset: asset.clone(),
                        original_decision_id: intent.original_decision_id.clone(),
                        original_planned_cloid: intent.original_planned_cloid.clone(),
                        recomputed_decision_id: None,
                        observed_at_mono: now,
                        waited_ms: 0,
                        recomputed_notional: None,
                        outcome: BookEvaluationOutcome::PendingBookEvaluation,
                    });
                }
                continue;
            }
            if let Some((book, received_at)) = fresh_book {
                let rules = execution_rules
                    .get(&asset)
                    .ok_or_else(|| LiveShadowError::InvalidMarket(asset.clone()))?;
                let snapshot = shadow_snapshot(book, *received_at)?;
                let target = decision
                    .projection
                    .constrained_targets
                    .get(&asset)
                    .copied()
                    .unwrap_or_default();
                let source_weights =
                    normalized_directional_weights(source_contributions.get(&asset), target)?;
                let pending_book_intent = self.pending_book.remove(&asset);
                if let Some(intent) = &pending_book_intent {
                    self.book_evaluation_events.push(BookEvaluationEvent {
                        asset: asset.clone(),
                        original_decision_id: intent.original_decision_id.clone(),
                        original_planned_cloid: intent.original_planned_cloid.clone(),
                        recomputed_decision_id: Some(decision.decision_id.to_string()),
                        observed_at_mono: now,
                        waited_ms: now.saturating_sub(intent.first_seen_at_mono),
                        recomputed_notional: Some(action.rounded_notional),
                        outcome: BookEvaluationOutcome::RecomputedAction,
                    });
                }
                let continuation = self.continuations.remove(&asset);
                let (root_planned_cloid, parent_planned_cloid) = match continuation {
                    Some(continuation) => (
                        continuation.root_planned_cloid,
                        Some(continuation.parent_planned_cloid),
                    ),
                    None => (
                        pending_book_intent
                            .as_ref()
                            .map(|intent| intent.original_planned_cloid.clone())
                            .unwrap_or_else(|| action.planned_cloid.to_string()),
                        None,
                    ),
                };
                if let Some(previous) = self.pending.get(&asset) {
                    if previous.action.planned_cloid != action.planned_cloid {
                        self.action_lifecycle_events.push(ActionLifecycleEvent {
                            asset: asset.clone(),
                            decision_id: previous.action.decision_id.to_string(),
                            planned_cloid: previous.action.planned_cloid.to_string(),
                            root_planned_cloid: previous.root_planned_cloid.clone(),
                            parent_planned_cloid: previous.parent_planned_cloid.clone(),
                            retry_generation: previous.action.retry_generation,
                            observed_at_mono: now,
                            requested_notional: previous.action.rounded_notional,
                            filled_quantity: None,
                            unfilled_quantity: None,
                            outcome: ActionAttemptOutcome::SupersededByNewTarget,
                        });
                    }
                }
                self.pending.insert(
                    asset,
                    PendingShadow {
                        action,
                        root_planned_cloid,
                        parent_planned_cloid,
                        decision_book: snapshot,
                        source_weights,
                        price_tick: rules.price_tick,
                        size_step: rules.size_step,
                        projection_input: {
                            let mut input = projection_input.clone();
                            input.unconstrained_targets =
                                decision.projection.constrained_targets.clone();
                            input
                        },
                        risk_policy_hash: decision.risk_policy_hash,
                        configuration_hash: decision.config_hash,
                    },
                );
            }
        }
        let pending_committed = self
            .pending
            .iter()
            .map(|(asset, pending)| {
                let signed = match pending.action.side {
                    Side::Buy => pending.action.rounded_notional,
                    Side::Sell => -pending.action.rounded_notional,
                };
                (asset.clone(), signed)
            })
            .collect::<BTreeMap<_, _>>();
        self.target_ledger
            .replace_absolute_targets(
                &decision.unconstrained_targets,
                &decision.projection.constrained_targets,
                &projection_filled_positions,
                &pending_committed,
                &BTreeMap::new(),
                decision.target_version,
                decision.snapshot_set_id,
            )
            .map_err(core)?;
        self.refresh_desired_books();
        Ok(Some(decision))
    }

    fn try_execute_pending(
        &mut self,
        book: &OrderBookResponse,
        received_at: Timestamp,
    ) -> Result<bool, LiveShadowError> {
        let Some(pending) = self.pending.get(&book.asset).cloned() else {
            return Ok(false);
        };
        let latency = self.config.latency_timeout_ms;
        if received_at
            < pending
                .decision_book
                .observed_at_mono
                .saturating_add(latency)
        {
            return Ok(false);
        }
        let evaluation = shadow_snapshot(book, received_at)?;
        let decision_midpoint = pending.decision_book.midpoint;
        let reference_price = evaluation.midpoint;
        let cushion = Decimal::from_f64(self.config.slippage_buffer_bps / 10_000.0)
            .ok_or(LiveShadowError::Arithmetic)?;
        let maximum_slippage = Decimal::from_f64(self.config.execution.max_slippage_bps / 10_000.0)
            .ok_or(LiveShadowError::Arithmetic)?;
        let market_rules = MarketRules {
            mark_price: reference_price,
            price_tick: pending.price_tick,
            size_step: pending.size_step,
        };
        let floor_policy = ExecutionFloorPolicy {
            exchange_minimum_notional: Decimal::from_f64(
                self.config.global_risk.min_order_notional_usd,
            )
            .ok_or(LiveShadowError::Arithmetic)?,
            rounding_buffer: Decimal::from_f64(self.config.global_risk.order_rounding_buffer_usd)
                .ok_or(LiveShadowError::Arithmetic)?,
            closeability_margin: Decimal::from_f64(self.config.global_risk.closeability_margin_usd)
                .ok_or(LiveShadowError::Arithmetic)?,
            maximum_slippage_fraction: maximum_slippage,
        };
        let (quantity, price_plan, required_notional) = if pending.action.reduce_only {
            let filled_quantity = self.authoritative_position(&book.asset);
            let filled_notional = filled_quantity
                .checked_mul(reference_price)
                .ok_or(LiveShadowError::Arithmetic)?;
            let desired = self
                .target_ledger
                .get(&book.asset)
                .map(|state| state.admitted_target_notional)
                .unwrap_or_default();
            let exit = match plan_risk_reducing_ioc(&ExitPlanningInput {
                asset: &book.asset,
                desired_target_notional: desired,
                filled_notional,
                filled_quantity,
                acknowledged_open_notional: Decimal::ZERO,
                unknown_result_notional: Decimal::ZERO,
                continuation_notional: Decimal::ZERO,
                reference_price,
                market_rules: &market_rules,
                market_snapshot: &evaluation,
                execution_floor_policy: floor_policy,
                execution_cushion: cushion,
                maximum_slippage,
            }) {
                Ok(exit) => exit,
                Err(ExitPlanningBlock::BelowExchangeMinimum { .. }) => {
                    self.pending.remove(&book.asset);
                    self.action_lifecycle_events.push(ActionLifecycleEvent {
                        asset: book.asset.clone(),
                        decision_id: pending.action.decision_id.to_string(),
                        planned_cloid: pending.action.planned_cloid.to_string(),
                        root_planned_cloid: pending.root_planned_cloid,
                        parent_planned_cloid: pending.parent_planned_cloid,
                        retry_generation: pending.action.retry_generation,
                        observed_at_mono: received_at,
                        requested_notional: pending.action.rounded_notional,
                        filled_quantity: None,
                        unfilled_quantity: None,
                        outcome: ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute,
                    });
                    self.refresh_desired_books();
                    return Ok(false);
                }
                Err(error) => {
                    return Err(LiveShadowError::Core(format!(
                        "exit planning blocked: {error:?}"
                    )))
                }
            };
            (
                exit.quantity,
                exit.ioc,
                exit.execution_floor.minimum_order_notional,
            )
        } else {
            let raw_quantity = pending
                .action
                .rounded_notional
                .checked_div(reference_price)
                .ok_or(LiveShadowError::Arithmetic)?;
            let quantity = round_exchange_step(raw_quantity, pending.size_step, false)?;
            let floor = copytrade_core::execution_floor::execution_floor_for_asset(
                pending.action.side,
                &market_rules,
                floor_policy,
            )
            .map_err(core)?;
            let price_plan = plan_marketable_ioc(
                pending.action.side,
                quantity,
                &evaluation,
                reference_price,
                cushion,
                maximum_slippage,
                pending.price_tick,
            )
            .map_err(core)?;
            (quantity, price_plan, floor.minimum_order_notional)
        };
        if quantity <= Decimal::ZERO {
            return Err(LiveShadowError::InvalidMarket(format!(
                "{} rounded to zero quantity",
                book.asset
            )));
        }
        if !validates_rounded_order(quantity, price_plan.limit_price, required_notional)
            .map_err(core)?
        {
            self.pending.remove(&book.asset);
            self.action_lifecycle_events.push(ActionLifecycleEvent {
                asset: book.asset.clone(),
                decision_id: pending.action.decision_id.to_string(),
                planned_cloid: pending.action.planned_cloid.to_string(),
                root_planned_cloid: pending.root_planned_cloid,
                parent_planned_cloid: pending.parent_planned_cloid,
                retry_generation: pending.action.retry_generation,
                observed_at_mono: received_at,
                requested_notional: pending.action.rounded_notional,
                filled_quantity: None,
                unfilled_quantity: Some(quantity),
                outcome: ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute,
            });
            self.refresh_desired_books();
            return Ok(false);
        }
        if let Some(identity) = self.production_identity.clone() {
            if self
                .emitted_production_cloids
                .insert(pending.action.planned_cloid)
            {
                let position_quantity = self.authoritative_position(&book.asset);
                let expected_committed_before = position_quantity
                    .checked_mul(reference_price)
                    .ok_or(LiveShadowError::Arithmetic)?;
                let signed_delta = quantity
                    .checked_mul(reference_price)
                    .map(|value| match pending.action.side {
                        Side::Buy => value,
                        Side::Sell => -value,
                    })
                    .ok_or(LiveShadowError::Arithmetic)?;
                let expected_committed_after = expected_committed_before
                    .checked_add(signed_delta)
                    .ok_or(LiveShadowError::Arithmetic)?;
                let mut projection_input = pending.projection_input.clone();
                if let Some(rules) = projection_input.market_rules.get_mut(&book.asset) {
                    rules.mark_price = reference_price;
                }
                projection_input
                    .filled_positions
                    .insert(book.asset.clone(), expected_committed_before);
                projection_input.unconstrained_targets = self.target_ledger.admitted_targets();
                let projection = project_and_validate_portfolio(&projection_input)
                    .map_err(|error| LiveShadowError::Core(error.to_string()))?;
                let projected_portfolio_hash = derive_projection_hash(&projection).map_err(core)?;
                let asset_index = self
                    .metadata
                    .as_ref()
                    .and_then(|metadata| {
                        metadata
                            .universe
                            .iter()
                            .position(|asset| asset.name == book.asset)
                    })
                    .ok_or_else(|| LiveShadowError::InvalidMarket(book.asset.clone()))?
                    .try_into()
                    .map_err(|_| LiveShadowError::Arithmetic)?;
                let root_cloid = parse_planned_cloid(&pending.root_planned_cloid)?;
                let parent_cloid = pending
                    .parent_planned_cloid
                    .as_deref()
                    .map(parse_planned_cloid)
                    .transpose()?;
                let intent = AuthorizedExecutionIntent {
                    schema_version: AUTHORIZED_INTENT_SCHEMA_VERSION,
                    decision_id: pending.action.decision_id,
                    target_version: pending.action.target_version,
                    root_cloid,
                    planned_cloid: pending.action.planned_cloid,
                    parent_cloid,
                    continuation_generation: pending.action.retry_generation,
                    asset: book.asset.clone(),
                    asset_index,
                    side: pending.action.side,
                    quantity,
                    limit_price: price_plan.limit_price,
                    reduce_only: pending.action.reduce_only,
                    time_in_force: TimeInForce::Ioc,
                    projected_portfolio_hash,
                    risk_policy_hash: pending.risk_policy_hash,
                    configuration_hash: pending.configuration_hash,
                    market_rules_hash: identity.market_rules_hash,
                    dynamic_floor_policy_hash: identity.dynamic_floor_policy_hash,
                    ioc_policy_hash: identity.ioc_policy_hash,
                    observer_release_hash: identity.observer_release_hash,
                    signer_release_hash: identity.signer_release_hash,
                    release_manifest_hash: identity.release_manifest_hash,
                    decision_reference_price: decision_midpoint,
                    expected_committed_before,
                    expected_committed_after,
                    decision_timestamp_ms: pending.decision_book.observed_at_mono,
                    expires_at: received_at
                        .checked_add(identity.expires_after_ms)
                        .ok_or(LiveShadowError::Arithmetic)?,
                    authorization: PreSigningContext {
                        now_mono: received_at,
                        deployed_risk_policy_hash: pending.risk_policy_hash,
                        projection_input,
                        exchange_minimum_notional: required_notional,
                    },
                    canonical_hash: [0; 32],
                }
                .seal()
                .map_err(LiveShadowError::Core)?;
                self.prepared_authorized_intents.push(intent);
            }
            return Ok(false);
        }
        self.accrue_funding(received_at)?;
        let (equity_before, source_equity_before) = self.accounting_equities()?;
        let funding_attribution = self
            .accrued_funding
            .get(&book.asset)
            .copied()
            .unwrap_or_default();
        let execution = execute_shadow_ioc(&ShadowExecutionInput {
            action: pending.action,
            decision_timestamp_mono: pending.decision_book.observed_at_mono,
            decision_market_snapshot: pending.decision_book,
            evaluation_market_snapshot: evaluation,
            latency_scenario: LatencyScenario::Expected,
            configured_latency_ms: latency,
            proposed_limit_price: price_plan.limit_price,
            rounded_quantity: quantity,
            taker_fee_rate: Decimal::from_f64(self.config.taker_fee_bps / 10_000.0)
                .ok_or(LiveShadowError::Arithmetic)?,
            funding_attribution,
            position_before: self.ledger.portfolio_position(&book.asset),
        })
        .map_err(core)?;
        if !execution.modeled_filled_quantity.is_zero() {
            self.accrued_funding.remove(&book.asset);
        }
        self.ledger
            .apply_portfolio_execution(&execution, received_at)
            .map_err(core)?;
        let allocations = allocate_source_fill(
            &self.ledger.source_positions_for_asset(&book.asset),
            execution.action.side,
            execution.modeled_filled_quantity,
            &pending.source_weights,
        )?;
        for (candidate, quantity) in &allocations {
            let source_execution = scaled_source_execution(
                &execution,
                *quantity,
                self.ledger.source_position(candidate, &book.asset),
            )?;
            self.ledger
                .apply_source_execution(candidate, &source_execution, received_at)
                .map_err(core)?;
        }
        validate_source_reconciliation(
            &self.ledger,
            &book.asset,
            execution.position_after,
            pending.size_step,
        )?;
        let (equity_after, source_equity_after) = self.accounting_equities()?;
        let deployment_after = self.deployment_equity()?;
        let net_pnl_delta = equity_after
            .checked_sub(equity_before)
            .ok_or(LiveShadowError::Arithmetic)?;
        let gross_pnl_delta = net_pnl_delta
            .checked_add(execution.fees)
            .and_then(|value| value.checked_add(execution.funding))
            .and_then(|value| value.checked_add(execution.slippage))
            .ok_or(LiveShadowError::Arithmetic)?;
        let portfolio_equity_return = if equity_before.is_zero() {
            Decimal::ZERO
        } else {
            net_pnl_delta
                .checked_div(equity_before)
                .ok_or(LiveShadowError::Arithmetic)?
        };
        let mut source_attributed_returns = BTreeMap::new();
        for candidate in allocations.keys() {
            let before = source_equity_before
                .get(candidate)
                .copied()
                .ok_or_else(|| LiveShadowError::Core("missing source equity".into()))?;
            let after = source_equity_after
                .get(candidate)
                .copied()
                .ok_or_else(|| LiveShadowError::Core("missing source equity".into()))?;
            let change = after
                .checked_sub(before)
                .ok_or(LiveShadowError::Arithmetic)?;
            source_attributed_returns.insert(
                candidate.clone(),
                if before.is_zero() {
                    Decimal::ZERO
                } else {
                    change
                        .checked_div(before)
                        .ok_or(LiveShadowError::Arithmetic)?
                },
            );
        }
        self.executions.push(ShadowActionAccounting {
            shadow_execution_id: execution.shadow_execution_id.to_string(),
            decision_id: execution.action.decision_id.to_string(),
            asset: book.asset.clone(),
            side: format!("{:?}", execution.action.side).to_ascii_lowercase(),
            execution_mode: match price_plan.pricing_mode {
                MarketableIocPricingMode::CompleteVisibleDepth => {
                    "marketable_ioc_complete_visible_depth"
                }
                MarketableIocPricingMode::BoundedReferenceFallback => {
                    "marketable_ioc_bounded_reference_fallback"
                }
            }
            .to_string(),
            root_planned_cloid: pending.root_planned_cloid.clone(),
            parent_planned_cloid: pending.parent_planned_cloid.clone(),
            retry_generation: execution.action.retry_generation,
            decision_timestamp_mono: execution.decision_timestamp_mono,
            evaluation_timestamp_mono: received_at,
            decision_midpoint,
            modeled_ioc_limit: price_plan.limit_price,
            worst_required_depth_price: price_plan.worst_required_depth_price,
            visible_executable_quantity: price_plan.visible_executable_quantity,
            requested_quantity: execution.rounded_quantity,
            filled_quantity: execution.modeled_filled_quantity,
            unfilled_quantity: execution.unfilled_ioc_remainder,
            average_fill_price: execution.modeled_average_fill_price,
            filled_notional: execution.modeled_filled_notional,
            fees: execution.fees,
            funding: execution.funding,
            execution_slippage: execution.slippage,
            gross_pnl_delta,
            net_pnl_delta,
            portfolio_equity_return,
            current_equity: deployment_after.current_equity,
            settled_equity: deployment_after.settled_equity,
            deployment_equity: deployment_after.deployment_equity,
            source_attributed_returns,
            position_before: execution.position_before,
            position_after: execution.position_after,
            source_filled_quantities: allocations,
        });
        self.pending.remove(&book.asset);
        let requires_recompute = !execution.unfilled_ioc_remainder.is_zero();
        self.action_lifecycle_events.push(ActionLifecycleEvent {
            asset: book.asset.clone(),
            decision_id: execution.action.decision_id.to_string(),
            planned_cloid: execution.action.planned_cloid.to_string(),
            root_planned_cloid: pending.root_planned_cloid.clone(),
            parent_planned_cloid: pending.parent_planned_cloid.clone(),
            retry_generation: execution.action.retry_generation,
            observed_at_mono: received_at,
            requested_notional: execution.action.rounded_notional,
            filled_quantity: Some(execution.modeled_filled_quantity),
            unfilled_quantity: Some(execution.unfilled_ioc_remainder),
            outcome: if requires_recompute {
                ActionAttemptOutcome::ExecutedPartiallyRemainderReplanned
            } else {
                ActionAttemptOutcome::ExecutedFully
            },
        });
        if requires_recompute {
            let next_retry_generation = execution
                .action
                .retry_generation
                .checked_add(1)
                .ok_or(LiveShadowError::Arithmetic)?;
            self.continuations.insert(
                book.asset.clone(),
                ContinuationIntent {
                    root_planned_cloid: pending.root_planned_cloid,
                    parent_planned_cloid: execution.action.planned_cloid.to_string(),
                    next_retry_generation,
                },
            );
        }
        self.metrics.shadow_executions += 1;
        self.refresh_desired_books();
        Ok(requires_recompute)
    }

    pub fn persist(&mut self, path: impl AsRef<std::path::Path>) -> Result<(), LiveShadowError> {
        self.ledger.save_atomic(path).map_err(|error| {
            self.metrics.persistence_failures += 1;
            core(error)
        })
    }
    pub fn persist_target_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(), LiveShadowError> {
        self.target_ledger.save_atomic(path).map_err(|error| {
            self.metrics.persistence_failures += 1;
            core(error)
        })
    }
    pub fn target_ledger(&self) -> &VirtualTargetLedger {
        &self.target_ledger
    }
    pub fn restore_target_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(), LiveShadowError> {
        let ledger = VirtualTargetLedger::load(path).map_err(core)?;
        if let Some(version) = ledger.latest_target_version() {
            self.previous_target = Some(PreviousTargetState {
                version,
                target_hash: canonical_target_hash(&ledger.admitted_targets()).map_err(core)?,
            });
        }
        self.target_ledger = ledger;
        Ok(())
    }
    pub fn ledger(&self) -> &DualLedger {
        &self.ledger
    }
    pub fn metrics(&self) -> &LiveShadowMetrics {
        &self.metrics
    }

    pub fn executions(&self) -> &[ShadowActionAccounting] {
        &self.executions
    }

    pub fn equity_buckets(&self) -> &[EquityReturnBucket] {
        &self.equity_buckets
    }

    pub fn plans(&self) -> &[DecisionPlanningAccounting] {
        &self.plans
    }

    pub fn book_evaluation_events(&self) -> &[BookEvaluationEvent] {
        &self.book_evaluation_events
    }

    pub fn action_lifecycle_events(&self) -> &[ActionLifecycleEvent] {
        &self.action_lifecycle_events
    }

    pub fn executable_density_summary(&self) -> Result<ExecutableDensitySummary, LiveShadowError> {
        let mut roots = BTreeSet::new();
        for event in &self.book_evaluation_events {
            roots.insert(event.original_planned_cloid.clone());
        }
        for event in &self.action_lifecycle_events {
            roots.insert(event.root_planned_cloid.clone());
        }
        for pending in self.pending.values() {
            roots.insert(pending.root_planned_cloid.clone());
        }
        for continuation in self.continuations.values() {
            roots.insert(continuation.root_planned_cloid.clone());
        }
        let mut terminal = BTreeMap::new();
        for event in &self.book_evaluation_events {
            let value = match event.outcome {
                BookEvaluationOutcome::NoLongerRequired => Some("no_longer_required"),
                BookEvaluationOutcome::BelowExchangeMinimumAfterCurrentRecompute => {
                    Some("below_minimum")
                }
                BookEvaluationOutcome::PendingBookEvaluation
                | BookEvaluationOutcome::RecomputedAction => None,
            };
            if let Some(value) = value {
                terminal.insert(event.original_planned_cloid.clone(), value);
            }
        }
        for event in &self.action_lifecycle_events {
            let value = match event.outcome {
                ActionAttemptOutcome::ExecutedFully => Some("satisfied"),
                ActionAttemptOutcome::SupersededByNewTarget
                | ActionAttemptOutcome::NoLongerRequired => Some("no_longer_required"),
                ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute => {
                    Some("below_minimum")
                }
                ActionAttemptOutcome::BlockedByCurrentRisk
                | ActionAttemptOutcome::ExchangeRejected => Some("hard_blocked_or_rejected"),
                ActionAttemptOutcome::ExecutedPartiallyRemainderReplanned => None,
            };
            if let Some(value) = value {
                terminal.insert(event.root_planned_cloid.clone(), value);
            }
        }
        let count_terminal = |name: &str| terminal.values().filter(|value| **value == name).count();
        let satisfied = count_terminal("satisfied");
        let no_longer_required = count_terminal("no_longer_required");
        let below_minimum = count_terminal("below_minimum");
        let hard_blocked_or_rejected = count_terminal("hard_blocked_or_rejected");
        let unresolved = roots
            .iter()
            .filter(|root| !terminal.contains_key(*root))
            .count();
        let fees = sum_execution_field(&self.executions, |execution| execution.fees)?;
        let funding = sum_execution_field(&self.executions, |execution| execution.funding)?;
        let slippage =
            sum_execution_field(&self.executions, |execution| execution.execution_slippage)?;
        let (equity, _) = self.accounting_equities()?;
        let starting = Decimal::from_f64(self.config.starting_equity_usd)
            .ok_or(LiveShadowError::Arithmetic)?;
        let net_pnl = equity
            .checked_sub(starting)
            .ok_or(LiveShadowError::Arithmetic)?;
        let terminal_total =
            satisfied + no_longer_required + below_minimum + hard_blocked_or_rejected + unresolved;
        let gross_turnover =
            sum_execution_field(&self.executions, |execution| execution.filled_notional)?;
        let realized_net_pnl = self.ledger.portfolio_realized_net_pnl().map_err(core)?;
        let net_pnl_per_dollar_traded = if gross_turnover.is_zero() {
            Decimal::ZERO
        } else {
            realized_net_pnl
                .checked_div(gross_turnover)
                .ok_or(LiveShadowError::Arithmetic)?
        };
        let elapsed_hours = self
            .plans
            .first()
            .zip(self.plans.last())
            .and_then(|(first, last)| last.created_at_mono.checked_sub(first.created_at_mono))
            .map(Decimal::from)
            .and_then(|elapsed| elapsed.checked_div(Decimal::from(3_600_000_u64)))
            .unwrap_or_default();
        let closed_episodes_per_hour = if elapsed_hours.is_zero() {
            Decimal::ZERO
        } else {
            Decimal::from(self.ledger.portfolio_closed().len())
                .checked_div(elapsed_hours)
                .ok_or(LiveShadowError::Arithmetic)?
        };
        Ok(ExecutableDensitySummary {
            global_risk_scale: self.config.global_risk.global_risk_scale,
            root_actionable_targets: roots.len(),
            initial_ioc_attempts: self
                .executions
                .iter()
                .filter(|execution| execution.retry_generation == 0)
                .count(),
            depth_priced_attempts: self
                .executions
                .iter()
                .filter(|execution| execution.execution_mode.contains("complete_visible_depth"))
                .count(),
            fallback_attempts: self
                .executions
                .iter()
                .filter(|execution| {
                    execution
                        .execution_mode
                        .contains("bounded_reference_fallback")
                })
                .count(),
            full_fills: self
                .executions
                .iter()
                .filter(|execution| {
                    execution.filled_quantity > Decimal::ZERO
                        && execution.unfilled_quantity.is_zero()
                })
                .count(),
            partial_fills: self
                .executions
                .iter()
                .filter(|execution| {
                    execution.filled_quantity > Decimal::ZERO
                        && execution.unfilled_quantity > Decimal::ZERO
                })
                .count(),
            zero_fills: self
                .executions
                .iter()
                .filter(|execution| execution.filled_quantity.is_zero())
                .count(),
            continuation_attempts: self
                .executions
                .iter()
                .filter(|execution| execution.retry_generation > 0)
                .count(),
            maximum_continuation_generation: self
                .executions
                .iter()
                .map(|execution| execution.retry_generation)
                .max()
                .unwrap_or(0),
            terminally_satisfied_targets: satisfied,
            no_longer_required_targets: no_longer_required,
            below_minimum_current_residuals: below_minimum,
            hard_blocked_or_rejected_targets: hard_blocked_or_rejected,
            unresolved_actionable_targets: unresolved,
            opened_portfolio_episodes: self.ledger.portfolio_open_count()
                + self.ledger.portfolio_closed().len(),
            closed_portfolio_episodes: self.ledger.portfolio_closed().len(),
            fees,
            funding,
            slippage,
            net_pnl,
            root_conservation_verified: roots.len() == terminal_total,
            raw_nonzero_target_changes: self.micro_density.raw_nonzero_target_changes,
            target_changes_above_dynamic_minimum: self
                .micro_density
                .target_changes_above_dynamic_minimum,
            admitted_new_positions: self.micro_density.admitted_new_positions,
            exits: self.micro_density.exits,
            rotations: self.micro_density.rotations,
            maximum_admitted_micro_slots: self.micro_density.maximum_admitted_micro_slots,
            gross_turnover,
            realized_net_pnl,
            net_pnl_per_dollar_traded,
            closed_episodes_per_hour,
        })
    }

    fn record_micro_density(&mut self, decision: &DecisionRecord) -> Result<(), LiveShadowError> {
        for (asset, slot) in &decision.micro_slots {
            let previous_raw = self
                .micro_density
                .previous_raw
                .get(asset)
                .copied()
                .unwrap_or_default();
            if slot.desired_notional != previous_raw && !slot.desired_notional.is_zero() {
                self.micro_density.raw_nonzero_target_changes = self
                    .micro_density
                    .raw_nonzero_target_changes
                    .checked_add(1)
                    .ok_or(LiveShadowError::Arithmetic)?;
                let required = if slot.filled_notional.is_zero() {
                    slot.execution_floor.minimum_opening_notional
                } else {
                    slot.execution_floor.minimum_order_notional
                };
                if slot
                    .desired_notional
                    .checked_sub(slot.filled_notional)
                    .ok_or(LiveShadowError::Arithmetic)?
                    .abs()
                    >= required
                {
                    self.micro_density.target_changes_above_dynamic_minimum = self
                        .micro_density
                        .target_changes_above_dynamic_minimum
                        .checked_add(1)
                        .ok_or(LiveShadowError::Arithmetic)?;
                }
            }
            let previous_admitted = self
                .micro_density
                .previous_admitted
                .get(asset)
                .copied()
                .unwrap_or_default();
            if previous_admitted.is_zero() && !slot.admitted_notional.is_zero() {
                self.micro_density.admitted_new_positions = self
                    .micro_density
                    .admitted_new_positions
                    .checked_add(1)
                    .ok_or(LiveShadowError::Arithmetic)?;
            }
            if !previous_admitted.is_zero() && slot.admitted_notional.is_zero() {
                self.micro_density.exits = self
                    .micro_density
                    .exits
                    .checked_add(1)
                    .ok_or(LiveShadowError::Arithmetic)?;
                if !slot.desired_notional.is_zero() {
                    self.micro_density.rotations = self
                        .micro_density
                        .rotations
                        .checked_add(1)
                        .ok_or(LiveShadowError::Arithmetic)?;
                }
            }
        }
        self.micro_density.previous_raw = decision.unconstrained_targets.clone();
        self.micro_density.previous_admitted = decision.projection.constrained_targets.clone();
        self.micro_density.maximum_admitted_micro_slots =
            self.micro_density.maximum_admitted_micro_slots.max(
                decision
                    .projection
                    .constrained_targets
                    .values()
                    .filter(|notional| !notional.is_zero())
                    .count(),
            );
        Ok(())
    }

    fn refresh_desired_books(&mut self) {
        self.desired_books = self
            .pending
            .keys()
            .chain(self.pending_book.keys())
            .chain(self.continuations.keys())
            .cloned()
            .collect();
        self.metrics.unreconciled_shadow_intents =
            (self.pending.len() + self.pending_book.len() + self.continuations.len()) as u64;
    }

    pub fn record_equity_boundary(&mut self, now: Timestamp) -> Result<(), LiveShadowError> {
        self.accrue_funding(now)?;
        let (equity, source_equities) = self.accounting_equities()?;
        self.record_equity_values(now, equity, source_equities)
    }

    fn accounting_equities(&self) -> Result<(Decimal, BTreeMap<String, Decimal>), LiveShadowError> {
        let marks = self
            .mids
            .as_ref()
            .map(|(snapshot, _)| snapshot.mids.clone())
            .unwrap_or_default();
        let starting = Decimal::from_f64(self.config.starting_equity_usd)
            .ok_or(LiveShadowError::Arithmetic)?;
        let unbooked_funding = self
            .accrued_funding
            .values()
            .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
            .ok_or(LiveShadowError::Arithmetic)?;
        let equity = self
            .ledger
            .portfolio_equity(starting, &marks, unbooked_funding)
            .map_err(core)?;
        let mut source_equities = BTreeMap::new();
        for candidate in &self.config.candidates {
            let id = candidate.address.to_ascii_lowercase();
            source_equities.insert(
                id.clone(),
                self.ledger
                    .source_equity(&id, starting, &marks)
                    .map_err(core)?,
            );
        }
        Ok((equity, source_equities))
    }

    pub fn deployment_equity(&self) -> Result<DeploymentEquity, LiveShadowError> {
        if let Some((current, settled, deployment)) = self.production_equities {
            let starting = Decimal::from_f64(self.config.starting_equity_usd)
                .ok_or(LiveShadowError::Arithmetic)?;
            let realized = settled
                .checked_sub(starting)
                .ok_or(LiveShadowError::Arithmetic)?;
            let mut result =
                calculate_deployment_equity(starting, current, realized).map_err(core)?;
            if result.deployment_equity != deployment {
                return Err(LiveShadowError::Core(
                    "production deployment equity mismatch".into(),
                ));
            }
            result.deployment_equity = deployment;
            return Ok(result);
        }
        let (current, _) = self.accounting_equities()?;
        let starting = Decimal::from_f64(self.config.starting_equity_usd)
            .ok_or(LiveShadowError::Arithmetic)?;
        let realized = self.ledger.portfolio_realized_net_pnl().map_err(core)?;
        calculate_deployment_equity(starting, current, realized).map_err(core)
    }

    fn record_equity_values(
        &mut self,
        now: Timestamp,
        equity: Decimal,
        source_equities: BTreeMap<String, Decimal>,
    ) -> Result<(), LiveShadowError> {
        if let Some((opened_at, previous, previous_sources)) = &self.last_bucket {
            if now == *opened_at {
                return Err(LiveShadowError::DuplicateBucket);
            }
            if now < *opened_at {
                return Err(LiveShadowError::InvalidBucketInterval);
            }
            let return_fraction = equity
                .checked_sub(*previous)
                .and_then(|change| change.checked_div(*previous))
                .ok_or(LiveShadowError::Arithmetic)?;
            let source_returns = source_equities
                .iter()
                .map(|(candidate, value)| {
                    let previous = previous_sources
                        .get(candidate)
                        .copied()
                        .ok_or(LiveShadowError::Arithmetic)?;
                    Ok((
                        candidate.clone(),
                        value
                            .checked_sub(previous)
                            .and_then(|change| change.checked_div(previous))
                            .ok_or(LiveShadowError::Arithmetic)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, LiveShadowError>>()?;
            let bucket_index = self.equity_buckets.len() as u64;
            self.equity_buckets.push(EquityReturnBucket {
                run_id: self.run_id.clone(),
                bucket_id: format!("{}:{bucket_index}", self.run_id),
                bucket_index,
                opened_at_mono: *opened_at,
                closed_at_mono: now,
                starting_equity: *previous,
                ending_equity: equity,
                return_fraction,
                source_returns,
            });
        }
        self.last_bucket = Some((now, equity, source_equities));
        Ok(())
    }

    pub fn last_equity_boundary(&self) -> Option<Timestamp> {
        self.last_bucket
            .as_ref()
            .map(|(timestamp, _, _)| *timestamp)
    }

    fn accrue_funding(&mut self, now: Timestamp) -> Result<(), LiveShadowError> {
        let Some(previous) = self.last_funding_accrual else {
            self.last_funding_accrual = Some(now);
            return Ok(());
        };
        let elapsed = now.saturating_sub(previous);
        if elapsed == 0 {
            return Ok(());
        }
        let (Some(metadata), Some((mids, _))) = (&self.metadata, &self.mids) else {
            self.last_funding_accrual = Some(now);
            return Ok(());
        };
        let hours = Decimal::from(elapsed)
            .checked_div(Decimal::from(3_600_000_u64))
            .ok_or(LiveShadowError::Arithmetic)?;
        for (asset, context) in &metadata.contexts {
            let quantity = self.ledger.portfolio_position(asset);
            let Some(mark) = mids.mids.get(asset) else {
                continue;
            };
            if quantity.is_zero() {
                continue;
            }
            let cost = quantity
                .checked_mul(*mark)
                .and_then(|value| value.checked_mul(context.funding_rate_hourly))
                .and_then(|value| value.checked_mul(hours))
                .ok_or(LiveShadowError::Arithmetic)?;
            let entry = self.accrued_funding.entry(asset.clone()).or_default();
            *entry = entry.checked_add(cost).ok_or(LiveShadowError::Arithmetic)?;
        }
        self.last_funding_accrual = Some(now);
        Ok(())
    }
}

fn requires_close_first(filled_notional: Decimal, planned_delta: Decimal) -> bool {
    !filled_notional.is_zero()
        && !planned_delta.is_zero()
        && filled_notional.is_sign_positive() != planned_delta.is_sign_positive()
}

fn build_market_rules<'a>(
    mids: &MarketSnapshotResponse,
    metadata: &MarketMetadataResponse,
    assets: impl Iterator<Item = &'a String>,
) -> Result<BTreeMap<String, MarketRules>, LiveShadowError> {
    let sizes = metadata
        .universe
        .iter()
        .map(|asset| (asset.name.as_str(), asset.size_decimals))
        .collect::<BTreeMap<_, _>>();
    assets
        .map(|asset| {
            let price = *mids
                .mids
                .get(asset)
                .ok_or_else(|| LiveShadowError::InvalidMarket(asset.clone()))?;
            let decimals = *sizes
                .get(asset.as_str())
                .ok_or_else(|| LiveShadowError::InvalidMarket(asset.clone()))?;
            Ok((
                asset.clone(),
                MarketRules {
                    mark_price: price,
                    price_tick: price_tick(price, decimals)?,
                    size_step: Decimal::new(1, decimals),
                },
            ))
        })
        .collect()
}

fn price_tick(price: Decimal, size_decimals: u32) -> Result<Decimal, LiveShadowError> {
    let integer_digits = price
        .trunc()
        .abs()
        .to_string()
        .trim_start_matches('-')
        .len() as u32;
    let maximum_decimals = 6_u32.saturating_sub(size_decimals);
    if integer_digits >= 5 {
        Ok(Decimal::from_i128_with_scale(
            10_i128
                .checked_pow(integer_digits - 5)
                .ok_or(LiveShadowError::Arithmetic)?,
            0,
        ))
    } else {
        Ok(Decimal::new(1, (5 - integer_digits).min(maximum_decimals)))
    }
}

fn round_exchange_step(
    value: Decimal,
    step: Decimal,
    round_up: bool,
) -> Result<Decimal, LiveShadowError> {
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(LiveShadowError::Arithmetic);
    }
    let units = value.checked_div(step).ok_or(LiveShadowError::Arithmetic)?;
    let units = if round_up {
        units.ceil()
    } else {
        units.floor()
    };
    units.checked_mul(step).ok_or(LiveShadowError::Arithmetic)
}

fn shadow_snapshot(
    book: &OrderBookResponse,
    observed_at: Timestamp,
) -> Result<ShadowMarketSnapshot, LiveShadowError> {
    let best_bid = book
        .bids
        .first()
        .ok_or_else(|| LiveShadowError::InvalidMarket(book.asset.clone()))?
        .price;
    let best_ask = book
        .asks
        .first()
        .ok_or_else(|| LiveShadowError::InvalidMarket(book.asset.clone()))?
        .price;
    let midpoint = best_bid
        .checked_add(best_ask)
        .and_then(|value| value.checked_div(Decimal::from(2)))
        .ok_or(LiveShadowError::Arithmetic)?;
    let bytes =
        serde_json::to_vec(book).map_err(|error| LiveShadowError::Core(error.to_string()))?;
    Ok(ShadowMarketSnapshot {
        snapshot_id: hash_market_snapshot_bytes(&bytes),
        observed_at_mono: observed_at,
        midpoint,
        bids: book
            .bids
            .iter()
            .map(|level| DepthLevel {
                price: level.price,
                quantity: level.quantity,
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .map(|level| DepthLevel {
                price: level.price,
                quantity: level.quantity,
            })
            .collect(),
    })
}

fn curve_leverage(config: &CopyTradeConfig, equity: f64) -> Result<f64, LiveShadowError> {
    let mut points = config.leverage_curve.clone();
    points.sort_by(|left, right| left.equity_usd.total_cmp(&right.equity_usd));
    let first = points.first().ok_or(LiveShadowError::Arithmetic)?;
    if equity <= first.equity_usd {
        return Ok(first.target_leverage.min(config.max_total_leverage));
    }
    let last = points.last().ok_or(LiveShadowError::Arithmetic)?;
    if equity >= last.equity_usd {
        return Ok(last.target_leverage.min(config.max_total_leverage));
    }
    for pair in points.windows(2) {
        if equity <= pair[1].equity_usd {
            let progress =
                (equity - pair[0].equity_usd) / (pair[1].equity_usd - pair[0].equity_usd);
            return Ok((pair[0].target_leverage
                + (pair[1].target_leverage - pair[0].target_leverage) * progress)
                .min(config.max_total_leverage));
        }
    }
    Err(LiveShadowError::Arithmetic)
}

fn core(error: impl Display) -> LiveShadowError {
    LiveShadowError::Core(error.to_string())
}

fn sum_execution_field(
    executions: &[ShadowActionAccounting],
    field: impl Fn(&ShadowActionAccounting) -> Decimal,
) -> Result<Decimal, LiveShadowError> {
    executions
        .iter()
        .try_fold(Decimal::ZERO, |sum, execution| {
            sum.checked_add(field(execution))
        })
        .ok_or(LiveShadowError::Arithmetic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::public_mainnet::{
        BookLevel, MarketAssetContext, MarketMetadataAsset, SourceAssetPosition,
    };
    use copytrade_core::decision::{DecisionId, PlannedAction, PlannedCloid, TargetVersion};
    use copytrade_core::scheduler::ReadRequestKind;
    use std::path::Path;
    #[test]
    fn exchange_tick_rule_is_deterministic() {
        assert_eq!(
            price_tick(Decimal::from(113_377), 5).unwrap(),
            Decimal::from(10)
        );
        assert_eq!(
            price_tick(Decimal::from(99), 4).unwrap(),
            Decimal::new(1, 2)
        );
        assert_eq!(
            round_exchange_step(Decimal::new(123_456, 5), Decimal::new(1, 2), false).unwrap(),
            Decimal::new(123, 2)
        );
        assert_eq!(
            round_exchange_step(Decimal::new(123_456, 5), Decimal::new(1, 2), true).unwrap(),
            Decimal::new(124, 2)
        );
    }

    #[test]
    fn micro_capital_curve_binds_launch_gross_limit() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        assert_eq!(curve_leverage(&config, 100.0).unwrap(), 8.75);
        let gross = Decimal::from(100)
            * Decimal::from_f64(curve_leverage(&config, 100.0).unwrap()).unwrap()
            * Decimal::from_f64(config.global_risk.global_risk_scale).unwrap();
        assert_eq!(gross, Decimal::new(875, 1));
    }

    #[test]
    fn source_fill_allocation_closes_before_opening_and_conserves_quantity() {
        let current = [
            ("a".to_string(), Decimal::from(-2)),
            ("b".to_string(), Decimal::from(-1)),
        ]
        .into_iter()
        .collect();
        let opening = [
            ("c".to_string(), Decimal::new(3, 1)),
            ("d".to_string(), Decimal::new(7, 1)),
        ]
        .into_iter()
        .collect();
        let allocated =
            allocate_source_fill(&current, Side::Buy, Decimal::from(5), &opening).unwrap();
        assert_eq!(allocated["a"], Decimal::from(2));
        assert_eq!(allocated["b"], Decimal::from(1));
        assert_eq!(allocated["c"], Decimal::new(6, 1));
        assert_eq!(allocated["d"], Decimal::new(14, 1));
        assert_eq!(
            allocated.values().copied().sum::<Decimal>(),
            Decimal::from(5)
        );
    }

    #[test]
    fn source_reconciliation_ignores_only_sub_lot_decimal_dust() {
        let (dust, tolerance) =
            reconciliation_difference(Decimal::new(-1, 28), Decimal::ZERO, Decimal::new(1, 6))
                .unwrap();
        assert!(dust <= tolerance);

        let (material, tolerance) =
            reconciliation_difference(Decimal::new(1, 12), Decimal::ZERO, Decimal::new(1, 6))
                .unwrap();
        assert!(material > tolerance);
        assert!(reconciliation_difference(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).is_err());
    }

    #[test]
    fn source_allocation_assigns_decimal_residual_to_stable_final_source() {
        let mut allocations = [
            ("a".to_string(), Decimal::new(333_333_333_333_333_333, 18)),
            ("b".to_string(), Decimal::new(333_333_333_333_333_333, 18)),
            ("c".to_string(), Decimal::new(333_333_333_333_333_333, 18)),
        ]
        .into_iter()
        .collect();
        canonicalize_allocation_total(&mut allocations, Decimal::ONE).unwrap();
        assert_eq!(allocations.values().copied().sum::<Decimal>(), Decimal::ONE);
        assert_eq!(allocations["c"], Decimal::new(333_333_333_333_333_334, 18));
    }

    #[test]
    fn missing_book_intent_is_retained_then_recomputed_or_explicitly_invalidated() {
        assert_eq!(
            resolve_book_action(false, false, true),
            BookActionResolution::AwaitBook
        );
        assert_eq!(
            resolve_book_action(true, true, true),
            BookActionResolution::RecomputeWithFreshBook
        );
        assert_eq!(
            resolve_book_action(true, true, false),
            BookActionResolution::InvalidateAfterRecompute
        );
        assert_eq!(
            resolve_book_action(false, true, false),
            BookActionResolution::NoIntent
        );
        let mut pending = BTreeMap::new();
        assert!(retain_pending_book_intent(
            &mut pending,
            "AR".into(),
            "decision-1".into(),
            "cloid-1".into(),
            10,
        ));
        assert!(!retain_pending_book_intent(
            &mut pending,
            "AR".into(),
            "decision-2".into(),
            "cloid-2".into(),
            20,
        ));
        assert_eq!(pending["AR"].original_decision_id, "decision-1");
        assert_eq!(pending["AR"].original_planned_cloid, "cloid-1");
    }

    #[test]
    fn held_asset_remains_in_target_universe_for_master_flattening() {
        let mut inputs = BTreeMap::new();
        inputs.insert("BTC".to_string(), Vec::new());
        include_held_assets_in_target_universe(&mut inputs, ["CFX".to_string(), "BTC".to_string()]);
        assert!(inputs.contains_key("CFX"));
        assert!(inputs["CFX"].is_empty());
        assert_eq!(inputs.len(), 2);
    }

    #[test]
    fn partial_fill_continuation_gets_a_new_deterministic_linkable_cloid() {
        let engine = EngineInstanceId([7; 16]);
        let base = PlannedAction {
            decision_id: DecisionId([8; 32]),
            target_version: TargetVersion(3),
            asset: "AR".to_string(),
            side: Side::Buy,
            rounded_notional: Decimal::from(18),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([9; 16]),
        };
        let mut first = base.clone();
        let mut replay = base.clone();
        reidentify_continuation_action(engine, &mut first, 1).unwrap();
        reidentify_continuation_action(engine, &mut replay, 1).unwrap();
        assert_eq!(first.retry_generation, 1);
        assert_ne!(first.planned_cloid, base.planned_cloid);
        assert_eq!(first.planned_cloid, replay.planned_cloid);
    }

    fn accepted(payload: PublicPayload, kind: ReadRequestKind, at: u64) -> AcceptedPublicResponse {
        AcceptedPublicResponse {
            request_kind: kind,
            source_tier: None,
            subject: "fixture".to_string(),
            requested_at_mono: at.saturating_sub(1),
            received_at_mono: at,
            valid_until_mono: at + 120_000,
            payload,
        }
    }

    #[test]
    fn missing_book_then_partial_ioc_retains_and_replans_the_current_target() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        config.candidates.truncate(1);
        let mut engine =
            LiveShadowEngine::new(config, b"partial-fixture", "run", 40_000, 80_000).unwrap();
        engine.set_source_tier(&candidate, SourceTier::Active);

        let positions = [(
            "BTC".to_string(),
            SourceAssetPosition {
                asset: "BTC".to_string(),
                signed_size: Decimal::from(5),
                signed_notional: Decimal::from(500),
            },
        )]
        .into_iter()
        .collect();
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        candidate_id: candidate,
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1_000,
                        positions,
                    }),
                    ReadRequestKind::SourceState,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: [("BTC".to_string(), Decimal::from(100))]
                            .into_iter()
                            .collect(),
                    }),
                    ReadRequestKind::MarketMids,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketMetadata(MarketMetadataResponse {
                        universe: vec![MarketMetadataAsset {
                            name: "BTC".to_string(),
                            size_decimals: 3,
                        }],
                        contexts: [(
                            "BTC".to_string(),
                            MarketAssetContext {
                                funding_rate_hourly: Decimal::ZERO,
                            },
                        )]
                        .into_iter()
                        .collect(),
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine.construct_next_decision(1_000).unwrap();
        assert!(engine.pending_book.contains_key("BTC"));

        let shallow_book = OrderBookResponse {
            asset: "BTC".to_string(),
            source_time_ms: 2_000,
            bids: vec![BookLevel {
                price: Decimal::new(9_999, 2),
                quantity: Decimal::new(5, 2),
            }],
            asks: vec![BookLevel {
                price: Decimal::new(10_001, 2),
                quantity: Decimal::new(5, 2),
            }],
        };
        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(shallow_book.clone()),
                    ReadRequestKind::OrderBook,
                    2_000,
                ),
                2_000,
            )
            .unwrap();
        assert!(engine.pending.contains_key("BTC"));
        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(shallow_book),
                    ReadRequestKind::OrderBook,
                    3_000,
                ),
                3_000,
            )
            .unwrap();

        assert_eq!(engine.executions.len(), 1);
        assert!(engine.executions[0].unfilled_quantity > Decimal::ZERO);
        let partial = engine.action_lifecycle_events.last().unwrap();
        assert_eq!(
            partial.outcome,
            ActionAttemptOutcome::ExecutedPartiallyRemainderReplanned
        );
        let follow_up = engine.pending.get("BTC").unwrap();
        assert_eq!(follow_up.action.retry_generation, 1);
        assert_eq!(
            follow_up.parent_planned_cloid.as_deref(),
            Some(partial.planned_cloid.as_str())
        );
        assert_ne!(
            follow_up.action.planned_cloid.to_string(),
            partial.planned_cloid
        );
        let density = engine.executable_density_summary().unwrap();
        assert_eq!(density.root_actionable_targets, 1);
        assert_eq!(density.initial_ioc_attempts, 1);
        assert_eq!(density.partial_fills, 1);
        assert_eq!(density.unresolved_actionable_targets, 1);
        assert!(density.root_conservation_verified);
    }

    #[test]
    fn every_direction_flip_is_close_first_even_when_target_crosses_zero() {
        assert!(requires_close_first(Decimal::from(20), Decimal::from(-30)));
        assert!(requires_close_first(Decimal::from(-20), Decimal::from(30)));
        assert!(!requires_close_first(Decimal::from(20), Decimal::from(10)));
    }
}
