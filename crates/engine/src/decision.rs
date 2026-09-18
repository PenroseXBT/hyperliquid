use crate::cohort_layer::{is_cohort_candidate_label, PreparedVeryProfitableLayer};
use crate::domain::authorized_intent::{
    AuthorizedExecutionIntent, PreSigningContext, TimeInForce, AUTHORIZED_INTENT_SCHEMA_VERSION,
};
use crate::domain::cohort::{
    aggregate_authoritative_positions, classify_attribution, AttributionTag, CohortAssetAggregate,
    CohortDecisionReason, CohortIndicatorRecord, CohortPenalties, CohortRiskFlags,
    CohortSignalInput, VeryProfitableCohortEngine,
};
use crate::domain::configuration::CopyTradeConfig;
use crate::domain::consensus::{
    bounded_additive_consensus, ConsensusInput, SourceExposureBook, SourceExposureState,
};
use crate::domain::decision::{
    canonical_target_hash, construct_decision, construct_planned_actions, derive_config_hash,
    derive_planned_cloid, derive_projection_hash, derive_risk_policy_hash,
    hash_market_snapshot_bytes, hash_payload_bytes, DecisionConstructionInput, DecisionRecord,
    EngineInstanceId, ExclusionReason, PlannedAction, PlannedCloidInput, PreviousTargetState, Side,
    SnapshotSetMember, SourceEligibilitySummary,
};
use crate::domain::deployment_equity::{calculate_deployment_equity, DeploymentEquity};
use crate::domain::execution_floor::{
    execution_floor_for_asset, validates_rounded_order, ExecutionFloorPolicy,
};
use crate::domain::exit_planning::{
    plan_risk_reducing_ioc, ExitPlanningBlock, ExitPlanningInput, ResidualClass,
};
#[cfg(test)]
use crate::domain::ioc::{
    execute_fixture_ioc, FixtureExecutionInput, LatencyScenario, MarketableIocPricingMode,
};
use crate::domain::ioc::{plan_marketable_ioc, DepthLevel, ExecutableBook, ExecutionFill};
use crate::domain::ledger::DualLedger;
use crate::domain::live_trading::{
    execution_from_verified_fill, FundingEventId, VerifiedExchangeFill, VerifiedFundingEvent,
};
use crate::domain::portfolio_risk::project_and_validate_portfolio;
use crate::domain::portfolio_risk::{
    MarketRules, OpenOrderExposure, OpenOrderLifecycle, OrderSide, PortfolioProjectionInput,
};
use crate::domain::scheduler::SourceTier;
use crate::domain::scheduler::{SnapshotAcceptance, SnapshotStore, SourceSnapshot, Timestamp};
use crate::domain::target_state::VirtualTargetLedger;
use crate::domain::technical::{
    CandleAcceptance, SignalArchetype, TechnicalContext, TechnicalEngine, TechnicalFunnel,
    TechnicalTarget,
};
use crate::hip3::{
    collateral_for_dex as hip3_collateral_for_dex, dex_for_market,
    dexes_for_assets as hip3_dexes_for_assets, execution_asset_id as hip3_execution_asset_id,
    execution_capabilities as hip3_execution_capabilities, is_hip3_market as hip3_is_market,
    EXCLUDED_HIP3_DEX,
};
use crate::learning::{executable_anchor, observe_executable};
use crate::mfce::evaluate_allocation_policy;
use crate::mfce::{
    allocate_cross_sectional, allocation_baseline_target, policy_sized_increment,
    remaining_explore_information_budget, LearningObjective, LegacyMfcePersistentState,
    LegacyMfcePersistentStateV12A, LegacyMfcePersistentStateV12B, LegacyMfcePersistentStateV12C,
    LegacyMfcePersistentStateV12D, LegacyMfcePersistentStateV12E, LegacyMfcePersistentStateV12F,
    LegacyMfcePersistentStateV12G, MfceAllocationInput, MfceCrossSectionalCandidate, MfceDirection,
    MfceEngine, MfceError, MfceFeatureVector, MfcePersistentState, MfceRejectionReason, MfceReport,
    MFCE_FEATURE_COUNT, MFCE_STATE_SCHEMA_VERSION,
};
use crate::mfce_delayed::{
    AlphaOrigin, DecisionKind, DecisionProvenance, DecisionSample, MarketFlow,
    PredictionObservation, RealizedOutcome, RemainingEdgePredictionPoint, HORIZONS_MS,
};
use crate::public_mainnet::{
    AcceptedPublicResponse, CandleResponse, MarketMetadataAsset, MarketMetadataResponse,
    MarketSnapshotResponse, OrderBookResponse, PublicPayload, SourceStateResponse,
};
use crate::source_state::Hip3SourceActivity;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct EngineMetrics {
    pub target_readiness_rejections: u64,
    pub accepted_source_snapshots: u64,
    pub stale_source_snapshots: u64,
    pub rejected_source_snapshots: u64,
    pub decisions: u64,
    pub projection_violations: u64,
    pub executions: u64,
    pub unreconciled_intents: u64,
    pub persistence_failures: u64,
    pub cohort_indicator_records: u64,
    pub cohort_runtime_rejections: u64,
    pub technical_decision_records: u64,
    pub mfce_retrain_attempts: u64,
    pub mfce_model_promotions: u64,
    pub mfce_retrain_failures: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Hip3PipelineStatus {
    pub hip3_source_markets_seen: usize,
    pub hip3_source_markets_known: usize,
    pub hip3_source_markets_confirmed: usize,
    pub hip3_mfce_candidates: usize,
    pub hip3_execution_unsupported: usize,
    pub hip3_actions_emitted: usize,
    pub hip3_fills: usize,
    pub hip3_execution_unsupported_source_fills: u64,
    pub hip3_execution_unsupported_unique_markets: usize,
    pub hip3_execution_unsupported_unique_wallets: usize,
}

fn normalized_directional_weights(
    contributions: Option<&BTreeMap<String, Decimal>>,
    target: Decimal,
) -> Result<BTreeMap<String, Decimal>, EngineError> {
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
        .ok_or(EngineError::Arithmetic)?;
    if total.is_zero() {
        return Ok(BTreeMap::new());
    }
    selected
        .into_iter()
        .map(|(candidate, value)| {
            Ok((
                candidate,
                value.checked_div(total).ok_or(EngineError::Arithmetic)?,
            ))
        })
        .collect()
}

fn absolute_directional_component_targets(
    contributions: Option<&BTreeMap<String, Decimal>>,
    portfolio_target: Decimal,
) -> Result<Option<BTreeMap<String, Decimal>>, EngineError> {
    if portfolio_target.is_zero() {
        return Ok(Some(BTreeMap::new()));
    }
    let weights = normalized_directional_weights(contributions, portfolio_target)?;
    if weights.is_empty() {
        return Ok(None);
    }
    let components = proportional_allocations(portfolio_target.abs(), &weights)?
        .into_iter()
        .map(|(component, notional)| {
            Ok((
                component,
                if portfolio_target.is_sign_positive() {
                    notional
                } else {
                    -notional
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, EngineError>>()?;
    Ok(Some(components))
}

fn exact_component_notionals(
    quantities: BTreeMap<String, Decimal>,
    mark: Decimal,
) -> Result<BTreeMap<String, Decimal>, EngineError> {
    quantities
        .into_iter()
        .map(|(source, quantity)| {
            quantity
                .checked_mul(mark)
                .map(|notional| (source, notional))
                .ok_or(EngineError::Arithmetic)
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

const BPS_PER_UNIT_RETURN: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);
const MFCE_LIVE_BOOK_MAX_AGE_MS: u64 = 40_000;
// Sampling cadence bounds unresolved continuation records. It never forces an
// exit; every action still follows the newly predicted RemainingEdge.
const MFCE_POSITION_EVALUATION_INTERVAL_MS: u64 = 30_000;

const fn mfce_indicator(value: bool) -> Decimal {
    if value {
        Decimal::ONE
    } else {
        Decimal::ZERO
    }
}

/// Edge-only policy friction. Learning truth, settled accounting, and
/// labels always use actual costs; the admission hurdle uses the live
/// base friction only (actual live_context.friction_bps). Fee-multiplier
/// buffers and re-entry penalties are accounting diagnostics only and
/// never gate size.
#[cfg(test)]
fn buffered_policy_friction_bps(
    base_friction_bps: Decimal,
    _taker_fee_bps: Decimal,
    _fee_multiplier: Decimal,
    _is_new_risk: bool,
) -> Result<Decimal, EngineError> {
    Ok(base_friction_bps)
}

/// Explicit same-thesis switching cost: abandon now, plausibly reacquire
/// later. Two buffered fee legs plus two expected slippage legs. EXIT must
/// clear this unless hard risk invalidation or a true opposite reversal
/// clears its own full transition cost.
#[cfg(test)]
fn same_side_switching_cost_bps(
    taker_fee_bps: Decimal,
    maximum_slippage_bps: Decimal,
    fee_multiplier: Decimal,
) -> Result<Decimal, EngineError> {
    taker_fee_bps
        .checked_mul(fee_multiplier)
        .and_then(|fee| fee.checked_mul(Decimal::from(2)))
        .and_then(|fees| {
            maximum_slippage_bps
                .checked_mul(Decimal::from(2))
                .and_then(|slip| slip.checked_add(fees))
        })
        .ok_or(EngineError::Arithmetic)
}

/// Re-entry probe gate (edge-only). The recency window is accounting
/// diagnostics only and never vetoes admission. Only net_q50 /
/// conservative_edge / net_q10 from evaluate_distribution may gate size.
fn reentry_probe_allowed(
    _penalty_applied: bool,
    _used_model: bool,
    _policy_state: crate::mfce::MfcePolicyState,
    _conservative_edge_bps: Decimal,
) -> bool {
    true
}

/// HOLD default for continuations: a Reject on an existing same-side
/// position holds the current exposure instead of flattening, unless the
/// desired target is a genuine reversal (opposite sign) which follows the
/// normal retirement path and must still clear full transition cost.
fn continuation_hold_target(current: Decimal, desired: Decimal, rejected: Decimal) -> Decimal {
    if current.is_zero() {
        return rejected;
    }
    if !desired.is_zero() && current.is_sign_positive() != desired.is_sign_positive() {
        // True reversal bypasses stickiness; downstream reversal logic and
        // full transition cost still apply.
        return rejected;
    }
    // Neutral / weak / noisy continuation: HOLD, never auto-flatten.
    current
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MfceLiveMarketContext {
    friction_bps: Decimal,
    spread_bps: Decimal,
    entry_depth_ratio: Decimal,
    exit_depth_ratio: Decimal,
    depth_imbalance: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MfceTargetContext {
    raw_desired_source_target: Decimal,
    current_source_target: Decimal,
    current_source_targets: BTreeMap<String, Decimal>,
}

fn position_evaluation_targets(
    current: Decimal,
    desired: Decimal,
) -> (LearningObjective, Decimal, Decimal) {
    if !desired.is_zero()
        && current.is_sign_positive() == desired.is_sign_positive()
        && desired.abs() > current.abs()
    {
        // The marginal decision is whether to add. Rejecting that slice must
        // preserve exposure whose continuation was not evaluated here.
        (LearningObjective::SizeQuality, desired, current)
    } else {
        let rejected = if desired.is_zero()
            || (current.is_sign_positive() == desired.is_sign_positive()
                && desired.abs() < current.abs())
        {
            desired
        } else {
            Decimal::ZERO
        };
        (LearningObjective::ContinuationQuality, current, rejected)
    }
}

fn mfce_live_market_context(
    asset: &str,
    book: &OrderBookResponse,
    direction: MfceDirection,
    current_position_notional: Decimal,
    proposed_position_notional: Decimal,
    taker_fee_bps: Decimal,
    maximum_slippage_bps: Decimal,
    funding_rate_hourly: Decimal,
    holding_hours: Decimal,
) -> Result<MfceLiveMarketContext, EngineError> {
    let snapshot = ioc_snapshot(book, 0)?;
    let result = (|| {
        let midpoint = snapshot.midpoint;
        let bid = book.bids.first()?.price;
        let ask = book.asks.first()?.price;
        if bid <= Decimal::ZERO
            || bid >= ask
            || taker_fee_bps < Decimal::ZERO
            || !(Decimal::ZERO..BPS_PER_UNIT_RETURN).contains(&maximum_slippage_bps)
            || holding_hours < Decimal::ZERO
        {
            return None;
        }
        let notional = proposed_position_notional.abs();
        let entry = if current_position_notional.is_sign_positive()
            == proposed_position_notional.is_sign_positive()
        {
            notional.checked_sub(current_position_notional.abs())?
        } else {
            notional
        };
        if entry < Decimal::ZERO {
            return None;
        }
        let leg = |side: Side, amount: Decimal| -> Option<(Decimal, Decimal)> {
            let sign = if side == Side::Buy {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            let quantity = amount.checked_div(midpoint)?;
            let limit = midpoint
                .checked_mul(Decimal::ONE + sign * maximum_slippage_bps / BPS_PER_UNIT_RETURN)?;
            let (filled, value) =
                crate::domain::ioc::quote_ioc(side, quantity, limit, &snapshot).ok()?;
            if filled.is_zero() {
                return None;
            }
            // Admission allowance only; delayed execution requires full depth.
            let allowance = (quantity - filled).checked_mul(limit)?.checked_add(value)?;
            let cost = allowance
                .checked_div(quantity)?
                .checked_sub(midpoint)?
                .checked_mul(sign)?
                .checked_div(midpoint)?
                .checked_mul(BPS_PER_UNIT_RETURN)?
                .max(Decimal::ZERO);
            let levels = if side == Side::Buy {
                &book.asks
            } else {
                &book.bids
            };
            let visible = levels
                .iter()
                .take_while(|level| {
                    if side == Side::Buy {
                        level.price <= limit
                    } else {
                        level.price >= limit
                    }
                })
                .try_fold(Decimal::ZERO, |sum, level| {
                    sum.checked_add(level.price.checked_mul(level.quantity)?)
                })?;
            Some((cost, visible))
        };
        let (entry_side, exit_side) = if direction == MfceDirection::Long {
            (Side::Buy, Side::Sell)
        } else {
            (Side::Sell, Side::Buy)
        };
        if entry.is_zero() {
            let (_, exit_depth) = leg(exit_side, notional)?;
            let (_, opposite_depth) = leg(entry_side, notional)?;
            let (bid_depth, ask_depth) = if direction == MfceDirection::Long {
                (exit_depth, opposite_depth)
            } else {
                (opposite_depth, exit_depth)
            };
            let funding = funding_rate_hourly
                .checked_mul(holding_hours)?
                .checked_mul(Decimal::from(direction.sign()))?
                .checked_mul(BPS_PER_UNIT_RETURN)?;
            return Some(MfceLiveMarketContext {
                // Holding defers one exit: equal expected exit fees and
                // stationary slippage cancel against exiting now.
                friction_bps: funding.max(Decimal::ZERO),
                spread_bps: (ask - bid)
                    .checked_div(midpoint)?
                    .checked_mul(BPS_PER_UNIT_RETURN)?,
                entry_depth_ratio: Decimal::ZERO,
                exit_depth_ratio: exit_depth.checked_div(notional)?.min(Decimal::from(10)),
                depth_imbalance: bid_depth
                    .checked_sub(ask_depth)?
                    .checked_div(bid_depth.checked_add(ask_depth)?)?,
            });
        }
        let (entry_cost, entry_depth) = leg(entry_side, entry)?;
        let (exit_cost, exit_depth) = leg(exit_side, notional)?;
        let (bid_depth, ask_depth) = if direction == MfceDirection::Long {
            (exit_depth, entry_depth)
        } else {
            (entry_depth, exit_depth)
        };
        let fraction = entry.checked_div(notional)?;
        let funding = funding_rate_hourly
            .checked_mul(holding_hours)?
            .checked_mul(Decimal::from(direction.sign()))?
            .checked_mul(BPS_PER_UNIT_RETURN)?;
        Some(MfceLiveMarketContext {
            friction_bps: entry_cost
                .checked_mul(fraction)?
                .checked_add(exit_cost)?
                .checked_add(taker_fee_bps.checked_mul(Decimal::ONE + fraction)?)?
                .checked_add(funding)?
                .max(Decimal::ZERO),
            spread_bps: (ask - bid)
                .checked_div(midpoint)?
                .checked_mul(BPS_PER_UNIT_RETURN)?,
            entry_depth_ratio: entry_depth.checked_div(entry)?.min(Decimal::from(10)),
            exit_depth_ratio: exit_depth.checked_div(notional)?.min(Decimal::from(10)),
            depth_imbalance: bid_depth
                .checked_sub(ask_depth)?
                .checked_div(bid_depth.checked_add(ask_depth)?)?,
        })
    })();
    result.ok_or_else(|| EngineError::InvalidMarket(asset.into()))
}

fn mfce_feature_vector(
    raw_source_exposure: Decimal,
    previous_source_exposure: Decimal,
    current_target: Decimal,
    proposed_target: Decimal,
    available_gross: Decimal,
    funding_rate_hourly: Decimal,
    technical: Option<&TechnicalContext>,
    fallback_atr_fraction: Option<Decimal>,
    live: Option<&MfceLiveMarketContext>,
) -> Result<MfceFeatureVector, EngineError> {
    let direction = MfceDirection::from_signed(raw_source_exposure)
        .ok_or_else(|| EngineError::Core("MFCE risk transition has no direction".into()))?;
    let sign = Decimal::from(direction.sign());
    let raw_reversal = !previous_source_exposure.is_zero()
        && previous_source_exposure.is_sign_positive() != raw_source_exposure.is_sign_positive();
    let opening = current_target.is_zero();
    let reversal =
        !opening && current_target.is_sign_positive() != proposed_target.is_sign_positive();
    let expansion = !opening && !reversal && proposed_target.abs() > current_target.abs();
    let conviction_change = if raw_reversal {
        raw_source_exposure.abs()
    } else {
        raw_source_exposure
            .abs()
            .checked_sub(previous_source_exposure.abs())
            .ok_or(EngineError::Arithmetic)?
    };
    let scale = |value: Decimal| {
        if available_gross.is_zero() {
            Ok(Decimal::ZERO)
        } else {
            value
                .checked_div(available_gross)
                .ok_or(EngineError::Arithmetic)
        }
    };
    let technical_missing = mfce_indicator(technical.is_none());
    let atr_fraction = technical
        .map(|context| context.atr_fraction)
        .or(fallback_atr_fraction)
        .unwrap_or_default();
    let aligned = |value: Decimal| value.checked_mul(sign).ok_or(EngineError::Arithmetic);
    let (
        raw_score,
        regime,
        trend_4h,
        trend_1h,
        trigger_30m,
        trigger_15m,
        ma50,
        ma100,
        trend_pullback,
        range_mean_reversion,
    ) = match technical {
        Some(context) => (
            aligned(context.raw_contextual_score)?,
            Decimal::from(context.state.regime_12h.direction)
                .checked_mul(sign)
                .ok_or(EngineError::Arithmetic)?,
            Decimal::from(context.state.trend_4h.score())
                .checked_mul(sign)
                .ok_or(EngineError::Arithmetic)?,
            Decimal::from(context.state.trend_1h.score())
                .checked_mul(sign)
                .ok_or(EngineError::Arithmetic)?,
            Decimal::from(context.state.trigger_30m.score())
                .checked_mul(sign)
                .ok_or(EngineError::Arithmetic)?,
            Decimal::from(context.state.trigger_15m.score())
                .checked_mul(sign)
                .ok_or(EngineError::Arithmetic)?,
            aligned(context.state.regime_12h.ma50_normalized_slope)?,
            aligned(context.state.regime_12h.ma100_normalized_slope)?,
            mfce_indicator(context.archetype == SignalArchetype::TrendPullback),
            mfce_indicator(context.archetype == SignalArchetype::RangeMeanReversion),
        ),
        None => (
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        ),
    };
    let values = [
        raw_source_exposure.abs(),
        conviction_change,
        sign,
        raw_source_exposure,
        scale(current_target)?,
        scale(
            proposed_target
                .checked_sub(current_target)
                .ok_or(EngineError::Arithmetic)?,
        )?,
        mfce_indicator(opening),
        mfce_indicator(expansion),
        mfce_indicator(reversal),
        atr_fraction
            .checked_mul(BPS_PER_UNIT_RETURN)
            .ok_or(EngineError::Arithmetic)?,
        mfce_indicator(live.is_none()),
        live.map_or(Decimal::ZERO, |context| context.spread_bps),
        live.map_or(Decimal::ZERO, |context| context.entry_depth_ratio),
        live.map_or(Decimal::ZERO, |context| context.exit_depth_ratio),
        live.map_or(Decimal::ZERO, |context| context.depth_imbalance),
        funding_rate_hourly
            .checked_mul(sign)
            .and_then(|value| value.checked_mul(BPS_PER_UNIT_RETURN))
            .ok_or(EngineError::Arithmetic)?,
        technical_missing,
        raw_score,
        regime,
        trend_4h,
        trend_1h,
        trigger_30m,
        trigger_15m,
        ma50,
        ma100,
        trend_pullback,
        range_mean_reversion,
    ];
    debug_assert_eq!(values.len(), MFCE_FEATURE_COUNT);
    Ok(MfceFeatureVector::new(values))
}

fn remaining_mfce_tail_budget(
    _config: &CopyTradeConfig,
    deployment: DeploymentEquity,
) -> Result<Decimal, EngineError> {
    // Edge-only: tail budget is effectively MAX so only net_q10 /
    // conservative_edge gate size. Solvency floor kept separately via
    // projection + RISK_ONLY + reconciliation. Zero/negative equity stays
    // at zero to avoid sizing on insolvency.
    if deployment.current_equity <= Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }
    // 1T USD is far above any realistic portfolio; avoids Decimal::MAX
    // overflow in downstream mul/div while acting as unbounded.
    Ok(Decimal::from(1_000_000_000_000u64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicAttribution {
    SourceOnly,
    CohortOnly,
    TechnicalOnly,
    Hybrid,
    Disagreement,
    UnattributedRecovered,
}

fn classify_economic_attribution(
    contributions: Option<&BTreeMap<String, Decimal>>,
) -> EconomicAttribution {
    let contributions = contributions
        .into_iter()
        .flatten()
        .filter(|(_, value)| !value.is_zero());
    let mut has_source = false;
    let mut has_cohort = false;
    let mut has_technical = false;
    let mut positive = false;
    let mut negative = false;
    for (component, value) in contributions {
        if component == "recovered:unattributed" {
            return EconomicAttribution::UnattributedRecovered;
        }
        has_technical |= component.starts_with("technical:");
        has_cohort |= component == "source:very_profitable_cohort";
        has_source |=
            !component.starts_with("technical:") && component != "source:very_profitable_cohort";
        positive |= value.is_sign_positive();
        negative |= value.is_sign_negative();
    }
    if positive && negative {
        EconomicAttribution::Disagreement
    } else if has_technical && (has_source || has_cohort) {
        EconomicAttribution::Hybrid
    } else if has_technical {
        EconomicAttribution::TechnicalOnly
    } else if has_cohort && !has_source {
        EconomicAttribution::CohortOnly
    } else {
        EconomicAttribution::SourceOnly
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComponentFillPartition {
    close_quantities: BTreeMap<String, Decimal>,
    open_quantities: BTreeMap<String, Decimal>,
    total_quantities: BTreeMap<String, Decimal>,
    remaining_quantities: BTreeMap<String, Decimal>,
}

fn split_component_quantities(
    current_positions: &BTreeMap<String, Decimal>,
    side: Side,
    allocations: BTreeMap<String, Decimal>,
) -> Result<ComponentFillPartition, EngineError> {
    let mut close_quantities = BTreeMap::new();
    let mut open_quantities = BTreeMap::new();
    for (component, quantity) in &allocations {
        let current = current_positions
            .get(component)
            .copied()
            .unwrap_or_default();
        let closes_position = match side {
            Side::Buy => current.is_sign_negative(),
            Side::Sell => current.is_sign_positive(),
        };
        let close = if closes_position {
            (*quantity).min(current.abs())
        } else {
            Decimal::ZERO
        };
        let open = quantity.checked_sub(close).ok_or(EngineError::Arithmetic)?;
        if !close.is_zero() {
            close_quantities.insert(component.clone(), close);
        }
        if !open.is_zero() {
            open_quantities.insert(component.clone(), open);
        }
    }
    Ok(ComponentFillPartition {
        close_quantities,
        open_quantities,
        total_quantities: allocations,
        remaining_quantities: BTreeMap::new(),
    })
}

fn pending_component_total(
    side: Side,
    component_remaining: &BTreeMap<String, Decimal>,
) -> Result<Decimal, EngineError> {
    if component_remaining.is_empty() {
        return Err(EngineError::Core(
            "pending action has no attributable remaining quantity".into(),
        ));
    }
    let total = component_remaining
        .values()
        .try_fold(Decimal::ZERO, |sum, quantity| {
            let matches_action = match side {
                Side::Buy => quantity.is_sign_positive(),
                Side::Sell => quantity.is_sign_negative(),
            };
            if quantity.is_zero() || !matches_action {
                return None;
            }
            sum.checked_add(quantity.abs())
        })
        .ok_or_else(|| EngineError::Core("pending action has invalid component quantity".into()))?;
    Ok(total)
}

fn consume_pending_component_fill(
    current_positions: &BTreeMap<String, Decimal>,
    side: Side,
    filled: Decimal,
    remaining_order_quantity: Decimal,
    component_remaining: &BTreeMap<String, Decimal>,
    size_step: Decimal,
) -> Result<ComponentFillPartition, EngineError> {
    if filled.is_zero() {
        return Ok(ComponentFillPartition {
            close_quantities: BTreeMap::new(),
            open_quantities: BTreeMap::new(),
            total_quantities: BTreeMap::new(),
            remaining_quantities: component_remaining.clone(),
        });
    }
    let component_total = pending_component_total(side, component_remaining)?;
    let (difference, tolerance) =
        reconciliation_difference(component_total, remaining_order_quantity, size_step)?;
    if difference > tolerance {
        return Err(EngineError::Core(
            "pending component quantity does not reconcile with order quantity".into(),
        ));
    }
    let remaining = component_remaining
        .iter()
        .map(|(component, quantity)| (component.clone(), quantity.abs()))
        .collect::<BTreeMap<_, _>>();
    let (overfill, tolerance) =
        reconciliation_difference(filled, remaining_order_quantity, size_step)?;
    if filled > remaining_order_quantity && overfill > tolerance {
        return Err(EngineError::Core(format!(
            "fill exceeds attributable pending order quantity: {filled} > {remaining_order_quantity}"
        )));
    }
    let full_fill = overfill <= tolerance;
    let mut allocations = if full_fill {
        remaining
    } else {
        proportional_allocations(filled, &remaining)?
    };
    canonicalize_allocation_total(&mut allocations, filled)?;

    let mut partition = split_component_quantities(current_positions, side, allocations.clone())?;
    if full_fill {
        return Ok(partition);
    }
    let mut remaining_quantities = component_remaining
        .iter()
        .map(|(component, quantity)| {
            let consumed = allocations.get(component).copied().unwrap_or_default();
            let remaining = quantity
                .abs()
                .checked_sub(consumed)
                .ok_or(EngineError::Arithmetic)?;
            if remaining < Decimal::ZERO {
                return Err(EngineError::Core(
                    "component fill exceeds its attributable remaining quantity".into(),
                ));
            }
            Ok((component.clone(), remaining))
        })
        .collect::<Result<BTreeMap<_, _>, EngineError>>()?;
    remaining_quantities.retain(|_, quantity| !quantity.is_zero());
    let expected_remaining = remaining_order_quantity
        .checked_sub(filled)
        .ok_or(EngineError::Arithmetic)?;
    if expected_remaining <= Decimal::ZERO || remaining_quantities.is_empty() {
        return Err(EngineError::Core(
            "partial fill has no attributable remaining quantity".into(),
        ));
    }
    canonicalize_allocation_total(&mut remaining_quantities, expected_remaining)?;
    partition.remaining_quantities = remaining_quantities
        .into_iter()
        .map(|(component, quantity)| {
            (
                component,
                match side {
                    Side::Buy => quantity,
                    Side::Sell => -quantity,
                },
            )
        })
        .collect();
    Ok(partition)
}

fn partition_component_fill(
    current_positions: &BTreeMap<String, Decimal>,
    side: Side,
    filled: Decimal,
    absolute_targets: &BTreeMap<String, Decimal>,
    reference_price: Decimal,
    portfolio_position_after: Decimal,
    size_step: Decimal,
) -> Result<ComponentFillPartition, EngineError> {
    if filled.is_zero() {
        return Ok(ComponentFillPartition {
            close_quantities: BTreeMap::new(),
            open_quantities: BTreeMap::new(),
            total_quantities: BTreeMap::new(),
            remaining_quantities: BTreeMap::new(),
        });
    }
    if reference_price <= Decimal::ZERO {
        return Err(EngineError::InvalidMarket(
            "component target reference price must be positive".into(),
        ));
    }
    let matches_side = |value: Decimal| match side {
        Side::Buy => value.is_sign_positive(),
        Side::Sell => value.is_sign_negative(),
    };
    let total = |quantities: &BTreeMap<String, Decimal>| {
        quantities
            .values()
            .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
            .ok_or(EngineError::Arithmetic)
    };

    // Preserve exact component quantities for a final flatten so sub-lot
    // Decimal dust cannot strand an attribution episode.
    if portfolio_position_after.is_zero() {
        let allocations = current_positions
            .iter()
            .filter(|(_, position)| !position.is_zero())
            .map(|(component, position)| (component.clone(), position.abs()))
            .collect::<BTreeMap<_, _>>();
        if allocations.is_empty()
            || current_positions
                .values()
                .any(|position| !position.is_zero() && matches_side(*position))
        {
            return Err(EngineError::Core(
                "portfolio flatten does not exclusively close component positions".into(),
            ));
        }
        let (difference, tolerance) =
            reconciliation_difference(total(&allocations)?, filled, size_step)?;
        if difference > tolerance {
            return Err(EngineError::Core(
                "portfolio flatten component quantities do not reconcile".into(),
            ));
        }
        return Ok(ComponentFillPartition {
            close_quantities: allocations.clone(),
            open_quantities: BTreeMap::new(),
            total_quantities: allocations,
            remaining_quantities: BTreeMap::new(),
        });
    }
    let signed_fill = match side {
        Side::Buy => filled,
        Side::Sell => -filled,
    };
    let portfolio_position_before = portfolio_position_after
        .checked_sub(signed_fill)
        .ok_or(EngineError::Arithmetic)?;
    let crossing = !portfolio_position_before.is_zero()
        && !portfolio_position_after.is_zero()
        && portfolio_position_before.is_sign_positive()
            != portfolio_position_after.is_sign_positive();

    if crossing {
        let closes = current_positions
            .iter()
            .filter(|(_, position)| !position.is_zero() && !matches_side(**position))
            .map(|(component, position)| (component.clone(), position.abs()))
            .collect::<BTreeMap<_, _>>();
        let close_total = total(&closes)?;
        let (difference, tolerance) =
            reconciliation_difference(close_total, portfolio_position_before.abs(), size_step)?;
        if closes.is_empty() || difference > tolerance {
            return Err(EngineError::Core(
                "crossing fill old-side component close quantity does not reconcile".into(),
            ));
        }
        let opening_quantity = filled
            .checked_sub(close_total)
            .ok_or(EngineError::Arithmetic)?;
        if opening_quantity <= Decimal::ZERO {
            return Err(EngineError::Core(
                "crossing fill has no quantity remaining for the new side".into(),
            ));
        }
        let opening_capacity = absolute_targets
            .iter()
            .filter_map(|(component, target_notional)| {
                let desired = target_notional.checked_div(reference_price)?;
                (matches_side(desired) && !desired.is_zero())
                    .then(|| (component.clone(), desired.abs()))
            })
            .collect::<BTreeMap<_, _>>();
        if opening_capacity.is_empty() {
            return Err(EngineError::Core(
                "crossing fill has no matching new-side component target".into(),
            ));
        }
        let capacity_total = total(&opening_capacity)?;
        let mut opens = if opening_quantity == capacity_total {
            opening_capacity
        } else {
            proportional_allocations(opening_quantity, &opening_capacity)?
        };
        canonicalize_allocation_total(&mut opens, opening_quantity)?;
        let mut total_quantities = closes.clone();
        for (component, quantity) in &opens {
            let combined = total_quantities
                .get(component)
                .copied()
                .unwrap_or_default()
                .checked_add(*quantity)
                .ok_or(EngineError::Arithmetic)?;
            total_quantities.insert(component.clone(), combined);
        }
        let (difference, tolerance) =
            reconciliation_difference(total(&total_quantities)?, filled, size_step)?;
        if difference > tolerance {
            return Err(EngineError::Core(
                "crossing fill component partition does not reconcile".into(),
            ));
        }
        return Ok(ComponentFillPartition {
            close_quantities: closes,
            open_quantities: opens,
            total_quantities,
            remaining_quantities: BTreeMap::new(),
        });
    }

    let components = current_positions
        .keys()
        .chain(absolute_targets.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let directional_deltas = components
        .into_iter()
        .filter_map(|component| {
            let current = current_positions
                .get(&component)
                .copied()
                .unwrap_or_default();
            let desired = absolute_targets
                .get(&component)
                .copied()
                .unwrap_or_default()
                .checked_div(reference_price)?;
            let delta = desired.checked_sub(current)?;
            (matches_side(delta) && !delta.is_zero()).then(|| (component, delta.abs()))
        })
        .collect::<BTreeMap<_, _>>();
    if directional_deltas.is_empty() {
        return Err(EngineError::Core(
            "filled portfolio delta has no matching component target delta".into(),
        ));
    }
    let directional_capacity = total(&directional_deltas)?;
    // Preserve exact component quantities when the exchange fill satisfies
    // the complete directional delta. Recomputing the same proportions can
    // move one Decimal quantum and strand an otherwise closed episode.
    let mut allocations = if filled == directional_capacity {
        directional_deltas
    } else {
        proportional_allocations(filled, &directional_deltas)?
    };
    canonicalize_allocation_total(&mut allocations, filled)?;
    if total(&allocations)? != filled {
        return Err(EngineError::Core(
            "source fill quantities do not reconcile".into(),
        ));
    }
    split_component_quantities(current_positions, side, allocations)
}

#[cfg(test)]
fn allocate_component_fill(
    current_positions: &BTreeMap<String, Decimal>,
    side: Side,
    filled: Decimal,
    absolute_targets: &BTreeMap<String, Decimal>,
    reference_price: Decimal,
    portfolio_position_after: Decimal,
    size_step: Decimal,
) -> Result<BTreeMap<String, Decimal>, EngineError> {
    Ok(partition_component_fill(
        current_positions,
        side,
        filled,
        absolute_targets,
        reference_price,
        portfolio_position_after,
        size_step,
    )?
    .total_quantities)
}

fn canonicalize_allocation_total(
    allocations: &mut BTreeMap<String, Decimal>,
    expected_total: Decimal,
) -> Result<(), EngineError> {
    if expected_total < Decimal::ZERO || allocations.values().any(|value| *value < Decimal::ZERO) {
        return Err(EngineError::Core(
            "source allocation quantities must be non-negative".into(),
        ));
    }
    let anchor = allocations
        .keys()
        .next_back()
        .cloned()
        .ok_or_else(|| EngineError::Core("source fill has no allocation anchor".into()))?;
    let actual_total = allocations
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(EngineError::Arithmetic)?;
    if actual_total < expected_total {
        let deficit = expected_total
            .checked_sub(actual_total)
            .ok_or(EngineError::Arithmetic)?;
        let value = allocations
            .get_mut(&anchor)
            .ok_or_else(|| EngineError::Core("source fill lost allocation anchor".into()))?;
        *value = value.checked_add(deficit).ok_or(EngineError::Arithmetic)?;
    } else if actual_total > expected_total {
        // Decimal proportional arithmetic can over-allocate by one or more
        // representable quanta. Trim the deterministic reverse-key tail rather
        // than failing the entire engine. This preserves every earlier exact
        // attribution whenever the stable anchor can absorb the residual.
        let mut excess = actual_total
            .checked_sub(expected_total)
            .ok_or(EngineError::Arithmetic)?;
        let keys = allocations.keys().rev().cloned().collect::<Vec<_>>();
        for key in keys {
            if excess.is_zero() {
                break;
            }
            let value = allocations
                .get_mut(&key)
                .ok_or_else(|| EngineError::Core("source allocation disappeared".into()))?;
            let reduction = (*value).min(excess);
            *value = value
                .checked_sub(reduction)
                .ok_or(EngineError::Arithmetic)?;
            excess = excess
                .checked_sub(reduction)
                .ok_or(EngineError::Arithmetic)?;
        }
        if !excess.is_zero() {
            return Err(EngineError::Core(
                "source allocation total cannot satisfy fill".into(),
            ));
        }
    }
    // Exact-zero residuals carry no attribution. Retaining one produces a
    // pending action that the shared execution restore invariant correctly
    // rejects even though all material quantity is fully attributable.
    allocations.retain(|_, quantity| !quantity.is_zero());
    let reconciled_total = allocations
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(EngineError::Arithmetic)?;
    if reconciled_total != expected_total {
        return Err(EngineError::Core(
            "source allocation total does not reconcile".into(),
        ));
    }
    Ok(())
}

fn proportional_allocations(
    total: Decimal,
    weights: &BTreeMap<String, Decimal>,
) -> Result<BTreeMap<String, Decimal>, EngineError> {
    if total.is_zero() {
        return Ok(BTreeMap::new());
    }
    let weight_sum = weights
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(EngineError::Arithmetic)?;
    if weight_sum <= Decimal::ZERO {
        return Err(EngineError::Core("invalid attribution weights".into()));
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
                .ok_or(EngineError::Arithmetic)?
                .min(remaining)
        };
        remaining = remaining
            .checked_sub(allocation)
            .ok_or(EngineError::Arithmetic)?;
        output.insert(candidate.clone(), allocation);
    }
    Ok(output)
}

fn scaled_source_execution(
    execution: &ExecutionFill,
    quantity: Decimal,
    position_before: Decimal,
) -> Result<ExecutionFill, EngineError> {
    let fraction = quantity
        .checked_div(execution.modeled_filled_quantity)
        .ok_or(EngineError::Arithmetic)?;
    let scale = |value: Decimal| value.checked_mul(fraction).ok_or(EngineError::Arithmetic);
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
        .ok_or(EngineError::Arithmetic)?;
    source.modeled_filled_notional = scale(execution.modeled_filled_notional)?;
    source.fees = scale(execution.fees)?;
    source.funding = scale(execution.funding)?;
    source.slippage = scale(execution.slippage)?;
    source.position_before = position_before;
    source.position_after = position_before
        .checked_add(signed)
        .ok_or(EngineError::Arithmetic)?;
    Ok(source)
}

fn validate_source_reconciliation(
    ledger: &DualLedger,
    asset: &str,
    portfolio_position: Decimal,
    size_step: Decimal,
) -> Result<(), EngineError> {
    let attributed = ledger
        .source_positions_for_asset(asset)
        .values()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
        .ok_or(EngineError::Arithmetic)?;
    // Source attribution divides one exchange-valid fill across independent
    // books. Decimal division can leave representation dust far below the
    // asset's exchange quantity precision even though every allocation is
    // conserved. Reconcile at an asset-specific sub-lot quantum rather than
    // treating 1e-28 bookkeeping dust as an economic position mismatch.
    let (difference, tolerance) =
        reconciliation_difference(attributed, portfolio_position, size_step)?;
    if difference > tolerance {
        return Err(EngineError::Core(format!(
            "source positions do not reconcile for {asset}: {attributed} != {portfolio_position} (difference {difference}, tolerance {tolerance})"
        )));
    }
    Ok(())
}

fn reconciliation_difference(
    attributed: Decimal,
    portfolio_position: Decimal,
    size_step: Decimal,
) -> Result<(Decimal, Decimal), EngineError> {
    if size_step <= Decimal::ZERO {
        return Err(EngineError::Core(
            "source reconciliation requires a positive size step".into(),
        ));
    }
    let tolerance = size_step
        .checked_mul(Decimal::new(1, 12))
        .ok_or(EngineError::Arithmetic)?;
    let difference = attributed
        .checked_sub(portfolio_position)
        .ok_or(EngineError::Arithmetic)?
        .abs();
    Ok((difference, tolerance))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettledActionAccounting {
    #[serde(rename = "shadow_execution_id")]
    pub execution_id: String,
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
    pub component_close_quantities: BTreeMap<String, Decimal>,
    pub component_open_quantities: BTreeMap<String, Decimal>,
    pub source_filled_quantities: BTreeMap<String, Decimal>,
    pub economic_attribution: EconomicAttribution,
    #[serde(default)]
    pub mfce_lineage: MfceExecutionLineage,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfceExecutionLineage {
    pub policy_state: Option<crate::mfce::MfcePolicyState>,
    pub admitted: bool,
    pub model_epoch: u64,
    pub transition_id: Option<u64>,
    pub allocation_fraction: Decimal,
    /// True only when MFCE explicitly authorized additional economic risk.
    /// Structural reductions remain executable with this field false.
    pub risk_increase_authorized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub micro_slots: BTreeMap<String, crate::domain::decision::MicroPositionSlot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecisionPlanCompactSummary {
    pub observed_plan_count: usize,
    pub raw_target_change_count: usize,
    pub constrained_target_change_count: usize,
    pub proposed_action_change_count: usize,
    pub executable_action_change_count: usize,
    pub below_minimum_action_count: usize,
    pub first_decision_id: Option<String>,
    pub last_decision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CohortDecisionCompactSummary {
    pub observed_record_count: usize,
    pub independent_execution_root_count: usize,
    pub reason_occurrence_counts: BTreeMap<CohortDecisionReason, usize>,
    pub attribution_occurrence_counts: BTreeMap<AttributionTag, usize>,
}

/// Why a new per-asset technical target version was created.
///
/// Materiality is evaluated against the last emitted target with the existing
/// slot-rank hysteresis. Consequently, repeated evaluations and small changes
/// below that accepted tolerance never create a new version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TechnicalDecisionReason {
    InitialActivation,
    DirectionReversal,
    MaterialIncrease,
    MaterialReduction,
    SignalNeutralized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TechnicalDecisionOutcome {
    Admitted,
    RetainedBelowDynamicFloor,
    CloseFirstStaged,
    RiskOrCapacityConstrained,
    OffsetBySource,
    Neutralized,
}

/// Immutable as-observed technical target decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TechnicalDecisionRecord {
    pub asset: String,
    pub observed_at_mono: Timestamp,
    pub candle_close_ms: u64,
    pub regime: crate::domain::technical::MarketRegime,
    pub archetype: crate::domain::technical::SignalArchetype,
    pub score: Decimal,
    pub raw_technical_target_notional: Decimal,
    pub leveraged_technical_target_notional: Decimal,
    pub post_grs_technical_target_notional: Decimal,
    pub post_source_netting_notional: Decimal,
    pub post_risk_cap_notional: Decimal,
    /// Signed, exchange-rounded order delta rather than the resulting
    /// absolute portfolio target.
    pub exchange_rounded_notional: Decimal,
    pub committed_position_notional: Decimal,
    pub order_delta_notional: Decimal,
    pub order_delta_meets_execution_floor: bool,
    pub source_target: Decimal,
    pub technical_target: Decimal,
    pub combined_target: Decimal,
    pub raw_target: Decimal,
    pub admitted_target: Decimal,
    pub dynamic_floor_notional: Decimal,
    pub target_version: u64,
    pub outcome: TechnicalDecisionOutcome,
    pub reason: TechnicalDecisionReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TechnicalDecisionUniquenessState {
    technical_target: Decimal,
    target_version: u64,
}

#[derive(Debug)]
pub enum EngineError {
    Core(String),
    InvalidMarket(String),
    Arithmetic,
    DuplicateBucket,
    InvalidBucketInterval,
}

impl Display for EngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl Error for EngineError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingAction {
    action: crate::domain::decision::PlannedAction,
    root_planned_cloid: String,
    remaining_order_quantity: Decimal,
    component_remaining: BTreeMap<String, Decimal>,
    #[serde(default)]
    mfce_lineage: MfceExecutionLineage,
    #[serde(skip)]
    execution: Option<PendingExecutionContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingExecutionContext {
    parent_planned_cloid: Option<String>,
    decision_book: ExecutableBook,
    price_tick: Decimal,
    size_step: Decimal,
    projection_input: PortfolioProjectionInput,
    risk_policy_hash: crate::domain::decision::RiskPolicyHash,
    configuration_hash: crate::domain::decision::ConfigHash,
    continuation_kind: Option<ContinuationKind>,
    economic_attribution: EconomicAttribution,
}

#[derive(Clone)]
struct PendingBookIntent {
    original_decision_id: String,
    original_planned_cloid: String,
    first_seen_at_mono: Timestamp,
}

fn projected_exposure_notional(
    current: Decimal,
    pending_signed: Option<Decimal>,
    admitted_target: Option<Decimal>,
    replaces_outstanding: bool,
) -> Result<Decimal, EngineError> {
    if replaces_outstanding {
        return Ok(current);
    }
    let mut projected = current;
    if let Some(pending_signed) = pending_signed {
        let endpoint = current
            .checked_add(pending_signed)
            .ok_or(EngineError::Arithmetic)?;
        if endpoint.abs() > projected.abs() {
            projected = endpoint;
        }
    }
    if let Some(admitted_target) = admitted_target {
        if admitted_target.abs() > projected.abs() {
            projected = admitted_target;
        }
    }
    Ok(projected)
}

fn intent_backed_target(
    awaiting_book: bool,
    continuing_ioc: bool,
    admitted_target: Option<Decimal>,
) -> Option<Decimal> {
    (awaiting_book || continuing_ioc)
        .then_some(admitted_target)
        .flatten()
}

fn action_notional_is_structurally_executable(
    reduce_only: bool,
    admitted_notional: Decimal,
    filled_notional: Decimal,
    action_notional: Decimal,
    minimum_order_notional: Decimal,
    minimum_opening_notional: Decimal,
) -> bool {
    if reduce_only && admitted_notional.is_zero() {
        return true;
    }
    let required = if filled_notional.is_zero() {
        minimum_opening_notional
    } else {
        minimum_order_notional
    };
    action_notional >= required
}

fn pending_action_is_immaterial_replacement(
    previous: &crate::domain::decision::PlannedAction,
    next: &crate::domain::decision::PlannedAction,
    notional_tolerance: Decimal,
) -> bool {
    previous.asset == next.asset
        && previous.side == next.side
        && previous.reduce_only == next.reduce_only
        && previous
            .rounded_notional
            .checked_sub(next.rounded_notional)
            .is_some_and(|difference| difference.abs() < notional_tolerance)
}

fn action_side_matches_target_delta(
    side: Side,
    current_notional: Decimal,
    desired_notional: Decimal,
) -> bool {
    let Some(delta) = desired_notional.checked_sub(current_notional) else {
        return false;
    };
    !delta.is_zero()
        && match side {
            Side::Buy => delta.is_sign_positive(),
            Side::Sell => delta.is_sign_negative(),
        }
}

fn is_selected_unissued_risk_increase(pending: &PendingAction, was_emitted: bool) -> bool {
    !was_emitted
        && !pending.action.reduce_only
        && pending.mfce_lineage.risk_increase_authorized
        && pending.mfce_lineage.admitted
        && matches!(
            pending.mfce_lineage.policy_state,
            Some(crate::mfce::MfcePolicyState::Explore | crate::mfce::MfcePolicyState::Exploit)
        )
}

fn owns_unissued_risk_increase(
    pending: &PendingAction,
    was_emitted: bool,
    context: &MfceTargetContext,
) -> bool {
    is_selected_unissued_risk_increase(pending, was_emitted)
        && action_side_matches_target_delta(
            pending.action.side,
            context.current_source_target,
            context.raw_desired_source_target,
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ContinuationKind {
    PartialIocRemainder,
    CloseFirstReversal,
}

impl Default for ContinuationKind {
    fn default() -> Self {
        Self::PartialIocRemainder
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ContinuationIntent {
    root_planned_cloid: String,
    parent_planned_cloid: String,
    next_retry_generation: u32,
    #[serde(default)]
    kind: ContinuationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionRecompute {
    None,
    #[cfg(test)]
    PartialIocRemainder,
    CloseFirstReversal,
}

impl ExecutionRecompute {
    fn is_required(self) -> bool {
        self != Self::None
    }
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
    action: &mut crate::domain::decision::PlannedAction,
    retry_generation: u32,
) -> Result<(), EngineError> {
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

fn parse_planned_cloid(value: &str) -> Result<crate::domain::decision::PlannedCloid, EngineError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 32 {
        return Err(EngineError::Core("invalid planned CLOID encoding".into()));
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| EngineError::Core("invalid planned CLOID encoding".into()))?;
    }
    Ok(crate::domain::decision::PlannedCloid(bytes))
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
    CloseFirstCompletedRecompute,
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
pub struct UnresolvedRootStatus {
    pub asset: String,
    pub root_planned_cloid: String,
    pub lifecycle: &'static str,
    pub planned_cloid: Option<String>,
    pub parent_planned_cloid: Option<String>,
    pub decision_id: Option<String>,
    pub retry_generation: Option<u32>,
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
    pub technical_funnel: TechnicalFunnel,
    pub technical_targets_admitted_sparse: u64,
    pub technical_roots_created: usize,
    pub technical_fills: u64,
    pub technical_episode_attributions: usize,
    pub close_first_staged: u64,
    pub close_first_completed: u64,
    pub post_reduction_recomputed: u64,
    pub technical_addition_emitted: u64,
    pub technical_addition_filled: u64,
    pub source_risk_increases_suppressed_below_cost_edge: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MicroDensityCounters {
    previous_raw: BTreeMap<String, Decimal>,
    previous_admitted: BTreeMap<String, Decimal>,
    raw_nonzero_target_changes: u64,
    target_changes_above_dynamic_minimum: u64,
    admitted_new_positions: u64,
    exits: u64,
    rotations: u64,
    maximum_admitted_micro_slots: usize,
    source_risk_increases_suppressed_below_cost_edge: u64,
}

#[derive(Debug, Clone, Default)]
struct TechnicalDeliveryCounters {
    targets_admitted_sparse: u64,
    roots: BTreeSet<String>,
    fills: u64,
    close_first_staged: u64,
    close_first_completed: u64,
    post_reduction_recomputed: u64,
    addition_emitted: u64,
    addition_filled: u64,
}

pub struct DecisionEngine {
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
    source_stream_mode: bool,
    source_wallet_ready: BTreeMap<String, Timestamp>,
    source_market_coverage: BTreeMap<String, Timestamp>,
    mids: Option<(MarketSnapshotResponse, Timestamp)>,
    metadata: Option<MarketMetadataResponse>,
    execution_dex_order: Vec<String>,
    /// Per-DEX collateral token identity. Native (`""`) is always USDC.
    /// Builder DEX entries are defaulted to USDC on discovery and may be
    /// overridden by operators; missing entries fail closed.
    dex_collateral: BTreeMap<String, String>,
    /// Markets using an aligned quote token (fee discount). Empty on current
    /// mainnet; retained so the fee formula structurally accepts alignment.
    aligned_quote_tokens: BTreeSet<String>,
    waiting_market_rules: BTreeSet<String>,
    metadata_received_at: Option<Timestamp>,
    metadata_valid_until: Option<Timestamp>,
    books: BTreeMap<String, (OrderBookResponse, Timestamp)>,
    stale_book_warning_assets: BTreeSet<String>,
    previous_target: Option<PreviousTargetState>,
    decision_sequence: u64,
    pending: BTreeMap<String, PendingAction>,
    pending_book: BTreeMap<String, PendingBookIntent>,
    continuations: BTreeMap<String, ContinuationIntent>,
    desired_books: BTreeSet<String>,
    ledger: DualLedger,
    metrics: EngineMetrics,
    executions: Vec<SettledActionAccounting>,
    equity_buckets: Vec<EquityReturnBucket>,
    plans: Vec<DecisionPlanningAccounting>,
    book_evaluation_events: Vec<BookEvaluationEvent>,
    action_lifecycle_events: Vec<ActionLifecycleEvent>,
    last_bucket: Option<(Timestamp, Decimal, BTreeMap<String, Decimal>)>,
    last_funding_accrual: Option<Timestamp>,
    accrued_funding: BTreeMap<String, Decimal>,
    applied_live_funding: BTreeSet<FundingEventId>,
    micro_density: MicroDensityCounters,
    technical_delivery: TechnicalDeliveryCounters,
    prepared_authorized_intents: Vec<AuthorizedExecutionIntent>,
    emitted_production_cloids: BTreeSet<crate::domain::decision::PlannedCloid>,
    production_identity: Option<ProductionIntentIdentity>,
    // Current-process target eligibility is never inferred from saved exposure.
    strategy_targets_ready: bool,
    production_positions: Option<BTreeMap<String, Decimal>>,
    production_equities: Option<(Decimal, Decimal, Decimal)>,
    reconciliation_mode: crate::domain::live_trading::ReconciliationMode,
    technical_engine: TechnicalEngine,
    technical_targets: BTreeMap<String, TechnicalTarget>,
    technical_decision_state: BTreeMap<String, TechnicalDecisionUniquenessState>,
    technical_decision_records: Vec<TechnicalDecisionRecord>,
    very_profitable_layer: Option<PreparedVeryProfitableLayer>,
    very_profitable_engine: VeryProfitableCohortEngine,
    cohort_indicator_records: Vec<CohortIndicatorRecord>,
    mfce: MfceEngine,
    last_delayed_service: u64,
    mfce_authorized_assets: BTreeMap<String, Timestamp>,
    /// Maps process-local monotonic timestamps into one restart-comparable
    /// durable timeline. The anchor is reconstructed from wall time once at
    /// process startup and advances only with the process monotonic clock.
    durable_time_offset: Timestamp,
    mfce_time_high_watermark: Timestamp,
    ledger_time_high_watermark: Timestamp,
    snapshot_generation: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct LiveExecutionCheckpoint {
    delayed: crate::mfce_delayed::DelayedLearning,
    ledger: DualLedger,
    pending: BTreeMap<String, PendingAction>,
    continuations: BTreeMap<String, ContinuationIntent>,
    accrued_funding: BTreeMap<String, Decimal>,
    applied_live_funding: BTreeSet<FundingEventId>,
    executions: Vec<SettledActionAccounting>,
    action_lifecycle_events: Vec<ActionLifecycleEvent>,
    desired_books: BTreeSet<String>,
    emitted_production_cloids: BTreeSet<crate::domain::decision::PlannedCloid>,
    metrics: EngineMetrics,
    technical_delivery: TechnicalDeliveryCounters,
    ledger_time_high_watermark: Timestamp,
}

pub const LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION: u32 = 12;
pub const UNSIGNED_SNAPSHOT_SCHEMA_VERSION: u32 = 13;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIdentity {
    pub source_tree_sha256: String,
    pub observer_binary_sha256: String,
    pub configuration_sha256: String,
    pub risk_policy_sha256: String,
}

pub type StateIdentity = SnapshotIdentity;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsignedObserverState {
    ledger: DualLedger,
    target_ledger: VirtualTargetLedger,
    technical_engine: TechnicalEngine,
    #[serde(default)]
    technical_decision_state: BTreeMap<String, TechnicalDecisionUniquenessState>,
    very_profitable_engine: VeryProfitableCohortEngine,
    very_profitable_layer_artifact_sha256: Option<String>,
    decision_sequence: u64,
    pending: BTreeMap<String, PendingAction>,
    continuations: BTreeMap<String, ContinuationIntent>,
    accrued_funding: BTreeMap<String, Decimal>,
    #[serde(default)]
    applied_live_funding: BTreeSet<FundingEventId>,
    last_mids: Option<MarketSnapshotResponse>,
    #[serde(default, rename = "history_complete_through_ms")]
    legacy_history_complete_through_ms: Option<u64>,
    mfce: MfcePersistentState,
    mfce_time_high_watermark: Timestamp,
    ledger_time_high_watermark: Timestamp,
    executions: Vec<SettledActionAccounting>,
    equity_buckets: Vec<EquityReturnBucket>,
    last_bucket: Option<(Timestamp, Decimal, BTreeMap<String, Decimal>)>,
    micro_density: MicroDensityCounters,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    waiting_market_rules: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsignedSnapshotEnvelope {
    pub schema_version: u32,
    pub generation: u64,
    pub identity: SnapshotIdentity,
    pub payload: UnsignedObserverState,
    pub checksum_sha256: [u8; 32],
}

/// Exact v12 positional payload. Only the MFCE field differs from the current
/// payload; keeping a dedicated type makes legacy checksum verification typed
/// and prevents permissive deserialization from becoming a runtime policy.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyUnsignedObserverStateV12<M> {
    ledger: DualLedger,
    target_ledger: VirtualTargetLedger,
    technical_engine: TechnicalEngine,
    #[serde(default)]
    technical_decision_state: BTreeMap<String, TechnicalDecisionUniquenessState>,
    very_profitable_engine: VeryProfitableCohortEngine,
    very_profitable_layer_artifact_sha256: Option<String>,
    decision_sequence: u64,
    pending: BTreeMap<String, PendingAction>,
    continuations: BTreeMap<String, ContinuationIntent>,
    accrued_funding: BTreeMap<String, Decimal>,
    #[serde(default)]
    applied_live_funding: BTreeSet<FundingEventId>,
    last_mids: Option<MarketSnapshotResponse>,
    #[serde(default, rename = "history_complete_through_ms")]
    legacy_history_complete_through_ms: Option<u64>,
    mfce: M,
    mfce_time_high_watermark: Timestamp,
    ledger_time_high_watermark: Timestamp,
    executions: Vec<SettledActionAccounting>,
    equity_buckets: Vec<EquityReturnBucket>,
    last_bucket: Option<(Timestamp, Decimal, BTreeMap<String, Decimal>)>,
    micro_density: MicroDensityCounters,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    waiting_market_rules: BTreeSet<String>,
}

impl<M: LegacyMfcePersistentState> LegacyUnsignedObserverStateV12<M> {
    fn migrate(self) -> UnsignedObserverState {
        UnsignedObserverState {
            ledger: self.ledger,
            target_ledger: self.target_ledger,
            technical_engine: self.technical_engine,
            technical_decision_state: self.technical_decision_state,
            very_profitable_engine: self.very_profitable_engine,
            very_profitable_layer_artifact_sha256: self.very_profitable_layer_artifact_sha256,
            decision_sequence: self.decision_sequence,
            pending: self.pending,
            continuations: self.continuations,
            accrued_funding: self.accrued_funding,
            applied_live_funding: self.applied_live_funding,
            last_mids: self.last_mids,
            legacy_history_complete_through_ms: self.legacy_history_complete_through_ms,
            mfce: self.mfce.migrate(),
            mfce_time_high_watermark: self.mfce_time_high_watermark,
            ledger_time_high_watermark: self.ledger_time_high_watermark,
            executions: self.executions,
            equity_buckets: self.equity_buckets,
            last_bucket: self.last_bucket,
            micro_density: self.micro_density,
            waiting_market_rules: self.waiting_market_rules,
        }
    }
}

#[cfg(test)]
impl<M> LegacyUnsignedObserverStateV12<M> {
    fn from_current(current: UnsignedObserverState, mfce: M) -> Self {
        Self {
            ledger: current.ledger,
            target_ledger: current.target_ledger,
            technical_engine: current.technical_engine,
            technical_decision_state: current.technical_decision_state,
            very_profitable_engine: current.very_profitable_engine,
            very_profitable_layer_artifact_sha256: current.very_profitable_layer_artifact_sha256,
            decision_sequence: current.decision_sequence,
            pending: current.pending,
            continuations: current.continuations,
            accrued_funding: current.accrued_funding,
            applied_live_funding: current.applied_live_funding,
            last_mids: current.last_mids,
            legacy_history_complete_through_ms: current.legacy_history_complete_through_ms,
            mfce,
            mfce_time_high_watermark: current.mfce_time_high_watermark,
            ledger_time_high_watermark: current.ledger_time_high_watermark,
            executions: current.executions,
            equity_buckets: current.equity_buckets,
            last_bucket: current.last_bucket,
            micro_density: current.micro_density,
            waiting_market_rules: current.waiting_market_rules,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyUnsignedSnapshotEnvelopeV12<M> {
    schema_version: u32,
    generation: u64,
    identity: SnapshotIdentity,
    payload: LegacyUnsignedObserverStateV12<M>,
    checksum_sha256: [u8; 32],
}

fn unsigned_snapshot_checksum(
    schema_version: u32,
    generation: u64,
    identity: &SnapshotIdentity,
    payload: &UnsignedObserverState,
) -> Result<[u8; 32], EngineError> {
    let canonical = rmp_serde::to_vec(&(schema_version, generation, identity, payload))
        .map_err(|error| EngineError::Core(error.to_string()))?;
    Ok(Sha256::digest(canonical).into())
}

fn encode_unsigned_snapshot(envelope: &UnsignedSnapshotEnvelope) -> Result<Vec<u8>, EngineError> {
    rmp_serde::to_vec(envelope).map_err(|error| EngineError::Core(error.to_string()))
}

fn legacy_unsigned_snapshot_checksum<M: Serialize>(
    schema_version: u32,
    generation: u64,
    identity: &SnapshotIdentity,
    payload: &LegacyUnsignedObserverStateV12<M>,
) -> Result<[u8; 32], EngineError> {
    let canonical = rmp_serde::to_vec(&(schema_version, generation, identity, payload))
        .map_err(|error| EngineError::Core(error.to_string()))?;
    Ok(Sha256::digest(canonical).into())
}

fn normalize_unsigned_snapshot_payload(
    payload: UnsignedObserverState,
) -> Result<UnsignedObserverState, EngineError> {
    let encoded =
        rmp_serde::to_vec(&payload).map_err(|error| EngineError::Core(error.to_string()))?;
    rmp_serde::from_slice(&encoded).map_err(|error| EngineError::Core(error.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyMfceWireVariant {
    V12A,
    V12B,
    V12C,
    V12D,
    V12E,
    V12F,
    V12G,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V13WireVariant {
    Current,
    /// Exact v13 layout written before MFCE execution lineage and the three
    /// trajectory-regret fields became durable.
    LegacyV13A,
}

fn wire_signature(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Bool(_) => "bool".into(),
        serde_json::Value::Number(number) if number.is_u64() => "u64".into(),
        serde_json::Value::Number(number) if number.is_i64() => "i64".into(),
        serde_json::Value::Number(_) => "number".into(),
        serde_json::Value::String(_) => "string".into(),
        serde_json::Value::Array(values) => {
            let members = values
                .iter()
                .take(16)
                .map(wire_signature)
                .collect::<Vec<_>>()
                .join(",");
            format!("array[{}]<{}>", values.len(), members)
        }
        serde_json::Value::Object(values) => format!("map[{}]", values.len()),
    }
}

fn wire_mismatch(path: &str, observed: &serde_json::Value, expected: &str) -> EngineError {
    EngineError::Core(format!(
        "unsigned snapshot wire mismatch path={path} observed={} expected={expected}",
        wire_signature(observed)
    ))
}

fn wire_child_path(path: &str, index: usize) -> String {
    let name = match (path, index) {
        ("$", 0) => Some("schema_version"),
        ("$", 1) => Some("generation"),
        ("$", 2) => Some("identity"),
        ("$", 3) => Some("payload"),
        ("$", 4) => Some("checksum_sha256"),
        ("$.payload", 7) => Some("pending"),
        ("$.payload", 13) => Some("mfce"),
        ("$.payload", 16) => Some("executions"),
        ("$.payload.mfce", 5) => Some("assets"),
        ("$.payload.mfce", 6) => Some("samples"),
        ("$.payload.mfce", 7) => Some("recovered_episodes"),
        ("$.payload.mfce", 8) => Some("incumbent"),
        ("$.payload.mfce", 9) => Some("pending_training"),
        ("$.payload.mfce", 10) => Some("decision_counts"),
        ("$.payload.mfce", 11) => Some("delayed"),
        ("$.payload.mfce.delayed", 0) => Some("samples"),
        ("$.payload.mfce.delayed", 4) => Some("turnover"),
        ("$.payload.mfce.delayed", 6) => Some("provenance"),
        _ => None,
    };
    name.map_or_else(
        || format!("{path}[{index}]"),
        |name| format!("{path}.{name}"),
    )
}

fn first_wire_difference(
    observed: &serde_json::Value,
    expected: &serde_json::Value,
    path: &str,
) -> Option<String> {
    match (observed, expected) {
        (serde_json::Value::Array(observed), serde_json::Value::Array(expected)) => {
            if observed.len() != expected.len() {
                return Some(format!(
                    "path={path} observed=array[{}] expected=array[{}]",
                    observed.len(),
                    expected.len()
                ));
            }
            observed
                .iter()
                .zip(expected)
                .enumerate()
                .find_map(|(index, (observed, expected))| {
                    first_wire_difference(observed, expected, &wire_child_path(path, index))
                })
        }
        (serde_json::Value::Object(observed), serde_json::Value::Object(expected)) => {
            if observed.len() != expected.len() || observed.keys().ne(expected.keys()) {
                return Some(format!(
                    "path={path} observed=map[{}] expected=map[{}]",
                    observed.len(),
                    expected.len()
                ));
            }
            observed
                .values()
                .zip(expected.values())
                .find_map(|(observed, expected)| {
                    first_wire_difference(observed, expected, &format!("{path}[*]"))
                })
        }
        _ if observed == expected => None,
        _ => Some(format!(
            "path={path} observed={} expected={}",
            wire_signature(observed),
            wire_signature(expected)
        )),
    }
}

fn noncanonical_wire_error(bytes: &[u8], canonical: &[u8]) -> EngineError {
    let mismatch = rmp_serde::from_slice::<serde_json::Value>(bytes)
        .ok()
        .zip(rmp_serde::from_slice::<serde_json::Value>(canonical).ok())
        .and_then(|(observed, expected)| first_wire_difference(&observed, &expected, "$"))
        .unwrap_or_else(|| {
            "path=$ observed=noncanonical-byte-encoding expected=canonical-messagepack".into()
        });
    EngineError::Core(format!("unsigned snapshot wire mismatch {mismatch}"))
}

fn observe_row_arity(
    value: &serde_json::Value,
    path: &str,
    current: usize,
    legacy: usize,
    saw_current: &mut bool,
    saw_legacy: &mut bool,
) -> Result<(), EngineError> {
    let fields = wire_array(value, path)?;
    match fields.len() {
        length if length == current => *saw_current = true,
        length if length == legacy => *saw_legacy = true,
        _ => {
            return Err(wire_mismatch(
                path,
                value,
                &format!("array[{legacy}]|array[{current}]"),
            ));
        }
    }
    Ok(())
}

fn inspect_v13_wire(bytes: &[u8]) -> Result<V13WireVariant, EngineError> {
    let value: serde_json::Value = rmp_serde::from_slice(bytes).map_err(|_| {
        EngineError::Core(
            "unsigned snapshot wire mismatch path=$ observed=invalid-msgpack expected=array[5]"
                .into(),
        )
    })?;
    let envelope = require_array_len(&value, "$", 5)?;
    let payload = wire_array(&envelope[3], "$.payload")?;
    if !matches!(payload.len(), 20 | 21) {
        return Err(wire_mismatch(
            "$.payload",
            &envelope[3],
            "array[20]|array[21]",
        ));
    }
    let mut saw_current = false;
    let mut saw_legacy = false;
    let pending = payload[7]
        .as_object()
        .ok_or_else(|| wire_mismatch("$.payload.pending", &payload[7], "map"))?;
    for row in pending.values() {
        observe_row_arity(
            row,
            "$.payload.pending[*]",
            5,
            4,
            &mut saw_current,
            &mut saw_legacy,
        )?;
    }
    let executions = wire_array(&payload[16], "$.payload.executions")?;
    for row in executions {
        observe_row_arity(
            row,
            "$.payload.executions[*]",
            36,
            35,
            &mut saw_current,
            &mut saw_legacy,
        )?;
    }
    let mfce = wire_array(&payload[13], "$.payload.mfce")?;
    if let Some(delayed) = mfce.get(11) {
        let delayed = require_array_len(delayed, "$.payload.mfce.delayed", 7)?;
        for row in wire_array(&delayed[0], "$.payload.mfce.delayed.samples")? {
            observe_row_arity(
                row,
                "$.payload.mfce.delayed.samples[*]",
                35,
                32,
                &mut saw_current,
                &mut saw_legacy,
            )?;
        }
        let turnover = delayed[4]
            .as_object()
            .ok_or_else(|| wire_mismatch("$.payload.mfce.delayed.turnover", &delayed[4], "map"))?;
        for row in turnover.values() {
            observe_row_arity(
                row,
                "$.payload.mfce.delayed.turnover[*]",
                4,
                3,
                &mut saw_current,
                &mut saw_legacy,
            )?;
        }
    }
    match (saw_current, saw_legacy) {
        (true, true) => Err(EngineError::Core(
            "unsigned snapshot wire mismatch path=$.payload observed=mixed-v13-topologies expected=uniform-current|LegacyV13A"
                .into(),
        )),
        (false, true) => Ok(V13WireVariant::LegacyV13A),
        _ => Ok(V13WireVariant::Current),
    }
}

fn append_wire_field<T: Serialize>(
    row: &mut serde_json::Value,
    value: &T,
) -> Result<(), EngineError> {
    let encoded = rmp_serde::to_vec(value).map_err(|error| EngineError::Core(error.to_string()))?;
    let value =
        rmp_serde::from_slice(&encoded).map_err(|error| EngineError::Core(error.to_string()))?;
    row.as_array_mut()
        .expect("exact legacy row was inspected before migration")
        .push(value);
    Ok(())
}

fn decode_legacy_v13a_snapshot(
    bytes: &[u8],
    expected_identity: &SnapshotIdentity,
) -> Result<UnsignedSnapshotEnvelope, EngineError> {
    let mut wire: serde_json::Value =
        rmp_serde::from_slice(bytes).map_err(|error| EngineError::Core(error.to_string()))?;
    let canonical =
        rmp_serde::to_vec(&wire).map_err(|error| EngineError::Core(error.to_string()))?;
    if canonical != bytes {
        return Err(noncanonical_wire_error(bytes, &canonical));
    }
    let envelope = wire
        .as_array_mut()
        .expect("exact v13 envelope was inspected before migration");
    let checksum: [u8; 32] = serde_json::from_value(envelope[4].clone())
        .map_err(|error| EngineError::Core(error.to_string()))?;
    let checksum_wire = serde_json::Value::Array(envelope[..4].to_vec());
    let checksum_bytes =
        rmp_serde::to_vec(&checksum_wire).map_err(|error| EngineError::Core(error.to_string()))?;
    let expected_checksum: [u8; 32] = Sha256::digest(checksum_bytes).into();
    if checksum != expected_checksum {
        return Err(EngineError::Core(
            "unsigned snapshot checksum mismatch".into(),
        ));
    }
    let payload = envelope[3]
        .as_array_mut()
        .expect("exact v13 payload was inspected before migration");
    let lineage = MfceExecutionLineage::default();
    for row in payload[7]
        .as_object_mut()
        .expect("exact pending map was inspected before migration")
        .values_mut()
    {
        append_wire_field(row, &lineage)?;
    }
    for row in payload[16]
        .as_array_mut()
        .expect("exact execution rows were inspected before migration")
    {
        append_wire_field(row, &lineage)?;
    }
    let mfce = payload[13]
        .as_array_mut()
        .expect("exact MFCE state was inspected before migration");
    if let Some(delayed) = mfce.get_mut(11) {
        let delayed = delayed
            .as_array_mut()
            .expect("exact delayed state was inspected before migration");
        for row in delayed[0]
            .as_array_mut()
            .expect("exact delayed samples were inspected before migration")
        {
            append_wire_field(row, &false)?;
            append_wire_field(
                row,
                &[Option::<Decimal>::None; crate::mfce_delayed::HORIZON_COUNT],
            )?;
            append_wire_field(
                row,
                &[Option::<Decimal>::None; crate::mfce_delayed::HORIZON_COUNT],
            )?;
        }
        for row in delayed[4]
            .as_object_mut()
            .expect("exact turnover map was inspected before migration")
            .values_mut()
        {
            append_wire_field(row, &0_u64)?;
        }
    }
    let migrated =
        rmp_serde::to_vec(&wire).map_err(|error| EngineError::Core(error.to_string()))?;
    let mut envelope: UnsignedSnapshotEnvelope = rmp_serde::from_slice(&migrated).map_err(|_| {
        EngineError::Core(
            "unsigned snapshot wire mismatch path=$ observed=LegacyV13A expected=exact-recovered-layout"
                .into(),
        )
    })?;
    if &envelope.identity != expected_identity {
        return Err(EngineError::Core(
            "unsigned snapshot identity mismatch".into(),
        ));
    }
    envelope.checksum_sha256 = unsigned_snapshot_checksum(
        envelope.schema_version,
        envelope.generation,
        &envelope.identity,
        &envelope.payload,
    )?;
    Ok(envelope)
}

fn wire_array<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Result<&'a [serde_json::Value], EngineError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| wire_mismatch(path, value, "array"))
}

fn wire_field<'a>(
    values: &'a [serde_json::Value],
    index: usize,
    path: &str,
) -> Result<&'a serde_json::Value, EngineError> {
    values.get(index).ok_or_else(|| {
        EngineError::Core(format!(
            "unsigned snapshot wire mismatch path={path} observed=array[{}] expected=index[{index}]",
            values.len()
        ))
    })
}

fn require_array_len<'a>(
    value: &'a serde_json::Value,
    path: &str,
    expected: usize,
) -> Result<&'a [serde_json::Value], EngineError> {
    let values = wire_array(value, path)?;
    if values.len() != expected {
        return Err(wire_mismatch(path, value, &format!("array[{expected}]")));
    }
    Ok(values)
}

fn require_optional_array_len(
    value: &serde_json::Value,
    path: &str,
    expected: usize,
) -> Result<(), EngineError> {
    if !value.is_null() {
        require_array_len(value, path, expected)?;
    }
    Ok(())
}

fn inspect_legacy_lifecycle_tail(
    mfce: &[serde_json::Value],
    recovered_index: Option<usize>,
    incumbent_index: usize,
    pending_index: usize,
    counts_index: Option<usize>,
) -> Result<(), EngineError> {
    if let Some(index) = recovered_index {
        wire_array(
            wire_field(mfce, index, "$.payload.mfce.recovered_episodes")?,
            "$.payload.mfce.recovered_episodes",
        )?;
    }
    require_optional_array_len(
        wire_field(mfce, incumbent_index, "$.payload.mfce.incumbent")?,
        "$.payload.mfce.incumbent",
        9,
    )?;
    if let Some(incumbent) = mfce.get(incumbent_index).filter(|value| !value.is_null()) {
        let incumbent = require_array_len(incumbent, "$.payload.mfce.incumbent", 9)?;
        require_array_len(&incumbent[8], "$.payload.mfce.incumbent.validation", 6)?;
    }
    require_optional_array_len(
        wire_field(mfce, pending_index, "$.payload.mfce.pending_training")?,
        "$.payload.mfce.pending_training",
        4,
    )?;
    if let Some(index) = counts_index {
        require_array_len(
            wire_field(mfce, index, "$.payload.mfce.decision_counts")?,
            "$.payload.mfce.decision_counts",
            4,
        )?;
    }
    Ok(())
}

fn inspect_legacy_feature_rows(
    mfce: &[serde_json::Value],
    feature_arity: usize,
) -> Result<(), EngineError> {
    let assets = wire_field(mfce, 5, "$.payload.mfce.assets")?
        .as_object()
        .ok_or_else(|| wire_mismatch("$.payload.mfce.assets", &mfce[5], "map"))?;
    for asset in assets.values() {
        let expected_asset_arity = if feature_arity == 2 { 7 } else { 8 };
        let fields = require_array_len(asset, "$.payload.mfce.assets[*]", expected_asset_arity)?;
        if let Some(active) = fields.get(4).filter(|value| !value.is_null()) {
            let active = require_array_len(active, "$.payload.mfce.assets[*].active", 9)?;
            require_array_len(
                &active[7],
                "$.payload.mfce.assets[*].active.features",
                feature_arity,
            )?;
        }
    }
    let samples = wire_array(
        wire_field(mfce, 6, "$.payload.mfce.samples")?,
        "$.payload.mfce.samples",
    )?;
    for sample in samples {
        let fields = require_array_len(sample, "$.payload.mfce.samples[*]", 10)?;
        require_array_len(
            &fields[6],
            "$.payload.mfce.samples[*].features",
            feature_arity,
        )?;
    }
    Ok(())
}

fn inspect_legacy_delayed(
    delayed: &serde_json::Value,
) -> Result<LegacyMfceWireVariant, EngineError> {
    let fields = wire_array(delayed, "$.payload.mfce.delayed")?;
    if !matches!(fields.len(), 6 | 7) {
        return Err(wire_mismatch(
            "$.payload.mfce.delayed",
            delayed,
            "array[6]|array[7]",
        ));
    }
    let samples = wire_array(&fields[0], "$.payload.mfce.delayed.samples")?;
    for sample in samples {
        let row = require_array_len(sample, "$.payload.mfce.delayed.samples[*]", 27)?;
        require_array_len(&row[8], "$.payload.mfce.delayed.samples[*].features", 3)?;
        if !row[9].is_null() {
            require_array_len(&row[9], "$.payload.mfce.delayed.samples[*].prediction", 3)?;
        }
        for (index, name) in [(13, "anchor"), (14, "larger_anchor")] {
            require_optional_array_len(
                &row[index],
                &format!("$.payload.mfce.delayed.samples[*].{name}"),
                5,
            )?;
        }
        for (index, name) in [
            (15, "forward"),
            (20, "entry_regret"),
            (21, "exit_regret"),
            (22, "cost_drag"),
            (26, "size_regret"),
        ] {
            require_array_len(
                &row[index],
                &format!("$.payload.mfce.delayed.samples[*].{name}"),
                5,
            )?;
        }
        for outcome in wire_array(&row[15], "$.payload.mfce.delayed.samples[*].forward")? {
            require_optional_array_len(outcome, "$.payload.mfce.delayed.samples[*].forward[*]", 5)?;
        }
        require_optional_array_len(
            &row[25],
            "$.payload.mfce.delayed.samples[*].actual_result",
            5,
        )?;
    }
    let flow = fields[3]
        .as_object()
        .ok_or_else(|| wire_mismatch("$.payload.mfce.delayed.flow", &fields[3], "map"))?;
    for market_flow in flow.values() {
        let market_flow = require_array_len(market_flow, "$.payload.mfce.delayed.flow[*]", 5)?;
        let bins = require_array_len(&market_flow[0], "$.payload.mfce.delayed.flow[*].bins", 30)?;
        for bin in bins {
            require_array_len(bin, "$.payload.mfce.delayed.flow[*].bins[*]", 4)?;
        }
    }
    let turnover = fields[4]
        .as_object()
        .ok_or_else(|| wire_mismatch("$.payload.mfce.delayed.turnover", &fields[4], "map"))?;
    for row in turnover.values() {
        require_array_len(row, "$.payload.mfce.delayed.turnover[*]", 3)?;
    }
    if fields.len() == 6 {
        return Ok(LegacyMfceWireVariant::V12D);
    }
    let provenance = require_array_len(&fields[6], "$.payload.mfce.delayed.provenance", 2)?;
    let episodes = provenance[0].as_object().ok_or_else(|| {
        wire_mismatch(
            "$.payload.mfce.delayed.provenance.episodes",
            &provenance[0],
            "map",
        )
    })?;
    let mut saw_four = false;
    let mut saw_five = false;
    for episode in episodes.values() {
        let values = wire_array(episode, "$.payload.mfce.delayed.provenance.episodes[*]")?;
        if !matches!(values.len(), 4 | 5) {
            return Err(wire_mismatch(
                "$.payload.mfce.delayed.provenance.episodes[*]",
                episode,
                "array[4]|array[5]",
            ));
        }
        if values.len() == 4 {
            saw_four = true;
        } else {
            saw_five = true;
            require_optional_array_len(
                &values[4],
                "$.payload.mfce.delayed.provenance.episodes[*].settled",
                3,
            )?;
            if values[4].is_null() {
                return Err(wire_mismatch(
                    "$.payload.mfce.delayed.provenance.episodes[*].settled",
                    &values[4],
                    "array[3]",
                ));
            }
        }
    }
    let actions = provenance[1].as_object().ok_or_else(|| {
        wire_mismatch(
            "$.payload.mfce.delayed.provenance.actions",
            &provenance[1],
            "map",
        )
    })?;
    for action in actions.values() {
        require_array_len(action, "$.payload.mfce.delayed.provenance.actions[*]", 3)?;
    }
    Ok(match (saw_four, saw_five) {
        (true, false) => LegacyMfceWireVariant::V12E,
        (true, true) => LegacyMfceWireVariant::V12G,
        // Empty provenance is byte-identical; select the latest uniform layout.
        (false, _) => LegacyMfceWireVariant::V12F,
    })
}

fn inspect_unsigned_snapshot_wire(
    bytes: &[u8],
) -> Result<(u32, Option<LegacyMfceWireVariant>), EngineError> {
    let value: serde_json::Value = rmp_serde::from_slice(bytes).map_err(|_| {
        EngineError::Core(
            "unsigned snapshot wire mismatch path=$ observed=invalid-msgpack expected=array[5]"
                .into(),
        )
    })?;
    let envelope = require_array_len(&value, "$", 5)?;
    let schema_u64 = envelope[0]
        .as_u64()
        .ok_or_else(|| wire_mismatch("$.schema_version", &envelope[0], "u32"))?;
    let schema = u32::try_from(schema_u64)
        .map_err(|_| wire_mismatch("$.schema_version", &envelope[0], "u32"))?;
    if schema == UNSIGNED_SNAPSHOT_SCHEMA_VERSION {
        return Ok((schema, None));
    }
    if schema != LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION {
        return Err(EngineError::Core(format!(
            "unsigned snapshot wire mismatch path=$.schema_version observed=u32 expected={LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION}|{UNSIGNED_SNAPSHOT_SCHEMA_VERSION}"
        )));
    }
    let payload = wire_array(&envelope[3], "$.payload")?;
    if !matches!(payload.len(), 20 | 21) {
        return Err(wire_mismatch(
            "$.payload",
            &envelope[3],
            "array[20]|array[21]",
        ));
    }
    let mfce_value = wire_field(payload, 13, "$.payload.mfce")?;
    let mfce = wire_array(mfce_value, "$.payload.mfce")?;
    let variant = match mfce.len() {
        9 => {
            inspect_legacy_feature_rows(mfce, 2)?;
            inspect_legacy_lifecycle_tail(mfce, None, 7, 8, None)?;
            LegacyMfceWireVariant::V12A
        }
        10 => {
            inspect_legacy_feature_rows(mfce, 2)?;
            inspect_legacy_lifecycle_tail(mfce, None, 7, 8, Some(9))?;
            LegacyMfceWireVariant::V12B
        }
        11 => {
            inspect_legacy_feature_rows(mfce, 2)?;
            inspect_legacy_lifecycle_tail(mfce, Some(7), 8, 9, Some(10))?;
            LegacyMfceWireVariant::V12C
        }
        12 => {
            inspect_legacy_feature_rows(mfce, 3)?;
            inspect_legacy_lifecycle_tail(mfce, Some(7), 8, 9, Some(10))?;
            inspect_legacy_delayed(&mfce[11])?
        }
        _ => {
            return Err(wire_mismatch(
                "$.payload.mfce",
                mfce_value,
                "array[9]|array[10]|array[11]|array[12]",
            ));
        }
    };
    Ok((schema, Some(variant)))
}

fn decode_legacy_unsigned_snapshot<M>(
    bytes: &[u8],
    expected_identity: &SnapshotIdentity,
    variant: LegacyMfceWireVariant,
) -> Result<UnsignedSnapshotEnvelope, EngineError>
where
    M: serde::de::DeserializeOwned + Serialize + LegacyMfcePersistentState,
{
    let legacy: LegacyUnsignedSnapshotEnvelopeV12<M> = rmp_serde::from_slice(bytes).map_err(|_| {
        EngineError::Core(format!(
            "unsigned snapshot wire mismatch path=$.payload.mfce observed={variant:?} expected=exact-recovered-layout"
        ))
    })?;
    let canonical =
        rmp_serde::to_vec(&legacy).map_err(|error| EngineError::Core(error.to_string()))?;
    if canonical != bytes {
        return Err(noncanonical_wire_error(bytes, &canonical));
    }
    if legacy.schema_version != LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION
        || !(1..MFCE_STATE_SCHEMA_VERSION).contains(&legacy.payload.mfce.schema_version())
    {
        return Err(EngineError::Core(
            "unsupported unsigned snapshot schema".into(),
        ));
    }
    if &legacy.identity != expected_identity {
        return Err(EngineError::Core(
            "unsigned snapshot identity mismatch".into(),
        ));
    }
    let checksum = legacy_unsigned_snapshot_checksum(
        legacy.schema_version,
        legacy.generation,
        &legacy.identity,
        &legacy.payload,
    )?;
    if legacy.checksum_sha256 != checksum {
        return Err(EngineError::Core(
            "unsigned snapshot checksum mismatch".into(),
        ));
    }
    Ok(UnsignedSnapshotEnvelope {
        schema_version: UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
        generation: legacy.generation,
        identity: legacy.identity,
        payload: legacy.payload.migrate(),
        checksum_sha256: legacy.checksum_sha256,
    })
}

fn decode_unsigned_snapshot(
    bytes: &[u8],
    expected_identity: &SnapshotIdentity,
) -> Result<UnsignedSnapshotEnvelope, EngineError> {
    let (schema, legacy_variant) = inspect_unsigned_snapshot_wire(bytes)?;
    if schema == UNSIGNED_SNAPSHOT_SCHEMA_VERSION {
        if inspect_v13_wire(bytes)? == V13WireVariant::LegacyV13A {
            return decode_legacy_v13a_snapshot(bytes, expected_identity);
        }
        let envelope: UnsignedSnapshotEnvelope = rmp_serde::from_slice(bytes).map_err(|_| {
            EngineError::Core(
                "unsigned snapshot wire mismatch path=$ observed=array[5] expected=strict-v13"
                    .into(),
            )
        })?;
        let canonical = encode_unsigned_snapshot(&envelope)?;
        if canonical != bytes {
            return Err(noncanonical_wire_error(bytes, &canonical));
        }
        if &envelope.identity != expected_identity {
            return Err(EngineError::Core(
                "unsigned snapshot identity mismatch".into(),
            ));
        }
        let checksum = unsigned_snapshot_checksum(
            envelope.schema_version,
            envelope.generation,
            &envelope.identity,
            &envelope.payload,
        )?;
        if envelope.checksum_sha256 != checksum {
            return Err(EngineError::Core(
                "unsigned snapshot checksum mismatch".into(),
            ));
        }
        return Ok(envelope);
    }
    match legacy_variant.expect("legacy schema always has an exact variant") {
        LegacyMfceWireVariant::V12A => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12A,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12A
        ),
        LegacyMfceWireVariant::V12B => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12B,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12B
        ),
        LegacyMfceWireVariant::V12C => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12C,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12C
        ),
        LegacyMfceWireVariant::V12D => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12D,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12D
        ),
        LegacyMfceWireVariant::V12E => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12E,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12E
        ),
        LegacyMfceWireVariant::V12F => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12F,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12F
        ),
        LegacyMfceWireVariant::V12G => decode_legacy_unsigned_snapshot::<
            LegacyMfcePersistentStateV12G,
        >(
            bytes, expected_identity, LegacyMfceWireVariant::V12G
        ),
    }
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

fn normalized_source_exposures(state: &SourceStateResponse) -> Option<BTreeMap<String, Decimal>> {
    if state.positions.is_empty() {
        return Some(BTreeMap::new());
    }
    if state.account_value <= Decimal::ZERO {
        return None;
    }
    state
        .positions
        .iter()
        .map(|(asset, position)| {
            position
                .signed_notional
                .checked_div(state.account_value)
                .map(|exposure| (asset.clone(), exposure))
        })
        .collect()
}

impl DecisionEngine {
    pub fn new(
        config: CopyTradeConfig,
        instance_seed: &[u8],
        run_id: impl Into<String>,
        active_freshness_ms: u64,
        inactive_freshness_ms: u64,
    ) -> Result<Self, EngineError> {
        let digest = Sha256::digest(instance_seed);
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        let source_store = SnapshotStore::new(crate::domain::scheduler::FreshnessPolicy {
            configured_max_age_ms: config.global_risk.source_snapshot_max_age_ms,
            maximum_future_skew_ms: 2_000,
        })
        .map_err(|error| EngineError::Core(error.to_string()))?;
        let technical_engine = TechnicalEngine::new(config.technical.clone()).map_err(|error| {
            EngineError::Core(format!("invalid technical configuration: {error:?}"))
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
            source_stream_mode: false,
            source_wallet_ready: BTreeMap::new(),
            source_market_coverage: BTreeMap::new(),
            mids: None,
            metadata: None,
            execution_dex_order: Vec::new(),
            dex_collateral: BTreeMap::new(),
            aligned_quote_tokens: BTreeSet::new(),
            waiting_market_rules: BTreeSet::new(),
            metadata_received_at: None,
            metadata_valid_until: None,
            books: BTreeMap::new(),
            stale_book_warning_assets: BTreeSet::new(),
            previous_target: None,
            decision_sequence: 0,
            pending: BTreeMap::new(),
            pending_book: BTreeMap::new(),
            continuations: BTreeMap::new(),
            desired_books: BTreeSet::new(),
            ledger: DualLedger::default(),
            metrics: EngineMetrics::default(),
            executions: Vec::new(),
            equity_buckets: Vec::new(),
            plans: Vec::new(),
            book_evaluation_events: Vec::new(),
            action_lifecycle_events: Vec::new(),
            last_bucket: None,
            last_funding_accrual: None,
            accrued_funding: BTreeMap::new(),
            applied_live_funding: BTreeSet::new(),
            micro_density: MicroDensityCounters::default(),
            technical_delivery: TechnicalDeliveryCounters::default(),
            prepared_authorized_intents: Vec::new(),
            emitted_production_cloids: BTreeSet::new(),
            production_identity: None,
            strategy_targets_ready: true,
            production_positions: None,
            production_equities: None,
            reconciliation_mode: crate::domain::live_trading::ReconciliationMode::Normal,
            technical_engine,
            technical_targets: BTreeMap::new(),
            technical_decision_state: BTreeMap::new(),
            technical_decision_records: Vec::new(),
            very_profitable_layer: None,
            very_profitable_engine: VeryProfitableCohortEngine::default(),
            cohort_indicator_records: Vec::new(),
            mfce: MfceEngine::default(),
            last_delayed_service: 0,
            mfce_authorized_assets: BTreeMap::new(),
            durable_time_offset: 0,
            mfce_time_high_watermark: 0,
            ledger_time_high_watermark: 0,
            snapshot_generation: None,
        })
    }

    pub fn install_very_profitable_layer(
        &mut self,
        layer: PreparedVeryProfitableLayer,
    ) -> Result<(), EngineError> {
        let identity = self.config.very_profitable_layer.as_ref().ok_or_else(|| {
            EngineError::Core(
                "prepared very_profitable layer is not bound into configuration identity".into(),
            )
        })?;
        if identity.artifact_sha256 != layer.artifact_sha256
            || identity.membership_set_hash != layer.resolution.membership_set_hash
            || identity.cohort_snapshot_timestamp_ms != layer.resolution.snapshot_timestamp_ms
        {
            return Err(EngineError::Core(
                "very_profitable layer identity mismatch".into(),
            ));
        }
        let scheduled = self
            .config
            .candidates
            .iter()
            .filter(|candidate| is_cohort_candidate_label(&candidate.label))
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if scheduled != layer.qualified_cohort_only_members {
            return Err(EngineError::Core(
                "qualified cohort-only scheduler set does not match the prepared layer".into(),
            ));
        }
        self.very_profitable_layer = Some(layer);
        Ok(())
    }

    /// Enables production intent emission. Pricing, floor, allocation and risk
    /// remain the same authoritative calculations used by the engine.
    pub fn enable_production_intents(&mut self, identity: ProductionIntentIdentity) {
        self.production_identity = Some(identity);
    }

    pub fn take_prepared_authorized_intents(&mut self) -> Vec<AuthorizedExecutionIntent> {
        let intents = std::mem::take(&mut self.prepared_authorized_intents);
        if self.execution_recovery_only() {
            for intent in intents {
                self.emitted_production_cloids.remove(&intent.planned_cloid);
            }
            return Vec::new();
        }
        intents
    }

    pub fn strategy_target_execution_enabled(&self) -> bool {
        !self.execution_recovery_only()
    }

    fn fresh_mfce_policy_output_at(
        &self,
        asset: &str,
        durable_now: Timestamp,
    ) -> Option<&crate::mfce::MfcePolicyOutput> {
        let output = self.mfce.policy_output(asset)?;
        if output.signal_observed_at == 0 || output.signal_observed_at > durable_now {
            return None;
        }
        let maximum_age = self.config.global_risk.source_snapshot_max_age_ms;
        if durable_now.saturating_sub(output.signal_observed_at) > maximum_age {
            return None;
        }
        Some(output)
    }

    fn suspend_strategy_target(&mut self, asset: &str, now: Timestamp) {
        // Edge-only: one bad asset never flips the global readiness latch.
        // Per-asset Hold with MfcePolicyOutput{admitted:false} only; global
        // strategy_targets_ready stays true. RISK_ONLY / reconciliation /
        // projection remain the sole solvency floor.
        self.metrics.target_readiness_rejections += 1;
        eprintln!("global_target_readiness_unavailable=false asset={asset} observed_at={now} action=HOLD nonfatal=true edge_only=true");
    }

    pub fn release_unaccepted_production_intent(
        &mut self,
        cloid: crate::domain::decision::PlannedCloid,
    ) {
        self.emitted_production_cloids.remove(&cloid);
    }

    /// After restoring the durable signer registry, a pending CLOID absent
    /// from it was never authorized or sent: registration is persisted before
    /// any external submission.
    /// Inspect restored pending state directly: `emitted_production_cloids` is
    /// intentionally runtime-only and is empty after restart. Retire the local
    /// commitment so it cannot survive as an exchange-unknown actionable root
    /// or consume execution capacity.
    pub fn retire_unregistered_pending_actions(
        &mut self,
        registered: &BTreeSet<crate::domain::decision::PlannedCloid>,
        observed_at: Timestamp,
    ) {
        let orphaned = self
            .pending
            .values()
            .map(|pending| pending.action.planned_cloid)
            .chain(self.emitted_production_cloids.iter().copied())
            .filter(|cloid| !registered.contains(cloid))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for cloid in orphaned {
            self.retire_no_action(cloid, observed_at);
        }
    }

    pub fn retire_no_action(
        &mut self,
        cloid: crate::domain::decision::PlannedCloid,
        observed_at: Timestamp,
    ) {
        if let Some((asset, pending)) = self
            .pending
            .iter()
            .find(|(_, pending)| pending.action.planned_cloid == cloid)
            .map(|(asset, pending)| (asset.clone(), pending.clone()))
        {
            self.retire_pending_action(
                &asset,
                &pending,
                observed_at,
                ActionAttemptOutcome::NoLongerRequired,
            );
        }
        self.emitted_production_cloids.remove(&cloid);
    }

    pub fn waiting_market_rules(&self) -> &BTreeSet<String> {
        &self.waiting_market_rules
    }

    pub fn install_execution_dex_order(&mut self, order: Vec<String>) {
        // Preserve the authoritative exchange order verbatim (including the
        // sunset `hyna` slot so later builder-perp asset IDs stay exact).
        self.execution_dex_order = order;
        // Default per-DEX collateral to USDC on first discovery; operators
        // may override via `set_dex_collateral`. Missing entries fail closed.
        for dex in &self.execution_dex_order {
            if dex.is_empty() || dex == EXCLUDED_HIP3_DEX {
                continue;
            }
            self.dex_collateral
                .entry(dex.clone())
                .or_insert_with(|| "USDC".to_string());
        }
    }

    pub fn execution_dex_order(&self) -> &[String] {
        &self.execution_dex_order
    }

    pub fn set_dex_collateral(&mut self, dex: &str, token: &str) {
        if dex.is_empty() || dex == EXCLUDED_HIP3_DEX || token.is_empty() {
            return;
        }
        self.dex_collateral
            .insert(dex.to_string(), token.to_string());
    }

    pub fn dex_collateral(&self) -> &BTreeMap<String, String> {
        &self.dex_collateral
    }

    /// Authoritative market -> execution asset-ID mapping (single resolver
    /// for order/cancel/modify/leverage/reconciliation paths).
    pub fn execution_asset_id(&self, asset: &str) -> Option<u32> {
        let metadata = self.metadata.as_ref()?;
        let universe = metadata
            .universe
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        hip3_execution_asset_id(&universe, &self.execution_dex_order, asset)
    }

    /// Capability-based production admission. Native perps keep existing
    /// admission; HIP-3 requires full execution capabilities and fails
    /// closed when any capability is missing.
    pub fn is_production_execution_supported(&self, asset: &str) -> bool {
        if !hip3_is_market(asset) {
            return true;
        }
        let Some(metadata) = self.metadata.as_ref() else {
            return false;
        };
        let Some((mids, _)) = self.mids.as_ref() else {
            return false;
        };
        let universe = metadata
            .universe
            .iter()
            .map(|entry| (entry.name.clone(), entry.size_decimals))
            .collect::<BTreeMap<_, _>>();
        let universe_order = metadata
            .universe
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        let live: BTreeSet<String> = mids.mids.keys().cloned().collect();
        hip3_execution_capabilities(
            asset,
            &universe,
            &self.execution_dex_order,
            &universe_order,
            &live,
            &self.dex_collateral,
        )
        .is_some()
    }

    /// Authoritative taker fee (bps) for MFCE after-cost edge. Single fee
    /// path for candidate construction, horizon friction, execution
    /// economics, and delayed labeling expectations.
    ///
    /// Native perps use the account's current user fee tier. HIP-3 markets
    /// scale it by the DEX `deployerFeeScale`, per-market `growthMode`, the
    /// active referral discount, and quote alignment, per Hyperliquid's
    /// documented formula. Actual fill fees remain authoritative post-trade.
    ///
    /// Missing refresh state falls back exactly as before (live rate, else
    /// the unsigned-replay configuration), so this replaces an inaccurate
    /// cost input without adding a new execution gate.
    pub fn taker_fee_bps_for(&self, asset: &str) -> Option<Decimal> {
        let dex = dex_for_market(asset);
        let metadata = self.metadata.as_ref()?;
        let (taker_rate, referral) = match &metadata.user_fee_state {
            Some(state) => (state.taker_rate, state.active_referral_discount),
            None => {
                let bps = metadata.live_taker_fee_bps.or_else(|| {
                    self.production_identity
                        .is_none()
                        .then(|| Decimal::from_f64(self.config.taker_fee_bps))
                        .flatten()
                })?;
                (bps.checked_div(BPS_PER_UNIT_RETURN)?, Decimal::ZERO)
            }
        };
        if dex.is_empty() {
            return taker_rate.checked_mul(BPS_PER_UNIT_RETURN);
        }
        let scale = metadata
            .dex_fee_scales
            .get(dex)
            .copied()
            .unwrap_or(Decimal::ONE);
        let growth = metadata
            .universe
            .iter()
            .find(|entry| entry.name == asset)
            .map(|entry| entry.growth_mode)
            .unwrap_or(false);
        crate::hip3_fees::taker_fee_bps(&crate::hip3_fees::PerpFeeContext {
            user_taker_rate: taker_rate,
            user_maker_rate: metadata
                .user_fee_state
                .map(|state| state.maker_rate)
                .unwrap_or(taker_rate),
            active_referral_discount: referral,
            deployer_fee_scale: scale,
            growth_mode: growth,
            aligned_quote_token: self.aligned_quote_tokens.contains(asset),
            is_hip3: true,
        })
    }

    /// DEXes with live involvement (positions, pending, recent lifecycle).
    pub fn active_hip3_dexes(&self) -> BTreeSet<String> {
        let mut owned: Vec<String> = self.pending.keys().cloned().collect();
        owned.extend(self.authoritative_assets());
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        hip3_dexes_for_assets(refs.into_iter())
    }

    pub fn merge_hydrated_metadata(
        &self,
        additional: MarketMetadataResponse,
        dex: &str,
    ) -> Option<MarketMetadataResponse> {
        let mut metadata = self.metadata.clone()?;
        let prefix = format!("{dex}:");
        let old = metadata
            .universe
            .iter()
            .filter(|asset| asset.name.starts_with(&prefix))
            .collect::<Vec<_>>();
        // An append-only universe preserves wire asset IDs. Never accept an
        // index reassignment or size-rule change under an outstanding intent.
        // Growth-mode flips only change fee economics and merge freely.
        let identity = |asset: &MarketMetadataAsset| (asset.name.clone(), asset.size_decimals);
        if !old
            .iter()
            .map(|asset| identity(asset))
            .zip(additional.universe.iter().map(identity))
            .all(|(a, b)| a == b)
            || additional.universe.len() < old.len()
        {
            return None;
        }
        metadata
            .universe
            .retain(|asset| !asset.name.starts_with(&prefix));
        metadata.universe.extend(additional.universe);
        for (fee_dex, scale) in additional.dex_fee_scales {
            metadata.dex_fee_scales.insert(fee_dex, scale);
        }
        if additional.user_fee_state.is_some() {
            metadata.user_fee_state = additional.user_fee_state;
        }
        if additional.live_taker_fee_bps.is_some() {
            metadata.live_taker_fee_bps = additional.live_taker_fee_bps;
        }
        Some(metadata)
    }

    fn complete_execution_rules(
        &mut self,
        input: &mut PortfolioProjectionInput,
        asset: &str,
    ) -> Result<bool, EngineError> {
        let assets = input
            .unconstrained_targets
            .keys()
            .chain(input.filled_positions.keys())
            .chain(
                input
                    .acknowledged_open_orders
                    .iter()
                    .map(|order| &order.asset),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        for missing in assets {
            if input.market_rules.contains_key(&missing) {
                continue;
            }
            let rules = match self.mids.as_ref().zip(self.metadata.as_ref()) {
                Some(((mids, _), metadata)) => {
                    match build_market_rules(mids, metadata, std::iter::once(&missing)) {
                        Ok(rules) => Some(rules),
                        Err(EngineError::InvalidMarket(_)) => None,
                        Err(error) => return Err(error),
                    }
                }
                None => None,
            };
            if let Some(rules) = rules {
                input.market_rules.extend(rules);
            } else {
                self.waiting_market_rules.insert(missing.clone());
                // This is an execution-only projection copy, never target
                // truth. Unready targets remain in the durable target ledger.
                // Real exposure is never omitted to pass the signing gate.
                if missing == asset
                    || input
                        .filled_positions
                        .get(&missing)
                        .is_some_and(|n| !n.is_zero())
                    || input
                        .acknowledged_open_orders
                        .iter()
                        .any(|order| order.asset == missing)
                {
                    return Ok(false);
                }
                input.unconstrained_targets.remove(&missing);
                input.filled_positions.remove(&missing); // absent/zero only
            }
        }
        Ok(true)
    }

    pub fn synchronize_live_state(
        &mut self,
        state: &crate::domain::live_trading::LiveTradingState,
    ) {
        self.production_positions = Some(state.positions());
        self.production_equities = Some((
            state.current_equity(),
            state.settled_equity(),
            state.deployment_equity(),
        ));
        self.reconciliation_mode = crate::domain::live_trading::ReconciliationMode::Normal;
    }

    pub fn enter_live_recovery_only(&mut self) {
        self.reconciliation_mode = crate::domain::live_trading::ReconciliationMode::RiskOnly;
    }

    pub fn observe_recovery_risk(
        &mut self,
        snapshot: Option<&crate::signing::transport::ExchangePositionSnapshot>,
        durable: &crate::domain::live_trading::LiveTradingState,
    ) {
        self.enter_live_recovery_only();
        if let Some(snapshot) = snapshot {
            self.production_positions = Some(snapshot.positions.clone());
            self.production_equities = Some((
                snapshot.account_equity,
                durable.settled_equity(),
                snapshot.account_equity.min(durable.settled_equity()),
            ));
        }
    }

    pub fn execution_recovery_only(&self) -> bool {
        self.reconciliation_mode == crate::domain::live_trading::ReconciliationMode::RiskOnly
    }

    pub fn exchange_risk_positions(&self) -> Option<&BTreeMap<String, Decimal>> {
        self.production_positions.as_ref()
    }

    pub(crate) fn live_execution_checkpoint(&self) -> LiveExecutionCheckpoint {
        LiveExecutionCheckpoint {
            delayed: self.mfce.delayed().clone(),
            ledger: self.ledger.clone(),
            pending: self.pending.clone(),
            continuations: self.continuations.clone(),
            accrued_funding: self.accrued_funding.clone(),
            applied_live_funding: self.applied_live_funding.clone(),
            executions: self.executions.clone(),
            action_lifecycle_events: self.action_lifecycle_events.clone(),
            desired_books: self.desired_books.clone(),
            emitted_production_cloids: self.emitted_production_cloids.clone(),
            metrics: self.metrics.clone(),
            technical_delivery: self.technical_delivery.clone(),
            ledger_time_high_watermark: self.ledger_time_high_watermark,
        }
    }

    pub(crate) fn restore_live_execution_checkpoint(
        &mut self,
        checkpoint: LiveExecutionCheckpoint,
    ) {
        *self.mfce.delayed_mut() = checkpoint.delayed;
        self.ledger = checkpoint.ledger;
        self.pending = checkpoint.pending;
        self.continuations = checkpoint.continuations;
        self.accrued_funding = checkpoint.accrued_funding;
        self.applied_live_funding = checkpoint.applied_live_funding;
        self.executions = checkpoint.executions;
        self.action_lifecycle_events = checkpoint.action_lifecycle_events;
        self.desired_books = checkpoint.desired_books;
        self.emitted_production_cloids = checkpoint.emitted_production_cloids;
        self.metrics = checkpoint.metrics;
        self.technical_delivery = checkpoint.technical_delivery;
        self.ledger_time_high_watermark = checkpoint.ledger_time_high_watermark;
    }

    pub fn live_positions_match(
        &self,
        state: &crate::domain::live_trading::LiveTradingState,
    ) -> bool {
        let mut local = self
            .ledger
            .portfolio_assets()
            .into_iter()
            .map(|asset| {
                let quantity = self.ledger.portfolio_position(&asset);
                (asset, quantity)
            })
            .filter(|(_, quantity)| !quantity.is_zero())
            .collect::<BTreeMap<_, _>>();
        local.retain(|_, quantity| !quantity.is_zero());
        let mut exchange = state.positions();
        exchange.retain(|_, quantity| !quantity.is_zero());
        local == exchange
    }

    /// Applies an authenticated exchange fill to the same target/component
    /// lineage used by execution. The fill consumes only the immutable
    /// allocation carried by the matching pending action; current MFCE targets
    /// remain authoritative solely for the next plan.
    pub fn apply_live_execution_fill(
        &mut self,
        fill: &VerifiedExchangeFill,
    ) -> Result<(), EngineError> {
        // Historical replay must not consume funding or collide with a newer
        // pending action for the same asset after the episode was recovered.
        if self.ledger.has_recovered_fill(fill).map_err(core)? {
            return Ok(());
        }
        let execution_id = execution_from_verified_fill(fill, Decimal::ZERO)
            .map_err(|error| EngineError::Core(error.to_string()))?
            .execution_id
            .to_string();
        if self
            .executions
            .iter()
            .any(|execution| execution.execution_id == execution_id)
        {
            return Ok(());
        }
        let Some(pending) = self.pending.get(&fill.asset).cloned() else {
            if self
                .pending
                .values()
                .any(|pending| pending.action.planned_cloid == fill.identity.cloid)
                || self.executions.iter().any(|prior| {
                    prior.root_planned_cloid == fill.root_cloid.to_string()
                        && prior.asset != fill.asset
                })
            {
                return Err(EngineError::Core(
                    "recovered fill conflicts with known action identity".into(),
                ));
            }
            let funding = self
                .accrued_funding
                .get(&fill.asset)
                .copied()
                .unwrap_or_default();
            self.ledger
                .recover_engine_fill(fill, funding)
                .map_err(core)?;
            self.accrued_funding.remove(&fill.asset);
            self.ledger_time_high_watermark = self.ledger_time_high_watermark.max(fill.occurred_at);
            return Ok(());
        };
        if pending.action.planned_cloid != fill.identity.cloid
            || pending.action.side != fill.side
            || pending.action.reduce_only != fill.reduce_only
        {
            return Err(EngineError::Core(
                "live fill identity does not match pending action".into(),
            ));
        }
        let size_step = self
            .metadata
            .as_ref()
            .and_then(|metadata| {
                metadata
                    .universe
                    .iter()
                    .find(|asset| asset.name == fill.asset)
            })
            .map(|asset| Decimal::new(1, asset.size_decimals))
            .ok_or_else(|| EngineError::InvalidMarket(fill.asset.clone()))?;
        let pending_component_ids = pending
            .component_remaining
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let (equity_before, source_equity_before) =
            self.accounting_equities_with_sources(&pending_component_ids)?;
        let position_before = self.ledger.portfolio_position(&fill.asset);
        let mut execution = execution_from_verified_fill(fill, position_before)
            .map_err(|error| EngineError::Core(error.to_string()))?;
        execution.funding = self.accrued_funding.remove(&fill.asset).unwrap_or_default();
        execution.action = pending.action.clone();
        execution.rounded_quantity = pending.remaining_order_quantity;
        execution.unfilled_ioc_remainder = pending
            .remaining_order_quantity
            .checked_sub(fill.filled_quantity)
            .unwrap_or_default()
            .max(Decimal::ZERO);
        let component_positions_before = self.ledger.source_positions_for_asset(&fill.asset);
        let component_partition = consume_pending_component_fill(
            &component_positions_before,
            fill.side,
            fill.filled_quantity,
            pending.remaining_order_quantity,
            &pending.component_remaining,
            size_step,
        )?;
        let ledger_timestamp = self
            .ledger_time_high_watermark
            .checked_add(1)
            .ok_or(EngineError::Arithmetic)?
            .max(fill.occurred_at);
        self.ledger_time_high_watermark = ledger_timestamp;
        execution.decision_timestamp_mono = ledger_timestamp;
        self.apply_attributed_execution(
            &fill.asset,
            &execution,
            ledger_timestamp,
            &component_partition.total_quantities,
        )?;
        validate_source_reconciliation(
            &self.ledger,
            &fill.asset,
            execution.position_after,
            size_step,
        )?;
        let (_, source_equity_after) =
            self.accounting_equities_with_sources(&pending_component_ids)?;
        // Per-fill cash is not the marked-equity move between two snapshots.
        let gross_pnl_delta = fill.exchange_closed_pnl;
        let net_pnl_delta = gross_pnl_delta
            .checked_sub(execution.fees)
            .and_then(|value| value.checked_sub(execution.funding))
            .ok_or(EngineError::Arithmetic)?;
        let mut source_attributed_returns = BTreeMap::new();
        for candidate in component_partition.total_quantities.keys() {
            let before = source_equity_before
                .get(candidate)
                .copied()
                .ok_or_else(|| EngineError::Core("missing source equity".into()))?;
            let after = source_equity_after
                .get(candidate)
                .copied()
                .ok_or_else(|| EngineError::Core("missing source equity".into()))?;
            let change = after.checked_sub(before).ok_or(EngineError::Arithmetic)?;
            source_attributed_returns.insert(
                candidate.clone(),
                if before.is_zero() {
                    Decimal::ZERO
                } else {
                    change.checked_div(before).ok_or(EngineError::Arithmetic)?
                },
            );
        }
        let deployment = self.deployment_equity()?;
        self.executions.push(SettledActionAccounting {
            execution_id: execution.execution_id.to_string(),
            decision_id: execution.action.decision_id.to_string(),
            asset: fill.asset.clone(),
            side: format!("{:?}", fill.side).to_ascii_lowercase(),
            execution_mode: "live_ioc".into(),
            root_planned_cloid: pending.root_planned_cloid.clone(),
            parent_planned_cloid: fill.parent_cloid.map(|cloid| cloid.to_string()),
            retry_generation: fill.continuation_generation,
            decision_timestamp_mono: ledger_timestamp,
            evaluation_timestamp_mono: ledger_timestamp,
            decision_midpoint: fill.decision_reference_price,
            modeled_ioc_limit: fill.submitted_limit_price,
            worst_required_depth_price: None,
            visible_executable_quantity: fill.filled_quantity,
            requested_quantity: pending.remaining_order_quantity,
            filled_quantity: fill.filled_quantity,
            unfilled_quantity: execution.unfilled_ioc_remainder,
            average_fill_price: Some(fill.average_fill_price),
            filled_notional: execution.modeled_filled_notional,
            fees: fill.fee_amount,
            funding: execution.funding,
            execution_slippage: execution.slippage,
            gross_pnl_delta,
            net_pnl_delta,
            portfolio_equity_return: if equity_before.is_zero() {
                Decimal::ZERO
            } else {
                net_pnl_delta
                    .checked_div(equity_before)
                    .ok_or(EngineError::Arithmetic)?
            },
            current_equity: deployment.current_equity,
            settled_equity: deployment.settled_equity,
            deployment_equity: deployment.deployment_equity,
            source_attributed_returns,
            position_before: execution.position_before,
            position_after: execution.position_after,
            component_close_quantities: component_partition.close_quantities,
            component_open_quantities: component_partition.open_quantities,
            source_filled_quantities: component_partition.total_quantities.clone(),
            economic_attribution: pending
                .execution
                .as_ref()
                .map(|context| context.economic_attribution)
                .unwrap_or_else(|| {
                    classify_economic_attribution(Some(&pending.component_remaining))
                }),
            mfce_lineage: pending.mfce_lineage.clone(),
        });
        if execution.unfilled_ioc_remainder.is_zero() {
            self.pending.remove(&fill.asset);
            self.emitted_production_cloids.remove(&fill.identity.cloid);
            self.action_lifecycle_events.push(ActionLifecycleEvent {
                asset: fill.asset.clone(),
                decision_id: fill.decision_id.to_string(),
                planned_cloid: fill.identity.cloid.to_string(),
                root_planned_cloid: pending.root_planned_cloid,
                parent_planned_cloid: fill.parent_cloid.map(|cloid| cloid.to_string()),
                retry_generation: fill.continuation_generation,
                observed_at_mono: ledger_timestamp,
                requested_notional: execution.action.rounded_notional,
                filled_quantity: Some(fill.filled_quantity),
                unfilled_quantity: Some(Decimal::ZERO),
                outcome: ActionAttemptOutcome::ExecutedFully,
            });
        } else {
            let retained = self.pending.get_mut(&fill.asset).ok_or_else(|| {
                EngineError::Core("live partial fill lost its pending action".into())
            })?;
            retained.remaining_order_quantity = execution.unfilled_ioc_remainder;
            retained.component_remaining = component_partition.remaining_quantities;
        }
        self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(ledger_timestamp);
        self.mfce.delayed_mut().fill(
            &fill.asset,
            &fill.identity.cloid.to_string(),
            ledger_timestamp,
            if fill.side == Side::Buy {
                fill.filled_quantity
            } else {
                -fill.filled_quantity
            },
            fill.average_fill_price,
            fill.fee_amount,
            position_before,
            fill.decision_reference_price,
            RealizedOutcome {
                gross_pnl: gross_pnl_delta,
                fees: execution.fees,
                slippage: execution.slippage,
                funding: -execution.funding,
                net_pnl: net_pnl_delta,
            },
        );
        self.metrics.executions = self.metrics.executions.saturating_add(1);
        self.refresh_desired_books();
        Ok(())
    }

    pub fn apply_external_fill(
        &mut self,
        event: &crate::domain::live_trading::ExternalFillAccounting,
    ) -> Result<(), EngineError> {
        if self.ledger.has_external_fill(event).map_err(core)? {
            return Ok(());
        }
        let funding = self
            .accrued_funding
            .get(&event.fill.asset)
            .copied()
            .unwrap_or_default();
        self.ledger
            .recover_external_fill(event, funding)
            .map_err(core)?;
        self.accrued_funding.remove(&event.fill.asset);
        self.ledger_time_high_watermark =
            self.ledger_time_high_watermark.max(event.fill.occurred_at);
        if let Some(episode) = self
            .ledger
            .portfolio_closed()
            .iter()
            .find(|e| e.episode_id == event.episode_id)
        {
            let id = episode
                .episode_id
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let evidence = self
                .mfce
                .delayed_mut()
                .provenance
                .episodes
                .entry(id)
                .or_default();
            evidence.asset = episode.asset.clone();
            evidence.opened_at = episode.opened_at;
            evidence.closed_at = Some(episode.closed_at);
            evidence.settled = Some(crate::mfce_delayed::SettledEpisodeEvidence {
                opening_action: episode.opening_decision_id.to_string(),
                external_manual_exit: true,
                outcome: RealizedOutcome::settled(
                    episode.realized_pnl,
                    episode.fees,
                    episode.slippage,
                    -episode.funding,
                )
                .ok_or(EngineError::Arithmetic)?,
            });
            // No decision-time features means no supervised sample. Economic
            // episode evidence is nevertheless durable and available to MFCE.
            self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(episode.closed_at);
        }
        Ok(())
    }

    pub fn apply_live_funding(&mut self, event: &VerifiedFundingEvent) -> Result<(), EngineError> {
        if self.applied_live_funding.contains(&event.event_id) {
            return Ok(());
        }
        let funding_cost = Decimal::ZERO
            .checked_sub(event.amount)
            .ok_or(EngineError::Arithmetic)?;
        let entry = self.accrued_funding.entry(event.asset.clone()).or_default();
        *entry = entry
            .checked_add(funding_cost)
            .ok_or(EngineError::Arithmetic)?;
        self.applied_live_funding.insert(event.event_id);
        Ok(())
    }

    /// Resolves an IOC after the exchange has made its terminal state
    /// authoritative. Any unfilled remainder is released back to current-target
    /// planning; an exchange-terminal action can no longer mutate the portfolio.
    pub fn resolve_live_execution_terminal(
        &mut self,
        cloid: crate::domain::decision::PlannedCloid,
        original_quantity: Decimal,
        filled_quantity: Decimal,
        observed_at: Timestamp,
        rejected: bool,
    ) -> Result<(), EngineError> {
        let Some((asset, pending)) = self
            .pending
            .iter()
            .find(|(_, pending)| pending.action.planned_cloid == cloid)
            .map(|(asset, pending)| (asset.clone(), pending.clone()))
        else {
            self.emitted_production_cloids.remove(&cloid);
            return Ok(());
        };
        let expected_remaining = original_quantity
            .checked_sub(filled_quantity)
            .ok_or(EngineError::Arithmetic)?
            .max(Decimal::ZERO);
        let size_step = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.universe.iter().find(|item| item.name == asset))
            .map(|item| Decimal::new(1, item.size_decimals))
            .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
        let (difference, tolerance) = reconciliation_difference(
            pending.remaining_order_quantity,
            expected_remaining,
            size_step,
        )?;
        if difference > tolerance {
            return Err(EngineError::Core(
                "terminal live action does not reconcile with applied fills".into(),
            ));
        }
        self.pending.remove(&asset);
        self.emitted_production_cloids.remove(&cloid);
        if !expected_remaining.is_zero() && !rejected {
            self.continuations.insert(
                asset.clone(),
                ContinuationIntent {
                    root_planned_cloid: pending.root_planned_cloid.clone(),
                    parent_planned_cloid: cloid.to_string(),
                    next_retry_generation: pending
                        .action
                        .retry_generation
                        .checked_add(1)
                        .ok_or(EngineError::Arithmetic)?,
                    kind: ContinuationKind::PartialIocRemainder,
                },
            );
        }
        self.action_lifecycle_events.push(ActionLifecycleEvent {
            asset: asset.clone(),
            decision_id: pending.action.decision_id.to_string(),
            planned_cloid: cloid.to_string(),
            root_planned_cloid: pending.root_planned_cloid,
            parent_planned_cloid: pending
                .execution
                .as_ref()
                .and_then(|context| context.parent_planned_cloid.clone()),
            retry_generation: pending.action.retry_generation,
            observed_at_mono: observed_at,
            requested_notional: pending.action.rounded_notional,
            filled_quantity: Some(filled_quantity),
            unfilled_quantity: Some(expected_remaining),
            outcome: if rejected {
                ActionAttemptOutcome::ExchangeRejected
            } else {
                ActionAttemptOutcome::ExecutedPartiallyRemainderReplanned
            },
        });
        self.refresh_desired_books();
        Ok(())
    }

    /// Returns the bounded MFCE state suitable for inclusion in
    /// the existing production trading-state commit.
    pub fn mfce_persistence_snapshot(&self) -> (MfcePersistentState, Timestamp) {
        (self.mfce.state().clone(), self.mfce_time_high_watermark)
    }

    /// Restores MFCE state before production event processing. Runtime model
    /// handles are reconstructed as one pair. The process clock is anchored
    /// once for every durable subsystem by `rebase_runtime_time` after the
    /// complete snapshot has been restored.
    pub fn restore_mfce_persistent_state(
        &mut self,
        state: MfcePersistentState,
        time_high_watermark: Timestamp,
    ) -> Result<(), EngineError> {
        if time_high_watermark < state.time_high_watermark() {
            return Err(EngineError::Core(
                "persisted MFCE time high-watermark regressed".into(),
            ));
        }
        let replacement = MfceEngine::from_state(state)
            .map_err(|error| EngineError::Core(format!("invalid persisted MFCE state: {error}")))?;
        self.mfce = replacement;
        self.mfce_authorized_assets.clear();
        self.mfce_time_high_watermark = time_high_watermark;
        self.refresh_desired_books();
        Ok(())
    }

    /// Establishes one restart-comparable durable timeline from a wall-clock
    /// sample taken at process startup and the process-local monotonic clock.
    /// Wall time is never consulted again during this process lifetime.
    pub fn rebase_runtime_time(
        &mut self,
        process_now: Timestamp,
        unix_now_ms: Timestamp,
    ) -> Result<(), EngineError> {
        let offset = unix_now_ms
            .checked_sub(process_now)
            .ok_or_else(|| EngineError::Core("runtime time anchor is invalid".into()))?;
        let durable_now = offset
            .checked_add(process_now)
            .ok_or(EngineError::Arithmetic)?;
        let persisted_high_watermark = self
            .mfce_time_high_watermark
            .max(self.ledger_time_high_watermark)
            .max(self.last_equity_boundary().unwrap_or_default())
            .max(
                self.equity_buckets
                    .iter()
                    .fold(0, |high, bucket| high.max(bucket.closed_at_mono)),
            )
            .max(self.executions.iter().fold(0, |high, execution| {
                high.max(execution.evaluation_timestamp_mono)
            }));
        if durable_now < persisted_high_watermark {
            return Err(EngineError::Core(
                "runtime wall-clock anchor precedes durable event time".into(),
            ));
        }
        self.durable_time_offset = offset;
        self.mfce.delayed_mut().gap();
        self.mfce.delayed_mut().expire(durable_now);
        Ok(())
    }

    pub fn durable_timestamp(&self, process_now: Timestamp) -> Result<Timestamp, EngineError> {
        self.durable_time_offset
            .checked_add(process_now)
            .ok_or(EngineError::Arithmetic)
    }

    pub fn recompute_after_production_updates(
        &mut self,
        now: Timestamp,
    ) -> Result<(), EngineError> {
        if self.production_identity.is_some() {
            self.construct_next_decision(now)?;
        }
        Ok(())
    }

    fn authoritative_position(&self, asset: &str) -> Decimal {
        self.production_positions
            .as_ref()
            .map(|positions| positions.get(asset).copied().unwrap_or(Decimal::ZERO))
            .unwrap_or_else(|| self.ledger.portfolio_position(asset))
    }

    fn authoritative_assets(&self) -> Vec<String> {
        self.production_positions
            .as_ref()
            .map(|positions| positions.keys().cloned().collect())
            .unwrap_or_else(|| self.ledger.portfolio_assets())
    }

    fn pending_open_orders_excluding(
        &self,
        replaced_assets: &BTreeSet<String>,
    ) -> Vec<OpenOrderExposure> {
        self.pending
            .iter()
            .filter(|(asset, _)| !replaced_assets.contains(asset.as_str()))
            .map(|(asset, pending)| OpenOrderExposure {
                asset: asset.clone(),
                side: Some(match pending.action.side {
                    Side::Buy => OrderSide::Buy,
                    Side::Sell => OrderSide::Sell,
                }),
                notional: Some(pending.action.rounded_notional),
                lifecycle: OpenOrderLifecycle::Acknowledged,
            })
            .collect()
    }

    fn projected_existing_notional(
        &self,
        asset: &str,
        current: Decimal,
        replaces_outstanding: bool,
    ) -> Result<Decimal, EngineError> {
        let pending_signed = self
            .pending
            .get(asset)
            .map(|pending| match pending.action.side {
                Side::Buy => pending.action.rounded_notional,
                Side::Sell => -pending.action.rounded_notional,
            });
        // A durable target is not capital occupation by itself. It contributes
        // only while backed by a real executable intent awaiting a book or an
        // IOC continuation. Filled exposure and concrete PendingAction deltas
        // are accounted independently above.
        let intent_target = intent_backed_target(
            self.pending_book.contains_key(asset),
            self.continuations.contains_key(asset),
            self.target_ledger
                .get(asset)
                .map(|target| target.admitted_target_notional),
        );
        projected_exposure_notional(current, pending_signed, intent_target, replaces_outstanding)
    }

    pub fn ingest(
        &mut self,
        response: AcceptedPublicResponse,
        now: Timestamp,
    ) -> Result<(), EngineError> {
        match response.payload {
            // Historical evidence is journaled by the runtime, never replayed
            // as a fresh source signal or a fabricated MFCE decision.
            PublicPayload::SourceFills(_) => {}
            PublicPayload::SourceState(mut state) => {
                let closed_candles = std::mem::take(&mut state.closed_candles);
                let mut technical_changed = false;
                for candle in closed_candles {
                    technical_changed |= self
                        .technical_engine
                        .accept_closed_candle(candle)
                        .map_err(|error| {
                            EngineError::Core(format!("invalid closed candle: {error:?}"))
                        })?
                        == CandleAcceptance::Accepted;
                }
                let candidate_id = state.candidate_id.clone();
                let sequence = self
                    .source_sequences
                    .entry(candidate_id.clone())
                    .or_insert(0);
                *sequence = sequence.checked_add(1).ok_or(EngineError::Arithmetic)?;
                let payload = serde_json::to_vec(&state)
                    .map_err(|error| EngineError::Core(error.to_string()))?;
                let payload_hash = hash_payload_bytes(&payload);
                // Individual public source records are external information,
                // not engine integrity. A zero-equity empty baseline is valid,
                // but a later streamed fill cannot be normalized until the
                // ordinary authoritative reconciliation refreshes its equity.
                // Retain the raw snapshot while omitting its unusable economic
                // contribution; never let one wallet stop the daemon.
                let exposures = normalized_source_exposures(&state).unwrap_or_default();
                let authorized_assets =
                    if let Some(previous) = self.source_exposure_book.get(&candidate_id) {
                        previous
                            .exposures
                            .keys()
                            .chain(exposures.keys())
                            .filter(|asset| previous.exposures.get(*asset) != exposures.get(*asset))
                            .cloned()
                            .collect::<BTreeSet<_>>()
                    } else {
                        // First post-start hydration is authoritative for both its
                        // visible positions and every durable aggregate lifecycle
                        // that may have changed while the daemon was offline.
                        exposures
                            .keys()
                            .chain(self.mfce.tracked_assets())
                            .cloned()
                            .collect::<BTreeSet<_>>()
                    };
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
                let source_transition_accepted = match self.source_store.accept(snapshot, now, true)
                {
                    SnapshotAcceptance::Accepted => {
                        self.source_exposure_book
                            .accept(candidate_id.clone(), exposure_state);
                        for asset in authorized_assets {
                            self.mfce_authorized_assets
                                .entry(asset)
                                .and_modify(|valid_until| {
                                    *valid_until = (*valid_until).max(response.valid_until_mono)
                                })
                                .or_insert(response.valid_until_mono);
                        }
                        self.mfce
                            .note_accepted_source_snapshot()
                            .map_err(mfce_error)?;
                        self.metrics.accepted_source_snapshots += 1;
                        true
                    }
                    SnapshotAcceptance::Expired => {
                        self.metrics.stale_source_snapshots += 1;
                        false
                    }
                    _ => {
                        self.metrics.rejected_source_snapshots += 1;
                        false
                    }
                };
                if source_transition_accepted || technical_changed {
                    self.construct_next_decision(response.received_at_mono)?;
                }
            }
            PublicPayload::MarketSnapshot(mids) => {
                self.mids = Some((mids, response.received_at_mono))
            }
            PublicPayload::MarketMetadata(metadata) => {
                self.accrue_funding(response.received_at_mono)?;
                self.waiting_market_rules
                    .retain(|name| !metadata.universe.iter().any(|asset| &asset.name == name));
                self.metadata = Some(metadata);
                self.metadata_received_at = Some(response.received_at_mono);
                self.metadata_valid_until = Some(response.valid_until_mono);
                self.last_funding_accrual = Some(response.received_at_mono);
            }
            PublicPayload::OrderBook(book) => {
                let asset = book.asset.clone();
                let triggers_recompute = self.pending_book.contains_key(&asset)
                    || self
                        .mfce
                        .awaiting_book_assets()
                        .any(|pending| pending == &asset);
                let refreshes_active_mfce = self.mfce.has_active_transition(&asset);
                self.books
                    .insert(asset.clone(), (book.clone(), response.received_at_mono));
                self.stale_book_warning_assets.remove(&asset);
                let prepared_intents_before = self.prepared_authorized_intents.len();
                let mut execution_recompute = if self.pending.contains_key(&asset) {
                    self.try_execute_pending(&book, response.received_at_mono)?
                } else {
                    ExecutionRecompute::None
                };
                let intent_prepared =
                    self.prepared_authorized_intents.len() > prepared_intents_before;
                let admission_recomputed = !intent_prepared
                    && refreshes_active_mfce
                    && self
                        .construct_next_decision(response.received_at_mono)?
                        .is_some();
                if !intent_prepared
                    && execution_recompute == ExecutionRecompute::None
                    && !(refreshes_active_mfce && !admission_recomputed)
                {
                    execution_recompute =
                        self.try_execute_pending(&book, response.received_at_mono)?;
                }
                if !intent_prepared
                    && ((!admission_recomputed && triggers_recompute)
                        || execution_recompute.is_required())
                {
                    let recomputed = self
                        .construct_next_decision(response.received_at_mono)?
                        .is_some();
                    if recomputed && execution_recompute == ExecutionRecompute::CloseFirstReversal {
                        self.technical_delivery.post_reduction_recomputed = self
                            .technical_delivery
                            .post_reduction_recomputed
                            .saturating_add(1);
                    }
                }
            }
            PublicPayload::Candle(CandleResponse { candle }) => {
                if self
                    .technical_engine
                    .accept_closed_candle(candle)
                    .map_err(|error| {
                        EngineError::Core(format!("invalid closed candle: {error:?}"))
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

    /// Restores post-gap eligibility only after the caller has installed an
    /// authoritative current-generation baseline and folded any newer
    /// buffered stream events into that exact state.
    pub fn admit_reconciled_source_wallet(
        &mut self,
        candidate: &str,
        source_time_ms: u64,
        now: Timestamp,
    ) -> Result<bool, EngineError> {
        let candidate = candidate.to_ascii_lowercase();
        if !self.source_stream_mode {
            return Err(EngineError::Core(
                "source wallet recovery admitted outside stream mode".into(),
            ));
        }
        let Some(snapshot) = self.source_store.latest(&candidate) else {
            return Ok(false);
        };
        if snapshot.payload.source_time_ms != source_time_ms {
            return Ok(false);
        }
        self.source_wallet_ready
            .insert(candidate, snapshot.requested_at);
        let mut assets = snapshot
            .payload
            .positions
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assets.extend(self.mfce.tracked_assets().cloned());
        for asset in assets {
            self.mfce_authorized_assets.insert(asset, Timestamp::MAX);
        }
        self.construct_next_decision(now)?;
        Ok(true)
    }

    pub fn assets_requiring_books(&self, now: Timestamp) -> BTreeSet<String> {
        let _ = now;
        self.desired_books.clone()
    }

    pub fn urgent_book_assets(&self) -> BTreeSet<String> {
        self.pending
            .keys()
            .chain(self.pending_book.keys())
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

    /// Switches source freshness from snapshot age to per-wallet,
    /// current-generation confirmation.
    pub fn enable_source_stream_mode(&mut self) {
        self.source_stream_mode = true;
    }

    pub fn mark_source_stream_gap(
        &mut self,
        affected_markets: &BTreeSet<String>,
        now: Timestamp,
    ) -> Result<(), EngineError> {
        if !self.source_stream_mode {
            return Err(EngineError::Core(
                "source stream gap recorded outside stream mode".into(),
            ));
        }
        self.source_market_coverage
            .retain(|market, _| !affected_markets.contains(market));
        self.source_wallet_ready.clear();
        self.construct_next_decision(now)?;
        Ok(())
    }

    pub fn pending_source_wallets(&self) -> BTreeSet<String> {
        let latest_gap = self.source_market_coverage.values().max();
        self.config
            .candidates
            .iter()
            .filter(|candidate| candidate.enabled)
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .filter(|wallet| {
                self.source_store.latest(wallet).is_none()
                    || self
                        .source_wallet_ready
                        .get(wallet)
                        .is_none_or(|baseline| latest_gap.is_some_and(|gap| baseline <= gap))
            })
            .collect()
    }

    pub fn live_confirmed_source_count(&self) -> usize {
        self.config
            .candidates
            .iter()
            .filter(|candidate| {
                let wallet = candidate.address.to_ascii_lowercase();
                self.source_store.latest(&wallet).is_some()
                    && self.source_wallet_ready.contains_key(&wallet)
            })
            .count()
    }

    fn source_market_confirmed(&self, wallet: &str, asset: &str) -> bool {
        !self.source_stream_mode
            || self
                .source_wallet_ready
                .get(wallet)
                .is_some_and(|baseline| {
                    self.source_market_coverage
                        .get(asset)
                        .is_some_and(|gap| baseline > gap)
                })
    }

    /// Retained subscriptions preserve their continuity boundary. A resumed
    /// market admits each wallet independently after its post-gap baseline.
    pub fn replace_source_market_coverage(
        &mut self,
        covered_markets: BTreeSet<String>,
        now: Timestamp,
    ) -> Result<(), EngineError> {
        if !self.source_stream_mode {
            return Err(EngineError::Core(
                "source market coverage changed outside stream mode".into(),
            ));
        }
        self.source_market_coverage
            .retain(|asset, _| covered_markets.contains(asset));
        for asset in covered_markets {
            self.source_market_coverage.entry(asset).or_insert(now);
        }
        self.construct_next_decision(now)?;
        Ok(())
    }

    pub fn source_market_coverage_count(&self) -> usize {
        self.source_market_coverage
            .keys()
            .filter(|asset| {
                self.source_wallet_ready
                    .keys()
                    .any(|wallet| self.source_market_confirmed(wallet, asset))
            })
            .count()
    }

    pub fn source_stream_healthy(&self) -> bool {
        !self.source_stream_mode || self.pending_source_wallets().is_empty()
    }

    pub fn hip3_pipeline_status(
        &self,
        source_activity: Option<&Hip3SourceActivity>,
    ) -> Hip3PipelineStatus {
        let source_markets_seen = source_activity
            .map(|activity| activity.markets.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let known_markets = self
            .metadata
            .as_ref()
            .map(|metadata| {
                metadata
                    .universe
                    .iter()
                    .filter(|asset| hip3_is_market(&asset.name))
                    .map(|asset| asset.name.clone())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let known_source_markets = source_markets_seen
            .intersection(&known_markets)
            .cloned()
            .collect::<BTreeSet<_>>();
        let confirmed_source_markets = known_source_markets
            .iter()
            .filter(|asset| {
                let asset = asset.as_str();
                self.config
                    .candidates
                    .iter()
                    .filter(|candidate| candidate.enabled)
                    .map(|candidate| candidate.address.to_ascii_lowercase())
                    .any(|wallet| {
                        self.source_store.latest(&wallet).is_some_and(|snapshot| {
                            snapshot.payload.positions.contains_key(asset)
                                && self.source_market_confirmed(&wallet, asset)
                        })
                    })
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let mfce_candidates = self
            .mfce
            .report()
            .policy_outputs
            .iter()
            .filter(|output| {
                hip3_is_market(&output.asset)
                    && (source_markets_seen.is_empty()
                        || source_markets_seen.contains(&output.asset))
            })
            .count();
        let unsupported_markets = if self.production_identity.is_some() {
            confirmed_source_markets
                .iter()
                .filter(|asset| !self.is_production_execution_supported(asset))
                .cloned()
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };
        let mut unsupported_source_fills = 0_u64;
        let mut unsupported_wallets = BTreeSet::new();
        if let Some(activity) = source_activity {
            for market in &unsupported_markets {
                if let Some(market_activity) = activity.markets.get(market) {
                    unsupported_source_fills =
                        unsupported_source_fills.saturating_add(market_activity.source_fills);
                    unsupported_wallets.extend(market_activity.source_wallets.iter().cloned());
                }
            }
        }
        let actions_emitted = self
            .pending
            .values()
            .filter(|pending| {
                hip3_is_market(&pending.action.asset)
                    && self
                        .emitted_production_cloids
                        .contains(&pending.action.planned_cloid)
            })
            .count();
        let fills = self
            .executions
            .iter()
            .filter(|execution| {
                hip3_is_market(&execution.asset) && execution.filled_quantity > Decimal::ZERO
            })
            .count();

        Hip3PipelineStatus {
            hip3_source_markets_seen: source_markets_seen.len(),
            hip3_source_markets_known: known_source_markets.len(),
            hip3_source_markets_confirmed: confirmed_source_markets.len(),
            hip3_mfce_candidates: mfce_candidates,
            hip3_execution_unsupported: unsupported_markets.len(),
            hip3_actions_emitted: actions_emitted,
            hip3_fills: fills,
            hip3_execution_unsupported_source_fills: unsupported_source_fills,
            hip3_execution_unsupported_unique_markets: unsupported_markets.len(),
            hip3_execution_unsupported_unique_wallets: unsupported_wallets.len(),
        }
    }

    fn fresh_source(
        &self,
        candidate: &str,
        now: Timestamp,
    ) -> Option<&SourceSnapshot<SourceStateResponse>> {
        if self.source_stream_mode {
            return self
                .source_wallet_ready
                .contains_key(candidate)
                .then(|| self.source_store.latest(candidate))
                .flatten();
        }
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

    fn record_cohort_runtime_rejection(
        &mut self,
        layer: &PreparedVeryProfitableLayer,
        asset: &str,
        reason: CohortDecisionReason,
        aggregate: Option<&CohortAssetAggregate>,
        existing_wallet_consensus: Decimal,
        technical_target: Decimal,
        source_budget: Decimal,
        cost_estimate_fraction: Decimal,
    ) -> Result<(), EngineError> {
        let existing_wallet_source_target = source_budget
            .checked_mul(existing_wallet_consensus)
            .ok_or(EngineError::Arithmetic)?;
        let source_target = existing_wallet_source_target;
        let combined_target = source_target
            .checked_add(technical_target)
            .ok_or(EngineError::Arithmetic)?;
        let active_twap = layer.active_twaps.get(asset);
        self.cohort_indicator_records.push(CohortIndicatorRecord {
            asset: asset.into(),
            cohort_snapshot_timestamp_ms: layer.resolution.snapshot_timestamp_ms,
            membership_set_hash: layer.resolution.membership_set_hash.clone(),
            wallet_count_before_filtering: layer.resolution.wallet_count_before_filtering,
            wallet_count_after_filtering: layer.qualified_members.len(),
            overlap_with_existing: layer.resolution.overlap_with_existing,
            long_notional: aggregate.map_or(Decimal::ZERO, |value| value.long_notional),
            short_notional: aggregate.map_or(Decimal::ZERO, |value| value.short_notional),
            long_trader_count: aggregate.map_or(0, |value| value.long_trader_count),
            short_trader_count: aggregate.map_or(0, |value| value.short_trader_count),
            impulse_5m: Decimal::ZERO,
            impulse_15m: Decimal::ZERO,
            impulse_1h: Decimal::ZERO,
            active_twap_direction: active_twap.map_or(0, |twap| twap.direction),
            active_twap_remaining_quantity: active_twap
                .map_or(Decimal::ZERO, |twap| twap.remaining_quantity),
            weighted_wallet_quality: aggregate
                .map_or(Decimal::ZERO, |value| value.weighted_realized_quality),
            dominant_wallet_percentage: aggregate
                .map_or(Decimal::ZERO, |value| value.dominant_wallet_percentage),
            cohort_score: Decimal::ZERO,
            chase_distance_r: None,
            cost_estimate_fraction,
            existing_wallet_source_target,
            very_profitable_cohort_target: Decimal::ZERO,
            source_target,
            technical_target,
            combined_target,
            target_version: self
                .very_profitable_engine
                .target_version(asset)
                .unwrap_or(0),
            independent_execution_root: false,
            reasons: [reason].into_iter().collect(),
            attribution: classify_attribution(
                existing_wallet_source_target,
                Decimal::ZERO,
                technical_target,
            ),
        });
        self.metrics.cohort_indicator_records =
            self.metrics.cohort_indicator_records.saturating_add(1);
        self.metrics.cohort_runtime_rejections =
            self.metrics.cohort_runtime_rejections.saturating_add(1);
        Ok(())
    }

    fn apply_very_profitable_cohort(
        &mut self,
        now: Timestamp,
        mids: &MarketSnapshotResponse,
        _metadata: &MarketMetadataResponse,
        fresh_states: &[SourceStateResponse],
        source_received_at: &BTreeMap<String, Timestamp>,
        existing_wallet_consensus: &BTreeMap<String, Decimal>,
        consensus_inputs: &mut BTreeMap<String, Vec<ConsensusInput>>,
        component_contributions: &mut BTreeMap<String, BTreeMap<String, Decimal>>,
    ) -> Result<(), EngineError> {
        let Some(layer) = self.very_profitable_layer.clone() else {
            return Ok(());
        };
        let observed_at_ms = fresh_states
            .iter()
            .filter(|state| {
                layer
                    .qualified_members
                    .contains(&state.candidate_id.to_ascii_lowercase())
            })
            .map(|state| state.source_time_ms)
            .max()
            .unwrap_or_default();
        let position_change_age_ms = if self.source_stream_mode {
            0
        } else {
            layer
                .qualified_members
                .iter()
                .filter_map(|address| source_received_at.get(address))
                .map(|received_at| now.saturating_sub(*received_at))
                .max()
                .unwrap_or_else(|| {
                    self.config
                        .global_risk
                        .source_snapshot_max_age_ms
                        .saturating_add(1)
                })
        };
        let assets = consensus_inputs.keys().cloned().collect::<Vec<_>>();
        // MFCE competes for the complete risk-engine-approved capacity. The
        // historical 35/65 source/technical sleeve split has no production
        // allocation authority.
        let source_budget = Decimal::ONE;
        let dominant_wallet_limit_pct =
            Decimal::from_f64(self.config.max_asset_notional_pct).ok_or(EngineError::Arithmetic)?;
        let rebalance_tolerance = Decimal::from_f64(self.config.global_risk.slot_rank_hysteresis)
            .ok_or(EngineError::Arithmetic)?;
        let wallets = match layer.authoritative_wallets(observed_at_ms, fresh_states.iter()) {
            Ok(wallets) => wallets,
            Err(_) => {
                for asset in &assets {
                    let technical_target = Decimal::ZERO;
                    self.record_cohort_runtime_rejection(
                        &layer,
                        asset,
                        CohortDecisionReason::AuthoritativeWalletHydrationFailed,
                        None,
                        existing_wallet_consensus
                            .get(asset)
                            .copied()
                            .unwrap_or_default(),
                        technical_target,
                        source_budget,
                        Decimal::ZERO,
                    )?;
                }
                return Ok(());
            }
        };

        for asset in assets {
            let confirmed_wallets = wallets
                .iter()
                .filter(|wallet| self.source_market_confirmed(&wallet.address, &asset))
                .cloned()
                .collect::<Vec<_>>();
            let existing_consensus = existing_wallet_consensus
                .get(&asset)
                .copied()
                .unwrap_or_default();
            let technical_target = Decimal::ZERO;
            let aggregate = match aggregate_authoritative_positions(
                &asset,
                observed_at_ms,
                &layer.qualified_members,
                &confirmed_wallets,
            ) {
                Ok(aggregate) => aggregate,
                Err(_) => {
                    self.record_cohort_runtime_rejection(
                        &layer,
                        &asset,
                        CohortDecisionReason::PositionAggregationFailed,
                        None,
                        existing_consensus,
                        technical_target,
                        source_budget,
                        Decimal::ZERO,
                    )?;
                    continue;
                }
            };
            let Some(current_price) = mids.mids.get(&asset).copied() else {
                self.record_cohort_runtime_rejection(
                    &layer,
                    &asset,
                    CohortDecisionReason::MissingMarketPrice,
                    Some(&aggregate),
                    existing_consensus,
                    technical_target,
                    source_budget,
                    Decimal::ZERO,
                )?;
                continue;
            };
            // ATR and expected move are MFCE features. Missing technical
            // history must not veto an otherwise authoritative source signal.
            let r_unit = self
                .technical_engine
                .latest_atr(&asset)
                .ok()
                .flatten()
                .filter(|value| *value > Decimal::ZERO)
                .unwrap_or(current_price);
            let expected_move_fraction = self
                .technical_engine
                .expected_move_fraction(&asset)
                .ok()
                .flatten()
                .unwrap_or_default();
            let cohort_wallet_inputs = fresh_states
                .iter()
                .filter(|state| self.source_market_confirmed(&state.candidate_id, &asset))
                .filter(|state| {
                    layer
                        .qualified_members
                        .contains(&state.candidate_id.to_ascii_lowercase())
                })
                .filter_map(|state| {
                    let position = state.positions.get(&asset)?;
                    if state.account_value <= Decimal::ZERO {
                        return None;
                    }
                    let address = state.candidate_id.to_ascii_lowercase();
                    let weight = layer.bounded_wallet_weights.get(&address)?;
                    Some(ConsensusInput {
                        candidate_id: address.clone(),
                        allocation_weight: weight.to_f64()?,
                        confidence_modifier: 1.0,
                        source_exposure: position
                            .signed_notional
                            .checked_div(state.account_value)?
                            .to_f64()?,
                        enabled: true,
                        quarantined: false,
                        snapshot_age_ms: if self.source_stream_mode {
                            0
                        } else {
                            source_received_at
                                .get(&address)
                                .map(|received_at| now.saturating_sub(*received_at))
                                .unwrap_or(u64::MAX)
                        },
                    })
                })
                .collect::<Vec<_>>();
            let cohort_wallet_consensus = match bounded_additive_consensus(
                &cohort_wallet_inputs,
                self.config.global_risk.max_source_exposure,
                self.config.global_risk.source_snapshot_max_age_ms,
            ) {
                Ok(consensus) => consensus,
                Err(_) => {
                    self.record_cohort_runtime_rejection(
                        &layer,
                        &asset,
                        CohortDecisionReason::FilteredWalletConsensusFailed,
                        Some(&aggregate),
                        existing_consensus,
                        technical_target,
                        source_budget,
                        Decimal::ZERO,
                    )?;
                    continue;
                }
            };
            let filtered_wallet_consensus = Decimal::from_f64(cohort_wallet_consensus.exposure)
                .ok_or(EngineError::Arithmetic)?;
            // Execution friction is evaluated once, from the live book, at
            // the MFCE admission seam below. The cohort layer remains a pure
            // source-candidate constructor in deferred mode.
            let estimated_round_trip_cost_fraction = Decimal::ZERO;
            let crowding_penalty = aggregate
                .dominant_wallet_percentage
                .saturating_sub(dominant_wallet_limit_pct)
                .checked_div(Decimal::from(100))
                .ok_or(EngineError::Arithmetic)?
                .clamp(Decimal::ZERO, Decimal::ONE);
            let active_twap = layer.active_twaps.get(&asset).filter(|twap| {
                observed_at_ms >= twap.observed_at_ms
                    && observed_at_ms.saturating_sub(twap.observed_at_ms)
                        <= self.config.global_risk.source_snapshot_max_age_ms
            });
            let input = CohortSignalInput {
                membership: layer.resolution.clone(),
                aggregate: aggregate.clone(),
                filtered_wallet_consensus,
                active_twap: active_twap.cloned(),
                // Preserve the observed technical target on the record below,
                // but do not let it influence source activation or sizing.
                market_confirmation: Decimal::ZERO,
                penalties: CohortPenalties {
                    chase: Decimal::ZERO,
                    crowding: crowding_penalty,
                    funding: Decimal::ZERO,
                    liquidation: Decimal::ZERO,
                },
                risk_flags: CohortRiskFlags {
                    incomplete_authoritative_wallet_state: confirmed_wallets.is_empty(),
                    adverse_funding: false,
                    ..CohortRiskFlags::default()
                },
                position_change_age_ms,
                maximum_position_change_age_ms: self.config.global_risk.source_snapshot_max_age_ms,
                dominant_wallet_limit_pct,
                current_price,
                r_unit,
                estimated_round_trip_cost_fraction,
                expected_move_fraction,
                source_budget_fraction: source_budget,
                existing_wallet_source_target: existing_consensus,
                technical_target,
                rebalance_tolerance,
                defer_market_context_admission: true,
            };
            let record = match self.very_profitable_engine.evaluate(input) {
                Ok(record) => record,
                Err(_) => {
                    self.record_cohort_runtime_rejection(
                        &layer,
                        &asset,
                        CohortDecisionReason::SignalEvaluationFailed,
                        Some(&aggregate),
                        existing_consensus,
                        technical_target,
                        source_budget,
                        estimated_round_trip_cost_fraction,
                    )?;
                    continue;
                }
            };
            let inputs = consensus_inputs.entry(asset.clone()).or_default();
            inputs.retain(|input| input.candidate_id != "source:aggregate");
            let contributions = component_contributions.entry(asset.clone()).or_default();
            contributions.remove("source:very_profitable_cohort");
            if !record.source_target.is_zero() {
                inputs.push(ConsensusInput {
                    candidate_id: "source:aggregate".into(),
                    allocation_weight: 1.0,
                    confidence_modifier: 1.0,
                    source_exposure: record
                        .source_target
                        .checked_div(source_budget)
                        .ok_or(EngineError::Arithmetic)?
                        .to_f64()
                        .ok_or(EngineError::Arithmetic)?,
                    enabled: true,
                    quarantined: false,
                    snapshot_age_ms: 0,
                });
            }
            if !record.very_profitable_cohort_target.is_zero() {
                contributions.insert(
                    "source:very_profitable_cohort".into(),
                    record.very_profitable_cohort_target,
                );
            }
            self.metrics.cohort_indicator_records =
                self.metrics.cohort_indicator_records.saturating_add(1);
            self.cohort_indicator_records.push(record);
        }
        Ok(())
    }

    /// Flow discovery has no wallet provenance and uses the shared MFCE path.
    pub fn ingest_market_flow(&mut self, asset: String, flow: MarketFlow, now: u64) {
        let delayed = self.mfce.delayed_mut();
        if delayed.flow.contains_key(&asset) || delayed.flow.len() < 850 {
            let changed_minute = delayed
                .flow
                .get(&asset)
                .is_none_or(|old| old.last_sent_ms / 60_000 != flow.last_sent_ms / 60_000);
            delayed.flow.insert(asset.clone(), flow);
            if changed_minute {
                self.mfce_authorized_assets
                    .insert(asset, now.saturating_add(40_000));
            }
        }
    }
    pub fn learning_book_assets(&self) -> BTreeSet<String> {
        self.mfce.delayed().markets()
    }
    pub fn learning_stream_gap(&mut self) {
        self.mfce.delayed_mut().gap();
    }
    pub fn service_delayed_learning(&mut self, now: u64) {
        if now.saturating_sub(self.last_delayed_service) < 1_000 {
            return;
        }
        self.last_delayed_service = now;
        let mut books = self
            .mfce
            .delayed()
            .markets()
            .into_iter()
            .filter_map(|asset| {
                self.books
                    .get(&asset)
                    .filter(|(_, at)| now.saturating_sub(*at) <= 2_000)
                    .cloned()
            })
            .collect::<Vec<_>>();
        books.sort_by_key(|(_, at)| *at);
        for (book, at) in books {
            let _ = self.observe_delayed_book(&book, at);
        }
        let Ok(durable) = self.durable_timestamp(now) else {
            return;
        };
        self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(durable);
        self.mfce.delayed_mut().expire(durable);
    }
    fn observe_delayed_book(&mut self, book: &OrderBookResponse, now: u64) -> Option<()> {
        // Authoritative per-asset taker fee (single fee path); missing fee
        // state skips labeling exactly as before (no new gate).
        let fee = self.taker_fee_bps_for(&book.asset)?;
        let metadata = self.metadata.as_ref().filter(|_| {
            self.metadata_valid_until.is_some_and(|end| now <= end)
                && self.metadata_received_at.is_some_and(|start| start <= now)
        })?;
        let asset = metadata.universe.iter().find(|m| m.name == book.asset)?;
        let durable = self.durable_timestamp(now).ok()?;
        if self.durable_time_offset != 0
            && (book.source_time_ms > durable
                || durable.saturating_sub(book.source_time_ms) > 2_000)
        {
            return None;
        }
        let snapshot = ioc_snapshot(book, durable).ok()?;
        let rules = MarketRules {
            mark_price: snapshot.midpoint,
            size_step: Decimal::new(1, asset.size_decimals),
            price_tick: price_tick(snapshot.midpoint, asset.size_decimals).ok()?,
        };
        let cushion = Decimal::from_f64(self.config.slippage_buffer_bps / 10_000.0)?;
        let slippage = Decimal::from_f64(self.config.execution.max_slippage_bps / 10_000.0)?;
        let rates = metadata
            .contexts
            .iter()
            .map(|(a, c)| (a.clone(), c.funding_rate_hourly))
            .collect();
        self.mfce.delayed_mut().funding(durable, &rates);
        let labels = observe_executable(
            self.mfce.delayed_mut(),
            &book.asset,
            durable,
            &snapshot,
            &rules,
            fee / BPS_PER_UNIT_RETURN,
            cushion,
            slippage,
        );
        for (features, direction, opened, remaining_net_edge) in labels {
            self.mfce.learn_delayed(
                &book.asset,
                features,
                direction,
                opened,
                durable,
                remaining_net_edge,
            );
        }
        self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(durable);
        Some(())
    }

    pub fn service_mfce_training(&mut self) -> bool {
        let before = (
            self.mfce.state().pending_training.clone(),
            self.mfce
                .state()
                .incumbent
                .as_ref()
                .map(|model| model.epoch),
        );
        match self.mfce.maybe_start_training() {
            Ok(true) => {
                self.metrics.mfce_retrain_attempts =
                    self.metrics.mfce_retrain_attempts.saturating_add(1)
            }
            Ok(false) => {}
            Err(_) => {
                self.metrics.mfce_retrain_failures =
                    self.metrics.mfce_retrain_failures.saturating_add(1)
            }
        }
        match self.mfce.poll_training() {
            Ok(true) => {
                self.metrics.mfce_model_promotions =
                    self.metrics.mfce_model_promotions.saturating_add(1)
            }
            Ok(false) => {}
            Err(_) => {
                self.metrics.mfce_retrain_failures =
                    self.metrics.mfce_retrain_failures.saturating_add(1)
            }
        }
        before
            != (
                self.mfce.state().pending_training.clone(),
                self.mfce
                    .state()
                    .incumbent
                    .as_ref()
                    .map(|model| model.epoch),
            )
    }

    pub fn construct_next_decision(
        &mut self,
        now: Timestamp,
    ) -> Result<Option<DecisionRecord>, EngineError> {
        // Atomic planning boundary: MFCE mutations inside one planning
        // attempt must either commit alongside a DecisionRecord +
        // DecisionSample, or be rolled back entirely. A late projection or
        // accounting failure must not leave MFCE advanced with no
        // corresponding decision. Metrics/readiness side-effects are
        // observability and are intentionally preserved on failure.
        let mfce_checkpoint = self.mfce.planning_checkpoint();
        let authorized_checkpoint = self.mfce_authorized_assets.clone();
        let result = self.construct_next_decision_inner(now);
        if !matches!(result, Ok(Some(_))) {
            self.mfce.restore_planning_checkpoint(mfce_checkpoint);
            self.mfce_authorized_assets = authorized_checkpoint;
        }
        result
    }

    fn construct_next_decision_inner(
        &mut self,
        now: Timestamp,
    ) -> Result<Option<DecisionRecord>, EngineError> {
        // Do not clear live strategy readiness merely because a planning pass
        // starts. Some passes intentionally return no new decision while an
        // asset-scoped hold waits for a fresh book or source confirmation;
        // treating that as a global no-orders latch made one manual AERO hold
        // intermittently block every unrelated market. Global unsafe states
        // below still clear readiness explicitly.
        // Recovery preserves the strategy target; divergence is not an exit
        // signal. Source ingestion continues, but MFCE replans only on convergence.
        // Edge-only: global readiness stays true; RISK_ONLY alone gates
        // execution via strategy_target_execution_enabled().
        if self.execution_recovery_only() {
            return Ok(None);
        }
        self.accrue_funding(now)?;
        let Some((mids, mids_received)) = self.mids.clone() else {
            return Ok(None);
        };
        if now.saturating_sub(mids_received) > self.config.global_risk.source_snapshot_max_age_ms {
            return Ok(None);
        }
        let Some(metadata) = self.metadata.clone() else {
            return Ok(None);
        };
        let live_metadata_context = self
            .metadata_received_at
            .zip(self.metadata_valid_until)
            .is_some_and(|(received_at, valid_until)| received_at <= now && now <= valid_until);
        let mfce_now = self.durable_timestamp(now)?;
        self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(mfce_now);
        let mut eligibility = SourceEligibilitySummary::default();
        let mut members = Vec::new();
        let mut consensus_inputs: BTreeMap<String, Vec<ConsensusInput>> = BTreeMap::new();
        let mut source_contributions: BTreeMap<String, BTreeMap<String, Decimal>> = BTreeMap::new();
        let mut unavailable_hold_assets = BTreeSet::<String>::new();
        let mut existing_wallet_consensus = BTreeMap::new();
        let mut fresh_source_states = Vec::new();
        let mut source_received_at = BTreeMap::new();
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
                EngineError::Core(format!("missing absolute exposure state for {id}"))
            })?;
            if exposure_state.accepted_sequence != snapshot.sequence {
                return Err(EngineError::Core(format!(
                    "exposure state sequence mismatch for {id}"
                )));
            }
            let Some(normalized_exposures) = normalized_source_exposures(&snapshot.payload) else {
                eligibility.excluded.insert(id, ExclusionReason::Stale);
                continue;
            };
            if normalized_exposures != exposure_state.exposures {
                return Err(EngineError::Core(format!(
                    "normalized exposure state mismatch for {id}"
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
            let mut source_state = snapshot.payload.clone();
            if self.source_stream_mode {
                source_state
                    .positions
                    .retain(|asset, _| self.source_market_confirmed(&id, asset));
            }
            fresh_source_states.push(source_state);
            source_received_at.insert(id.clone(), snapshot.received_at);
            if is_cohort_candidate_label(&candidate.label) {
                // Cohort-only wallets are scheduled for authoritative state,
                // but vote only through the single deduplicated cohort target.
                continue;
            }
            if self.very_profitable_layer.as_ref().is_some_and(|layer| {
                layer.qualified_members.contains(&id)
                    && layer.resolution.overlapping_members.contains(&id)
            }) {
                // The address votes through the full-cohort composite below,
                // never through both source sets.
                continue;
            }
            for (asset, absolute_exposure) in &normalized_exposures {
                if !self.source_market_confirmed(&id, asset) {
                    continue;
                }
                let Some(exposure) = absolute_exposure.to_f64() else {
                    continue;
                };
                let input = ConsensusInput {
                    candidate_id: id.clone(),
                    allocation_weight: candidate.allocation_weight,
                    confidence_modifier: candidate
                        .confidence_modifier
                        .ok_or_else(|| EngineError::Core("missing confidence".into()))?,
                    source_exposure: exposure,
                    enabled: true,
                    quarantined: false,
                    snapshot_age_ms: if self.source_stream_mode {
                        0
                    } else {
                        now.saturating_sub(snapshot.received_at)
                    },
                };
                // MFCE is the sole conditional admission engine. Historical
                // technical-gating metadata remains provenance only; every
                // accepted source transition enters the same pooled model.
                consensus_inputs
                    .entry(asset.clone())
                    .or_default()
                    .push(input);
            }
        }
        for asset in self
            .mfce
            .delayed()
            .flow
            .keys()
            .filter(|a| mids.mids.contains_key(*a))
        {
            consensus_inputs.entry(asset.clone()).or_default();
        }
        for asset in self.technical_engine.tracked_assets() {
            consensus_inputs.entry(asset.clone()).or_default();
        }
        for asset in self.mfce.tracked_assets() {
            if mids.mids.contains_key(asset) {
                consensus_inputs.entry(asset.clone()).or_default();
            }
        }
        if let Some(layer) = &self.very_profitable_layer {
            for state in &fresh_source_states {
                if layer
                    .qualified_members
                    .contains(&state.candidate_id.to_ascii_lowercase())
                {
                    for asset in state.positions.keys() {
                        consensus_inputs.entry(asset.clone()).or_default();
                    }
                }
            }
        }
        // Collapse the complete source book to one bounded source component.
        // MFCE is the sole economic allocator; technical state is context and
        // receives no independent production sleeve.
        for (asset, inputs) in &mut consensus_inputs {
            let source = bounded_additive_consensus(
                inputs,
                self.config.global_risk.max_source_exposure,
                self.config.global_risk.source_snapshot_max_age_ms,
            )
            .map_err(|error| EngineError::Core(error.to_string()))?;
            let raw_contributions = source
                .contribution_by_candidate
                .into_iter()
                .map(|(candidate, contribution)| {
                    Decimal::from_f64(contribution)
                        .map(|value| (candidate, value))
                        .ok_or(EngineError::Arithmetic)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let source_component_target =
                Decimal::from_f64(source.exposure).ok_or(EngineError::Arithmetic)?;
            existing_wallet_consensus.insert(
                asset.clone(),
                Decimal::from_f64(source.exposure).ok_or(EngineError::Arithmetic)?,
            );
            let Some(scaled_contributions) = absolute_directional_component_targets(
                Some(&raw_contributions),
                source_component_target,
            )?
            else {
                // Edge-only: per-asset Hold, never a global abort. One bad
                // asset cannot flip global readiness.
                self.suspend_strategy_target(asset, now);
                source_contributions.insert(asset.clone(), BTreeMap::new());
                inputs.clear();
                continue;
            };
            source_contributions.insert(asset.clone(), scaled_contributions);
            inputs.clear();
            if source.exposure != 0.0 {
                inputs.push(ConsensusInput {
                    candidate_id: "source:aggregate".into(),
                    allocation_weight: 1.0,
                    confidence_modifier: 1.0,
                    source_exposure: source.exposure,
                    enabled: true,
                    quarantined: false,
                    snapshot_age_ms: 0,
                });
            }
        }
        // Assets already held by the follower remain in the target universe
        // even after every source goes flat. An empty consensus input produces
        // target zero and therefore an explicit flattening delta.
        let authoritative_assets = self.authoritative_assets();
        self.mfce
            .delayed_mut()
            .retain_open_trajectories(&authoritative_assets.iter().cloned().collect());
        include_held_assets_in_target_universe(&mut consensus_inputs, authoritative_assets);
        if let Some(layer) = self.very_profitable_layer.clone() {
            let missing_unheld_mids = consensus_inputs
                .keys()
                .filter(|asset| {
                    !mids.mids.contains_key(*asset) && self.authoritative_position(asset).is_zero()
                })
                .cloned()
                .collect::<Vec<_>>();
            let source_budget = Decimal::ONE;
            for asset in missing_unheld_mids {
                self.record_cohort_runtime_rejection(
                    &layer,
                    &asset,
                    CohortDecisionReason::MissingMarketPrice,
                    None,
                    existing_wallet_consensus
                        .get(&asset)
                        .copied()
                        .unwrap_or_default(),
                    Decimal::ZERO,
                    source_budget,
                    Decimal::ZERO,
                )?;
                consensus_inputs.remove(&asset);
                source_contributions.remove(&asset);
            }
        }
        for asset in consensus_inputs.keys().cloned().collect::<Vec<_>>() {
            if !metadata.universe.iter().any(|rules| rules.name == asset) {
                self.waiting_market_rules.insert(asset.clone());
                if !self.authoritative_position(&asset).is_zero()
                    || self.target_ledger.get(&asset).is_some_and(|state| {
                        !state.acknowledged_open_notional.is_zero()
                            || !state.unknown_result_notional.is_zero()
                    })
                    || self.pending.get(&asset).is_some_and(|pending| {
                        self.emitted_production_cloids
                            .contains(&pending.action.planned_cloid)
                    })
                {
                    return Ok(None);
                }
                consensus_inputs.remove(&asset);
                source_contributions.remove(&asset);
            }
        }
        if self.production_identity.is_some() {
            // Venue-aware reconciliation now covers admitted builder-perp
            // DEXes. Only markets lacking full execution capabilities
            // (metadata, asset ID, collateral, live state) stay
            // evaluation-only and fail closed here.
            for asset in consensus_inputs.keys().cloned().collect::<Vec<_>>() {
                if !self.is_production_execution_supported(&asset) && hip3_is_market(&asset) {
                    consensus_inputs.remove(&asset);
                    source_contributions.remove(&asset);
                }
            }
        }
        let market_rules = build_market_rules(&mids, &metadata, consensus_inputs.keys())?;
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
        let equity_f64 = equity.to_f64().ok_or(EngineError::Arithmetic)?;
        let leverage = Decimal::from_f64(curve_leverage(&self.config, equity_f64)?)
            .ok_or(EngineError::Arithmetic)?;
        let available_gross = equity
            .checked_mul(leverage)
            .and_then(|value| {
                value.checked_mul(Decimal::from_f64(
                    self.config.global_risk.global_risk_scale,
                )?)
            })
            .ok_or(EngineError::Arithmetic)?;
        // Technical indicators are read-only MFCE features. They no longer
        // create a separate target sleeve or apply their own cost gate.
        self.technical_targets.clear();
        self.apply_very_profitable_cohort(
            now,
            &mids,
            &metadata,
            &fresh_source_states,
            &source_received_at,
            &existing_wallet_consensus,
            &mut consensus_inputs,
            &mut source_contributions,
        )?;
        // The source and all-market flow families share one candidate set,
        // sleeve and risk projector. Source absence is explicit provenance.
        let mut origins = BTreeMap::new();
        let mut flow_candidates = self
            .mfce
            .delayed()
            .flow
            .iter()
            .filter_map(|(asset, flow)| {
                let f = flow.features(mfce_now)?;
                let strength =
                    ((f[1] + f[2]) / Decimal::from(2) * f[0].min(Decimal::ONE)).round_dp(1);
                (!strength.is_zero()
                    && mids.mids.contains_key(asset)
                    && execution_rules.contains_key(asset)
                    && (self.mfce.state().assets.contains_key(asset)
                        || self.mfce.state().assets.len()
                            < crate::mfce::MFCE_MAX_ASSETS.saturating_sub(32)))
                .then(|| (asset.clone(), strength))
            })
            .collect::<Vec<_>>();
        // A selected same-direction FlowAlpha probe keeps its place in the
        // bounded candidate set until issuance. Without this ownership rule,
        // ordinary rank movement around the top-32 boundary makes the target
        // disappear and releases/recreates the same information reservation
        // before the submission latency can elapse.
        let owns_flow_probe = |asset: &str, strength: Decimal| {
            self.pending.get(asset).is_some_and(|pending| {
                is_selected_unissued_risk_increase(
                    pending,
                    self.emitted_production_cloids
                        .contains(&pending.action.planned_cloid),
                ) && action_side_matches_target_delta(pending.action.side, Decimal::ZERO, strength)
            })
        };
        flow_candidates.sort_by(|a, b| {
            owns_flow_probe(&b.0, b.1)
                .cmp(&owns_flow_probe(&a.0, a.1))
                .then_with(|| b.1.abs().cmp(&a.1.abs()))
                .then_with(|| a.0.cmp(&b.0))
        });
        for (asset, strength) in flow_candidates.into_iter().take(32) {
            let inputs = consensus_inputs.entry(asset.clone()).or_default();
            let source_present = inputs.iter().any(|i| i.source_exposure != 0.0);
            origins.insert(
                asset.clone(),
                if source_present {
                    AlphaOrigin::SourceFlowConfluence
                } else {
                    AlphaOrigin::Flow
                },
            );
            inputs.push(ConsensusInput {
                candidate_id: "flow:market".into(),
                allocation_weight: 1.0,
                confidence_modifier: 1.0,
                source_exposure: strength.to_f64().unwrap_or_default(),
                enabled: true,
                quarantined: false,
                snapshot_age_ms: 0,
            });
            source_contributions
                .entry(asset)
                .or_default()
                .insert("flow:market".into(), strength * available_gross);
        }
        // Exchange exposure is risk authority, not a replacement strategy vote.
        // Missing observations suspend signing without changing durable targets.
        for asset in self.authoritative_assets() {
            let source_confirmed = members
                .iter()
                .any(|member| self.source_market_confirmed(&member.candidate_id, &asset));
            let prior_origin = self
                .mfce
                .state()
                .assets
                .get(&asset)
                .and_then(|state| state.target_origin);
            let source_known = source_confirmed
                && (matches!(
                    prior_origin,
                    Some(AlphaOrigin::Source | AlphaOrigin::SourceFlowConfluence)
                ) || source_contributions
                    .get(&asset)
                    .is_some_and(|values| values.values().any(|v| !v.is_zero())));
            let flow_known = self
                .mfce
                .delayed()
                .flow
                .get(&asset)
                .and_then(|flow| flow.features(mfce_now))
                .is_some();
            let unavailable = self.production_identity.is_some()
                && !self.authoritative_position(&asset).is_zero()
                && !source_known
                && !flow_known;
            let mark = mids
                .mids
                .get(&asset)
                .copied()
                .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
            let held_notional = self
                .authoritative_position(&asset)
                .checked_mul(mark)
                .ok_or(EngineError::Arithmetic)?;
            if unavailable {
                unavailable_hold_assets.insert(asset.clone());
                self.metrics.target_readiness_rejections =
                    self.metrics.target_readiness_rejections.saturating_add(1);
                eprintln!(
                    "asset_target_hold_unavailable=true asset={asset} observed_at={now} action=ASSET_HOLD nonfatal=true"
                );
                let exposure = held_notional
                    .checked_div(available_gross)
                    .ok_or(EngineError::Arithmetic)?
                    .clamp(-Decimal::ONE, Decimal::ONE);
                consensus_inputs.insert(
                    asset.clone(),
                    vec![ConsensusInput {
                        candidate_id: "source:unavailable-hold".into(),
                        allocation_weight: 1.0,
                        confidence_modifier: 1.0,
                        source_exposure: exposure.to_f64().ok_or(EngineError::Arithmetic)?,
                        enabled: true,
                        quarantined: false,
                        snapshot_age_ms: 0,
                    }],
                );
            }

            // A continuation may preserve exposure after the source target has
            // gone flat. Attribute that held slice from the exact ledger
            // components so a missing target component cannot globally abort
            // unrelated decisions. This cannot authorize an increase: the
            // synthetic target above is exactly the authenticated exposure.
            let has_directional_component =
                source_contributions.get(&asset).is_some_and(|values| {
                    values.values().any(|value| {
                        !value.is_zero()
                            && value.is_sign_positive() == held_notional.is_sign_positive()
                    })
                });
            if !has_directional_component {
                let mut components = exact_component_notionals(
                    self.ledger.source_positions_for_asset(&asset),
                    mark,
                )?;
                if components.is_empty() && unavailable {
                    components.insert("source:unavailable-hold".into(), held_notional);
                }
                if !components.is_empty() {
                    source_contributions.insert(asset, components);
                }
            }
        }
        for (asset, state) in &self.mfce.state().assets {
            let origin = state.target_origin.unwrap_or_else(|| {
                state
                    .active
                    .as_ref()
                    .map_or(AlphaOrigin::Source, |a| a.features.origin())
            });
            if origin == AlphaOrigin::Source
                || origins.contains_key(asset)
                || !mids.mids.contains_key(asset)
            {
                continue;
            }
            let inputs = consensus_inputs.entry(asset.clone()).or_default();
            if inputs.iter().any(|i| i.source_exposure != 0.0) {
                continue;
            }
            origins.insert(asset.clone(), origin);
            // Regression-only compatibility for historical shadow fixtures.
            // Production never turns a held quantity into a strategy target.
            #[cfg(test)]
            if self.production_identity.is_none()
                && self
                    .mfce
                    .delayed()
                    .flow
                    .get(asset)
                    .and_then(|flow| flow.features(mfce_now))
                    .is_none()
            {
                let held = self.authoritative_position(asset) * mids.mids[asset];
                inputs.push(ConsensusInput {
                    candidate_id: "flow:market".into(),
                    allocation_weight: 1.0,
                    confidence_modifier: 1.0,
                    source_exposure: held
                        .checked_div(available_gross)
                        .and_then(|value| value.to_f64())
                        .unwrap_or_default(),
                    enabled: true,
                    quarantined: false,
                    snapshot_age_ms: 0,
                });
            }
        }
        if self.production_identity.is_some() {
            // Cohort and flow candidates are merged after the initial source
            // gate above. Apply the capability boundary to the completed
            // candidate set so a late builder-perp candidate cannot reach
            // projection without authenticated market rules, asset ID,
            // collateral, and live state.
            for asset in consensus_inputs.keys().cloned().collect::<Vec<_>>() {
                if hip3_is_market(&asset) && !self.is_production_execution_supported(&asset) {
                    consensus_inputs.remove(&asset);
                    source_contributions.remove(&asset);
                    origins.remove(&asset);
                }
            }
        }
        // Authoritative per-asset taker fees (single fee path). Assets
        // without refreshed fee state keep the previous pending semantics
        // via `taker_fee_bps_for` (unsigned replays use the configured rate,
        // production exposure increases fail pending).
        let mut taker_fee_bps_by_asset = BTreeMap::<String, Decimal>::new();
        for asset in consensus_inputs.keys() {
            if let Some(fee) = self.taker_fee_bps_for(asset) {
                taker_fee_bps_by_asset.insert(asset.clone(), fee);
            }
        }
        let taker_fee_bps_for_asset = |asset: &str| taker_fee_bps_by_asset.get(asset).copied();
        let maximum_slippage_bps = Decimal::from_f64(self.config.execution.max_slippage_bps)
            .ok_or(EngineError::Arithmetic)?;
        let maximum_slippage = maximum_slippage_bps
            .checked_div(BPS_PER_UNIT_RETURN)
            .ok_or(EngineError::Arithmetic)?;
        let execution_floor_policy = ExecutionFloorPolicy {
            exchange_minimum_notional: Decimal::from_f64(
                self.config.global_risk.min_order_notional_usd,
            )
            .ok_or(EngineError::Arithmetic)?,
            rounding_buffer: Decimal::from_f64(self.config.global_risk.order_rounding_buffer_usd)
                .ok_or(EngineError::Arithmetic)?,
            closeability_margin: Decimal::from_f64(self.config.global_risk.closeability_margin_usd)
                .ok_or(EngineError::Arithmetic)?,
            maximum_slippage_fraction: maximum_slippage,
        };
        let total_tail_budget = remaining_mfce_tail_budget(&self.config, deployment)?;
        let source_capacity = available_gross;
        let has_usable_source = !members.is_empty();
        let mut allocation_candidates = Vec::new();
        let mut target_contexts = BTreeMap::new();
        let mut learning_seeds = BTreeMap::new();
        let mut remaining_edge_curves =
            BTreeMap::<String, Vec<RemainingEdgePredictionPoint>>::new();
        let mut continuation_targets = BTreeMap::new();
        for (asset, inputs) in &mut consensus_inputs {
            if !origins.contains_key(asset)
                && (!has_usable_source
                    || (self.source_stream_mode
                        && !members.iter().any(|member| {
                            self.source_market_confirmed(&member.candidate_id, asset)
                        })))
            {
                continue;
            }
            let source_inputs = inputs
                .iter()
                .filter(|input| !input.candidate_id.starts_with("technical:"))
                .cloned()
                .collect::<Vec<_>>();
            let desired_source = bounded_additive_consensus(
                &source_inputs,
                self.config.global_risk.max_source_exposure,
                self.config.global_risk.source_snapshot_max_age_ms,
            )
            .map_err(|error| EngineError::Core(error.to_string()))?;
            let raw_desired_source_target = available_gross
                .checked_mul(
                    Decimal::from_f64(desired_source.exposure).ok_or(EngineError::Arithmetic)?,
                )
                .ok_or(EngineError::Arithmetic)?;
            let raw_source_exposure =
                Decimal::from_f64(desired_source.exposure).ok_or(EngineError::Arithmetic)?;
            let mark = mids
                .mids
                .get(asset)
                .copied()
                .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
            let mut current_source_targets = self
                .ledger
                .source_positions_for_asset(asset)
                .into_iter()
                .filter(|(component, _)| !component.starts_with("technical:"))
                .map(|(component, quantity)| {
                    quantity
                        .checked_mul(mark)
                        .map(|notional| (component, notional))
                        .ok_or(EngineError::Arithmetic)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let current_source_target = if self.production_positions.is_some() {
                // Production fills live in the durable LiveTradingState, not
                // the production DualLedger. Position-aware MFCE admission must
                // therefore use the reconciled authoritative quantity.
                let authoritative = self
                    .authoritative_position(asset)
                    .checked_mul(mark)
                    .ok_or(EngineError::Arithmetic)?;
                current_source_targets.clear();
                if !authoritative.is_zero() {
                    current_source_targets.insert("source:aggregate".into(), authoritative);
                }
                authoritative
            } else {
                current_source_targets
                    .values()
                    .try_fold(Decimal::ZERO, |sum, target| sum.checked_add(*target))
                    .ok_or(EngineError::Arithmetic)?
            };
            target_contexts.insert(
                asset.clone(),
                MfceTargetContext {
                    raw_desired_source_target,
                    current_source_target,
                    current_source_targets: current_source_targets.clone(),
                },
            );
            let previous_source_exposure = self.mfce.previous_source_exposure(asset);
            if raw_source_exposure.is_zero()
                && previous_source_exposure.is_zero()
                && current_source_target.is_zero()
            {
                continue;
            }
            let technical = if self.source_stream_mode {
                // Streaming mode has no REST candle refresh. Until candles
                // are aggregated from the live trade stream, persisted
                // technical state is explicitly missing rather than silently
                // represented as current MFCE context.
                None
            } else {
                self.technical_engine
                    .feature_context(asset)
                    .map_err(|error| {
                        EngineError::Core(format!(
                            "technical feature extraction failed for {asset}: {error:?}"
                        ))
                    })?
            };
            let fallback_atr_fraction = self
                .technical_engine
                .latest_atr(asset)
                .ok()
                .flatten()
                .and_then(|atr| atr.checked_div(mark));
            let funding_rate_hourly = live_metadata_context
                .then(|| metadata.contexts.get(asset))
                .flatten()
                .map(|context| context.funding_rate_hourly);
            let origin = origins.get(asset).copied().unwrap_or_default();
            let previous_origin = self
                .mfce
                .state()
                .assets
                .get(asset)
                .and_then(|state| state.target_origin)
                .or_else(|| {
                    self.mfce
                        .delayed()
                        .provenance
                        .episodes
                        .values()
                        .filter(|episode| episode.asset == *asset && episode.closed_at.is_none())
                        .max_by_key(|episode| episode.opened_at)
                        .and_then(|episode| episode.transitions.last().map(|(_, origin)| *origin))
                });
            let provenance_changed = previous_origin.is_some_and(|previous| previous != origin);
            let position_age_ready = self
                .ledger
                .portfolio_open_state(asset)
                .map(|position| {
                    mfce_now.saturating_sub(position.opened_at)
                        >= MFCE_POSITION_EVALUATION_INTERVAL_MS
                })
                .or_else(|| {
                    self.mfce.delayed().turnover.get(asset).map(|turnover| {
                        mfce_now.saturating_sub(turnover.opened_at)
                            >= MFCE_POSITION_EVALUATION_INTERVAL_MS
                    })
                })
                // A reconciled position whose opening clock is unavailable is
                // eligible for evaluation, but its trajectory remains None.
                .unwrap_or(true);
            let heartbeat_due = position_age_ready
                && self.mfce.delayed().position_evaluation_due(
                    asset,
                    mfce_now,
                    MFCE_POSITION_EVALUATION_INTERVAL_MS,
                );
            let material_age_ready = self
                .ledger
                .portfolio_open_state(asset)
                .is_none_or(|position| mfce_now.saturating_sub(position.opened_at) >= 1_000);
            let feature_exposure = if !current_source_target.is_zero() {
                current_source_target
                    .checked_div(available_gross)
                    .ok_or(EngineError::Arithmetic)?
            } else if raw_source_exposure.is_zero() {
                if previous_source_exposure.is_zero() {
                    if current_source_target.is_sign_positive() {
                        Decimal::ONE
                    } else {
                        -Decimal::ONE
                    }
                } else {
                    previous_source_exposure
                }
            } else {
                raw_source_exposure
            };
            let mut preliminary_features = {
                mfce_feature_vector(
                    feature_exposure,
                    previous_source_exposure,
                    current_source_target,
                    raw_desired_source_target,
                    available_gross,
                    funding_rate_hourly.unwrap_or_default(),
                    technical.as_ref(),
                    fallback_atr_fraction,
                    None,
                )?
            };
            let flow = self
                .mfce
                .delayed()
                .flow
                .get(asset)
                .and_then(|f| f.features(mfce_now));
            let mut context = [Decimal::ZERO; 16];
            context[0] = mfce_indicator(flow.is_none());
            if let Some(flow) = flow {
                context[1..6].copy_from_slice(&flow[..5]);
                context[14] = flow[5];
                context[15] = flow[6];
                context[6] = raw_source_exposure * flow[1];
                context[7] = preliminary_features.values[17] * flow[1];
                context[8] = flow[1] / (Decimal::ONE + preliminary_features.values[12].abs());
            }
            if let Some(turnover) = self.mfce.delayed().turnover.get(asset) {
                context[9] = Decimal::from(mfce_now.saturating_sub(turnover.opened_at))
                    / Decimal::from(3_600_000);
                context[10] = turnover.notional.checked_div(equity).unwrap_or_default();
                context[13] = Decimal::from(mfce_now.saturating_sub(turnover.last_fill))
                    / Decimal::from(3_600_000);
            }
            context[11] = Decimal::from(match origin {
                AlphaOrigin::Source => 0,
                AlphaOrigin::Flow => 1,
                AlphaOrigin::SourceFlowConfluence => 2,
            });
            context[12] = mfce_indicator(origin == AlphaOrigin::Flow);
            if origin == AlphaOrigin::Flow {
                for i in [0, 1, 3] {
                    preliminary_features.values[i] = Decimal::ZERO;
                }
            }
            let source_value = existing_wallet_consensus
                .get(asset)
                .copied()
                .unwrap_or_default();
            let previous_source = self
                .mfce
                .delayed()
                .samples
                .iter()
                .rev()
                .find(|s| &s.asset == asset)
                .map_or(source_value, |s| s.features.values[3]);
            preliminary_features.values[0] = source_value.abs();
            preliminary_features.values[1] = source_value - previous_source;
            preliminary_features.values[3] = source_value;
            if let Some(flow) = flow {
                context[6] = source_value * flow[1];
            }
            preliminary_features.context = Some(context);
            let source_context_known = members
                .iter()
                .any(|member| self.source_market_confirmed(&member.candidate_id, asset));
            let accepted = origins.contains_key(asset)
                || self
                    .mfce_authorized_assets
                    .get(asset)
                    .is_some_and(|until| now <= *until)
                || (!current_source_target.is_zero() && (flow.is_some() || source_context_known));
            if !current_source_target.is_zero() {
                let current_policy = self
                    .fresh_mfce_policy_output_at(asset, mfce_now)
                    .map(|output| output.policy_state);
                let trajectory = self
                    .ledger
                    .portfolio_open_state(asset)
                    .and_then(|position| {
                        self.mfce.delayed_mut().observe_position(
                            asset,
                            &position.episode_id,
                            position.opened_at,
                            mfce_now,
                            position.signed_quantity,
                            position.average_entry_price,
                            position.realized_pnl,
                            mark,
                            equity,
                            current_policy,
                        )
                    });
                let (objective, _, _) =
                    position_evaluation_targets(current_source_target, raw_desired_source_target);
                preliminary_features.set_position_learning(trajectory, objective);
            }
            // Populate the material-change fingerprint with real current-book
            // liquidity before deciding whether to evaluate. This quote does
            // not authorize an action and never supplies delayed labels.
            // Edge-only: use last-known book even when stale (warning only).
            if !current_source_target.is_zero() {
                if let (Some((book, _)), Some(direction), Some(funding), Some(fee)) = (
                    self.books.get(asset),
                    MfceDirection::from_signed(current_source_target),
                    funding_rate_hourly,
                    taker_fee_bps_for_asset(asset),
                ) {
                    if let Ok(live) = mfce_live_market_context(
                        asset,
                        book,
                        direction,
                        current_source_target,
                        current_source_target,
                        fee,
                        maximum_slippage_bps,
                        funding,
                        Decimal::from(30u64) / Decimal::from(3_600u64),
                    ) {
                        preliminary_features.values[10..15].copy_from_slice(&[
                            Decimal::ZERO,
                            live.spread_bps,
                            live.entry_depth_ratio,
                            live.exit_depth_ratio,
                            live.depth_imbalance,
                        ]);
                    }
                }
            }
            let material_change_due = !current_source_target.is_zero()
                && material_age_ready
                && self.mfce.delayed().position_feature_change_due(
                    asset,
                    mfce_now,
                    &preliminary_features,
                );
            let position_evaluation_due = !current_source_target.is_zero()
                && (raw_source_exposure != previous_source_exposure
                    || provenance_changed
                    || material_change_due
                    || heartbeat_due);
            let continuation = position_evaluation_due && accepted;
            if accepted
                && (continuation
                    || raw_source_exposure != previous_source_exposure
                    || self.mfce_authorized_assets.contains_key(asset))
            {
                learning_seeds.insert(asset.clone(), (preliminary_features.clone(), origin));
            }
            let transition = if continuation {
                let (_, proposed, rejected) =
                    position_evaluation_targets(current_source_target, raw_desired_source_target);
                continuation_targets.insert(asset.clone(), (proposed, rejected));
                self.mfce.observe_position_state(
                    asset,
                    raw_source_exposure,
                    current_source_target,
                    proposed,
                    mark,
                    mfce_now,
                    preliminary_features,
                )
            } else {
                self.mfce.observe_raw_source(
                    asset,
                    accepted,
                    raw_source_exposure,
                    raw_desired_source_target,
                    current_source_target,
                    mark,
                    mfce_now,
                    preliminary_features,
                )
            }
            .map_err(mfce_error)?;
            if transition.needs_live_book {
                let transition_id = transition.transition_id.ok_or_else(|| {
                    EngineError::Core("pending MFCE transition has no identity".into())
                })?;
                // Edge-only: stale books become warnings, not blocks. Use
                // last-known book with widened uncertainty instead of
                // AwaitingLiveBook/Reject. Labeling truth still requires
                // fresh books; admission does not.
                let book_entry = self.books.get(asset);
                if let Some((_, received_at)) = book_entry {
                    let age = now.saturating_sub(*received_at);
                    if age > MFCE_LIVE_BOOK_MAX_AGE_MS
                        && self.stale_book_warning_assets.insert(asset.clone())
                    {
                        eprintln!("stale_book_used=true asset={asset} age_ms={age} action=ADMIT_WITH_WIDENED_UNCERTAINTY nonfatal=true");
                    }
                }
                let fresh_book = book_entry;
                if let (
                    Some((book, _)),
                    Some(direction),
                    Some(funding_rate_hourly),
                    Some(taker_fee_bps),
                ) = (
                    fresh_book,
                    MfceDirection::from_signed(if continuation {
                        current_source_target
                    } else {
                        raw_source_exposure
                    }),
                    funding_rate_hourly,
                    taker_fee_bps_for_asset(asset),
                ) {
                    let admission_target = self
                        .mfce
                        .pending_position_notional(asset, transition_id, raw_desired_source_target)
                        .map_err(mfce_error)?;
                    // Evaluate a curve. Each point uses one atomic q10/q50
                    // generation and horizon-matched funding friction. The
                    // best conservative point informs policy; no point is an
                    // exit timer.
                    let horizon_indices: &[usize] = if continuation {
                        &[0, 1, 2, 3]
                    } else {
                        &[0, 1, 2, 3, 4, 5, 6]
                    };
                    let mut curve_candidates = Vec::new();
                    for &horizon_index in horizon_indices {
                        let horizon_ms = HORIZONS_MS[horizon_index];
                        let holding_hours = Decimal::from(horizon_ms)
                            .checked_div(Decimal::from(3_600_000u64))
                            .ok_or(EngineError::Arithmetic)?;
                        let Ok(live_context) = mfce_live_market_context(
                            asset,
                            book,
                            direction,
                            current_source_target,
                            admission_target,
                            taker_fee_bps,
                            maximum_slippage_bps,
                            funding_rate_hourly,
                            holding_hours,
                        ) else {
                            continue;
                        };
                        self.mfce
                            .update_pending_live_context(
                                asset,
                                transition_id,
                                [
                                    Decimal::ZERO,
                                    live_context.spread_bps,
                                    live_context.entry_depth_ratio,
                                    live_context.exit_depth_ratio,
                                    live_context.depth_imbalance,
                                ],
                            )
                            .map_err(mfce_error)?;
                        // Edge-only action hurdle: base friction only
                        // (actual live_context.friction_bps). Fee-multiplier
                        // buffer + 15m re-entry penalty removed from hurdle;
                        // settled accounting + labels keep actuals.
                        let horizon_friction = live_context.friction_bps;
                        let reentry_penalty_applied = false;
                        if let Ok(prediction) = self.mfce.predict_pending_at_horizon(
                            asset,
                            transition_id,
                            horizon_ms,
                            horizon_friction,
                        ) {
                            let input = MfceAllocationInput {
                                prediction,
                                friction_bps: horizon_friction,
                                copytrade_conviction: raw_source_exposure.abs().min(Decimal::ONE),
                                current_position_notional: current_source_target,
                                proposed_position_notional: admission_target,
                                remaining_tail_loss_budget_usd: total_tail_budget,
                            };
                            if let Ok(policy) = evaluate_allocation_policy(&input) {
                                if reentry_probe_allowed(
                                    reentry_penalty_applied,
                                    input.prediction.used_model,
                                    policy.policy_state,
                                    policy.conservative_edge_bps,
                                ) {
                                    curve_candidates.push((
                                        horizon_ms,
                                        policy,
                                        input,
                                        live_context,
                                    ));
                                }
                            }
                        }
                    }
                    curve_candidates.sort_by(|a, b| {
                        b.1.conservative_edge_bps
                            .cmp(&a.1.conservative_edge_bps)
                            .then_with(|| a.0.cmp(&b.0))
                    });
                    if let Some((selected_horizon, selected_policy, selected_input, live_context)) =
                        curve_candidates.first().cloned()
                    {
                        let rules = execution_rules
                            .get(asset)
                            .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
                        let side = if direction == MfceDirection::Long {
                            Side::Buy
                        } else {
                            Side::Sell
                        };
                        let floor = execution_floor_for_asset(side, rules, execution_floor_policy)
                            .map_err(core)?;
                        let baseline_order_notional =
                            allocation_baseline_target(current_source_target, admission_target)
                                .checked_sub(current_source_target)
                                .ok_or(EngineError::Arithmetic)?
                                .abs();
                        let required_order_notional = if current_source_target.is_zero() {
                            floor.minimum_opening_notional
                        } else {
                            floor.minimum_order_notional
                        };
                        let minimum_executable_increment = required_order_notional
                            .checked_sub(baseline_order_notional)
                            .unwrap_or(Decimal::ZERO)
                            .max(Decimal::ZERO);
                        let maximum_safe_increment =
                            policy_sized_increment(&selected_input, &selected_policy)
                                .map_err(mfce_error)?;
                        if !maximum_safe_increment.is_zero()
                            && maximum_safe_increment < minimum_executable_increment
                        {
                            let mut structural_rejection = selected_policy;
                            structural_rejection.admitted = false;
                            structural_rejection.reason =
                                Some(MfceRejectionReason::BelowExchangeMinimum);
                            structural_rejection.allocation_fraction = Decimal::ZERO;
                            structural_rejection.modeled_tail_loss_usd = Decimal::ZERO;
                            self.mfce
                                .record_allocation(
                                    asset,
                                    transition_id,
                                    &structural_rejection,
                                    &selected_input.prediction,
                                    current_source_target,
                                )
                                .map_err(mfce_error)?;
                            // Structurally impossible probes remain evaluated without
                            // reserving Explore capacity. Flat probes do not consume a
                            // delayed-label slot; open-position continuations may retain
                            // one bounded position-quality counterfactual.
                            if !continuation {
                                learning_seeds.remove(asset);
                            }
                            continue;
                        }
                        self.mfce
                            .freeze_pending_learning_horizon(asset, transition_id, selected_horizon)
                            .map_err(mfce_error)?;
                        // Freeze the exact liquidity and selected horizon used
                        // by policy before allocation/risk can alter exposure.
                        if let Some((features, _)) = learning_seeds.get_mut(asset) {
                            features.values[10..15].copy_from_slice(&[
                                Decimal::ZERO,
                                live_context.spread_bps,
                                live_context.entry_depth_ratio,
                                live_context.exit_depth_ratio,
                                live_context.depth_imbalance,
                            ]);
                            features.set_learning_horizon_ms(selected_horizon);
                        }
                        let prediction_curve = curve_candidates
                            .iter()
                            .map(|(horizon_ms, policy, input, _)| {
                                let (objective, feature_snapshot_id) = self
                                    .mfce
                                    .pending_feature_identity(asset, transition_id, *horizon_ms)
                                    .map_err(mfce_error)?;
                                Ok(RemainingEdgePredictionPoint {
                                    horizon_ms: *horizon_ms,
                                    model_epoch: input.prediction.model_epoch,
                                    q10_net_bps: policy.net_q10_bps,
                                    q50_net_bps: policy.net_q50_bps,
                                    uncertainty_bps: input.prediction.uncertainty_bps,
                                    used_model: input.prediction.used_model,
                                    objective,
                                    feature_snapshot_id,
                                })
                            })
                            .collect::<Result<Vec<_>, EngineError>>()?;
                        remaining_edge_curves.insert(asset.clone(), prediction_curve);
                        let prior_tail_reservation = self
                            .mfce
                            .reserved_tail_loss_usd(asset, current_source_target)
                            .map_err(mfce_error)?;
                        allocation_candidates.push(MfceCrossSectionalCandidate {
                            asset: asset.clone(),
                            transition_id,
                            input: selected_input,
                            prior_tail_loss_usd: prior_tail_reservation,
                            minimum_executable_increment,
                        });
                    } else {
                        self.mfce
                            .reject_pending_without_prediction(
                                asset,
                                transition_id,
                                MfceRejectionReason::InsufficientPooledSupport,
                            )
                            .map_err(mfce_error)?;
                    }
                }
            }
        }
        // Once MFCE, allocation and hard risk have selected an executable
        // increase, that exact action owns its reservation until issuance.
        // Ordinary same-side target repricing may inform a later marginal ADD,
        // but cannot continuously replace the selected entry before its
        // latency boundary. A flat/reversed target is deliberately excluded
        // here and follows the normal retirement path below.
        let selected_unissued_increase_assets = self
            .pending
            .iter()
            .filter(|(asset, pending)| {
                target_contexts.get(asset.as_str()).is_some_and(|context| {
                    owns_unissued_risk_increase(
                        pending,
                        self.emitted_production_cloids
                            .contains(&pending.action.planned_cloid),
                        context,
                    )
                })
            })
            .map(|(asset, _)| asset.clone())
            .collect::<BTreeSet<_>>();
        let allocation_replacement_assets;
        {
            allocation_replacement_assets = allocation_candidates
                .iter()
                .map(|candidate| candidate.asset.clone())
                .filter(|asset| !selected_unissued_increase_assets.contains(asset))
                .collect::<BTreeSet<_>>();
            let mut committed_source_gross = Decimal::ZERO;
            let mut committed_explore_information_gross = Decimal::ZERO;
            let mut existing_tail_reservations = Decimal::ZERO;
            for (asset, context) in &target_contexts {
                let replaces_outstanding = allocation_replacement_assets.contains(asset);
                let committed_target = if continuation_targets.contains_key(asset) {
                    context.current_source_target
                } else if replaces_outstanding {
                    allocation_baseline_target(
                        context.current_source_target,
                        context.raw_desired_source_target,
                    )
                } else {
                    self.mfce.effective_target(
                        asset,
                        context.current_source_target,
                        context.raw_desired_source_target,
                    )
                };
                let projected_notional = self.projected_existing_notional(
                    asset,
                    committed_target,
                    replaces_outstanding,
                )?;
                let projected_magnitude = projected_notional.abs().max(committed_target.abs());
                committed_source_gross = committed_source_gross
                    .checked_add(projected_magnitude)
                    .ok_or(EngineError::Arithmetic)?;
                if self.mfce.has_information_probe_admission(asset) {
                    committed_explore_information_gross = committed_explore_information_gross
                        .checked_add(projected_notional.abs())
                        .ok_or(EngineError::Arithmetic)?;
                }
                existing_tail_reservations = existing_tail_reservations
                    .checked_add(
                        self.mfce
                            .reserved_tail_loss_usd(asset, projected_notional)
                            .map_err(mfce_error)?,
                    )
                    .ok_or(EngineError::Arithmetic)?;
            }
            // Edge-only: full size on positive edge. Budget/capacity caps
            // removed (diagnostics only); only exchange-minimum and the
            // sovereign projection tail floor may still limit size.
            let _ = (&committed_source_gross, &existing_tail_reservations);
            let _ = remaining_explore_information_budget(
                source_capacity,
                committed_explore_information_gross,
            )
            .map_err(mfce_error)?;
            let available_increment_notional = source_capacity;
            let remaining_tail_budget = total_tail_budget;
            let remaining_explore_information_notional = source_capacity;
            let decisions = allocate_cross_sectional(
                &allocation_candidates,
                available_increment_notional,
                remaining_tail_budget,
                remaining_explore_information_notional,
            )
            .map_err(mfce_error)?;
            for candidate in &allocation_candidates {
                let decision = decisions.get(&candidate.asset).ok_or_else(|| {
                    EngineError::Core("cross-sectional MFCE decision missing".into())
                })?;
                self.mfce
                    .record_allocation(
                        &candidate.asset,
                        candidate.transition_id,
                        decision,
                        &candidate.input.prediction,
                        candidate.input.current_position_notional,
                    )
                    .map_err(mfce_error)?;
            }

            // Only after the complete candidate set is ranked do model-sized
            // targets replace raw source targets. This makes allocation
            // independent of BTreeMap iteration and tail-reservation mutation.
            for (asset, inputs) in &mut consensus_inputs {
                let Some(context) = target_contexts.get(asset) else {
                    continue;
                };
                let effective_target = if let Some((positive, rejected)) =
                    continuation_targets.get(asset)
                {
                    if self
                        .fresh_mfce_policy_output_at(asset, mfce_now)
                        .is_some_and(|output| {
                            output.policy_state == crate::mfce::MfcePolicyState::Reject
                        })
                    {
                        // ENTRY != HOLD: a rejected continuation holds valid
                        // exposure instead of auto-flattening on noise. Only a
                        // genuine reversal (opposite-side desired target)
                        // follows the retirement path; it must still clear
                        // full transition cost downstream.
                        continuation_hold_target(
                            context.current_source_target,
                            context.raw_desired_source_target,
                            *rejected,
                        )
                    } else {
                        self.mfce
                            .effective_target(asset, context.current_source_target, *positive)
                    }
                } else {
                    self.mfce.effective_target(
                        asset,
                        context.current_source_target,
                        context.raw_desired_source_target,
                    )
                };
                if effective_target == context.raw_desired_source_target {
                    continue;
                }
                inputs.clear();
                if !effective_target.is_zero() && !source_capacity.is_zero() {
                    inputs.push(ConsensusInput {
                        candidate_id: "source:aggregate".into(),
                        allocation_weight: 1.0,
                        confidence_modifier: 1.0,
                        source_exposure: effective_target
                            .checked_div(source_capacity)
                            .ok_or(EngineError::Arithmetic)?
                            .clamp(-Decimal::ONE, Decimal::ONE)
                            .to_f64()
                            .ok_or(EngineError::Arithmetic)?,
                        enabled: true,
                        quarantined: false,
                        snapshot_age_ms: 0,
                    });
                }
                let contributions = source_contributions.entry(asset.clone()).or_default();
                if effective_target.is_zero() {
                    contributions.clear();
                } else if self.mfce.uses_current_position_attribution(asset)
                    || continuation_targets.get(asset).is_some_and(|_| {
                        effective_target.abs() <= context.current_source_target.abs()
                    })
                {
                    *contributions = context.current_source_targets.clone();
                }
            }
            self.mfce.finish_source_observation_cycle();
            self.mfce_authorized_assets.clear();
        }
        let retained_selected_increase_assets = selected_unissued_increase_assets
            .into_iter()
            .filter(|asset| {
                self.fresh_mfce_policy_output_at(asset, mfce_now)
                    .is_some_and(|output| {
                        matches!(
                            output.policy_state,
                            crate::mfce::MfcePolicyState::Explore
                                | crate::mfce::MfcePolicyState::Exploit
                        )
                    })
            })
            .collect::<BTreeSet<_>>();
        let in_flight_pending_assets = self
            .pending
            .iter()
            .filter(|(_, pending)| {
                self.emitted_production_cloids
                    .contains(&pending.action.planned_cloid)
            })
            .map(|(asset, _)| asset.clone())
            .collect::<BTreeSet<_>>();
        let pending_replacement_assets = allocation_replacement_assets
            .iter()
            .filter(|asset| !in_flight_pending_assets.contains(asset.as_str()))
            .cloned()
            .chain(
                self.pending
                    .keys()
                    .filter(|asset| !in_flight_pending_assets.contains(asset.as_str()))
                    .filter(|asset| !retained_selected_increase_assets.contains(asset.as_str()))
                    .cloned(),
            )
            .chain(
                unavailable_hold_assets
                    .iter()
                    .filter(|asset| self.pending.contains_key(asset.as_str()))
                    .cloned(),
            )
            .filter(|asset| !self.waiting_market_rules.contains(asset))
            .collect::<BTreeSet<_>>();
        let projection_filled_positions = filled_positions.clone();
        let market_bytes =
            serde_json::to_vec(&mids).map_err(|error| EngineError::Core(error.to_string()))?;
        let projection_input = PortfolioProjectionInput {
            current_equity: equity,
            curve_leverage: leverage,
            global_risk_scale: Decimal::from_f64(self.config.global_risk.global_risk_scale)
                .ok_or(EngineError::Arithmetic)?,
            max_single_asset_equity_pct: Decimal::from_f64(
                self.config.global_risk.max_single_asset_equity_pct,
            )
            .ok_or(EngineError::Arithmetic)?,
            max_net_equity_pct: Decimal::from_f64(self.config.global_risk.max_net_equity_pct)
                .ok_or(EngineError::Arithmetic)?,
            filled_positions,
            filled_position_state_complete: true,
            acknowledged_open_orders: self
                .pending_open_orders_excluding(&pending_replacement_assets),
            open_order_state_complete: true,
            unconstrained_targets: BTreeMap::new(),
            market_rules,
        };
        let execution_cushion = Decimal::from_f64(self.config.slippage_buffer_bps / 10_000.0)
            .ok_or(EngineError::Arithmetic)?;
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
            execution_floor_policy,
            slot_rank_hysteresis: Decimal::from_f64(
                self.config.global_risk.slot_rank_hysteresis,
            )
            .ok_or(EngineError::Arithmetic)?,
            projection_input: projection_input.clone(),
            previous_target: self.previous_target.clone(),
            created_at_mono: now,
        })
        .map_err(|error| {
            self.metrics.projection_violations += 1;
            EngineError::Core(format!(
                "{error}; equity={equity}; leverage={leverage}; filled_positions={projection_filled_positions:?}"
            ))
        })?;
        let mut component_targets_by_asset = BTreeMap::new();
        for (asset, target) in &decision.projection.constrained_targets {
            let Some(components) =
                absolute_directional_component_targets(source_contributions.get(asset), *target)?
            else {
                // Edge-only: per-asset Hold with empty components, never a
                // global abort.
                self.suspend_strategy_target(asset, now);
                component_targets_by_asset.insert(asset.clone(), BTreeMap::new());
                continue;
            };
            component_targets_by_asset.insert(asset.clone(), components);
        }
        let mut durable_desired = decision.unconstrained_targets.clone();
        let mut durable_admitted = decision.projection.constrained_targets.clone();
        for asset in &self.waiting_market_rules {
            if let Some(previous) = self.target_ledger.get(asset) {
                durable_desired.insert(asset.clone(), previous.raw_desired_notional);
                durable_admitted.insert(asset.clone(), previous.admitted_target_notional);
            }
        }
        self.target_ledger
            .replace_absolute_targets(
                &durable_desired,
                &durable_admitted,
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
            .ok_or(EngineError::Arithmetic)?;
        self.metrics.decisions += 1;
        self.record_micro_density(&decision)?;
        for (asset, target) in &self.technical_targets {
            if !target.score.is_zero()
                && decision.micro_slots.get(asset).is_some_and(|slot| {
                    !slot.admitted_notional.is_zero()
                        && slot.admitted_notional.is_sign_positive()
                            == target.score.is_sign_positive()
                })
            {
                self.technical_delivery.targets_admitted_sparse = self
                    .technical_delivery
                    .targets_admitted_sparse
                    .saturating_add(1);
            }
        }
        let reduce_only_by_asset = decision
            .projection
            .proposed_deltas
            .iter()
            .filter_map(|(asset, delta)| {
                let filled = projection_filled_positions
                    .get(asset)
                    .copied()
                    .unwrap_or_default();
                decision.micro_slots.get(asset).and_then(|slot| {
                    requires_reduce_only(filled, *delta, slot.admitted_notional)
                        .then(|| (asset.clone(), true))
                })
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
        // Submitted IOCs and selected, same-side unissued increases retain
        // their exact ownership identity. Any later desired increment is a
        // separate SizeQuality decision after this root reaches a terminal
        // exchange outcome.
        actions.retain(|action| {
            !in_flight_pending_assets.contains(&action.asset)
                && !retained_selected_increase_assets.contains(&action.asset)
                && !unavailable_hold_assets.contains(&action.asset)
        });
        let proposed_action_notionals = actions
            .iter()
            .map(|action| (action.asset.clone(), action.rounded_notional))
            .collect::<BTreeMap<_, _>>();
        let executable_actions = actions
            .into_iter()
            .filter(|action| !self.execution_recovery_only() || action.reduce_only)
            .filter(|action| {
                decision.micro_slots.get(&action.asset).is_some_and(|slot| {
                    // A full flatten remains eligible for sovereign exit
                    // planning. Tiny partial reductions are structurally
                    // non-executable and must not churn pending roots.
                    action_notional_is_structurally_executable(
                        action.reduce_only,
                        slot.admitted_notional,
                        slot.filled_notional,
                        action.rounded_notional,
                        slot.execution_floor.minimum_order_notional,
                        slot.execution_floor.minimum_opening_notional,
                    )
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
        for asset in &pending_replacement_assets {
            let should_retain = match (self.pending.get(asset), executable_by_asset.get(asset)) {
                (Some(previous), Some(replacement)) => {
                    let rules = execution_rules
                        .get(asset)
                        .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
                    let tolerance = rules
                        .mark_price
                        .checked_mul(rules.size_step)
                        .ok_or(EngineError::Arithmetic)?;
                    pending_action_is_immaterial_replacement(
                        &previous.action,
                        replacement,
                        tolerance,
                    )
                }
                _ => false,
            };
            if should_retain {
                continue;
            }
            if let Some(previous) = self.pending.remove(asset) {
                let outcome = if unavailable_hold_assets.contains(asset) {
                    ActionAttemptOutcome::BlockedByCurrentRisk
                } else {
                    ActionAttemptOutcome::SupersededByNewTarget
                };
                self.action_lifecycle_events.push(ActionLifecycleEvent {
                    asset: asset.clone(),
                    decision_id: previous.action.decision_id.to_string(),
                    planned_cloid: previous.action.planned_cloid.to_string(),
                    root_planned_cloid: previous.root_planned_cloid,
                    parent_planned_cloid: previous
                        .execution
                        .as_ref()
                        .and_then(|context| context.parent_planned_cloid.clone()),
                    retry_generation: previous.action.retry_generation,
                    observed_at_mono: now,
                    requested_notional: previous.action.rounded_notional,
                    filled_quantity: None,
                    unfilled_quantity: None,
                    outcome,
                });
            }
        }
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
                    .ok_or_else(|| EngineError::Core("missing pending book intent".into()))?;
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
                .ok_or_else(|| EngineError::Core("missing continuation".into()))?;
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
                        .ok_or_else(|| EngineError::Core("missing retained intent".into()))?;
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
                    .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
                let snapshot = ioc_snapshot(book, *received_at)?;
                let target = decision
                    .projection
                    .constrained_targets
                    .get(&asset)
                    .copied()
                    .unwrap_or_default();
                let component_targets = component_targets_by_asset[&asset].clone();
                let economic_attribution =
                    classify_economic_attribution(source_contributions.get(&asset));
                let execution_context = |parent_planned_cloid, continuation_kind| {
                    let mut input = projection_input.clone();
                    input.unconstrained_targets = decision.projection.constrained_targets.clone();
                    PendingExecutionContext {
                        parent_planned_cloid,
                        decision_book: snapshot.clone(),
                        price_tick: rules.price_tick,
                        size_step: rules.size_step,
                        projection_input: input,
                        risk_policy_hash: decision.risk_policy_hash,
                        configuration_hash: decision.config_hash,
                        continuation_kind,
                        economic_attribution,
                    }
                };
                let notional_tolerance = rules
                    .mark_price
                    .checked_mul(rules.size_step)
                    .ok_or(EngineError::Arithmetic)?;
                let preserved_root = self.pending.get(&asset).and_then(|previous| {
                    pending_action_is_immaterial_replacement(
                        &previous.action,
                        &action,
                        notional_tolerance,
                    )
                    .then(|| previous.root_planned_cloid.clone())
                });
                if let Some(root_planned_cloid) = preserved_root {
                    let previous = self.pending.get_mut(&asset).unwrap();
                    if previous.execution.is_none() {
                        previous.execution = Some(execution_context(None, None));
                    }
                    if previous
                        .component_remaining
                        .keys()
                        .any(|candidate| candidate.starts_with("technical:"))
                    {
                        self.technical_delivery.roots.insert(root_planned_cloid);
                    }
                    continue;
                }
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
                let continuation_kind = continuation.as_ref().map(|intent| intent.kind);
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
                if component_targets
                    .keys()
                    .any(|candidate| candidate.starts_with("technical:"))
                {
                    self.technical_delivery
                        .roots
                        .insert(root_planned_cloid.clone());
                }
                if continuation_kind == Some(ContinuationKind::CloseFirstReversal)
                    && !action.reduce_only
                {
                    self.technical_delivery.addition_emitted =
                        self.technical_delivery.addition_emitted.saturating_add(1);
                }
                if let Some(previous) = self.pending.get(&asset) {
                    if previous.action.planned_cloid != action.planned_cloid {
                        self.action_lifecycle_events.push(ActionLifecycleEvent {
                            asset: asset.clone(),
                            decision_id: previous.action.decision_id.to_string(),
                            planned_cloid: previous.action.planned_cloid.to_string(),
                            root_planned_cloid: previous.root_planned_cloid.clone(),
                            parent_planned_cloid: previous
                                .execution
                                .as_ref()
                                .and_then(|context| context.parent_planned_cloid.clone()),
                            retry_generation: previous.action.retry_generation,
                            observed_at_mono: now,
                            requested_notional: previous.action.rounded_notional,
                            filled_quantity: None,
                            unfilled_quantity: None,
                            outcome: ActionAttemptOutcome::SupersededByNewTarget,
                        });
                    }
                }
                let mut preissue_outcome = None;
                let remaining_order_quantity = if action.reduce_only {
                    let reference_price = snapshot.midpoint;
                    let filled_quantity = self.authoritative_position(&asset);
                    let filled_notional = filled_quantity
                        .checked_mul(reference_price)
                        .ok_or(EngineError::Arithmetic)?;
                    let current_rules = MarketRules {
                        mark_price: reference_price,
                        price_tick: rules.price_tick,
                        size_step: rules.size_step,
                    };
                    let input = ExitPlanningInput {
                        asset: &asset,
                        desired_target_notional: target,
                        filled_notional,
                        filled_quantity,
                        acknowledged_open_notional: Decimal::ZERO,
                        unknown_result_notional: Decimal::ZERO,
                        continuation_notional: Decimal::ZERO,
                        reference_price,
                        market_rules: &current_rules,
                        market_snapshot: &snapshot,
                        execution_floor_policy,
                        execution_cushion,
                        maximum_slippage,
                    };
                    let (quantity, outcome) = preissue_reduce_only_quantity(&action, &input)?;
                    preissue_outcome = outcome;
                    quantity
                } else {
                    round_exchange_step(
                        action
                            .rounded_notional
                            .checked_div(rules.mark_price)
                            .ok_or(EngineError::Arithmetic)?,
                        rules.size_step,
                        false,
                    )?
                };
                if remaining_order_quantity <= Decimal::ZERO {
                    self.action_lifecycle_events.push(ActionLifecycleEvent {
                        asset,
                        decision_id: action.decision_id.to_string(),
                        planned_cloid: action.planned_cloid.to_string(),
                        root_planned_cloid,
                        parent_planned_cloid,
                        retry_generation: action.retry_generation,
                        observed_at_mono: now,
                        requested_notional: action.rounded_notional,
                        filled_quantity: None,
                        unfilled_quantity: None,
                        outcome: preissue_outcome.unwrap_or(
                            ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute,
                        ),
                    });
                    continue;
                }
                let signed_order_quantity = match action.side {
                    Side::Buy => remaining_order_quantity,
                    Side::Sell => -remaining_order_quantity,
                };
                let portfolio_position_after = self
                    .ledger
                    .portfolio_position(&asset)
                    .checked_add(signed_order_quantity)
                    .ok_or(EngineError::Arithmetic)?;
                let component_allocation = match partition_component_fill(
                    &self.ledger.source_positions_for_asset(&asset),
                    action.side,
                    remaining_order_quantity,
                    &component_targets,
                    rules.mark_price,
                    portfolio_position_after,
                    rules.size_step,
                ) {
                    Ok(allocation) => allocation,
                    Err(EngineError::Core(message)) => {
                        let reason = message.replace(|c: char| !c.is_ascii_alphanumeric(), "_");
                        eprintln!(
                            "component_attribution_unavailable=true asset={asset} \
                             decision_id={} planned_cloid={} requested_notional={} \
                             remaining_quantity={} side={:?} action=REJECT_ORDER \
                             alert=true nonfatal=true reason={reason}",
                            action.decision_id,
                            action.planned_cloid,
                            action.rounded_notional,
                            remaining_order_quantity,
                            action.side
                        );
                        self.action_lifecycle_events.push(ActionLifecycleEvent {
                            asset,
                            decision_id: action.decision_id.to_string(),
                            planned_cloid: action.planned_cloid.to_string(),
                            root_planned_cloid,
                            parent_planned_cloid,
                            retry_generation: action.retry_generation,
                            observed_at_mono: now,
                            requested_notional: action.rounded_notional,
                            filled_quantity: None,
                            unfilled_quantity: None,
                            outcome: ActionAttemptOutcome::BlockedByCurrentRisk,
                        });
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let mut component_remaining = component_allocation.total_quantities;
                canonicalize_allocation_total(&mut component_remaining, remaining_order_quantity)?;
                let component_remaining = component_remaining
                    .into_iter()
                    .map(|(component, quantity)| {
                        (
                            component,
                            if action.side == Side::Buy {
                                quantity
                            } else {
                                -quantity
                            },
                        )
                    })
                    .collect();
                let policy = self.fresh_mfce_policy_output_at(&asset, mfce_now).cloned();
                if !action.reduce_only
                    && !policy.as_ref().is_some_and(|output| {
                        output.admitted
                            && matches!(
                                output.policy_state,
                                crate::mfce::MfcePolicyState::Explore
                                    | crate::mfce::MfcePolicyState::Exploit
                            )
                    })
                {
                    self.action_lifecycle_events.push(ActionLifecycleEvent {
                        asset,
                        decision_id: action.decision_id.to_string(),
                        planned_cloid: action.planned_cloid.to_string(),
                        root_planned_cloid,
                        parent_planned_cloid,
                        retry_generation: action.retry_generation,
                        observed_at_mono: now,
                        requested_notional: action.rounded_notional,
                        filled_quantity: None,
                        unfilled_quantity: None,
                        outcome: ActionAttemptOutcome::BlockedByCurrentRisk,
                    });
                    continue;
                }
                let mfce_lineage = MfceExecutionLineage {
                    policy_state: policy.as_ref().map(|output| output.policy_state),
                    admitted: policy.as_ref().is_some_and(|output| output.admitted),
                    model_epoch: policy.as_ref().map_or(0, |output| output.model_epoch),
                    transition_id: policy.as_ref().map(|output| output.transition_id),
                    allocation_fraction: policy
                        .as_ref()
                        .map_or(Decimal::ZERO, |output| output.allocation_fraction),
                    risk_increase_authorized: !action.reduce_only,
                };
                self.pending.insert(
                    asset,
                    PendingAction {
                        action,
                        root_planned_cloid,
                        remaining_order_quantity,
                        component_remaining,
                        mfce_lineage,
                        execution: Some(execution_context(parent_planned_cloid, continuation_kind)),
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
        for (asset, (features, origin)) in learning_seeds {
            let Some(context) = target_contexts.get(&asset) else {
                continue;
            };
            let Some(rules) = execution_rules.get(&asset) else {
                continue;
            };
            let current = context.current_source_target;
            let requested = context.raw_desired_source_target;
            let selected = decision
                .projection
                .constrained_targets
                .get(&asset)
                .copied()
                .unwrap_or_default();
            let delta = selected - current;
            let output = self.fresh_mfce_policy_output_at(&asset, mfce_now);
            let provenance = match output.and_then(|o| o.reason) {
                Some(MfceRejectionReason::AllocationBudgetExhausted) => {
                    DecisionProvenance::AllocatorDisplaced
                }
                Some(MfceRejectionReason::TailBudgetExceeded) => DecisionProvenance::RiskLimited,
                Some(_) => DecisionProvenance::MfceRejected,
                None if selected != self.mfce.effective_target(&asset, current, requested) => {
                    DecisionProvenance::RiskLimited
                }
                None if self.pending.contains_key(&asset) => DecisionProvenance::Selected,
                _ => DecisionProvenance::Unexecuted,
            };
            let kind = if selected.is_zero() && !current.is_zero() {
                DecisionKind::Exit
            } else if !current.is_zero()
                && (selected.is_sign_positive() != current.is_sign_positive()
                    || selected.abs() < current.abs())
            {
                DecisionKind::Reduce
            } else if delta.is_zero() && provenance == DecisionProvenance::MfceRejected {
                DecisionKind::Reject
            } else if delta.is_zero()
                && matches!(
                    provenance,
                    DecisionProvenance::AllocatorDisplaced | DecisionProvenance::RiskLimited
                )
            {
                DecisionKind::BudgetConstrained
            } else if delta.is_zero() {
                DecisionKind::Hold
            } else if current.is_zero() {
                DecisionKind::Open
            } else {
                DecisionKind::Add
            };
            let continuation_retention =
                matches!(kind, DecisionKind::Reject | DecisionKind::BudgetConstrained)
                    && features.objective() == LearningObjective::ContinuationQuality;
            let retaining = matches!(
                kind,
                DecisionKind::Reduce | DecisionKind::Exit | DecisionKind::Hold
            ) || continuation_retention;
            let Some(direction) =
                MfceDirection::from_signed(if retaining { current } else { requested })
            else {
                continue;
            };
            let side = if (direction == MfceDirection::Long) != retaining {
                Side::Buy
            } else {
                Side::Sell
            };
            let quantity = if retaining {
                (if delta.is_zero() {
                    current.abs()
                } else {
                    delta.abs().min(current.abs())
                }) / rules.mark_price
            } else if !delta.is_zero() {
                delta.abs() / rules.mark_price
            } else {
                decision
                    .micro_slots
                    .get(&asset)
                    .map(|s| s.execution_floor.minimum_opening_notional)
                    .unwrap_or_default()
                    / rules.mark_price
            };
            let quantity = (quantity / rules.size_step).ceil() * rules.size_step;
            // Authoritative per-asset taker fee (single fee path).
            // Edge-only: use last-known book even when stale (warning only).
            let taker_fee_bps = taker_fee_bps_for_asset(&asset);
            let anchor = self
                .books
                .get(&asset)
                .and_then(|(book, at)| ioc_snapshot(book, *at).ok())
                .and_then(|book| {
                    executable_anchor(
                        &book,
                        side,
                        quantity,
                        rules,
                        taker_fee_bps? / BPS_PER_UNIT_RETURN,
                        execution_cushion,
                        maximum_slippage,
                    )
                });
            // Reuse the sovereign projector to bound a three-slice capacity
            // curve. Probes never borrow another candidate's allocation or
            // reserve live risk, and every point must walk observed L2.
            let larger_anchors = if matches!(
                kind,
                DecisionKind::Open | DecisionKind::Add | DecisionKind::Hold
            ) && anchor.is_some()
            {
                let mut probe = projection_input.clone();
                probe.unconstrained_targets = decision.projection.constrained_targets.clone();
                probe.unconstrained_targets.insert(asset.clone(), requested);
                project_and_validate_portfolio(&probe)
                    .ok()
                    .filter(|projection| {
                        decision
                            .projection
                            .constrained_targets
                            .iter()
                            .all(|(other, target)| {
                                other == &asset
                                    || projection.constrained_targets.get(other) == Some(target)
                            })
                    })
                    .and_then(|projection| projection.constrained_targets.get(&asset).copied())
                    .map(|target| {
                        let maximum =
                            ((target - current).abs() / rules.mark_price / rules.size_step).floor()
                                * rules.size_step;
                        let Some((book, at)) = self.books.get(&asset) else {
                            return Vec::new();
                        };
                        let Ok(book) = ioc_snapshot(book, *at) else {
                            return Vec::new();
                        };
                        let Some(fee) = taker_fee_bps else {
                            return Vec::new();
                        };
                        let span = maximum.saturating_sub(quantity);
                        let mut seen = BTreeSet::new();
                        (1..=3)
                            .filter_map(|slice| {
                                let candidate = if kind == DecisionKind::Hold {
                                    (maximum * Decimal::from(slice)
                                        / Decimal::from(3)
                                        / rules.size_step)
                                        .floor()
                                        * rules.size_step
                                } else {
                                    quantity
                                        + (span * Decimal::from(slice)
                                            / Decimal::from(3)
                                            / rules.size_step)
                                            .floor()
                                            * rules.size_step
                                };
                                if candidate <= Decimal::ZERO
                                    || (kind != DecisionKind::Hold && candidate <= quantity)
                                    || !seen.insert(candidate)
                                {
                                    return None;
                                }
                                let probe_side = if kind == DecisionKind::Hold {
                                    if direction == MfceDirection::Long {
                                        Side::Buy
                                    } else {
                                        Side::Sell
                                    }
                                } else {
                                    side
                                };
                                executable_anchor(
                                    &book,
                                    probe_side,
                                    candidate,
                                    rules,
                                    fee / BPS_PER_UNIT_RETURN,
                                    execution_cushion,
                                    maximum_slippage,
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            let selected_prediction_identity = remaining_edge_curves
                .get(&asset)
                .and_then(|curve| curve.first())
                .cloned();
            let prediction =
                output
                    .zip(selected_prediction_identity.as_ref())
                    .map(|(o, identity)| PredictionObservation {
                        predicted_q10_gross_edge: o.q10_gross_bps,
                        predicted_gross_edge: o.q50_gross_bps,
                        expected_friction: o.friction_bps,
                        predicted_q10_net_edge: o.net_q10_bps,
                        predicted_net_edge: o.net_q50_bps,
                        horizon_ms: identity.horizon_ms,
                        model_epoch: o.model_epoch,
                        objective: identity.objective,
                        feature_snapshot_id: identity.feature_snapshot_id,
                    });
            let mode = output.map(|o| o.policy_state);
            self.mfce.delayed_mut().capture(DecisionSample {
                id: 0,
                asset: asset.clone(),
                decision_at: mfce_now,
                kind,
                origin,
                provenance,
                mode,
                direction,
                features,
                prediction,
                proposed_delta: (requested - current) / rules.mark_price,
                actual_delta: Decimal::ZERO,
                reentry_after_exit: false,
                cloid: self
                    .pending
                    .get(&asset)
                    .map(|p| p.action.planned_cloid.to_string()),
                anchor,
                forward: Default::default(),
                pending_mask: 0,
                mfe_net_bps: None,
                mae_net_bps: None,
                time_to_mfe_ms: None,
                entry_regret_bps: Default::default(),
                exit_regret_bps: Default::default(),
                hold_regret_bps: Default::default(),
                reentry_regret_bps: Default::default(),
                cost_drag_bps: Default::default(),
                funding_credit: funding_rate_for(&metadata, &asset).map(|_| Decimal::ZERO),
                funding_start: mfce_now,
                actual_result: None,
                size_regret_bps: Default::default(),
                prediction_curve: remaining_edge_curves.remove(&asset).unwrap_or_default(),
                larger_anchors,
                size_regret_curve_bps: Default::default(),
                label_unavailable: Default::default(),
                component_unavailable: Default::default(),
                size_regret_unavailable: Default::default(),
            });
        }
        // Current justification may migrate without rewriting issuance origin
        // or creating an order. Only already-open economic episodes are updated.
        let provenance = self
            .ledger
            .portfolio_assets()
            .into_iter()
            .filter_map(|asset| {
                Some((
                    self.ledger.portfolio_open_episode_id(&asset)?,
                    self.ledger.portfolio_opened_at(&asset)?,
                    asset.clone(),
                    self.mfce.state().assets.get(&asset)?.target_origin?,
                ))
            })
            .collect::<Vec<_>>();
        for (id, opened_at, asset, origin) in provenance {
            self.mfce
                .delayed_mut()
                .observe_episode_origin(&id, &asset, opened_at, mfce_now, origin);
        }
        self.refresh_desired_books();

        self.strategy_targets_ready = true;
        Ok(Some(decision))
    }

    fn try_execute_pending(
        &mut self,
        book: &OrderBookResponse,
        received_at: Timestamp,
    ) -> Result<ExecutionRecompute, EngineError> {
        if self.production_identity.is_some() && !self.strategy_target_execution_enabled() {
            return Ok(ExecutionRecompute::None);
        }
        let Some(pending) = self.pending.get(&book.asset).cloned() else {
            return Ok(ExecutionRecompute::None);
        };
        let Some(context) = pending.execution.clone() else {
            return Ok(ExecutionRecompute::None);
        };
        let latency = self.config.latency_timeout_ms;
        if received_at
            < context
                .decision_book
                .observed_at_mono
                .saturating_add(latency)
        {
            return Ok(ExecutionRecompute::None);
        }
        if self.production_identity.is_some()
            && !pending.action.reduce_only
            && !self
                .fresh_mfce_policy_output_at(&book.asset, self.durable_timestamp(received_at)?)
                .is_some_and(|output| {
                    matches!(
                        output.policy_state,
                        crate::mfce::MfcePolicyState::Explore
                            | crate::mfce::MfcePolicyState::Exploit
                    )
                })
        {
            self.retire_pending_action(
                &book.asset,
                &pending,
                received_at,
                ActionAttemptOutcome::BlockedByCurrentRisk,
            );
            return Ok(ExecutionRecompute::None);
        }
        let evaluation = ioc_snapshot(book, received_at)?;
        let decision_midpoint = context.decision_book.midpoint;
        let reference_price = evaluation.midpoint;
        let cushion = Decimal::from_f64(self.config.slippage_buffer_bps / 10_000.0)
            .ok_or(EngineError::Arithmetic)?;
        let maximum_slippage = Decimal::from_f64(self.config.execution.max_slippage_bps / 10_000.0)
            .ok_or(EngineError::Arithmetic)?;
        let market_rules = MarketRules {
            mark_price: reference_price,
            price_tick: context.price_tick,
            size_step: context.size_step,
        };
        let floor_policy = ExecutionFloorPolicy {
            exchange_minimum_notional: Decimal::from_f64(
                self.config.global_risk.min_order_notional_usd,
            )
            .ok_or(EngineError::Arithmetic)?,
            rounding_buffer: Decimal::from_f64(self.config.global_risk.order_rounding_buffer_usd)
                .ok_or(EngineError::Arithmetic)?,
            closeability_margin: Decimal::from_f64(self.config.global_risk.closeability_margin_usd)
                .ok_or(EngineError::Arithmetic)?,
            maximum_slippage_fraction: maximum_slippage,
        };
        let (quantity, price_plan, required_notional) = if pending.action.reduce_only {
            let filled_quantity = self.authoritative_position(&book.asset);
            let filled_notional = filled_quantity
                .checked_mul(reference_price)
                .ok_or(EngineError::Arithmetic)?;
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
                Ok(exit) if exit.residual_class == ResidualClass::DirectionFlipCloseLeg => {
                    self.retire_pending_action(
                        &book.asset,
                        &pending,
                        received_at,
                        ActionAttemptOutcome::SupersededByNewTarget,
                    );
                    return Ok(ExecutionRecompute::None);
                }
                Ok(exit) => exit,
                Err(ExitPlanningBlock::AlreadySatisfied) => {
                    self.retire_pending_action(
                        &book.asset,
                        &pending,
                        received_at,
                        ActionAttemptOutcome::NoLongerRequired,
                    );
                    return Ok(ExecutionRecompute::None);
                }
                Err(ExitPlanningBlock::ExposureIncreasing) => {
                    self.retire_pending_action(
                        &book.asset,
                        &pending,
                        received_at,
                        ActionAttemptOutcome::SupersededByNewTarget,
                    );
                    return Ok(ExecutionRecompute::None);
                }
                Err(ExitPlanningBlock::BelowExchangeMinimum { .. }) => {
                    self.retire_pending_action(
                        &book.asset,
                        &pending,
                        received_at,
                        ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute,
                    );
                    return Ok(ExecutionRecompute::None);
                }
                Err(error) => {
                    return Err(EngineError::Core(format!(
                        "exit planning blocked: {error:?}"
                    )))
                }
            };
            if exit.side != pending.action.side {
                self.retire_pending_action(
                    &book.asset,
                    &pending,
                    received_at,
                    ActionAttemptOutcome::SupersededByNewTarget,
                );
                return Ok(ExecutionRecompute::None);
            }
            (
                exit.quantity.min(pending.remaining_order_quantity),
                exit.ioc,
                exit.execution_floor.minimum_order_notional,
            )
        } else {
            let quantity = pending.remaining_order_quantity;
            let floor = crate::domain::execution_floor::execution_floor_for_asset(
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
                context.price_tick,
            )
            .map_err(core)?;
            (quantity, price_plan, floor.minimum_order_notional)
        };
        if quantity <= Decimal::ZERO {
            return Err(EngineError::InvalidMarket(format!(
                "{} rounded to zero quantity",
                book.asset
            )));
        }
        if !validates_rounded_order(quantity, price_plan.limit_price, required_notional)
            .map_err(core)?
        {
            self.retire_pending_action(
                &book.asset,
                &pending,
                received_at,
                ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute,
            );
            return Ok(ExecutionRecompute::None);
        }
        if let Some(identity) = self.production_identity.clone() {
            if !self
                .emitted_production_cloids
                .contains(&pending.action.planned_cloid)
            {
                let position_quantity = self.authoritative_position(&book.asset);
                let expected_committed_before = position_quantity
                    .checked_mul(reference_price)
                    .ok_or(EngineError::Arithmetic)?;
                let signed_delta = quantity
                    .checked_mul(reference_price)
                    .map(|value| match pending.action.side {
                        Side::Buy => value,
                        Side::Sell => -value,
                    })
                    .ok_or(EngineError::Arithmetic)?;
                let expected_committed_after = expected_committed_before
                    .checked_add(signed_delta)
                    .ok_or(EngineError::Arithmetic)?;
                let mut projection_input = context.projection_input.clone();
                if let Some(rules) = projection_input.market_rules.get_mut(&book.asset) {
                    rules.mark_price = reference_price;
                }
                projection_input
                    .filled_positions
                    .insert(book.asset.clone(), expected_committed_before);
                projection_input.unconstrained_targets = self.target_ledger.admitted_targets();
                if !self.complete_execution_rules(&mut projection_input, &book.asset)? {
                    return Ok(ExecutionRecompute::None);
                }
                let projection = project_and_validate_portfolio(&projection_input)
                    .map_err(|error| EngineError::Core(error.to_string()))?;
                let projected_portfolio_hash = derive_projection_hash(&projection).map_err(core)?;
                // Single authoritative market -> execution asset-ID mapping.
                // Unknown/unresolved HIP-3 markets fail closed here before any
                // signed action can be emitted.
                let Some(asset_index) = self.execution_asset_id(&book.asset) else {
                    self.waiting_market_rules.insert(book.asset.clone());
                    return Ok(ExecutionRecompute::None);
                };
                // HIP-3 targets additionally require live collateral support;
                // never emit an intent the exchange would reject for margin.
                if hip3_is_market(&book.asset) {
                    let dex = dex_for_market(&book.asset);
                    if hip3_collateral_for_dex(dex, &self.dex_collateral).is_none() {
                        self.waiting_market_rules.insert(book.asset.clone());
                        return Ok(ExecutionRecompute::None);
                    }
                    if !self.is_production_execution_supported(&book.asset) {
                        self.waiting_market_rules.insert(book.asset.clone());
                        return Ok(ExecutionRecompute::None);
                    }
                }
                let root_cloid = parse_planned_cloid(&pending.root_planned_cloid)?;
                let parent_cloid = pending
                    .execution
                    .as_ref()
                    .and_then(|context| context.parent_planned_cloid.as_deref())
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
                    risk_policy_hash: context.risk_policy_hash,
                    configuration_hash: context.configuration_hash,
                    market_rules_hash: identity.market_rules_hash,
                    dynamic_floor_policy_hash: identity.dynamic_floor_policy_hash,
                    ioc_policy_hash: identity.ioc_policy_hash,
                    observer_release_hash: identity.observer_release_hash,
                    signer_release_hash: identity.signer_release_hash,
                    release_manifest_hash: identity.release_manifest_hash,
                    decision_reference_price: decision_midpoint,
                    expected_committed_before,
                    expected_committed_after,
                    decision_timestamp_ms: context.decision_book.observed_at_mono,
                    expires_at: received_at
                        .checked_add(identity.expires_after_ms)
                        .ok_or(EngineError::Arithmetic)?,
                    authorization: PreSigningContext {
                        now_mono: received_at,
                        deployed_risk_policy_hash: context.risk_policy_hash,
                        projection_input,
                        exchange_minimum_notional: required_notional,
                    },
                    canonical_hash: [0; 32],
                }
                .seal()
                .map_err(EngineError::Core)?;
                self.emitted_production_cloids
                    .insert(pending.action.planned_cloid);
                self.prepared_authorized_intents.push(intent);
            }
            return Ok(ExecutionRecompute::None);
        }
        #[cfg(test)]
        {
            self.accrue_funding(received_at)?;
            let pending_component_ids = pending
                .component_remaining
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let (equity_before, source_equity_before) =
                self.accounting_equities_with_sources(&pending_component_ids)?;
            let funding_attribution = self
                .accrued_funding
                .get(&book.asset)
                .copied()
                .unwrap_or_default();
            let raw_desired_notional = self
                .target_ledger
                .get(&book.asset)
                .map(|state| state.raw_desired_notional)
                .unwrap_or_default();
            let position_before = self.ledger.portfolio_position(&book.asset);
            let technical_desired_notional = self
                .technical_targets
                .get(&book.asset)
                .map(|target| target.desired_notional)
                .unwrap_or_default();
            let pending_reduce_only = pending.action.reduce_only;
            let mut execution = execute_fixture_ioc(&FixtureExecutionInput {
                action: pending.action,
                decision_timestamp_mono: context.decision_book.observed_at_mono,
                decision_market_snapshot: context.decision_book.clone(),
                evaluation_market_snapshot: evaluation,
                latency_scenario: LatencyScenario::Expected,
                configured_latency_ms: latency,
                proposed_limit_price: price_plan.limit_price,
                rounded_quantity: quantity,
                taker_fee_rate: self
                    .taker_fee_bps_for(&book.asset)
                    .and_then(|f| f.checked_div(BPS_PER_UNIT_RETURN))
                    .or_else(|| {
                        // Test-only fixture path predates live fee refreshes;
                        // keep the configured default as the last resort.
                        Decimal::from_f64(self.config.taker_fee_bps / 10_000.0)
                    })
                    .ok_or(EngineError::Arithmetic)?,
                funding_attribution,
                position_before,
            })
            .map_err(core)?;
            if !execution.modeled_filled_quantity.is_zero() {
                self.accrued_funding.remove(&book.asset);
            }
            let ledger_timestamp = self.durable_timestamp(received_at)?;
            self.ledger_time_high_watermark = self.ledger_time_high_watermark.max(ledger_timestamp);
            execution.decision_timestamp_mono = ledger_timestamp;
            // Consume the immutable per-component allocation carried by the
            // issued action. Current strategy targets decide only what to plan
            // after this action resolves.
            let component_positions_before = self.ledger.source_positions_for_asset(&book.asset);
            let component_partition = consume_pending_component_fill(
                &component_positions_before,
                execution.action.side,
                execution.modeled_filled_quantity,
                pending.remaining_order_quantity,
                &pending.component_remaining,
                context.size_step,
            )?;
            let allocations = &component_partition.total_quantities;
            self.apply_attributed_execution(
                &book.asset,
                &execution,
                ledger_timestamp,
                allocations,
            )?;
            if context.continuation_kind == Some(ContinuationKind::CloseFirstReversal)
                && !execution.modeled_filled_quantity.is_zero()
            {
                self.technical_delivery.addition_filled =
                    self.technical_delivery.addition_filled.saturating_add(1);
            }
            validate_source_reconciliation(
                &self.ledger,
                &book.asset,
                execution.position_after,
                context.size_step,
            )?;
            let (equity_after, source_equity_after) =
                self.accounting_equities_with_sources(&pending_component_ids)?;
            let deployment_after = self.deployment_equity()?;
            let net_pnl_delta = equity_after
                .checked_sub(equity_before)
                .ok_or(EngineError::Arithmetic)?;
            let gross_pnl_delta = net_pnl_delta
                .checked_add(execution.fees)
                .and_then(|value| value.checked_add(execution.funding))
                .ok_or(EngineError::Arithmetic)?;
            let portfolio_equity_return = if equity_before.is_zero() {
                Decimal::ZERO
            } else {
                net_pnl_delta
                    .checked_div(equity_before)
                    .ok_or(EngineError::Arithmetic)?
            };
            let mut source_attributed_returns = BTreeMap::new();
            for candidate in allocations.keys() {
                let before = source_equity_before
                    .get(candidate)
                    .copied()
                    .ok_or_else(|| EngineError::Core("missing source equity".into()))?;
                let after = source_equity_after
                    .get(candidate)
                    .copied()
                    .ok_or_else(|| EngineError::Core("missing source equity".into()))?;
                let change = after.checked_sub(before).ok_or(EngineError::Arithmetic)?;
                source_attributed_returns.insert(
                    candidate.clone(),
                    if before.is_zero() {
                        Decimal::ZERO
                    } else {
                        change.checked_div(before).ok_or(EngineError::Arithmetic)?
                    },
                );
            }
            self.executions.push(SettledActionAccounting {
                execution_id: execution.execution_id.to_string(),
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
                parent_planned_cloid: context.parent_planned_cloid.clone(),
                retry_generation: execution.action.retry_generation,
                decision_timestamp_mono: execution.decision_timestamp_mono,
                evaluation_timestamp_mono: ledger_timestamp,
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
                component_close_quantities: component_partition.close_quantities,
                component_open_quantities: component_partition.open_quantities,
                source_filled_quantities: component_partition.total_quantities,
                economic_attribution: context.economic_attribution,
                mfce_lineage: pending.mfce_lineage.clone(),
            });
            self.pending.remove(&book.asset);
            let partial_remainder = !execution.unfilled_ioc_remainder.is_zero();
            let close_first_completed = completed_close_first_reversal(
                pending_reduce_only,
                raw_desired_notional,
                technical_desired_notional,
                execution.position_before,
                execution.position_after,
                execution.modeled_filled_quantity,
                execution.unfilled_ioc_remainder,
            );
            let execution_recompute = if partial_remainder {
                ExecutionRecompute::PartialIocRemainder
            } else if close_first_completed {
                ExecutionRecompute::CloseFirstReversal
            } else {
                ExecutionRecompute::None
            };
            self.action_lifecycle_events.push(ActionLifecycleEvent {
                asset: book.asset.clone(),
                decision_id: execution.action.decision_id.to_string(),
                planned_cloid: execution.action.planned_cloid.to_string(),
                root_planned_cloid: pending.root_planned_cloid.clone(),
                parent_planned_cloid: context.parent_planned_cloid.clone(),
                retry_generation: execution.action.retry_generation,
                observed_at_mono: received_at,
                requested_notional: execution.action.rounded_notional,
                filled_quantity: Some(execution.modeled_filled_quantity),
                unfilled_quantity: Some(execution.unfilled_ioc_remainder),
                outcome: if partial_remainder {
                    ActionAttemptOutcome::ExecutedPartiallyRemainderReplanned
                } else if close_first_completed {
                    ActionAttemptOutcome::CloseFirstCompletedRecompute
                } else {
                    ActionAttemptOutcome::ExecutedFully
                },
            });
            if execution_recompute.is_required() {
                let next_retry_generation = execution
                    .action
                    .retry_generation
                    .checked_add(1)
                    .ok_or(EngineError::Arithmetic)?;
                self.continuations.insert(
                    book.asset.clone(),
                    ContinuationIntent {
                        root_planned_cloid: pending.root_planned_cloid,
                        parent_planned_cloid: execution.action.planned_cloid.to_string(),
                        next_retry_generation,
                        kind: match execution_recompute {
                            ExecutionRecompute::PartialIocRemainder => {
                                ContinuationKind::PartialIocRemainder
                            }
                            ExecutionRecompute::CloseFirstReversal => {
                                ContinuationKind::CloseFirstReversal
                            }
                            ExecutionRecompute::None => unreachable!(),
                        },
                    },
                );
            }
            if close_first_completed {
                self.technical_delivery.close_first_completed = self
                    .technical_delivery
                    .close_first_completed
                    .saturating_add(1);
            }
            if let Some(price) = execution.modeled_average_fill_price {
                self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(ledger_timestamp);
                self.mfce.delayed_mut().fill(
                    &book.asset,
                    &execution.action.planned_cloid.to_string(),
                    ledger_timestamp,
                    if execution.action.side == Side::Buy {
                        execution.modeled_filled_quantity
                    } else {
                        -execution.modeled_filled_quantity
                    },
                    price,
                    execution.fees,
                    position_before,
                    context.decision_book.midpoint,
                    RealizedOutcome {
                        gross_pnl: gross_pnl_delta,
                        fees: execution.fees,
                        slippage: execution.slippage,
                        funding: -execution.funding,
                        net_pnl: net_pnl_delta,
                    },
                );
            }
            self.metrics.executions += 1;
            self.refresh_desired_books();
            return Ok(execution_recompute);
        }
        #[cfg(not(test))]
        Err(EngineError::Core(
            "production execution intent path is not initialized".into(),
        ))
    }

    fn apply_attributed_execution(
        &mut self,
        asset: &str,
        execution: &crate::domain::ioc::ExecutionFill,
        ledger_timestamp: Timestamp,
        allocations: &BTreeMap<String, Decimal>,
    ) -> Result<(), EngineError> {
        let attributed_quantity = allocations
            .values()
            .try_fold(Decimal::ZERO, |sum, quantity| sum.checked_add(*quantity))
            .ok_or(EngineError::Arithmetic)?;
        if attributed_quantity != execution.modeled_filled_quantity {
            return Err(EngineError::Core(format!(
                "execution attribution quantity {} does not equal portfolio fill {}",
                attributed_quantity, execution.modeled_filled_quantity
            )));
        }
        let previous_episode = self.ledger.portfolio_open_episode_id(asset);
        let closed_portfolio_episode = self
            .ledger
            .apply_portfolio_execution(execution, ledger_timestamp)
            .map_err(core)?
            .cloned();
        let current_episode = self.ledger.portfolio_open_episode_id(asset);
        self.mfce.delayed_mut().position_fill(
            asset,
            previous_episode.as_deref(),
            current_episode.as_deref(),
            &execution.action.planned_cloid.to_string(),
            ledger_timestamp,
        );
        if allocations
            .iter()
            .any(|(candidate, quantity)| candidate.starts_with("technical:") && !quantity.is_zero())
        {
            self.technical_delivery.fills = self.technical_delivery.fills.saturating_add(1);
        }
        let mut closed_attributions = Vec::new();
        for (candidate, quantity) in allocations {
            let source_execution = scaled_source_execution(
                execution,
                *quantity,
                self.ledger.source_position(candidate, asset),
            )?;
            if let Some(closed) = self
                .ledger
                .apply_source_execution(candidate, &source_execution, ledger_timestamp)
                .map_err(core)?
            {
                closed_attributions.push(closed);
            }
        }
        if let Some(portfolio_episode) = &closed_portfolio_episode {
            let mut attributed_gross = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_gross_pnl)
                })
                .ok_or(EngineError::Arithmetic)?;
            let mut attributed_fees = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_fees)
                })
                .ok_or(EngineError::Arithmetic)?;
            let mut attributed_funding = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_funding)
                })
                .ok_or(EngineError::Arithmetic)?;
            let mut attributed_slippage = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_slippage)
                })
                .ok_or(EngineError::Arithmetic)?;
            let gross_residual = portfolio_episode
                .realized_pnl
                .checked_sub(attributed_gross)
                .ok_or(EngineError::Arithmetic)?;
            let fees_residual = portfolio_episode
                .fees
                .checked_sub(attributed_fees)
                .ok_or(EngineError::Arithmetic)?;
            let funding_residual = portfolio_episode
                .funding
                .checked_sub(attributed_funding)
                .ok_or(EngineError::Arithmetic)?;
            let slippage_residual = portfolio_episode
                .slippage
                .checked_sub(attributed_slippage)
                .ok_or(EngineError::Arithmetic)?;
            if [
                gross_residual,
                fees_residual,
                funding_residual,
                slippage_residual,
            ]
            .iter()
            .any(|residual| !residual.is_zero())
            {
                let (anchor_index, anchor) = closed_attributions
                    .iter()
                    .enumerate()
                    .max_by(|(_, left), (_, right)| left.candidate_id.cmp(&right.candidate_id))
                    .ok_or_else(|| {
                        EngineError::Core(
                            "portfolio episode closed without a component attribution".into(),
                        )
                    })?;
                let corrected = self
                    .ledger
                    .assign_source_episode_economic_residuals(
                        &anchor.candidate_id,
                        anchor.source_episode_id,
                        gross_residual,
                        fees_residual,
                        funding_residual,
                        slippage_residual,
                    )
                    .map_err(core)?;
                closed_attributions[anchor_index] = corrected;
                attributed_gross = portfolio_episode.realized_pnl;
                attributed_fees = portfolio_episode.fees;
                attributed_funding = portfolio_episode.funding;
                attributed_slippage = portfolio_episode.slippage;
            }
            for (scope, portfolio, attributed) in [
                ("gross", portfolio_episode.realized_pnl, attributed_gross),
                ("fees", portfolio_episode.fees, attributed_fees),
                ("funding", portfolio_episode.funding, attributed_funding),
                ("slippage", portfolio_episode.slippage, attributed_slippage),
            ] {
                if portfolio != attributed {
                    return Err(EngineError::Core(format!(
                        "accounting invariant failure for portfolio episode {:?}: portfolio {scope} {portfolio} != attributed {scope} {attributed}",
                        portfolio_episode.episode_id
                    )));
                }
            }
            let mut attributed_net = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_net_pnl)
                })
                .ok_or(EngineError::Arithmetic)?;
            if attributed_net != portfolio_episode.net_pnl {
                let residual = portfolio_episode
                    .net_pnl
                    .checked_sub(attributed_net)
                    .ok_or(EngineError::Arithmetic)?;
                let (anchor_index, anchor) = closed_attributions
                    .iter()
                    .enumerate()
                    .max_by(|(_, left), (_, right)| left.candidate_id.cmp(&right.candidate_id))
                    .ok_or_else(|| {
                        EngineError::Core(
                            "accounting invariant failure: closed portfolio episode has no attribution anchor"
                                .into(),
                        )
                    })?;
                let corrected = self
                    .ledger
                    .assign_source_attribution_residual(
                        &anchor.candidate_id,
                        anchor.source_episode_id,
                        residual,
                    )
                    .map_err(core)?;
                closed_attributions[anchor_index] = corrected;
                attributed_net = closed_attributions
                    .iter()
                    .try_fold(Decimal::ZERO, |sum, episode| {
                        sum.checked_add(episode.modeled_net_pnl)
                    })
                    .ok_or(EngineError::Arithmetic)?;
                if attributed_net != portfolio_episode.net_pnl {
                    return Err(EngineError::Core(format!(
                        "accounting invariant failure for portfolio episode {:?}: deterministic residual assignment did not reconcile {} != {}",
                        portfolio_episode.episode_id,
                        portfolio_episode.net_pnl,
                        attributed_net
                    )));
                }
            }
        }
        Ok(())
    }
    pub fn persist(&mut self, path: impl AsRef<std::path::Path>) -> Result<(), EngineError> {
        self.ledger.save_atomic(path).map_err(|error| {
            self.metrics.persistence_failures += 1;
            core(error)
        })
    }
    pub fn persist_target_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(), EngineError> {
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
    ) -> Result<(), EngineError> {
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

    pub fn persist_unsigned_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
        identity: &StateIdentity,
    ) -> Result<u64, EngineError> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| EngineError::Core("state path has no parent".into()))?;
        std::fs::create_dir_all(parent).map_err(|error| {
            self.metrics.persistence_failures += 1;
            EngineError::Core(error.to_string())
        })?;
        let payload = normalize_unsigned_snapshot_payload(UnsignedObserverState {
            ledger: self.ledger.clone(),
            target_ledger: self.target_ledger.clone(),
            technical_engine: self.technical_engine.clone(),
            technical_decision_state: self.technical_decision_state.clone(),
            very_profitable_engine: self.very_profitable_engine.clone(),
            very_profitable_layer_artifact_sha256: self
                .very_profitable_layer
                .as_ref()
                .map(|layer| layer.artifact_sha256.clone()),
            decision_sequence: self.decision_sequence,
            pending: self.pending.clone(),
            continuations: self.continuations.clone(),
            accrued_funding: self.accrued_funding.clone(),
            applied_live_funding: self.applied_live_funding.clone(),
            last_mids: self.mids.as_ref().map(|(mids, _)| mids.clone()),
            legacy_history_complete_through_ms: None,
            mfce: self.mfce.state().clone(),
            mfce_time_high_watermark: self.mfce_time_high_watermark,
            ledger_time_high_watermark: self.ledger_time_high_watermark,
            executions: self.executions.clone(),
            equity_buckets: self.equity_buckets.clone(),
            last_bucket: self.last_bucket.clone(),
            micro_density: self.micro_density.clone(),
            waiting_market_rules: self.waiting_market_rules.clone(),
        })
        .map_err(|error| {
            self.metrics.persistence_failures += 1;
            error
        })?;
        let generation = match self.snapshot_generation {
            Some(previous) => previous.checked_add(1).ok_or(EngineError::Arithmetic)?,
            None => 0,
        };
        let checksum_sha256 = unsigned_snapshot_checksum(
            UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            generation,
            identity,
            &payload,
        )
        .map_err(|error| {
            self.metrics.persistence_failures += 1;
            error
        })?;
        let envelope = UnsignedSnapshotEnvelope {
            schema_version: UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            generation,
            identity: identity.clone(),
            payload,
            checksum_sha256,
        };
        let bytes = encode_unsigned_snapshot(&envelope).map_err(|error| {
            self.metrics.persistence_failures += 1;
            error
        })?;
        let temporary = path.with_extension("tmp");
        let result = (|| -> Result<(), std::io::Error> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            std::fs::File::open(parent)?.sync_all()
        })();
        result.map_err(|error| {
            self.metrics.persistence_failures += 1;
            EngineError::Core(error.to_string())
        })?;
        self.snapshot_generation = Some(generation);
        Ok(generation)
    }

    pub fn restore_unsigned_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
        expected_identity: &StateIdentity,
    ) -> Result<u64, EngineError> {
        let bytes = std::fs::read(path).map_err(core)?;
        let envelope = decode_unsigned_snapshot(&bytes, expected_identity)?;
        let mut state = envelope.payload;
        state.ledger.validate_integrity().map_err(core)?;
        if state.ledger.migrate_cash_accounting().map_err(core)? {
            for execution in &mut state.executions {
                execution.net_pnl_delta += execution.execution_slippage;
            }
            state
                .mfce
                .delayed
                .migrate_actual_cash()
                .ok_or(EngineError::Arithmetic)?;
        }
        state.target_ledger.validate_integrity().map_err(core)?;
        // Normalize legacy feature/model state before validating the complete
        // payload. This is still pre-mutation: a failed migration cannot alter
        // the live engine being restored into.
        state.mfce = MfceEngine::from_state(state.mfce)
            .map_err(mfce_error)?
            .state()
            .clone();
        for pending in state.pending.values_mut() {
            if pending.remaining_order_quantity <= Decimal::ZERO {
                return Err(EngineError::Core(
                    "pending action has invalid remaining order quantity".into(),
                ));
            }
            // Schema-v12 snapshots may contain a zero residual emitted by the
            // former stable-anchor canonicalizer. The envelope checksum is
            // verified before this normalization; dropping an exact zero is
            // semantics-preserving and leaves every material attribution and
            // order quantity unchanged.
            pending
                .component_remaining
                .retain(|_, quantity| !quantity.is_zero());
            pending_component_total(pending.action.side, &pending.component_remaining)?;
        }
        if state.mfce_time_high_watermark < state.mfce.time_high_watermark() {
            return Err(EngineError::Core(
                "unsigned engine MFCE time high-watermark regressed".into(),
            ));
        }
        if state.technical_engine.configuration() != &self.config.technical {
            return Err(EngineError::Core(
                "unsigned engine technical configuration mismatch".into(),
            ));
        }
        let installed_layer_hash = self
            .very_profitable_layer
            .as_ref()
            .map(|layer| layer.artifact_sha256.as_str());
        if state.very_profitable_layer_artifact_sha256.as_deref() != installed_layer_hash {
            return Err(EngineError::Core(
                "unsigned engine cohort-layer identity mismatch".into(),
            ));
        }
        self.ledger = state.ledger;
        self.strategy_targets_ready = true;
        self.target_ledger = state.target_ledger;
        self.technical_engine = state.technical_engine;
        self.technical_engine.reset_funnel();
        self.technical_decision_state = state.technical_decision_state;
        self.very_profitable_engine = state.very_profitable_engine;
        self.decision_sequence = state.decision_sequence;
        self.pending = state.pending;
        self.waiting_market_rules = state.waiting_market_rules;
        self.continuations = state.continuations;
        self.accrued_funding = state.accrued_funding;
        self.applied_live_funding = state.applied_live_funding;
        self.mids = state.last_mids.map(|mids| (mids, 0));
        self.restore_mfce_persistent_state(state.mfce, state.mfce_time_high_watermark)?;
        self.ledger_time_high_watermark = state.ledger_time_high_watermark;
        self.executions = state.executions;
        self.equity_buckets = state.equity_buckets;
        self.last_bucket = state.last_bucket;
        self.micro_density = state.micro_density;
        self.snapshot_generation = Some(envelope.generation);
        self.last_funding_accrual = None;
        self.previous_target = self
            .target_ledger
            .latest_target_version()
            .map(|version| {
                Ok(PreviousTargetState {
                    version,
                    target_hash: canonical_target_hash(&self.target_ledger.admitted_targets())
                        .map_err(core)?,
                })
            })
            .transpose()?;
        self.pending_book.clear();
        self.technical_targets.clear();
        self.technical_decision_records.clear();
        self.refresh_desired_books();
        Ok(envelope.generation)
    }

    pub fn snapshot_generation(&self) -> Option<u64> {
        self.snapshot_generation
    }

    pub fn metrics(&self) -> &EngineMetrics {
        &self.metrics
    }

    pub fn unresolved_actionable_root_count(&self) -> usize {
        self.pending
            .values()
            .map(|pending| pending.root_planned_cloid.as_str())
            .chain(
                self.pending_book
                    .values()
                    .map(|pending| pending.original_planned_cloid.as_str()),
            )
            .chain(
                self.continuations
                    .values()
                    .map(|pending| pending.root_planned_cloid.as_str()),
            )
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn unresolved_actionable_roots(&self) -> Vec<UnresolvedRootStatus> {
        let mut roots = Vec::new();
        for (asset, pending) in &self.pending {
            roots.push(UnresolvedRootStatus {
                asset: asset.clone(),
                root_planned_cloid: pending.root_planned_cloid.clone(),
                lifecycle: if self.waiting_market_rules.contains(asset) {
                    "waiting_for_market_rules"
                } else {
                    "pending_action"
                },
                planned_cloid: Some(pending.action.planned_cloid.to_string()),
                parent_planned_cloid: pending
                    .execution
                    .as_ref()
                    .and_then(|context| context.parent_planned_cloid.clone()),
                decision_id: Some(pending.action.decision_id.to_string()),
                retry_generation: Some(pending.action.retry_generation),
            });
        }
        for (asset, pending) in &self.pending_book {
            roots.push(UnresolvedRootStatus {
                asset: asset.clone(),
                root_planned_cloid: pending.original_planned_cloid.clone(),
                lifecycle: "pending_live_book",
                planned_cloid: Some(pending.original_planned_cloid.clone()),
                parent_planned_cloid: None,
                decision_id: Some(pending.original_decision_id.clone()),
                retry_generation: None,
            });
        }
        for (asset, continuation) in &self.continuations {
            roots.push(UnresolvedRootStatus {
                asset: asset.clone(),
                root_planned_cloid: continuation.root_planned_cloid.clone(),
                lifecycle: match continuation.kind {
                    ContinuationKind::PartialIocRemainder => "partial_ioc_remainder",
                    ContinuationKind::CloseFirstReversal => "close_first_reversal",
                },
                planned_cloid: None,
                parent_planned_cloid: Some(continuation.parent_planned_cloid.clone()),
                decision_id: None,
                retry_generation: Some(continuation.next_retry_generation),
            });
        }
        roots.sort_by(|left, right| {
            left.root_planned_cloid
                .cmp(&right.root_planned_cloid)
                .then_with(|| left.asset.cmp(&right.asset))
                .then_with(|| left.lifecycle.cmp(right.lifecycle))
        });
        roots
    }

    pub fn mfce_report(&self) -> MfceReport {
        self.mfce.report()
    }

    pub fn fresh_mfce_report_at(&self, durable_now: Timestamp) -> MfceReport {
        let mut report = self.mfce.report();
        let maximum_age = self.config.global_risk.source_snapshot_max_age_ms;
        report.policy_outputs.retain(|output| {
            output.signal_observed_at != 0
                && output.signal_observed_at <= durable_now
                && durable_now.saturating_sub(output.signal_observed_at) <= maximum_age
        });
        report
    }

    /// Keep exact economic records for the rolling 30-day horizon and only a
    /// small recent execution/lifecycle tail. Repeated planning and indicator
    /// evaluations are working data, not durable production evidence.
    pub fn compact_runtime_history(&mut self, cutoff: Timestamp) -> Result<(), EngineError> {
        self.ledger.compact_closed_before(cutoff).map_err(core)?;
        self.mfce.delayed_mut().compact_provenance_before(cutoff);
        self.executions
            .retain(|record| record.evaluation_timestamp_mono >= cutoff);
        self.equity_buckets
            .retain(|record| record.closed_at_mono >= cutoff);
        if self.plans.len() > 2 {
            self.plans.drain(..self.plans.len() - 2);
        }
        if self.book_evaluation_events.len() > 1_000 {
            self.book_evaluation_events
                .drain(..self.book_evaluation_events.len() - 1_000);
        }
        if self.action_lifecycle_events.len() > 1_000 {
            self.action_lifecycle_events
                .drain(..self.action_lifecycle_events.len() - 1_000);
        }
        self.technical_decision_records.clear();
        self.cohort_indicator_records.clear();
        Ok(())
    }

    pub fn executions(&self) -> &[SettledActionAccounting] {
        &self.executions
    }

    #[cfg(test)]
    pub(crate) fn persistence_semantic_fingerprint(&self) -> [u8; 32] {
        let bytes = rmp_serde::to_vec(&(
            &self.ledger,
            &self.target_ledger,
            self.decision_sequence,
            &self.pending,
            &self.continuations,
            &self.accrued_funding,
            &self.applied_live_funding,
            &self.executions,
            self.mfce.state(),
        ))
        .expect("persistence semantic state is serializable");
        Sha256::digest(bytes).into()
    }

    pub fn equity_buckets(&self) -> &[EquityReturnBucket] {
        &self.equity_buckets
    }

    pub fn plans(&self) -> &[DecisionPlanningAccounting] {
        &self.plans
    }

    pub fn compact_decision_plan_summary(&self) -> DecisionPlanCompactSummary {
        let pair_changes =
            |field: fn(&DecisionPlanningAccounting) -> &BTreeMap<String, Decimal>| {
                self.plans
                    .windows(2)
                    .filter(|pair| field(&pair[0]) != field(&pair[1]))
                    .count()
                    .saturating_add(usize::from(!self.plans.is_empty()))
            };
        DecisionPlanCompactSummary {
            observed_plan_count: self.plans.len(),
            raw_target_change_count: pair_changes(|plan| &plan.raw_desired_targets),
            constrained_target_change_count: pair_changes(|plan| &plan.constrained_targets),
            proposed_action_change_count: pair_changes(|plan| &plan.proposed_action_notionals),
            executable_action_change_count: pair_changes(|plan| &plan.executable_action_notionals),
            below_minimum_action_count: self
                .plans
                .iter()
                .map(|plan| plan.below_minimum_action_count)
                .sum(),
            first_decision_id: self.plans.first().map(|plan| plan.decision_id.clone()),
            last_decision_id: self.plans.last().map(|plan| plan.decision_id.clone()),
        }
    }

    /// Retain exact material cohort decisions without retaining one repeated
    /// evaluation record per scheduler tick. A failed window keeps the raw
    /// replay journal, which remains the complete diagnostic source.
    pub fn compact_cohort_decision_records(&self) -> Vec<&CohortIndicatorRecord> {
        let mut material = BTreeMap::<(String, u64), &CohortIndicatorRecord>::new();
        let mut rejection_classes =
            BTreeMap::<(String, BTreeSet<CohortDecisionReason>), &CohortIndicatorRecord>::new();
        for record in &self.cohort_indicator_records {
            if record.independent_execution_root
                || !record.very_profitable_cohort_target.is_zero()
                || record.target_version > 0
            {
                material.insert((record.asset.clone(), record.target_version), record);
            } else {
                rejection_classes.insert((record.asset.clone(), record.reasons.clone()), record);
            }
        }
        let mut compact = material
            .into_values()
            .chain(rejection_classes.into_values())
            .collect::<Vec<_>>();
        compact.sort_by(|left, right| {
            left.cohort_snapshot_timestamp_ms
                .cmp(&right.cohort_snapshot_timestamp_ms)
                .then_with(|| left.asset.cmp(&right.asset))
                .then_with(|| left.target_version.cmp(&right.target_version))
        });
        compact
    }

    pub fn compact_cohort_decision_summary(&self) -> CohortDecisionCompactSummary {
        let mut reason_occurrence_counts = BTreeMap::new();
        let mut attribution_occurrence_counts = BTreeMap::new();
        for record in &self.cohort_indicator_records {
            for reason in &record.reasons {
                *reason_occurrence_counts.entry(*reason).or_default() += 1;
            }
            for attribution in &record.attribution {
                *attribution_occurrence_counts
                    .entry(*attribution)
                    .or_default() += 1;
            }
        }
        CohortDecisionCompactSummary {
            observed_record_count: self.cohort_indicator_records.len(),
            independent_execution_root_count: self
                .cohort_indicator_records
                .iter()
                .filter(|record| record.independent_execution_root)
                .count(),
            reason_occurrence_counts,
            attribution_occurrence_counts,
        }
    }

    pub fn cohort_indicator_records(&self) -> &[CohortIndicatorRecord] {
        &self.cohort_indicator_records
    }

    pub fn technical_decision_records(&self) -> &[TechnicalDecisionRecord] {
        &self.technical_decision_records
    }

    pub fn book_evaluation_events(&self) -> &[BookEvaluationEvent] {
        &self.book_evaluation_events
    }

    pub fn action_lifecycle_events(&self) -> &[ActionLifecycleEvent] {
        &self.action_lifecycle_events
    }

    pub fn executable_density_summary(&self) -> Result<ExecutableDensitySummary, EngineError> {
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
                ActionAttemptOutcome::ExecutedPartiallyRemainderReplanned
                | ActionAttemptOutcome::CloseFirstCompletedRecompute => None,
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
        let starting =
            Decimal::from_f64(self.config.starting_equity_usd).ok_or(EngineError::Arithmetic)?;
        let net_pnl = equity
            .checked_sub(starting)
            .ok_or(EngineError::Arithmetic)?;
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
                .ok_or(EngineError::Arithmetic)?
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
                .ok_or(EngineError::Arithmetic)?
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
            technical_funnel: self.technical_engine.funnel().clone(),
            technical_targets_admitted_sparse: self.technical_delivery.targets_admitted_sparse,
            technical_roots_created: self.technical_delivery.roots.len(),
            technical_fills: self.technical_delivery.fills,
            technical_episode_attributions: self
                .ledger
                .all_source_closed()
                .iter()
                .filter(|episode| episode.candidate_id.starts_with("technical:"))
                .count(),
            close_first_staged: self.technical_delivery.close_first_staged,
            close_first_completed: self.technical_delivery.close_first_completed,
            post_reduction_recomputed: self.technical_delivery.post_reduction_recomputed,
            technical_addition_emitted: self.technical_delivery.addition_emitted,
            technical_addition_filled: self.technical_delivery.addition_filled,
            source_risk_increases_suppressed_below_cost_edge: self
                .micro_density
                .source_risk_increases_suppressed_below_cost_edge,
        })
    }

    fn record_micro_density(&mut self, decision: &DecisionRecord) -> Result<(), EngineError> {
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
                    .ok_or(EngineError::Arithmetic)?;
                let required = if slot.filled_notional.is_zero() {
                    slot.execution_floor.minimum_opening_notional
                } else {
                    slot.execution_floor.minimum_order_notional
                };
                if slot
                    .desired_notional
                    .checked_sub(slot.filled_notional)
                    .ok_or(EngineError::Arithmetic)?
                    .abs()
                    >= required
                {
                    self.micro_density.target_changes_above_dynamic_minimum = self
                        .micro_density
                        .target_changes_above_dynamic_minimum
                        .checked_add(1)
                        .ok_or(EngineError::Arithmetic)?;
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
                    .ok_or(EngineError::Arithmetic)?;
            }
            if !previous_admitted.is_zero() && slot.admitted_notional.is_zero() {
                self.micro_density.exits = self
                    .micro_density
                    .exits
                    .checked_add(1)
                    .ok_or(EngineError::Arithmetic)?;
                if !slot.desired_notional.is_zero() {
                    self.micro_density.rotations = self
                        .micro_density
                        .rotations
                        .checked_add(1)
                        .ok_or(EngineError::Arithmetic)?;
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

    fn retire_pending_action(
        &mut self,
        asset: &str,
        pending: &PendingAction,
        observed_at_mono: Timestamp,
        outcome: ActionAttemptOutcome,
    ) {
        self.pending.remove(asset);
        self.action_lifecycle_events.push(ActionLifecycleEvent {
            asset: asset.to_string(),
            decision_id: pending.action.decision_id.to_string(),
            planned_cloid: pending.action.planned_cloid.to_string(),
            root_planned_cloid: pending.root_planned_cloid.clone(),
            parent_planned_cloid: pending
                .execution
                .as_ref()
                .and_then(|context| context.parent_planned_cloid.clone()),
            retry_generation: pending.action.retry_generation,
            observed_at_mono,
            requested_notional: pending.action.rounded_notional,
            filled_quantity: None,
            unfilled_quantity: None,
            outcome,
        });
        self.refresh_desired_books();
    }

    fn refresh_desired_books(&mut self) {
        self.desired_books = self
            .pending
            .keys()
            .chain(self.pending_book.keys())
            .chain(self.continuations.keys())
            .chain(self.mfce.awaiting_book_assets())
            .cloned()
            .collect();
        self.metrics.unreconciled_intents =
            (self.pending.len() + self.pending_book.len() + self.continuations.len()) as u64;
    }

    pub fn record_equity_boundary(&mut self, now: Timestamp) -> Result<(), EngineError> {
        self.accrue_funding(now)?;
        let (equity, source_equities) = self.accounting_equities()?;
        let durable_now = self.durable_timestamp(now)?;
        self.record_equity_values(durable_now, equity, source_equities)
    }

    fn accounting_equities(&self) -> Result<(Decimal, BTreeMap<String, Decimal>), EngineError> {
        self.accounting_equities_with_sources(&BTreeSet::new())
    }

    fn accounting_equities_with_sources(
        &self,
        additional_sources: &BTreeSet<String>,
    ) -> Result<(Decimal, BTreeMap<String, Decimal>), EngineError> {
        let marks = self
            .mids
            .as_ref()
            .map(|(snapshot, _)| snapshot.mids.clone())
            .unwrap_or_default();
        let starting =
            Decimal::from_f64(self.config.starting_equity_usd).ok_or(EngineError::Arithmetic)?;
        let unbooked_funding = self
            .accrued_funding
            .values()
            .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))
            .ok_or(EngineError::Arithmetic)?;
        let equity = self
            .ledger
            .portfolio_equity(starting, &marks, unbooked_funding)
            .map_err(core)?;
        let source_ids = self
            .config
            .candidates
            .iter()
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .chain(self.ledger.source_ids())
            .chain(additional_sources.iter().cloned())
            .collect::<BTreeSet<_>>();
        let mut source_equities = BTreeMap::new();
        for id in source_ids {
            source_equities.insert(
                id.clone(),
                self.ledger
                    .source_equity(&id, starting, &marks)
                    .map_err(core)?,
            );
        }
        Ok((equity, source_equities))
    }

    pub fn deployment_equity(&self) -> Result<DeploymentEquity, EngineError> {
        if let Some((current, settled, deployment)) = self.production_equities {
            let starting = Decimal::from_f64(self.config.starting_equity_usd)
                .ok_or(EngineError::Arithmetic)?;
            let realized = settled
                .checked_sub(starting)
                .ok_or(EngineError::Arithmetic)?;
            let mut result =
                calculate_deployment_equity(starting, current, realized).map_err(core)?;
            if result.deployment_equity != deployment {
                return Err(EngineError::Core(
                    "production deployment equity mismatch".into(),
                ));
            }
            result.deployment_equity = deployment;
            return Ok(result);
        }
        let (current, _) = self.accounting_equities()?;
        let starting =
            Decimal::from_f64(self.config.starting_equity_usd).ok_or(EngineError::Arithmetic)?;
        let realized = self.ledger.portfolio_realized_net_pnl().map_err(core)?;
        calculate_deployment_equity(starting, current, realized).map_err(core)
    }

    fn record_equity_values(
        &mut self,
        now: Timestamp,
        equity: Decimal,
        source_equities: BTreeMap<String, Decimal>,
    ) -> Result<(), EngineError> {
        let source_starting_equity =
            Decimal::from_f64(self.config.starting_equity_usd).ok_or(EngineError::Arithmetic)?;
        if let Some((opened_at, previous, previous_sources)) = &self.last_bucket {
            if now == *opened_at {
                return Err(EngineError::DuplicateBucket);
            }
            if now < *opened_at {
                return Err(EngineError::InvalidBucketInterval);
            }
            let return_fraction = equity
                .checked_sub(*previous)
                .and_then(|change| change.checked_div(*previous))
                .ok_or(EngineError::Arithmetic)?;
            let source_returns = source_equities
                .iter()
                .map(|(candidate, value)| {
                    let previous = previous_sources
                        .get(candidate)
                        .copied()
                        .unwrap_or(source_starting_equity);
                    Ok((
                        candidate.clone(),
                        value
                            .checked_sub(previous)
                            .and_then(|change| change.checked_div(previous))
                            .ok_or(EngineError::Arithmetic)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?;
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

    fn accrue_funding(&mut self, now: Timestamp) -> Result<(), EngineError> {
        if self.production_positions.is_some() {
            self.last_funding_accrual = Some(now);
            return Ok(());
        }
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
            .ok_or(EngineError::Arithmetic)?;
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
                .ok_or(EngineError::Arithmetic)?;
            let entry = self.accrued_funding.entry(asset.clone()).or_default();
            *entry = entry.checked_add(cost).ok_or(EngineError::Arithmetic)?;
        }
        self.last_funding_accrual = Some(now);
        Ok(())
    }
}

fn preissue_reduce_only_quantity(
    action: &PlannedAction,
    input: &ExitPlanningInput<'_>,
) -> Result<(Decimal, Option<ActionAttemptOutcome>), EngineError> {
    debug_assert!(action.reduce_only);
    match plan_risk_reducing_ioc(input) {
        Ok(exit)
            if exit.residual_class != ResidualClass::DirectionFlipCloseLeg
                && exit.side == action.side =>
        {
            Ok((exit.quantity, None))
        }
        Ok(_) | Err(ExitPlanningBlock::ExposureIncreasing) => Ok((
            Decimal::ZERO,
            Some(ActionAttemptOutcome::SupersededByNewTarget),
        )),
        Err(ExitPlanningBlock::AlreadySatisfied) => {
            Ok((Decimal::ZERO, Some(ActionAttemptOutcome::NoLongerRequired)))
        }
        Err(ExitPlanningBlock::BelowExchangeMinimum { .. }) => Ok((
            Decimal::ZERO,
            Some(ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute),
        )),
        Err(error) => Err(EngineError::Core(format!(
            "pre-issuance exit planning blocked: {error:?}"
        ))),
    }
}

fn requires_reduce_only(
    filled_notional: Decimal,
    planned_delta: Decimal,
    destination_notional: Decimal,
) -> bool {
    !filled_notional.is_zero()
        && !planned_delta.is_zero()
        && filled_notional.is_sign_positive() != planned_delta.is_sign_positive()
        && (destination_notional.is_zero()
            || destination_notional.is_sign_positive() == filled_notional.is_sign_positive())
}

#[cfg(test)]
fn completed_close_first_reversal(
    reduce_only: bool,
    raw_desired_notional: Decimal,
    technical_desired_notional: Decimal,
    position_before: Decimal,
    position_after: Decimal,
    filled_quantity: Decimal,
    unfilled_quantity: Decimal,
) -> bool {
    reduce_only
        && !raw_desired_notional.is_zero()
        && !technical_desired_notional.is_zero()
        && technical_desired_notional.is_sign_positive() == raw_desired_notional.is_sign_positive()
        && !position_before.is_zero()
        && raw_desired_notional.is_sign_positive() != position_before.is_sign_positive()
        && !filled_quantity.is_zero()
        && position_after.is_zero()
        && unfilled_quantity.is_zero()
}

fn build_market_rules<'a>(
    mids: &MarketSnapshotResponse,
    metadata: &MarketMetadataResponse,
    assets: impl Iterator<Item = &'a String>,
) -> Result<BTreeMap<String, MarketRules>, EngineError> {
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
                .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
            let decimals = *sizes
                .get(asset.as_str())
                .ok_or_else(|| EngineError::InvalidMarket(asset.clone()))?;
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

fn price_tick(price: Decimal, size_decimals: u32) -> Result<Decimal, EngineError> {
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
                .ok_or(EngineError::Arithmetic)?,
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
) -> Result<Decimal, EngineError> {
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(EngineError::Arithmetic);
    }
    let units = value.checked_div(step).ok_or(EngineError::Arithmetic)?;
    let units = if round_up {
        units.ceil()
    } else {
        units.floor()
    };
    units.checked_mul(step).ok_or(EngineError::Arithmetic)
}

fn ioc_snapshot(
    book: &OrderBookResponse,
    observed_at: Timestamp,
) -> Result<ExecutableBook, EngineError> {
    let best_bid = book
        .bids
        .first()
        .ok_or_else(|| EngineError::InvalidMarket(book.asset.clone()))?
        .price;
    let best_ask = book
        .asks
        .first()
        .ok_or_else(|| EngineError::InvalidMarket(book.asset.clone()))?
        .price;
    let midpoint = best_bid
        .checked_add(best_ask)
        .and_then(|value| value.checked_div(Decimal::from(2)))
        .ok_or(EngineError::Arithmetic)?;
    let bytes = serde_json::to_vec(book).map_err(|error| EngineError::Core(error.to_string()))?;
    Ok(ExecutableBook {
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

fn curve_leverage(config: &CopyTradeConfig, equity: f64) -> Result<f64, EngineError> {
    let mut points = config.leverage_curve.clone();
    points.sort_by(|left, right| left.equity_usd.total_cmp(&right.equity_usd));
    let first = points.first().ok_or(EngineError::Arithmetic)?;
    if equity <= first.equity_usd {
        return Ok(first.target_leverage.min(config.max_total_leverage));
    }
    let last = points.last().ok_or(EngineError::Arithmetic)?;
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
    Err(EngineError::Arithmetic)
}

fn core(error: impl Display) -> EngineError {
    EngineError::Core(error.to_string())
}

fn mfce_error(error: MfceError) -> EngineError {
    EngineError::Core(format!("MFCE: {error}"))
}

fn sum_execution_field(
    executions: &[SettledActionAccounting],
    field: impl Fn(&SettledActionAccounting) -> Decimal,
) -> Result<Decimal, EngineError> {
    executions
        .iter()
        .try_fold(Decimal::ZERO, |sum, execution| {
            sum.checked_add(field(execution))
        })
        .ok_or(EngineError::Arithmetic)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::cohort_layer::{
        PreparedVeryProfitableLayer, VeryProfitableLayerArtifact, COHORT_LAYER_SCHEMA_VERSION,
        HYPERLIQUID_INFO_AUTHORITY,
    };
    use crate::domain::cohort::{
        AttributionTag, HyperdashMembershipSnapshot, MembershipCompleteness,
        WalletQualityObservation, VERY_PROFITABLE_COHORT_ID, VERY_PROFITABLE_COHORT_URL,
    };
    use crate::domain::decision::MarketSnapshotId;
    use crate::domain::decision::{
        DecisionId, PayloadHash, PlannedAction, PlannedCloid, SnapshotSetId, TargetVersion,
    };
    use crate::domain::ioc::ExecutionFillId;
    use crate::domain::live_trading::{
        ExchangeFillIdentity, ExchangeOrderId, ExchangeTradeId, LiquidityClassification,
    };
    use crate::domain::scheduler::ReadRequestKind;
    use crate::domain::technical::{CandleInterval, ClosedCandle};
    use crate::public_mainnet::{
        BookLevel, MarketAssetContext, MarketMetadataAsset, SourceAssetPosition,
    };
    use crate::source_state::Hip3SourceActivity;
    use crate::source_state::Hip3SourceMarketActivity;
    use serde::ser::SerializeMap;
    use std::path::Path;

    fn legacy_fixture_bytes<M>(
        current: UnsignedObserverState,
        mfce: M,
        identity: &SnapshotIdentity,
    ) -> Vec<u8>
    where
        M: Serialize,
    {
        let payload = LegacyUnsignedObserverStateV12::from_current(current, mfce);
        let checksum_sha256 = legacy_unsigned_snapshot_checksum(
            LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            0,
            identity,
            &payload,
        )
        .unwrap();
        rmp_serde::to_vec(&LegacyUnsignedSnapshotEnvelopeV12 {
            schema_version: LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            generation: 0,
            identity: identity.clone(),
            payload,
            checksum_sha256,
        })
        .unwrap()
    }

    pub(crate) fn live_fill_engine(asset: &str) -> (DecisionEngine, VerifiedExchangeFill) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let mut engine = DecisionEngine::new(config, b"live-fill", "run", 40_000, 80_000).unwrap();
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![MarketMetadataAsset {
                name: asset.into(),
                size_decimals: 2,
                growth_mode: false,
            }],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        });
        engine.mids = Some((
            MarketSnapshotResponse {
                mids: BTreeMap::from([(asset.into(), Decimal::from(100))]),
            },
            1,
        ));
        let action = PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            asset: asset.into(),
            side: Side::Buy,
            rounded_notional: Decimal::from(100),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([2; 16]),
        };
        engine.pending.insert(
            asset.into(),
            PendingAction {
                action,
                root_planned_cloid: PlannedCloid([2; 16]).to_string(),
                remaining_order_quantity: Decimal::ONE,
                component_remaining: BTreeMap::from([
                    ("source:a".into(), Decimal::new(6, 1)),
                    ("source:b".into(), Decimal::new(4, 1)),
                ]),
                mfce_lineage: Default::default(),
                execution: None,
            },
        );
        let fill = VerifiedExchangeFill {
            identity: ExchangeFillIdentity {
                exchange_order_id: ExchangeOrderId("order-1".into()),
                trade_id: ExchangeTradeId("trade-1".into()),
                cloid: PlannedCloid([2; 16]),
            },
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            root_cloid: PlannedCloid([2; 16]),
            parent_cloid: None,
            continuation_generation: 0,
            asset: asset.into(),
            side: Side::Buy,
            reduce_only: false,
            filled_quantity: Decimal::new(25, 2),
            average_fill_price: Decimal::from(100),
            submitted_limit_price: Decimal::from(101),
            fee_amount: Decimal::new(125, 4),
            exchange_closed_pnl: Decimal::ZERO,
            fee_asset: "USDC".into(),
            liquidity: LiquidityClassification::Taker,
            decision_reference_price: Decimal::from(99),
            occurred_at: 10,
            decision_timestamp: 5,
            exchange_equity_after: Decimal::new(999875, 4),
            source_hash: PayloadHash([3; 32]),
        };
        (engine, fill)
    }

    pub(crate) fn assert_recovery_ingestion_progress(engine: &mut DecisionEngine, now: u64) {
        assert!(engine.execution_recovery_only());
        let source_before = engine.metrics.accepted_source_snapshots;
        let decisions_before = engine.decision_sequence;
        let candidate_id = engine.config.candidates[0].address.to_ascii_lowercase();
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id,
                        account_value: Decimal::from(100),
                        source_time_ms: now,
                        positions: BTreeMap::new(),
                        closed_candles: vec![],
                    }),
                    ReadRequestKind::ExpandedSourceState,
                    now,
                ),
                now,
            )
            .unwrap();
        let mut flow = MarketFlow::default();
        flow.trade(now, now, Decimal::from(1_000), true);
        flow.last_sent_ms = now;
        engine.ingest_market_flow("HYPE".into(), flow.clone(), now);
        assert_eq!(engine.metrics.accepted_source_snapshots, source_before + 1);
        assert_eq!(engine.mfce.delayed().flow["HYPE"], flow);
        assert!(engine.construct_next_decision(now).unwrap().is_none());
        assert_eq!(engine.decision_sequence, decisions_before);
        assert!(engine.take_prepared_authorized_intents().is_empty());
    }

    #[test]
    fn risk_only_preserves_nonzero_target_and_exchange_authority_even_when_exchange_flat() {
        let (mut engine, fill) = live_fill_engine("HYPE");
        engine.apply_live_execution_fill(&fill).unwrap();
        engine
            .target_ledger
            .replace_absolute_targets(
                &BTreeMap::from([("HYPE".into(), Decimal::from(25))]),
                &BTreeMap::from([("HYPE".into(), Decimal::from(25))]),
                &BTreeMap::new(),
                &BTreeMap::new(),
                &BTreeMap::new(),
                TargetVersion(1),
                SnapshotSetId([0; 32]),
            )
            .unwrap();
        let targets = engine.target_ledger.clone();
        let durable =
            crate::domain::live_trading::LiveTradingState::new(Decimal::from(100), 0).unwrap();
        for positions in [
            BTreeMap::from([("HYPE".into(), Decimal::new(25, 2))]),
            BTreeMap::new(),
        ] {
            let snapshot = crate::signing::transport::ExchangePositionSnapshot {
                positions: positions.clone(),
                account_equity: Decimal::from(101),
                observed_at_ms: 20,
                source_hash: PayloadHash([0; 32]),
            };
            engine.observe_recovery_risk(Some(&snapshot), &durable);
            assert_eq!(
                engine.authoritative_position("HYPE"),
                positions.get("HYPE").copied().unwrap_or_default()
            );
            assert!(engine.construct_next_decision(20).unwrap().is_none());
            assert_eq!(
                engine.target_ledger, targets,
                "recovery must not synthesize target=0"
            );
            assert_eq!(
                engine.ledger.portfolio_position("HYPE"),
                Decimal::new(25, 2),
                "durable evidence is preserved"
            );
            assert!(engine.take_prepared_authorized_intents().is_empty());
        }
    }

    #[test]
    fn authenticated_batch_rollback_and_replay_are_exactly_once() {
        let (mut engine, mut fill) = live_fill_engine("BTC");
        engine.apply_live_execution_fill(&fill).unwrap();
        assert_eq!(
            engine.pending["BTC"].remaining_order_quantity,
            Decimal::new(75, 2)
        );
        assert_eq!(
            engine.pending["BTC"].component_remaining,
            BTreeMap::from([
                ("source:a".into(), Decimal::new(45, 2)),
                ("source:b".into(), Decimal::new(30, 2)),
            ])
        );

        let checkpoint = engine.live_execution_checkpoint();
        let funding = VerifiedFundingEvent {
            event_id: FundingEventId([4; 32]),
            asset: "BTC".into(),
            amount: Decimal::new(-1, 2),
            occurred_at: 11,
            source_hash: PayloadHash([5; 32]),
        };
        fill.identity.trade_id = ExchangeTradeId("trade-2".into());
        fill.filled_quantity = Decimal::new(25, 2);
        fill.fee_amount = Decimal::new(125, 4);
        fill.occurred_at = 12;
        fill.exchange_equity_after = Decimal::new(99975, 3);
        engine.apply_live_execution_fill(&fill).unwrap();
        engine.apply_live_funding(&funding).unwrap();

        let mut invalid_final_fill = fill.clone();
        invalid_final_fill.identity.trade_id = ExchangeTradeId("trade-3".into());
        invalid_final_fill.asset = "ETH".into();
        invalid_final_fill.filled_quantity = Decimal::new(5, 1);
        invalid_final_fill.occurred_at = 13;
        assert!(engine
            .apply_live_execution_fill(&invalid_final_fill)
            .is_err());
        engine.restore_live_execution_checkpoint(checkpoint);

        assert_eq!(engine.executions.len(), 1);
        assert_eq!(
            engine.pending["BTC"].remaining_order_quantity,
            Decimal::new(75, 2)
        );
        assert!(!engine.applied_live_funding.contains(&funding.event_id));

        engine.apply_live_execution_fill(&fill).unwrap();
        engine.apply_live_funding(&funding).unwrap();
        let mut final_fill = invalid_final_fill;
        final_fill.asset = "BTC".into();
        final_fill.fee_amount = Decimal::new(25, 3);
        final_fill.exchange_equity_after = Decimal::new(9995, 2);
        engine.apply_live_execution_fill(&final_fill).unwrap();
        assert_eq!(
            engine.ledger.source_position("source:a", "BTC"),
            Decimal::new(6, 1)
        );
        assert_eq!(
            engine.ledger.source_position("source:b", "BTC"),
            Decimal::new(4, 1)
        );
        assert_eq!(engine.ledger.portfolio_position("BTC"), Decimal::ONE);
        assert!(!engine.pending.contains_key("BTC"));
        assert_eq!(engine.executions.last().unwrap().execution_mode, "live_ioc");

        let executions = engine.executions.len();
        let accrued_funding = engine.accrued_funding.clone();
        engine.apply_live_execution_fill(&fill).unwrap();
        engine.apply_live_funding(&funding).unwrap();
        engine.apply_live_execution_fill(&final_fill).unwrap();
        assert_eq!(engine.executions.len(), executions);
        assert_eq!(engine.ledger.portfolio_position("BTC"), Decimal::ONE);
        assert_eq!(engine.accrued_funding, accrued_funding);
    }

    #[test]
    fn projected_usage_counts_outstanding_increment_before_fill_without_double_counting() {
        assert_eq!(
            projected_exposure_notional(
                Decimal::from(10),
                Some(Decimal::from(5)),
                Some(Decimal::from(15)),
                false,
            )
            .unwrap(),
            Decimal::from(15)
        );
        assert_eq!(
            projected_exposure_notional(Decimal::from(15), None, Some(Decimal::from(15)), false,)
                .unwrap(),
            Decimal::from(15)
        );
        assert_eq!(
            projected_exposure_notional(
                Decimal::from(15),
                Some(Decimal::from(-7)),
                Some(Decimal::from(8)),
                false,
            )
            .unwrap(),
            Decimal::from(15),
            "a reduction reserves no additional gross risk"
        );
    }

    #[test]
    fn durable_target_without_real_intent_does_not_reserve_allocation_capacity() {
        let admitted = Some(Decimal::from(25));
        assert_eq!(intent_backed_target(false, false, admitted), None);
        assert_eq!(
            intent_backed_target(true, false, admitted),
            Some(Decimal::from(25))
        );
        assert_eq!(
            intent_backed_target(false, true, admitted),
            Some(Decimal::from(25))
        );
    }

    #[test]
    fn partial_reduce_below_floor_is_suppressed_but_full_flatten_reaches_exit_planning() {
        assert!(!action_notional_is_structurally_executable(
            true,
            Decimal::from(15),
            Decimal::from(20),
            Decimal::ONE,
            Decimal::TEN,
            Decimal::from(11),
        ));
        assert!(action_notional_is_structurally_executable(
            true,
            Decimal::ZERO,
            Decimal::from(5),
            Decimal::from(5),
            Decimal::TEN,
            Decimal::from(11),
        ));
    }

    #[test]
    fn projected_usage_handles_reversal_and_atomic_target_replacement() {
        assert_eq!(
            projected_exposure_notional(
                Decimal::from(10),
                Some(Decimal::from(-15)),
                Some(Decimal::from(-5)),
                false,
            )
            .unwrap(),
            Decimal::from(10),
            "a long-ten to short-five crossing has no endpoint above ten"
        );
        assert_eq!(
            projected_exposure_notional(
                Decimal::from(10),
                Some(Decimal::from(20)),
                Some(Decimal::from(30)),
                true,
            )
            .unwrap(),
            Decimal::from(10),
            "reevaluation replaces the old outstanding target instead of accumulating it"
        );
    }

    #[test]
    fn stream_continuity_replaces_wallet_age_and_gaps_fail_closed() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(1);
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine =
            DecisionEngine::new(config, b"stream-freshness", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine
            .replace_source_market_coverage(BTreeSet::from(["BTC".into()]), 0)
            .unwrap();
        engine.set_source_tier(&candidate, SourceTier::Inactive);
        engine
            .ingest(
                AcceptedPublicResponse {
                    request_kind: ReadRequestKind::ExpandedSourceState,
                    source_tier: Some(SourceTier::Inactive),
                    subject: candidate.clone(),
                    requested_at_mono: 1,
                    received_at_mono: 1,
                    valid_until_mono: 80_001,
                    payload: PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate.clone(),
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1,
                        positions: BTreeMap::new(),
                        closed_candles: Vec::new(),
                    }),
                },
                1,
            )
            .unwrap();

        assert_eq!(engine.active_source_count(1), 0);
        assert_eq!(
            engine.pending_source_wallets(),
            BTreeSet::from([candidate.clone()])
        );
        assert!(engine
            .admit_reconciled_source_wallet(&candidate, 1, 2)
            .unwrap());
        assert!(engine.source_stream_healthy());
        assert!(engine.source_is_fresh(&candidate, 10_000_000));
        engine
            .mark_source_stream_gap(&BTreeSet::from(["BTC".into()]), 3)
            .unwrap();
        assert!(!engine.source_is_fresh(&candidate, 2));
        engine
            .ingest(
                AcceptedPublicResponse {
                    request_kind: ReadRequestKind::ExpandedSourceState,
                    source_tier: Some(SourceTier::Inactive),
                    subject: candidate.clone(),
                    requested_at_mono: 4,
                    received_at_mono: 4,
                    valid_until_mono: 80_004,
                    payload: PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate.clone(),
                        account_value: Decimal::from(1_000),
                        source_time_ms: 2,
                        positions: BTreeMap::new(),
                        closed_candles: Vec::new(),
                    }),
                },
                4,
            )
            .unwrap();
        assert!(!engine.source_is_fresh(&candidate, 4));
        assert!(!engine.source_stream_healthy());
        assert_eq!(
            engine.pending_source_wallets(),
            BTreeSet::from([candidate.clone()])
        );
        engine
            .admit_reconciled_source_wallet(&candidate, 2, 4)
            .unwrap();
        assert!(engine.source_stream_healthy());
        assert!(engine.source_is_fresh(&candidate, 4));
    }

    #[test]
    fn snapshot_presence_with_one_unconfirmed_wallet_rearms_without_completion_transition() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates = (1..=375)
            .map(|index| {
                let mut candidate = config.candidates[0].clone();
                candidate.address = format!("0x{index:040x}");
                candidate
            })
            .collect();
        let wallets = config
            .candidates
            .iter()
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let mut engine =
            DecisionEngine::new(config, b"partial-recovery", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();

        for (index, wallet) in wallets.iter().enumerate() {
            let source_time_ms = u64::try_from(index + 1).unwrap();
            engine
                .ingest(
                    AcceptedPublicResponse {
                        request_kind: ReadRequestKind::ExpandedSourceState,
                        source_tier: Some(SourceTier::Inactive),
                        subject: wallet.clone(),
                        requested_at_mono: source_time_ms,
                        received_at_mono: source_time_ms,
                        valid_until_mono: 80_000 + source_time_ms,
                        payload: PublicPayload::SourceState(SourceStateResponse {
                            block_number: None,
                            candidate_id: wallet.clone(),
                            account_value: Decimal::from(1_000),
                            source_time_ms,
                            positions: BTreeMap::new(),
                            closed_candles: Vec::new(),
                        }),
                    },
                    source_time_ms,
                )
                .unwrap();
        }

        assert_eq!(engine.live_confirmed_source_count(), 0);
        assert_eq!(engine.pending_source_wallets().len(), 375);
        assert!(!engine.source_stream_healthy());

        for (index, wallet) in wallets.iter().take(374).enumerate() {
            assert!(engine
                .admit_reconciled_source_wallet(wallet, index as u64 + 1, 400)
                .unwrap());
        }
        assert_eq!(engine.live_confirmed_source_count(), 374);
        assert_eq!(
            engine.pending_source_wallets(),
            BTreeSet::from([wallets[374].clone()])
        );
        assert!(!engine.source_stream_healthy());
        assert_eq!(engine.active_source_count(400), 374);

        assert!(!engine
            .admit_reconciled_source_wallet(&wallets[374], 1, 401)
            .unwrap());
        assert_eq!(
            engine.pending_source_wallets(),
            BTreeSet::from([wallets[374].clone()])
        );

        assert!(engine
            .admit_reconciled_source_wallet(&wallets[374], 375, 402)
            .unwrap());
        assert_eq!(engine.live_confirmed_source_count(), 375);
        assert!(engine.pending_source_wallets().is_empty());
        assert!(engine.source_stream_healthy());
        // Exact former crash: all snapshots still exist, but one current
        // generation confirmation was cleared after the apparent completion.
        engine.source_wallet_ready.remove(&wallets[374]);
        assert_eq!(engine.active_source_count(403), 374);
        assert_eq!(
            engine.pending_source_wallets(),
            BTreeSet::from([wallets[374].clone()])
        );
        engine
            .admit_reconciled_source_wallet(&wallets[374], 375, 404)
            .unwrap();
        assert!(engine.pending_source_wallets().is_empty());
    }

    #[test]
    fn unusable_wallet_normalization_is_omitted_without_stopping_valid_sources() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(2);
        config.technical.enabled = false;
        for candidate in &mut config.candidates {
            candidate.allocation_weight = 1.0;
            candidate.confidence_modifier = Some(1.0);
        }
        let valid = config.candidates[0].address.to_ascii_lowercase();
        let unusable = config.candidates[1].address.to_ascii_lowercase();
        let mut engine =
            DecisionEngine::new(config, b"source-normalization", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine
            .replace_source_market_coverage(BTreeSet::from(["BTC".into()]), 0)
            .unwrap();
        engine.set_source_tier(&valid, SourceTier::Inactive);
        engine.set_source_tier(&unusable, SourceTier::Inactive);

        let source = |candidate: String, account_value: Decimal, notional: Decimal, at| {
            SourceStateResponse {
                block_number: None,
                candidate_id: candidate,
                account_value,
                source_time_ms: at,
                positions: (!notional.is_zero())
                    .then(|| {
                        BTreeMap::from([(
                            "BTC".into(),
                            SourceAssetPosition {
                                asset: "BTC".into(),
                                signed_size: Decimal::ONE,
                                signed_notional: notional,
                                entry_price: Some(Decimal::from(100)),
                                unrealized_pnl: Some(Decimal::ZERO),
                            },
                        )])
                    })
                    .unwrap_or_default(),
                closed_candles: Vec::new(),
            }
        };

        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(source(
                        valid.clone(),
                        Decimal::from(1_000),
                        Decimal::from(100),
                        1_000,
                    )),
                    ReadRequestKind::ExpandedSourceState,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(source(
                        unusable.clone(),
                        Decimal::ZERO,
                        Decimal::ZERO,
                        1_000,
                    )),
                    ReadRequestKind::ExpandedSourceState,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        // A streamed opening fill retains its raw position while the wallet's
        // unchanged zero-equity baseline makes its normalized contribution
        // temporarily unusable.
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(source(
                        unusable.clone(),
                        Decimal::ZERO,
                        Decimal::from(900),
                        1_001,
                    )),
                    ReadRequestKind::ExpandedSourceState,
                    1_001,
                ),
                1_001,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
                    }),
                    ReadRequestKind::MarketMids,
                    1_002,
                ),
                1_002,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketMetadata(MarketMetadataResponse {
                        universe: vec![MarketMetadataAsset {
                            name: "BTC".into(),
                            size_decimals: 3,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_002,
                ),
                1_002,
            )
            .unwrap();
        engine
            .admit_reconciled_source_wallet(&valid, 1_000, 1_003)
            .unwrap();
        engine
            .admit_reconciled_source_wallet(&unusable, 1_001, 1_003)
            .unwrap();

        assert_eq!(
            engine.mfce.previous_source_exposure("BTC"),
            Decimal::new(1, 1)
        );
        assert_eq!(engine.mfce_report().observed_transitions, 1);
        assert_eq!(engine.mfce_report().decision_counts.reject, 0);

        // A later authoritative positive-equity state rejoins naturally with
        // no explicit uncertainty state or schema transition.
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(source(
                        unusable,
                        Decimal::from(1_000),
                        Decimal::from(200),
                        1_004,
                    )),
                    ReadRequestKind::ExpandedSourceState,
                    1_004,
                ),
                1_004,
            )
            .unwrap();
        assert_eq!(
            engine.mfce.previous_source_exposure("BTC"),
            Decimal::new(3, 1)
        );
        assert_eq!(engine.mfce_report().observed_transitions, 2);
    }

    #[test]
    fn zero_equity_source_is_valid_only_while_it_has_no_position() {
        let empty = SourceStateResponse {
            block_number: None,
            candidate_id: "0x0000000000000000000000000000000000000001".into(),
            account_value: Decimal::ZERO,
            source_time_ms: 1,
            positions: BTreeMap::new(),
            closed_candles: Vec::new(),
        };
        assert_eq!(normalized_source_exposures(&empty), Some(BTreeMap::new()));

        let mut nonempty = empty;
        nonempty.positions.insert(
            "BTC".into(),
            SourceAssetPosition {
                asset: "BTC".into(),
                signed_size: Decimal::ONE,
                signed_notional: Decimal::from(100),
                entry_price: Some(Decimal::from(100)),
                unrealized_pnl: Some(Decimal::ZERO),
            },
        );
        assert_eq!(normalized_source_exposures(&nonempty), None);
    }

    #[test]
    fn resumed_market_cannot_create_mfce_risk_until_post_gap_reconciliation() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine =
            DecisionEngine::new(config, b"coverage-pending", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine.set_source_tier(&candidate, SourceTier::Inactive);
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate.clone(),
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1_000,
                        positions: BTreeMap::from([(
                            "BTC".into(),
                            SourceAssetPosition {
                                asset: "BTC".into(),
                                signed_size: Decimal::ONE,
                                signed_notional: Decimal::from(100),
                                entry_price: Some(Decimal::from(100)),
                                unrealized_pnl: Some(Decimal::ZERO),
                            },
                        )]),
                        closed_candles: Vec::new(),
                    }),
                    ReadRequestKind::ExpandedSourceState,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
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
                            name: "BTC".into(),
                            size_decimals: 3,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();

        engine
            .admit_reconciled_source_wallet(&candidate, 1_000, 1_001)
            .unwrap();
        engine
            .replace_source_market_coverage(BTreeSet::new(), 1_001)
            .unwrap();
        assert_eq!(engine.source_market_coverage_count(), 0);
        assert_eq!(engine.mfce_report().observed_transitions, 0);
        assert_eq!(engine.mfce_report().decision_counts.reject, 0);

        engine
            .replace_source_market_coverage(BTreeSet::from(["BTC".into()]), 1_002)
            .unwrap();
        assert_eq!(engine.source_market_coverage_count(), 0);
        assert_eq!(engine.mfce_report().observed_transitions, 0);
        let mut snapshot = engine
            .source_store
            .latest(&candidate)
            .unwrap()
            .payload
            .clone();
        snapshot.source_time_ms = 1_003;
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(snapshot),
                    ReadRequestKind::ExpandedSourceState,
                    1_004,
                ),
                1_004,
            )
            .unwrap();
        engine
            .admit_reconciled_source_wallet(&candidate, 1_003, 1_004)
            .unwrap();
        assert_eq!(engine.source_market_coverage_count(), 1);
        assert_eq!(engine.mfce_report().observed_transitions, 1);
    }

    #[test]
    fn resubscribed_market_readmits_wallets_independently_without_resetting_other_markets() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(2);
        let wallets = config
            .candidates
            .iter()
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let mut engine =
            DecisionEngine::new(config, b"market-frontiers", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        let markets = BTreeSet::from(["BTC".into(), "ETH".into()]);
        engine
            .replace_source_market_coverage(markets.clone(), 0)
            .unwrap();
        for wallet in &wallets {
            engine
                .ingest(
                    accepted(
                        PublicPayload::SourceState(SourceStateResponse {
                            block_number: None,
                            candidate_id: wallet.clone(),
                            account_value: Decimal::from(1_000),
                            source_time_ms: 2,
                            positions: BTreeMap::new(),
                            closed_candles: Vec::new(),
                        }),
                        ReadRequestKind::ExpandedSourceState,
                        2,
                    ),
                    2,
                )
                .unwrap();
            engine.admit_reconciled_source_wallet(wallet, 2, 2).unwrap();
        }
        engine
            .replace_source_market_coverage(BTreeSet::from(["ETH".into()]), 3)
            .unwrap();
        engine
            .replace_source_market_coverage(markets.clone(), 4)
            .unwrap();
        for wallet in &wallets {
            assert!(engine.source_market_confirmed(wallet, "ETH"));
            assert!(!engine.source_market_confirmed(wallet, "BTC"));
        }
        let mut snapshot = engine
            .source_store
            .latest(&wallets[0])
            .unwrap()
            .payload
            .clone();
        snapshot.source_time_ms = 6;
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(snapshot),
                    ReadRequestKind::ExpandedSourceState,
                    6,
                ),
                6,
            )
            .unwrap();
        engine
            .admit_reconciled_source_wallet(&wallets[0], 6, 6)
            .unwrap();
        assert!(engine.source_market_confirmed(&wallets[0], "BTC"));
        assert!(!engine.source_market_confirmed(&wallets[1], "BTC"));
        assert_eq!(
            engine.pending_source_wallets(),
            BTreeSet::from([wallets[1].clone()])
        );
        // Repeating the subscription set cannot move its existing frontier.
        engine.replace_source_market_coverage(markets, 7).unwrap();
        assert!(engine.source_market_confirmed(&wallets[0], "BTC"));
        assert!(engine.source_market_confirmed(&wallets[1], "ETH"));
    }

    #[test]
    fn subscription_gap_invalidates_only_its_dependent_markets() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine =
            DecisionEngine::new(config, b"local-stream-gap", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine
            .replace_source_market_coverage(BTreeSet::from(["BTC".into(), "ETH".into()]), 0)
            .unwrap();
        engine.set_source_tier(&candidate, SourceTier::Inactive);
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate.clone(),
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1_000,
                        positions: BTreeMap::from([
                            (
                                "BTC".into(),
                                SourceAssetPosition {
                                    asset: "BTC".into(),
                                    signed_size: Decimal::ONE,
                                    signed_notional: Decimal::from(100),
                                    entry_price: Some(Decimal::from(100)),
                                    unrealized_pnl: Some(Decimal::ZERO),
                                },
                            ),
                            (
                                "ETH".into(),
                                SourceAssetPosition {
                                    asset: "ETH".into(),
                                    signed_size: Decimal::ONE,
                                    signed_notional: Decimal::from(200),
                                    entry_price: Some(Decimal::from(200)),
                                    unrealized_pnl: Some(Decimal::ZERO),
                                },
                            ),
                        ]),
                        closed_candles: Vec::new(),
                    }),
                    ReadRequestKind::ExpandedSourceState,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: BTreeMap::from([
                            ("BTC".into(), Decimal::from(100)),
                            ("ETH".into(), Decimal::from(200)),
                        ]),
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
                        universe: vec![
                            MarketMetadataAsset {
                                name: "BTC".into(),
                                size_decimals: 3,
                                growth_mode: false,
                            },
                            MarketMetadataAsset {
                                name: "ETH".into(),
                                size_decimals: 3,
                                growth_mode: false,
                            },
                        ],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .admit_reconciled_source_wallet(&candidate, 1_000, 1_001)
            .unwrap();

        engine
            .replace_source_market_coverage(BTreeSet::from(["ETH".into()]), 1_002)
            .unwrap();
        assert!(engine.source_is_fresh(&candidate, 1_002));
        assert!(!engine.source_market_confirmed(&candidate, "BTC"));
        assert!(engine.source_market_confirmed(&candidate, "ETH"));
        assert_eq!(engine.source_market_coverage_count(), 1);
        assert_eq!(
            engine.mfce.previous_source_exposure("BTC"),
            Decimal::new(1, 1)
        );
        assert_eq!(
            engine.mfce.previous_source_exposure("ETH"),
            Decimal::new(2, 1)
        );

        engine
            .replace_source_market_coverage(BTreeSet::new(), 1_003)
            .unwrap();
        assert_eq!(engine.source_market_coverage_count(), 0);
    }

    #[test]
    fn production_execution_is_capability_based_not_blanket_prefixed() {
        // Capability gate: unknown/unresolved HIP-3 fails closed, ready
        // HIP-3 admits, native stays admitted.
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let mut engine = DecisionEngine::new(config, b"hip3-cap", "run", 40_000, 80_000).unwrap();
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![
                MarketMetadataAsset {
                    name: "BTC".into(),
                    size_decimals: 5,
                    growth_mode: false,
                },
                MarketMetadataAsset {
                    name: "xyz:NVDA".into(),
                    size_decimals: 2,
                    growth_mode: false,
                },
            ],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        });
        engine.mids = Some((
            MarketSnapshotResponse {
                mids: BTreeMap::from([
                    ("BTC".into(), Decimal::from(100)),
                    ("xyz:NVDA".into(), Decimal::from(50)),
                ]),
            },
            0,
        ));
        // No dex order yet: HIP-3 unresolved -> fail closed, native admitted.
        assert!(engine.is_production_execution_supported("BTC"));
        assert!(!engine.is_production_execution_supported("xyz:NVDA"));
        assert!(!engine.is_production_execution_supported("io:ANTH"));
        // Admit xyz with collateral + dex order: ready HIP-3 admits.
        engine.install_execution_dex_order(vec!["".into(), "xyz".into()]);
        assert!(engine.is_production_execution_supported("xyz:NVDA"));
        assert!(!engine.is_production_execution_supported("foo:GOLD"));
        assert!(!engine.is_production_execution_supported("hyna:GOLD"));
    }

    #[test]
    fn hip3_regression_matrix_covers_venue_separation_and_fail_closed() {
        use crate::hip3::{
            dexes_for_assets as hip3_dexes, execution_asset_id as hip3_id,
            parse_perp_market as hip3_parse,
        };
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let mut engine =
            DecisionEngine::new(config, b"hip3-matrix", "run", 40_000, 80_000).unwrap();
        // Universe: native BTC + GOLD, xyz DEX (NVDA, GOLD), foo DEX (GOLD).
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![
                MarketMetadataAsset {
                    name: "BTC".into(),
                    size_decimals: 5,
                    growth_mode: false,
                },
                MarketMetadataAsset {
                    name: "GOLD".into(),
                    size_decimals: 3,
                    growth_mode: false,
                },
                MarketMetadataAsset {
                    name: "xyz:NVDA".into(),
                    size_decimals: 2,
                    growth_mode: false,
                },
                MarketMetadataAsset {
                    name: "xyz:GOLD".into(),
                    size_decimals: 2,
                    growth_mode: false,
                },
                MarketMetadataAsset {
                    name: "foo:GOLD".into(),
                    size_decimals: 3,
                    growth_mode: false,
                },
            ],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        });
        engine.mids = Some((
            MarketSnapshotResponse {
                mids: BTreeMap::from([
                    ("BTC".into(), Decimal::from(100)),
                    ("GOLD".into(), Decimal::from(50)),
                    ("xyz:NVDA".into(), Decimal::from(10)),
                    ("xyz:GOLD".into(), Decimal::from(51)),
                    ("foo:GOLD".into(), Decimal::from(52)),
                ]),
            },
            0,
        ));
        engine.install_execution_dex_order(vec!["".into(), "xyz".into(), "foo".into()]);
        let universe_order = engine
            .metadata
            .as_ref()
            .unwrap()
            .universe
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        let dex_order = engine.execution_dex_order().to_vec();
        // 1. Native asset IDs unchanged (positional in native slice).
        assert_eq!(hip3_id(&universe_order, &dex_order, "BTC"), Some(0));
        // 2. HIP-3 IDs resolve from dex index + local index.
        assert_eq!(
            hip3_id(&universe_order, &dex_order, "xyz:NVDA"),
            Some(110_000)
        );
        assert_eq!(
            hip3_id(&universe_order, &dex_order, "xyz:GOLD"),
            Some(110_001)
        );
        assert_eq!(
            hip3_id(&universe_order, &dex_order, "foo:GOLD"),
            Some(120_000)
        );
        // 3. Two builder DEXes do not share asset IDs.
        assert_ne!(
            hip3_id(&universe_order, &dex_order, "xyz:GOLD"),
            hip3_id(&universe_order, &dex_order, "foo:GOLD")
        );
        // 4. Same-symbol markets remain distinct venues.
        assert_ne!(hip3_parse("GOLD"), hip3_parse("xyz:GOLD"));
        assert_ne!(hip3_parse("xyz:GOLD"), hip3_parse("foo:GOLD"));
        assert_eq!(
            hip3_dexes(["GOLD", "xyz:GOLD", "foo:GOLD"].into_iter()),
            BTreeSet::from(["xyz".to_string(), "foo".to_string()])
        );
        // 5/6. Unknown DEX / market fail closed.
        assert_eq!(hip3_id(&universe_order, &dex_order, "bar:GOLD"), None);
        assert_eq!(hip3_id(&universe_order, &dex_order, "xyz:UNKNOWN"), None);
        assert!(!engine.is_production_execution_supported("bar:GOLD"));
        assert!(!engine.is_production_execution_supported("xyz:UNKNOWN"));
        // 7. Missing collateral fails closed (remove foo entry).
        engine.dex_collateral.remove("foo");
        assert!(!engine.is_production_execution_supported("foo:GOLD"));
        assert!(engine.is_production_execution_supported("xyz:NVDA"));
        engine.set_dex_collateral("foo", "USDC");
        assert!(engine.is_production_execution_supported("foo:GOLD"));
        // 8. Missing live state prevents executable action.
        engine.mids.as_mut().unwrap().0.mids.remove("xyz:NVDA");
        assert!(!engine.is_production_execution_supported("xyz:NVDA"));
        engine
            .mids
            .as_mut()
            .unwrap()
            .0
            .mids
            .insert("xyz:NVDA".into(), Decimal::from(10));
        // 9. Valid HIP-3 market reaches PendingAction admission.
        assert!(engine.is_production_execution_supported("xyz:NVDA"));
        // 10. Signing uses the HIP-3 asset ID (single resolver).
        assert_eq!(engine.execution_asset_id("xyz:NVDA"), Some(110_000));
        assert_eq!(engine.execution_asset_id("foo:GOLD"), Some(120_000));
        // 11. Cancel/reconciliation use the same wire identity (no alias).
        assert_ne!("GOLD", "xyz:GOLD");
        // hyna sunset never admits even with dex order containing it.
        engine.install_execution_dex_order(vec![
            "".into(),
            "hyna".into(),
            "xyz".into(),
            "foo".into(),
        ]);
        assert!(!engine.is_production_execution_supported("hyna:GOLD"));
        // xyz/foo IDs shift with the hyna slot preserved (exactness).
        let dex_order2 = engine.execution_dex_order().to_vec();
        assert_eq!(
            hip3_id(&universe_order, &dex_order2, "xyz:NVDA"),
            Some(120_000)
        );
        assert_eq!(
            hip3_id(&universe_order, &dex_order2, "foo:GOLD"),
            Some(130_000)
        );
    }

    #[test]
    fn hip3_pending_action_and_registry_survive_restart_without_aliasing() {
        // 12/13: fills update the correct venue position; restart preserves
        // PendingAction market identity + CLOID ownership with no duplicates.
        let (mut engine, fill) = live_fill_engine("xyz:GOLD");
        // Seed a same-symbol native pending to prove no aliasing.
        engine.pending.insert(
            "GOLD".into(),
            PendingAction {
                action: PlannedAction {
                    decision_id: DecisionId([9; 32]),
                    target_version: TargetVersion(1),
                    asset: "GOLD".into(),
                    side: Side::Buy,
                    rounded_notional: Decimal::from(50),
                    reduce_only: false,
                    action_ordinal: 0,
                    retry_generation: 0,
                    planned_cloid: PlannedCloid([8; 16]),
                },
                root_planned_cloid: PlannedCloid([8; 16]).to_string(),
                remaining_order_quantity: Decimal::ONE,
                component_remaining: BTreeMap::from([("source:a".into(), Decimal::ONE)]),
                mfce_lineage: Default::default(),
                execution: None,
            },
        );
        engine.apply_live_execution_fill(&fill).unwrap();
        // xyz:GOLD fill must not touch GOLD pending/position.
        assert!(engine.pending.contains_key("xyz:GOLD"));
        assert!(engine.pending.contains_key("GOLD"));
        assert!(
            !engine.ledger.portfolio_position("GOLD").is_zero()
                || engine.ledger.portfolio_position("xyz:GOLD") != Decimal::ZERO
        );
        // 14. Settled accounting uses the real exchange fill economics on the
        // correct venue (no native-fee contamination, no aliasing).
        let settled = engine
            .executions
            .iter()
            .find(|execution| execution.asset == "xyz:GOLD")
            .expect("hip3 settled accounting must exist");
        assert_eq!(settled.filled_quantity, fill.filled_quantity);
        assert_eq!(settled.fees, fill.fee_amount);
        assert!(engine
            .executions
            .iter()
            .all(|execution| execution.asset != "GOLD"
                || execution.filled_quantity.is_zero()
                || execution.execution_id != settled.execution_id));
        // Simulate restart: pending keys + CLOIDs round-trip through the
        // canonical snapshot encoding without venue aliasing.
        let pending_keys = engine.pending.keys().cloned().collect::<BTreeSet<_>>();
        assert!(pending_keys.contains("xyz:GOLD"));
        assert!(pending_keys.contains("GOLD"));
        let cloids = engine
            .pending
            .values()
            .map(|pending| pending.action.planned_cloid)
            .collect::<BTreeSet<_>>();
        assert_eq!(cloids.len(), 2);
    }

    #[test]
    fn hip3_pipeline_transitions_from_unsupported_to_action_when_ready() {
        // 9/15 acceptance shape: unsupported -> 0 for ready markets.
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine = DecisionEngine::new(config, b"hip3-ready", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [0; 32],
            signer_release_hash: [0; 32],
            release_manifest_hash: [0; 32],
            market_rules_hash: [0; 32],
            dynamic_floor_policy_hash: [0; 32],
            ioc_policy_hash: [0; 32],
            expires_after_ms: 20_000,
        });
        engine
            .replace_source_market_coverage(BTreeSet::from(["xyz:NVDA".to_string()]), 0)
            .unwrap();
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![MarketMetadataAsset {
                name: "xyz:NVDA".into(),
                size_decimals: 2,
                growth_mode: false,
            }],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        });
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate.clone(),
                        account_value: Decimal::from(1_000),
                        source_time_ms: 10,
                        positions: BTreeMap::from([(
                            "xyz:NVDA".into(),
                            SourceAssetPosition {
                                asset: "xyz:NVDA".into(),
                                signed_size: Decimal::ONE,
                                signed_notional: Decimal::from(100),
                                entry_price: Some(Decimal::from(100)),
                                unrealized_pnl: Some(Decimal::ZERO),
                            },
                        )]),
                        closed_candles: Vec::new(),
                    }),
                    ReadRequestKind::ExpandedSourceState,
                    10,
                ),
                11,
            )
            .unwrap();
        assert!(engine
            .admit_reconciled_source_wallet(&candidate, 10, 11)
            .unwrap());
        let activity = Hip3SourceActivity {
            markets: BTreeMap::from([(
                "xyz:NVDA".to_string(),
                Hip3SourceMarketActivity {
                    source_fills: 2,
                    source_wallets: BTreeSet::from([candidate]),
                },
            )]),
        };
        let before = engine.hip3_pipeline_status(Some(&activity));
        assert_eq!(before.hip3_execution_unsupported, 1);
        // Admit the DEX with metadata + live state: unsupported -> 0.
        engine.mids = Some((
            MarketSnapshotResponse {
                mids: BTreeMap::from([("xyz:NVDA".into(), Decimal::from(100))]),
            },
            11,
        ));
        engine.install_execution_dex_order(vec!["".into(), "xyz".into()]);
        let after = engine.hip3_pipeline_status(Some(&activity));
        assert_eq!(after.hip3_execution_unsupported, 0);
    }

    #[test]
    fn hip3_authoritative_fees_flow_into_remaining_edge_by_exact_delta() {
        use crate::hip3_fees::UserPerpFeeState;
        use std::str::FromStr;
        fn fee_engine(growth: bool) -> DecisionEngine {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
            let mut engine =
                DecisionEngine::new(config, b"hip3-fee-edge", "run", 40_000, 80_000).unwrap();
            engine.metadata = Some(MarketMetadataResponse {
                universe: vec![MarketMetadataAsset {
                    name: "xyz:NVDA".into(),
                    size_decimals: 2,
                    growth_mode: growth,
                }],
                contexts: BTreeMap::new(),
                live_taker_fee_bps: None,
                dex_fee_scales: BTreeMap::from([("xyz".into(), Decimal::ONE)]),
                user_fee_state: Some(UserPerpFeeState {
                    taker_rate: Decimal::from_str("0.00045").unwrap(),
                    maker_rate: Decimal::from_str("0.00015").unwrap(),
                    active_referral_discount: Decimal::ZERO,
                }),
            });
            engine.mids = Some((
                MarketSnapshotResponse {
                    mids: BTreeMap::from([("xyz:NVDA".into(), Decimal::from(100))]),
                },
                0,
            ));
            engine.install_execution_dex_order(vec!["".into(), "xyz".into()]);
            engine
        }
        // Otherwise-identical candidate under two fee contexts: growth off
        // (2x => 9 bps) vs growth on (0.1x => 0.9 bps).
        let plain = fee_engine(false);
        let growth = fee_engine(true);
        let fee_plain = plain.taker_fee_bps_for("xyz:NVDA").unwrap();
        let fee_growth = growth.taker_fee_bps_for("xyz:NVDA").unwrap();
        assert_eq!(fee_plain, Decimal::from_str("9").unwrap());
        assert_eq!(fee_growth, Decimal::from_str("0.9").unwrap());
        // Edge-only: policy hurdle uses base friction only; fee delta lives
        // in settled accounting, not the admission hurdle.
        let multiplier = Decimal::from(4);
        let friction_plain =
            buffered_policy_friction_bps(Decimal::from(10), fee_plain, multiplier, true).unwrap();
        let friction_growth =
            buffered_policy_friction_bps(Decimal::from(10), fee_growth, multiplier, true).unwrap();
        assert_eq!(friction_plain, Decimal::from(10));
        assert_eq!(friction_growth, Decimal::from(10));
        assert_eq!(friction_plain - friction_growth, Decimal::ZERO);
        // Native path is untouched: account tier straight through.
        assert_eq!(
            plain.taker_fee_bps_for("BTC"),
            Some(Decimal::from_str("4.5").unwrap())
        );
    }

    #[test]
    fn hip3_pipeline_status_separates_discovery_confirmation_and_execution_support() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine =
            DecisionEngine::new(config, b"hip3-status", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [0; 32],
            signer_release_hash: [0; 32],
            release_manifest_hash: [0; 32],
            market_rules_hash: [0; 32],
            dynamic_floor_policy_hash: [0; 32],
            ioc_policy_hash: [0; 32],
            expires_after_ms: 20_000,
        });
        engine
            .replace_source_market_coverage(BTreeSet::from(["xyz:AMD".to_string()]), 0)
            .unwrap();
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![
                MarketMetadataAsset {
                    name: "BTC".into(),
                    size_decimals: 5,
                    growth_mode: false,
                },
                MarketMetadataAsset {
                    name: "xyz:AMD".into(),
                    size_decimals: 2,
                    growth_mode: false,
                },
            ],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        });
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate.clone(),
                        account_value: Decimal::from(1_000),
                        source_time_ms: 10,
                        positions: BTreeMap::from([(
                            "xyz:AMD".into(),
                            SourceAssetPosition {
                                asset: "xyz:AMD".into(),
                                signed_size: Decimal::ONE,
                                signed_notional: Decimal::from(100),
                                entry_price: Some(Decimal::from(100)),
                                unrealized_pnl: Some(Decimal::ZERO),
                            },
                        )]),
                        closed_candles: Vec::new(),
                    }),
                    ReadRequestKind::ExpandedSourceState,
                    10,
                ),
                10,
            )
            .unwrap();
        assert!(engine
            .admit_reconciled_source_wallet(&candidate, 10, 11)
            .unwrap());
        let activity = Hip3SourceActivity {
            markets: BTreeMap::from([
                (
                    "xyz:AMD".to_string(),
                    Hip3SourceMarketActivity {
                        source_fills: 3,
                        source_wallets: BTreeSet::from([candidate.clone()]),
                    },
                ),
                (
                    "missing:BAD".to_string(),
                    Hip3SourceMarketActivity {
                        source_fills: 5,
                        source_wallets: BTreeSet::from([candidate]),
                    },
                ),
            ]),
        };

        let status = engine.hip3_pipeline_status(Some(&activity));

        assert_eq!(status.hip3_source_markets_seen, 2);
        assert_eq!(status.hip3_source_markets_known, 1);
        assert_eq!(status.hip3_source_markets_confirmed, 1);
        assert_eq!(status.hip3_mfce_candidates, 0);
        assert_eq!(status.hip3_execution_unsupported, 1);
        assert_eq!(status.hip3_actions_emitted, 0);
        assert_eq!(status.hip3_fills, 0);
        assert_eq!(status.hip3_execution_unsupported_source_fills, 3);
        assert_eq!(status.hip3_execution_unsupported_unique_markets, 1);
        assert_eq!(status.hip3_execution_unsupported_unique_wallets, 1);
    }

    #[test]
    fn missing_builder_rules_defer_only_that_market_and_rearm_after_hydration() {
        let (mut engine, _) =
            pending_reduce_only_engine("BTC", Decimal::ONE, Decimal::from(100), Decimal::from(100));
        let mut input = engine.pending["BTC"]
            .execution
            .as_ref()
            .unwrap()
            .projection_input
            .clone();
        input
            .unconstrained_targets
            .insert("xyz:AMD".into(), Decimal::from(20));
        engine
            .target_ledger
            .replace_absolute_targets(
                &input.unconstrained_targets,
                &input.unconstrained_targets,
                &input.filled_positions,
                &BTreeMap::new(),
                &BTreeMap::new(),
                TargetVersion(42),
                SnapshotSetId([42; 32]),
            )
            .unwrap();
        let durable_targets = engine.target_ledger.clone();
        let held_positions = engine.authoritative_position("BTC");
        engine
            .mids
            .as_mut()
            .unwrap()
            .0
            .mids
            .insert("xyz:AMD".into(), Decimal::from(100));
        assert!(!engine
            .complete_execution_rules(&mut input.clone(), "xyz:AMD")
            .unwrap());
        assert!(engine.waiting_market_rules.contains("xyz:AMD"));
        let mut btc_input = input.clone();
        assert!(engine
            .complete_execution_rules(&mut btc_input, "BTC")
            .unwrap());
        assert!(!btc_input.unconstrained_targets.contains_key("xyz:AMD"));
        assert!(btc_input.filled_positions.contains_key("BTC"));
        assert!(project_and_validate_portfolio(&btc_input).is_ok());
        assert_eq!(engine.target_ledger, durable_targets);
        assert_eq!(engine.authoritative_position("BTC"), held_positions);
        let identity = StateIdentity {
            source_tree_sha256: "test".into(),
            observer_binary_sha256: "test".into(),
            configuration_sha256: "test".into(),
            risk_policy_sha256: "test".into(),
        };
        let path =
            std::env::temp_dir().join(format!("rules-pending-{}.msgpack", std::process::id()));
        engine.persist_unsigned_state(&path, &identity).unwrap();
        let persisted =
            decode_unsigned_snapshot(&std::fs::read(&path).unwrap(), &identity).unwrap();
        assert!(persisted.payload.waiting_market_rules.contains("xyz:AMD"));
        assert_eq!(persisted.payload.target_ledger, durable_targets);
        std::fs::remove_file(path).unwrap();
        let mut held_unknown = input.clone();
        held_unknown
            .filled_positions
            .insert("xyz:AMD".into(), Decimal::from(10));
        assert!(!engine
            .complete_execution_rules(&mut held_unknown, "BTC")
            .unwrap());
        assert_eq!(held_unknown.filled_positions["xyz:AMD"], Decimal::from(10));

        let additional = MarketMetadataResponse {
            universe: vec![MarketMetadataAsset {
                name: "xyz:AMD".into(),
                size_decimals: 3,
                growth_mode: false,
            }],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        };
        let merged = engine
            .merge_hydrated_metadata(additional.clone(), "xyz")
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketMetadata(merged),
                    ReadRequestKind::ExchangeMetadata,
                    2_000,
                ),
                2_000,
            )
            .unwrap();
        assert!(engine
            .complete_execution_rules(&mut input, "xyz:AMD")
            .unwrap());
        assert_eq!(input.market_rules["xyz:AMD"].size_step, Decimal::new(1, 3));
        assert!(!engine.waiting_market_rules.contains("xyz:AMD"));
        assert_eq!(engine.target_ledger, durable_targets);
        let mut inconsistent = additional;
        inconsistent.universe[0].size_decimals = 2;
        assert!(engine
            .merge_hydrated_metadata(inconsistent, "xyz")
            .is_none());
        let cloid = engine.pending["BTC"].action.planned_cloid;
        engine.retire_no_action(cloid, 2_001);
        assert!(!engine.pending.contains_key("BTC"));
        assert_eq!(
            engine.action_lifecycle_events.last().unwrap().outcome,
            ActionAttemptOutcome::NoLongerRequired
        );
    }

    fn seed_positive_mfce_backoff(engine: &mut DecisionEngine, asset: &str) {
        let mut state = MfcePersistentState::default();
        for sample_id in 1..=8_u64 {
            state.samples.push_back(crate::mfce::MfceTrainingSample {
                sample_id,
                asset: asset.to_string(),
                direction: MfceDirection::Long,
                transition_kind: crate::mfce::MfceTransitionKind::Opening,
                opened_at_mono: sample_id * 1_000,
                completed_at_mono: sample_id * 1_000 + 500,
                features: MfceFeatureVector::new([Decimal::ZERO; MFCE_FEATURE_COUNT]),
                objective: crate::mfce::LearningObjective::EntryQuality,
                remaining_net_edge_bps: Decimal::from(100),
                lifetime_seconds: Decimal::new(5, 1),
                admitted: false,
            });
        }
        state.next_sample_id = 9;
        engine.mfce.replace_state(state).unwrap();
        engine.mfce_time_high_watermark = engine.mfce.state().time_high_watermark();
        engine.durable_time_offset = engine.mfce_time_high_watermark + 1;
    }

    fn mfce_test_book(
        asset: &str,
        source_time_ms: u64,
        bid: Decimal,
        ask: Decimal,
        quantity: Decimal,
    ) -> OrderBookResponse {
        OrderBookResponse {
            asset: asset.to_string(),
            source_time_ms,
            bids: vec![BookLevel {
                price: bid,
                quantity,
            }],
            asks: vec![BookLevel {
                price: ask,
                quantity,
            }],
        }
    }

    fn flow_engine(now: u64) -> DecisionEngine {
        let config = CopyTradeConfig::from_path(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json"),
        )
        .unwrap();
        let mut engine =
            DecisionEngine::new(config, b"flow-only", "flow-only", 40_000, 80_000).unwrap();
        engine.mids = Some((
            MarketSnapshotResponse {
                mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
            },
            now,
        ));
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![MarketMetadataAsset {
                name: "BTC".into(),
                size_decimals: 3,
                growth_mode: false,
            }],
            contexts: BTreeMap::from([(
                "BTC".into(),
                crate::public_mainnet::MarketAssetContext {
                    funding_rate_hourly: Decimal::ZERO,
                },
            )]),
            live_taker_fee_bps: Some(Decimal::ONE),
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        });
        engine.metadata_received_at = Some(now);
        engine.metadata_valid_until = Some(now + 40_000);
        engine.books.insert(
            "BTC".into(),
            (
                mfce_test_book(
                    "BTC",
                    now,
                    Decimal::new(9999, 2),
                    Decimal::new(10001, 2),
                    Decimal::from(100),
                ),
                now,
            ),
        );
        let mut flow = MarketFlow::default();
        flow.trade(now, 1, Decimal::from(10_000), true);
        flow.last_sent_ms = now;
        engine.ingest_market_flow("BTC".into(), flow, now);
        // Edge-only: seed Flow-origin positive labels so Flow probes have
        // positive edge (conservative>0) and admit Explore/Exploit.
        // Bootstrap (0/0/0) alone would Reject on costs; seeded history
        // gives the conditional quantiles to clear friction. Preserve
        // existing delayed flow/assets; only append training history.
        {
            let mut state = engine.mfce.state().clone();
            let base_id = state.next_sample_id;
            for offset in 0..8_u64 {
                let sample_id = base_id + offset;
                let mut features = MfceFeatureVector::new([Decimal::ZERO; MFCE_FEATURE_COUNT]);
                let mut context = [Decimal::ZERO; 16];
                context[11] = Decimal::ONE;
                context[12] = Decimal::ONE;
                features.context = Some(context);
                state.samples.push_back(crate::mfce::MfceTrainingSample {
                    sample_id,
                    asset: "BTC".to_string(),
                    direction: MfceDirection::Long,
                    transition_kind: crate::mfce::MfceTransitionKind::Opening,
                    opened_at_mono: sample_id * 1_000,
                    completed_at_mono: sample_id * 1_000 + 500,
                    features,
                    objective: crate::mfce::LearningObjective::EntryQuality,
                    remaining_net_edge_bps: Decimal::from(100),
                    lifetime_seconds: Decimal::new(5, 1),
                    admitted: false,
                });
            }
            state.next_sample_id = base_id + 8;
            // Keep ring bound intact.
            while state.samples.len() > crate::mfce::MFCE_MAX_SAMPLES {
                state.samples.pop_front();
            }
            engine.mfce.replace_state(state).unwrap();
        }
        engine
    }

    #[test]
    fn pure_flow_can_explore_without_source_members_and_keeps_risk_limits() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        let decision = engine.construct_next_decision(now).unwrap().unwrap();
        assert_eq!(engine.metrics.accepted_source_snapshots, 0);
        // Edge-only: seeded +100bps Flow history gives net_q10>1 => Exploit
        // (full size). Old support/model gates would have held this to
        // Explore; edge-only concentrates on proven lower-decile edge.
        assert_eq!(
            engine.mfce.policy_output("BTC").unwrap().policy_state,
            crate::mfce::MfcePolicyState::Exploit
        );
        let sample = engine.mfce.delayed().samples.back().unwrap();
        assert_eq!(sample.origin, AlphaOrigin::Flow);
        assert_eq!(sample.features.values[0], Decimal::ZERO);
        assert_eq!(sample.features.context.unwrap()[12], Decimal::ONE);
        assert!(sample.anchor.is_some());
        assert!(!sample.prediction_curve.is_empty());
        assert!(sample
            .prediction_curve
            .iter()
            .all(|point| point.q10_net_bps <= point.q50_net_bps));
        assert_eq!(
            sample
                .prediction_curve
                .iter()
                .map(|point| point.model_epoch)
                .collect::<BTreeSet<_>>()
                .len(),
            1
        );
        let selected = sample.prediction.as_ref().unwrap();
        let selected_curve = &sample.prediction_curve[0];
        assert_eq!(selected.horizon_ms, selected_curve.horizon_ms);
        assert_eq!(selected.model_epoch, selected_curve.model_epoch);
        assert_eq!(selected.objective, selected_curve.objective);
        assert_eq!(selected.objective, sample.features.objective());
        assert_eq!(
            selected.feature_snapshot_id,
            selected_curve.feature_snapshot_id
        );
        assert_eq!(
            selected.feature_snapshot_id,
            sample.features.snapshot_id().unwrap()
        );
        assert_ne!(selected.feature_snapshot_id, [0; 32]);
        assert_eq!(selected.predicted_q10_net_edge, selected_curve.q10_net_bps);
        assert_eq!(selected.predicted_net_edge, selected_curve.q50_net_bps);
        assert_eq!(
            selected.predicted_q10_gross_edge - selected.expected_friction,
            selected.predicted_q10_net_edge
        );
        assert_eq!(
            selected.predicted_gross_edge - selected.expected_friction,
            selected.predicted_net_edge
        );
        assert_eq!(
            sample
                .prediction_curve
                .iter()
                .map(|point| point.feature_snapshot_id)
                .collect::<BTreeSet<_>>()
                .len(),
            sample.prediction_curve.len()
        );
        let frozen = sample.features.clone();
        assert_eq!(
            frozen,
            engine.mfce.state().assets["BTC"]
                .active
                .as_ref()
                .unwrap()
                .features
        );
        assert!(sample
            .larger_anchors
            .iter()
            .all(|a| a.quantity * a.midpoint <= Decimal::from(65)));
        assert!(engine.pending.contains_key("BTC"));
        assert!(decision.projection.constrained_targets["BTC"] <= Decimal::from(65));
        let before = engine.pending["BTC"].action.clone();
        engine.service_delayed_learning(now + 14_403_000);
        assert_eq!(engine.pending["BTC"].action, before);
        assert_eq!(engine.mfce.delayed().samples[0].features, frozen);
        assert!(engine.mfce.delayed().samples[0]
            .forward
            .iter()
            .all(Option::is_none));
        // Missing context must still hold the filled slice after the active
        // increase transition has retired, including the second recomputation.
        engine.pending.clear();
        engine.production_positions = Some(BTreeMap::from([("BTC".into(), Decimal::new(25, 2))]));
        engine.learning_stream_gap();
        let mut normalized_target = None;
        for at in [now + 1_000, now + 2_000] {
            // Shadow compatibility may still materialize a no-action decision;
            // production suspends earlier through its identity gate. Neither
            // path may mutate the held target or create an executable root.
            let _ = engine.construct_next_decision(at).unwrap();
            assert_eq!(engine.authoritative_position("BTC"), Decimal::new(25, 2));
            assert_eq!(
                engine.target_ledger.get("BTC").unwrap().filled_notional,
                Decimal::from(25)
            );
            if let Some(previous) = &normalized_target {
                assert_eq!(&engine.target_ledger, previous);
            } else {
                normalized_target = Some(engine.target_ledger.clone());
            }
            assert!(!engine.pending.contains_key("BTC"));
            assert_eq!(
                engine.mfce.state().assets["BTC"].target_origin,
                Some(AlphaOrigin::Flow)
            );
        }
    }

    #[test]
    fn recovered_short_without_current_strategy_direction_holds_only_that_asset() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.construct_next_decision(now).unwrap().unwrap();
        engine.pending.clear();
        engine.production_positions = Some(BTreeMap::from([("BTC".into(), Decimal::new(-6, 2))]));
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });
        engine.learning_stream_gap();
        for at in [now + 1_000, now + 2_000] {
            let decision = engine.construct_next_decision(at).unwrap().unwrap();
            assert_eq!(engine.authoritative_position("BTC"), Decimal::new(-6, 2));
            assert_eq!(
                decision.projection.constrained_targets["BTC"],
                Decimal::from(-6)
            );
            assert!(engine.strategy_target_execution_enabled());
            assert!(engine.take_prepared_authorized_intents().is_empty());
            assert!(engine.pending.is_empty());
        }
        assert_eq!(engine.metrics.target_readiness_rejections, 2);
        engine.mids = None;
        assert!(engine
            .construct_next_decision(now + 3_000)
            .unwrap()
            .is_none());
        assert!(
            engine.strategy_target_execution_enabled(),
            "a no-op planning pass must not turn an asset-scoped hold into a global blocker"
        );
    }

    #[test]
    fn unavailable_manual_hold_retires_existing_pending_root() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });
        engine.construct_next_decision(now).unwrap().unwrap();
        let selected = engine.pending["BTC"].action.clone();

        engine.production_positions = Some(BTreeMap::from([("BTC".into(), Decimal::new(-6, 2))]));
        engine.learning_stream_gap();
        engine
            .construct_next_decision(now + 1_000)
            .unwrap()
            .unwrap();

        assert!(engine.pending.is_empty());
        assert!(engine.pending_book.is_empty());
        assert!(engine.take_prepared_authorized_intents().is_empty());
        assert!(engine.strategy_target_execution_enabled());
        assert!(engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == selected.planned_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::BlockedByCurrentRisk
        }));
    }

    #[test]
    fn malformed_nonzero_target_without_same_side_component_is_unavailable() {
        let opposite = BTreeMap::from([("source:wallet".into(), Decimal::from(-1))]);
        assert_eq!(
            absolute_directional_component_targets(Some(&opposite), Decimal::from(10)).unwrap(),
            None
        );
        assert_eq!(
            absolute_directional_component_targets(None, Decimal::from(10)).unwrap(),
            None
        );
    }

    #[test]
    fn held_component_attribution_preserves_repeating_decimal_quantities_exactly() {
        let first = Decimal::from_str_exact("0.0342219590347383045321807012").unwrap();
        let second = Decimal::from_str_exact("0.0256664692760537283991355254").unwrap();
        let mark = Decimal::from_str_exact("125.86").unwrap();
        let quantities = BTreeMap::from([
            ("source:first".into(), first),
            ("source:second".into(), second),
        ]);

        let components = exact_component_notionals(quantities, mark).unwrap();

        assert_eq!(components["source:first"], first * mark);
        assert_eq!(components["source:second"], second * mark);
    }

    #[test]
    fn rejecting_a_marginal_add_preserves_the_open_position() {
        let current = Decimal::from(20);
        let desired = Decimal::from(50);

        let (objective, proposed, rejected) = position_evaluation_targets(current, desired);

        assert_eq!(objective, LearningObjective::SizeQuality);
        assert_eq!(proposed, desired);
        assert_eq!(rejected, current);
    }

    #[test]
    fn rejecting_negative_hold_edge_can_exit_the_open_position() {
        let current = Decimal::from(20);

        let (objective, proposed, rejected) = position_evaluation_targets(current, current);

        assert_eq!(objective, LearningObjective::ContinuationQuality);
        assert_eq!(proposed, current);
        assert_eq!(rejected, Decimal::ZERO);
    }

    #[test]
    fn churn_rejected_continuation_holds_instead_of_flattening() {
        // R1 (AAVE noise): small oscillations must HOLD, not OPEN->EXIT->OPEN.
        for current in [Decimal::from(20), Decimal::from(-20)] {
            let (_, _, rejected) = position_evaluation_targets(current, current);
            assert_eq!(rejected, Decimal::ZERO);
            // Neutral same-side re-evaluation holds the valid exposure.
            assert_eq!(
                continuation_hold_target(current, current, rejected),
                current
            );
            // Weak reduction signal also holds core exposure; marginal
            // REDUCE is a separate sized decision, never auto-flatten.
            let weaker = if current.is_sign_positive() {
                current - Decimal::ONE
            } else {
                current + Decimal::ONE
            };
            let (_, _, rejected) = position_evaluation_targets(current, weaker);
            assert_eq!(continuation_hold_target(current, weaker, rejected), current);
        }
    }

    #[test]
    fn churn_true_reversal_bypasses_hold_stickiness_symmetrically() {
        // R9: strong opposite thesis exits/reverses without cooldown, LONG/SHORT symmetric.
        for (current, desired) in [
            (Decimal::from(20), Decimal::from(-20)),
            (Decimal::from(-20), Decimal::from(20)),
        ] {
            let (_, _, rejected) = position_evaluation_targets(current, current);
            assert_eq!(
                continuation_hold_target(current, desired, rejected),
                rejected
            );
        }
    }

    #[test]
    fn churn_buffered_fee_hurdle_suppresses_marginal_turnover() {
        // Edge-only: fee-multiplier buffer removed from hurdle; base friction
        // only. Settled accounting keeps actuals. HOLD and new risk both use
        // base.
        let base = Decimal::new(10, 0);
        let taker = Decimal::new(45, 1);
        let mult = Decimal::from(4);
        let buffered = buffered_policy_friction_bps(base, taker, mult, true).unwrap();
        assert_eq!(buffered, Decimal::new(10, 0));
        // HOLD adds no new fee: base as well.
        assert_eq!(
            buffered_policy_friction_bps(base, taker, mult, false).unwrap(),
            base
        );
        // Switching cost helper retained for accounting diagnostics only;
        // it no longer gates admission.
        let switching = same_side_switching_cost_bps(taker, Decimal::from(4), mult).unwrap();
        assert_eq!(switching, Decimal::new(44, 0));
        // Symmetry: identical magnitude for LONG/SHORT (fee/slip symmetric).
        assert_eq!(
            same_side_switching_cost_bps(taker, Decimal::from(4), mult).unwrap(),
            switching
        );
    }

    #[test]
    fn churn_reentry_penalty_is_economic_not_a_cooldown() {
        // Edge-only: re-entry penalty removed from hurdle; base friction
        // only. reentry_probe_allowed always true (diagnostics only).
        let taker = Decimal::new(45, 1);
        let slip = Decimal::from(4);
        let mult = Decimal::from(4);
        let buffered =
            buffered_policy_friction_bps(Decimal::new(10, 0), taker, mult, true).unwrap();
        assert_eq!(buffered, Decimal::new(10, 0));
        let _reacquire = same_side_switching_cost_bps(taker, slip, mult).unwrap();
        let penalized = buffered;
        assert_eq!(penalized, Decimal::new(10, 0));

        let candidate =
            |q10: i64, q50: i64, used_model: bool, proposed: Decimal| MfceAllocationInput {
                prediction: crate::mfce::MfcePrediction {
                    model_epoch: if used_model { 3 } else { 0 },
                    q10_gross_bps: Decimal::from(q10),
                    q50_gross_bps: Decimal::from(q50),
                    uncertainty_bps: Decimal::from(20),
                    pooled_sample_count: 128,
                    direction_sample_count: if used_model { 64 } else { 0 },
                    asset_direction_sample_count: if used_model { 16 } else { 0 },
                    used_model,
                },
                friction_bps: penalized,
                copytrade_conviction: Decimal::new(5, 1),
                current_position_notional: Decimal::ZERO,
                proposed_position_notional: proposed,
                remaining_tail_loss_budget_usd: Decimal::from(1_000),
            };
        for proposed in [Decimal::from(100), Decimal::from(-100)] {
            // Edge-only: weak state with base friction still Explores when
            // upper>0; re-entry never vetoes.
            let weak = evaluate_allocation_policy(&candidate(-50, 5, false, proposed)).unwrap();
            assert_eq!(weak.policy_state, crate::mfce::MfcePolicyState::Explore);
            assert!(weak.conservative_edge_bps < Decimal::ZERO);
            assert!(reentry_probe_allowed(
                true,
                false,
                weak.policy_state,
                weak.conservative_edge_bps
            ));
            assert!(reentry_probe_allowed(
                false,
                false,
                weak.policy_state,
                weak.conservative_edge_bps
            ));

            // Materially stronger state: edge clears base hurdle → Exploit.
            let strong = evaluate_allocation_policy(&candidate(200, 300, true, proposed)).unwrap();
            assert_eq!(strong.policy_state, crate::mfce::MfcePolicyState::Exploit);
            assert!(reentry_probe_allowed(
                true,
                true,
                strong.policy_state,
                strong.conservative_edge_bps
            ));
        }
    }

    #[test]
    fn churn_canonical_notional_ignores_export_fee_and_amount() {
        // R11: exported fee=0 / wrong USDAmount never drive economics.
        // Canonical fill notional is abs(size*price); authenticated fee truth
        // comes from VerifiedExchangeFill.fee_amount.
        let size = Decimal::new(-16, 2);
        let price = Decimal::new(12648, 2);
        let canonical = size.checked_mul(price).unwrap().abs();
        assert_eq!(canonical, Decimal::new(202368, 4));
        assert!(canonical > Decimal::ZERO);
        // Zero export fee must not zero the authenticated fee path: the
        // production fill path carries fee_amount independently.
        let export_fee = Decimal::ZERO;
        let authenticated_fee = Decimal::new(1, 2);
        assert_ne!(export_fee, authenticated_fee);
    }

    #[test]
    fn profitable_flow_reaches_allocation_and_hard_risk_without_source_events() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        seed_positive_mfce_backoff(&mut engine, "BTC");
        let mut state = engine.mfce.state().clone();
        for sample in &mut state.samples {
            let mut context = [Decimal::ZERO; 16];
            context[11] = Decimal::ONE;
            context[12] = Decimal::ONE;
            sample.features.context = Some(context);
        }
        engine.mfce.replace_state(state).unwrap();
        let durable = engine.durable_timestamp(now).unwrap();
        let mut flow = MarketFlow::default();
        flow.trade(durable, 1, Decimal::from(10_000), true);
        flow.last_sent_ms = durable;
        engine.ingest_market_flow("BTC".into(), flow, now);
        let decision = engine.construct_next_decision(now).unwrap().unwrap();
        assert_eq!(engine.metrics.accepted_source_snapshots, 0);
        let output = engine.mfce.policy_output("BTC").unwrap();
        assert!(output.q50_gross_bps > Decimal::ZERO);
        assert!(output.net_q50_bps > Decimal::ZERO);
        assert_eq!(
            output.net_q50_bps,
            output.q50_gross_bps - output.friction_bps
        );
        assert!(matches!(
            output.policy_state,
            crate::mfce::MfcePolicyState::Explore | crate::mfce::MfcePolicyState::Exploit
        ));
        let sample = engine.mfce.delayed().samples.back().unwrap();
        assert_eq!(sample.origin, AlphaOrigin::Flow);
        assert!(sample.anchor.is_some());
        assert_eq!(sample.features.values[3], Decimal::ZERO);
        let action = &engine.pending["BTC"].action;
        assert!(!action.reduce_only);
        assert!(action.rounded_notional > Decimal::ZERO);
        assert!(decision.projection.constrained_targets["BTC"] > Decimal::ZERO);
        assert!(decision.projection.constrained_targets["BTC"] <= Decimal::from(65));
    }

    #[test]
    fn selected_flow_probe_survives_same_side_repricing_until_one_issue() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });
        engine.construct_next_decision(now).unwrap().unwrap();
        let selected = engine.pending["BTC"].action.clone();

        for (offset, id, notional) in [(5, 2, 20_000), (10, 3, 35_000)] {
            let at = now + offset;
            let durable = engine.durable_timestamp(at).unwrap();
            let mut flow = engine.mfce.delayed().flow["BTC"].clone();
            flow.trade(durable, id, Decimal::from(notional), true);
            flow.last_sent_ms = durable;
            engine.ingest_market_flow("BTC".into(), flow, at);
            engine.construct_next_decision(at).unwrap().unwrap();

            assert_eq!(engine.pending["BTC"].action, selected);
            assert_eq!(engine.unresolved_actionable_root_count(), 1);
            assert!(!engine.action_lifecycle_events.iter().any(|event| {
                event.planned_cloid == selected.planned_cloid.to_string()
                    && event.outcome == ActionAttemptOutcome::SupersededByNewTarget
            }));
        }

        let book = engine.books["BTC"].0.clone();
        engine
            .try_execute_pending(&book, now + engine.config.latency_timeout_ms)
            .unwrap();
        let intents = engine.take_prepared_authorized_intents();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].planned_cloid, selected.planned_cloid);
        assert_eq!(engine.unresolved_actionable_root_count(), 1);
    }

    #[test]
    fn streamed_book_executes_existing_mfce_pending_before_recompute() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });
        engine.construct_next_decision(now).unwrap().unwrap();
        let selected = engine.pending["BTC"].action.clone();
        let at = now + engine.config.latency_timeout_ms;
        let book = mfce_test_book(
            "BTC",
            engine.durable_timestamp(at).unwrap(),
            Decimal::new(9_999, 2),
            Decimal::new(10_001, 2),
            Decimal::from(1_000),
        );

        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(book),
                    ReadRequestKind::OrderBook,
                    at,
                ),
                at,
            )
            .unwrap();

        let intents = engine.take_prepared_authorized_intents();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].planned_cloid, selected.planned_cloid);
        assert!(!engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == selected.planned_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::SupersededByNewTarget
        }));
    }

    #[test]
    fn selected_flow_probe_keeps_its_bounded_candidate_rank() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });
        engine.construct_next_decision(now).unwrap().unwrap();
        let selected = engine.pending["BTC"].action.clone();

        for index in 0..32 {
            let asset = format!("A{index:02}");
            engine
                .mids
                .as_mut()
                .unwrap()
                .0
                .mids
                .insert(asset.clone(), Decimal::from(100));
            let metadata = engine.metadata.as_mut().unwrap();
            metadata.universe.push(MarketMetadataAsset {
                name: asset.clone(),
                size_decimals: 3,
                growth_mode: false,
            });
            metadata.contexts.insert(
                asset.clone(),
                crate::public_mainnet::MarketAssetContext {
                    funding_rate_hourly: Decimal::ZERO,
                },
            );
            engine.books.insert(
                asset.clone(),
                (
                    mfce_test_book(
                        &asset,
                        now + 5,
                        Decimal::new(9_999, 2),
                        Decimal::new(10_001, 2),
                        Decimal::from(100),
                    ),
                    now + 5,
                ),
            );
            let durable = engine.durable_timestamp(now + 5).unwrap();
            let mut flow = MarketFlow::default();
            flow.trade(durable, index + 10, Decimal::from(10_000), true);
            flow.last_sent_ms = durable;
            engine.ingest_market_flow(asset, flow, now + 5);
        }

        engine.construct_next_decision(now + 5).unwrap().unwrap();

        assert_eq!(engine.pending["BTC"].action, selected);
        assert_eq!(
            engine
                .pending
                .values()
                .filter(|pending| pending.action.asset == "BTC")
                .count(),
            1
        );
        assert!(!engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == selected.planned_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::SupersededByNewTarget
        }));
    }

    #[test]
    fn unattributable_manual_portfolio_delta_rejects_order_without_killing_engine() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });

        let manual_open = crossing_test_execution(
            "BTC",
            Side::Buy,
            Decimal::ONE,
            Decimal::from(100),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        engine
            .ledger
            .apply_portfolio_execution(&manual_open, 1)
            .unwrap();
        assert_eq!(engine.ledger.portfolio_position("BTC"), Decimal::ONE);
        assert!(engine.ledger.source_positions_for_asset("BTC").is_empty());

        let decision = engine.construct_next_decision(now).unwrap().unwrap();

        assert!(decision.projection.constrained_targets.contains_key("BTC"));
        assert!(engine.pending.is_empty());
        assert!(engine.take_prepared_authorized_intents().is_empty());
        assert!(engine.strategy_target_execution_enabled());
        assert!(engine.action_lifecycle_events.iter().any(|event| {
            event.asset == "BTC" && event.outcome == ActionAttemptOutcome::BlockedByCurrentRisk
        }));
    }

    #[test]
    fn partition_component_core_failures_are_attribution_integrity() {
        struct Case {
            name: &'static str,
            current: BTreeMap<String, Decimal>,
            side: Side,
            filled: Decimal,
            targets: BTreeMap<String, Decimal>,
            after: Decimal,
            reason: &'static str,
        }

        let cases = [
            Case {
                name: "flatten_same_side_component",
                current: BTreeMap::from([("source:manual".into(), Decimal::ONE)]),
                side: Side::Buy,
                filled: Decimal::ONE,
                targets: BTreeMap::new(),
                after: Decimal::ZERO,
                reason: "portfolio flatten does not exclusively close component positions",
            },
            Case {
                name: "flatten_quantity_mismatch",
                current: BTreeMap::from([("source:manual".into(), -Decimal::ONE)]),
                side: Side::Buy,
                filled: Decimal::from(2),
                targets: BTreeMap::new(),
                after: Decimal::ZERO,
                reason: "portfolio flatten component quantities do not reconcile",
            },
            Case {
                name: "crossing_missing_old_side_close",
                current: BTreeMap::new(),
                side: Side::Sell,
                filled: Decimal::from(2),
                targets: BTreeMap::from([("source:probe".into(), -Decimal::ONE)]),
                after: -Decimal::ONE,
                reason: "crossing fill old-side component close quantity does not reconcile",
            },
            Case {
                name: "crossing_no_new_side_target",
                current: BTreeMap::from([("source:long".into(), Decimal::ONE)]),
                side: Side::Sell,
                filled: Decimal::from(2),
                targets: BTreeMap::new(),
                after: -Decimal::ONE,
                reason: "crossing fill has no matching new-side component target",
            },
            Case {
                name: "missing_directional_delta",
                current: BTreeMap::new(),
                side: Side::Buy,
                filled: Decimal::ONE,
                targets: BTreeMap::new(),
                after: Decimal::ONE,
                reason: "filled portfolio delta has no matching component target delta",
            },
        ];

        for case in cases {
            let error = partition_component_fill(
                &case.current,
                case.side,
                case.filled,
                &case.targets,
                Decimal::ONE,
                case.after,
                Decimal::new(1, 6),
            )
            .expect_err(case.name);
            match error {
                EngineError::Core(reason) => assert_eq!(reason, case.reason, "{}", case.name),
                other => panic!("{} returned non-Core error: {other:?}", case.name),
            }
        }
    }

    #[test]
    fn selected_flow_probe_is_not_issued_without_current_mfce_authorization() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: 10_000,
        });
        engine.construct_next_decision(now).unwrap().unwrap();
        let selected = engine.pending["BTC"].action.clone();
        engine
            .mfce
            .replace_state(engine.mfce.state().clone())
            .unwrap();

        let book = engine.books["BTC"].0.clone();
        engine
            .try_execute_pending(&book, now + engine.config.latency_timeout_ms)
            .unwrap();

        assert!(!engine.pending.contains_key("BTC"));
        assert!(engine.take_prepared_authorized_intents().is_empty());
        assert!(engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == selected.planned_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::BlockedByCurrentRisk
        }));
    }

    #[test]
    fn stale_mfce_policy_output_is_neither_reported_nor_executable() {
        let now = 60_000;
        let mut engine = flow_engine(now);
        let maximum_age = engine.config.global_risk.source_snapshot_max_age_ms;
        engine.enable_production_intents(ProductionIntentIdentity {
            observer_release_hash: [1; 32],
            signer_release_hash: [1; 32],
            release_manifest_hash: [2; 32],
            market_rules_hash: [3; 32],
            dynamic_floor_policy_hash: [4; 32],
            ioc_policy_hash: [5; 32],
            expires_after_ms: maximum_age + engine.config.latency_timeout_ms + 10_000,
        });
        engine.construct_next_decision(now).unwrap().unwrap();
        let selected = engine.pending["BTC"].action.clone();
        assert!(engine
            .fresh_mfce_report_at(engine.durable_timestamp(now).unwrap())
            .policy_outputs
            .iter()
            .any(|output| output.asset == "BTC"));

        let stale_at = now + maximum_age + engine.config.latency_timeout_ms + 1;
        assert!(engine
            .fresh_mfce_report_at(engine.durable_timestamp(stale_at).unwrap())
            .policy_outputs
            .iter()
            .all(|output| output.asset != "BTC"));

        let mut book = engine.books["BTC"].0.clone();
        book.source_time_ms = stale_at;
        engine.try_execute_pending(&book, stale_at).unwrap();

        assert!(!engine.pending.contains_key("BTC"));
        assert!(engine.take_prepared_authorized_intents().is_empty());
        assert!(engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == selected.planned_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::BlockedByCurrentRisk
        }));
    }

    #[test]
    fn selected_flow_probe_retires_when_target_disappears_or_reverses() {
        let now = 60_000;
        for reverse in [false, true] {
            let mut engine = flow_engine(now);
            engine.enable_production_intents(ProductionIntentIdentity {
                observer_release_hash: [1; 32],
                signer_release_hash: [1; 32],
                release_manifest_hash: [2; 32],
                market_rules_hash: [3; 32],
                dynamic_floor_policy_hash: [4; 32],
                ioc_policy_hash: [5; 32],
                expires_after_ms: 10_000,
            });
            engine.construct_next_decision(now).unwrap().unwrap();
            let selected = engine.pending["BTC"].action.clone();
            if reverse {
                let at = now + 5;
                let durable = engine.durable_timestamp(at).unwrap();
                let mut flow = MarketFlow::default();
                flow.trade(durable, 2, Decimal::from(50_000), false);
                flow.last_sent_ms = durable;
                engine.ingest_market_flow("BTC".into(), flow, at);
            } else {
                engine.mfce.delayed_mut().flow.clear();
            }

            engine.construct_next_decision(now + 5).unwrap();

            if reverse {
                let replacement = &engine.pending["BTC"].action;
                assert_ne!(replacement.planned_cloid, selected.planned_cloid);
                assert_ne!(replacement.side, selected.side);
            } else {
                assert!(!engine.pending.contains_key("BTC"));
            }
            assert!(engine.action_lifecycle_events.iter().any(|event| {
                event.planned_cloid == selected.planned_cloid.to_string()
                    && event.outcome == ActionAttemptOutcome::SupersededByNewTarget
            }));
            assert!(engine.take_prepared_authorized_intents().is_empty());
        }
    }

    #[test]
    fn selected_risk_increase_ownership_requires_same_nonzero_direction() {
        let (mut engine, _) = live_fill_engine("BTC");
        let pending = engine.pending.get_mut("BTC").unwrap();
        pending.action.reduce_only = false;
        pending.action.side = Side::Sell;
        pending.mfce_lineage = MfceExecutionLineage {
            policy_state: Some(crate::mfce::MfcePolicyState::Explore),
            admitted: true,
            risk_increase_authorized: true,
            ..MfceExecutionLineage::default()
        };
        let context = |desired| MfceTargetContext {
            raw_desired_source_target: desired,
            current_source_target: Decimal::ZERO,
            current_source_targets: BTreeMap::new(),
        };

        assert!(owns_unissued_risk_increase(
            pending,
            false,
            &context(Decimal::from(-20))
        ));
        assert!(!owns_unissued_risk_increase(
            pending,
            false,
            &context(Decimal::ZERO)
        ));
        assert!(!owns_unissued_risk_increase(
            pending,
            false,
            &context(Decimal::from(20))
        ));
    }

    #[test]
    fn source_episode_migrates_to_flow_without_trade_and_survives_restart() {
        let now = 60_000;
        let (mut engine, mut fill) = live_fill_engine("BTC");
        // Edge-only cold start: break-even upper (net=0, unc=0) admits
        // Explore so the Flow migration may expose a larger target as a
        // same-direction probe. It must never become an exit or reversal;
        // provenance migration alone authorizes no opposite-side execution.
        fill.filled_quantity = Decimal::new(36, 2);
        // Frozen source opening attribution is a durable decision-time fact.
        let cloid = fill.identity.cloid.to_string();
        engine.mfce.delayed_mut().provenance.actions.insert(
            cloid.clone(),
            crate::mfce_delayed::ActionProvenance {
                decision_at: fill.decision_timestamp,
                origin: AlphaOrigin::Source,
                episodes: BTreeSet::new(),
            },
        );
        engine.apply_live_execution_fill(&fill).unwrap();
        engine.pending.clear(); // predecessor IOC's remainder is canceled
        let episode_id = engine.ledger.portfolio_open_episode_id("BTC").unwrap();
        let opening = engine.mfce.delayed().provenance.actions[&cloid].clone();
        let flow = flow_engine(now);
        engine.mids = flow.mids;
        engine.metadata = flow.metadata;
        engine.metadata_received_at = flow.metadata_received_at;
        engine.metadata_valid_until = flow.metadata_valid_until;
        engine.books = flow.books;
        engine.ingest_market_flow("BTC".into(), flow.mfce.delayed().flow["BTC"].clone(), now);
        let execution_count = engine.executions.len();
        engine.construct_next_decision(now).unwrap().unwrap();
        assert_eq!(
            engine.mfce.state().assets["BTC"].target_origin,
            Some(AlphaOrigin::Flow)
        );
        let continuation = engine.mfce.delayed().samples.back().unwrap();
        // Edge-only: break-even upper (net=0, unc=0) admits Explore so cold
        // start can buy labels. The Flow target may exceed current, yielding
        // Add kind; the sampled continuation still records zero actual_delta
        // while any pending probe remains a same-direction risk increase.
        assert!(
            matches!(
                continuation.kind,
                DecisionKind::Hold
                    | DecisionKind::Reduce
                    | DecisionKind::BudgetConstrained
                    | DecisionKind::Add
            ),
            "unexpected continuation kind: {:?}",
            continuation.kind
        );
        assert_eq!(continuation.origin, AlphaOrigin::Flow);
        // Edge-only: negative upper edge Rejects (Hold) rather than Exploring
        // at a loss; break-even upper Explores (Add/Hold with zero sampled
        // delta). Old support gate would have given Explore here even at a loss.
        assert!(
            matches!(
                continuation.mode,
                Some(crate::mfce::MfcePolicyState::Explore)
                    | Some(crate::mfce::MfcePolicyState::Reject)
            ),
            "unexpected continuation mode: {:?}",
            continuation.mode
        );
        assert!(continuation.actual_delta.is_zero());
        assert_eq!(
            continuation.features.objective(),
            LearningObjective::SizeQuality
        );
        assert!(continuation
            .features
            .position
            .as_ref()
            .and_then(|position| position.trajectory.as_ref())
            .is_some());
        // Break-even Explore may leave a same-direction probe pending; it
        // must never be an exit, reversal, or reduce-only flattening.
        if engine.pending.is_empty() {
            // Reject/Hold path: no new execution on migration.
        } else {
            let pending = engine.pending.get("BTC").expect("BTC probe");
            assert_eq!(pending.action.side, Side::Buy);
            assert!(!pending.action.reduce_only);
            assert!(matches!(
                pending.mfce_lineage.policy_state,
                Some(crate::mfce::MfcePolicyState::Explore)
            ));
            assert!(pending.mfce_lineage.admitted);
            assert!(pending.mfce_lineage.risk_increase_authorized);
        }
        assert_eq!(engine.executions.len(), execution_count);
        assert_eq!(
            engine.ledger.portfolio_position("BTC"),
            fill.filled_quantity
        );
        let history = engine.mfce.delayed().provenance.clone();
        assert_eq!(
            history.episodes[&episode_id]
                .transitions
                .iter()
                .map(|(_, origin)| *origin)
                .collect::<Vec<_>>(),
            vec![AlphaOrigin::Source, AlphaOrigin::Flow]
        );
        assert_eq!(history.actions[&cloid], opening);
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("unsigned-observer-state.msgpack");
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "engine".into(),
            configuration_sha256: "config".into(),
            risk_policy_sha256: "risk".into(),
        };
        engine.persist_unsigned_state(&file, &identity).unwrap();
        let (mut restarted, _) = live_fill_engine("BTC");
        restarted.restore_unsigned_state(&file, &identity).unwrap();
        restarted.rebase_runtime_time(0, now + 1_000).unwrap();
        assert_eq!(
            restarted.ledger.portfolio_open_episode_id("BTC"),
            Some(episode_id)
        );
        assert_eq!(restarted.mfce.delayed().provenance, history);
        assert_eq!(
            restarted.ledger.portfolio_position("BTC"),
            fill.filled_quantity
        );
        // Pending Explore probes are durable across restart; Reject/Hold
        // leaves none. Either way the open episode and attribution survive.
        if engine.pending.is_empty() {
            assert!(restarted.pending.is_empty());
        } else {
            assert_eq!(restarted.pending.len(), engine.pending.len());
            let probe = restarted.pending.get("BTC").expect("BTC probe");
            assert_eq!(probe.action.side, Side::Buy);
            assert!(!probe.action.reduce_only);
        }
    }

    #[test]
    fn all_alpha_origins_reach_the_same_authorized_engine_intent_path() {
        for origin in [
            AlphaOrigin::Source,
            AlphaOrigin::Flow,
            AlphaOrigin::SourceFlowConfluence,
        ] {
            let now = 60_000;
            let mut engine = flow_engine(now);
            engine.config.candidates.truncate(1);
            engine.config.candidates[0].allocation_weight = 1.0;
            seed_positive_mfce_backoff(&mut engine, "BTC");
            let mut state = engine.mfce.state().clone();
            for sample in &mut state.samples {
                let mut context = [Decimal::ZERO; 16];
                context[11] = Decimal::from(match origin {
                    AlphaOrigin::Source => 0,
                    AlphaOrigin::Flow => 1,
                    AlphaOrigin::SourceFlowConfluence => 2,
                });
                context[12] = mfce_indicator(origin == AlphaOrigin::Flow);
                sample.features.context = Some(context);
            }
            engine.mfce.replace_state(state).unwrap();
            engine.enable_production_intents(ProductionIntentIdentity {
                observer_release_hash: [1; 32],
                signer_release_hash: [1; 32],
                release_manifest_hash: [2; 32],
                market_rules_hash: [3; 32],
                dynamic_floor_policy_hash: [4; 32],
                ioc_policy_hash: [5; 32],
                expires_after_ms: 10_000,
            });
            if origin == AlphaOrigin::Source {
                engine.mfce.delayed_mut().flow.clear();
            } else {
                let durable = engine.durable_timestamp(now).unwrap();
                let mut flow = MarketFlow::default();
                flow.trade(durable, 1, Decimal::from(10_000), true);
                flow.last_sent_ms = durable;
                engine.ingest_market_flow("BTC".into(), flow, now);
            }
            if origin != AlphaOrigin::Flow {
                let wallet = engine.config.candidates[0].address.to_ascii_lowercase();
                engine.set_source_tier(&wallet, SourceTier::Active);
                engine
                    .ingest(
                        accepted(
                            PublicPayload::SourceState(SourceStateResponse {
                                block_number: None,
                                candidate_id: wallet,
                                account_value: Decimal::from(1000),
                                source_time_ms: now,
                                positions: BTreeMap::from([(
                                    "BTC".into(),
                                    SourceAssetPosition {
                                        asset: "BTC".into(),
                                        signed_size: Decimal::from(10),
                                        signed_notional: Decimal::from(1000),
                                        entry_price: Some(Decimal::from(100)),
                                        unrealized_pnl: Some(Decimal::ZERO),
                                    },
                                )]),
                                closed_candles: vec![],
                            }),
                            ReadRequestKind::SourceState,
                            now,
                        ),
                        now,
                    )
                    .unwrap();
            }
            engine.construct_next_decision(now).unwrap();
            let sample = engine.mfce.delayed().samples.back().unwrap();
            assert_eq!(sample.origin, origin);
            assert!(sample.anchor.is_some());
            let output = engine.mfce.policy_output("BTC").unwrap();
            assert!(matches!(
                output.policy_state,
                crate::mfce::MfcePolicyState::Explore | crate::mfce::MfcePolicyState::Exploit
            ));
            assert!(output.net_q50_bps > Decimal::ZERO, "{origin:?}: {output:?}");
            let book = engine.books["BTC"].0.clone();
            engine
                .try_execute_pending(&book, now + engine.config.latency_timeout_ms)
                .unwrap();
            let intents = engine.take_prepared_authorized_intents();
            assert_eq!(intents.len(), 1, "{origin:?}: {:?}", engine.pending);
            assert!(!engine.pending["BTC"].action.reduce_only);
            assert!(engine.pending["BTC"].action.rounded_notional <= Decimal::from(65));
        }
    }

    fn warm_flat_technical_context(engine: &mut DecisionEngine, asset: &str) {
        for interval in CandleInterval::ALL {
            for index in 0..interval.warmup_candles() {
                let duration = interval.duration_ms();
                let open_time_ms = u64::try_from(index).unwrap() * duration;
                engine
                    .technical_engine
                    .accept_closed_candle(ClosedCandle {
                        asset: asset.to_string(),
                        interval,
                        open_time_ms,
                        close_time_ms: open_time_ms + duration,
                        open: Decimal::from(100),
                        high: Decimal::from(101),
                        low: Decimal::from(99),
                        close: Decimal::from(100),
                        base_volume: Decimal::from(10),
                        trade_count: 5,
                    })
                    .unwrap();
            }
        }
        assert!(engine
            .technical_engine
            .feature_context(asset)
            .unwrap()
            .is_some());
    }

    #[test]
    fn mfce_execution_friction_is_external_to_gross_prediction() {
        let book = mfce_test_book(
            "BTC",
            1_000,
            Decimal::new(9_999, 2),
            Decimal::new(10_001, 2),
            Decimal::from(1_000),
        );
        let low_fee = mfce_live_market_context(
            "BTC",
            &book,
            MfceDirection::Long,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::ONE,
            Decimal::from(50),
            Decimal::ZERO,
            Decimal::from(6),
        )
        .unwrap();
        let high_fee = mfce_live_market_context(
            "BTC",
            &book,
            MfceDirection::Long,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::from(5),
            Decimal::from(50),
            Decimal::ZERO,
            Decimal::from(6),
        )
        .unwrap();
        let expansion = mfce_live_market_context(
            "BTC",
            &book,
            MfceDirection::Long,
            Decimal::from(800),
            Decimal::from(1_000),
            Decimal::ONE,
            Decimal::from(50),
            Decimal::ZERO,
            Decimal::from(6),
        )
        .unwrap();
        let funded = mfce_live_market_context(
            "BTC",
            &book,
            MfceDirection::Long,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::from(5),
            Decimal::from(50),
            Decimal::new(1, 4),
            Decimal::from(6),
        )
        .unwrap();
        let shallow_wide = mfce_live_market_context(
            "BTC",
            &mfce_test_book(
                "BTC",
                1_000,
                Decimal::new(996, 1),
                Decimal::new(1_004, 1),
                Decimal::new(1, 1),
            ),
            MfceDirection::Long,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::ONE,
            Decimal::from(50),
            Decimal::ZERO,
            Decimal::from(6),
        )
        .unwrap();

        assert_eq!(low_fee.spread_bps, high_fee.spread_bps);
        assert_eq!(low_fee.entry_depth_ratio, high_fee.entry_depth_ratio);
        assert_eq!(low_fee.exit_depth_ratio, high_fee.exit_depth_ratio);
        assert!(high_fee.friction_bps > low_fee.friction_bps);
        assert!(expansion.friction_bps < low_fee.friction_bps);
        assert!(funded.friction_bps > high_fee.friction_bps);
        assert!(shallow_wide.friction_bps > low_fee.friction_bps);
        assert!(shallow_wide.spread_bps > low_fee.spread_bps);
        assert!(shallow_wide.entry_depth_ratio < low_fee.entry_depth_ratio);
        assert!(mfce_live_market_context(
            "BTC",
            &mfce_test_book(
                "BTC",
                1_000,
                Decimal::from(99),
                Decimal::from(101),
                Decimal::ONE,
            ),
            MfceDirection::Long,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::ONE,
            Decimal::from(50),
            Decimal::ZERO,
            Decimal::from(6),
        )
        .is_err());

        // Fees change only the separately computed admission friction. The
        // learned gross-return feature row has no friction/fee threshold in it.
        let low_fee_features = mfce_feature_vector(
            Decimal::new(5, 1),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::from(2_000),
            Decimal::ZERO,
            None,
            Some(Decimal::new(1, 2)),
            Some(&low_fee),
        )
        .unwrap();
        let high_fee_features = mfce_feature_vector(
            Decimal::new(5, 1),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::from(1_000),
            Decimal::from(2_000),
            Decimal::ZERO,
            None,
            Some(Decimal::new(1, 2)),
            Some(&high_fee),
        )
        .unwrap();
        assert_eq!(low_fee_features, high_fee_features);

        let separating_median = low_fee
            .friction_bps
            .checked_add(high_fee.friction_bps)
            .and_then(|sum| sum.checked_div(Decimal::from(2)))
            .unwrap();
        let gross_prediction = crate::mfce::MfcePrediction {
            model_epoch: 7,
            q10_gross_bps: separating_median,
            q50_gross_bps: separating_median,
            uncertainty_bps: Decimal::ZERO,
            pooled_sample_count: 64,
            direction_sample_count: 32,
            asset_direction_sample_count: 8,
            used_model: true,
        };
        let low_cost_decision = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: gross_prediction.clone(),
            friction_bps: low_fee.friction_bps,
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(10_000),
        })
        .unwrap();
        let high_cost_decision = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: gross_prediction.clone(),
            friction_bps: high_fee.friction_bps,
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(10_000),
        })
        .unwrap();
        assert!(low_cost_decision.admitted);
        assert!(!high_cost_decision.admitted);
        assert_eq!(gross_prediction.model_epoch, 7);
        assert_eq!(gross_prediction.q50_gross_bps, separating_median);
    }

    #[test]
    fn mfce_transition_features_follow_follower_position_state() {
        let opening = mfce_feature_vector(
            Decimal::new(5, 1),
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::from(50),
            Decimal::from(100),
            Decimal::ZERO,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(opening.values[6], Decimal::ONE);
        assert_eq!(opening.values[7], Decimal::ZERO);
        assert_eq!(opening.values[8], Decimal::ZERO);

        let reversal = mfce_feature_vector(
            Decimal::new(-5, 1),
            Decimal::ONE,
            Decimal::from(40),
            Decimal::from(-50),
            Decimal::from(100),
            Decimal::ZERO,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(reversal.values[6], Decimal::ZERO);
        assert_eq!(reversal.values[7], Decimal::ZERO);
        assert_eq!(reversal.values[8], Decimal::ONE);
    }

    #[test]
    fn restored_pending_mfce_requests_book_and_survives_pre_source_market_ticks() {
        use crate::mfce::{
            MfceActiveTransition, MfceAdmissionState, MfceAssetState, MfceTransitionKind,
        };

        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let config = CopyTradeConfig::from_path(path).unwrap();
        let mut engine =
            DecisionEngine::new(config, b"mfce-restore", "run", 40_000, 80_000).unwrap();
        let mut state = MfcePersistentState::default();
        state.source_epoch = 1;
        state.next_transition_id = 2;
        state.assets.insert(
            "BTC".into(),
            MfceAssetState {
                last_observed_source_epoch: 1,
                last_raw_source_exposure: Decimal::ONE,
                approved_target_notional: Decimal::ZERO,
                reserved_tail_loss_bps: Decimal::ZERO,
                active: Some(MfceActiveTransition {
                    transition_id: 1,
                    entry_midpoint: Decimal::from(100),
                    entry_timestamp_mono: 100,
                    direction: MfceDirection::Long,
                    transition_kind: MfceTransitionKind::Opening,
                    proposed_source_exposure: Decimal::ONE,
                    proposed_target_notional: Decimal::from(100),
                    features: MfceFeatureVector::new([Decimal::ZERO; MFCE_FEATURE_COUNT]),
                    admitted: false,
                }),
                admission: MfceAdmissionState::AwaitingLiveBook { transition_id: 1 },
                last_counted_transition_id: None,
                target_origin: None,
            },
        );
        engine.restore_mfce_persistent_state(state, 100).unwrap();
        assert!(engine.desired_books.contains("BTC"));

        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
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
                            name: "BTC".into(),
                            size_decimals: 3,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::from([(
                            "BTC".into(),
                            MarketAssetContext {
                                funding_rate_hourly: Decimal::ZERO,
                            },
                        )]),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine.construct_next_decision(1_001).unwrap();
        assert!(engine.mfce.state().assets["BTC"].active.is_some());
        assert!(engine.mfce.state().samples.is_empty());
    }

    #[test]
    fn technical_context_cannot_originate_a_live_target() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        config.candidates.truncate(1);
        let mut engine =
            DecisionEngine::new(config, b"technical-feature-only", "run", 40_000, 80_000).unwrap();
        warm_flat_technical_context(&mut engine, "TECH");
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: BTreeMap::from([("TECH".into(), Decimal::from(100))]),
                    }),
                    ReadRequestKind::MarketMids,
                    10_000,
                ),
                10_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketMetadata(MarketMetadataResponse {
                        universe: vec![MarketMetadataAsset {
                            name: "TECH".into(),
                            size_decimals: 3,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    10_000,
                ),
                10_000,
            )
            .unwrap();

        engine.construct_next_decision(10_001).unwrap();

        assert!(engine.technical_targets.is_empty());
        assert!(engine.mfce.tracked_assets().all(|asset| asset != "TECH"));
        assert_eq!(
            engine
                .target_ledger
                .get("TECH")
                .map_or(Decimal::ZERO, |state| state.admitted_target_notional),
            Decimal::ZERO
        );
        assert!(!engine.pending.contains_key("TECH"));
        assert!(!engine.pending_book.contains_key("TECH"));
    }

    #[test]
    fn stale_book_keeps_source_transition_pending_and_requests_replacement() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let mut engine =
            DecisionEngine::new(config, b"mfce-stale-book", "run", 40_000, 80_000).unwrap();
        engine.set_source_tier(&candidate, SourceTier::Active);
        seed_positive_mfce_backoff(&mut engine, "BTC");

        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(mfce_test_book(
                        "BTC",
                        50_000,
                        Decimal::new(9_999, 2),
                        Decimal::new(10_001, 2),
                        Decimal::from(1_000_000),
                    )),
                    ReadRequestKind::OrderBook,
                    50_000,
                ),
                50_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate,
                        account_value: Decimal::from(1_000),
                        source_time_ms: 100_000,
                        positions: BTreeMap::from([(
                            "BTC".into(),
                            SourceAssetPosition {
                                asset: "BTC".into(),
                                signed_size: Decimal::ONE,
                                signed_notional: Decimal::from(100),
                                entry_price: Some(Decimal::from(100)),
                                unrealized_pnl: Some(Decimal::ZERO),
                            },
                        )]),
                        closed_candles: Vec::new(),
                    }),
                    ReadRequestKind::SourceState,
                    100_000,
                ),
                100_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketSnapshot(MarketSnapshotResponse {
                        mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
                    }),
                    ReadRequestKind::MarketMids,
                    100_000,
                ),
                100_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::MarketMetadata(MarketMetadataResponse {
                        universe: vec![MarketMetadataAsset {
                            name: "BTC".into(),
                            size_decimals: 3,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::from([(
                            "BTC".into(),
                            MarketAssetContext {
                                funding_rate_hourly: Decimal::ZERO,
                            },
                        )]),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    100_000,
                ),
                100_000,
            )
            .unwrap();

        engine.construct_next_decision(100_001).unwrap();

        // Edge-only: stale books are warnings, not blocks. Last-known book
        // is used with widened uncertainty; transition admits on edge.
        assert!(!engine
            .mfce
            .awaiting_book_assets()
            .any(|asset| asset == "BTC"));
        assert!(engine.mfce.policy_output("BTC").is_some());

        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(mfce_test_book(
                        "BTC",
                        100_002,
                        Decimal::new(9_999, 2),
                        Decimal::new(10_001, 2),
                        Decimal::from(1_000_000),
                    )),
                    ReadRequestKind::OrderBook,
                    100_002,
                ),
                100_002,
            )
            .unwrap();

        assert!(!engine
            .mfce
            .awaiting_book_assets()
            .any(|asset| asset == "BTC"));
        assert!(matches!(
            engine.mfce.state().assets["BTC"].admission,
            crate::mfce::MfceAdmissionState::Rejected {
                reason: MfceRejectionReason::BelowExchangeMinimum,
                ..
            }
        ));
        assert!(engine.mfce.delayed().samples.is_empty());
    }

    #[test]
    fn economic_attribution_is_mutually_exclusive_and_detects_disagreement() {
        let classify = |entries: &[(&str, Decimal)]| {
            let values = entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), *value))
                .collect::<BTreeMap<_, _>>();
            classify_economic_attribution(Some(&values))
        };
        assert_eq!(
            classify(&[("source:wallet", Decimal::ONE)]),
            EconomicAttribution::SourceOnly
        );
        assert_eq!(
            classify(&[("source:very_profitable_cohort", Decimal::ONE)]),
            EconomicAttribution::CohortOnly
        );
        assert_eq!(
            classify(&[("technical:range_mean_reversion:range", Decimal::ONE)]),
            EconomicAttribution::TechnicalOnly
        );
        assert_eq!(
            classify(&[
                ("source:wallet", Decimal::ONE),
                ("technical:range_mean_reversion:range", Decimal::ONE),
            ]),
            EconomicAttribution::Hybrid
        );
        assert_eq!(
            classify(&[
                ("source:wallet", Decimal::ONE),
                ("technical:range_mean_reversion:range", -Decimal::ONE),
            ]),
            EconomicAttribution::Disagreement
        );
    }

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
    fn component_fill_allocation_follows_absolute_target_deltas_and_conserves_quantity() {
        let current = [
            ("a".to_string(), Decimal::from(-2)),
            ("b".to_string(), Decimal::from(-1)),
        ]
        .into_iter()
        .collect();
        let targets = [
            ("c".to_string(), Decimal::new(6, 1)),
            ("d".to_string(), Decimal::new(14, 1)),
        ]
        .into_iter()
        .collect();
        let allocated = allocate_component_fill(
            &current,
            Side::Buy,
            Decimal::from(5),
            &targets,
            Decimal::ONE,
            Decimal::from(2),
            Decimal::ONE,
        )
        .unwrap();
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
    fn source_exit_closes_only_source_component_and_preserves_technical_target() {
        let contributions = [
            ("source:wallet".to_string(), Decimal::new(35, 2)),
            (
                "technical:trend_pullback:trending".to_string(),
                Decimal::new(65, 2),
            ),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let opened_targets =
            absolute_directional_component_targets(Some(&contributions), Decimal::from(100))
                .unwrap()
                .unwrap();
        assert_eq!(opened_targets["source:wallet"], Decimal::from(35));
        assert_eq!(
            opened_targets["technical:trend_pullback:trending"],
            Decimal::from(65)
        );

        let current = opened_targets.clone();
        let technical_only = [(
            "technical:trend_pullback:trending".to_string(),
            Decimal::from(65),
        )]
        .into_iter()
        .collect();
        let allocated = allocate_component_fill(
            &current,
            Side::Sell,
            Decimal::from(35),
            &technical_only,
            Decimal::ONE,
            Decimal::from(65),
            Decimal::ONE,
        )
        .unwrap();

        assert_eq!(
            allocated,
            [("source:wallet".to_string(), Decimal::from(35))]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn crossing_fill_closes_source_then_opens_subfloor_technical_destination() {
        let current = [("source:aggregate".to_string(), Decimal::new(-3061, 2))]
            .into_iter()
            .collect();
        let contributions = [
            ("source:aggregate".to_string(), Decimal::new(-3061, 2)),
            (
                "technical:range_mean_reversion:range".to_string(),
                Decimal::new(3696, 2),
            ),
        ]
        .into_iter()
        .collect();
        let targets =
            absolute_directional_component_targets(Some(&contributions), Decimal::new(634, 2))
                .unwrap()
                .unwrap();
        assert_eq!(
            targets,
            [(
                "technical:range_mean_reversion:range".to_string(),
                Decimal::new(634, 2)
            )]
            .into_iter()
            .collect()
        );

        let allocated = allocate_component_fill(
            &current,
            Side::Buy,
            Decimal::new(3695, 2),
            &targets,
            Decimal::ONE,
            Decimal::new(634, 2),
            Decimal::new(1, 2),
        )
        .unwrap();
        assert_eq!(allocated["source:aggregate"], Decimal::new(3061, 2));
        assert_eq!(
            allocated["technical:range_mean_reversion:range"],
            Decimal::new(634, 2)
        );
        assert_eq!(
            allocated.values().copied().sum::<Decimal>(),
            Decimal::new(3695, 2)
        );
    }

    #[test]
    fn installed_cohort_is_one_source_target_and_technical_context_never_vetoes() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        config.candidates.truncate(1);
        let existing = config.candidates[0].address.to_ascii_lowercase();
        let cohort_only = "0x000000000000000000000000000000000000d00d".to_string();
        let cohort_only_two = "0x000000000000000000000000000000000000d00e".to_string();
        let passing = |address: String| WalletQualityObservation {
            address,
            recent_activity_age_ms: 1,
            closed_trades: 100,
            realized_pnl_usd: Decimal::from(10_000),
            annualized_sharpe: Decimal::from(20),
            win_rate_pct: Decimal::from(99),
            maximum_drawdown_pct: Decimal::ONE,
            account_equity_usd: Decimal::from(1_000_000),
            maximum_observed_leverage: Decimal::ONE,
            maximum_margin_utilization_pct: Some(Decimal::from(10)),
            maximum_asset_concentration_pct: Decimal::from(10),
            median_holding_duration_ms: Some(60_000),
            retained_fill_count: 100,
            history_complete: true,
        };
        let artifact = VeryProfitableLayerArtifact {
            schema_version: COHORT_LAYER_SCHEMA_VERSION,
            admission_policy: crate::cohort_layer::CohortAdmissionPolicy::QualityFiltered,
            membership: HyperdashMembershipSnapshot {
                schema_version: 1,
                source: VERY_PROFITABLE_COHORT_URL.into(),
                cohort_id: VERY_PROFITABLE_COHORT_ID.into(),
                observed_at_ms: 1_000,
                displayed_min_all_time_pnl_usd: Decimal::from(100_000),
                displayed_max_all_time_pnl_usd: Decimal::from(1_000_000),
                completeness: MembershipCompleteness::Complete,
                reported_member_count: 3,
                captured_member_count: 3,
                wallets: vec![
                    existing.clone(),
                    cohort_only.clone(),
                    cohort_only_two.clone(),
                ],
            },
            hydrated_at_ms: 1_000,
            authoritative_source: HYPERLIQUID_INFO_AUTHORITY.into(),
            authoritative_history_sha256: "ab".repeat(32),
            wallet_quality: vec![
                passing(existing.clone()),
                passing(cohort_only.clone()),
                passing(cohort_only_two.clone()),
            ],
            active_twaps: BTreeMap::new(),
        };
        let layer = PreparedVeryProfitableLayer::prepare(&artifact, &config).unwrap();
        assert_eq!(layer.resolution.overlap_with_existing, 1);
        assert_eq!(layer.merge_candidates(&mut config).unwrap(), 2);
        assert_eq!(config.candidates[1].allocation_weight, 0.0);
        let mut engine =
            DecisionEngine::new(config, b"cohort-integration", "run", 40_000, 80_000).unwrap();
        engine.install_very_profitable_layer(layer).unwrap();
        for index in 0..15u64 {
            let close = Decimal::from(100 + index);
            engine
                .technical_engine
                .accept_closed_candle(ClosedCandle {
                    asset: "BTC".into(),
                    interval: CandleInterval::OneHour,
                    open_time_ms: index * 3_600_000,
                    close_time_ms: (index + 1) * 3_600_000,
                    open: close,
                    high: close + Decimal::ONE,
                    low: close - Decimal::ONE,
                    close,
                    base_volume: Decimal::from(10),
                    trade_count: 5,
                })
                .unwrap();
        }
        let mids = MarketSnapshotResponse {
            mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
        };
        let metadata = MarketMetadataResponse {
            universe: vec![MarketMetadataAsset {
                name: "BTC".into(),
                size_decimals: 3,
                growth_mode: false,
            }],
            contexts: BTreeMap::from([(
                "BTC".into(),
                MarketAssetContext {
                    funding_rate_hourly: Decimal::ZERO,
                },
            )]),
            live_taker_fee_bps: Some(Decimal::from(5)),
            dex_fee_scales: BTreeMap::new(),
            user_fee_state: None,
        };
        let source_state = |address: String, at: u64, notional: Decimal| SourceStateResponse {
            block_number: None,
            candidate_id: address,
            account_value: Decimal::from(1_000),
            source_time_ms: at,
            positions: BTreeMap::from([(
                "BTC".into(),
                SourceAssetPosition {
                    asset: "BTC".into(),
                    signed_size: Decimal::ONE,
                    signed_notional: notional,
                    entry_price: Some(Decimal::from(100)),
                    unrealized_pnl: Some(Decimal::ZERO),
                },
            )]),
            closed_candles: Vec::new(),
        };
        let inputs = || {
            BTreeMap::from([(
                "BTC".into(),
                vec![ConsensusInput {
                    candidate_id: "source:aggregate".into(),
                    allocation_weight: 1.0,
                    confidence_modifier: 1.0,
                    source_exposure: 1.0,
                    enabled: true,
                    quarantined: false,
                    snapshot_age_ms: 0,
                }],
            )])
        };
        let contributions = || {
            BTreeMap::from([(
                "BTC".into(),
                BTreeMap::from([("source:existing".into(), Decimal::ONE)]),
            )])
        };

        let mut first_inputs = inputs();
        let mut first_contributions = contributions();
        engine
            .apply_very_profitable_cohort(
                1_000,
                &mids,
                &metadata,
                &[
                    source_state(existing.clone(), 1_000, Decimal::from(100)),
                    source_state(cohort_only.clone(), 1_000, Decimal::from(100)),
                ],
                &BTreeMap::from([(existing.clone(), 1_000), (cohort_only.clone(), 1_000)]),
                &BTreeMap::from([("BTC".into(), Decimal::ONE)]),
                &mut first_inputs,
                &mut first_contributions,
            )
            .unwrap();
        let partial = engine.cohort_indicator_records.last().unwrap();
        assert_eq!(partial.long_trader_count, 2);
        assert!(!partial
            .reasons
            .contains(&CohortDecisionReason::IncompleteAuthoritativeWalletState));
        assert_eq!(
            first_inputs["BTC"]
                .iter()
                .filter(|input| input.candidate_id.starts_with("source:"))
                .count(),
            1
        );
        assert!(!first_inputs["BTC"]
            .iter()
            .any(|input| input.candidate_id.starts_with("technical:")));

        let mut missing_atr_inputs = BTreeMap::from([(
            "ETH".into(),
            vec![ConsensusInput {
                candidate_id: "source:aggregate".into(),
                allocation_weight: 1.0,
                confidence_modifier: 1.0,
                source_exposure: 1.0,
                enabled: true,
                quarantined: false,
                snapshot_age_ms: 0,
            }],
        )]);
        let mut missing_atr_contributions = BTreeMap::new();
        engine
            .apply_very_profitable_cohort(
                2_000,
                &MarketSnapshotResponse {
                    mids: BTreeMap::from([("ETH".into(), Decimal::from(100))]),
                },
                &MarketMetadataResponse {
                    universe: vec![MarketMetadataAsset {
                        name: "ETH".into(),
                        size_decimals: 3,
                        growth_mode: false,
                    }],
                    contexts: BTreeMap::new(),
                    live_taker_fee_bps: Some(Decimal::from(5)),
                    dex_fee_scales: BTreeMap::new(),
                    user_fee_state: None,
                },
                &[
                    source_state(existing.clone(), 2_000, Decimal::from(100)),
                    source_state(cohort_only.clone(), 2_000, Decimal::from(100)),
                    source_state(cohort_only_two.clone(), 2_000, Decimal::from(100)),
                ],
                &BTreeMap::from([
                    (existing.clone(), 2_000),
                    (cohort_only.clone(), 2_000),
                    (cohort_only_two.clone(), 2_000),
                ]),
                &BTreeMap::from([("ETH".into(), Decimal::ONE)]),
                &mut missing_atr_inputs,
                &mut missing_atr_contributions,
            )
            .unwrap();
        let rejection = engine.cohort_indicator_records.last().unwrap();
        assert_eq!(rejection.asset, "ETH");
        assert!(!rejection
            .reasons
            .contains(&CohortDecisionReason::MissingAtr));
        assert!(!rejection
            .reasons
            .contains(&CohortDecisionReason::MissingExpectedMove));
        assert_eq!(rejection.very_profitable_cohort_target, Decimal::ZERO);

        let mut second_inputs = inputs();
        let mut second_contributions = contributions();
        engine
            .apply_very_profitable_cohort(
                301_000,
                &mids,
                &metadata,
                &[
                    source_state(existing.clone(), 301_000, Decimal::from(200)),
                    source_state(cohort_only.clone(), 301_000, Decimal::from(200)),
                ],
                &BTreeMap::from([(existing, 301_000), (cohort_only, 301_000)]),
                &BTreeMap::from([("BTC".into(), Decimal::ONE)]),
                &mut second_inputs,
                &mut second_contributions,
            )
            .unwrap();
        let record = engine.cohort_indicator_records.last().unwrap();
        assert!(
            record.very_profitable_cohort_target > Decimal::ZERO,
            "{record:?}"
        );
        assert!(record.source_target > Decimal::ZERO, "{record:?}");
        assert!(record.source_target <= Decimal::ONE);
        assert_eq!(record.technical_target, Decimal::ZERO);
        assert_eq!(record.combined_target, record.source_target);
        assert!(record.independent_execution_root);
        assert!(record
            .attribution
            .contains(&AttributionTag::BothSourceSetsAgreeing));
        assert!(!record
            .attribution
            .contains(&AttributionTag::SourceTechnicalHybrid));
        assert_eq!(record.overlap_with_existing, 1);
        assert_eq!(record.long_trader_count, 2);
        assert_eq!(
            second_inputs["BTC"]
                .iter()
                .filter(|input| input.candidate_id.starts_with("source:"))
                .count(),
            1
        );
        assert_eq!(
            second_contributions["BTC"]
                .keys()
                .filter(|component| component.starts_with("source:"))
                .count(),
            2
        );
    }

    #[test]
    fn first_synthetic_technical_fill_has_a_starting_equity_baseline() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        config.candidates.truncate(1);
        let expected_starting = Decimal::from_f64(config.starting_equity_usd).unwrap();
        let mut engine =
            DecisionEngine::new(config, b"technical-equity", "run", 40_000, 80_000).unwrap();
        let technical = "technical:trend_pullback:trending".to_string();
        let additional = [technical.clone()].into_iter().collect::<BTreeSet<_>>();

        let (_, before_first_fill) = engine
            .accounting_equities_with_sources(&additional)
            .unwrap();

        assert_eq!(before_first_fill[&technical], expected_starting);

        let (_, existing_sources) = engine.accounting_equities().unwrap();
        engine
            .record_equity_values(1, expected_starting, existing_sources)
            .unwrap();
        let mut after_first_fill = before_first_fill;
        after_first_fill.insert(technical.clone(), expected_starting - Decimal::ONE);
        engine
            .record_equity_values(2, expected_starting - Decimal::ONE, after_first_fill)
            .unwrap();
        assert_eq!(
            engine.equity_buckets[0].source_returns[&technical],
            -Decimal::ONE / expected_starting
        );
    }

    #[test]
    fn railway_window_one_aave_close_preserves_every_exact_source_quantity() {
        // Exact quantities extracted from Railway Window 1
        // 1785202130363-303-91ab220e921d. The failed release recomputed these
        // proportions and assigned the final source one quantum too little,
        // leaving its first AAVE episode open.
        let current = [
            (
                "0x06ce0e9a8217e21142b0b91fcb1182750f9ac3b7".to_string(),
                Decimal::from_str_exact("-0.0945499679493828371440766706").unwrap(),
            ),
            (
                "0x77375d6d902c1abf9bb11732d11db19269225107".to_string(),
                Decimal::from_str_exact("-0.1984857384217668323139284628").unwrap(),
            ),
            (
                "0xa20fb0c9e04063eec5be286e9269028d966646fa".to_string(),
                Decimal::from_str_exact("-0.0169642936288503305419948666").unwrap(),
            ),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let filled = Decimal::from_str_exact("0.31").unwrap();

        let allocated = allocate_component_fill(
            &current,
            Side::Buy,
            filled,
            &BTreeMap::new(),
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::new(1, 2),
        )
        .unwrap();

        for (candidate, position) in &current {
            assert_eq!(allocated[candidate], position.abs());
        }
        assert_eq!(allocated.values().copied().sum::<Decimal>(), filled);
    }

    #[test]
    fn su6r1_aster_flatten_closes_all_sixteen_dusty_component_books() {
        // Exact source positions from SU6R1 bundle
        // 1785634427188-314-f4df7175a2ac immediately before the fatal close.
        let quantities = [
            "-10.569933806207641802364056286",
            "-0.0012204330333344814929016578",
            "-0.0256329142486161707056936183",
            "-1.4182336411127518167954272272",
            "-4.1319697527129420318749869447",
            "-1.1935335833931224295511663717",
            "-0.0532461852933140999267770672",
            "-0.4611020753173003541646836818",
            "-0.6108706814973284498170154000",
            "-0.0506168250254359694888496665",
            "-0.8809903463866215981964416265",
            "-2.0013754079445062024992574994",
            "-7.2450692729585961379361249075",
            "-0.1744696213398916464937353789",
            "-0.0786723805107424623460488596",
            "-0.103063073017854346346833809",
        ];
        let current = quantities
            .into_iter()
            .enumerate()
            .map(|(index, quantity)| {
                (
                    format!("source:{index:02}"),
                    Decimal::from_str_exact(quantity).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let attributed_before = current.values().copied().sum::<Decimal>();
        assert_ne!(attributed_before, Decimal::from(-29));
        assert!(
            reconciliation_difference(attributed_before, Decimal::from(-29), Decimal::ONE)
                .unwrap()
                .0
                <= Decimal::new(1, 12)
        );

        let allocated = allocate_component_fill(
            &current,
            Side::Buy,
            Decimal::from(29),
            &BTreeMap::new(),
            Decimal::from_str_exact("0.603555").unwrap(),
            Decimal::ZERO,
            Decimal::ONE,
        )
        .unwrap();

        assert_eq!(allocated.len(), 16);
        assert_ne!(
            allocated.values().copied().sum::<Decimal>(),
            Decimal::from(29)
        );
        let mut issued = allocated.clone();
        canonicalize_allocation_total(&mut issued, Decimal::from(29)).unwrap();
        assert_eq!(issued.values().copied().sum::<Decimal>(), Decimal::from(29));
        for (candidate, position) in current.iter().take(current.len() - 1) {
            assert_eq!(issued[candidate], position.abs());
        }
        let pending = issued
            .iter()
            .map(|(component, quantity)| (component.clone(), *quantity))
            .collect();
        let consumed = consume_pending_component_fill(
            &current,
            Side::Buy,
            Decimal::from(29),
            Decimal::from(29),
            &pending,
            Decimal::ONE,
        )
        .unwrap();
        assert_eq!(consumed.total_quantities, issued);
        assert!(consumed.remaining_quantities.is_empty());
    }

    #[test]
    fn repeated_dusty_portfolio_flattens_never_strand_a_component() {
        for cycle in 1..=1_000 {
            let dust = Decimal::new(i64::from(cycle % 9 + 1), 28);
            let current = [
                ("source:a".to_string(), Decimal::new(-7, 1)),
                ("source:b".to_string(), Decimal::new(-2, 1)),
                ("source:c".to_string(), -Decimal::new(1, 1) - dust),
            ]
            .into_iter()
            .collect::<BTreeMap<_, _>>();
            let allocated = allocate_component_fill(
                &current,
                Side::Buy,
                Decimal::ONE,
                &BTreeMap::new(),
                Decimal::ONE,
                Decimal::ZERO,
                Decimal::new(1, 6),
            )
            .unwrap();

            assert_eq!(allocated.len(), current.len());
            assert!(current
                .iter()
                .all(|(component, position)| allocated[component] == position.abs()));
        }
    }

    fn crossing_test_execution(
        asset: &str,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        position_before: Decimal,
        fees: Decimal,
        funding: Decimal,
        slippage: Decimal,
    ) -> ExecutionFill {
        let signed = match side {
            Side::Buy => quantity,
            Side::Sell => -quantity,
        };
        ExecutionFill {
            execution_id: ExecutionFillId([31; 32]),
            action: PlannedAction {
                decision_id: DecisionId([29; 32]),
                target_version: TargetVersion(1),
                asset: asset.to_string(),
                side,
                rounded_notional: quantity * price,
                reduce_only: false,
                action_ordinal: 0,
                retry_generation: 0,
                planned_cloid: PlannedCloid([23; 16]),
            },
            decision_timestamp_mono: 1,
            decision_market_snapshot_id: MarketSnapshotId([17; 32]),
            evaluation_market_snapshot_id: MarketSnapshotId([19; 32]),
            latency_scenario: LatencyScenario::Expected,
            configured_latency_ms: 0,
            proposed_limit_price: price,
            rounded_quantity: quantity,
            modeled_filled_quantity: quantity,
            modeled_average_fill_price: Some(price),
            unfilled_ioc_remainder: Decimal::ZERO,
            modeled_filled_notional: quantity * price,
            fees,
            funding,
            slippage,
            position_before,
            position_after: position_before + signed,
        }
    }

    #[test]
    fn exact_residual_assignment_absorbs_decimal_partition_dust() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let mut engine =
            DecisionEngine::new(config, b"decimal-residual", "run", 40_000, 80_000).unwrap();
        let opening = crossing_test_execution(
            "BTC",
            Side::Buy,
            Decimal::ONE,
            Decimal::from(100),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        engine
            .ledger
            .apply_portfolio_execution(&opening, 1)
            .unwrap();
        let quantities = BTreeMap::from([
            ("source:a".to_string(), Decimal::new(936, 3)),
            ("source:b".to_string(), Decimal::new(64, 3)),
        ]);
        for (candidate, quantity) in &quantities {
            let mut component =
                scaled_source_execution(&opening, *quantity, Decimal::ZERO).unwrap();
            if candidate == "source:b" {
                let price = Decimal::from_str_exact("100.0000000000000000000000001").unwrap();
                component.modeled_average_fill_price = Some(price);
                component.modeled_filled_notional = quantity.checked_mul(price).unwrap();
            }
            engine
                .ledger
                .apply_source_execution(candidate, &component, 1)
                .unwrap();
        }
        let closing = crossing_test_execution(
            "BTC",
            Side::Sell,
            Decimal::ONE,
            Decimal::from(101),
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        engine
            .apply_attributed_execution("BTC", &closing, 2, &quantities)
            .unwrap();

        let portfolio = engine.ledger.portfolio_closed().last().unwrap();
        let attributed_gross = engine
            .ledger
            .all_source_closed()
            .iter()
            .map(|episode| episode.modeled_gross_pnl)
            .sum::<Decimal>();
        assert_eq!(portfolio.realized_pnl, attributed_gross);
    }

    fn sum_map(values: &BTreeMap<String, Decimal>) -> Decimal {
        values.values().copied().sum()
    }

    #[test]
    fn crossing_partition_covers_reductions_flattens_reversal_sizes_and_disagreement() {
        struct Case {
            current: BTreeMap<String, Decimal>,
            side: Side,
            fill: Decimal,
            targets: BTreeMap<String, Decimal>,
            after: Decimal,
            expected_close: Decimal,
            expected_open: Decimal,
        }
        let cases = [
            Case {
                current: BTreeMap::from([("source:a".into(), Decimal::from(10))]),
                side: Side::Sell,
                fill: Decimal::from(4),
                targets: BTreeMap::from([("source:a".into(), Decimal::from(6))]),
                after: Decimal::from(6),
                expected_close: Decimal::from(4),
                expected_open: Decimal::ZERO,
            },
            Case {
                current: BTreeMap::from([("source:a".into(), Decimal::from(10))]),
                side: Side::Sell,
                fill: Decimal::from(10),
                targets: BTreeMap::new(),
                after: Decimal::ZERO,
                expected_close: Decimal::from(10),
                expected_open: Decimal::ZERO,
            },
            Case {
                current: BTreeMap::from([("source:a".into(), Decimal::from(10))]),
                side: Side::Sell,
                fill: Decimal::from(12),
                targets: BTreeMap::from([("technical:range".into(), Decimal::from(-2))]),
                after: Decimal::from(-2),
                expected_close: Decimal::from(10),
                expected_open: Decimal::from(2),
            },
            Case {
                current: BTreeMap::from([("source:a".into(), Decimal::from(10))]),
                side: Side::Sell,
                fill: Decimal::from(25),
                targets: BTreeMap::from([("technical:range".into(), Decimal::from(-15))]),
                after: Decimal::from(-15),
                expected_close: Decimal::from(10),
                expected_open: Decimal::from(15),
            },
            Case {
                current: BTreeMap::from([
                    ("source:a".into(), Decimal::from(6)),
                    ("source:b".into(), Decimal::from(4)),
                ]),
                side: Side::Sell,
                fill: Decimal::from(13),
                targets: BTreeMap::from([("technical:trend".into(), Decimal::from(-3))]),
                after: Decimal::from(-3),
                expected_close: Decimal::from(10),
                expected_open: Decimal::from(3),
            },
            Case {
                current: BTreeMap::from([
                    ("source:very_profitable_cohort".into(), Decimal::from(-7)),
                    ("source:a".into(), Decimal::from(-3)),
                ]),
                side: Side::Buy,
                fill: Decimal::from(14),
                targets: BTreeMap::from([("technical:breakout".into(), Decimal::from(4))]),
                after: Decimal::from(4),
                expected_close: Decimal::from(10),
                expected_open: Decimal::from(4),
            },
        ];
        for case in cases {
            let partition = partition_component_fill(
                &case.current,
                case.side,
                case.fill,
                &case.targets,
                Decimal::ONE,
                case.after,
                Decimal::new(1, 6),
            )
            .unwrap();
            assert_eq!(sum_map(&partition.close_quantities), case.expected_close);
            assert_eq!(sum_map(&partition.open_quantities), case.expected_open);
            assert_eq!(sum_map(&partition.total_quantities), case.fill);
        }
    }

    #[test]
    fn crossing_partition_is_durable_for_one_thousand_direction_flips() {
        for cycle in 1..=1_000u64 {
            let old_a = Decimal::from((cycle % 17) + 1);
            let old_b = Decimal::from((cycle % 11) + 1);
            let opening = Decimal::from((cycle % 7) + 1);
            let before = old_a + old_b;
            let fill = before + opening;
            let current = BTreeMap::from([
                ("source:a".to_string(), old_a),
                ("source:very_profitable_cohort".to_string(), old_b),
            ]);
            let targets = BTreeMap::from([("technical:range".to_string(), -opening)]);
            let partition = partition_component_fill(
                &current,
                Side::Sell,
                fill,
                &targets,
                Decimal::ONE,
                -opening,
                Decimal::new(1, 6),
            )
            .unwrap();
            assert_eq!(partition.close_quantities, current);
            assert_eq!(sum_map(&partition.open_quantities), opening);
            assert_eq!(sum_map(&partition.total_quantities), fill);

            let portfolio_open = crossing_test_execution(
                "AAVE",
                Side::Buy,
                before,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            );
            let portfolio_cross = crossing_test_execution(
                "AAVE",
                Side::Sell,
                fill,
                Decimal::from(101),
                before,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            );
            let mut ledger = DualLedger::default();
            ledger
                .apply_portfolio_execution(&portfolio_open, 1)
                .unwrap();
            for (component, quantity) in &current {
                let component_open =
                    scaled_source_execution(&portfolio_open, *quantity, Decimal::ZERO).unwrap();
                ledger
                    .apply_source_execution(component, &component_open, 1)
                    .unwrap();
            }
            let portfolio_closed = ledger
                .apply_portfolio_execution(&portfolio_cross, 2)
                .unwrap()
                .unwrap()
                .clone();
            let mut component_closed = Vec::new();
            for (component, quantity) in &partition.total_quantities {
                let component_cross = scaled_source_execution(
                    &portfolio_cross,
                    *quantity,
                    ledger.source_position(component, "AAVE"),
                )
                .unwrap();
                if let Some(closed) = ledger
                    .apply_source_execution(component, &component_cross, 2)
                    .unwrap()
                {
                    component_closed.push(closed);
                }
            }
            assert_eq!(component_closed.len(), 2);
            assert_eq!(ledger.portfolio_position("AAVE"), -opening);
            assert_eq!(
                sum_map(&ledger.source_positions_for_asset("AAVE")),
                -opening
            );
            assert_eq!(
                component_closed
                    .iter()
                    .map(|episode| episode.modeled_gross_pnl)
                    .sum::<Decimal>(),
                portfolio_closed.realized_pnl
            );
        }
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
    fn source_allocation_drops_an_exact_zero_anchor_residual() {
        let expected = Decimal::new(148, 1);
        let mut allocations = BTreeMap::from([
            ("source:a".to_string(), expected),
            ("source:b".to_string(), Decimal::ONE),
        ]);
        canonicalize_allocation_total(&mut allocations, expected).unwrap();
        assert_eq!(
            allocations,
            BTreeMap::from([("source:a".to_string(), expected)])
        );
    }

    #[test]
    fn source_allocation_trims_decimal_overrun_without_failing_engine() {
        let expected = Decimal::ONE;
        let quantum = Decimal::new(1, 28);
        let mut allocations = BTreeMap::from([
            ("source:a".to_string(), Decimal::new(6, 1)),
            ("source:b".to_string(), Decimal::new(4, 1) + quantum),
        ]);

        canonicalize_allocation_total(&mut allocations, expected).unwrap();

        assert_eq!(allocations.values().copied().sum::<Decimal>(), expected);
        assert_eq!(allocations["source:a"], Decimal::new(6, 1));
        assert_eq!(allocations["source:b"], Decimal::new(4, 1));
    }

    #[test]
    fn pending_component_quantity_stays_exact_across_partial_fills() {
        let total = Decimal::from(24);
        let first_fill = Decimal::from(7);
        let final_fill = Decimal::from(17);
        let current = BTreeMap::from([
            ("source:a".to_string(), Decimal::from(8)),
            ("source:b".to_string(), Decimal::from(8)),
            ("source:c".to_string(), Decimal::from(8)),
        ]);
        let mut issued = BTreeMap::from([
            ("source:a".to_string(), Decimal::from(8)),
            ("source:b".to_string(), Decimal::from(8)),
            (
                "source:c".to_string(),
                Decimal::from_str_exact("7.999999999999999999999999999").unwrap(),
            ),
        ]);
        assert_eq!(
            issued.values().copied().sum::<Decimal>(),
            Decimal::from_str_exact("23.999999999999999999999999999").unwrap()
        );
        canonicalize_allocation_total(&mut issued, total).unwrap();
        assert_eq!(issued.values().copied().sum::<Decimal>(), total);
        let pending = issued
            .into_iter()
            .map(|(component, quantity)| (component, -quantity))
            .collect::<BTreeMap<_, _>>();

        let first = consume_pending_component_fill(
            &current,
            Side::Sell,
            first_fill,
            total,
            &pending,
            Decimal::ONE,
        )
        .unwrap();
        assert_eq!(
            first.total_quantities.values().copied().sum::<Decimal>(),
            first_fill
        );
        assert_eq!(
            pending_component_total(Side::Sell, &first.remaining_quantities).unwrap(),
            final_fill
        );

        let current_after_first = current
            .into_iter()
            .map(|(component, quantity)| {
                let consumed = first
                    .total_quantities
                    .get(&component)
                    .copied()
                    .unwrap_or_default();
                (component, quantity - consumed)
            })
            .collect::<BTreeMap<_, _>>();
        let second = consume_pending_component_fill(
            &current_after_first,
            Side::Sell,
            final_fill,
            final_fill,
            &first.remaining_quantities,
            Decimal::ONE,
        )
        .unwrap();
        assert_eq!(
            second.total_quantities.values().copied().sum::<Decimal>(),
            final_fill
        );
        assert!(second.remaining_quantities.is_empty());
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

    #[test]
    fn sub_lot_replanning_preserves_the_existing_economic_root() {
        let previous = PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            asset: "BTC".to_string(),
            side: Side::Buy,
            rounded_notional: Decimal::from(100),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([2; 16]),
        };
        let mut refreshed = previous.clone();
        refreshed.decision_id = DecisionId([3; 32]);
        refreshed.target_version = TargetVersion(2);
        refreshed.planned_cloid = PlannedCloid([4; 16]);
        refreshed.rounded_notional = Decimal::new(1_005, 1);

        assert!(pending_action_is_immaterial_replacement(
            &previous,
            &refreshed,
            Decimal::ONE,
        ));

        refreshed.rounded_notional = Decimal::from(101);
        assert!(!pending_action_is_immaterial_replacement(
            &previous,
            &refreshed,
            Decimal::ONE,
        ));
        refreshed.rounded_notional = Decimal::from(100);
        refreshed.side = Side::Sell;
        assert!(!pending_action_is_immaterial_replacement(
            &previous,
            &refreshed,
            Decimal::ONE,
        ));
    }

    #[test]
    fn preissue_reduce_only_uses_current_position_and_target_not_trigger_size() {
        let action = PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            asset: "TEST".into(),
            side: Side::Sell,
            rounded_notional: Decimal::new(1, 2),
            reduce_only: true,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([2; 16]),
        };
        let snapshot = ExecutableBook {
            snapshot_id: MarketSnapshotId([3; 32]),
            observed_at_mono: 1,
            midpoint: Decimal::from(100),
            bids: vec![DepthLevel {
                price: Decimal::from(99),
                quantity: Decimal::from(10),
            }],
            asks: vec![DepthLevel {
                price: Decimal::from(101),
                quantity: Decimal::from(10),
            }],
        };
        let rules = MarketRules {
            mark_price: Decimal::from(100),
            price_tick: Decimal::new(1, 1),
            size_step: Decimal::new(1, 2),
        };
        let policy = ExecutionFloorPolicy {
            exchange_minimum_notional: Decimal::from(10),
            rounding_buffer: Decimal::new(2, 1),
            closeability_margin: Decimal::new(5, 1),
            maximum_slippage_fraction: Decimal::new(1, 2),
        };
        let input = |desired_target_notional, filled_notional, filled_quantity| ExitPlanningInput {
            asset: &action.asset,
            desired_target_notional,
            filled_notional,
            filled_quantity,
            acknowledged_open_notional: Decimal::ZERO,
            unknown_result_notional: Decimal::ZERO,
            continuation_notional: Decimal::ZERO,
            reference_price: Decimal::from(100),
            market_rules: &rules,
            market_snapshot: &snapshot,
            execution_floor_policy: policy,
            execution_cushion: Decimal::new(1, 3),
            maximum_slippage: Decimal::new(1, 2),
        };

        assert_eq!(
            round_exchange_step(
                action.rounded_notional / Decimal::from(100),
                rules.size_step,
                false,
            )
            .unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            preissue_reduce_only_quantity(
                &action,
                &input(Decimal::ZERO, Decimal::from(100), Decimal::ONE),
            )
            .unwrap(),
            (Decimal::ONE, None)
        );
        assert_eq!(
            preissue_reduce_only_quantity(
                &action,
                &input(Decimal::from(50), Decimal::from(100), Decimal::ONE),
            )
            .unwrap(),
            (Decimal::new(5, 1), None)
        );
        assert_eq!(
            preissue_reduce_only_quantity(
                &action,
                &input(Decimal::ZERO, Decimal::from(5), Decimal::new(5, 2)),
            )
            .unwrap(),
            (
                Decimal::ZERO,
                Some(ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute),
            )
        );
        assert_eq!(
            preissue_reduce_only_quantity(
                &action,
                &input(Decimal::from(100), Decimal::from(100), Decimal::ONE),
            )
            .unwrap(),
            (Decimal::ZERO, Some(ActionAttemptOutcome::NoLongerRequired),)
        );
        assert_eq!(
            preissue_reduce_only_quantity(
                &action,
                &input(Decimal::from(120), Decimal::from(100), Decimal::ONE),
            )
            .unwrap(),
            (
                Decimal::ZERO,
                Some(ActionAttemptOutcome::SupersededByNewTarget),
            )
        );
    }

    #[test]
    fn issued_pending_action_with_zero_remaining_quantity_is_still_invalid() {
        let (mut engine, book) = pending_reduce_only_engine(
            "TEST",
            Decimal::from(1_000),
            Decimal::new(11, 2),
            Decimal::new(1, 1),
        );
        engine
            .pending
            .get_mut("TEST")
            .unwrap()
            .remaining_order_quantity = Decimal::ZERO;

        assert!(matches!(
            engine.try_execute_pending(&book, 10_000),
            Err(EngineError::InvalidMarket(message))
                if message == "TEST rounded to zero quantity"
        ));
    }

    #[test]
    fn startup_retires_restored_pending_action_missing_from_durable_signer_registry() {
        let (mut engine, _) = pending_reduce_only_engine(
            "GAS",
            Decimal::from(1_000),
            Decimal::new(11, 2),
            Decimal::new(1, 1),
        );
        let cloid = engine.pending["GAS"].action.planned_cloid;

        engine.retire_unregistered_pending_actions(&BTreeSet::new(), 10_000);

        assert!(!engine.pending.contains_key("GAS"));
        assert!(!engine.emitted_production_cloids.contains(&cloid));
        assert_eq!(
            engine.action_lifecycle_events.last().unwrap().outcome,
            ActionAttemptOutcome::NoLongerRequired
        );
        assert_eq!(engine.unresolved_actionable_root_count(), 0);
    }

    #[test]
    fn startup_preserves_restored_pending_action_registered_with_signer() {
        let (mut engine, _) = pending_reduce_only_engine(
            "GAS",
            Decimal::from(1_000),
            Decimal::new(11, 2),
            Decimal::new(1, 1),
        );
        let cloid = engine.pending["GAS"].action.planned_cloid;

        engine.retire_unregistered_pending_actions(&BTreeSet::from([cloid]), 10_000);

        assert!(engine.pending.contains_key("GAS"));
        assert_eq!(engine.unresolved_actionable_root_count(), 1);
    }

    #[test]
    fn unissued_pending_action_is_superseded_from_actual_committed_position() {
        let asset = "TEST";
        let component = "source:pending";
        let (mut engine, _) = pending_reduce_only_engine(
            asset,
            Decimal::from(1_000),
            Decimal::new(11, 2),
            Decimal::new(1, 1),
        );
        engine.ledger = DualLedger::default();
        let pending = engine.pending.get_mut(asset).unwrap();
        pending.action.side = Side::Sell;
        pending.action.rounded_notional = Decimal::from(20);
        pending.action.reduce_only = false;
        pending.remaining_order_quantity = Decimal::from(200);
        pending.component_remaining =
            BTreeMap::from([(component.to_string(), Decimal::from(-200))]);
        let old_cloid = pending.action.planned_cloid;

        let decision = engine.construct_next_decision(2_000).unwrap().unwrap();

        assert_eq!(
            decision
                .projection
                .constrained_targets
                .get(asset)
                .copied()
                .unwrap_or_default(),
            Decimal::ZERO
        );
        assert!(!engine.pending.contains_key(asset));
        assert!(engine.executions.is_empty());
        assert!(engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == old_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::SupersededByNewTarget
        }));
    }

    #[test]
    fn emitted_pending_action_waits_for_terminal_resolution_before_replan() {
        let asset = "TEST";
        let component = "source:pending";
        let (mut engine, _) = pending_reduce_only_engine(
            asset,
            Decimal::from(1_000),
            Decimal::new(11, 2),
            Decimal::new(1, 1),
        );
        engine.ledger = DualLedger::default();
        let pending = engine.pending.get_mut(asset).unwrap();
        pending.action.side = Side::Sell;
        pending.action.rounded_notional = Decimal::from(20);
        pending.action.reduce_only = false;
        pending.remaining_order_quantity = Decimal::from(200);
        pending.component_remaining =
            BTreeMap::from([(component.to_string(), Decimal::from(-200))]);
        let old_cloid = pending.action.planned_cloid;
        let old_attribution = pending.component_remaining.clone();
        engine.emitted_production_cloids.insert(old_cloid);
        engine.production_positions = Some(BTreeMap::from([(asset.to_string(), Decimal::ZERO)]));
        engine.production_equities =
            Some((Decimal::from(100), Decimal::from(100), Decimal::from(100)));

        engine.construct_next_decision(2_000).unwrap().unwrap();

        assert_eq!(engine.pending[asset].action.planned_cloid, old_cloid);
        assert_eq!(engine.pending[asset].component_remaining, old_attribution);
        assert!(!engine.action_lifecycle_events.iter().any(|event| {
            event.planned_cloid == old_cloid.to_string()
                && event.outcome == ActionAttemptOutcome::SupersededByNewTarget
        }));
    }

    #[test]
    fn target_replacement_does_not_rewrite_live_action_attribution() {
        let asset = "TEST";
        let component = "source:issuance";
        let issued_notional = Decimal::new(113, 1);
        let reference_price = Decimal::new(1, 1);
        let issued_quantity = issued_notional.checked_div(reference_price).unwrap();
        let (mut engine, book) = pending_reduce_only_engine(
            asset,
            Decimal::from(1_000),
            Decimal::new(11, 2),
            reference_price,
        );
        engine.ledger = DualLedger::default();
        let pending = engine.pending.get_mut(asset).unwrap();
        pending.action.side = Side::Sell;
        pending.action.rounded_notional = issued_notional;
        pending.action.reduce_only = false;
        pending.remaining_order_quantity = issued_quantity;
        pending.component_remaining = BTreeMap::from([(component.to_string(), -issued_quantity)]);
        pending
            .execution
            .as_mut()
            .unwrap()
            .projection_input
            .filled_positions
            .clear();
        engine
            .target_ledger
            .replace_absolute_targets(
                &BTreeMap::from([(asset.to_string(), Decimal::ZERO)]),
                &BTreeMap::from([(asset.to_string(), Decimal::ZERO)]),
                &BTreeMap::new(),
                &BTreeMap::from([(asset.to_string(), -issued_notional)]),
                &BTreeMap::new(),
                TargetVersion(12_917),
                SnapshotSetId([13; 32]),
            )
            .unwrap();

        // A current target replacement cannot rewrite issuance provenance for
        // the already-retained action.
        assert_eq!(
            engine.pending[asset].component_remaining,
            BTreeMap::from([(component.to_string(), -issued_quantity)])
        );

        engine.try_execute_pending(&book, 10_000).unwrap();
        let opening = engine.executions.last().unwrap();
        assert_eq!(opening.asset, asset);
        assert_eq!(opening.side, "sell");
        assert_eq!(opening.position_before, Decimal::ZERO);
        assert!(opening.position_after < Decimal::ZERO);
        assert_eq!(
            opening.source_filled_quantities.keys().collect::<Vec<_>>(),
            vec![component]
        );
        assert_eq!(
            engine.ledger.source_position(component, asset),
            opening.position_after
        );

        // The newest desired state remains zero. Normal reconciliation must
        // therefore emit a separate reduce-only BUY and return both ledgers
        // to zero without reusing or duplicating the opening attribution.
        engine.construct_next_decision(10_001).unwrap().unwrap();
        let flatten = &engine.pending[asset].action;
        assert_eq!(flatten.side, Side::Buy);
        assert!(flatten.reduce_only);
        engine.try_execute_pending(&book, 20_000).unwrap();
        assert_eq!(engine.authoritative_position(asset), Decimal::ZERO);
        assert_eq!(
            engine.ledger.source_position(component, asset),
            Decimal::ZERO
        );
        assert_eq!(engine.executions.len(), 2);
        assert_eq!(
            engine.executions[1].component_close_quantities,
            BTreeMap::from([(component.to_string(), engine.executions[0].filled_quantity)])
        );
        assert!(engine.executions[1].component_open_quantities.is_empty());
        validate_source_reconciliation(&engine.ledger, asset, Decimal::ZERO, Decimal::ONE).unwrap();
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

    fn pending_reduce_only_engine(
        asset: &str,
        opening_quantity: Decimal,
        opening_price: Decimal,
        current_reference_price: Decimal,
    ) -> (DecisionEngine, OrderBookResponse) {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let mut engine =
            DecisionEngine::new(config, b"stale-reduce-only", "run", 40_000, 80_000).unwrap();
        engine.set_source_tier(&candidate, SourceTier::Active);

        let opening = crossing_test_execution(
            asset,
            Side::Buy,
            opening_quantity,
            opening_price,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        engine
            .ledger
            .apply_portfolio_execution(&opening, 1)
            .unwrap();
        engine
            .ledger
            .apply_source_execution("technical:range_mean_reversion:range", &opening, 1)
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate,
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1_000,
                        positions: BTreeMap::new(),
                        closed_candles: Vec::new(),
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
                        mids: BTreeMap::from([(asset.to_string(), current_reference_price)]),
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
                            name: asset.to_string(),
                            size_decimals: 0,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        let price_step = Decimal::new(1, 6);
        let book = OrderBookResponse {
            asset: asset.to_string(),
            source_time_ms: 1_000,
            bids: vec![BookLevel {
                price: current_reference_price - price_step,
                quantity: opening_quantity * Decimal::from(2),
            }],
            asks: vec![BookLevel {
                price: current_reference_price + price_step,
                quantity: opening_quantity * Decimal::from(2),
            }],
        };
        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(book.clone()),
                    ReadRequestKind::OrderBook,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine.construct_next_decision(1_001).unwrap().unwrap();
        assert!(engine.pending[asset].action.reduce_only);
        (engine, book)
    }

    fn replace_current_target(
        engine: &mut DecisionEngine,
        asset: &str,
        desired: Decimal,
        target_version: TargetVersion,
        reference_price: Decimal,
    ) {
        let filled = engine.authoritative_position(asset) * reference_price;
        let targets = BTreeMap::from([(asset.to_string(), desired)]);
        engine
            .target_ledger
            .replace_absolute_targets(
                &targets,
                &targets,
                &BTreeMap::from([(asset.to_string(), filled)]),
                &BTreeMap::new(),
                &BTreeMap::new(),
                target_version,
                SnapshotSetId([41; 32]),
            )
            .unwrap();
    }

    #[test]
    fn stale_reduce_only_reversal_is_canceled_for_normal_crossing_replan() {
        let (mut engine, book) = pending_reduce_only_engine(
            "RESOLV",
            Decimal::from(2_085),
            Decimal::new(17_723_128_537_170_263, 18),
            Decimal::new(17_698, 6),
        );
        let pending_version = engine.pending["RESOLV"].action.target_version;
        replace_current_target(
            &mut engine,
            "RESOLV",
            Decimal::from(-20),
            TargetVersion(pending_version.0 + 1),
            Decimal::new(17_698, 6),
        );

        assert_eq!(
            engine.try_execute_pending(&book, 10_000).unwrap(),
            ExecutionRecompute::None
        );
        assert!(!engine.pending.contains_key("RESOLV"));
        assert!(engine.executions.is_empty());
        assert_eq!(
            engine.action_lifecycle_events.last().unwrap().outcome,
            ActionAttemptOutcome::SupersededByNewTarget
        );
    }

    #[test]
    fn stale_reduce_only_that_is_already_satisfied_is_canceled_cleanly() {
        let reference_price = Decimal::new(17_698, 6);
        let (mut engine, book) = pending_reduce_only_engine(
            "RESOLV",
            Decimal::from(2_085),
            Decimal::new(17_723_128_537_170_263, 18),
            reference_price,
        );
        let pending_version = engine.pending["RESOLV"].action.target_version;
        let committed = engine.authoritative_position("RESOLV") * reference_price;
        replace_current_target(
            &mut engine,
            "RESOLV",
            committed,
            TargetVersion(pending_version.0 + 1),
            reference_price,
        );

        assert_eq!(
            engine.try_execute_pending(&book, 10_000).unwrap(),
            ExecutionRecompute::None
        );
        assert!(!engine.pending.contains_key("RESOLV"));
        assert!(engine.executions.is_empty());
        assert_eq!(
            engine.action_lifecycle_events.last().unwrap().outcome,
            ActionAttemptOutcome::NoLongerRequired
        );
        let density = engine.executable_density_summary().unwrap();
        assert_eq!(density.unresolved_actionable_targets, 0);
        assert!(density.root_conservation_verified);
    }

    #[test]
    fn stale_reduce_only_that_remains_a_reduction_is_resized_from_current_quantity() {
        let reference_price = Decimal::new(17_698, 6);
        let (mut engine, book) = pending_reduce_only_engine(
            "RESOLV",
            Decimal::from(2_085),
            Decimal::new(17_723_128_537_170_263, 18),
            reference_price,
        );
        let pending_version = engine.pending["RESOLV"].action.target_version;
        let position_before = engine.authoritative_position("RESOLV");
        let desired = position_before * reference_price / Decimal::from(2);
        replace_current_target(
            &mut engine,
            "RESOLV",
            desired,
            TargetVersion(pending_version.0 + 1),
            reference_price,
        );

        assert!(engine.pending["RESOLV"].action.reduce_only);
        engine.try_execute_pending(&book, 10_000).unwrap();
        let execution = engine.executions.last().unwrap();
        assert_eq!(execution.side, "sell");
        assert!(execution.filled_quantity > Decimal::ZERO);
        assert!(execution.filled_quantity <= position_before);
        let position_after = engine.authoritative_position("RESOLV");
        assert!(position_after > Decimal::ZERO);
        assert!(position_after < position_before);
    }

    #[test]
    fn same_version_market_drift_cancellation_is_asset_agnostic() {
        for asset in ["BTC", "ETH", "BABY", "0G", "kPEPE", "FUTURE-PAIR"] {
            let reference_price = Decimal::from(4);
            let (mut engine, book) = pending_reduce_only_engine(
                asset,
                Decimal::from(10),
                Decimal::from(5),
                reference_price,
            );
            let pending_version = engine.pending[asset].action.target_version;
            replace_current_target(
                &mut engine,
                asset,
                Decimal::from(45),
                pending_version,
                reference_price,
            );

            assert_eq!(
                engine.try_execute_pending(&book, 10_000).unwrap(),
                ExecutionRecompute::None,
                "asset {asset}"
            );
            assert!(!engine.pending.contains_key(asset), "asset {asset}");
            assert!(engine.executions.is_empty(), "asset {asset}");
            assert_eq!(
                engine.action_lifecycle_events.last().unwrap().outcome,
                ActionAttemptOutcome::SupersededByNewTarget,
                "asset {asset}"
            );
        }
    }

    #[test]
    fn partial_position_change_obsoletes_pending_reduce_only_without_execution() {
        let asset = "PARTIAL";
        let reference_price = Decimal::from(4);
        let (mut engine, book) =
            pending_reduce_only_engine(asset, Decimal::from(10), Decimal::from(5), reference_price);
        let pending_version = engine.pending[asset].action.target_version;
        let partial_close = crossing_test_execution(
            asset,
            Side::Sell,
            Decimal::from(5),
            reference_price,
            Decimal::from(10),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        engine
            .ledger
            .apply_portfolio_execution(&partial_close, 2)
            .unwrap();
        engine
            .ledger
            .apply_source_execution("technical:range_mean_reversion:range", &partial_close, 2)
            .unwrap();
        replace_current_target(
            &mut engine,
            asset,
            Decimal::from(30),
            pending_version,
            reference_price,
        );

        assert_eq!(
            engine.try_execute_pending(&book, 10_000).unwrap(),
            ExecutionRecompute::None
        );
        assert!(!engine.pending.contains_key(asset));
        assert_eq!(engine.authoritative_position(asset), Decimal::from(5));
    }

    #[test]
    fn sub_step_destination_increase_obsoletes_pending_reduce_only() {
        let asset = "ROUNDING";
        let reference_price = Decimal::from(4);
        let (mut engine, book) =
            pending_reduce_only_engine(asset, Decimal::from(10), Decimal::from(5), reference_price);
        let pending_version = engine.pending[asset].action.target_version;
        replace_current_target(
            &mut engine,
            asset,
            Decimal::new(416, 1),
            pending_version,
            reference_price,
        );

        assert_eq!(
            engine.try_execute_pending(&book, 10_000).unwrap(),
            ExecutionRecompute::None
        );
        assert!(!engine.pending.contains_key(asset));
        assert!(engine.executions.is_empty());
    }

    #[test]
    fn anime_neutralized_technical_position_reaches_reduce_only_exit_planner() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let mut engine =
            DecisionEngine::new(config, b"anime-neutralization", "run", 40_000, 80_000).unwrap();
        engine.set_source_tier(&candidate, SourceTier::Active);

        let opening = crossing_test_execution(
            "ANIME",
            Side::Buy,
            Decimal::from(14_276),
            Decimal::new(259, 5),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        engine
            .ledger
            .apply_portfolio_execution(&opening, 1)
            .unwrap();
        engine
            .ledger
            .apply_source_execution("technical:range_mean_reversion:range", &opening, 1)
            .unwrap();

        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate,
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1_000,
                        positions: BTreeMap::new(),
                        closed_candles: Vec::new(),
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
                        mids: BTreeMap::from([("ANIME".into(), Decimal::new(259, 5))]),
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
                            name: "ANIME".into(),
                            size_decimals: 0,
                            growth_mode: false,
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .ingest(
                accepted(
                    PublicPayload::OrderBook(OrderBookResponse {
                        asset: "ANIME".into(),
                        source_time_ms: 1_000,
                        bids: vec![BookLevel {
                            price: Decimal::new(2_589, 6),
                            quantity: Decimal::from(20_000),
                        }],
                        asks: vec![BookLevel {
                            price: Decimal::new(2_591, 6),
                            quantity: Decimal::from(20_000),
                        }],
                    }),
                    ReadRequestKind::OrderBook,
                    1_000,
                ),
                1_000,
            )
            .unwrap();

        let decision = engine.construct_next_decision(1_001).unwrap().unwrap();
        assert_eq!(
            decision.micro_slots["ANIME"].admitted_notional,
            Decimal::ZERO
        );
        assert!(decision.projection.rounded_deltas["ANIME"].is_sign_negative());
        let pending = &engine.pending["ANIME"];
        assert!(pending.action.reduce_only);
        assert_eq!(pending.action.side, Side::Sell);
        assert_ne!(pending.action.rounded_notional, Decimal::ZERO);
    }

    #[test]
    fn missing_book_then_partial_ioc_retains_and_replans_the_current_target() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        config.candidates.truncate(1);
        config.starting_equity_usd = 200.0;
        // This fixture exercises book retention, partial execution, and
        // snapshot continuity. Keep technical demand out of the economic
        // decision now that it cannot independently authorize risk.
        config.technical.enabled = false;
        let mut engine =
            DecisionEngine::new(config, b"partial-fixture", "run", 40_000, 80_000).unwrap();
        engine.set_source_tier(&candidate, SourceTier::Active);
        seed_positive_mfce_backoff(&mut engine, "BTC");
        let persisted_candle = ClosedCandle {
            asset: "BTC".into(),
            interval: CandleInterval::FifteenMinutes,
            open_time_ms: 0,
            close_time_ms: 900_000,
            open: Decimal::from(100),
            high: Decimal::from(102),
            low: Decimal::from(99),
            close: Decimal::from(101),
            base_volume: Decimal::from(10),
            trade_count: 5,
        };
        assert_eq!(
            engine
                .technical_engine
                .accept_closed_candle(persisted_candle.clone())
                .unwrap(),
            CandleAcceptance::Accepted
        );
        for index in 0..15_u64 {
            engine
                .technical_engine
                .accept_closed_candle(ClosedCandle {
                    asset: "BTC".into(),
                    interval: CandleInterval::OneHour,
                    open_time_ms: index * 3_600_000,
                    close_time_ms: (index + 1) * 3_600_000,
                    open: Decimal::from(100),
                    high: Decimal::from(110),
                    low: Decimal::from(90),
                    close: Decimal::from(100),
                    base_volume: Decimal::from(10),
                    trade_count: 5,
                })
                .unwrap();
        }
        let expected_hysteresis = Decimal::new(7, 1);
        let mut technical_state = serde_json::to_value(&engine.technical_engine).unwrap();
        technical_state["assets"]["BTC"]["current_score"] =
            serde_json::to_value(expected_hysteresis).unwrap();
        engine.technical_engine = serde_json::from_value(technical_state).unwrap();
        let positions = [(
            "BTC".to_string(),
            SourceAssetPosition {
                asset: "BTC".to_string(),
                signed_size: Decimal::from(10),
                signed_notional: Decimal::from(1_000),
                entry_price: Some(Decimal::from(100)),
                unrealized_pnl: Some(Decimal::ZERO),
            },
        )]
        .into_iter()
        .collect();
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        block_number: None,
                        candidate_id: candidate,
                        account_value: Decimal::from(1_000),
                        source_time_ms: 1_000,
                        positions,
                        closed_candles: Vec::new(),
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
                            growth_mode: false,
                        }],
                        contexts: [(
                            "BTC".to_string(),
                            MarketAssetContext {
                                funding_rate_hourly: Decimal::new(1, 5),
                            },
                        )]
                        .into_iter()
                        .collect(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                        dex_fee_scales: BTreeMap::new(),
                        user_fee_state: None,
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine.construct_next_decision(1_000).unwrap();
        assert!(engine
            .mfce
            .awaiting_book_assets()
            .any(|asset| asset == "BTC"));
        assert!(engine.desired_books.contains("BTC"));

        let shallow_book = OrderBookResponse {
            asset: "BTC".to_string(),
            source_time_ms: 2_000,
            bids: vec![BookLevel {
                price: Decimal::new(9_999, 2),
                quantity: Decimal::new(1, 2),
            }],
            asks: vec![BookLevel {
                price: Decimal::new(10_001, 2),
                quantity: Decimal::new(1, 2),
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
        let original_pending_root = engine.pending["BTC"].root_planned_cloid.clone();
        let original_pending_cloid = engine.pending["BTC"].action.planned_cloid;
        let lifecycle_count = engine.action_lifecycle_events.len();
        engine.construct_next_decision(2_001).unwrap();
        assert_eq!(
            engine.pending["BTC"].root_planned_cloid,
            original_pending_root
        );
        assert_eq!(
            engine.pending["BTC"].action.planned_cloid,
            original_pending_cloid
        );
        assert_eq!(engine.action_lifecycle_events.len(), lifecycle_count);
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
        assert_eq!(
            pending_component_total(follow_up.action.side, &follow_up.component_remaining).unwrap(),
            follow_up.remaining_order_quantity
        );
        assert_eq!(follow_up.action.retry_generation, 1);
        assert_eq!(
            follow_up
                .execution
                .as_ref()
                .and_then(|context| context.parent_planned_cloid.as_deref()),
            Some(partial.planned_cloid.as_str())
        );
        assert_ne!(
            follow_up.action.planned_cloid.to_string(),
            partial.planned_cloid
        );
        let expected_root = follow_up.root_planned_cloid.clone();
        let expected_retry_generation = follow_up.action.retry_generation;
        let density = engine.executable_density_summary().unwrap();
        assert_eq!(density.root_actionable_targets, 1);
        assert_eq!(density.initial_ioc_attempts, 1);
        assert_eq!(density.partial_fills, 1);
        assert_eq!(density.unresolved_actionable_targets, 1);
        assert!(density.root_conservation_verified);
        let unresolved = engine.unresolved_actionable_roots();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].asset, "BTC");
        assert_eq!(unresolved[0].lifecycle, "pending_action");
        assert_eq!(unresolved[0].root_planned_cloid, expected_root);
        assert_eq!(
            unresolved[0].parent_planned_cloid.as_deref(),
            Some(partial.planned_cloid.as_str())
        );
        assert_eq!(
            unresolved[0].retry_generation,
            Some(expected_retry_generation)
        );
        engine.accrue_funding(3_603_000).unwrap();
        assert!(engine.accrued_funding["BTC"] > Decimal::ZERO);
        engine.technical_engine.reset_funnel();

        let state_path = std::env::temp_dir().join(format!(
            "engine-unsigned-state-{}-{}.msgpack",
            std::process::id(),
            engine.decision_sequence
        ));
        let identity = StateIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        let expected_ledger = engine.ledger.clone();
        let expected_targets = engine.target_ledger.clone();
        let expected_sequence = engine.decision_sequence;
        let expected_deployment_equity = engine.deployment_equity().unwrap();
        let expected_technical_state = rmp_serde::to_vec(&engine.technical_engine).unwrap();
        let expected_pending = rmp_serde::to_vec(&engine.pending).unwrap();
        let expected_funding = engine.accrued_funding.clone();
        let expected_mfce = engine.mfce.state().clone();
        let expected_mfce_time = engine.mfce_time_high_watermark;
        let expected_executions = rmp_serde::to_vec(&engine.executions).unwrap();
        let expected_equity_buckets = rmp_serde::to_vec(&engine.equity_buckets).unwrap();
        let expected_last_bucket = rmp_serde::to_vec(&engine.last_bucket).unwrap();
        let expected_micro_density = rmp_serde::to_vec(&engine.micro_density).unwrap();
        assert!(expected_mfce.assets["BTC"].active.is_some());
        assert_eq!(
            engine
                .persist_unsigned_state(&state_path, &identity)
                .unwrap(),
            0
        );

        let mut wrong_identity = identity.clone();
        wrong_identity.observer_binary_sha256 = "different-observer".into();
        let mut rejected = DecisionEngine::new(
            engine.config.clone(),
            b"partial-fixture",
            "rejected-run",
            40_000,
            80_000,
        )
        .unwrap();
        assert!(rejected
            .restore_unsigned_state(&state_path, &wrong_identity)
            .is_err());

        let mut restored = DecisionEngine::new(
            engine.config.clone(),
            b"partial-fixture",
            "restored-run",
            40_000,
            80_000,
        )
        .unwrap();
        assert_eq!(
            restored
                .restore_unsigned_state(&state_path, &identity)
                .unwrap(),
            0
        );
        restored.rebase_runtime_time(10, 10_000_000_000).unwrap();
        std::fs::remove_file(state_path).unwrap();

        assert_eq!(restored.ledger, expected_ledger);
        assert_eq!(restored.target_ledger, expected_targets);
        assert_eq!(restored.decision_sequence, expected_sequence);
        assert_eq!(restored.ledger.portfolio_open_count(), 1);
        assert_eq!(restored.durable_timestamp(10).unwrap(), 10_000_000_000);
        assert_eq!(
            rmp_serde::to_vec(&restored.pending).unwrap(),
            expected_pending
        );
        assert_eq!(restored.pending["BTC"].root_planned_cloid, expected_root);
        assert_eq!(
            restored.pending["BTC"].component_remaining,
            engine.pending["BTC"].component_remaining
        );
        assert_eq!(
            restored.pending["BTC"].remaining_order_quantity,
            engine.pending["BTC"].remaining_order_quantity
        );
        assert!(engine.pending["BTC"].execution.is_some());
        assert!(restored.pending["BTC"].execution.is_none());
        assert_eq!(
            restored.pending["BTC"].action.retry_generation,
            expected_retry_generation
        );
        assert!(restored.continuations.is_empty());
        assert_eq!(restored.accrued_funding, expected_funding);
        assert_eq!(
            rmp_serde::to_vec(&restored.executions).unwrap(),
            expected_executions
        );
        assert_eq!(
            rmp_serde::to_vec(&restored.equity_buckets).unwrap(),
            expected_equity_buckets
        );
        assert_eq!(
            rmp_serde::to_vec(&restored.last_bucket).unwrap(),
            expected_last_bucket
        );
        assert_eq!(
            rmp_serde::to_vec(&restored.micro_density).unwrap(),
            expected_micro_density
        );
        assert_eq!(restored.mfce.state(), &expected_mfce);
        assert_eq!(restored.mfce_time_high_watermark, expected_mfce_time);
        assert_eq!(
            rmp_serde::to_vec(&restored.technical_engine).unwrap(),
            expected_technical_state
        );
        assert_eq!(
            serde_json::to_value(&restored.technical_engine).unwrap()["assets"]["BTC"]
                ["current_score"],
            serde_json::to_value(expected_hysteresis).unwrap()
        );
        assert_eq!(
            restored
                .technical_engine
                .accept_closed_candle(persisted_candle)
                .unwrap(),
            CandleAcceptance::Duplicate
        );
        let restored_equity = restored.deployment_equity().unwrap();
        assert_eq!(
            restored_equity.current_equity,
            expected_deployment_equity.current_equity
        );
        assert_eq!(
            restored_equity.settled_equity,
            expected_deployment_equity.settled_equity
        );
        assert_eq!(
            restored_equity.deployment_equity,
            expected_deployment_equity.deployment_equity
        );
    }

    #[test]
    fn unsigned_snapshot_restore_allows_open_episode_while_marks_rehydrate() {
        let (mut engine, fill) = live_fill_engine("AERO");
        engine.apply_live_execution_fill(&fill).unwrap();
        assert_eq!(engine.ledger.portfolio_open_count(), 1);
        engine.mids = None;
        let path = std::env::temp_dir().join(format!(
            "open-without-mark-{}-{}.msgpack",
            std::process::id(),
            engine.decision_sequence
        ));
        let identity = StateIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        engine.persist_unsigned_state(&path, &identity).unwrap();

        let mut restored = DecisionEngine::new(
            engine.config.clone(),
            b"open-without-mark",
            "restored-run",
            40_000,
            80_000,
        )
        .unwrap();
        restored.restore_unsigned_state(&path, &identity).unwrap();
        assert_eq!(
            restored.ledger.portfolio_position("AERO"),
            Decimal::new(25, 2)
        );
        assert!(matches!(
            restored.record_equity_boundary(300_000),
            Err(EngineError::Core(reason)) if reason == "MissingMarkPrice"
        ));
    }

    #[test]
    fn unsigned_snapshot_persistence_canonicalizes_decimal_signed_zero() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let config = CopyTradeConfig::from_path(path).unwrap();
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        let first_path = std::env::temp_dir().join(format!(
            "copytrade-signed-zero-first-{}.msgpack",
            std::process::id()
        ));
        let second_path = std::env::temp_dir().join(format!(
            "copytrade-signed-zero-second-{}.msgpack",
            std::process::id()
        ));
        let mut engine =
            DecisionEngine::new(config.clone(), b"signed-zero", "first", 40_000, 80_000).unwrap();
        let negative_zero = -Decimal::ZERO;
        assert_eq!(negative_zero.to_string(), "-0");
        engine
            .accrued_funding
            .insert("signed-zero".into(), negative_zero);
        assert_eq!(
            engine
                .persist_unsigned_state(&first_path, &identity)
                .unwrap(),
            0
        );
        let first_bytes = std::fs::read(&first_path).unwrap();

        let mut restored =
            DecisionEngine::new(config, b"signed-zero", "second", 40_000, 80_000).unwrap();
        assert_eq!(
            restored
                .restore_unsigned_state(&first_path, &identity)
                .unwrap(),
            0
        );
        assert_eq!(restored.accrued_funding["signed-zero"], Decimal::ZERO);
        assert_eq!(restored.accrued_funding["signed-zero"].to_string(), "0");

        restored.snapshot_generation = None;
        assert_eq!(
            restored
                .persist_unsigned_state(&second_path, &identity)
                .unwrap(),
            0
        );
        let second_bytes = std::fs::read(&second_path).unwrap();
        std::fs::remove_file(first_path).unwrap();
        std::fs::remove_file(second_path).unwrap();
        assert_eq!(first_bytes, second_bytes);
        assert!(decode_unsigned_snapshot(&second_bytes, &identity).is_ok());
    }

    #[test]
    fn legacy_v12_wire_migrates_once_and_round_trips_current_semantics() {
        use crate::mfce::{
            LegacyMfceFeatureVectorV12A, LegacyMfceTrainingSampleV12A, MfceDecisionCounts,
            MfceTransitionKind,
        };

        let identity = SnapshotIdentity {
            source_tree_sha256: "legacy-source".into(),
            observer_binary_sha256: "legacy-engine".into(),
            configuration_sha256: "legacy-configuration".into(),
            risk_policy_sha256: "legacy-risk".into(),
        };
        let legacy_path =
            std::env::temp_dir().join(format!("engine-legacy-v12-{}.msgpack", std::process::id()));
        let current_path =
            std::env::temp_dir().join(format!("engine-current-v13-{}.msgpack", std::process::id()));
        let (mut source, _) = live_fill_engine("BNB");
        source
            .accrued_funding
            .insert("BNB".into(), Decimal::new(-7, 4));
        source
            .persist_unsigned_state(&current_path, &identity)
            .unwrap();
        let current = decode_unsigned_snapshot(&std::fs::read(&current_path).unwrap(), &identity)
            .unwrap()
            .payload;
        let old_features = LegacyMfceFeatureVectorV12A {
            version: 1,
            values: [Decimal::ZERO; MFCE_FEATURE_COUNT],
        };
        let legacy_mfce = LegacyMfcePersistentStateV12C {
            schema_version: 1,
            source_epoch: 17,
            next_transition_id: 23,
            next_sample_id: 2,
            last_retrain_attempt_sample_id: 1,
            assets: BTreeMap::new(),
            samples: std::collections::VecDeque::from([LegacyMfceTrainingSampleV12A {
                sample_id: 1,
                asset: "BNB".into(),
                direction: MfceDirection::Short,
                transition_kind: MfceTransitionKind::Opening,
                opened_at_mono: 10,
                completed_at_mono: 20,
                features: old_features,
                gross_return_bps: Decimal::from(25),
                lifetime_seconds: Decimal::from(10),
                admitted: true,
            }]),
            recovered_episodes: Vec::new(),
            incumbent: None,
            pending_training: None,
            decision_counts: MfceDecisionCounts {
                explore: 3,
                exploit: 2,
                reject: 1,
                allocated: 4,
            },
        };
        let legacy_payload = LegacyUnsignedObserverStateV12::from_current(current, legacy_mfce);
        let checksum_sha256 = legacy_unsigned_snapshot_checksum(
            LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            0,
            &identity,
            &legacy_payload,
        )
        .unwrap();
        let legacy = LegacyUnsignedSnapshotEnvelopeV12 {
            schema_version: LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            generation: 0,
            identity: identity.clone(),
            payload: legacy_payload,
            checksum_sha256,
        };
        std::fs::write(&legacy_path, rmp_serde::to_vec(&legacy).unwrap()).unwrap();

        let config = source.config.clone();
        let mut migrated =
            DecisionEngine::new(config.clone(), b"legacy-v12", "migrated", 40_000, 80_000).unwrap();
        migrated
            .restore_unsigned_state(&legacy_path, &identity)
            .unwrap();
        assert_eq!(
            migrated.pending["BNB"].remaining_order_quantity,
            Decimal::ONE
        );
        assert_eq!(migrated.accrued_funding["BNB"], Decimal::new(-7, 4));
        assert!(migrated.executions.is_empty());
        assert!(migrated.mfce.state().samples.is_empty());
        assert_eq!(
            migrated.mfce.state().schema_version,
            MFCE_STATE_SCHEMA_VERSION
        );
        assert_eq!(migrated.mfce.state().decision_counts.explore, 3);
        let semantic_a = migrated.persistence_semantic_fingerprint();
        migrated
            .persist_unsigned_state(&current_path, &identity)
            .unwrap();
        assert_eq!(
            decode_unsigned_snapshot(&std::fs::read(&current_path).unwrap(), &identity)
                .unwrap()
                .schema_version,
            UNSIGNED_SNAPSHOT_SCHEMA_VERSION
        );

        let mut restored =
            DecisionEngine::new(config, b"legacy-v12", "restored", 40_000, 80_000).unwrap();
        restored
            .restore_unsigned_state(&current_path, &identity)
            .unwrap();
        assert_eq!(restored.persistence_semantic_fingerprint(), semantic_a);
        std::fs::remove_file(legacy_path).unwrap();
        std::fs::remove_file(current_path).unwrap();
    }

    #[test]
    fn every_recovered_v12_mfce_topology_has_one_exact_decoder() {
        use crate::mfce::MfceDecisionCounts;
        use crate::mfce_delayed::{
            LegacyDelayedLearningV12A, LegacyDelayedLearningV12B, LegacyDelayedLearningV12C,
            LegacyDelayedLearningV12D, LegacyEpisodeProvenanceV12B, LegacyEpisodeProvenanceV12C,
            LegacyEpisodeProvenanceV12D, LegacyProvenanceHistoryV12B, LegacyProvenanceHistoryV12C,
            LegacyProvenanceHistoryV12D, SettledEpisodeEvidence,
        };

        let identity = SnapshotIdentity {
            source_tree_sha256: "schema-fixture".into(),
            observer_binary_sha256: "schema-fixture".into(),
            configuration_sha256: "schema-fixture".into(),
            risk_policy_sha256: "schema-fixture".into(),
        };
        let path = std::env::temp_dir().join(format!(
            "engine-v12-lineage-fixture-{}.msgpack",
            std::process::id()
        ));
        let (mut source, _) = live_fill_engine("BNB");
        source.persist_unsigned_state(&path, &identity).unwrap();
        let current_bytes = std::fs::read(&path).unwrap();
        let current = || {
            decode_unsigned_snapshot(&current_bytes, &identity)
                .unwrap()
                .payload
        };
        let counts = MfceDecisionCounts {
            explore: 3,
            exploit: 2,
            reject: 1,
            allocated: 4,
        };
        let delayed_a = LegacyDelayedLearningV12A {
            samples: Default::default(),
            next_id: 7,
            dropped: 2,
            flow: Default::default(),
            turnover: Default::default(),
            funding_at: None,
        };
        let delayed_b = LegacyDelayedLearningV12B {
            samples: Default::default(),
            next_id: 7,
            dropped: 2,
            flow: Default::default(),
            turnover: Default::default(),
            funding_at: None,
            provenance: LegacyProvenanceHistoryV12B {
                episodes: BTreeMap::from([(
                    "episode-b".into(),
                    LegacyEpisodeProvenanceV12B {
                        asset: "BNB".into(),
                        opened_at: 1,
                        closed_at: None,
                        transitions: vec![(1, AlphaOrigin::Source)],
                    },
                )]),
                actions: Default::default(),
            },
        };
        let delayed_c = LegacyDelayedLearningV12C {
            samples: Default::default(),
            next_id: 7,
            dropped: 2,
            flow: Default::default(),
            turnover: Default::default(),
            funding_at: None,
            provenance: LegacyProvenanceHistoryV12C {
                episodes: BTreeMap::from([(
                    "episode-c".into(),
                    LegacyEpisodeProvenanceV12C {
                        asset: "BNB".into(),
                        opened_at: 1,
                        closed_at: Some(2),
                        transitions: vec![(1, AlphaOrigin::Flow)],
                        settled: Some(SettledEpisodeEvidence {
                            opening_action: "action-c".into(),
                            external_manual_exit: false,
                            outcome: RealizedOutcome::settled(
                                Decimal::ONE,
                                Decimal::ZERO,
                                Decimal::ZERO,
                                Decimal::ZERO,
                            )
                            .unwrap(),
                        }),
                    },
                )]),
                actions: Default::default(),
            },
        };
        let delayed_d = LegacyDelayedLearningV12D {
            samples: Default::default(),
            next_id: 7,
            dropped: 2,
            flow: Default::default(),
            turnover: Default::default(),
            funding_at: None,
            provenance: LegacyProvenanceHistoryV12D {
                episodes: BTreeMap::from([
                    (
                        "episode-open".into(),
                        LegacyEpisodeProvenanceV12D {
                            asset: "BNB".into(),
                            opened_at: 1,
                            closed_at: None,
                            transitions: Vec::new(),
                            settled: None,
                        },
                    ),
                    (
                        "episode-settled".into(),
                        LegacyEpisodeProvenanceV12D {
                            asset: "HYPE".into(),
                            opened_at: 2,
                            closed_at: Some(3),
                            transitions: vec![(2, AlphaOrigin::Flow)],
                            settled: Some(SettledEpisodeEvidence {
                                opening_action: "action-d".into(),
                                external_manual_exit: true,
                                outcome: RealizedOutcome::settled(
                                    Decimal::ONE,
                                    Decimal::ZERO,
                                    Decimal::ZERO,
                                    Decimal::ZERO,
                                )
                                .unwrap(),
                            }),
                        },
                    ),
                ]),
                actions: Default::default(),
            },
        };
        let fixtures = vec![
            (
                LegacyMfceWireVariant::V12A,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12A {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        incumbent: None,
                        pending_training: None,
                    },
                    &identity,
                ),
            ),
            (
                LegacyMfceWireVariant::V12B,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12B {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        incumbent: None,
                        pending_training: None,
                        decision_counts: counts,
                    },
                    &identity,
                ),
            ),
            (
                LegacyMfceWireVariant::V12C,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12C {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        recovered_episodes: Vec::new(),
                        incumbent: None,
                        pending_training: None,
                        decision_counts: counts,
                    },
                    &identity,
                ),
            ),
            (
                LegacyMfceWireVariant::V12D,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12D {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        recovered_episodes: Vec::new(),
                        incumbent: None,
                        pending_training: None,
                        decision_counts: counts,
                        delayed: delayed_a,
                    },
                    &identity,
                ),
            ),
            (
                LegacyMfceWireVariant::V12E,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12E {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        recovered_episodes: Vec::new(),
                        incumbent: None,
                        pending_training: None,
                        decision_counts: counts,
                        delayed: delayed_b,
                    },
                    &identity,
                ),
            ),
            (
                LegacyMfceWireVariant::V12F,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12F {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        recovered_episodes: Vec::new(),
                        incumbent: None,
                        pending_training: None,
                        decision_counts: counts,
                        delayed: delayed_c,
                    },
                    &identity,
                ),
            ),
            (
                LegacyMfceWireVariant::V12G,
                legacy_fixture_bytes(
                    current(),
                    LegacyMfcePersistentStateV12G {
                        schema_version: 1,
                        source_epoch: 1,
                        next_transition_id: 2,
                        next_sample_id: 3,
                        last_retrain_attempt_sample_id: 0,
                        assets: Default::default(),
                        samples: Default::default(),
                        recovered_episodes: Vec::new(),
                        incumbent: None,
                        pending_training: None,
                        decision_counts: counts,
                        delayed: delayed_d,
                    },
                    &identity,
                ),
            ),
        ];
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12B>,
        >(&fixtures[0].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12A>,
        >(&fixtures[1].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12F>,
        >(&fixtures[4].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12E>,
        >(&fixtures[5].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12G>,
        >(&fixtures[4].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12G>,
        >(&fixtures[5].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12E>,
        >(&fixtures[6].1)
        .is_err());
        assert!(rmp_serde::from_slice::<
            LegacyUnsignedSnapshotEnvelopeV12<LegacyMfcePersistentStateV12F>,
        >(&fixtures[6].1)
        .is_err());
        for (expected_variant, bytes) in fixtures {
            assert_eq!(
                inspect_unsigned_snapshot_wire(&bytes).unwrap(),
                (
                    LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
                    Some(expected_variant)
                )
            );
            let decoded = decode_unsigned_snapshot(&bytes, &identity).unwrap();
            assert_eq!(decoded.schema_version, UNSIGNED_SNAPSHOT_SCHEMA_VERSION);
            assert!(decoded.payload.mfce.samples.is_empty());
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn unknown_legacy_shape_fails_before_engine_mutation_or_snapshot_write() {
        let identity = SnapshotIdentity {
            source_tree_sha256: "no-secret-values".into(),
            observer_binary_sha256: "no-secret-values".into(),
            configuration_sha256: "no-secret-values".into(),
            risk_policy_sha256: "no-secret-values".into(),
        };
        let root = tempfile::tempdir().unwrap();
        let current_path = root.path().join("current.msgpack");
        let unknown_path = root.path().join("unknown.msgpack");
        let (mut source, _) = live_fill_engine("BNB");
        source
            .persist_unsigned_state(&current_path, &identity)
            .unwrap();
        let current = decode_unsigned_snapshot(&std::fs::read(&current_path).unwrap(), &identity)
            .unwrap()
            .payload;
        let bytes = legacy_fixture_bytes(
            current,
            LegacyMfcePersistentStateV12A {
                schema_version: 1,
                source_epoch: 1,
                next_transition_id: 2,
                next_sample_id: 3,
                last_retrain_attempt_sample_id: 0,
                assets: Default::default(),
                samples: Default::default(),
                incumbent: None,
                pending_training: None,
            },
            &identity,
        );
        let mut wire: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        wire.as_array_mut().unwrap()[3].as_array_mut().unwrap()[13]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::Null);
        let unknown = rmp_serde::to_vec(&wire).unwrap();
        std::fs::write(&unknown_path, &unknown).unwrap();
        let before_hash = Sha256::digest(std::fs::read(&unknown_path).unwrap());
        let config = source.config.clone();
        let mut target =
            DecisionEngine::new(config, b"unknown-wire", "target", 40_000, 80_000).unwrap();
        let before_semantics = target.persistence_semantic_fingerprint();
        let error = target
            .restore_unsigned_state(&unknown_path, &identity)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("path=$.payload.mfce.decision_counts"),
            "{error}"
        );
        assert!(error.contains("observed=null"), "{error}");
        assert!(!error.contains("no-secret-values"));
        assert_eq!(target.persistence_semantic_fingerprint(), before_semantics);
        assert!(target.take_prepared_authorized_intents().is_empty());
        assert!(target.snapshot_generation().is_none());
        assert_eq!(
            Sha256::digest(std::fs::read(&unknown_path).unwrap()),
            before_hash
        );
        assert!(!unknown_path.with_extension("tmp").exists());
    }

    #[test]
    fn exact_legacy_v13a_pending_rows_migrate_and_neighboring_shapes_fail() {
        let identity = SnapshotIdentity {
            source_tree_sha256: "v13a-source".into(),
            observer_binary_sha256: "v13a-binary".into(),
            configuration_sha256: "v13a-configuration".into(),
            risk_policy_sha256: "v13a-risk".into(),
        };
        let path = std::env::temp_dir().join(format!(
            "engine-v13a-pending-fixture-{}.msgpack",
            std::process::id()
        ));
        let (mut source, _) = live_fill_engine("BNB");
        source.persist_unsigned_state(&path, &identity).unwrap();
        let mut wire: serde_json::Value =
            rmp_serde::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let envelope = wire.as_array_mut().unwrap();
        let payload = envelope[3].as_array_mut().unwrap();
        let pending = payload[7].as_object_mut().unwrap();
        assert!(!pending.is_empty());
        for row in pending.values_mut() {
            assert_eq!(row.as_array().unwrap().len(), 5);
            row.as_array_mut().unwrap().pop();
        }
        for row in payload[16].as_array_mut().unwrap() {
            assert_eq!(row.as_array().unwrap().len(), 36);
            row.as_array_mut().unwrap().pop();
        }
        let checksum_wire = serde_json::Value::Array(envelope[..4].to_vec());
        envelope[4] = serde_json::to_value(<[u8; 32]>::from(Sha256::digest(
            rmp_serde::to_vec(&checksum_wire).unwrap(),
        )))
        .unwrap();
        let legacy = rmp_serde::to_vec(&wire).unwrap();
        assert_eq!(
            inspect_v13_wire(&legacy).unwrap(),
            V13WireVariant::LegacyV13A
        );
        let decoded = decode_unsigned_snapshot(&legacy, &identity).unwrap();
        assert!(decoded
            .payload
            .pending
            .values()
            .all(|pending| pending.mfce_lineage == MfceExecutionLineage::default()));
        assert!(decoded
            .payload
            .executions
            .iter()
            .all(|execution| execution.mfce_lineage == MfceExecutionLineage::default()));

        let mut neighbor: serde_json::Value = rmp_serde::from_slice(&legacy).unwrap();
        let pending = neighbor.as_array_mut().unwrap()[3].as_array_mut().unwrap()[7]
            .as_object_mut()
            .unwrap();
        pending
            .values_mut()
            .next()
            .unwrap()
            .as_array_mut()
            .unwrap()
            .pop();
        let neighbor = rmp_serde::to_vec(&neighbor).unwrap();
        let error = inspect_v13_wire(&neighbor).unwrap_err().to_string();
        assert!(error.contains("path=$.payload.pending[*]"), "{error}");
        assert!(error.contains("observed=array[3]"), "{error}");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn unsigned_snapshot_restore_drops_only_legacy_zero_pending_components() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let config = CopyTradeConfig::from_path(path).unwrap();
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        let state_path = std::env::temp_dir().join(format!(
            "copytrade-zero-pending-component-{}.msgpack",
            std::process::id()
        ));
        let quantity = Decimal::new(148, 1);
        let mut first =
            DecisionEngine::new(config.clone(), b"zero-pending", "first", 40_000, 80_000).unwrap();
        first.pending.insert(
            "NEAR".into(),
            PendingAction {
                action: PlannedAction {
                    decision_id: DecisionId([1; 32]),
                    target_version: TargetVersion(1),
                    asset: "NEAR".into(),
                    side: Side::Sell,
                    rounded_notional: Decimal::from(28),
                    reduce_only: true,
                    action_ordinal: 0,
                    retry_generation: 0,
                    planned_cloid: PlannedCloid([2; 16]),
                },
                root_planned_cloid: PlannedCloid([2; 16]).to_string(),
                remaining_order_quantity: quantity,
                component_remaining: BTreeMap::from([
                    ("source:a".into(), -quantity),
                    ("source:b".into(), Decimal::ZERO),
                ]),
                mfce_lineage: Default::default(),
                execution: None,
            },
        );
        first
            .persist_unsigned_state(&state_path, &identity)
            .unwrap();
        let encoded = std::fs::read(&state_path).unwrap();
        assert_eq!(
            decode_unsigned_snapshot(&encoded, &identity)
                .unwrap()
                .payload
                .pending["NEAR"]
                .component_remaining
                .len(),
            2
        );

        let mut restored =
            DecisionEngine::new(config, b"zero-pending", "restored", 40_000, 80_000).unwrap();
        restored
            .restore_unsigned_state(&state_path, &identity)
            .unwrap();
        std::fs::remove_file(state_path).unwrap();
        assert_eq!(
            restored.pending["NEAR"].component_remaining,
            BTreeMap::from([("source:a".to_string(), -quantity)])
        );
        assert_eq!(
            pending_component_total(Side::Sell, &restored.pending["NEAR"].component_remaining)
                .unwrap(),
            quantity
        );
    }

    #[test]
    fn durable_state_rebases_across_process_clocks_without_economic_amnesia() {
        const INTERVAL: u64 = 300_000;
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let config = CopyTradeConfig::from_path(path).unwrap();
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        let state_path = std::env::temp_dir().join(format!(
            "copytrade-cross-process-time-{}.msgpack",
            std::process::id()
        ));

        let mut first =
            DecisionEngine::new(config.clone(), b"time-rebase", "first", 40_000, 80_000).unwrap();
        seed_positive_mfce_backoff(&mut first, "BTC");
        first.pending.insert(
            "BTC".into(),
            PendingAction {
                action: PlannedAction {
                    decision_id: DecisionId([1; 32]),
                    target_version: TargetVersion(1),
                    asset: "BTC".into(),
                    side: Side::Buy,
                    rounded_notional: Decimal::from(100),
                    reduce_only: false,
                    action_ordinal: 0,
                    retry_generation: 0,
                    planned_cloid: PlannedCloid([2; 16]),
                },
                root_planned_cloid: PlannedCloid([2; 16]).to_string(),
                remaining_order_quantity: Decimal::ONE,
                component_remaining: BTreeMap::from([("source:a".into(), Decimal::ONE)]),
                mfce_lineage: Default::default(),
                execution: None,
            },
        );
        first
            .rebase_runtime_time(1_500_000, 30_000_000_000)
            .unwrap();
        first.record_equity_boundary(1_500_000).unwrap();
        first.record_equity_boundary(1_800_000).unwrap();
        assert_eq!(first.equity_buckets.len(), 1);
        let historical_bucket = rmp_serde::to_vec(&first.equity_buckets[0]).unwrap();
        let open_bucket = rmp_serde::to_vec(&first.last_bucket).unwrap();
        let mfce_state = first.mfce.state().clone();
        let pending_state = rmp_serde::to_vec(&first.pending).unwrap();
        first
            .persist_unsigned_state(&state_path, &identity)
            .unwrap();

        let mut second =
            DecisionEngine::new(config, b"time-rebase", "second", 40_000, 80_000).unwrap();
        second
            .restore_unsigned_state(&state_path, &identity)
            .unwrap();
        second.rebase_runtime_time(10, 30_000_420_000).unwrap();
        assert_eq!(
            rmp_serde::to_vec(&second.equity_buckets[0]).unwrap(),
            historical_bucket
        );
        assert_eq!(rmp_serde::to_vec(&second.last_bucket).unwrap(), open_bucket);
        assert_eq!(second.mfce.state(), &mfce_state);
        assert_eq!(rmp_serde::to_vec(&second.pending).unwrap(), pending_state);
        assert_eq!(
            second.last_equity_boundary().unwrap() / INTERVAL,
            second.durable_timestamp(10).unwrap() / INTERVAL
        );

        // The replacement process starts near monotonic zero. Its next
        // wall-aligned boundary closes the restored open bucket exactly once.
        second.record_equity_boundary(180_010).unwrap();
        assert_eq!(second.equity_buckets.len(), 2);
        assert_eq!(second.equity_buckets[1].opened_at_mono, 30_000_300_000);
        assert_eq!(second.equity_buckets[1].closed_at_mono, 30_000_600_000);
        assert_eq!(second.last_equity_boundary(), Some(30_000_600_000));
        assert_eq!(second.mfce.state(), &mfce_state);
        assert_eq!(rmp_serde::to_vec(&second.pending).unwrap(), pending_state);
        std::fs::remove_file(state_path).unwrap();
    }

    #[test]
    fn unsigned_snapshot_rejects_every_noncanonical_or_unverified_form() {
        struct ReverseDecimalMap<'a>(&'a BTreeMap<String, Decimal>);

        impl serde::Serialize for ReverseDecimalMap<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut map = serializer.serialize_map(Some(self.0.len()))?;
                for (key, value) in self.0.iter().rev() {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }

        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let config = CopyTradeConfig::from_path(path).unwrap();
        let mut engine =
            DecisionEngine::new(config, b"snapshot-fixture", "snapshot-run", 40_000, 80_000)
                .unwrap();
        engine.accrued_funding = [
            ("alpha".to_string(), Decimal::ONE),
            ("zeta".to_string(), Decimal::from(2)),
        ]
        .into_iter()
        .collect();
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        let state_path = std::env::temp_dir().join(format!(
            "copytrade-unsigned-snapshot-corruption-{}.msgpack",
            std::process::id()
        ));
        engine
            .persist_unsigned_state(&state_path, &identity)
            .unwrap();
        let canonical = std::fs::read(&state_path).unwrap();
        std::fs::remove_file(state_path).unwrap();
        assert_eq!(
            decode_unsigned_snapshot(&canonical, &identity)
                .unwrap()
                .generation,
            0
        );

        assert!(decode_unsigned_snapshot(&canonical[..canonical.len() - 1], &identity).is_err());

        let mut changed_payload = decode_unsigned_snapshot(&canonical, &identity).unwrap();
        changed_payload.payload.decision_sequence += 1;
        assert!(decode_unsigned_snapshot(
            &encode_unsigned_snapshot(&changed_payload).unwrap(),
            &identity
        )
        .is_err());

        let mut changed_identity = decode_unsigned_snapshot(&canonical, &identity).unwrap();
        changed_identity.identity.observer_binary_sha256 = "changed-observer".into();
        changed_identity.checksum_sha256 = unsigned_snapshot_checksum(
            changed_identity.schema_version,
            changed_identity.generation,
            &changed_identity.identity,
            &changed_identity.payload,
        )
        .unwrap();
        assert!(decode_unsigned_snapshot(
            &encode_unsigned_snapshot(&changed_identity).unwrap(),
            &identity
        )
        .is_err());

        let mut unsupported_schema = decode_unsigned_snapshot(&canonical, &identity).unwrap();
        unsupported_schema.schema_version += 1;
        unsupported_schema.checksum_sha256 = unsigned_snapshot_checksum(
            unsupported_schema.schema_version,
            unsupported_schema.generation,
            &unsupported_schema.identity,
            &unsupported_schema.payload,
        )
        .unwrap();
        assert!(decode_unsigned_snapshot(
            &encode_unsigned_snapshot(&unsupported_schema).unwrap(),
            &identity
        )
        .is_err());

        let mut checksum_mismatch = decode_unsigned_snapshot(&canonical, &identity).unwrap();
        checksum_mismatch.checksum_sha256[0] ^= 0xff;
        assert!(decode_unsigned_snapshot(
            &encode_unsigned_snapshot(&checksum_mismatch).unwrap(),
            &identity
        )
        .is_err());

        let canonical_map = rmp_serde::to_vec(&engine.accrued_funding).unwrap();
        let reversed_map = rmp_serde::to_vec(&ReverseDecimalMap(&engine.accrued_funding)).unwrap();
        assert_eq!(canonical_map.len(), reversed_map.len());
        assert_ne!(canonical_map, reversed_map);
        let offset = canonical
            .windows(canonical_map.len())
            .position(|window| window == canonical_map)
            .expect("canonical accrued-funding map is embedded in the snapshot");
        let mut noncanonical_map_order = canonical.clone();
        noncanonical_map_order[offset..offset + canonical_map.len()].copy_from_slice(&reversed_map);
        assert!(decode_unsigned_snapshot(&noncanonical_map_order, &identity).is_err());

        let named_map_encoding =
            rmp_serde::to_vec_named(&decode_unsigned_snapshot(&canonical, &identity).unwrap())
                .unwrap();
        assert!(decode_unsigned_snapshot(&named_map_encoding, &identity).is_err());
    }

    #[test]
    fn technical_decision_uniqueness_state_survives_restart() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let config = CopyTradeConfig::from_path(path).unwrap();
        let mut engine =
            DecisionEngine::new(config.clone(), b"technical-state", "run", 40_000, 80_000).unwrap();
        engine.technical_decision_state.insert(
            "BTC".into(),
            TechnicalDecisionUniquenessState {
                technical_target: Decimal::from(42),
                target_version: 7,
            },
        );
        let identity = SnapshotIdentity {
            source_tree_sha256: "source".into(),
            observer_binary_sha256: "observer".into(),
            configuration_sha256: "configuration".into(),
            risk_policy_sha256: "risk".into(),
        };
        let state_path = std::env::temp_dir().join(format!(
            "copytrade-technical-decision-state-{}.msgpack",
            std::process::id()
        ));
        engine
            .persist_unsigned_state(&state_path, &identity)
            .unwrap();

        let mut restored =
            DecisionEngine::new(config, b"technical-state", "restored", 40_000, 80_000).unwrap();
        restored
            .restore_unsigned_state(&state_path, &identity)
            .unwrap();
        std::fs::remove_file(state_path).unwrap();
        assert_eq!(
            restored.technical_decision_state["BTC"],
            TechnicalDecisionUniquenessState {
                technical_target: Decimal::from(42),
                target_version: 7,
            }
        );
        assert!(restored.technical_decision_records.is_empty());
    }

    #[test]
    fn only_reductions_and_flattens_are_reduce_only() {
        assert!(requires_reduce_only(
            Decimal::from(20),
            Decimal::from(-20),
            Decimal::ZERO,
        ));
        assert!(requires_reduce_only(
            Decimal::from(20),
            Decimal::from(-10),
            Decimal::from(10),
        ));
        assert!(!requires_reduce_only(
            Decimal::from(20),
            Decimal::from(-30),
            Decimal::from(-10),
        ));
        assert!(!requires_reduce_only(
            Decimal::from(-20),
            Decimal::from(30),
            Decimal::from(10),
        ));
        assert!(!requires_reduce_only(
            Decimal::from(20),
            Decimal::from(10),
            Decimal::from(30),
        ));
        assert!(!requires_reduce_only(
            Decimal::new(-3061, 2),
            Decimal::new(3695, 2),
            Decimal::new(634, 2),
        ));
    }

    #[test]
    fn completed_technical_close_first_requires_same_root_recompute() {
        assert!(completed_close_first_reversal(
            true,
            Decimal::from(-30),
            Decimal::from(-20),
            Decimal::from(2),
            Decimal::ZERO,
            Decimal::from(2),
            Decimal::ZERO,
        ));
        assert!(completed_close_first_reversal(
            true,
            Decimal::from(30),
            Decimal::from(20),
            Decimal::from(-2),
            Decimal::ZERO,
            Decimal::from(2),
            Decimal::ZERO,
        ));
        assert!(!completed_close_first_reversal(
            true,
            Decimal::from(-30),
            Decimal::from(-20),
            Decimal::from(2),
            Decimal::ZERO,
            Decimal::ONE,
            Decimal::ONE,
        ));
        assert!(!completed_close_first_reversal(
            false,
            Decimal::from(-30),
            Decimal::from(-20),
            Decimal::from(2),
            Decimal::ZERO,
            Decimal::from(2),
            Decimal::ZERO,
        ));
    }
}

fn funding_rate_for(metadata: &MarketMetadataResponse, asset: &str) -> Option<Decimal> {
    metadata.contexts.get(asset).map(|c| c.funding_rate_hourly)
}
