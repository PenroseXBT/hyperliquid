use crate::cohort_layer::{is_cohort_candidate_label, PreparedVeryProfitableLayer};
#[cfg(test)]
use crate::mfce::evaluate_allocation_policy;
use crate::mfce::{
    allocate_cross_sectional, allocation_baseline_target, MfceAllocationInput,
    MfceCrossSectionalCandidate, MfceDirection, MfceEngine, MfceError, MfceFeatureVector,
    MfcePersistentState, MfceRejectionReason, MfceReport, MFCE_FEATURE_COUNT,
};
use crate::public_mainnet::{
    AcceptedPublicResponse, BookLevel, CandleResponse, MarketMetadataResponse,
    MarketSnapshotResponse, OrderBookResponse, PublicPayload, SourceStateResponse,
};
use copytrade_core::authorized_intent::{
    AuthorizedExecutionIntent, PreSigningContext, TimeInForce, AUTHORIZED_INTENT_SCHEMA_VERSION,
};
use copytrade_core::cohort::{
    aggregate_authoritative_positions, classify_attribution, AttributionTag, CohortAssetAggregate,
    CohortDecisionReason, CohortIndicatorRecord, CohortPenalties, CohortRiskFlags,
    CohortSignalInput, VeryProfitableCohortEngine,
};
use copytrade_core::configuration::CopyTradeConfig;
use copytrade_core::consensus::{
    bounded_additive_consensus, ConsensusInput, SourceExposureBook, SourceExposureState,
};
use copytrade_core::decision::{
    canonical_target_hash, construct_decision, construct_planned_actions, derive_config_hash,
    derive_planned_cloid, derive_projection_hash, derive_risk_policy_hash,
    hash_market_snapshot_bytes, hash_payload_bytes, DecisionConstructionInput, DecisionRecord,
    EngineInstanceId, ExclusionReason, PlannedCloidInput, PreviousTargetState, Side,
    SnapshotSetMember, SourceEligibilitySummary,
};
use copytrade_core::deployment_equity::{calculate_deployment_equity, DeploymentEquity};
use copytrade_core::execution_floor::{validates_rounded_order, ExecutionFloorPolicy};
use copytrade_core::exit_planning::{
    plan_risk_reducing_ioc, ExitPlanningBlock, ExitPlanningInput, ResidualClass,
};
use copytrade_core::ledger::DualLedger;
use copytrade_core::live_trading::{
    shadow_execution_from_verified_fill, VerifiedExchangeFill, VerifiedFundingEvent,
};
use copytrade_core::portfolio_risk::project_and_validate_portfolio;
use copytrade_core::portfolio_risk::{
    MarketRules, OpenOrderExposure, OpenOrderLifecycle, OrderSide, PortfolioProjectionInput,
};
use copytrade_core::scheduler::SourceTier;
use copytrade_core::scheduler::{SnapshotAcceptance, SnapshotStore, SourceSnapshot, Timestamp};
use copytrade_core::shadow::{
    execute_shadow_ioc, plan_marketable_ioc, DepthLevel, LatencyScenario, MarketableIocPricingMode,
    ShadowExecution, ShadowExecutionInput, ShadowMarketSnapshot,
};
use copytrade_core::target_state::VirtualTargetLedger;
use copytrade_core::technical::{
    CandleAcceptance, SignalArchetype, TechnicalContext, TechnicalEngine, TechnicalFunnel,
    TechnicalTarget,
};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
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
    pub cohort_indicator_records: u64,
    pub cohort_runtime_rejections: u64,
    pub technical_decision_records: u64,
    pub mfce_retrain_attempts: u64,
    pub mfce_model_promotions: u64,
    pub mfce_retrain_failures: u64,
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

fn absolute_directional_component_targets(
    contributions: Option<&BTreeMap<String, Decimal>>,
    portfolio_target: Decimal,
) -> Result<BTreeMap<String, Decimal>, LiveShadowError> {
    if portfolio_target.is_zero() {
        return Ok(BTreeMap::new());
    }
    let weights = normalized_directional_weights(contributions, portfolio_target)?;
    if weights.is_empty() {
        return Err(LiveShadowError::Core(
            "nonzero portfolio target has no directional component target".into(),
        ));
    }
    proportional_allocations(portfolio_target.abs(), &weights)?
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

const fn mfce_indicator(value: bool) -> Decimal {
    if value {
        Decimal::ONE
    } else {
        Decimal::ZERO
    }
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

fn mfce_leg_cost(
    asset: &str,
    levels: &[BookLevel],
    midpoint: Decimal,
    requested_notional: Decimal,
    is_buy: bool,
    maximum_slippage_bps: Decimal,
) -> Result<(Decimal, Decimal), LiveShadowError> {
    if levels.is_empty()
        || midpoint <= Decimal::ZERO
        || requested_notional <= Decimal::ZERO
        || maximum_slippage_bps < Decimal::ZERO
        || maximum_slippage_bps >= BPS_PER_UNIT_RETURN
    {
        return Err(LiveShadowError::InvalidMarket(asset.to_string()));
    }
    let maximum_slippage_fraction = maximum_slippage_bps
        .checked_div(BPS_PER_UNIT_RETURN)
        .ok_or(LiveShadowError::Arithmetic)?;
    let limit_price = if is_buy {
        Decimal::ONE.checked_add(maximum_slippage_fraction)
    } else {
        Decimal::ONE.checked_sub(maximum_slippage_fraction)
    }
    .and_then(|factor| midpoint.checked_mul(factor))
    .ok_or(LiveShadowError::Arithmetic)?;
    let executable = |price: Decimal| {
        if is_buy {
            price <= limit_price
        } else {
            price >= limit_price
        }
    };
    let requested_quantity = requested_notional
        .checked_div(midpoint)
        .ok_or(LiveShadowError::Arithmetic)?;
    let visible_notional = levels.iter().try_fold(Decimal::ZERO, |sum, level| {
        if level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO {
            return Err(LiveShadowError::InvalidMarket(asset.to_string()));
        }
        if !executable(level.price) {
            return Ok(sum);
        }
        level
            .price
            .checked_mul(level.quantity)
            .and_then(|notional| sum.checked_add(notional))
            .ok_or(LiveShadowError::Arithmetic)
    })?;
    // A displayed best quote outside the configured execution envelope cannot
    // be replaced by invented, better liquidity at the cap.
    if !levels.iter().any(|level| executable(level.price)) {
        return Err(LiveShadowError::InvalidMarket(asset.to_string()));
    }
    let depth_ratio = visible_notional
        .checked_div(requested_notional)
        .ok_or(LiveShadowError::Arithmetic)?
        .clamp(Decimal::ZERO, Decimal::from(10));
    let mut remaining = requested_quantity;
    let mut price_quantity = Decimal::ZERO;
    for level in levels {
        if remaining.is_zero() {
            break;
        }
        if !executable(level.price) {
            continue;
        }
        let quantity = remaining.min(level.quantity);
        price_quantity = level
            .price
            .checked_mul(quantity)
            .and_then(|value| price_quantity.checked_add(value))
            .ok_or(LiveShadowError::Arithmetic)?;
        remaining = remaining
            .checked_sub(quantity)
            .ok_or(LiveShadowError::Arithmetic)?;
    }
    if !remaining.is_zero() {
        // Missing displayed depth is conservatively charged at the execution
        // cap, which is never better than any executable visible level.
        price_quantity = limit_price
            .checked_mul(remaining)
            .and_then(|value| price_quantity.checked_add(value))
            .ok_or(LiveShadowError::Arithmetic)?;
    }
    let vwap = price_quantity
        .checked_div(requested_quantity)
        .ok_or(LiveShadowError::Arithmetic)?;
    let slippage_fraction = if is_buy {
        vwap.checked_sub(midpoint)
    } else {
        midpoint.checked_sub(vwap)
    }
    .and_then(|difference| difference.checked_div(midpoint))
    .ok_or(LiveShadowError::Arithmetic)?;
    let cost_bps = slippage_fraction
        .checked_mul(BPS_PER_UNIT_RETURN)
        .ok_or(LiveShadowError::Arithmetic)?
        .max(Decimal::ZERO);
    Ok((cost_bps, depth_ratio))
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
) -> Result<MfceLiveMarketContext, LiveShadowError> {
    let best_bid = book
        .bids
        .first()
        .ok_or_else(|| LiveShadowError::InvalidMarket(asset.to_string()))?
        .price;
    let best_ask = book
        .asks
        .first()
        .ok_or_else(|| LiveShadowError::InvalidMarket(asset.to_string()))?
        .price;
    if best_bid <= Decimal::ZERO || best_ask <= best_bid {
        return Err(LiveShadowError::InvalidMarket(asset.to_string()));
    }
    let midpoint = best_bid
        .checked_add(best_ask)
        .and_then(|value| value.checked_div(Decimal::from(2)))
        .ok_or(LiveShadowError::Arithmetic)?;
    let spread_bps = best_ask
        .checked_sub(best_bid)
        .and_then(|spread| spread.checked_div(midpoint))
        .and_then(|fraction| fraction.checked_mul(BPS_PER_UNIT_RETURN))
        .ok_or(LiveShadowError::Arithmetic)?;
    let notional = proposed_position_notional.abs();
    if notional.is_zero() {
        return Err(LiveShadowError::InvalidMarket(asset.to_string()));
    }
    let entry_notional = if !current_position_notional.is_zero()
        && current_position_notional.is_sign_positive()
            == proposed_position_notional.is_sign_positive()
    {
        notional
            .checked_sub(current_position_notional.abs())
            .ok_or(LiveShadowError::Arithmetic)?
            .max(Decimal::ZERO)
    } else {
        notional
    };
    if entry_notional.is_zero() {
        return Err(LiveShadowError::InvalidMarket(asset.to_string()));
    }
    let (entry_levels, exit_levels, entry_is_buy) = match direction {
        MfceDirection::Long => (&book.asks[..], &book.bids[..], true),
        MfceDirection::Short => (&book.bids[..], &book.asks[..], false),
    };
    let (entry_slippage_bps, entry_depth_ratio) = mfce_leg_cost(
        asset,
        entry_levels,
        midpoint,
        entry_notional,
        entry_is_buy,
        maximum_slippage_bps,
    )?;
    let (exit_slippage_bps, exit_depth_ratio) = mfce_leg_cost(
        asset,
        exit_levels,
        midpoint,
        notional,
        !entry_is_buy,
        maximum_slippage_bps,
    )?;
    let maximum_slippage_fraction = maximum_slippage_bps
        .checked_div(BPS_PER_UNIT_RETURN)
        .ok_or(LiveShadowError::Arithmetic)?;
    let bid_floor = Decimal::ONE
        .checked_sub(maximum_slippage_fraction)
        .and_then(|factor| midpoint.checked_mul(factor))
        .ok_or(LiveShadowError::Arithmetic)?;
    let ask_ceiling = Decimal::ONE
        .checked_add(maximum_slippage_fraction)
        .and_then(|factor| midpoint.checked_mul(factor))
        .ok_or(LiveShadowError::Arithmetic)?;
    let executable_depth = |levels: &[BookLevel], is_bid: bool| {
        levels.iter().try_fold(Decimal::ZERO, |sum, level| {
            if level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO {
                return Err(LiveShadowError::InvalidMarket(asset.to_string()));
            }
            let executable = if is_bid {
                level.price >= bid_floor
            } else {
                level.price <= ask_ceiling
            };
            if !executable {
                return Ok(sum);
            }
            level
                .price
                .checked_mul(level.quantity)
                .and_then(|value| sum.checked_add(value))
                .ok_or(LiveShadowError::Arithmetic)
        })
    };
    let bid_depth = executable_depth(&book.bids, true)?;
    let ask_depth = executable_depth(&book.asks, false)?;
    let depth_imbalance = bid_depth
        .checked_add(ask_depth)
        .filter(|total| !total.is_zero())
        .and_then(|total| bid_depth.checked_sub(ask_depth)?.checked_div(total))
        .unwrap_or_default();
    let direction_sign = Decimal::from(direction.sign());
    let funding_bps = funding_rate_hourly
        .checked_mul(holding_hours)
        .and_then(|value| value.checked_mul(direction_sign))
        .and_then(|value| value.checked_mul(BPS_PER_UNIT_RETURN))
        .ok_or(LiveShadowError::Arithmetic)?;
    // Entry/exit slippage are measured from midpoint, so together they
    // already include the live spread as well as depth impact.
    let entry_fraction = entry_notional
        .checked_div(notional)
        .ok_or(LiveShadowError::Arithmetic)?;
    let friction_bps = entry_slippage_bps
        .checked_mul(entry_fraction)
        .and_then(|entry| entry.checked_add(exit_slippage_bps))
        .and_then(|value| {
            taker_fee_bps
                .checked_mul(Decimal::ONE.checked_add(entry_fraction)?)
                .and_then(|fees| value.checked_add(fees))
        })
        .and_then(|value| value.checked_add(funding_bps))
        .ok_or(LiveShadowError::Arithmetic)?
        .max(Decimal::ZERO);
    Ok(MfceLiveMarketContext {
        friction_bps,
        spread_bps,
        entry_depth_ratio,
        exit_depth_ratio,
        depth_imbalance,
    })
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
) -> Result<MfceFeatureVector, LiveShadowError> {
    let direction = MfceDirection::from_signed(raw_source_exposure)
        .ok_or_else(|| LiveShadowError::Core("MFCE risk transition has no direction".into()))?;
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
            .ok_or(LiveShadowError::Arithmetic)?
    };
    let scale = |value: Decimal| {
        if available_gross.is_zero() {
            Ok(Decimal::ZERO)
        } else {
            value
                .checked_div(available_gross)
                .ok_or(LiveShadowError::Arithmetic)
        }
    };
    let technical_missing = mfce_indicator(technical.is_none());
    let atr_fraction = technical
        .map(|context| context.atr_fraction)
        .or(fallback_atr_fraction)
        .unwrap_or_default();
    let aligned = |value: Decimal| value.checked_mul(sign).ok_or(LiveShadowError::Arithmetic);
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
                .ok_or(LiveShadowError::Arithmetic)?,
            Decimal::from(context.state.trend_4h.score())
                .checked_mul(sign)
                .ok_or(LiveShadowError::Arithmetic)?,
            Decimal::from(context.state.trend_1h.score())
                .checked_mul(sign)
                .ok_or(LiveShadowError::Arithmetic)?,
            Decimal::from(context.state.trigger_30m.score())
                .checked_mul(sign)
                .ok_or(LiveShadowError::Arithmetic)?,
            Decimal::from(context.state.trigger_15m.score())
                .checked_mul(sign)
                .ok_or(LiveShadowError::Arithmetic)?,
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
                .ok_or(LiveShadowError::Arithmetic)?,
        )?,
        mfce_indicator(opening),
        mfce_indicator(expansion),
        mfce_indicator(reversal),
        atr_fraction
            .checked_mul(BPS_PER_UNIT_RETURN)
            .ok_or(LiveShadowError::Arithmetic)?,
        mfce_indicator(live.is_none()),
        live.map_or(Decimal::ZERO, |context| context.spread_bps),
        live.map_or(Decimal::ZERO, |context| context.entry_depth_ratio),
        live.map_or(Decimal::ZERO, |context| context.exit_depth_ratio),
        live.map_or(Decimal::ZERO, |context| context.depth_imbalance),
        funding_rate_hourly
            .checked_mul(sign)
            .and_then(|value| value.checked_mul(BPS_PER_UNIT_RETURN))
            .ok_or(LiveShadowError::Arithmetic)?,
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
    config: &CopyTradeConfig,
    deployment: DeploymentEquity,
) -> Result<Decimal, LiveShadowError> {
    let maximum_loss = deployment
        .starting_equity
        .checked_mul(
            Decimal::from_f64(config.max_account_drawdown_pct)
                .ok_or(LiveShadowError::Arithmetic)?,
        )
        .and_then(|value| value.checked_div(Decimal::from(100)))
        .ok_or(LiveShadowError::Arithmetic)?;
    let loss_already_realized_or_marked = deployment
        .starting_equity
        .checked_sub(deployment.current_equity)
        .ok_or(LiveShadowError::Arithmetic)?
        .max(Decimal::ZERO);
    Ok(maximum_loss
        .checked_sub(loss_already_realized_or_marked)
        .unwrap_or(Decimal::ZERO)
        .max(Decimal::ZERO))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicAttribution {
    SourceOnly,
    CohortOnly,
    TechnicalOnly,
    Hybrid,
    Disagreement,
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
) -> Result<ComponentFillPartition, LiveShadowError> {
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
        let open = quantity
            .checked_sub(close)
            .ok_or(LiveShadowError::Arithmetic)?;
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
) -> Result<Decimal, LiveShadowError> {
    if component_remaining.is_empty() {
        return Err(LiveShadowError::Core(
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
        .ok_or_else(|| {
            LiveShadowError::Core("pending action has invalid component quantity".into())
        })?;
    Ok(total)
}

fn consume_pending_component_fill(
    current_positions: &BTreeMap<String, Decimal>,
    side: Side,
    filled: Decimal,
    remaining_order_quantity: Decimal,
    component_remaining: &BTreeMap<String, Decimal>,
    size_step: Decimal,
) -> Result<ComponentFillPartition, LiveShadowError> {
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
        return Err(LiveShadowError::Core(
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
        return Err(LiveShadowError::Core(format!(
            "fill exceeds attributable pending order quantity: {filled} > {remaining_order_quantity}"
        )));
    }
    let mut allocations = if overfill <= tolerance {
        remaining
    } else {
        proportional_allocations(filled, &remaining)?
    };
    if overfill > tolerance {
        canonicalize_allocation_total(&mut allocations, filled)?;
    }

    let mut partition = split_component_quantities(current_positions, side, allocations.clone())?;
    partition.remaining_quantities = component_remaining
        .iter()
        .filter_map(|(component, quantity)| {
            let consumed = allocations.get(component).copied().unwrap_or_default();
            let remaining = quantity.abs().checked_sub(consumed)?;
            (!remaining.is_zero()).then(|| {
                (
                    component.clone(),
                    match side {
                        Side::Buy => remaining,
                        Side::Sell => -remaining,
                    },
                )
            })
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
) -> Result<ComponentFillPartition, LiveShadowError> {
    if filled.is_zero() {
        return Ok(ComponentFillPartition {
            close_quantities: BTreeMap::new(),
            open_quantities: BTreeMap::new(),
            total_quantities: BTreeMap::new(),
            remaining_quantities: BTreeMap::new(),
        });
    }
    if reference_price <= Decimal::ZERO {
        return Err(LiveShadowError::InvalidMarket(
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
            .ok_or(LiveShadowError::Arithmetic)
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
            return Err(LiveShadowError::Core(
                "portfolio flatten does not exclusively close component positions".into(),
            ));
        }
        let (difference, tolerance) =
            reconciliation_difference(total(&allocations)?, filled, size_step)?;
        if difference > tolerance {
            return Err(LiveShadowError::Core(
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
        .ok_or(LiveShadowError::Arithmetic)?;
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
            return Err(LiveShadowError::Core(
                "crossing fill old-side component close quantity does not reconcile".into(),
            ));
        }
        let opening_quantity = filled
            .checked_sub(close_total)
            .ok_or(LiveShadowError::Arithmetic)?;
        if opening_quantity <= Decimal::ZERO {
            return Err(LiveShadowError::Core(
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
            return Err(LiveShadowError::Core(
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
                .ok_or(LiveShadowError::Arithmetic)?;
            total_quantities.insert(component.clone(), combined);
        }
        let (difference, tolerance) =
            reconciliation_difference(total(&total_quantities)?, filled, size_step)?;
        if difference > tolerance {
            return Err(LiveShadowError::Core(
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
        return Err(LiveShadowError::Core(
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
        return Err(LiveShadowError::Core(
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
) -> Result<BTreeMap<String, Decimal>, LiveShadowError> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub component_close_quantities: BTreeMap<String, Decimal>,
    pub component_open_quantities: BTreeMap<String, Decimal>,
    pub source_filled_quantities: BTreeMap<String, Decimal>,
    pub economic_attribution: EconomicAttribution,
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
    pub micro_slots: BTreeMap<String, copytrade_core::decision::MicroPositionSlot>,
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
    pub regime: copytrade_core::technical::MarketRegime,
    pub archetype: copytrade_core::technical::SignalArchetype,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingAction {
    action: copytrade_core::decision::PlannedAction,
    root_planned_cloid: String,
    remaining_order_quantity: Decimal,
    component_remaining: BTreeMap<String, Decimal>,
    #[serde(skip)]
    execution: Option<PendingExecutionContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingExecutionContext {
    parent_planned_cloid: Option<String>,
    decision_book: ShadowMarketSnapshot,
    price_tick: Decimal,
    size_step: Decimal,
    projection_input: PortfolioProjectionInput,
    risk_policy_hash: copytrade_core::decision::RiskPolicyHash,
    configuration_hash: copytrade_core::decision::ConfigHash,
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
) -> Result<Decimal, LiveShadowError> {
    if replaces_outstanding {
        return Ok(current);
    }
    let mut projected = current;
    if let Some(pending_signed) = pending_signed {
        let endpoint = current
            .checked_add(pending_signed)
            .ok_or(LiveShadowError::Arithmetic)?;
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

fn pending_action_is_immaterial_replacement(
    previous: &copytrade_core::decision::PlannedAction,
    next: &copytrade_core::decision::PlannedAction,
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

#[derive(Debug, Default)]
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
    source_stream_mode: bool,
    source_stream_healthy: bool,
    source_market_coverage: BTreeSet<String>,
    mids: Option<(MarketSnapshotResponse, Timestamp)>,
    metadata: Option<MarketMetadataResponse>,
    metadata_received_at: Option<Timestamp>,
    metadata_valid_until: Option<Timestamp>,
    books: BTreeMap<String, (OrderBookResponse, Timestamp)>,
    previous_target: Option<PreviousTargetState>,
    decision_sequence: u64,
    pending: BTreeMap<String, PendingAction>,
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
    technical_delivery: TechnicalDeliveryCounters,
    prepared_authorized_intents: Vec<AuthorizedExecutionIntent>,
    emitted_production_cloids: BTreeSet<copytrade_core::decision::PlannedCloid>,
    production_identity: Option<ProductionIntentIdentity>,
    production_positions: Option<BTreeMap<String, Decimal>>,
    production_equities: Option<(Decimal, Decimal, Decimal)>,
    production_exposure_blocked: bool,
    technical_engine: TechnicalEngine,
    technical_targets: BTreeMap<String, TechnicalTarget>,
    technical_decision_state: BTreeMap<String, TechnicalDecisionUniquenessState>,
    technical_decision_records: Vec<TechnicalDecisionRecord>,
    very_profitable_layer: Option<PreparedVeryProfitableLayer>,
    very_profitable_engine: VeryProfitableCohortEngine,
    cohort_indicator_records: Vec<CohortIndicatorRecord>,
    mfce: MfceEngine,
    mfce_authorized_assets: BTreeMap<String, Timestamp>,
    mfce_time_offset: Timestamp,
    mfce_time_high_watermark: Timestamp,
    ledger_time_offset: Timestamp,
    ledger_time_high_watermark: Timestamp,
    snapshot_generation: Option<u64>,
}

pub const UNSIGNED_SNAPSHOT_SCHEMA_VERSION: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIdentity {
    pub source_tree_sha256: String,
    pub observer_binary_sha256: String,
    pub configuration_sha256: String,
    pub risk_policy_sha256: String,
}

pub type UnsignedShadowStateIdentity = SnapshotIdentity;

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
    last_mids: Option<MarketSnapshotResponse>,
    mfce: MfcePersistentState,
    mfce_time_high_watermark: Timestamp,
    ledger_time_high_watermark: Timestamp,
    executions: Vec<ShadowActionAccounting>,
    equity_buckets: Vec<EquityReturnBucket>,
    last_bucket: Option<(Timestamp, Decimal, BTreeMap<String, Decimal>)>,
    micro_density: MicroDensityCounters,
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

fn unsigned_snapshot_checksum(
    schema_version: u32,
    generation: u64,
    identity: &SnapshotIdentity,
    payload: &UnsignedObserverState,
) -> Result<[u8; 32], LiveShadowError> {
    let canonical = rmp_serde::to_vec(&(schema_version, generation, identity, payload))
        .map_err(|error| LiveShadowError::Core(error.to_string()))?;
    Ok(Sha256::digest(canonical).into())
}

fn encode_unsigned_snapshot(
    envelope: &UnsignedSnapshotEnvelope,
) -> Result<Vec<u8>, LiveShadowError> {
    rmp_serde::to_vec(envelope).map_err(|error| LiveShadowError::Core(error.to_string()))
}

fn decode_unsigned_snapshot(
    bytes: &[u8],
    expected_identity: &SnapshotIdentity,
) -> Result<UnsignedSnapshotEnvelope, LiveShadowError> {
    let envelope: UnsignedSnapshotEnvelope = rmp_serde::from_slice(bytes)
        .map_err(|error| LiveShadowError::Core(format!("invalid unsigned snapshot: {error}")))?;
    let canonical = encode_unsigned_snapshot(&envelope)?;
    if canonical != bytes {
        return Err(LiveShadowError::Core(
            "unsigned snapshot is not canonically encoded".into(),
        ));
    }
    if envelope.schema_version != UNSIGNED_SNAPSHOT_SCHEMA_VERSION {
        return Err(LiveShadowError::Core(
            "unsupported unsigned snapshot schema".into(),
        ));
    }
    if &envelope.identity != expected_identity {
        return Err(LiveShadowError::Core(
            "unsigned snapshot identity mismatch".into(),
        ));
    }
    let expected_checksum = unsigned_snapshot_checksum(
        envelope.schema_version,
        envelope.generation,
        &envelope.identity,
        &envelope.payload,
    )?;
    if envelope.checksum_sha256 != expected_checksum {
        return Err(LiveShadowError::Core(
            "unsigned snapshot checksum mismatch".into(),
        ));
    }
    Ok(envelope)
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
            source_stream_mode: false,
            source_stream_healthy: false,
            source_market_coverage: BTreeSet::new(),
            mids: None,
            metadata: None,
            metadata_received_at: None,
            metadata_valid_until: None,
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
            technical_delivery: TechnicalDeliveryCounters::default(),
            prepared_authorized_intents: Vec::new(),
            emitted_production_cloids: BTreeSet::new(),
            production_identity: None,
            production_positions: None,
            production_equities: None,
            production_exposure_blocked: false,
            technical_engine,
            technical_targets: BTreeMap::new(),
            technical_decision_state: BTreeMap::new(),
            technical_decision_records: Vec::new(),
            very_profitable_layer: None,
            very_profitable_engine: VeryProfitableCohortEngine::default(),
            cohort_indicator_records: Vec::new(),
            mfce: MfceEngine::default(),
            mfce_authorized_assets: BTreeMap::new(),
            mfce_time_offset: 0,
            mfce_time_high_watermark: 0,
            ledger_time_offset: 0,
            ledger_time_high_watermark: 0,
            snapshot_generation: None,
        })
    }

    pub fn install_very_profitable_layer(
        &mut self,
        layer: PreparedVeryProfitableLayer,
    ) -> Result<(), LiveShadowError> {
        let identity = self.config.very_profitable_layer.as_ref().ok_or_else(|| {
            LiveShadowError::Core(
                "prepared very_profitable layer is not bound into configuration identity".into(),
            )
        })?;
        if identity.artifact_sha256 != layer.artifact_sha256
            || identity.membership_set_hash != layer.resolution.membership_set_hash
            || identity.cohort_snapshot_timestamp_ms != layer.resolution.snapshot_timestamp_ms
        {
            return Err(LiveShadowError::Core(
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
            return Err(LiveShadowError::Core(
                "qualified cohort-only scheduler set does not match the prepared layer".into(),
            ));
        }
        self.very_profitable_layer = Some(layer);
        Ok(())
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

    pub fn synchronize_live_state(
        &mut self,
        state: &copytrade_core::live_trading::LiveTradingState,
    ) {
        self.production_positions = Some(state.positions());
        self.production_equities = Some((
            state.current_equity(),
            state.settled_equity(),
            state.deployment_equity(),
        ));
        self.production_exposure_blocked = false;
    }

    /// Applies an authenticated exchange fill to the same target/component
    /// lineage used by shadow execution. The fill consumes only the immutable
    /// allocation carried by the matching pending action; current MFCE targets
    /// remain authoritative solely for the next plan.
    pub fn apply_live_execution_fill(
        &mut self,
        fill: &VerifiedExchangeFill,
    ) -> Result<(), LiveShadowError> {
        let pending = self.pending.get(&fill.asset).cloned().ok_or_else(|| {
            LiveShadowError::Core("live fill has no matching pending action".into())
        })?;
        if pending.action.planned_cloid != fill.identity.cloid
            || pending.action.side != fill.side
            || pending.action.reduce_only != fill.reduce_only
        {
            return Err(LiveShadowError::Core(
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
            .ok_or_else(|| LiveShadowError::InvalidMarket(fill.asset.clone()))?;
        let pending_component_ids = pending
            .component_remaining
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let (equity_before, source_equity_before) =
            self.accounting_equities_with_sources(&pending_component_ids)?;
        let position_before = self.ledger.portfolio_position(&fill.asset);
        let mut execution = shadow_execution_from_verified_fill(fill, position_before)
            .map_err(|error| LiveShadowError::Core(error.to_string()))?;
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
            .max(fill.decision_timestamp)
            .checked_add(1)
            .ok_or(LiveShadowError::Arithmetic)?;
        self.ledger_time_high_watermark = ledger_timestamp;
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
        let (equity_after, source_equity_after) =
            self.accounting_equities_with_sources(&pending_component_ids)?;
        let net_pnl_delta = equity_after
            .checked_sub(equity_before)
            .ok_or(LiveShadowError::Arithmetic)?;
        let gross_pnl_delta = net_pnl_delta
            .checked_add(execution.fees)
            .and_then(|value| value.checked_add(execution.funding))
            .and_then(|value| value.checked_add(execution.slippage))
            .ok_or(LiveShadowError::Arithmetic)?;
        let mut source_attributed_returns = BTreeMap::new();
        for candidate in component_partition.total_quantities.keys() {
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
        let deployment = self.deployment_equity()?;
        self.executions.push(ShadowActionAccounting {
            shadow_execution_id: execution.shadow_execution_id.to_string(),
            decision_id: execution.action.decision_id.to_string(),
            asset: fill.asset.clone(),
            side: format!("{:?}", fill.side).to_ascii_lowercase(),
            execution_mode: "live_ioc".into(),
            root_planned_cloid: pending.root_planned_cloid.clone(),
            parent_planned_cloid: fill.parent_cloid.map(|cloid| cloid.to_string()),
            retry_generation: fill.continuation_generation,
            decision_timestamp_mono: fill.decision_timestamp,
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
                    .ok_or(LiveShadowError::Arithmetic)?
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
                LiveShadowError::Core("live partial fill lost its pending action".into())
            })?;
            retained.remaining_order_quantity = execution.unfilled_ioc_remainder;
            retained.component_remaining = component_partition.remaining_quantities;
        }
        self.metrics.shadow_executions = self.metrics.shadow_executions.saturating_add(1);
        self.refresh_desired_books();
        Ok(())
    }

    pub fn apply_live_funding(
        &mut self,
        event: &VerifiedFundingEvent,
    ) -> Result<(), LiveShadowError> {
        let funding_cost = Decimal::ZERO
            .checked_sub(event.amount)
            .ok_or(LiveShadowError::Arithmetic)?;
        let entry = self.accrued_funding.entry(event.asset.clone()).or_default();
        *entry = entry
            .checked_add(funding_cost)
            .ok_or(LiveShadowError::Arithmetic)?;
        Ok(())
    }

    /// Resolves an IOC after the exchange has made its terminal state
    /// authoritative. Any unfilled remainder is released back to current-target
    /// planning; an exchange-terminal action can no longer mutate the portfolio.
    pub fn resolve_live_execution_terminal(
        &mut self,
        cloid: copytrade_core::decision::PlannedCloid,
        original_quantity: Decimal,
        filled_quantity: Decimal,
        observed_at: Timestamp,
        rejected: bool,
    ) -> Result<(), LiveShadowError> {
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
            .ok_or(LiveShadowError::Arithmetic)?
            .max(Decimal::ZERO);
        let size_step = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.universe.iter().find(|item| item.name == asset))
            .map(|item| Decimal::new(1, item.size_decimals))
            .ok_or_else(|| LiveShadowError::InvalidMarket(asset.clone()))?;
        let (difference, tolerance) = reconciliation_difference(
            pending.remaining_order_quantity,
            expected_remaining,
            size_step,
        )?;
        if difference > tolerance {
            return Err(LiveShadowError::Core(
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
                        .ok_or(LiveShadowError::Arithmetic)?,
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

    /// Returns the bounded observer-only MFCE state suitable for inclusion in
    /// the existing production trading-state commit.
    pub fn mfce_persistence_snapshot(&self) -> (MfcePersistentState, Timestamp) {
        (self.mfce.state().clone(), self.mfce_time_high_watermark)
    }

    /// Restores MFCE state before production event processing. Runtime model
    /// handles are reconstructed as one pair, and the process-local monotonic
    /// clock is rebased beyond every persisted transition/sample timestamp.
    pub fn restore_mfce_persistent_state(
        &mut self,
        state: MfcePersistentState,
        time_high_watermark: Timestamp,
    ) -> Result<(), LiveShadowError> {
        if time_high_watermark < state.time_high_watermark() {
            return Err(LiveShadowError::Core(
                "persisted MFCE time high-watermark regressed".into(),
            ));
        }
        let time_offset = time_high_watermark
            .checked_add(1)
            .ok_or(LiveShadowError::Arithmetic)?;
        let replacement = MfceEngine::from_state(state).map_err(|error| {
            LiveShadowError::Core(format!("invalid persisted MFCE state: {error}"))
        })?;
        self.mfce = replacement;
        self.mfce_authorized_assets.clear();
        self.mfce_time_offset = time_offset;
        self.mfce_time_high_watermark = time_high_watermark;
        self.refresh_desired_books();
        Ok(())
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
    ) -> Result<Decimal, LiveShadowError> {
        let pending_signed = self
            .pending
            .get(asset)
            .map(|pending| match pending.action.side {
                Side::Buy => pending.action.rounded_notional,
                Side::Sell => -pending.action.rounded_notional,
            });
        projected_exposure_notional(
            current,
            pending_signed,
            self.target_ledger
                .get(asset)
                .map(|target| target.admitted_target_notional),
            replaces_outstanding,
        )
    }

    pub fn ingest(
        &mut self,
        response: AcceptedPublicResponse,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        match response.payload {
            PublicPayload::SourceState(mut state) => {
                let closed_candles = std::mem::take(&mut state.closed_candles);
                let mut technical_changed = false;
                for candle in closed_candles {
                    technical_changed |= self
                        .technical_engine
                        .accept_closed_candle(candle)
                        .map_err(|error| {
                            LiveShadowError::Core(format!("invalid closed candle: {error:?}"))
                        })?
                        == CandleAcceptance::Accepted;
                }
                let candidate_id = state.candidate_id.clone();
                let sequence = self
                    .source_sequences
                    .entry(candidate_id.clone())
                    .or_insert(0);
                *sequence = sequence.checked_add(1).ok_or(LiveShadowError::Arithmetic)?;
                let payload = serde_json::to_vec(&state)
                    .map_err(|error| LiveShadowError::Core(error.to_string()))?;
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
                            .accept(candidate_id, exposure_state);
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
                let admission_recomputed = refreshes_active_mfce
                    && self
                        .construct_next_decision(response.received_at_mono)?
                        .is_some();
                let execution_recompute = if refreshes_active_mfce && !admission_recomputed {
                    ExecutionRecompute::None
                } else {
                    self.try_execute_pending(&book, response.received_at_mono)?
                };
                if (!admission_recomputed && triggers_recompute)
                    || execution_recompute.is_required()
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

    /// Switches source freshness from snapshot age to the streaming invariant:
    /// every configured wallet has an authoritative baseline and the public
    /// trade stream has no unreconciled gap.
    pub fn enable_source_stream_mode(&mut self) {
        self.source_stream_mode = true;
        self.source_stream_healthy = false;
    }

    pub fn mark_source_stream_gap(
        &mut self,
        affected_markets: &BTreeSet<String>,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        if !self.source_stream_mode {
            return Err(LiveShadowError::Core(
                "source stream gap recorded outside stream mode".into(),
            ));
        }
        self.source_market_coverage
            .retain(|market| !affected_markets.contains(market));
        self.source_stream_healthy = !self.source_market_coverage.is_empty();
        self.construct_next_decision(now)?;
        Ok(())
    }

    pub fn complete_source_stream_reconciliation(
        &mut self,
        covered_markets: BTreeSet<String>,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        if !self.source_stream_mode {
            return Err(LiveShadowError::Core(
                "source stream reconciliation completed outside stream mode".into(),
            ));
        }
        if self.config.candidates.iter().any(|candidate| {
            self.source_store
                .latest(&candidate.address.to_ascii_lowercase())
                .is_none()
        }) {
            return Err(LiveShadowError::Core(
                "source stream reconciliation completed before every baseline".into(),
            ));
        }
        // Stream health and explicit market coverage, rather than the age of
        // an inactive wallet's last fill/baseline, authorize source
        // transitions after reconciliation.
        self.source_market_coverage = covered_markets;
        let valid_until = Timestamp::MAX;
        let mut assets = self.mfce.tracked_assets().cloned().collect::<BTreeSet<_>>();
        for candidate in &self.config.candidates {
            if let Some(snapshot) = self
                .source_store
                .latest(&candidate.address.to_ascii_lowercase())
            {
                assets.extend(snapshot.payload.positions.keys().cloned());
            }
        }
        for asset in assets {
            self.mfce_authorized_assets.insert(asset, valid_until);
        }
        self.source_stream_healthy = true;
        self.mfce
            .note_accepted_source_snapshot()
            .map_err(mfce_error)?;
        self.construct_next_decision(now)?;
        Ok(())
    }

    /// Replaces the markets whose complete tracked-wallet state is known
    /// since their public-trade coverage most recently resumed. A market
    /// outside this set is an explicit unknown and cannot contribute source
    /// exposure until the existing REST reconciliation sweep completes.
    pub fn replace_source_market_coverage(
        &mut self,
        covered_markets: BTreeSet<String>,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        if !self.source_stream_mode {
            return Err(LiveShadowError::Core(
                "source market coverage changed outside stream mode".into(),
            ));
        }
        self.source_market_coverage = covered_markets;
        if self.source_stream_healthy {
            self.construct_next_decision(now)?;
        }
        Ok(())
    }

    pub fn complete_source_market_reconciliation(
        &mut self,
        covered_markets: BTreeSet<String>,
        now: Timestamp,
    ) -> Result<(), LiveShadowError> {
        if !self.source_stream_mode || !self.source_stream_healthy {
            return Err(LiveShadowError::Core(
                "market reconciliation completed without a healthy source stream".into(),
            ));
        }
        self.source_market_coverage = covered_markets;
        let mut assets = self.mfce.tracked_assets().cloned().collect::<BTreeSet<_>>();
        for candidate in &self.config.candidates {
            if let Some(snapshot) = self
                .source_store
                .latest(&candidate.address.to_ascii_lowercase())
            {
                assets.extend(snapshot.payload.positions.keys().cloned());
            }
        }
        for asset in assets {
            if self.source_market_coverage.contains(&asset) {
                self.mfce_authorized_assets.insert(asset, Timestamp::MAX);
            }
        }
        self.mfce
            .note_accepted_source_snapshot()
            .map_err(mfce_error)?;
        self.construct_next_decision(now)?;
        Ok(())
    }

    pub fn source_market_coverage_count(&self) -> usize {
        self.source_market_coverage.len()
    }

    pub fn source_stream_healthy(&self) -> bool {
        !self.source_stream_mode || self.source_stream_healthy
    }

    #[cfg(test)]
    fn set_source_stream_healthy_for_test(&mut self, healthy: bool) {
        self.source_stream_healthy = healthy;
    }

    fn fresh_source(
        &self,
        candidate: &str,
        now: Timestamp,
    ) -> Option<&SourceSnapshot<SourceStateResponse>> {
        if self.source_stream_mode {
            return self
                .source_stream_healthy
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
    ) -> Result<(), LiveShadowError> {
        let existing_wallet_source_target = source_budget
            .checked_mul(existing_wallet_consensus)
            .ok_or(LiveShadowError::Arithmetic)?;
        let source_target = existing_wallet_source_target;
        let combined_target = source_target
            .checked_add(technical_target)
            .ok_or(LiveShadowError::Arithmetic)?;
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
    ) -> Result<(), LiveShadowError> {
        let Some(layer) = self.very_profitable_layer.clone() else {
            return Ok(());
        };
        let fresh_member_ids = fresh_states
            .iter()
            .map(|state| state.candidate_id.to_ascii_lowercase())
            .filter(|address| layer.qualified_members.contains(address))
            .collect::<BTreeSet<_>>();
        let complete_authoritative_state = fresh_member_ids == layer.qualified_members;
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
        let dominant_wallet_limit_pct = Decimal::from_f64(self.config.max_asset_notional_pct)
            .ok_or(LiveShadowError::Arithmetic)?;
        let rebalance_tolerance = Decimal::from_f64(self.config.global_risk.slot_rank_hysteresis)
            .ok_or(LiveShadowError::Arithmetic)?;
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
            let existing_consensus = existing_wallet_consensus
                .get(&asset)
                .copied()
                .unwrap_or_default();
            let technical_target = Decimal::ZERO;
            let aggregate = match aggregate_authoritative_positions(
                &asset,
                observed_at_ms,
                &layer.qualified_members,
                &wallets,
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
                .ok_or(LiveShadowError::Arithmetic)?;
            // Execution friction is evaluated once, from the live book, at
            // the MFCE admission seam below. The cohort layer remains a pure
            // source-candidate constructor in deferred mode.
            let estimated_round_trip_cost_fraction = Decimal::ZERO;
            let crowding_penalty = aggregate
                .dominant_wallet_percentage
                .saturating_sub(dominant_wallet_limit_pct)
                .checked_div(Decimal::from(100))
                .ok_or(LiveShadowError::Arithmetic)?
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
                    incomplete_authoritative_wallet_state: !complete_authoritative_state,
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
                        .ok_or(LiveShadowError::Arithmetic)?
                        .to_f64()
                        .ok_or(LiveShadowError::Arithmetic)?,
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

    pub fn construct_next_decision(
        &mut self,
        now: Timestamp,
    ) -> Result<Option<DecisionRecord>, LiveShadowError> {
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
        let mfce_now = self
            .mfce_time_offset
            .checked_add(now)
            .ok_or(LiveShadowError::Arithmetic)?;
        self.mfce_time_high_watermark = self.mfce_time_high_watermark.max(mfce_now);
        let mut eligibility = SourceEligibilitySummary::default();
        let mut members = Vec::new();
        let mut consensus_inputs: BTreeMap<String, Vec<ConsensusInput>> = BTreeMap::new();
        let mut source_contributions: BTreeMap<String, BTreeMap<String, Decimal>> = BTreeMap::new();
        let mut existing_wallet_consensus = BTreeMap::new();
        let mut fresh_source_states = Vec::new();
        let mut source_received_at = BTreeMap::new();
        // MFCE raw lifecycles are durable, while source snapshots are
        // intentionally ephemeral. After restart, do not manufacture a flat
        // transition/label from missing pre-hydration sources. Normal target
        // construction may still fail-safe toward flat until every enabled
        // configured source has supplied an authoritative post-start state.
        let mfce_sources_hydrated = self
            .config
            .candidates
            .iter()
            .filter(|candidate| candidate.enabled)
            .all(|candidate| {
                self.source_exposure_book
                    .get(&candidate.address.to_ascii_lowercase())
                    .is_some()
            });
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
            let Some(normalized_exposures) = normalized_source_exposures(&snapshot.payload) else {
                eligibility.excluded.insert(id, ExclusionReason::Stale);
                continue;
            };
            if normalized_exposures != exposure_state.exposures {
                return Err(LiveShadowError::Core(format!(
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
                    .retain(|asset, _| self.source_market_coverage.contains(asset));
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
                if self.source_stream_mode && !self.source_market_coverage.contains(asset) {
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
                        .ok_or_else(|| LiveShadowError::Core("missing confidence".into()))?,
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
            .map_err(|error| LiveShadowError::Core(error.to_string()))?;
            let raw_contributions = source
                .contribution_by_candidate
                .into_iter()
                .map(|(candidate, contribution)| {
                    Decimal::from_f64(contribution)
                        .map(|value| (candidate, value))
                        .ok_or(LiveShadowError::Arithmetic)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let source_component_target =
                Decimal::from_f64(source.exposure).ok_or(LiveShadowError::Arithmetic)?;
            existing_wallet_consensus.insert(
                asset.clone(),
                Decimal::from_f64(source.exposure).ok_or(LiveShadowError::Arithmetic)?,
            );
            let scaled_contributions = absolute_directional_component_targets(
                Some(&raw_contributions),
                source_component_target,
            )?;
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
        include_held_assets_in_target_universe(&mut consensus_inputs, self.authoritative_assets());
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
        // Source transitions are the only candidate generator. MFCE observes
        // their raw lifecycle before follower admission so rejected
        // transitions still receive the same gross counterfactual label.
        let configured_taker_fee_bps =
            Decimal::from_f64(self.config.taker_fee_bps).ok_or(LiveShadowError::Arithmetic)?;
        let taker_fee_bps = metadata.live_taker_fee_bps.or_else(|| {
            // Unsigned historical replays predate the live fee field and keep
            // the hash-bound conservative configuration. Production exposure
            // increases fail pending unless the public userFees refresh is
            // present in the replayable metadata payload.
            self.production_identity
                .is_none()
                .then_some(configured_taker_fee_bps)
        });
        let maximum_slippage_bps = Decimal::from_f64(self.config.execution.max_slippage_bps)
            .ok_or(LiveShadowError::Arithmetic)?;
        let total_tail_budget = remaining_mfce_tail_budget(&self.config, deployment)?;
        let source_capacity = available_gross;
        let mut allocation_candidates = Vec::new();
        let mut target_contexts = BTreeMap::new();
        for (asset, inputs) in &mut consensus_inputs {
            if !mfce_sources_hydrated {
                inputs.clear();
                source_contributions
                    .entry(asset.clone())
                    .or_default()
                    .clear();
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
            .map_err(|error| LiveShadowError::Core(error.to_string()))?;
            let raw_desired_source_target = available_gross
                .checked_mul(
                    Decimal::from_f64(desired_source.exposure)
                        .ok_or(LiveShadowError::Arithmetic)?,
                )
                .ok_or(LiveShadowError::Arithmetic)?;
            let raw_source_exposure =
                Decimal::from_f64(desired_source.exposure).ok_or(LiveShadowError::Arithmetic)?;
            let mark = mids
                .mids
                .get(asset)
                .copied()
                .ok_or_else(|| LiveShadowError::InvalidMarket(asset.clone()))?;
            let mut current_source_targets = self
                .ledger
                .source_positions_for_asset(asset)
                .into_iter()
                .filter(|(component, _)| !component.starts_with("technical:"))
                .map(|(component, quantity)| {
                    quantity
                        .checked_mul(mark)
                        .map(|notional| (component, notional))
                        .ok_or(LiveShadowError::Arithmetic)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let current_source_target = if self.production_positions.is_some() {
                // Production fills live in the durable LiveTradingState, not
                // the shadow DualLedger. Position-aware MFCE admission must
                // therefore use the reconciled authoritative quantity.
                let authoritative = self
                    .authoritative_position(asset)
                    .checked_mul(mark)
                    .ok_or(LiveShadowError::Arithmetic)?;
                current_source_targets.clear();
                if !authoritative.is_zero() {
                    current_source_targets.insert("source:aggregate".into(), authoritative);
                }
                authoritative
            } else {
                current_source_targets
                    .values()
                    .try_fold(Decimal::ZERO, |sum, target| sum.checked_add(*target))
                    .ok_or(LiveShadowError::Arithmetic)?
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
                        LiveShadowError::Core(format!(
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
            let preliminary_features = if raw_source_exposure.is_zero() {
                MfceFeatureVector::new([Decimal::ZERO; MFCE_FEATURE_COUNT])
            } else {
                mfce_feature_vector(
                    raw_source_exposure,
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
            let transition = self
                .mfce
                .observe_raw_source(
                    asset,
                    self.mfce_authorized_assets
                        .get(asset)
                        .is_some_and(|valid_until| now <= *valid_until),
                    raw_source_exposure,
                    raw_desired_source_target,
                    current_source_target,
                    mark,
                    mfce_now,
                    preliminary_features,
                )
                .map_err(mfce_error)?;
            if transition.needs_live_book {
                let transition_id = transition.transition_id.ok_or_else(|| {
                    LiveShadowError::Core("pending MFCE transition has no identity".into())
                })?;
                let fresh_book = self.books.get(asset).filter(|(_, received_at)| {
                    now.saturating_sub(*received_at) <= MFCE_LIVE_BOOK_MAX_AGE_MS
                });
                if let (
                    Some((book, _)),
                    Some(direction),
                    Some(funding_rate_hourly),
                    Some(taker_fee_bps),
                ) = (
                    fresh_book,
                    MfceDirection::from_signed(raw_source_exposure),
                    funding_rate_hourly,
                    taker_fee_bps,
                ) {
                    let admission_target = self
                        .mfce
                        .pending_position_notional(asset, transition_id, raw_desired_source_target)
                        .map_err(mfce_error)?;
                    let holding_hours = self.mfce.expected_holding_hours(
                        asset,
                        direction,
                        Decimal::from(self.config.technical.expected_holding_hours),
                    );
                    if let Ok(live_context) = mfce_live_market_context(
                        asset,
                        book,
                        direction,
                        current_source_target,
                        admission_target,
                        taker_fee_bps,
                        maximum_slippage_bps,
                        funding_rate_hourly,
                        holding_hours,
                    ) {
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
                                admission_target,
                            )
                            .map_err(mfce_error)?;
                        match self.mfce.predict_pending(asset, transition_id) {
                            Ok(prediction) => {
                                let prior_tail_reservation = self
                                    .mfce
                                    .reserved_tail_loss_usd(asset, current_source_target)
                                    .map_err(mfce_error)?;
                                allocation_candidates.push(MfceCrossSectionalCandidate {
                                    asset: asset.clone(),
                                    transition_id,
                                    input: MfceAllocationInput {
                                        prediction,
                                        friction_bps: live_context.friction_bps,
                                        copytrade_conviction: raw_source_exposure
                                            .abs()
                                            .min(Decimal::ONE),
                                        current_position_notional: current_source_target,
                                        proposed_position_notional: admission_target,
                                        remaining_tail_loss_budget_usd: total_tail_budget,
                                    },
                                    prior_tail_loss_usd: prior_tail_reservation,
                                });
                            }
                            Err(MfceError::InvalidState(_)) => self
                                .mfce
                                .reject_pending_without_prediction(
                                    asset,
                                    transition_id,
                                    MfceRejectionReason::InsufficientPooledSupport,
                                )
                                .map_err(mfce_error)?,
                            Err(_) => self
                                .mfce
                                .reject_pending_without_prediction(
                                    asset,
                                    transition_id,
                                    MfceRejectionReason::InvalidPrediction,
                                )
                                .map_err(mfce_error)?,
                        }
                    }
                }
            }
        }
        let mut allocation_replacement_assets = BTreeSet::new();
        if mfce_sources_hydrated {
            allocation_replacement_assets = allocation_candidates
                .iter()
                .map(|candidate| candidate.asset.clone())
                .collect::<BTreeSet<_>>();
            let mut committed_source_gross = Decimal::ZERO;
            let mut existing_tail_reservations = Decimal::ZERO;
            for (asset, context) in &target_contexts {
                let replaces_outstanding = allocation_replacement_assets.contains(asset);
                let committed_target = if replaces_outstanding {
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
                    .ok_or(LiveShadowError::Arithmetic)?;
                existing_tail_reservations = existing_tail_reservations
                    .checked_add(
                        self.mfce
                            .reserved_tail_loss_usd(asset, projected_notional)
                            .map_err(mfce_error)?,
                    )
                    .ok_or(LiveShadowError::Arithmetic)?;
            }
            let available_increment_notional = source_capacity
                .checked_sub(committed_source_gross)
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO);
            let remaining_tail_budget = total_tail_budget
                .checked_sub(existing_tail_reservations)
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO);
            let decisions = allocate_cross_sectional(
                &allocation_candidates,
                available_increment_notional,
                remaining_tail_budget,
            )
            .map_err(mfce_error)?;
            for candidate in &allocation_candidates {
                let decision = decisions.get(&candidate.asset).ok_or_else(|| {
                    LiveShadowError::Core("cross-sectional MFCE decision missing".into())
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
                let effective_target = self.mfce.effective_target(
                    asset,
                    context.current_source_target,
                    context.raw_desired_source_target,
                );
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
                            .ok_or(LiveShadowError::Arithmetic)?
                            .clamp(-Decimal::ONE, Decimal::ONE)
                            .to_f64()
                            .ok_or(LiveShadowError::Arithmetic)?,
                        enabled: true,
                        quarantined: false,
                        snapshot_age_ms: 0,
                    });
                }
                let contributions = source_contributions.entry(asset.clone()).or_default();
                if effective_target.is_zero() {
                    contributions.clear();
                } else if self.mfce.uses_current_position_attribution(asset) {
                    *contributions = context.current_source_targets.clone();
                }
            }
            self.mfce.finish_source_observation_cycle();
            self.mfce_authorized_assets.clear();
        }
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
        let projection_filled_positions = filled_positions.clone();
        let market_bytes =
            serde_json::to_vec(&mids).map_err(|error| LiveShadowError::Core(error.to_string()))?;
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
            acknowledged_open_orders: self
                .pending_open_orders_excluding(&allocation_replacement_assets),
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
        let proposed_action_notionals = actions
            .iter()
            .map(|action| (action.asset.clone(), action.rounded_notional))
            .collect::<BTreeMap<_, _>>();
        let executable_actions = actions
            .into_iter()
            .filter(|action| !self.production_exposure_blocked || action.reduce_only)
            .filter(|action| {
                action.reduce_only
                    || decision.micro_slots.get(&action.asset).is_some_and(|slot| {
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
        for asset in &allocation_replacement_assets {
            let rules = execution_rules
                .get(asset)
                .ok_or_else(|| LiveShadowError::InvalidMarket(asset.clone()))?;
            let tolerance = rules
                .mark_price
                .checked_mul(rules.size_step)
                .ok_or(LiveShadowError::Arithmetic)?;
            let should_retain = self.pending.get(asset).is_some_and(|previous| {
                executable_by_asset.get(asset).is_some_and(|replacement| {
                    pending_action_is_immaterial_replacement(
                        &previous.action,
                        replacement,
                        tolerance,
                    )
                })
            });
            if should_retain {
                continue;
            }
            if let Some(previous) = self.pending.remove(asset) {
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
                    outcome: ActionAttemptOutcome::SupersededByNewTarget,
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
                let component_targets = absolute_directional_component_targets(
                    source_contributions.get(&asset),
                    target,
                )?;
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
                    .ok_or(LiveShadowError::Arithmetic)?;
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
                let remaining_order_quantity = round_exchange_step(
                    action
                        .rounded_notional
                        .checked_div(rules.mark_price)
                        .ok_or(LiveShadowError::Arithmetic)?,
                    rules.size_step,
                    false,
                )?;
                if remaining_order_quantity <= Decimal::ZERO {
                    return Err(LiveShadowError::InvalidMarket(format!(
                        "{asset} pending action rounded to zero quantity"
                    )));
                }
                let signed_order_quantity = match action.side {
                    Side::Buy => remaining_order_quantity,
                    Side::Sell => -remaining_order_quantity,
                };
                let portfolio_position_after = self
                    .ledger
                    .portfolio_position(&asset)
                    .checked_add(signed_order_quantity)
                    .ok_or(LiveShadowError::Arithmetic)?;
                let component_allocation = partition_component_fill(
                    &self.ledger.source_positions_for_asset(&asset),
                    action.side,
                    remaining_order_quantity,
                    &component_targets,
                    rules.mark_price,
                    portfolio_position_after,
                    rules.size_step,
                )?;
                let component_remaining = component_allocation
                    .total_quantities
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
                self.pending.insert(
                    asset,
                    PendingAction {
                        action,
                        root_planned_cloid,
                        remaining_order_quantity,
                        component_remaining,
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
        self.refresh_desired_books();
        Ok(Some(decision))
    }

    fn try_execute_pending(
        &mut self,
        book: &OrderBookResponse,
        received_at: Timestamp,
    ) -> Result<ExecutionRecompute, LiveShadowError> {
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
        let evaluation = shadow_snapshot(book, received_at)?;
        let decision_midpoint = context.decision_book.midpoint;
        let reference_price = evaluation.midpoint;
        let cushion = Decimal::from_f64(self.config.slippage_buffer_bps / 10_000.0)
            .ok_or(LiveShadowError::Arithmetic)?;
        let maximum_slippage = Decimal::from_f64(self.config.execution.max_slippage_bps / 10_000.0)
            .ok_or(LiveShadowError::Arithmetic)?;
        let market_rules = MarketRules {
            mark_price: reference_price,
            price_tick: context.price_tick,
            size_step: context.size_step,
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
                    return Err(LiveShadowError::Core(format!(
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
                context.price_tick,
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
            self.retire_pending_action(
                &book.asset,
                &pending,
                received_at,
                ActionAttemptOutcome::BelowExchangeMinimumAfterCurrentRecompute,
            );
            return Ok(ExecutionRecompute::None);
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
                let mut projection_input = context.projection_input.clone();
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
                    .and_then(|metadata| execution_asset_index(metadata, &book.asset))
                    .ok_or_else(|| LiveShadowError::InvalidMarket(book.asset.clone()))?;
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
                        .ok_or(LiveShadowError::Arithmetic)?,
                    authorization: PreSigningContext {
                        now_mono: received_at,
                        deployed_risk_policy_hash: context.risk_policy_hash,
                        projection_input,
                        exchange_minimum_notional: required_notional,
                    },
                    canonical_hash: [0; 32],
                }
                .seal()
                .map_err(LiveShadowError::Core)?;
                self.prepared_authorized_intents.push(intent);
            }
            return Ok(ExecutionRecompute::None);
        }
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
        let execution = execute_shadow_ioc(&ShadowExecutionInput {
            action: pending.action,
            decision_timestamp_mono: context.decision_book.observed_at_mono,
            decision_market_snapshot: context.decision_book.clone(),
            evaluation_market_snapshot: evaluation,
            latency_scenario: LatencyScenario::Expected,
            configured_latency_ms: latency,
            proposed_limit_price: price_plan.limit_price,
            rounded_quantity: quantity,
            taker_fee_rate: Decimal::from_f64(self.config.taker_fee_bps / 10_000.0)
                .ok_or(LiveShadowError::Arithmetic)?,
            funding_attribution,
            position_before,
        })
        .map_err(core)?;
        if !execution.modeled_filled_quantity.is_zero() {
            self.accrued_funding.remove(&book.asset);
        }
        let ledger_timestamp = self
            .ledger_time_offset
            .checked_add(received_at)
            .ok_or(LiveShadowError::Arithmetic)?;
        self.ledger_time_high_watermark = self.ledger_time_high_watermark.max(ledger_timestamp);
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
        self.apply_attributed_execution(&book.asset, &execution, ledger_timestamp, allocations)?;
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
            parent_planned_cloid: context.parent_planned_cloid.clone(),
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
            component_close_quantities: component_partition.close_quantities,
            component_open_quantities: component_partition.open_quantities,
            source_filled_quantities: component_partition.total_quantities,
            economic_attribution: context.economic_attribution,
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
                .ok_or(LiveShadowError::Arithmetic)?;
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
        self.metrics.shadow_executions += 1;
        self.refresh_desired_books();
        Ok(execution_recompute)
    }

    fn apply_attributed_execution(
        &mut self,
        asset: &str,
        execution: &copytrade_core::shadow::ShadowExecution,
        ledger_timestamp: Timestamp,
        allocations: &BTreeMap<String, Decimal>,
    ) -> Result<(), LiveShadowError> {
        let closed_portfolio_episode = self
            .ledger
            .apply_portfolio_execution(execution, ledger_timestamp)
            .map_err(core)?
            .cloned();
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
                .ok_or(LiveShadowError::Arithmetic)?;
            let mut attributed_fees = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_fees)
                })
                .ok_or(LiveShadowError::Arithmetic)?;
            let mut attributed_funding = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_funding)
                })
                .ok_or(LiveShadowError::Arithmetic)?;
            let mut attributed_slippage = closed_attributions
                .iter()
                .try_fold(Decimal::ZERO, |sum, episode| {
                    sum.checked_add(episode.modeled_slippage)
                })
                .ok_or(LiveShadowError::Arithmetic)?;
            let maximum_rounding_residual = Decimal::new(
                i64::try_from(closed_attributions.len()).unwrap_or(i64::MAX),
                28,
            );
            let gross_residual = portfolio_episode
                .realized_pnl
                .checked_sub(attributed_gross)
                .ok_or(LiveShadowError::Arithmetic)?;
            let fees_residual = portfolio_episode
                .fees
                .checked_sub(attributed_fees)
                .ok_or(LiveShadowError::Arithmetic)?;
            let funding_residual = portfolio_episode
                .funding
                .checked_sub(attributed_funding)
                .ok_or(LiveShadowError::Arithmetic)?;
            let slippage_residual = portfolio_episode
                .slippage
                .checked_sub(attributed_slippage)
                .ok_or(LiveShadowError::Arithmetic)?;
            for (scope, residual) in [
                ("gross", gross_residual),
                ("fees", fees_residual),
                ("funding", funding_residual),
                ("slippage", slippage_residual),
            ] {
                if residual.abs() > maximum_rounding_residual {
                    return Err(LiveShadowError::Core(format!(
                        "accounting invariant failure for portfolio episode {:?}: unassignable {scope} residual {residual}",
                        portfolio_episode.episode_id
                    )));
                }
            }
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
                        LiveShadowError::Core(
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
                    return Err(LiveShadowError::Core(format!(
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
                .ok_or(LiveShadowError::Arithmetic)?;
            if attributed_net != portfolio_episode.net_pnl {
                let residual = portfolio_episode
                    .net_pnl
                    .checked_sub(attributed_net)
                    .ok_or(LiveShadowError::Arithmetic)?;
                if residual.abs() > maximum_rounding_residual {
                    return Err(LiveShadowError::Core(format!(
                        "accounting invariant failure for portfolio episode {:?}: portfolio net {} != attributed net {} (unassignable residual {})",
                        portfolio_episode.episode_id,
                        portfolio_episode.net_pnl,
                        attributed_net,
                        residual
                    )));
                }
                let (anchor_index, anchor) = closed_attributions
                    .iter()
                    .enumerate()
                    .max_by(|(_, left), (_, right)| left.candidate_id.cmp(&right.candidate_id))
                    .ok_or_else(|| {
                        LiveShadowError::Core(
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
                    .ok_or(LiveShadowError::Arithmetic)?;
                if attributed_net != portfolio_episode.net_pnl {
                    return Err(LiveShadowError::Core(format!(
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

    pub fn persist_unsigned_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
        identity: &UnsignedShadowStateIdentity,
    ) -> Result<u64, LiveShadowError> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| LiveShadowError::Core("state path has no parent".into()))?;
        std::fs::create_dir_all(parent).map_err(|error| {
            self.metrics.persistence_failures += 1;
            LiveShadowError::Core(error.to_string())
        })?;
        let payload = UnsignedObserverState {
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
            last_mids: self.mids.as_ref().map(|(mids, _)| mids.clone()),
            mfce: self.mfce.state().clone(),
            mfce_time_high_watermark: self.mfce_time_high_watermark,
            ledger_time_high_watermark: self.ledger_time_high_watermark,
            executions: self.executions.clone(),
            equity_buckets: self.equity_buckets.clone(),
            last_bucket: self.last_bucket.clone(),
            micro_density: self.micro_density.clone(),
        };
        let generation = match self.snapshot_generation {
            Some(previous) => previous.checked_add(1).ok_or(LiveShadowError::Arithmetic)?,
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
            LiveShadowError::Core(error.to_string())
        })?;
        self.snapshot_generation = Some(generation);
        Ok(generation)
    }

    pub fn restore_unsigned_state(
        &mut self,
        path: impl AsRef<std::path::Path>,
        expected_identity: &UnsignedShadowStateIdentity,
    ) -> Result<u64, LiveShadowError> {
        let bytes = std::fs::read(path).map_err(core)?;
        let envelope = decode_unsigned_snapshot(&bytes, expected_identity)?;
        let state = envelope.payload;
        state.ledger.validate_integrity().map_err(core)?;
        state.target_ledger.validate_integrity().map_err(core)?;
        state.mfce.validate().map_err(mfce_error)?;
        for pending in state.pending.values() {
            if pending.remaining_order_quantity <= Decimal::ZERO {
                return Err(LiveShadowError::Core(
                    "pending action has invalid remaining order quantity".into(),
                ));
            }
            pending_component_total(pending.action.side, &pending.component_remaining)?;
        }
        if state.mfce_time_high_watermark < state.mfce.time_high_watermark() {
            return Err(LiveShadowError::Core(
                "unsigned shadow MFCE time high-watermark regressed".into(),
            ));
        }
        if state.technical_engine.configuration() != &self.config.technical {
            return Err(LiveShadowError::Core(
                "unsigned shadow technical configuration mismatch".into(),
            ));
        }
        let installed_layer_hash = self
            .very_profitable_layer
            .as_ref()
            .map(|layer| layer.artifact_sha256.as_str());
        if state.very_profitable_layer_artifact_sha256.as_deref() != installed_layer_hash {
            return Err(LiveShadowError::Core(
                "unsigned shadow cohort-layer identity mismatch".into(),
            ));
        }
        for asset in state.ledger.portfolio_assets() {
            if state
                .last_mids
                .as_ref()
                .and_then(|mids| mids.mids.get(&asset))
                .is_none()
            {
                return Err(LiveShadowError::Core(format!(
                    "unsigned shadow state missing mark for open asset {asset}"
                )));
            }
        }
        self.ledger = state.ledger;
        self.target_ledger = state.target_ledger;
        self.technical_engine = state.technical_engine;
        self.technical_engine.reset_funnel();
        self.technical_decision_state = state.technical_decision_state;
        self.very_profitable_engine = state.very_profitable_engine;
        self.decision_sequence = state.decision_sequence;
        self.pending = state.pending;
        self.continuations = state.continuations;
        self.accrued_funding = state.accrued_funding;
        self.mids = state.last_mids.map(|mids| (mids, 0));
        self.restore_mfce_persistent_state(state.mfce, state.mfce_time_high_watermark)?;
        self.ledger_time_offset = state
            .ledger_time_high_watermark
            .checked_add(1)
            .ok_or(LiveShadowError::Arithmetic)?;
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

    pub fn metrics(&self) -> &LiveShadowMetrics {
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
                lifecycle: "pending_action",
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

    /// Keep exact economic records for the rolling 30-day horizon and only a
    /// small recent execution/lifecycle tail. Repeated planning and indicator
    /// evaluations are working data, not durable production evidence.
    pub fn compact_runtime_history(&mut self, cutoff: Timestamp) -> Result<(), LiveShadowError> {
        self.ledger.compact_closed_before(cutoff).map_err(core)?;
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

    pub fn executions(&self) -> &[ShadowActionAccounting] {
        &self.executions
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
        self.metrics.unreconciled_shadow_intents =
            (self.pending.len() + self.pending_book.len() + self.continuations.len()) as u64;
    }

    pub fn record_equity_boundary(&mut self, now: Timestamp) -> Result<(), LiveShadowError> {
        self.accrue_funding(now)?;
        let (equity, source_equities) = self.accounting_equities()?;
        self.record_equity_values(now, equity, source_equities)
    }

    fn accounting_equities(&self) -> Result<(Decimal, BTreeMap<String, Decimal>), LiveShadowError> {
        self.accounting_equities_with_sources(&BTreeSet::new())
    }

    fn accounting_equities_with_sources(
        &self,
        additional_sources: &BTreeSet<String>,
    ) -> Result<(Decimal, BTreeMap<String, Decimal>), LiveShadowError> {
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
        let source_starting_equity = Decimal::from_f64(self.config.starting_equity_usd)
            .ok_or(LiveShadowError::Arithmetic)?;
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
                        .unwrap_or(source_starting_equity);
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

/// Hyperliquid signs default perps by their local metadata index. Builder
/// perps use the documented DEX offset; `xyz` is the first builder DEX and
/// therefore begins at 110000. Its local index is stable within its own
/// metadata response even though the observer stores one merged universe.
fn execution_asset_index(metadata: &MarketMetadataResponse, asset: &str) -> Option<u32> {
    if asset.starts_with("xyz:") {
        let local = metadata
            .universe
            .iter()
            .filter(|candidate| candidate.name.starts_with("xyz:"))
            .position(|candidate| candidate.name == asset)?;
        return u32::try_from(local)
            .ok()
            .and_then(|local| 110_000u32.checked_add(local));
    }
    if asset.contains(':') {
        return None;
    }
    metadata
        .universe
        .iter()
        .take_while(|candidate| !candidate.name.contains(':'))
        .position(|candidate| candidate.name == asset)
        .and_then(|index| u32::try_from(index).ok())
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

fn mfce_error(error: MfceError) -> LiveShadowError {
    LiveShadowError::Core(format!("MFCE: {error}"))
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
    use crate::cohort_layer::{
        PreparedVeryProfitableLayer, VeryProfitableLayerArtifact, COHORT_LAYER_SCHEMA_VERSION,
        HYPERLIQUID_INFO_AUTHORITY,
    };
    use crate::public_mainnet::{
        BookLevel, MarketAssetContext, MarketMetadataAsset, SourceAssetPosition,
    };
    use copytrade_core::cohort::{
        AttributionTag, HyperdashMembershipSnapshot, MembershipCompleteness,
        WalletQualityObservation, VERY_PROFITABLE_COHORT_ID, VERY_PROFITABLE_COHORT_URL,
    };
    use copytrade_core::decision::MarketSnapshotId;
    use copytrade_core::decision::{
        DecisionId, PayloadHash, PlannedAction, PlannedCloid, SnapshotSetId, TargetVersion,
    };
    use copytrade_core::live_trading::{
        ExchangeFillIdentity, ExchangeOrderId, ExchangeTradeId, LiquidityClassification,
    };
    use copytrade_core::scheduler::ReadRequestKind;
    use copytrade_core::shadow::ShadowExecutionId;
    use copytrade_core::technical::{CandleInterval, ClosedCandle};
    use serde::ser::SerializeMap;
    use serde::Deserialize;
    use std::path::Path;

    #[test]
    fn authenticated_fill_consumes_frozen_pending_component_quantities() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let mut engine =
            LiveShadowEngine::new(config, b"live-fill", "run", 40_000, 80_000).unwrap();
        engine.metadata = Some(MarketMetadataResponse {
            universe: vec![MarketMetadataAsset {
                name: "BTC".into(),
                size_decimals: 2,
            }],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
        });
        engine.mids = Some((
            MarketSnapshotResponse {
                mids: BTreeMap::from([("BTC".into(), Decimal::from(100))]),
            },
            1,
        ));
        let action = PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            asset: "BTC".into(),
            side: Side::Buy,
            rounded_notional: Decimal::from(100),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([2; 16]),
        };
        engine.pending.insert(
            "BTC".into(),
            PendingAction {
                action,
                root_planned_cloid: PlannedCloid([2; 16]).to_string(),
                remaining_order_quantity: Decimal::ONE,
                component_remaining: BTreeMap::from([
                    ("source:a".into(), Decimal::new(6, 1)),
                    ("source:b".into(), Decimal::new(4, 1)),
                ]),
                execution: None,
            },
        );
        let mut fill = VerifiedExchangeFill {
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
            asset: "BTC".into(),
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

        fill.identity.trade_id = ExchangeTradeId("trade-2".into());
        fill.filled_quantity = Decimal::new(75, 2);
        fill.fee_amount = Decimal::new(375, 4);
        fill.occurred_at = 11;
        fill.exchange_equity_after = Decimal::new(9995, 2);
        engine.apply_live_execution_fill(&fill).unwrap();
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
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine =
            LiveShadowEngine::new(config, b"stream-freshness", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
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
        engine.set_source_stream_healthy_for_test(true);
        assert!(engine.source_is_fresh(&candidate, 10_000_000));
        engine.set_source_stream_healthy_for_test(false);
        assert!(!engine.source_is_fresh(&candidate, 2));
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
            LiveShadowEngine::new(config, b"source-normalization", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine.set_source_tier(&valid, SourceTier::Inactive);
        engine.set_source_tier(&unusable, SourceTier::Inactive);

        let source = |candidate: String, account_value: Decimal, notional: Decimal, at| {
            SourceStateResponse {
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
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_002,
                ),
                1_002,
            )
            .unwrap();
        engine
            .complete_source_stream_reconciliation(BTreeSet::from(["BTC".into()]), 1_003)
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
            LiveShadowEngine::new(config, b"coverage-pending", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine.set_source_tier(&candidate, SourceTier::Inactive);
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        candidate_id: candidate,
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
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();

        engine
            .complete_source_stream_reconciliation(BTreeSet::new(), 1_001)
            .unwrap();
        assert_eq!(engine.source_market_coverage_count(), 0);
        assert_eq!(engine.mfce_report().observed_transitions, 0);
        assert_eq!(engine.mfce_report().decision_counts.reject, 0);

        engine
            .complete_source_market_reconciliation(BTreeSet::from(["BTC".into()]), 1_002)
            .unwrap();
        assert_eq!(engine.source_market_coverage_count(), 1);
        assert_eq!(engine.mfce_report().observed_transitions, 1);
    }

    #[test]
    fn stream_gap_invalidates_only_its_dependent_markets() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        let mut engine =
            LiveShadowEngine::new(config, b"local-stream-gap", "run", 40_000, 80_000).unwrap();
        engine.enable_source_stream_mode();
        engine.set_source_tier(&candidate, SourceTier::Inactive);
        engine
            .ingest(
                accepted(
                    PublicPayload::SourceState(SourceStateResponse {
                        candidate_id: candidate,
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
                            },
                            MarketMetadataAsset {
                                name: "ETH".into(),
                                size_decimals: 3,
                            },
                        ],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    1_000,
                ),
                1_000,
            )
            .unwrap();
        engine
            .complete_source_stream_reconciliation(
                BTreeSet::from(["BTC".into(), "ETH".into()]),
                1_001,
            )
            .unwrap();

        engine
            .mark_source_stream_gap(&BTreeSet::from(["BTC".into()]), 1_002)
            .unwrap();
        assert!(engine.source_stream_healthy());
        assert_eq!(engine.source_market_coverage_count(), 1);
        assert_eq!(engine.mfce.previous_source_exposure("BTC"), Decimal::ZERO);
        assert_eq!(
            engine.mfce.previous_source_exposure("ETH"),
            Decimal::new(2, 1)
        );

        engine
            .mark_source_stream_gap(&BTreeSet::from(["ETH".into()]), 1_003)
            .unwrap();
        assert!(!engine.source_stream_healthy());
        assert_eq!(engine.source_market_coverage_count(), 0);
    }

    #[test]
    fn execution_indices_cover_default_and_xyz_perpetuals() {
        let metadata = MarketMetadataResponse {
            universe: vec![
                MarketMetadataAsset {
                    name: "BTC".into(),
                    size_decimals: 5,
                },
                MarketMetadataAsset {
                    name: "ETH".into(),
                    size_decimals: 4,
                },
                MarketMetadataAsset {
                    name: "xyz:SP500".into(),
                    size_decimals: 2,
                },
                MarketMetadataAsset {
                    name: "xyz:GOLD".into(),
                    size_decimals: 3,
                },
            ],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
        };
        assert_eq!(execution_asset_index(&metadata, "BTC"), Some(0));
        assert_eq!(execution_asset_index(&metadata, "ETH"), Some(1));
        assert_eq!(execution_asset_index(&metadata, "xyz:SP500"), Some(110_000));
        assert_eq!(execution_asset_index(&metadata, "xyz:GOLD"), Some(110_001));
        assert_eq!(execution_asset_index(&metadata, "other:UNKNOWN"), None);
    }

    fn seed_positive_mfce_backoff(engine: &mut LiveShadowEngine, asset: &str) {
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
                gross_return_bps: Decimal::from(100),
                lifetime_seconds: Decimal::new(5, 1),
                admitted: false,
            });
        }
        state.next_sample_id = 9;
        engine.mfce.replace_state(state).unwrap();
        engine.mfce_time_high_watermark = engine.mfce.state().time_high_watermark();
        engine.mfce_time_offset = engine.mfce_time_high_watermark + 1;
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

    fn warm_flat_technical_context(engine: &mut LiveShadowEngine, asset: &str) {
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
            LiveShadowEngine::new(config, b"mfce-restore", "run", 40_000, 80_000).unwrap();
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
                        }],
                        contexts: BTreeMap::from([(
                            "BTC".into(),
                            MarketAssetContext {
                                funding_rate_hourly: Decimal::ZERO,
                            },
                        )]),
                        live_taker_fee_bps: Some(Decimal::from(5)),
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
            LiveShadowEngine::new(config, b"technical-feature-only", "run", 40_000, 80_000)
                .unwrap();
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
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
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
            LiveShadowEngine::new(config, b"mfce-stale-book", "run", 40_000, 80_000).unwrap();
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
                        }],
                        contexts: BTreeMap::from([(
                            "BTC".into(),
                            MarketAssetContext {
                                funding_rate_hourly: Decimal::ZERO,
                            },
                        )]),
                        live_taker_fee_bps: Some(Decimal::from(5)),
                    }),
                    ReadRequestKind::ExchangeMetadata,
                    100_000,
                ),
                100_000,
            )
            .unwrap();

        engine.construct_next_decision(100_001).unwrap();

        assert!(engine
            .mfce
            .awaiting_book_assets()
            .any(|asset| asset == "BTC"));
        assert!(engine.desired_books.contains("BTC"));
        assert!(!engine.pending.contains_key("BTC"));

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
            crate::mfce::MfceAdmissionState::Admitted { .. }
        ));
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
            LiveShadowEngine::new(config, b"cohort-integration", "run", 40_000, 80_000).unwrap();
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
            }],
            contexts: BTreeMap::from([(
                "BTC".into(),
                MarketAssetContext {
                    funding_rate_hourly: Decimal::ZERO,
                },
            )]),
            live_taker_fee_bps: Some(Decimal::from(5)),
        };
        let source_state = |address: String, at: u64, notional: Decimal| SourceStateResponse {
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
                    source_state(cohort_only_two.clone(), 1_000, Decimal::from(100)),
                ],
                &BTreeMap::from([
                    (existing.clone(), 1_000),
                    (cohort_only.clone(), 1_000),
                    (cohort_only_two.clone(), 1_000),
                ]),
                &BTreeMap::from([("BTC".into(), Decimal::ONE)]),
                &mut first_inputs,
                &mut first_contributions,
            )
            .unwrap();
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
                    }],
                    contexts: BTreeMap::new(),
                    live_taker_fee_bps: Some(Decimal::from(5)),
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
                    source_state(existing.clone(), 301_000, Decimal::from(100)),
                    source_state(cohort_only.clone(), 301_000, Decimal::from(200)),
                    source_state(cohort_only_two.clone(), 301_000, Decimal::from(200)),
                ],
                &BTreeMap::from([
                    (existing, 301_000),
                    (cohort_only, 301_000),
                    (cohort_only_two, 301_000),
                ]),
                &BTreeMap::from([("BTC".into(), Decimal::ONE)]),
                &mut second_inputs,
                &mut second_contributions,
            )
            .unwrap();
        let record = engine.cohort_indicator_records.last().unwrap();
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
        assert_eq!(record.long_trader_count, 3);
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
            LiveShadowEngine::new(config, b"technical-equity", "run", 40_000, 80_000).unwrap();
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
        for (candidate, position) in &current {
            assert_eq!(allocated[candidate], position.abs());
        }
        assert_ne!(
            allocated.values().copied().sum::<Decimal>(),
            Decimal::from(29)
        );
        let pending = allocated
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
        assert_eq!(consumed.total_quantities, allocated);
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

    #[derive(Deserialize)]
    struct RailwayAccountingFixture {
        source_bundle_id: String,
        source_journal_sha256: String,
        asset: String,
        opened_at_mono: u64,
        closed_at_mono: u64,
        filled_quantity: Decimal,
        portfolio_net_pnl: Decimal,
        closed_source_net_pnl: Vec<Decimal>,
        stranded_source_net_pnl: Decimal,
        source_positions_before_close: BTreeMap<String, Decimal>,
        failed_close_allocation_for_stranded_source: Decimal,
    }

    #[derive(Deserialize)]
    struct R4AaveCrossingFixture {
        source_bundle_id: String,
        source_journal_sha256: String,
        failing_event_sequence: u64,
        asset: String,
        portfolio_quantity_before: Decimal,
        source_component_quantities_before: BTreeMap<String, Decimal>,
        desired_source_target_notional: Decimal,
        desired_technical_target_notional: Decimal,
        reference_price: Decimal,
        modeled_fill_quantity: Decimal,
        modeled_fill_price: Decimal,
        portfolio_quantity_after: Decimal,
        expected_component_close_quantity: Decimal,
        expected_component_open_quantity: Decimal,
        portfolio_realized_net: Decimal,
        legacy_closed_component_net: Decimal,
        legacy_unassignable_residual: Decimal,
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
    ) -> ShadowExecution {
        let signed = match side {
            Side::Buy => quantity,
            Side::Sell => -quantity,
        };
        ShadowExecution {
            shadow_execution_id: ShadowExecutionId([31; 32]),
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

    fn sum_map(values: &BTreeMap<String, Decimal>) -> Decimal {
        values.values().copied().sum()
    }

    #[test]
    fn r4_aave_crossing_fixture_reproduces_legacy_escape_and_partitions_exactly() {
        let fixture: R4AaveCrossingFixture = serde_json::from_str(include_str!(
            "../../../fixtures/su6r1-r4-aave-crossing-accounting-failure.json"
        ))
        .unwrap();
        assert_eq!(fixture.source_bundle_id, "1785730528506-314-5f64d2389e3f");
        assert_eq!(
            fixture.source_journal_sha256,
            "f6bbf39913d49a588a86fb52813023c15f4034e90eb08f001eab58b7c48d4fb6"
        );
        assert_eq!(fixture.failing_event_sequence, 3_178);
        assert_eq!(fixture.asset, "AAVE");
        assert_eq!(fixture.desired_technical_target_notional, Decimal::ZERO);
        assert_eq!(fixture.legacy_closed_component_net, Decimal::ZERO);
        assert_eq!(
            fixture.legacy_unassignable_residual,
            fixture.portfolio_realized_net
        );

        let opening_component = "source:r4-new-short".to_string();
        let mut legacy_weights = fixture
            .source_component_quantities_before
            .iter()
            .map(|(component, position)| (component.clone(), position.abs()))
            .collect::<BTreeMap<_, _>>();
        legacy_weights.insert(
            opening_component.clone(),
            fixture
                .desired_source_target_notional
                .checked_div(fixture.reference_price)
                .unwrap()
                .abs(),
        );
        let legacy =
            proportional_allocations(fixture.modeled_fill_quantity, &legacy_weights).unwrap();
        assert!(fixture
            .source_component_quantities_before
            .iter()
            .all(|(component, position)| { legacy[component] < position.abs() }));

        let targets = BTreeMap::from([(
            opening_component.clone(),
            fixture.desired_source_target_notional,
        )]);
        let partition = partition_component_fill(
            &fixture.source_component_quantities_before,
            Side::Sell,
            fixture.modeled_fill_quantity,
            &targets,
            fixture.reference_price,
            fixture.portfolio_quantity_after,
            Decimal::new(1, 2),
        )
        .unwrap();
        assert_eq!(
            partition.close_quantities,
            fixture.source_component_quantities_before
        );
        assert_eq!(
            sum_map(&partition.close_quantities),
            fixture.expected_component_close_quantity
        );
        assert_eq!(
            sum_map(&partition.open_quantities),
            fixture.expected_component_open_quantity
        );
        assert_eq!(
            partition.open_quantities[&opening_component],
            fixture.expected_component_open_quantity
        );
        assert_eq!(
            sum_map(&partition.total_quantities),
            fixture.modeled_fill_quantity
        );
        assert_eq!(
            fixture.portfolio_quantity_before - fixture.modeled_fill_quantity,
            fixture.portfolio_quantity_after
        );
    }

    #[test]
    fn r4_aave_crossing_closes_old_component_episodes_and_reconciles_economics() {
        let fixture: R4AaveCrossingFixture = serde_json::from_str(include_str!(
            "../../../fixtures/su6r1-r4-aave-crossing-accounting-failure.json"
        ))
        .unwrap();
        let opening = crossing_test_execution(
            &fixture.asset,
            Side::Buy,
            fixture.portfolio_quantity_before,
            Decimal::from_str_exact("91.676").unwrap(),
            Decimal::ZERO,
            Decimal::from_str_exact("0.0132013440").unwrap(),
            Decimal::ZERO,
            Decimal::from_str_exact("0.00096").unwrap(),
        );
        let crossing = crossing_test_execution(
            &fixture.asset,
            Side::Sell,
            fixture.modeled_fill_quantity,
            fixture.modeled_fill_price,
            fixture.portfolio_quantity_before,
            Decimal::from_str_exact("0.0152715870").unwrap(),
            Decimal::from_str_exact("0.000019").unwrap(),
            Decimal::from_str_exact("0.00111").unwrap(),
        );
        let opening_component = "source:r4-new-short".to_string();
        let targets = BTreeMap::from([(
            opening_component.clone(),
            fixture.desired_source_target_notional,
        )]);
        let partition = partition_component_fill(
            &fixture.source_component_quantities_before,
            Side::Sell,
            fixture.modeled_fill_quantity,
            &targets,
            fixture.reference_price,
            fixture.portfolio_quantity_after,
            Decimal::new(1, 2),
        )
        .unwrap();

        let mut ledger = DualLedger::default();
        ledger.apply_portfolio_execution(&opening, 1).unwrap();
        for (component, quantity) in &fixture.source_component_quantities_before {
            let component_open =
                scaled_source_execution(&opening, *quantity, Decimal::ZERO).unwrap();
            assert!(ledger
                .apply_source_execution(component, &component_open, 1)
                .unwrap()
                .is_none());
        }
        let portfolio_closed = ledger
            .apply_portfolio_execution(&crossing, 2)
            .unwrap()
            .unwrap()
            .clone();
        let mut component_closed = Vec::new();
        for (component, quantity) in &partition.total_quantities {
            let component_fill = scaled_source_execution(
                &crossing,
                *quantity,
                ledger.source_position(component, &fixture.asset),
            )
            .unwrap();
            if let Some(closed) = ledger
                .apply_source_execution(component, &component_fill, 2)
                .unwrap()
            {
                component_closed.push(closed);
            }
        }
        assert_eq!(
            component_closed.len(),
            fixture.source_component_quantities_before.len()
        );
        assert_eq!(
            ledger.source_position(&opening_component, &fixture.asset),
            -fixture.expected_component_open_quantity
        );
        assert_eq!(
            sum_map(&ledger.source_positions_for_asset(&fixture.asset)),
            fixture.portfolio_quantity_after
        );

        let gross: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_gross_pnl)
            .sum();
        let fees: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_fees)
            .sum();
        let funding: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_funding)
            .sum();
        let slippage: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_slippage)
            .sum();
        let gross_residual = portfolio_closed.realized_pnl - gross;
        let fees_residual = portfolio_closed.fees - fees;
        let funding_residual = portfolio_closed.funding - funding;
        let slippage_residual = portfolio_closed.slippage - slippage;
        let quantum = Decimal::new(component_closed.len() as i64, 28);
        assert!([
            gross_residual,
            fees_residual,
            funding_residual,
            slippage_residual
        ]
        .iter()
        .all(|residual| residual.abs() <= quantum));
        let anchor_index = component_closed
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.candidate_id.cmp(&right.candidate_id))
            .unwrap()
            .0;
        let anchor_candidate = component_closed[anchor_index].candidate_id.clone();
        let anchor_episode = component_closed[anchor_index].source_episode_id;
        component_closed[anchor_index] = ledger
            .assign_source_episode_economic_residuals(
                &anchor_candidate,
                anchor_episode,
                gross_residual,
                fees_residual,
                funding_residual,
                slippage_residual,
            )
            .unwrap();
        let corrected_gross: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_gross_pnl)
            .sum();
        let corrected_fees: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_fees)
            .sum();
        let corrected_funding: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_funding)
            .sum();
        let corrected_slippage: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_slippage)
            .sum();
        let net: Decimal = component_closed
            .iter()
            .map(|episode| episode.modeled_net_pnl)
            .sum();
        assert_eq!(corrected_gross, portfolio_closed.realized_pnl);
        assert_eq!(corrected_fees, portfolio_closed.fees);
        assert_eq!(corrected_funding, portfolio_closed.funding);
        assert_eq!(corrected_slippage, portfolio_closed.slippage);
        assert_eq!(net, portfolio_closed.net_pnl);
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
    fn first_real_railway_accounting_divergence_reproduces_and_reconciles_exactly() {
        let fixture: RailwayAccountingFixture = serde_json::from_str(include_str!(
            "../../../fixtures/railway-window1-first-accounting-divergence.json"
        ))
        .unwrap();
        assert_eq!(fixture.source_bundle_id, "1785202130363-303-91ab220e921d");
        assert_eq!(
            fixture.source_journal_sha256,
            "4899f5a9eda495ec78df37d375aa59a20e4570152f82a3344f5a329b6439f35c"
        );
        assert_eq!(fixture.asset, "AAVE");
        assert_eq!(
            (fixture.opened_at_mono, fixture.closed_at_mono),
            (2079, 12073)
        );

        let failed_source_net = fixture
            .closed_source_net_pnl
            .iter()
            .copied()
            .sum::<Decimal>();
        assert_ne!(failed_source_net, fixture.portfolio_net_pnl);
        assert_eq!(
            failed_source_net + fixture.stranded_source_net_pnl,
            fixture.portfolio_net_pnl
        );

        let closes = fixture
            .source_positions_before_close
            .iter()
            .map(|(candidate, position)| (candidate.clone(), position.abs()))
            .collect::<BTreeMap<_, _>>();
        let failed = proportional_allocations(fixture.filled_quantity, &closes).unwrap();
        assert_eq!(
            failed["0xa20fb0c9e04063eec5be286e9269028d966646fa"],
            fixture.failed_close_allocation_for_stranded_source
        );

        let corrected = allocate_component_fill(
            &fixture.source_positions_before_close,
            Side::Buy,
            fixture.filled_quantity,
            &BTreeMap::new(),
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::new(1, 2),
        )
        .unwrap();
        for (candidate, position) in &fixture.source_positions_before_close {
            assert_eq!(corrected[candidate], position.abs());
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
    fn target_replacement_does_not_rewrite_live_action_attribution() {
        let asset = "TEST";
        let component = "source:issuance";
        let issued_notional = Decimal::new(113, 1);
        let reference_price = Decimal::new(1, 1);
        let issued_quantity = issued_notional.checked_div(reference_price).unwrap();
        let (mut engine, book) = pending_reduce_only_engine(
            asset,
            Decimal::from(100),
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

    #[derive(Deserialize)]
    struct R7ResolvStaleReduceOnlyFixture {
        source_bundle_id: String,
        source_archive_sha256: String,
        source_journal_sha256: String,
        failure_event_sequence: u64,
        last_valid_snapshot_generation: u64,
        asset: String,
        opening_quantity: Decimal,
        opening_average_fill_price: Decimal,
        current_reference_price: Decimal,
        current_combined_target_fraction: Decimal,
        pending_target_version: u64,
        current_target_version: u64,
    }

    #[derive(Deserialize)]
    struct R7BabySameVersionPriceDriftFixture {
        source_bundle_id: String,
        source_archive_sha256: String,
        source_journal_gzip_sha256: String,
        fatal_evidence_sha256: String,
        last_valid_snapshot_generation: u64,
        asset: String,
        committed_quantity: Decimal,
        opening_average_fill_price: Decimal,
        current_reference_price: Decimal,
        current_destination_notional: Decimal,
        pending_target_version: u64,
        current_target_version: u64,
    }

    fn pending_reduce_only_engine(
        asset: &str,
        opening_quantity: Decimal,
        opening_price: Decimal,
        current_reference_price: Decimal,
    ) -> (LiveShadowEngine, OrderBookResponse) {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        let mut config = CopyTradeConfig::from_path(path).unwrap();
        let candidate = config.candidates[0].address.to_ascii_lowercase();
        config.candidates.truncate(1);
        config.technical.enabled = false;
        let mut engine =
            LiveShadowEngine::new(config, b"stale-reduce-only", "run", 40_000, 80_000).unwrap();
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
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
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
        engine: &mut LiveShadowEngine,
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
    fn r7_resolv_stale_reduce_only_is_canceled_without_fatal_or_unresolved_root() {
        let fixture: R7ResolvStaleReduceOnlyFixture = serde_json::from_str(include_str!(
            "../../../fixtures/su6r1-r7-resolv-stale-reduce-only.json"
        ))
        .unwrap();
        assert_eq!(fixture.source_bundle_id, "1786293985564-314-1b7682138393");
        assert_eq!(fixture.source_archive_sha256.len(), 64);
        assert_eq!(fixture.source_journal_sha256.len(), 64);
        assert_eq!(fixture.failure_event_sequence, 38_149);
        assert_eq!(fixture.last_valid_snapshot_generation, 238);

        let (mut engine, book) = pending_reduce_only_engine(
            &fixture.asset,
            fixture.opening_quantity,
            fixture.opening_average_fill_price,
            fixture.current_reference_price,
        );
        let original_position = engine.authoritative_position(&fixture.asset);
        let pending_version = engine.pending[&fixture.asset].action.target_version;
        assert_eq!(
            fixture.pending_target_version + 1,
            fixture.current_target_version
        );
        let current_destination = fixture.current_combined_target_fraction * Decimal::from(100);
        let current_committed = original_position * fixture.current_reference_price;
        assert!(current_destination > current_committed);
        replace_current_target(
            &mut engine,
            &fixture.asset,
            current_destination,
            TargetVersion(pending_version.0 + 1),
            fixture.current_reference_price,
        );

        let result = engine.try_execute_pending(&book, 10_000).unwrap();
        assert_eq!(result, ExecutionRecompute::None);
        assert!(!engine.pending.contains_key(&fixture.asset));
        assert_eq!(
            engine.authoritative_position(&fixture.asset),
            original_position
        );
        assert!(engine.executions.is_empty());
        assert_eq!(
            engine.action_lifecycle_events.last().unwrap().outcome,
            ActionAttemptOutcome::SupersededByNewTarget
        );
        let density = engine.executable_density_summary().unwrap();
        assert_eq!(density.unresolved_actionable_targets, 0);
        assert!(density.root_conservation_verified);
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
    fn r7_baby_same_version_price_drift_cancels_reduce_only_for_normal_replan() {
        let fixture: R7BabySameVersionPriceDriftFixture = serde_json::from_str(include_str!(
            "../../../fixtures/su6r1-r7-baby-same-version-price-drift.json"
        ))
        .unwrap();
        assert_eq!(fixture.source_bundle_id, "1786299897034-314-8e72025fcc2c");
        assert_eq!(fixture.source_archive_sha256.len(), 64);
        assert_eq!(fixture.source_journal_gzip_sha256.len(), 64);
        assert_eq!(fixture.fatal_evidence_sha256.len(), 64);
        assert_eq!(fixture.last_valid_snapshot_generation, 1_048);
        assert_eq!(
            fixture.pending_target_version,
            fixture.current_target_version
        );

        let (mut engine, book) = pending_reduce_only_engine(
            &fixture.asset,
            fixture.committed_quantity,
            fixture.opening_average_fill_price,
            fixture.current_reference_price,
        );
        let original_position = engine.authoritative_position(&fixture.asset);
        let pending_version = engine.pending[&fixture.asset].action.target_version;
        let current_notional = original_position * fixture.current_reference_price;
        assert!(fixture.current_destination_notional > current_notional);
        replace_current_target(
            &mut engine,
            &fixture.asset,
            fixture.current_destination_notional,
            pending_version,
            fixture.current_reference_price,
        );

        assert_eq!(
            engine.try_execute_pending(&book, 10_000).unwrap(),
            ExecutionRecompute::None
        );
        assert!(!engine.pending.contains_key(&fixture.asset));
        assert_eq!(
            engine.authoritative_position(&fixture.asset),
            original_position
        );
        assert!(engine.executions.is_empty());
        assert_eq!(
            engine.action_lifecycle_events.last().unwrap().outcome,
            ActionAttemptOutcome::SupersededByNewTarget
        );
        let density = engine.executable_density_summary().unwrap();
        assert_eq!(density.unresolved_actionable_targets, 0);
        assert!(density.root_conservation_verified);
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
            LiveShadowEngine::new(config, b"anime-neutralization", "run", 40_000, 80_000).unwrap();
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
                        }],
                        contexts: BTreeMap::new(),
                        live_taker_fee_bps: Some(Decimal::from(5)),
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
            LiveShadowEngine::new(config, b"partial-fixture", "run", 40_000, 80_000).unwrap();
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
            "copytrade-unsigned-shadow-state-{}-{}.msgpack",
            std::process::id(),
            engine.decision_sequence
        ));
        let identity = UnsignedShadowStateIdentity {
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
        let mut rejected = LiveShadowEngine::new(
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

        let mut restored = LiveShadowEngine::new(
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
        std::fs::remove_file(state_path).unwrap();

        assert_eq!(restored.ledger, expected_ledger);
        assert_eq!(restored.target_ledger, expected_targets);
        assert_eq!(restored.decision_sequence, expected_sequence);
        assert_eq!(restored.ledger.portfolio_open_count(), 1);
        assert!(restored.ledger_time_offset > engine.ledger_time_high_watermark);
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
        assert!(restored.mfce_time_offset > engine.mfce_time_high_watermark);
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
            LiveShadowEngine::new(config, b"snapshot-fixture", "snapshot-run", 40_000, 80_000)
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
            LiveShadowEngine::new(config.clone(), b"technical-state", "run", 40_000, 80_000)
                .unwrap();
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
            LiveShadowEngine::new(config, b"technical-state", "restored", 40_000, 80_000).unwrap();
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
