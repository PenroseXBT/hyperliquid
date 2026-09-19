//! Deterministic, signer-free HL1B portfolio projection.

use rust_decimal::prelude::{FromPrimitive, Signed};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

pub type Asset = String;
pub type SignedNotional = Decimal;

const MIN_LAUNCH_RISK_SCALE: Decimal = Decimal::from_parts(25, 0, 0, false, 3);
const MAX_LAUNCH_RISK_SCALE: Decimal = Decimal::from_parts(10, 0, 0, false, 2);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenOrderLifecycle {
    Acknowledged,
    CancelRequested,
    CancelConfirmed,
    /// Submission outcome is unknown, but side and maximum quantity are known.
    SubmissionUnknown,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenOrderExposure {
    pub asset: Asset,
    pub side: Option<OrderSide>,
    pub notional: Option<Decimal>,
    pub lifecycle: OpenOrderLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRules {
    pub mark_price: Decimal,
    pub price_tick: Decimal,
    pub size_step: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExposureRange {
    pub minimum: Decimal,
    pub maximum: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioProjectionInput {
    pub current_equity: Decimal,
    pub curve_leverage: Decimal,
    pub global_risk_scale: Decimal,
    pub max_single_asset_equity_pct: Decimal,
    pub max_net_equity_pct: Decimal,
    pub filled_positions: BTreeMap<Asset, SignedNotional>,
    pub filled_position_state_complete: bool,
    pub acknowledged_open_orders: Vec<OpenOrderExposure>,
    pub open_order_state_complete: bool,
    pub unconstrained_targets: BTreeMap<Asset, SignedNotional>,
    pub market_rules: BTreeMap<Asset, MarketRules>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gross_cap_override: Option<Decimal>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub held_manual_assets: BTreeSet<Asset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioProjection {
    pub constrained_targets: BTreeMap<Asset, SignedNotional>,
    pub proposed_deltas: BTreeMap<Asset, SignedNotional>,
    pub rounded_deltas: BTreeMap<Asset, SignedNotional>,
    pub projected_ranges: BTreeMap<Asset, ExposureRange>,
    pub maximum_projected_gross: Decimal,
    pub minimum_projected_net: Decimal,
    pub maximum_projected_net: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskViolation {
    InvalidInput(String),
    MissingMarketRules(Asset),
    IncompleteFilledPositionState,
    IncompleteOpenOrderState,
    UnknownOpenOrderState(Asset),
    ArithmeticOverflow(&'static str),
    AssetCapExceeded(Asset),
    GrossCapExceeded { projected: Decimal, cap: Decimal },
    NetCapExceeded,
    ExistingRiskIncreased(Asset),
}

impl Display for RiskViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for RiskViolation {}

pub fn decimal_from_f64(value: f64, name: &str) -> Result<Decimal, RiskViolation> {
    Decimal::from_f64(value).ok_or_else(|| {
        RiskViolation::InvalidInput(format!("{name} is not finite or representable"))
    })
}

pub fn project_and_validate_portfolio(
    input: &PortfolioProjectionInput,
) -> Result<PortfolioProjection, RiskViolation> {
    validate_input(input)?;

    let asset_cap = checked_mul(
        input.current_equity,
        input.max_single_asset_equity_pct,
        "single-asset cap",
    )?;
    let legacy_gross_cap = checked_mul(
        checked_mul(
            input.current_equity,
            input.curve_leverage,
            "leveraged equity",
        )?,
        input.global_risk_scale,
        "gross cap",
    )?;
    let gross_cap = input.gross_cap_override.unwrap_or(legacy_gross_cap);
    if gross_cap <= Decimal::ZERO {
        return Err(RiskViolation::InvalidInput(
            "gross cap override must be positive".to_string(),
        ));
    }
    let net_cap = checked_mul(input.current_equity, input.max_net_equity_pct, "net cap")?;

    let assets = portfolio_assets(input);
    let open_by_asset = active_open_exposure(input)?;
    let mut constrained_targets = BTreeMap::new();
    for asset in &assets {
        let target = input
            .unconstrained_targets
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO)
            .clamp(-asset_cap, asset_cap);
        constrained_targets.insert(asset.clone(), target);
    }

    scale_targets_to_gross_cap(&mut constrained_targets, gross_cap)?;
    enforce_target_net_cap(&mut constrained_targets, net_cap)?;

    let mut proposed_deltas = BTreeMap::new();
    let mut rounded_deltas = BTreeMap::new();
    for asset in &assets {
        let filled = input
            .filled_positions
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let target = constrained_targets
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let (open_buy, open_sell) = open_by_asset
            .get(asset)
            .copied()
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));
        let signed_open = checked_sub(open_buy, open_sell, "signed open exposure")?;
        let proposed = checked_sub(
            checked_sub(target, filled, "target less filled")?,
            signed_open,
            "target less open exposure",
        )?;
        let rules = input
            .market_rules
            .get(asset)
            .ok_or_else(|| RiskViolation::MissingMarketRules(asset.clone()))?;
        let rounded = round_delta(proposed, rules)?;
        proposed_deltas.insert(asset.clone(), proposed);
        rounded_deltas.insert(asset.clone(), rounded);
    }

    let mut projected_exposure_ranges =
        projected_ranges(input, &assets, &open_by_asset, &rounded_deltas)?;
    let (mut maximum_projected_gross, mut minimum_projected_net, mut maximum_projected_net) =
        range_metrics(&projected_exposure_ranges)?;
    // Held-manual assets are excluded from the gross breach *trigger* only.
    // Asset/net caps still vet all new risk in full. Reported ranges and
    // metrics stay complete; final validation still vets all new risk.
    let transition_breaches = {
        let held = &input.held_manual_assets;
        let asset_breach = projected_exposure_ranges
            .values()
            .any(|range| range.minimum.abs().max(range.maximum.abs()) > asset_cap);
        if asset_breach {
            true
        } else {
            let filtered_gross: Decimal = {
                let filtered: BTreeMap<Asset, ExposureRange> = projected_exposure_ranges
                    .iter()
                    .filter(|(asset, _)| !held.contains(asset.as_str()))
                    .map(|(asset, range)| (asset.clone(), range.clone()))
                    .collect();
                let (gross, _, _) = range_metrics(&filtered)?;
                gross
            };
            let net_breach = minimum_projected_net.abs().max(maximum_projected_net.abs()) > net_cap;
            filtered_gross > gross_cap || net_breach
        }
    };
    if transition_breaches
        && stage_risk_reducing_deltas(input, &assets, &open_by_asset, &mut rounded_deltas)?
    {
        projected_exposure_ranges =
            projected_ranges(input, &assets, &open_by_asset, &rounded_deltas)?;
        (
            maximum_projected_gross,
            minimum_projected_net,
            maximum_projected_net,
        ) = range_metrics(&projected_exposure_ranges)?;
    }
    validate_final_ranges(
        input,
        &projected_exposure_ranges,
        &rounded_deltas,
        asset_cap,
        gross_cap,
        net_cap,
        maximum_projected_gross,
        minimum_projected_net,
        maximum_projected_net,
        &open_by_asset,
    )?;

    Ok(PortfolioProjection {
        constrained_targets,
        proposed_deltas,
        rounded_deltas,
        projected_ranges: projected_exposure_ranges,
        maximum_projected_gross,
        minimum_projected_net,
        maximum_projected_net,
    })
}

fn stage_risk_reducing_deltas(
    input: &PortfolioProjectionInput,
    assets: &BTreeSet<Asset>,
    open_by_asset: &BTreeMap<Asset, (Decimal, Decimal)>,
    rounded_deltas: &mut BTreeMap<Asset, Decimal>,
) -> Result<bool, RiskViolation> {
    let original_deltas = rounded_deltas.clone();
    let empty = BTreeMap::new();
    let baseline = projected_ranges(input, assets, open_by_asset, &empty)?;
    let mut staged = BTreeMap::new();
    let mut has_reduction = false;
    for asset in assets {
        let delta = rounded_deltas.get(asset).copied().unwrap_or(Decimal::ZERO);
        if delta.is_zero() {
            staged.insert(asset.clone(), Decimal::ZERO);
            continue;
        }
        let mut isolated = BTreeMap::new();
        isolated.insert(asset.clone(), delta);
        let with_delta = projected_ranges(input, assets, open_by_asset, &isolated)?;
        let baseline_magnitude = baseline[asset]
            .minimum
            .abs()
            .max(baseline[asset].maximum.abs());
        let projected_magnitude = with_delta[asset]
            .minimum
            .abs()
            .max(with_delta[asset].maximum.abs());
        if projected_magnitude <= baseline_magnitude {
            staged.insert(asset.clone(), delta);
            has_reduction = true;
        } else {
            staged.insert(asset.clone(), Decimal::ZERO);
        }
    }
    if has_reduction {
        // Per-asset reductions can still worsen portfolio net exposure. For
        // example, closing small longs while repairing an oversized short
        // moves the worst-case net further short. Prune deltas on the side
        // causing that deterioration until both projected net endpoints are
        // no worse than the current portfolio. This remains deterministic
        // because removal uses magnitude then stable asset ordering.
        let (_, baseline_minimum_net, baseline_maximum_net) = range_metrics(&baseline)?;
        let baseline_worst_net = baseline_minimum_net.abs().max(baseline_maximum_net.abs());
        loop {
            let candidate_ranges = projected_ranges(input, assets, open_by_asset, &staged)?;
            let (_, candidate_minimum_net, candidate_maximum_net) =
                range_metrics(&candidate_ranges)?;
            let candidate_worst_net = candidate_minimum_net.abs().max(candidate_maximum_net.abs());
            if candidate_worst_net <= baseline_worst_net {
                break;
            }
            let offending_sign = if candidate_minimum_net.abs() >= candidate_maximum_net.abs()
                && candidate_minimum_net < Decimal::ZERO
            {
                -1_i8
            } else {
                1_i8
            };
            let remove = staged
                .iter()
                .filter(|(_, delta)| {
                    (offending_sign < 0 && **delta < Decimal::ZERO)
                        || (offending_sign > 0 && **delta > Decimal::ZERO)
                })
                .min_by(|(left_asset, left), (right_asset, right)| {
                    left.abs()
                        .cmp(&right.abs())
                        .then_with(|| left_asset.cmp(right_asset))
                })
                .map(|(asset, _)| asset.clone())
                .ok_or(RiskViolation::ArithmeticOverflow(
                    "net-worsening staged reduction",
                ))?;
            staged.insert(remove, Decimal::ZERO);
        }
    }
    // The caller invokes this function only after detecting a transition
    // breach. Even when no portfolio-safe reduction remains, the safe staged
    // transition is the all-zero delta vector. Returning `false` here would
    // leave the original violating additions in place.
    for (asset, staged_delta) in &staged {
        let original = original_deltas.get(asset).copied().unwrap_or(Decimal::ZERO);
        if !original.is_zero() && staged_delta.is_zero() {
            eprintln!(
                "projection_breach_fallback=true asset={asset} action=STAGE_RISK_REDUCTION nonfatal=true"
            );
        }
    }
    *rounded_deltas = staged;
    Ok(true)
}

fn validate_input(input: &PortfolioProjectionInput) -> Result<(), RiskViolation> {
    if input.current_equity <= Decimal::ZERO {
        return Err(RiskViolation::InvalidInput(
            "current_equity must be positive".to_string(),
        ));
    }
    if input.curve_leverage <= Decimal::ZERO {
        return Err(RiskViolation::InvalidInput(
            "curve_leverage must be positive".to_string(),
        ));
    }
    if !(MIN_LAUNCH_RISK_SCALE..=MAX_LAUNCH_RISK_SCALE).contains(&input.global_risk_scale) {
        return Err(RiskViolation::InvalidInput(
            "global_risk_scale is outside the approved launch range".to_string(),
        ));
    }
    for (name, value) in [
        (
            "max_single_asset_equity_pct",
            input.max_single_asset_equity_pct,
        ),
        ("max_net_equity_pct", input.max_net_equity_pct),
    ] {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
            return Err(RiskViolation::InvalidInput(format!(
                "{name} must be within [0, 1]"
            )));
        }
    }
    if !input.filled_position_state_complete {
        return Err(RiskViolation::IncompleteFilledPositionState);
    }
    if !input.open_order_state_complete {
        return Err(RiskViolation::IncompleteOpenOrderState);
    }
    for asset in portfolio_assets(input) {
        let rules = input
            .market_rules
            .get(&asset)
            .ok_or_else(|| RiskViolation::MissingMarketRules(asset.clone()))?;
        if rules.mark_price <= Decimal::ZERO
            || rules.price_tick <= Decimal::ZERO
            || rules.size_step <= Decimal::ZERO
        {
            return Err(RiskViolation::InvalidInput(format!(
                "invalid market rules for {asset}"
            )));
        }
    }
    Ok(())
}

fn portfolio_assets(input: &PortfolioProjectionInput) -> BTreeSet<Asset> {
    input
        .filled_positions
        .keys()
        .chain(input.unconstrained_targets.keys())
        .chain(
            input
                .acknowledged_open_orders
                .iter()
                .filter(|order| order.lifecycle != OpenOrderLifecycle::CancelConfirmed)
                .map(|order| &order.asset),
        )
        .cloned()
        .collect()
}

fn active_open_exposure(
    input: &PortfolioProjectionInput,
) -> Result<BTreeMap<Asset, (Decimal, Decimal)>, RiskViolation> {
    let mut exposure = BTreeMap::<Asset, (Decimal, Decimal)>::new();
    for order in &input.acknowledged_open_orders {
        match order.lifecycle {
            OpenOrderLifecycle::CancelConfirmed => continue,
            OpenOrderLifecycle::Unknown => {
                return Err(RiskViolation::UnknownOpenOrderState(order.asset.clone()))
            }
            OpenOrderLifecycle::Acknowledged
            | OpenOrderLifecycle::CancelRequested
            | OpenOrderLifecycle::SubmissionUnknown => {}
        }
        let side = order
            .side
            .ok_or_else(|| RiskViolation::UnknownOpenOrderState(order.asset.clone()))?;
        let notional = order
            .notional
            .ok_or_else(|| RiskViolation::UnknownOpenOrderState(order.asset.clone()))?;
        if notional < Decimal::ZERO {
            return Err(RiskViolation::InvalidInput(format!(
                "open-order notional must be non-negative for {}",
                order.asset
            )));
        }
        let entry = exposure.entry(order.asset.clone()).or_default();
        match side {
            OrderSide::Buy => entry.0 = checked_add(entry.0, notional, "open buys")?,
            OrderSide::Sell => entry.1 = checked_add(entry.1, notional, "open sells")?,
        }
    }
    Ok(exposure)
}

fn scale_targets_to_gross_cap(
    targets: &mut BTreeMap<Asset, Decimal>,
    gross_cap: Decimal,
) -> Result<(), RiskViolation> {
    let gross = sum_checked(targets.values().map(|value| value.abs()), "target gross")?;
    if gross <= gross_cap || gross.is_zero() {
        return Ok(());
    }
    let scale = checked_div(gross_cap, gross, "gross scaling")?;
    for target in targets.values_mut() {
        *target = checked_mul(*target, scale, "scaled target")?;
    }
    // Decimal division is finite precision. With a large target vector, a
    // common scale rounded upward can leave the sum a few atomic decimal units
    // above the cap. Remove only that representational residual from the
    // largest target using deterministic BTreeMap tie-breaking.
    trim_gross_residual(targets, gross_cap)?;
    Ok(())
}

fn trim_gross_residual(
    targets: &mut BTreeMap<Asset, Decimal>,
    gross_cap: Decimal,
) -> Result<(), RiskViolation> {
    let scaled_gross = sum_checked(
        targets.values().map(|value| value.abs()),
        "scaled target gross",
    )?;
    if scaled_gross <= gross_cap {
        return Ok(());
    }
    let residual = checked_sub(scaled_gross, gross_cap, "gross scaling residual")?;
    let asset = targets
        .iter()
        .filter(|(_, value)| !value.is_zero())
        .max_by(|(left_asset, left), (right_asset, right)| {
            left.abs()
                .cmp(&right.abs())
                .then_with(|| right_asset.cmp(left_asset))
        })
        .map(|(asset, _)| asset.clone())
        .ok_or(RiskViolation::ArithmeticOverflow("gross residual target"))?;
    let target = targets
        .get_mut(&asset)
        .ok_or(RiskViolation::ArithmeticOverflow("gross residual lookup"))?;
    if target.abs() < residual {
        return Err(RiskViolation::ArithmeticOverflow(
            "gross residual exceeds target",
        ));
    }
    *target = if target.is_sign_positive() {
        checked_sub(*target, residual, "positive gross residual")?
    } else {
        checked_add(*target, residual, "negative gross residual")?
    };
    Ok(())
}

fn enforce_target_net_cap(
    targets: &mut BTreeMap<Asset, Decimal>,
    net_cap: Decimal,
) -> Result<(), RiskViolation> {
    let positive = sum_checked(
        targets
            .values()
            .filter(|value| **value > Decimal::ZERO)
            .copied(),
        "positive targets",
    )?;
    let negative = sum_checked(
        targets
            .values()
            .filter(|value| **value < Decimal::ZERO)
            .map(|value| value.abs()),
        "negative targets",
    )?;
    let net = checked_sub(positive, negative, "target net")?;
    if net > net_cap && !positive.is_zero() {
        let allowed_positive = checked_add(negative, net_cap, "allowed positive target")?;
        let scale = checked_div(allowed_positive, positive, "positive net scaling")?;
        for target in targets.values_mut().filter(|value| **value > Decimal::ZERO) {
            *target = checked_mul(*target, scale, "net-scaled positive target")?;
        }
    } else if net < -net_cap && !negative.is_zero() {
        let allowed_negative = checked_add(positive, net_cap, "allowed negative target")?;
        let scale = checked_div(allowed_negative, negative, "negative net scaling")?;
        for target in targets.values_mut().filter(|value| **value < Decimal::ZERO) {
            *target = checked_mul(*target, scale, "net-scaled negative target")?;
        }
    }
    trim_net_residual(targets, net_cap)?;
    Ok(())
}

fn trim_net_residual(
    targets: &mut BTreeMap<Asset, Decimal>,
    net_cap: Decimal,
) -> Result<(), RiskViolation> {
    let net = sum_checked(targets.values().copied(), "net-scaled target net")?;
    let residual = if net > net_cap {
        checked_sub(net, net_cap, "positive net scaling residual")?
    } else if net < -net_cap {
        checked_add(net, net_cap, "negative net scaling residual")?
    } else {
        return Ok(());
    };
    let positive_residual = residual > Decimal::ZERO;
    let asset = targets
        .iter()
        .filter(|(_, value)| {
            (positive_residual && **value > Decimal::ZERO)
                || (!positive_residual && **value < Decimal::ZERO)
        })
        .max_by(|(left_asset, left), (right_asset, right)| {
            left.abs()
                .cmp(&right.abs())
                .then_with(|| right_asset.cmp(left_asset))
        })
        .map(|(asset, _)| asset.clone())
        .ok_or(RiskViolation::ArithmeticOverflow("net residual target"))?;
    let target = targets
        .get_mut(&asset)
        .ok_or(RiskViolation::ArithmeticOverflow("net residual lookup"))?;
    *target = checked_sub(*target, residual, "trim net residual")?;
    Ok(())
}

fn round_delta(delta: Decimal, rules: &MarketRules) -> Result<Decimal, RiskViolation> {
    if delta.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let rounded_price = if delta > Decimal::ZERO {
        round_up_to_step(rules.mark_price, rules.price_tick)?
    } else {
        round_down_to_step(rules.mark_price, rules.price_tick)?
    };
    // Size against the price that will actually be carried by the rounded
    // action. Sizing a buy against the lower, unrounded mark and then valuing
    // it at an outward-rounded price can make a vector that was scaled exactly
    // to the gross cap breach that cap after rounding.
    let quantity = checked_div(delta.abs(), rounded_price, "unrounded quantity")?;
    let rounded_quantity = round_down_to_step(quantity, rules.size_step)?;
    let notional = checked_mul(rounded_quantity, rounded_price, "rounded notional")?;
    Ok(if delta > Decimal::ZERO {
        notional
    } else {
        -notional
    })
}

fn round_down_to_step(value: Decimal, step: Decimal) -> Result<Decimal, RiskViolation> {
    let units = checked_div(value, step, "round-down units")?.floor();
    checked_mul(units, step, "round-down result")
}

fn round_up_to_step(value: Decimal, step: Decimal) -> Result<Decimal, RiskViolation> {
    let units = checked_div(value, step, "round-up units")?.ceil();
    checked_mul(units, step, "round-up result")
}

fn projected_ranges(
    input: &PortfolioProjectionInput,
    assets: &BTreeSet<Asset>,
    open_by_asset: &BTreeMap<Asset, (Decimal, Decimal)>,
    rounded_deltas: &BTreeMap<Asset, Decimal>,
) -> Result<BTreeMap<Asset, ExposureRange>, RiskViolation> {
    let mut ranges = BTreeMap::new();
    for asset in assets {
        let filled = input
            .filled_positions
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let (open_buy, open_sell) = open_by_asset
            .get(asset)
            .copied()
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));
        let delta = rounded_deltas.get(asset).copied().unwrap_or(Decimal::ZERO);
        let proposed_buy = delta.max(Decimal::ZERO);
        let proposed_sell = (-delta).max(Decimal::ZERO);
        let minimum = checked_sub(
            checked_sub(filled, open_sell, "range minimum open sells")?,
            proposed_sell,
            "range minimum proposed sells",
        )?;
        let maximum = checked_add(
            checked_add(filled, open_buy, "range maximum open buys")?,
            proposed_buy,
            "range maximum proposed buys",
        )?;
        ranges.insert(asset.clone(), ExposureRange { minimum, maximum });
    }
    Ok(ranges)
}

fn range_metrics(
    ranges: &BTreeMap<Asset, ExposureRange>,
) -> Result<(Decimal, Decimal, Decimal), RiskViolation> {
    let gross = sum_checked(
        ranges
            .values()
            .map(|range| range.minimum.abs().max(range.maximum.abs())),
        "projected gross",
    )?;
    let minimum_net = sum_checked(ranges.values().map(|range| range.minimum), "minimum net")?;
    let maximum_net = sum_checked(ranges.values().map(|range| range.maximum), "maximum net")?;
    Ok((gross, minimum_net, maximum_net))
}

#[allow(clippy::too_many_arguments)]
fn validate_final_ranges(
    input: &PortfolioProjectionInput,
    ranges: &BTreeMap<Asset, ExposureRange>,
    rounded_deltas: &BTreeMap<Asset, Decimal>,
    asset_cap: Decimal,
    gross_cap: Decimal,
    net_cap: Decimal,
    projected_gross: Decimal,
    minimum_net: Decimal,
    maximum_net: Decimal,
    open_by_asset: &BTreeMap<Asset, (Decimal, Decimal)>,
) -> Result<(), RiskViolation> {
    let filled_ranges = input
        .filled_positions
        .iter()
        .map(|(asset, filled)| {
            (
                asset.clone(),
                ExposureRange {
                    minimum: *filled,
                    maximum: *filled,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let (filled_gross, filled_minimum_net, filled_maximum_net) = range_metrics(&filled_ranges)?;
    let filled_asset_violation = filled_ranges
        .values()
        .any(|range| range.minimum.abs().max(range.maximum.abs()) > asset_cap);
    let filled_violation = filled_asset_violation
        || filled_gross > gross_cap
        || filled_minimum_net.abs().max(filled_maximum_net.abs()) > net_cap;

    let final_asset_violation = ranges
        .iter()
        .find(|(_, range)| range.minimum.abs().max(range.maximum.abs()) > asset_cap)
        .map(|(asset, _)| asset.clone());
    let final_gross_violation = projected_gross > gross_cap;
    let final_net_violation = minimum_net.abs().max(maximum_net.abs()) > net_cap;
    if !filled_violation {
        if let Some(asset) = final_asset_violation {
            return Err(RiskViolation::AssetCapExceeded(asset));
        }
        if final_gross_violation {
            return Err(RiskViolation::GrossCapExceeded {
                projected: projected_gross,
                cap: gross_cap,
            });
        }
        if final_net_violation {
            return Err(RiskViolation::NetCapExceeded);
        }
        return Ok(());
    }

    let baseline_ranges = projected_ranges(
        input,
        &portfolio_assets(input),
        open_by_asset,
        &BTreeMap::new(),
    )?;
    let (baseline_gross, baseline_minimum_net, baseline_maximum_net) =
        range_metrics(&baseline_ranges)?;
    for (asset, delta) in rounded_deltas {
        if delta.is_zero() {
            continue;
        }
        let filled = input
            .filled_positions
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO);
        if filled.is_zero()
            || delta.signum() == filled.signum()
            || delta.abs() > filled.abs()
            || ranges[asset].minimum.abs().max(ranges[asset].maximum.abs())
                > baseline_ranges[asset]
                    .minimum
                    .abs()
                    .max(baseline_ranges[asset].maximum.abs())
        {
            return Err(RiskViolation::ExistingRiskIncreased(asset.clone()));
        }
    }
    if projected_gross > baseline_gross
        || minimum_net.abs().max(maximum_net.abs())
            > baseline_minimum_net.abs().max(baseline_maximum_net.abs())
    {
        return Err(RiskViolation::GrossCapExceeded {
            projected: projected_gross,
            cap: baseline_gross,
        });
    }
    Ok(())
}

fn sum_checked<I>(values: I, operation: &'static str) -> Result<Decimal, RiskViolation>
where
    I: IntoIterator<Item = Decimal>,
{
    values.into_iter().try_fold(Decimal::ZERO, |total, value| {
        checked_add(total, value, operation)
    })
}

fn checked_add(
    left: Decimal,
    right: Decimal,
    operation: &'static str,
) -> Result<Decimal, RiskViolation> {
    left.checked_add(right)
        .ok_or(RiskViolation::ArithmeticOverflow(operation))
}

fn checked_sub(
    left: Decimal,
    right: Decimal,
    operation: &'static str,
) -> Result<Decimal, RiskViolation> {
    left.checked_sub(right)
        .ok_or(RiskViolation::ArithmeticOverflow(operation))
}

fn checked_mul(
    left: Decimal,
    right: Decimal,
    operation: &'static str,
) -> Result<Decimal, RiskViolation> {
    left.checked_mul(right)
        .ok_or(RiskViolation::ArithmeticOverflow(operation))
}

fn checked_div(
    left: Decimal,
    right: Decimal,
    operation: &'static str,
) -> Result<Decimal, RiskViolation> {
    left.checked_div(right)
        .ok_or(RiskViolation::ArithmeticOverflow(operation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn rules(mark: &str) -> MarketRules {
        MarketRules {
            mark_price: d(mark),
            price_tick: d("0.01"),
            size_step: d("0.0001"),
        }
    }

    fn input_with_targets(targets: &[(&str, &str)]) -> PortfolioProjectionInput {
        let unconstrained_targets = targets
            .iter()
            .map(|(asset, target)| ((*asset).to_string(), d(target)))
            .collect::<BTreeMap<_, _>>();
        let market_rules = targets
            .iter()
            .map(|(asset, _)| ((*asset).to_string(), rules("100")))
            .collect::<BTreeMap<_, _>>();
        PortfolioProjectionInput {
            current_equity: d("1000"),
            curve_leverage: d("8"),
            global_risk_scale: d("0.025"),
            max_single_asset_equity_pct: d("0.65"),
            max_net_equity_pct: d("0.65"),
            filled_positions: BTreeMap::new(),
            filled_position_state_complete: true,
            acknowledged_open_orders: Vec::new(),
            open_order_state_complete: true,
            unconstrained_targets,
            market_rules,
            gross_cap_override: None,
            held_manual_assets: BTreeSet::new(),
        }
    }

    #[test]
    fn launch_configuration_has_two_hundred_dollar_gross_cap() {
        let projection =
            project_and_validate_portfolio(&input_with_targets(&[("BTC", "1000")])).unwrap();
        assert_eq!(projection.constrained_targets["BTC"], d("200"));
        assert_eq!(projection.maximum_projected_gross, d("200"));
    }

    #[test]
    fn single_asset_cap_is_unleveraged_equity() {
        let mut input = input_with_targets(&[("BTC", "5000")]);
        input.global_risk_scale = d("0.10");
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(projection.constrained_targets["BTC"], d("650"));
    }

    #[test]
    fn portfolio_net_is_clamped_to_unleveraged_limit() {
        let mut input = input_with_targets(&[("BTC", "500"), ("ETH", "500")]);
        input.global_risk_scale = d("0.10");
        let projection = project_and_validate_portfolio(&input).unwrap();
        let net = projection
            .constrained_targets
            .values()
            .copied()
            .sum::<Decimal>();
        assert_eq!(net, d("650"));
    }

    #[test]
    fn individually_valid_assets_scale_to_portfolio_gross_cap() {
        let projection =
            project_and_validate_portfolio(&input_with_targets(&[("BTC", "150"), ("ETH", "150")]))
                .unwrap();
        assert_eq!(projection.maximum_projected_gross, d("200"));
        assert_eq!(projection.constrained_targets["BTC"], d("100"));
        assert_eq!(projection.constrained_targets["ETH"], d("100"));
    }

    #[test]
    fn opposing_open_orders_are_projected_as_a_range() {
        let mut input = input_with_targets(&[("BTC", "0")]);
        input.acknowledged_open_orders = vec![
            open(
                "BTC",
                OrderSide::Buy,
                "150",
                OpenOrderLifecycle::Acknowledged,
            ),
            open(
                "BTC",
                OrderSide::Sell,
                "150",
                OpenOrderLifecycle::Acknowledged,
            ),
        ];
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(projection.projected_ranges["BTC"].minimum, d("-150"));
        assert_eq!(projection.projected_ranges["BTC"].maximum, d("150"));
        assert_eq!(projection.maximum_projected_gross, d("150"));
    }

    #[test]
    fn cancel_requested_remains_but_confirmed_cancel_is_removed() {
        let mut requested = input_with_targets(&[("BTC", "0")]);
        requested.acknowledged_open_orders = vec![open(
            "BTC",
            OrderSide::Buy,
            "50",
            OpenOrderLifecycle::CancelRequested,
        )];
        let requested_projection = project_and_validate_portfolio(&requested).unwrap();
        assert_eq!(
            requested_projection.projected_ranges["BTC"].maximum,
            d("50")
        );

        requested.acknowledged_open_orders[0].lifecycle = OpenOrderLifecycle::CancelConfirmed;
        let confirmed_projection = project_and_validate_portfolio(&requested).unwrap();
        assert_eq!(
            confirmed_projection.projected_ranges["BTC"].maximum,
            Decimal::ZERO
        );

        let mut cancelled_only = input_with_targets(&[]);
        cancelled_only.acknowledged_open_orders = requested.acknowledged_open_orders;
        let cancelled_only_projection = project_and_validate_portfolio(&cancelled_only).unwrap();
        assert!(cancelled_only_projection.projected_ranges.is_empty());
    }

    #[test]
    fn outward_price_rounding_never_increases_buy_asset_exposure() {
        let mut input = input_with_targets(&[("BTC", "650")]);
        input.global_risk_scale = d("0.10");
        input.market_rules.insert(
            "BTC".to_string(),
            MarketRules {
                mark_price: d("99.95"),
                price_tick: d("1"),
                size_step: d("0.0001"),
            },
        );
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert!(projection.projected_ranges["BTC"].maximum <= d("650"));
    }

    #[test]
    fn outward_price_rounding_never_increases_vector_gross() {
        let mut input = input_with_targets(&[("BTC", "100"), ("ETH", "100")]);
        for rules in input.market_rules.values_mut() {
            rules.mark_price = d("99.95");
            rules.price_tick = d("1");
            rules.size_step = d("0.0001");
        }
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert!(projection.maximum_projected_gross <= d("200"));
    }

    #[test]
    fn large_vector_scaling_cannot_exceed_cap_from_decimal_residuals() {
        let targets = (0..169)
            .map(|index| (format!("ASSET{index:03}"), d("10")))
            .collect::<BTreeMap<_, _>>();
        let mut input = input_with_targets(&[]);
        input.unconstrained_targets = targets;
        input.market_rules = input
            .unconstrained_targets
            .keys()
            .map(|asset| {
                (
                    asset.clone(),
                    MarketRules {
                        mark_price: d("1"),
                        price_tick: d("0.00001"),
                        size_step: d("0.00001"),
                    },
                )
            })
            .collect();

        let projection = project_and_validate_portfolio(&input).unwrap();
        let constrained_gross: Decimal = projection
            .constrained_targets
            .values()
            .map(|value| value.abs())
            .sum();
        assert!(constrained_gross <= d("200"));
        assert!(projection.maximum_projected_gross <= d("200"));
    }

    #[test]
    fn full_cap_rotation_stages_reductions_before_new_risk() {
        let mut input = input_with_targets(&[("AR", "50"), ("SAGA", "150")]);
        input.filled_positions.insert("AR".into(), d("100"));
        input.market_rules.insert(
            "AR".into(),
            MarketRules {
                mark_price: d("1"),
                price_tick: d("0.01"),
                size_step: d("0.01"),
            },
        );
        input.market_rules.insert(
            "SAGA".into(),
            MarketRules {
                mark_price: d("1"),
                price_tick: d("0.01"),
                size_step: d("0.01"),
            },
        );

        let reduction = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(reduction.rounded_deltas["AR"], d("-50"));
        assert_eq!(reduction.rounded_deltas["SAGA"], Decimal::ZERO);
        assert!(reduction.maximum_projected_gross <= d("200"));

        input.filled_positions.insert("AR".into(), d("50"));
        let addition = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(addition.rounded_deltas["AR"], Decimal::ZERO);
        assert_eq!(addition.rounded_deltas["SAGA"], d("150"));
        assert!(addition.maximum_projected_gross <= d("200"));
    }

    #[test]
    fn outward_price_rounding_never_increases_vector_net() {
        let mut input = input_with_targets(&[("BTC", "650")]);
        input.global_risk_scale = d("0.10");
        input.max_single_asset_equity_pct = d("1");
        input.market_rules.insert(
            "BTC".to_string(),
            MarketRules {
                mark_price: d("99.95"),
                price_tick: d("1"),
                size_step: d("0.0001"),
            },
        );
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert!(projection.maximum_projected_net <= d("650"));
    }

    #[test]
    fn finite_precision_net_scaling_is_trimmed_inside_cap() {
        for sign in [Decimal::ONE, -Decimal::ONE] {
            let mut targets = (0..17)
                .map(|index| (format!("A{index:02}"), sign * d("100")))
                .collect::<BTreeMap<_, _>>();
            enforce_target_net_cap(&mut targets, d("65.6682351")).unwrap();
            let net: Decimal = targets.values().copied().sum();
            assert!(net.abs() <= d("65.6682351"));
        }
    }

    #[test]
    fn equity_reduction_immediately_tightens_all_caps() {
        let full = project_and_validate_portfolio(&input_with_targets(&[("BTC", "200")])).unwrap();
        let mut reduced_input = input_with_targets(&[("BTC", "200")]);
        reduced_input.current_equity = d("500");
        let reduced = project_and_validate_portfolio(&reduced_input).unwrap();
        assert_eq!(full.maximum_projected_gross, d("200"));
        assert_eq!(reduced.maximum_projected_gross, d("100"));
    }

    #[test]
    fn existing_over_limit_position_allows_only_reduction() {
        let mut input = input_with_targets(&[("BTC", "0")]);
        input.filled_positions.insert("BTC".to_string(), d("700"));
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert!(projection.rounded_deltas["BTC"] < Decimal::ZERO);

        input.acknowledged_open_orders.push(open(
            "BTC",
            OrderSide::Buy,
            "10",
            OpenOrderLifecycle::Acknowledged,
        ));
        assert!(project_and_validate_portfolio(&input).is_err());
    }

    #[test]
    fn equity_loss_recovery_does_not_close_longs_that_worsen_oversized_short_net() {
        let mut input = input_with_targets(&[
            ("ADA", "0"),
            ("AIXBT", "0"),
            ("ATOM", "0"),
            ("BTC", "-64.80"),
        ]);
        input.current_equity = d("99.77203834502715287484533334");
        input.global_risk_scale = d("0.10");
        input.filled_positions = [
            ("ADA".into(), d("0.180195")),
            ("AIXBT".into(), d("0.018913")),
            ("ATOM".into(), d("0.014667")),
            ("BTC".into(), d("-64.889230")),
        ]
        .into_iter()
        .collect();
        input.market_rules = input
            .unconstrained_targets
            .keys()
            .map(|asset| {
                (
                    asset.clone(),
                    MarketRules {
                        mark_price: d("1"),
                        price_tick: d("0.000001"),
                        size_step: d("0.000001"),
                    },
                )
            })
            .collect();

        let projection = project_and_validate_portfolio(&input).unwrap();
        assert!(projection.rounded_deltas["BTC"] > Decimal::ZERO);
        assert_eq!(projection.rounded_deltas["ADA"], Decimal::ZERO);
        assert_eq!(projection.rounded_deltas["AIXBT"], Decimal::ZERO);
        assert_eq!(projection.rounded_deltas["ATOM"], Decimal::ZERO);
    }

    #[test]
    fn near_full_micro_book_stages_rotation_without_restoring_unsafe_addition() {
        let mut input =
            input_with_targets(&[("BTC", "-64.207695"), ("ETHFI", "13.429030"), ("SOL", "12")]);
        input.current_equity = d("99.95998779829860184027777778");
        input.global_risk_scale = d("0.10");
        input.filled_positions = [
            ("BTC".into(), d("-64.207695")),
            ("ETHFI".into(), d("13.429030")),
        ]
        .into_iter()
        .collect();
        input.market_rules = input
            .unconstrained_targets
            .keys()
            .map(|asset| {
                (
                    asset.clone(),
                    MarketRules {
                        mark_price: d("1"),
                        price_tick: d("0.000001"),
                        size_step: d("0.000001"),
                    },
                )
            })
            .collect();

        let projection = project_and_validate_portfolio(&input).unwrap();
        let cap = input.current_equity * input.curve_leverage * input.global_risk_scale;
        assert!(projection.maximum_projected_gross <= cap);
        assert_eq!(projection.rounded_deltas["SOL"], Decimal::ZERO);
    }

    #[test]
    fn missing_or_unknown_open_order_state_fails_closed() {
        let mut input = input_with_targets(&[("BTC", "0")]);
        input.filled_position_state_complete = false;
        assert_eq!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::IncompleteFilledPositionState)
        );

        input.filled_position_state_complete = true;
        input.open_order_state_complete = false;
        assert_eq!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::IncompleteOpenOrderState)
        );

        input.open_order_state_complete = true;
        input.acknowledged_open_orders.push(OpenOrderExposure {
            asset: "BTC".to_string(),
            side: None,
            notional: Some(d("10")),
            lifecycle: OpenOrderLifecycle::Acknowledged,
        });
        assert!(matches!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::UnknownOpenOrderState(_))
        ));

        input.acknowledged_open_orders[0].side = Some(OrderSide::Buy);
        input.acknowledged_open_orders[0].notional = None;
        assert!(matches!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::UnknownOpenOrderState(_))
        ));

        input.acknowledged_open_orders[0].notional = Some(d("10"));
        input.acknowledged_open_orders[0].lifecycle = OpenOrderLifecycle::Unknown;
        assert!(matches!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::UnknownOpenOrderState(_))
        ));
    }

    #[test]
    fn invalid_numeric_conversion_negative_equity_and_overflow_fail_closed() {
        assert!(decimal_from_f64(f64::NAN, "nan").is_err());
        assert!(decimal_from_f64(f64::INFINITY, "infinity").is_err());

        let mut input = input_with_targets(&[("BTC", "1")]);
        input.current_equity = d("-1");
        assert!(matches!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::InvalidInput(_))
        ));

        input.current_equity = Decimal::MAX;
        input.curve_leverage = d("8");
        assert!(matches!(
            project_and_validate_portfolio(&input),
            Err(RiskViolation::ArithmeticOverflow(_))
        ));
    }

    #[test]
    fn map_insertion_order_cannot_change_projection() {
        let first = input_with_targets(&[("BTC", "150"), ("ETH", "150"), ("SOL", "-50")]);
        let second = input_with_targets(&[("SOL", "-50"), ("ETH", "150"), ("BTC", "150")]);
        assert_eq!(
            project_and_validate_portfolio(&first).unwrap(),
            project_and_validate_portfolio(&second).unwrap()
        );
    }

    #[test]
    fn gross_cap_override_expands_ceiling() {
        let mut legacy = input_with_targets(&[("BTC", "1000")]);
        let legacy_projection = project_and_validate_portfolio(&legacy).unwrap();
        assert_eq!(legacy_projection.constrained_targets["BTC"], d("200"));

        legacy.gross_cap_override = Some(d("1000"));
        let overridden = project_and_validate_portfolio(&legacy).unwrap();
        // Asset cap (0.65 * 1000 = 650) still applies; gross override lifts
        // the 200 ceiling to 650 for this single-asset vector.
        assert_eq!(overridden.constrained_targets["BTC"], d("650"));
        assert_eq!(overridden.maximum_projected_gross, d("650"));
    }

    #[test]
    fn held_manual_assets_excluded_from_gross_trigger() {
        // Live shape: settled book inside the dynamic ceiling, fresh probe
        // inside asset/net caps. Override carries the ceiling; held set
        // carries the manual book for trigger filtering.
        let mut input = input_with_targets(&[("BTC", "10")]);
        input.current_equity = d("427");
        input.curve_leverage = d("8");
        input.global_risk_scale = d("0.10");
        input.gross_cap_override = Some(d("1068"));
        input.filled_positions = BTreeMap::from([
            ("AERO".to_string(), d("200")),
            ("WIF".to_string(), d("186")),
        ]);
        input.market_rules.insert("AERO".into(), rules("1"));
        input.market_rules.insert("WIF".into(), rules("1"));
        input.market_rules.insert("BTC".into(), rules("1"));
        input
            .unconstrained_targets
            .insert("AERO".to_string(), d("0"));
        input
            .unconstrained_targets
            .insert("WIF".to_string(), d("0"));
        input.held_manual_assets = BTreeSet::from(["AERO".to_string(), "WIF".to_string()]);
        // Book 386 inside 1068, fresh 10 inside asset (277) and net caps.
        // Use mixed signs to keep net inside cap: AERO long, WIF short.
        input.filled_positions.insert("WIF".to_string(), d("-186"));
        let projection = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(projection.rounded_deltas["BTC"], d("10"));
        assert!(projection.maximum_projected_gross <= d("1068"));
    }

    #[test]
    fn genuine_breach_still_vetoes_with_override_and_held() {
        // Existing strategy position over asset cap + fresh addition that
        // increases risk must still fail closed, even with override + held.
        let mut input = input_with_targets(&[("ETH", "50")]);
        input.current_equity = d("100");
        input.curve_leverage = d("8");
        input.global_risk_scale = d("0.10");
        input.gross_cap_override = Some(d("250"));
        input.filled_positions = BTreeMap::from([("BTC".to_string(), d("700"))]);
        input.market_rules.insert("BTC".into(), rules("1"));
        input.market_rules.insert("ETH".into(), rules("1"));
        input
            .unconstrained_targets
            .insert("BTC".to_string(), d("0"));
        // BTC is strategy-owned (not held), so filled violation is genuine.
        // Fresh ETH addition must be vetoed via staging (zeroed, no intent).
        let vetoed = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(vetoed.rounded_deltas["ETH"], Decimal::ZERO);

        // Same shape but BTC held-manual: asset breach still triggers
        // staging (asset/net vet full), fresh ETH is zeroed, not admitted.
        // With gross-only exclusion, asset breach still stages.
        input.held_manual_assets = BTreeSet::from(["BTC".to_string()]);
        let staged = project_and_validate_portfolio(&input).unwrap();
        assert_eq!(staged.rounded_deltas["ETH"], Decimal::ZERO);
    }

    fn open(
        asset: &str,
        side: OrderSide,
        notional: &str,
        lifecycle: OpenOrderLifecycle,
    ) -> OpenOrderExposure {
        OpenOrderExposure {
            asset: asset.to_string(),
            side: Some(side),
            notional: Some(d(notional)),
            lifecycle,
        }
    }
}
