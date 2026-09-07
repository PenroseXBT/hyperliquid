use crate::domain::decision::{MarketSnapshotId, PlannedAction, Side};
use rust_decimal::Decimal;
#[cfg(test)]
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LatencyScenario {
    Optimistic,
    Expected,
    Conservative,
}

#[cfg(test)]
impl LatencyScenario {
    fn code(self) -> u8 {
        match self {
            Self::Optimistic => 0,
            Self::Expected => 1,
            Self::Conservative => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutableBook {
    pub snapshot_id: MarketSnapshotId,
    pub observed_at_mono: u64,
    pub midpoint: Decimal,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketableIocPricingMode {
    CompleteVisibleDepth,
    BoundedReferenceFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketableIocPlan {
    pub limit_price: Decimal,
    pub requested_quantity: Decimal,
    pub visible_executable_quantity: Decimal,
    pub worst_required_depth_price: Option<Decimal>,
    pub pricing_mode: MarketableIocPricingMode,
}

#[derive(Debug, Clone)]
pub struct FixtureExecutionInput {
    pub action: PlannedAction,
    pub decision_timestamp_mono: u64,
    pub decision_market_snapshot: ExecutableBook,
    pub evaluation_market_snapshot: ExecutableBook,
    pub latency_scenario: LatencyScenario,
    pub configured_latency_ms: u64,
    pub proposed_limit_price: Decimal,
    pub rounded_quantity: Decimal,
    pub taker_fee_rate: Decimal,
    pub funding_attribution: Decimal,
    pub position_before: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionFillId(pub [u8; 32]);

impl Display for ExecutionFillId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("0x")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFill {
    pub execution_id: ExecutionFillId,
    pub action: PlannedAction,
    pub decision_timestamp_mono: u64,
    pub decision_market_snapshot_id: MarketSnapshotId,
    pub evaluation_market_snapshot_id: MarketSnapshotId,
    pub latency_scenario: LatencyScenario,
    pub configured_latency_ms: u64,
    pub proposed_limit_price: Decimal,
    pub rounded_quantity: Decimal,
    pub modeled_filled_quantity: Decimal,
    pub modeled_average_fill_price: Option<Decimal>,
    pub unfilled_ioc_remainder: Decimal,
    pub modeled_filled_notional: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    pub slippage: Decimal,
    pub position_before: Decimal,
    pub position_after: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IocError {
    InvalidInput(&'static str),
    InvalidBook(&'static str),
    SnapshotTooEarly,
    ArithmeticOverflow(&'static str),
}

impl Display for IocError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for IocError {}

/// Builds the capped, marketable IOC price used by production execution.
/// Complete visible depth is preferred. If the
/// book cannot cover the requested quantity, the plan falls back to the most
/// aggressive price permitted by the configured reference-price boundary.
pub fn plan_marketable_ioc(
    side: Side,
    quantity: Decimal,
    evaluation_market_snapshot: &ExecutableBook,
    reference_price: Decimal,
    execution_cushion: Decimal,
    maximum_slippage: Decimal,
    price_tick: Decimal,
) -> Result<MarketableIocPlan, IocError> {
    if quantity <= Decimal::ZERO
        || reference_price <= Decimal::ZERO
        || execution_cushion < Decimal::ZERO
        || maximum_slippage < Decimal::ZERO
        || price_tick <= Decimal::ZERO
    {
        return Err(IocError::InvalidInput("marketable IOC pricing inputs"));
    }
    validate_book(evaluation_market_snapshot)?;

    let levels = match side {
        Side::Buy => &evaluation_market_snapshot.asks,
        Side::Sell => &evaluation_market_snapshot.bids,
    };
    let mut visible = Decimal::ZERO;
    let mut worst_required = None;
    for level in levels {
        visible = visible
            .checked_add(level.quantity)
            .ok_or(IocError::ArithmeticOverflow("visible depth"))?;
        if visible >= quantity {
            worst_required = Some(level.price);
            break;
        }
    }

    let maximum_raw = match side {
        Side::Buy => reference_price.checked_mul(Decimal::ONE + maximum_slippage),
        Side::Sell => reference_price.checked_mul(Decimal::ONE - maximum_slippage),
    }
    .ok_or(IocError::ArithmeticOverflow("maximum IOC price"))?;
    let maximum_limit = round_price_outward(maximum_raw, price_tick, side)?;

    let (limit_price, pricing_mode) = if let Some(worst_price) = worst_required {
        let depth_raw = match side {
            Side::Buy => worst_price.checked_mul(Decimal::ONE + execution_cushion),
            Side::Sell => worst_price.checked_mul(Decimal::ONE - execution_cushion),
        }
        .ok_or(IocError::ArithmeticOverflow("depth IOC price"))?;
        (
            round_price_outward(depth_raw, price_tick, side)?,
            MarketableIocPricingMode::CompleteVisibleDepth,
        )
    } else {
        (
            maximum_limit,
            MarketableIocPricingMode::BoundedReferenceFallback,
        )
    };

    let visible_executable_quantity = levels
        .iter()
        .take_while(|level| match side {
            Side::Buy => level.price <= limit_price,
            Side::Sell => level.price >= limit_price,
        })
        .try_fold(Decimal::ZERO, |sum, level| sum.checked_add(level.quantity))
        .ok_or(IocError::ArithmeticOverflow("executable visible depth"))?
        .min(quantity);

    Ok(MarketableIocPlan {
        limit_price,
        requested_quantity: quantity,
        visible_executable_quantity,
        worst_required_depth_price: worst_required,
        pricing_mode,
    })
}

fn round_price_outward(value: Decimal, tick: Decimal, side: Side) -> Result<Decimal, IocError> {
    if value <= Decimal::ZERO || tick <= Decimal::ZERO {
        return Err(IocError::InvalidInput("price and tick"));
    }
    let units = value
        .checked_div(tick)
        .ok_or(IocError::ArithmeticOverflow("price units"))?;
    let units = match side {
        Side::Buy => units.ceil(),
        Side::Sell => units.floor(),
    };
    units
        .checked_mul(tick)
        .ok_or(IocError::ArithmeticOverflow("rounded IOC price"))
}

#[cfg(test)]
pub fn execute_fixture_ioc(input: &FixtureExecutionInput) -> Result<ExecutionFill, IocError> {
    validate_input(input)?;
    let expected_evaluation_at = input
        .decision_timestamp_mono
        .checked_add(input.configured_latency_ms)
        .ok_or(IocError::ArithmeticOverflow("latency timestamp"))?;
    if input.evaluation_market_snapshot.observed_at_mono < expected_evaluation_at {
        return Err(IocError::SnapshotTooEarly);
    }

    let (filled, fill_value) = quote_ioc(
        input.action.side,
        input.rounded_quantity,
        input.proposed_limit_price,
        &input.evaluation_market_snapshot,
    )?;
    let remaining = input.rounded_quantity - filled;
    let average = if filled.is_zero() {
        None
    } else {
        Some(
            fill_value
                .checked_div(filled)
                .ok_or(IocError::ArithmeticOverflow("average fill price"))?,
        )
    };
    let fees = fill_value
        .checked_mul(input.taker_fee_rate)
        .ok_or(IocError::ArithmeticOverflow("fees"))?;
    let slippage = match average {
        None => Decimal::ZERO,
        Some(average) => {
            let price_cost = match input.action.side {
                Side::Buy => average.checked_sub(input.decision_market_snapshot.midpoint),
                Side::Sell => input.decision_market_snapshot.midpoint.checked_sub(average),
            }
            .ok_or(IocError::ArithmeticOverflow("slippage price"))?;
            price_cost
                .checked_mul(filled)
                .ok_or(IocError::ArithmeticOverflow("slippage"))?
        }
    };
    let signed_fill = match input.action.side {
        Side::Buy => filled,
        Side::Sell => -filled,
    };
    let position_after = input
        .position_before
        .checked_add(signed_fill)
        .ok_or(IocError::ArithmeticOverflow("position after"))?;
    Ok(ExecutionFill {
        execution_id: derive_execution_id(input),
        action: input.action.clone(),
        decision_timestamp_mono: input.decision_timestamp_mono,
        decision_market_snapshot_id: input.decision_market_snapshot.snapshot_id,
        evaluation_market_snapshot_id: input.evaluation_market_snapshot.snapshot_id,
        latency_scenario: input.latency_scenario,
        configured_latency_ms: input.configured_latency_ms,
        proposed_limit_price: input.proposed_limit_price,
        rounded_quantity: input.rounded_quantity,
        modeled_filled_quantity: filled,
        modeled_average_fill_price: average,
        unfilled_ioc_remainder: remaining,
        modeled_filled_notional: fill_value,
        fees,
        funding: input.funding_attribution,
        slippage,
        position_before: input.position_before,
        position_after,
    })
}

/// The single visible-depth walker for execution, admission costs and delayed
/// learning. A partial fill stays partial; unseen liquidity is never invented.
pub fn quote_ioc(
    side: Side,
    quantity: Decimal,
    limit: Decimal,
    book: &ExecutableBook,
) -> Result<(Decimal, Decimal), IocError> {
    validate_book(book)?;
    if quantity <= Decimal::ZERO || limit <= Decimal::ZERO {
        return Err(IocError::InvalidInput("IOC quote"));
    }
    let levels = match side {
        Side::Buy => &book.asks,
        Side::Sell => &book.bids,
    };
    let mut filled = Decimal::ZERO;
    let mut value = Decimal::ZERO;
    for level in levels.iter().take_while(|level| match side {
        Side::Buy => level.price <= limit,
        Side::Sell => level.price >= limit,
    }) {
        let take = (quantity - filled).min(level.quantity);
        value = take
            .checked_mul(level.price)
            .and_then(|v| value.checked_add(v))
            .ok_or(IocError::ArithmeticOverflow("IOC value"))?;
        filled += take;
        if filled == quantity {
            break;
        }
    }
    Ok((filled, value))
}

#[cfg(test)]
pub fn execute_latency_scenarios(
    scenarios: BTreeMap<LatencyScenario, FixtureExecutionInput>,
) -> Result<BTreeMap<LatencyScenario, ExecutionFill>, IocError> {
    scenarios
        .into_iter()
        .map(|(scenario, mut input)| {
            input.latency_scenario = scenario;
            Ok((scenario, execute_fixture_ioc(&input)?))
        })
        .collect()
}

#[cfg(test)]
fn validate_input(input: &FixtureExecutionInput) -> Result<(), IocError> {
    if input.rounded_quantity <= Decimal::ZERO
        || input.proposed_limit_price <= Decimal::ZERO
        || input.taker_fee_rate < Decimal::ZERO
        || input.decision_market_snapshot.midpoint <= Decimal::ZERO
    {
        return Err(IocError::InvalidInput(
            "quantity, prices, midpoint, and fee rate",
        ));
    }
    validate_book(&input.decision_market_snapshot)?;
    validate_book(&input.evaluation_market_snapshot)?;
    Ok(())
}

fn validate_book(snapshot: &ExecutableBook) -> Result<(), IocError> {
    for levels in [&snapshot.bids, &snapshot.asks] {
        if levels
            .iter()
            .any(|level| level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO)
        {
            return Err(IocError::InvalidBook("non-positive depth"));
        }
    }
    if snapshot
        .bids
        .windows(2)
        .any(|pair| pair[0].price <= pair[1].price)
    {
        return Err(IocError::InvalidBook("bids must be strictly descending"));
    }
    if snapshot
        .asks
        .windows(2)
        .any(|pair| pair[0].price >= pair[1].price)
    {
        return Err(IocError::InvalidBook("asks must be strictly ascending"));
    }
    Ok(())
}

#[cfg(test)]
fn derive_execution_id(input: &FixtureExecutionInput) -> ExecutionFillId {
    let mut hash = Sha256::new();
    hash.update(b"HL1F/SHADOW_EXECUTION/V1");
    hash.update(input.action.planned_cloid.0);
    hash.update(input.decision_market_snapshot.snapshot_id.0);
    hash.update(input.evaluation_market_snapshot.snapshot_id.0);
    hash.update([input.latency_scenario.code()]);
    hash.update(input.configured_latency_ms.to_be_bytes());
    hash.update(
        input
            .proposed_limit_price
            .normalize()
            .mantissa()
            .to_be_bytes(),
    );
    hash.update(input.proposed_limit_price.normalize().scale().to_be_bytes());
    hash.update(input.rounded_quantity.normalize().mantissa().to_be_bytes());
    hash.update(input.rounded_quantity.normalize().scale().to_be_bytes());
    ExecutionFillId(hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::{DecisionId, PlannedCloid, TargetVersion};
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn action(side: Side) -> PlannedAction {
        PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(0),
            asset: "BTC".to_string(),
            side,
            rounded_notional: d("202"),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([2; 16]),
        }
    }

    fn snapshot(id: u8, observed: u64) -> ExecutableBook {
        ExecutableBook {
            snapshot_id: MarketSnapshotId([id; 32]),
            observed_at_mono: observed,
            midpoint: d("100"),
            bids: vec![
                DepthLevel {
                    price: d("99"),
                    quantity: d("1"),
                },
                DepthLevel {
                    price: d("98"),
                    quantity: d("2"),
                },
            ],
            asks: vec![
                DepthLevel {
                    price: d("101"),
                    quantity: d("1"),
                },
                DepthLevel {
                    price: d("102"),
                    quantity: d("2"),
                },
            ],
        }
    }

    fn input(side: Side) -> FixtureExecutionInput {
        FixtureExecutionInput {
            action: action(side),
            decision_timestamp_mono: 1_000,
            decision_market_snapshot: snapshot(3, 1_000),
            evaluation_market_snapshot: snapshot(4, 1_050),
            latency_scenario: LatencyScenario::Expected,
            configured_latency_ms: 50,
            proposed_limit_price: if side == Side::Buy { d("102") } else { d("98") },
            rounded_quantity: d("2.5"),
            taker_fee_rate: d("0.00045"),
            funding_attribution: d("0.01"),
            position_before: Decimal::ZERO,
        }
    }

    #[test]
    fn deterministic_ioc_walks_depth_and_cancels_remainder() {
        let mut input = input(Side::Buy);
        input.rounded_quantity = d("4");
        let execution = execute_fixture_ioc(&input).unwrap();
        assert_eq!(execution.modeled_filled_quantity, d("3"));
        assert_eq!(execution.unfilled_ioc_remainder, d("1"));
        assert_eq!(execution.modeled_filled_notional, d("305"));
        assert_eq!(
            execution.modeled_average_fill_price,
            Some(d("101.66666666666666666666666667"))
        );
        assert_eq!(execution.position_after, d("3"));
        assert_eq!(execution, execute_fixture_ioc(&input).unwrap());
    }

    #[test]
    fn sell_side_fees_funding_and_slippage_are_explicit() {
        let execution = execute_fixture_ioc(&input(Side::Sell)).unwrap();
        assert_eq!(execution.modeled_filled_quantity, d("2.5"));
        assert_eq!(execution.modeled_filled_notional, d("246"));
        assert_eq!(execution.fees, d("0.11070"));
        assert_eq!(execution.funding, d("0.01"));
        assert_eq!(execution.slippage, d("4"));
        assert_eq!(execution.position_after, d("-2.5"));
    }

    #[test]
    fn early_snapshot_and_invalid_book_fail_closed() {
        let mut input = input(Side::Buy);
        input.evaluation_market_snapshot.observed_at_mono = 1_049;
        assert_eq!(execute_fixture_ioc(&input), Err(IocError::SnapshotTooEarly));
        input.evaluation_market_snapshot.observed_at_mono = 1_050;
        input.evaluation_market_snapshot.asks.reverse();
        assert!(matches!(
            execute_fixture_ioc(&input),
            Err(IocError::InvalidBook(_))
        ));
    }

    #[test]
    fn fixed_scenarios_are_all_retained_without_best_case_selection() {
        let base = input(Side::Buy);
        let scenarios = [
            (LatencyScenario::Optimistic, base.clone()),
            (LatencyScenario::Expected, base.clone()),
            (LatencyScenario::Conservative, base),
        ]
        .into_iter()
        .collect();
        let results = execute_latency_scenarios(scenarios).unwrap();
        assert_eq!(results.len(), 3);
        assert_ne!(
            results[&LatencyScenario::Optimistic].execution_id,
            results[&LatencyScenario::Conservative].execution_id
        );
    }

    #[test]
    fn complete_depth_sets_limit_from_worst_level_plus_outward_cushion() {
        let plan = plan_marketable_ioc(
            Side::Buy,
            d("2.5"),
            &snapshot(4, 1_050),
            d("100"),
            d("0.001"),
            d("0.03"),
            d("0.1"),
        )
        .unwrap();
        assert_eq!(
            plan.pricing_mode,
            MarketableIocPricingMode::CompleteVisibleDepth
        );
        assert_eq!(plan.worst_required_depth_price, Some(d("102")));
        assert_eq!(plan.limit_price, d("102.2"));
        assert_eq!(plan.visible_executable_quantity, d("2.5"));
    }

    #[test]
    fn insufficient_depth_uses_capped_reference_fallback() {
        let buy = plan_marketable_ioc(
            Side::Buy,
            d("4"),
            &snapshot(4, 1_050),
            d("100"),
            d("0.001"),
            d("0.005"),
            d("0.1"),
        )
        .unwrap();
        assert_eq!(
            buy.pricing_mode,
            MarketableIocPricingMode::BoundedReferenceFallback
        );
        assert_eq!(buy.limit_price, d("100.5"));
        assert_eq!(buy.visible_executable_quantity, Decimal::ZERO);

        let sell = plan_marketable_ioc(
            Side::Sell,
            d("2"),
            &snapshot(4, 1_050),
            d("100"),
            d("0.001"),
            d("0.03"),
            d("0.1"),
        )
        .unwrap();
        assert_eq!(
            sell.pricing_mode,
            MarketableIocPricingMode::CompleteVisibleDepth
        );
        assert_eq!(sell.worst_required_depth_price, Some(d("98")));
        assert_eq!(sell.limit_price, d("97.9"));
    }

    #[test]
    fn zero_fill_ioc_preserves_the_complete_cancelled_remainder() {
        let mut input = input(Side::Buy);
        input.proposed_limit_price = d("100.5");
        let execution = execute_fixture_ioc(&input).unwrap();
        assert_eq!(execution.modeled_filled_quantity, Decimal::ZERO);
        assert_eq!(execution.unfilled_ioc_remainder, input.rounded_quantity);
        assert_eq!(execution.position_after, input.position_before);
    }
}
