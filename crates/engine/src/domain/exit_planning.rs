use crate::domain::decision::Side;
use crate::domain::execution_floor::{
    execution_floor_for_asset, AssetExecutionFloor, ExecutionFloorPolicy,
};
use crate::domain::ioc::{plan_marketable_ioc, ExecutableBook, MarketableIocPlan};
use crate::domain::portfolio_risk::{Asset, MarketRules};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct ExitPlanningInput<'a> {
    pub asset: &'a Asset,
    pub desired_target_notional: Decimal,
    pub filled_notional: Decimal,
    pub filled_quantity: Decimal,
    pub acknowledged_open_notional: Decimal,
    pub unknown_result_notional: Decimal,
    pub continuation_notional: Decimal,
    pub reference_price: Decimal,
    pub market_rules: &'a MarketRules,
    pub market_snapshot: &'a ExecutableBook,
    pub execution_floor_policy: ExecutionFloorPolicy,
    pub execution_cushion: Decimal,
    pub maximum_slippage: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidualClass {
    FullExit,
    RiskReduction,
    DirectionFlipCloseLeg,
    ExposureIncrease,
    AlreadySatisfied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitPlan {
    pub asset: Asset,
    pub desired_target_notional: Decimal,
    pub committed_notional: Decimal,
    pub signed_residual_notional: Decimal,
    pub residual_class: ResidualClass,
    pub side: Side,
    pub reduce_only: bool,
    pub quantity: Decimal,
    pub planned_notional: Decimal,
    pub execution_floor: AssetExecutionFloor,
    pub ioc: MarketableIocPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitPlanningBlock {
    InvalidInput(&'static str),
    ArithmeticOverflow(&'static str),
    AlreadySatisfied,
    ExposureIncreasing,
    OutstandingExposureRequiresReconciliation,
    BelowExchangeMinimum {
        residual_notional: Decimal,
        required_notional: Decimal,
    },
    Pricing(String),
}

pub fn plan_risk_reducing_ioc(
    input: &ExitPlanningInput<'_>,
) -> Result<ExitPlan, ExitPlanningBlock> {
    validate(input)?;
    let committed = input
        .filled_notional
        .checked_add(input.acknowledged_open_notional)
        .and_then(|value| value.checked_add(input.unknown_result_notional))
        .and_then(|value| value.checked_add(input.continuation_notional))
        .ok_or(ExitPlanningBlock::ArithmeticOverflow("committed exposure"))?;
    let residual = input
        .desired_target_notional
        .checked_sub(committed)
        .ok_or(ExitPlanningBlock::ArithmeticOverflow("signed residual"))?;
    let class = classify(committed, input.desired_target_notional);
    if class == ResidualClass::AlreadySatisfied {
        return Err(ExitPlanningBlock::AlreadySatisfied);
    }
    if class == ResidualClass::ExposureIncrease {
        return Err(ExitPlanningBlock::ExposureIncreasing);
    }
    if !input.acknowledged_open_notional.is_zero()
        || !input.unknown_result_notional.is_zero()
        || !input.continuation_notional.is_zero()
    {
        return Err(ExitPlanningBlock::OutstandingExposureRequiresReconciliation);
    }
    let side = if input.filled_notional > Decimal::ZERO {
        Side::Sell
    } else {
        Side::Buy
    };
    let intended_notional = match class {
        ResidualClass::FullExit | ResidualClass::DirectionFlipCloseLeg => {
            input.filled_notional.abs()
        }
        ResidualClass::RiskReduction => residual.abs().min(input.filled_notional.abs()),
        _ => return Err(ExitPlanningBlock::ExposureIncreasing),
    };
    let raw_quantity = match class {
        ResidualClass::FullExit | ResidualClass::DirectionFlipCloseLeg => {
            input.filled_quantity.abs()
        }
        ResidualClass::RiskReduction => {
            let destination_quantity = input
                .desired_target_notional
                .abs()
                .checked_div(input.reference_price)
                .ok_or(ExitPlanningBlock::ArithmeticOverflow(
                    "destination quantity",
                ))?;
            input
                .filled_quantity
                .abs()
                .checked_sub(destination_quantity)
                .ok_or(ExitPlanningBlock::ArithmeticOverflow("reduction quantity"))?
                .max(Decimal::ZERO)
        }
        _ => return Err(ExitPlanningBlock::ExposureIncreasing),
    };
    let quantity =
        round_down(raw_quantity, input.market_rules.size_step)?.min(input.filled_quantity.abs());
    let floor = execution_floor_for_asset(side, input.market_rules, input.execution_floor_policy)
        .map_err(|error| ExitPlanningBlock::Pricing(format!("{error:?}")))?;
    let planned_notional = quantity
        .checked_mul(floor.bounded_ioc_price)
        .ok_or(ExitPlanningBlock::ArithmeticOverflow("exit notional"))?;
    if quantity.is_zero() || planned_notional < floor.minimum_order_notional {
        return Err(ExitPlanningBlock::BelowExchangeMinimum {
            residual_notional: intended_notional,
            required_notional: floor.minimum_order_notional,
        });
    }
    let ioc = plan_marketable_ioc(
        side,
        quantity,
        input.market_snapshot,
        input.reference_price,
        input.execution_cushion,
        input.maximum_slippage,
        input.market_rules.price_tick,
    )
    .map_err(|error| ExitPlanningBlock::Pricing(format!("{error:?}")))?;
    Ok(ExitPlan {
        asset: input.asset.clone(),
        desired_target_notional: input.desired_target_notional,
        committed_notional: committed,
        signed_residual_notional: residual,
        residual_class: class,
        side,
        reduce_only: true,
        quantity,
        planned_notional,
        execution_floor: floor,
        ioc,
    })
}

fn classify(committed: Decimal, desired: Decimal) -> ResidualClass {
    if committed == desired {
        ResidualClass::AlreadySatisfied
    } else if committed.is_zero() {
        ResidualClass::ExposureIncrease
    } else if desired.is_zero() {
        ResidualClass::FullExit
    } else if committed.is_sign_positive() != desired.is_sign_positive() {
        ResidualClass::DirectionFlipCloseLeg
    } else if desired.abs() < committed.abs() {
        ResidualClass::RiskReduction
    } else {
        ResidualClass::ExposureIncrease
    }
}

fn round_down(value: Decimal, step: Decimal) -> Result<Decimal, ExitPlanningBlock> {
    value
        .checked_div(step)
        .and_then(|units| units.floor().checked_mul(step))
        .ok_or(ExitPlanningBlock::ArithmeticOverflow("quantity rounding"))
}

fn validate(input: &ExitPlanningInput<'_>) -> Result<(), ExitPlanningBlock> {
    if input.asset.is_empty()
        || input.reference_price <= Decimal::ZERO
        || input.market_rules.size_step <= Decimal::ZERO
        || input.filled_notional.is_zero() != input.filled_quantity.is_zero()
    {
        return Err(ExitPlanningBlock::InvalidInput("exit planning input"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::MarketSnapshotId;
    use crate::domain::ioc::DepthLevel;

    fn snapshot() -> ExecutableBook {
        ExecutableBook {
            snapshot_id: MarketSnapshotId([1; 32]),
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
        }
    }

    fn rules() -> MarketRules {
        MarketRules {
            mark_price: Decimal::from(100),
            price_tick: Decimal::new(1, 1),
            size_step: Decimal::new(1, 3),
        }
    }

    fn policy() -> ExecutionFloorPolicy {
        ExecutionFloorPolicy {
            exchange_minimum_notional: Decimal::from(10),
            rounding_buffer: Decimal::new(2, 1),
            closeability_margin: Decimal::new(5, 1),
            maximum_slippage_fraction: Decimal::new(1, 2),
        }
    }

    fn input(desired: Decimal) -> ExitPlanningInput<'static> {
        let rules = Box::leak(Box::new(rules()));
        let snapshot = Box::leak(Box::new(snapshot()));
        ExitPlanningInput {
            asset: Box::leak(Box::new("BTC".to_string())),
            desired_target_notional: desired,
            filled_notional: Decimal::from(20),
            filled_quantity: Decimal::new(2, 1),
            acknowledged_open_notional: Decimal::ZERO,
            unknown_result_notional: Decimal::ZERO,
            continuation_notional: Decimal::ZERO,
            reference_price: Decimal::from(100),
            market_rules: rules,
            market_snapshot: snapshot,
            execution_floor_policy: policy(),
            execution_cushion: Decimal::new(1, 3),
            maximum_slippage: Decimal::new(1, 2),
        }
    }

    #[test]
    fn direction_flip_plans_only_reduce_only_close_leg() {
        let plan = plan_risk_reducing_ioc(&input(Decimal::from(-10))).unwrap();
        assert_eq!(plan.residual_class, ResidualClass::DirectionFlipCloseLeg);
        assert_eq!(plan.side, Side::Sell);
        assert!(plan.reduce_only);
        assert_eq!(plan.quantity, Decimal::new(2, 1));
    }

    #[test]
    fn reductions_are_quantity_based_for_both_sides() {
        let cases = [
            (
                Decimal::from(20),
                Decimal::new(2, 1),
                Decimal::ZERO,
                Side::Sell,
                Decimal::new(2, 1),
            ),
            (
                Decimal::from(-20),
                Decimal::new(-2, 1),
                Decimal::ZERO,
                Side::Buy,
                Decimal::new(2, 1),
            ),
            (
                Decimal::from(40),
                Decimal::new(4, 1),
                Decimal::from(20),
                Side::Sell,
                Decimal::new(2, 1),
            ),
            (
                Decimal::from(-40),
                Decimal::new(-4, 1),
                Decimal::from(-20),
                Side::Buy,
                Decimal::new(2, 1),
            ),
        ];
        for (filled_notional, filled_quantity, desired, side, expected_quantity) in cases {
            let mut value = input(desired);
            value.filled_notional = filled_notional;
            value.filled_quantity = filled_quantity;
            let plan = plan_risk_reducing_ioc(&value).unwrap();
            assert!(plan.reduce_only);
            assert_eq!(plan.side, side);
            assert_eq!(plan.quantity, expected_quantity);
            assert!(plan.quantity <= filled_quantity.abs());
        }
    }

    #[test]
    fn size_rounding_cannot_overshoot_a_partial_reduction() {
        let mut value = input(Decimal::from(5));
        value.filled_notional = Decimal::new(205, 1);
        value.filled_quantity = Decimal::new(205, 3);
        value.market_rules = Box::leak(Box::new(MarketRules {
            size_step: Decimal::new(1, 2),
            ..rules()
        }));
        let plan = plan_risk_reducing_ioc(&value).unwrap();
        assert_eq!(plan.quantity, Decimal::new(15, 2));
        assert!(plan.quantity <= value.filled_quantity.abs());
        assert!(value.filled_quantity - plan.quantity > Decimal::ZERO);
    }

    #[test]
    fn outstanding_unknown_blocks_another_exit_order() {
        let mut value = input(Decimal::ZERO);
        value.unknown_result_notional = Decimal::from(-10);
        assert_eq!(
            plan_risk_reducing_ioc(&value),
            Err(ExitPlanningBlock::OutstandingExposureRequiresReconciliation)
        );
    }

    #[test]
    fn exchange_dust_is_retained_and_never_submitted() {
        let mut value = input(Decimal::ZERO);
        value.filled_notional = Decimal::from(5);
        value.filled_quantity = Decimal::new(5, 2);
        assert!(matches!(
            plan_risk_reducing_ioc(&value),
            Err(ExitPlanningBlock::BelowExchangeMinimum { .. })
        ));
    }

    #[test]
    fn flatten_quantity_is_committed_quantity_despite_price_drift() {
        let rules = Box::leak(Box::new(MarketRules {
            mark_price: Decimal::new(1464, 4),
            price_tick: Decimal::new(1, 4),
            size_step: Decimal::new(1, 2),
        }));
        let snapshot = Box::leak(Box::new(ExecutableBook {
            snapshot_id: MarketSnapshotId([2; 32]),
            observed_at_mono: 1,
            midpoint: Decimal::new(1464, 4),
            bids: vec![DepthLevel {
                price: Decimal::new(1463, 4),
                quantity: Decimal::from(100),
            }],
            asks: vec![],
        }));
        let plan = plan_risk_reducing_ioc(&ExitPlanningInput {
            asset: Box::leak(Box::new("ACE".to_string())),
            desired_target_notional: Decimal::ZERO,
            filled_notional: Decimal::new(1_302_667_2, 6),
            filled_quantity: Decimal::new(8_898, 2),
            acknowledged_open_notional: Decimal::ZERO,
            unknown_result_notional: Decimal::ZERO,
            continuation_notional: Decimal::ZERO,
            reference_price: Decimal::new(1464, 4),
            market_rules: rules,
            market_snapshot: snapshot,
            execution_floor_policy: policy(),
            execution_cushion: Decimal::new(1, 3),
            maximum_slippage: Decimal::new(1, 2),
        })
        .unwrap();

        assert!(plan.reduce_only);
        assert_eq!(plan.quantity, Decimal::new(8_898, 2));
    }
}
