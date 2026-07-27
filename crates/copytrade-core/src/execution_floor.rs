//! Exchange-valid, per-asset notional floors for micro-capital execution.

use crate::decision::Side;
use crate::portfolio_risk::MarketRules;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionFloorPolicy {
    pub exchange_minimum_notional: Decimal,
    pub rounding_buffer: Decimal,
    pub closeability_margin: Decimal,
    pub maximum_slippage_fraction: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetExecutionFloor {
    pub bounded_ioc_price: Decimal,
    pub minimum_quantity: Decimal,
    pub minimum_order_notional: Decimal,
    pub minimum_opening_notional: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionFloorError {
    InvalidInput(&'static str),
    ArithmeticOverflow(&'static str),
}

impl Display for ExecutionFloorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl Error for ExecutionFloorError {}

pub fn execution_floor_for_asset(
    side: Side,
    rules: &MarketRules,
    policy: ExecutionFloorPolicy,
) -> Result<AssetExecutionFloor, ExecutionFloorError> {
    validate(rules, policy)?;
    let raw_ioc_price = match side {
        Side::Buy => rules
            .mark_price
            .checked_mul(Decimal::ONE + policy.maximum_slippage_fraction),
        Side::Sell => rules
            .mark_price
            .checked_mul(Decimal::ONE - policy.maximum_slippage_fraction),
    }
    .ok_or(ExecutionFloorError::ArithmeticOverflow("bounded IOC price"))?;
    let bounded_ioc_price = round_price_outward(raw_ioc_price, rules.price_tick, side)?;
    let required_order = policy
        .exchange_minimum_notional
        .checked_add(policy.rounding_buffer)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("order buffer"))?;
    let required_opening = required_order
        .checked_add(policy.closeability_margin)
        .ok_or(ExecutionFloorError::ArithmeticOverflow(
            "closeability margin",
        ))?;
    let minimum_quantity =
        quantity_for_notional(required_order, bounded_ioc_price, rules.size_step)?;
    let opening_quantity =
        quantity_for_notional(required_opening, bounded_ioc_price, rules.size_step)?;
    Ok(AssetExecutionFloor {
        bounded_ioc_price,
        minimum_quantity,
        minimum_order_notional: minimum_quantity
            .checked_mul(bounded_ioc_price)
            .ok_or(ExecutionFloorError::ArithmeticOverflow("minimum notional"))?,
        minimum_opening_notional: opening_quantity
            .checked_mul(bounded_ioc_price)
            .ok_or(ExecutionFloorError::ArithmeticOverflow("opening notional"))?,
    })
}

pub fn validates_rounded_order(
    quantity: Decimal,
    ioc_price: Decimal,
    exchange_minimum_notional: Decimal,
) -> Result<bool, ExecutionFloorError> {
    if quantity < Decimal::ZERO || ioc_price <= Decimal::ZERO {
        return Err(ExecutionFloorError::InvalidInput("rounded order"));
    }
    Ok(quantity
        .checked_mul(ioc_price)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("rounded notional"))?
        >= exchange_minimum_notional)
}

fn quantity_for_notional(
    notional: Decimal,
    price: Decimal,
    size_step: Decimal,
) -> Result<Decimal, ExecutionFloorError> {
    let raw_quantity = notional
        .checked_div(price)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("minimum quantity"))?;
    let units = raw_quantity
        .checked_div(size_step)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("lot units"))?
        .ceil();
    units
        .checked_mul(size_step)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("rounded quantity"))
}

fn round_price_outward(
    price: Decimal,
    tick: Decimal,
    side: Side,
) -> Result<Decimal, ExecutionFloorError> {
    let units = price
        .checked_div(tick)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("price units"))?;
    let rounded = match side {
        Side::Buy => units.ceil(),
        Side::Sell => units.floor(),
    };
    rounded
        .checked_mul(tick)
        .ok_or(ExecutionFloorError::ArithmeticOverflow("rounded price"))
}

fn validate(rules: &MarketRules, policy: ExecutionFloorPolicy) -> Result<(), ExecutionFloorError> {
    if rules.mark_price <= Decimal::ZERO
        || rules.price_tick <= Decimal::ZERO
        || rules.size_step <= Decimal::ZERO
        || policy.exchange_minimum_notional < Decimal::from(10_u32)
        || policy.rounding_buffer <= Decimal::ZERO
        || policy.closeability_margin < Decimal::ZERO
        || !(Decimal::ZERO..Decimal::ONE).contains(&policy.maximum_slippage_fraction)
    {
        return Err(ExecutionFloorError::InvalidInput("execution floor policy"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }
    fn rules() -> MarketRules {
        MarketRules {
            mark_price: d("101.23"),
            price_tick: d("0.01"),
            size_step: d("0.001"),
        }
    }
    fn policy() -> ExecutionFloorPolicy {
        ExecutionFloorPolicy {
            exchange_minimum_notional: d("10"),
            rounding_buffer: d("0.20"),
            closeability_margin: d("0.50"),
            maximum_slippage_fraction: d("0.0004"),
        }
    }

    #[test]
    fn floor_is_dynamic_and_exchange_valid_after_outward_rounding() {
        let buy = execution_floor_for_asset(Side::Buy, &rules(), policy()).unwrap();
        let sell = execution_floor_for_asset(Side::Sell, &rules(), policy()).unwrap();
        assert!(buy.minimum_order_notional >= d("10.20"));
        assert!(sell.minimum_order_notional >= d("10.20"));
        assert!(buy.minimum_opening_notional >= buy.minimum_order_notional + d("0.50"));
        assert!(
            validates_rounded_order(buy.minimum_quantity, buy.bounded_ioc_price, d("10")).unwrap()
        );
    }

    #[test]
    fn coarse_lot_size_raises_asset_specific_floor() {
        let coarse = MarketRules {
            mark_price: d("40000"),
            price_tick: d("1"),
            size_step: d("0.001"),
        };
        let floor = execution_floor_for_asset(Side::Buy, &coarse, policy()).unwrap();
        assert!(floor.minimum_order_notional >= d("40"));
    }

    #[test]
    fn exact_ten_without_buffer_is_rejected() {
        let mut invalid = policy();
        invalid.rounding_buffer = Decimal::ZERO;
        assert!(execution_floor_for_asset(Side::Buy, &rules(), invalid).is_err());
    }
}
