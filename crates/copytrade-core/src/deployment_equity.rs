//! Realized-equity compounding with asymmetric treatment of open PnL.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentEquity {
    pub starting_equity: Decimal,
    pub current_equity: Decimal,
    pub settled_equity: Decimal,
    pub deployment_equity: Decimal,
    pub realized_net_pnl: Decimal,
    pub unrealized_net_pnl: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentEquityError {
    NonPositiveEquity,
    ArithmeticOverflow,
}

impl Display for DeploymentEquityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl Error for DeploymentEquityError {}

pub fn calculate_deployment_equity(
    starting_equity: Decimal,
    current_equity: Decimal,
    realized_net_pnl: Decimal,
) -> Result<DeploymentEquity, DeploymentEquityError> {
    let settled_equity = starting_equity
        .checked_add(realized_net_pnl)
        .ok_or(DeploymentEquityError::ArithmeticOverflow)?;
    if starting_equity <= Decimal::ZERO
        || current_equity <= Decimal::ZERO
        || settled_equity <= Decimal::ZERO
    {
        return Err(DeploymentEquityError::NonPositiveEquity);
    }
    let unrealized_net_pnl = current_equity
        .checked_sub(settled_equity)
        .ok_or(DeploymentEquityError::ArithmeticOverflow)?;
    Ok(DeploymentEquity {
        starting_equity,
        current_equity,
        settled_equity,
        deployment_equity: current_equity.min(settled_equity),
        realized_net_pnl,
        unrealized_net_pnl,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn realized_profit_increases_deployment_capacity() {
        let state = calculate_deployment_equity(d("100"), d("110"), d("10")).unwrap();
        assert_eq!(state.settled_equity, d("110"));
        assert_eq!(state.deployment_equity, d("110"));
    }

    #[test]
    fn open_profit_does_not_compound() {
        let state = calculate_deployment_equity(d("100"), d("110"), Decimal::ZERO).unwrap();
        assert_eq!(state.deployment_equity, d("100"));
        assert_eq!(state.unrealized_net_pnl, d("10"));
    }

    #[test]
    fn unrealized_and_realized_losses_tighten_immediately() {
        assert_eq!(
            calculate_deployment_equity(d("100"), d("92"), Decimal::ZERO)
                .unwrap()
                .deployment_equity,
            d("92")
        );
        assert_eq!(
            calculate_deployment_equity(d("100"), d("94"), d("-6"))
                .unwrap()
                .deployment_equity,
            d("94")
        );
    }
}
