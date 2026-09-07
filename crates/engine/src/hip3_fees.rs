#![forbid(unsafe_code)]

//! Authoritative Hyperliquid perp fee calculation.
//!
//! Mirrors the documented `feeRates` formula (Hyperliquid docs, "Fee formula
//! for developers") using decimal arithmetic consistent with the rest of the
//! engine. Native perps use the account's current user fee tier directly;
//! HIP-3 markets scale it by the DEX `deployerFeeScale`, per-market
//! `growthMode`, the active referral discount, and aligned-quote status.
//!
//! ```typescript
//! scaleIfHip3 = deployerFeeScale < 1 ? deployerFeeScale + 1 : deployerFeeScale * 2
//! deployerShare = deployerFeeScale < 1 ? deployerFeeScale / (1 + deployerFeeScale) : 0.5
//! growthModeScale = growthMode ? 0.1 : 1
//! makerPercentage = makerRate * 100 * growthModeScale
//!   (> 0)  *= scaleIfHip3 * (1 - referral)
//!   (<= 0) *= aligned ? (1 - share) * 1.5 + share : 1
//! takerPercentage = takerRate * 100 * scaleIfHip3 * growthModeScale * (1 - referral)
//!   (aligned) *= (1 - share) * 0.8 + share
//! ```

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Account-level perp fee state from the `userFees` info endpoint.
/// Rates are unit fractions (e.g. `0.00045`); the referral discount is a
/// fraction in `[0, 1)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserPerpFeeState {
    pub taker_rate: Decimal,
    pub maker_rate: Decimal,
    pub active_referral_discount: Decimal,
}

impl UserPerpFeeState {
    pub fn validate(&self) -> bool {
        self.taker_rate >= Decimal::ZERO
            && self.taker_rate <= Decimal::ONE
            && self.maker_rate >= -Decimal::ONE
            && self.maker_rate <= Decimal::ONE
            && self.active_referral_discount >= Decimal::ZERO
            && self.active_referral_discount < Decimal::ONE
    }
}

/// Full context for one perp fee evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerpFeeContext {
    pub user_taker_rate: Decimal,
    pub user_maker_rate: Decimal,
    pub active_referral_discount: Decimal,
    /// DEX deployer fee scale in `[0, 10)`. Ignored for native perps.
    pub deployer_fee_scale: Decimal,
    /// Per-market growth mode (HIP-3 metadata `growthMode == "enabled"`).
    pub growth_mode: bool,
    /// Whether the market uses an aligned quote token (fee discount).
    pub aligned_quote_token: bool,
    /// False for native perps (no deployer scale, no growth mode).
    pub is_hip3: bool,
}

/// Fee rates as percentages (e.g. `0.045` = 0.045%), matching the documented
/// `feeRates` return shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerpFeeRates {
    pub maker_pct: Decimal,
    pub taker_pct: Decimal,
}

/// Authoritative perp fee rates. Returns `None` on invalid inputs or
/// arithmetic overflow (callers treat missing fees as pending, never as a
/// new execution gate).
pub fn perp_fee_rates(context: &PerpFeeContext) -> Option<PerpFeeRates> {
    if context.user_taker_rate < Decimal::ZERO
        || context.user_taker_rate > Decimal::ONE
        || context.user_maker_rate < -Decimal::ONE
        || context.user_maker_rate > Decimal::ONE
        || context.active_referral_discount < Decimal::ZERO
        || context.active_referral_discount >= Decimal::ONE
        || context.deployer_fee_scale < Decimal::ZERO
        || context.deployer_fee_scale >= Decimal::from(10)
    {
        return None;
    }
    let one = Decimal::ONE;
    let hundred = Decimal::from(100);
    let (scale_if_hip3, deployer_share, growth_scale) = if context.is_hip3 {
        let scale_if_hip3 = if context.deployer_fee_scale < one {
            context.deployer_fee_scale.checked_add(one)?
        } else {
            context.deployer_fee_scale.checked_mul(Decimal::from(2))?
        };
        let deployer_share = if context.deployer_fee_scale < one {
            context
                .deployer_fee_scale
                .checked_div(context.deployer_fee_scale.checked_add(one)?)?
        } else {
            Decimal::new(5, 1)
        };
        let growth_scale = if context.growth_mode {
            Decimal::new(1, 1)
        } else {
            one
        };
        (scale_if_hip3, deployer_share, growth_scale)
    } else {
        (one, Decimal::ZERO, one)
    };
    let referral_factor = one.checked_sub(context.active_referral_discount)?;

    let maker_base = context
        .user_maker_rate
        .checked_mul(hundred)?
        .checked_mul(growth_scale)?;
    let maker_pct = if maker_base > Decimal::ZERO {
        maker_base
            .checked_mul(scale_if_hip3)?
            .checked_mul(referral_factor)?
    } else if context.aligned_quote_token {
        // Rebate path: aligned quote scales the rebate by the deployer share.
        let aligned = one
            .checked_sub(deployer_share)?
            .checked_mul(Decimal::new(15, 1))?
            .checked_add(deployer_share)?;
        maker_base.checked_mul(aligned)?
    } else {
        maker_base
    };

    let mut taker_pct = context
        .user_taker_rate
        .checked_mul(hundred)?
        .checked_mul(scale_if_hip3)?
        .checked_mul(growth_scale)?
        .checked_mul(referral_factor)?;
    if context.aligned_quote_token {
        let aligned = one
            .checked_sub(deployer_share)?
            .checked_mul(Decimal::new(8, 1))?
            .checked_add(deployer_share)?;
        taker_pct = taker_pct.checked_mul(aligned)?;
    }
    Some(PerpFeeRates {
        maker_pct,
        taker_pct,
    })
}

/// Taker cost in basis points (percentage × 100).
pub fn taker_fee_bps(context: &PerpFeeContext) -> Option<Decimal> {
    perp_fee_rates(context)?
        .taker_pct
        .checked_mul(Decimal::from(100))
}

/// Maker cost in basis points (percentage × 100, may be negative = rebate).
pub fn maker_fee_bps(context: &PerpFeeContext) -> Option<Decimal> {
    perp_fee_rates(context)?
        .maker_pct
        .checked_mul(Decimal::from(100))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn native_context() -> PerpFeeContext {
        PerpFeeContext {
            user_taker_rate: d("0.00045"),
            user_maker_rate: d("0.00015"),
            active_referral_discount: d("0"),
            deployer_fee_scale: Decimal::ZERO,
            growth_mode: false,
            aligned_quote_token: false,
            is_hip3: false,
        }
    }

    #[test]
    fn native_perp_uses_the_account_fee_tier_directly() {
        let rates = perp_fee_rates(&native_context()).unwrap();
        // 0.00045 * 100 = 0.045% taker, 0.015% maker.
        assert_eq!(rates.taker_pct, d("0.045"));
        assert_eq!(rates.maker_pct, d("0.015"));
        assert_eq!(taker_fee_bps(&native_context()).unwrap(), d("4.5"));
    }

    #[test]
    fn documented_hip3_table_reproduces_exactly() {
        // Documented examples assume a positive normal user fee of 1 unit and
        // non-aligned collateral (x = y = 1). One unit == takerRate 0.01.
        let table = [
            // (growth, scale, expected protocol P, expected deployer D)
            (false, "0", "1", "0"),
            (false, "0.5", "1", "0.5"),
            (false, "1", "1", "1"),
            (false, "3", "3", "3"),
            (true, "0", "0.1", "0"),
            (true, "0.5", "0.1", "0.05"),
            (true, "1", "0.1", "0.1"),
            (true, "3.01", "0.301", "0.301"),
            (true, "9.99", "0.999", "0.999"),
        ];
        for (growth, scale, protocol, deployer) in table {
            let context = PerpFeeContext {
                user_taker_rate: d("0.01"),
                user_maker_rate: d("0.01"),
                active_referral_discount: d("0"),
                deployer_fee_scale: d(scale),
                growth_mode: growth,
                aligned_quote_token: false,
                is_hip3: true,
            };
            let rates = perp_fee_rates(&context).unwrap();
            // Taker percentage with takerRate 0.01 is numerically the total
            // fee in units (P + D).
            let expected_total = d(protocol).checked_add(d(deployer)).unwrap();
            assert_eq!(
                rates.taker_pct, expected_total,
                "growth={growth} scale={scale}"
            );
        }
    }

    #[test]
    fn fee_matrix_covers_scale_growth_referral_and_alignment() {
        let base = PerpFeeContext {
            user_taker_rate: d("0.00045"),
            user_maker_rate: d("0.00015"),
            active_referral_discount: d("0"),
            deployer_fee_scale: d("0.5"),
            growth_mode: false,
            aligned_quote_token: false,
            is_hip3: true,
        };
        // scale < 1: (1 + 0.5) * 0.045% = 0.0675%.
        assert_eq!(perp_fee_rates(&base).unwrap().taker_pct, d("0.0675"));

        // scale = 1: 2x.
        let scale_one = PerpFeeContext {
            deployer_fee_scale: Decimal::ONE,
            ..base
        };
        assert_eq!(perp_fee_rates(&scale_one).unwrap().taker_pct, d("0.09"));

        // scale > 1: 2 * scale.
        let scale_high = PerpFeeContext {
            deployer_fee_scale: d("3"),
            ..base
        };
        assert_eq!(perp_fee_rates(&scale_high).unwrap().taker_pct, d("0.27"));

        // growth mode: additional 0.1x.
        let growth = PerpFeeContext {
            growth_mode: true,
            ..base
        };
        assert_eq!(perp_fee_rates(&growth).unwrap().taker_pct, d("0.00675"));

        // referral discount: (1 - 0.04).
        let referral = PerpFeeContext {
            active_referral_discount: d("0.04"),
            ..base
        };
        assert_eq!(perp_fee_rates(&referral).unwrap().taker_pct, d("0.0648"));

        // aligned quote: share = 0.5/1.5 = 1/3; factor = (1-1/3)*0.8 + 1/3.
        let aligned = PerpFeeContext {
            aligned_quote_token: true,
            ..base
        };
        let unaligned = perp_fee_rates(&base).unwrap().taker_pct;
        let aligned_rate = perp_fee_rates(&aligned).unwrap().taker_pct;
        assert!(aligned_rate < unaligned);
        // (2/3)*0.8 + 1/3 = 0.8666...; 0.0675 * 13/15 = 0.0585.
        assert_eq!(aligned_rate, d("0.0585"));
    }

    #[test]
    fn same_dex_assets_differ_by_growth_mode_and_dexes_by_scale() {
        let nvda = PerpFeeContext {
            user_taker_rate: d("0.00045"),
            user_maker_rate: d("0.00015"),
            active_referral_discount: d("0"),
            deployer_fee_scale: Decimal::ONE,
            growth_mode: true,
            aligned_quote_token: false,
            is_hip3: true,
        };
        let gold = PerpFeeContext {
            growth_mode: false,
            ..nvda
        };
        // xyz:NVDA (growth) vs xyz:GOLD (no growth): 10x apart.
        assert_eq!(taker_fee_bps(&nvda).unwrap(), d("0.9"));
        assert_eq!(taker_fee_bps(&gold).unwrap(), d("9"));
        // xyz (scale 1.0) vs foo (scale 0.5) on the same coin.
        let foo = PerpFeeContext {
            deployer_fee_scale: d("0.5"),
            growth_mode: false,
            ..nvda
        };
        assert_eq!(taker_fee_bps(&foo).unwrap(), d("6.75"));
    }

    #[test]
    fn invalid_inputs_fail_closed_without_panic() {
        let mut bad = native_context();
        bad.active_referral_discount = Decimal::ONE;
        assert!(perp_fee_rates(&bad).is_none());
        let mut bad = native_context();
        bad.deployer_fee_scale = Decimal::from(10);
        bad.is_hip3 = true;
        assert!(perp_fee_rates(&bad).is_none());
    }
}
