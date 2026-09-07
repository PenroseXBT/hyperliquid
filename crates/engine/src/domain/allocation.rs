//! Deterministic sparse target admission for small deployment portfolios.

use crate::domain::execution_floor::AssetExecutionFloor;
use crate::domain::portfolio_risk::Asset;
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct SparseAllocationInput<'a> {
    pub raw_targets: &'a BTreeMap<Asset, Decimal>,
    pub convictions: &'a BTreeMap<Asset, Decimal>,
    pub agreement_weights: &'a BTreeMap<Asset, Decimal>,
    pub execution_floors: &'a BTreeMap<Asset, AssetExecutionFloor>,
    pub filled_positions: &'a BTreeMap<Asset, Decimal>,
    pub current_equity: Decimal,
    pub curve_leverage: Decimal,
    pub global_risk_scale: Decimal,
    pub max_single_asset_equity_pct: Decimal,
    pub max_net_equity_pct: Decimal,
    pub rank_hysteresis: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseAllocation {
    pub admitted_targets: BTreeMap<Asset, Decimal>,
    pub retained_below_minimum: BTreeMap<Asset, Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllocationError {
    InvalidInput(&'static str),
    MissingExecutionFloor,
    ArithmeticOverflow(&'static str),
}

impl Display for AllocationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl Error for AllocationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    asset: Asset,
    desired: Decimal,
    current: Decimal,
    conviction: Decimal,
    agreement_weight: Decimal,
    class: u8,
}

pub fn allocate_sparse_portfolio(
    input: &SparseAllocationInput<'_>,
) -> Result<SparseAllocation, AllocationError> {
    validate(input)?;
    let asset_cap = checked_mul(
        input.current_equity,
        input.max_single_asset_equity_pct,
        "asset cap",
    )?;
    let gross_cap = checked_mul(
        checked_mul(
            input.current_equity,
            input.curve_leverage,
            "leveraged equity",
        )?,
        input.global_risk_scale,
        "gross cap",
    )?;
    let net_cap = checked_mul(input.current_equity, input.max_net_equity_pct, "net cap")?;
    let assets = input
        .raw_targets
        .keys()
        .chain(input.filled_positions.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut admitted = assets
        .iter()
        .map(|asset| {
            (
                asset.clone(),
                input
                    .filled_positions
                    .get(asset)
                    .copied()
                    .unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut candidates = Vec::new();
    let mut retained = BTreeMap::new();
    for asset in assets {
        let current = admitted[&asset];
        let raw = input.raw_targets.get(&asset).copied().unwrap_or_default();
        let desired = raw.clamp(-asset_cap, asset_cap);
        let residual = checked_sub(desired, current, "allocation residual")?;
        if residual.is_zero() {
            continue;
        }
        let class = transition_class(current, desired);
        let floor = input
            .execution_floors
            .get(&asset)
            .ok_or(AllocationError::MissingExecutionFloor)?;
        let required_notional = if current.is_zero() {
            floor.minimum_opening_notional
        } else {
            floor.minimum_order_notional
        };
        if class >= 2 && residual.abs() < required_notional {
            retained.insert(asset, residual);
            continue;
        }
        // A direction flip is normally a deterministic close-first
        // transition. If the destination itself is below the opening floor,
        // splitting the executable residual into two exchange orders would
        // make the second leg invalid. Preserve that narrow reversal as one
        // crossing order: the execution ledger still closes the old
        // attribution before opening the new one.
        let staged_desired = if class == 1
            && !current.is_zero()
            && !desired.is_zero()
            && current.is_sign_positive() != desired.is_sign_positive()
            && desired.abs() >= floor.minimum_opening_notional
        {
            Decimal::ZERO
        } else {
            desired
        };
        candidates.push(Candidate {
            conviction: input.convictions.get(&asset).copied().unwrap_or_default(),
            agreement_weight: input
                .agreement_weights
                .get(&asset)
                .copied()
                .unwrap_or_default(),
            asset,
            desired: staged_desired,
            current,
            class,
        });
    }
    candidates.sort_by(compare_candidates);
    for candidate in candidates {
        let mut proposed = admitted.clone();
        proposed.insert(candidate.asset.clone(), candidate.desired);
        if within_caps(&proposed, asset_cap, gross_cap, net_cap)? {
            admitted = proposed;
        } else if candidate.class <= 1 {
            // Risk-reducing changes must not be suppressed by allocation
            // ranking. A failure here means the starting portfolio is already
            // inconsistent with the declared caps and the HL1B projector must
            // own recovery-only enforcement.
            admitted.insert(candidate.asset, candidate.desired);
        } else if candidate.class == 3 {
            if let Some(victim) = weakest_displaceable_slot(input, &admitted, &candidate) {
                // Rotation is close-first. The stronger candidate remains in
                // the absolute target ledger and is admitted only after the
                // weaker filled slot has actually been reconciled closed.
                admitted.insert(victim, Decimal::ZERO);
            }
        }
    }
    Ok(SparseAllocation {
        admitted_targets: admitted,
        retained_below_minimum: retained,
    })
}

fn transition_class(current: Decimal, desired: Decimal) -> u8 {
    if !current.is_zero() && desired.is_zero() {
        0 // required exit
    } else if !current.is_zero()
        && (current.is_sign_positive() != desired.is_sign_positive()
            || desired.abs() < current.abs())
    {
        1 // reduction or close-first flip
    } else if !current.is_zero() {
        2 // existing-position increase
    } else {
        3 // new position
    }
}

fn compare_candidates(left: &Candidate, right: &Candidate) -> Ordering {
    left.class.cmp(&right.class).then_with(|| {
        right
            .conviction
            .abs()
            .cmp(&left.conviction.abs())
            .then_with(|| right.agreement_weight.cmp(&left.agreement_weight))
            .then_with(|| {
                right
                    .desired
                    .checked_sub(right.current)
                    .unwrap_or_default()
                    .abs()
                    .cmp(
                        &left
                            .desired
                            .checked_sub(left.current)
                            .unwrap_or_default()
                            .abs(),
                    )
            })
            .then_with(|| left.asset.cmp(&right.asset))
    })
}

fn weakest_displaceable_slot(
    input: &SparseAllocationInput<'_>,
    admitted: &BTreeMap<Asset, Decimal>,
    incoming: &Candidate,
) -> Option<Asset> {
    let threshold = incoming
        .conviction
        .abs()
        .checked_sub(input.rank_hysteresis)
        .unwrap_or_default();
    admitted
        .iter()
        .filter(|(asset, target)| {
            !target.is_zero()
                && input
                    .filled_positions
                    .get(*asset)
                    .is_some_and(|filled| !filled.is_zero())
                && input
                    .convictions
                    .get(*asset)
                    .is_some_and(|conviction| conviction.abs() < threshold)
        })
        .min_by(|(left_asset, _), (right_asset, _)| {
            input.convictions[*left_asset]
                .abs()
                .cmp(&input.convictions[*right_asset].abs())
                .then_with(|| {
                    input
                        .agreement_weights
                        .get(*left_asset)
                        .copied()
                        .unwrap_or_default()
                        .cmp(
                            &input
                                .agreement_weights
                                .get(*right_asset)
                                .copied()
                                .unwrap_or_default(),
                        )
                })
                .then_with(|| left_asset.cmp(right_asset))
        })
        .map(|(asset, _)| asset.clone())
}

fn within_caps(
    targets: &BTreeMap<Asset, Decimal>,
    asset_cap: Decimal,
    gross_cap: Decimal,
    net_cap: Decimal,
) -> Result<bool, AllocationError> {
    let mut gross = Decimal::ZERO;
    let mut net = Decimal::ZERO;
    for value in targets.values() {
        if value.abs() > asset_cap {
            return Ok(false);
        }
        gross = gross
            .checked_add(value.abs())
            .ok_or(AllocationError::ArithmeticOverflow("gross"))?;
        net = net
            .checked_add(*value)
            .ok_or(AllocationError::ArithmeticOverflow("net"))?;
    }
    Ok(gross <= gross_cap && net.abs() <= net_cap)
}

fn checked_mul(
    left: Decimal,
    right: Decimal,
    operation: &'static str,
) -> Result<Decimal, AllocationError> {
    left.checked_mul(right)
        .ok_or(AllocationError::ArithmeticOverflow(operation))
}

fn checked_sub(
    left: Decimal,
    right: Decimal,
    operation: &'static str,
) -> Result<Decimal, AllocationError> {
    left.checked_sub(right)
        .ok_or(AllocationError::ArithmeticOverflow(operation))
}

fn validate(input: &SparseAllocationInput<'_>) -> Result<(), AllocationError> {
    if input.current_equity <= Decimal::ZERO
        || input.curve_leverage <= Decimal::ZERO
        || input.global_risk_scale <= Decimal::ZERO
        || input.rank_hysteresis < Decimal::ZERO
    {
        return Err(AllocationError::InvalidInput(
            "non-positive allocation input",
        ));
    }
    if input
        .raw_targets
        .keys()
        .chain(input.filled_positions.keys())
        .any(|asset| !input.execution_floors.contains_key(asset))
    {
        return Err(AllocationError::MissingExecutionFloor);
    }
    if !(Decimal::ZERO..=Decimal::ONE).contains(&input.max_single_asset_equity_pct)
        || !(Decimal::ZERO..=Decimal::ONE).contains(&input.max_net_equity_pct)
    {
        return Err(AllocationError::InvalidInput("invalid risk percentage"));
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
    fn input<'a>(
        raw: &'a BTreeMap<String, Decimal>,
        convictions: &'a BTreeMap<String, Decimal>,
        filled: &'a BTreeMap<String, Decimal>,
        agreement_weights: &'a BTreeMap<String, Decimal>,
        floors: &'a BTreeMap<String, AssetExecutionFloor>,
    ) -> SparseAllocationInput<'a> {
        SparseAllocationInput {
            raw_targets: raw,
            convictions,
            agreement_weights,
            execution_floors: floors,
            filled_positions: filled,
            current_equity: d("100"),
            curve_leverage: d("8"),
            global_risk_scale: d("0.1"),
            max_single_asset_equity_pct: d("0.65"),
            max_net_equity_pct: d("0.65"),
            rank_hysteresis: d("0.02"),
        }
    }

    fn floors(assets: impl IntoIterator<Item = String>) -> BTreeMap<String, AssetExecutionFloor> {
        assets
            .into_iter()
            .map(|asset| {
                (
                    asset,
                    AssetExecutionFloor {
                        bounded_ioc_price: d("100"),
                        minimum_quantity: d("0.102"),
                        minimum_order_notional: d("10.2"),
                        minimum_opening_notional: d("10.7"),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn admits_sparse_portfolio_by_conviction_without_redistribution() {
        let raw: BTreeMap<String, Decimal> = (0..8).map(|n| (format!("A{n}"), d("12"))).collect();
        let convictions = (0..8)
            .map(|n| (format!("A{n}"), Decimal::from(8 - n)))
            .collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &convictions,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(
            result
                .admitted_targets
                .values()
                .filter(|v| !v.is_zero())
                .count(),
            5
        ); // signed net cap $65
        assert_eq!(result.admitted_targets["A0"], d("12"));
        assert_eq!(result.admitted_targets["A5"], Decimal::ZERO);
    }

    #[test]
    fn subminimum_target_is_retained_not_admitted() {
        let raw: BTreeMap<String, Decimal> = [("ETH".into(), d("9.5"))].into_iter().collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(result.admitted_targets["ETH"], Decimal::ZERO);
        assert_eq!(result.retained_below_minimum["ETH"], d("9.5"));
    }

    #[test]
    fn exit_precedes_new_exposure() {
        let raw: BTreeMap<String, Decimal> =
            [("OLD".into(), Decimal::ZERO), ("NEW".into(), d("60"))]
                .into_iter()
                .collect();
        let filled = [("OLD".into(), d("60"))].into_iter().collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &BTreeMap::new(),
            &filled,
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(result.admitted_targets["OLD"], Decimal::ZERO);
        assert_eq!(result.admitted_targets["NEW"], d("60"));
    }

    #[test]
    fn direction_flip_is_close_first() {
        let raw: BTreeMap<String, Decimal> = [("ETH".into(), d("-20"))].into_iter().collect();
        let filled = [("ETH".into(), d("20"))].into_iter().collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &BTreeMap::new(),
            &filled,
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(result.admitted_targets["ETH"], Decimal::ZERO);
    }

    #[test]
    fn executable_reversal_delta_is_not_lost_to_subminimum_destination() {
        let raw: BTreeMap<String, Decimal> = [("PENDLE".into(), d("6.34"))].into_iter().collect();
        let filled = [("PENDLE".into(), d("-30.61"))].into_iter().collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &BTreeMap::new(),
            &filled,
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(result.admitted_targets["PENDLE"], d("6.34"));
        assert!(result.retained_below_minimum.is_empty());
        assert_eq!(
            result.admitted_targets["PENDLE"] - filled["PENDLE"],
            d("36.95")
        );
    }

    #[test]
    fn map_order_cannot_change_admission() {
        let raw_a: BTreeMap<String, Decimal> = [("B".into(), d("40")), ("A".into(), d("40"))]
            .into_iter()
            .collect();
        let raw_b: BTreeMap<String, Decimal> = [("A".into(), d("40")), ("B".into(), d("40"))]
            .into_iter()
            .collect();
        let convictions = [("A".into(), d("1")), ("B".into(), d("1"))]
            .into_iter()
            .collect();
        let floors = floors(raw_a.keys().cloned());
        assert_eq!(
            allocate_sparse_portfolio(&input(
                &raw_a,
                &convictions,
                &BTreeMap::new(),
                &BTreeMap::new(),
                &floors
            ))
            .unwrap(),
            allocate_sparse_portfolio(&input(
                &raw_b,
                &convictions,
                &BTreeMap::new(),
                &BTreeMap::new(),
                &floors
            ))
            .unwrap()
        );
    }

    #[test]
    fn agreeing_source_weight_breaks_equal_conviction_ties() {
        let raw: BTreeMap<String, Decimal> = [("A".into(), d("40")), ("B".into(), d("40"))]
            .into_iter()
            .collect();
        let convictions = [("A".into(), d("0.4")), ("B".into(), d("0.4"))]
            .into_iter()
            .collect();
        let agreement = [("A".into(), d("1")), ("B".into(), d("3"))]
            .into_iter()
            .collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &convictions,
            &BTreeMap::new(),
            &agreement,
            &floors,
        ))
        .unwrap();
        assert_eq!(result.admitted_targets["B"], d("40"));
        assert_eq!(result.admitted_targets["A"], Decimal::ZERO);
    }

    #[test]
    fn stronger_new_signal_stages_weaker_slot_exit_before_entry() {
        let raw: BTreeMap<String, Decimal> = [("OLD".into(), d("60")), ("NEW".into(), d("60"))]
            .into_iter()
            .collect();
        let filled = [("OLD".into(), d("60"))].into_iter().collect();
        let convictions = [("OLD".into(), d("0.2")), ("NEW".into(), d("0.8"))]
            .into_iter()
            .collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &convictions,
            &filled,
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(result.admitted_targets["OLD"], Decimal::ZERO);
        assert_eq!(result.admitted_targets["NEW"], Decimal::ZERO);
    }

    #[test]
    fn eighty_dollar_envelope_supports_bounded_micro_slots() {
        let raw: BTreeMap<String, Decimal> = (0..7)
            .map(|n| {
                let sign = if n % 2 == 0 {
                    Decimal::ONE
                } else {
                    -Decimal::ONE
                };
                (format!("A{n}"), d("10.7") * sign)
            })
            .collect();
        let convictions = raw
            .keys()
            .cloned()
            .map(|asset| (asset, Decimal::ONE))
            .collect();
        let floors = floors(raw.keys().cloned());
        let result = allocate_sparse_portfolio(&input(
            &raw,
            &convictions,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &floors,
        ))
        .unwrap();
        assert_eq!(
            result
                .admitted_targets
                .values()
                .filter(|target| !target.is_zero())
                .count(),
            7
        );
        assert!(
            result
                .admitted_targets
                .values()
                .map(|target| target.abs())
                .sum::<Decimal>()
                <= d("80")
        );
    }

    #[test]
    fn genuine_absolute_target_progression_creates_one_micro_admission() {
        let mut admissions = 0;
        for desired in ["6", "9", "11.2"] {
            let raw: BTreeMap<String, Decimal> = [("ETH".into(), d(desired))].into_iter().collect();
            let convictions = [("ETH".into(), d("0.5"))].into_iter().collect();
            let floors = floors(raw.keys().cloned());
            let result = allocate_sparse_portfolio(&input(
                &raw,
                &convictions,
                &BTreeMap::new(),
                &BTreeMap::new(),
                &floors,
            ))
            .unwrap();
            admissions += usize::from(!result.admitted_targets["ETH"].is_zero());
        }
        assert_eq!(admissions, 1);
    }
}
