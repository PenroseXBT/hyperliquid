//! Deterministic `very_profitable` cohort discovery and source-signal logic.
//!
//! Hyperdash is deliberately confined to snapshot discovery and contextual
//! inputs. Wallet positions and quality observations supplied to this module
//! are expected to have been rehydrated from Hyperliquid.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};

pub const VERY_PROFITABLE_COHORT_ID: &str = "very_profitable";
pub const VERY_PROFITABLE_COHORT_URL: &str =
    "https://hyperdash.com/explore/cohorts/very_profitable";
pub const VERY_PROFITABLE_MIN_ALL_TIME_PNL_USD: i64 = 100_000;
pub const VERY_PROFITABLE_MAX_ALL_TIME_PNL_USD: i64 = 1_000_000;
pub const COHORT_ACTIVATION_SCORE: Decimal = Decimal::from_parts(35, 0, 0, false, 2);
pub const COHORT_CHASE_LIMIT_R: Decimal = Decimal::from_parts(15, 0, 0, false, 2);
pub const HIGH_DENSITY_RETAINED_FILL_COUNT: usize = 10_000;

const FIVE_MINUTES_MS: u64 = 5 * 60 * 1_000;
const FIFTEEN_MINUTES_MS: u64 = 15 * 60 * 1_000;
const ONE_HOUR_MS: u64 = 60 * 60 * 1_000;
const HISTORY_RETENTION_MS: u64 = 2 * ONE_HOUR_MS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CohortError {
    InvalidMembership(String),
    InvalidQualityPolicy,
    InvalidQualityObservation(String),
    InvalidWalletState(String),
    InvalidSignalInput(String),
    NonMonotonicSnapshot,
    ConflictingSnapshot,
    Arithmetic,
}

impl Display for CohortError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CohortError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperdashMembershipSnapshot {
    pub schema_version: u32,
    pub source: String,
    pub cohort_id: String,
    pub observed_at_ms: u64,
    pub displayed_min_all_time_pnl_usd: Decimal,
    pub displayed_max_all_time_pnl_usd: Decimal,
    pub completeness: MembershipCompleteness,
    pub reported_member_count: usize,
    pub captured_member_count: usize,
    pub wallets: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipCompleteness {
    Complete,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortMembershipResolution {
    pub snapshot_timestamp_ms: u64,
    pub membership_set_hash: String,
    pub completeness: MembershipCompleteness,
    pub reported_member_count: usize,
    pub captured_member_count: usize,
    pub wallet_count_before_filtering: usize,
    pub unique_wallet_count: usize,
    pub overlap_with_existing: usize,
    pub all_members: BTreeSet<String>,
    pub overlapping_members: BTreeSet<String>,
    pub cohort_only_members: BTreeSet<String>,
}

pub fn resolve_very_profitable_membership(
    snapshot: &HyperdashMembershipSnapshot,
    existing_addresses: impl IntoIterator<Item = String>,
) -> Result<CohortMembershipResolution, CohortError> {
    validate_membership_snapshot(snapshot)?;
    let all_members = snapshot
        .wallets
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let existing = existing_addresses
        .into_iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let overlapping_members = all_members
        .intersection(&existing)
        .cloned()
        .collect::<BTreeSet<_>>();
    let cohort_only_members = all_members
        .difference(&existing)
        .cloned()
        .collect::<BTreeSet<_>>();
    Ok(CohortMembershipResolution {
        snapshot_timestamp_ms: snapshot.observed_at_ms,
        membership_set_hash: membership_hash(&all_members),
        completeness: snapshot.completeness,
        reported_member_count: snapshot.reported_member_count,
        captured_member_count: snapshot.captured_member_count,
        wallet_count_before_filtering: snapshot.wallets.len(),
        unique_wallet_count: all_members.len(),
        overlap_with_existing: overlapping_members.len(),
        all_members,
        overlapping_members,
        cohort_only_members,
    })
}

fn validate_membership_snapshot(snapshot: &HyperdashMembershipSnapshot) -> Result<(), CohortError> {
    if snapshot.schema_version != 1
        || snapshot.source != VERY_PROFITABLE_COHORT_URL
        || snapshot.cohort_id != VERY_PROFITABLE_COHORT_ID
        || snapshot.observed_at_ms == 0
        || snapshot.wallets.is_empty()
        || snapshot.captured_member_count != snapshot.wallets.len()
        || snapshot.reported_member_count < snapshot.captured_member_count
        || match snapshot.completeness {
            MembershipCompleteness::Complete => {
                snapshot.reported_member_count != snapshot.captured_member_count
            }
            MembershipCompleteness::Partial => {
                snapshot.reported_member_count <= snapshot.captured_member_count
            }
        }
        || snapshot.displayed_min_all_time_pnl_usd
            != Decimal::from(VERY_PROFITABLE_MIN_ALL_TIME_PNL_USD)
        || snapshot.displayed_max_all_time_pnl_usd
            != Decimal::from(VERY_PROFITABLE_MAX_ALL_TIME_PNL_USD)
        || snapshot
            .wallets
            .iter()
            .any(|address| !valid_address(address))
    {
        return Err(CohortError::InvalidMembership(
            "invalid or incomplete Hyperdash very_profitable snapshot".into(),
        ));
    }
    Ok(())
}

fn membership_hash(members: &BTreeSet<String>) -> String {
    let mut canonical = Vec::new();
    canonical.extend_from_slice(&(members.len() as u64).to_be_bytes());
    for address in members {
        canonical.extend_from_slice(&(address.len() as u64).to_be_bytes());
        canonical.extend_from_slice(address.as_bytes());
    }
    hex(Sha256::digest(canonical).into())
}

fn valid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && value[2..].bytes().any(|byte| byte != b'0')
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletQualityPolicy {
    pub maximum_recent_activity_age_ms: u64,
    pub minimum_closed_trades: usize,
    pub minimum_realized_pnl_usd: Decimal,
    pub minimum_annualized_sharpe: Decimal,
    pub target_annualized_sharpe: Decimal,
    pub minimum_win_rate_pct: Decimal,
    pub maximum_drawdown_pct: Decimal,
    pub minimum_account_equity_usd: Decimal,
    pub maximum_observed_leverage: Decimal,
    pub maximum_asset_concentration_pct: Decimal,
    pub confidence_decay_lambda: f64,
}

impl WalletQualityPolicy {
    pub fn validate(&self) -> Result<(), CohortError> {
        if self.maximum_recent_activity_age_ms == 0
            || self.minimum_closed_trades == 0
            || self.minimum_account_equity_usd < Decimal::ZERO
            || self.maximum_observed_leverage <= Decimal::ZERO
            || !(Decimal::ZERO..=Decimal::from(100)).contains(&self.minimum_win_rate_pct)
            || !(Decimal::ZERO..=Decimal::from(100)).contains(&self.maximum_drawdown_pct)
            || !(Decimal::ZERO..=Decimal::from(100)).contains(&self.maximum_asset_concentration_pct)
            || self.target_annualized_sharpe < self.minimum_annualized_sharpe
            || !self.confidence_decay_lambda.is_finite()
            || self.confidence_decay_lambda < 0.0
        {
            return Err(CohortError::InvalidQualityPolicy);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletQualityObservation {
    pub address: String,
    pub recent_activity_age_ms: u64,
    pub closed_trades: usize,
    pub realized_pnl_usd: Decimal,
    pub annualized_sharpe: Decimal,
    pub win_rate_pct: Decimal,
    pub maximum_drawdown_pct: Decimal,
    pub account_equity_usd: Decimal,
    pub maximum_observed_leverage: Decimal,
    pub maximum_margin_utilization_pct: Option<Decimal>,
    pub maximum_asset_concentration_pct: Decimal,
    pub median_holding_duration_ms: Option<u64>,
    /// Hyperliquid exposes at most the 10,000 most recent fills. These fields
    /// preserve the previously accepted high-density qualification pathway.
    #[serde(default)]
    pub retained_fill_count: usize,
    #[serde(default)]
    pub history_complete: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletQualificationPathway {
    #[default]
    Standard,
    HighDensity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletFilterReason {
    StaleActivity,
    InsufficientTradeHistory,
    RealizedPnl,
    Sharpe,
    WinRate,
    Drawdown,
    AccountEquity,
    Leverage,
    MissingMarginBehavior,
    InvalidMarginBehavior,
    AssetConcentration,
    MissingHoldingDurationProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletQualityDecision {
    pub address: String,
    pub qualified: bool,
    pub reasons: BTreeSet<WalletFilterReason>,
    #[serde(default)]
    pub qualification_pathway: WalletQualificationPathway,
    /// The strict standard-policy result remains visible when the established
    /// high-density ingestion pathway admits a retention-limited wallet.
    #[serde(default)]
    pub standard_quality_reasons: BTreeSet<WalletFilterReason>,
    pub bounded_wallet_weight: Decimal,
    pub realized_quality_score: Decimal,
}

pub fn apply_wallet_quality_policy(
    observation: &WalletQualityObservation,
    policy: &WalletQualityPolicy,
    base_allocation_weight: Decimal,
) -> Result<WalletQualityDecision, CohortError> {
    policy.validate()?;
    if !valid_address(&observation.address)
        || base_allocation_weight < Decimal::ZERO
        || !(Decimal::ZERO..=Decimal::from(100)).contains(&observation.win_rate_pct)
        || !(Decimal::ZERO..=Decimal::from(100)).contains(&observation.maximum_drawdown_pct)
        || observation.maximum_observed_leverage < Decimal::ZERO
        || !(Decimal::ZERO..=Decimal::from(100))
            .contains(&observation.maximum_asset_concentration_pct)
    {
        return Err(CohortError::InvalidQualityObservation(
            observation.address.clone(),
        ));
    }
    let mut standard_quality_reasons = BTreeSet::new();
    if observation.recent_activity_age_ms > policy.maximum_recent_activity_age_ms {
        standard_quality_reasons.insert(WalletFilterReason::StaleActivity);
    }
    if observation.closed_trades < policy.minimum_closed_trades {
        standard_quality_reasons.insert(WalletFilterReason::InsufficientTradeHistory);
    }
    if observation.realized_pnl_usd < policy.minimum_realized_pnl_usd {
        standard_quality_reasons.insert(WalletFilterReason::RealizedPnl);
    }
    if observation.annualized_sharpe < policy.minimum_annualized_sharpe {
        standard_quality_reasons.insert(WalletFilterReason::Sharpe);
    }
    if observation.win_rate_pct < policy.minimum_win_rate_pct {
        standard_quality_reasons.insert(WalletFilterReason::WinRate);
    }
    if observation.maximum_drawdown_pct > policy.maximum_drawdown_pct {
        standard_quality_reasons.insert(WalletFilterReason::Drawdown);
    }
    if observation.account_equity_usd <= policy.minimum_account_equity_usd {
        standard_quality_reasons.insert(WalletFilterReason::AccountEquity);
    }
    if observation.maximum_observed_leverage > policy.maximum_observed_leverage {
        standard_quality_reasons.insert(WalletFilterReason::Leverage);
    }
    match observation.maximum_margin_utilization_pct {
        None => {
            standard_quality_reasons.insert(WalletFilterReason::MissingMarginBehavior);
        }
        Some(value) if !(Decimal::ZERO..=Decimal::from(100)).contains(&value) => {
            standard_quality_reasons.insert(WalletFilterReason::InvalidMarginBehavior);
        }
        Some(_) => {}
    }
    if observation.maximum_asset_concentration_pct > policy.maximum_asset_concentration_pct {
        standard_quality_reasons.insert(WalletFilterReason::AssetConcentration);
    }
    if observation.median_holding_duration_ms.is_none() {
        standard_quality_reasons.insert(WalletFilterReason::MissingHoldingDurationProfile);
    }

    let history = Decimal::from(observation.closed_trades as u64)
        .checked_div(Decimal::from(policy.minimum_closed_trades as u64))
        .ok_or(CohortError::Arithmetic)?
        .min(Decimal::ONE);
    let win_rate = observation
        .win_rate_pct
        .checked_div(Decimal::from(100))
        .ok_or(CohortError::Arithmetic)?;
    let sharpe_modifier = if observation.annualized_sharpe >= policy.target_annualized_sharpe {
        Decimal::ONE
    } else if observation.annualized_sharpe < policy.minimum_annualized_sharpe {
        Decimal::ZERO
    } else {
        let gap = policy
            .target_annualized_sharpe
            .checked_sub(observation.annualized_sharpe)
            .ok_or(CohortError::Arithmetic)?
            .to_f64()
            .ok_or(CohortError::Arithmetic)?;
        Decimal::from_f64((-policy.confidence_decay_lambda * gap).exp())
            .ok_or(CohortError::Arithmetic)?
    };
    let standard_quality_score = history
        .checked_mul(win_rate)
        .and_then(|value| value.checked_mul(sharpe_modifier))
        .ok_or(CohortError::Arithmetic)?
        .clamp(Decimal::ZERO, Decimal::ONE);
    let high_density = !observation.history_complete
        && observation.retained_fill_count == HIGH_DENSITY_RETAINED_FILL_COUNT;
    let (qualification_pathway, reasons, realized_quality_score) = if high_density {
        let mut ingestion_reasons = BTreeSet::new();
        if observation.closed_trades == 0 {
            ingestion_reasons.insert(WalletFilterReason::InsufficientTradeHistory);
        }
        if observation.realized_pnl_usd <= Decimal::ZERO {
            ingestion_reasons.insert(WalletFilterReason::RealizedPnl);
        }
        if observation.account_equity_usd <= Decimal::ZERO {
            ingestion_reasons.insert(WalletFilterReason::AccountEquity);
        }
        let high_density_quality_score = history
            .checked_mul(win_rate)
            .ok_or(CohortError::Arithmetic)?
            .clamp(Decimal::ZERO, Decimal::ONE);
        (
            WalletQualificationPathway::HighDensity,
            ingestion_reasons,
            high_density_quality_score,
        )
    } else {
        (
            WalletQualificationPathway::Standard,
            standard_quality_reasons.clone(),
            standard_quality_score,
        )
    };
    let bounded_wallet_weight = if reasons.is_empty() {
        base_allocation_weight
            .clamp(Decimal::ZERO, Decimal::ONE)
            .checked_mul(realized_quality_score)
            .ok_or(CohortError::Arithmetic)?
    } else {
        Decimal::ZERO
    };
    Ok(WalletQualityDecision {
        address: observation.address.to_ascii_lowercase(),
        qualified: reasons.is_empty(),
        reasons,
        qualification_pathway,
        standard_quality_reasons,
        bounded_wallet_weight,
        realized_quality_score,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeWalletPosition {
    pub signed_notional: Decimal,
    pub entry_price: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilteredCohortWallet {
    pub address: String,
    pub observed_at_ms: u64,
    pub bounded_wallet_weight: Decimal,
    pub realized_quality_score: Decimal,
    pub positions: BTreeMap<String, AuthoritativeWalletPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortAssetAggregate {
    pub asset: String,
    pub observed_at_ms: u64,
    pub wallet_count_after_filtering: usize,
    pub positioned_wallet_count: usize,
    pub long_notional: Decimal,
    pub short_notional: Decimal,
    /// Quality-bounded notionals used for signal construction. The raw fields
    /// above remain authoritative diagnostics and must not be substituted for
    /// these values when deriving impulse.
    #[serde(default)]
    pub weighted_long_notional: Decimal,
    #[serde(default)]
    pub weighted_short_notional: Decimal,
    pub long_trader_count: usize,
    pub short_trader_count: usize,
    pub notional_bias: Decimal,
    pub trader_count_bias: Decimal,
    pub cohort_position: Decimal,
    pub weighted_realized_quality: Decimal,
    #[serde(default)]
    pub unrealized_profit_fraction: Decimal,
    pub dominant_wallet_percentage: Decimal,
    pub observed_entry_opportunity: Option<Decimal>,
}

pub fn aggregate_authoritative_positions(
    asset: &str,
    observed_at_ms: u64,
    allowed_members: &BTreeSet<String>,
    wallets: &[FilteredCohortWallet],
) -> Result<CohortAssetAggregate, CohortError> {
    if asset.is_empty() || observed_at_ms == 0 {
        return Err(CohortError::InvalidWalletState(asset.into()));
    }
    let mut seen = BTreeSet::new();
    let mut long_notional = Decimal::ZERO;
    let mut short_notional = Decimal::ZERO;
    let mut weighted_long_notional = Decimal::ZERO;
    let mut weighted_short_notional = Decimal::ZERO;
    let mut long_trader_count = 0usize;
    let mut short_trader_count = 0usize;
    let mut positioned_wallet_count = 0usize;
    let mut dominant_notional = Decimal::ZERO;
    let mut signed_weighted_quality = Decimal::ZERO;
    let mut wallet_weight_total = Decimal::ZERO;
    let mut long_entry_numerator = Decimal::ZERO;
    let mut long_entry_denominator = Decimal::ZERO;
    let mut short_entry_numerator = Decimal::ZERO;
    let mut short_entry_denominator = Decimal::ZERO;
    let mut weighted_positive_unrealized_pnl = Decimal::ZERO;
    for wallet in wallets {
        let address = wallet.address.to_ascii_lowercase();
        if !valid_address(&address)
            || !allowed_members.contains(&address)
            || !seen.insert(address.clone())
            || wallet.observed_at_ms != observed_at_ms
            || !(Decimal::ZERO..=Decimal::ONE).contains(&wallet.bounded_wallet_weight)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&wallet.realized_quality_score)
        {
            return Err(CohortError::InvalidWalletState(address));
        }
        let Some(position) = wallet.positions.get(asset) else {
            continue;
        };
        if position.signed_notional.is_zero() {
            continue;
        }
        if position.entry_price <= Decimal::ZERO {
            return Err(CohortError::InvalidWalletState(address));
        }
        positioned_wallet_count += 1;
        let weighted_notional = position
            .signed_notional
            .abs()
            .checked_mul(wallet.bounded_wallet_weight)
            .ok_or(CohortError::Arithmetic)?;
        weighted_positive_unrealized_pnl = weighted_positive_unrealized_pnl
            .checked_add(
                position
                    .unrealized_pnl
                    .max(Decimal::ZERO)
                    .checked_mul(wallet.bounded_wallet_weight)
                    .ok_or(CohortError::Arithmetic)?,
            )
            .ok_or(CohortError::Arithmetic)?;
        dominant_notional = dominant_notional.max(weighted_notional);
        let raw_notional = position.signed_notional.abs();
        let direction = if position.signed_notional.is_sign_positive() {
            long_trader_count += 1;
            long_notional = long_notional
                .checked_add(raw_notional)
                .ok_or(CohortError::Arithmetic)?;
            weighted_long_notional = weighted_long_notional
                .checked_add(weighted_notional)
                .ok_or(CohortError::Arithmetic)?;
            long_entry_numerator = long_entry_numerator
                .checked_add(
                    position
                        .entry_price
                        .checked_mul(weighted_notional)
                        .ok_or(CohortError::Arithmetic)?,
                )
                .ok_or(CohortError::Arithmetic)?;
            long_entry_denominator = long_entry_denominator
                .checked_add(weighted_notional)
                .ok_or(CohortError::Arithmetic)?;
            Decimal::ONE
        } else {
            short_trader_count += 1;
            short_notional = short_notional
                .checked_add(raw_notional)
                .ok_or(CohortError::Arithmetic)?;
            weighted_short_notional = weighted_short_notional
                .checked_add(weighted_notional)
                .ok_or(CohortError::Arithmetic)?;
            short_entry_numerator = short_entry_numerator
                .checked_add(
                    position
                        .entry_price
                        .checked_mul(weighted_notional)
                        .ok_or(CohortError::Arithmetic)?,
                )
                .ok_or(CohortError::Arithmetic)?;
            short_entry_denominator = short_entry_denominator
                .checked_add(weighted_notional)
                .ok_or(CohortError::Arithmetic)?;
            -Decimal::ONE
        };
        signed_weighted_quality = signed_weighted_quality
            .checked_add(
                wallet
                    .realized_quality_score
                    .checked_mul(wallet.bounded_wallet_weight)
                    .and_then(|value| value.checked_mul(direction))
                    .ok_or(CohortError::Arithmetic)?,
            )
            .ok_or(CohortError::Arithmetic)?;
        wallet_weight_total = wallet_weight_total
            .checked_add(wallet.bounded_wallet_weight)
            .ok_or(CohortError::Arithmetic)?;
    }
    let weighted_gross = weighted_long_notional
        .checked_add(weighted_short_notional)
        .ok_or(CohortError::Arithmetic)?;
    let notional_bias = if weighted_gross.is_zero() {
        Decimal::ZERO
    } else {
        weighted_long_notional
            .checked_sub(weighted_short_notional)
            .and_then(|value| value.checked_div(weighted_gross))
            .ok_or(CohortError::Arithmetic)?
    };
    let trader_total = long_trader_count + short_trader_count;
    let trader_count_bias = if trader_total == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(long_trader_count as i64 - short_trader_count as i64)
            .checked_div(Decimal::from(trader_total as u64))
            .ok_or(CohortError::Arithmetic)?
    };
    let cohort_position = Decimal::new(70, 2)
        .checked_mul(notional_bias)
        .and_then(|value| {
            Decimal::new(30, 2)
                .checked_mul(trader_count_bias)
                .and_then(|count| value.checked_add(count))
        })
        .ok_or(CohortError::Arithmetic)?
        .clamp(-Decimal::ONE, Decimal::ONE);
    let weighted_realized_quality = if wallet_weight_total.is_zero() {
        Decimal::ZERO
    } else {
        signed_weighted_quality
            .checked_div(wallet_weight_total)
            .ok_or(CohortError::Arithmetic)?
            .clamp(-Decimal::ONE, Decimal::ONE)
    };
    let dominant_wallet_percentage = if weighted_gross.is_zero() {
        Decimal::ZERO
    } else {
        dominant_notional
            .checked_mul(Decimal::from(100))
            .and_then(|value| value.checked_div(weighted_gross))
            .ok_or(CohortError::Arithmetic)?
    };
    let unrealized_profit_fraction = if weighted_gross.is_zero() {
        Decimal::ZERO
    } else {
        weighted_positive_unrealized_pnl
            .checked_div(weighted_gross)
            .ok_or(CohortError::Arithmetic)?
    };
    let observed_entry_opportunity =
        if cohort_position.is_sign_positive() && !long_entry_denominator.is_zero() {
            Some(
                long_entry_numerator
                    .checked_div(long_entry_denominator)
                    .ok_or(CohortError::Arithmetic)?,
            )
        } else if cohort_position.is_sign_negative() && !short_entry_denominator.is_zero() {
            Some(
                short_entry_numerator
                    .checked_div(short_entry_denominator)
                    .ok_or(CohortError::Arithmetic)?,
            )
        } else {
            None
        };
    Ok(CohortAssetAggregate {
        asset: asset.into(),
        observed_at_ms,
        wallet_count_after_filtering: wallets.len(),
        positioned_wallet_count,
        long_notional,
        short_notional,
        weighted_long_notional,
        weighted_short_notional,
        long_trader_count,
        short_trader_count,
        notional_bias,
        trader_count_bias,
        cohort_position,
        weighted_realized_quality,
        unrealized_profit_fraction,
        dominant_wallet_percentage,
        observed_entry_opportunity,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTwapContext {
    pub observed_at_ms: u64,
    pub direction: i8,
    pub total_quantity: Decimal,
    pub executed_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub progress: Decimal,
    pub nearly_completed: bool,
}

impl ActiveTwapContext {
    fn pressure(&self) -> Result<Decimal, CohortError> {
        if ![-1, 1].contains(&self.direction)
            || self.total_quantity <= Decimal::ZERO
            || self.executed_quantity < Decimal::ZERO
            || self.remaining_quantity < Decimal::ZERO
            || self.executed_quantity + self.remaining_quantity != self.total_quantity
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.progress)
        {
            return Err(CohortError::InvalidSignalInput(
                "invalid active TWAP context".into(),
            ));
        }
        Decimal::from(self.direction)
            .checked_mul(
                self.remaining_quantity
                    .checked_div(self.total_quantity)
                    .ok_or(CohortError::Arithmetic)?,
            )
            .ok_or(CohortError::Arithmetic)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortRiskFlags {
    pub incomplete_authoritative_wallet_state: bool,
    pub stale_unrealized_profit_dominated: bool,
    pub short_covering_not_initiation: bool,
    #[serde(default)]
    pub long_covering_not_initiation: bool,
    pub adverse_funding: bool,
    pub liquidation_cascade: bool,
    pub recent_additions_unwound: bool,
    pub original_filtered_wallets_closed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortPenalties {
    pub chase: Decimal,
    pub crowding: Decimal,
    pub funding: Decimal,
    pub liquidation: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortSignalInput {
    pub membership: CohortMembershipResolution,
    pub aggregate: CohortAssetAggregate,
    pub filtered_wallet_consensus: Decimal,
    pub active_twap: Option<ActiveTwapContext>,
    /// When set, market context is retained as diagnostics but cannot veto a
    /// source-authoritative transition candidate. This is false by default so
    /// older serialized inputs preserve the legacy admission behavior.
    #[serde(default)]
    pub defer_market_context_admission: bool,
    pub market_confirmation: Decimal,
    pub penalties: CohortPenalties,
    pub risk_flags: CohortRiskFlags,
    pub position_change_age_ms: u64,
    pub maximum_position_change_age_ms: u64,
    pub dominant_wallet_limit_pct: Decimal,
    pub current_price: Decimal,
    pub r_unit: Decimal,
    pub estimated_round_trip_cost_fraction: Decimal,
    pub expected_move_fraction: Decimal,
    pub source_budget_fraction: Decimal,
    pub existing_wallet_source_target: Decimal,
    pub technical_target: Decimal,
    pub rebalance_tolerance: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CohortDecisionReason {
    EligibleLong,
    EligibleShort,
    NeutralScore,
    DirectionalDisagreement,
    IncompleteAuthoritativeWalletState,
    ChaseLimit,
    StaleUnrealizedProfit,
    NearlyCompletedTwap,
    ShortCovering,
    LongCovering,
    AdverseFunding,
    LiquidationCascade,
    DominantWallet,
    StalePositionChange,
    RecentAdditionsUnwound,
    OriginalFilteredWalletsClosed,
    ExpectedMoveDoesNotCoverCosts,
    MissingEntryOpportunity,
    AuthoritativeWalletHydrationFailed,
    MissingMarketPrice,
    MissingAtr,
    MissingExpectedMove,
    PositionAggregationFailed,
    FilteredWalletConsensusFailed,
    SignalEvaluationFailed,
    ImmaterialRefresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributionTag {
    ExistingWalletSourceOnly,
    VeryProfitableCohortOnly,
    BothSourceSetsAgreeing,
    SourceSetsDisagreeing,
    TechnicalOnly,
    SourceTechnicalHybrid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortIndicatorRecord {
    pub asset: String,
    pub cohort_snapshot_timestamp_ms: u64,
    pub membership_set_hash: String,
    pub wallet_count_before_filtering: usize,
    pub wallet_count_after_filtering: usize,
    pub overlap_with_existing: usize,
    pub long_notional: Decimal,
    pub short_notional: Decimal,
    pub long_trader_count: usize,
    pub short_trader_count: usize,
    pub impulse_5m: Decimal,
    pub impulse_15m: Decimal,
    pub impulse_1h: Decimal,
    pub active_twap_direction: i8,
    pub active_twap_remaining_quantity: Decimal,
    pub weighted_wallet_quality: Decimal,
    pub dominant_wallet_percentage: Decimal,
    pub cohort_score: Decimal,
    pub chase_distance_r: Option<Decimal>,
    pub cost_estimate_fraction: Decimal,
    /// Admitted existing-wallet sub-signal after applying the shared source
    /// budget, before the two source sets are netted and capped together.
    #[serde(default)]
    pub existing_wallet_source_target: Decimal,
    /// Admitted `very_profitable` sub-signal after applying the shared source
    /// budget. A rejected cohort decision records zero here.
    #[serde(default)]
    pub very_profitable_cohort_target: Decimal,
    /// Final source sleeve target after netting both source sets and applying
    /// the source budget exactly once.
    pub source_target: Decimal,
    pub technical_target: Decimal,
    pub combined_target: Decimal,
    pub target_version: u64,
    pub independent_execution_root: bool,
    pub reasons: BTreeSet<CohortDecisionReason>,
    pub attribution: BTreeSet<AttributionTag>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AggregateFrame {
    observed_at_ms: u64,
    net_notional: Decimal,
    gross_notional: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MaterialTargetState {
    version: u64,
    cohort_direction: i8,
    impulse: Decimal,
    filtered_wallet_consensus: Decimal,
    twap_pressure: Decimal,
    existing_wallet_source_target: Decimal,
    very_profitable_cohort_target: Decimal,
    source_target: Decimal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VeryProfitableCohortEngine {
    history: BTreeMap<String, VecDeque<AggregateFrame>>,
    targets: BTreeMap<String, MaterialTargetState>,
}

impl VeryProfitableCohortEngine {
    pub fn target_version(&self, asset: &str) -> Option<u64> {
        self.targets.get(asset).map(|target| target.version)
    }

    pub fn evaluate(
        &mut self,
        input: CohortSignalInput,
    ) -> Result<CohortIndicatorRecord, CohortError> {
        validate_signal_input(&input)?;
        let asset = input.aggregate.asset.clone();
        let current = AggregateFrame {
            observed_at_ms: input.aggregate.observed_at_ms,
            net_notional: input
                .aggregate
                .weighted_long_notional
                .checked_sub(input.aggregate.weighted_short_notional)
                .ok_or(CohortError::Arithmetic)?,
            gross_notional: input
                .aggregate
                .weighted_long_notional
                .checked_add(input.aggregate.weighted_short_notional)
                .ok_or(CohortError::Arithmetic)?,
        };
        let history = self.history.entry(asset.clone()).or_default();
        let previous_frame = history.back().cloned();
        if let Some(last) = history.back() {
            if current.observed_at_ms < last.observed_at_ms {
                return Err(CohortError::NonMonotonicSnapshot);
            }
            if current.observed_at_ms == last.observed_at_ms && &current != last {
                return Err(CohortError::ConflictingSnapshot);
            }
        }
        let impulse_5m = impulse_at(
            history,
            &current,
            FIVE_MINUTES_MS,
            input.maximum_position_change_age_ms,
        )?;
        let impulse_15m = impulse_at(
            history,
            &current,
            FIFTEEN_MINUTES_MS,
            input.maximum_position_change_age_ms,
        )?;
        let impulse_1h = impulse_at(
            history,
            &current,
            ONE_HOUR_MS,
            input.maximum_position_change_age_ms,
        )?;
        let impulse = Decimal::new(20, 2)
            .checked_mul(impulse_5m)
            .and_then(|value| {
                Decimal::new(50, 2)
                    .checked_mul(impulse_15m)
                    .and_then(|component| value.checked_add(component))
            })
            .and_then(|value| {
                Decimal::new(30, 2)
                    .checked_mul(impulse_1h)
                    .and_then(|component| value.checked_add(component))
            })
            .ok_or(CohortError::Arithmetic)?
            .clamp(-Decimal::ONE, Decimal::ONE);
        if history.back() != Some(&current) {
            history.push_back(current.clone());
        }
        while history.front().is_some_and(|frame| {
            frame.observed_at_ms.saturating_add(HISTORY_RETENTION_MS) < current.observed_at_ms
        }) {
            history.pop_front();
        }

        let twap_pressure = match &input.active_twap {
            Some(twap) => twap.pressure()?,
            None => Decimal::ZERO,
        };
        let raw_score = Decimal::new(30, 2)
            .checked_mul(input.aggregate.cohort_position)
            .and_then(|value| {
                Decimal::new(20, 2)
                    .checked_mul(impulse)
                    .and_then(|component| value.checked_add(component))
            })
            .and_then(|value| {
                Decimal::new(20, 2)
                    .checked_mul(input.filtered_wallet_consensus)
                    .and_then(|component| value.checked_add(component))
            })
            .and_then(|value| {
                Decimal::new(15, 2)
                    .checked_mul(input.aggregate.weighted_realized_quality)
                    .and_then(|component| value.checked_add(component))
            })
            .and_then(|value| {
                Decimal::new(10, 2)
                    .checked_mul(twap_pressure)
                    .and_then(|component| value.checked_add(component))
            })
            .and_then(|value| {
                Decimal::new(5, 2)
                    .checked_mul(input.market_confirmation)
                    .and_then(|component| value.checked_add(component))
            })
            .ok_or(CohortError::Arithmetic)?;
        let penalty = input
            .penalties
            .chase
            .checked_add(input.penalties.crowding)
            .and_then(|value| value.checked_add(input.penalties.funding))
            .and_then(|value| value.checked_add(input.penalties.liquidation))
            .ok_or(CohortError::Arithmetic)?;
        let cohort_score = attenuate_magnitude(raw_score, penalty);
        let agreement = directional_agreement([
            input.aggregate.cohort_position,
            impulse,
            input.filtered_wallet_consensus,
        ]);
        let proposed_direction = if agreement && cohort_score >= COHORT_ACTIVATION_SCORE {
            1
        } else if agreement && cohort_score <= -COHORT_ACTIVATION_SCORE {
            -1
        } else {
            0
        };
        let chase_distance_r = (proposed_direction != 0)
            .then_some(input.aggregate.observed_entry_opportunity)
            .flatten()
            .filter(|_| input.current_price > Decimal::ZERO && input.r_unit > Decimal::ZERO)
            .and_then(|opportunity| {
                let distance = if proposed_direction > 0 {
                    input.current_price.checked_sub(opportunity)
                } else {
                    opportunity.checked_sub(input.current_price)
                }?;
                distance.checked_div(input.r_unit)
            });
        let previous_target = self.targets.get(&asset).cloned();
        let derived_short_covering = previous_frame.as_ref().is_some_and(|previous| {
            previous.net_notional < Decimal::ZERO
                && current.net_notional < Decimal::ZERO
                && current.net_notional > previous.net_notional
        });
        let derived_long_covering = previous_frame.as_ref().is_some_and(|previous| {
            previous.net_notional > Decimal::ZERO
                && current.net_notional > Decimal::ZERO
                && current.net_notional < previous.net_notional
        });
        let derived_recent_additions_unwound = previous_target.as_ref().is_some_and(|previous| {
            (previous.cohort_direction > 0 && impulse < -input.rebalance_tolerance)
                || (previous.cohort_direction < 0 && impulse > input.rebalance_tolerance)
        });
        let derived_stale_unrealized_profit = proposed_direction != 0
            && input.aggregate.unrealized_profit_fraction > input.expected_move_fraction
            && impulse.abs() <= input.rebalance_tolerance;

        let mut reasons = BTreeSet::new();
        if !agreement {
            reasons.insert(CohortDecisionReason::DirectionalDisagreement);
        } else if proposed_direction == 0 {
            reasons.insert(CohortDecisionReason::NeutralScore);
        }
        if input.risk_flags.incomplete_authoritative_wallet_state {
            reasons.insert(CohortDecisionReason::IncompleteAuthoritativeWalletState);
        }
        if !input.defer_market_context_admission {
            if proposed_direction != 0 && chase_distance_r.is_none() {
                reasons.insert(CohortDecisionReason::MissingEntryOpportunity);
            }
            if chase_distance_r.is_some_and(|distance| distance > COHORT_CHASE_LIMIT_R) {
                reasons.insert(CohortDecisionReason::ChaseLimit);
            }
            if input.risk_flags.stale_unrealized_profit_dominated || derived_stale_unrealized_profit
            {
                reasons.insert(CohortDecisionReason::StaleUnrealizedProfit);
            }
        }
        if input
            .active_twap
            .as_ref()
            .is_some_and(|twap| twap.nearly_completed)
        {
            reasons.insert(CohortDecisionReason::NearlyCompletedTwap);
        }
        // Covering vetoes are LONG/SHORT symmetric and apply only when the
        // candidate would reduce/cover that side. Initiation or expansion of
        // the same side (proposed_direction == side sign) is never vetoed as
        // "covering", so new SHORT selection stays production-reachable while
        // actual cover-reductions remain gated.
        for veto in covering_vetoes(
            proposed_direction,
            input.aggregate.cohort_position,
            input.risk_flags.short_covering_not_initiation || derived_short_covering,
            input.risk_flags.long_covering_not_initiation || derived_long_covering,
        ) {
            reasons.insert(veto);
        }
        if !input.defer_market_context_admission && input.risk_flags.adverse_funding {
            reasons.insert(CohortDecisionReason::AdverseFunding);
        }
        if input.risk_flags.liquidation_cascade {
            reasons.insert(CohortDecisionReason::LiquidationCascade);
        }
        if input.aggregate.dominant_wallet_percentage > input.dominant_wallet_limit_pct {
            reasons.insert(CohortDecisionReason::DominantWallet);
        }
        if input.position_change_age_ms > input.maximum_position_change_age_ms {
            reasons.insert(CohortDecisionReason::StalePositionChange);
        }
        if input.risk_flags.recent_additions_unwound || derived_recent_additions_unwound {
            reasons.insert(CohortDecisionReason::RecentAdditionsUnwound);
        }
        if input.risk_flags.original_filtered_wallets_closed {
            reasons.insert(CohortDecisionReason::OriginalFilteredWalletsClosed);
        }
        if !input.defer_market_context_admission
            && input.expected_move_fraction <= input.estimated_round_trip_cost_fraction
        {
            reasons.insert(CohortDecisionReason::ExpectedMoveDoesNotCoverCosts);
        }
        let eligible = reasons.is_empty();
        let admitted_cohort_score = if eligible {
            reasons.insert(if proposed_direction > 0 {
                CohortDecisionReason::EligibleLong
            } else {
                CohortDecisionReason::EligibleShort
            });
            cohort_score
        } else {
            Decimal::ZERO
        };
        let combined_source_score = input
            .existing_wallet_source_target
            .checked_add(admitted_cohort_score)
            .ok_or(CohortError::Arithmetic)?
            .clamp(-Decimal::ONE, Decimal::ONE);
        let candidate_source_target = input
            .source_budget_fraction
            .checked_mul(combined_source_score)
            .ok_or(CohortError::Arithmetic)?;
        let candidate_existing_wallet_source_target = input
            .source_budget_fraction
            .checked_mul(input.existing_wallet_source_target)
            .ok_or(CohortError::Arithmetic)?;
        let candidate_very_profitable_cohort_target = input
            .source_budget_fraction
            .checked_mul(admitted_cohort_score)
            .ok_or(CohortError::Arithmetic)?;
        let proposed = MaterialTargetState {
            version: 0,
            cohort_direction: sign(input.aggregate.cohort_position),
            impulse,
            filtered_wallet_consensus: input.filtered_wallet_consensus,
            twap_pressure,
            existing_wallet_source_target: candidate_existing_wallet_source_target,
            very_profitable_cohort_target: candidate_very_profitable_cohort_target,
            source_target: candidate_source_target,
        };
        let previous = previous_target;
        let material = previous
            .as_ref()
            .map(|previous| material_change(previous, &proposed, input.rebalance_tolerance))
            .unwrap_or(!candidate_source_target.is_zero());
        let (
            target_version,
            existing_wallet_source_target,
            very_profitable_cohort_target,
            source_target,
            independent_execution_root,
        ) = match previous {
            None => {
                self.targets.insert(asset.clone(), proposed);
                (
                    0,
                    candidate_existing_wallet_source_target,
                    candidate_very_profitable_cohort_target,
                    candidate_source_target,
                    material,
                )
            }
            Some(previous) if material => {
                let version = previous
                    .version
                    .checked_add(1)
                    .ok_or(CohortError::Arithmetic)?;
                self.targets.insert(
                    asset.clone(),
                    MaterialTargetState {
                        version,
                        ..proposed
                    },
                );
                let creates_execution_root =
                    !previous.source_target.is_zero() || !candidate_source_target.is_zero();
                (
                    version,
                    candidate_existing_wallet_source_target,
                    candidate_very_profitable_cohort_target,
                    candidate_source_target,
                    creates_execution_root,
                )
            }
            Some(previous) => {
                reasons.insert(CohortDecisionReason::ImmaterialRefresh);
                (
                    previous.version,
                    previous.existing_wallet_source_target,
                    previous.very_profitable_cohort_target,
                    previous.source_target,
                    false,
                )
            }
        };
        let combined_target = source_target
            .checked_add(input.technical_target)
            .ok_or(CohortError::Arithmetic)?;
        let attribution = classify_attribution(
            existing_wallet_source_target,
            very_profitable_cohort_target,
            input.technical_target,
        );
        Ok(CohortIndicatorRecord {
            asset,
            cohort_snapshot_timestamp_ms: input.membership.snapshot_timestamp_ms,
            membership_set_hash: input.membership.membership_set_hash,
            wallet_count_before_filtering: input.membership.wallet_count_before_filtering,
            wallet_count_after_filtering: input.aggregate.wallet_count_after_filtering,
            overlap_with_existing: input.membership.overlap_with_existing,
            long_notional: input.aggregate.long_notional,
            short_notional: input.aggregate.short_notional,
            long_trader_count: input.aggregate.long_trader_count,
            short_trader_count: input.aggregate.short_trader_count,
            impulse_5m,
            impulse_15m,
            impulse_1h,
            active_twap_direction: input.active_twap.as_ref().map_or(0, |twap| twap.direction),
            active_twap_remaining_quantity: input
                .active_twap
                .as_ref()
                .map_or(Decimal::ZERO, |twap| twap.remaining_quantity),
            weighted_wallet_quality: input.aggregate.weighted_realized_quality,
            dominant_wallet_percentage: input.aggregate.dominant_wallet_percentage,
            cohort_score,
            chase_distance_r,
            cost_estimate_fraction: input.estimated_round_trip_cost_fraction,
            existing_wallet_source_target,
            very_profitable_cohort_target,
            source_target,
            technical_target: input.technical_target,
            combined_target,
            target_version,
            independent_execution_root,
            reasons,
            attribution,
        })
    }
}

fn validate_signal_input(input: &CohortSignalInput) -> Result<(), CohortError> {
    let unit = -Decimal::ONE..=Decimal::ONE;
    let penalties = [
        input.penalties.chase,
        input.penalties.crowding,
        input.penalties.funding,
        input.penalties.liquidation,
    ];
    if input.aggregate.observed_at_ms < input.membership.snapshot_timestamp_ms
        || !unit.contains(&input.filtered_wallet_consensus)
        || !unit.contains(&input.market_confirmation)
        || !unit.contains(&input.existing_wallet_source_target)
        || !unit.contains(&input.technical_target)
        || penalties
            .into_iter()
            .any(|penalty| !(Decimal::ZERO..=Decimal::ONE).contains(&penalty))
        || input.maximum_position_change_age_ms == 0
        || !(Decimal::ZERO..=Decimal::from(100)).contains(&input.dominant_wallet_limit_pct)
        || if input.defer_market_context_admission {
            input.current_price < Decimal::ZERO || input.r_unit < Decimal::ZERO
        } else {
            input.current_price <= Decimal::ZERO || input.r_unit <= Decimal::ZERO
        }
        || input.estimated_round_trip_cost_fraction < Decimal::ZERO
        || input.expected_move_fraction < Decimal::ZERO
        || !(Decimal::ZERO..=Decimal::ONE).contains(&input.source_budget_fraction)
        || input.rebalance_tolerance < Decimal::ZERO
    {
        return Err(CohortError::InvalidSignalInput(
            input.aggregate.asset.clone(),
        ));
    }
    if let Some(twap) = &input.active_twap {
        if twap.observed_at_ms < input.membership.snapshot_timestamp_ms {
            return Err(CohortError::InvalidSignalInput(
                "TWAP predates membership snapshot".into(),
            ));
        }
    }
    Ok(())
}

fn impulse_at(
    history: &VecDeque<AggregateFrame>,
    current: &AggregateFrame,
    horizon_ms: u64,
    maximum_observation_age_ms: u64,
) -> Result<Decimal, CohortError> {
    let cutoff = current.observed_at_ms.saturating_sub(horizon_ms);
    let Some(previous) = history.iter().rev().find(|frame| {
        frame.observed_at_ms <= cutoff
            && cutoff.saturating_sub(frame.observed_at_ms) <= maximum_observation_age_ms
    }) else {
        return Ok(Decimal::ZERO);
    };
    let denominator = current.gross_notional.max(previous.gross_notional);
    if denominator.is_zero() {
        return Ok(Decimal::ZERO);
    }
    current
        .net_notional
        .checked_sub(previous.net_notional)
        .and_then(|value| value.checked_div(denominator))
        .map(|value| value.clamp(-Decimal::ONE, Decimal::ONE))
        .ok_or(CohortError::Arithmetic)
}

fn directional_agreement(values: [Decimal; 3]) -> bool {
    let directions = values.map(sign);
    directions[0] != 0 && directions[0] == directions[1] && directions[1] == directions[2]
}

/// Symmetric covering vetoes. A covering signal vetoes only reduction of the
/// side being covered (proposed_direction != side sign). Initiation or
/// expansion of that same side is never vetoed, keeping new SHORT (and LONG)
/// selection production-reachable. Returns zero, one, or (in mixed states)
/// both vetoes; ordering is deterministic.
fn covering_vetoes(
    proposed_direction: i8,
    cohort_position: Decimal,
    short_covering: bool,
    long_covering: bool,
) -> Vec<CohortDecisionReason> {
    let mut vetoes = Vec::new();
    if proposed_direction >= 0 && short_covering && cohort_position < Decimal::ZERO {
        vetoes.push(CohortDecisionReason::ShortCovering);
    }
    if proposed_direction <= 0 && long_covering && cohort_position > Decimal::ZERO {
        vetoes.push(CohortDecisionReason::LongCovering);
    }
    vetoes
}

fn sign(value: Decimal) -> i8 {
    if value > Decimal::ZERO {
        1
    } else if value < Decimal::ZERO {
        -1
    } else {
        0
    }
}

fn attenuate_magnitude(value: Decimal, penalty: Decimal) -> Decimal {
    let magnitude = value.abs().saturating_sub(penalty).max(Decimal::ZERO);
    if magnitude.is_zero() {
        // rust_decimal preserves the sign bit on zero, but deserializing "-0"
        // normalizes it to positive zero. Never emit a signed zero into an
        // identity-bound record because its typed JSON round trip is not
        // byte-stable.
        Decimal::ZERO
    } else if value.is_sign_negative() {
        -magnitude
    } else {
        magnitude
    }
}

fn material_change(
    previous: &MaterialTargetState,
    proposed: &MaterialTargetState,
    tolerance: Decimal,
) -> bool {
    previous.cohort_direction != proposed.cohort_direction
        || sign(previous.impulse) != sign(proposed.impulse)
        || sign(previous.filtered_wallet_consensus) != sign(proposed.filtered_wallet_consensus)
        || sign(previous.twap_pressure) != sign(proposed.twap_pressure)
        || sign(previous.existing_wallet_source_target)
            != sign(proposed.existing_wallet_source_target)
        || sign(previous.very_profitable_cohort_target)
            != sign(proposed.very_profitable_cohort_target)
        || sign(previous.source_target) != sign(proposed.source_target)
        || (previous.impulse - proposed.impulse).abs() > tolerance
        || (previous.filtered_wallet_consensus - proposed.filtered_wallet_consensus).abs()
            > tolerance
        || (previous.twap_pressure - proposed.twap_pressure).abs() > tolerance
        || (previous.existing_wallet_source_target - proposed.existing_wallet_source_target).abs()
            > tolerance
        || (previous.very_profitable_cohort_target - proposed.very_profitable_cohort_target).abs()
            > tolerance
        || (previous.source_target - proposed.source_target).abs() > tolerance
}

pub fn classify_attribution(
    existing_wallet_source: Decimal,
    very_profitable_cohort: Decimal,
    technical: Decimal,
) -> BTreeSet<AttributionTag> {
    let mut tags = BTreeSet::new();
    let existing = sign(existing_wallet_source);
    let cohort = sign(very_profitable_cohort);
    let technical_direction = sign(technical);
    if existing != 0 && cohort == 0 {
        tags.insert(AttributionTag::ExistingWalletSourceOnly);
    } else if existing == 0 && cohort != 0 {
        tags.insert(AttributionTag::VeryProfitableCohortOnly);
    } else if existing != 0 && cohort != 0 && existing == cohort {
        tags.insert(AttributionTag::BothSourceSetsAgreeing);
    } else if existing != 0 && cohort != 0 {
        tags.insert(AttributionTag::SourceSetsDisagreeing);
    }
    if existing == 0 && cohort == 0 && technical_direction != 0 {
        tags.insert(AttributionTag::TechnicalOnly);
    }
    if (existing != 0 || cohort != 0) && technical_direction != 0 {
        tags.insert(AttributionTag::SourceTechnicalHybrid);
    }
    tags
}

fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(index: u8) -> String {
        format!("0x{index:040x}")
    }

    fn membership() -> CohortMembershipResolution {
        let snapshot = HyperdashMembershipSnapshot {
            schema_version: 1,
            source: VERY_PROFITABLE_COHORT_URL.into(),
            cohort_id: VERY_PROFITABLE_COHORT_ID.into(),
            observed_at_ms: 1,
            displayed_min_all_time_pnl_usd: Decimal::from(100_000),
            displayed_max_all_time_pnl_usd: Decimal::from(1_000_000),
            completeness: MembershipCompleteness::Complete,
            reported_member_count: 4,
            captured_member_count: 4,
            wallets: vec![
                address(1),
                address(2),
                address(2).to_ascii_uppercase().replacen("0X", "0x", 1),
                address(3),
            ],
        };
        resolve_very_profitable_membership(&snapshot, vec![address(2), address(9)]).unwrap()
    }

    #[test]
    fn membership_is_snapshot_versioned_deduplicated_and_overlap_is_not_a_second_vote() {
        let resolved = membership();
        assert_eq!(resolved.wallet_count_before_filtering, 4);
        assert_eq!(resolved.unique_wallet_count, 3);
        assert_eq!(resolved.overlap_with_existing, 1);
        assert_eq!(
            resolved.overlapping_members,
            [address(2)].into_iter().collect()
        );
        assert_eq!(
            resolved.cohort_only_members,
            [address(1), address(3)].into_iter().collect()
        );
        assert_eq!(resolved.membership_set_hash.len(), 64);
    }

    #[test]
    fn incomplete_or_wrong_taxonomy_membership_fails_closed() {
        let mut snapshot = HyperdashMembershipSnapshot {
            schema_version: 1,
            source: VERY_PROFITABLE_COHORT_URL.into(),
            cohort_id: VERY_PROFITABLE_COHORT_ID.into(),
            observed_at_ms: 1,
            displayed_min_all_time_pnl_usd: Decimal::from(100_000),
            displayed_max_all_time_pnl_usd: Decimal::from(1_000_000),
            completeness: MembershipCompleteness::Partial,
            reported_member_count: 2,
            captured_member_count: 1,
            wallets: vec![address(1)],
        };
        assert!(resolve_very_profitable_membership(&snapshot, vec![]).is_ok());
        snapshot.completeness = MembershipCompleteness::Complete;
        assert!(resolve_very_profitable_membership(&snapshot, vec![]).is_err());
        snapshot.reported_member_count = 1;
        snapshot.displayed_min_all_time_pnl_usd = Decimal::from(99_999);
        assert!(resolve_very_profitable_membership(&snapshot, vec![]).is_err());
    }

    fn quality_policy() -> WalletQualityPolicy {
        WalletQualityPolicy {
            maximum_recent_activity_age_ms: 168 * ONE_HOUR_MS,
            minimum_closed_trades: 50,
            minimum_realized_pnl_usd: Decimal::ZERO,
            minimum_annualized_sharpe: Decimal::from(10),
            target_annualized_sharpe: Decimal::from(15),
            minimum_win_rate_pct: Decimal::from(98),
            maximum_drawdown_pct: Decimal::from(8),
            minimum_account_equity_usd: Decimal::ZERO,
            maximum_observed_leverage: Decimal::new(875, 2),
            maximum_asset_concentration_pct: Decimal::from(65),
            confidence_decay_lambda: 0.32,
        }
    }

    fn quality_observation() -> WalletQualityObservation {
        WalletQualityObservation {
            address: address(1),
            recent_activity_age_ms: 1,
            closed_trades: 50,
            realized_pnl_usd: Decimal::ONE,
            annualized_sharpe: Decimal::from(15),
            win_rate_pct: Decimal::from(98),
            maximum_drawdown_pct: Decimal::from(8),
            account_equity_usd: Decimal::ONE,
            maximum_observed_leverage: Decimal::new(875, 2),
            maximum_margin_utilization_pct: Some(Decimal::from(90)),
            maximum_asset_concentration_pct: Decimal::from(65),
            median_holding_duration_ms: Some(1),
            retained_fill_count: 100,
            history_complete: true,
        }
    }

    #[test]
    fn quality_policy_uses_every_existing_metric_and_bounds_weight() {
        let accepted = apply_wallet_quality_policy(
            &quality_observation(),
            &quality_policy(),
            Decimal::from(2),
        )
        .unwrap();
        assert!(accepted.qualified);
        assert_eq!(accepted.bounded_wallet_weight, Decimal::new(98, 2));

        let mut rejected = quality_observation();
        rejected.annualized_sharpe = Decimal::from(9);
        rejected.maximum_margin_utilization_pct = None;
        rejected.median_holding_duration_ms = None;
        let rejected =
            apply_wallet_quality_policy(&rejected, &quality_policy(), Decimal::ONE).unwrap();
        assert!(!rejected.qualified);
        assert_eq!(rejected.bounded_wallet_weight, Decimal::ZERO);
        assert!(rejected.reasons.contains(&WalletFilterReason::Sharpe));
        assert!(rejected
            .reasons
            .contains(&WalletFilterReason::MissingMarginBehavior));
        assert!(rejected
            .reasons
            .contains(&WalletFilterReason::MissingHoldingDurationProfile));
    }

    #[test]
    fn established_high_density_pathway_keeps_retention_limited_profitable_wallets() {
        let mut observation = quality_observation();
        observation.retained_fill_count = HIGH_DENSITY_RETAINED_FILL_COUNT;
        observation.history_complete = false;
        observation.annualized_sharpe = Decimal::ZERO;
        observation.win_rate_pct = Decimal::from(55);
        observation.maximum_drawdown_pct = Decimal::from(50);
        let decision =
            apply_wallet_quality_policy(&observation, &quality_policy(), Decimal::ONE).unwrap();
        assert!(decision.qualified);
        assert_eq!(
            decision.qualification_pathway,
            WalletQualificationPathway::HighDensity
        );
        assert!(decision
            .standard_quality_reasons
            .contains(&WalletFilterReason::Sharpe));
        assert!(decision
            .standard_quality_reasons
            .contains(&WalletFilterReason::WinRate));
        assert!(decision
            .standard_quality_reasons
            .contains(&WalletFilterReason::Drawdown));
        assert!(decision.bounded_wallet_weight > Decimal::ZERO);

        observation.realized_pnl_usd = Decimal::ZERO;
        let rejected =
            apply_wallet_quality_policy(&observation, &quality_policy(), Decimal::ONE).unwrap();
        assert!(!rejected.qualified);
        assert!(rejected.reasons.contains(&WalletFilterReason::RealizedPnl));
    }

    fn wallet(
        index: u8,
        notional: i64,
        weight: Decimal,
        observed_at_ms: u64,
    ) -> FilteredCohortWallet {
        let mut positions = BTreeMap::new();
        if notional != 0 {
            positions.insert(
                "BTC".into(),
                AuthoritativeWalletPosition {
                    signed_notional: Decimal::from(notional),
                    entry_price: Decimal::from(100),
                    unrealized_pnl: Decimal::ZERO,
                },
            );
        }
        FilteredCohortWallet {
            address: address(index),
            observed_at_ms,
            bounded_wallet_weight: weight,
            realized_quality_score: weight,
            positions,
        }
    }

    #[test]
    fn cohort_position_is_seventy_percent_notional_and_thirty_percent_count() {
        let observed = 10;
        let members = [address(1), address(2), address(3)]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let aggregate = aggregate_authoritative_positions(
            "BTC",
            observed,
            &members,
            &[
                wallet(1, 90, Decimal::ONE, observed),
                wallet(2, -10, Decimal::ONE, observed),
                wallet(3, 0, Decimal::ONE, observed),
            ],
        )
        .unwrap();
        assert_eq!(aggregate.notional_bias, Decimal::new(8, 1));
        assert_eq!(aggregate.trader_count_bias, Decimal::ZERO);
        assert_eq!(aggregate.cohort_position, Decimal::new(56, 2));
        assert_eq!(aggregate.wallet_count_after_filtering, 3);
        assert_eq!(aggregate.positioned_wallet_count, 2);
    }

    #[test]
    fn raw_diagnostics_stay_authoritative_while_signal_notionals_are_quality_bounded() {
        let observed = 10;
        let members = [address(1), address(2)]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let aggregate = aggregate_authoritative_positions(
            "BTC",
            observed,
            &members,
            &[
                wallet(1, 100, Decimal::new(5, 1), observed),
                wallet(2, -40, Decimal::new(25, 2), observed),
            ],
        )
        .unwrap();

        assert_eq!(aggregate.long_notional, Decimal::from(100));
        assert_eq!(aggregate.short_notional, Decimal::from(40));
        assert_eq!(aggregate.weighted_long_notional, Decimal::from(50));
        assert_eq!(aggregate.weighted_short_notional, Decimal::from(10));
        assert_eq!(aggregate.notional_bias, Decimal::from(2) / Decimal::from(3));
        // Quality is weighted by the bounded wallet weights, rather than giving
        // a small low-confidence wallet the same vote as the larger one.
        assert_eq!(aggregate.weighted_realized_quality, Decimal::new(25, 2));
    }

    #[test]
    fn entry_opportunity_uses_only_the_direction_selected_by_cohort_position() {
        let observed = 10;
        let members = [address(1), address(2)]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut long = wallet(1, 90, Decimal::ONE, observed);
        long.positions.get_mut("BTC").unwrap().entry_price = Decimal::from(110);
        let mut short = wallet(2, -10, Decimal::ONE, observed);
        short.positions.get_mut("BTC").unwrap().entry_price = Decimal::from(50);
        let long_dominant =
            aggregate_authoritative_positions("BTC", observed, &members, &[long, short]).unwrap();
        assert!(long_dominant.cohort_position > Decimal::ZERO);
        assert_eq!(
            long_dominant.observed_entry_opportunity,
            Some(Decimal::from(110))
        );

        let mut long = wallet(1, 10, Decimal::ONE, observed);
        long.positions.get_mut("BTC").unwrap().entry_price = Decimal::from(110);
        let mut short = wallet(2, -90, Decimal::ONE, observed);
        short.positions.get_mut("BTC").unwrap().entry_price = Decimal::from(50);
        let short_dominant =
            aggregate_authoritative_positions("BTC", observed, &members, &[long, short]).unwrap();
        assert!(short_dominant.cohort_position < Decimal::ZERO);
        assert_eq!(
            short_dominant.observed_entry_opportunity,
            Some(Decimal::from(50))
        );
    }

    fn aggregate(at: u64, long: i64, short: i64) -> CohortAssetAggregate {
        let gross = long + short;
        let bias = if gross == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(long - short) / Decimal::from(gross)
        };
        CohortAssetAggregate {
            asset: "BTC".into(),
            observed_at_ms: at,
            wallet_count_after_filtering: 10,
            positioned_wallet_count: 10,
            long_notional: Decimal::from(long),
            short_notional: Decimal::from(short),
            weighted_long_notional: Decimal::from(long),
            weighted_short_notional: Decimal::from(short),
            long_trader_count: 8,
            short_trader_count: 2,
            notional_bias: bias,
            trader_count_bias: Decimal::new(6, 1),
            cohort_position: Decimal::new(8, 1),
            weighted_realized_quality: Decimal::new(8, 1),
            unrealized_profit_fraction: Decimal::ZERO,
            dominant_wallet_percentage: Decimal::from(20),
            observed_entry_opportunity: Some(Decimal::from(100)),
        }
    }

    fn signal_input(at: u64, long: i64, short: i64) -> CohortSignalInput {
        CohortSignalInput {
            membership: membership(),
            aggregate: aggregate(at, long, short),
            filtered_wallet_consensus: Decimal::new(8, 1),
            active_twap: Some(ActiveTwapContext {
                observed_at_ms: at,
                direction: 1,
                total_quantity: Decimal::from(10),
                executed_quantity: Decimal::from(2),
                remaining_quantity: Decimal::from(8),
                progress: Decimal::new(2, 1),
                nearly_completed: false,
            }),
            defer_market_context_admission: false,
            market_confirmation: Decimal::new(8, 1),
            penalties: CohortPenalties::default(),
            risk_flags: CohortRiskFlags::default(),
            position_change_age_ms: 1,
            maximum_position_change_age_ms: 75_000,
            dominant_wallet_limit_pct: Decimal::from(65),
            current_price: Decimal::from(101),
            r_unit: Decimal::from(10),
            estimated_round_trip_cost_fraction: Decimal::new(1, 3),
            expected_move_fraction: Decimal::new(5, 3),
            source_budget_fraction: Decimal::new(35, 2),
            existing_wallet_source_target: Decimal::ZERO,
            technical_target: Decimal::new(30, 2),
            rebalance_tolerance: Decimal::new(1, 2),
        }
    }

    #[test]
    fn impulses_and_composite_activate_once_per_material_asset_target() {
        let base = ONE_HOUR_MS + 10;
        let mut engine = VeryProfitableCohortEngine::default();
        let warmup = engine.evaluate(signal_input(base, 10, 90)).unwrap();
        assert_eq!(warmup.source_target, Decimal::ZERO);

        let five = engine
            .evaluate(signal_input(base + FIVE_MINUTES_MS, 40, 60))
            .unwrap();
        assert_eq!(five.impulse_5m, Decimal::new(6, 1));
        assert_eq!(five.impulse_15m, Decimal::ZERO);

        let fifteen = engine
            .evaluate(signal_input(base + FIFTEEN_MINUTES_MS, 70, 30))
            .unwrap();
        assert!(fifteen.impulse_15m > Decimal::ZERO);

        let one_hour = engine
            .evaluate(signal_input(base + ONE_HOUR_MS, 90, 10))
            .unwrap();
        assert!(one_hour.cohort_score >= COHORT_ACTIVATION_SCORE);
        assert!(one_hour.source_target > Decimal::ZERO);
        assert!(one_hour.independent_execution_root);
        let version = one_hour.target_version;

        let duplicate = engine
            .evaluate(signal_input(base + ONE_HOUR_MS, 90, 10))
            .unwrap();
        assert_eq!(duplicate.target_version, version);
        assert!(!duplicate.independent_execution_root);
        assert!(duplicate
            .reasons
            .contains(&CohortDecisionReason::ImmaterialRefresh));
    }

    #[test]
    fn agreement_chase_and_protection_flags_fail_closed_without_blocking_technical_target() {
        let base = ONE_HOUR_MS + 10;
        let mut engine = VeryProfitableCohortEngine::default();
        engine.evaluate(signal_input(base, 10, 90)).unwrap();
        engine
            .evaluate(signal_input(base + FIFTEEN_MINUTES_MS, 50, 50))
            .unwrap();
        let mut input = signal_input(base + ONE_HOUR_MS, 90, 10);
        input.current_price = Decimal::from(102);
        input.risk_flags.liquidation_cascade = true;
        let record = engine.evaluate(input).unwrap();
        assert_eq!(record.source_target, Decimal::ZERO);
        assert_eq!(record.combined_target, record.technical_target);
        assert!(record.reasons.contains(&CohortDecisionReason::ChaseLimit));
        assert!(record
            .reasons
            .contains(&CohortDecisionReason::LiquidationCascade));
    }

    #[test]
    fn deferred_market_context_cannot_veto_but_source_protections_remain() {
        let base = ONE_HOUR_MS + 10;
        let evaluate = |input: CohortSignalInput| {
            let mut engine = VeryProfitableCohortEngine::default();
            engine.evaluate(signal_input(base, 89, 11)).unwrap();
            engine.evaluate(input).unwrap()
        };
        let adverse_input = || {
            let mut input = signal_input(base + ONE_HOUR_MS, 90, 10);
            input.current_price = Decimal::from(108);
            input.aggregate.unrealized_profit_fraction = Decimal::new(1, 2);
            input.risk_flags.adverse_funding = true;
            input.estimated_round_trip_cost_fraction = Decimal::new(2, 3);
            input.expected_move_fraction = Decimal::new(1, 3);
            input
        };

        let legacy = evaluate(adverse_input());
        assert_eq!(legacy.source_target, Decimal::ZERO);
        for reason in [
            CohortDecisionReason::ChaseLimit,
            CohortDecisionReason::StaleUnrealizedProfit,
            CohortDecisionReason::AdverseFunding,
            CohortDecisionReason::ExpectedMoveDoesNotCoverCosts,
        ] {
            assert!(legacy.reasons.contains(&reason));
        }

        let mut deferred_input = adverse_input();
        deferred_input.defer_market_context_admission = true;
        let deferred = evaluate(deferred_input);
        assert!(deferred.source_target > Decimal::ZERO);
        assert_eq!(deferred.chase_distance_r, Some(Decimal::new(8, 1)));
        assert!(deferred
            .reasons
            .contains(&CohortDecisionReason::EligibleLong));
        for reason in [
            CohortDecisionReason::MissingEntryOpportunity,
            CohortDecisionReason::ChaseLimit,
            CohortDecisionReason::StaleUnrealizedProfit,
            CohortDecisionReason::AdverseFunding,
            CohortDecisionReason::ExpectedMoveDoesNotCoverCosts,
        ] {
            assert!(!deferred.reasons.contains(&reason));
        }

        let mut missing_market_context = adverse_input();
        missing_market_context.defer_market_context_admission = true;
        missing_market_context.current_price = Decimal::ZERO;
        missing_market_context.r_unit = Decimal::ZERO;
        missing_market_context.aggregate.observed_entry_opportunity = None;
        let missing_market_context = evaluate(missing_market_context);
        assert!(missing_market_context.source_target > Decimal::ZERO);
        assert_eq!(missing_market_context.chase_distance_r, None);

        let mut protected_input = adverse_input();
        protected_input.defer_market_context_admission = true;
        protected_input.filtered_wallet_consensus = -Decimal::new(8, 1);
        protected_input
            .risk_flags
            .incomplete_authoritative_wallet_state = true;
        protected_input.position_change_age_ms = protected_input.maximum_position_change_age_ms + 1;
        protected_input.aggregate.dominant_wallet_percentage = Decimal::from(90);
        let protected = evaluate(protected_input);
        assert_eq!(protected.source_target, Decimal::ZERO);
        for reason in [
            CohortDecisionReason::DirectionalDisagreement,
            CohortDecisionReason::IncompleteAuthoritativeWalletState,
            CohortDecisionReason::DominantWallet,
            CohortDecisionReason::StalePositionChange,
        ] {
            assert!(protected.reasons.contains(&reason));
        }
    }

    #[test]
    fn covering_vetoes_are_symmetric_and_never_block_initiation() {
        let short_pos = Decimal::new(-8, 1);
        let long_pos = Decimal::new(8, 1);
        // New SHORT initiation/expansion while shorts cover elsewhere: allowed.
        assert!(covering_vetoes(-1, short_pos, true, false).is_empty());
        // New LONG initiation/expansion while longs cover elsewhere: allowed.
        assert!(covering_vetoes(1, long_pos, false, true).is_empty());
        // Reducing/covering the side being covered: vetoed, symmetric.
        assert_eq!(
            covering_vetoes(0, short_pos, true, false),
            vec![CohortDecisionReason::ShortCovering]
        );
        assert_eq!(
            covering_vetoes(0, long_pos, false, true),
            vec![CohortDecisionReason::LongCovering]
        );
        assert_eq!(
            covering_vetoes(1, short_pos, true, false),
            vec![CohortDecisionReason::ShortCovering]
        );
        assert_eq!(
            covering_vetoes(-1, long_pos, false, true),
            vec![CohortDecisionReason::LongCovering]
        );
        // Opposite-side initiation is not a cover of this side.
        assert!(covering_vetoes(1, long_pos, true, false).is_empty());
        assert!(covering_vetoes(-1, short_pos, false, true).is_empty());
        // No covering signal: no veto either side.
        assert!(covering_vetoes(-1, short_pos, false, false).is_empty());
        assert!(covering_vetoes(1, long_pos, false, false).is_empty());
    }

    #[test]
    fn attribution_is_multi_label_and_preserves_disagreement_and_hybrid() {
        let tags = classify_attribution(Decimal::ONE, -Decimal::ONE, Decimal::ONE);
        assert!(tags.contains(&AttributionTag::SourceSetsDisagreeing));
        assert!(tags.contains(&AttributionTag::SourceTechnicalHybrid));
        assert_eq!(
            classify_attribution(Decimal::ZERO, Decimal::ZERO, Decimal::ONE),
            [AttributionTag::TechnicalOnly].into_iter().collect()
        );
    }

    #[test]
    fn existing_and_cohort_subsignals_share_one_capped_source_sleeve() {
        let base = ONE_HOUR_MS + 10;

        let mut existing_only_engine = VeryProfitableCohortEngine::default();
        let mut existing_only = signal_input(base, 10, 90);
        existing_only.existing_wallet_source_target = Decimal::new(8, 1);
        let existing_only = existing_only_engine.evaluate(existing_only).unwrap();
        assert_eq!(
            existing_only.existing_wallet_source_target,
            Decimal::new(28, 2)
        );
        assert_eq!(existing_only.very_profitable_cohort_target, Decimal::ZERO);
        assert_eq!(existing_only.source_target, Decimal::new(28, 2));
        assert!(existing_only
            .attribution
            .contains(&AttributionTag::ExistingWalletSourceOnly));

        let mut cohort_only_engine = VeryProfitableCohortEngine::default();
        cohort_only_engine
            .evaluate(signal_input(base, 10, 90))
            .unwrap();
        cohort_only_engine
            .evaluate(signal_input(base + FIFTEEN_MINUTES_MS, 50, 50))
            .unwrap();
        let cohort_only = cohort_only_engine
            .evaluate(signal_input(base + ONE_HOUR_MS, 90, 10))
            .unwrap();
        assert_eq!(cohort_only.existing_wallet_source_target, Decimal::ZERO);
        assert!(cohort_only.very_profitable_cohort_target > Decimal::ZERO);
        assert!(cohort_only
            .attribution
            .contains(&AttributionTag::VeryProfitableCohortOnly));

        let mut agreeing_engine = VeryProfitableCohortEngine::default();
        agreeing_engine
            .evaluate(signal_input(base, 10, 90))
            .unwrap();
        agreeing_engine
            .evaluate(signal_input(base + FIFTEEN_MINUTES_MS, 50, 50))
            .unwrap();
        let mut agreeing = signal_input(base + ONE_HOUR_MS, 90, 10);
        agreeing.existing_wallet_source_target = Decimal::ONE;
        let agreeing = agreeing_engine.evaluate(agreeing).unwrap();
        assert_eq!(agreeing.source_target, Decimal::new(35, 2));
        assert!(agreeing
            .attribution
            .contains(&AttributionTag::BothSourceSetsAgreeing));
    }

    #[test]
    fn rejected_context_changes_do_not_create_execution_roots() {
        let base = ONE_HOUR_MS + 10;
        let mut engine = VeryProfitableCohortEngine::default();
        let mut first_input = signal_input(base, 10, 90);
        first_input.risk_flags.liquidation_cascade = true;
        let first = engine.evaluate(first_input).unwrap();
        assert!(!first.independent_execution_root);
        let mut changed_input = signal_input(base + FIVE_MINUTES_MS, 20, 80);
        changed_input.risk_flags.liquidation_cascade = true;
        let changed = engine.evaluate(changed_input).unwrap();
        assert_eq!(changed.source_target, Decimal::ZERO);
        assert!(!changed.independent_execution_root);
    }

    #[test]
    fn impulse_requires_a_fresh_horizon_bracket() {
        let current = AggregateFrame {
            observed_at_ms: 2 * ONE_HOUR_MS,
            net_notional: Decimal::from(100),
            gross_notional: Decimal::from(100),
        };
        let history = VecDeque::from([AggregateFrame {
            observed_at_ms: FIVE_MINUTES_MS,
            net_notional: Decimal::ZERO,
            gross_notional: Decimal::from(100),
        }]);
        assert_eq!(
            impulse_at(&history, &current, FIVE_MINUTES_MS, 75_000).unwrap(),
            Decimal::ZERO
        );
    }

    #[test]
    fn impulse_uses_quality_bounded_notionals_not_raw_diagnostics() {
        let base = ONE_HOUR_MS + 10;
        let mut engine = VeryProfitableCohortEngine::default();
        let mut initial = signal_input(base, 1_000, 0);
        initial.aggregate.weighted_long_notional = Decimal::from(10);
        engine.evaluate(initial).unwrap();

        let mut changed = signal_input(base + FIVE_MINUTES_MS, 1_000, 0);
        changed.aggregate.weighted_long_notional = Decimal::from(20);
        let record = engine.evaluate(changed).unwrap();
        assert_eq!(record.impulse_5m, Decimal::new(5, 1));
    }
}
#[test]
fn attenuation_never_emits_noncanonical_negative_zero() {
    let attenuated = attenuate_magnitude(Decimal::new(-1, 6), Decimal::ONE);
    assert_eq!(attenuated, Decimal::ZERO);
    assert_eq!(attenuated.to_string(), "0");
    assert_eq!(serde_json::to_string(&attenuated).unwrap(), "\"0\"");
}
