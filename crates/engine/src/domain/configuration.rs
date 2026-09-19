use crate::domain::cohort::{
    MembershipCompleteness, WalletQualityPolicy, VERY_PROFITABLE_COHORT_ID,
};
use crate::domain::technical::TechnicalStrategyConfig;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

pub const LIVE_CONFIG_SCHEMA_VERSION: u32 = 2;
const MIN_APPROVED_GLOBAL_RISK_SCALE: f64 = 0.025;
const MAX_APPROVED_GLOBAL_RISK_SCALE: f64 = 0.10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(String);

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CopyTradeConfig {
    pub schema_version: u32,
    pub global_risk: GlobalRiskConfig,
    pub unlimited_mode: bool,
    pub starting_equity_usd: f64,
    pub min_win_rate_pct: f64,
    pub max_active_traders: Option<usize>,
    pub min_closed_trades: usize,
    pub vetting_lookback_days: i64,
    pub min_closed_pnl_usd: f64,
    pub poll_interval_secs: u64,
    pub max_account_drawdown_pct: f64,
    #[serde(alias = "global_max_leverage")]
    pub max_total_leverage: f64,
    pub leverage_curve: Vec<LeverageCurvePoint>,
    #[serde(alias = "per_asset_max_exposure_pct")]
    pub max_asset_notional_pct: f64,
    #[serde(alias = "max_order_usd")]
    pub max_single_order_notional_usd: f64,
    #[serde(alias = "min_order_usd")]
    pub min_order_notional_usd: f64,
    pub min_trader_confidence_score: f64,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    #[serde(alias = "max_slippage_bps")]
    pub slippage_buffer_bps: f64,
    pub dynamic_slippage_enabled: bool,
    pub latency_timeout_ms: u64,
    pub order_rate_limit_per_sec: f64,
    pub sharpe_target: f64,
    pub sharpe_lookback_hours: i64,
    pub min_sharpe_samples: usize,
    pub enforce_sharpe_gate: bool,
    pub trader_sharpe_floor: f64,
    pub confidence_decay_lambda: f64,
    pub ledger_path: String,
    pub prune_inactive_trader_hours: i64,
    pub follower_address: Option<String>,
    pub vetting: VettingConfig,
    pub execution: ExecutionConfig,
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub technical: TechnicalStrategyConfig,
    /// Runtime-derived identity for an installed discovery snapshot. The
    /// artifact path is deliberately excluded; its content hash and immutable
    /// membership identity are included in the strategy/configuration hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub very_profitable_layer: Option<VeryProfitableLayerIdentity>,
    /// High-density source wallets retained as flow context even when the
    /// ordinary quality gate rejects them. They may contribute only when an
    /// independently active technical target agrees with their direction.
    /// The value records cohort provenance without creating duplicate votes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub technical_gated_high_density_wallets: BTreeMap<String, BTreeSet<String>>,
    pub candidates: Vec<TraderCandidate>,
}

impl CopyTradeConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path)?;
        let config = serde_json::from_str::<Self>(&text)?;
        config.validate_production()?;
        Ok(config)
    }

    pub fn validate_production(&self) -> Result<(), ConfigError> {
        if self.schema_version != LIVE_CONFIG_SCHEMA_VERSION {
            return Err(ConfigError::new(format!(
                "copytrade schema_version must be {LIVE_CONFIG_SCHEMA_VERSION}"
            )));
        }
        self.global_risk.validate_production()?;
        self.technical.validate().map_err(|error| {
            ConfigError::new(format!("invalid technical configuration: {error:?}"))
        })?;
        if let Some(identity) = &self.very_profitable_layer {
            identity.validate()?;
        }
        self.validate_numeric_fields()?;
        if self.candidates.is_empty() || self.candidates.iter().all(|candidate| !candidate.enabled)
        {
            return Err(ConfigError::new(
                "candidate universe must contain at least one enabled candidate",
            ));
        }

        let mut addresses = BTreeSet::new();
        for candidate in &self.candidates {
            candidate.validate_production()?;
            let normalized = candidate.address.to_ascii_lowercase();
            if !addresses.insert(normalized) {
                return Err(ConfigError::new(format!(
                    "duplicate candidate address: {}",
                    candidate.address
                )));
            }
        }
        for (address, cohorts) in &self.technical_gated_high_density_wallets {
            let normalized = address.to_ascii_lowercase();
            if !valid_hyperliquid_address(address)
                || !addresses.contains(&normalized)
                || cohorts.is_empty()
                || cohorts.iter().any(|cohort| {
                    cohort != "extremely_profitable" && cohort != VERY_PROFITABLE_COHORT_ID
                })
            {
                return Err(ConfigError::new(format!(
                    "invalid technical-gated high-density wallet metadata: {address}"
                )));
            }
        }
        Ok(())
    }

    pub fn is_technical_gated_high_density(&self, address: &str) -> bool {
        self.technical_gated_high_density_wallets
            .contains_key(&address.to_ascii_lowercase())
    }

    pub fn deterministic_fingerprint(&self) -> Result<String, ConfigError> {
        let bytes = serde_json::to_vec(self)?;
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        Ok(format!("fnv1a64:{hash:016x}"))
    }

    /// Projects the already-approved wallet cutoffs into the cohort filter.
    /// No cohort-specific thresholds are introduced here.
    pub fn cohort_wallet_quality_policy(&self) -> Result<WalletQualityPolicy, ConfigError> {
        let maximum_recent_activity_age_ms = u64::try_from(
            self.prune_inactive_trader_hours
                .checked_mul(60 * 60 * 1_000)
                .ok_or_else(|| ConfigError::new("cohort activity horizon overflow"))?,
        )
        .map_err(|_| ConfigError::new("cohort activity horizon must be positive"))?;
        let decimal = |name: &str, value: f64| {
            Decimal::from_f64(value)
                .ok_or_else(|| ConfigError::new(format!("{name} is not representable")))
        };
        let policy = WalletQualityPolicy {
            maximum_recent_activity_age_ms,
            minimum_closed_trades: self.min_closed_trades,
            minimum_realized_pnl_usd: decimal("minimum realized pnl", self.min_closed_pnl_usd)?,
            minimum_annualized_sharpe: decimal(
                "minimum annualized sharpe",
                self.trader_sharpe_floor,
            )?,
            target_annualized_sharpe: decimal("target annualized sharpe", self.sharpe_target)?,
            minimum_win_rate_pct: decimal("minimum win rate", self.min_win_rate_pct)?,
            maximum_drawdown_pct: decimal(
                "maximum account drawdown",
                self.max_account_drawdown_pct,
            )?,
            minimum_account_equity_usd: Decimal::ZERO,
            maximum_observed_leverage: decimal(
                "maximum observed leverage",
                self.max_total_leverage,
            )?,
            maximum_asset_concentration_pct: decimal(
                "maximum asset concentration",
                self.max_asset_notional_pct,
            )?,
            confidence_decay_lambda: self.confidence_decay_lambda,
        };
        policy
            .validate()
            .map_err(|_| ConfigError::new("approved cohort quality policy is invalid"))?;
        Ok(policy)
    }

    fn validate_numeric_fields(&self) -> Result<(), ConfigError> {
        for (name, value) in [
            ("starting_equity_usd", self.starting_equity_usd),
            ("min_closed_pnl_usd", self.min_closed_pnl_usd),
            ("maker_fee_bps", self.maker_fee_bps),
            ("taker_fee_bps", self.taker_fee_bps),
            ("slippage_buffer_bps", self.slippage_buffer_bps),
            ("sharpe_target", self.sharpe_target),
            ("trader_sharpe_floor", self.trader_sharpe_floor),
            ("confidence_decay_lambda", self.confidence_decay_lambda),
            (
                "execution.max_slippage_bps",
                self.execution.max_slippage_bps,
            ),
            ("execution.min_order_usd", self.execution.min_order_usd),
            ("execution.max_order_usd", self.execution.max_order_usd),
            (
                "execution.order_rate_limit_per_sec",
                self.execution.order_rate_limit_per_sec,
            ),
            ("performance.target_sharpe", self.performance.target_sharpe),
            (
                "performance.trader_sharpe_floor",
                self.performance.trader_sharpe_floor,
            ),
            (
                "performance.confidence_decay_lambda",
                self.performance.confidence_decay_lambda,
            ),
        ] {
            finite(name, value)?;
        }
        finite_range("min_win_rate_pct", self.min_win_rate_pct, 0.0, 100.0)?;
        finite_range(
            "min_trader_confidence_score",
            self.min_trader_confidence_score,
            0.0,
            100.0,
        )?;
        finite_range(
            "max_account_drawdown_pct",
            self.max_account_drawdown_pct,
            0.0,
            100.0,
        )?;
        finite_range(
            "max_asset_notional_pct",
            self.max_asset_notional_pct,
            0.0,
            100.0,
        )?;
        finite_range("taker_fee_bps", self.taker_fee_bps, 0.0, 100.0)?;
        finite_range(
            "execution.fee_multiplier",
            self.execution.fee_multiplier,
            3.0,
            5.0,
        )?;
        if self.starting_equity_usd <= 0.0
            || !self.max_total_leverage.is_finite()
            || self.max_total_leverage <= 0.0
            || !self.max_single_order_notional_usd.is_finite()
            || self.max_single_order_notional_usd <= 0.0
            || !self.min_order_notional_usd.is_finite()
            || self.min_order_notional_usd <= 0.0
            || !self.order_rate_limit_per_sec.is_finite()
            || self.order_rate_limit_per_sec <= 0.0
        {
            return Err(ConfigError::new(
                "equity, leverage, order notionals, and order rate must be finite and positive",
            ));
        }
        if self.poll_interval_secs == 0
            || self.latency_timeout_ms == 0
            || self.vetting_lookback_days <= 0
            || self.sharpe_lookback_hours <= 0
            || self.min_sharpe_samples == 0
        {
            return Err(ConfigError::new(
                "polling, latency, lookback, and sample limits must be positive",
            ));
        }
        if self.leverage_curve.is_empty()
            || self.leverage_curve.iter().any(|point| {
                !point.equity_usd.is_finite()
                    || point.equity_usd <= 0.0
                    || !point.target_leverage.is_finite()
                    || point.target_leverage <= 0.0
            })
        {
            return Err(ConfigError::new(
                "leverage_curve requires finite positive points",
            ));
        }
        if (self.min_order_notional_usd - self.global_risk.min_order_notional_usd).abs()
            > f64::EPSILON
            || (self.execution.min_order_usd - self.global_risk.min_order_notional_usd).abs()
                > f64::EPSILON
        {
            return Err(ConfigError::new(
                "global_risk, top-level, and execution minimum order notionals must match",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VeryProfitableLayerIdentity {
    pub cohort_id: String,
    pub cohort_snapshot_timestamp_ms: u64,
    pub membership_completeness: MembershipCompleteness,
    pub reported_member_count: usize,
    pub captured_member_count: usize,
    pub membership_set_hash: String,
    pub artifact_sha256: String,
}

impl VeryProfitableLayerIdentity {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.cohort_id != VERY_PROFITABLE_COHORT_ID
            || self.cohort_snapshot_timestamp_ms == 0
            || self.captured_member_count == 0
            || self.reported_member_count < self.captured_member_count
            || match self.membership_completeness {
                MembershipCompleteness::Complete => {
                    self.reported_member_count != self.captured_member_count
                }
                MembershipCompleteness::Partial => {
                    self.reported_member_count <= self.captured_member_count
                }
            }
            || !valid_sha256_hex(&self.membership_set_hash)
            || !valid_sha256_hex(&self.artifact_sha256)
        {
            return Err(ConfigError::new(
                "very_profitable layer identity is malformed",
            ));
        }
        Ok(())
    }
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalRiskConfig {
    pub global_risk_scale: f64,
    pub max_single_asset_equity_pct: f64,
    pub max_net_equity_pct: f64,
    pub max_source_exposure: f64,
    pub min_order_notional_usd: f64,
    pub order_rounding_buffer_usd: f64,
    pub closeability_margin_usd: f64,
    pub slot_rank_hysteresis: f64,
    pub source_snapshot_max_age_ms: u64,
}

impl GlobalRiskConfig {
    fn validate_production(&self) -> Result<(), ConfigError> {
        finite_range(
            "global_risk.global_risk_scale",
            self.global_risk_scale,
            MIN_APPROVED_GLOBAL_RISK_SCALE,
            MAX_APPROVED_GLOBAL_RISK_SCALE,
        )?;
        finite_range(
            "global_risk.max_single_asset_equity_pct",
            self.max_single_asset_equity_pct,
            0.0,
            1.0,
        )?;
        finite_range(
            "global_risk.max_net_equity_pct",
            self.max_net_equity_pct,
            0.0,
            1.0,
        )?;
        finite_range(
            "global_risk.max_source_exposure",
            self.max_source_exposure,
            0.0,
            1.0,
        )?;
        if !self.min_order_notional_usd.is_finite() || self.min_order_notional_usd <= 0.0 {
            return Err(ConfigError::new(
                "global_risk.min_order_notional_usd must be finite and positive",
            ));
        }
        if self.min_order_notional_usd < 10.0
            || !self.order_rounding_buffer_usd.is_finite()
            || self.order_rounding_buffer_usd <= 0.0
            || !self.closeability_margin_usd.is_finite()
            || self.closeability_margin_usd < 0.0
            || !self.slot_rank_hysteresis.is_finite()
            || !(0.0..=1.0).contains(&self.slot_rank_hysteresis)
        {
            return Err(ConfigError::new(
                "dynamic execution-floor and slot-hysteresis policy is invalid",
            ));
        }
        if self.source_snapshot_max_age_ms == 0 {
            return Err(ConfigError::new(
                "global_risk.source_snapshot_max_age_ms must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraderCandidate {
    pub address: String,
    pub label: String,
    pub allocation_weight: f64,
    pub confidence_modifier: Option<f64>,
    pub enabled: bool,
}

impl TraderCandidate {
    fn validate_production(&self) -> Result<(), ConfigError> {
        if !valid_hyperliquid_address(&self.address) {
            return Err(ConfigError::new(format!(
                "invalid candidate address: {}",
                self.address
            )));
        }
        if !self.allocation_weight.is_finite() || self.allocation_weight < 0.0 {
            return Err(ConfigError::new(format!(
                "allocation_weight must be finite and non-negative for {}",
                self.address
            )));
        }
        let confidence = self.confidence_modifier.ok_or_else(|| {
            ConfigError::new(format!(
                "confidence_modifier is required for {}",
                self.address
            ))
        })?;
        finite_range(
            &format!("confidence_modifier for {}", self.address),
            confidence,
            0.0,
            1.0,
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VettingConfig {
    pub min_win_rate_pct: f64,
    pub min_closed_trades: usize,
    pub lookback_days: i64,
}

fn default_fee_multiplier() -> f64 {
    4.0
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub dynamic_slippage_enabled: bool,
    pub max_slippage_bps: f64,
    pub min_order_usd: f64,
    pub max_order_usd: f64,
    pub latency_timeout_ms: u64,
    pub order_rate_limit_per_sec: f64,
    /// Conservative multiplier applied to expected fees for discretionary
    /// policy hurdles only. Never contaminates settled accounting, labels,
    /// or reconciliation truth. Supported range 3.0..=5.0, default 4.0.
    #[serde(default = "default_fee_multiplier")]
    pub fee_multiplier: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LeverageCurvePoint {
    pub equity_usd: f64,
    pub target_leverage: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceConfig {
    pub target_sharpe: f64,
    pub lookback_hours: i64,
    pub min_samples: usize,
    pub enforce_sharpe_gate: Option<bool>,
    pub trader_sharpe_floor: f64,
    pub confidence_decay_lambda: f64,
    pub prune_inactive_trader_hours: i64,
}

fn valid_hyperliquid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && value[2..].bytes().any(|byte| byte != b'0')
}

fn finite(name: &str, value: f64) -> Result<(), ConfigError> {
    if !value.is_finite() {
        return Err(ConfigError::new(format!("{name} must be finite")));
    }
    Ok(())
}

fn finite_range(name: &str, value: f64, minimum: f64, maximum: f64) -> Result<(), ConfigError> {
    if !value.is_finite() || !(minimum..=maximum).contains(&value) {
        return Err(ConfigError::new(format!(
            "{name} must be finite and within [{minimum}, {maximum}]"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn production_config() -> CopyTradeConfig {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json");
        CopyTradeConfig::from_path(path).unwrap()
    }

    #[test]
    fn production_configuration_is_explicit_and_valid() {
        let config = production_config();
        assert_eq!(config.schema_version, LIVE_CONFIG_SCHEMA_VERSION);
        assert_eq!(config.candidates.len(), 375);
        assert_eq!(config.starting_equity_usd, 100.0);
        assert_eq!(config.global_risk.global_risk_scale, 0.10);
        assert_eq!(config.global_risk.min_order_notional_usd, 10.0);
        assert!(config.global_risk.order_rounding_buffer_usd > 0.0);
        assert!(config.global_risk.closeability_margin_usd > 0.0);
        assert!(config
            .candidates
            .iter()
            .all(|candidate| candidate.confidence_modifier.is_some()));
    }

    #[test]
    fn deterministic_fingerprint_is_stable() {
        let config = production_config();
        assert_eq!(
            config.deterministic_fingerprint().unwrap(),
            config.clone().deterministic_fingerprint().unwrap()
        );
    }

    #[test]
    fn cohort_policy_reuses_only_approved_wallet_cutoffs() {
        let config = production_config();
        let policy = config.cohort_wallet_quality_policy().unwrap();
        assert_eq!(policy.minimum_closed_trades, config.min_closed_trades);
        assert_eq!(
            policy.minimum_win_rate_pct,
            Decimal::from_f64(config.min_win_rate_pct).unwrap()
        );
        assert_eq!(
            policy.minimum_annualized_sharpe,
            Decimal::from_f64(config.trader_sharpe_floor).unwrap()
        );
        assert_eq!(
            policy.maximum_drawdown_pct,
            Decimal::from_f64(config.max_account_drawdown_pct).unwrap()
        );
        assert_eq!(
            policy.maximum_observed_leverage,
            Decimal::from_f64(config.max_total_leverage).unwrap()
        );
        assert_eq!(
            policy.maximum_asset_concentration_pct,
            Decimal::from_f64(config.max_asset_notional_pct).unwrap()
        );
    }

    #[test]
    fn technical_gated_high_density_metadata_must_reference_a_candidate() {
        let mut config = production_config();
        let address = config.candidates[0].address.to_ascii_lowercase();
        config.technical_gated_high_density_wallets.insert(
            address,
            ["extremely_profitable".to_string()].into_iter().collect(),
        );
        assert!(config.validate_production().is_ok());
        config.technical_gated_high_density_wallets.insert(
            "0x0000000000000000000000000000000000000001".into(),
            [VERY_PROFITABLE_COHORT_ID.to_string()]
                .into_iter()
                .collect(),
        );
        assert!(config.validate_production().is_err());
    }

    #[test]
    fn missing_confidence_fails_closed() {
        let mut config = production_config();
        config.candidates[0].confidence_modifier = None;
        assert!(config.validate_production().is_err());
    }

    #[test]
    fn duplicate_candidate_fails_closed() {
        let mut config = production_config();
        config.candidates[1].address = config.candidates[0].address.to_ascii_uppercase();
        config.candidates[1].address.replace_range(..2, "0x");
        assert!(config.validate_production().is_err());
    }
}
