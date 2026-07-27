use crate::technical::TechnicalStrategyConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
        Ok(())
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub dynamic_slippage_enabled: bool,
    pub max_slippage_bps: f64,
    pub min_order_usd: f64,
    pub max_order_usd: f64,
    pub latency_timeout_ms: u64,
    pub order_rate_limit_per_sec: f64,
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
        assert_eq!(config.candidates.len(), 169);
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
