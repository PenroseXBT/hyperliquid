use chrono::Utc;
use dotenv::dotenv;
use ethers::signers::{LocalWallet, Signer};
use futures_util::stream::{FuturesUnordered, StreamExt};
use hyperliquid_rust_sdk::{
    BaseUrl, ClientLimit, ClientOrder, ClientOrderRequest, ExchangeClient, ExchangeResponseStatus,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    error::Error,
    fs,
    hash::{Hash, Hasher},
    io::{self, IsTerminal, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep, timeout},
};
use uuid::Uuid;

const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const HOURS_PER_YEAR: f64 = 365.25 * 24.0;
const LIVE_CONFIG_SCHEMA_VERSION: u32 = 2;
const MIN_APPROVED_GLOBAL_RISK_SCALE: f64 = 0.025;
const MAX_APPROVED_GLOBAL_RISK_SCALE: f64 = 0.10;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CopyTradeConfig {
    pub schema_version: Option<u32>,
    pub global_risk: Option<GlobalRiskConfig>,
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
    pub candidates: Vec<TraderCandidate>,
}

impl Default for CopyTradeConfig {
    fn default() -> Self {
        Self {
            schema_version: None,
            global_risk: None,
            unlimited_mode: true,
            starting_equity_usd: 100.0,
            min_win_rate_pct: 98.0,
            max_active_traders: Some(0),
            min_closed_trades: 50,
            vetting_lookback_days: 14,
            min_closed_pnl_usd: 0.0,
            poll_interval_secs: 20,
            max_account_drawdown_pct: 8.0,
            max_total_leverage: 8.0,
            leverage_curve: default_leverage_curve(),
            max_asset_notional_pct: 65.0,
            max_single_order_notional_usd: 2_500.0,
            min_order_notional_usd: 10.0,
            min_trader_confidence_score: 95.0,
            maker_fee_bps: 1.5,
            taker_fee_bps: 4.5,
            slippage_buffer_bps: 4.0,
            dynamic_slippage_enabled: true,
            latency_timeout_ms: 200,
            order_rate_limit_per_sec: 5.0,
            sharpe_target: 15.0,
            sharpe_lookback_hours: 96,
            min_sharpe_samples: 50,
            enforce_sharpe_gate: true,
            trader_sharpe_floor: 10.0,
            confidence_decay_lambda: 0.32,
            ledger_path: "config/ledger.json".to_string(),
            prune_inactive_trader_hours: 168,
            follower_address: None,
            vetting: VettingConfig::default(),
            execution: ExecutionConfig::default(),
            performance: PerformanceConfig::default(),
            candidates: Vec::new(),
        }
    }
}

impl CopyTradeConfig {
    pub fn from_path(path: &str) -> Result<Self, Box<dyn Error>> {
        let text = fs::read_to_string(path)?;
        let mut config = serde_json::from_str::<Self>(&text)?;
        config.normalize_nested_overrides();
        Ok(config)
    }

    pub fn validate_live(&self) -> Result<(), Box<dyn Error>> {
        if self.schema_version != Some(LIVE_CONFIG_SCHEMA_VERSION) {
            return Err(format!(
                "copytrade schema_version must be explicitly set to {LIVE_CONFIG_SCHEMA_VERSION}"
            )
            .into());
        }
        let risk = self
            .global_risk
            .as_ref()
            .ok_or("global_risk is required in live mode")?;
        risk.validate_live()?;
        self.validate_live_numeric_fields()?;
        if self.candidates.is_empty() || self.candidates.iter().all(|candidate| !candidate.enabled)
        {
            return Err(
                "live candidate universe must contain at least one enabled candidate".into(),
            );
        }

        let mut addresses = HashSet::with_capacity(self.candidates.len());
        for candidate in &self.candidates {
            validate_live_candidate(candidate)?;
            let normalized = candidate.address.to_ascii_lowercase();
            if !addresses.insert(normalized) {
                return Err(format!("duplicate candidate address: {}", candidate.address).into());
            }
        }
        Ok(())
    }

    fn validate_live_numeric_fields(&self) -> Result<(), Box<dyn Error>> {
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
            if !value.is_finite() {
                return Err(format!("{name} must be finite").into());
            }
        }
        ensure_finite_in_range("min_win_rate_pct", self.min_win_rate_pct, 0.0, 100.0)?;
        ensure_finite_in_range(
            "min_trader_confidence_score",
            self.min_trader_confidence_score,
            0.0,
            100.0,
        )?;
        ensure_finite_in_range(
            "max_account_drawdown_pct",
            self.max_account_drawdown_pct,
            0.0,
            100.0,
        )?;
        ensure_finite_in_range(
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
            return Err(
                "equity, leverage, order notionals, and order rate must be finite and positive"
                    .into(),
            );
        }
        if self.poll_interval_secs == 0
            || self.latency_timeout_ms == 0
            || self.vetting_lookback_days <= 0
            || self.sharpe_lookback_hours <= 0
            || self.min_sharpe_samples == 0
        {
            return Err("polling, latency, lookback, and sample limits must be positive".into());
        }
        if self.leverage_curve.is_empty()
            || self.leverage_curve.iter().any(|point| {
                !point.equity_usd.is_finite()
                    || point.equity_usd <= 0.0
                    || !point.target_leverage.is_finite()
                    || point.target_leverage <= 0.0
            })
        {
            return Err("leverage_curve requires finite positive points".into());
        }
        let risk_minimum = self
            .global_risk
            .as_ref()
            .expect("validated global risk must exist")
            .min_order_notional_usd;
        if (self.min_order_notional_usd - risk_minimum).abs() > f64::EPSILON
            || (self.execution.min_order_usd - risk_minimum).abs() > f64::EPSILON
        {
            return Err(
                "global_risk, top-level, and execution minimum order notionals must match".into(),
            );
        }
        Ok(())
    }

    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        dotenv().ok();

        let path =
            env::var("COPYTRADE_CONFIG").unwrap_or_else(|_| "config/copytrade.json".to_string());
        let mut config = if fs::metadata(&path).is_ok() {
            Self::from_path(&path)?
        } else {
            Self::default()
        };
        config.normalize_nested_overrides();

        config.starting_equity_usd =
            env_f64("COPYTRADE_STARTING_EQUITY_USD", config.starting_equity_usd);
        config.min_win_rate_pct = env_f64("COPYTRADE_MIN_WIN_RATE_PCT", config.min_win_rate_pct);
        config.max_account_drawdown_pct = env_f64(
            "COPYTRADE_MAX_ACCOUNT_DRAWDOWN_PCT",
            config.max_account_drawdown_pct,
        );
        config.max_total_leverage =
            env_f64("COPYTRADE_MAX_TOTAL_LEVERAGE", config.max_total_leverage);
        config.max_total_leverage =
            env_f64("COPYTRADE_GLOBAL_MAX_LEVERAGE", config.max_total_leverage);
        config.max_asset_notional_pct = env_f64(
            "COPYTRADE_PER_ASSET_MAX_EXPOSURE_PCT",
            config.max_asset_notional_pct,
        );
        config.min_trader_confidence_score = env_f64(
            "COPYTRADE_MIN_TRADER_CONFIDENCE",
            config.min_trader_confidence_score,
        );
        config.min_closed_trades =
            env_usize("COPYTRADE_MIN_CLOSED_TRADES", config.min_closed_trades);
        config.vetting_lookback_days = env_i64(
            "COPYTRADE_VETTING_LOOKBACK_DAYS",
            config.vetting_lookback_days,
        );
        config.min_closed_pnl_usd =
            env_f64("COPYTRADE_MIN_CLOSED_PNL_USD", config.min_closed_pnl_usd);
        config.maker_fee_bps = env_f64("COPYTRADE_MAKER_FEE_BPS", config.maker_fee_bps);
        config.taker_fee_bps = env_f64("COPYTRADE_TAKER_FEE_BPS", config.taker_fee_bps);
        config.unlimited_mode = env_bool("COPYTRADE_UNLIMITED_MODE", config.unlimited_mode);
        config.dynamic_slippage_enabled = env_bool(
            "COPYTRADE_DYNAMIC_SLIPPAGE",
            config.dynamic_slippage_enabled,
        );
        config.latency_timeout_ms =
            env_u64("COPYTRADE_LATENCY_TIMEOUT_MS", config.latency_timeout_ms);
        config.order_rate_limit_per_sec = env_f64(
            "COPYTRADE_ORDER_RATE_LIMIT_PER_SEC",
            config.order_rate_limit_per_sec,
        );
        config.sharpe_target = env_f64("COPYTRADE_SHARPE_TARGET", config.sharpe_target);
        config.sharpe_lookback_hours = env_i64(
            "COPYTRADE_SHARPE_LOOKBACK_HOURS",
            config.sharpe_lookback_hours,
        );
        config.min_sharpe_samples =
            env_usize("COPYTRADE_MIN_SHARPE_SAMPLES", config.min_sharpe_samples);
        config.enforce_sharpe_gate =
            env_bool("COPYTRADE_ENFORCE_SHARPE_GATE", config.enforce_sharpe_gate);
        config.trader_sharpe_floor =
            env_f64("COPYTRADE_TRADER_SHARPE_FLOOR", config.trader_sharpe_floor);
        config.confidence_decay_lambda = env_f64(
            "COPYTRADE_CONFIDENCE_DECAY_LAMBDA",
            config.confidence_decay_lambda,
        );
        config.ledger_path = env::var("COPYTRADE_LEDGER_PATH").unwrap_or(config.ledger_path);
        config.prune_inactive_trader_hours = env_i64(
            "COPYTRADE_PRUNE_INACTIVE_TRADER_HOURS",
            config.prune_inactive_trader_hours,
        );
        config.follower_address = env::var("COPYTRADE_FOLLOWER_ADDRESS")
            .ok()
            .or(config.follower_address);

        config.validate_live()?;
        Ok(config)
    }
    fn active_limit(&self) -> usize {
        if self.unlimited_mode || self.max_active_traders == Some(0) {
            usize::MAX
        } else {
            self.max_active_traders.unwrap_or(usize::MAX)
        }
    }

    fn target_leverage_for_equity(&self, equity_usd: f64) -> f64 {
        let mut curve = self
            .leverage_curve
            .iter()
            .filter(|point| point.equity_usd > 0.0 && point.target_leverage > 0.0)
            .cloned()
            .collect::<Vec<_>>();
        curve.sort_by(|a, b| {
            a.equity_usd
                .partial_cmp(&b.equity_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let curve_leverage = match curve.as_slice() {
            [] => self.max_total_leverage,
            [only] => only.target_leverage,
            points if equity_usd <= points[0].equity_usd => points[0].target_leverage,
            points if equity_usd >= points[points.len() - 1].equity_usd => {
                points[points.len() - 1].target_leverage
            }
            points => points
                .windows(2)
                .find_map(|window| {
                    let left = &window[0];
                    let right = &window[1];
                    if equity_usd < left.equity_usd || equity_usd > right.equity_usd {
                        return None;
                    }

                    let left_x = left.equity_usd.ln();
                    let right_x = right.equity_usd.ln();
                    let equity_x = equity_usd.max(1.0).ln();
                    let progress = if right_x > left_x {
                        (equity_x - left_x) / (right_x - left_x)
                    } else {
                        0.0
                    }
                    .clamp(0.0, 1.0);

                    Some(
                        left.target_leverage
                            + (right.target_leverage - left.target_leverage) * progress,
                    )
                })
                .unwrap_or(self.max_total_leverage),
        };

        curve_leverage.min(self.max_total_leverage).max(0.0)
    }

    fn normalize_nested_overrides(&mut self) {
        if self.vetting.min_win_rate_pct > 0.0 {
            self.min_win_rate_pct = self.vetting.min_win_rate_pct;
        }
        if self.vetting.min_closed_trades > 0 {
            self.min_closed_trades = self.vetting.min_closed_trades;
        }
        if self.vetting.lookback_days > 0 {
            self.vetting_lookback_days = self.vetting.lookback_days;
        }

        if self.execution.max_slippage_bps > 0.0 {
            self.slippage_buffer_bps = self.execution.max_slippage_bps;
        }
        if self.execution.min_order_usd > 0.0 {
            self.min_order_notional_usd = self.execution.min_order_usd;
        }
        if self.execution.max_order_usd > 0.0 {
            self.max_single_order_notional_usd = self.execution.max_order_usd;
        }
        if self.execution.latency_timeout_ms > 0 {
            self.latency_timeout_ms = self.execution.latency_timeout_ms;
        }
        if self.execution.order_rate_limit_per_sec > 0.0 {
            self.order_rate_limit_per_sec = self.execution.order_rate_limit_per_sec;
        }
        self.dynamic_slippage_enabled = self.execution.dynamic_slippage_enabled;

        if self.performance.target_sharpe > 0.0 {
            self.sharpe_target = self.performance.target_sharpe;
        }
        if self.performance.lookback_hours > 0 {
            self.sharpe_lookback_hours = self.performance.lookback_hours;
        }
        if self.performance.min_samples > 0 {
            self.min_sharpe_samples = self.performance.min_samples;
        }
        if let Some(enforce_sharpe_gate) = self.performance.enforce_sharpe_gate {
            self.enforce_sharpe_gate = enforce_sharpe_gate;
        }
        if self.performance.trader_sharpe_floor > 0.0 {
            self.trader_sharpe_floor = self.performance.trader_sharpe_floor;
        }
        if self.performance.confidence_decay_lambda > 0.0 {
            self.confidence_decay_lambda = self.performance.confidence_decay_lambda;
        }
        if self.performance.prune_inactive_trader_hours > 0 {
            self.prune_inactive_trader_hours = self.performance.prune_inactive_trader_hours;
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VettingConfig {
    pub min_win_rate_pct: f64,
    pub min_closed_trades: usize,
    pub lookback_days: i64,
}

impl Default for VettingConfig {
    fn default() -> Self {
        Self {
            min_win_rate_pct: 0.0,
            min_closed_trades: 0,
            lookback_days: 0,
        }
    }
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
    fn validate_live(&self) -> Result<(), Box<dyn Error>> {
        ensure_finite_in_range(
            "global_risk.global_risk_scale",
            self.global_risk_scale,
            MIN_APPROVED_GLOBAL_RISK_SCALE,
            MAX_APPROVED_GLOBAL_RISK_SCALE,
        )?;
        ensure_finite_in_range(
            "global_risk.max_single_asset_equity_pct",
            self.max_single_asset_equity_pct,
            0.0,
            1.0,
        )?;
        ensure_finite_in_range(
            "global_risk.max_net_equity_pct",
            self.max_net_equity_pct,
            0.0,
            1.0,
        )?;
        ensure_finite_in_range(
            "global_risk.max_source_exposure",
            self.max_source_exposure,
            0.0,
            1.0,
        )?;
        if !self.min_order_notional_usd.is_finite() || self.min_order_notional_usd < 10.0 {
            return Err("global_risk.min_order_notional_usd must be finite and positive".into());
        }
        if !self.order_rounding_buffer_usd.is_finite()
            || self.order_rounding_buffer_usd <= 0.0
            || !self.closeability_margin_usd.is_finite()
            || self.closeability_margin_usd < 0.0
            || !self.slot_rank_hysteresis.is_finite()
            || !(0.0..=1.0).contains(&self.slot_rank_hysteresis)
        {
            return Err("dynamic execution-floor and slot-hysteresis policy is invalid".into());
        }
        if self.source_snapshot_max_age_ms == 0 {
            return Err("global_risk.source_snapshot_max_age_ms must be positive".into());
        }
        Ok(())
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            dynamic_slippage_enabled: true,
            max_slippage_bps: 0.0,
            min_order_usd: 0.0,
            max_order_usd: 0.0,
            latency_timeout_ms: 0,
            order_rate_limit_per_sec: 0.0,
        }
    }
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

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            target_sharpe: 0.0,
            lookback_hours: 0,
            min_samples: 0,
            enforce_sharpe_gate: None,
            trader_sharpe_floor: 0.0,
            confidence_decay_lambda: 0.0,
            prune_inactive_trader_hours: 0,
        }
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

impl Default for TraderCandidate {
    fn default() -> Self {
        Self {
            address: String::new(),
            label: String::new(),
            allocation_weight: 1.0,
            confidence_modifier: None,
            enabled: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearinghouseState {
    margin_summary: MarginSummary,
    asset_positions: Vec<AssetPosition>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarginSummary {
    account_value: String,
}

#[derive(Debug, Deserialize)]
struct AssetPosition {
    position: Position,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Position {
    coin: String,
    szi: String,
    entry_px: Option<String>,
}

#[derive(Debug, Clone)]
struct NormalizedPosition {
    coin: String,
    size: f64,
    entry_px: f64,
}

#[derive(Debug, Clone)]
struct AccountSnapshot {
    account_value_usd: f64,
    positions: Vec<NormalizedPosition>,
    observed_at_ms: i64,
}

#[derive(Debug, Clone)]
struct VettedTrader {
    candidate: TraderCandidate,
    account: AccountSnapshot,
    stats: FillStats,
}

#[derive(Debug, Clone)]
struct FillStats {
    closed_trades: usize,
    wins: usize,
    closed_pnl_usd: f64,
}

impl FillStats {
    fn win_rate_pct(&self) -> f64 {
        if self.closed_trades == 0 {
            return 0.0;
        }
        self.wins as f64 / self.closed_trades as f64 * 100.0
    }

    fn confidence_score(&self) -> f64 {
        let sample_weight = (self.closed_trades as f64 / 50.0).min(1.0);
        self.win_rate_pct() * sample_weight
    }

    fn confidence_modifier(&self) -> f64 {
        (self.confidence_score() / 100.0).clamp(0.0, 1.0)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserFill {
    dir: Option<String>,
    closed_pnl: Option<String>,
    fee: Option<String>,
    px: Option<String>,
    sz: Option<String>,
    crossed: Option<bool>,
    cloid: Option<String>,
    time: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct PerformanceSnapshot {
    sample_count: usize,
    maker_fills: usize,
    taker_fills: usize,
    estimated_fee_fills: usize,
    gross_notional_usd: f64,
    gross_closed_pnl_usd: f64,
    total_fees_usd: f64,
    net_closed_pnl_usd: f64,
    mean_return: f64,
    std_return: f64,
    annualized_sharpe: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SignalLedger {
    orders: HashMap<String, LedgerOrder>,
    traders: HashMap<String, LedgerTraderStats>,
    reconciled_fills: HashSet<String>,
    #[serde(default)]
    active_attributions: HashMap<String, Vec<LedgerAttribution>>,
    last_persist_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerOrder {
    cloid: String,
    coin: String,
    side: String,
    submitted_notional_usd: f64,
    submitted_at_ms: i64,
    attributions: Vec<LedgerAttribution>,
    filled_notional_usd: f64,
    gross_realized_pnl_usd: f64,
    total_fees_usd: f64,
    status: LedgerOrderStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum LedgerOrderStatus {
    Submitted,
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerAttribution {
    trader_address: String,
    execution_weight: f64,
    signal_notional_usd: f64,
    confidence_modifier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerTraderStats {
    closed_samples: usize,
    net_pnl_usd: f64,
    total_fees_usd: f64,
    #[serde(default)]
    pending_fees_usd: f64,
    gross_notional_usd: f64,
    #[serde(default)]
    returns: Vec<f64>,
    #[serde(default)]
    return_samples: Vec<LedgerReturn>,
    confidence_modifier: f64,
    last_signal_ms: i64,
    last_fill_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerReturn {
    timestamp_ms: i64,
    value: f64,
}

impl Default for LedgerTraderStats {
    fn default() -> Self {
        Self {
            closed_samples: 0,
            net_pnl_usd: 0.0,
            total_fees_usd: 0.0,
            pending_fees_usd: 0.0,
            gross_notional_usd: 0.0,
            returns: Vec::new(),
            return_samples: Vec::new(),
            confidence_modifier: 1.0,
            last_signal_ms: 0,
            last_fill_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct TraderPerformance {
    sample_count: usize,
    sharpe: Option<f64>,
    confidence_modifier: f64,
}

impl SignalLedger {
    fn load(path: &str) -> Self {
        fs::read_to_string(path)
            .ok()
            .and_then(|text| serde_json::from_str::<Self>(&text).ok())
            .unwrap_or_default()
    }

    fn save(&mut self, path: &str) -> Result<(), Box<dyn Error>> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        self.last_persist_ms = Utc::now().timestamp_millis();
        fs::write(path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    fn record_submitted_order(&mut self, order: &PlannedOrder, submitted_at_ms: i64) {
        for attribution in &order.attributions {
            self.traders
                .entry(attribution.trader_address.clone())
                .or_default()
                .last_signal_ms = submitted_at_ms;
        }

        self.orders.insert(
            order.cloid.clone(),
            LedgerOrder {
                cloid: order.cloid.clone(),
                coin: order.coin.clone(),
                side: order.side.to_string(),
                submitted_notional_usd: order.notional_usd,
                submitted_at_ms,
                attributions: order.attributions.clone(),
                filled_notional_usd: 0.0,
                gross_realized_pnl_usd: 0.0,
                total_fees_usd: 0.0,
                status: LedgerOrderStatus::Submitted,
            },
        );
    }

    fn mark_order_status(&mut self, cloid: &str, status: LedgerOrderStatus) {
        if let Some(order) = self.orders.get_mut(cloid) {
            order.status = status;
        }
    }

    fn reconcile_fill(
        &mut self,
        fill: &UserFill,
        fill_key: String,
        fee_usd: f64,
        follower_equity: f64,
        lookback_hours: i64,
        target_sharpe: f64,
        sharpe_floor: f64,
        decay_lambda: f64,
        min_samples: usize,
    ) -> bool {
        if self.reconciled_fills.contains(&fill_key) {
            return false;
        }

        let Some(cloid) = fill.cloid.as_deref() else {
            return false;
        };
        let Some(order) = self.orders.get_mut(cloid) else {
            return false;
        };
        let now_ms = Utc::now().timestamp_millis();
        let fill_time_ms = fill.time.unwrap_or(now_ms);
        let closed_pnl = fill.closed_pnl.as_deref().map(parse_f64).unwrap_or(0.0);
        let notional_usd = fill_notional_usd(fill);
        order.status = LedgerOrderStatus::Accepted;
        order.filled_notional_usd += notional_usd;
        order.gross_realized_pnl_usd += closed_pnl;
        order.total_fees_usd += fee_usd;

        let equity_base = follower_equity.max(1.0);
        let closes_position = fill_closes_position(fill);
        for attribution in &order.attributions {
            let weight = attribution.execution_weight.clamp(0.0, 1.0);
            if weight == 0.0 {
                continue;
            }

            let trader = self
                .traders
                .entry(attribution.trader_address.clone())
                .or_default();
            let weighted_fee = fee_usd * weight;
            trader.total_fees_usd += weighted_fee;
            trader.gross_notional_usd += notional_usd * weight;
            trader.last_fill_ms = fill_time_ms;

            if closes_position {
                let weighted_net_pnl = closed_pnl * weight - weighted_fee - trader.pending_fees_usd;
                let return_value = weighted_net_pnl / equity_base;
                trader.closed_samples += 1;
                trader.net_pnl_usd += weighted_net_pnl;
                trader.pending_fees_usd = 0.0;
                trader.return_samples.push(LedgerReturn {
                    timestamp_ms: fill_time_ms,
                    value: return_value,
                });
                if trader.closed_samples >= min_samples {
                    trader.confidence_modifier = trader.decayed_confidence(
                        target_sharpe,
                        sharpe_floor,
                        decay_lambda,
                        lookback_hours,
                    );
                }
            } else {
                trader.pending_fees_usd += weighted_fee;
            }
        }

        self.reconciled_fills.insert(fill_key);
        true
    }

    fn trader_performance(
        &self,
        trader_address: &str,
        lookback_hours: i64,
    ) -> Option<TraderPerformance> {
        self.traders
            .get(trader_address)
            .map(|trader| TraderPerformance {
                sample_count: trader.closed_samples,
                sharpe: trader.sharpe(lookback_hours),
                confidence_modifier: trader.confidence_modifier,
            })
    }

    fn prune_inactive(&mut self, cutoff_ms: i64) -> usize {
        let before_traders = self.traders.len();
        self.traders.retain(|_, trader| {
            trader.last_signal_ms >= cutoff_ms || trader.last_fill_ms >= cutoff_ms
        });
        let active_traders = self.traders.keys().cloned().collect::<HashSet<_>>();
        self.orders.retain(|_, order| {
            order.submitted_at_ms >= cutoff_ms
                || order
                    .attributions
                    .iter()
                    .any(|attribution| active_traders.contains(&attribution.trader_address))
        });
        before_traders.saturating_sub(self.traders.len())
    }
}

impl LedgerTraderStats {
    fn sharpe(&self, lookback_hours: i64) -> Option<f64> {
        let cutoff_ms = Utc::now().timestamp_millis() - lookback_hours.max(1) * 60 * 60 * 1_000;
        let recent_returns = if self.return_samples.is_empty() {
            self.returns.clone()
        } else {
            fixed_hourly_returns(
                self.return_samples
                    .iter()
                    .filter(|sample| sample.timestamp_ms >= cutoff_ms)
                    .map(|sample| (sample.timestamp_ms, sample.value)),
                lookback_hours,
            )
        };
        if recent_returns.len() < 2 {
            return None;
        }

        let mean = recent_returns.iter().sum::<f64>() / recent_returns.len() as f64;
        let variance = recent_returns
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (recent_returns.len() - 1) as f64;
        let std = variance.sqrt();
        if std == 0.0 {
            return if mean > 0.0 {
                Some(f64::INFINITY)
            } else if mean < 0.0 {
                Some(f64::NEG_INFINITY)
            } else {
                None
            };
        }

        Some(mean / std * HOURS_PER_YEAR.sqrt())
    }

    fn decayed_confidence(
        &self,
        target_sharpe: f64,
        sharpe_floor: f64,
        decay_lambda: f64,
        lookback_hours: i64,
    ) -> f64 {
        let Some(sharpe) = self.sharpe(lookback_hours) else {
            return self.confidence_modifier.clamp(0.0, 1.0);
        };
        if sharpe < sharpe_floor {
            return 0.0;
        }
        if sharpe >= target_sharpe {
            return 1.0;
        }

        let penalty = (-decay_lambda * (target_sharpe - sharpe)).exp();
        penalty.clamp(0.0, 1.0)
    }
}

#[derive(Debug, Deserialize)]
struct L2Book {
    levels: Vec<Vec<BookLevel>>,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    px: String,
    sz: String,
}

#[derive(Debug, Clone)]
struct AssetTarget {
    notional: f64,
    concurrency_factor: usize,
    has_long: bool,
    has_short: bool,
    confidence_score: f64,
    attributions: Vec<SignalAttribution>,
}

impl Default for AssetTarget {
    fn default() -> Self {
        Self {
            notional: 0.0,
            concurrency_factor: 0,
            has_long: false,
            has_short: false,
            confidence_score: 0.0,
            attributions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct SignalAttribution {
    trader_address: String,
    notional_usd: f64,
    confidence_modifier: f64,
}

#[derive(Debug, Clone, Serialize)]
struct PlannedOrder {
    cloid: String,
    coin: String,
    side: &'static str,
    reduce_only: bool,
    size: f64,
    limit_px: f64,
    notional_usd: f64,
    estimated_fee_usd: f64,
    confidence_score: f64,
    attributions: Vec<LedgerAttribution>,
    reason: String,
}

#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(refill_per_sec: f64) -> Self {
        let capacity = refill_per_sec.max(1.0);
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec: capacity,
            last_refill: Instant::now(),
        }
    }

    async fn acquire(&mut self) {
        loop {
            self.refill();
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return;
            }

            let missing = 1.0 - self.tokens;
            let wait_secs = missing / self.refill_per_sec;
            sleep(Duration::from_secs_f64(wait_secs)).await;
        }
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }

        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = Instant::now();
    }
}

pub struct CopyTradeEngine {
    config: CopyTradeConfig,
    client: Client,
    exchange_client: Option<ExchangeClient>,
    order_bucket: Mutex<TokenBucket>,
    signal_ledger: Arc<RwLock<SignalLedger>>,
    starting_equity_usd: f64,
}

impl CopyTradeEngine {
    pub async fn new(mut config: CopyTradeConfig) -> Result<Self, Box<dyn Error>> {
        config.validate_live()?;
        let wallet = load_wallet("HYPERLIQUID_PRIVATE_KEY")?;
        if config.follower_address.is_none() {
            config.follower_address = Some(format!("{:?}", wallet.address()));
        }
        let exchange_client =
            ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None).await?;

        let order_rate_limit_per_sec = config.order_rate_limit_per_sec;
        Ok(Self {
            starting_equity_usd: config.starting_equity_usd,
            signal_ledger: Arc::new(RwLock::new(SignalLedger::load(&config.ledger_path))),
            config,
            client: Client::builder().no_proxy().build()?,
            exchange_client: Some(exchange_client),
            order_bucket: Mutex::new(TokenBucket::new(order_rate_limit_per_sec)),
        })
    }

    #[cfg(test)]
    fn test_only(mut config: CopyTradeConfig) -> Self {
        if config.global_risk.is_none() {
            config.global_risk = Some(GlobalRiskConfig {
                // Legacy planner tests exercise mechanics outside the HL1B boundary.
                global_risk_scale: 1.0,
                max_single_asset_equity_pct: 100.0,
                max_net_equity_pct: 0.65,
                max_source_exposure: 1.0,
                min_order_notional_usd: 10.0,
                order_rounding_buffer_usd: 0.2,
                closeability_margin_usd: 0.5,
                slot_rank_hysteresis: 0.02,
                source_snapshot_max_age_ms: 40_000,
            });
        }
        let order_rate_limit_per_sec = config.order_rate_limit_per_sec;
        Self {
            starting_equity_usd: config.starting_equity_usd,
            signal_ledger: Arc::new(RwLock::new(SignalLedger::default())),
            config,
            client: Client::builder()
                .no_proxy()
                .build()
                .expect("test HTTP client should build"),
            exchange_client: None,
            order_bucket: Mutex::new(TokenBucket::new(order_rate_limit_per_sec)),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        if self
            .config
            .candidates
            .iter()
            .all(|candidate| !candidate.enabled)
        {
            return Err("no enabled candidate addresses; use Hyperdash Explore for discovery, then add wallet addresses to config/copytrade.json candidates".into());
        }

        println!(
            "copytrade production started with {} candidate address(es), signer {}, ${:.2} starting equity, max {:.2}x leverage",
            self.config
                .candidates
                .iter()
                .filter(|candidate| candidate.enabled)
                .count(),
            self.exchange_client()?.wallet.address(),
            self.config.starting_equity_usd,
            self.config.max_total_leverage
        );

        let mut ticker = interval(Duration::from_secs(self.config.poll_interval_secs));
        loop {
            ticker.tick().await;
            self.tick().await?;
        }
    }

    async fn tick(&self) -> Result<(), Box<dyn Error>> {
        let follower = match &self.config.follower_address {
            Some(address) => Some(self.fetch_user_state(address).await?),
            None => None,
        };
        let follower_equity = follower
            .as_ref()
            .map(|snapshot| snapshot.account_value_usd)
            .unwrap_or(self.config.starting_equity_usd);
        let performance = match self.config.follower_address.as_deref() {
            Some(address) => Some(
                self.fetch_performance_snapshot(address, follower_equity)
                    .await?,
            ),
            None => None,
        };
        if let Some(snapshot) = performance.as_ref() {
            self.log_performance_snapshot(snapshot);
        }
        self.prune_and_persist_ledger().await?;

        let drawdown_active =
            self.drawdown_pct(follower_equity) >= self.config.max_account_drawdown_pct;
        if drawdown_active {
            println!(
                "drawdown guard active: equity ${:.2}, drawdown {:.2}%; reductions remain enabled",
                follower_equity,
                self.drawdown_pct(follower_equity)
            );
        }

        let vetted_traders = self.vet_candidates().await?;
        if vetted_traders.is_empty() {
            println!(
                "{} no candidates passed local Hyperliquid vetting; syncing existing follower exposure to zero",
                Utc::now().to_rfc3339()
            );
        }

        let mids = self.fetch_all_mids().await?;
        let current_notional_by_coin = self.current_notional_by_coin(follower.as_ref(), &mids);
        let target_leverage = self.config.target_leverage_for_equity(follower_equity);
        let risk = self
            .config
            .global_risk
            .as_ref()
            .ok_or("global_risk is required before target calculation")?;
        let trader_performance = self.trader_performance_by_address().await;
        let decision_time_ms = Utc::now().timestamp_millis();
        let mut consensus_inputs_by_coin: HashMap<String, Vec<SourceConsensusInput>> =
            HashMap::new();
        let mut confidence_score_by_candidate = HashMap::<String, f64>::new();
        let mut confidence_modifier_by_candidate = HashMap::<String, f64>::new();
        for trader in vetted_traders {
            let snapshot = trader.account;
            if snapshot.account_value_usd <= 0.0 {
                continue;
            }
            let confidence_score = trader.stats.confidence_score();
            let ledger_performance = trader_performance.get(&trader.candidate.address);
            let ledger_modifier = ledger_performance
                .map(|performance| performance.confidence_modifier)
                .unwrap_or(1.0);
            if let Some(performance) = ledger_performance {
                if performance.sample_count >= self.config.min_sharpe_samples
                    && ledger_modifier < 1.0
                {
                    println!(
                        "{} isolated trader sharpe penalty {} samples, sharpe={}, ledger_cm={:.4}: {}",
                        Utc::now().to_rfc3339(),
                        performance.sample_count,
                        format_sharpe(performance.sharpe),
                        ledger_modifier,
                        candidate_label(&trader.candidate)
                    );
                }
            }
            let configured_modifier = trader.candidate.confidence_modifier.ok_or_else(|| {
                format!(
                    "confidence_modifier is required for {}",
                    trader.candidate.address
                )
            })?;
            let confidence_modifier = configured_modifier * ledger_modifier;
            let snapshot_age_ms = decision_time_ms
                .saturating_sub(snapshot.observed_at_ms)
                .max(0) as u64;
            confidence_score_by_candidate
                .insert(trader.candidate.address.clone(), confidence_score);
            confidence_modifier_by_candidate
                .insert(trader.candidate.address.clone(), confidence_modifier);

            for position in snapshot.positions {
                let mid_px = mids
                    .get(&position.coin)
                    .copied()
                    .unwrap_or(position.entry_px);
                if mid_px <= 0.0 || position.size == 0.0 {
                    continue;
                }

                let trader_exposure = position.size * mid_px / snapshot.account_value_usd;
                consensus_inputs_by_coin
                    .entry(position.coin)
                    .or_default()
                    .push(SourceConsensusInput {
                        candidate_address: trader.candidate.address.clone(),
                        allocation_weight: trader.candidate.allocation_weight,
                        confidence_modifier,
                        source_exposure: trader_exposure,
                        enabled: trader.candidate.enabled,
                        quarantined: false,
                        snapshot_age_ms,
                    });
            }
        }

        let target_scale = follower_equity * target_leverage * risk.global_risk_scale;
        let mut target_by_coin: HashMap<String, AssetTarget> = HashMap::new();
        for (coin, inputs) in consensus_inputs_by_coin {
            let consensus = bounded_additive_consensus(
                &inputs,
                risk.max_source_exposure,
                risk.source_snapshot_max_age_ms,
            )?;
            let mut target = AssetTarget {
                notional: target_scale * consensus.exposure,
                concurrency_factor: consensus.contribution_by_candidate.len(),
                ..Default::default()
            };
            for (address, exposure_contribution) in consensus.contribution_by_candidate {
                let contribution_notional = target_scale * exposure_contribution;
                target.has_long |= contribution_notional > 0.0;
                target.has_short |= contribution_notional < 0.0;
                target.confidence_score = target.confidence_score.max(
                    confidence_score_by_candidate
                        .get(&address)
                        .copied()
                        .unwrap_or_default(),
                );
                target.attributions.push(SignalAttribution {
                    trader_address: address.clone(),
                    notional_usd: contribution_notional,
                    confidence_modifier: confidence_modifier_by_candidate
                        .get(&address)
                        .copied()
                        .unwrap_or_default(),
                });
            }
            target_by_coin.insert(coin, target);
        }

        let mut target_notional_by_coin: HashMap<String, f64> = HashMap::new();
        let mut capacity_by_coin: HashMap<String, f64> = HashMap::new();
        let mut confidence_by_coin: HashMap<String, f64> = HashMap::new();
        let mut attribution_by_coin: HashMap<String, Vec<SignalAttribution>> = HashMap::new();
        for (coin, target) in target_by_coin {
            if target.has_long && target.has_short {
                println!(
                    "{} netting conflicting {} copy signals to ${:.2}",
                    Utc::now().to_rfc3339(),
                    coin,
                    target.notional
                );
            }

            let Some(mid_px) = mids.get(&coin).copied().filter(|px| *px > 0.0) else {
                continue;
            };
            let max_asset_notional = follower_equity * risk.max_single_asset_equity_pct;
            let side_depth = if target.notional == 0.0 {
                max_asset_notional
            } else {
                self.fetch_l2_depth_within_bps(&coin, target.notional > 0.0, mid_px)
                    .await
                    .unwrap_or(self.config.max_single_order_notional_usd)
            };
            let concurrency_factor = target.concurrency_factor.max(1) as f64;
            let book_capacity = side_depth / concurrency_factor;
            let available_capacity = max_asset_notional.min(book_capacity);
            target_notional_by_coin.insert(
                coin.clone(),
                target
                    .notional
                    .clamp(-available_capacity, available_capacity),
            );
            capacity_by_coin.insert(coin.clone(), available_capacity);
            confidence_by_coin.insert(coin.clone(), target.confidence_score);
            attribution_by_coin.insert(coin, target.attributions);
        }

        let max_asset_notional = follower_equity * risk.max_single_asset_equity_pct;
        for (coin, current_notional) in &current_notional_by_coin {
            target_notional_by_coin.entry(coin.clone()).or_insert(0.0);
            capacity_by_coin
                .entry(coin.clone())
                .or_insert(max_asset_notional.max(current_notional.abs()));
            confidence_by_coin.entry(coin.clone()).or_insert(0.0);
            attribution_by_coin.entry(coin.clone()).or_default();
        }
        let fallback_attribution_by_coin =
            self.signal_ledger.read().await.active_attributions.clone();
        let next_active_attributions = attribution_by_coin.clone();
        let planned_orders = self.plan_orders(
            follower_equity,
            &mids,
            target_notional_by_coin,
            current_notional_by_coin.clone(),
            capacity_by_coin,
            confidence_by_coin,
            attribution_by_coin,
            fallback_attribution_by_coin,
        );
        let mut planned_orders =
            self.filter_orders_for_performance(planned_orders, performance.as_ref());
        if drawdown_active {
            planned_orders = self.filter_orders_for_drawdown(planned_orders);
        }

        self.update_active_attributions(&next_active_attributions, &current_notional_by_coin)
            .await?;

        if planned_orders.is_empty() {
            println!("{} no copytrade rebalance needed", Utc::now().to_rfc3339());
            return Ok(());
        }

        self.execute_orders(planned_orders).await?;
        Ok(())
    }

    async fn vet_candidates(&self) -> Result<Vec<VettedTrader>, Box<dyn Error>> {
        let mut vetted = Vec::new();
        let mut pending = FuturesUnordered::new();
        for candidate in self
            .config
            .candidates
            .iter()
            .filter(|candidate| candidate.enabled)
        {
            let candidate = candidate.clone();
            pending.push(async move {
                let account = self.fetch_user_state(&candidate.address).await?;
                let stats = self.fetch_fill_stats(&candidate.address).await?;
                Ok::<_, Box<dyn Error>>((candidate, account, stats))
            });
        }

        while let Some(result) = pending.next().await {
            let (candidate, account, stats) = result?;
            let passed = account.account_value_usd > 0.0
                && stats.closed_trades >= self.config.min_closed_trades
                && stats.win_rate_pct() >= self.config.min_win_rate_pct
                && stats.confidence_score() >= self.config.min_trader_confidence_score
                && stats.closed_pnl_usd >= self.config.min_closed_pnl_usd;

            println!(
                "candidate {} account ${:.2}, {} closed trades, {:.2}% win rate, {:.2} confidence, ${:.2} closed pnl: {}",
                candidate_label(&candidate),
                account.account_value_usd,
                stats.closed_trades,
                stats.win_rate_pct(),
                stats.confidence_score(),
                stats.closed_pnl_usd,
                if passed { "pass" } else { "skip" }
            );

            if passed {
                vetted.push(VettedTrader {
                    candidate,
                    account,
                    stats,
                });
            }
        }

        vetted.sort_by(|a, b| {
            b.stats
                .win_rate_pct()
                .partial_cmp(&a.stats.win_rate_pct())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    b.stats
                        .closed_pnl_usd
                        .partial_cmp(&a.stats.closed_pnl_usd)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });
        vetted.truncate(self.config.active_limit());
        Ok(vetted)
    }

    async fn fetch_user_state(&self, address: &str) -> Result<AccountSnapshot, Box<dyn Error>> {
        let response = self
            .client
            .post(HYPERLIQUID_INFO_URL)
            .json(&json!({
                "type": "clearinghouseState",
                "user": address
            }))
            .send()
            .await?
            .error_for_status()?;

        let state = response.json::<ClearinghouseState>().await?;
        Ok(AccountSnapshot {
            account_value_usd: parse_f64(&state.margin_summary.account_value),
            positions: state
                .asset_positions
                .into_iter()
                .map(|asset_position| {
                    let position = asset_position.position;
                    NormalizedPosition {
                        coin: position.coin,
                        size: parse_f64(&position.szi),
                        entry_px: position.entry_px.as_deref().map(parse_f64).unwrap_or(0.0),
                    }
                })
                .collect(),
            observed_at_ms: Utc::now().timestamp_millis(),
        })
    }

    async fn fetch_fill_stats(&self, address: &str) -> Result<FillStats, Box<dyn Error>> {
        let end_time = Utc::now().timestamp_millis();
        let start_time = end_time - self.config.vetting_lookback_days.max(1) * 24 * 60 * 60 * 1_000;
        let fills = self
            .fetch_user_fills(address, start_time, end_time, true)
            .await?;
        let mut stats = FillStats {
            closed_trades: 0,
            wins: 0,
            closed_pnl_usd: 0.0,
        };

        for fill in fills {
            if !fill_closes_position(&fill) {
                continue;
            }
            let closed_pnl = fill.closed_pnl.as_deref().map(parse_f64).unwrap_or(0.0);
            stats.closed_trades += 1;
            stats.closed_pnl_usd += closed_pnl;
            if closed_pnl > 0.0 {
                stats.wins += 1;
            }
        }

        Ok(stats)
    }

    async fn fetch_performance_snapshot(
        &self,
        address: &str,
        follower_equity: f64,
    ) -> Result<PerformanceSnapshot, Box<dyn Error>> {
        let end_time = Utc::now().timestamp_millis();
        let lookback_hours = self.config.sharpe_lookback_hours.max(1);
        let start_time = end_time - lookback_hours * 60 * 60 * 1_000;
        let fills = self
            .fetch_user_fills(address, start_time, end_time, false)
            .await?;
        let reconciled = self
            .reconcile_fills_to_ledger(&fills, follower_equity, lookback_hours)
            .await?;
        if reconciled > 0 {
            println!(
                "{} reconciled {} ledger fill(s) from follower userFillsByTime",
                Utc::now().to_rfc3339(),
                reconciled
            );
        }

        Ok(self.performance_from_fills(&fills, follower_equity, lookback_hours))
    }

    async fn reconcile_fills_to_ledger(
        &self,
        fills: &[UserFill],
        follower_equity: f64,
        lookback_hours: i64,
    ) -> Result<usize, Box<dyn Error>> {
        let mut reconciled = 0;
        {
            let mut ledger = self.signal_ledger.write().await;
            let mut ordered_fills = fills.iter().collect::<Vec<_>>();
            ordered_fills.sort_by_key(|fill| fill.time.unwrap_or_default());
            for fill in ordered_fills {
                let Some(cloid) = fill.cloid.as_deref() else {
                    continue;
                };
                let notional_usd = fill_notional_usd(fill);
                let (fee_usd, _) = self.fill_fee_usd(fill, notional_usd);
                if ledger.reconcile_fill(
                    fill,
                    fill_key(cloid, fill),
                    fee_usd,
                    follower_equity,
                    lookback_hours,
                    self.config.sharpe_target,
                    self.config.trader_sharpe_floor,
                    self.config.confidence_decay_lambda,
                    self.config.min_sharpe_samples,
                ) {
                    reconciled += 1;
                }
            }
        }

        if reconciled > 0 {
            self.persist_ledger().await?;
        }

        Ok(reconciled)
    }

    async fn trader_performance_by_address(&self) -> HashMap<String, TraderPerformance> {
        let ledger = self.signal_ledger.read().await;
        ledger
            .traders
            .keys()
            .filter_map(|address| {
                ledger
                    .trader_performance(address, self.config.sharpe_lookback_hours)
                    .map(|performance| (address.clone(), performance))
            })
            .collect()
    }

    async fn prune_and_persist_ledger(&self) -> Result<(), Box<dyn Error>> {
        if self.config.prune_inactive_trader_hours <= 0 {
            return Ok(());
        }

        let cutoff_ms = Utc::now().timestamp_millis()
            - self.config.prune_inactive_trader_hours * 60 * 60 * 1_000;
        let pruned = self.signal_ledger.write().await.prune_inactive(cutoff_ms);
        if pruned > 0 {
            println!(
                "{} pruned {} inactive trader(s) from signal ledger",
                Utc::now().to_rfc3339(),
                pruned
            );
            self.persist_ledger().await?;
        }

        Ok(())
    }

    async fn persist_ledger(&self) -> Result<(), Box<dyn Error>> {
        self.signal_ledger
            .write()
            .await
            .save(&self.config.ledger_path)
    }

    async fn update_active_attributions(
        &self,
        current_signals: &HashMap<String, Vec<SignalAttribution>>,
        current_positions: &HashMap<String, f64>,
    ) -> Result<(), Box<dyn Error>> {
        {
            let mut ledger = self.signal_ledger.write().await;
            let mut coins = current_signals.keys().cloned().collect::<HashSet<_>>();
            coins.extend(current_positions.keys().cloned());

            for coin in coins {
                let signals = current_signals.get(&coin).map(Vec::as_slice).unwrap_or(&[]);
                if !signals.is_empty() {
                    ledger
                        .active_attributions
                        .insert(coin, normalized_signal_attributions(signals));
                } else if current_positions.get(&coin).copied().unwrap_or(0.0) == 0.0 {
                    ledger.active_attributions.remove(&coin);
                }
            }
        }
        self.persist_ledger().await
    }

    async fn fetch_user_fills(
        &self,
        address: &str,
        start_time: i64,
        end_time: i64,
        aggregate_by_time: bool,
    ) -> Result<Vec<UserFill>, Box<dyn Error>> {
        let response = self
            .client
            .post(HYPERLIQUID_INFO_URL)
            .json(&json!({
                "type": "userFillsByTime",
                "user": address,
                "startTime": start_time,
                "endTime": end_time,
                "aggregateByTime": aggregate_by_time
            }))
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json::<Vec<UserFill>>().await?)
    }

    fn performance_from_fills(
        &self,
        fills: &[UserFill],
        follower_equity: f64,
        lookback_hours: i64,
    ) -> PerformanceSnapshot {
        let equity_base = follower_equity.max(1.0);
        let mut timed_returns = Vec::new();
        let mut snapshot = PerformanceSnapshot {
            sample_count: 0,
            maker_fills: 0,
            taker_fills: 0,
            estimated_fee_fills: 0,
            gross_notional_usd: 0.0,
            gross_closed_pnl_usd: 0.0,
            total_fees_usd: 0.0,
            net_closed_pnl_usd: 0.0,
            mean_return: 0.0,
            std_return: 0.0,
            annualized_sharpe: None,
        };

        for fill in fills {
            let closed_pnl = fill.closed_pnl.as_deref().map(parse_f64).unwrap_or(0.0);
            let notional_usd = fill_notional_usd(fill);
            let (fee_usd, fee_was_estimated) = self.fill_fee_usd(fill, notional_usd);
            if fill.crossed == Some(false) {
                snapshot.maker_fills += 1;
            } else {
                snapshot.taker_fills += 1;
            }
            if fee_was_estimated {
                snapshot.estimated_fee_fills += 1;
            }

            snapshot.gross_notional_usd += notional_usd;
            snapshot.gross_closed_pnl_usd += closed_pnl;
            snapshot.total_fees_usd += fee_usd;
            if fill_closes_position(fill) {
                snapshot.sample_count += 1;
            }
            timed_returns.push((
                fill.time.unwrap_or_else(|| Utc::now().timestamp_millis()),
                (closed_pnl - fee_usd) / equity_base,
            ));
        }
        snapshot.net_closed_pnl_usd = snapshot.gross_closed_pnl_usd - snapshot.total_fees_usd;
        let returns = fixed_hourly_returns(timed_returns.into_iter(), lookback_hours);

        if returns.is_empty() {
            return snapshot;
        }

        snapshot.mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        if returns.len() > 1 {
            let variance = returns
                .iter()
                .map(|value| (value - snapshot.mean_return).powi(2))
                .sum::<f64>()
                / (returns.len() - 1) as f64;
            snapshot.std_return = variance.sqrt();

            snapshot.annualized_sharpe = if snapshot.std_return > 0.0 {
                Some(snapshot.mean_return / snapshot.std_return * HOURS_PER_YEAR.sqrt())
            } else if snapshot.mean_return > 0.0 {
                Some(f64::INFINITY)
            } else if snapshot.mean_return < 0.0 {
                Some(f64::NEG_INFINITY)
            } else {
                None
            };
        }

        snapshot
    }

    fn fill_fee_usd(&self, fill: &UserFill, notional_usd: f64) -> (f64, bool) {
        if let Some(fee) = fill.fee.as_deref().map(parse_f64) {
            return (fee, false);
        }

        let fee_bps = if fill.crossed == Some(false) {
            self.config.maker_fee_bps
        } else {
            self.config.taker_fee_bps
        };
        (notional_usd * fee_bps / 10_000.0, true)
    }

    fn log_performance_snapshot(&self, snapshot: &PerformanceSnapshot) {
        let fee_drag_bps = if snapshot.gross_notional_usd > 0.0 {
            snapshot.total_fees_usd / snapshot.gross_notional_usd * 10_000.0
        } else {
            0.0
        };

        println!(
            "{} performance {}h: {} closed fills, net_pnl=${:.2}, fees=${:.2}, fee_drag={:.2} bps, maker/taker={}/{}, estimated_fee_fills={}, sharpe={} target={:.2}",
            Utc::now().to_rfc3339(),
            self.config.sharpe_lookback_hours,
            snapshot.sample_count,
            snapshot.net_closed_pnl_usd,
            snapshot.total_fees_usd,
            fee_drag_bps,
            snapshot.maker_fills,
            snapshot.taker_fills,
            snapshot.estimated_fee_fills,
            format_sharpe(snapshot.annualized_sharpe),
            self.config.sharpe_target
        );
    }

    fn filter_orders_for_performance(
        &self,
        planned_orders: Vec<PlannedOrder>,
        performance: Option<&PerformanceSnapshot>,
    ) -> Vec<PlannedOrder> {
        let Some(snapshot) = performance else {
            return planned_orders;
        };
        if !self.sharpe_gate_active(snapshot) {
            return planned_orders;
        }

        let planned_count = planned_orders.len();
        let filtered_orders = planned_orders
            .into_iter()
            .filter(|order| order.reduce_only)
            .collect::<Vec<_>>();
        println!(
            "{} sharpe guard active: measured sharpe {} below target {:.2} with {} samples; allowing {} reduce-only order(s), blocking {} new-risk order(s)",
            Utc::now().to_rfc3339(),
            format_sharpe(snapshot.annualized_sharpe),
            self.config.sharpe_target,
            snapshot.sample_count,
            filtered_orders.len(),
            planned_count.saturating_sub(filtered_orders.len())
        );

        filtered_orders
    }

    fn filter_orders_for_drawdown(&self, planned_orders: Vec<PlannedOrder>) -> Vec<PlannedOrder> {
        let planned_count = planned_orders.len();
        let filtered_orders = planned_orders
            .into_iter()
            .filter(|order| order.reduce_only)
            .collect::<Vec<_>>();
        println!(
            "{} drawdown guard allowing {} reduce-only order(s), blocking {} new-risk order(s)",
            Utc::now().to_rfc3339(),
            filtered_orders.len(),
            planned_count.saturating_sub(filtered_orders.len())
        );
        filtered_orders
    }

    fn sharpe_gate_active(&self, snapshot: &PerformanceSnapshot) -> bool {
        if !self.config.enforce_sharpe_gate || self.config.sharpe_target <= 0.0 {
            return false;
        }
        if snapshot.sample_count < self.config.min_sharpe_samples {
            return false;
        }

        snapshot
            .annualized_sharpe
            .map(|sharpe| sharpe < self.config.sharpe_target)
            .unwrap_or(true)
    }

    async fn fetch_all_mids(&self) -> Result<HashMap<String, f64>, Box<dyn Error>> {
        let response = self
            .client
            .post(HYPERLIQUID_INFO_URL)
            .json(&json!({ "type": "allMids" }))
            .send()
            .await?
            .error_for_status()?;
        let mids = response.json::<HashMap<String, String>>().await?;
        Ok(mids
            .into_iter()
            .map(|(coin, px)| (coin, parse_f64(&px)))
            .collect())
    }

    async fn fetch_l2_depth_within_bps(
        &self,
        coin: &str,
        is_buy: bool,
        mid_px: f64,
    ) -> Result<f64, Box<dyn Error>> {
        let response = self
            .client
            .post(HYPERLIQUID_INFO_URL)
            .json(&json!({
                "type": "l2Book",
                "coin": coin
            }))
            .send()
            .await?
            .error_for_status()?;
        let book = response.json::<L2Book>().await?;
        let side_index = if is_buy { 1 } else { 0 };
        let Some(levels) = book.levels.get(side_index) else {
            return Ok(0.0);
        };
        let slippage_bps = self.effective_slippage_bps();
        let max_px = mid_px * (1.0 + slippage_bps / 10_000.0);
        let min_px = mid_px * (1.0 - slippage_bps / 10_000.0);

        let depth = levels
            .iter()
            .filter_map(|level| {
                let px = parse_f64(&level.px);
                let sz = parse_f64(&level.sz);
                if px <= 0.0 || sz <= 0.0 {
                    return None;
                }
                let inside_slippage = if is_buy { px <= max_px } else { px >= min_px };
                inside_slippage.then_some(px * sz)
            })
            .sum();

        Ok(depth)
    }

    fn current_notional_by_coin(
        &self,
        follower: Option<&AccountSnapshot>,
        mids: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {
        let mut current = HashMap::new();
        if let Some(snapshot) = follower {
            for position in &snapshot.positions {
                let mid_px = mids
                    .get(&position.coin)
                    .copied()
                    .unwrap_or(position.entry_px);
                current.insert(position.coin.clone(), position.size * mid_px);
            }
        }
        current
    }

    fn plan_orders(
        &self,
        follower_equity: f64,
        mids: &HashMap<String, f64>,
        mut target_notional_by_coin: HashMap<String, f64>,
        current_notional_by_coin: HashMap<String, f64>,
        capacity_by_coin: HashMap<String, f64>,
        confidence_by_coin: HashMap<String, f64>,
        attribution_by_coin: HashMap<String, Vec<SignalAttribution>>,
        fallback_attribution_by_coin: HashMap<String, Vec<LedgerAttribution>>,
    ) -> Vec<PlannedOrder> {
        let target_leverage = self.config.target_leverage_for_equity(follower_equity);
        let risk = self
            .config
            .global_risk
            .as_ref()
            .expect("validated or test risk configuration must exist");
        let max_total_notional = follower_equity * target_leverage * risk.global_risk_scale;
        let max_asset_notional = follower_equity * risk.max_single_asset_equity_pct;
        let current_gross: f64 = current_notional_by_coin
            .values()
            .map(|value| value.abs())
            .sum();
        let mut projected_gross = current_gross;
        let mut plans = Vec::new();

        for coin in current_notional_by_coin.keys() {
            target_notional_by_coin.entry(coin.clone()).or_insert(0.0);
        }

        let mut target_entries = target_notional_by_coin.into_iter().collect::<Vec<_>>();
        target_entries.sort_by(|(coin_a, target_a), (coin_b, target_b)| {
            let current_a = current_notional_by_coin.get(coin_a).copied().unwrap_or(0.0);
            let current_b = current_notional_by_coin.get(coin_b).copied().unwrap_or(0.0);
            let reduction_a = is_reducing_delta(current_a, target_a - current_a);
            let reduction_b = is_reducing_delta(current_b, target_b - current_b);
            reduction_b.cmp(&reduction_a).then_with(|| {
                let confidence_a = confidence_by_coin.get(coin_a).copied().unwrap_or(0.0);
                let confidence_b = confidence_by_coin.get(coin_b).copied().unwrap_or(0.0);
                confidence_b
                    .partial_cmp(&confidence_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| coin_a.cmp(coin_b))
            })
        });

        for (coin, target_notional) in target_entries {
            let Some(mid_px) = mids.get(&coin).copied().filter(|px| *px > 0.0) else {
                continue;
            };

            let available_capacity = capacity_by_coin
                .get(&coin)
                .copied()
                .unwrap_or(max_asset_notional)
                .min(max_asset_notional);
            let capped_target = target_notional.clamp(-available_capacity, available_capacity);
            let current_notional = current_notional_by_coin.get(&coin).copied().unwrap_or(0.0);
            let mut delta_notional = capped_target - current_notional;

            if delta_notional.abs() < self.config.min_order_notional_usd {
                continue;
            }

            if delta_notional.abs() > self.config.max_single_order_notional_usd {
                delta_notional =
                    delta_notional.signum() * self.config.max_single_order_notional_usd;
            }

            let reduce_only = is_reducing_delta(current_notional, delta_notional);
            if reduce_only && delta_notional.abs() > current_notional.abs() {
                delta_notional = -current_notional;
            }

            if reduce_only {
                projected_gross = (projected_gross - delta_notional.abs()).max(0.0);
            } else {
                let remaining_gross = (max_total_notional - projected_gross).max(0.0);
                let allowed = remaining_gross.min(delta_notional.abs());
                if allowed < self.config.min_order_notional_usd {
                    continue;
                }
                delta_notional = delta_notional.signum() * allowed;
                projected_gross += allowed;
            }

            let slippage_bps = self.effective_slippage_bps();
            let estimated_fee_usd = delta_notional.abs() * self.config.taker_fee_bps / 10_000.0;
            let order_notional = delta_notional.abs();
            let size = (order_notional / mid_px) * delta_notional.signum();
            let limit_px = if delta_notional > 0.0 {
                mid_px * (1.0 + slippage_bps / 10_000.0)
            } else {
                mid_px * (1.0 - slippage_bps / 10_000.0)
            };
            let confidence_score = confidence_by_coin.get(&coin).copied().unwrap_or(0.0);
            let side_sign = delta_notional.signum();
            let attributions = ledger_attributions_for_order(
                attribution_by_coin
                    .get(&coin)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                side_sign,
                fallback_attribution_by_coin
                    .get(&coin)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            );
            let cloid = generate_cloid(&coin, if delta_notional > 0.0 { "buy" } else { "sell" });

            plans.push(PlannedOrder {
                cloid,
                coin,
                side: if delta_notional > 0.0 { "buy" } else { "sell" },
                reduce_only,
                size,
                limit_px,
                notional_usd: order_notional,
                estimated_fee_usd,
                confidence_score,
                attributions,
                reason: format!(
                    "target ${:.2}, current ${:.2}, capacity ${:.2}, leverage {:.2}x, confidence {:.2}, taker_fee ${:.4}, slippage {:.2} bps",
                    capped_target,
                    current_notional,
                    available_capacity,
                    target_leverage,
                    confidence_score,
                    estimated_fee_usd,
                    slippage_bps
                ),
            });
        }

        plans.sort_by(|a, b| {
            b.reduce_only.cmp(&a.reduce_only).then_with(|| {
                b.confidence_score
                    .partial_cmp(&a.confidence_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });
        plans
    }

    fn effective_slippage_bps(&self) -> f64 {
        if self.config.dynamic_slippage_enabled {
            self.config.slippage_buffer_bps.clamp(2.5, 4.0)
        } else {
            self.config.slippage_buffer_bps
        }
    }

    async fn execute_orders(
        &self,
        planned_orders: Vec<PlannedOrder>,
    ) -> Result<(), Box<dyn Error>> {
        for order in planned_orders {
            let request = ClientOrderRequest {
                asset: order.coin.clone(),
                is_buy: order.side == "buy",
                reduce_only: order.reduce_only,
                limit_px: order.limit_px,
                sz: order.size.abs(),
                cloid: Some(parse_cloid(&order.cloid)?),
                order_type: ClientOrder::Limit(ClientLimit {
                    tif: "Ioc".to_string(),
                }),
            };

            println!(
                "{} submitting IOC {} {} cloid={} sz {:.8} @ {:.8} reduce_only={} notional=${:.2} est_fee=${:.4} reason={}",
                Utc::now().to_rfc3339(),
                order.side,
                order.coin,
                order.cloid,
                order.size.abs(),
                order.limit_px,
                order.reduce_only,
                order.notional_usd,
                order.estimated_fee_usd,
                order.reason
            );

            self.record_order_in_ledger(&order).await?;
            self.order_bucket.lock().await.acquire().await;
            let response = timeout(
                Duration::from_millis(self.config.latency_timeout_ms),
                self.exchange_client()?.order(request, None),
            )
            .await
            .map_err(|_| {
                format!(
                    "exchange order timeout for {} after {}ms",
                    order.coin, self.config.latency_timeout_ms
                )
            })??;
            match response {
                ExchangeResponseStatus::Ok(exchange_response) => {
                    self.mark_ledger_order_status(&order.cloid, LedgerOrderStatus::Accepted)
                        .await?;
                    println!("exchange response: {exchange_response:?}");
                }
                ExchangeResponseStatus::Err(error) => {
                    self.mark_ledger_order_status(&order.cloid, LedgerOrderStatus::Rejected)
                        .await?;
                    return Err(format!("exchange rejected {} order: {}", order.coin, error).into());
                }
            }
        }

        Ok(())
    }

    async fn record_order_in_ledger(&self, order: &PlannedOrder) -> Result<(), Box<dyn Error>> {
        self.signal_ledger
            .write()
            .await
            .record_submitted_order(order, Utc::now().timestamp_millis());
        self.persist_ledger().await
    }

    async fn mark_ledger_order_status(
        &self,
        cloid: &str,
        status: LedgerOrderStatus,
    ) -> Result<(), Box<dyn Error>> {
        self.signal_ledger
            .write()
            .await
            .mark_order_status(cloid, status);
        self.persist_ledger().await
    }

    fn exchange_client(&self) -> Result<&ExchangeClient, Box<dyn Error>> {
        self.exchange_client
            .as_ref()
            .ok_or_else(|| "exchange client is unavailable".into())
    }

    fn drawdown_pct(&self, equity: f64) -> f64 {
        if self.starting_equity_usd <= 0.0 {
            return 0.0;
        }
        ((self.starting_equity_usd - equity) / self.starting_equity_usd * 100.0).max(0.0)
    }
}

pub async fn approve_agent_wallet() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let master_wallet = load_wallet("HYPERLIQUID_MASTER_PRIVATE_KEY")?;
    let master_address = master_wallet.address();
    let exchange_client =
        ExchangeClient::new(None, master_wallet, Some(BaseUrl::Mainnet), None, None).await?;
    let (agent_private_key, response) = exchange_client.approve_agent(None).await?;
    let agent_wallet: LocalWallet = agent_private_key.parse()?;

    println!("approved agent wallet for master account {master_address:?}");
    println!("agent address: {:?}", agent_wallet.address());
    if let Ok(path) = env::var("HYPERLIQUID_AGENT_KEY_OUT") {
        write_secret_file(
            &path,
            &format!("HYPERLIQUID_PRIVATE_KEY={agent_private_key}\n"),
        )?;
        println!("agent private key written to {path} with 0600 permissions");
    } else {
        println!("HYPERLIQUID_PRIVATE_KEY={agent_private_key}");
        println!("warning: agent key printed to terminal; prefer HYPERLIQUID_AGENT_KEY_OUT for future approvals");
    }
    println!("approval response: {response:?}");
    Ok(())
}

fn load_wallet(env_key: &str) -> Result<LocalWallet, Box<dyn Error>> {
    let private_key = load_secret(env_key)?;
    private_key
        .parse::<LocalWallet>()
        .map_err(|error| format!("failed parsing {env_key}: {error}").into())
}

fn load_secret(env_key: &str) -> Result<String, Box<dyn Error>> {
    if let Ok(value) = env::var(env_key) {
        return Ok(value.trim().to_string());
    }

    let file_key = format!("{env_key}_FILE");
    if let Ok(path) = env::var(&file_key) {
        let value = fs::read_to_string(&path)?;
        return Ok(value.trim().to_string());
    }

    if io::stdin().is_terminal() {
        return prompt_secret(env_key);
    }

    Err(format!("{env_key} or {file_key} is required").into())
}

fn prompt_secret(env_key: &str) -> Result<String, Box<dyn Error>> {
    eprint!("{env_key}: ");
    io::stderr().flush()?;
    let echo_disabled = Command::new("stty")
        .arg("-echo")
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    let mut value = String::new();
    let read_result = io::stdin().read_line(&mut value);

    if echo_disabled {
        let _ = Command::new("stty").arg("echo").status();
        eprintln!();
    }

    read_result?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(format!("{env_key} was empty").into());
    }

    Ok(value)
}

fn write_secret_file(path: &str, contents: &str) -> Result<(), Box<dyn Error>> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
    }

    fs::write(path, contents)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn validate_live_candidate(candidate: &TraderCandidate) -> Result<(), Box<dyn Error>> {
    if !valid_hyperliquid_address(&candidate.address) {
        return Err(format!("invalid candidate address: {}", candidate.address).into());
    }
    if !candidate.allocation_weight.is_finite() || candidate.allocation_weight < 0.0 {
        return Err(format!(
            "allocation_weight must be finite and non-negative for {}",
            candidate.address
        )
        .into());
    }
    let confidence_modifier = candidate.confidence_modifier.ok_or_else(|| {
        format!(
            "confidence_modifier is required in live mode for {}",
            candidate.address
        )
    })?;
    ensure_finite_in_range(
        &format!("confidence_modifier for {}", candidate.address),
        confidence_modifier,
        0.0,
        1.0,
    )
}

fn valid_hyperliquid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && value[2..].bytes().any(|byte| byte != b'0')
}

fn ensure_finite_in_range(
    name: &str,
    value: f64,
    minimum: f64,
    maximum: f64,
) -> Result<(), Box<dyn Error>> {
    if !value.is_finite() || !(minimum..=maximum).contains(&value) {
        return Err(format!("{name} must be finite and within [{minimum}, {maximum}]").into());
    }
    Ok(())
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(default)
}

fn parse_f64(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.0)
}

fn default_leverage_curve() -> Vec<LeverageCurvePoint> {
    vec![
        LeverageCurvePoint {
            equity_usd: 100.0,
            target_leverage: 4.0,
        },
        LeverageCurvePoint {
            equity_usd: 1_000.0,
            target_leverage: 8.0,
        },
        LeverageCurvePoint {
            equity_usd: 10_000.0,
            target_leverage: 6.0,
        },
        LeverageCurvePoint {
            equity_usd: 100_000.0,
            target_leverage: 3.0,
        },
        LeverageCurvePoint {
            equity_usd: 500_000.0,
            target_leverage: 1.25,
        },
    ]
}

fn fill_notional_usd(fill: &UserFill) -> f64 {
    let px = fill.px.as_deref().map(parse_f64).unwrap_or(0.0);
    let sz = fill.sz.as_deref().map(parse_f64).unwrap_or(0.0).abs();
    px * sz
}

fn fill_closes_position(fill: &UserFill) -> bool {
    let closed_pnl = fill.closed_pnl.as_deref().map(parse_f64).unwrap_or(0.0);
    if closed_pnl != 0.0 {
        return true;
    }

    fill.dir
        .as_deref()
        .map(|direction| {
            let direction = direction.to_ascii_lowercase();
            direction.contains("close") || direction.contains('>')
        })
        .unwrap_or(false)
}

fn fixed_hourly_returns<I>(samples: I, lookback_hours: i64) -> Vec<f64>
where
    I: IntoIterator<Item = (i64, f64)>,
{
    const HOUR_MS: i64 = 60 * 60 * 1_000;
    let bucket_count = lookback_hours.max(1) as usize;
    let end_hour = Utc::now().timestamp_millis().div_euclid(HOUR_MS) * HOUR_MS;
    let start_hour = end_hour - (bucket_count.saturating_sub(1) as i64) * HOUR_MS;
    let mut returns = vec![0.0; bucket_count];

    for (timestamp_ms, value) in samples {
        let hour = timestamp_ms.div_euclid(HOUR_MS) * HOUR_MS;
        if hour < start_hour || hour > end_hour {
            continue;
        }
        let index = ((hour - start_hour) / HOUR_MS) as usize;
        returns[index] += value;
    }
    returns
}

fn copied_target_notional(
    trader_exposure: f64,
    follower_equity: f64,
    target_leverage: f64,
    allocation_weight: f64,
    confidence_modifier: f64,
) -> f64 {
    trader_exposure
        * follower_equity.max(0.0)
        * target_leverage.max(0.0)
        * allocation_weight.max(0.0)
        * confidence_modifier.clamp(0.0, 1.0)
}

#[derive(Debug, Clone)]
struct SourceConsensusInput {
    candidate_address: String,
    allocation_weight: f64,
    confidence_modifier: f64,
    source_exposure: f64,
    enabled: bool,
    quarantined: bool,
    snapshot_age_ms: u64,
}

#[derive(Debug, Clone)]
struct ConsensusResult {
    exposure: f64,
    contribution_by_candidate: Vec<(String, f64)>,
}

fn bounded_additive_consensus(
    inputs: &[SourceConsensusInput],
    max_source_exposure: f64,
    source_snapshot_max_age_ms: u64,
) -> Result<ConsensusResult, Box<dyn Error>> {
    if !max_source_exposure.is_finite() || max_source_exposure < 0.0 {
        return Err("max_source_exposure must be finite and non-negative".into());
    }
    if source_snapshot_max_age_ms == 0 {
        return Err("source_snapshot_max_age_ms must be positive".into());
    }

    let mut contributions = BTreeMap::new();
    for input in inputs {
        if !input.allocation_weight.is_finite() || input.allocation_weight < 0.0 {
            return Err(
                format!("invalid allocation_weight for {}", input.candidate_address).into(),
            );
        }
        ensure_finite_in_range(
            &format!("confidence_modifier for {}", input.candidate_address),
            input.confidence_modifier,
            0.0,
            1.0,
        )?;
        if !input.source_exposure.is_finite() {
            return Err(
                format!("non-finite source exposure for {}", input.candidate_address).into(),
            );
        }

        if !input.enabled
            || input.quarantined
            || input.snapshot_age_ms > source_snapshot_max_age_ms
            || input.allocation_weight == 0.0
        {
            continue;
        }
        if contributions.contains_key(&input.candidate_address) {
            return Err(format!(
                "duplicate candidate contribution for {}",
                input.candidate_address
            )
            .into());
        }
        let contribution = input.allocation_weight
            * input.confidence_modifier
            * input
                .source_exposure
                .clamp(-max_source_exposure, max_source_exposure);
        if !contribution.is_finite() {
            return Err(format!(
                "non-finite source contribution for {}",
                input.candidate_address
            )
            .into());
        }
        contributions.insert(input.candidate_address.clone(), contribution);
    }

    if contributions.is_empty() {
        return Ok(ConsensusResult {
            exposure: 0.0,
            contribution_by_candidate: Vec::new(),
        });
    }
    let exposure = contributions.values().try_fold(0.0_f64, |sum, value| {
        let next = sum + value;
        next.is_finite().then_some(next)
    });
    let Some(exposure) = exposure else {
        return Err("consensus exposure is non-finite".into());
    };
    Ok(ConsensusResult {
        exposure: exposure.clamp(-1.0, 1.0),
        contribution_by_candidate: contributions.into_iter().collect(),
    })
}

fn is_reducing_delta(current_notional: f64, delta_notional: f64) -> bool {
    current_notional != 0.0
        && delta_notional != 0.0
        && delta_notional.signum() != current_notional.signum()
}

fn ledger_attributions_for_order(
    attributions: &[SignalAttribution],
    side_sign: f64,
    fallback_attributions: &[LedgerAttribution],
) -> Vec<LedgerAttribution> {
    let mut selected = attributions
        .iter()
        .filter(|attribution| attribution.notional_usd.signum() == side_sign)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        selected = attributions.iter().collect();
    }
    if selected.is_empty() {
        return normalize_ledger_attributions(fallback_attributions);
    }

    let total_abs = selected
        .iter()
        .map(|attribution| attribution.notional_usd.abs())
        .sum::<f64>();
    if total_abs <= 0.0 {
        return Vec::new();
    }

    selected
        .into_iter()
        .map(|attribution| LedgerAttribution {
            trader_address: attribution.trader_address.clone(),
            execution_weight: attribution.notional_usd.abs() / total_abs,
            signal_notional_usd: attribution.notional_usd,
            confidence_modifier: attribution.confidence_modifier,
        })
        .collect()
}

fn normalized_signal_attributions(attributions: &[SignalAttribution]) -> Vec<LedgerAttribution> {
    let total_abs = attributions
        .iter()
        .map(|attribution| attribution.notional_usd.abs())
        .sum::<f64>();
    if total_abs <= 0.0 {
        return Vec::new();
    }

    attributions
        .iter()
        .map(|attribution| LedgerAttribution {
            trader_address: attribution.trader_address.clone(),
            execution_weight: attribution.notional_usd.abs() / total_abs,
            signal_notional_usd: attribution.notional_usd,
            confidence_modifier: attribution.confidence_modifier,
        })
        .collect()
}

fn normalize_ledger_attributions(attributions: &[LedgerAttribution]) -> Vec<LedgerAttribution> {
    let total_abs = attributions
        .iter()
        .map(|attribution| attribution.signal_notional_usd.abs())
        .sum::<f64>();
    if total_abs <= 0.0 {
        return Vec::new();
    }

    attributions
        .iter()
        .cloned()
        .map(|mut attribution| {
            attribution.execution_weight = attribution.signal_notional_usd.abs() / total_abs;
            attribution
        })
        .collect()
}

fn generate_cloid(coin: &str, side: &str) -> String {
    let now_ms = Utc::now().timestamp_millis();
    let seed = format!("copytrade:{coin}:{side}:{now_ms}");
    format!(
        "0x{:016x}{:016x}",
        hash_value(&seed),
        hash_value(&(seed.clone() + ":1"))
    )
}

fn parse_cloid(cloid: &str) -> Result<Uuid, Box<dyn Error>> {
    Uuid::parse_str(cloid.trim_start_matches("0x"))
        .map_err(|error| format!("invalid generated cloid {cloid}: {error}").into())
}

fn fill_key(cloid: &str, fill: &UserFill) -> String {
    let raw = format!(
        "{}:{}:{}:{}:{}:{}",
        cloid,
        fill.time.unwrap_or_default(),
        fill.px.as_deref().unwrap_or_default(),
        fill.sz.as_deref().unwrap_or_default(),
        fill.closed_pnl.as_deref().unwrap_or_default(),
        fill.fee.as_deref().unwrap_or_default()
    );
    format!("{:016x}", hash_value(&raw))
}

fn hash_value<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn format_sharpe(sharpe: Option<f64>) -> String {
    match sharpe {
        Some(value) if value.is_finite() => format!("{value:.2}"),
        Some(value) if value.is_sign_positive() => "inf".to_string(),
        Some(_) => "-inf".to_string(),
        None => "n/a".to_string(),
    }
}

fn candidate_label(candidate: &TraderCandidate) -> String {
    if candidate.label.is_empty() {
        candidate.address.clone()
    } else {
        format!("{} ({})", candidate.label, candidate.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_candidate(address_suffix: u8) -> TraderCandidate {
        TraderCandidate {
            address: format!("0x{:040x}", address_suffix),
            label: format!("candidate-{address_suffix}"),
            allocation_weight: 1.0,
            confidence_modifier: Some(1.0),
            enabled: true,
        }
    }

    fn live_risk() -> GlobalRiskConfig {
        GlobalRiskConfig {
            global_risk_scale: 0.025,
            max_single_asset_equity_pct: 0.65,
            max_net_equity_pct: 0.65,
            max_source_exposure: 1.0,
            min_order_notional_usd: 10.0,
            order_rounding_buffer_usd: 0.2,
            closeability_margin_usd: 0.5,
            slot_rank_hysteresis: 0.02,
            source_snapshot_max_age_ms: 40_000,
        }
    }

    fn live_config() -> CopyTradeConfig {
        let mut config = CopyTradeConfig {
            schema_version: Some(LIVE_CONFIG_SCHEMA_VERSION),
            global_risk: Some(live_risk()),
            candidates: vec![live_candidate(1)],
            ..Default::default()
        };
        config.execution.min_order_usd = 10.0;
        config
    }

    fn consensus_input(address_suffix: u8, exposure: f64) -> SourceConsensusInput {
        SourceConsensusInput {
            candidate_address: format!("0x{:040x}", address_suffix),
            allocation_weight: 1.0,
            confidence_modifier: 1.0,
            source_exposure: exposure,
            enabled: true,
            quarantined: false,
            snapshot_age_ms: 0,
        }
    }

    #[test]
    fn live_configuration_requires_explicit_schema_risk_and_candidates() {
        let mut config = live_config();
        assert!(config.validate_live().is_ok());

        config.schema_version = None;
        assert!(config.validate_live().is_err());
        config.schema_version = Some(LIVE_CONFIG_SCHEMA_VERSION);
        config.global_risk = None;
        assert!(config.validate_live().is_err());
        config.global_risk = Some(live_risk());
        config.candidates.clear();
        assert!(config.validate_live().is_err());
    }

    #[test]
    fn live_candidate_validation_fails_closed() {
        let mut config = live_config();
        config.candidates[0].confidence_modifier = None;
        assert!(config.validate_live().is_err());
        config.candidates[0].confidence_modifier = Some(f64::NAN);
        assert!(config.validate_live().is_err());
        config.candidates[0].confidence_modifier = Some(f64::INFINITY);
        assert!(config.validate_live().is_err());
        config.candidates[0].confidence_modifier = Some(1.000_001);
        assert!(config.validate_live().is_err());
        config.candidates[0].confidence_modifier = Some(1.0);
        config.candidates[0].allocation_weight = -0.01;
        assert!(config.validate_live().is_err());
    }

    #[test]
    fn production_candidate_deserialization_has_no_silent_defaults() {
        let missing_weight = r#"{
            "address":"0x0000000000000000000000000000000000000001",
            "label":"missing-weight",
            "confidence_modifier":1.0,
            "enabled":true
        }"#;
        assert!(serde_json::from_str::<TraderCandidate>(missing_weight).is_err());

        let unknown_field = r#"{
            "address":"0x0000000000000000000000000000000000000001",
            "label":"unknown-field",
            "allocation_weight":1.0,
            "confidence_modifier":1.0,
            "enabled":true,
            "confidence_modifer":1.0
        }"#;
        assert!(serde_json::from_str::<TraderCandidate>(unknown_field).is_err());
    }

    #[test]
    fn live_configuration_rejects_duplicate_addresses() {
        let mut config = live_config();
        let mut duplicate = live_candidate(1);
        duplicate.address = duplicate
            .address
            .to_ascii_uppercase()
            .replacen("0X", "0x", 1);
        config.candidates.push(duplicate);
        assert!(config.validate_live().is_err());
    }

    #[test]
    fn live_risk_validation_enforces_approved_bounds() {
        let mut config = live_config();
        config.global_risk.as_mut().unwrap().global_risk_scale = 0.100_001;
        assert!(config.validate_live().is_err());
        config.global_risk.as_mut().unwrap().global_risk_scale = 0.025;
        config
            .global_risk
            .as_mut()
            .unwrap()
            .max_single_asset_equity_pct = f64::INFINITY;
        assert!(config.validate_live().is_err());
    }

    #[test]
    fn non_finite_values_anywhere_in_live_configuration_fail_closed() {
        let mut config = live_config();
        config.leverage_curve[0].target_leverage = f64::NAN;
        assert!(config.validate_live().is_err());

        let mut config = live_config();
        config.performance.target_sharpe = f64::INFINITY;
        assert!(config.validate_live().is_err());
    }

    #[test]
    fn same_side_sources_add_and_saturate() {
        let one = bounded_additive_consensus(&[consensus_input(1, 0.10)], 1.0, 40_000).unwrap();
        assert!((one.exposure - 0.10).abs() < 1e-12);

        let two = bounded_additive_consensus(
            &[consensus_input(1, 0.10), consensus_input(2, 0.10)],
            1.0,
            40_000,
        )
        .unwrap();
        assert!((two.exposure - 0.20).abs() < 1e-12);

        let inputs = (1..=169)
            .map(|index| consensus_input(index as u8, 0.10))
            .collect::<Vec<_>>();
        let consensus = bounded_additive_consensus(&inputs, 1.0, 40_000).unwrap();
        assert_eq!(consensus.exposure, 1.0);
    }

    #[test]
    fn launch_scale_maps_ten_percent_consensus_to_twenty_dollars() {
        let consensus =
            bounded_additive_consensus(&[consensus_input(1, 0.10)], 1.0, 40_000).unwrap();
        let target = 1_000.0 * 8.0 * 0.025 * consensus.exposure;
        assert!((target - 20.0).abs() < 1e-12);
    }

    #[test]
    fn equal_opposing_sources_net_to_zero() {
        let inputs = vec![consensus_input(1, 0.10), consensus_input(2, -0.10)];
        let consensus = bounded_additive_consensus(&inputs, 1.0, 40_000).unwrap();
        assert!(consensus.exposure.abs() < 1e-12);
    }

    #[test]
    fn confidence_attenuates_only_its_source_contribution() {
        let mut input = consensus_input(1, 0.10);
        input.confidence_modifier = 0.10;
        let consensus = bounded_additive_consensus(&[input], 1.0, 40_000).unwrap();
        assert!((consensus.exposure - 0.01).abs() < 1e-12);
    }

    #[test]
    fn stale_and_quarantined_sources_are_excluded_from_both_sides() {
        let active = consensus_input(1, 0.10);
        let mut stale = consensus_input(2, -0.90);
        stale.allocation_weight = 100.0;
        stale.snapshot_age_ms = 40_001;
        let mut quarantined = consensus_input(3, -0.90);
        quarantined.allocation_weight = 100.0;
        quarantined.quarantined = true;

        let consensus =
            bounded_additive_consensus(&[active, stale, quarantined], 1.0, 40_000).unwrap();
        assert!((consensus.exposure - 0.10).abs() < 1e-12);
    }

    #[test]
    fn no_active_sources_produces_zero_target_exposure() {
        let mut input = consensus_input(1, 0.10);
        input.enabled = false;
        let consensus = bounded_additive_consensus(&[input], 1.0, 40_000).unwrap();
        assert_eq!(consensus.exposure, 0.0);
        assert!(consensus.contribution_by_candidate.is_empty());
    }

    #[test]
    fn non_finite_consensus_inputs_reject_the_decision() {
        let mut input = consensus_input(1, f64::NAN);
        assert!(bounded_additive_consensus(&[input.clone()], 1.0, 40_000).is_err());
        input.source_exposure = f64::INFINITY;
        assert!(bounded_additive_consensus(&[input.clone()], 1.0, 40_000).is_err());
        input.source_exposure = 0.10;
        input.allocation_weight = f64::NEG_INFINITY;
        assert!(bounded_additive_consensus(&[input], 1.0, 40_000).is_err());
    }

    #[test]
    fn default_active_limit_removes_three_wallet_ceiling() {
        let config = CopyTradeConfig::default();

        assert_eq!(config.active_limit(), usize::MAX);
    }

    #[test]
    fn fill_stats_calculates_local_win_rate() {
        let stats = FillStats {
            closed_trades: 4,
            wins: 3,
            closed_pnl_usd: 42.0,
        };

        assert_eq!(stats.win_rate_pct(), 75.0);
    }

    #[test]
    fn plan_orders_caps_single_order_and_sets_reduce_only() {
        let config = CopyTradeConfig {
            max_single_order_notional_usd: 100.0,
            ..Default::default()
        };
        let engine = CopyTradeEngine::test_only(config);
        let mids = HashMap::from([("BTC".to_string(), 50_000.0)]);
        let target = HashMap::from([("BTC".to_string(), -500.0)]);
        let current = HashMap::from([("BTC".to_string(), 200.0)]);

        let plans = engine.plan_orders(
            1_000.0,
            &mids,
            target,
            current,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].side, "sell");
        assert!(plans[0].reduce_only);
        assert!(plans[0].notional_usd <= 101.0);
    }

    #[test]
    fn zero_active_traders_means_unlimited() {
        let config = CopyTradeConfig {
            unlimited_mode: false,
            max_active_traders: Some(0),
            ..Default::default()
        };

        assert_eq!(config.active_limit(), usize::MAX);
    }

    #[test]
    fn leverage_curve_matches_base_case_and_delevers_at_size() {
        let config = CopyTradeConfig::default();

        assert_eq!(config.starting_equity_usd, 100.0);
        assert!((config.target_leverage_for_equity(100.0) - 4.0).abs() < 0.0001);
        assert!((config.target_leverage_for_equity(1_000.0) - 8.0).abs() < 0.0001);
        assert!((config.target_leverage_for_equity(10_000.0) - 6.0).abs() < 0.0001);
        assert!((config.target_leverage_for_equity(100_000.0) - 3.0).abs() < 0.0001);
        assert!((config.target_leverage_for_equity(500_000.0) - 1.25).abs() < 0.0001);
    }

    #[test]
    fn leverage_curve_respects_static_hard_cap() {
        let config = CopyTradeConfig {
            max_total_leverage: 5.0,
            ..Default::default()
        };

        assert!((config.target_leverage_for_equity(1_000.0) - 5.0).abs() < 0.0001);
        assert!((config.target_leverage_for_equity(500_000.0) - 1.25).abs() < 0.0001);
    }

    #[test]
    fn confidence_score_requires_sample_size() {
        let low_sample = FillStats {
            closed_trades: 10,
            wins: 10,
            closed_pnl_usd: 10.0,
        };
        let full_sample = FillStats {
            closed_trades: 50,
            wins: 49,
            closed_pnl_usd: 10.0,
        };

        assert_eq!(low_sample.confidence_score(), 20.0);
        assert_eq!(full_sample.confidence_score(), 98.0);
        assert!((full_sample.confidence_modifier() - 0.98).abs() < 0.001);
    }

    #[test]
    fn plan_orders_respects_dynamic_capacity() {
        let config = CopyTradeConfig {
            max_single_order_notional_usd: 2_500.0,
            min_order_notional_usd: 15.0,
            ..Default::default()
        };
        let engine = CopyTradeEngine::test_only(config);
        let mids = HashMap::from([("ETH".to_string(), 2_000.0)]);
        let target = HashMap::from([("ETH".to_string(), 1_000.0)]);
        let current = HashMap::new();
        let capacity = HashMap::from([("ETH".to_string(), 300.0)]);
        let confidence = HashMap::from([("ETH".to_string(), 99.0)]);

        let plans = engine.plan_orders(
            10_000.0,
            &mids,
            target,
            current,
            capacity,
            confidence,
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].side, "buy");
        assert!(plans[0].notional_usd < 301.0);
        assert_eq!(plans[0].confidence_score, 99.0);
    }

    #[test]
    fn performance_snapshot_nets_actual_and_estimated_fees() {
        let config = CopyTradeConfig {
            maker_fee_bps: 1.5,
            taker_fee_bps: 4.5,
            ..Default::default()
        };
        let engine = CopyTradeEngine::test_only(config);
        let now_ms = Utc::now().timestamp_millis();
        let fills = vec![
            UserFill {
                dir: None,
                closed_pnl: Some("5.0".to_string()),
                fee: Some("0.45".to_string()),
                px: Some("100.0".to_string()),
                sz: Some("10.0".to_string()),
                crossed: Some(true),
                cloid: Some("0xabc".to_string()),
                time: Some(now_ms - 2 * 60 * 60 * 1_000),
            },
            UserFill {
                dir: None,
                closed_pnl: Some("4.0".to_string()),
                fee: None,
                px: Some("100.0".to_string()),
                sz: Some("10.0".to_string()),
                crossed: Some(false),
                cloid: Some("0xdef".to_string()),
                time: Some(now_ms - 60 * 60 * 1_000),
            },
            UserFill {
                dir: None,
                closed_pnl: Some("-1.0".to_string()),
                fee: None,
                px: Some("100.0".to_string()),
                sz: Some("10.0".to_string()),
                crossed: Some(true),
                cloid: Some("0xghi".to_string()),
                time: Some(now_ms),
            },
        ];

        let snapshot = engine.performance_from_fills(&fills, 1_000.0, 96);

        assert_eq!(snapshot.sample_count, 3);
        assert_eq!(snapshot.maker_fills, 1);
        assert_eq!(snapshot.taker_fills, 2);
        assert_eq!(snapshot.estimated_fee_fills, 2);
        assert!((snapshot.total_fees_usd - 1.05).abs() < 0.0001);
        assert!((snapshot.net_closed_pnl_usd - 6.95).abs() < 0.0001);
        assert!(snapshot.annualized_sharpe.is_some());
    }

    #[test]
    fn performance_includes_entry_fees_without_counting_entry_as_close() {
        let engine = CopyTradeEngine::test_only(CopyTradeConfig::default());
        let fills = vec![
            UserFill {
                dir: Some("Open Long".to_string()),
                closed_pnl: Some("0.0".to_string()),
                fee: Some("0.45".to_string()),
                px: Some("100.0".to_string()),
                sz: Some("10.0".to_string()),
                crossed: Some(true),
                cloid: Some("0xopen".to_string()),
                time: Some(1),
            },
            UserFill {
                dir: Some("Close Long".to_string()),
                closed_pnl: Some("10.0".to_string()),
                fee: Some("0.45".to_string()),
                px: Some("110.0".to_string()),
                sz: Some("10.0".to_string()),
                crossed: Some(true),
                cloid: Some("0xclose".to_string()),
                time: Some(2),
            },
        ];

        let snapshot = engine.performance_from_fills(&fills, 1_000.0, 96);

        assert_eq!(snapshot.sample_count, 1);
        assert!((snapshot.total_fees_usd - 0.90).abs() < 0.0001);
        assert!((snapshot.net_closed_pnl_usd - 9.10).abs() < 0.0001);
    }

    #[test]
    fn splitting_a_fill_does_not_inflate_hourly_sharpe() {
        let engine = CopyTradeEngine::test_only(CopyTradeConfig::default());
        let now_ms = Utc::now().timestamp_millis();
        let make_fill = |pnl: &str, size: &str| UserFill {
            dir: Some("Close Long".to_string()),
            closed_pnl: Some(pnl.to_string()),
            fee: Some("0.0".to_string()),
            px: Some("100.0".to_string()),
            sz: Some(size.to_string()),
            crossed: Some(true),
            cloid: None,
            time: Some(now_ms),
        };

        let single = engine.performance_from_fills(&[make_fill("10.0", "1.0")], 1_000.0, 96);
        let split = engine.performance_from_fills(
            &[make_fill("5.0", "0.5"), make_fill("5.0", "0.5")],
            1_000.0,
            96,
        );

        assert_eq!(single.annualized_sharpe, split.annualized_sharpe);
        assert_eq!(single.mean_return, split.mean_return);
        assert_eq!(single.std_return, split.std_return);
    }

    #[test]
    fn sharpe_gate_blocks_new_risk_but_allows_reductions() {
        let config = CopyTradeConfig {
            sharpe_target: 15.0,
            min_sharpe_samples: 2,
            enforce_sharpe_gate: true,
            ..Default::default()
        };
        let engine = CopyTradeEngine::test_only(config);
        let snapshot = PerformanceSnapshot {
            sample_count: 5,
            annualized_sharpe: Some(3.0),
            ..Default::default()
        };
        let plans = vec![
            PlannedOrder {
                cloid: "0x1".to_string(),
                coin: "BTC".to_string(),
                side: "buy",
                reduce_only: false,
                size: 0.01,
                limit_px: 50_000.0,
                notional_usd: 500.0,
                estimated_fee_usd: 0.225,
                confidence_score: 99.0,
                attributions: Vec::new(),
                reason: "new risk".to_string(),
            },
            PlannedOrder {
                cloid: "0x2".to_string(),
                coin: "ETH".to_string(),
                side: "sell",
                reduce_only: true,
                size: 0.2,
                limit_px: 2_000.0,
                notional_usd: 400.0,
                estimated_fee_usd: 0.18,
                confidence_score: 90.0,
                attributions: Vec::new(),
                reason: "reduce risk".to_string(),
            },
        ];

        let filtered = engine.filter_orders_for_performance(plans, Some(&snapshot));

        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].reduce_only);
        assert_eq!(filtered[0].coin, "ETH");
    }

    #[test]
    fn ledger_fractionalizes_fill_pnl_and_fee_by_execution_weight() {
        let mut ledger = SignalLedger::default();
        let order = PlannedOrder {
            cloid: "0xabc".to_string(),
            coin: "BTC".to_string(),
            side: "buy",
            reduce_only: false,
            size: 0.01,
            limit_px: 50_000.0,
            notional_usd: 500.0,
            estimated_fee_usd: 0.225,
            confidence_score: 99.0,
            attributions: vec![
                LedgerAttribution {
                    trader_address: "wallet-a".to_string(),
                    execution_weight: 0.75,
                    signal_notional_usd: 750.0,
                    confidence_modifier: 1.0,
                },
                LedgerAttribution {
                    trader_address: "wallet-b".to_string(),
                    execution_weight: 0.25,
                    signal_notional_usd: 250.0,
                    confidence_modifier: 1.0,
                },
            ],
            reason: "test".to_string(),
        };
        ledger.record_submitted_order(&order, 1);
        let fill = UserFill {
            dir: None,
            closed_pnl: Some("10.0".to_string()),
            fee: Some("1.0".to_string()),
            px: Some("100.0".to_string()),
            sz: Some("10.0".to_string()),
            crossed: Some(true),
            cloid: Some("0xabc".to_string()),
            time: Some(2),
        };

        assert!(ledger.reconcile_fill(
            &fill,
            "fill-1".to_string(),
            1.0,
            1_000.0,
            96,
            15.0,
            10.0,
            0.1,
            2
        ));

        let wallet_a = ledger.traders.get("wallet-a").unwrap();
        let wallet_b = ledger.traders.get("wallet-b").unwrap();
        assert!((wallet_a.net_pnl_usd - 6.75).abs() < 0.0001);
        assert!((wallet_a.total_fees_usd - 0.75).abs() < 0.0001);
        assert!((wallet_b.net_pnl_usd - 2.25).abs() < 0.0001);
        assert!((wallet_b.total_fees_usd - 0.25).abs() < 0.0001);
    }

    #[test]
    fn ledger_decays_confidence_when_isolated_sharpe_misses_target() {
        let mut ledger = SignalLedger::default();
        let order = PlannedOrder {
            cloid: "0xdecay".to_string(),
            coin: "BTC".to_string(),
            side: "buy",
            reduce_only: false,
            size: 0.01,
            limit_px: 50_000.0,
            notional_usd: 500.0,
            estimated_fee_usd: 0.225,
            confidence_score: 99.0,
            attributions: vec![LedgerAttribution {
                trader_address: "wallet-a".to_string(),
                execution_weight: 1.0,
                signal_notional_usd: 500.0,
                confidence_modifier: 1.0,
            }],
            reason: "test".to_string(),
        };
        ledger.record_submitted_order(&order, 1);
        let now_ms = Utc::now().timestamp_millis();
        let first_fill = UserFill {
            dir: None,
            closed_pnl: Some("10.0".to_string()),
            fee: Some("1.0".to_string()),
            px: Some("100.0".to_string()),
            sz: Some("10.0".to_string()),
            crossed: Some(true),
            cloid: Some("0xdecay".to_string()),
            time: Some(now_ms - 1),
        };
        let second_fill = UserFill {
            dir: None,
            closed_pnl: Some("-1.0".to_string()),
            fee: Some("1.0".to_string()),
            px: Some("100.0".to_string()),
            sz: Some("10.0".to_string()),
            crossed: Some(true),
            cloid: Some("0xdecay".to_string()),
            time: Some(now_ms),
        };

        assert!(ledger.reconcile_fill(
            &first_fill,
            "fill-1".to_string(),
            1.0,
            1_000.0,
            96,
            30.0,
            10.0,
            0.1,
            2
        ));
        assert!(ledger.reconcile_fill(
            &second_fill,
            "fill-2".to_string(),
            1.0,
            1_000.0,
            96,
            30.0,
            10.0,
            0.1,
            2
        ));

        let wallet_a = ledger.traders.get("wallet-a").unwrap();
        assert_eq!(wallet_a.closed_samples, 2);
        assert!(wallet_a.sharpe(96).unwrap() < 30.0);
        assert!(wallet_a.confidence_modifier < 1.0);
    }

    #[test]
    fn leverage_curve_scales_copied_exposure() {
        let target = copied_target_notional(0.10, 1_000.0, 8.0, 1.0, 1.0);

        assert!((target - 800.0).abs() < 0.0001);
    }

    #[test]
    fn plan_orders_closes_position_when_target_asset_disappears() {
        let engine = CopyTradeEngine::test_only(CopyTradeConfig::default());
        let mids = HashMap::from([("SOL".to_string(), 100.0)]);
        let current = HashMap::from([("SOL".to_string(), 400.0)]);
        let fallback = HashMap::from([(
            "SOL".to_string(),
            vec![LedgerAttribution {
                trader_address: "wallet-a".to_string(),
                execution_weight: 1.0,
                signal_notional_usd: 400.0,
                confidence_modifier: 1.0,
            }],
        )]);

        let plans = engine.plan_orders(
            1_000.0,
            &mids,
            HashMap::new(),
            current,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            fallback,
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].side, "sell");
        assert!(plans[0].reduce_only);
        assert!((plans[0].notional_usd - 400.0).abs() < 0.0001);
        assert_eq!(plans[0].attributions[0].trader_address, "wallet-a");
    }

    #[test]
    fn conflicting_signals_emit_single_net_order_with_driver_attribution() {
        let config = CopyTradeConfig {
            max_single_order_notional_usd: 10_000.0,
            ..Default::default()
        };
        let engine = CopyTradeEngine::test_only(config);
        let mids = HashMap::from([("ETH".to_string(), 2_000.0)]);
        let target = HashMap::from([("ETH".to_string(), 4_000.0)]);
        let signals = HashMap::from([(
            "ETH".to_string(),
            vec![
                SignalAttribution {
                    trader_address: "wallet-a".to_string(),
                    notional_usd: 5_000.0,
                    confidence_modifier: 1.0,
                },
                SignalAttribution {
                    trader_address: "wallet-b".to_string(),
                    notional_usd: -2_000.0,
                    confidence_modifier: 1.0,
                },
                SignalAttribution {
                    trader_address: "wallet-c".to_string(),
                    notional_usd: 1_000.0,
                    confidence_modifier: 1.0,
                },
            ],
        )]);

        let plans = engine.plan_orders(
            1_000.0,
            &mids,
            target,
            HashMap::new(),
            HashMap::from([("ETH".to_string(), 5_200.0)]),
            HashMap::new(),
            signals,
            HashMap::new(),
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].side, "buy");
        assert!((plans[0].notional_usd - 4_000.0).abs() < 0.0001);
        assert_eq!(plans[0].attributions.len(), 2);
        assert!(plans[0]
            .attributions
            .iter()
            .all(|attribution| attribution.trader_address != "wallet-b"));
    }

    #[test]
    fn perfect_signal_standoff_emits_no_order() {
        let engine = CopyTradeEngine::test_only(CopyTradeConfig::default());
        let mids = HashMap::from([("ETH".to_string(), 2_000.0)]);

        let plans = engine.plan_orders(
            1_000.0,
            &mids,
            HashMap::from([("ETH".to_string(), 0.0)]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        assert!(plans.is_empty());
    }

    #[test]
    fn reductions_execute_first_and_release_gross_capacity() {
        let engine = CopyTradeEngine::test_only(CopyTradeConfig::default());
        let mids = HashMap::from([("ETH".to_string(), 2_000.0), ("BTC".to_string(), 50_000.0)]);
        let target = HashMap::from([("ETH".to_string(), 0.0), ("BTC".to_string(), 260.0)]);
        let current = HashMap::from([("ETH".to_string(), 400.0)]);
        let confidence = HashMap::from([("ETH".to_string(), 10.0), ("BTC".to_string(), 99.0)]);

        let plans = engine.plan_orders(
            100.0,
            &mids,
            target,
            current,
            HashMap::new(),
            confidence,
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].coin, "ETH");
        assert!(plans[0].reduce_only);
        assert_eq!(plans[1].coin, "BTC");
        assert!(!plans[1].reduce_only);
        assert!((plans[1].notional_usd - 260.0).abs() < 0.0001);
    }

    #[test]
    fn direction_flip_closes_before_opening_opposite_risk() {
        let config = CopyTradeConfig {
            max_single_order_notional_usd: 1_000.0,
            ..Default::default()
        };
        let engine = CopyTradeEngine::test_only(config);
        let mids = HashMap::from([("BTC".to_string(), 50_000.0)]);

        let plans = engine.plan_orders(
            1_000.0,
            &mids,
            HashMap::from([("BTC".to_string(), -500.0)]),
            HashMap::from([("BTC".to_string(), 200.0)]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(plans.len(), 1);
        assert!(plans[0].reduce_only);
        assert_eq!(plans[0].side, "sell");
        assert!((plans[0].notional_usd - 200.0).abs() < 0.0001);
    }

    #[test]
    fn drawdown_guard_blocks_entries_but_keeps_exits() {
        let engine = CopyTradeEngine::test_only(CopyTradeConfig::default());
        let plans = vec![
            PlannedOrder {
                cloid: generate_cloid("BTC", "buy"),
                coin: "BTC".to_string(),
                side: "buy",
                reduce_only: false,
                size: 0.01,
                limit_px: 50_000.0,
                notional_usd: 500.0,
                estimated_fee_usd: 0.225,
                confidence_score: 99.0,
                attributions: Vec::new(),
                reason: "entry".to_string(),
            },
            PlannedOrder {
                cloid: generate_cloid("ETH", "sell"),
                coin: "ETH".to_string(),
                side: "sell",
                reduce_only: true,
                size: 0.2,
                limit_px: 2_000.0,
                notional_usd: 400.0,
                estimated_fee_usd: 0.18,
                confidence_score: 90.0,
                attributions: Vec::new(),
                reason: "exit".to_string(),
            },
        ];

        let filtered = engine.filter_orders_for_drawdown(plans);

        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].reduce_only);
    }

    #[test]
    fn opening_fill_is_reconciled_without_counting_closed_sample() {
        let mut ledger = SignalLedger::default();
        let cloid = generate_cloid("BTC", "buy");
        let order = PlannedOrder {
            cloid: cloid.clone(),
            coin: "BTC".to_string(),
            side: "buy",
            reduce_only: false,
            size: 0.01,
            limit_px: 50_000.0,
            notional_usd: 500.0,
            estimated_fee_usd: 0.225,
            confidence_score: 99.0,
            attributions: vec![LedgerAttribution {
                trader_address: "wallet-a".to_string(),
                execution_weight: 1.0,
                signal_notional_usd: 500.0,
                confidence_modifier: 1.0,
            }],
            reason: "entry".to_string(),
        };
        ledger.record_submitted_order(&order, 1);
        let fill = UserFill {
            dir: Some("Open Long".to_string()),
            closed_pnl: Some("0.0".to_string()),
            fee: Some("0.225".to_string()),
            px: Some("50000.0".to_string()),
            sz: Some("0.01".to_string()),
            crossed: Some(true),
            cloid: Some(cloid.clone()),
            time: Some(Utc::now().timestamp_millis()),
        };

        assert!(ledger.reconcile_fill(
            &fill,
            "opening-fill".to_string(),
            0.225,
            1_000.0,
            96,
            15.0,
            10.0,
            0.32,
            50,
        ));
        assert_eq!(ledger.orders[&cloid].status, LedgerOrderStatus::Accepted);
        assert_eq!(ledger.orders[&cloid].filled_notional_usd, 500.0);
        assert_eq!(ledger.traders["wallet-a"].closed_samples, 0);
        assert!((ledger.traders["wallet-a"].pending_fees_usd - 0.225).abs() < 0.0001);
    }

    #[test]
    fn generated_cloid_is_exchange_parseable() {
        let cloid = generate_cloid("SOL", "buy");

        assert!(parse_cloid(&cloid).is_ok());
    }

    #[test]
    fn isolated_sharpe_ignores_expired_return_samples() {
        let stale_ms = Utc::now().timestamp_millis() - 200 * 60 * 60 * 1_000;
        let stats = LedgerTraderStats {
            return_samples: vec![
                LedgerReturn {
                    timestamp_ms: stale_ms,
                    value: 0.01,
                },
                LedgerReturn {
                    timestamp_ms: stale_ms + 1,
                    value: -0.01,
                },
            ],
            ..Default::default()
        };

        assert!(stats.sharpe(96).is_none());
    }
}
