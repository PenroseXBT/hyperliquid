use rust_decimal::prelude::{Signed, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CandleInterval {
    FifteenMinutes,
    ThirtyMinutes,
    OneHour,
    FourHours,
    TwelveHours,
}

impl CandleInterval {
    pub const ALL: [Self; 5] = [
        Self::FifteenMinutes,
        Self::ThirtyMinutes,
        Self::OneHour,
        Self::FourHours,
        Self::TwelveHours,
    ];

    pub const fn api_name(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::ThirtyMinutes => "30m",
            Self::OneHour => "1h",
            Self::FourHours => "4h",
            Self::TwelveHours => "12h",
        }
    }

    pub const fn duration_ms(self) -> u64 {
        match self {
            Self::FifteenMinutes => 15 * 60 * 1_000,
            Self::ThirtyMinutes => 30 * 60 * 1_000,
            Self::OneHour => 60 * 60 * 1_000,
            Self::FourHours => 4 * 60 * 60 * 1_000,
            Self::TwelveHours => 12 * 60 * 60 * 1_000,
        }
    }

    pub const fn warmup_candles(self) -> usize {
        match self {
            Self::FifteenMinutes => 22,
            Self::ThirtyMinutes => 51,
            Self::OneHour => 51,
            Self::FourHours => 101,
            Self::TwelveHours => 201,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedCandle {
    pub asset: String,
    pub interval: CandleInterval,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub base_volume: Decimal,
    pub trade_count: u64,
}

impl ClosedCandle {
    pub fn payload_hash(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(&(
            &self.asset,
            self.interval,
            self.open_time_ms,
            self.close_time_ms,
            self.open.normalize(),
            self.high.normalize(),
            self.low.normalize(),
            self.close.normalize(),
            self.base_volume.normalize(),
            self.trade_count,
        ))
        .expect("closed candle is serializable");
        Sha256::digest(bytes).into()
    }

    fn validate(&self) -> Result<(), TechnicalError> {
        if self.asset.is_empty()
            || self.open_time_ms >= self.close_time_ms
            || self.open <= Decimal::ZERO
            || self.high <= Decimal::ZERO
            || self.low <= Decimal::ZERO
            || self.close <= Decimal::ZERO
            || self.base_volume < Decimal::ZERO
            || self.high < self.low
            || self.high < self.open.max(self.close)
            || self.low > self.open.min(self.close)
        {
            return Err(TechnicalError::InvalidCandle);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    StrongBull,
    TransitionUp,
    Range,
    TransitionDown,
    StrongBear,
}

impl MarketRegime {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StrongBull => "strong_bull",
            Self::TransitionUp => "transition_up",
            Self::Range => "range",
            Self::TransitionDown => "transition_down",
            Self::StrongBear => "strong_bear",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalArchetype {
    TrendPullback,
    RangeMeanReversion,
    RegimeBreakout,
}

impl SignalArchetype {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TrendPullback => "trend_pullback",
            Self::RangeMeanReversion => "range_mean_reversion",
            Self::RegimeBreakout => "regime_breakout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrendState {
    Bullish,
    Neutral,
    Bearish,
}

impl TrendState {
    pub const fn score(self) -> i8 {
        match self {
            Self::Bullish => 1,
            Self::Neutral => 0,
            Self::Bearish => -1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerState {
    Bullish,
    Neutral,
    Bearish,
}

impl TriggerState {
    pub const fn score(self) -> i8 {
        match self {
            Self::Bullish => 1,
            Self::Neutral => 0,
            Self::Bearish => -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegimeState {
    pub regime: MarketRegime,
    pub direction: i8,
    pub ma50_normalized_slope: Decimal,
    pub ma100_normalized_slope: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TechnicalState {
    pub regime_12h: RegimeState,
    pub trend_4h: TrendState,
    pub trend_1h: TrendState,
    pub trigger_30m: TriggerState,
    pub trigger_15m: TriggerState,
}

/// Raw, read-only technical features for conditioning another decision model.
///
/// Unlike [`TechnicalTarget`], this context has not passed through the entry
/// threshold, position hysteresis, exit timing, sizing, or execution-cost
/// admission in [`TechnicalEngine::evaluate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TechnicalContext {
    pub state: TechnicalState,
    pub archetype: SignalArchetype,
    pub raw_contextual_score: Decimal,
    pub atr_fraction: Decimal,
    pub candle_close_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TechnicalTarget {
    pub asset: String,
    pub score: Decimal,
    pub desired_notional: Decimal,
    pub regime: MarketRegime,
    pub archetype: SignalArchetype,
    pub candle_close_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TechnicalStrategyConfig {
    pub enabled: bool,
    /// Legacy research-only sleeve metadata. The production MFCE allocator
    /// does not read either fraction.
    pub source_budget_fraction: f64,
    /// Legacy research-only sleeve metadata. Technical state is context only
    /// in the production daemon.
    pub technical_budget_fraction: f64,
    pub universe_size: usize,
    pub enter_threshold: f64,
    pub maintain_threshold: f64,
    pub reduce_threshold: f64,
    pub flatten_threshold: f64,
    pub pullback_volume_z: f64,
    pub breakout_volume_z: f64,
    pub expected_atr_fraction: f64,
    pub minimum_cost_multiple: f64,
    pub expected_holding_hours: u32,
}

impl Default for TechnicalStrategyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            source_budget_fraction: 0.35,
            technical_budget_fraction: 0.65,
            universe_size: 25,
            enter_threshold: 0.65,
            maintain_threshold: 0.50,
            reduce_threshold: 0.45,
            flatten_threshold: 0.25,
            pullback_volume_z: 1.0,
            breakout_volume_z: 1.5,
            expected_atr_fraction: 0.5,
            minimum_cost_multiple: 2.0,
            expected_holding_hours: 6,
        }
    }
}

impl TechnicalStrategyConfig {
    pub fn validate(&self) -> Result<(), TechnicalError> {
        let finite = [
            self.source_budget_fraction,
            self.technical_budget_fraction,
            self.enter_threshold,
            self.maintain_threshold,
            self.reduce_threshold,
            self.flatten_threshold,
            self.pullback_volume_z,
            self.breakout_volume_z,
            self.expected_atr_fraction,
            self.minimum_cost_multiple,
        ]
        .into_iter()
        .all(f64::is_finite);
        if !finite
            || self.source_budget_fraction < 0.0
            || self.technical_budget_fraction < 0.0
            || (self.source_budget_fraction + self.technical_budget_fraction - 1.0).abs() > 1e-12
            || self.universe_size == 0
            || self.universe_size > 30
            || !(0.0..=1.0).contains(&self.enter_threshold)
            || !(0.0..=1.0).contains(&self.maintain_threshold)
            || !(0.0..=1.0).contains(&self.reduce_threshold)
            || !(0.0..=1.0).contains(&self.flatten_threshold)
            || !(self.enter_threshold > self.maintain_threshold
                && self.maintain_threshold > self.reduce_threshold
                && self.reduce_threshold > self.flatten_threshold)
            || self.expected_atr_fraction <= 0.0
            || self.minimum_cost_multiple < 1.0
            || self.expected_holding_hours == 0
        {
            return Err(TechnicalError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CostEstimate {
    pub entry_fee_fraction: f64,
    pub exit_fee_fraction: f64,
    pub entry_slippage_fraction: f64,
    pub exit_slippage_fraction: f64,
    pub hourly_funding_fraction: f64,
}

impl CostEstimate {
    pub fn round_trip(self, expected_holding_hours: u32) -> Result<f64, TechnicalError> {
        let values = [
            self.entry_fee_fraction,
            self.exit_fee_fraction,
            self.entry_slippage_fraction,
            self.exit_slippage_fraction,
            self.hourly_funding_fraction,
        ];
        if values
            .into_iter()
            .any(|value| !value.is_finite() || value < 0.0)
        {
            return Err(TechnicalError::InvalidCost);
        }
        Ok(self.entry_fee_fraction
            + self.exit_fee_fraction
            + self.entry_slippage_fraction
            + self.exit_slippage_fraction
            + self.hourly_funding_fraction * f64::from(expected_holding_hours))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandleAcceptance {
    Accepted,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandleFieldDifference {
    pub field: String,
    pub accepted: String,
    pub conflicting: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandleConflict {
    pub asset: String,
    pub interval: CandleInterval,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub accepted_payload_sha256: String,
    pub conflicting_payload_sha256: String,
    pub field_differences: Vec<CandleFieldDifference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TechnicalError {
    InvalidCandle,
    ConflictingCandle(CandleConflict),
    NonMonotonicCandle,
    InvalidConfiguration,
    InvalidCost,
    Arithmetic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct CandleIdentity {
    interval: CandleInterval,
    open_time_ms: u64,
    close_time_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitTiming {
    Hold,
    Reduce,
    Flatten,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AssetCandles {
    intervals: BTreeMap<CandleInterval, VecDeque<ClosedCandle>>,
    accepted: BTreeSet<CandleIdentity>,
    current_score: Decimal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TechnicalFunnel {
    pub closed_candles_accepted: u64,
    pub identical_candle_duplicates: u64,
    pub conflicting_candles: u64,
    pub evaluations: u64,
    pub indicators_ready: u64,
    pub regime_classifications: u64,
    pub scores_crossing_entry_threshold: u64,
    pub primary_trigger_candidates: u64,
    pub secondary_trigger_confirmations: u64,
    pub trigger_confirmations: u64,
    pub nonzero_targets_before_cost: u64,
    pub targets_surviving_cost_admission: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TechnicalEngine {
    config: TechnicalStrategyConfig,
    assets: BTreeMap<String, AssetCandles>,
    #[serde(default)]
    funnel: TechnicalFunnel,
}

impl TechnicalEngine {
    pub fn new(config: TechnicalStrategyConfig) -> Result<Self, TechnicalError> {
        config.validate()?;
        Ok(Self {
            config,
            assets: BTreeMap::new(),
            funnel: TechnicalFunnel::default(),
        })
    }

    pub fn configuration(&self) -> &TechnicalStrategyConfig {
        &self.config
    }

    /// Returns the latest raw technical features without changing engine state.
    ///
    /// This deliberately performs classification only. It does not update the
    /// funnel or current score, and it does not apply trading thresholds,
    /// hysteresis, exit timing, sizing, or execution-cost admission.
    pub fn feature_context(&self, asset: &str) -> Result<Option<TechnicalContext>, TechnicalError> {
        let Some(candles) = self.assets.get(asset) else {
            return Ok(None);
        };
        let Some((state, archetype, candle_close_ms, atr_fraction)) =
            classify(candles, &self.config)?
        else {
            return Ok(None);
        };
        Ok(Some(TechnicalContext {
            raw_contextual_score: Decimal::from_f64_retain(contextual_score(&state))
                .ok_or(TechnicalError::Arithmetic)?,
            state,
            archetype,
            atr_fraction: Decimal::from_f64_retain(atr_fraction)
                .ok_or(TechnicalError::Arithmetic)?,
            candle_close_ms,
        }))
    }

    pub fn accept_closed_candle(
        &mut self,
        candle: ClosedCandle,
    ) -> Result<CandleAcceptance, TechnicalError> {
        candle.validate()?;
        let identity = CandleIdentity {
            interval: candle.interval,
            open_time_ms: candle.open_time_ms,
            close_time_ms: candle.close_time_ms,
        };
        let asset = self.assets.entry(candle.asset.clone()).or_default();
        if asset.accepted.contains(&identity) {
            let accepted = asset
                .intervals
                .get(&candle.interval)
                .and_then(|candles| {
                    candles.iter().find(|accepted| {
                        accepted.open_time_ms == candle.open_time_ms
                            && accepted.close_time_ms == candle.close_time_ms
                    })
                })
                .expect("accepted candle identity must retain its canonical payload");
            if accepted == &candle {
                self.funnel.identical_candle_duplicates =
                    self.funnel.identical_candle_duplicates.saturating_add(1);
                return Ok(CandleAcceptance::Duplicate);
            }
            self.funnel.conflicting_candles = self.funnel.conflicting_candles.saturating_add(1);
            return Err(TechnicalError::ConflictingCandle(candle_conflict(
                accepted, &candle,
            )));
        }
        let candles = asset.intervals.entry(candle.interval).or_default();
        if candles
            .back()
            .is_some_and(|last| candle.open_time_ms <= last.open_time_ms)
        {
            return Err(TechnicalError::NonMonotonicCandle);
        }
        candles.push_back(candle.clone());
        while candles.len() > 500 {
            if let Some(expired) = candles.pop_front() {
                asset.accepted.remove(&CandleIdentity {
                    interval: expired.interval,
                    open_time_ms: expired.open_time_ms,
                    close_time_ms: expired.close_time_ms,
                });
            }
        }
        asset.accepted.insert(identity);
        self.funnel.closed_candles_accepted = self.funnel.closed_candles_accepted.saturating_add(1);
        Ok(CandleAcceptance::Accepted)
    }

    pub fn evaluate(
        &mut self,
        asset: &str,
        available_gross: Decimal,
        costs: CostEstimate,
    ) -> Result<Option<TechnicalTarget>, TechnicalError> {
        self.funnel.evaluations = self.funnel.evaluations.saturating_add(1);
        let Some(candles) = self.assets.get_mut(asset) else {
            return Ok(None);
        };
        let Some((state, archetype, close_ms, atr_fraction)) = classify(candles, &self.config)?
        else {
            return Ok(None);
        };
        self.funnel.indicators_ready = self.funnel.indicators_ready.saturating_add(1);
        self.funnel.regime_classifications = self.funnel.regime_classifications.saturating_add(1);
        if state.trigger_30m != TriggerState::Neutral {
            self.funnel.primary_trigger_candidates =
                self.funnel.primary_trigger_candidates.saturating_add(1);
        }
        if state.trigger_30m != TriggerState::Neutral && state.trigger_30m == state.trigger_15m {
            self.funnel.secondary_trigger_confirmations = self
                .funnel
                .secondary_trigger_confirmations
                .saturating_add(1);
        }
        let raw_score = technical_score(&state, archetype, self.config.enter_threshold);
        let mut candidate = Decimal::from_f64_retain(raw_score.clamp(-1.0, 1.0))
            .ok_or(TechnicalError::Arithmetic)?;
        let current = candles.current_score;
        let exit_timing = primary_exit_30m(candles, current)?;
        let abs = candidate.abs().to_f64().ok_or(TechnicalError::Arithmetic)?;
        let current_sign = current.signum();
        let candidate_sign = candidate.signum();
        let is_new_side =
            current.is_zero() || (current_sign != candidate_sign && !candidate.is_zero());
        if is_new_side {
            if abs >= self.config.enter_threshold {
                self.funnel.scores_crossing_entry_threshold = self
                    .funnel
                    .scores_crossing_entry_threshold
                    .saturating_add(1);
            }
            let confirmed = confirmed_trigger_direction(&state)
                .is_some_and(|direction| direction == candidate_sign.to_i8().unwrap_or_default());
            if confirmed {
                self.funnel.trigger_confirmations =
                    self.funnel.trigger_confirmations.saturating_add(1);
            }
            if abs < self.config.enter_threshold || !confirmed {
                candidate = Decimal::ZERO;
            }
        } else if !current.is_zero() && current_sign == candidate_sign {
            if abs < self.config.flatten_threshold {
                candidate = Decimal::ZERO;
            } else if abs < self.config.reduce_threshold {
                candidate = current_sign
                    * Decimal::from_f64_retain(self.config.flatten_threshold)
                        .ok_or(TechnicalError::Arithmetic)?;
            } else if abs < self.config.maintain_threshold {
                candidate = current;
            }
        } else if !current.is_zero() && current_sign != candidate_sign {
            candidate = if abs >= self.config.enter_threshold {
                Decimal::ZERO
            } else {
                current
            };
        }
        candidate = match exit_timing {
            ExitTiming::Flatten => Decimal::ZERO,
            ExitTiming::Reduce if !current.is_zero() && candidate.signum() == current_sign => {
                current_sign
                    * Decimal::from_f64_retain(self.config.reduce_threshold)
                        .ok_or(TechnicalError::Arithmetic)?
            }
            ExitTiming::Hold | ExitTiming::Reduce => candidate,
        };
        if !candidate.is_zero() {
            self.funnel.nonzero_targets_before_cost =
                self.funnel.nonzero_targets_before_cost.saturating_add(1);
        }
        if !candidate.is_zero() {
            let expected = self.config.expected_atr_fraction * atr_fraction;
            let round_trip = costs.round_trip(self.config.expected_holding_hours)?;
            if expected < self.config.minimum_cost_multiple * round_trip {
                candidate = Decimal::ZERO;
            }
        }
        if !candidate.is_zero() {
            self.funnel.targets_surviving_cost_admission = self
                .funnel
                .targets_surviving_cost_admission
                .saturating_add(1);
        }
        candles.current_score = candidate;
        let desired_notional = available_gross
            .checked_mul(
                Decimal::from_f64_retain(self.config.technical_budget_fraction)
                    .ok_or(TechnicalError::Arithmetic)?,
            )
            .and_then(|budget| budget.checked_mul(candidate))
            .ok_or(TechnicalError::Arithmetic)?;
        Ok(Some(TechnicalTarget {
            asset: asset.to_string(),
            score: candidate,
            desired_notional,
            regime: state.regime_12h.regime,
            archetype,
            candle_close_ms: close_ms,
        }))
    }

    pub fn tracked_assets(&self) -> impl Iterator<Item = &String> {
        self.assets.keys()
    }

    /// Latest authoritative one-hour ATR in price units. This is exposed so
    /// source-entry chase distance can use the same retained market history as
    /// the technical engine instead of introducing a cohort-specific scale.
    pub fn latest_atr(&self, asset: &str) -> Result<Option<Decimal>, TechnicalError> {
        let Some(candles) = self
            .assets
            .get(asset)
            .and_then(|asset| asset.intervals.get(&CandleInterval::OneHour))
        else {
            return Ok(None);
        };
        let Some(value) = atr(candles, 14) else {
            return Ok(None);
        };
        Decimal::from_f64_retain(value)
            .map(Some)
            .ok_or(TechnicalError::Arithmetic)
    }

    /// Expected move under the already-approved technical ATR fraction.
    pub fn expected_move_fraction(&self, asset: &str) -> Result<Option<Decimal>, TechnicalError> {
        let Some(candles) = self
            .assets
            .get(asset)
            .and_then(|asset| asset.intervals.get(&CandleInterval::OneHour))
        else {
            return Ok(None);
        };
        let (Some(value), Some(last)) = (atr(candles, 14), candles.back()) else {
            return Ok(None);
        };
        let price = f64_close(last)?;
        Decimal::from_f64_retain(self.config.expected_atr_fraction * value / price)
            .map(Some)
            .ok_or(TechnicalError::Arithmetic)
    }

    pub fn funnel(&self) -> &TechnicalFunnel {
        &self.funnel
    }

    pub fn reset_funnel(&mut self) {
        self.funnel = TechnicalFunnel::default();
    }
}

fn candle_conflict(accepted: &ClosedCandle, conflicting: &ClosedCandle) -> CandleConflict {
    let mut field_differences = Vec::new();
    let mut record = |field: &str, left: String, right: String| {
        if left != right {
            field_differences.push(CandleFieldDifference {
                field: field.to_string(),
                accepted: left,
                conflicting: right,
            });
        }
    };
    record(
        "open",
        accepted.open.to_string(),
        conflicting.open.to_string(),
    );
    record(
        "high",
        accepted.high.to_string(),
        conflicting.high.to_string(),
    );
    record("low", accepted.low.to_string(), conflicting.low.to_string());
    record(
        "close",
        accepted.close.to_string(),
        conflicting.close.to_string(),
    );
    record(
        "base_volume",
        accepted.base_volume.to_string(),
        conflicting.base_volume.to_string(),
    );
    record(
        "trade_count",
        accepted.trade_count.to_string(),
        conflicting.trade_count.to_string(),
    );
    CandleConflict {
        asset: accepted.asset.clone(),
        interval: accepted.interval,
        open_time_ms: accepted.open_time_ms,
        close_time_ms: accepted.close_time_ms,
        accepted_payload_sha256: hex_sha256(accepted.payload_hash()),
        conflicting_payload_sha256: hex_sha256(conflicting.payload_hash()),
        field_differences,
    }
}

fn hex_sha256(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn technical_score(
    state: &TechnicalState,
    _archetype: SignalArchetype,
    enter_threshold: f64,
) -> f64 {
    let contextual = contextual_score(state);
    let Some(direction) = confirmed_trigger_direction(state) else {
        return contextual;
    };
    let direction = f64::from(direction);
    if contextual * direction >= enter_threshold {
        contextual
    } else {
        direction * enter_threshold
    }
}

fn contextual_score(state: &TechnicalState) -> f64 {
    0.40 * f64::from(state.regime_12h.direction)
        + 0.25 * f64::from(state.trend_4h.score())
        + 0.20 * f64::from(state.trend_1h.score())
        + 0.10 * f64::from(state.trigger_30m.score())
        + 0.05 * f64::from(state.trigger_15m.score())
}

fn confirmed_trigger_direction(state: &TechnicalState) -> Option<i8> {
    match (state.trigger_30m, state.trigger_15m) {
        (TriggerState::Bullish, TriggerState::Bullish) => Some(1),
        (TriggerState::Bearish, TriggerState::Bearish) => Some(-1),
        _ => None,
    }
}

fn primary_exit_30m(
    candles: &AssetCandles,
    current: Decimal,
) -> Result<ExitTiming, TechnicalError> {
    if current.is_zero() {
        return Ok(ExitTiming::Hold);
    }
    let Some(series) = candles.intervals.get(&CandleInterval::ThirtyMinutes) else {
        return Ok(ExitTiming::Hold);
    };
    let closes = closes(series)?;
    if closes.len() < 22 {
        return Ok(ExitTiming::Hold);
    }
    let previous = &closes[..closes.len() - 1];
    let close = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let previous_close = *previous.last().ok_or(TechnicalError::Arithmetic)?;
    let ma7 = sma(&closes, 7).ok_or(TechnicalError::Arithmetic)?;
    let ma21 = sma(&closes, 21).ok_or(TechnicalError::Arithmetic)?;
    let previous_ma21 = sma(previous, 21).ok_or(TechnicalError::Arithmetic)?;
    let rsi14 = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    Ok(if current.is_sign_positive() {
        if (close < ma21 && previous_close >= previous_ma21) || rsi14 <= 42.0 {
            ExitTiming::Flatten
        } else if close < ma7 || rsi14 < 50.0 {
            ExitTiming::Reduce
        } else {
            ExitTiming::Hold
        }
    } else if (close > ma21 && previous_close <= previous_ma21) || rsi14 >= 58.0 {
        ExitTiming::Flatten
    } else if close > ma7 || rsi14 > 50.0 {
        ExitTiming::Reduce
    } else {
        ExitTiming::Hold
    })
}

fn classify(
    candles: &AssetCandles,
    config: &TechnicalStrategyConfig,
) -> Result<Option<(TechnicalState, SignalArchetype, u64, f64)>, TechnicalError> {
    let Some(h12) = candles.intervals.get(&CandleInterval::TwelveHours) else {
        return Ok(None);
    };
    let Some(h4) = candles.intervals.get(&CandleInterval::FourHours) else {
        return Ok(None);
    };
    let Some(h1) = candles.intervals.get(&CandleInterval::OneHour) else {
        return Ok(None);
    };
    let Some(m30) = candles.intervals.get(&CandleInterval::ThirtyMinutes) else {
        return Ok(None);
    };
    let Some(m15) = candles.intervals.get(&CandleInterval::FifteenMinutes) else {
        return Ok(None);
    };
    if h12.len() < 201 || h4.len() < 101 || h1.len() < 51 || m30.len() < 51 || m15.len() < 22 {
        return Ok(None);
    }
    let regime = regime_12h(h12)?;
    let trend4 = trend_4h(h4)?;
    let trend1 = trend_1h(h1)?;
    let (trigger30, archetype) = trigger_30m(m30, &regime, trend4, trend1, config)?;
    let trigger15 = trigger_15m(m15, trigger30)?;
    let atr = atr(h1, 14).ok_or(TechnicalError::Arithmetic)?;
    let price = f64_close(h1.back().ok_or(TechnicalError::Arithmetic)?)?;
    Ok(Some((
        TechnicalState {
            regime_12h: regime,
            trend_4h: trend4,
            trend_1h: trend1,
            trigger_30m: trigger30,
            trigger_15m: trigger15,
        },
        archetype,
        m15.back().ok_or(TechnicalError::Arithmetic)?.close_time_ms,
        atr / price,
    )))
}

fn regime_12h(candles: &VecDeque<ClosedCandle>) -> Result<RegimeState, TechnicalError> {
    let closes = closes(candles)?;
    let last = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let ma7 = sma(&closes, 7).ok_or(TechnicalError::Arithmetic)?;
    let ma14 = sma(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    let ma21 = sma(&closes, 21).ok_or(TechnicalError::Arithmetic)?;
    let ma50 = sma(&closes, 50).ok_or(TechnicalError::Arithmetic)?;
    let ma100 = sma(&closes, 100).ok_or(TechnicalError::Arithmetic)?;
    let ma200 = sma(&closes, 200).ok_or(TechnicalError::Arithmetic)?;
    let atr14 = atr(candles, 14).ok_or(TechnicalError::Arithmetic)?;
    let previous = &closes[..closes.len() - 1];
    let slope50 = (ma50 - sma(previous, 50).ok_or(TechnicalError::Arithmetic)?) / atr14;
    let slope100 = (ma100 - sma(previous, 100).ok_or(TechnicalError::Arithmetic)?) / atr14;
    let rsi14 = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    let quote_volumes = quote_volumes(candles)?;
    let volume_above = *quote_volumes.last().ok_or(TechnicalError::Arithmetic)?
        > sma(&quote_volumes[..quote_volumes.len() - 1], 20).ok_or(TechnicalError::Arithmetic)?;
    let previous_ma21 = sma(previous, 21).ok_or(TechnicalError::Arithmetic)?;
    let previous_ma50 = sma(previous, 50).ok_or(TechnicalError::Arithmetic)?;
    let regime = if last > ma21
        && ma7 > ma14
        && ma14 > ma21
        && ma21 > ma50
        && ma50 > ma100
        && ma100 > ma200
        && slope50 > 0.0
        && slope100 > 0.0
        && rsi14 >= 55.0
        && volume_above
    {
        MarketRegime::StrongBull
    } else if last < ma21
        && ma7 < ma14
        && ma14 < ma21
        && ma21 < ma50
        && ma50 < ma100
        && ma100 < ma200
        && slope50 < 0.0
        && slope100 < 0.0
        && rsi14 <= 45.0
        && volume_above
    {
        MarketRegime::StrongBear
    } else if ma7 > ma14
        && ma14 > ma21
        && previous_ma21 <= previous_ma50
        && ma21 > ma50
        && slope50 >= 0.0
        && rsi14 > 50.0
        && volume_above
    {
        MarketRegime::TransitionUp
    } else if ma7 < ma14
        && ma14 < ma21
        && previous_ma21 >= previous_ma50
        && ma21 < ma50
        && slope50 <= 0.0
        && rsi14 < 50.0
        && volume_above
    {
        MarketRegime::TransitionDown
    } else {
        MarketRegime::Range
    };
    let direction = match regime {
        MarketRegime::StrongBull | MarketRegime::TransitionUp => 1,
        MarketRegime::StrongBear | MarketRegime::TransitionDown => -1,
        MarketRegime::Range => 0,
    };
    Ok(RegimeState {
        regime,
        direction,
        ma50_normalized_slope: Decimal::from_f64_retain(slope50)
            .ok_or(TechnicalError::Arithmetic)?,
        ma100_normalized_slope: Decimal::from_f64_retain(slope100)
            .ok_or(TechnicalError::Arithmetic)?,
    })
}

fn trend_4h(candles: &VecDeque<ClosedCandle>) -> Result<TrendState, TechnicalError> {
    let closes = closes(candles)?;
    let previous = &closes[..closes.len() - 1];
    let close = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let ma21 = sma(&closes, 21).ok_or(TechnicalError::Arithmetic)?;
    let ma50 = sma(&closes, 50).ok_or(TechnicalError::Arithmetic)?;
    let ma100 = sma(&closes, 100).ok_or(TechnicalError::Arithmetic)?;
    let slope21 = ma21 - sma(previous, 21).ok_or(TechnicalError::Arithmetic)?;
    let slope50 = ma50 - sma(previous, 50).ok_or(TechnicalError::Arithmetic)?;
    let rsi = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    let volumes = quote_volumes(candles)?;
    let volume_confirms = *volumes.last().ok_or(TechnicalError::Arithmetic)?
        >= sma(&volumes[..volumes.len() - 1], 20).ok_or(TechnicalError::Arithmetic)?;
    Ok(
        if close > ma21
            && ma21 > ma50
            && ma50 > ma100
            && slope21 > 0.0
            && slope50 >= 0.0
            && rsi > 50.0
            && volume_confirms
        {
            TrendState::Bullish
        } else if close < ma21
            && ma21 < ma50
            && ma50 < ma100
            && slope21 < 0.0
            && slope50 <= 0.0
            && rsi < 50.0
            && volume_confirms
        {
            TrendState::Bearish
        } else {
            TrendState::Neutral
        },
    )
}

fn trend_1h(candles: &VecDeque<ClosedCandle>) -> Result<TrendState, TechnicalError> {
    let closes = closes(candles)?;
    let close = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let ma7 = sma(&closes, 7).ok_or(TechnicalError::Arithmetic)?;
    let ma14 = sma(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    let ma21 = sma(&closes, 21).ok_or(TechnicalError::Arithmetic)?;
    let ma50 = sma(&closes, 50).ok_or(TechnicalError::Arithmetic)?;
    let rsi = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    Ok(
        if ma7 > ma14 && ma14 > ma21 && close > ma50 && rsi >= 50.0 {
            TrendState::Bullish
        } else if ma7 < ma14 && ma14 < ma21 && close < ma50 && rsi <= 50.0 {
            TrendState::Bearish
        } else {
            TrendState::Neutral
        },
    )
}

fn trigger_30m(
    candles: &VecDeque<ClosedCandle>,
    regime: &RegimeState,
    trend4: TrendState,
    trend1: TrendState,
    config: &TechnicalStrategyConfig,
) -> Result<(TriggerState, SignalArchetype), TechnicalError> {
    let closes = closes(candles)?;
    let rsi_now = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    let previous_rsi = (1..=3)
        .filter_map(|offset| rsi(&closes[..closes.len() - offset], 14))
        .collect::<Vec<_>>();
    let close = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let ma7 = sma(&closes, 7).ok_or(TechnicalError::Arithmetic)?;
    let ma21 = sma(&closes, 21).ok_or(TechnicalError::Arithmetic)?;
    let ma50 = sma(&closes, 50).ok_or(TechnicalError::Arithmetic)?;
    let volume_z = volume_z(candles)?;
    let previous = &closes[..closes.len() - 1];
    let previous_close = *previous.last().ok_or(TechnicalError::Arithmetic)?;
    let previous_ma7 = sma(previous, 7).ok_or(TechnicalError::Arithmetic)?;
    let previous_ma21 = sma(previous, 21).ok_or(TechnicalError::Arithmetic)?;
    let previous_ma50 = sma(previous, 50).ok_or(TechnicalError::Arithmetic)?;
    // Trend activation is a majority vote among the independently sampled
    // slow horizons. Requiring all three to agree made a valid timed pullback
    // depend on universal confluence and left entry-grade contextual scores
    // permanently inert.
    let regime_bullish = regime.direction > 0;
    let regime_bearish = regime.direction < 0;
    let trend4_bullish = trend4 == TrendState::Bullish;
    let trend4_bearish = trend4 == TrendState::Bearish;
    let trend1_bullish = trend1 == TrendState::Bullish;
    let trend1_bearish = trend1 == TrendState::Bearish;
    let bull_context = (regime_bullish && (trend4_bullish || trend1_bullish))
        || (trend4_bullish && trend1_bullish);
    let bear_context = (regime_bearish && (trend4_bearish || trend1_bearish))
        || (trend4_bearish && trend1_bearish);
    let bullish_momentum_washout = previous_rsi.iter().any(|value| *value < 45.0);
    let bearish_momentum_washout = previous_rsi.iter().any(|value| *value > 55.0);
    let bullish_price_pullback = previous_close <= previous_ma7;
    let bearish_price_pullback = previous_close >= previous_ma7;
    let pullback_participation = volume_z >= config.pullback_volume_z;
    // A full medium-term structure break is more specific than a fast-MA
    // reclaim, so classify it before the pullback archetypes.
    let breakout_up_context = regime.regime == MarketRegime::TransitionUp
        || (regime.regime == MarketRegime::Range && trend4_bullish);
    let breakout_down_context = regime.regime == MarketRegime::TransitionDown
        || (regime.regime == MarketRegime::Range && trend4_bearish);
    if breakout_up_context
        && previous_close <= previous_ma21.max(previous_ma50)
        && close > ma21
        && close > ma50
        && volume_z >= config.breakout_volume_z
    {
        return Ok((TriggerState::Bullish, SignalArchetype::RegimeBreakout));
    }
    if breakout_down_context
        && previous_close >= previous_ma21.min(previous_ma50)
        && close < ma21
        && close < ma50
        && volume_z >= config.breakout_volume_z
    {
        return Ok((TriggerState::Bearish, SignalArchetype::RegimeBreakout));
    }
    if bull_context
        && (bullish_momentum_washout || (bullish_price_pullback && pullback_participation))
        && rsi_now > 50.0
        && close > ma7
    {
        return Ok((TriggerState::Bullish, SignalArchetype::TrendPullback));
    }
    if bear_context
        && (bearish_momentum_washout || (bearish_price_pullback && pullback_participation))
        && rsi_now < 50.0
        && close < ma7
    {
        return Ok((TriggerState::Bearish, SignalArchetype::TrendPullback));
    }
    // A 12-hour range is its own archetype. Requiring both subordinate trends
    // to be neutral prevented the reversal itself from changing either vote.
    let range_context = regime.regime == MarketRegime::Range;
    if range_context
        && (bullish_momentum_washout || pullback_participation)
        && rsi_now > 50.0
        && previous_close <= previous_ma7
        && close > ma7
    {
        return Ok((TriggerState::Bullish, SignalArchetype::RangeMeanReversion));
    }
    if range_context
        && (bearish_momentum_washout || pullback_participation)
        && rsi_now < 50.0
        && previous_close >= previous_ma7
        && close < ma7
    {
        return Ok((TriggerState::Bearish, SignalArchetype::RangeMeanReversion));
    }
    Ok((TriggerState::Neutral, SignalArchetype::TrendPullback))
}

fn trigger_15m(
    candles: &VecDeque<ClosedCandle>,
    primary: TriggerState,
) -> Result<TriggerState, TechnicalError> {
    let closes = closes(candles)?;
    let close = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let ma7 = sma(&closes, 7).ok_or(TechnicalError::Arithmetic)?;
    let rsi = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    // The primary setup has already enforced its archetype-specific structure
    // and participation. The faster interval confirms only direction; making
    // it repeat the volume gate and a full MA stack recreated an excessive-
    // confluence veto rather than an independent timing check.
    Ok(match primary {
        TriggerState::Bullish if close > ma7 && rsi >= 50.0 => TriggerState::Bullish,
        TriggerState::Bearish if close < ma7 && rsi <= 50.0 => TriggerState::Bearish,
        _ => TriggerState::Neutral,
    })
}

fn closes(candles: &VecDeque<ClosedCandle>) -> Result<Vec<f64>, TechnicalError> {
    candles.iter().map(f64_close).collect()
}

fn f64_close(candle: &ClosedCandle) -> Result<f64, TechnicalError> {
    candle.close.to_f64().ok_or(TechnicalError::Arithmetic)
}

fn sma(values: &[f64], period: usize) -> Option<f64> {
    (values.len() >= period)
        .then(|| values[values.len() - period..].iter().sum::<f64>() / period as f64)
}

fn rsi(values: &[f64], period: usize) -> Option<f64> {
    if values.len() <= period {
        return None;
    }
    let window = &values[values.len() - period - 1..];
    let (gain, loss) = window.windows(2).fold((0.0, 0.0), |(gain, loss), pair| {
        let delta = pair[1] - pair[0];
        if delta >= 0.0 {
            (gain + delta, loss)
        } else {
            (gain, loss - delta)
        }
    });
    if loss == 0.0 {
        Some(100.0)
    } else {
        Some(100.0 - 100.0 / (1.0 + gain / loss))
    }
}

fn atr(candles: &VecDeque<ClosedCandle>, period: usize) -> Option<f64> {
    if candles.len() <= period {
        return None;
    }
    let values = candles.iter().collect::<Vec<_>>();
    let window = &values[values.len() - period - 1..];
    let sum = window.windows(2).try_fold(0.0, |sum, pair| {
        let previous_close = pair[0].close.to_f64()?;
        let high = pair[1].high.to_f64()?;
        let low = pair[1].low.to_f64()?;
        Some(
            sum + (high - low)
                .max((high - previous_close).abs())
                .max((low - previous_close).abs()),
        )
    })?;
    Some(sum / period as f64)
}

fn quote_volumes(candles: &VecDeque<ClosedCandle>) -> Result<Vec<f64>, TechnicalError> {
    candles
        .iter()
        .map(|candle| {
            let volume = candle
                .base_volume
                .checked_mul(candle.close)
                .ok_or(TechnicalError::Arithmetic)?
                .to_f64()
                .ok_or(TechnicalError::Arithmetic)?;
            Ok(volume.max(f64::MIN_POSITIVE))
        })
        .collect()
}

fn volume_z(candles: &VecDeque<ClosedCandle>) -> Result<f64, TechnicalError> {
    let values = quote_volumes(candles)?;
    if values.len() < 21 {
        return Err(TechnicalError::Arithmetic);
    }
    let logs = values[values.len() - 21..]
        .iter()
        .map(|value| value.ln())
        .collect::<Vec<_>>();
    let baseline = &logs[..20];
    let mean = baseline.iter().sum::<f64>() / baseline.len() as f64;
    let variance = baseline
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / baseline.len() as f64;
    let deviation = variance.sqrt();
    Ok(if deviation <= f64::EPSILON {
        0.0
    } else {
        (logs[20] - mean) / deviation
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(interval: CandleInterval, index: u64, close: i64) -> ClosedCandle {
        ClosedCandle {
            asset: "BTC".into(),
            interval,
            open_time_ms: index * 1_000,
            close_time_ms: (index + 1) * 1_000,
            open: Decimal::from(close),
            high: Decimal::from(close + 2),
            low: Decimal::from(close - 2),
            close: Decimal::from(close),
            base_volume: Decimal::from(100 + index),
            trade_count: 10,
        }
    }

    fn candle_with_volume(
        interval: CandleInterval,
        index: u64,
        close: i64,
        base_volume: i64,
    ) -> ClosedCandle {
        ClosedCandle {
            base_volume: Decimal::from(base_volume),
            ..candle(interval, index, close)
        }
    }

    fn reversal_closes(bullish: bool) -> Vec<i64> {
        let mut closes = (0..45)
            .map(|index| if index % 2 == 0 { 99 } else { 101 })
            .collect::<Vec<_>>();
        closes.extend([100, 99, 96, 92, 90, 106]);
        if !bullish {
            for close in &mut closes {
                *close = 200 - *close;
            }
        }
        closes
    }

    fn confirmation_closes(bullish: bool) -> Vec<i64> {
        let mut closes = (0..15)
            .map(|index| if index % 2 == 0 { 99 } else { 101 })
            .collect::<Vec<_>>();
        closes.extend([96, 97, 98, 100, 102, 105, 110]);
        if !bullish {
            for close in &mut closes {
                *close = 200 - *close;
            }
        }
        closes
    }

    fn series(interval: CandleInterval, closes: &[i64]) -> VecDeque<ClosedCandle> {
        let last = closes.len().saturating_sub(1);
        closes
            .iter()
            .enumerate()
            .map(|(index, close)| {
                candle_with_volume(
                    interval,
                    u64::try_from(index).unwrap(),
                    *close,
                    if index == last {
                        10_000
                    } else {
                        100 + i64::try_from(index % 3).unwrap()
                    },
                )
            })
            .collect()
    }

    fn zero_costs() -> CostEstimate {
        CostEstimate {
            entry_fee_fraction: 0.0,
            exit_fee_fraction: 0.0,
            entry_slippage_fraction: 0.0,
            exit_slippage_fraction: 0.0,
            hourly_funding_fraction: 0.0,
        }
    }

    fn accept_series(engine: &mut TechnicalEngine, candles: VecDeque<ClosedCandle>) {
        for candle in candles {
            assert_eq!(
                engine.accept_closed_candle(candle).unwrap(),
                CandleAcceptance::Accepted
            );
        }
    }

    fn range_engine(bullish: bool) -> TechnicalEngine {
        let mut engine = TechnicalEngine::new(TechnicalStrategyConfig {
            enabled: true,
            ..TechnicalStrategyConfig::default()
        })
        .unwrap();
        for (interval, count) in [
            (CandleInterval::TwelveHours, 201),
            (CandleInterval::FourHours, 101),
            (CandleInterval::OneHour, 51),
        ] {
            let closes = vec![100; count];
            accept_series(&mut engine, series(interval, &closes));
        }
        accept_series(
            &mut engine,
            series(CandleInterval::ThirtyMinutes, &reversal_closes(bullish)),
        );
        accept_series(
            &mut engine,
            series(
                CandleInterval::FifteenMinutes,
                &confirmation_closes(bullish),
            ),
        );
        engine
    }

    #[test]
    fn feature_context_returns_none_until_all_context_is_ready() {
        let mut engine = TechnicalEngine::new(TechnicalStrategyConfig::default()).unwrap();

        assert_eq!(engine.feature_context("BTC").unwrap(), None);

        assert_eq!(
            engine
                .accept_closed_candle(candle(CandleInterval::FifteenMinutes, 1, 100))
                .unwrap(),
            CandleAcceptance::Accepted
        );
        assert_eq!(engine.feature_context("BTC").unwrap(), None);
    }

    #[test]
    fn feature_context_is_serializable_deterministic_and_read_only() {
        let engine = range_engine(true);
        let engine_before = serde_json::to_vec(&engine).unwrap();
        let funnel_before = engine.funnel().clone();
        let score_before = engine.assets.get("BTC").unwrap().current_score;

        let first = engine
            .feature_context("BTC")
            .unwrap()
            .expect("warmed indicators must expose feature context");
        let second = engine
            .feature_context("BTC")
            .unwrap()
            .expect("repeated read must expose the same feature context");

        assert_eq!(first, second);
        assert_eq!(first.archetype, SignalArchetype::RangeMeanReversion);
        assert_eq!(
            first.raw_contextual_score,
            Decimal::from_f64_retain(0.10_f64 + 0.05).unwrap()
        );
        assert!(
            first.raw_contextual_score
                < Decimal::from_f64_retain(engine.configuration().enter_threshold).unwrap()
        );
        assert_eq!(first.atr_fraction, Decimal::from_f64_retain(0.04).unwrap());
        assert_eq!(first.candle_close_ms, 22_000);
        let serialized = serde_json::to_vec(&first).unwrap();
        assert_eq!(
            serde_json::from_slice::<TechnicalContext>(&serialized).unwrap(),
            first
        );

        assert_eq!(serde_json::to_vec(&engine).unwrap(), engine_before);
        assert_eq!(engine.funnel(), &funnel_before);
        assert_eq!(
            engine.assets.get("BTC").unwrap().current_score,
            score_before
        );
    }

    #[test]
    fn duplicate_closed_candle_is_idempotent_and_conflicts_fail() {
        let mut engine = TechnicalEngine::new(TechnicalStrategyConfig::default()).unwrap();
        let accepted = candle(CandleInterval::FifteenMinutes, 1, 100);
        assert_eq!(
            engine.accept_closed_candle(accepted.clone()).unwrap(),
            CandleAcceptance::Accepted
        );
        assert_eq!(
            engine.accept_closed_candle(accepted.clone()).unwrap(),
            CandleAcceptance::Duplicate
        );
        let mut conflict = accepted;
        conflict.close = Decimal::from(101);
        let error = engine.accept_closed_candle(conflict).unwrap_err();
        let TechnicalError::ConflictingCandle(conflict) = error else {
            panic!("expected conflicting candle");
        };
        assert_ne!(
            conflict.accepted_payload_sha256,
            conflict.conflicting_payload_sha256
        );
        assert_eq!(conflict.field_differences.len(), 1);
        assert_eq!(conflict.field_differences[0].field, "close");
        assert_eq!(engine.funnel().conflicting_candles, 1);
    }

    #[test]
    fn fixed_budget_and_hysteresis_configuration_is_valid() {
        let config = TechnicalStrategyConfig {
            enabled: true,
            ..TechnicalStrategyConfig::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(
            CostEstimate {
                entry_fee_fraction: 0.00045,
                exit_fee_fraction: 0.00045,
                entry_slippage_fraction: 0.0004,
                exit_slippage_fraction: 0.0004,
                hourly_funding_fraction: 0.0001,
            }
            .round_trip(6)
            .unwrap(),
            0.0023
        );
    }

    #[test]
    fn range_mean_reversion_activates_without_trend_votes() {
        for bullish in [true, false] {
            let mut engine = range_engine(bullish);
            let target = engine
                .evaluate("BTC", Decimal::from(1_000), zero_costs())
                .unwrap()
                .expect("warmed range indicators must produce a target");
            assert_eq!(target.regime, MarketRegime::Range);
            assert_eq!(target.archetype, SignalArchetype::RangeMeanReversion);
            assert_eq!(target.score.is_sign_positive(), bullish);
            assert_eq!(target.score.abs().to_f64().unwrap(), 0.65);
            assert_eq!(target.desired_notional.is_sign_positive(), bullish);
            assert!((target.desired_notional.abs().to_f64().unwrap() - 422.5).abs() < 1e-9);
            assert_eq!(engine.funnel().trigger_confirmations, 1);
            assert_eq!(engine.funnel().targets_surviving_cost_admission, 1);
        }
    }

    #[test]
    fn each_confirmed_archetype_reaches_the_existing_entry_threshold_independently() {
        let config = TechnicalStrategyConfig::default();
        for bullish in [true, false] {
            let direction = if bullish { 1 } else { -1 };
            let trigger = if bullish {
                TriggerState::Bullish
            } else {
                TriggerState::Bearish
            };
            let trend = if bullish {
                TrendState::Bullish
            } else {
                TrendState::Bearish
            };
            let cases = [
                (
                    SignalArchetype::TrendPullback,
                    trend,
                    trend,
                    "trend pullback",
                ),
                (
                    SignalArchetype::RangeMeanReversion,
                    TrendState::Neutral,
                    TrendState::Neutral,
                    "range mean reversion",
                ),
                (
                    SignalArchetype::RegimeBreakout,
                    trend,
                    TrendState::Neutral,
                    "regime breakout",
                ),
            ];
            for (archetype, trend4, trend1, label) in cases {
                let state = TechnicalState {
                    regime_12h: RegimeState {
                        regime: MarketRegime::Range,
                        direction: 0,
                        ma50_normalized_slope: Decimal::ZERO,
                        ma100_normalized_slope: Decimal::ZERO,
                    },
                    trend_4h: trend4,
                    trend_1h: trend1,
                    trigger_30m: trigger,
                    trigger_15m: trigger,
                };
                assert_eq!(confirmed_trigger_direction(&state), Some(direction));
                let score = technical_score(&state, archetype, config.enter_threshold);
                assert!(
                    (score - f64::from(direction) * config.enter_threshold).abs() < 1e-12,
                    "{label} did not independently reach entry"
                );
            }
        }
    }

    #[test]
    fn primary_archetypes_do_not_require_universal_or_duplicate_confluence() {
        let mut no_volume_fallback = TechnicalStrategyConfig {
            pullback_volume_z: 1_000_000.0,
            breakout_volume_z: 1_000_000.0,
            ..TechnicalStrategyConfig::default()
        };
        for bullish in [true, false] {
            let regime = RegimeState {
                regime: if bullish {
                    MarketRegime::StrongBull
                } else {
                    MarketRegime::StrongBear
                },
                direction: if bullish { 1 } else { -1 },
                ma50_normalized_slope: Decimal::from(if bullish { 1 } else { -1 }),
                ma100_normalized_slope: Decimal::from(if bullish { 1 } else { -1 }),
            };
            let trend4 = if bullish {
                TrendState::Bullish
            } else {
                TrendState::Bearish
            };
            let expected = if bullish {
                TriggerState::Bullish
            } else {
                TriggerState::Bearish
            };
            assert_eq!(
                trigger_30m(
                    &series(CandleInterval::ThirtyMinutes, &reversal_closes(bullish)),
                    &regime,
                    trend4,
                    TrendState::Neutral,
                    &no_volume_fallback,
                )
                .unwrap(),
                (expected, SignalArchetype::TrendPullback)
            );

            let range = RegimeState {
                regime: MarketRegime::Range,
                direction: 0,
                ma50_normalized_slope: Decimal::ZERO,
                ma100_normalized_slope: Decimal::ZERO,
            };
            assert_eq!(
                trigger_30m(
                    &series(CandleInterval::ThirtyMinutes, &reversal_closes(bullish)),
                    &range,
                    trend4,
                    TrendState::Neutral,
                    &no_volume_fallback,
                )
                .unwrap(),
                (expected, SignalArchetype::RangeMeanReversion)
            );
            assert_eq!(
                trigger_15m(
                    &series(
                        CandleInterval::FifteenMinutes,
                        &confirmation_closes(bullish),
                    ),
                    expected,
                )
                .unwrap(),
                expected
            );
        }

        no_volume_fallback.breakout_volume_z = TechnicalStrategyConfig::default().breakout_volume_z;
        for bullish in [true, false] {
            let mut breakout_closes = vec![100; 50];
            breakout_closes.push(if bullish { 120 } else { 80 });
            let trend4 = if bullish {
                TrendState::Bullish
            } else {
                TrendState::Bearish
            };
            let expected = if bullish {
                TriggerState::Bullish
            } else {
                TriggerState::Bearish
            };
            assert_eq!(
                trigger_30m(
                    &series(CandleInterval::ThirtyMinutes, &breakout_closes),
                    &RegimeState {
                        regime: MarketRegime::Range,
                        direction: 0,
                        ma50_normalized_slope: Decimal::ZERO,
                        ma100_normalized_slope: Decimal::ZERO,
                    },
                    trend4,
                    TrendState::Neutral,
                    &no_volume_fallback,
                )
                .unwrap(),
                (expected, SignalArchetype::RegimeBreakout)
            );
        }
    }

    #[test]
    fn entry_grade_slow_context_still_requires_a_timed_archetype() {
        let state = TechnicalState {
            regime_12h: RegimeState {
                regime: MarketRegime::StrongBull,
                direction: 1,
                ma50_normalized_slope: Decimal::ONE,
                ma100_normalized_slope: Decimal::ONE,
            },
            trend_4h: TrendState::Bullish,
            trend_1h: TrendState::Bullish,
            trigger_30m: TriggerState::Neutral,
            trigger_15m: TriggerState::Neutral,
        };
        let config = TechnicalStrategyConfig::default();
        assert!(
            technical_score(
                &state,
                SignalArchetype::TrendPullback,
                config.enter_threshold
            ) >= config.enter_threshold
        );
        assert_eq!(confirmed_trigger_direction(&state), None);
    }

    #[test]
    fn trend_pullback_and_regime_breakout_triggers_remain_distinct() {
        let config = TechnicalStrategyConfig::default();
        let bullish_regime = RegimeState {
            regime: MarketRegime::StrongBull,
            direction: 1,
            ma50_normalized_slope: Decimal::ONE,
            ma100_normalized_slope: Decimal::ONE,
        };
        assert_eq!(
            trigger_30m(
                &series(CandleInterval::ThirtyMinutes, &reversal_closes(true)),
                &bullish_regime,
                TrendState::Bullish,
                TrendState::Bullish,
                &config,
            )
            .unwrap(),
            (TriggerState::Bullish, SignalArchetype::TrendPullback)
        );

        let mut breakout_closes = vec![100; 50];
        breakout_closes.push(120);
        let transition = RegimeState {
            regime: MarketRegime::TransitionUp,
            direction: 1,
            ma50_normalized_slope: Decimal::ONE,
            ma100_normalized_slope: Decimal::ZERO,
        };
        assert_eq!(
            trigger_30m(
                &series(CandleInterval::ThirtyMinutes, &breakout_closes),
                &transition,
                TrendState::Bullish,
                TrendState::Neutral,
                &config,
            )
            .unwrap(),
            (TriggerState::Bullish, SignalArchetype::RegimeBreakout)
        );
    }

    #[test]
    fn thirty_minute_structure_break_flattens_before_slow_regime_changes() {
        let mut asset = AssetCandles {
            current_score: Decimal::ONE,
            ..AssetCandles::default()
        };
        let series = asset
            .intervals
            .entry(CandleInterval::ThirtyMinutes)
            .or_default();
        for index in 0..21 {
            series.push_back(candle(
                CandleInterval::ThirtyMinutes,
                index,
                100 + i64::try_from(index).unwrap(),
            ));
        }
        series.push_back(candle(CandleInterval::ThirtyMinutes, 21, 80));
        assert_eq!(
            primary_exit_30m(&asset, Decimal::ONE).unwrap(),
            ExitTiming::Flatten
        );
    }
}
