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
        let bytes = serde_json::to_vec(self).expect("closed candle is serializable");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalArchetype {
    TrendPullback,
    RegimeBreakout,
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
    pub source_budget_fraction: f64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TechnicalError {
    InvalidCandle,
    ConflictingCandle,
    InvalidConfiguration,
    InvalidCost,
    Arithmetic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CandleIdentity {
    interval: CandleInterval,
    open_time_ms: u64,
    close_time_ms: u64,
    payload_hash: [u8; 32],
}

#[derive(Debug, Default)]
struct AssetCandles {
    intervals: BTreeMap<CandleInterval, VecDeque<ClosedCandle>>,
    accepted: BTreeSet<CandleIdentity>,
    current_score: Decimal,
}

#[derive(Debug)]
pub struct TechnicalEngine {
    config: TechnicalStrategyConfig,
    assets: BTreeMap<String, AssetCandles>,
}

impl TechnicalEngine {
    pub fn new(config: TechnicalStrategyConfig) -> Result<Self, TechnicalError> {
        config.validate()?;
        Ok(Self {
            config,
            assets: BTreeMap::new(),
        })
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
            payload_hash: candle.payload_hash(),
        };
        let asset = self.assets.entry(candle.asset.clone()).or_default();
        if asset.accepted.contains(&identity) {
            return Ok(CandleAcceptance::Duplicate);
        }
        if asset.accepted.iter().any(|existing| {
            existing.interval == identity.interval
                && existing.open_time_ms == identity.open_time_ms
                && existing.close_time_ms == identity.close_time_ms
        }) {
            return Err(TechnicalError::ConflictingCandle);
        }
        let candles = asset.intervals.entry(candle.interval).or_default();
        if candles
            .back()
            .is_some_and(|last| candle.open_time_ms <= last.open_time_ms)
        {
            return Err(TechnicalError::ConflictingCandle);
        }
        candles.push_back(candle);
        while candles.len() > 500 {
            candles.pop_front();
        }
        asset.accepted.insert(identity);
        Ok(CandleAcceptance::Accepted)
    }

    pub fn evaluate(
        &mut self,
        asset: &str,
        available_gross: Decimal,
        costs: CostEstimate,
    ) -> Result<Option<TechnicalTarget>, TechnicalError> {
        let Some(candles) = self.assets.get_mut(asset) else {
            return Ok(None);
        };
        let Some((state, archetype, close_ms, atr_fraction)) = classify(candles, &self.config)?
        else {
            return Ok(None);
        };
        let raw_score = 0.40 * f64::from(state.regime_12h.direction)
            + 0.25 * f64::from(state.trend_4h.score())
            + 0.20 * f64::from(state.trend_1h.score())
            + 0.10 * f64::from(state.trigger_30m.score())
            + 0.05 * f64::from(state.trigger_15m.score());
        let mut candidate = Decimal::from_f64_retain(raw_score.clamp(-1.0, 1.0))
            .ok_or(TechnicalError::Arithmetic)?;
        let current = candles.current_score;
        let abs = candidate.abs().to_f64().ok_or(TechnicalError::Arithmetic)?;
        let current_sign = current.signum();
        let candidate_sign = candidate.signum();
        let is_new_side =
            current.is_zero() || (current_sign != candidate_sign && !candidate.is_zero());
        if is_new_side {
            let confirmed = match candidate_sign.to_i8().unwrap_or_default() {
                1 => {
                    state.trigger_30m == TriggerState::Bullish
                        && state.trigger_15m == TriggerState::Bullish
                }
                -1 => {
                    state.trigger_30m == TriggerState::Bearish
                        && state.trigger_15m == TriggerState::Bearish
                }
                _ => false,
            };
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
        if !candidate.is_zero() {
            let expected = self.config.expected_atr_fraction * atr_fraction;
            let round_trip = costs.round_trip(self.config.expected_holding_hours)?;
            if expected < self.config.minimum_cost_multiple * round_trip {
                candidate = Decimal::ZERO;
            }
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
    let trigger15 = trigger_15m(m15, trigger30, config)?;
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
    {
        MarketRegime::StrongBear
    } else if ma7 > ma14
        && ma14 > ma21
        && previous_ma21 <= previous_ma50
        && ma21 > ma50
        && slope50 >= 0.0
        && volume_above
    {
        MarketRegime::TransitionUp
    } else if ma7 < ma14
        && ma14 < ma21
        && previous_ma21 >= previous_ma50
        && ma21 < ma50
        && slope50 <= 0.0
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
    let slope21 = ma21 - sma(previous, 21).ok_or(TechnicalError::Arithmetic)?;
    let slope50 = ma50 - sma(previous, 50).ok_or(TechnicalError::Arithmetic)?;
    let rsi = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    Ok(
        if close > ma21 && ma21 > ma50 && slope21 > 0.0 && slope50 >= 0.0 && rsi > 50.0 {
            TrendState::Bullish
        } else if close < ma21 && ma21 < ma50 && slope21 < 0.0 && slope50 <= 0.0 && rsi < 50.0 {
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
    let bull_context = matches!(
        regime.regime,
        MarketRegime::StrongBull | MarketRegime::TransitionUp
    ) && trend4 == TrendState::Bullish
        && trend1 == TrendState::Bullish;
    let bear_context = matches!(
        regime.regime,
        MarketRegime::StrongBear | MarketRegime::TransitionDown
    ) && trend4 == TrendState::Bearish
        && trend1 == TrendState::Bearish;
    if bull_context
        && previous_rsi.iter().any(|value| *value < 45.0)
        && rsi_now > 50.0
        && close > ma7
        && volume_z >= config.pullback_volume_z
    {
        return Ok((TriggerState::Bullish, SignalArchetype::TrendPullback));
    }
    if bear_context
        && previous_rsi.iter().any(|value| *value > 55.0)
        && rsi_now < 50.0
        && close < ma7
        && volume_z >= config.pullback_volume_z
    {
        return Ok((TriggerState::Bearish, SignalArchetype::TrendPullback));
    }
    let previous = &closes[..closes.len() - 1];
    let previous_close = *previous.last().ok_or(TechnicalError::Arithmetic)?;
    let previous_ma21 = sma(previous, 21).ok_or(TechnicalError::Arithmetic)?;
    let previous_ma50 = sma(previous, 50).ok_or(TechnicalError::Arithmetic)?;
    if regime.regime == MarketRegime::TransitionUp
        && trend4 == TrendState::Bullish
        && previous_close <= previous_ma21.max(previous_ma50)
        && close > ma21
        && close > ma50
        && volume_z >= config.breakout_volume_z
    {
        return Ok((TriggerState::Bullish, SignalArchetype::RegimeBreakout));
    }
    if regime.regime == MarketRegime::TransitionDown
        && trend4 == TrendState::Bearish
        && previous_close >= previous_ma21.min(previous_ma50)
        && close < ma21
        && close < ma50
        && volume_z >= config.breakout_volume_z
    {
        return Ok((TriggerState::Bearish, SignalArchetype::RegimeBreakout));
    }
    Ok((TriggerState::Neutral, SignalArchetype::TrendPullback))
}

fn trigger_15m(
    candles: &VecDeque<ClosedCandle>,
    primary: TriggerState,
    config: &TechnicalStrategyConfig,
) -> Result<TriggerState, TechnicalError> {
    let closes = closes(candles)?;
    let close = *closes.last().ok_or(TechnicalError::Arithmetic)?;
    let ma7 = sma(&closes, 7).ok_or(TechnicalError::Arithmetic)?;
    let ma21 = sma(&closes, 21).ok_or(TechnicalError::Arithmetic)?;
    let rsi = rsi(&closes, 14).ok_or(TechnicalError::Arithmetic)?;
    let volume = volume_z(candles)?;
    Ok(match primary {
        TriggerState::Bullish
            if close > ma7 && ma7 > ma21 && rsi >= 50.0 && volume >= config.pullback_volume_z =>
        {
            TriggerState::Bullish
        }
        TriggerState::Bearish
            if close < ma7 && ma7 < ma21 && rsi <= 50.0 && volume >= config.pullback_volume_z =>
        {
            TriggerState::Bearish
        }
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
        assert_eq!(
            engine.accept_closed_candle(conflict),
            Err(TechnicalError::ConflictingCandle)
        );
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
}
