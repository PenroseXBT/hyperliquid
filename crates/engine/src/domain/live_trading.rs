use crate::domain::decision::{
    DecisionId, MarketSnapshotId, PayloadHash, PlannedAction, PlannedCloid, Side, TargetVersion,
};
use crate::domain::ioc::{ExecutionFill, ExecutionFillId, LatencyScenario};
use crate::domain::ledger::{DualLedger, EpisodeId, LedgerError, PortfolioEpisode};
use crate::domain::technical::{MarketRegime, SignalArchetype};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationMode {
    Normal,
    RiskOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExchangeOrderId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExchangeTradeId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FundingEventId(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExchangeFillIdentity {
    pub exchange_order_id: ExchangeOrderId,
    pub trade_id: ExchangeTradeId,
    pub cloid: PlannedCloid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityClassification {
    Maker,
    Taker,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyComponent {
    SourceOnly,
    TechnicalOnly,
    SourceTechnicalAgreement,
    SourceTechnicalDisagreement,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyComponentAttribution {
    pub component: StrategyComponent,
    pub source_fraction: Decimal,
    pub technical_fraction: Decimal,
    pub archetype: Option<SignalArchetype>,
    pub regime: Option<MarketRegime>,
    pub side: Side,
}

impl StrategyComponentAttribution {
    pub fn validate(&self) -> Result<(), LedgerError> {
        if self.source_fraction < Decimal::ZERO
            || self.technical_fraction < Decimal::ZERO
            || self.source_fraction > Decimal::ONE
            || self.technical_fraction > Decimal::ONE
            || self
                .source_fraction
                .checked_add(self.technical_fraction)
                .is_none_or(|total| total != Decimal::ONE)
        {
            return Err(LedgerError::InvalidLiveEvent(
                "strategy attribution fractions must sum to one",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedExchangeFill {
    pub identity: ExchangeFillIdentity,
    pub decision_id: DecisionId,
    pub target_version: TargetVersion,
    pub root_cloid: PlannedCloid,
    pub parent_cloid: Option<PlannedCloid>,
    pub continuation_generation: u32,
    pub asset: String,
    pub side: Side,
    pub reduce_only: bool,
    pub filled_quantity: Decimal,
    pub average_fill_price: Decimal,
    pub submitted_limit_price: Decimal,
    pub fee_amount: Decimal,
    pub exchange_closed_pnl: Decimal,
    pub fee_asset: String,
    pub liquidity: LiquidityClassification,
    pub decision_reference_price: Decimal,
    pub occurred_at: u64,
    pub decision_timestamp: u64,
    pub exchange_equity_after: Decimal,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedFundingEvent {
    pub event_id: FundingEventId,
    pub asset: String,
    /// Signed exchange equity delta: negative when funding is paid, positive when received.
    pub amount: Decimal,
    pub occurred_at: u64,
    pub source_hash: PayloadHash,
}

/// An authenticated external reduction has economic identity, not an invented
/// strategy decision or CLOID. The exchange's pre-fill position proves linkage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalReduction {
    pub exchange_order_id: String,
    pub trade_id: String,
    pub asset: String,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub fee_asset: String,
    pub closed_pnl: Decimal,
    pub position_before: Decimal,
    pub occurred_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalFillAccounting {
    pub fill: ExternalReduction,
    pub episode_id: EpisodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveFillAccounting {
    pub identity: ExchangeFillIdentity,
    pub decision_id: DecisionId,
    pub root_cloid: PlannedCloid,
    pub parent_cloid: Option<PlannedCloid>,
    pub continuation_generation: u32,
    pub asset: String,
    pub side: Side,
    pub filled_quantity: Decimal,
    pub average_fill_price: Decimal,
    pub submitted_limit_price: Decimal,
    pub fee_amount: Decimal,
    pub exchange_closed_pnl: Decimal,
    pub fee_asset: String,
    pub liquidity: LiquidityClassification,
    pub decision_reference_price: Decimal,
    pub decision_to_fill_slippage: Decimal,
    pub position_before: Decimal,
    pub position_after: Decimal,
    pub current_equity: Decimal,
    pub settled_equity: Decimal,
    pub deployment_equity: Decimal,
    pub occurred_at: u64,
    #[serde(default)]
    pub strategy_attribution: Option<StrategyComponentAttribution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingAccounting {
    pub event: VerifiedFundingEvent,
    pub current_equity: Decimal,
    pub settled_equity: Decimal,
    pub deployment_equity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveTradingState {
    schema_version: u32,
    starting_equity: Decimal,
    current_equity: Decimal,
    settled_equity: Decimal,
    deployment_equity: Decimal,
    ledger: DualLedger,
    applied_fills: BTreeSet<ExchangeFillIdentity>,
    applied_funding: BTreeSet<FundingEventId>,
    fill_events: Vec<LiveFillAccounting>,
    funding_events: Vec<FundingAccounting>,
    #[serde(default)]
    verified_fills: Vec<VerifiedExchangeFill>,
    #[serde(default)]
    verified_funding: Vec<VerifiedFundingEvent>,
    #[serde(default)]
    exchange_gross_pnl_by_open_episode: BTreeMap<String, Decimal>,
    #[serde(default)]
    open_episode_attribution: BTreeMap<String, StrategyComponentAttribution>,
    #[serde(default)]
    closed_episode_attribution: BTreeMap<EpisodeId, StrategyComponentAttribution>,
    fill_cursor_ms: u64,
    #[serde(default)]
    funding_cursor_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    external_fills: Vec<ExternalFillAccounting>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppliedFillResult {
    Applied {
        accounting: LiveFillAccounting,
        closed_episode: Option<PortfolioEpisode>,
    },
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppliedFundingResult {
    Applied(FundingAccounting),
    AlreadyApplied,
}

impl LiveTradingState {
    pub fn new(starting_equity: Decimal, fill_cursor_ms: u64) -> Result<Self, LedgerError> {
        if starting_equity <= Decimal::ZERO {
            return Err(LedgerError::InvalidLiveEvent(
                "starting equity must be positive",
            ));
        }
        Ok(Self {
            schema_version: 1,
            starting_equity,
            current_equity: starting_equity,
            settled_equity: starting_equity,
            deployment_equity: starting_equity,
            ledger: DualLedger::default(),
            applied_fills: BTreeSet::new(),
            applied_funding: BTreeSet::new(),
            fill_events: Vec::new(),
            funding_events: Vec::new(),
            verified_fills: Vec::new(),
            verified_funding: Vec::new(),
            exchange_gross_pnl_by_open_episode: BTreeMap::new(),
            open_episode_attribution: BTreeMap::new(),
            closed_episode_attribution: BTreeMap::new(),
            fill_cursor_ms,
            funding_cursor_ms: fill_cursor_ms,
            external_fills: Vec::new(),
        })
    }

    pub fn position(&self, asset: &str) -> Decimal {
        self.ledger.portfolio_position(asset)
    }

    pub fn external_fills(&self) -> &[ExternalFillAccounting] {
        &self.external_fills
    }
    pub fn ledger(&self) -> &DualLedger {
        &self.ledger
    }

    pub fn apply_external_reduction(
        &mut self,
        fill: ExternalReduction,
    ) -> Result<bool, LedgerError> {
        if let Some(prior) = self.external_fills.iter().find(|prior| {
            prior.fill.asset == fill.asset
                && prior.fill.exchange_order_id == fill.exchange_order_id
                && prior.fill.trade_id == fill.trade_id
        }) {
            return if prior.fill == fill {
                Ok(false)
            } else {
                Err(LedgerError::InvalidLiveEvent(
                    "external fill identity payload conflict",
                ))
            };
        }
        let mut next = self.clone();
        let episode_id = next.ledger.reduce_external(&fill)?;
        if fill.position_before.is_zero()
            || (fill.side == Side::Buy) == fill.position_before.is_sign_positive()
        {
            next.open_episode_attribution.remove(&fill.asset);
        }
        let closed_previous = !fill.position_before.is_zero()
            && fill.quantity >= fill.position_before.abs()
            && (fill.side == Side::Buy) != fill.position_before.is_sign_positive();
        if closed_previous {
            next.exchange_gross_pnl_by_open_episode.remove(&fill.asset);
            if let Some(attribution) = next.open_episode_attribution.remove(&fill.asset) {
                next.closed_episode_attribution
                    .insert(episode_id, attribution);
            }
            next.settled_equity = next
                .starting_equity
                .checked_add(next.ledger.portfolio_realized_net_pnl()?)
                .ok_or(LedgerError::ArithmeticOverflow("external settled cash"))?;
        } else {
            let gross = next
                .exchange_gross_pnl_by_open_episode
                .entry(fill.asset.clone())
                .or_default();
            *gross = gross
                .checked_add(fill.closed_pnl)
                .ok_or(LedgerError::ArithmeticOverflow("external gross"))?;
        }
        next.external_fills
            .push(ExternalFillAccounting { fill, episode_id });
        next.recompute_deployment();
        *self = next;
        Ok(true)
    }

    pub fn positions(&self) -> BTreeMap<String, Decimal> {
        self.ledger
            .portfolio_assets()
            .into_iter()
            .map(|asset| {
                let position = self.ledger.portfolio_position(&asset);
                (asset, position)
            })
            .collect()
    }

    pub fn current_equity(&self) -> Decimal {
        self.current_equity
    }

    pub fn settled_equity(&self) -> Decimal {
        self.settled_equity
    }

    pub fn deployment_equity(&self) -> Decimal {
        self.deployment_equity
    }

    pub fn fill_cursor_ms(&self) -> u64 {
        self.fill_cursor_ms
    }

    pub fn advance_fill_cursor(&mut self, cursor_ms: u64) -> Result<(), LedgerError> {
        if cursor_ms < self.fill_cursor_ms {
            return Err(LedgerError::InvalidLiveEvent(
                "fill cursor cannot move backwards",
            ));
        }
        self.fill_cursor_ms = cursor_ms;
        Ok(())
    }

    pub fn funding_cursor_ms(&self) -> u64 {
        self.funding_cursor_ms
    }

    pub fn advance_funding_cursor(&mut self, cursor_ms: u64) -> Result<(), LedgerError> {
        if cursor_ms < self.funding_cursor_ms {
            return Err(LedgerError::InvalidLiveEvent(
                "funding cursor cannot move backwards",
            ));
        }
        self.funding_cursor_ms = cursor_ms;
        Ok(())
    }

    pub fn reconcile_equity(&mut self, exchange_equity: Decimal) -> Result<(), LedgerError> {
        if exchange_equity <= Decimal::ZERO {
            return Err(LedgerError::InvalidLiveEvent(
                "exchange equity must be positive",
            ));
        }
        self.current_equity = exchange_equity;
        self.recompute_deployment();
        Ok(())
    }

    pub fn reconcile_positions(
        &self,
        exchange_positions: &BTreeMap<String, Decimal>,
    ) -> Result<(), LedgerError> {
        if self.positions() == *exchange_positions {
            Ok(())
        } else {
            Err(LedgerError::PositionDivergence)
        }
    }

    pub fn fill_events(&self) -> &[LiveFillAccounting] {
        &self.fill_events
    }

    pub fn funding_events(&self) -> &[FundingAccounting] {
        &self.funding_events
    }

    pub fn verified_fills(&self) -> &[VerifiedExchangeFill] {
        &self.verified_fills
    }

    pub fn verified_funding(&self) -> &[VerifiedFundingEvent] {
        &self.verified_funding
    }

    pub fn latest_exchange_event_timestamp(&self) -> Option<u64> {
        self.verified_fills
            .last()
            .map(|fill| fill.occurred_at)
            .into_iter()
            .chain(
                self.verified_funding
                    .last()
                    .map(|funding| funding.occurred_at),
            )
            .chain(
                self.external_fills
                    .last()
                    .map(|event| event.fill.occurred_at),
            )
            .max()
    }

    pub fn set_strategy_attribution(
        &mut self,
        asset: impl Into<String>,
        attribution: StrategyComponentAttribution,
    ) -> Result<(), LedgerError> {
        attribution.validate()?;
        let asset = asset.into();
        if self.ledger.open_episode_unattributed(&asset) {
            return Err(LedgerError::InvalidLiveEvent(
                "unattributed episode cannot acquire strategy credit",
            ));
        }
        self.open_episode_attribution.insert(asset, attribution);
        Ok(())
    }

    pub fn closed_episode_attribution(
        &self,
        episode_id: EpisodeId,
    ) -> Option<&StrategyComponentAttribution> {
        self.closed_episode_attribution
            .get(&episode_id)
            .filter(|_| !self.ledger.episode_unattributed(episode_id))
    }

    pub fn save_atomic(&self, path: impl AsRef<Path>) -> Result<(), LedgerError> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| LedgerError::Persistence("live ledger path has no parent".into()))?;
        std::fs::create_dir_all(parent)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        let temporary = path.with_extension("tmp");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        std::io::Write::write_all(&mut file, &bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        std::fs::rename(&temporary, path)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let bytes =
            std::fs::read(path).map_err(|error| LedgerError::Persistence(error.to_string()))?;
        let mut state: Self = serde_json::from_slice(&bytes)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        if state.schema_version != 1
            || state.starting_equity <= Decimal::ZERO
            || state.current_equity <= Decimal::ZERO
            || state.settled_equity <= Decimal::ZERO
            || state.deployment_equity != state.current_equity.min(state.settled_equity)
        {
            return Err(LedgerError::SchemaMismatch);
        }
        let fill_identities: BTreeSet<_> = state
            .fill_events
            .iter()
            .map(|event| event.identity.clone())
            .collect();
        let funding_identities: BTreeSet<_> = state
            .funding_events
            .iter()
            .map(|event| event.event.event_id)
            .collect();
        if fill_identities.len() != state.fill_events.len()
            || fill_identities != state.applied_fills
            || funding_identities.len() != state.funding_events.len()
            || funding_identities != state.applied_funding
        {
            return Err(LedgerError::SchemaMismatch);
        }
        let migrated_cash = state.ledger.migrate_cash_accounting()?;
        state.ledger.migrate_ownership(&state.external_fills);
        state.ledger.validate_integrity()?;
        if state.external_fills.iter().any(|event| {
            (event.fill.position_before.is_zero()
                || (event.fill.side == Side::Buy) == event.fill.position_before.is_sign_positive())
                && !state.ledger.episode_unattributed(event.episode_id)
        }) {
            return Err(LedgerError::InvalidLiveEvent(
                "external ownership tombstone missing",
            ));
        }
        if state
            .open_episode_attribution
            .keys()
            .any(|asset| state.ledger.open_episode_unattributed(asset))
            || state
                .closed_episode_attribution
                .keys()
                .any(|id| state.ledger.episode_unattributed(*id))
        {
            return Err(LedgerError::InvalidLiveEvent(
                "unattributed episode reclaimed",
            ));
        }
        if migrated_cash {
            state.settled_equity = state
                .starting_equity
                .checked_add(state.ledger.portfolio_realized_net_pnl()?)
                .ok_or(LedgerError::ArithmeticOverflow("migrated settled cash"))?;
            state.recompute_deployment();
        }
        Ok(state)
    }

    fn recompute_deployment(&mut self) {
        self.deployment_equity = self.current_equity.min(self.settled_equity);
    }
}

pub fn execution_from_verified_fill(
    fill: &VerifiedExchangeFill,
    position_before: Decimal,
) -> Result<ExecutionFill, LedgerError> {
    let signed_delta = match fill.side {
        Side::Buy => fill.filled_quantity,
        Side::Sell => -fill.filled_quantity,
    };
    let position_after = position_before
        .checked_add(signed_delta)
        .ok_or(LedgerError::ArithmeticOverflow("live position"))?;
    let filled_notional = fill
        .filled_quantity
        .checked_mul(fill.average_fill_price)
        .ok_or(LedgerError::ArithmeticOverflow("live filled notional"))?;
    let slippage = fill
        .average_fill_price
        .checked_sub(fill.decision_reference_price)
        .and_then(|difference| {
            difference.checked_mul(match fill.side {
                Side::Buy => fill.filled_quantity,
                Side::Sell => -fill.filled_quantity,
            })
        })
        .ok_or(LedgerError::ArithmeticOverflow("live slippage"))?;
    Ok(ExecutionFill {
        execution_id: ExecutionFillId(derive_execution_id(&fill.identity)),
        action: PlannedAction {
            decision_id: fill.decision_id,
            target_version: fill.target_version,
            asset: fill.asset.clone(),
            side: fill.side,
            rounded_notional: filled_notional,
            reduce_only: fill.reduce_only,
            action_ordinal: 0,
            retry_generation: fill.continuation_generation,
            planned_cloid: fill.identity.cloid,
        },
        decision_timestamp_mono: fill.decision_timestamp,
        decision_market_snapshot_id: MarketSnapshotId(fill.source_hash.0),
        evaluation_market_snapshot_id: MarketSnapshotId(fill.source_hash.0),
        latency_scenario: LatencyScenario::Expected,
        configured_latency_ms: fill.occurred_at.saturating_sub(fill.decision_timestamp),
        proposed_limit_price: fill.submitted_limit_price,
        rounded_quantity: fill.filled_quantity,
        modeled_filled_quantity: fill.filled_quantity,
        modeled_average_fill_price: Some(fill.average_fill_price),
        unfilled_ioc_remainder: Decimal::ZERO,
        modeled_filled_notional: filled_notional,
        fees: fill.fee_amount,
        funding: Decimal::ZERO,
        slippage,
        position_before,
        position_after,
    })
}

pub fn apply_exchange_fill(
    state: &mut LiveTradingState,
    fill: VerifiedExchangeFill,
) -> Result<AppliedFillResult, LedgerError> {
    validate_fill(&fill)?;
    if state.applied_fills.contains(&fill.identity) {
        return if state
            .verified_fills
            .iter()
            .any(|existing| existing == &fill)
        {
            Ok(AppliedFillResult::AlreadyApplied)
        } else {
            Err(LedgerError::InvalidLiveEvent(
                "exchange fill identity payload conflict",
            ))
        };
    }
    // Apply to a clone so every validation/arithmetic failure is transactional.
    let mut next = state.clone();
    let verified_fill = fill.clone();
    let position_before = next.ledger.portfolio_position(&fill.asset);
    let strategy_attribution = next.open_episode_attribution.get(&fill.asset).cloned();
    let mut execution = execution_from_verified_fill(&fill, position_before)?;
    execution.decision_timestamp_mono = fill.occurred_at;
    let position_after = execution.position_after;
    let slippage = execution.slippage;
    let closed_before = next.ledger.portfolio_closed().len();
    next.ledger
        .apply_portfolio_execution(&execution, fill.occurred_at)?;
    let exchange_gross = next
        .exchange_gross_pnl_by_open_episode
        .entry(fill.asset.clone())
        .or_default();
    *exchange_gross = exchange_gross
        .checked_add(fill.exchange_closed_pnl)
        .ok_or(LedgerError::ArithmeticOverflow("exchange gross pnl"))?;
    let did_close = next.ledger.portfolio_closed().len() > closed_before;
    if did_close {
        let reconciled_gross = next
            .exchange_gross_pnl_by_open_episode
            .remove(&fill.asset)
            .ok_or(LedgerError::PositionDivergence)?;
        next.ledger
            .reconcile_last_portfolio_gross_pnl(&fill.asset, reconciled_gross)?;
    }
    let closed_episode = did_close
        .then(|| next.ledger.portfolio_closed().last().cloned())
        .flatten();
    if let Some(episode) = &closed_episode {
        if let Some(attribution) = next.open_episode_attribution.remove(&episode.asset) {
            next.closed_episode_attribution
                .insert(episode.episode_id, attribution);
        }
        next.settled_equity = next
            .settled_equity
            .checked_add(episode.net_pnl)
            .ok_or(LedgerError::ArithmeticOverflow("settled equity"))?;
    }
    next.current_equity = fill.exchange_equity_after;
    if next.current_equity <= Decimal::ZERO || next.settled_equity <= Decimal::ZERO {
        return Err(LedgerError::InvalidLiveEvent("equity became nonpositive"));
    }
    next.recompute_deployment();
    let accounting = LiveFillAccounting {
        identity: fill.identity.clone(),
        decision_id: fill.decision_id,
        root_cloid: fill.root_cloid,
        parent_cloid: fill.parent_cloid,
        continuation_generation: fill.continuation_generation,
        asset: fill.asset,
        side: fill.side,
        filled_quantity: fill.filled_quantity,
        average_fill_price: fill.average_fill_price,
        submitted_limit_price: fill.submitted_limit_price,
        fee_amount: fill.fee_amount,
        exchange_closed_pnl: fill.exchange_closed_pnl,
        fee_asset: fill.fee_asset,
        liquidity: fill.liquidity,
        decision_reference_price: fill.decision_reference_price,
        decision_to_fill_slippage: slippage,
        position_before,
        position_after,
        current_equity: next.current_equity,
        settled_equity: next.settled_equity,
        deployment_equity: next.deployment_equity,
        occurred_at: fill.occurred_at,
        strategy_attribution,
    };
    next.applied_fills.insert(fill.identity);
    next.fill_events.push(accounting.clone());
    next.verified_fills.push(verified_fill);
    *state = next;
    Ok(AppliedFillResult::Applied {
        accounting,
        closed_episode,
    })
}

pub fn apply_funding_event(
    state: &mut LiveTradingState,
    event: VerifiedFundingEvent,
) -> Result<AppliedFundingResult, LedgerError> {
    if event.asset.is_empty() {
        return Err(LedgerError::InvalidLiveEvent("funding asset is empty"));
    }
    if state.applied_funding.contains(&event.event_id) {
        return if state
            .verified_funding
            .iter()
            .any(|existing| existing == &event)
        {
            Ok(AppliedFundingResult::AlreadyApplied)
        } else {
            Err(LedgerError::InvalidLiveEvent(
                "funding event identity payload conflict",
            ))
        };
    }
    let mut next = state.clone();
    let verified_funding = event.clone();
    let funding_cost = -event.amount;
    next.ledger
        .apply_portfolio_funding(&event.asset, funding_cost)?;
    next.current_equity = next
        .current_equity
        .checked_add(event.amount)
        .ok_or(LedgerError::ArithmeticOverflow("funding equity"))?;
    if next.current_equity <= Decimal::ZERO {
        return Err(LedgerError::InvalidLiveEvent(
            "funding made equity nonpositive",
        ));
    }
    next.recompute_deployment();
    let accounting = FundingAccounting {
        event: event.clone(),
        current_equity: next.current_equity,
        settled_equity: next.settled_equity,
        deployment_equity: next.deployment_equity,
    };
    next.applied_funding.insert(event.event_id);
    next.funding_events.push(accounting.clone());
    next.verified_funding.push(verified_funding);
    *state = next;
    Ok(AppliedFundingResult::Applied(accounting))
}

fn validate_fill(fill: &VerifiedExchangeFill) -> Result<(), LedgerError> {
    if fill.asset.is_empty()
        || fill.filled_quantity <= Decimal::ZERO
        || fill.average_fill_price <= Decimal::ZERO
        || fill.submitted_limit_price <= Decimal::ZERO
        || fill.fee_asset.is_empty()
        || fill.decision_reference_price <= Decimal::ZERO
        || fill.exchange_equity_after <= Decimal::ZERO
        || fill.occurred_at < fill.decision_timestamp
        || fill.identity.cloid != fill.root_cloid && fill.continuation_generation == 0
        || fill.continuation_generation > 0 && fill.parent_cloid.is_none()
    {
        return Err(LedgerError::InvalidLiveEvent(
            "invalid verified exchange fill",
        ));
    }
    Ok(())
}

fn derive_execution_id(identity: &ExchangeFillIdentity) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"LIVE/EXCHANGE_FILL/V1");
    hash.update((identity.exchange_order_id.0.len() as u32).to_be_bytes());
    hash.update(identity.exchange_order_id.0.as_bytes());
    hash.update((identity.trade_id.0.len() as u32).to_be_bytes());
    hash.update(identity.trade_id.0.as_bytes());
    hash.update(identity.cloid.0);
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    #[test]
    fn manual_scale_in_and_partial_exit_preserve_cash_without_strategy_credit() {
        use super::*;
        for side in [Side::Buy, Side::Sell] {
            let sign = if side == Side::Buy {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            let mut live = LiveTradingState::new(Decimal::from(1000), 0).unwrap();
            let mut opening = fill(
                "owned",
                side,
                if side == Side::Buy { 100 } else { 120 },
                1000,
            );
            opening.reduce_only = false;
            opening.exchange_closed_pnl = Decimal::ZERO;
            apply_exchange_fill(&mut live, opening.clone()).unwrap();
            let addition = ExternalReduction {
                exchange_order_id: "manual".into(),
                trade_id: "add".into(),
                asset: "BTC".into(),
                side,
                quantity: Decimal::ONE,
                price: Decimal::from(if side == Side::Buy { 120 } else { 100 }),
                fee: Decimal::new(2, 1),
                fee_asset: "USDC".into(),
                closed_pnl: Decimal::ZERO,
                position_before: sign,
                occurred_at: 30,
            };
            assert!(live.apply_external_reduction(addition.clone()).unwrap());
            assert!(!live.apply_external_reduction(addition.clone()).unwrap());
            assert_eq!(live.position("BTC"), sign * Decimal::from(2));
            let mixed_id = live.external_fills()[0].episode_id;
            let attribution = StrategyComponentAttribution {
                component: StrategyComponent::SourceOnly,
                source_fraction: Decimal::ONE,
                technical_fraction: Decimal::ZERO,
                archetype: None,
                regime: None,
                side,
            };
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("live.json");
            live.save_atomic(&path).unwrap();
            live = LiveTradingState::load(&path).unwrap();
            assert!(live.ledger().episode_unattributed(mixed_id));
            assert!(live
                .set_strategy_attribution("BTC", attribution.clone())
                .is_err());
            let mut later = live.clone();
            let mut engine_add = fill("later-owned", side, 110, 1000);
            engine_add.reduce_only = false;
            engine_add.exchange_closed_pnl = Decimal::ZERO;
            apply_exchange_fill(&mut later, engine_add).unwrap();
            later.save_atomic(&path).unwrap();
            later = LiveTradingState::load(&path).unwrap();
            assert!(later.ledger().episode_unattributed(mixed_id));
            assert!(later
                .set_strategy_attribution("BTC", attribution.clone())
                .is_err());
            let wire = rmp_serde::to_vec(live.ledger()).unwrap();
            assert_eq!(
                rmp_serde::from_slice::<DualLedger>(&wire).unwrap(),
                *live.ledger()
            );
            let mut close = addition.clone();
            close.side = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            close.price = Decimal::from(if side == Side::Buy { 130 } else { 90 });
            close.quantity = Decimal::new(5, 1);
            close.position_before = sign * Decimal::from(2);
            close.trade_id = "partial".into();
            close.fee = Decimal::new(1, 1);
            close.closed_pnl = Decimal::from(10);
            close.occurred_at = 40;
            live.apply_external_reduction(close.clone()).unwrap();
            assert_eq!(live.position("BTC"), sign * Decimal::new(15, 1));
            close.quantity = Decimal::new(15, 1);
            close.position_before = sign * close.quantity;
            close.trade_id = "final".into();
            close.fee = Decimal::new(3, 1);
            close.closed_pnl = Decimal::from(30);
            close.occurred_at = 50;
            live.apply_external_reduction(close).unwrap();
            assert!(live.positions().is_empty());
            assert_eq!(live.settled_equity(), Decimal::new(10393, 1));
            let episode = &live.ledger().portfolio_closed()[0];
            assert_eq!(episode.opening_decision_id, DecisionId([0; 32]));
            assert_eq!(episode.net_pnl, Decimal::new(393, 1));
            live.save_atomic(&path).unwrap();
            live = LiveTradingState::load(&path).unwrap();
            assert!(live.closed_episode_attribution(mixed_id).is_none());
            // Even a persisted attribution written by a future pass is rejected.
            live.closed_episode_attribution
                .insert(mixed_id, attribution);
            assert!(live.closed_episode_attribution(mixed_id).is_none());
            live.closed_episode_attribution.clear();
            let mut corrupted = serde_json::to_value(&live).unwrap();
            corrupted["ledger"]["portfolio"]["closed"][0]["opening_decision_id"] =
                serde_json::to_value(DecisionId([1; 32])).unwrap();
            std::fs::write(&path, serde_json::to_vec(&corrupted).unwrap()).unwrap();
            assert!(LiveTradingState::load(&path).is_err());
            corrupted["ledger"]["recovered"]["unattributed_episodes"] = serde_json::json!([]);
            std::fs::write(&path, serde_json::to_vec(&corrupted).unwrap()).unwrap();
            assert!(LiveTradingState::load(&path).is_err());
            let mut replay = DualLedger::default();
            for _ in 0..2 {
                replay.recover_engine_fill(&opening, Decimal::ZERO).unwrap();
                for event in live.external_fills() {
                    replay.recover_external_fill(event, Decimal::ZERO).unwrap();
                }
            }
            assert_eq!(replay.portfolio_closed(), live.ledger().portfolio_closed());
            assert!(replay.source_positions_for_asset("BTC").is_empty());
            let mut attributed = DualLedger::default();
            let execution = execution_from_verified_fill(&opening, Decimal::ZERO).unwrap();
            attributed
                .apply_portfolio_execution(&execution, opening.occurred_at)
                .unwrap();
            attributed
                .apply_source_execution("copied-wallet", &execution, opening.occurred_at)
                .unwrap();
            for event in live.external_fills() {
                attributed
                    .recover_external_fill(event, Decimal::ZERO)
                    .unwrap();
            }
            assert_eq!(attributed.source_closed_count("copied-wallet"), 0);
            assert_eq!(attributed.source_closed_count("recovered:unattributed"), 1);
            attributed.compact_closed_before(100).unwrap();
            attributed.save_atomic(&path).unwrap();
            assert!(DualLedger::load(&path)
                .unwrap()
                .episode_unattributed(mixed_id));
            let mut conflicting = addition;
            conflicting.fee = Decimal::ONE;
            assert!(live.apply_external_reduction(conflicting).is_err());
        }
    }

    #[test]
    fn external_reversal_closes_strategy_and_opens_manual_once() {
        use super::*;
        let mut live = LiveTradingState::new(Decimal::from(1000), 0).unwrap();
        let opening = fill("open", Side::Buy, 100, 1000);
        apply_exchange_fill(&mut live, opening).unwrap();
        let reversal = ExternalReduction {
            exchange_order_id: "external-flip".into(),
            trade_id: "flip-1".into(),
            asset: "BTC".into(),
            side: Side::Sell,
            quantity: Decimal::from(2),
            price: Decimal::from(90),
            fee: Decimal::new(2, 1),
            fee_asset: "USDC".into(),
            closed_pnl: Decimal::from(-10),
            position_before: Decimal::ONE,
            occurred_at: 2000,
        };
        assert!(live.apply_external_reduction(reversal.clone()).unwrap());
        assert!(!live.apply_external_reduction(reversal).unwrap());
        assert_eq!(live.position("BTC"), -Decimal::ONE);
        assert_eq!(live.ledger().portfolio_closed().len(), 1);
        assert_eq!(live.ledger().portfolio_closed()[0].fees, Decimal::new(2, 1));
        assert_eq!(live.settled_equity(), Decimal::new(9898, 1));
        let mut replay = DualLedger::default();
        for _ in 0..2 {
            for fill in live.verified_fills() {
                replay.recover_engine_fill(fill, Decimal::ZERO).unwrap();
            }
            for event in live.external_fills() {
                replay.recover_external_fill(event, Decimal::ZERO).unwrap();
            }
        }
        assert_eq!(replay.portfolio_position("BTC"), -Decimal::ONE);
        assert_eq!(replay.portfolio_closed().len(), 1);
    }

    #[test]
    fn manual_hip3_round_trip_replays_without_strategy_identity() {
        use super::*;
        for side in [Side::Buy, Side::Sell] {
            let mut live = LiveTradingState::new(Decimal::from(315), 0).unwrap();
            let open = ExternalReduction {
                exchange_order_id: "manual-open".into(),
                trade_id: "1".into(),
                asset: "xyz:SKHY".into(),
                side,
                quantity: Decimal::ONE,
                price: Decimal::from(100),
                fee: Decimal::new(1, 2),
                fee_asset: "USDC".into(),
                closed_pnl: Decimal::ZERO,
                position_before: Decimal::ZERO,
                occurred_at: 1000,
            };
            let mut close = open.clone();
            close.exchange_order_id = "manual-close".into();
            close.trade_id = "2".into();
            close.side = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            close.price = if side == Side::Buy {
                Decimal::from(103)
            } else {
                Decimal::from(97)
            };
            close.closed_pnl = Decimal::from(3);
            close.position_before = if side == Side::Buy {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            close.occurred_at = 2000;
            assert!(live.apply_external_reduction(open.clone()).unwrap());
            assert!(live.apply_external_reduction(close.clone()).unwrap());
            assert!(!live.apply_external_reduction(open).unwrap());
            assert!(!live.apply_external_reduction(close.clone()).unwrap());
            assert_eq!(live.settled_equity(), Decimal::new(31798, 2));
            assert!(live.positions().is_empty());
            assert!(live.verified_fills().is_empty());
            assert_eq!(live.latest_exchange_event_timestamp(), Some(2000));
            let path =
                std::env::temp_dir().join(format!("hl-manual-replay-{}.json", std::process::id()));
            live.save_atomic(&path).unwrap();
            assert_eq!(LiveTradingState::load(&path).unwrap(), live);
            std::fs::remove_file(path).unwrap();
            let episode = &live.ledger().portfolio_closed()[0];
            assert_eq!(episode.opening_decision_id, DecisionId([0; 32]));
            assert_eq!(episode.net_pnl, Decimal::new(298, 2));
            let mut replay = DualLedger::default();
            for _ in 0..2 {
                for event in live.external_fills() {
                    replay.recover_external_fill(event, Decimal::ZERO).unwrap();
                }
            }
            assert_eq!(replay.portfolio_closed().len(), 1);
            close.fee = Decimal::ONE;
            assert!(live.apply_external_reduction(close).is_err());
        }
    }

    use super::*;

    fn fill(trade: &str, side: Side, price: i64, equity: i64) -> VerifiedExchangeFill {
        VerifiedExchangeFill {
            identity: ExchangeFillIdentity {
                exchange_order_id: ExchangeOrderId("42".into()),
                trade_id: ExchangeTradeId(trade.into()),
                cloid: PlannedCloid([2; 16]),
            },
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            root_cloid: PlannedCloid([2; 16]),
            parent_cloid: None,
            continuation_generation: 0,
            asset: "BTC".into(),
            side,
            reduce_only: side == Side::Sell,
            filled_quantity: Decimal::ONE,
            average_fill_price: Decimal::from(price),
            submitted_limit_price: Decimal::from(price),
            fee_amount: Decimal::new(1, 1),
            exchange_closed_pnl: if side == Side::Sell {
                Decimal::from(2)
            } else {
                Decimal::ZERO
            },
            fee_asset: "USDC".into(),
            liquidity: LiquidityClassification::Taker,
            decision_reference_price: Decimal::from(price),
            occurred_at: if side == Side::Buy { 10 } else { 20 },
            decision_timestamp: if side == Side::Buy { 9 } else { 19 },
            exchange_equity_after: Decimal::from(equity),
            source_hash: PayloadHash([3; 32]),
        }
    }

    #[test]
    fn duplicate_fill_and_funding_are_exactly_once() {
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        let entry = fill("1", Side::Buy, 10, 99);
        assert!(matches!(
            apply_exchange_fill(&mut state, entry.clone()).unwrap(),
            AppliedFillResult::Applied { .. }
        ));
        assert_eq!(
            apply_exchange_fill(&mut state, entry).unwrap(),
            AppliedFillResult::AlreadyApplied
        );
        let funding = VerifiedFundingEvent {
            event_id: FundingEventId([4; 32]),
            asset: "BTC".into(),
            amount: Decimal::new(-1, 1),
            occurred_at: 15,
            source_hash: PayloadHash([5; 32]),
        };
        assert!(matches!(
            apply_funding_event(&mut state, funding.clone()).unwrap(),
            AppliedFundingResult::Applied(_)
        ));
        assert_eq!(
            apply_funding_event(&mut state, funding).unwrap(),
            AppliedFundingResult::AlreadyApplied
        );
        assert_eq!(state.fill_events().len(), 1);
        assert_eq!(state.funding_events().len(), 1);
    }

    #[test]
    fn duplicate_exchange_identities_with_changed_payload_fail_closed() {
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        let entry = fill("1", Side::Buy, 10, 99);
        apply_exchange_fill(&mut state, entry.clone()).unwrap();
        let mut conflicting_fill = entry;
        conflicting_fill.fee_amount = Decimal::ONE;
        assert!(apply_exchange_fill(&mut state, conflicting_fill).is_err());

        let funding = VerifiedFundingEvent {
            event_id: FundingEventId([4; 32]),
            asset: "BTC".into(),
            amount: Decimal::new(-1, 1),
            occurred_at: 15,
            source_hash: PayloadHash([5; 32]),
        };
        apply_funding_event(&mut state, funding.clone()).unwrap();
        let mut conflicting_funding = funding;
        conflicting_funding.amount = Decimal::ONE;
        assert!(apply_funding_event(&mut state, conflicting_funding).is_err());
    }

    #[test]
    fn audit_actual_fill_cash_identity_with_nonzero_reference_slippage() {
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        let mut entry = fill("audit-entry", Side::Buy, 10, 100);
        entry.decision_reference_price = Decimal::from(9);
        apply_exchange_fill(&mut state, entry).unwrap();
        apply_funding_event(
            &mut state,
            VerifiedFundingEvent {
                event_id: FundingEventId([8; 32]),
                asset: "BTC".into(),
                amount: Decimal::new(-1, 1),
                occurred_at: 15,
                source_hash: PayloadHash([5; 32]),
            },
        )
        .unwrap();
        apply_exchange_fill(&mut state, fill("audit-close", Side::Sell, 12, 101)).unwrap();
        let episode = state.ledger.portfolio_closed().last().unwrap();
        eprintln!("AUDIT gross={} fees={} funding_cost={} reference_slippage={} ledger_net={} cash_net=1.7",
            episode.realized_pnl, episode.fees, episode.funding, episode.slippage, episode.net_pnl);
        assert_eq!(
            episode.net_pnl,
            Decimal::new(17, 1),
            "actual 10-to-12 fill PnL already includes execution prices"
        );
    }

    #[test]
    fn long_and_short_cash_ignore_adverse_or_favorable_reference_slippage() {
        for side in [Side::Buy, Side::Sell] {
            for improvement in [false, true] {
                let sign = if side == Side::Buy {
                    Decimal::ONE
                } else {
                    -Decimal::ONE
                };
                let close_side = if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                };
                let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
                let mut entry = fill("entry", side, 10, 100);
                entry.reduce_only = false;
                entry.exchange_closed_pnl = Decimal::ZERO;
                entry.occurred_at = 10;
                entry.decision_timestamp = 9;
                entry.decision_reference_price =
                    Decimal::from(10) + if improvement { sign } else { -sign };
                apply_exchange_fill(&mut state, entry).unwrap();
                apply_funding_event(
                    &mut state,
                    VerifiedFundingEvent {
                        event_id: FundingEventId([9; 32]),
                        asset: "BTC".into(),
                        amount: Decimal::new(-1, 1),
                        occurred_at: 15,
                        source_hash: PayloadHash([5; 32]),
                    },
                )
                .unwrap();
                let mut close = fill(
                    "close",
                    close_side,
                    if side == Side::Buy { 12 } else { 8 },
                    101,
                );
                close.reduce_only = true;
                close.exchange_closed_pnl = Decimal::from(2);
                close.occurred_at = 20;
                close.decision_timestamp = 19;
                apply_exchange_fill(&mut state, close).unwrap();
                let episode = state.ledger.portfolio_closed().last().unwrap();
                assert_eq!(episode.net_pnl, Decimal::new(17, 1));
                assert_eq!(state.settled_equity(), Decimal::new(1017, 1));
                assert_eq!(
                    episode.slippage,
                    if improvement {
                        -Decimal::ONE
                    } else {
                        Decimal::ONE
                    }
                );
                assert_eq!(
                    state.fill_events()[0].decision_to_fill_slippage,
                    episode.slippage
                );
            }
        }
    }

    #[test]
    fn settled_equity_changes_only_when_episode_closes() {
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        apply_exchange_fill(&mut state, fill("1", Side::Buy, 10, 99)).unwrap();
        assert_eq!(state.settled_equity(), Decimal::from(100));
        let result = apply_exchange_fill(&mut state, fill("2", Side::Sell, 12, 101)).unwrap();
        let AppliedFillResult::Applied { closed_episode, .. } = result else {
            panic!("fill was not applied")
        };
        assert!(closed_episode.is_some());
        assert_eq!(state.settled_equity(), Decimal::new(1018, 1));
        assert_eq!(
            state.deployment_equity(),
            Decimal::new(1018, 1).min(Decimal::from(101))
        );
    }

    #[test]
    fn funding_is_realized_into_settled_equity_when_episode_closes() {
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        apply_exchange_fill(&mut state, fill("1", Side::Buy, 10, 99)).unwrap();
        apply_funding_event(
            &mut state,
            VerifiedFundingEvent {
                event_id: FundingEventId([8; 32]),
                asset: "BTC".into(),
                amount: Decimal::new(-1, 1),
                occurred_at: 15,
                source_hash: PayloadHash([5; 32]),
            },
        )
        .unwrap();
        assert_eq!(state.settled_equity(), Decimal::from(100));
        apply_exchange_fill(&mut state, fill("2", Side::Sell, 12, 101)).unwrap();
        assert_eq!(state.settled_equity(), Decimal::new(1017, 1));
    }

    #[test]
    fn failed_live_event_is_transactional() {
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        let before = state.clone();
        let event = VerifiedFundingEvent {
            event_id: FundingEventId([9; 32]),
            asset: "BTC".into(),
            amount: Decimal::from(-101),
            occurred_at: 1,
            source_hash: PayloadHash([1; 32]),
        };
        assert!(apply_funding_event(&mut state, event).is_err());
        assert_eq!(state, before);
    }
}
