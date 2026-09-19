use crate::domain::decision::{DecisionId, PlannedCloid, Side};
use crate::domain::ioc::ExecutionFill;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::Path;

/// Ledger component carrying operator-owned exposure the strategy must never
/// trade. Exchange authority proves the position exists; this label
/// explicitly disclaims it as a strategy decision (see `reduce_external`).
pub const RECOVERED_UNATTRIBUTED_COMPONENT: &str = "recovered:unattributed";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EpisodeId(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioEpisode {
    pub episode_id: EpisodeId,
    pub asset: String,
    pub opened_at: u64,
    pub closed_at: u64,
    pub opening_decision_id: DecisionId,
    pub entry_notional: Decimal,
    pub exit_notional: Decimal,
    pub realized_pnl: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    pub slippage: Decimal,
    #[serde(default)]
    pub attribution_residual: Decimal,
    pub net_pnl: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEpisode {
    pub candidate_id: String,
    pub source_episode_id: EpisodeId,
    pub asset: String,
    pub opened_at: u64,
    pub closed_at: u64,
    pub modeled_entry: Decimal,
    pub modeled_exit: Decimal,
    pub modeled_fees: Decimal,
    pub modeled_funding: Decimal,
    pub modeled_slippage: Decimal,
    pub modeled_gross_pnl: Decimal,
    #[serde(default)]
    pub attribution_residual: Decimal,
    pub modeled_net_pnl: Decimal,
    pub net_return: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OpenEpisode {
    episode_id: EpisodeId,
    asset: String,
    opened_at: u64,
    opening_decision_id: DecisionId,
    signed_quantity: Decimal,
    average_entry_price: Decimal,
    entry_notional: Decimal,
    exit_notional: Decimal,
    realized_pnl: Decimal,
    fees: Decimal,
    funding: Decimal,
    slippage: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPositionState {
    pub episode_id: String,
    pub opened_at: u64,
    pub signed_quantity: Decimal,
    pub average_entry_price: Decimal,
    pub realized_pnl: Decimal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct EpisodeBook {
    positions: BTreeMap<String, Decimal>,
    open: BTreeMap<String, OpenEpisode>,
    closed: Vec<PortfolioEpisode>,
    #[serde(default)]
    archived: BTreeMap<String, EpisodeTotals>,
    next_episode_sequence: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeTotals {
    pub closed_count: u64,
    pub entry_notional: Decimal,
    pub exit_notional: Decimal,
    pub gross_pnl: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    pub slippage: Decimal,
    pub net_pnl: Decimal,
    pub gains: Decimal,
    pub losses: Decimal,
}

impl EpisodeTotals {
    fn add_episode(&mut self, episode: &PortfolioEpisode) -> Result<(), LedgerError> {
        self.closed_count = self
            .closed_count
            .checked_add(1)
            .ok_or(LedgerError::ArithmeticOverflow("archived closed count"))?;
        self.entry_notional = checked_add(
            self.entry_notional,
            episode.entry_notional,
            "archived entry notional",
        )?;
        self.exit_notional = checked_add(
            self.exit_notional,
            episode.exit_notional,
            "archived exit notional",
        )?;
        self.gross_pnl = checked_add(self.gross_pnl, episode.realized_pnl, "archived gross pnl")?;
        self.fees = checked_add(self.fees, episode.fees, "archived fees")?;
        self.funding = checked_add(self.funding, episode.funding, "archived funding")?;
        self.slippage = checked_add(self.slippage, episode.slippage, "archived slippage")?;
        self.net_pnl = checked_add(self.net_pnl, episode.net_pnl, "archived net pnl")?;
        if episode.net_pnl > Decimal::ZERO {
            self.gains = checked_add(self.gains, episode.net_pnl, "archived gains")?;
        } else if episode.net_pnl < Decimal::ZERO {
            self.losses = checked_add(self.losses, episode.net_pnl.abs(), "archived losses")?;
        }
        Ok(())
    }
}

fn checked_add(left: Decimal, right: Decimal, field: &'static str) -> Result<Decimal, LedgerError> {
    left.checked_add(right)
        .ok_or(LedgerError::ArithmeticOverflow(field))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DualLedger {
    schema_version: u32,
    portfolio: EpisodeBook,
    sources: BTreeMap<String, EpisodeBook>,
    #[serde(default, skip_serializing_if = "RecoveredExecutions::is_empty")]
    recovered: RecoveredExecutions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RecoveredExecutions {
    fills: Vec<crate::domain::live_trading::VerifiedExchangeFill>,
    external: Vec<crate::domain::live_trading::ExternalFillAccounting>,
    /// Append-only ownership tombstones; retained even after episode compaction.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    unattributed_episodes: BTreeSet<EpisodeId>,
}

impl RecoveredExecutions {
    fn is_empty(&self) -> bool {
        self.fills.is_empty() && self.external.is_empty() && self.unattributed_episodes.is_empty()
    }
}

impl Default for DualLedger {
    fn default() -> Self {
        Self {
            schema_version: 4,
            portfolio: EpisodeBook::default(),
            sources: BTreeMap::new(),
            recovered: RecoveredExecutions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    PositionDivergence,
    MissingFillPrice,
    MissingMarkPrice,
    ArithmeticOverflow(&'static str),
    InvalidExecution,
    Persistence(String),
    SchemaMismatch,
    InvalidLiveEvent(&'static str),
}

impl Display for LedgerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for LedgerError {}

impl DualLedger {
    pub fn recovered_verified_fills(&self) -> &[crate::domain::live_trading::VerifiedExchangeFill] {
        &self.recovered.fills
    }

    pub fn recovered_execution_stats(&self, cutoff: u64) -> Result<(usize, Decimal), LedgerError> {
        self.recovered
            .fills
            .iter()
            .map(|f| (f.occurred_at, f.filled_quantity, f.average_fill_price))
            .chain(
                self.recovered
                    .external
                    .iter()
                    .map(|e| (e.fill.occurred_at, e.fill.quantity, e.fill.price)),
            )
            .filter(|(time, _, _)| *time >= cutoff)
            .try_fold(
                (0, Decimal::ZERO),
                |(count, total), (_, quantity, price)| {
                    let total = quantity
                        .checked_mul(price)
                        .and_then(|value| total.checked_add(value))
                        .ok_or(LedgerError::ArithmeticOverflow("recovered turnover"))?;
                    Ok((count + 1, total))
                },
            )
    }

    pub fn has_recovered_fill(
        &self,
        fill: &crate::domain::live_trading::VerifiedExchangeFill,
    ) -> Result<bool, LedgerError> {
        match self
            .recovered
            .fills
            .iter()
            .find(|previous| previous.identity == fill.identity)
        {
            Some(previous) if previous != fill => {
                Err(LedgerError::InvalidLiveEvent("recovered fill changed"))
            }
            previous => Ok(previous.is_some()),
        }
    }

    pub fn has_external_fill(
        &self,
        event: &crate::domain::live_trading::ExternalFillAccounting,
    ) -> Result<bool, LedgerError> {
        match self.recovered.external.iter().find(|p| {
            p.fill.asset == event.fill.asset
                && p.fill.exchange_order_id == event.fill.exchange_order_id
                && p.fill.trade_id == event.fill.trade_id
        }) {
            Some(previous) if previous != event => {
                Err(LedgerError::InvalidLiveEvent("external receipt changed"))
            }
            previous => Ok(previous.is_some()),
        }
    }

    /// Used only when the signer proves a real engine action but its older
    /// decision snapshot lacks the pending allocation. Unknown alpha stays
    /// explicitly unattributed; no source wallet or model decision is invented.
    pub fn recover_engine_fill(
        &mut self,
        fill: &crate::domain::live_trading::VerifiedExchangeFill,
        funding_cost: Decimal,
    ) -> Result<(), LedgerError> {
        if self.has_recovered_fill(fill)? {
            return Ok(());
        }
        let mut next = self.clone();
        let mut execution = crate::domain::live_trading::execution_from_verified_fill(
            fill,
            next.portfolio_position(&fill.asset),
        )?;
        execution.funding = funding_cost;
        execution.decision_timestamp_mono = fill.occurred_at;
        // A missing allocation cannot be blended into a previously attributed
        // position. Preserve that case as reconciliation-required.
        let unknown = RECOVERED_UNATTRIBUTED_COMPONENT;
        if next
            .source_positions_for_asset(&fill.asset)
            .keys()
            .any(|id| id != unknown)
        {
            return Err(LedgerError::PositionDivergence);
        }
        next.apply_portfolio_execution(&execution, fill.occurred_at)?;
        next.apply_source_execution(unknown, &execution, fill.occurred_at)?;
        next.recovered.fills.push(fill.clone());
        *self = next;
        Ok(())
    }

    pub fn recover_external_fill(
        &mut self,
        event: &crate::domain::live_trading::ExternalFillAccounting,
        funding_cost: Decimal,
    ) -> Result<(), LedgerError> {
        if self.has_external_fill(event)? {
            return Ok(());
        }
        let mut next = self.clone();
        if !funding_cost.is_zero() {
            next.apply_portfolio_funding(&event.fill.asset, funding_cost)?;
            let positions = next.source_positions_for_asset(&event.fill.asset);
            let mut remaining = funding_cost;
            for (index, (component, quantity)) in positions.iter().enumerate() {
                let part = if index + 1 == positions.len() {
                    remaining
                } else {
                    funding_cost
                        .checked_mul(*quantity)
                        .and_then(|v| v.checked_div(event.fill.position_before))
                        .ok_or(LedgerError::ArithmeticOverflow(
                            "external funding allocation",
                        ))?
                };
                remaining = remaining
                    .checked_sub(part)
                    .ok_or(LedgerError::ArithmeticOverflow("external funding residual"))?;
                let open = next
                    .sources
                    .get_mut(component)
                    .and_then(|book| book.open.get_mut(&event.fill.asset))
                    .ok_or(LedgerError::PositionDivergence)?;
                open.funding = checked_add(open.funding, part, "external source funding")?;
            }
        }
        if next.reduce_external(&event.fill)? != event.episode_id {
            return Err(LedgerError::PositionDivergence);
        }
        next.recovered.external.push(event.clone());
        *self = next;
        Ok(())
    }

    pub fn reduce_external(
        &mut self,
        fill: &crate::domain::live_trading::ExternalReduction,
    ) -> Result<EpisodeId, LedgerError> {
        let current = self.portfolio_position(&fill.asset);
        let delta = if fill.side == Side::Buy {
            fill.quantity
        } else {
            -fill.quantity
        };
        // A single exchange fill can close an episode and open the opposite
        // side. Preserve one exchange identity and allocate its fee exactly.
        if current == fill.position_before
            && !current.is_zero()
            && current.is_sign_positive() != delta.is_sign_positive()
            && fill.quantity > current.abs()
            && fill.price > Decimal::ZERO
            && fill.fee_asset == "USDC"
        {
            let mut next = self.clone();
            let mut close = fill.clone();
            close.quantity = current.abs();
            close.fee = fill
                .fee
                .checked_mul(close.quantity)
                .and_then(|v| v.checked_div(fill.quantity))
                .ok_or(LedgerError::ArithmeticOverflow("external reversal fee"))?;
            let id = next.reduce_external(&close)?;
            let mut open = fill.clone();
            open.quantity = fill.quantity - close.quantity;
            open.fee = fill.fee - close.fee;
            open.closed_pnl = Decimal::ZERO;
            open.position_before = Decimal::ZERO;
            next.reduce_external(&open)?;
            *self = next;
            return Ok(id);
        }
        // Authenticated manual entries are economic events, with no invented
        // strategy decision. Their actual exchange startPosition must match.
        if current == fill.position_before
            && fill.quantity > Decimal::ZERO
            && fill.price > Decimal::ZERO
            && fill.fee_asset == "USDC"
            && fill.closed_pnl.is_zero()
            && (current.is_zero() || current.is_sign_positive() == delta.is_sign_positive())
        {
            let mut next = self.clone();
            let id = next.portfolio.add_external(fill, delta)?;
            next.recovered.unattributed_episodes.insert(id);
            if !next.source_positions_for_asset(&fill.asset).is_empty() {
                // Manual additions make the shared episode unattributed. Retain
                // its actual cash/position and prior closed source history; do
                // not label the added exposure as a strategy decision.
                for book in next.sources.values_mut() {
                    book.positions.remove(&fill.asset);
                    book.open.remove(&fill.asset);
                }
                let book = next
                    .sources
                    .entry(RECOVERED_UNATTRIBUTED_COMPONENT.into())
                    .or_default();
                let open = next.portfolio.open[&fill.asset].clone();
                book.positions
                    .insert(fill.asset.clone(), open.signed_quantity);
                book.open.insert(fill.asset.clone(), open);
            }
            *self = next;
            return Ok(id);
        }
        if current != fill.position_before
            || current.is_zero()
            || fill.quantity <= Decimal::ZERO
            || fill.price <= Decimal::ZERO
            || fill.quantity > current.abs()
            || current.is_sign_positive() == delta.is_sign_positive()
            || fill.fee_asset != "USDC"
        {
            return Err(LedgerError::PositionDivergence);
        }
        let mut next = self.clone();
        let id = next.portfolio.reduce_external(fill)?;
        let sources = next.source_positions_for_asset(&fill.asset);
        if !sources.is_empty() {
            let closes_all = fill.quantity == current.abs();
            let total = sources
                .values()
                .try_fold(Decimal::ZERO, |sum, q| sum.checked_add(*q))
                .ok_or(LedgerError::ArithmeticOverflow("external component total"))?;
            let residual = total
                .checked_sub(current)
                .ok_or(LedgerError::ArithmeticOverflow(
                    "external component residual",
                ))?
                .abs();
            if (total != current && !(closes_all && residual <= Decimal::new(1, 24)))
                || sources
                    .values()
                    .any(|q| q.is_sign_positive() != current.is_sign_positive())
            {
                return Err(LedgerError::PositionDivergence);
            }
            let mut quantity_left = fill.quantity;
            let mut fee_left = fill.fee;
            let mut gross_left = fill.closed_pnl;
            for (index, (component, quantity)) in sources.iter().enumerate() {
                let fraction = quantity
                    .abs()
                    .checked_div(current.abs())
                    .ok_or(LedgerError::ArithmeticOverflow("external fraction"))?;
                let mut part = fill.clone();
                part.position_before = *quantity;
                part.quantity = if closes_all {
                    quantity.abs()
                } else if index + 1 == sources.len() {
                    quantity_left
                } else {
                    fill.quantity
                        .checked_mul(fraction)
                        .ok_or(LedgerError::ArithmeticOverflow("external size allocation"))?
                };
                part.fee = if index + 1 == sources.len() {
                    fee_left
                } else {
                    fill.fee
                        .checked_mul(fraction)
                        .ok_or(LedgerError::ArithmeticOverflow("external fee allocation"))?
                };
                part.closed_pnl = if index + 1 == sources.len() {
                    gross_left
                } else {
                    fill.closed_pnl
                        .checked_mul(fraction)
                        .ok_or(LedgerError::ArithmeticOverflow("external gross allocation"))?
                };
                if !closes_all {
                    quantity_left = quantity_left
                        .checked_sub(part.quantity)
                        .filter(|residual| *residual >= Decimal::ZERO)
                        .ok_or(LedgerError::ArithmeticOverflow("external size residual"))?;
                }
                fee_left = fee_left
                    .checked_sub(part.fee)
                    .ok_or(LedgerError::ArithmeticOverflow("external fee residual"))?;
                gross_left = gross_left
                    .checked_sub(part.closed_pnl)
                    .ok_or(LedgerError::ArithmeticOverflow("external gross residual"))?;
                next.sources
                    .get_mut(component)
                    .ok_or(LedgerError::PositionDivergence)?
                    .reduce_external(&part)?;
            }
            if !closes_all && !quantity_left.is_zero() {
                return Err(LedgerError::PositionDivergence);
            }
        }
        *self = next;
        Ok(id)
    }

    pub fn reconcile_last_portfolio_gross_pnl(
        &mut self,
        asset: &str,
        exchange_realized_pnl: Decimal,
    ) -> Result<(), LedgerError> {
        let episode = self
            .portfolio
            .closed
            .last_mut()
            .filter(|episode| episode.asset == asset)
            .ok_or(LedgerError::PositionDivergence)?;
        episode.realized_pnl = exchange_realized_pnl;
        episode.net_pnl = exchange_realized_pnl
            .checked_sub(episode.fees)
            .and_then(|value| value.checked_sub(episode.funding))
            .ok_or(LedgerError::ArithmeticOverflow(
                "exchange reconciled episode net pnl",
            ))?;
        Ok(())
    }

    pub fn apply_portfolio_funding(
        &mut self,
        asset: &str,
        funding_cost: Decimal,
    ) -> Result<(), LedgerError> {
        let episode = self
            .portfolio
            .open
            .get_mut(asset)
            .ok_or(LedgerError::PositionDivergence)?;
        episode.funding = episode
            .funding
            .checked_add(funding_cost)
            .ok_or(LedgerError::ArithmeticOverflow("funding"))?;
        Ok(())
    }
    pub fn apply_portfolio_execution(
        &mut self,
        execution: &ExecutionFill,
        closed_at: u64,
    ) -> Result<Option<&PortfolioEpisode>, LedgerError> {
        let before = self.portfolio.closed.len();
        self.portfolio.apply(execution, closed_at)?;
        Ok((self.portfolio.closed.len() > before).then(|| {
            self.portfolio
                .closed
                .last()
                .expect("closed episode was appended")
        }))
    }

    pub fn apply_source_execution(
        &mut self,
        candidate_id: &str,
        execution: &ExecutionFill,
        closed_at: u64,
    ) -> Result<Option<SourceEpisode>, LedgerError> {
        let mixed = self.open_episode_unattributed(&execution.action.asset)
            || self.portfolio.closed.last().is_some_and(|episode| {
                episode.asset == execution.action.asset
                    && episode.closed_at == closed_at
                    && self.episode_unattributed(episode.episode_id)
            });
        if mixed && candidate_id != RECOVERED_UNATTRIBUTED_COMPONENT {
            return Err(LedgerError::InvalidLiveEvent(
                "unattributed episode cannot acquire source credit",
            ));
        }
        let book = self.sources.entry(candidate_id.to_string()).or_default();
        let before = book.closed.len();
        book.apply(execution, closed_at)?;
        Ok((book.closed.len() > before).then(|| {
            let episode = book.closed.last().expect("closed episode was appended");
            SourceEpisode {
                candidate_id: candidate_id.to_string(),
                source_episode_id: episode.episode_id,
                asset: episode.asset.clone(),
                opened_at: episode.opened_at,
                closed_at: episode.closed_at,
                modeled_entry: episode.entry_notional,
                modeled_exit: episode.exit_notional,
                modeled_fees: episode.fees,
                modeled_funding: episode.funding,
                modeled_slippage: episode.slippage,
                modeled_gross_pnl: episode.realized_pnl,
                attribution_residual: episode.attribution_residual,
                modeled_net_pnl: episode.net_pnl,
                net_return: if episode.entry_notional.is_zero() {
                    Decimal::ZERO
                } else {
                    episode
                        .net_pnl
                        .checked_div(episode.entry_notional)
                        .unwrap_or(Decimal::ZERO)
                },
            }
        }))
    }

    pub fn portfolio_closed(&self) -> &[PortfolioEpisode] {
        &self.portfolio.closed
    }

    pub fn episode_unattributed(&self, id: EpisodeId) -> bool {
        self.recovered.unattributed_episodes.contains(&id)
    }

    pub fn open_episode_unattributed(&self, asset: &str) -> bool {
        self.portfolio
            .open
            .get(asset)
            .is_some_and(|e| self.episode_unattributed(e.episode_id))
    }

    pub fn portfolio_closed_count(&self) -> u64 {
        self.portfolio.total_closed_count()
    }

    pub fn portfolio_archived_totals(&self) -> &BTreeMap<String, EpisodeTotals> {
        &self.portfolio.archived
    }

    pub fn portfolio_realized_net_pnl(&self) -> Result<Decimal, LedgerError> {
        self.portfolio
            .closed
            .iter()
            .try_fold(Decimal::ZERO, |sum, episode| {
                sum.checked_add(episode.net_pnl)
                    .ok_or(LedgerError::ArithmeticOverflow("realized net pnl"))
            })?
            .checked_add(self.portfolio.archived_net_pnl()?)
            .ok_or(LedgerError::ArithmeticOverflow("realized net pnl"))
    }

    pub fn source_closed_count(&self, candidate_id: &str) -> usize {
        self.sources
            .get(candidate_id)
            .map_or(0, |book| book.total_closed_count() as usize)
    }

    pub fn assign_source_attribution_residual(
        &mut self,
        candidate_id: &str,
        episode_id: EpisodeId,
        residual: Decimal,
    ) -> Result<SourceEpisode, LedgerError> {
        let episode = self
            .sources
            .get_mut(candidate_id)
            .and_then(|book| book.closed.last_mut())
            .filter(|episode| episode.episode_id == episode_id)
            .ok_or(LedgerError::PositionDivergence)?;
        episode.attribution_residual = episode
            .attribution_residual
            .checked_add(residual)
            .ok_or(LedgerError::ArithmeticOverflow("attribution residual"))?;
        episode.net_pnl =
            episode
                .net_pnl
                .checked_add(residual)
                .ok_or(LedgerError::ArithmeticOverflow(
                    "attribution residual net pnl",
                ))?;
        Ok(SourceEpisode {
            candidate_id: candidate_id.to_string(),
            source_episode_id: episode.episode_id,
            asset: episode.asset.clone(),
            opened_at: episode.opened_at,
            closed_at: episode.closed_at,
            modeled_entry: episode.entry_notional,
            modeled_exit: episode.exit_notional,
            modeled_fees: episode.fees,
            modeled_funding: episode.funding,
            modeled_slippage: episode.slippage,
            modeled_gross_pnl: episode.realized_pnl,
            attribution_residual: episode.attribution_residual,
            modeled_net_pnl: episode.net_pnl,
            net_return: if episode.entry_notional.is_zero() {
                Decimal::ZERO
            } else {
                episode
                    .net_pnl
                    .checked_div(episode.entry_notional)
                    .unwrap_or(Decimal::ZERO)
            },
        })
    }

    pub fn assign_source_episode_economic_residuals(
        &mut self,
        candidate_id: &str,
        episode_id: EpisodeId,
        gross_residual: Decimal,
        fees_residual: Decimal,
        funding_residual: Decimal,
        slippage_residual: Decimal,
    ) -> Result<SourceEpisode, LedgerError> {
        let episode = self
            .sources
            .get_mut(candidate_id)
            .and_then(|book| book.closed.last_mut())
            .filter(|episode| episode.episode_id == episode_id)
            .ok_or(LedgerError::PositionDivergence)?;
        episode.realized_pnl = episode.realized_pnl.checked_add(gross_residual).ok_or(
            LedgerError::ArithmeticOverflow("gross attribution residual"),
        )?;
        episode.fees = episode
            .fees
            .checked_add(fees_residual)
            .ok_or(LedgerError::ArithmeticOverflow("fee attribution residual"))?;
        episode.funding = episode.funding.checked_add(funding_residual).ok_or(
            LedgerError::ArithmeticOverflow("funding attribution residual"),
        )?;
        episode.slippage = episode.slippage.checked_add(slippage_residual).ok_or(
            LedgerError::ArithmeticOverflow("slippage attribution residual"),
        )?;
        episode.net_pnl = episode
            .realized_pnl
            .checked_sub(episode.fees)
            .and_then(|value| value.checked_sub(episode.funding))
            .and_then(|value| value.checked_add(episode.attribution_residual))
            .ok_or(LedgerError::ArithmeticOverflow(
                "economic attribution residual net pnl",
            ))?;
        Ok(SourceEpisode {
            candidate_id: candidate_id.to_string(),
            source_episode_id: episode.episode_id,
            asset: episode.asset.clone(),
            opened_at: episode.opened_at,
            closed_at: episode.closed_at,
            modeled_entry: episode.entry_notional,
            modeled_exit: episode.exit_notional,
            modeled_fees: episode.fees,
            modeled_funding: episode.funding,
            modeled_slippage: episode.slippage,
            modeled_gross_pnl: episode.realized_pnl,
            attribution_residual: episode.attribution_residual,
            modeled_net_pnl: episode.net_pnl,
            net_return: if episode.entry_notional.is_zero() {
                Decimal::ZERO
            } else {
                episode
                    .net_pnl
                    .checked_div(episode.entry_notional)
                    .unwrap_or(Decimal::ZERO)
            },
        })
    }

    pub fn source_position(&self, candidate_id: &str, asset: &str) -> Decimal {
        self.sources
            .get(candidate_id)
            .and_then(|book| book.positions.get(asset))
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    pub fn source_assets(&self, candidate_id: &str) -> Vec<String> {
        self.sources
            .get(candidate_id)
            .map(|book| book.positions.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn source_ids(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }

    pub fn total_source_closed(&self) -> usize {
        self.sources
            .values()
            .map(|book| book.total_closed_count() as usize)
            .sum()
    }

    /// Move closed episodes older than `cutoff` into bounded per-asset totals.
    /// Open episodes and exact recent records remain untouched.
    pub fn compact_closed_before(&mut self, cutoff: u64) -> Result<(), LedgerError> {
        self.portfolio.compact_closed_before(cutoff)?;
        for book in self.sources.values_mut() {
            book.compact_closed_before(cutoff)?;
        }
        Ok(())
    }

    pub fn portfolio_open_count(&self) -> usize {
        self.portfolio.open.len()
    }

    pub fn portfolio_open_episode_id(&self, asset: &str) -> Option<String> {
        self.portfolio.open.get(asset).map(|episode| {
            episode
                .episode_id
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        })
    }

    pub fn portfolio_opened_at(&self, asset: &str) -> Option<u64> {
        self.portfolio
            .open
            .get(asset)
            .map(|episode| episode.opened_at)
    }

    pub fn portfolio_open_state(&self, asset: &str) -> Option<OpenPositionState> {
        let episode = self.portfolio.open.get(asset)?;
        Some(OpenPositionState {
            episode_id: episode
                .episode_id
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            opened_at: episode.opened_at,
            signed_quantity: episode.signed_quantity,
            average_entry_price: episode.average_entry_price,
            realized_pnl: episode.realized_pnl,
        })
    }

    pub fn all_source_closed(&self) -> Vec<SourceEpisode> {
        let mut episodes = Vec::new();
        for (candidate_id, book) in &self.sources {
            for episode in &book.closed {
                episodes.push(SourceEpisode {
                    candidate_id: candidate_id.clone(),
                    source_episode_id: episode.episode_id,
                    asset: episode.asset.clone(),
                    opened_at: episode.opened_at,
                    closed_at: episode.closed_at,
                    modeled_entry: episode.entry_notional,
                    modeled_exit: episode.exit_notional,
                    modeled_fees: episode.fees,
                    modeled_funding: episode.funding,
                    modeled_slippage: episode.slippage,
                    modeled_gross_pnl: episode.realized_pnl,
                    attribution_residual: episode.attribution_residual,
                    modeled_net_pnl: episode.net_pnl,
                    net_return: if episode.entry_notional.is_zero() {
                        Decimal::ZERO
                    } else {
                        episode
                            .net_pnl
                            .checked_div(episode.entry_notional)
                            .unwrap_or_default()
                    },
                });
            }
        }
        episodes
    }

    pub fn source_positions_for_asset(&self, asset: &str) -> BTreeMap<String, Decimal> {
        self.sources
            .iter()
            .filter_map(|(candidate, book)| {
                book.positions
                    .get(asset)
                    .copied()
                    .filter(|position| !position.is_zero())
                    .map(|position| (candidate.clone(), position))
            })
            .collect()
    }

    pub fn portfolio_equity(
        &self,
        starting_equity: Decimal,
        marks: &BTreeMap<String, Decimal>,
        unbooked_funding: Decimal,
    ) -> Result<Decimal, LedgerError> {
        let mut equity = starting_equity
            .checked_add(self.portfolio.archived_net_pnl()?)
            .ok_or(LedgerError::ArithmeticOverflow("archived closed equity"))?;
        for episode in &self.portfolio.closed {
            equity = equity
                .checked_add(episode.net_pnl)
                .ok_or(LedgerError::ArithmeticOverflow("closed equity"))?;
        }
        for (asset, open) in &self.portfolio.open {
            let mark = marks.get(asset).ok_or(LedgerError::MissingMarkPrice)?;
            let price_move = if open.signed_quantity.is_sign_positive() {
                mark.checked_sub(open.average_entry_price)
            } else {
                open.average_entry_price.checked_sub(*mark)
            }
            .ok_or(LedgerError::ArithmeticOverflow("unrealized pnl"))?;
            let unrealized = price_move
                .checked_mul(open.signed_quantity.abs())
                .ok_or(LedgerError::ArithmeticOverflow("unrealized pnl"))?;
            equity = equity
                .checked_add(unrealized)
                .and_then(|value| value.checked_sub(open.fees))
                .and_then(|value| value.checked_sub(open.funding))
                .ok_or(LedgerError::ArithmeticOverflow("open equity"))?;
        }
        equity
            .checked_sub(unbooked_funding)
            .ok_or(LedgerError::ArithmeticOverflow("unbooked funding"))
    }

    pub fn portfolio_position(&self, asset: &str) -> Decimal {
        self.portfolio
            .positions
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    pub fn portfolio_assets(&self) -> Vec<String> {
        self.portfolio.positions.keys().cloned().collect()
    }

    pub fn save_atomic(&self, path: impl AsRef<Path>) -> Result<(), LedgerError> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| LedgerError::Persistence("ledger path has no parent".to_string()))?;
        std::fs::create_dir_all(parent)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec(self)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        std::fs::write(&temporary, bytes)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        std::fs::rename(&temporary, path)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let bytes =
            std::fs::read(path).map_err(|error| LedgerError::Persistence(error.to_string()))?;
        let mut ledger = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        ledger.migrate_cash_accounting()?;
        ledger.migrate_ownership(&[]);
        Ok(ledger)
    }

    pub(crate) fn migrate_ownership(
        &mut self,
        external: &[crate::domain::live_trading::ExternalFillAccounting],
    ) {
        if self.schema_version >= 4 {
            return;
        }
        self.recovered.unattributed_episodes.extend(
            self.portfolio
                .open
                .values()
                .filter(|e| e.opening_decision_id == DecisionId([0; 32]))
                .map(|e| e.episode_id),
        );
        self.recovered.unattributed_episodes.extend(
            self.portfolio
                .closed
                .iter()
                .filter(|e| e.opening_decision_id == DecisionId([0; 32]))
                .map(|e| e.episode_id),
        );
        self.recovered.unattributed_episodes.extend(
            external
                .iter()
                .chain(&self.recovered.external)
                .filter(|e| {
                    e.fill.position_before.is_zero()
                        || (e.fill.side == Side::Buy) == e.fill.position_before.is_sign_positive()
                })
                .map(|e| e.episode_id),
        );
        self.schema_version = 4;
    }

    pub fn validate_integrity(&self) -> Result<(), LedgerError> {
        if !matches!(self.schema_version, 2..=4) {
            return Err(LedgerError::SchemaMismatch);
        }
        self.validate()
    }

    /// Version 2 charged reference slippage twice. Preserve every episode and
    /// its diagnostic, changing only the derived cash result after validation.
    pub fn migrate_cash_accounting(&mut self) -> Result<bool, LedgerError> {
        self.validate_integrity()?;
        if self.schema_version >= 3 {
            return Ok(false);
        }
        let mut next = self.clone();
        for book in std::iter::once(&mut next.portfolio).chain(next.sources.values_mut()) {
            // Aggregated winners/losers cannot be faithfully reclassified
            // without their original episodes. Never invent replacement PF.
            if book
                .archived
                .values()
                .any(|totals| !totals.slippage.is_zero())
            {
                return Err(LedgerError::InvalidLiveEvent(
                    "legacy archived cash requires fill replay",
                ));
            }
            for episode in &mut book.closed {
                episode.net_pnl = checked_add(episode.net_pnl, episode.slippage, "cash migration")?;
            }
        }
        next.schema_version = 3;
        *self = next;
        Ok(true)
    }

    fn validate(&self) -> Result<(), LedgerError> {
        if self.schema_version >= 4
            && self.recovered.external.iter().any(|event| {
                (event.fill.position_before.is_zero()
                    || (event.fill.side == Side::Buy)
                        == event.fill.position_before.is_sign_positive())
                    && !self.episode_unattributed(event.episode_id)
            })
        {
            return Err(LedgerError::InvalidLiveEvent(
                "external ownership tombstone missing",
            ));
        }
        for (id, decision) in self
            .portfolio
            .open
            .values()
            .map(|e| (e.episode_id, e.opening_decision_id))
            .chain(
                self.portfolio
                    .closed
                    .iter()
                    .map(|e| (e.episode_id, e.opening_decision_id)),
            )
        {
            if self.episode_unattributed(id) && decision != DecisionId([0; 32]) {
                return Err(LedgerError::InvalidLiveEvent(
                    "unattributed episode reclaimed",
                ));
            }
        }
        for book in std::iter::once(&self.portfolio).chain(self.sources.values()) {
            for (asset, position) in &book.positions {
                let open_quantity = book
                    .open
                    .get(asset)
                    .map_or(Decimal::ZERO, |episode| episode.signed_quantity);
                if *position != open_quantity {
                    return Err(LedgerError::PositionDivergence);
                }
            }
            for (asset, episode) in &book.open {
                if book.positions.get(asset).copied() != Some(episode.signed_quantity) {
                    return Err(LedgerError::PositionDivergence);
                }
            }
        }
        Ok(())
    }

    pub fn source_equity(
        &self,
        candidate_id: &str,
        starting_equity: Decimal,
        marks: &BTreeMap<String, Decimal>,
    ) -> Result<Decimal, LedgerError> {
        match self.sources.get(candidate_id) {
            Some(book) => book.equity(starting_equity, marks),
            None => Ok(starting_equity),
        }
    }
}

impl EpisodeBook {
    fn add_external(
        &mut self,
        fill: &crate::domain::live_trading::ExternalReduction,
        delta: Decimal,
    ) -> Result<EpisodeId, LedgerError> {
        let value = fill
            .quantity
            .checked_mul(fill.price)
            .ok_or(LedgerError::ArithmeticOverflow("external entry value"))?;
        let id = if let Some(open) = self.open.get_mut(&fill.asset) {
            open.opening_decision_id = DecisionId([0; 32]);
            let after = checked_add(open.signed_quantity, delta, "external position")?;
            open.average_entry_price = open
                .average_entry_price
                .checked_mul(open.signed_quantity.abs())
                .and_then(|v| v.checked_add(value))
                .and_then(|v| v.checked_div(after.abs()))
                .ok_or(LedgerError::ArithmeticOverflow("external average entry"))?;
            open.signed_quantity = after;
            open.entry_notional = checked_add(open.entry_notional, value, "external entry")?;
            open.fees = checked_add(open.fees, fill.fee, "external fee")?;
            open.episode_id
        } else {
            let mut hash = Sha256::new();
            hash.update(b"HL1G/EXTERNAL_EPISODE/V1");
            for text in [&fill.asset, &fill.exchange_order_id, &fill.trade_id] {
                hash.update((text.len() as u64).to_be_bytes());
                hash.update(text.as_bytes());
            }
            let id = EpisodeId(hash.finalize().into());
            self.open.insert(
                fill.asset.clone(),
                OpenEpisode {
                    episode_id: id,
                    asset: fill.asset.clone(),
                    opened_at: fill.occurred_at,
                    // Explicit absent-strategy marker, never an authorized action.
                    opening_decision_id: DecisionId([0; 32]),
                    signed_quantity: delta,
                    average_entry_price: fill.price,
                    entry_notional: value,
                    exit_notional: Decimal::ZERO,
                    realized_pnl: Decimal::ZERO,
                    fees: fill.fee,
                    funding: Decimal::ZERO,
                    slippage: Decimal::ZERO,
                },
            );
            id
        };
        self.positions
            .insert(fill.asset.clone(), self.open[&fill.asset].signed_quantity);
        Ok(id)
    }

    fn reduce_external(
        &mut self,
        fill: &crate::domain::live_trading::ExternalReduction,
    ) -> Result<EpisodeId, LedgerError> {
        let open = self
            .open
            .get_mut(&fill.asset)
            .ok_or(LedgerError::PositionDivergence)?;
        if open.signed_quantity != fill.position_before
            || fill.quantity > open.signed_quantity.abs()
        {
            return Err(LedgerError::PositionDivergence);
        }
        let id = open.episode_id;
        open.realized_pnl = checked_add(open.realized_pnl, fill.closed_pnl, "external gross")?;
        open.exit_notional = checked_add(
            open.exit_notional,
            fill.quantity
                .checked_mul(fill.price)
                .ok_or(LedgerError::ArithmeticOverflow("external notional"))?,
            "external exit",
        )?;
        open.fees = checked_add(open.fees, fill.fee, "external fee")?;
        open.signed_quantity = checked_add(
            open.signed_quantity,
            if fill.side == Side::Buy {
                fill.quantity
            } else {
                -fill.quantity
            },
            "external position",
        )?;
        let after = open.signed_quantity;
        if after.is_zero() {
            self.finish_episode(&fill.asset, fill.occurred_at)?;
            self.positions.remove(&fill.asset);
        } else {
            self.positions.insert(fill.asset.clone(), after);
        }
        Ok(id)
    }

    fn total_closed_count(&self) -> u64 {
        self.archived
            .values()
            .fold(self.closed.len() as u64, |count, totals| {
                count.saturating_add(totals.closed_count)
            })
    }

    fn archived_net_pnl(&self) -> Result<Decimal, LedgerError> {
        self.archived
            .values()
            .try_fold(Decimal::ZERO, |sum, totals| {
                checked_add(sum, totals.net_pnl, "archived net pnl")
            })
    }

    fn compact_closed_before(&mut self, cutoff: u64) -> Result<(), LedgerError> {
        let split = self
            .closed
            .partition_point(|episode| episode.closed_at < cutoff);
        for episode in self.closed.drain(..split) {
            self.archived
                .entry(episode.asset.clone())
                .or_default()
                .add_episode(&episode)?;
        }
        Ok(())
    }

    fn equity(
        &self,
        starting_equity: Decimal,
        marks: &BTreeMap<String, Decimal>,
    ) -> Result<Decimal, LedgerError> {
        let mut equity = starting_equity
            .checked_add(self.archived_net_pnl()?)
            .ok_or(LedgerError::ArithmeticOverflow("archived book equity"))?;
        for episode in &self.closed {
            equity = equity
                .checked_add(episode.net_pnl)
                .ok_or(LedgerError::ArithmeticOverflow("book equity"))?;
        }
        for (asset, open) in &self.open {
            let mark = marks.get(asset).ok_or(LedgerError::MissingMarkPrice)?;
            let movement = if open.signed_quantity.is_sign_positive() {
                mark.checked_sub(open.average_entry_price)
            } else {
                open.average_entry_price.checked_sub(*mark)
            }
            .ok_or(LedgerError::ArithmeticOverflow("book unrealized pnl"))?;
            equity = equity
                .checked_add(
                    movement
                        .checked_mul(open.signed_quantity.abs())
                        .ok_or(LedgerError::ArithmeticOverflow("book unrealized pnl"))?,
                )
                .and_then(|value| value.checked_sub(open.fees))
                .and_then(|value| value.checked_sub(open.funding))
                .ok_or(LedgerError::ArithmeticOverflow("book open equity"))?;
        }
        Ok(equity)
    }
}

impl EpisodeBook {
    fn apply(&mut self, execution: &ExecutionFill, closed_at: u64) -> Result<(), LedgerError> {
        let asset = &execution.action.asset;
        let current = self.positions.get(asset).copied().unwrap_or(Decimal::ZERO);
        if current != execution.position_before {
            return Err(LedgerError::PositionDivergence);
        }
        if execution.modeled_filled_quantity.is_zero() {
            if execution.position_after != current {
                return Err(LedgerError::PositionDivergence);
            }
            return Ok(());
        }
        let price = execution
            .modeled_average_fill_price
            .ok_or(LedgerError::MissingFillPrice)?;
        let signed_delta = match execution.action.side {
            Side::Buy => execution.modeled_filled_quantity,
            Side::Sell => -execution.modeled_filled_quantity,
        };
        let calculated_after = current
            .checked_add(signed_delta)
            .ok_or(LedgerError::ArithmeticOverflow("position"))?;
        if calculated_after != execution.position_after {
            return Err(LedgerError::PositionDivergence);
        }

        if current.is_zero() || current.is_sign_positive() == signed_delta.is_sign_positive() {
            self.add_same_direction(execution, price, signed_delta)?;
        } else {
            self.close_or_flip(execution, price, signed_delta, closed_at)?;
        }
        if calculated_after.is_zero() {
            self.positions.remove(asset);
        } else {
            self.positions.insert(asset.clone(), calculated_after);
        }
        Ok(())
    }

    fn add_same_direction(
        &mut self,
        execution: &ExecutionFill,
        price: Decimal,
        signed_delta: Decimal,
    ) -> Result<(), LedgerError> {
        let asset = execution.action.asset.clone();
        if let Some(open) = self.open.get_mut(&asset) {
            let old_quantity = open.signed_quantity.abs();
            let added = signed_delta.abs();
            let total = old_quantity
                .checked_add(added)
                .ok_or(LedgerError::ArithmeticOverflow("episode quantity"))?;
            let old_value = old_quantity
                .checked_mul(open.average_entry_price)
                .ok_or(LedgerError::ArithmeticOverflow("entry value"))?;
            let added_value = added
                .checked_mul(price)
                .ok_or(LedgerError::ArithmeticOverflow("entry value"))?;
            open.average_entry_price = old_value
                .checked_add(added_value)
                .and_then(|value| value.checked_div(total))
                .ok_or(LedgerError::ArithmeticOverflow("average entry"))?;
            open.signed_quantity = open
                .signed_quantity
                .checked_add(signed_delta)
                .ok_or(LedgerError::ArithmeticOverflow("episode quantity"))?;
            open.entry_notional = open
                .entry_notional
                .checked_add(added_value)
                .ok_or(LedgerError::ArithmeticOverflow("entry notional"))?;
            add_costs(open, execution, Decimal::ONE)?;
        } else {
            self.open_new(execution, price, signed_delta, Decimal::ONE)?;
        }
        Ok(())
    }

    fn close_or_flip(
        &mut self,
        execution: &ExecutionFill,
        price: Decimal,
        signed_delta: Decimal,
        closed_at: u64,
    ) -> Result<(), LedgerError> {
        let asset = execution.action.asset.clone();
        let fill_quantity = signed_delta.abs();
        let current_quantity = self
            .open
            .get(&asset)
            .ok_or(LedgerError::PositionDivergence)?
            .signed_quantity
            .abs();
        let closing_quantity = fill_quantity.min(current_quantity);
        let closing_fraction = closing_quantity
            .checked_div(fill_quantity)
            .ok_or(LedgerError::ArithmeticOverflow("closing fraction"))?;
        {
            let open = self
                .open
                .get_mut(&asset)
                .ok_or(LedgerError::PositionDivergence)?;
            let pnl_per_unit = if open.signed_quantity.is_sign_positive() {
                price.checked_sub(open.average_entry_price)
            } else {
                open.average_entry_price.checked_sub(price)
            }
            .ok_or(LedgerError::ArithmeticOverflow("realized pnl"))?;
            open.realized_pnl = open
                .realized_pnl
                .checked_add(
                    pnl_per_unit
                        .checked_mul(closing_quantity)
                        .ok_or(LedgerError::ArithmeticOverflow("realized pnl"))?,
                )
                .ok_or(LedgerError::ArithmeticOverflow("realized pnl"))?;
            open.exit_notional = open
                .exit_notional
                .checked_add(
                    price
                        .checked_mul(closing_quantity)
                        .ok_or(LedgerError::ArithmeticOverflow("exit notional"))?,
                )
                .ok_or(LedgerError::ArithmeticOverflow("exit notional"))?;
            add_costs(open, execution, closing_fraction)?;
            let signed_close = if open.signed_quantity.is_sign_positive() {
                -closing_quantity
            } else {
                closing_quantity
            };
            open.signed_quantity = open
                .signed_quantity
                .checked_add(signed_close)
                .ok_or(LedgerError::ArithmeticOverflow("episode close"))?;
        }
        if self
            .open
            .get(&asset)
            .is_some_and(|open| open.signed_quantity.is_zero())
        {
            self.finish_episode(&asset, closed_at)?;
        }
        let residual = fill_quantity
            .checked_sub(closing_quantity)
            .ok_or(LedgerError::ArithmeticOverflow("flip residual"))?;
        if !residual.is_zero() {
            let residual_fraction = Decimal::ONE
                .checked_sub(closing_fraction)
                .ok_or(LedgerError::ArithmeticOverflow("residual fraction"))?;
            let residual_signed = if signed_delta.is_sign_positive() {
                residual
            } else {
                -residual
            };
            self.open_new(execution, price, residual_signed, residual_fraction)?;
        }
        Ok(())
    }

    fn open_new(
        &mut self,
        execution: &ExecutionFill,
        price: Decimal,
        signed_quantity: Decimal,
        cost_fraction: Decimal,
    ) -> Result<(), LedgerError> {
        let sequence = self.next_episode_sequence;
        self.next_episode_sequence = sequence
            .checked_add(1)
            .ok_or(LedgerError::ArithmeticOverflow("episode sequence"))?;
        let quantity = signed_quantity.abs();
        let mut open = OpenEpisode {
            episode_id: derive_episode_id(
                &execution.action.asset,
                execution.action.decision_id,
                execution.action.planned_cloid,
                sequence,
            ),
            asset: execution.action.asset.clone(),
            opened_at: execution.decision_timestamp_mono,
            opening_decision_id: execution.action.decision_id,
            signed_quantity,
            average_entry_price: price,
            entry_notional: quantity
                .checked_mul(price)
                .ok_or(LedgerError::ArithmeticOverflow("entry notional"))?,
            exit_notional: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            slippage: Decimal::ZERO,
        };
        add_costs(&mut open, execution, cost_fraction)?;
        self.open.insert(execution.action.asset.clone(), open);
        Ok(())
    }

    fn finish_episode(&mut self, asset: &str, closed_at: u64) -> Result<(), LedgerError> {
        let open = self
            .open
            .remove(asset)
            .ok_or(LedgerError::PositionDivergence)?;
        let net_pnl = open
            .realized_pnl
            .checked_sub(open.fees)
            .and_then(|value| value.checked_sub(open.funding))
            .ok_or(LedgerError::ArithmeticOverflow("net pnl"))?;
        self.closed.push(PortfolioEpisode {
            episode_id: open.episode_id,
            asset: open.asset,
            opened_at: open.opened_at,
            closed_at,
            opening_decision_id: open.opening_decision_id,
            entry_notional: open.entry_notional,
            exit_notional: open.exit_notional,
            realized_pnl: open.realized_pnl,
            fees: open.fees,
            funding: open.funding,
            slippage: open.slippage,
            attribution_residual: Decimal::ZERO,
            net_pnl,
        });
        Ok(())
    }
}

fn add_costs(
    open: &mut OpenEpisode,
    execution: &ExecutionFill,
    fraction: Decimal,
) -> Result<(), LedgerError> {
    open.fees = add_fraction(open.fees, execution.fees, fraction, "fees")?;
    open.funding = add_fraction(open.funding, execution.funding, fraction, "funding")?;
    open.slippage = add_fraction(open.slippage, execution.slippage, fraction, "slippage")?;
    Ok(())
}

fn add_fraction(
    current: Decimal,
    value: Decimal,
    fraction: Decimal,
    name: &'static str,
) -> Result<Decimal, LedgerError> {
    current
        .checked_add(
            value
                .checked_mul(fraction)
                .ok_or(LedgerError::ArithmeticOverflow(name))?,
        )
        .ok_or(LedgerError::ArithmeticOverflow(name))
}

fn derive_episode_id(
    asset: &str,
    decision_id: DecisionId,
    cloid: PlannedCloid,
    sequence: u64,
) -> EpisodeId {
    let mut hash = Sha256::new();
    hash.update(b"HL1G/EPISODE/V1");
    hash.update((asset.len() as u32).to_be_bytes());
    hash.update(asset.as_bytes());
    hash.update(decision_id.0);
    hash.update(cloid.0);
    hash.update(sequence.to_be_bytes());
    EpisodeId(hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::{MarketSnapshotId, PlannedAction, PlannedCloid, TargetVersion};
    use crate::domain::ioc::{ExecutionFillId, LatencyScenario};
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn execution(side: Side, quantity: &str, price: &str, before: &str) -> ExecutionFill {
        let quantity = d(quantity);
        let price = d(price);
        let before = d(before);
        let signed = if side == Side::Buy {
            quantity
        } else {
            -quantity
        };
        ExecutionFill {
            execution_id: ExecutionFillId([3; 32]),
            action: PlannedAction {
                decision_id: DecisionId([1; 32]),
                target_version: TargetVersion(0),
                asset: "BTC".to_string(),
                side,
                rounded_notional: quantity * price,
                reduce_only: false,
                action_ordinal: 0,
                retry_generation: 0,
                planned_cloid: PlannedCloid([2; 16]),
            },
            decision_timestamp_mono: 1,
            decision_market_snapshot_id: MarketSnapshotId([1; 32]),
            evaluation_market_snapshot_id: MarketSnapshotId([2; 32]),
            latency_scenario: LatencyScenario::Expected,
            configured_latency_ms: 1,
            proposed_limit_price: price,
            rounded_quantity: quantity,
            modeled_filled_quantity: quantity,
            modeled_average_fill_price: Some(price),
            unfilled_ioc_remainder: Decimal::ZERO,
            modeled_filled_notional: quantity * price,
            fees: d("1"),
            funding: d("0.5"),
            slippage: d("0.25"),
            position_before: before,
            position_after: before + signed,
        }
    }

    #[test]
    fn flat_open_close_creates_one_net_episode_not_fill_samples() {
        let mut ledger = DualLedger::default();
        assert!(ledger
            .apply_portfolio_execution(&execution(Side::Buy, "2", "100", "0"), 2)
            .unwrap()
            .is_none());
        let episode = ledger
            .apply_portfolio_execution(&execution(Side::Sell, "2", "110", "2"), 3)
            .unwrap()
            .unwrap();
        assert_eq!(episode.realized_pnl, d("20"));
        assert_eq!(episode.fees, d("2"));
        assert_eq!(episode.funding, d("1.0"));
        assert_eq!(episode.slippage, d("0.50"));
        assert_eq!(episode.net_pnl, d("17.0"));
        assert_eq!(ledger.portfolio_closed().len(), 1);
    }

    #[test]
    fn compacted_closed_episode_preserves_equity_counts_and_exact_totals() {
        let mut ledger = DualLedger::default();
        ledger
            .apply_portfolio_execution(&execution(Side::Buy, "2", "100", "0"), 2)
            .unwrap();
        ledger
            .apply_portfolio_execution(&execution(Side::Sell, "2", "110", "2"), 3)
            .unwrap();
        let realized = ledger.portfolio_realized_net_pnl().unwrap();
        let equity = ledger
            .portfolio_equity(d("100"), &BTreeMap::new(), Decimal::ZERO)
            .unwrap();

        ledger.compact_closed_before(4).unwrap();

        assert!(ledger.portfolio_closed().is_empty());
        assert_eq!(ledger.portfolio_closed_count(), 1);
        assert_eq!(ledger.portfolio_realized_net_pnl().unwrap(), realized);
        assert_eq!(
            ledger
                .portfolio_equity(d("100"), &BTreeMap::new(), Decimal::ZERO)
                .unwrap(),
            equity
        );
        let totals = &ledger.portfolio_archived_totals()["BTC"];
        assert_eq!(totals.closed_count, 1);
        assert_eq!(totals.gross_pnl, d("20"));
        assert_eq!(totals.net_pnl, d("17.0"));
    }

    #[test]
    fn direction_flip_closes_previous_and_opens_residual_episode() {
        let mut ledger = DualLedger::default();
        ledger
            .apply_portfolio_execution(&execution(Side::Buy, "1", "100", "0"), 1)
            .unwrap();
        ledger
            .apply_portfolio_execution(&execution(Side::Sell, "3", "90", "1"), 2)
            .unwrap();
        assert_eq!(ledger.portfolio_closed().len(), 1);
        assert_eq!(ledger.portfolio_position("BTC"), d("-2"));
    }

    #[test]
    fn source_books_are_independent_from_portfolio_book() {
        let mut ledger = DualLedger::default();
        ledger
            .apply_source_execution("source-a", &execution(Side::Buy, "1", "100", "0"), 1)
            .unwrap();
        let closed = ledger
            .apply_source_execution("source-a", &execution(Side::Sell, "1", "110", "1"), 2)
            .unwrap()
            .unwrap();
        assert_eq!(ledger.source_closed_count("source-a"), 1);
        assert_eq!(ledger.portfolio_closed().len(), 0);
        assert!(closed.net_return > Decimal::ZERO);
    }

    #[test]
    fn external_full_close_uses_exact_repeating_decimal_component_quantities() {
        let first = "0.4101938475726400486994657338";
        let second = "1.78980615242735995130053427";
        let mut ledger = DualLedger::default();
        ledger
            .apply_portfolio_execution(&execution(Side::Buy, "2.2", "1", "0"), 1)
            .unwrap();
        ledger
            .apply_source_execution("source-a", &execution(Side::Buy, first, "1", "0"), 1)
            .unwrap();
        ledger
            .apply_source_execution("source-b", &execution(Side::Buy, second, "1", "0"), 1)
            .unwrap();

        ledger
            .reduce_external(&crate::domain::live_trading::ExternalReduction {
                exchange_order_id: "external-order".into(),
                trade_id: "external-trade".into(),
                asset: "BTC".into(),
                side: Side::Sell,
                quantity: d("2.2"),
                price: d("1.1"),
                fee: d("0.01"),
                fee_asset: "USDC".into(),
                closed_pnl: d("0.22"),
                position_before: d("2.2"),
                occurred_at: 2,
            })
            .unwrap();

        assert_eq!(ledger.portfolio_position("BTC"), Decimal::ZERO);
        assert!(ledger.source_positions_for_asset("BTC").is_empty());
    }

    #[test]
    fn synthetic_component_books_are_discoverable_for_equity_accounting() {
        let mut ledger = DualLedger::default();
        ledger
            .apply_source_execution(
                "technical:trend_pullback:trending",
                &execution(Side::Buy, "1", "100", "0"),
                1,
            )
            .unwrap();

        assert_eq!(
            ledger.source_ids(),
            vec!["technical:trend_pullback:trending".to_string()]
        );
    }

    #[test]
    fn source_attribution_residual_is_explicit_and_changes_only_net_pnl() {
        let mut ledger = DualLedger::default();
        ledger
            .apply_source_execution("source-a", &execution(Side::Buy, "1", "100", "0"), 1)
            .unwrap();
        let before = ledger
            .apply_source_execution("source-a", &execution(Side::Sell, "1", "110", "1"), 2)
            .unwrap()
            .unwrap();
        let residual = Decimal::new(1, 28);
        let after = ledger
            .assign_source_attribution_residual("source-a", before.source_episode_id, residual)
            .unwrap();
        assert_eq!(after.attribution_residual, residual);
        assert_eq!(after.modeled_net_pnl, before.modeled_net_pnl + residual);
        assert_eq!(after.modeled_gross_pnl, before.modeled_gross_pnl);
        assert_eq!(after.modeled_fees, before.modeled_fees);
        assert_eq!(after.modeled_funding, before.modeled_funding);
        assert_eq!(after.modeled_slippage, before.modeled_slippage);
    }

    #[test]
    fn persistence_round_trip_is_exact_and_corruption_fails_closed() {
        let mut ledger = DualLedger::default();
        ledger
            .apply_portfolio_execution(&execution(Side::Buy, "1", "100", "0"), 1)
            .unwrap();
        let path = std::env::temp_dir().join(format!("hl1g-ledger-{}.json", std::process::id()));
        ledger.save_atomic(&path).unwrap();
        assert_eq!(ledger, DualLedger::load(&path).unwrap());
        std::fs::write(&path, b"corrupt").unwrap();
        assert!(DualLedger::load(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
