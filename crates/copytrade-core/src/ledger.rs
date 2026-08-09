use crate::decision::{DecisionId, PlannedCloid, Side};
use crate::shadow::ShadowExecution;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::Path;

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct EpisodeBook {
    positions: BTreeMap<String, Decimal>,
    open: BTreeMap<String, OpenEpisode>,
    closed: Vec<PortfolioEpisode>,
    next_episode_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DualLedger {
    schema_version: u32,
    portfolio: EpisodeBook,
    sources: BTreeMap<String, EpisodeBook>,
}

impl Default for DualLedger {
    fn default() -> Self {
        Self {
            schema_version: 2,
            portfolio: EpisodeBook::default(),
            sources: BTreeMap::new(),
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
            .and_then(|value| value.checked_sub(episode.slippage))
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
        execution: &ShadowExecution,
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
        execution: &ShadowExecution,
        closed_at: u64,
    ) -> Result<Option<SourceEpisode>, LedgerError> {
        let book = self.sources.entry(candidate_id.to_string()).or_default();
        let before = book.closed.len();
        book.apply(execution, closed_at)?;
        Ok((book.closed.len() > before).then(|| {
            let episode = book.closed.last().expect("closed episode was appended");
            SourceEpisode {
                candidate_id: candidate_id.to_string(),
                source_episode_id: episode.episode_id,
                asset: episode.asset.clone(),
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

    pub fn portfolio_realized_net_pnl(&self) -> Result<Decimal, LedgerError> {
        self.portfolio
            .closed
            .iter()
            .try_fold(Decimal::ZERO, |sum, episode| {
                sum.checked_add(episode.net_pnl)
                    .ok_or(LedgerError::ArithmeticOverflow("realized net pnl"))
            })
    }

    pub fn source_closed_count(&self, candidate_id: &str) -> usize {
        self.sources
            .get(candidate_id)
            .map_or(0, |book| book.closed.len())
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
            .and_then(|value| value.checked_sub(episode.slippage))
            .and_then(|value| value.checked_add(episode.attribution_residual))
            .ok_or(LedgerError::ArithmeticOverflow(
                "economic attribution residual net pnl",
            ))?;
        Ok(SourceEpisode {
            candidate_id: candidate_id.to_string(),
            source_episode_id: episode.episode_id,
            asset: episode.asset.clone(),
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
        self.sources.values().map(|book| book.closed.len()).sum()
    }

    pub fn portfolio_open_count(&self) -> usize {
        self.portfolio.open.len()
    }

    pub fn all_source_closed(&self) -> Vec<SourceEpisode> {
        let mut episodes = Vec::new();
        for (candidate_id, book) in &self.sources {
            for episode in &book.closed {
                episodes.push(SourceEpisode {
                    candidate_id: candidate_id.clone(),
                    source_episode_id: episode.episode_id,
                    asset: episode.asset.clone(),
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
        let mut equity = starting_equity;
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
                .and_then(|value| value.checked_sub(open.slippage))
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
        let ledger = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| LedgerError::Persistence(error.to_string()))?;
        if ledger.schema_version != 2 {
            return Err(LedgerError::SchemaMismatch);
        }
        ledger.validate()?;
        Ok(ledger)
    }

    pub fn validate_integrity(&self) -> Result<(), LedgerError> {
        if self.schema_version != 2 {
            return Err(LedgerError::SchemaMismatch);
        }
        self.validate()
    }

    fn validate(&self) -> Result<(), LedgerError> {
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
    fn equity(
        &self,
        starting_equity: Decimal,
        marks: &BTreeMap<String, Decimal>,
    ) -> Result<Decimal, LedgerError> {
        let mut equity = starting_equity;
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
                .and_then(|value| value.checked_sub(open.slippage))
                .ok_or(LedgerError::ArithmeticOverflow("book open equity"))?;
        }
        Ok(equity)
    }
}

impl EpisodeBook {
    fn apply(&mut self, execution: &ShadowExecution, closed_at: u64) -> Result<(), LedgerError> {
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
        execution: &ShadowExecution,
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
        execution: &ShadowExecution,
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
        execution: &ShadowExecution,
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
            .and_then(|value| value.checked_sub(open.slippage))
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
    execution: &ShadowExecution,
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
    use crate::decision::{MarketSnapshotId, PlannedAction, PlannedCloid, TargetVersion};
    use crate::shadow::{LatencyScenario, ShadowExecutionId};
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn execution(side: Side, quantity: &str, price: &str, before: &str) -> ShadowExecution {
        let quantity = d(quantity);
        let price = d(price);
        let before = d(before);
        let signed = if side == Side::Buy {
            quantity
        } else {
            -quantity
        };
        ShadowExecution {
            shadow_execution_id: ShadowExecutionId([3; 32]),
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
        assert_eq!(episode.net_pnl, d("16.50"));
        assert_eq!(ledger.portfolio_closed().len(), 1);
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
