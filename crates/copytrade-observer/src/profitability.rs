use crate::live_shadow::{EconomicAttribution, EquityReturnBucket, ShadowActionAccounting};
use copytrade_core::ledger::{DualLedger, PortfolioEpisode, SourceEpisode};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct ProfitabilitySummary {
    pub realized_pnl_scope: &'static str,
    pub turnover_scope: &'static str,
    pub settled_equity_scope: &'static str,
    pub total_net_pnl_formula: &'static str,
    pub execution_slippage_sign_convention: &'static str,
    pub all_action_cost_scope: &'static str,
    pub starting_equity: Decimal,
    pub ending_equity: Decimal,
    pub equity_change_including_open_positions: Decimal,
    pub ending_settled_equity: Decimal,
    pub settled_equity_growth: Decimal,
    pub settled_equity_return_on_starting_equity: Decimal,
    pub gross_turnover: Decimal,
    pub gross_turnover_multiple_on_starting_equity: Decimal,
    pub total_gross_pnl: Decimal,
    pub total_net_pnl: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    pub execution_slippage: Decimal,
    pub all_action_fees: Decimal,
    pub all_action_funding: Decimal,
    pub all_action_slippage: Decimal,
    pub shadow_executions: usize,
    pub closed_portfolio_episodes: usize,
    pub closed_source_episodes: usize,
    pub win_rate: Decimal,
    pub profit_factor: Option<Decimal>,
    pub maximum_drawdown: Decimal,
    pub pnl_by_asset: BTreeMap<String, Decimal>,
    pub pnl_by_source: BTreeMap<String, Decimal>,
    pub execution_net_pnl_by_attribution: BTreeMap<EconomicAttribution, Decimal>,
    pub settled_equity_return_by_attribution: BTreeMap<EconomicAttribution, Decimal>,
    pub maximum_asset_pnl_concentration: Decimal,
    pub maximum_source_pnl_concentration: Decimal,
    pub portfolio_annualized_sharpe_5m: f64,
    pub source_annualized_sharpe_5m: BTreeMap<String, f64>,
    pub five_minute_bucket_count: usize,
    pub zero_return_bucket_count: usize,
    pub bucket_structure_verified: bool,
    pub episode_cost_formula_verified: bool,
    pub portfolio_source_books_reconcile: bool,
    pub engineering_measurement_complete: bool,
    pub positive_edge_signal: bool,
}

pub fn summarize_profitability(
    starting_equity: Decimal,
    executions: &[ShadowActionAccounting],
    buckets: &[EquityReturnBucket],
    ledger: &DualLedger,
) -> Result<ProfitabilitySummary, String> {
    let episodes = ledger.portfolio_closed();
    let source_episodes = ledger.all_source_closed();
    let ending_equity = buckets
        .last()
        .map_or(starting_equity, |bucket| bucket.ending_equity);
    let equity_change_including_open_positions = ending_equity
        .checked_sub(starting_equity)
        .ok_or("equity change overflow")?;
    let ending_settled_equity = executions
        .last()
        .map_or(starting_equity, |execution| execution.settled_equity);
    let settled_equity_growth = ending_settled_equity
        .checked_sub(starting_equity)
        .ok_or("settled equity growth overflow")?;
    let settled_equity_return_on_starting_equity = if starting_equity.is_zero() {
        Decimal::ZERO
    } else {
        settled_equity_growth
            .checked_div(starting_equity)
            .ok_or("settled equity return overflow")?
    };
    let gross_turnover = sum(executions.iter().map(|execution| execution.filled_notional))?;
    let gross_turnover_multiple_on_starting_equity = if starting_equity.is_zero() {
        Decimal::ZERO
    } else {
        gross_turnover
            .checked_div(starting_equity)
            .ok_or("turnover multiple overflow")?
    };
    let all_action_fees = sum(executions.iter().map(|execution| execution.fees))?;
    let all_action_funding = sum(executions.iter().map(|execution| execution.funding))?;
    let all_action_slippage = sum(executions
        .iter()
        .map(|execution| execution.execution_slippage))?;
    // Qualified profitability is realized closed-episode performance only.
    // Open mark-to-market remains visible through ending equity and bucket
    // returns, but it cannot make the profitability signal positive.
    let total_gross_pnl = sum(episodes.iter().map(|episode| episode.realized_pnl))?;
    let total_net_pnl = sum(episodes.iter().map(|episode| episode.net_pnl))?;
    let fees = sum(episodes.iter().map(|episode| episode.fees))?;
    let funding = sum(episodes.iter().map(|episode| episode.funding))?;
    let execution_slippage = sum(episodes.iter().map(|episode| episode.slippage))?;
    let wins = episodes
        .iter()
        .filter(|episode| episode.net_pnl > Decimal::ZERO)
        .count();
    let win_rate = if episodes.is_empty() {
        Decimal::ZERO
    } else {
        Decimal::from(wins as u64)
            .checked_div(Decimal::from(episodes.len() as u64))
            .ok_or("win rate overflow")?
    };
    let gains = sum(episodes
        .iter()
        .filter(|episode| episode.net_pnl > Decimal::ZERO)
        .map(|episode| episode.net_pnl))?;
    let losses = sum(episodes
        .iter()
        .filter(|episode| episode.net_pnl < Decimal::ZERO)
        .map(|episode| episode.net_pnl.abs()))?;
    let profit_factor = (!losses.is_zero())
        .then(|| gains.checked_div(losses).ok_or("profit factor overflow"))
        .transpose()?;
    let pnl_by_asset = group_portfolio(episodes)?;
    let pnl_by_source = group_sources(&source_episodes)?;
    let mut execution_net_pnl_by_attribution = BTreeMap::new();
    for execution in executions {
        let entry = execution_net_pnl_by_attribution
            .entry(execution.economic_attribution)
            .or_insert(Decimal::ZERO);
        *entry = entry
            .checked_add(execution.net_pnl_delta)
            .ok_or("execution attribution PnL overflow")?;
    }
    let settled_equity_return_by_attribution = execution_net_pnl_by_attribution
        .iter()
        .map(|(attribution, pnl)| {
            Ok((
                *attribution,
                if starting_equity.is_zero() {
                    Decimal::ZERO
                } else {
                    pnl.checked_div(starting_equity)
                        .ok_or("execution attribution return overflow")?
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let episode_cost_formula_verified = episodes.iter().all(cost_formula_holds);
    let portfolio_closed_net = sum(episodes.iter().map(|episode| episode.net_pnl))?;
    let source_closed_net = sum(source_episodes
        .iter()
        .map(|episode| episode.modeled_net_pnl))?;
    let portfolio_source_books_reconcile = portfolio_closed_net == source_closed_net;
    let portfolio_returns = buckets
        .iter()
        .map(|bucket| bucket.return_fraction)
        .collect::<Vec<_>>();
    let mut source_returns: BTreeMap<String, Vec<Decimal>> = BTreeMap::new();
    for bucket in buckets {
        for (candidate, value) in &bucket.source_returns {
            source_returns
                .entry(candidate.clone())
                .or_default()
                .push(*value);
        }
    }
    let source_annualized_sharpe_5m = source_returns
        .into_iter()
        .map(|(candidate, returns)| (candidate, annualized_sharpe_5m(&returns)))
        .collect();
    let maximum_drawdown = maximum_drawdown(buckets)?;
    let maximum_asset_pnl_concentration = maximum_concentration(&pnl_by_asset)?;
    let maximum_source_pnl_concentration = maximum_concentration(&pnl_by_source)?;
    let bucket_structure_verified = verify_bucket_structure(buckets);
    let engineering_measurement_complete = !executions.is_empty()
        && !episodes.is_empty()
        && !source_episodes.is_empty()
        && episode_cost_formula_verified
        && portfolio_source_books_reconcile
        && bucket_structure_verified
        && buckets.len() == 48;
    let positive_edge_signal = engineering_measurement_complete
        && total_net_pnl > Decimal::ZERO
        && profit_factor.is_some_and(|value| value > Decimal::ONE);
    Ok(ProfitabilitySummary {
        realized_pnl_scope: "closed_portfolio_episodes",
        turnover_scope: "all_modeled_execution_filled_notional_in_window",
        settled_equity_scope: "realized_execution_state_only_excluding_open_mark_to_market",
        total_net_pnl_formula:
            "total_gross_pnl - fees - funding - execution_slippage = total_net_pnl",
        execution_slippage_sign_convention:
            "signed_cost_subtracted_from_gross; negative values improve total_net_pnl",
        all_action_cost_scope: "all_modeled_executions_including_open_positions",
        starting_equity,
        ending_equity,
        equity_change_including_open_positions,
        ending_settled_equity,
        settled_equity_growth,
        settled_equity_return_on_starting_equity,
        gross_turnover,
        gross_turnover_multiple_on_starting_equity,
        total_gross_pnl,
        total_net_pnl,
        fees,
        funding,
        execution_slippage,
        all_action_fees,
        all_action_funding,
        all_action_slippage,
        shadow_executions: executions.len(),
        closed_portfolio_episodes: episodes.len(),
        closed_source_episodes: source_episodes.len(),
        win_rate,
        profit_factor,
        maximum_drawdown,
        pnl_by_asset,
        pnl_by_source,
        execution_net_pnl_by_attribution,
        settled_equity_return_by_attribution,
        maximum_asset_pnl_concentration,
        maximum_source_pnl_concentration,
        portfolio_annualized_sharpe_5m: annualized_sharpe_5m(&portfolio_returns),
        source_annualized_sharpe_5m,
        five_minute_bucket_count: buckets.len(),
        zero_return_bucket_count: portfolio_returns
            .iter()
            .filter(|value| value.is_zero())
            .count(),
        bucket_structure_verified,
        episode_cost_formula_verified,
        portfolio_source_books_reconcile,
        engineering_measurement_complete,
        positive_edge_signal,
    })
}

fn verify_bucket_structure(buckets: &[EquityReturnBucket]) -> bool {
    let mut ids = std::collections::BTreeSet::new();
    let run_id = buckets.first().map(|bucket| bucket.run_id.as_str());
    buckets.iter().enumerate().all(|(index, bucket)| {
        Some(bucket.run_id.as_str()) == run_id
            && bucket.bucket_index == index as u64
            && bucket.bucket_id == format!("{}:{}", bucket.run_id, bucket.bucket_index)
            && ids.insert(bucket.bucket_id.clone())
            && bucket.opened_at_mono < bucket.closed_at_mono
            && (index == 0 || buckets[index - 1].closed_at_mono == bucket.opened_at_mono)
    })
}

fn cost_formula_holds(episode: &PortfolioEpisode) -> bool {
    episode
        .realized_pnl
        .checked_sub(episode.fees)
        .and_then(|value| value.checked_sub(episode.funding))
        .and_then(|value| value.checked_sub(episode.slippage))
        == Some(episode.net_pnl)
}

fn group_portfolio(episodes: &[PortfolioEpisode]) -> Result<BTreeMap<String, Decimal>, String> {
    let mut grouped: BTreeMap<String, Decimal> = BTreeMap::new();
    for episode in episodes {
        let entry = grouped.entry(episode.asset.clone()).or_default();
        *entry = entry
            .checked_add(episode.net_pnl)
            .ok_or("asset PnL overflow")?;
    }
    Ok(grouped)
}

fn group_sources(episodes: &[SourceEpisode]) -> Result<BTreeMap<String, Decimal>, String> {
    let mut grouped: BTreeMap<String, Decimal> = BTreeMap::new();
    for episode in episodes {
        let entry = grouped.entry(episode.candidate_id.clone()).or_default();
        *entry = entry
            .checked_add(episode.modeled_net_pnl)
            .ok_or("source PnL overflow")?;
    }
    Ok(grouped)
}

fn maximum_concentration(values: &BTreeMap<String, Decimal>) -> Result<Decimal, String> {
    let total = sum(values.values().map(|value| value.abs()))?;
    if total.is_zero() {
        return Ok(Decimal::ZERO);
    }
    values
        .values()
        .map(|value| {
            value
                .abs()
                .checked_div(total)
                .ok_or("concentration overflow".into())
        })
        .collect::<Result<Vec<_>, String>>()?
        .into_iter()
        .max()
        .ok_or_else(|| "missing concentration".into())
}

fn maximum_drawdown(buckets: &[EquityReturnBucket]) -> Result<Decimal, String> {
    let Some(first) = buckets.first() else {
        return Ok(Decimal::ZERO);
    };
    let mut peak = first.starting_equity;
    let mut maximum = Decimal::ZERO;
    for bucket in buckets {
        peak = peak.max(bucket.ending_equity);
        if !peak.is_zero() {
            let drawdown = peak
                .checked_sub(bucket.ending_equity)
                .and_then(|value| value.checked_div(peak))
                .ok_or("drawdown overflow")?;
            maximum = maximum.max(drawdown);
        }
    }
    Ok(maximum)
}

fn annualized_sharpe_5m(returns: &[Decimal]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let values = returns
        .iter()
        .filter_map(ToPrimitive::to_f64)
        .collect::<Vec<_>>();
    if values.len() != returns.len() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    let deviation = variance.sqrt();
    if deviation == 0.0 {
        0.0
    } else {
        mean / deviation * (365.0_f64 * 24.0 * 12.0).sqrt()
    }
}

fn sum(mut values: impl Iterator<Item = Decimal>) -> Result<Decimal, String> {
    values.try_fold(Decimal::ZERO, |sum, value| {
        sum.checked_add(value).ok_or_else(|| "sum overflow".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::decision::{
        DecisionId, MarketSnapshotId, PlannedAction, PlannedCloid, Side, TargetVersion,
    };
    use copytrade_core::shadow::{LatencyScenario, ShadowExecution, ShadowExecutionId};

    #[test]
    fn zero_trade_buckets_remain_in_sharpe_sample() {
        let buckets = (0..48)
            .map(|index| EquityReturnBucket {
                run_id: "test-run".to_string(),
                bucket_id: format!("test-run:{index}"),
                bucket_index: index,
                opened_at_mono: index * 300_000,
                closed_at_mono: (index + 1) * 300_000,
                starting_equity: Decimal::from(1_000),
                ending_equity: Decimal::from(1_000),
                return_fraction: Decimal::ZERO,
                source_returns: [("source".to_string(), Decimal::ZERO)]
                    .into_iter()
                    .collect(),
            })
            .collect::<Vec<_>>();
        let summary =
            summarize_profitability(Decimal::from(1_000), &[], &buckets, &DualLedger::default())
                .unwrap();
        assert_eq!(summary.five_minute_bucket_count, 48);
        assert!(summary.bucket_structure_verified);
        assert_eq!(buckets.last().unwrap().closed_at_mono, 14_400_000);
        assert_eq!(summary.ending_equity, buckets.last().unwrap().ending_equity);
        assert_eq!(summary.zero_return_bucket_count, 48);
        assert_eq!(summary.portfolio_annualized_sharpe_5m, 0.0);
        assert_eq!(summary.source_annualized_sharpe_5m["source"], 0.0);
        assert!(!summary.engineering_measurement_complete);
    }

    #[test]
    fn early_stop_is_structurally_valid_but_cannot_complete_the_four_hour_gate() {
        let buckets = (0..12)
            .map(|index| EquityReturnBucket {
                run_id: "early-stop".to_string(),
                bucket_id: format!("early-stop:{index}"),
                bucket_index: index,
                opened_at_mono: index * 300_000,
                closed_at_mono: (index + 1) * 300_000,
                starting_equity: Decimal::from(1_000),
                ending_equity: Decimal::from(1_000),
                return_fraction: Decimal::ZERO,
                source_returns: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        let summary =
            summarize_profitability(Decimal::from(1_000), &[], &buckets, &DualLedger::default())
                .unwrap();
        assert!(summary.bucket_structure_verified);
        assert_eq!(summary.five_minute_bucket_count, 12);
        assert!(!summary.engineering_measurement_complete);
    }

    #[test]
    fn duplicate_deadline_bucket_is_rejected() {
        let mut buckets = (0..48)
            .map(|index| EquityReturnBucket {
                run_id: "duplicate".to_string(),
                bucket_id: format!("duplicate:{index}"),
                bucket_index: index,
                opened_at_mono: index * 300_000,
                closed_at_mono: (index + 1) * 300_000,
                starting_equity: Decimal::from(1_000),
                ending_equity: Decimal::from(1_000),
                return_fraction: Decimal::ZERO,
                source_returns: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        buckets.push(EquityReturnBucket {
            run_id: "duplicate".to_string(),
            bucket_id: "duplicate:48".to_string(),
            bucket_index: 48,
            opened_at_mono: 14_400_000,
            closed_at_mono: 14_400_000,
            starting_equity: Decimal::from(1_000),
            ending_equity: Decimal::from(1_000),
            return_fraction: Decimal::ZERO,
            source_returns: BTreeMap::new(),
        });
        assert!(!verify_bucket_structure(&buckets));
    }

    #[test]
    fn open_mark_to_market_cannot_become_qualified_realized_profit() {
        let action = PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            asset: "BTC".to_string(),
            side: Side::Buy,
            rounded_notional: Decimal::from(100),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([2; 16]),
        };
        let execution = ShadowExecution {
            shadow_execution_id: ShadowExecutionId([3; 32]),
            action,
            decision_timestamp_mono: 0,
            decision_market_snapshot_id: MarketSnapshotId([4; 32]),
            evaluation_market_snapshot_id: MarketSnapshotId([5; 32]),
            latency_scenario: LatencyScenario::Expected,
            configured_latency_ms: 200,
            proposed_limit_price: Decimal::from(100),
            rounded_quantity: Decimal::ONE,
            modeled_filled_quantity: Decimal::ONE,
            modeled_average_fill_price: Some(Decimal::from(100)),
            unfilled_ioc_remainder: Decimal::ZERO,
            modeled_filled_notional: Decimal::from(100),
            fees: Decimal::ONE,
            funding: Decimal::ZERO,
            slippage: Decimal::from(2),
            position_before: Decimal::ZERO,
            position_after: Decimal::ONE,
        };
        let mut ledger = DualLedger::default();
        ledger.apply_portfolio_execution(&execution, 1).unwrap();
        let buckets = vec![EquityReturnBucket {
            run_id: "open-mtm".to_string(),
            bucket_id: "open-mtm:0".to_string(),
            bucket_index: 0,
            opened_at_mono: 0,
            closed_at_mono: 300_000,
            starting_equity: Decimal::from(1_000),
            ending_equity: Decimal::from(1_010),
            return_fraction: Decimal::new(1, 2),
            source_returns: BTreeMap::new(),
        }];
        let accounting = ShadowActionAccounting {
            shadow_execution_id: "shadow".to_string(),
            decision_id: "decision".to_string(),
            asset: "BTC".to_string(),
            side: "buy".to_string(),
            execution_mode: "marketable_ioc_complete_visible_depth".to_string(),
            root_planned_cloid: "root".to_string(),
            parent_planned_cloid: None,
            retry_generation: 0,
            decision_timestamp_mono: 0,
            evaluation_timestamp_mono: 200,
            decision_midpoint: Decimal::from(100),
            modeled_ioc_limit: Decimal::from(100),
            worst_required_depth_price: Some(Decimal::from(100)),
            visible_executable_quantity: Decimal::ONE,
            requested_quantity: Decimal::ONE,
            filled_quantity: Decimal::ONE,
            unfilled_quantity: Decimal::ZERO,
            average_fill_price: Some(Decimal::from(100)),
            filled_notional: Decimal::from(100),
            fees: Decimal::ONE,
            funding: Decimal::ZERO,
            execution_slippage: Decimal::from(2),
            gross_pnl_delta: Decimal::ZERO,
            net_pnl_delta: Decimal::from(-3),
            portfolio_equity_return: Decimal::ZERO,
            current_equity: Decimal::from(997),
            settled_equity: Decimal::from(997),
            deployment_equity: Decimal::from(997),
            source_attributed_returns: BTreeMap::new(),
            position_before: Decimal::ZERO,
            position_after: Decimal::ONE,
            component_close_quantities: BTreeMap::new(),
            component_open_quantities: BTreeMap::new(),
            source_filled_quantities: BTreeMap::new(),
            economic_attribution: EconomicAttribution::TechnicalOnly,
        };
        let summary =
            summarize_profitability(Decimal::from(1_000), &[accounting], &buckets, &ledger)
                .unwrap();
        assert_eq!(
            summary.equity_change_including_open_positions,
            Decimal::from(10)
        );
        assert_eq!(summary.total_net_pnl, Decimal::ZERO);
        assert_eq!(summary.fees, Decimal::ZERO);
        assert_eq!(summary.all_action_fees, Decimal::ONE);
        assert_eq!(summary.gross_turnover, Decimal::from(100));
        assert_eq!(
            summary.execution_net_pnl_by_attribution[&EconomicAttribution::TechnicalOnly],
            Decimal::from(-3)
        );
        assert!(!summary.positive_edge_signal);
    }
}
