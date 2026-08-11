use crate::profitability::exact_decimal_sum;
use crate::qualification_evidence::{RunHeader, TerminalSummary};
use copytrade_core::ledger::{EpisodeId, PortfolioEpisode, SourceEpisode};
use copytrade_core::release::ReleaseManifest;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const AGGREGATE_SCHEMA_VERSION: u32 = 1;
const FOUR_HOURS_SECONDS: u64 = 14_400;
const FIVE_MINUTES_MS: u64 = 300_000;
const ONE_HOUR_BUCKETS: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateEquityBucket {
    run_id: String,
    bucket_id: String,
    bucket_index: u64,
    opened_at_mono: u64,
    closed_at_mono: u64,
    starting_equity: Decimal,
    ending_equity: Decimal,
    return_fraction: Decimal,
    source_returns: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WindowProfitability {
    #[serde(default)]
    realized_pnl_scope: String,
    #[serde(default)]
    total_net_pnl_formula: String,
    #[serde(default)]
    execution_slippage_sign_convention: String,
    #[serde(default)]
    all_action_cost_scope: String,
    starting_equity: Decimal,
    ending_equity: Decimal,
    equity_change_including_open_positions: Decimal,
    total_net_pnl: Decimal,
    fees: Decimal,
    funding: Decimal,
    execution_slippage: Decimal,
    all_action_fees: Decimal,
    all_action_funding: Decimal,
    all_action_slippage: Decimal,
    shadow_executions: usize,
    closed_portfolio_episodes: usize,
    closed_source_episodes: usize,
    five_minute_bucket_count: usize,
    bucket_structure_verified: bool,
    episode_cost_formula_verified: bool,
    portfolio_source_books_reconcile: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WindowActionCost {
    fees: Decimal,
    funding: Decimal,
    #[serde(rename = "execution_slippage")]
    slippage: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProfitabilityIdentity {
    frozen_identity_sha256: String,
    source_tree_sha256: String,
    observer_binary_sha256: String,
    configuration_sha256: String,
    risk_policy_sha256: String,
    release_manifest_sha256: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct WindowDensity {
    #[serde(default)]
    root_actionable_targets: usize,
    #[serde(default)]
    unresolved_actionable_targets: usize,
    #[serde(default)]
    root_conservation_verified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowRecord {
    schema_version: u32,
    run_id: String,
    bundle_sha256: String,
    identity: ProfitabilityIdentity,
    start_wall_clock_utc_ms: u64,
    expected_duration_seconds: u64,
    monotonic_elapsed_seconds: u64,
    completed_four_hours: bool,
    evidence_verified: bool,
    unsigned: bool,
    planned_only: bool,
    submission_capable: bool,
    api_wallet_present: bool,
    risk_invariant_violations: u64,
    mutation_requests: u64,
    key_file_opens: u64,
    operational_gate_passed: bool,
    accounting_gate_passed: bool,
    profitability_signal_positive: bool,
    portfolio_episodes: Vec<PortfolioEpisode>,
    source_episodes: Vec<SourceEpisode>,
    equity_buckets: Vec<AggregateEquityBucket>,
    profitability: Option<WindowProfitability>,
    all_action_fees: Decimal,
    all_action_funding: Decimal,
    all_action_slippage: Decimal,
    density: WindowDensity,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct AttributionSummary {
    closed_episodes: usize,
    modeled_net_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize)]
pub struct RailwayAggregateSummary {
    schema_version: u32,
    profitability_identity: Option<ProfitabilityIdentity>,
    total_windows: usize,
    completed_four_hour_windows: usize,
    incomplete_windows: usize,
    completed_observation_seconds: u64,
    minimum_observation_seconds: u64,
    minimum_duration_met: bool,
    independent_closed_episodes: usize,
    minimum_closed_episodes: usize,
    minimum_closed_episodes_met: bool,
    record_complete: bool,
    cumulative_net_pnl: Decimal,
    profit_factor: Option<Decimal>,
    realized_fees: Decimal,
    realized_funding: Decimal,
    realized_slippage: Decimal,
    all_action_fees: Decimal,
    all_action_funding: Decimal,
    all_action_slippage: Decimal,
    one_hour_return_count: usize,
    one_hour_annualized_sharpe: f64,
    maximum_drawdown: Decimal,
    pnl_by_asset: BTreeMap<String, Decimal>,
    episode_count_by_asset: BTreeMap<String, usize>,
    maximum_asset_pnl_concentration: Decimal,
    maximum_asset_episode_share: Decimal,
    source_attribution: AttributionSummary,
    technical_attribution: AttributionSummary,
    source_technical_reconciliation_difference: Decimal,
    technical_archetype_attribution: BTreeMap<String, AttributionSummary>,
    technical_regime_attribution: BTreeMap<String, AttributionSummary>,
    technical_regime_count: usize,
    root_actionable_targets: usize,
    unresolved_roots: usize,
    root_conservation_incidents: usize,
    risk_invariant_violations: u64,
    mutation_requests: u64,
    key_file_opens: u64,
    portfolio_accounting_incidents: usize,
    source_attribution_reconciliation_incidents: usize,
    unverified_evidence_windows: usize,
    unsafe_runtime_windows: usize,
}

pub fn aggregate_railway_window(
    bundle: &Path,
    aggregate_root: &Path,
    frozen_identity_sha256: &str,
    minimum_observation_seconds: u64,
    minimum_closed_episodes: usize,
) -> Result<RailwayAggregateSummary, Box<dyn Error>> {
    if minimum_observation_seconds < 7 * 24 * 60 * 60 {
        return Err("aggregate minimum observation duration must be at least seven days".into());
    }
    if minimum_closed_episodes < 100 {
        return Err("aggregate minimum closed episodes must be at least 100".into());
    }
    let record = load_window_record(bundle, frozen_identity_sha256)?;
    aggregate_window_record(
        record,
        aggregate_root,
        minimum_observation_seconds,
        minimum_closed_episodes,
    )
}

fn aggregate_window_record(
    record: WindowRecord,
    aggregate_root: &Path,
    minimum_observation_seconds: u64,
    minimum_closed_episodes: usize,
) -> Result<RailwayAggregateSummary, Box<dyn Error>> {
    let windows = aggregate_root.join("windows");
    std::fs::create_dir_all(&windows)?;
    sync_directory(aggregate_root)?;
    let record_path = windows.join(format!("{}.json", record.run_id));
    let record_bytes = pretty_json(&record)?;
    let mut records = load_window_records(&windows)?;
    let existing = records
        .iter()
        .find(|existing| existing.run_id == record.run_id)
        .cloned();
    if let Some(existing) = &existing {
        if existing != &record || std::fs::read(&record_path)? != record_bytes {
            return Err(
                format!("aggregate window identity collision for {}", record.run_id).into(),
            );
        }
    } else {
        records.push(record.clone());
        sort_window_records(&mut records);
    }

    // Validate identity, interval, evidence, and accounting invariants against
    // the prospective aggregate before making this candidate durable. A bad
    // candidate must not poison every later aggregate attempt.
    let summary = summarize(
        &records,
        minimum_observation_seconds,
        minimum_closed_episodes,
    )?;
    if existing.is_none() {
        write_atomic(&record_path, &record_bytes)?;
    }
    write_atomic(
        &aggregate_root.join("summary.json"),
        &pretty_json(&summary)?,
    )?;
    Ok(summary)
}

fn load_window_record(
    bundle: &Path,
    frozen_identity_sha256: &str,
) -> Result<WindowRecord, Box<dyn Error>> {
    let header: RunHeader = read_json(&bundle.join("run-header.json"))?;
    let release_manifest_path = bundle.join("release-manifest.json");
    let release_manifest: ReleaseManifest = read_json(&release_manifest_path)?;
    if !is_sha256(frozen_identity_sha256)
        || !is_safe_run_id(&header.run_id)
        || bundle.file_name().and_then(|name| name.to_str()) != Some(header.run_id.as_str())
        || header.stage != "HL1-PROFITABILITY-4H"
        || header.expected_minimum_duration_seconds != FOUR_HOURS_SECONDS
    {
        return Err("aggregate accepts only identified four-hour profitability windows".into());
    }
    if normalize_sha256(&release_manifest.source_tree_sha256)?
        != normalize_sha256(&header.source_tree_sha256)?
        || normalize_sha256(&release_manifest.binary_sha256)?
            != normalize_sha256(&header.binary_sha256)?
        || normalize_sha256(&release_manifest.configuration_sha256)?
            != normalize_sha256(&header.configuration_sha256)?
        || normalize_sha256(&release_manifest.risk_policy_sha256)?
            != normalize_sha256(&header.risk_policy_sha256)?
    {
        return Err("window release manifest does not match its run header".into());
    }
    let terminal_path = bundle.join("terminal-summary.json");
    let terminal: Option<TerminalSummary> = terminal_path
        .exists()
        .then(|| read_json(&terminal_path))
        .transpose()?;
    if terminal
        .as_ref()
        .is_some_and(|summary| summary.stage != header.stage)
    {
        return Err("window header and terminal stage mismatch".into());
    }
    let portfolio_episodes: Vec<PortfolioEpisode> =
        read_json_or_default(&bundle.join("portfolio-episodes.json"))?;
    let source_episodes: Vec<SourceEpisode> =
        read_json_or_default(&bundle.join("source-episodes.json"))?;
    let equity_buckets: Vec<AggregateEquityBucket> =
        read_json_or_default(&bundle.join("five-minute-return-buckets.json"))?;
    let profitability_path = bundle.join("profitability-summary.json");
    let profitability: Option<WindowProfitability> = profitability_path
        .exists()
        .then(|| read_json(&profitability_path))
        .transpose()?;
    let action_costs: Vec<WindowActionCost> =
        read_json_or_default(&bundle.join("shadow-actions.json"))?;
    let all_action_fees = decimal_sum(action_costs.iter().map(|action| action.fees))?;
    let all_action_funding = decimal_sum(action_costs.iter().map(|action| action.funding))?;
    let all_action_slippage = decimal_sum(action_costs.iter().map(|action| action.slippage))?;
    if profitability.as_ref().is_some_and(|summary| {
        summary.all_action_fees != all_action_fees
            || summary.all_action_funding != all_action_funding
            || summary.all_action_slippage != all_action_slippage
    }) {
        return Err("window action-cost summary mismatch".into());
    }
    let density_path = bundle.join("executable-density-summary.json");
    let density = if density_path.exists() {
        read_json(&density_path)?
    } else {
        WindowDensity::default()
    };
    let evidence_verified = terminal.as_ref().is_some_and(|summary| {
        summary.evidence_verified
            && summary.event_chain_verified
            && summary.isolation_pre_run_passed
            && summary.isolation_post_run_passed
    });
    let elapsed = terminal
        .as_ref()
        .map_or(0, |summary| summary.monotonic_elapsed_seconds);
    let completed_four_hours = evidence_verified
        && terminal.as_ref().is_some_and(|summary| {
            summary.unsigned
                && summary.planned_only
                && !summary.submission_capable
                && !summary.api_wallet_present
        })
        && elapsed >= header.expected_minimum_duration_seconds
        && profitability.as_ref().is_some_and(|summary| {
            summary.five_minute_bucket_count == 48
                && summary.bucket_structure_verified
                && equity_buckets.len() == 48
        });
    let terminal = terminal.unwrap_or_else(|| TerminalSummary {
        stage: header.stage.clone(),
        passed: false,
        failure_reason: Some("missing_terminal_summary".into()),
        secondary_reason: None,
        unsigned: header.unsigned,
        planned_only: header.planned_only,
        submission_capable: header.submission_capable,
        api_wallet_present: header.api_wallet_present,
        candidate_count: header.candidate_count,
        monotonic_elapsed_seconds: 0,
        process_restarts: 0,
        risk_invariant_violations: 0,
        mutation_requests: 0,
        key_file_opens: 0,
        closed_shadow_episodes: 0,
        closed_source_episodes: 0,
        event_chain_verified: false,
        all_candidates_scheduled: false,
        unreconciled_shadow_intents: 0,
        isolation_pre_run_passed: false,
        isolation_post_run_passed: false,
        evidence_verified: false,
        profitability_gate_passed: false,
        operational_gate_passed: false,
        accounting_gate_passed: false,
        profitability_signal_positive: false,
        sharpe_target_proven: false,
    });
    Ok(WindowRecord {
        schema_version: AGGREGATE_SCHEMA_VERSION,
        run_id: header.run_id,
        bundle_sha256: hash_bundle(bundle)?,
        identity: ProfitabilityIdentity {
            frozen_identity_sha256: normalize_sha256(frozen_identity_sha256)?,
            source_tree_sha256: normalize_sha256(&header.source_tree_sha256)?,
            observer_binary_sha256: normalize_sha256(&header.binary_sha256)?,
            configuration_sha256: normalize_sha256(&header.configuration_sha256)?,
            risk_policy_sha256: normalize_sha256(&header.risk_policy_sha256)?,
            release_manifest_sha256: hash_file(&release_manifest_path)?,
        },
        start_wall_clock_utc_ms: header.start_wall_clock_utc_ms,
        expected_duration_seconds: header.expected_minimum_duration_seconds,
        monotonic_elapsed_seconds: terminal.monotonic_elapsed_seconds,
        completed_four_hours,
        evidence_verified,
        unsigned: terminal.unsigned,
        planned_only: terminal.planned_only,
        submission_capable: terminal.submission_capable,
        api_wallet_present: terminal.api_wallet_present,
        risk_invariant_violations: terminal.risk_invariant_violations,
        mutation_requests: terminal.mutation_requests,
        key_file_opens: terminal.key_file_opens,
        operational_gate_passed: terminal.operational_gate_passed,
        accounting_gate_passed: terminal.accounting_gate_passed,
        profitability_signal_positive: terminal.profitability_signal_positive,
        portfolio_episodes,
        source_episodes,
        equity_buckets,
        profitability,
        all_action_fees,
        all_action_funding,
        all_action_slippage,
        density,
    })
}

fn load_window_records(root: &Path) -> Result<Vec<WindowRecord>, Box<dyn Error>> {
    let mut paths = std::fs::read_dir(root)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    let mut records = paths
        .into_iter()
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .map(|path| read_json(&path))
        .collect::<Result<Vec<WindowRecord>, _>>()?;
    sort_window_records(&mut records);
    let mut run_ids = BTreeSet::new();
    if records.iter().any(|record| {
        record.schema_version != AGGREGATE_SCHEMA_VERSION || !run_ids.insert(record.run_id.clone())
    }) {
        return Err("invalid or duplicate aggregate window record".into());
    }
    Ok(records)
}

fn sort_window_records(records: &mut [WindowRecord]) {
    records.sort_by(|left, right| {
        (left.start_wall_clock_utc_ms, left.run_id.as_str())
            .cmp(&(right.start_wall_clock_utc_ms, right.run_id.as_str()))
    });
}

fn summarize(
    records: &[WindowRecord],
    minimum_observation_seconds: u64,
    minimum_closed_episodes: usize,
) -> Result<RailwayAggregateSummary, Box<dyn Error>> {
    let profitability_identity = records.first().map(|record| record.identity.clone());
    if let Some(expected) = &profitability_identity {
        if records.iter().any(|record| &record.identity != expected) {
            return Err("aggregate contains more than one profitability identity".into());
        }
    }
    validate_verified_window_buckets(records)?;
    for pair in records.windows(2) {
        let previous_end = pair[0]
            .start_wall_clock_utc_ms
            .checked_add(
                pair[0]
                    .monotonic_elapsed_seconds
                    .checked_mul(1_000)
                    .ok_or("aggregate window interval overflow")?,
            )
            .ok_or("aggregate window interval overflow")?;
        if pair[1].start_wall_clock_utc_ms < previous_end {
            return Err(format!(
                "aggregate windows overlap: {} and {}",
                pair[0].run_id, pair[1].run_id
            )
            .into());
        }
    }
    let completed_four_hour_windows = records
        .iter()
        .filter(|record| record.completed_four_hours)
        .count();
    let completed_observation_seconds = records
        .iter()
        .filter(|record| record.completed_four_hours)
        .try_fold(0_u64, |sum, record| {
            sum.checked_add(record.expected_duration_seconds)
                .ok_or("aggregate observation duration overflow")
        })?;
    let mut portfolio = BTreeMap::<EpisodeId, PortfolioEpisode>::new();
    let mut sources = BTreeMap::<(String, EpisodeId), SourceEpisode>::new();
    for record in records.iter().filter(|record| {
        record.evidence_verified
            && record.unsigned
            && record.planned_only
            && !record.submission_capable
            && !record.api_wallet_present
    }) {
        for episode in &record.portfolio_episodes {
            insert_identical(&mut portfolio, episode.episode_id, episode.clone())?;
        }
        for episode in &record.source_episodes {
            insert_identical(
                &mut sources,
                (episode.candidate_id.clone(), episode.source_episode_id),
                episode.clone(),
            )?;
        }
    }
    let cumulative_net_pnl = decimal_sum(portfolio.values().map(|episode| episode.net_pnl))?;
    let gains = decimal_sum(
        portfolio
            .values()
            .filter(|episode| episode.net_pnl > Decimal::ZERO)
            .map(|episode| episode.net_pnl),
    )?;
    let losses = decimal_sum(
        portfolio
            .values()
            .filter(|episode| episode.net_pnl < Decimal::ZERO)
            .map(|episode| episode.net_pnl.abs()),
    )?;
    let profit_factor = if losses.is_zero() {
        None
    } else {
        Some(
            gains
                .checked_div(losses)
                .ok_or("aggregate profit factor overflow")?,
        )
    };
    let realized_fees = decimal_sum(portfolio.values().map(|episode| episode.fees))?;
    let realized_funding = decimal_sum(portfolio.values().map(|episode| episode.funding))?;
    let realized_slippage = decimal_sum(portfolio.values().map(|episode| episode.slippage))?;
    let verified_records = records
        .iter()
        .filter(|record| record.evidence_verified)
        .collect::<Vec<_>>();
    let all_action_fees =
        decimal_sum(verified_records.iter().map(|record| record.all_action_fees))?;
    let all_action_funding = decimal_sum(
        verified_records
            .iter()
            .map(|record| record.all_action_funding),
    )?;
    let all_action_slippage = decimal_sum(
        verified_records
            .iter()
            .map(|record| record.all_action_slippage),
    )?;
    let hourly_returns = hourly_returns(records)?;
    let maximum_drawdown = aggregate_drawdown(records)?;
    let mut pnl_by_asset = BTreeMap::<String, Decimal>::new();
    let mut episode_count_by_asset = BTreeMap::<String, usize>::new();
    for episode in portfolio.values() {
        checked_add_decimal(
            pnl_by_asset.entry(episode.asset.clone()).or_default(),
            episode.net_pnl,
        )?;
        *episode_count_by_asset
            .entry(episode.asset.clone())
            .or_default() += 1;
    }
    let maximum_asset_pnl_concentration = concentration(&pnl_by_asset)?;
    let maximum_asset_episode_share = count_concentration(&episode_count_by_asset)?;
    let mut source_attribution = AttributionSummary::default();
    let mut technical_attribution = AttributionSummary::default();
    let mut technical_archetype_attribution = BTreeMap::new();
    let mut technical_regime_attribution = BTreeMap::new();
    for episode in sources.values() {
        let target = if let Some(technical) = episode.candidate_id.strip_prefix("technical:") {
            let mut parts = technical.splitn(2, ':');
            let archetype = parts.next().unwrap_or("unknown");
            let regime = parts.next().unwrap_or("unknown");
            update_attribution(
                technical_archetype_attribution
                    .entry(archetype.to_string())
                    .or_default(),
                episode.modeled_net_pnl,
            )?;
            update_attribution(
                technical_regime_attribution
                    .entry(regime.to_string())
                    .or_default(),
                episode.modeled_net_pnl,
            )?;
            &mut technical_attribution
        } else {
            &mut source_attribution
        };
        update_attribution(target, episode.modeled_net_pnl)?;
    }
    let attributed_total = source_attribution
        .modeled_net_pnl
        .checked_add(technical_attribution.modeled_net_pnl)
        .ok_or("aggregate attribution overflow")?;
    let exact_portfolio_total =
        exact_decimal_sum(portfolio.values().map(|episode| episode.net_pnl))?;
    let exact_source_total =
        exact_decimal_sum(sources.values().map(|episode| episode.modeled_net_pnl))?;
    let source_technical_reconciliation_difference = if exact_portfolio_total == exact_source_total
    {
        Decimal::ZERO
    } else {
        cumulative_net_pnl
            .checked_sub(attributed_total)
            .ok_or("aggregate attribution difference overflow")?
    };
    let root_actionable_targets = records
        .iter()
        .map(|record| record.density.root_actionable_targets)
        .sum();
    // This field is a point-in-time density snapshot. Summing it would count a
    // continuation once in every window through which it remained unresolved.
    let unresolved_roots = records
        .last()
        .map_or(0, |record| record.density.unresolved_actionable_targets);
    let root_conservation_incidents = records
        .iter()
        .filter(|record| record.evidence_verified && !record.density.root_conservation_verified)
        .count();
    let risk_invariant_violations = checked_u64_sum(
        records
            .iter()
            .map(|record| record.risk_invariant_violations),
    )?;
    let mutation_requests = checked_u64_sum(records.iter().map(|record| record.mutation_requests))?;
    let key_file_opens = checked_u64_sum(records.iter().map(|record| record.key_file_opens))?;
    let portfolio_accounting_incidents =
        count_failed_verified_records(records, portfolio_accounting_verified)?;
    let source_attribution_reconciliation_incidents =
        count_failed_verified_records(records, source_reconciliation_verified)?;
    let unverified_evidence_windows = records
        .iter()
        .filter(|record| !record.evidence_verified)
        .count();
    let unsafe_runtime_windows = records
        .iter()
        .filter(|record| {
            !record.unsigned
                || !record.planned_only
                || record.submission_capable
                || record.api_wallet_present
                || record.mutation_requests > 0
                || record.key_file_opens > 0
        })
        .count();
    let minimum_duration_met = completed_observation_seconds >= minimum_observation_seconds;
    let minimum_closed_episodes_met = portfolio.len() >= minimum_closed_episodes;
    Ok(RailwayAggregateSummary {
        schema_version: AGGREGATE_SCHEMA_VERSION,
        profitability_identity,
        total_windows: records.len(),
        completed_four_hour_windows,
        incomplete_windows: records.len() - completed_four_hour_windows,
        completed_observation_seconds,
        minimum_observation_seconds,
        minimum_duration_met,
        independent_closed_episodes: portfolio.len(),
        minimum_closed_episodes,
        minimum_closed_episodes_met,
        record_complete: minimum_duration_met
            && minimum_closed_episodes_met
            && unsafe_runtime_windows == 0
            && risk_invariant_violations == 0
            && root_conservation_incidents == 0
            && portfolio_accounting_incidents == 0
            && source_attribution_reconciliation_incidents == 0,
        cumulative_net_pnl,
        profit_factor,
        realized_fees,
        realized_funding,
        realized_slippage,
        all_action_fees,
        all_action_funding,
        all_action_slippage,
        one_hour_return_count: hourly_returns.len(),
        one_hour_annualized_sharpe: annualized_hourly_sharpe(&hourly_returns),
        maximum_drawdown,
        pnl_by_asset,
        episode_count_by_asset,
        maximum_asset_pnl_concentration,
        maximum_asset_episode_share,
        source_attribution,
        technical_attribution,
        source_technical_reconciliation_difference,
        technical_regime_count: technical_regime_attribution.len(),
        technical_archetype_attribution,
        technical_regime_attribution,
        root_actionable_targets,
        unresolved_roots,
        root_conservation_incidents,
        risk_invariant_violations,
        mutation_requests,
        key_file_opens,
        portfolio_accounting_incidents,
        source_attribution_reconciliation_incidents,
        unverified_evidence_windows,
        unsafe_runtime_windows,
    })
}

fn validate_verified_window_buckets(records: &[WindowRecord]) -> Result<(), Box<dyn Error>> {
    for record in records.iter().filter(|record| record.evidence_verified) {
        validate_window_buckets(record)?;
    }
    Ok(())
}

fn validate_window_buckets(record: &WindowRecord) -> Result<(), Box<dyn Error>> {
    if record.completed_four_hours
        && (record.equity_buckets.len() != 48
            || record.monotonic_elapsed_seconds < record.expected_duration_seconds)
    {
        return Err(format!(
            "completed window {} does not contain a full four-hour measurement",
            record.run_id
        )
        .into());
    }
    let measured_bucket_ms = (record.equity_buckets.len() as u64)
        .checked_mul(FIVE_MINUTES_MS)
        .ok_or("aggregate bucket duration overflow")?;
    let elapsed_ms = record
        .monotonic_elapsed_seconds
        .checked_mul(1_000)
        .ok_or("aggregate elapsed duration overflow")?;
    if measured_bucket_ms > elapsed_ms {
        return Err(format!(
            "five-minute buckets exceed elapsed time for {}",
            record.run_id
        )
        .into());
    }
    for (index, bucket) in record.equity_buckets.iter().enumerate() {
        let expected_id = format!("{}:{index}", record.run_id);
        if bucket.run_id != record.run_id
            || bucket.bucket_index != index as u64
            || bucket.bucket_id != expected_id
            || bucket.closed_at_mono.checked_sub(bucket.opened_at_mono) != Some(FIVE_MINUTES_MS)
            || (index > 0
                && record.equity_buckets[index - 1].closed_at_mono != bucket.opened_at_mono)
            || (index > 0
                && record.equity_buckets[index - 1].ending_equity != bucket.starting_equity)
            || bucket.starting_equity.is_zero()
        {
            return Err(format!(
                "invalid five-minute bucket structure for {} at index {index}",
                record.run_id
            )
            .into());
        }
        let expected_return = bucket
            .ending_equity
            .checked_sub(bucket.starting_equity)
            .and_then(|change| change.checked_div(bucket.starting_equity))
            .ok_or("aggregate five-minute return overflow")?;
        if bucket.return_fraction != expected_return {
            return Err(format!(
                "five-minute bucket return mismatch for {} at index {index}",
                record.run_id
            )
            .into());
        }
    }

    if let Some(profitability) = &record.profitability {
        if profitability.five_minute_bucket_count != record.equity_buckets.len() {
            return Err(
                format!("profitability bucket count mismatch for {}", record.run_id).into(),
            );
        }
        let measured_end = record
            .equity_buckets
            .last()
            .map_or(profitability.starting_equity, |bucket| bucket.ending_equity);
        let measured_change = measured_end
            .checked_sub(profitability.starting_equity)
            .ok_or("aggregate equity change overflow")?;
        if profitability.ending_equity != measured_end
            || profitability.equity_change_including_open_positions != measured_change
        {
            return Err(format!(
                "profitability equity continuity mismatch for {}",
                record.run_id
            )
            .into());
        }
    } else if record.completed_four_hours {
        return Err(format!(
            "completed window {} has no profitability summary",
            record.run_id
        )
        .into());
    }
    Ok(())
}

fn count_failed_verified_records(
    records: &[WindowRecord],
    check: fn(&WindowRecord) -> Result<bool, Box<dyn Error>>,
) -> Result<usize, Box<dyn Error>> {
    records
        .iter()
        .filter(|record| record.evidence_verified)
        .try_fold(0_usize, |count, record| {
            if check(record)? {
                Ok(count)
            } else {
                count
                    .checked_add(1)
                    .ok_or_else(|| "aggregate incident count overflow".into())
            }
        })
}

fn portfolio_accounting_verified(record: &WindowRecord) -> Result<bool, Box<dyn Error>> {
    let Some(summary) = &record.profitability else {
        return Ok(false);
    };
    let episode_formula_verified = record.portfolio_episodes.iter().all(|episode| {
        episode
            .realized_pnl
            .checked_sub(episode.fees)
            .and_then(|value| value.checked_sub(episode.funding))
            .and_then(|value| value.checked_sub(episode.slippage))
            == Some(episode.net_pnl)
    });
    Ok(summary.bucket_structure_verified
        && summary.episode_cost_formula_verified
        && episode_formula_verified
        && summary.closed_portfolio_episodes == record.portfolio_episodes.len()
        && summary.total_net_pnl
            == decimal_sum(
                record
                    .portfolio_episodes
                    .iter()
                    .map(|episode| episode.net_pnl),
            )?
        && summary.fees
            == decimal_sum(record.portfolio_episodes.iter().map(|episode| episode.fees))?
        && summary.funding
            == decimal_sum(
                record
                    .portfolio_episodes
                    .iter()
                    .map(|episode| episode.funding),
            )?
        && summary.execution_slippage
            == decimal_sum(
                record
                    .portfolio_episodes
                    .iter()
                    .map(|episode| episode.slippage),
            )?
        && summary.all_action_fees == record.all_action_fees
        && summary.all_action_funding == record.all_action_funding
        && summary.all_action_slippage == record.all_action_slippage)
}

fn source_reconciliation_verified(record: &WindowRecord) -> Result<bool, Box<dyn Error>> {
    let Some(summary) = &record.profitability else {
        return Ok(false);
    };
    let portfolio_net = exact_decimal_sum(
        record
            .portfolio_episodes
            .iter()
            .map(|episode| episode.net_pnl),
    )?;
    let source_net = exact_decimal_sum(
        record
            .source_episodes
            .iter()
            .map(|episode| episode.modeled_net_pnl),
    )?;
    let source_formula_verified = record.source_episodes.iter().all(|episode| {
        episode
            .modeled_gross_pnl
            .checked_sub(episode.modeled_fees)
            .and_then(|value| value.checked_sub(episode.modeled_funding))
            .and_then(|value| value.checked_sub(episode.modeled_slippage))
            .and_then(|value| value.checked_add(episode.attribution_residual))
            == Some(episode.modeled_net_pnl)
    });
    Ok(source_formula_verified
        && summary.closed_source_episodes == record.source_episodes.len()
        && portfolio_net == source_net)
}

fn hourly_returns(records: &[WindowRecord]) -> Result<Vec<Decimal>, Box<dyn Error>> {
    let mut returns = Vec::new();
    for record in records.iter().filter(|record| record.evidence_verified) {
        for hour in record.equity_buckets.chunks(ONE_HOUR_BUCKETS) {
            if hour.len() != ONE_HOUR_BUCKETS {
                continue;
            }
            returns.push(
                hour[ONE_HOUR_BUCKETS - 1]
                    .ending_equity
                    .checked_sub(hour[0].starting_equity)
                    .and_then(|change| change.checked_div(hour[0].starting_equity))
                    .ok_or("aggregate one-hour return overflow")?,
            );
        }
    }
    Ok(returns)
}

fn aggregate_drawdown(records: &[WindowRecord]) -> Result<Decimal, Box<dyn Error>> {
    let mut peak: Option<Decimal> = None;
    let mut maximum = Decimal::ZERO;
    for bucket in records
        .iter()
        .filter(|record| record.evidence_verified)
        .flat_map(|record| record.equity_buckets.iter())
    {
        let next_peak = peak
            .unwrap_or(bucket.starting_equity)
            .max(bucket.starting_equity)
            .max(bucket.ending_equity);
        if !next_peak.is_zero() {
            maximum = maximum.max(
                next_peak
                    .checked_sub(bucket.ending_equity)
                    .and_then(|change| change.checked_div(next_peak))
                    .ok_or("aggregate drawdown overflow")?,
            );
        }
        peak = Some(next_peak);
    }
    Ok(maximum)
}

fn annualized_hourly_sharpe(returns: &[Decimal]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let values = returns
        .iter()
        .filter_map(|value| value.to_string().parse::<f64>().ok())
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
        mean / deviation * (365.0_f64 * 24.0).sqrt()
    }
}

fn concentration(values: &BTreeMap<String, Decimal>) -> Result<Decimal, Box<dyn Error>> {
    let total = decimal_sum(values.values().map(|value| value.abs()))?;
    if total.is_zero() {
        return Ok(Decimal::ZERO);
    }
    Ok(values
        .values()
        .map(|value| value.abs().checked_div(total).unwrap_or_default())
        .max()
        .unwrap_or_default())
}

fn count_concentration(values: &BTreeMap<String, usize>) -> Result<Decimal, Box<dyn Error>> {
    let total: usize = values.values().sum();
    if total == 0 {
        return Ok(Decimal::ZERO);
    }
    Decimal::from(*values.values().max().unwrap_or(&0) as u64)
        .checked_div(Decimal::from(total as u64))
        .ok_or_else(|| "aggregate episode concentration overflow".into())
}

fn update_attribution(
    summary: &mut AttributionSummary,
    pnl: Decimal,
) -> Result<(), Box<dyn Error>> {
    summary.closed_episodes = summary
        .closed_episodes
        .checked_add(1)
        .ok_or("aggregate attribution count overflow")?;
    checked_add_decimal(&mut summary.modeled_net_pnl, pnl)?;
    Ok(())
}

fn checked_add_decimal(target: &mut Decimal, value: Decimal) -> Result<(), Box<dyn Error>> {
    *target = target
        .checked_add(value)
        .ok_or("aggregate decimal overflow")?;
    Ok(())
}

fn checked_u64_sum(mut values: impl Iterator<Item = u64>) -> Result<u64, &'static str> {
    values.try_fold(0_u64, |sum, value| {
        sum.checked_add(value).ok_or("aggregate counter overflow")
    })
}

fn decimal_sum(mut values: impl Iterator<Item = Decimal>) -> Result<Decimal, &'static str> {
    values.try_fold(Decimal::ZERO, |sum, value| {
        sum.checked_add(value).ok_or("aggregate decimal overflow")
    })
}

fn insert_identical<K: Ord + Clone, V: PartialEq>(
    values: &mut BTreeMap<K, V>,
    key: K,
    value: V,
) -> Result<(), Box<dyn Error>> {
    if let Some(existing) = values.get(&key) {
        if existing != &value {
            return Err("aggregate episode identity collision".into());
        }
    } else {
        values.insert(key, value);
    }
    Ok(())
}

fn hash_bundle(root: &Path) -> Result<String, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_regular_files(root, root, &mut files)?;
    files.sort();
    let mut hash = Sha256::new();
    for relative in files {
        let name = relative
            .to_str()
            .ok_or("bundle contains a non-UTF-8 path")?
            .as_bytes();
        hash.update((name.len() as u64).to_be_bytes());
        hash.update(name);
        let mut file = File::open(root.join(&relative))?;
        let size = file.metadata()?.len();
        hash.update(size.to_be_bytes());
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn collect_regular_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err("bundle may not contain symlinks".into());
        }
        if file_type.is_dir() {
            collect_regular_files(root, &entry.path(), files)?;
        } else if file_type.is_file() {
            files.push(entry.path().strip_prefix(root)?.to_path_buf());
        } else {
            return Err("bundle may contain only regular files and directories".into());
        }
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Box<dyn Error>> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn read_json_or_default<T>(path: &Path) -> Result<T, Box<dyn Error>>
where
    T: for<'de> Deserialize<'de> + Default,
{
    if path.exists() {
        read_json(path)
    } else {
        Ok(T::default())
    }
}

fn is_sha256(value: &str) -> bool {
    let value = value.strip_prefix("0x").unwrap_or(value);
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_safe_run_id(value: &str) -> bool {
    let mut parts = value.split('-');
    let wall_clock = parts.next().unwrap_or_default();
    let process = parts.next().unwrap_or_default();
    let binary = parts.next().unwrap_or_default();
    parts.next().is_none()
        && wall_clock.len() >= 10
        && wall_clock.bytes().all(|byte| byte.is_ascii_digit())
        && !process.is_empty()
        && process.bytes().all(|byte| byte.is_ascii_digit())
        && (12..=64).contains(&binary.len())
        && binary
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn normalize_sha256(value: &str) -> Result<String, Box<dyn Error>> {
    if !is_sha256(value) {
        return Err("invalid profitability identity hash".into());
    }
    Ok(value
        .strip_prefix("0x")
        .unwrap_or(value)
        .to_ascii_lowercase())
}

fn hash_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn pretty_json(value: &impl Serialize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let parent = path.parent().ok_or("aggregate path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    sync_directory(parent)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), Box<dyn Error>> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::decision::DecisionId;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

    fn episode(id: u8, asset: &str, pnl: i64) -> PortfolioEpisode {
        PortfolioEpisode {
            episode_id: EpisodeId([id; 32]),
            asset: asset.into(),
            opened_at: 1,
            closed_at: 2,
            opening_decision_id: DecisionId([id; 32]),
            entry_notional: Decimal::from(10),
            exit_notional: Decimal::from(10),
            realized_pnl: Decimal::from(pnl),
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            slippage: Decimal::ZERO,
            attribution_residual: Decimal::ZERO,
            net_pnl: Decimal::from(pnl),
        }
    }

    fn source_episode(episode: &PortfolioEpisode) -> SourceEpisode {
        SourceEpisode {
            candidate_id: "source".into(),
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
            net_return: Decimal::ZERO,
        }
    }

    fn buckets(run_id: &str, count: usize) -> Vec<AggregateEquityBucket> {
        (0..count)
            .map(|index| AggregateEquityBucket {
                run_id: run_id.into(),
                bucket_id: format!("{run_id}:{index}"),
                bucket_index: index as u64,
                opened_at_mono: index as u64 * FIVE_MINUTES_MS,
                closed_at_mono: (index as u64 + 1) * FIVE_MINUTES_MS,
                starting_equity: Decimal::from(100),
                ending_equity: Decimal::from(100),
                return_fraction: Decimal::ZERO,
                source_returns: BTreeMap::new(),
            })
            .collect()
    }

    fn record(id: u8, episodes: Vec<PortfolioEpisode>) -> WindowRecord {
        let run_id = format!("window-{id}");
        let total_net_pnl = decimal_sum(episodes.iter().map(|episode| episode.net_pnl)).unwrap();
        let fees = decimal_sum(episodes.iter().map(|episode| episode.fees)).unwrap();
        let funding = decimal_sum(episodes.iter().map(|episode| episode.funding)).unwrap();
        let execution_slippage =
            decimal_sum(episodes.iter().map(|episode| episode.slippage)).unwrap();
        let equity_buckets = buckets(&run_id, 48);
        let profitability = WindowProfitability {
            realized_pnl_scope: "closed_portfolio_episodes".into(),
            total_net_pnl_formula:
                "total_gross_pnl - fees - funding - execution_slippage = total_net_pnl".into(),
            execution_slippage_sign_convention:
                "signed_cost_subtracted_from_gross; negative values improve total_net_pnl".into(),
            all_action_cost_scope: "all_modeled_executions_including_open_positions".into(),
            starting_equity: Decimal::from(100),
            ending_equity: Decimal::from(100),
            equity_change_including_open_positions: Decimal::ZERO,
            total_net_pnl,
            fees,
            funding,
            execution_slippage,
            all_action_fees: Decimal::ZERO,
            all_action_funding: Decimal::ZERO,
            all_action_slippage: Decimal::ZERO,
            shadow_executions: 0,
            closed_portfolio_episodes: episodes.len(),
            closed_source_episodes: 0,
            five_minute_bucket_count: equity_buckets.len(),
            bucket_structure_verified: true,
            episode_cost_formula_verified: true,
            portfolio_source_books_reconcile: total_net_pnl.is_zero(),
        };
        WindowRecord {
            schema_version: AGGREGATE_SCHEMA_VERSION,
            run_id,
            bundle_sha256: format!("{id:064x}"),
            identity: ProfitabilityIdentity {
                frozen_identity_sha256: "01".repeat(32),
                source_tree_sha256: "02".repeat(32),
                observer_binary_sha256: "03".repeat(32),
                configuration_sha256: "04".repeat(32),
                risk_policy_sha256: "05".repeat(32),
                release_manifest_sha256: "06".repeat(32),
            },
            start_wall_clock_utc_ms: u64::from(id) * 14_400_000,
            expected_duration_seconds: FOUR_HOURS_SECONDS,
            monotonic_elapsed_seconds: FOUR_HOURS_SECONDS,
            completed_four_hours: true,
            evidence_verified: true,
            unsigned: true,
            planned_only: true,
            submission_capable: false,
            api_wallet_present: false,
            risk_invariant_violations: 0,
            mutation_requests: 0,
            key_file_opens: 0,
            operational_gate_passed: true,
            accounting_gate_passed: true,
            profitability_signal_positive: false,
            portfolio_episodes: episodes,
            source_episodes: Vec::new(),
            equity_buckets,
            profitability: Some(profitability),
            all_action_fees: Decimal::ZERO,
            all_action_funding: Decimal::ZERO,
            all_action_slippage: Decimal::ZERO,
            density: WindowDensity {
                root_conservation_verified: true,
                ..WindowDensity::default()
            },
        }
    }

    fn set_bucket_count(record: &mut WindowRecord, count: usize) {
        record.equity_buckets.truncate(count);
        let profitability = record.profitability.as_mut().unwrap();
        profitability.five_minute_bucket_count = count;
        profitability.ending_equity = record
            .equity_buckets
            .last()
            .map_or(profitability.starting_equity, |bucket| bucket.ending_equity);
        profitability.equity_change_including_open_positions = profitability
            .ending_equity
            .checked_sub(profitability.starting_equity)
            .unwrap();
    }

    fn temporary_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "copytrade-railway-aggregate-{name}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn cumulative_episode_snapshots_are_deduplicated() {
        let first = episode(1, "BTC", -2);
        let second = episode(2, "ETH", 4);
        let records = vec![
            record(1, vec![first.clone()]),
            record(2, vec![first, second]),
        ];
        let summary = summarize(&records, 604_800, 100).unwrap();
        assert_eq!(summary.independent_closed_episodes, 2);
        assert_eq!(summary.cumulative_net_pnl, Decimal::from(2));
        assert_eq!(summary.profit_factor, Some(Decimal::from(2)));
        assert!(!summary.record_complete);
    }

    #[test]
    fn inactive_and_incomplete_windows_remain_counted() {
        let mut incomplete = record(2, Vec::new());
        incomplete.completed_four_hours = false;
        incomplete.monotonic_elapsed_seconds = 60;
        set_bucket_count(&mut incomplete, 0);
        let summary = summarize(&[record(1, Vec::new()), incomplete], 604_800, 100).unwrap();
        assert_eq!(summary.total_windows, 2);
        assert_eq!(summary.completed_four_hour_windows, 1);
        assert_eq!(summary.incomplete_windows, 1);
        assert_eq!(summary.independent_closed_episodes, 0);
    }

    #[test]
    fn invalid_candidate_is_not_written_before_cross_record_validation() {
        let root = temporary_root("prevalidation");
        let first = record(1, Vec::new());
        aggregate_window_record(first.clone(), &root, 604_800, 100).unwrap();
        let summary_before = std::fs::read(root.join("summary.json")).unwrap();

        let mut overlapping = record(2, Vec::new());
        overlapping.start_wall_clock_utc_ms = first.start_wall_clock_utc_ms + FIVE_MINUTES_MS;
        let error = aggregate_window_record(overlapping, &root, 604_800, 100)
            .unwrap_err()
            .to_string();
        assert!(error.contains("overlap"));
        assert!(!root.join("windows/window-2.json").exists());
        assert_eq!(
            std::fs::read(root.join("summary.json")).unwrap(),
            summary_before
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn full_bucket_validator_checks_trailing_structure_equity_and_return() {
        let mut partial = record(1, Vec::new());
        partial.completed_four_hours = false;
        partial.monotonic_elapsed_seconds = 13 * 300;
        set_bucket_count(&mut partial, 13);
        assert!(validate_window_buckets(&partial).is_ok());
        assert_eq!(hourly_returns(&[partial.clone()]).unwrap().len(), 1);

        let mut restored_equity = partial.clone();
        for bucket in &mut restored_equity.equity_buckets {
            bucket.starting_equity = Decimal::from(125);
            bucket.ending_equity = Decimal::from(125);
        }
        let profitability = restored_equity.profitability.as_mut().unwrap();
        profitability.ending_equity = Decimal::from(125);
        profitability.equity_change_including_open_positions = Decimal::from(25);
        assert!(validate_window_buckets(&restored_equity).is_ok());

        let mut bad_id = partial.clone();
        bad_id.equity_buckets[12].bucket_index = 99;
        assert!(validate_window_buckets(&bad_id).is_err());

        let mut bad_time = partial.clone();
        bad_time.equity_buckets[12].opened_at_mono += 1;
        bad_time.equity_buckets[12].closed_at_mono += 1;
        assert!(validate_window_buckets(&bad_time).is_err());

        let mut bad_equity = partial.clone();
        bad_equity.equity_buckets[12].starting_equity += Decimal::ONE;
        assert!(validate_window_buckets(&bad_equity).is_err());

        let mut bad_return = partial;
        bad_return.equity_buckets[12].return_fraction = Decimal::ONE;
        assert!(validate_window_buckets(&bad_return).is_err());
    }

    #[test]
    fn unresolved_roots_are_the_latest_snapshot_not_a_sum() {
        let mut first = record(1, Vec::new());
        first.density.unresolved_actionable_targets = 3;
        let mut second = record(2, Vec::new());
        second.density.unresolved_actionable_targets = 1;
        let summary = summarize(&[first, second], 604_800, 100).unwrap();
        assert_eq!(summary.unresolved_roots, 1);
    }

    #[test]
    fn completion_allows_unverified_interrupt_but_blocks_incidents() {
        let episodes = (0..100).map(|id| episode(id, "BTC", 0)).collect::<Vec<_>>();
        let mut complete = record(1, episodes);
        complete.source_episodes = complete
            .portfolio_episodes
            .iter()
            .map(source_episode)
            .collect();
        let profitability = complete.profitability.as_mut().unwrap();
        profitability.closed_source_episodes = complete.source_episodes.len();
        profitability.portfolio_source_books_reconcile = true;

        let mut interrupted = record(2, Vec::new());
        interrupted.completed_four_hours = false;
        interrupted.evidence_verified = false;
        interrupted.monotonic_elapsed_seconds = 60;
        set_bucket_count(&mut interrupted, 0);
        interrupted.profitability = None;

        let summary = summarize(&[complete.clone(), interrupted.clone()], 14_400, 100).unwrap();
        assert_eq!(summary.unverified_evidence_windows, 1);
        assert!(summary.record_complete);

        let mut risky = complete.clone();
        risky.risk_invariant_violations = 1;
        assert!(
            !summarize(&[risky, interrupted.clone()], 14_400, 100)
                .unwrap()
                .record_complete
        );

        let mut bad_roots = complete.clone();
        bad_roots.density.root_conservation_verified = false;
        assert!(
            !summarize(&[bad_roots, interrupted.clone()], 14_400, 100)
                .unwrap()
                .record_complete
        );

        let mut bad_accounting = complete.clone();
        bad_accounting
            .profitability
            .as_mut()
            .unwrap()
            .episode_cost_formula_verified = false;
        assert!(
            !summarize(&[bad_accounting, interrupted.clone()], 14_400, 100)
                .unwrap()
                .record_complete
        );

        let mut legacy_order_mismatch = complete.clone();
        legacy_order_mismatch
            .profitability
            .as_mut()
            .unwrap()
            .portfolio_source_books_reconcile = false;
        assert!(
            summarize(&[legacy_order_mismatch, interrupted.clone()], 14_400, 100)
                .unwrap()
                .record_complete
        );

        let mut bad_reconciliation = complete;
        bad_reconciliation.source_episodes[0].modeled_gross_pnl += Decimal::ONE;
        bad_reconciliation.source_episodes[0].modeled_net_pnl += Decimal::ONE;
        bad_reconciliation
            .profitability
            .as_mut()
            .unwrap()
            .portfolio_source_books_reconcile = false;
        assert!(
            !summarize(&[bad_reconciliation, interrupted], 14_400, 100)
                .unwrap()
                .record_complete
        );
    }

    #[test]
    fn run_id_validation_rejects_paths_and_accepts_observer_shape() {
        assert!(is_safe_run_id("1785115756447-46864-2b091b0cebb2"));
        assert!(!is_safe_run_id("../../summary"));
        assert!(!is_safe_run_id("1785115756447-46864-2B091B0CEBB2"));
    }
}
