use crate::{
    backtest::sharpe_stats,
    copytrade::{CopyTradeConfig, TraderCandidate},
};
use chrono::{TimeZone, Utc};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    error::Error,
    fs,
    path::Path,
    time::Duration,
};
use tokio::time::sleep;

const HYPERDASH_COHORT_URL: &str = "https://hyperdash.com/explore/cohorts/extremely_profitable";
const HYPERDASH_COHORT_ID: &str = "extremely_profitable";
const EXPECTED_COHORT_SIZE: usize = 304;
const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const WINDOW_START: &str = "2025-07-17T00:00:00Z";
const WINDOW_END_EXCLUSIVE: &str = "2026-07-17T00:00:00Z";
const FILLS_PAGE_LIMIT: usize = 2_000;
const AVAILABLE_FILLS_LIMIT: usize = 10_000;
const HOUR_MS: i64 = 60 * 60 * 1_000;
const HOURS_PER_YEAR: f64 = 365.25 * 24.0;
const INFO_REQUEST_INTERVAL: Duration = Duration::from_millis(1_100);
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct SeedOptions {
    pub base_config: String,
    pub staged_config: String,
    pub report: String,
    pub cohort_file: String,
    pub promote_to: Option<String>,
}

impl Default for SeedOptions {
    fn default() -> Self {
        Self {
            base_config: "config/copytrade.example.json".to_string(),
            staged_config: "config/copytrade.vetted.json".to_string(),
            report: "data/candidate-vetting-2025-07-17_2026-07-17.json".to_string(),
            cohort_file: "data/hyperdash-extremely-profitable-2026-07-17.json".to_string(),
            promote_to: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct HyperdashCohortSnapshot {
    source: String,
    cohort_id: String,
    fetched_at: String,
    total_traders: usize,
    total_count: usize,
    count: usize,
    unique_count: usize,
    complete: bool,
    wallets: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearinghouseState {
    margin_summary: MarginSummary,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarginSummary {
    account_value: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserFill {
    dir: Option<String>,
    closed_pnl: Option<String>,
    time: Option<i64>,
    hash: Option<String>,
    oid: Option<u64>,
    tid: Option<u64>,
    px: Option<String>,
    sz: Option<String>,
    fee: Option<String>,
}

#[derive(Debug, Serialize)]
struct VettingReport {
    generated_at: String,
    window_start: &'static str,
    window_end_exclusive: &'static str,
    window_days: i64,
    source: String,
    source_snapshot_at: String,
    source_candidates: usize,
    passed_candidates: usize,
    criteria: ReportCriteria,
    candidates: Vec<CandidateResult>,
}

#[derive(Debug, Serialize)]
struct ReportCriteria {
    mode: &'static str,
    require_positive_live_equity: bool,
    min_closed_trades: usize,
    require_positive_fee_adjusted_pnl: bool,
    high_density_fill_count: usize,
    performance_statistics_are_observational_only: bool,
}

#[derive(Debug, Serialize)]
struct CandidateResult {
    address: String,
    label: String,
    passed: bool,
    reasons: Vec<String>,
    account_value_usd: f64,
    fetched_fills: usize,
    closed_trades: usize,
    wins: usize,
    win_rate_pct: f64,
    confidence_score: f64,
    closed_pnl_usd: f64,
    total_fees_usd: f64,
    fee_adjusted_pnl_usd: f64,
    pathway: VettingPathway,
    history_breadth_days: f64,
    hourly_return_samples: usize,
    effective_hourly_samples: f64,
    annualized_sharpe: Option<f64>,
    sharpe_lower_95: Option<f64>,
    history_checked: bool,
    history_complete: bool,
    first_fill_at: Option<String>,
    last_fill_at: Option<String>,
}

struct FillHistory {
    fills: Vec<UserFill>,
    complete: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum VettingPathway {
    Standard,
    HighDensity,
}

pub async fn seed_candidates(options: SeedOptions) -> Result<(), Box<dyn Error>> {
    let mut config = CopyTradeConfig::from_path(&options.base_config)?;
    let client = Client::builder().build()?;
    let snapshot: HyperdashCohortSnapshot =
        serde_json::from_str(&fs::read_to_string(&options.cohort_file)?)?;
    let seeds = seeds_from_hyperdash_snapshot(&snapshot)?;
    let seed_count = seeds.len();

    let start_ms = parse_window(WINDOW_START)?;
    let end_exclusive_ms = parse_window(WINDOW_END_EXCLUSIVE)?;
    let mut results = Vec::with_capacity(seed_count);
    let mut passed = Vec::new();
    for (index, seed) in seeds.into_iter().enumerate() {
        println!(
            "vetting candidate {}/{}: {}",
            index + 1,
            seed_count,
            seed.address
        );
        let result = vet_one(&client, &seed, start_ms, end_exclusive_ms).await?;
        if result.passed {
            passed.push(seed);
        }
        results.push(result);
    }

    passed.sort_by(|left, right| {
        result_for(&results, &right.address)
            .fee_adjusted_pnl_usd
            .partial_cmp(&result_for(&results, &left.address).fee_adjusted_pnl_usd)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                result_for(&results, &right.address)
                    .closed_pnl_usd
                    .partial_cmp(&result_for(&results, &left.address).closed_pnl_usd)
                    .unwrap_or(Ordering::Equal)
            })
    });
    config.vetting_lookback_days = 365;
    config.vetting.lookback_days = 365;
    config.candidates = passed;

    let report = VettingReport {
        generated_at: Utc::now().to_rfc3339(),
        window_start: WINDOW_START,
        window_end_exclusive: WINDOW_END_EXCLUSIVE,
        window_days: (end_exclusive_ms - start_ms) / 86_400_000,
        source: snapshot.source,
        source_snapshot_at: snapshot.fetched_at,
        source_candidates: results.len(),
        passed_candidates: config.candidates.len(),
        criteria: ReportCriteria {
            mode: "pure_ingestion",
            require_positive_live_equity: true,
            min_closed_trades: 1,
            require_positive_fee_adjusted_pnl: true,
            high_density_fill_count: AVAILABLE_FILLS_LIMIT,
            performance_statistics_are_observational_only: true,
        },
        candidates: results,
    };

    write_json(&options.report, &report)?;
    write_json(&options.staged_config, &config)?;
    if let Some(production_path) = options.promote_to.as_deref() {
        if config.candidates.is_empty() {
            return Err(format!(
                "no candidates passed; report written to {} and production config was not changed",
                options.report
            )
            .into());
        }
        write_json(production_path, &config)?;
        println!(
            "promoted {} vetted candidate(s) to {production_path}",
            config.candidates.len()
        );
    }
    println!(
        "candidate vetting complete: {} passed; report {}, staged config {}",
        config.candidates.len(),
        options.report,
        options.staged_config
    );
    Ok(())
}

fn seeds_from_hyperdash_snapshot(
    snapshot: &HyperdashCohortSnapshot,
) -> Result<Vec<TraderCandidate>, Box<dyn Error>> {
    if snapshot.source != HYPERDASH_COHORT_URL || snapshot.cohort_id != HYPERDASH_COHORT_ID {
        return Err("candidate snapshot is not the Hyperdash Extremely Profitable cohort".into());
    }
    if !snapshot.complete {
        return Err("Hyperdash cohort snapshot is marked incomplete".into());
    }
    let unique = snapshot
        .wallets
        .iter()
        .map(|address| address.to_lowercase())
        .collect::<HashSet<_>>();
    if snapshot.total_traders != EXPECTED_COHORT_SIZE
        || snapshot.total_count != EXPECTED_COHORT_SIZE
        || snapshot.count != EXPECTED_COHORT_SIZE
        || snapshot.unique_count != EXPECTED_COHORT_SIZE
        || snapshot.wallets.len() != EXPECTED_COHORT_SIZE
        || unique.len() != EXPECTED_COHORT_SIZE
    {
        return Err(format!(
            "Hyperdash cohort snapshot must contain exactly {EXPECTED_COHORT_SIZE} unique wallets"
        )
        .into());
    }
    if snapshot
        .wallets
        .iter()
        .any(|address| !valid_address(address))
    {
        return Err("Hyperdash cohort snapshot contains an invalid wallet address".into());
    }
    Ok(snapshot
        .wallets
        .iter()
        .enumerate()
        .map(|(index, address)| TraderCandidate {
            address: address.to_lowercase(),
            label: format!("hyperdash-extremely-profitable-{}", index + 1),
            allocation_weight: 1.0,
            confidence_modifier: Some(1.0),
            enabled: true,
        })
        .collect())
}

async fn vet_one(
    client: &Client,
    seed: &TraderCandidate,
    start_ms: i64,
    end_exclusive_ms: i64,
) -> Result<CandidateResult, Box<dyn Error>> {
    let account_value = fetch_account_value(client, &seed.address).await?;
    let mut account_reasons = Vec::new();
    if account_value <= 0.0 {
        account_reasons.push("live account has no positive equity".to_string());
    }
    if !account_reasons.is_empty() {
        return Ok(CandidateResult {
            address: seed.address.clone(),
            label: seed.label.clone(),
            passed: false,
            reasons: account_reasons,
            account_value_usd: account_value,
            fetched_fills: 0,
            closed_trades: 0,
            wins: 0,
            win_rate_pct: 0.0,
            confidence_score: 0.0,
            closed_pnl_usd: 0.0,
            total_fees_usd: 0.0,
            fee_adjusted_pnl_usd: 0.0,
            pathway: VettingPathway::Standard,
            history_breadth_days: 0.0,
            hourly_return_samples: 0,
            effective_hourly_samples: 0.0,
            annualized_sharpe: None,
            sharpe_lower_95: None,
            history_checked: false,
            history_complete: false,
            first_fill_at: None,
            last_fill_at: None,
        });
    }
    let history = fetch_fill_history(client, &seed.address, start_ms, end_exclusive_ms).await?;
    let pathway = pathway_for(&history)?;
    let history_breadth_days = fill_history_breadth_days(&history.fills);
    let hourly_returns = hourly_net_returns(&history.fills, account_value);
    let sharpe = sharpe_stats(&hourly_returns, HOURS_PER_YEAR, 24);
    let annualized_sharpe = sharpe.annualized_sharpe.filter(|value| value.is_finite());
    let sharpe_lower_95 = sharpe.lower_95_sharpe.filter(|value| value.is_finite());
    let mut closed_trades = 0;
    let mut wins = 0;
    let mut closed_pnl = 0.0;
    let mut total_fees = 0.0;
    for fill in &history.fills {
        total_fees += fill.fee.as_deref().map(parse_f64).unwrap_or_default();
        if !fill_closes_position(fill) {
            continue;
        }
        let pnl = fill
            .closed_pnl
            .as_deref()
            .map(parse_f64)
            .unwrap_or_default();
        closed_trades += 1;
        closed_pnl += pnl;
        if pnl > 0.0 {
            wins += 1;
        }
    }
    let win_rate = if closed_trades == 0 {
        0.0
    } else {
        wins as f64 / closed_trades as f64 * 100.0
    };
    let confidence = win_rate * (closed_trades as f64 / 50.0).min(1.0);
    let fee_adjusted_pnl = closed_pnl - total_fees;
    let reasons = ingestion_rejection_reasons(closed_trades, fee_adjusted_pnl);
    let first = history.fills.iter().filter_map(|fill| fill.time).min();
    let last = history.fills.iter().filter_map(|fill| fill.time).max();
    Ok(CandidateResult {
        address: seed.address.clone(),
        label: seed.label.clone(),
        passed: reasons.is_empty(),
        reasons,
        account_value_usd: account_value,
        fetched_fills: history.fills.len(),
        closed_trades,
        wins,
        win_rate_pct: win_rate,
        confidence_score: confidence,
        closed_pnl_usd: closed_pnl,
        total_fees_usd: total_fees,
        fee_adjusted_pnl_usd: fee_adjusted_pnl,
        pathway,
        history_breadth_days,
        hourly_return_samples: sharpe.samples,
        effective_hourly_samples: sharpe.effective_samples,
        annualized_sharpe,
        sharpe_lower_95,
        history_checked: true,
        history_complete: history.complete,
        first_fill_at: timestamp(first),
        last_fill_at: timestamp(last),
    })
}

fn ingestion_rejection_reasons(closed_trades: usize, fee_adjusted_pnl: f64) -> Vec<String> {
    let mut reasons = Vec::new();
    if closed_trades == 0 {
        reasons.push("no closed fills in retained history".to_string());
    }
    if fee_adjusted_pnl <= 0.0 {
        reasons.push("fee-adjusted empirical PnL is not positive".to_string());
    }
    reasons
}

fn pathway_for(history: &FillHistory) -> Result<VettingPathway, Box<dyn Error>> {
    if history.complete {
        Ok(VettingPathway::Standard)
    } else if history.fills.len() == AVAILABLE_FILLS_LIMIT {
        Ok(VettingPathway::HighDensity)
    } else {
        Err("fill history is incomplete before reaching the 10,000-fill retention limit".into())
    }
}

fn fill_history_breadth_days(fills: &[UserFill]) -> f64 {
    let first = fills.iter().filter_map(|fill| fill.time).min();
    let last = fills.iter().filter_map(|fill| fill.time).max();
    match (first, last) {
        (Some(first), Some(last)) if last >= first => (last - first) as f64 / 86_400_000.0,
        _ => 0.0,
    }
}

fn hourly_net_returns(fills: &[UserFill], account_value: f64) -> Vec<f64> {
    if account_value <= 0.0 {
        return Vec::new();
    }
    let Some(first_hour) = fills
        .iter()
        .filter_map(|fill| fill.time)
        .min()
        .map(|time| time.div_euclid(HOUR_MS) * HOUR_MS)
    else {
        return Vec::new();
    };
    let Some(last_hour) = fills
        .iter()
        .filter_map(|fill| fill.time)
        .max()
        .map(|time| time.div_euclid(HOUR_MS) * HOUR_MS)
    else {
        return Vec::new();
    };
    let mut hourly_pnl = BTreeMap::<i64, f64>::new();
    for fill in fills {
        let Some(time) = fill.time else { continue };
        let hour = time.div_euclid(HOUR_MS) * HOUR_MS;
        let closed_pnl = fill
            .closed_pnl
            .as_deref()
            .map(parse_f64)
            .unwrap_or_default();
        let fee = fill.fee.as_deref().map(parse_f64).unwrap_or_default();
        *hourly_pnl.entry(hour).or_default() += closed_pnl - fee;
    }
    (first_hour..=last_hour)
        .step_by(HOUR_MS as usize)
        .map(|hour| hourly_pnl.get(&hour).copied().unwrap_or_default() / account_value)
        .collect()
}

async fn fetch_account_value(client: &Client, address: &str) -> Result<f64, Box<dyn Error>> {
    let state = post_info::<ClearinghouseState>(
        client,
        json!({"type":"clearinghouseState","user":address}),
    )
    .await?;
    Ok(parse_f64(&state.margin_summary.account_value))
}

async fn fetch_fill_history(
    client: &Client,
    address: &str,
    start_ms: i64,
    end_exclusive_ms: i64,
) -> Result<FillHistory, Box<dyn Error>> {
    let mut cursor = start_ms;
    let mut fills = Vec::new();
    let mut seen = HashSet::new();
    let mut complete = false;
    while cursor < end_exclusive_ms && fills.len() < AVAILABLE_FILLS_LIMIT {
        let page = post_info::<Vec<UserFill>>(
            client,
            json!({
                "type":"userFillsByTime", "user":address, "startTime":cursor,
                "endTime":end_exclusive_ms - 1, "aggregateByTime":true
            }),
        )
        .await?;
        if page.is_empty() {
            complete = true;
            break;
        }
        let page_len = page.len();
        let next_cursor = page
            .iter()
            .filter_map(|fill| fill.time)
            .max()
            .ok_or("Hyperliquid returned fills without timestamps")?
            + 1;
        for fill in page {
            let key = fill_key(&fill);
            if seen.insert(key) {
                fills.push(fill);
            }
        }
        if next_cursor <= cursor {
            return Err("fill pagination did not advance".into());
        }
        cursor = next_cursor;
        if page_len < FILLS_PAGE_LIMIT {
            complete = true;
            break;
        }
    }
    if cursor >= end_exclusive_ms {
        complete = true;
    }
    if fills.len() >= AVAILABLE_FILLS_LIMIT && cursor < end_exclusive_ms {
        complete = false;
    }
    fills.sort_by_key(|fill| fill.time.unwrap_or_default());
    Ok(FillHistory { fills, complete })
}

async fn post_info<T: DeserializeOwned>(
    client: &Client,
    body: serde_json::Value,
) -> Result<T, Box<dyn Error>> {
    for attempt in 1..=5 {
        sleep(INFO_REQUEST_INTERVAL).await;
        let response = client.post(INFO_URL).json(&body).send().await?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            if attempt == 5 {
                return Err("Hyperliquid rate limit persisted after 5 retries".into());
            }
            sleep(RATE_LIMIT_RETRY_DELAY).await;
            continue;
        }
        return Ok(response.error_for_status()?.json::<T>().await?);
    }
    unreachable!("bounded retry loop always returns")
}

fn fill_key(fill: &UserFill) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        fill.hash.as_deref().unwrap_or(""),
        fill.oid.unwrap_or_default(),
        fill.tid.unwrap_or_default(),
        fill.time.unwrap_or_default(),
        fill.px.as_deref().unwrap_or(""),
        fill.sz.as_deref().unwrap_or("")
    )
}

fn fill_closes_position(fill: &UserFill) -> bool {
    fill.dir
        .as_deref()
        .map(|dir| dir.starts_with("Close") || dir.starts_with("Liquidated"))
        .unwrap_or(false)
}

fn valid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && value[2..].bytes().any(|byte| byte != b'0')
}

fn parse_f64(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or_default()
}

fn parse_window(value: &str) -> Result<i64, Box<dyn Error>> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)?.timestamp_millis())
}

fn timestamp(value: Option<i64>) -> Option<String> {
    value
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
        .map(|time| time.to_rfc3339())
}

fn result_for<'a>(results: &'a [CandidateResult], address: &str) -> &'a CandidateResult {
    results
        .iter()
        .find(|result| result.address == address)
        .expect("vetted candidate must have a result")
}

fn write_json(path: &str, value: &impl Serialize) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = format!("{path}.tmp");
    fs::write(
        &temporary,
        format!("{}\n", serde_json::to_string_pretty(value)?),
    )?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_window_is_exactly_365_days() {
        assert_eq!(
            (parse_window(WINDOW_END_EXCLUSIVE).unwrap() - parse_window(WINDOW_START).unwrap())
                / 86_400_000,
            365
        );
    }

    #[test]
    fn address_validation_rejects_placeholder_and_bad_hex() {
        assert!(!valid_address("0x0000000000000000000000000000000000000000"));
        assert!(!valid_address("0xgg00000000000000000000000000000000000001"));
        assert!(valid_address("0x0000000000000000000000000000000000000001"));
    }

    #[test]
    fn hyperdash_snapshot_rejects_incomplete_cohort() {
        let snapshot = HyperdashCohortSnapshot {
            source: HYPERDASH_COHORT_URL.to_string(),
            cohort_id: HYPERDASH_COHORT_ID.to_string(),
            fetched_at: "2026-07-17T00:00:00Z".to_string(),
            total_traders: EXPECTED_COHORT_SIZE,
            total_count: EXPECTED_COHORT_SIZE,
            count: 1,
            unique_count: 1,
            complete: false,
            wallets: vec!["0x0000000000000000000000000000000000000001".to_string()],
        };
        assert!(seeds_from_hyperdash_snapshot(&snapshot).is_err());
    }

    #[test]
    fn truncated_ten_thousand_fill_history_uses_high_density_pathway() {
        let fill = UserFill {
            dir: None,
            closed_pnl: None,
            time: Some(0),
            hash: None,
            oid: None,
            tid: None,
            px: None,
            sz: None,
            fee: None,
        };
        let history = FillHistory {
            fills: vec![fill; AVAILABLE_FILLS_LIMIT],
            complete: false,
        };
        assert_eq!(pathway_for(&history).unwrap(), VettingPathway::HighDensity);
    }

    #[test]
    fn hourly_returns_include_zero_hours_and_all_fees() {
        let fill = |time, closed_pnl: &str, fee: &str| UserFill {
            dir: Some("Close Long".to_string()),
            closed_pnl: Some(closed_pnl.to_string()),
            time: Some(time),
            hash: None,
            oid: None,
            tid: None,
            px: None,
            sz: None,
            fee: Some(fee.to_string()),
        };
        let returns = hourly_net_returns(&[fill(0, "10", "1"), fill(2 * HOUR_MS, "0", "2")], 100.0);
        assert_eq!(returns, vec![0.09, 0.0, -0.02]);
    }

    #[test]
    fn pure_ingestion_requires_a_profitable_closed_sample() {
        assert!(ingestion_rejection_reasons(1, f64::MIN_POSITIVE).is_empty());
        assert_eq!(ingestion_rejection_reasons(0, 1.0).len(), 1);
        assert_eq!(ingestion_rejection_reasons(1, 0.0).len(), 1);
        assert_eq!(ingestion_rejection_reasons(0, -1.0).len(), 2);
    }
}
