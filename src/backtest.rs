use std::{collections::BTreeMap, error::Error, fs, path::Path};

const HOUR_MS: i64 = 60 * 60 * 1_000;
const HOURS_PER_YEAR: f64 = 365.25 * 24.0;
const ONE_SIDED_95_Z: f64 = 1.644_853_626_951_472_2;
const MIN_BACKTEST_DAYS: f64 = 365.0;
const MIN_HOURLY_COVERAGE: f64 = 0.80;
const TARGET_SHARPE: f64 = 15.0;
const REFERENCE_SHARPE: f64 = 17.8;

#[derive(Debug, Clone, Copy)]
struct ReturnPoint {
    timestamp_ms: i64,
    net_return: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SharpeStats {
    pub(crate) samples: usize,
    pub(crate) effective_samples: f64,
    mean: f64,
    std_dev: f64,
    pub(crate) annualized_sharpe: Option<f64>,
    pub(crate) lower_95_sharpe: Option<f64>,
}

pub fn run_backtest_report(path: &str) -> Result<(), Box<dyn Error>> {
    let points = load_return_points(Path::new(path))?;
    let hourly_returns = aggregate_hourly(&points);
    if hourly_returns.len() < 10 {
        return Err("backtest requires at least ten hourly return buckets".into());
    }

    let first_hour = hourly_returns[0].0;
    let last_hour = hourly_returns[hourly_returns.len() - 1].0;
    let covered_hours = ((last_hour - first_hour) / HOUR_MS + 1).max(1) as usize;
    let elapsed_days = covered_hours as f64 / 24.0;
    let coverage = hourly_returns.len() as f64 / covered_hours as f64;
    let returns = hourly_returns
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    let overall = sharpe_stats(&returns, HOURS_PER_YEAR, 24);

    let train_end = returns.len() * 60 / 100;
    let validation_end = returns.len() * 80 / 100;
    let train = sharpe_stats(&returns[..train_end.max(2)], HOURS_PER_YEAR, 24);
    let validation = sharpe_stats(
        &returns[train_end..validation_end.max(train_end + 2)],
        HOURS_PER_YEAR,
        24,
    );
    let test = sharpe_stats(&returns[validation_end..], HOURS_PER_YEAR, 24);

    println!("backtest source: {path}");
    println!(
        "hourly buckets: {}, elapsed_days={elapsed_days:.1}, coverage={:.1}%",
        returns.len(),
        coverage * 100.0
    );
    print_stats("overall", overall);
    print_stats("train 60%", train);
    print_stats("validation 20%", validation);
    print_stats("test 20%", test);

    println!("non-overlapping horizon diagnostics:");
    for window_hours in [4usize, 8, 12, 24, 48, 96] {
        let window_returns = compound_non_overlapping(&returns, window_hours);
        let stats = sharpe_stats(&window_returns, HOURS_PER_YEAR / window_hours as f64, 1);
        println!(
            "  {:>2}h: n={:<5} effective_n={:<7.1} sharpe={:<8} lower95={}",
            window_hours,
            stats.samples,
            stats.effective_samples,
            format_optional(stats.annualized_sharpe),
            format_optional(stats.lower_95_sharpe),
        );
    }

    let required_96h =
        minimum_independent_samples(REFERENCE_SHARPE, TARGET_SHARPE, HOURS_PER_YEAR / 96.0);
    println!(
        "reference requirement: observed Sharpe {REFERENCE_SHARPE:.1} needs at least {required_96h} independent 96h windows (~{:.0} days) for a one-sided 95% lower bound above {TARGET_SHARPE:.1}",
        required_96h as f64 * 4.0
    );

    let mut failures = Vec::new();
    if elapsed_days < MIN_BACKTEST_DAYS {
        failures.push(format!(
            "history is {elapsed_days:.1} days; require at least {MIN_BACKTEST_DAYS:.0}"
        ));
    }
    if coverage < MIN_HOURLY_COVERAGE {
        failures.push(format!(
            "hourly coverage is {:.1}%; require at least {:.0}%",
            coverage * 100.0,
            MIN_HOURLY_COVERAGE * 100.0
        ));
    }
    if overall.lower_95_sharpe.unwrap_or(f64::NEG_INFINITY) < TARGET_SHARPE {
        failures.push(format!(
            "overall lower 95% Sharpe is {}; require at least {TARGET_SHARPE:.1}",
            format_optional(overall.lower_95_sharpe)
        ));
    }
    for (label, stats) in [("validation", validation), ("test", test)] {
        if stats.annualized_sharpe.unwrap_or(f64::NEG_INFINITY) < TARGET_SHARPE {
            failures.push(format!(
                "{label} Sharpe is {}; require at least {TARGET_SHARPE:.1}",
                format_optional(stats.annualized_sharpe)
            ));
        }
    }

    if failures.is_empty() {
        println!("SHARPE GATE: PASS");
        Ok(())
    } else {
        println!("SHARPE GATE: FAIL");
        for failure in &failures {
            println!("  - {failure}");
        }
        Err(format!(
            "backtest did not satisfy {} acceptance gate(s)",
            failures.len()
        )
        .into())
    }
}

fn load_return_points(path: &Path) -> Result<Vec<ReturnPoint>, Box<dyn Error>> {
    let text = fs::read_to_string(path)?;
    let mut points = Vec::new();
    for (line_index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let columns = line.split(',').map(str::trim).collect::<Vec<_>>();
        if columns.len() < 2 {
            return Err(format!(
                "{}:{} expected timestamp_ms,net_return",
                path.display(),
                line_index + 1
            )
            .into());
        }
        if points.is_empty() && columns[0].parse::<i64>().is_err() {
            continue;
        }

        let timestamp_ms = columns[0].parse::<i64>().map_err(|error| {
            format!(
                "{}:{} invalid timestamp_ms: {error}",
                path.display(),
                line_index + 1
            )
        })?;
        let net_return = columns[1].parse::<f64>().map_err(|error| {
            format!(
                "{}:{} invalid net_return: {error}",
                path.display(),
                line_index + 1
            )
        })?;
        if !net_return.is_finite() || net_return <= -1.0 {
            return Err(format!(
                "{}:{} net_return must be finite and greater than -1.0",
                path.display(),
                line_index + 1
            )
            .into());
        }
        points.push(ReturnPoint {
            timestamp_ms,
            net_return,
        });
    }

    points.sort_by_key(|point| point.timestamp_ms);
    Ok(points)
}

fn aggregate_hourly(points: &[ReturnPoint]) -> Vec<(i64, f64)> {
    let mut hourly_growth = BTreeMap::<i64, f64>::new();
    for point in points {
        let hour = point.timestamp_ms.div_euclid(HOUR_MS) * HOUR_MS;
        *hourly_growth.entry(hour).or_insert(1.0) *= 1.0 + point.net_return;
    }
    hourly_growth
        .into_iter()
        .map(|(hour, growth)| (hour, growth - 1.0))
        .collect()
}

fn compound_non_overlapping(returns: &[f64], window: usize) -> Vec<f64> {
    returns
        .chunks_exact(window)
        .map(|chunk| {
            chunk
                .iter()
                .fold(1.0, |growth, value| growth * (1.0 + value))
                - 1.0
        })
        .collect()
}

pub(crate) fn sharpe_stats(
    returns: &[f64],
    periods_per_year: f64,
    max_autocorrelation_lag: usize,
) -> SharpeStats {
    if returns.len() < 2 {
        return SharpeStats {
            samples: returns.len(),
            ..Default::default()
        };
    }

    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    let std_dev = variance.sqrt();
    if std_dev == 0.0 {
        return SharpeStats {
            samples: returns.len(),
            effective_samples: returns.len() as f64,
            mean,
            std_dev,
            annualized_sharpe: if mean > 0.0 {
                Some(f64::INFINITY)
            } else {
                None
            },
            lower_95_sharpe: None,
        };
    }

    let period_sharpe = mean / std_dev;
    let annualized_sharpe = period_sharpe * periods_per_year.sqrt();
    let effective_samples = effective_sample_size(returns, mean, variance, max_autocorrelation_lag);
    let annualized_standard_error =
        (periods_per_year * (1.0 + 0.5 * period_sharpe.powi(2)) / effective_samples).sqrt();
    let lower_95_sharpe = annualized_sharpe - ONE_SIDED_95_Z * annualized_standard_error;

    SharpeStats {
        samples: returns.len(),
        effective_samples,
        mean,
        std_dev,
        annualized_sharpe: Some(annualized_sharpe),
        lower_95_sharpe: Some(lower_95_sharpe),
    }
}

fn effective_sample_size(returns: &[f64], mean: f64, variance: f64, max_lag: usize) -> f64 {
    if variance <= 0.0 || returns.len() < 3 {
        return returns.len() as f64;
    }
    let max_lag = max_lag.min(returns.len() - 2);
    let mut positive_autocorrelation_sum = 0.0;
    for lag in 1..=max_lag {
        let covariance = returns[..returns.len() - lag]
            .iter()
            .zip(&returns[lag..])
            .map(|(left, right)| (left - mean) * (right - mean))
            .sum::<f64>()
            / (returns.len() - lag) as f64;
        positive_autocorrelation_sum += (covariance / variance).max(0.0);
    }
    (returns.len() as f64 / (1.0 + 2.0 * positive_autocorrelation_sum))
        .clamp(2.0, returns.len() as f64)
}

fn minimum_independent_samples(
    observed_annual_sharpe: f64,
    target_annual_sharpe: f64,
    periods_per_year: f64,
) -> usize {
    if observed_annual_sharpe <= target_annual_sharpe {
        return usize::MAX;
    }
    let period_sharpe = observed_annual_sharpe / periods_per_year.sqrt();
    let annualized_variance_factor = periods_per_year * (1.0 + 0.5 * period_sharpe.powi(2));
    ((ONE_SIDED_95_Z * annualized_variance_factor.sqrt()
        / (observed_annual_sharpe - target_annual_sharpe))
        .powi(2))
    .ceil() as usize
}

fn print_stats(label: &str, stats: SharpeStats) {
    println!(
        "{label}: n={}, effective_n={:.1}, mean={:.8}, std={:.8}, sharpe={}, lower95={}",
        stats.samples,
        stats.effective_samples,
        stats.mean,
        stats.std_dev,
        format_optional(stats.annualized_sharpe),
        format_optional(stats.lower_95_sharpe),
    );
}

fn format_optional(value: Option<f64>) -> String {
    match value {
        Some(value) if value.is_finite() => format!("{value:.2}"),
        Some(value) if value.is_sign_positive() => "inf".to_string(),
        Some(_) => "-inf".to_string(),
        None => "n/a".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hourly_aggregation_compounds_duplicate_hours() {
        let points = vec![
            ReturnPoint {
                timestamp_ms: HOUR_MS,
                net_return: 0.10,
            },
            ReturnPoint {
                timestamp_ms: HOUR_MS + 1,
                net_return: -0.05,
            },
        ];

        let hourly = aggregate_hourly(&points);

        assert_eq!(hourly.len(), 1);
        assert!((hourly[0].1 - 0.045).abs() < 0.000_001);
    }

    #[test]
    fn reference_target_requires_about_a_year_of_96h_windows() {
        let samples = minimum_independent_samples(17.8, 15.0, HOURS_PER_YEAR / 96.0);

        assert_eq!(samples, 87);
        assert_eq!(samples * 4, 348);
    }

    #[test]
    fn positive_autocorrelation_reduces_effective_sample_size() {
        let returns = (0..200)
            .map(|index| if index % 20 < 10 { 0.01 } else { -0.005 })
            .collect::<Vec<_>>();
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (returns.len() - 1) as f64;

        assert!(effective_sample_size(&returns, mean, variance, 10) < returns.len() as f64);
    }
}
