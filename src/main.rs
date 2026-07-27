use crate::share_state::SharedState;
use backtest::run_backtest_report;
use bybit::Bybit;
use candidate_vetting::{seed_candidates, SeedOptions};
use copytrade_core::CopyTradeConfig;
use hyperliquid::HyperLiquidStruct;
use std::{env, process, sync::Arc, time::Duration};
use tokio::time::interval;

mod backtest;
mod bybit;
mod candidate_vetting;
mod compare_price;
#[allow(dead_code)] // HL1 intentionally disconnects the legacy live runtime from the binary.
mod copytrade;
mod create_tweet;
mod hyperliquid;
#[allow(dead_code)] // HL1B exposes this through tests until the observer target exists.
mod portfolio_risk;
mod share_state;
mod utils;

fn get_common_tickers(bybit_tickers: Vec<String>, hyperliquid_tickers: Vec<String>) -> Vec<String> {
    let common_tickers: Vec<String> = bybit_tickers
        .iter()
        .filter(|ticker| hyperliquid_tickers.contains(&ticker))
        .cloned()
        .collect();
    common_tickers
}

#[tokio::main]
async fn main() {
    let args = env::args().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--seed-candidates") {
        let mut options = SeedOptions::default();
        options.base_config = option_value(&args, "--base-config").unwrap_or(options.base_config);
        options.staged_config =
            option_value(&args, "--staged-config").unwrap_or(options.staged_config);
        options.report = option_value(&args, "--candidate-report").unwrap_or(options.report);
        options.cohort_file = option_value(&args, "--cohort-file").unwrap_or(options.cohort_file);
        if args.iter().any(|arg| arg == "--promote-candidates") {
            options.promote_to = Some(
                option_value(&args, "--production-config")
                    .unwrap_or_else(|| "config/copytrade.json".to_string()),
            );
        }
        if let Err(error) = seed_candidates(options).await {
            eprintln!("candidate seeding failed: {error}");
            process::exit(1);
        }
        return;
    }

    if let Some(index) = args.iter().position(|arg| arg == "--backtest-returns") {
        let path = args
            .get(index + 1)
            .expect("--backtest-returns requires a CSV path");
        if let Err(error) = run_backtest_report(path) {
            eprintln!("backtest Sharpe gate failed: {error}");
            process::exit(1);
        }
        return;
    }

    if args.iter().any(|arg| arg == "--validate-copytrade-config") {
        let path =
            option_value(&args, "--config").unwrap_or_else(|| "config/copytrade.json".to_string());
        let config = CopyTradeConfig::from_path(&path).expect("failed loading copytrade config");
        config
            .validate_production()
            .expect("copytrade configuration failed HL1A validation");
        println!("copytrade configuration passed HL1A validation: {path}");
        return;
    }

    if args.iter().any(|arg| arg == "--approve-agent") {
        eprintln!("agent approval is disabled throughout HL1 qualification");
        process::exit(1);
    }

    if args.iter().any(|arg| arg == "--copytrade") {
        eprintln!("copytrade live runtime is disabled throughout HL1 qualification");
        process::exit(1);
    }

    let hyper_liquid = HyperLiquidStruct::new().await;
    let bybit = Bybit::new();
    let shared_state = Arc::new(SharedState::new());

    let hyperliquid_tickers = hyper_liquid.get_tickers().await;

    let bybit_tickers = bybit
        .get_tickers()
        .await
        .expect("Error calling bybit get tickers");

    let common_tickers = get_common_tickers(bybit_tickers, hyperliquid_tickers);

    {
        let mut bybit_prices = shared_state.bybit_prices.write().await;
        let mut hyperliquid_price = shared_state.hyperliquid_prices.write().await;
        for ticker in &common_tickers {
            bybit_prices.insert(ticker.clone(), 0.0);
            hyperliquid_price.insert(ticker.clone(), 0.0);
        }
    }

    let shared_state_clone_reset = Arc::clone(&shared_state);
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            {
                let mut tweet_symbols = shared_state_clone_reset.tweeted_symbols.write().await;
                tweet_symbols.clear();
                println!("**************************************** Tweet symbols reset ****************************************");
            }
        }
    });

    tokio::join!(
        hyper_liquid.hyperliquid_ws(&shared_state),
        bybit.bybit_ws(&common_tickers, &shared_state)
    );
}

fn option_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .cloned()
}
