# Hyperliquid Trading Tools

Rust tooling for Hyperliquid market monitoring and copy-trading research.

The original default mode still runs the Bybit/Hyperliquid arbitrage monitor. Production
copy-trading is split between signer-free observer/core crates and the isolated
`copytrade-signer` binary.

## Copytrade Scalping Mode

> **Legacy lock:** live copytrading and agent approval through the root binary are disabled. Both
> `--copytrade` and `--approve-agent` fail closed before configuration, secret loading,
> signer construction, or network mutation. Production submission exists only in the separate,
> manifest-bound `copytrade-signer` executable.

HL1A introduces schema version `1`, an explicit `global_risk` block, and a mandatory
`confidence_modifier` for every candidate. Live configuration validation rejects missing,
duplicate, non-finite, out-of-range, or inconsistent values. The initial launch-scale
configuration is `global_risk_scale = 0.10`, `max_single_asset_equity_pct = 0.65`,
`max_net_equity_pct = 0.65`, `max_source_exposure = 1.0`, and an asset-specific dynamic
order floor above Hyperliquid's exchange minimum.

HL1B adds one signer-free decimal portfolio-projection boundary in
`crates/copytrade-core/src/portfolio_risk.rs`.
It clamps unleveraged single-asset exposure, scales the complete vector to the launch gross
cap, enforces signed net exposure, accounts for filled and acknowledged open exposure as
worst-case ranges, rounds against explicit market rules, and rejects any post-rounding
violation. Cancel-requested orders remain exposed until cancellation is confirmed. Existing
over-limit positions can produce recovery-only reductions but cannot increase exposure.

HL1C places configuration, normalized consensus, and portfolio projection in the
`copytrade-core` crate and provides a dedicated `copytrade-observer` executable. The observer
depends only on the signer-free core, validates configuration, constructs read-only state, and
prints deterministic build/configuration metadata. It contains no authenticated exchange client,
key loader, approval path, mutation command, or network runtime. Run the permanent dependency,
source, negative-compile, environment-trap, and release-artifact checks with:

```bash
bash scripts/check_hl1c_isolation.sh
```

The only observer CLI operations are read-only:

```bash
cargo run -p copytrade-observer -- --config config/copytrade.json
cargo run -p copytrade-observer -- --config config/copytrade.json --validate-config
cargo run -p copytrade-observer -- --config config/copytrade.json --print-build-manifest
cargo run -p copytrade-observer -- --config config/copytrade.json --check-dependency-policy
cargo run -p copytrade-observer -- observe --config config/copytrade.json \
  --request-policy config/read-api-policy.json
```

HL1D adds a deterministic weighted read scheduler to `copytrade-core`. Every request uses an
explicit read kind and policy weight; normal polling cannot consume the reserved recovery budget.
Concurrency permits, pending work, and one possible in-flight successor are bounded independently.
Expired and superseded work is discarded before transport, while rate limits and transient failures
use deterministic bounded backoff. Source snapshots carry request, receipt, source-observation,
expiration, and sequence metadata and fail closed when stale, future-dated, malformed, or out of
order. The observer `observe` command only validates and schedules modeled reads during HL1D; it
does not perform network transport or construct trading targets.

HL1E adds canonical snapshot-set, decision, target-version, and planned-CLOID identities plus a
bounded signer-free lifecycle registry. `plan` emits inert actions only. HL1F adds deterministic
IOC depth replay under fixed latency scenarios, and `shadow` reports modeled fills explicitly as
unsigned shadow results. HL1G records closed position episodes in independent portfolio and source
books with atomic restart snapshots. HL1H applies reversible discrete confidence decay and
quarantine rules. The combined failure-injection and signing-isolation gate is:

```bash
bash scripts/check_hl1i_qualification.sh
```

API-wallet authorization is external to this runtime. The master key is never loaded by either
copytrade binary. The signer accepts only a chmod-0600 API-wallet secret file and refuses to start
unless its exact binary and the clean production release policy are manifest-bound. Do not use
the disabled root `--approve-agent` or `--copytrade` commands.

Use Hyperdash Explore/Copytrading only as a discovery front-end. Paste interesting wallet addresses into `config/copytrade.json`; the bot does its own vetting directly against Hyperliquid public account state and fills.

### Offline candidate seeding

Candidate discovery and historical vetting run without an agent key and cannot place orders. The routine consumes the verified 304-wallet snapshot of Hyperdash's `Extremely Profitable` cohort and applies the local gates over the fixed half-open UTC window `[2025-07-17, 2026-07-17)` (exactly 365 days). It fails closed unless the source metadata and all 304 unique addresses are present; it never falls back to the Hyperliquid public leaderboard:

```bash
cargo run -- --seed-candidates
```

By default this writes an audit report to `data/candidate-vetting-2025-07-17_2026-07-17.json` and a reviewable config to `config/copytrade.vetted.json`. It does not touch `config/copytrade.json`. After reviewing the report, rerun with explicit promotion:

```bash
cargo run -- --seed-candidates \
  --promote-candidates
```

Optional paths are `--cohort-file`, `--base-config`, `--staged-config`, `--candidate-report`, and `--production-config`. Promotion is refused when no wallet passes. `userFillsByTime` is paginated at 2,000 fills per response and Hyperliquid exposes only a user's most recent 10,000 fills.

Offline seeding is a pure ingestion funnel. A cohort wallet is included when it has positive live equity, at least one closed fill in the retained audit history, and positive fee-adjusted empirical PnL (`sum(closedPnl) - sum(all fill fees) > 0`). Complete and 10,000-fill-truncated histories are both accepted. Breadth, win rate, confidence, autocorrelation-adjusted Sharpe, and its one-sided 95% lower bound are recorded for analysis but are not offline admission gates. Hourly returns include zero-return hours and subtract all recorded fill fees.

The live engine separately filters and monitors candidates according to its configured runtime gates:

- recent realized win rate from Hyperliquid `userFillsByTime` >= `98.0%`
- derived confidence score >= `95.0`
- positive live Hyperliquid account value (`> $0`), with no upper wallet-equity ceiling
- at least `min_closed_trades` in the vetting window
- closed PnL >= `min_closed_pnl_usd`
- `enabled: true`

There is no default wallet ceiling. `unlimited_mode: true` or `max_active_traders: 0` allows every vetted candidate to contribute to the portfolio.

The signer must point at a Hyperliquid account with USDC collateral. The agent wallet can trade for the approved master account, but it cannot withdraw funds.

## Copytrade Risk Model

The planner copies exposure, not raw fills. For each followed wallet:

```text
trader_exposure = trader_position_notional / trader_account_value
raw_target_notional = trader_exposure * follower_equity * leverage_curve * allocation_weight * confidence_modifier
target_notional = min(raw_target_notional, asset_capacity, l2_book_depth / concurrency_factor)
```

Then it applies risk gates:

- starting equity defaults to `$100`
- local candidate vetting defaults to a `14` day lookback and `50` minimum closed trades
- account drawdown stop defaults to `8%`
- launch curve leverage is fixed at `8.75x` for the approximately `$100` account
- `max_total_leverage` remains a hard ceiling over the curve
- per-asset exposure is capped at `65%` of current unleveraged equity
- launch gross exposure is capped at `equity × curve leverage × global_risk_scale`
- worst-case signed net exposure is capped at `65%` of current unleveraged equity
- single rebalance order defaults to `$2,500`
- the order floor is derived from current exchange minimums, tick/lot rounding, and the configured buffer
- maker fee estimate defaults to `1.5 bps` (`0.015%`)
- taker fee estimate defaults to `4.5 bps` (`0.045%`)
- dynamic slippage defaults to enabled with a `4 bps` ceiling
- orders are sent as IOC limits with a `200ms` exchange timeout
- signed orders are confidence-prioritized and throttled through a token bucket
- realized follower fills are measured net of fees over a `96h` Sharpe window
- new risk is gated once at least `50` closed-fill samples produce annualized Sharpe below `15.0`
- per-trader SignalLedger attribution persists to `config/ledger.json`
- isolated trader Sharpe below target decays that wallet's confidence modifier; below `10.0` it drops to zero
- ledger entries for traders inactive longer than `168h` are pruned automatically
- long/short signals on the same asset are arithmetically netted before execution

The engine submits deterministic exchange-side `cloid` values and uses them to reconcile follower fills back to SignalLedger attribution.

Every rebalance considers the union of desired master assets and current follower assets. If a master position disappears, the follower therefore receives an explicit zero target and exits reduce-only. Direction flips are staged: the existing side is closed first, then the opposite side may open on a later tick. Drawdown and Sharpe guards block new risk while preserving these reductions.

Copytrade state is polled at the configured cadence (`20s` by default). This is deterministic polling, not a claim of WebSocket-level reaction time; lower intervals increase Hyperliquid API load as the candidate list grows.

## Sharpe Backtest Gate

`sharpe.png` is a target visualization, not a validated backtest artifact. A valid input must contain chronological, net-of-fees-and-slippage portfolio returns with one row for every UTC hour, including zero-return hours:

```csv
timestamp_ms,net_return
1704067200000,0.00012
1704070800000,0.0
```

Run the statistical gate with:

```bash
cargo run -- --backtest-returns data/portfolio_hourly_returns.csv
```

The report uses fixed hourly returns, non-overlapping `4h` through `96h` diagnostics, an autocorrelation-adjusted effective sample count, a one-sided 95% Sharpe lower bound, and chronological `60/20/20` train/validation/test splits. Passing requires at least `365` elapsed days, `80%` hourly coverage, an overall lower confidence bound of at least `15`, and validation/test point estimates of at least `15`.

At the chart's approximate `17.8` observed 96-hour Sharpe, the optimistic IID-normal calculation still requires at least `87` independent 96-hour windows—about `348` days—before the one-sided 95% lower bound exceeds `15`. Strategy variants and wallet-selection trials should additionally be tracked for a deflated-Sharpe correction.

By default the engine uses the signer address as the follower account, so it accounts for existing positions before rebalancing. Set `COPYTRADE_FOLLOWER_ADDRESS` or `follower_address` only when trading on behalf of a different account.

## Config Overrides

Configuration is loaded from `config/copytrade.json` by default. Override the path:

```bash
COPYTRADE_CONFIG=/path/to/copytrade.json cargo run -- --copytrade
```

Useful environment overrides:

```env
COPYTRADE_UNLIMITED_MODE=true
COPYTRADE_STARTING_EQUITY_USD=100
COPYTRADE_MIN_WIN_RATE_PCT=98
COPYTRADE_MIN_TRADER_CONFIDENCE=95
COPYTRADE_MIN_CLOSED_TRADES=50
COPYTRADE_VETTING_LOOKBACK_DAYS=14
COPYTRADE_MIN_CLOSED_PNL_USD=0
COPYTRADE_MAX_ACCOUNT_DRAWDOWN_PCT=8
COPYTRADE_MAX_TOTAL_LEVERAGE=8
COPYTRADE_PER_ASSET_MAX_EXPOSURE_PCT=65
COPYTRADE_LEDGER_PATH=config/ledger.json
COPYTRADE_MAKER_FEE_BPS=1.5
COPYTRADE_TAKER_FEE_BPS=4.5
COPYTRADE_DYNAMIC_SLIPPAGE=true
COPYTRADE_LATENCY_TIMEOUT_MS=200
COPYTRADE_ORDER_RATE_LIMIT_PER_SEC=5
COPYTRADE_SHARPE_TARGET=15
COPYTRADE_SHARPE_LOOKBACK_HOURS=96
COPYTRADE_MIN_SHARPE_SAMPLES=50
COPYTRADE_ENFORCE_SHARPE_GATE=true
COPYTRADE_TRADER_SHARPE_FLOOR=10
COPYTRADE_CONFIDENCE_DECAY_LAMBDA=0.32
COPYTRADE_PRUNE_INACTIVE_TRADER_HOURS=168
COPYTRADE_FOLLOWER_ADDRESS=0x...
```

## API Notes

- Hyperliquid Info endpoint: `POST https://api.hyperliquid.xyz/info`
- Hyperliquid Exchange endpoint: `POST https://api.hyperliquid.xyz/exchange`
- The engine uses `allMids` for marks, `l2Book` for execution capacity, `clearinghouseState` for copied/follower account positions, and `userFillsByTime` for local win-rate and closed-PnL vetting.
- The isolated signer uses a typed authenticated IOC transport and typed reconciliation reads;
  observer code cannot import it.
- Hyperdash is not trusted for scoring or execution. It is useful for finding addresses, but the monitor/risk/execution pipeline is built directly on Hyperliquid's public API.
- Hyperdash docs describe their managed copytrading as exposure-based portfolio replication. This implementation follows that exposure model while keeping our own candidate selection and risk gates.

## Default Arbitrage Mode

Run without flags:

```bash
cargo run
```

This starts the existing Bybit/Hyperliquid monitor and Twitter alert flow.
