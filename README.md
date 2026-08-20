# Hyperliquid

The continuous observer's public ingestion plane is documented in
`HD1_STREAMING_DATA_PLANE.md`. It uses public all-perp trade and market-context
streams with demand-driven L2 books; REST is limited to bounded bootstrap,
reconciliation, slow metadata, and follower/account safety reads.

A deterministic, risk-bounded Hyperliquid copy-trading engine. Accepted wallet and cohort exposure transitions are the only candidate generator. One embedded MFCE engine conditions q10/q50 gross-transition-return estimates on source conviction, volatility, liquidity, transition state, and read-only multi-timeframe technical context.

Execution friction is computed separately from the current order book, the follower's live `userFees` taker rate, and live funding. MFCE classifies risk increases as `exploit`, `explore`, or `reject`, then ranks the complete current transition set by after-cost edge per unit of conditional downside. Before an incumbent exists, structurally valid source transitions default to small conviction-prior Explore allocations so learning cannot deadlock. Exploit opportunities compete for the main source sleeve, while all simultaneous Explore opportunities share one bounded global sleeve. Only model-backed negative expectancy beyond its uncertainty band earns an economic Reject; q10 and uncertainty otherwise shrink sizing, with genuine tail-budget exhaustion still failing closed. Reductions and exits retain the existing reduce-only, target-ledger, accounting, projection, and signing paths.

MFCE owns economic admission and relative source-risk allocation. The downstream portfolio projection remains authoritative for single-asset, signed-net, gross leverage, liquidity/exchange, reduce-only, accounting, and root-integrity invariants. See [MFCE_ALLOCATION_PLAN.md](MFCE_ALLOCATION_PLAN.md) for the staged rollout.

MFCE runs entirely in the observer process: Rust owns bounded samples and durable state, while statically linked LightGBM C++ is accessed through an observer-only safe wrapper. Chronological validation happens off the hot path, and q10/q50 model strings are promoted as one pair. No Python pipeline, model service, additional database, or standalone model artifacts are used.

Native dependency attribution and exact source locations are recorded in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
