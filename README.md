# Hyperliquid

A deterministic, risk-bounded Hyperliquid copy-trading engine. Accepted wallet and cohort exposure transitions are the only candidate generator. One embedded MFCE engine conditions q10/q50 gross-transition-return estimates on source conviction, volatility, liquidity, transition state, and read-only multi-timeframe technical context.

Execution friction is computed separately from the current order book, the follower's live `userFees` taker rate, and live funding. Risk increases require the conditional median to clear friction plus uncertainty and the conditional q10 to fit the remaining position-aware tail budget. Reductions and exits retain the existing reduce-only, target-ledger, accounting, projection, and signing paths.

MFCE runs entirely in the observer process: Rust owns bounded samples and durable state, while statically linked LightGBM C++ is accessed through an observer-only safe wrapper. Chronological validation happens off the hot path, and q10/q50 model strings are promoted as one pair. No Python pipeline, model service, additional database, or standalone model artifacts are used.

Native dependency attribution and exact source locations are recorded in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
