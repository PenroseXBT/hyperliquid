# HD1 — Hyperliquid Streaming Data Plane

HD1 changes public read ingestion only. MFCE features, allocation, portfolio
risk, reduce-only behavior, target/ledger accounting, signer isolation, and
execution reconciliation are unchanged.

## Runtime contract

1. Subscribe once to `allDexsAssetCtxs`.
2. Fetch `perpDexs` plus `allPerpMetas` at startup and every six hours.
3. Subscribe to public `trades` for every discovered perpetual market when the
   universe fits the provider-safe 850-market budget. If it does not, active
   source and MFCE-hot markets remain pinned while only cold markets rotate.
   Builder markets retain Hyperliquid's `{dex}:{coin}` names.
4. Filter trade messages inside the WebSocket actor against the bounded 375
   address set. Untracked trades never cross the actor channel.
5. Buffer and deduplicate tracked trades by `(time, coin, tid)` while REST
   baselines are being hydrated. A buyer adds fill size and a seller subtracts
   fill size from the one-way source position.
6. A source market is eligible for new MFCE risk only after every wallet has a
   REST baseline newer than its last stream/subscription gap and its trade
   coverage has remained continuous. Wallet inactivity does not make it stale.
7. Disconnect or 15-second stream silence invalidates all source freshness and
   starts one staggered reconciliation epoch. Trading remains fail-closed until
   every baseline has been reconciled and buffered fills have been replayed.
8. L2 book subscriptions follow the existing MFCE desired-book set. Assets are
   unsubscribed after a bounded 30-second grace period.
9. REST source reconciliation runs hourly and after stream gaps. It is spread
   over thirty minutes; it is not recurring ingestion. At the conservative
   all-DEX charge of 42 weight per wallet, this is long enough to hydrate all
   375 baselines without queue-expiry pressure.
10. A rotated-out market is explicitly coverage-stale. Resubscription pins it
    and starts a priority, token-budgeted 375-wallet reconciliation; it remains
    ineligible for new risk until every post-gap response completes. Coverage
    uncertainty is not an economic MFCE Reject and cannot block reductions or
    flattens.

## REST budget

The scheduler operates below the public external ceiling:

- Operating ceiling: 900 weight/minute.
- Protected reserve: 120 weight/minute.
- Source bootstrap/reconciliation sleeve: 700 weight/minute.
- `clearinghouseState`: 2 per DEX, conservatively charged as 42 per wallet for
  the default DEX plus up to twenty builder DEXes.
- Metadata discovery: 60, covering `allPerpMetas`, `perpDexs`, and optional
  follower `userFees` at 20 each.
- Physical REST concurrency remains bounded at eight: two scheduler jobs, each
  with at most four DEX reads in flight.

The queue remains 256. No capacity increase or retry subsystem was added.
Retries retain the same logical `RequestKey`, and replaceable refresh shedding
remains nonfatal.

## Bounded state

- At most 512 tracked wallets (375 configured).
- At most 850 simultaneous trade-market subscriptions and 1,000 total
  WebSocket subscriptions; discovered metadata remains bounded by the
  transport response limit.
- At most 128 hot L2 books.
- At most 32,768 tracked buffered trades.
- At most 65,536 deduplication identities.
- One public WebSocket connection with exponential reconnect backoff.

Technical candle REST polling is disabled in streaming mode. Persisted candle
state is not served as current context; technical context is explicitly
missing until fresh candles are aggregated from the live trade stream.

The default production binary excludes the legacy research CLI. Deterministic
qualification/replay commands are CI/development tools and require an explicit
`--features research-cli` build; the Railway daemon build has no such feature.
