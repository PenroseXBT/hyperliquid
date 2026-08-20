//! Event-driven public Hyperliquid ingestion for the unsigned observer.
//!
//! The stream carries only public market data. It has no signing dependency
//! and cannot submit mutations. REST remains the authoritative bootstrap and
//! reconciliation source; stream gaps therefore fail source freshness closed
//! until a bounded reconciliation sweep completes.

use crate::public_mainnet::{
    BookLevel, MarketAssetContext, MarketMetadataAsset, MarketMetadataResponse,
    MarketSnapshotResponse, OrderBookResponse, SourceAssetPosition, SourceStateResponse,
};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const APPROVED_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const MAX_TRACKED_WALLETS: usize = 512;
const MAX_MARKETS: usize = 850;
pub(crate) const MAX_HOT_BOOKS: usize = 128;
const MAX_BUFFERED_TRADES: usize = 32_768;
const MAX_DEDUP_TRADES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamingPolicy {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_initial_backoff_ms: u64,
    #[serde(default = "default_reconnect_maximum_ms")]
    pub reconnect_maximum_backoff_ms: u64,
    #[serde(default = "default_reconciliation_interval_ms")]
    pub reconciliation_interval_ms: u64,
    #[serde(default = "default_reconciliation_spread_ms")]
    pub reconciliation_spread_ms: u64,
    #[serde(default = "default_hot_book_grace_ms")]
    pub hot_book_grace_ms: u64,
    #[serde(default = "default_maximum_message_bytes")]
    pub maximum_message_bytes: usize,
}

impl Default for StreamingPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_timeout_ms: default_idle_timeout_ms(),
            reconnect_initial_backoff_ms: default_reconnect_backoff_ms(),
            reconnect_maximum_backoff_ms: default_reconnect_maximum_ms(),
            reconciliation_interval_ms: default_reconciliation_interval_ms(),
            reconciliation_spread_ms: default_reconciliation_spread_ms(),
            hot_book_grace_ms: default_hot_book_grace_ms(),
            maximum_message_bytes: default_maximum_message_bytes(),
        }
    }
}

impl StreamingPolicy {
    pub fn validate(&self) -> Result<(), StreamingError> {
        if self.idle_timeout_ms < 5_000
            || self.reconnect_initial_backoff_ms == 0
            || self.reconnect_maximum_backoff_ms < self.reconnect_initial_backoff_ms
            || self.reconciliation_interval_ms < 10 * 60_000
            || self.reconciliation_spread_ms < 60_000
            || self.reconciliation_spread_ms >= self.reconciliation_interval_ms
            || self.hot_book_grace_ms < 5_000
            || !(1_024..=16 * 1024 * 1024).contains(&self.maximum_message_bytes)
        {
            return Err(StreamingError::InvalidPolicy);
        }
        Ok(())
    }
}

const fn default_idle_timeout_ms() -> u64 {
    15_000
}
const fn default_reconnect_backoff_ms() -> u64 {
    1_000
}
const fn default_reconnect_maximum_ms() -> u64 {
    30_000
}
const fn default_reconciliation_interval_ms() -> u64 {
    60 * 60_000
}
const fn default_reconciliation_spread_ms() -> u64 {
    30 * 60_000
}
const fn default_hot_book_grace_ms() -> u64 {
    30_000
}
const fn default_maximum_message_bytes() -> usize {
    4 * 1024 * 1024
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingError {
    InvalidPolicy,
    Capacity,
    InvalidPayload,
    ChannelClosed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicTrade {
    pub coin: String,
    pub price: Decimal,
    pub size: Decimal,
    pub time_ms: u64,
    pub trade_id: u64,
    pub hash: String,
    pub buyer: String,
    pub seller: String,
}

impl PublicTrade {
    fn identity(&self) -> TradeIdentity {
        TradeIdentity {
            time_ms: self.time_ms,
            coin: self.coin.clone(),
            trade_id: self.trade_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TradeIdentity {
    time_ms: u64,
    coin: String,
    trade_id: u64,
}

#[derive(Debug, Clone)]
pub enum StreamingEvent {
    Connected { epoch: u64 },
    Gap { epoch: u64 },
    Trades(Vec<PublicTrade>),
    AssetContexts(Value),
    OrderBook(OrderBookResponse),
}

#[derive(Debug)]
enum StreamingCommand {
    ReplaceMarkets(BTreeSet<String>),
    ReplaceHotBooks(BTreeSet<String>),
    Shutdown,
}

#[derive(Debug, Default)]
pub struct StreamingMetrics {
    pub connections: AtomicU64,
    pub gaps: AtomicU64,
    pub messages: AtomicU64,
    pub trades: AtomicU64,
    pub tracked_trade_updates: AtomicU64,
    pub invalid_messages: AtomicU64,
    pub book_updates: AtomicU64,
}

pub struct StreamingHandle {
    commands: mpsc::Sender<StreamingCommand>,
    events: mpsc::Receiver<StreamingEvent>,
    metrics: Arc<StreamingMetrics>,
}

impl StreamingHandle {
    pub fn start(
        policy: StreamingPolicy,
        tracked_wallets: impl IntoIterator<Item = String>,
    ) -> Result<Self, StreamingError> {
        policy.validate()?;
        if !policy.enabled {
            return Err(StreamingError::InvalidPolicy);
        }
        let tracked_wallets = tracked_wallets
            .into_iter()
            .map(|wallet| wallet.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if tracked_wallets.is_empty()
            || tracked_wallets.len() > MAX_TRACKED_WALLETS
            || tracked_wallets.iter().any(|wallet| !valid_address(wallet))
        {
            return Err(StreamingError::Capacity);
        }
        let (command_tx, command_rx) = mpsc::channel(32);
        let (event_tx, event_rx) = mpsc::channel(4_096);
        let metrics = Arc::new(StreamingMetrics::default());
        tokio::spawn(run_stream(
            policy,
            command_rx,
            event_tx,
            Arc::clone(&metrics),
            tracked_wallets,
        ));
        Ok(Self {
            commands: command_tx,
            events: event_rx,
            metrics,
        })
    }

    pub async fn replace_markets(&self, markets: BTreeSet<String>) -> Result<(), StreamingError> {
        validate_markets(&markets)?;
        self.commands
            .send(StreamingCommand::ReplaceMarkets(markets))
            .await
            .map_err(|_| StreamingError::ChannelClosed)
    }

    pub async fn replace_hot_books(&self, assets: BTreeSet<String>) -> Result<(), StreamingError> {
        let assets = sanitize_hot_books(assets);
        self.commands
            .send(StreamingCommand::ReplaceHotBooks(assets))
            .await
            .map_err(|_| StreamingError::ChannelClosed)
    }

    pub async fn recv(&mut self) -> Option<StreamingEvent> {
        self.events.recv().await
    }

    pub fn metrics(&self) -> Arc<StreamingMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn note_invalid_payload(&self) {
        self.metrics.invalid_messages.fetch_add(1, Ordering::SeqCst);
    }

    pub async fn shutdown(&self) {
        let _ = self.commands.send(StreamingCommand::Shutdown).await;
    }
}

async fn run_stream(
    policy: StreamingPolicy,
    mut commands: mpsc::Receiver<StreamingCommand>,
    events: mpsc::Sender<StreamingEvent>,
    metrics: Arc<StreamingMetrics>,
    tracked_wallets: BTreeSet<String>,
) {
    let mut markets = BTreeSet::new();
    let mut hot_books = BTreeSet::new();
    let mut epoch = 0_u64;
    let mut backoff_ms = policy.reconnect_initial_backoff_ms;
    loop {
        let connection = tokio_tungstenite::connect_async(APPROVED_WS_URL).await;
        let Ok((socket, _)) = connection else {
            epoch = epoch.saturating_add(1);
            metrics.gaps.fetch_add(1, Ordering::SeqCst);
            if events.send(StreamingEvent::Gap { epoch }).await.is_err() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                command = commands.recv() => {
                    if apply_disconnected_command(command, &mut markets, &mut hot_books) {
                        return;
                    }
                }
            }
            backoff_ms = backoff_ms
                .saturating_mul(2)
                .min(policy.reconnect_maximum_backoff_ms);
            continue;
        };
        let connected_at = Instant::now();
        epoch = epoch.saturating_add(1);
        metrics.connections.fetch_add(1, Ordering::SeqCst);
        let (mut writer, mut reader) = socket.split();
        if subscribe_initial(&mut writer, &markets, &hot_books)
            .await
            .is_err()
        {
            continue;
        }
        if events
            .send(StreamingEvent::Connected { epoch })
            .await
            .is_err()
        {
            return;
        }
        let mut last_received = Instant::now();
        let mut watchdog = tokio::time::interval(Duration::from_millis(1_000));
        let disconnected = loop {
            tokio::select! {
                command = commands.recv() => {
                    match command {
                        Some(StreamingCommand::ReplaceMarkets(next)) => {
                            if replace_subscriptions(&mut writer, "trades", &markets, &next).await.is_err() {
                                break true;
                            }
                            markets = next;
                        }
                        Some(StreamingCommand::ReplaceHotBooks(next)) => {
                            if replace_subscriptions(&mut writer, "l2Book", &hot_books, &next).await.is_err() {
                                break true;
                            }
                            hot_books = next;
                        }
                        Some(StreamingCommand::Shutdown) | None => break false,
                    }
                }
                message = reader.next() => {
                    let Some(Ok(message)) = message else { break true; };
                    last_received = Instant::now();
                    if let Message::Text(text) = message {
                        metrics.messages.fetch_add(1, Ordering::SeqCst);
                        if text.len() > policy.maximum_message_bytes {
                            metrics.invalid_messages.fetch_add(1, Ordering::SeqCst);
                            break true;
                        }
                        match parse_message(&text) {
                            Ok(Some(mut event)) => {
                                match &mut event {
                                    StreamingEvent::Trades(trades) => {
                                        metrics.trades.fetch_add(trades.len() as u64, Ordering::SeqCst);
                                        trades.retain(|trade| {
                                            tracked_wallets.contains(&trade.buyer)
                                                || tracked_wallets.contains(&trade.seller)
                                        });
                                        if trades.is_empty() {
                                            continue;
                                        }
                                        metrics
                                            .tracked_trade_updates
                                            .fetch_add(trades.len() as u64, Ordering::SeqCst);
                                    }
                                    StreamingEvent::OrderBook(_) => {
                                        metrics.book_updates.fetch_add(1, Ordering::SeqCst);
                                    }
                                    _ => {}
                                }
                                if events.send(event).await.is_err() { return; }
                            }
                            Ok(None) => {}
                            Err(_) => {
                                metrics.invalid_messages.fetch_add(1, Ordering::SeqCst);
                                // A malformed public payload makes stream
                                // continuity unknowable. Reconnect and force
                                // the same bounded REST reconciliation used
                                // for every other gap instead of silently
                                // carrying potentially divergent source state.
                                break true;
                            }
                        }
                    } else if let Message::Ping(payload) = message {
                        if writer.send(Message::Pong(payload)).await.is_err() { break true; }
                    } else if message.is_close() {
                        break true;
                    }
                }
                _ = watchdog.tick() => {
                    if last_received.elapsed() > Duration::from_millis(policy.idle_timeout_ms) {
                        break true;
                    }
                }
            }
        };
        if !disconnected {
            return;
        }
        metrics.gaps.fetch_add(1, Ordering::SeqCst);
        if events.send(StreamingEvent::Gap { epoch }).await.is_err() {
            return;
        }
        backoff_ms = if connected_at.elapsed() >= Duration::from_secs(60) {
            policy.reconnect_initial_backoff_ms
        } else {
            backoff_ms
                .saturating_mul(2)
                .min(policy.reconnect_maximum_backoff_ms)
        };
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            command = commands.recv() => {
                if apply_disconnected_command(command, &mut markets, &mut hot_books) {
                    return;
                }
            }
        }
    }
}

fn apply_disconnected_command(
    command: Option<StreamingCommand>,
    markets: &mut BTreeSet<String>,
    hot_books: &mut BTreeSet<String>,
) -> bool {
    match command {
        Some(StreamingCommand::ReplaceMarkets(next)) => *markets = next,
        Some(StreamingCommand::ReplaceHotBooks(next)) => *hot_books = next,
        Some(StreamingCommand::Shutdown) | None => return true,
    }
    false
}

async fn subscribe_initial<S>(
    writer: &mut S,
    markets: &BTreeSet<String>,
    hot_books: &BTreeSet<String>,
) -> Result<(), StreamingError>
where
    S: futures_util::Sink<Message> + Unpin,
{
    send_subscription(writer, true, "allDexsAssetCtxs", None).await?;
    for market in markets {
        send_subscription(writer, true, "trades", Some(market)).await?;
    }
    for asset in hot_books {
        send_subscription(writer, true, "l2Book", Some(asset)).await?;
    }
    Ok(())
}

async fn replace_subscriptions<S>(
    writer: &mut S,
    kind: &str,
    current: &BTreeSet<String>,
    next: &BTreeSet<String>,
) -> Result<(), StreamingError>
where
    S: futures_util::Sink<Message> + Unpin,
{
    for asset in current.difference(next) {
        send_subscription(writer, false, kind, Some(asset)).await?;
    }
    for asset in next.difference(current) {
        send_subscription(writer, true, kind, Some(asset)).await?;
    }
    Ok(())
}

async fn send_subscription<S>(
    writer: &mut S,
    subscribe: bool,
    kind: &str,
    coin: Option<&str>,
) -> Result<(), StreamingError>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let subscription = if let Some(coin) = coin {
        json!({"type":kind,"coin":coin})
    } else {
        json!({"type":kind})
    };
    let message = json!({
        "method": if subscribe { "subscribe" } else { "unsubscribe" },
        "subscription": subscription,
    });
    writer
        .send(Message::Text(message.to_string()))
        .await
        .map_err(|_| StreamingError::ChannelClosed)
}

fn parse_message(text: &str) -> Result<Option<StreamingEvent>, StreamingError> {
    let value: Value = serde_json::from_str(text).map_err(|_| StreamingError::InvalidPayload)?;
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .ok_or(StreamingError::InvalidPayload)?;
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    match channel {
        "subscriptionResponse" | "pong" => Ok(None),
        "trades" => parse_trades(&data).map(|trades| Some(StreamingEvent::Trades(trades))),
        "allDexsAssetCtxs" => Ok(Some(StreamingEvent::AssetContexts(data))),
        "l2Book" => parse_book(&data).map(|book| Some(StreamingEvent::OrderBook(book))),
        _ => Ok(None),
    }
}

fn parse_trades(data: &Value) -> Result<Vec<PublicTrade>, StreamingError> {
    let rows = data.as_array().ok_or(StreamingError::InvalidPayload)?;
    rows.iter()
        .map(|row| {
            let users = row
                .get("users")
                .and_then(Value::as_array)
                .filter(|users| users.len() == 2)
                .ok_or(StreamingError::InvalidPayload)?;
            let buyer = users[0]
                .as_str()
                .ok_or(StreamingError::InvalidPayload)?
                .to_ascii_lowercase();
            let seller = users[1]
                .as_str()
                .ok_or(StreamingError::InvalidPayload)?
                .to_ascii_lowercase();
            if !valid_address(&buyer) || !valid_address(&seller) {
                return Err(StreamingError::InvalidPayload);
            }
            let coin = string_field(row, "coin")?.to_string();
            if !valid_market(&coin) {
                return Err(StreamingError::InvalidPayload);
            }
            let price = decimal_field(row, "px")?;
            let size = decimal_field(row, "sz")?;
            if price <= Decimal::ZERO || size <= Decimal::ZERO {
                return Err(StreamingError::InvalidPayload);
            }
            Ok(PublicTrade {
                coin,
                price,
                size,
                time_ms: u64_field(row, "time")?,
                trade_id: u64_field(row, "tid")?,
                hash: string_field(row, "hash")?.to_string(),
                buyer,
                seller,
            })
        })
        .collect()
}

fn parse_book(data: &Value) -> Result<OrderBookResponse, StreamingError> {
    let asset = string_field(data, "coin")?.to_string();
    if !valid_market(&asset) {
        return Err(StreamingError::InvalidPayload);
    }
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .filter(|levels| levels.len() == 2)
        .ok_or(StreamingError::InvalidPayload)?;
    Ok(OrderBookResponse {
        asset,
        source_time_ms: u64_field(data, "time")?,
        bids: parse_levels(&levels[0])?,
        asks: parse_levels(&levels[1])?,
    })
}

fn parse_levels(value: &Value) -> Result<Vec<BookLevel>, StreamingError> {
    value
        .as_array()
        .ok_or(StreamingError::InvalidPayload)?
        .iter()
        .take(64)
        .map(|level| {
            let price = decimal_field(level, "px")?;
            let quantity = decimal_field(level, "sz")?;
            if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
                return Err(StreamingError::InvalidPayload);
            }
            Ok(BookLevel { price, quantity })
        })
        .collect()
}

#[derive(Debug, Clone, Default)]
pub struct MarketDirectory {
    pub by_dex: BTreeMap<String, Vec<MarketMetadataAsset>>,
}

impl MarketDirectory {
    pub fn from_metadata(metadata: &MarketMetadataResponse) -> Result<Self, StreamingError> {
        if metadata.universe.is_empty() {
            return Err(StreamingError::InvalidPayload);
        }
        let mut by_dex = BTreeMap::<String, Vec<MarketMetadataAsset>>::new();
        for asset in &metadata.universe {
            let dex = asset
                .name
                .split_once(':')
                .map_or("", |(dex, _)| dex)
                .to_string();
            by_dex.entry(dex).or_default().push(asset.clone());
        }
        Ok(Self { by_dex })
    }

    pub fn markets(&self) -> BTreeSet<String> {
        self.by_dex
            .values()
            .flatten()
            .map(|asset| asset.name.clone())
            .collect()
    }

    /// Keep economically active markets pinned and rotate only cold coverage
    /// through the remaining provider-safe subscription capacity.
    pub fn rotated_markets(
        &self,
        priority: &BTreeSet<String>,
        cursor: usize,
    ) -> Result<(BTreeSet<String>, usize), StreamingError> {
        let all = self.markets();
        let pinned = priority
            .intersection(&all)
            .cloned()
            .collect::<BTreeSet<_>>();
        if pinned.len() > MAX_MARKETS {
            return Err(StreamingError::Capacity);
        }
        let cold = all.difference(&pinned).cloned().collect::<Vec<_>>();
        let remaining = MAX_MARKETS.saturating_sub(pinned.len());
        if cold.len() <= remaining {
            return Ok((all, 0));
        }
        let start = cursor % cold.len();
        let mut selected = pinned;
        for offset in 0..remaining {
            selected.insert(cold[(start + offset) % cold.len()].clone());
        }
        Ok((selected, (start + remaining) % cold.len()))
    }
}

pub fn parse_asset_contexts(
    data: &Value,
    directory: &MarketDirectory,
    live_taker_fee_bps: Option<Decimal>,
) -> Result<(MarketSnapshotResponse, MarketMetadataResponse), StreamingError> {
    let raw_ctxs = data.get("ctxs").ok_or(StreamingError::InvalidPayload)?;
    let mut ctxs = BTreeMap::<String, &Value>::new();
    if let Some(object) = raw_ctxs.as_object() {
        ctxs.extend(object.iter().map(|(dex, rows)| (dex.clone(), rows)));
    } else if let Some(entries) = raw_ctxs.as_array() {
        for entry in entries {
            let pair = entry
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or(StreamingError::InvalidPayload)?;
            let dex = pair[0]
                .as_str()
                .ok_or(StreamingError::InvalidPayload)?
                .to_string();
            if ctxs.insert(dex, &pair[1]).is_some() {
                return Err(StreamingError::InvalidPayload);
            }
        }
    } else {
        return Err(StreamingError::InvalidPayload);
    }
    let mut mids = BTreeMap::new();
    let mut contexts = BTreeMap::new();
    for (dex, assets) in &directory.by_dex {
        let rows = ctxs
            .get(dex)
            .or_else(|| (dex.is_empty()).then(|| ctxs.get("hyperliquid")).flatten())
            .copied()
            .and_then(Value::as_array)
            .ok_or(StreamingError::InvalidPayload)?;
        if rows.len() != assets.len() {
            return Err(StreamingError::InvalidPayload);
        }
        for (asset, row) in assets.iter().zip(rows) {
            let mark = row
                .get("midPx")
                .filter(|value| !value.is_null())
                .map(parse_decimal_value)
                .transpose()?
                .unwrap_or(decimal_field(row, "markPx")?);
            let funding = decimal_field(row, "funding")?;
            if mark <= Decimal::ZERO {
                return Err(StreamingError::InvalidPayload);
            }
            mids.insert(asset.name.clone(), mark);
            contexts.insert(
                asset.name.clone(),
                MarketAssetContext {
                    funding_rate_hourly: funding,
                },
            );
        }
    }
    let universe = directory.by_dex.values().flatten().cloned().collect();
    Ok((
        MarketSnapshotResponse { mids },
        MarketMetadataResponse {
            universe,
            contexts,
            live_taker_fee_bps,
        },
    ))
}

pub struct StreamingSourceBook {
    tracked: BTreeSet<String>,
    states: BTreeMap<String, SourceStateResponse>,
    buffered: VecDeque<PublicTrade>,
    dedup_order: VecDeque<TradeIdentity>,
    dedup: BTreeSet<TradeIdentity>,
}

impl StreamingSourceBook {
    pub fn new(wallets: impl IntoIterator<Item = String>) -> Result<Self, StreamingError> {
        let tracked = wallets
            .into_iter()
            .map(|wallet| wallet.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if tracked.is_empty()
            || tracked.len() > MAX_TRACKED_WALLETS
            || tracked.iter().any(|wallet| !valid_address(wallet))
        {
            return Err(StreamingError::Capacity);
        }
        Ok(Self {
            tracked,
            states: BTreeMap::new(),
            buffered: VecDeque::new(),
            dedup_order: VecDeque::new(),
            dedup: BTreeSet::new(),
        })
    }

    pub fn install_baseline(
        &mut self,
        mut baseline: SourceStateResponse,
    ) -> Result<SourceStateResponse, StreamingError> {
        let wallet = baseline.candidate_id.to_ascii_lowercase();
        if !self.tracked.contains(&wallet) {
            return Err(StreamingError::InvalidPayload);
        }
        baseline.candidate_id = wallet.clone();
        let baseline_time_ms = baseline.source_time_ms;
        for trade in &self.buffered {
            if trade.time_ms > baseline_time_ms {
                apply_trade_to_wallet(&mut baseline, trade, &wallet)?;
            }
        }
        self.states.insert(wallet, baseline.clone());
        Ok(baseline)
    }

    pub fn apply_trade(
        &mut self,
        trade: PublicTrade,
    ) -> Result<Vec<SourceStateResponse>, StreamingError> {
        if !self.tracked.contains(&trade.buyer) && !self.tracked.contains(&trade.seller) {
            return Ok(Vec::new());
        }
        let identity = trade.identity();
        if !self.dedup.insert(identity.clone()) {
            return Ok(Vec::new());
        }
        self.dedup_order.push_back(identity);
        while self.dedup_order.len() > MAX_DEDUP_TRADES {
            if let Some(expired) = self.dedup_order.pop_front() {
                self.dedup.remove(&expired);
            }
        }
        if trade.buyer == trade.seller {
            return Ok(Vec::new());
        }
        self.buffered.push_back(trade.clone());
        while self.buffered.len() > MAX_BUFFERED_TRADES {
            self.buffered.pop_front();
        }
        let mut changed = Vec::new();
        for wallet in [&trade.buyer, &trade.seller] {
            if let Some(state) = self.states.get_mut(wallet) {
                if trade.time_ms >= state.source_time_ms
                    && apply_trade_to_wallet(state, &trade, wallet)?
                {
                    changed.push(state.clone());
                }
            }
        }
        Ok(changed)
    }

    pub fn all_hydrated(&self) -> bool {
        self.states.len() == self.tracked.len()
    }

    pub fn hydrated_count(&self) -> usize {
        self.states.len()
    }

    pub fn active_assets(&self) -> BTreeSet<String> {
        self.states
            .values()
            .flat_map(|state| state.positions.keys().cloned())
            .collect()
    }
}

fn apply_trade_to_wallet(
    state: &mut SourceStateResponse,
    trade: &PublicTrade,
    wallet: &str,
) -> Result<bool, StreamingError> {
    let delta = if trade.buyer == wallet {
        trade.size
    } else if trade.seller == wallet {
        -trade.size
    } else {
        return Ok(false);
    };
    let previous = state.positions.get(&trade.coin).cloned();
    let old_size = previous
        .as_ref()
        .map_or(Decimal::ZERO, |position| position.signed_size);
    let new_size = old_size
        .checked_add(delta)
        .ok_or(StreamingError::InvalidPayload)?;
    if new_size == Decimal::ZERO {
        state.positions.remove(&trade.coin);
    } else {
        let entry_price =
            next_entry_price(previous.as_ref(), old_size, new_size, delta, trade.price)?;
        let signed_notional = new_size
            .checked_mul(trade.price)
            .ok_or(StreamingError::InvalidPayload)?;
        state.positions.insert(
            trade.coin.clone(),
            SourceAssetPosition {
                asset: trade.coin.clone(),
                signed_size: new_size,
                signed_notional,
                entry_price,
                unrealized_pnl: None,
            },
        );
    }
    state.source_time_ms = state.source_time_ms.max(trade.time_ms);
    Ok(true)
}

fn next_entry_price(
    previous: Option<&SourceAssetPosition>,
    old_size: Decimal,
    new_size: Decimal,
    delta: Decimal,
    fill_price: Decimal,
) -> Result<Option<Decimal>, StreamingError> {
    if old_size == Decimal::ZERO || old_size.is_sign_negative() != new_size.is_sign_negative() {
        return Ok(Some(fill_price));
    }
    if old_size.is_sign_negative() != delta.is_sign_negative() {
        return Ok(previous.and_then(|position| position.entry_price));
    }
    let old_entry = previous
        .and_then(|position| position.entry_price)
        .unwrap_or(fill_price);
    let numerator = old_entry
        .checked_mul(old_size.abs())
        .and_then(|value| value.checked_add(fill_price.checked_mul(delta.abs())?))
        .ok_or(StreamingError::InvalidPayload)?;
    numerator
        .checked_div(new_size.abs())
        .map(Some)
        .ok_or(StreamingError::InvalidPayload)
}

fn validate_markets(markets: &BTreeSet<String>) -> Result<(), StreamingError> {
    if markets.is_empty()
        || markets.len() > MAX_MARKETS
        || markets
            .len()
            .saturating_add(MAX_HOT_BOOKS)
            .saturating_add(1)
            > 1_000
        || markets.iter().any(|market| !valid_market(market))
    {
        return Err(StreamingError::Capacity);
    }
    Ok(())
}

pub(crate) fn valid_market(market: &str) -> bool {
    !market.is_empty()
        && market.len() <= 64
        && market
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'-' | b'_' | b'.'))
}

fn sanitize_hot_books(assets: BTreeSet<String>) -> BTreeSet<String> {
    assets
        .into_iter()
        .filter(|asset| valid_market(asset))
        .take(MAX_HOT_BOOKS)
        .collect()
}

fn valid_address(address: &str) -> bool {
    address.len() == 42
        && address.starts_with("0x")
        && address[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn string_field<'a>(value: &'a Value, field: &str) -> Result<&'a str, StreamingError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(StreamingError::InvalidPayload)
}

fn decimal_field(value: &Value, field: &str) -> Result<Decimal, StreamingError> {
    parse_decimal_value(value.get(field).ok_or(StreamingError::InvalidPayload)?)
}

fn parse_decimal_value(value: &Value) -> Result<Decimal, StreamingError> {
    let text = value.as_str().ok_or(StreamingError::InvalidPayload)?;
    Decimal::from_str(text).map_err(|_| StreamingError::InvalidPayload)
}

fn u64_field(value: &Value, field: &str) -> Result<u64, StreamingError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(StreamingError::InvalidPayload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_book_command_input_is_bounded_and_malformed_markets_are_omitted() {
        let mut requested = (0..=MAX_HOT_BOOKS)
            .map(|index| format!("M{index:03}"))
            .collect::<BTreeSet<_>>();
        requested.insert("bad market".into());

        let sanitized = sanitize_hot_books(requested);

        assert_eq!(sanitized.len(), MAX_HOT_BOOKS);
        assert!(!sanitized.contains("bad market"));
    }

    fn wallet(byte: char) -> String {
        format!("0x{}", byte.to_string().repeat(40))
    }

    fn baseline(candidate: String, time_ms: u64) -> SourceStateResponse {
        SourceStateResponse {
            candidate_id: candidate,
            account_value: Decimal::from(1_000),
            source_time_ms: time_ms,
            positions: BTreeMap::new(),
            closed_candles: Vec::new(),
        }
    }

    fn trade(time_ms: u64, trade_id: u64) -> PublicTrade {
        PublicTrade {
            coin: "xyz:XYZ100".into(),
            price: Decimal::from(10),
            size: Decimal::from(2),
            time_ms,
            trade_id,
            hash: format!("hash-{trade_id}"),
            buyer: wallet('a'),
            seller: wallet('b'),
        }
    }

    #[test]
    fn buffered_trade_after_baseline_is_applied_once() {
        let mut book = StreamingSourceBook::new([wallet('a'), wallet('b')]).unwrap();
        assert!(book.apply_trade(trade(101, 1)).unwrap().is_empty());
        let buyer = book.install_baseline(baseline(wallet('a'), 100)).unwrap();
        assert_eq!(buyer.positions["xyz:XYZ100"].signed_size, Decimal::from(2));
        let seller = book.install_baseline(baseline(wallet('b'), 100)).unwrap();
        assert_eq!(
            seller.positions["xyz:XYZ100"].signed_size,
            Decimal::from(-2)
        );
        assert!(book.all_hydrated());
        assert!(book.apply_trade(trade(101, 1)).unwrap().is_empty());
    }

    #[test]
    fn tracked_trade_updates_open_reduce_and_cross() {
        let mut book = StreamingSourceBook::new([wallet('a')]).unwrap();
        book.install_baseline(baseline(wallet('a'), 100)).unwrap();
        let first = book.apply_trade(trade(101, 1)).unwrap().pop().unwrap();
        assert_eq!(first.positions["xyz:XYZ100"].signed_size, Decimal::from(2));
        let mut sell = trade(102, 2);
        sell.buyer = wallet('c');
        sell.seller = wallet('a');
        sell.size = Decimal::from(3);
        let crossed = book.apply_trade(sell).unwrap().pop().unwrap();
        assert_eq!(
            crossed.positions["xyz:XYZ100"].signed_size,
            Decimal::from(-1)
        );
        assert_eq!(
            crossed.positions["xyz:XYZ100"].entry_price,
            Some(Decimal::from(10))
        );
    }

    #[test]
    fn streamed_fill_retains_raw_position_from_zero_equity_empty_baseline() {
        let candidate = wallet('a');
        let mut book = StreamingSourceBook::new([candidate.clone()]).unwrap();
        let mut empty = baseline(candidate, 100);
        empty.account_value = Decimal::ZERO;
        book.install_baseline(empty).unwrap();

        let changed = book.apply_trade(trade(101, 1)).unwrap().pop().unwrap();
        assert_eq!(changed.account_value, Decimal::ZERO);
        assert_eq!(
            changed.positions["xyz:XYZ100"].signed_notional,
            Decimal::from(20)
        );
    }

    #[test]
    fn parses_public_trade_users_and_hip3_coin() {
        let message = json!({
            "channel":"trades",
            "data":[{
                "coin":"xyz:XYZ100","side":"B","px":"12.5","sz":"3",
                "hash":"0xabc","time":123,"tid":7,"users":[wallet('a'),wallet('b')]
            }]
        });
        let Some(StreamingEvent::Trades(trades)) = parse_message(&message.to_string()).unwrap()
        else {
            panic!("expected trades");
        };
        assert_eq!(trades[0].coin, "xyz:XYZ100");
        assert_eq!(trades[0].buyer, wallet('a'));
        assert_eq!(trades[0].seller, wallet('b'));
    }

    #[test]
    fn parses_all_dex_contexts_by_metadata_order() {
        let metadata = MarketMetadataResponse {
            universe: vec![
                MarketMetadataAsset {
                    name: "BTC".into(),
                    size_decimals: 5,
                },
                MarketMetadataAsset {
                    name: "xyz:XYZ100".into(),
                    size_decimals: 2,
                },
            ],
            contexts: BTreeMap::new(),
            live_taker_fee_bps: Some(Decimal::new(35, 2)),
        };
        let directory = MarketDirectory::from_metadata(&metadata).unwrap();
        let data = json!({"ctxs":[
            ["",[{"markPx":"100","midPx":"101","funding":"0.0001"}]],
            ["xyz",[{"markPx":"10","midPx":null,"funding":"-0.0002"}]]
        ]});
        let (mids, contexts) =
            parse_asset_contexts(&data, &directory, metadata.live_taker_fee_bps).unwrap();
        assert_eq!(mids.mids["BTC"], Decimal::from(101));
        assert_eq!(mids.mids["xyz:XYZ100"], Decimal::from(10));
        assert_eq!(
            contexts.contexts["xyz:XYZ100"].funding_rate_hourly,
            Decimal::new(-2, 4)
        );
    }

    #[test]
    fn active_markets_are_pinned_while_cold_coverage_rotates() {
        let directory = MarketDirectory {
            by_dex: BTreeMap::from([(
                String::new(),
                (0..900)
                    .map(|index| MarketMetadataAsset {
                        name: format!("M{index:03}"),
                        size_decimals: 2,
                    })
                    .collect(),
            )]),
        };
        // The selector is deliberately agnostic to why a market is pinned:
        // both active source exposure and an MFCE-hot book use this priority
        // set and therefore remain continuously subscribed.
        let priority = BTreeSet::from(["M898".to_string(), "M899".to_string()]);
        let (first, cursor) = directory.rotated_markets(&priority, 0).unwrap();
        let (second, _) = directory.rotated_markets(&priority, cursor).unwrap();
        assert_eq!(first.len(), MAX_MARKETS);
        assert_eq!(second.len(), MAX_MARKETS);
        assert!(first.contains("M898"));
        assert!(second.contains("M898"));
        assert!(first.contains("M899"));
        assert!(second.contains("M899"));
        assert_ne!(first, second);
        assert!(directory
            .markets()
            .difference(&first)
            .any(|market| second.contains(market)));
    }

    #[test]
    fn complete_market_universe_never_rotates_when_it_fits_the_safe_budget() {
        let directory = MarketDirectory {
            by_dex: BTreeMap::from([(
                String::new(),
                (0..MAX_MARKETS)
                    .map(|index| MarketMetadataAsset {
                        name: format!("M{index:03}"),
                        size_decimals: 2,
                    })
                    .collect(),
            )]),
        };
        let all = directory.markets();
        let (first, first_cursor) = directory.rotated_markets(&BTreeSet::new(), 0).unwrap();
        let (second, second_cursor) = directory.rotated_markets(&BTreeSet::new(), 500).unwrap();
        assert_eq!(first, all);
        assert_eq!(second, all);
        assert_eq!(first_cursor, 0);
        assert_eq!(second_cursor, 0);
    }
}
