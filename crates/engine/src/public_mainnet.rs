use crate::domain::decision::hash_payload_bytes;
use crate::domain::scheduler::{
    Clock, ReadFailure, ReadOnlyDataSource, ReadRequestKind, ReadResponse, RequestSubject,
    ScheduledReadRequest,
};
use crate::domain::technical::{CandleInterval, ClosedCandle};
use crate::ingestion::{
    CandidateAuditSnapshot, CandidateAuditState, DecodeStage, IngestionFailure,
    IngestionFailureClassification,
};
use crate::streaming::StreamingPolicy;
use futures_util::{stream, StreamExt};
use reqwest::{redirect::Policy, Client, StatusCode, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const APPROVED_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const CLOSED_CANDLE_FINALITY_DELAY_MS: u64 = 5_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicTransportPolicy {
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub maximum_response_bytes: usize,
    pub active_source_interval_ms: u64,
    pub inactive_source_interval_ms: u64,
    pub queue_delay_allowance_ms: u64,
    pub transport_p99_allowance_ms: u64,
    pub market_validity_ms: u64,
    /// Additional builder-deployed perpetual DEXes whose markets are merged
    /// into the default Hyperliquid perpetual universe. The empty/default DEX
    /// is always queried and must not be listed here.
    #[serde(default)]
    pub perp_dexes: Vec<String>,
    /// Event-driven public data plane. When enabled, REST source reads are
    /// bootstrap/reconciliation only and market/book freshness comes from WS.
    #[serde(default)]
    pub streaming: StreamingPolicy,
}

impl PublicTransportPolicy {
    pub fn validate(&self) -> Result<(), PublicReadError> {
        if self.connect_timeout_ms == 0
            || self.request_timeout_ms == 0
            || self.request_timeout_ms < self.connect_timeout_ms
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > 16 * 1024 * 1024
            || self.active_source_interval_ms == 0
            || self.inactive_source_interval_ms < self.active_source_interval_ms
            || self.queue_delay_allowance_ms == 0
            || self.transport_p99_allowance_ms == 0
            || self.market_validity_ms == 0
            || self.perp_dexes.len() > 4
            || self.perp_dexes.iter().any(|dex| {
                dex != "xyz"
                    || dex.is_empty()
                    || dex.len() > 32
                    || !dex
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            })
            || self.perp_dexes.iter().collect::<BTreeSet<_>>().len() != self.perp_dexes.len()
        {
            return Err(PublicReadError::InvalidPolicy);
        }
        self.streaming
            .validate()
            .map_err(|_| PublicReadError::InvalidPolicy)?;
        Ok(())
    }

    pub fn source_deadline_ms(&self, tier: crate::domain::scheduler::SourceTier) -> Option<u64> {
        let interval = match tier {
            crate::domain::scheduler::SourceTier::Active => self.active_source_interval_ms,
            crate::domain::scheduler::SourceTier::Inactive => self.inactive_source_interval_ms,
        };
        interval
            .checked_add(self.queue_delay_allowance_ms)?
            .checked_add(self.transport_p99_allowance_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceAssetPosition {
    pub asset: String,
    pub signed_size: Decimal,
    pub signed_notional: Decimal,
    /// Hyperliquid clearinghouse-state entry price. Older retained replay
    /// records may omit it; cohort entries fail closed when it is unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_price: Option<Decimal>,
    /// Hyperliquid clearinghouse-state unrealized PnL, retained for stale-profit
    /// and chase diagnostics rather than sourced from Hyperdash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrealized_pnl: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStateResponse {
    pub candidate_id: String,
    pub account_value: Decimal,
    pub source_time_ms: u64,
    pub positions: BTreeMap<String, SourceAssetPosition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub closed_candles: Vec<ClosedCandle>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearinghouseStateWire {
    margin_summary: MarginSummaryWire,
    asset_positions: Vec<AssetPositionWire>,
    time: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarginSummaryWire {
    account_value: String,
}

#[derive(Debug, Deserialize)]
struct AssetPositionWire {
    position: PositionWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionWire {
    coin: String,
    szi: String,
    position_value: String,
    #[serde(default)]
    entry_px: Option<String>,
    #[serde(default)]
    unrealized_pnl: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserFeesWire {
    user_cross_rate: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshotResponse {
    pub mids: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketMetadataResponse {
    pub universe: Vec<MarketMetadataAsset>,
    pub contexts: BTreeMap<String, MarketAssetContext>,
    /// Current follower crossing/taker rate from the public `userFees` info
    /// endpoint, converted from a unit fraction to basis points.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_taker_fee_bps: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAssetContext {
    pub funding_rate_hourly: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketMetadataAsset {
    pub name: String,
    pub size_decimals: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookResponse {
    pub asset: String,
    pub source_time_ms: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandleResponse {
    pub candle: ClosedCandle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicPayload {
    SourceState(SourceStateResponse),
    SourceFills(crate::source_state::SourceFillPage),
    MarketSnapshot(MarketSnapshotResponse),
    MarketMetadata(MarketMetadataResponse),
    OrderBook(OrderBookResponse),
    Candle(CandleResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedPublicResponse {
    pub request_kind: ReadRequestKind,
    pub source_tier: Option<crate::domain::scheduler::SourceTier>,
    pub subject: String,
    pub requested_at_mono: u64,
    pub received_at_mono: u64,
    pub valid_until_mono: u64,
    pub payload: PublicPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicReadError {
    InvalidPolicy,
    InvalidSubject,
    InvalidPayload,
    Transport,
}

#[derive(Debug, Default)]
pub struct PublicTransportMetrics {
    pub requests_started: AtomicU64,
    pub responses_accepted: AtomicU64,
    pub rate_limited: AtomicU64,
    pub invalid_responses: AtomicU64,
    pub mutation_requests: AtomicU64,
    pub key_file_opens: AtomicU64,
}

#[derive(Clone)]
pub struct HyperliquidPublicTransport<C: Clock> {
    client: Client,
    endpoint: Url,
    clock: C,
    policy: PublicTransportPolicy,
    accepted: Arc<Mutex<VecDeque<AcceptedPublicResponse>>>,
    metrics: Arc<PublicTransportMetrics>,
    market_assets: Arc<Mutex<BTreeSet<String>>>,
    candidate_audit: Arc<Mutex<CandidateAuditSnapshot>>,
    technical_fetch: Arc<Mutex<TechnicalCandleFetchState>>,
    fee_user: Arc<Mutex<Option<String>>>,
    expanded_source_candidates: Arc<Mutex<BTreeSet<String>>>,
    discovered_perp_dexes: Arc<Mutex<BTreeSet<String>>>,
    discovered_perp_dex_order: Arc<Mutex<Vec<String>>>,
}

const TECHNICAL_UNIVERSE_LIMIT: usize = 25;
const TECHNICAL_BATCH_INTERVAL_MS: u64 = 5_000;

#[derive(Debug, Default)]
struct TechnicalCandleFetchState {
    technical_assets: BTreeSet<String>,
    source_assets_by_candidate: BTreeMap<String, BTreeSet<String>>,
    requested_buckets: BTreeMap<(String, CandleInterval), u64>,
    last_batch_started_ms: Option<u64>,
}

impl TechnicalCandleFetchState {
    fn replace_technical_assets(&mut self, assets: impl IntoIterator<Item = String>) {
        self.technical_assets = assets.into_iter().collect();
        self.prune_inactive_buckets();
    }

    fn replace_candidate_assets(
        &mut self,
        candidate_id: &str,
        positions: &BTreeMap<String, SourceAssetPosition>,
    ) {
        self.source_assets_by_candidate.insert(
            candidate_id.to_ascii_lowercase(),
            positions.keys().cloned().collect(),
        );
        self.prune_inactive_buckets();
    }

    fn all_assets(&self) -> BTreeSet<String> {
        let mut assets = self.technical_assets.clone();
        for source_assets in self.source_assets_by_candidate.values() {
            assets.extend(source_assets.iter().cloned());
        }
        assets
    }

    fn prune_inactive_buckets(&mut self) {
        let active_assets = self.all_assets();
        self.requested_buckets
            .retain(|(asset, _), _| active_assets.contains(asset));
    }
}

impl<C: Clock> HyperliquidPublicTransport<C> {
    pub fn new(clock: C, policy: PublicTransportPolicy) -> Result<Self, PublicReadError> {
        policy.validate()?;
        let endpoint = Url::parse(APPROVED_INFO_URL).map_err(|_| PublicReadError::InvalidPolicy)?;
        validate_endpoint(&endpoint)?;
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_millis(policy.connect_timeout_ms))
            .timeout(Duration::from_millis(policy.request_timeout_ms))
            .https_only(true)
            .build()
            .map_err(|_| PublicReadError::Transport)?;
        Ok(Self {
            client,
            endpoint,
            clock,
            policy,
            accepted: Arc::new(Mutex::new(VecDeque::new())),
            metrics: Arc::new(PublicTransportMetrics::default()),
            market_assets: Arc::new(Mutex::new(BTreeSet::new())),
            candidate_audit: Arc::new(Mutex::new(BTreeMap::new())),
            technical_fetch: Arc::new(Mutex::new(TechnicalCandleFetchState::default())),
            fee_user: Arc::new(Mutex::new(None)),
            expanded_source_candidates: Arc::new(Mutex::new(BTreeSet::new())),
            discovered_perp_dexes: Arc::new(Mutex::new(BTreeSet::new())),
            discovered_perp_dex_order: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Selects the already-bounded source cohort whose authoritative state is
    /// read across the configured builder perpetual DEXes as one logical
    /// snapshot. Other candidates retain default-perp polling.
    pub fn set_expanded_source_candidates(
        &self,
        candidates: impl IntoIterator<Item = String>,
    ) -> Result<(), PublicReadError> {
        let mut validated = BTreeSet::new();
        for candidate in candidates {
            validate_candidate_address(&candidate).map_err(|_| PublicReadError::InvalidSubject)?;
            validated.insert(candidate.to_ascii_lowercase());
        }
        *self
            .expanded_source_candidates
            .lock()
            .expect("expanded source candidate mutex poisoned") = validated;
        Ok(())
    }

    pub fn set_fee_user(&self, user: Option<&str>) -> Result<(), PublicReadError> {
        if let Some(user) = user {
            validate_candidate_address(user).map_err(|_| PublicReadError::InvalidSubject)?;
        }
        *self.fee_user.lock().expect("fee user mutex poisoned") = user.map(str::to_ascii_lowercase);
        Ok(())
    }

    pub fn take_accepted(&self, request: &ScheduledReadRequest) -> Option<AcceptedPublicResponse> {
        let subject = match &request.key.subject {
            RequestSubject::SourceHistory { candidate, .. } => candidate.as_str(),
            RequestSubject::Candidate(value)
            | RequestSubject::Follower(value)
            | RequestSubject::Asset(value) => value.as_str(),
            RequestSubject::Market => "market",
        };
        let mut accepted = self
            .accepted
            .lock()
            .expect("public response queue mutex poisoned");
        let index = accepted.iter().position(|response| {
            response.request_kind == request.kind
                && response.subject == subject
                && response.requested_at_mono == request.created_at
        })?;
        accepted.remove(index)
    }

    pub fn metrics(&self) -> Arc<PublicTransportMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn discovered_perp_dexes(&self) -> BTreeSet<String> {
        self.discovered_perp_dexes
            .lock()
            .expect("discovered perp DEX mutex poisoned")
            .clone()
    }

    pub fn discovered_perp_dex_order(&self) -> Vec<String> {
        self.discovered_perp_dex_order
            .lock()
            .expect("perp DEX order mutex poisoned")
            .clone()
    }

    pub fn candidate_audit_snapshot(&self) -> CandidateAuditSnapshot {
        self.candidate_audit
            .lock()
            .expect("candidate audit mutex poisoned")
            .clone()
    }

    async fn fetch_live_taker_fee_bps(&self, user: &str) -> Result<Decimal, ReadFailure> {
        self.metrics.requests_started.fetch_add(1, Ordering::SeqCst);
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, "engine-mfce-fees/1")
            .json(&json!({"type":"userFees","user":user}))
            .send()
            .await
            .map_err(classify_transport_error)?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            self.metrics.rate_limited.fetch_add(1, Ordering::SeqCst);
            return Err(ReadFailure::RateLimited {
                retry_after_ms: retry_after_ms(response.headers()),
            });
        }
        if response.status().is_server_error() {
            return Err(ReadFailure::Server);
        }
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > self.policy.maximum_response_bytes as u64)
        {
            return Err(ReadFailure::InvalidResponse);
        }
        let bytes = read_bounded(response, self.policy.maximum_response_bytes).await?;
        parse_user_taker_fee_bps(&bytes).map_err(|_| ReadFailure::InvalidResponse)
    }

    async fn fetch_auxiliary_info(&self, body: &Value) -> Result<Vec<u8>, ReadFailure> {
        self.metrics.requests_started.fetch_add(1, Ordering::SeqCst);
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, "engine-multi-perp/1")
            .json(body)
            .send()
            .await
            .map_err(classify_transport_error)?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            self.metrics.rate_limited.fetch_add(1, Ordering::SeqCst);
            return Err(ReadFailure::RateLimited {
                retry_after_ms: retry_after_ms(response.headers()),
            });
        }
        if response.status().is_server_error() {
            return Err(ReadFailure::Server);
        }
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > self.policy.maximum_response_bytes as u64)
        {
            return Err(ReadFailure::InvalidResponse);
        }
        read_bounded(response, self.policy.maximum_response_bytes).await
    }

    /// Initializes a technical series from Hyperliquid's public candle snapshot
    /// endpoint. The caller supplies a bounded time range and receives only
    /// fully closed candles, capped at the exchange's documented 5,000 rows.
    pub async fn fetch_candle_snapshot(
        &self,
        asset: &str,
        interval: CandleInterval,
        start_time_ms: u64,
        end_time_ms: u64,
        now_ms: u64,
    ) -> Result<Vec<ClosedCandle>, PublicReadError> {
        if asset.is_empty() || start_time_ms >= end_time_ms {
            return Err(PublicReadError::InvalidSubject);
        }
        self.metrics.requests_started.fetch_add(1, Ordering::SeqCst);
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, "engine-hybrid/1")
            .json(&json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": asset,
                    "interval": interval.api_name(),
                    "startTime": start_time_ms,
                    "endTime": end_time_ms
                }
            }))
            .send()
            .await
            .map_err(|_| PublicReadError::Transport)?;
        if !response.status().is_success() {
            return Err(PublicReadError::Transport);
        }
        let bytes = read_bounded(response, self.policy.maximum_response_bytes)
            .await
            .map_err(|_| PublicReadError::InvalidPayload)?;
        let candles = parse_candle_snapshot(asset, interval, &bytes, now_ms)?;
        self.metrics
            .responses_accepted
            .fetch_add(1, Ordering::SeqCst);
        Ok(candles)
    }

    async fn fetch_due_source_candles(
        &self,
        candidate_id: &str,
        positions: &BTreeMap<String, SourceAssetPosition>,
        now_ms: u64,
    ) -> Vec<ClosedCandle> {
        let reservation = {
            let mut state = self
                .technical_fetch
                .lock()
                .expect("technical candle fetch mutex poisoned");
            state.replace_candidate_assets(candidate_id, positions);
            if state
                .last_batch_started_ms
                .is_some_and(|last| now_ms.saturating_sub(last) < TECHNICAL_BATCH_INTERVAL_MS)
            {
                None
            } else {
                let assets = state.all_assets();
                let due = assets.iter().find_map(|asset| {
                    let intervals = CandleInterval::ALL
                        .into_iter()
                        .filter(|interval| {
                            let bucket = now_ms.saturating_sub(CLOSED_CANDLE_FINALITY_DELAY_MS)
                                / interval.duration_ms();
                            state
                                .requested_buckets
                                .get(&(asset.clone(), *interval))
                                .is_none_or(|requested| *requested < bucket)
                        })
                        .collect::<Vec<_>>();
                    (!intervals.is_empty()).then(|| (asset.clone(), intervals))
                });
                if let Some((asset, intervals)) = &due {
                    state.last_batch_started_ms = Some(now_ms);
                    for interval in intervals {
                        state.requested_buckets.insert(
                            (asset.clone(), *interval),
                            now_ms.saturating_sub(CLOSED_CANDLE_FINALITY_DELAY_MS)
                                / interval.duration_ms(),
                        );
                    }
                }
                due
            }
        };
        let Some((asset, intervals)) = reservation else {
            return Vec::new();
        };
        let mut accepted = Vec::new();
        let mut failed = Vec::new();
        for interval in intervals {
            let duration = interval.duration_ms();
            let history = u64::try_from(interval.warmup_candles() + 2).unwrap_or(u64::MAX);
            let start = now_ms.saturating_sub(duration.saturating_mul(history));
            match self
                .fetch_candle_snapshot(&asset, interval, start, now_ms, now_ms)
                .await
            {
                Ok(mut candles) => accepted.append(&mut candles),
                Err(_) => failed.push(interval),
            }
        }
        if !failed.is_empty() {
            let mut state = self
                .technical_fetch
                .lock()
                .expect("technical candle fetch mutex poisoned");
            for interval in failed {
                state.requested_buckets.remove(&(asset.clone(), interval));
            }
        }
        accepted.sort_by_key(|candle| (candle.interval, candle.open_time_ms));
        accepted
    }

    async fn perform(
        &self,
        request: &ScheduledReadRequest,
    ) -> Result<AcceptedPublicResponse, ReadFailure> {
        let scoped_metadata = request.kind == ReadRequestKind::ExchangeMetadata
            && matches!(request.key.subject, RequestSubject::Asset(_));
        let (subject, body) = if self.policy.streaming.enabled
            && request.kind == ReadRequestKind::ExchangeMetadata
            && !scoped_metadata
        {
            ("market".to_string(), json!({"type":"allPerpMetas"}))
        } else {
            modeled_body(request)?
        };
        let expanded_source = self
            .expanded_source_candidates
            .lock()
            .expect("expanded source candidate mutex poisoned")
            .contains(&subject);
        if request.kind == ReadRequestKind::ExpandedSourceState && !expanded_source {
            return Err(ReadFailure::InvalidResponse);
        }
        if self.policy.streaming.enabled && request.kind == ReadRequestKind::ExchangeMetadata {
            let dex_bytes = self
                .fetch_auxiliary_info(&json!({"type":"perpDexs"}))
                .await?;
            let dex_order =
                parse_perp_dex_order(&dex_bytes).map_err(|_| ReadFailure::InvalidResponse)?;
            *self
                .discovered_perp_dex_order
                .lock()
                .expect("discovered perp DEX order mutex poisoned") = dex_order;
        }
        if request.kind.is_source_state() {
            validate_candidate_address(&subject).map_err(|failure| {
                self.record_candidate_failure(request, &subject, failure);
                ReadFailure::InvalidResponse
            })?;
            self.with_candidate_audit(request, &subject, |audit| audit.record_request());
        }
        self.metrics.requests_started.fetch_add(1, Ordering::SeqCst);
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, "engine-hl1k/1")
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                if request.kind.is_source_state() {
                    self.record_candidate_failure(
                        request,
                        &subject,
                        IngestionFailure::new(
                            IngestionFailureClassification::TransportFailure,
                            DecodeStage::AddressValidated,
                            error.to_string(),
                        ),
                    );
                }
                classify_transport_error(error)
            })?;
        if request.kind.is_source_state() {
            self.with_candidate_audit(request, &subject, |audit| {
                audit.record_http(response.status().as_u16())
            });
        }
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            self.metrics.rate_limited.fetch_add(1, Ordering::SeqCst);
            if request.kind.is_source_state() {
                self.record_candidate_failure(
                    request,
                    &subject,
                    IngestionFailure::new(
                        IngestionFailureClassification::TransportFailure,
                        DecodeStage::HttpValidated,
                        "HTTP 429 rate limited",
                    ),
                );
            }
            return Err(ReadFailure::RateLimited {
                retry_after_ms: retry_after_ms(response.headers()),
            });
        }
        if response.status().is_server_error() {
            if request.kind.is_source_state() {
                self.record_candidate_failure(
                    request,
                    &subject,
                    IngestionFailure::new(
                        IngestionFailureClassification::TransportFailure,
                        DecodeStage::HttpValidated,
                        format!("server returned HTTP {}", response.status()),
                    ),
                );
            }
            return Err(ReadFailure::Server);
        }
        if !response.status().is_success() {
            if request.kind.is_source_state() {
                let classification = if response.status() == StatusCode::NOT_FOUND {
                    IngestionFailureClassification::AccountGenuinelyAbsent
                } else {
                    IngestionFailureClassification::TransportFailure
                };
                self.record_candidate_failure(
                    request,
                    &subject,
                    IngestionFailure::new(
                        classification,
                        DecodeStage::HttpValidated,
                        format!("public info endpoint returned HTTP {}", response.status()),
                    ),
                );
            }
            return Err(ReadFailure::Permanent);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.policy.maximum_response_bytes as u64)
        {
            if request.kind.is_source_state() {
                self.record_candidate_failure(
                    request,
                    &subject,
                    IngestionFailure::new(
                        IngestionFailureClassification::SchemaLayoutMismatch,
                        DecodeStage::HttpValidated,
                        "declared response body exceeds configured limit",
                    ),
                );
            }
            return Err(ReadFailure::InvalidResponse);
        }
        let bytes = read_bounded(response, self.policy.maximum_response_bytes)
            .await
            .map_err(|error| {
                if request.kind.is_source_state() {
                    self.record_candidate_failure(
                        request,
                        &subject,
                        IngestionFailure::new(
                            IngestionFailureClassification::SchemaLayoutMismatch,
                            DecodeStage::HttpValidated,
                            "response body could not be read within configured bounds",
                        ),
                    );
                }
                error
            })?;
        if request.kind.is_source_state() {
            let hash = hash_payload_bytes(&bytes);
            self.with_candidate_audit(request, &subject, |audit| audit.record_payload(hash));
        }
        let mut payload = self
            .parse_payload(request, &subject, &bytes)
            .map_err(|failure| {
                self.metrics
                    .invalid_responses
                    .fetch_add(1, Ordering::SeqCst);
                if request.kind.is_source_state() {
                    self.record_candidate_failure(request, &subject, failure);
                }
                ReadFailure::InvalidResponse
            })?;
        match &mut payload {
            PublicPayload::SourceState(state)
                if request.kind == ReadRequestKind::ExpandedSourceState && expanded_source =>
            {
                let assets = self
                    .market_assets
                    .lock()
                    .expect("market asset mutex poisoned")
                    .clone();
                let dexes = if self.policy.streaming.enabled {
                    self.discovered_perp_dexes()
                } else {
                    self.policy.perp_dexes.iter().cloned().collect()
                };
                let fetches = stream::iter(dexes.into_iter().map(|dex| {
                    let subject = subject.clone();
                    let assets = assets.clone();
                    async move {
                        let auxiliary = self
                            .fetch_auxiliary_info(
                                &json!({"type":"clearinghouseState","user":subject,"dex":dex}),
                            )
                            .await?;
                        let additional =
                            parse_source_state_with_metadata(&subject, &auxiliary, &assets)
                                .map_err(|_| ReadFailure::InvalidResponse)?;
                        Ok::<_, ReadFailure>((dex, additional))
                    }
                }))
                .buffer_unordered(4)
                .collect::<Vec<_>>()
                .await;
                let mut additional = fetches.into_iter().collect::<Result<Vec<_>, _>>()?;
                additional.sort_by(|left, right| left.0.cmp(&right.0));
                for (_, additional) in additional {
                    merge_source_states(state, additional)?;
                }
            }
            PublicPayload::MarketSnapshot(snapshot) => {
                if self.policy.streaming.enabled {
                    return Err(ReadFailure::Permanent);
                }
                for dex in &self.policy.perp_dexes {
                    let auxiliary = self
                        .fetch_auxiliary_info(&json!({"type":"allMids","dex":dex}))
                        .await?;
                    let additional =
                        parse_mids(&auxiliary).map_err(|_| ReadFailure::InvalidResponse)?;
                    merge_market_snapshot(snapshot, additional)?;
                }
            }
            PublicPayload::MarketMetadata(metadata) => {
                if !self.policy.streaming.enabled && !scoped_metadata {
                    for dex in &self.policy.perp_dexes {
                        let auxiliary = self
                            .fetch_auxiliary_info(&json!({"type":"metaAndAssetCtxs","dex":dex}))
                            .await?;
                        let (additional, _) = parse_metadata_with_technical_universe(&auxiliary)
                            .map_err(|_| ReadFailure::InvalidResponse)?;
                        merge_market_metadata(metadata, additional)?;
                    }
                }
                let mut assets = self
                    .market_assets
                    .lock()
                    .expect("market asset mutex poisoned");
                if !scoped_metadata {
                    assets.clear();
                }
                assets.extend(metadata.universe.iter().map(|asset| asset.name.clone()));
            }
            _ => {}
        }
        if let PublicPayload::MarketMetadata(metadata) = &mut payload {
            let fee_user = self
                .fee_user
                .lock()
                .expect("fee user mutex poisoned")
                .clone();
            if let Some(fee_user) = fee_user.filter(|_| !scoped_metadata) {
                metadata.live_taker_fee_bps = Some(self.fetch_live_taker_fee_bps(&fee_user).await?);
            }
        }
        if !self.policy.streaming.enabled {
            if let PublicPayload::SourceState(state) = &payload {
                let maximum_age = self
                    .policy
                    .source_deadline_ms(request.source_tier.ok_or(ReadFailure::InvalidResponse)?)
                    .ok_or(ReadFailure::InvalidResponse)?;
                let age = wall_clock_ms().saturating_sub(state.source_time_ms);
                if age > maximum_age {
                    let failure = IngestionFailure::new(
                        IngestionFailureClassification::StaleFutureTimestamp,
                        DecodeStage::MetadataResolved,
                        format!("source state age {age}ms exceeds {maximum_age}ms"),
                    );
                    self.record_candidate_failure(request, &subject, failure);
                    return Err(ReadFailure::InvalidResponse);
                }
            }
        }
        if !self.policy.streaming.enabled {
            if let PublicPayload::SourceState(state) = &mut payload {
                let candidate_id = state.candidate_id.clone();
                state.closed_candles = self
                    .fetch_due_source_candles(&candidate_id, &state.positions, wall_clock_ms())
                    .await;
            }
        }
        let received_at_mono = self.clock.now_ms();
        let validity = match request.kind {
            ReadRequestKind::SourceState | ReadRequestKind::ExpandedSourceState => self
                .policy
                .source_deadline_ms(request.source_tier.ok_or(ReadFailure::InvalidResponse)?)
                .ok_or(ReadFailure::InvalidResponse)?,
            _ => self.policy.market_validity_ms,
        };
        let valid_until_mono = received_at_mono
            .checked_add(validity)
            .ok_or(ReadFailure::InvalidResponse)?;
        if request.kind.is_source_state() {
            let (account_value, position_count) = match &payload {
                PublicPayload::SourceState(state) => (state.account_value, state.positions.len()),
                _ => return Err(ReadFailure::InvalidResponse),
            };
            self.with_candidate_audit(request, &subject, |audit| {
                audit.record_success(wall_clock_ms(), account_value, position_count)
            });
        }
        Ok(AcceptedPublicResponse {
            request_kind: request.kind,
            source_tier: request.source_tier,
            subject,
            requested_at_mono: request.created_at,
            received_at_mono,
            valid_until_mono,
            payload,
        })
    }

    fn parse_payload(
        &self,
        request: &ScheduledReadRequest,
        subject: &str,
        bytes: &[u8],
    ) -> Result<PublicPayload, IngestionFailure> {
        match request.kind {
            ReadRequestKind::SourceState | ReadRequestKind::ExpandedSourceState => {
                let assets = self
                    .market_assets
                    .lock()
                    .expect("market asset mutex poisoned")
                    .clone();
                parse_source_state_with_metadata(subject, bytes, &assets)
                    .map(PublicPayload::SourceState)
            }
            ReadRequestKind::SourceFills => {
                crate::source_state::SourceFillPage::parse(&request.key.subject, bytes)
                    .map(PublicPayload::SourceFills)
                    .map_err(|error| {
                        IngestionFailure::new(
                            IngestionFailureClassification::OtherExplicitDecodeFailure,
                            DecodeStage::NotAttempted,
                            error,
                        )
                    })
            }
            ReadRequestKind::MarketMids => parse_mids(bytes)
                .map(PublicPayload::MarketSnapshot)
                .map_err(generic_decode_failure),
            ReadRequestKind::ExchangeMetadata => {
                if let RequestSubject::Asset(dex) = &request.key.subject {
                    return parse_named_perp_meta(bytes, dex)
                        .map(PublicPayload::MarketMetadata)
                        .map_err(generic_decode_failure);
                }
                let (metadata, technical_assets, dexes) = if self.policy.streaming.enabled {
                    let dex_order = self
                        .discovered_perp_dex_order
                        .lock()
                        .expect("discovered perp DEX order mutex poisoned")
                        .clone();
                    let (metadata, dexes) =
                        parse_all_perp_metas(bytes, &dex_order).map_err(generic_decode_failure)?;
                    let technical = ranked_technical_assets(&metadata.universe, &BTreeMap::new());
                    (metadata, technical, dexes)
                } else {
                    let (metadata, technical_assets) =
                        parse_metadata_with_technical_universe(bytes)
                            .map_err(generic_decode_failure)?;
                    (metadata, technical_assets, BTreeSet::new())
                };
                if self.policy.streaming.enabled {
                    *self
                        .discovered_perp_dexes
                        .lock()
                        .expect("discovered perp DEX mutex poisoned") = dexes;
                }
                *self
                    .market_assets
                    .lock()
                    .expect("market asset mutex poisoned") = metadata
                    .universe
                    .iter()
                    .map(|asset| asset.name.clone())
                    .collect();
                self.technical_fetch
                    .lock()
                    .expect("technical candle fetch mutex poisoned")
                    .replace_technical_assets(technical_assets);
                Ok(PublicPayload::MarketMetadata(metadata))
            }
            ReadRequestKind::OrderBook => parse_book(subject, bytes)
                .map(PublicPayload::OrderBook)
                .map_err(generic_decode_failure),
            _ => Err(IngestionFailure::new(
                IngestionFailureClassification::OtherExplicitDecodeFailure,
                DecodeStage::NotAttempted,
                "unsupported read request kind",
            )),
        }
    }

    fn with_candidate_audit(
        &self,
        request: &ScheduledReadRequest,
        candidate: &str,
        update: impl FnOnce(&mut CandidateAuditState),
    ) {
        let tier = request
            .source_tier
            .unwrap_or(crate::domain::scheduler::SourceTier::Inactive);
        let mut audit = self
            .candidate_audit
            .lock()
            .expect("candidate audit mutex poisoned");
        let state = audit
            .entry(candidate.to_ascii_lowercase())
            .or_insert_with(|| CandidateAuditState::new(candidate.to_ascii_lowercase(), tier));
        update(state);
    }

    fn record_candidate_failure(
        &self,
        request: &ScheduledReadRequest,
        candidate: &str,
        failure: IngestionFailure,
    ) {
        self.with_candidate_audit(request, candidate, |audit| {
            audit.record_failure(wall_clock_ms(), failure)
        });
    }
}

pub fn candle_subscription_request(asset: &str, interval: CandleInterval) -> Value {
    json!({
        "method": "subscribe",
        "subscription": {
            "type": "candle",
            "coin": asset,
            "interval": interval.api_name()
        }
    })
}

pub fn parse_candle_stream_message(
    bytes: &[u8],
    now_ms: u64,
) -> Result<Option<CandleResponse>, PublicReadError> {
    let envelope: Value =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    if envelope.get("channel").and_then(Value::as_str) != Some("candle") {
        return Ok(None);
    }
    let data = envelope
        .get("data")
        .ok_or(PublicReadError::InvalidPayload)?;
    let candle = parse_candle_value(data, None, None)?;
    if candle.close_time_ms > now_ms {
        return Ok(None);
    }
    Ok(Some(CandleResponse { candle }))
}

fn parse_candle_snapshot(
    asset: &str,
    interval: CandleInterval,
    bytes: &[u8],
    now_ms: u64,
) -> Result<Vec<ClosedCandle>, PublicReadError> {
    let values: Vec<Value> =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    if values.len() > 5_000 {
        return Err(PublicReadError::InvalidPayload);
    }
    values
        .iter()
        .map(|value| parse_candle_value(value, Some(asset), Some(interval)))
        .filter_map(|result| match result {
            Ok(candle)
                if candle
                    .close_time_ms
                    .checked_add(CLOSED_CANDLE_FINALITY_DELAY_MS)
                    .is_some_and(|finalized_at| finalized_at <= now_ms) =>
            {
                Some(Ok(candle))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn parse_candle_value(
    value: &Value,
    expected_asset: Option<&str>,
    expected_interval: Option<CandleInterval>,
) -> Result<ClosedCandle, PublicReadError> {
    let text = |key| {
        value
            .get(key)
            .and_then(Value::as_str)
            .ok_or(PublicReadError::InvalidPayload)
    };
    let number = |key| {
        value
            .get(key)
            .and_then(Value::as_u64)
            .ok_or(PublicReadError::InvalidPayload)
    };
    let asset = text("s")?;
    let interval_name = text("i")?;
    let interval = CandleInterval::ALL
        .into_iter()
        .find(|candidate| candidate.api_name() == interval_name)
        .ok_or(PublicReadError::InvalidPayload)?;
    if expected_asset.is_some_and(|expected| expected != asset)
        || expected_interval.is_some_and(|expected| expected != interval)
    {
        return Err(PublicReadError::InvalidPayload);
    }
    let decimal = |key| Decimal::from_str(text(key)?).map_err(|_| PublicReadError::InvalidPayload);
    Ok(ClosedCandle {
        asset: asset.to_string(),
        interval,
        open_time_ms: number("t")?,
        close_time_ms: number("T")?,
        open: decimal("o")?,
        high: decimal("h")?,
        low: decimal("l")?,
        close: decimal("c")?,
        base_volume: decimal("v")?,
        trade_count: number("n")?,
    })
}

impl<C: Clock> ReadOnlyDataSource for HyperliquidPublicTransport<C> {
    fn execute(
        &self,
        request: ScheduledReadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ReadResponse, ReadFailure>> + Send + '_>> {
        Box::pin(async move {
            let accepted = self.perform(&request).await?;
            let response = ReadResponse {
                received_at: accepted.received_at_mono,
                valid_until: accepted.valid_until_mono.min(request.expires_at),
                payload_valid: true,
                actual_weight: request.weight,
            };
            self.accepted
                .lock()
                .expect("public response queue mutex poisoned")
                .push_back(accepted);
            self.metrics
                .responses_accepted
                .fetch_add(1, Ordering::SeqCst);
            Ok(response)
        })
    }
}

fn validate_endpoint(endpoint: &Url) -> Result<(), PublicReadError> {
    if endpoint.scheme() != "https"
        || endpoint.host_str() != Some("api.hyperliquid.xyz")
        || endpoint.path() != "/info"
        || endpoint.port().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(PublicReadError::InvalidPolicy);
    }
    Ok(())
}

fn modeled_body(request: &ScheduledReadRequest) -> Result<(String, Value), ReadFailure> {
    match (&request.kind, &request.key.subject) {
        (
            ReadRequestKind::SourceFills,
            RequestSubject::SourceHistory {
                candidate,
                start_ms,
                end_ms,
            },
        ) if start_ms <= end_ms
            && end_ms - start_ms <= 86_400_000
            && validate_candidate_address(candidate).is_ok() =>
        {
            Ok((
                candidate.clone(),
                json!({"type":"userFillsByTime","user":candidate,"startTime":start_ms,"endTime":end_ms,"aggregateByTime":false}),
            ))
        }
        (
            ReadRequestKind::SourceState | ReadRequestKind::ExpandedSourceState,
            RequestSubject::Candidate(candidate),
        ) => Ok((
            candidate.clone(),
            json!({"type":"clearinghouseState","user":candidate}),
        )),
        (ReadRequestKind::MarketMids, RequestSubject::Market) => {
            Ok(("market".to_string(), json!({"type":"allMids"})))
        }
        (ReadRequestKind::ExchangeMetadata, RequestSubject::Market) => {
            Ok(("market".to_string(), json!({"type":"metaAndAssetCtxs"})))
        }
        (ReadRequestKind::ExchangeMetadata, RequestSubject::Asset(dex))
            if !dex.is_empty()
                && dex.len() <= 32
                && dex
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) =>
        {
            Ok((dex.clone(), json!({"type":"meta","dex":dex})))
        }
        (ReadRequestKind::OrderBook, RequestSubject::Asset(asset)) => {
            Ok((asset.clone(), json!({"type":"l2Book","coin":asset})))
        }
        _ => Err(ReadFailure::Permanent),
    }
}

async fn read_bounded(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, ReadFailure> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(classify_transport_error)? {
        if bytes
            .len()
            .checked_add(chunk.len())
            .is_none_or(|size| size > maximum)
        {
            return Err(ReadFailure::InvalidResponse);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
fn parse_source_state(
    candidate: &str,
    bytes: &[u8],
) -> Result<SourceStateResponse, IngestionFailure> {
    parse_source_state_with_metadata(candidate, bytes, &BTreeSet::new())
}

fn parse_source_state_with_metadata(
    candidate: &str,
    bytes: &[u8],
    market_assets: &BTreeSet<String>,
) -> Result<SourceStateResponse, IngestionFailure> {
    validate_candidate_address(candidate)?;
    let wire: ClearinghouseStateWire = serde_json::from_slice(bytes).map_err(|error| {
        IngestionFailure::new(
            IngestionFailureClassification::SchemaLayoutMismatch,
            DecodeStage::BodyHashed,
            format!("clearinghouseState schema decode failed: {error}"),
        )
    })?;
    let account_value = parse_wire_decimal(&wire.margin_summary.account_value, "accountValue")?;
    if account_value < Decimal::ZERO {
        return Err(IngestionFailure::new(
            IngestionFailureClassification::MissingRequiredEquity,
            DecodeStage::NumericsValidated,
            "accountValue is negative",
        ));
    }
    let now = wall_clock_ms();
    if wire.time > now.saturating_add(5_000) {
        return Err(IngestionFailure::new(
            IngestionFailureClassification::StaleFutureTimestamp,
            DecodeStage::NumericsValidated,
            format!(
                "source timestamp {} is more than 5s in the future",
                wire.time
            ),
        ));
    }
    let mut parsed = BTreeMap::new();
    for item in wire.asset_positions {
        let position = item.position;
        let asset = position.coin;
        if asset.is_empty() {
            return Err(IngestionFailure::new(
                IngestionFailureClassification::SchemaLayoutMismatch,
                DecodeStage::SchemaDecoded,
                "position coin is empty",
            ));
        }
        if !market_assets.is_empty() && !market_assets.contains(&asset) {
            return Err(IngestionFailure::new(
                IngestionFailureClassification::AssetMetadataDependencyMissing,
                DecodeStage::NumericsValidated,
                format!("position asset {asset} is absent from current market metadata"),
            ));
        }
        let signed_size = parse_wire_decimal(&position.szi, "szi")?;
        let notional = parse_wire_decimal(&position.position_value, "positionValue")?;
        let signed_notional = if signed_size.is_sign_negative() {
            -notional.abs()
        } else {
            notional.abs()
        };
        let entry_price = position
            .entry_px
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| parse_wire_decimal(value, "entryPx"))
            .transpose()?;
        let unrealized_pnl = position
            .unrealized_pnl
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| parse_wire_decimal(value, "unrealizedPnl"))
            .transpose()?;
        if entry_price.is_some_and(|value| value <= Decimal::ZERO) {
            return Err(IngestionFailure::new(
                IngestionFailureClassification::InvalidNumericValue,
                DecodeStage::NumericsValidated,
                "entryPx must be positive when supplied",
            ));
        }
        if !signed_size.is_zero() {
            parsed.insert(
                asset.clone(),
                SourceAssetPosition {
                    asset,
                    signed_size,
                    signed_notional,
                    entry_price,
                    unrealized_pnl,
                },
            );
        }
    }
    if account_value.is_zero() && !parsed.is_empty() {
        return Err(IngestionFailure::new(
            IngestionFailureClassification::MissingRequiredEquity,
            DecodeStage::MetadataResolved,
            "zero accountValue cannot carry a nonzero position",
        ));
    }
    Ok(SourceStateResponse {
        candidate_id: candidate.to_ascii_lowercase(),
        account_value,
        source_time_ms: wire.time,
        positions: parsed,
        closed_candles: Vec::new(),
    })
}

fn merge_source_states(
    aggregate: &mut SourceStateResponse,
    additional: SourceStateResponse,
) -> Result<(), ReadFailure> {
    if aggregate.candidate_id != additional.candidate_id {
        return Err(ReadFailure::InvalidResponse);
    }
    aggregate.account_value = aggregate
        .account_value
        .checked_add(additional.account_value)
        .ok_or(ReadFailure::InvalidResponse)?;
    aggregate.source_time_ms = aggregate.source_time_ms.min(additional.source_time_ms);
    for (asset, position) in additional.positions {
        if aggregate.positions.insert(asset, position).is_some() {
            return Err(ReadFailure::InvalidResponse);
        }
    }
    Ok(())
}

fn validate_candidate_address(candidate: &str) -> Result<(), IngestionFailure> {
    if candidate.len() == 42
        && candidate.starts_with("0x")
        && candidate[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && candidate[2..].bytes().any(|byte| byte != b'0')
    {
        Ok(())
    } else {
        Err(IngestionFailure::new(
            IngestionFailureClassification::AccountGenuinelyAbsent,
            DecodeStage::NotAttempted,
            "candidate address is not a nonzero 20-byte hexadecimal address",
        ))
    }
}

fn parse_wire_decimal(value: &str, field: &str) -> Result<Decimal, IngestionFailure> {
    Decimal::from_str(value).map_err(|error| {
        IngestionFailure::new(
            IngestionFailureClassification::InvalidNumericValue,
            DecodeStage::SchemaDecoded,
            format!("{field} is not a Decimal: {error}"),
        )
    })
}

fn generic_decode_failure(error: PublicReadError) -> IngestionFailure {
    IngestionFailure::new(
        IngestionFailureClassification::OtherExplicitDecodeFailure,
        DecodeStage::BodyHashed,
        format!("public payload decode failed: {error:?}"),
    )
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn parse_mids(bytes: &[u8]) -> Result<MarketSnapshotResponse, PublicReadError> {
    let raw: BTreeMap<String, String> =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    let mids: BTreeMap<String, Decimal> = raw
        .into_iter()
        .map(|(asset, value)| parse_positive(&value).map(|price| (asset, price)))
        .collect::<Result<_, _>>()?;
    if mids.is_empty() {
        return Err(PublicReadError::InvalidPayload);
    }
    Ok(MarketSnapshotResponse { mids })
}

fn merge_market_snapshot(
    aggregate: &mut MarketSnapshotResponse,
    additional: MarketSnapshotResponse,
) -> Result<(), ReadFailure> {
    for (asset, mid) in additional.mids {
        if aggregate.mids.insert(asset, mid).is_some() {
            return Err(ReadFailure::InvalidResponse);
        }
    }
    Ok(())
}

fn merge_market_metadata(
    aggregate: &mut MarketMetadataResponse,
    additional: MarketMetadataResponse,
) -> Result<(), ReadFailure> {
    let mut names = aggregate
        .universe
        .iter()
        .map(|asset| asset.name.as_str())
        .collect::<BTreeSet<_>>();
    if additional
        .universe
        .iter()
        .any(|asset| !names.insert(asset.name.as_str()))
    {
        return Err(ReadFailure::InvalidResponse);
    }
    drop(names);
    aggregate.universe.extend(additional.universe);
    for (asset, context) in additional.contexts {
        if aggregate.contexts.insert(asset, context).is_some() {
            return Err(ReadFailure::InvalidResponse);
        }
    }
    Ok(())
}

fn parse_user_taker_fee_bps(bytes: &[u8]) -> Result<Decimal, PublicReadError> {
    let wire: UserFeesWire =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    Decimal::from_str(&wire.user_cross_rate)
        .ok()
        .and_then(|rate| rate.checked_mul(Decimal::from(10_000)))
        .filter(|fee| *fee >= Decimal::ZERO && *fee <= Decimal::from(100))
        .ok_or(PublicReadError::InvalidPayload)
}

#[cfg(test)]
fn parse_metadata(bytes: &[u8]) -> Result<MarketMetadataResponse, PublicReadError> {
    parse_metadata_with_technical_universe(bytes).map(|(metadata, _)| metadata)
}

fn parse_metadata_with_technical_universe(
    bytes: &[u8],
) -> Result<(MarketMetadataResponse, Vec<String>), PublicReadError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    let (metadata, raw_contexts) = match value.as_array() {
        Some(parts) if parts.len() == 2 => (&parts[0], parts[1].as_array()),
        None => (&value, None),
        _ => return Err(PublicReadError::InvalidPayload),
    };
    let universe = metadata
        .get("universe")
        .and_then(Value::as_array)
        .ok_or(PublicReadError::InvalidPayload)?;
    let mut parsed = Vec::with_capacity(universe.len());
    for asset in universe {
        let name = asset
            .get("name")
            .and_then(Value::as_str)
            .ok_or(PublicReadError::InvalidPayload)?
            .to_string();
        let size_decimals = asset
            .get("szDecimals")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(PublicReadError::InvalidPayload)?;
        if name.is_empty() || size_decimals > 18 {
            return Err(PublicReadError::InvalidPayload);
        }
        parsed.push(MarketMetadataAsset {
            name,
            size_decimals,
        });
    }
    if parsed.is_empty() {
        return Err(PublicReadError::InvalidPayload);
    }
    let mut contexts = BTreeMap::new();
    let mut day_notionals = BTreeMap::new();
    if let Some(raw_contexts) = raw_contexts {
        if raw_contexts.len() != parsed.len() {
            return Err(PublicReadError::InvalidPayload);
        }
        for (asset, context) in parsed.iter().zip(raw_contexts) {
            let funding_rate_hourly = decimal_at(context, &["funding"])?;
            if let Some(day_notional) = optional_decimal_at(context, "dayNtlVlm")? {
                if day_notional < Decimal::ZERO {
                    return Err(PublicReadError::InvalidPayload);
                }
                day_notionals.insert(asset.name.clone(), day_notional);
            }
            contexts.insert(
                asset.name.clone(),
                MarketAssetContext {
                    funding_rate_hourly,
                },
            );
        }
    }
    let technical_assets = ranked_technical_assets(&parsed, &day_notionals);
    Ok((
        MarketMetadataResponse {
            universe: parsed,
            contexts,
            live_taker_fee_bps: None,
        },
        technical_assets,
    ))
}

fn parse_named_perp_meta(
    bytes: &[u8],
    dex: &str,
) -> Result<MarketMetadataResponse, PublicReadError> {
    let raw: Value = serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    let rows = raw
        .get("universe")
        .and_then(Value::as_array)
        .ok_or(PublicReadError::InvalidPayload)?;
    let prefix = format!("{dex}:");
    let mut names = BTreeSet::new();
    if rows.is_empty() || rows.len() > 1_000 {
        return Err(PublicReadError::InvalidPayload);
    }
    for row in rows {
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .ok_or(PublicReadError::InvalidPayload)?;
        if !name.starts_with(&prefix)
            || name.len() <= prefix.len()
            || name.len() > 64
            || !names.insert(name)
            || !row
                .get("szDecimals")
                .and_then(Value::as_u64)
                .is_some_and(|n| n <= 6)
            || !row
                .get("maxLeverage")
                .and_then(Value::as_u64)
                .is_some_and(|n| n > 0)
        {
            return Err(PublicReadError::InvalidPayload);
        }
        if let Some(id) = row.get("marginTableId") {
            let table = raw
                .get("marginTables")
                .and_then(Value::as_array)
                .and_then(|tables| tables.iter().find(|table| table.get(0) == Some(id)))
                .and_then(|table| table.get(1))
                .and_then(|table| table.get("marginTiers"))
                .and_then(Value::as_array)
                .filter(|tiers| !tiers.is_empty())
                .ok_or(PublicReadError::InvalidPayload)?;
            if table.iter().any(|tier| {
                !tier
                    .get("maxLeverage")
                    .and_then(Value::as_u64)
                    .is_some_and(|n| n > 0)
                    || decimal_at(tier, &["lowerBound"]).map_or(true, |n| n < Decimal::ZERO)
            }) {
                return Err(PublicReadError::InvalidPayload);
            }
        }
    }
    parse_metadata_with_technical_universe(bytes).map(|(metadata, _)| metadata)
}

/// Parses the `allPerpMetas` response without assuming a fixed builder-DEX
/// list. Hyperliquid has returned both pair-shaped and object-shaped aggregate
/// encodings over time, so the decoder accepts only bounded containers that
/// contain an explicit `universe` and an unambiguous DEX name.
fn parse_all_perp_metas(
    bytes: &[u8],
    dex_order: &[String],
) -> Result<(MarketMetadataResponse, BTreeSet<String>), PublicReadError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    let mut groups = Vec::<(String, &Value)>::new();
    if let Some(rows) = value.as_array().filter(|rows| {
        !rows.is_empty()
            && rows
                .iter()
                .all(|row| row.get("universe").and_then(Value::as_array).is_some())
    }) {
        if rows.len() != dex_order.len() {
            return Err(PublicReadError::InvalidPayload);
        }
        groups.extend(
            rows.iter()
                .zip(dex_order)
                .map(|(metadata, dex)| (dex.clone(), metadata)),
        );
    } else {
        collect_perp_meta_groups(&value, None, &mut groups)?;
    }
    if groups.is_empty() || groups.len() > 21 {
        return Err(PublicReadError::InvalidPayload);
    }
    let mut universe = Vec::new();
    let mut names = BTreeSet::new();
    let mut dexes = BTreeSet::new();
    for (dex, metadata) in groups {
        if !dex.is_empty() {
            if dex.len() > 32
                || !dex
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            {
                return Err(PublicReadError::InvalidPayload);
            }
            // Hyna is being sunset. Keep its place in `dex_order` so later
            // builder-perp asset indices remain exact, but do not admit its
            // markets into metadata, subscriptions, or candidate production.
            if dex == "hyna" {
                continue;
            }
            dexes.insert(dex.clone());
        }
        let rows = metadata
            .get("universe")
            .and_then(Value::as_array)
            .ok_or(PublicReadError::InvalidPayload)?;
        if rows.is_empty() || rows.len() > 1_000 {
            return Err(PublicReadError::InvalidPayload);
        }
        for row in rows {
            let raw_name = row
                .get("name")
                .and_then(Value::as_str)
                .ok_or(PublicReadError::InvalidPayload)?;
            let name = if dex.is_empty() || raw_name.contains(':') {
                raw_name.to_string()
            } else {
                format!("{dex}:{raw_name}")
            };
            let size_decimals = row
                .get("szDecimals")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value <= 18)
                .ok_or(PublicReadError::InvalidPayload)?;
            if name.is_empty() || name.len() > 64 || !names.insert(name.clone()) {
                return Err(PublicReadError::InvalidPayload);
            }
            universe.push(MarketMetadataAsset {
                name,
                size_decimals,
            });
        }
    }
    if universe.is_empty() || universe.len() > 850 {
        return Err(PublicReadError::InvalidPayload);
    }
    Ok((
        MarketMetadataResponse {
            universe,
            contexts: BTreeMap::new(),
            live_taker_fee_bps: None,
        },
        dexes,
    ))
}

fn parse_perp_dex_order(bytes: &[u8]) -> Result<Vec<String>, PublicReadError> {
    let rows: Vec<Value> =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    if rows.is_empty() || rows.len() > 21 || !rows[0].is_null() {
        return Err(PublicReadError::InvalidPayload);
    }
    let mut order = vec![String::new()];
    let mut unique = BTreeSet::new();
    for row in rows.into_iter().skip(1) {
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .ok_or(PublicReadError::InvalidPayload)?;
        if name.is_empty()
            || name.len() > 32
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            || !unique.insert(name.to_string())
        {
            return Err(PublicReadError::InvalidPayload);
        }
        order.push(name.to_string());
    }
    Ok(order)
}

fn collect_perp_meta_groups<'a>(
    value: &'a Value,
    dex_hint: Option<&str>,
    groups: &mut Vec<(String, &'a Value)>,
) -> Result<(), PublicReadError> {
    if value.get("universe").and_then(Value::as_array).is_some() {
        groups.push((dex_hint.unwrap_or("").to_string(), value));
        return Ok(());
    }
    if let Some(parts) = value.as_array() {
        if parts.len() == 2
            && (parts[0].is_null() || parts[0].is_string())
            && parts[1].get("universe").and_then(Value::as_array).is_some()
        {
            let dex = parts[0].as_str().unwrap_or("");
            groups.push((dex.to_string(), &parts[1]));
            return Ok(());
        }
        if parts.len() > 21 {
            return Err(PublicReadError::InvalidPayload);
        }
        for part in parts {
            collect_perp_meta_groups(part, dex_hint, groups)?;
        }
        return Ok(());
    }
    if let Some(object) = value.as_object() {
        if let Some(meta) = object.get("meta") {
            let dex = object
                .get("dex")
                .or_else(|| object.get("name"))
                .and_then(Value::as_str)
                .or(dex_hint);
            return collect_perp_meta_groups(meta, dex, groups);
        }
        if object.len() > 21 {
            return Err(PublicReadError::InvalidPayload);
        }
        for (dex, child) in object {
            if child.get("universe").and_then(Value::as_array).is_some() {
                collect_perp_meta_groups(child, Some(dex), groups)?;
            }
        }
    }
    Ok(())
}

fn ranked_technical_assets(
    universe: &[MarketMetadataAsset],
    day_notionals: &BTreeMap<String, Decimal>,
) -> Vec<String> {
    let mut ranked = universe
        .iter()
        .map(|asset| asset.name.clone())
        .collect::<Vec<_>>();
    ranked.sort_by(
        |left, right| match (day_notionals.get(left), day_notionals.get(right)) {
            (Some(left_volume), Some(right_volume)) => {
                right_volume.cmp(left_volume).then_with(|| left.cmp(right))
            }
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => left.cmp(right),
        },
    );
    ranked.truncate(TECHNICAL_UNIVERSE_LIMIT);
    ranked
}

fn parse_book(asset: &str, bytes: &[u8]) -> Result<OrderBookResponse, PublicReadError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| PublicReadError::InvalidPayload)?;
    if value.get("coin").and_then(Value::as_str) != Some(asset) {
        return Err(PublicReadError::InvalidPayload);
    }
    let source_time_ms = value
        .get("time")
        .and_then(Value::as_u64)
        .ok_or(PublicReadError::InvalidPayload)?;
    let levels = value
        .get("levels")
        .and_then(Value::as_array)
        .filter(|levels| levels.len() == 2)
        .ok_or(PublicReadError::InvalidPayload)?;
    let bids = parse_levels(&levels[0])?;
    let asks = parse_levels(&levels[1])?;
    if bids.is_empty() || asks.is_empty() {
        return Err(PublicReadError::InvalidPayload);
    }
    Ok(OrderBookResponse {
        asset: asset.to_string(),
        source_time_ms,
        bids,
        asks,
    })
}

fn parse_levels(value: &Value) -> Result<Vec<BookLevel>, PublicReadError> {
    value
        .as_array()
        .ok_or(PublicReadError::InvalidPayload)?
        .iter()
        .map(|level| {
            let price = decimal_at(level, &["px"])?;
            let quantity = decimal_at(level, &["sz"])?;
            if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
                return Err(PublicReadError::InvalidPayload);
            }
            Ok(BookLevel { price, quantity })
        })
        .collect()
}

fn decimal_at(value: &Value, path: &[&str]) -> Result<Decimal, PublicReadError> {
    let mut current = value;
    for part in path {
        current = current.get(*part).ok_or(PublicReadError::InvalidPayload)?;
    }
    parse_decimal_value(current)
}

fn optional_decimal_at(value: &Value, field: &str) -> Result<Option<Decimal>, PublicReadError> {
    value.get(field).map(parse_decimal_value).transpose()
}

fn parse_decimal_value(value: &Value) -> Result<Decimal, PublicReadError> {
    match value {
        Value::String(text) => Decimal::from_str(text),
        Value::Number(number) => Decimal::from_str(&number.to_string()),
        _ => return Err(PublicReadError::InvalidPayload),
    }
    .map_err(|_| PublicReadError::InvalidPayload)
}

fn parse_positive(value: &str) -> Result<Decimal, PublicReadError> {
    let value = Decimal::from_str(value).map_err(|_| PublicReadError::InvalidPayload)?;
    if value <= Decimal::ZERO {
        Err(PublicReadError::InvalidPayload)
    } else {
        Ok(value)
    }
}

fn classify_transport_error(error: reqwest::Error) -> ReadFailure {
    if error.is_timeout() {
        ReadFailure::Timeout
    } else if error.is_connect() {
        ReadFailure::Transport
    } else {
        ReadFailure::InvalidResponse
    }
}

fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1_000)
        .filter(|delay| *delay > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::scheduler::ManualClock;

    fn policy() -> PublicTransportPolicy {
        PublicTransportPolicy {
            connect_timeout_ms: 100,
            request_timeout_ms: 200,
            maximum_response_bytes: 1024 * 1024,
            active_source_interval_ms: 20_000,
            inactive_source_interval_ms: 60_000,
            queue_delay_allowance_ms: 5_000,
            transport_p99_allowance_ms: 10_000,
            market_validity_ms: 20_000,
            perp_dexes: Vec::new(),
            streaming: StreamingPolicy::default(),
        }
    }

    #[test]
    fn host_policy_and_limits_fail_closed() {
        assert!(HyperliquidPublicTransport::new(ManualClock::new(0), policy()).is_ok());
        let mut invalid = policy();
        invalid.maximum_response_bytes = 0;
        assert!(HyperliquidPublicTransport::new(ManualClock::new(0), invalid).is_err());
        assert!(validate_endpoint(&Url::parse("https://example.com/info").unwrap()).is_err());
        assert!(
            validate_endpoint(&Url::parse("https://api.hyperliquid.xyz/exchange").unwrap())
                .is_err()
        );
    }

    #[test]
    fn all_perp_metas_discovers_default_and_builder_markets() {
        let bytes = serde_json::to_vec(&json!([
            {"universe":[{"name":"BTC","szDecimals":5}]},
            {"universe":[{"name":"XYZ100","szDecimals":2}]}
        ]))
        .unwrap();
        let dex_bytes = serde_json::to_vec(&json!([null,{"name":"xyz"}])).unwrap();
        let dex_order = parse_perp_dex_order(&dex_bytes).unwrap();
        let (metadata, dexes) = parse_all_perp_metas(&bytes, &dex_order).unwrap();
        assert_eq!(
            metadata
                .universe
                .iter()
                .map(|asset| asset.name.as_str())
                .collect::<Vec<_>>(),
            vec!["BTC", "xyz:XYZ100"]
        );
        assert_eq!(dexes, BTreeSet::from(["xyz".to_string()]));
    }

    #[test]
    fn all_perp_metas_excludes_sunset_hyna_without_reordering_other_dexes() {
        let bytes = serde_json::to_vec(&json!([
            {"universe":[{"name":"BTC","szDecimals":5}]},
            {"universe":[{"name":"HYPE100","szDecimals":2}]},
            {"universe":[{"name":"XYZ100","szDecimals":2}]}
        ]))
        .unwrap();
        let dex_bytes = serde_json::to_vec(&json!([
            null,
            {"name":"hyna"},
            {"name":"xyz"}
        ]))
        .unwrap();
        let dex_order = parse_perp_dex_order(&dex_bytes).unwrap();
        assert_eq!(dex_order, vec!["", "hyna", "xyz"]);

        let (metadata, dexes) = parse_all_perp_metas(&bytes, &dex_order).unwrap();
        assert_eq!(
            metadata
                .universe
                .iter()
                .map(|asset| asset.name.as_str())
                .collect::<Vec<_>>(),
            vec!["BTC", "xyz:XYZ100"]
        );
        assert_eq!(dexes, BTreeSet::from(["xyz".to_string()]));
    }

    #[test]
    fn live_user_cross_rate_is_parsed_as_bounded_taker_bps() {
        assert_eq!(
            parse_user_taker_fee_bps(br#"{"userCrossRate":"0.000315"}"#).unwrap(),
            Decimal::new(315, 2)
        );
        assert!(parse_user_taker_fee_bps(br#"{"userCrossRate":"-0.1"}"#).is_err());
        assert!(parse_user_taker_fee_bps(br#"{"userCrossRate":"nan"}"#).is_err());
    }

    #[test]
    fn public_payload_parsers_reject_malformed_and_accept_documented_shapes() {
        let state = br#"{"marginSummary":{"accountValue":"100"},"assetPositions":[{"position":{"coin":"BTC","szi":"-0.01","positionValue":"650","entryPx":"64000","unrealizedPnl":"10.5"}}],"time":1}"#;
        let address = "0x1111111111111111111111111111111111111111";
        let parsed = parse_source_state(address, state).unwrap();
        assert_eq!(parsed.positions["BTC"].signed_notional, Decimal::from(-650));
        assert_eq!(
            parsed.positions["BTC"].entry_price,
            Some(Decimal::from(64_000))
        );
        assert_eq!(
            parsed.positions["BTC"].unrealized_pnl,
            Some(Decimal::new(105, 1))
        );
        assert!(parse_source_state(address, b"{}").is_err());
        let zero = br#"{"marginSummary":{"accountValue":"0"},"assetPositions":[],"time":1}"#;
        let parsed_zero = parse_source_state(address, zero).unwrap();
        assert!(parsed_zero.positions.is_empty());
        assert_eq!(parsed_zero.account_value, Decimal::ZERO);
        let metadata =
            br#"[{"universe":[{"name":"BTC","szDecimals":5}]},[{"funding":"0.0000125"}]]"#;
        let parsed_metadata = parse_metadata(metadata).unwrap();
        assert_eq!(
            parsed_metadata.contexts["BTC"].funding_rate_hourly,
            Decimal::from_str("0.0000125").unwrap()
        );
        let book = br#"{"coin":"BTC","time":1,"levels":[[{"px":"99","sz":"2","n":1}],[{"px":"101","sz":"3","n":1}]]}"#;
        assert_eq!(
            parse_book("BTC", book).unwrap().asks[0].quantity,
            Decimal::from(3)
        );
        assert!(parse_book("ETH", book).is_err());
    }

    #[test]
    fn default_and_xyz_perpetual_surfaces_merge_without_collisions() {
        let mut metadata = parse_metadata(
            br#"[{"universe":[{"name":"BTC","szDecimals":5}]},[{"funding":"0.00001"}]]"#,
        )
        .unwrap();
        let xyz = parse_metadata(
            br#"[{"universe":[{"name":"xyz:SP500","szDecimals":2},{"name":"xyz:GOLD","szDecimals":3}]},[{"funding":"0.00002"},{"funding":"-0.00001"}]]"#,
        )
        .unwrap();
        merge_market_metadata(&mut metadata, xyz).unwrap();
        assert_eq!(metadata.universe.len(), 3);
        assert!(metadata.contexts.contains_key("xyz:SP500"));

        let mut mids = parse_mids(br#"{"BTC":"65000"}"#).unwrap();
        merge_market_snapshot(
            &mut mids,
            parse_mids(br#"{"xyz:SP500":"6400","xyz:GOLD":"3400"}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(mids.mids["xyz:GOLD"], Decimal::from(3_400));
        assert!(
            merge_market_snapshot(&mut mids, parse_mids(br#"{"BTC":"65001"}"#).unwrap()).is_err()
        );
    }

    #[test]
    fn multi_dex_source_states_merge_as_one_atomic_candidate_snapshot() {
        let address = "0x1111111111111111111111111111111111111111";
        let mut aggregate = parse_source_state(
            address,
            br#"{"marginSummary":{"accountValue":"100"},"assetPositions":[{"position":{"coin":"BTC","szi":"0.01","positionValue":"65"}}],"time":100}"#,
        )
        .unwrap();
        let xyz = parse_source_state(
            address,
            br#"{"marginSummary":{"accountValue":"40"},"assetPositions":[{"position":{"coin":"xyz:SP500","szi":"0.02","positionValue":"128"}}],"time":95}"#,
        )
        .unwrap();
        merge_source_states(&mut aggregate, xyz).unwrap();
        assert_eq!(aggregate.account_value, Decimal::from(140));
        assert_eq!(aggregate.source_time_ms, 95);
        assert!(aggregate.positions.contains_key("BTC"));
        assert!(aggregate.positions.contains_key("xyz:SP500"));
    }

    #[test]
    fn metadata_day_notional_selects_a_deterministic_bounded_technical_universe() {
        let universe = (0..27)
            .map(|index| {
                json!({
                    "name": format!("A{index:02}"),
                    "szDecimals": 2
                })
            })
            .collect::<Vec<_>>();
        let contexts = (0..27)
            .map(|index| {
                let day_notional = if index < 2 { 1_000 } else { 1_000 - index };
                json!({
                    "funding": "0",
                    "dayNtlVlm": day_notional.to_string()
                })
            })
            .collect::<Vec<_>>();
        let payload = serde_json::to_vec(&json!([
            { "universe": universe },
            contexts
        ]))
        .unwrap();

        let (_, technical_assets) = parse_metadata_with_technical_universe(&payload).unwrap();

        assert_eq!(technical_assets.len(), TECHNICAL_UNIVERSE_LIMIT);
        assert_eq!(&technical_assets[..3], &["A00", "A01", "A02"]);
        assert_eq!(technical_assets.last().map(String::as_str), Some("A24"));
        assert!(!technical_assets.contains(&"A25".to_string()));
        assert!(!technical_assets.contains(&"A26".to_string()));
    }

    #[test]
    fn source_held_assets_are_unioned_outside_the_ranked_technical_limit() {
        let mut state = TechnicalCandleFetchState::default();
        state.replace_technical_assets(
            (0..TECHNICAL_UNIVERSE_LIMIT).map(|index| format!("TECH{index:02}")),
        );
        let source_asset = SourceAssetPosition {
            asset: "EXIT_ONLY".to_string(),
            signed_size: Decimal::ONE,
            signed_notional: Decimal::from(100),
            entry_price: Some(Decimal::from(100)),
            unrealized_pnl: Some(Decimal::ZERO),
        };
        state.replace_candidate_assets(
            "0x1111111111111111111111111111111111111111",
            &BTreeMap::from([(source_asset.asset.clone(), source_asset)]),
        );

        let assets = state.all_assets();
        assert_eq!(assets.len(), TECHNICAL_UNIVERSE_LIMIT + 1);
        assert!(assets.contains("EXIT_ONLY"));

        state.replace_technical_assets(["ROTATED".to_string()]);
        assert!(state.all_assets().contains("EXIT_ONLY"));

        state.replace_candidate_assets(
            "0x1111111111111111111111111111111111111111",
            &BTreeMap::new(),
        );
        assert!(!state.all_assets().contains("EXIT_ONLY"));
    }

    #[test]
    fn candle_parser_accepts_every_required_interval_and_only_closed_rows() {
        for interval in CandleInterval::ALL {
            let payload = format!(
                r#"[{{"t":1,"T":99,"s":"BTC","i":"{}","o":"100","c":"101","h":"102","l":"99","v":"12","n":3}},{{"t":100,"T":201,"s":"BTC","i":"{}","o":"101","c":"102","h":"103","l":"100","v":"13","n":4}}]"#,
                interval.api_name(),
                interval.api_name()
            );
            let candles = parse_candle_snapshot(
                "BTC",
                interval,
                payload.as_bytes(),
                99 + CLOSED_CANDLE_FINALITY_DELAY_MS,
            )
            .unwrap();
            assert_eq!(candles.len(), 1);
            assert_eq!(candles[0].interval, interval);
        }
    }

    #[test]
    fn candle_parser_does_not_accept_a_just_closed_row_as_final() {
        let payload = br#"[{"t":1,"T":99,"s":"BTC","i":"15m","o":"100","c":"101","h":"102","l":"99","v":"12","n":3}]"#;
        let before_finality =
            parse_candle_snapshot("BTC", CandleInterval::FifteenMinutes, payload, 5_098).unwrap();
        assert!(before_finality.is_empty());
        let finalized =
            parse_candle_snapshot("BTC", CandleInterval::FifteenMinutes, payload, 5_099).unwrap();
        assert_eq!(finalized.len(), 1);
    }

    #[test]
    fn source_state_failures_have_exact_persistent_classifications() {
        let address = "0x1111111111111111111111111111111111111111";
        let missing_equity = br#"{"marginSummary":{},"assetPositions":[],"time":1}"#;
        assert_eq!(
            parse_source_state(address, missing_equity)
                .unwrap_err()
                .classification,
            IngestionFailureClassification::SchemaLayoutMismatch
        );
        let invalid_numeric =
            br#"{"marginSummary":{"accountValue":"NaN"},"assetPositions":[],"time":1}"#;
        assert_eq!(
            parse_source_state(address, invalid_numeric)
                .unwrap_err()
                .classification,
            IngestionFailureClassification::InvalidNumericValue
        );
        let position_without_equity = br#"{"marginSummary":{"accountValue":"0"},"assetPositions":[{"position":{"coin":"BTC","szi":"1","positionValue":"10"}}],"time":1}"#;
        assert_eq!(
            parse_source_state(address, position_without_equity)
                .unwrap_err()
                .classification,
            IngestionFailureClassification::MissingRequiredEquity
        );
        let metadata = BTreeSet::from(["ETH".to_string()]);
        let missing_asset = br#"{"marginSummary":{"accountValue":"100"},"assetPositions":[{"position":{"coin":"BTC","szi":"1","positionValue":"10"}}],"time":1}"#;
        assert_eq!(
            parse_source_state_with_metadata(address, missing_asset, &metadata)
                .unwrap_err()
                .classification,
            IngestionFailureClassification::AssetMetadataDependencyMissing
        );
    }

    #[test]
    fn only_modeled_bodies_exist() {
        let now = 1;
        let request = ScheduledReadRequest {
            key: crate::domain::scheduler::RequestKey {
                subject: RequestSubject::Asset("BTC".into()),
                kind: ReadRequestKind::OrderBook,
            },
            kind: ReadRequestKind::OrderBook,
            candidate_id: None,
            source_tier: None,
            weight: 2,
            priority: crate::domain::scheduler::RequestPriority::Normal,
            budget_class: crate::domain::scheduler::BudgetClass::Normal,
            created_at: now,
            not_before: now,
            expires_at: 2,
            attempt: 0,
        };
        assert_eq!(
            modeled_body(&request).unwrap().1,
            json!({"type":"l2Book","coin":"BTC"})
        );
        let mut invalid = request;
        invalid.kind = ReadRequestKind::FollowerOpenOrders;
        assert!(modeled_body(&invalid).is_err());
        let metadata = ScheduledReadRequest {
            key: crate::domain::scheduler::RequestKey {
                subject: RequestSubject::Market,
                kind: ReadRequestKind::ExchangeMetadata,
            },
            kind: ReadRequestKind::ExchangeMetadata,
            candidate_id: None,
            source_tier: None,
            weight: 20,
            priority: crate::domain::scheduler::RequestPriority::High,
            budget_class: crate::domain::scheduler::BudgetClass::Normal,
            created_at: now,
            not_before: now,
            expires_at: 2,
            attempt: 0,
        };
        assert_eq!(
            modeled_body(&metadata).unwrap().1,
            json!({"type":"metaAndAssetCtxs"})
        );
        let mut scoped = metadata;
        scoped.key.subject = RequestSubject::Asset("xyz".into());
        assert_eq!(
            modeled_body(&scoped).unwrap().1,
            json!({"type":"meta","dex":"xyz"})
        );
        scoped.kind = ReadRequestKind::SourceFills;
        scoped.key.kind = ReadRequestKind::SourceFills;
        let wallet = format!("0x{}", "1".repeat(40));
        scoped.key.subject = RequestSubject::SourceHistory {
            candidate: wallet.clone(),
            start_ms: 100,
            end_ms: 200,
        };
        assert_eq!(
            modeled_body(&scoped).unwrap().1,
            json!({"type":"userFillsByTime","user":wallet,"startTime":100,"endTime":200,"aggregateByTime":false})
        );
        scoped.key.subject = RequestSubject::SourceHistory {
            candidate: wallet,
            start_ms: 100,
            end_ms: 86_400_101,
        };
        assert!(modeled_body(&scoped).is_err());
    }

    #[test]
    fn named_dex_metadata_rejects_corruption_without_native_fallback() {
        let good = json!({"universe":[{"name":"xyz:AMD","szDecimals":3,"maxLeverage":10,"marginTableId":10}],
            "marginTables":[[10,{"marginTiers":[{"lowerBound":"0.0","maxLeverage":10}]}]]});
        let parsed = parse_named_perp_meta(&serde_json::to_vec(&good).unwrap(), "xyz").unwrap();
        assert_eq!(parsed.universe[0].name, "xyz:AMD");
        assert_eq!(parsed.universe[0].size_decimals, 3);
        for (field, value) in [
            ("name", json!("AMD")),
            ("name", json!("other:AMD")),
            ("szDecimals", json!(7)),
            ("szDecimals", json!(null)),
            ("maxLeverage", json!(0)),
            ("marginTableId", json!(99)),
        ] {
            let mut bad = good.clone();
            bad["universe"][0][field] = value;
            assert!(parse_named_perp_meta(&serde_json::to_vec(&bad).unwrap(), "xyz").is_err());
        }
        let mut duplicate = good.clone();
        duplicate["universe"]
            .as_array_mut()
            .unwrap()
            .push(good["universe"][0].clone());
        assert!(parse_named_perp_meta(&serde_json::to_vec(&duplicate).unwrap(), "xyz").is_err());
    }
}
