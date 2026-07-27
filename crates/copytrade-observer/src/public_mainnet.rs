use crate::ingestion::{
    CandidateAuditSnapshot, CandidateAuditState, DecodeStage, IngestionFailure,
    IngestionFailureClassification,
};
use copytrade_core::decision::hash_payload_bytes;
use copytrade_core::scheduler::{
    Clock, ReadFailure, ReadOnlyDataSource, ReadRequestKind, ReadResponse, RequestSubject,
    ScheduledReadRequest,
};
use copytrade_core::technical::{CandleInterval, ClosedCandle};
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
        {
            return Err(PublicReadError::InvalidPolicy);
        }
        Ok(())
    }

    pub fn source_deadline_ms(&self, tier: copytrade_core::scheduler::SourceTier) -> Option<u64> {
        let interval = match tier {
            copytrade_core::scheduler::SourceTier::Active => self.active_source_interval_ms,
            copytrade_core::scheduler::SourceTier::Inactive => self.inactive_source_interval_ms,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStateResponse {
    pub candidate_id: String,
    pub account_value: Decimal,
    pub source_time_ms: u64,
    pub positions: BTreeMap<String, SourceAssetPosition>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshotResponse {
    pub mids: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketMetadataResponse {
    pub universe: Vec<MarketMetadataAsset>,
    pub contexts: BTreeMap<String, MarketAssetContext>,
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
    MarketSnapshot(MarketSnapshotResponse),
    MarketMetadata(MarketMetadataResponse),
    OrderBook(OrderBookResponse),
    Candle(CandleResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedPublicResponse {
    pub request_kind: ReadRequestKind,
    pub source_tier: Option<copytrade_core::scheduler::SourceTier>,
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
        })
    }

    pub fn take_accepted(&self, request: &ScheduledReadRequest) -> Option<AcceptedPublicResponse> {
        let subject = match &request.key.subject {
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

    pub fn candidate_audit_snapshot(&self) -> CandidateAuditSnapshot {
        self.candidate_audit
            .lock()
            .expect("candidate audit mutex poisoned")
            .clone()
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
            .header(reqwest::header::USER_AGENT, "copytrade-observer-hybrid/1")
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

    async fn perform(
        &self,
        request: &ScheduledReadRequest,
    ) -> Result<AcceptedPublicResponse, ReadFailure> {
        let (subject, body) = modeled_body(request)?;
        if request.kind == ReadRequestKind::SourceState {
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
            .header(reqwest::header::USER_AGENT, "copytrade-observer-hl1k/1")
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                if request.kind == ReadRequestKind::SourceState {
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
        if request.kind == ReadRequestKind::SourceState {
            self.with_candidate_audit(request, &subject, |audit| {
                audit.record_http(response.status().as_u16())
            });
        }
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            self.metrics.rate_limited.fetch_add(1, Ordering::SeqCst);
            if request.kind == ReadRequestKind::SourceState {
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
            if request.kind == ReadRequestKind::SourceState {
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
            if request.kind == ReadRequestKind::SourceState {
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
            if request.kind == ReadRequestKind::SourceState {
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
                if request.kind == ReadRequestKind::SourceState {
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
        if request.kind == ReadRequestKind::SourceState {
            let hash = hash_payload_bytes(&bytes);
            self.with_candidate_audit(request, &subject, |audit| audit.record_payload(hash));
        }
        let payload = self
            .parse_payload(request.kind, &subject, &bytes)
            .map_err(|failure| {
                self.metrics
                    .invalid_responses
                    .fetch_add(1, Ordering::SeqCst);
                if request.kind == ReadRequestKind::SourceState {
                    self.record_candidate_failure(request, &subject, failure);
                }
                ReadFailure::InvalidResponse
            })?;
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
        let received_at_mono = self.clock.now_ms();
        let validity = match request.kind {
            ReadRequestKind::SourceState => self
                .policy
                .source_deadline_ms(request.source_tier.ok_or(ReadFailure::InvalidResponse)?)
                .ok_or(ReadFailure::InvalidResponse)?,
            _ => self.policy.market_validity_ms,
        };
        let valid_until_mono = received_at_mono
            .checked_add(validity)
            .ok_or(ReadFailure::InvalidResponse)?;
        if request.kind == ReadRequestKind::SourceState {
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
        kind: ReadRequestKind,
        subject: &str,
        bytes: &[u8],
    ) -> Result<PublicPayload, IngestionFailure> {
        match kind {
            ReadRequestKind::SourceState => {
                let assets = self
                    .market_assets
                    .lock()
                    .expect("market asset mutex poisoned")
                    .clone();
                parse_source_state_with_metadata(subject, bytes, &assets)
                    .map(PublicPayload::SourceState)
            }
            ReadRequestKind::MarketMids => parse_mids(bytes)
                .map(PublicPayload::MarketSnapshot)
                .map_err(generic_decode_failure),
            ReadRequestKind::ExchangeMetadata => {
                let metadata = parse_metadata(bytes).map_err(generic_decode_failure)?;
                *self
                    .market_assets
                    .lock()
                    .expect("market asset mutex poisoned") = metadata
                    .universe
                    .iter()
                    .map(|asset| asset.name.clone())
                    .collect();
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
            .unwrap_or(copytrade_core::scheduler::SourceTier::Inactive);
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
            Ok(candle) if candle.close_time_ms <= now_ms => Some(Ok(candle)),
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
        (ReadRequestKind::SourceState, RequestSubject::Candidate(candidate)) => Ok((
            candidate.clone(),
            json!({"type":"clearinghouseState","user":candidate}),
        )),
        (ReadRequestKind::MarketMids, RequestSubject::Market) => {
            Ok(("market".to_string(), json!({"type":"allMids"})))
        }
        (ReadRequestKind::ExchangeMetadata, RequestSubject::Market) => {
            Ok(("market".to_string(), json!({"type":"metaAndAssetCtxs"})))
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
        if !signed_size.is_zero() {
            parsed.insert(
                asset.clone(),
                SourceAssetPosition {
                    asset,
                    signed_size,
                    signed_notional,
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
    })
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

fn parse_metadata(bytes: &[u8]) -> Result<MarketMetadataResponse, PublicReadError> {
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
    if let Some(raw_contexts) = raw_contexts {
        if raw_contexts.len() != parsed.len() {
            return Err(PublicReadError::InvalidPayload);
        }
        for (asset, context) in parsed.iter().zip(raw_contexts) {
            let funding_rate_hourly = decimal_at(context, &["funding"])?;
            contexts.insert(
                asset.name.clone(),
                MarketAssetContext {
                    funding_rate_hourly,
                },
            );
        }
    }
    Ok(MarketMetadataResponse {
        universe: parsed,
        contexts,
    })
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
    use copytrade_core::scheduler::ManualClock;

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
    fn public_payload_parsers_reject_malformed_and_accept_documented_shapes() {
        let state = br#"{"marginSummary":{"accountValue":"100"},"assetPositions":[{"position":{"coin":"BTC","szi":"-0.01","positionValue":"650"}}],"time":1}"#;
        let address = "0x1111111111111111111111111111111111111111";
        let parsed = parse_source_state(address, state).unwrap();
        assert_eq!(parsed.positions["BTC"].signed_notional, Decimal::from(-650));
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
            key: copytrade_core::scheduler::RequestKey {
                subject: RequestSubject::Asset("BTC".into()),
                kind: ReadRequestKind::OrderBook,
            },
            kind: ReadRequestKind::OrderBook,
            candidate_id: None,
            source_tier: None,
            weight: 2,
            priority: copytrade_core::scheduler::RequestPriority::Normal,
            budget_class: copytrade_core::scheduler::BudgetClass::Normal,
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
            key: copytrade_core::scheduler::RequestKey {
                subject: RequestSubject::Market,
                kind: ReadRequestKind::ExchangeMetadata,
            },
            kind: ReadRequestKind::ExchangeMetadata,
            candidate_id: None,
            source_tier: None,
            weight: 20,
            priority: copytrade_core::scheduler::RequestPriority::High,
            budget_class: copytrade_core::scheduler::BudgetClass::Normal,
            created_at: now,
            not_before: now,
            expires_at: 2,
            attempt: 0,
        };
        assert_eq!(
            modeled_body(&metadata).unwrap().1,
            json!({"type":"metaAndAssetCtxs"})
        );
    }
}
