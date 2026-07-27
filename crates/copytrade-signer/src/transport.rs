use async_trait::async_trait;
use copytrade_core::decision::{hash_payload_bytes, PayloadHash, PlannedCloid};
use ethers::types::{Signature, H160};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;

const EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const MAXIMUM_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct SignedIocRequest {
    pub asset_index: u32,
    pub is_buy: bool,
    pub limit_price: Decimal,
    pub quantity: Decimal,
    pub reduce_only: bool,
    pub cloid: PlannedCloid,
    pub nonce: u64,
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionResponse {
    Acknowledged,
    Filled {
        exchange_order_id: String,
        filled_quantity: Decimal,
        average_fill_price: Decimal,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillCursor {
    pub start_time_ms: u64,
    pub end_time_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingCursor {
    pub start_time_ms: u64,
    pub end_time_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserFill {
    pub cloid: PlannedCloid,
    pub exchange_order_id: String,
    pub trade_id: String,
    pub asset: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub closed_pnl: Decimal,
    pub fee: Decimal,
    pub fee_token: String,
    pub crossed: bool,
    pub occurred_at_ms: u64,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserFillBatch {
    pub fills: Vec<UserFill>,
    pub next_cursor: FillCursor,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingRecord {
    pub event_hash: String,
    pub asset: String,
    pub signed_usdc_delta: Decimal,
    pub occurred_at_ms: u64,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingBatch {
    pub events: Vec<FundingRecord>,
    pub next_cursor: FundingCursor,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangePositionSnapshot {
    pub positions: BTreeMap<String, Decimal>,
    pub account_equity: Decimal,
    pub observed_at_ms: u64,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeEquitySnapshot {
    pub equity: Decimal,
    pub observed_at_ms: u64,
    pub source_hash: PayloadHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderObservation {
    NotFound,
    Open {
        exchange_order_id: String,
        original_quantity: Decimal,
        remaining_quantity: Decimal,
    },
    Filled {
        exchange_order_id: String,
        filled_quantity: Decimal,
    },
    Cancelled {
        exchange_order_id: String,
        filled_quantity: Decimal,
        reason: String,
    },
    Rejected {
        exchange_order_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionTransportError {
    UnknownResult,
    Http(u16),
    InvalidResponse(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationError {
    Http(u16),
    Transport(String),
    InvalidResponse(String),
    Arithmetic,
    IdentityMismatch,
}

impl std::fmt::Display for ReconciliationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ReconciliationError {}

#[async_trait]
pub trait AuthenticatedExchangeTransport: Send + Sync {
    async fn submit_ioc(
        &self,
        request: SignedIocRequest,
    ) -> Result<SubmissionResponse, SubmissionTransportError>;

    async fn lookup_order(
        &self,
        cloid: PlannedCloid,
    ) -> Result<OrderObservation, ReconciliationError>;

    async fn read_fills(&self, cursor: FillCursor) -> Result<UserFillBatch, ReconciliationError>;

    async fn read_positions(&self) -> Result<ExchangePositionSnapshot, ReconciliationError>;

    async fn read_equity(&self) -> Result<ExchangeEquitySnapshot, ReconciliationError>;

    async fn read_funding(
        &self,
        cursor: FundingCursor,
    ) -> Result<FundingBatch, ReconciliationError>;
}

#[derive(Clone)]
pub struct HyperliquidMainnetTransport {
    client: reqwest::Client,
    master_account: String,
}

impl HyperliquidMainnetTransport {
    pub fn new(master_account: String, timeout_ms: u64) -> Result<Self, ReconciliationError> {
        if !valid_address(&master_account) || timeout_ms == 0 {
            return Err(ReconciliationError::InvalidResponse(
                "invalid transport configuration".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()
            .map_err(|error| ReconciliationError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            master_account: master_account.to_ascii_lowercase(),
        })
    }

    async fn info_bytes(&self, body: serde_json::Value) -> Result<Vec<u8>, ReconciliationError> {
        let response = self
            .client
            .post(INFO_URL)
            .json(&body)
            .send()
            .await
            .map_err(|error| ReconciliationError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ReconciliationError::Http(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(ReconciliationError::InvalidResponse(
                "response exceeds size limit".into(),
            ));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| ReconciliationError::Transport(error.to_string()))?;
        if bytes.len() > MAXIMUM_RESPONSE_BYTES {
            return Err(ReconciliationError::InvalidResponse(
                "response exceeds size limit".into(),
            ));
        }
        Ok(bytes.to_vec())
    }

    async fn info<T: DeserializeOwned>(
        &self,
        body: serde_json::Value,
    ) -> Result<(T, PayloadHash), ReconciliationError> {
        let bytes = self.info_bytes(body).await?;
        let hash = hash_payload_bytes(&bytes);
        let decoded = serde_json::from_slice(&bytes)
            .map_err(|error| ReconciliationError::InvalidResponse(error.to_string()))?;
        Ok((decoded, hash))
    }
}

#[async_trait]
impl AuthenticatedExchangeTransport for HyperliquidMainnetTransport {
    async fn submit_ioc(
        &self,
        request: SignedIocRequest,
    ) -> Result<SubmissionResponse, SubmissionTransportError> {
        validate_decimal(request.limit_price).map_err(SubmissionTransportError::InvalidResponse)?;
        validate_decimal(request.quantity).map_err(SubmissionTransportError::InvalidResponse)?;
        let payload = ExchangePayload {
            action: OrderAction {
                orders: vec![WireOrder {
                    asset: request.asset_index,
                    is_buy: request.is_buy,
                    limit_px: request.limit_price.normalize().to_string(),
                    size: request.quantity.normalize().to_string(),
                    reduce_only: request.reduce_only,
                    order_type: OrderType {
                        limit: LimitOrder { tif: "Ioc" },
                    },
                    cloid: cloid_hex(request.cloid),
                }],
                grouping: "na",
            },
            signature: request.signature,
            nonce: request.nonce,
            vault_address: None,
        };
        let response = self
            .client
            .post(EXCHANGE_URL)
            .json(&payload)
            .send()
            .await
            .map_err(|_| SubmissionTransportError::UnknownResult)?;
        if response.status().is_server_error() {
            return Err(SubmissionTransportError::UnknownResult);
        }
        if !response.status().is_success() {
            return Err(SubmissionTransportError::Http(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(SubmissionTransportError::InvalidResponse(
                "response exceeds size limit".into(),
            ));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| SubmissionTransportError::UnknownResult)?;
        if bytes.len() > MAXIMUM_RESPONSE_BYTES {
            return Err(SubmissionTransportError::InvalidResponse(
                "response exceeds size limit".into(),
            ));
        }
        let wire: ExchangeResponseStatus =
            serde_json::from_slice(&bytes).map_err(|_| SubmissionTransportError::UnknownResult)?;
        parse_submission_response(wire)
    }

    async fn lookup_order(
        &self,
        cloid: PlannedCloid,
    ) -> Result<OrderObservation, ReconciliationError> {
        let (wire, _): (OrderStatusWire, _) = self
            .info(serde_json::json!({
                "type":"orderStatus",
                "user":self.master_account,
                "oid":cloid_hex(cloid),
            }))
            .await?;
        parse_order_observation(wire, cloid)
    }

    async fn read_fills(&self, cursor: FillCursor) -> Result<UserFillBatch, ReconciliationError> {
        let mut body = serde_json::json!({
            "type":"userFillsByTime",
            "user":self.master_account,
            "startTime":cursor.start_time_ms,
            "aggregateByTime":false,
        });
        if let Some(end) = cursor.end_time_ms {
            body.as_object_mut()
                .expect("constructed object")
                .insert("endTime".into(), serde_json::json!(end));
        }
        let (wire, source_hash): (Vec<UserFillWire>, _) = self.info(body).await?;
        let mut fills = Vec::with_capacity(wire.len());
        let mut next = cursor.start_time_ms;
        for fill in wire {
            let cloid = fill
                .cloid
                .as_deref()
                .ok_or_else(|| ReconciliationError::InvalidResponse("fill missing CLOID".into()))
                .and_then(parse_cloid)?;
            next = next.max(fill.time.saturating_add(1));
            fills.push(UserFill {
                cloid,
                exchange_order_id: fill.oid.to_string(),
                trade_id: fill.tid.to_string(),
                asset: fill.coin,
                side: fill.side,
                price: parse_decimal(&fill.px)?,
                quantity: parse_decimal(&fill.sz)?,
                closed_pnl: parse_signed_decimal(&fill.closed_pnl)?,
                fee: parse_signed_decimal(&fill.fee)?,
                fee_token: fill.fee_token,
                crossed: fill.crossed,
                occurred_at_ms: fill.time,
                source_hash,
            });
        }
        Ok(UserFillBatch {
            fills,
            next_cursor: FillCursor {
                start_time_ms: next,
                end_time_ms: cursor.end_time_ms,
            },
            source_hash,
        })
    }

    async fn read_positions(&self) -> Result<ExchangePositionSnapshot, ReconciliationError> {
        let (wire, source_hash): (ClearinghouseStateWire, _) = self
            .info(serde_json::json!({
                "type":"clearinghouseState",
                "user":self.master_account,
            }))
            .await?;
        let mut positions = BTreeMap::new();
        for position in wire.asset_positions {
            let quantity = parse_signed_decimal(&position.position.szi)?;
            if !quantity.is_zero() {
                positions.insert(position.position.coin, quantity);
            }
        }
        Ok(ExchangePositionSnapshot {
            positions,
            account_equity: parse_decimal(&wire.margin_summary.account_value)?,
            observed_at_ms: wire.time,
            source_hash,
        })
    }

    async fn read_equity(&self) -> Result<ExchangeEquitySnapshot, ReconciliationError> {
        let (wire, source_hash): (ClearinghouseStateWire, _) = self
            .info(serde_json::json!({
                "type":"clearinghouseState",
                "user":self.master_account,
            }))
            .await?;
        let equity = parse_decimal(&wire.margin_summary.account_value)?;
        Ok(ExchangeEquitySnapshot {
            equity,
            observed_at_ms: wire.time,
            source_hash,
        })
    }

    async fn read_funding(
        &self,
        cursor: FundingCursor,
    ) -> Result<FundingBatch, ReconciliationError> {
        let mut body = serde_json::json!({
            "type":"userFunding",
            "user":self.master_account,
            "startTime":cursor.start_time_ms,
        });
        if let Some(end) = cursor.end_time_ms {
            body.as_object_mut()
                .expect("constructed object")
                .insert("endTime".into(), serde_json::json!(end));
        }
        let (wire, source_hash): (Vec<FundingWire>, _) = self.info(body).await?;
        let mut next = cursor.start_time_ms;
        let events = wire
            .into_iter()
            .map(|event| {
                next = next.max(event.time.saturating_add(1));
                Ok(FundingRecord {
                    event_hash: event.hash,
                    asset: event.delta.coin,
                    signed_usdc_delta: parse_signed_decimal(&event.delta.usdc)?,
                    occurred_at_ms: event.time,
                    source_hash,
                })
            })
            .collect::<Result<_, ReconciliationError>>()?;
        Ok(FundingBatch {
            events,
            next_cursor: FundingCursor {
                start_time_ms: next,
                end_time_ms: cursor.end_time_ms,
            },
            source_hash,
        })
    }
}

fn validate_decimal(value: Decimal) -> Result<(), String> {
    if value <= Decimal::ZERO || value.scale() > 8 {
        Err("order decimal must be positive with at most eight decimal places".into())
    } else {
        Ok(())
    }
}

fn parse_decimal(value: &str) -> Result<Decimal, ReconciliationError> {
    let value = parse_signed_decimal(value)?;
    if value < Decimal::ZERO {
        Err(ReconciliationError::InvalidResponse(
            "expected nonnegative decimal".into(),
        ))
    } else {
        Ok(value)
    }
}

fn parse_signed_decimal(value: &str) -> Result<Decimal, ReconciliationError> {
    Decimal::from_str(value)
        .map_err(|error| ReconciliationError::InvalidResponse(error.to_string()))
}

fn valid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        && value[2..].bytes().any(|byte| byte != b'0')
}

pub fn cloid_hex(cloid: PlannedCloid) -> String {
    cloid.to_hex()
}

fn parse_cloid(value: &str) -> Result<PlannedCloid, ReconciliationError> {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    if raw.len() != 32 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ReconciliationError::InvalidResponse("invalid cloid".into()));
    }
    let mut bytes = [0u8; 16];
    for (index, chunk) in raw.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk)
            .map_err(|_| ReconciliationError::InvalidResponse("invalid cloid".into()))?;
        bytes[index] = u8::from_str_radix(text, 16)
            .map_err(|_| ReconciliationError::InvalidResponse("invalid cloid".into()))?;
    }
    Ok(PlannedCloid(bytes))
}

fn parse_order_observation(
    wire: OrderStatusWire,
    expected: PlannedCloid,
) -> Result<OrderObservation, ReconciliationError> {
    let OrderStatusWire::Order { order } = wire else {
        return Ok(OrderObservation::NotFound);
    };
    if order.order.cloid.as_deref().map(parse_cloid).transpose()? != Some(expected) {
        return Err(ReconciliationError::IdentityMismatch);
    }
    let original = parse_decimal(&order.order.orig_sz)?;
    let remaining = parse_decimal(&order.order.sz)?;
    let filled = original
        .checked_sub(remaining)
        .ok_or(ReconciliationError::Arithmetic)?;
    let oid = order.order.oid.to_string();
    Ok(match order.status.as_str() {
        "open" => OrderObservation::Open {
            exchange_order_id: oid,
            original_quantity: original,
            remaining_quantity: remaining,
        },
        "filled" => OrderObservation::Filled {
            exchange_order_id: oid,
            filled_quantity: filled,
        },
        "canceled"
        | "marginCanceled"
        | "vaultWithdrawalCanceled"
        | "openInterestCapCanceled"
        | "selfTradeCanceled"
        | "reduceOnlyCanceled"
        | "siblingFilledCanceled"
        | "delistedCanceled"
        | "liquidatedCanceled"
        | "scheduledCancel" => OrderObservation::Cancelled {
            exchange_order_id: oid,
            filled_quantity: filled,
            reason: order.status,
        },
        status if status.ends_with("Rejected") || status == "rejected" => {
            OrderObservation::Rejected {
                exchange_order_id: oid,
                reason: status.to_string(),
            }
        }
        _ => {
            return Err(ReconciliationError::InvalidResponse(
                "unknown order status".into(),
            ))
        }
    })
}

fn parse_submission_response(
    wire: ExchangeResponseStatus,
) -> Result<SubmissionResponse, SubmissionTransportError> {
    let response = match wire {
        ExchangeResponseStatus::Err(reason) => return Ok(SubmissionResponse::Rejected { reason }),
        ExchangeResponseStatus::Ok(response) if response.response_type == "order" => response,
        _ => {
            return Err(SubmissionTransportError::InvalidResponse(
                "unexpected exchange response".into(),
            ))
        }
    };
    let mut statuses = response
        .data
        .ok_or_else(|| SubmissionTransportError::InvalidResponse("missing statuses".into()))?
        .statuses;
    if statuses.len() != 1 {
        return Err(SubmissionTransportError::InvalidResponse(
            "expected one order status".into(),
        ));
    }
    Ok(match statuses.remove(0) {
        ExchangeDataStatus::Success | ExchangeDataStatus::WaitingForFill => {
            SubmissionResponse::Acknowledged
        }
        ExchangeDataStatus::Filled(fill) => SubmissionResponse::Filled {
            exchange_order_id: fill.oid.to_string(),
            filled_quantity: Decimal::from_str(&fill.total_sz)
                .map_err(|error| SubmissionTransportError::InvalidResponse(error.to_string()))?,
            average_fill_price: Decimal::from_str(&fill.avg_px)
                .map_err(|error| SubmissionTransportError::InvalidResponse(error.to_string()))?,
        },
        ExchangeDataStatus::Error(reason) => SubmissionResponse::Rejected { reason },
        ExchangeDataStatus::Resting(order) => {
            return Err(SubmissionTransportError::InvalidResponse(format!(
                "IOC unexpectedly rested as order {}",
                order.oid
            )))
        }
        ExchangeDataStatus::WaitingForTrigger => {
            return Err(SubmissionTransportError::InvalidResponse(
                "IOC returned a trigger-waiting status".into(),
            ))
        }
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExchangePayload {
    action: OrderAction,
    signature: Signature,
    nonce: u64,
    vault_address: Option<H160>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename = "order")]
struct OrderAction {
    orders: Vec<WireOrder>,
    grouping: &'static str,
}

#[derive(Debug, Serialize)]
struct WireOrder {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    limit_px: String,
    #[serde(rename = "s")]
    size: String,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: OrderType,
    #[serde(rename = "c")]
    cloid: String,
}

#[derive(Debug, Serialize)]
struct OrderType {
    limit: LimitOrder,
}

#[derive(Debug, Serialize)]
struct LimitOrder {
    tif: &'static str,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", tag = "status", content = "response")]
enum ExchangeResponseStatus {
    Ok(ExchangeResponse),
    Err(String),
}

#[derive(Debug, Clone, Deserialize)]
struct ExchangeResponse {
    #[serde(rename = "type")]
    response_type: String,
    data: Option<ExchangeDataStatuses>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExchangeDataStatuses {
    statuses: Vec<ExchangeDataStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ExchangeDataStatus {
    Success,
    WaitingForFill,
    WaitingForTrigger,
    Error(String),
    Resting(RestingOrder),
    Filled(FilledOrder),
}

#[derive(Debug, Clone, Deserialize)]
struct RestingOrder {
    oid: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FilledOrder {
    total_sz: String,
    avg_px: String,
    oid: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
enum OrderStatusWire {
    #[serde(rename = "unknownOid")]
    UnknownOid,
    #[serde(rename = "order")]
    Order { order: OrderStatusInfoWire },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderStatusInfoWire {
    order: BasicOrderInfoWire,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BasicOrderInfoWire {
    oid: u64,
    sz: String,
    orig_sz: String,
    cloid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserFillWire {
    closed_pnl: String,
    coin: String,
    crossed: bool,
    oid: u64,
    px: String,
    side: String,
    sz: String,
    time: u64,
    fee: String,
    fee_token: String,
    tid: u64,
    cloid: Option<String>,
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
struct PositionWire {
    coin: String,
    szi: String,
}

#[derive(Debug, Deserialize)]
struct FundingWire {
    time: u64,
    hash: String,
    delta: FundingDeltaWire,
}

#[derive(Debug, Deserialize)]
struct FundingDeltaWire {
    coin: String,
    usdc: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_order_status_and_fill_payloads_decode_exactly() {
        let cloid = PlannedCloid([1; 16]);
        let status: OrderStatusWire = serde_json::from_value(serde_json::json!({
            "status":"order",
            "order":{"order":{"oid":42,"sz":"0","origSz":"1","cloid":cloid_hex(cloid)},"status":"filled"}
        }))
        .unwrap();
        assert_eq!(
            parse_order_observation(status, cloid).unwrap(),
            OrderObservation::Filled {
                exchange_order_id: "42".into(),
                filled_quantity: Decimal::ONE,
            }
        );
        let fill: UserFillWire = serde_json::from_value(serde_json::json!({
            "closedPnl":"1.2","coin":"BTC","crossed":true,"oid":42,"px":"100","side":"B",
            "sz":"0.1","time":1,"fee":"0.01","feeToken":"USDC","tid":7,"cloid":cloid_hex(cloid)
        }))
        .unwrap();
        assert_eq!(fill.tid, 7);
    }

    #[test]
    fn typed_trait_surface_has_no_generic_request_operation() {
        fn accepts_transport<T: AuthenticatedExchangeTransport>() {}
        accepts_transport::<HyperliquidMainnetTransport>();
    }
}
