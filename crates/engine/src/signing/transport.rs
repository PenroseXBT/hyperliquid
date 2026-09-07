use crate::domain::decision::{hash_payload_bytes, PayloadHash, PlannedCloid};
use async_trait::async_trait;
use ethers::types::Signature;
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

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
    pub cloid: Option<PlannedCloid>,
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
    #[serde(default)]
    pub position_before: Option<Decimal>,
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
pub struct ExchangeOpenOrder {
    pub cloid: Option<PlannedCloid>,
    pub exchange_order_id: String,
    pub asset: String,
    pub is_buy: bool,
    pub limit_price: Decimal,
    pub original_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub reduce_only: bool,
    pub order_type: String,
    pub is_trigger: bool,
    pub is_position_tpsl: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeOpenOrdersSnapshot {
    pub orders: Vec<ExchangeOpenOrder>,
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
    fn install_perp_dex_order(&self, _order: Vec<String>) -> Result<(), ReconciliationError> {
        Ok(())
    }

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

    async fn read_open_orders(&self) -> Result<ExchangeOpenOrdersSnapshot, ReconciliationError>;

    async fn read_funding(
        &self,
        cursor: FundingCursor,
    ) -> Result<FundingBatch, ReconciliationError>;
}

#[derive(Clone)]
pub struct HyperliquidMainnetTransport {
    client: reqwest::Client,
    execution_account: String,
    perp_dex_order: Arc<Mutex<Vec<String>>>,
}

impl HyperliquidMainnetTransport {
    pub fn new(execution_account: String, timeout_ms: u64) -> Result<Self, ReconciliationError> {
        if !valid_address(&execution_account) || timeout_ms == 0 {
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
            execution_account: execution_account.to_ascii_lowercase(),
            perp_dex_order: Arc::new(Mutex::new(vec![String::new()])),
        })
    }

    /// The caller first proves the key derives this economic account's address.
    /// Only a standalone user is supported; no agent or subaccount routing.
    pub async fn verify_direct_user(&self) -> Result<(), ReconciliationError> {
        let (role, _): (UserRoleWire, _) = self
            .info(serde_json::json!({
                "type":"userRole", "user":self.execution_account,
            }))
            .await?;
        role.require_direct_user()
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

    fn builder_dexes(&self) -> Result<Vec<String>, ReconciliationError> {
        let order = self
            .perp_dex_order
            .lock()
            .map_err(|_| ReconciliationError::Transport("perp DEX mutex poisoned".into()))?;
        Ok(order
            .iter()
            .filter(|dex| !dex.is_empty())
            .cloned()
            .collect())
    }

    async fn read_clearinghouse_state(
        &self,
        dex: &str,
    ) -> Result<(ClearinghouseStateWire, PayloadHash), ReconciliationError> {
        let mut body = serde_json::json!({
            "type":"clearinghouseState",
            "user":self.execution_account,
        });
        if !dex.is_empty() {
            body.as_object_mut()
                .expect("constructed object")
                .insert("dex".into(), serde_json::json!(dex));
        }
        self.info(body).await
    }

    async fn read_open_orders_for_dex(
        &self,
        dex: &str,
    ) -> Result<(Vec<OpenOrderWire>, PayloadHash), ReconciliationError> {
        let mut body = serde_json::json!({
            "type":"frontendOpenOrders",
            "user":self.execution_account,
        });
        if !dex.is_empty() {
            body.as_object_mut()
                .expect("constructed object")
                .insert("dex".into(), serde_json::json!(dex));
        }
        self.info(body).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "role")]
enum UserRoleWire {
    #[serde(rename = "user")]
    User,
    #[serde(other)]
    Other,
}

impl UserRoleWire {
    fn require_direct_user(self) -> Result<(), ReconciliationError> {
        match self {
            Self::User => Ok(()),
            Self::Other => Err(ReconciliationError::IdentityMismatch),
        }
    }
}

#[async_trait]
impl AuthenticatedExchangeTransport for HyperliquidMainnetTransport {
    fn install_perp_dex_order(&self, order: Vec<String>) -> Result<(), ReconciliationError> {
        validate_perp_dex_order(&order)?;
        *self
            .perp_dex_order
            .lock()
            .map_err(|_| ReconciliationError::Transport("perp DEX mutex poisoned".into()))? = order;
        Ok(())
    }

    async fn submit_ioc(
        &self,
        request: SignedIocRequest,
    ) -> Result<SubmissionResponse, SubmissionTransportError> {
        let payload = ExchangePayload::try_from(request)?;
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
                "user":self.execution_account,
                "oid":cloid_hex(cloid),
            }))
            .await?;
        parse_order_observation(wire, cloid)
    }

    async fn read_fills(&self, cursor: FillCursor) -> Result<UserFillBatch, ReconciliationError> {
        let mut body = serde_json::json!({
            "type":"userFillsByTime",
            "user":self.execution_account,
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
            next = next.max(fill.time.saturating_add(1));
            fills.push(fill.into_record(source_hash)?);
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
        let mut states = vec![(String::new(), self.read_clearinghouse_state("").await?)];
        for dex in self.builder_dexes()? {
            states.push((dex.clone(), self.read_clearinghouse_state(&dex).await?));
        }
        let mut positions = BTreeMap::new();
        let mut account_equity = Decimal::ZERO;
        let mut observed_at_ms = u64::MAX;
        let mut hashes = Vec::with_capacity(states.len());
        for (dex, (wire, source_hash)) in states {
            hashes.push(source_hash);
            account_equity = account_equity
                .checked_add(parse_decimal(&wire.margin_summary.account_value)?)
                .ok_or(ReconciliationError::Arithmetic)?;
            observed_at_ms = observed_at_ms.min(wire.time);
            for position in wire.asset_positions {
                let quantity = parse_signed_decimal(&position.position.szi)?;
                if !quantity.is_zero() {
                    positions.insert(prefixed_asset(&dex, position.position.coin), quantity);
                }
            }
        }
        Ok(ExchangePositionSnapshot {
            positions,
            account_equity,
            observed_at_ms,
            source_hash: combine_payload_hashes("clearinghouseState", &hashes),
        })
    }

    async fn read_open_orders(&self) -> Result<ExchangeOpenOrdersSnapshot, ReconciliationError> {
        let mut pages = vec![(String::new(), self.read_open_orders_for_dex("").await?)];
        for dex in self.builder_dexes()? {
            pages.push((dex.clone(), self.read_open_orders_for_dex(&dex).await?));
        }
        let mut orders = Vec::new();
        let mut identities = std::collections::BTreeSet::new();
        let mut hashes = Vec::with_capacity(pages.len());
        for (dex, (wire, source_hash)) in pages {
            hashes.push(source_hash);
            for order in wire {
                let original_quantity = parse_decimal(&order.orig_sz)?;
                let remaining_quantity = parse_decimal(&order.sz)?;
                if remaining_quantity > original_quantity {
                    return Err(ReconciliationError::InvalidResponse(
                        "open order remaining quantity exceeds original quantity".into(),
                    ));
                }
                let is_buy = match order.side.as_str() {
                    "B" => true,
                    "A" => false,
                    _ => {
                        return Err(ReconciliationError::InvalidResponse(
                            "unknown open order side".into(),
                        ))
                    }
                };
                let order = ExchangeOpenOrder {
                    cloid: order.cloid.as_deref().map(parse_cloid).transpose()?,
                    exchange_order_id: order.oid.to_string(),
                    asset: prefixed_asset(&dex, order.coin),
                    is_buy,
                    limit_price: parse_decimal(&order.limit_px)?,
                    original_quantity,
                    remaining_quantity,
                    reduce_only: order.reduce_only,
                    order_type: order.order_type,
                    is_trigger: order.is_trigger,
                    is_position_tpsl: order.is_position_tpsl,
                };
                let key = (order.asset.clone(), order.exchange_order_id.clone());
                if !identities.insert(key) {
                    return Err(ReconciliationError::InvalidResponse(
                        "duplicate open order identity across DEX scopes".into(),
                    ));
                }
                orders.push(order);
            }
        }
        Ok(ExchangeOpenOrdersSnapshot {
            orders,
            source_hash: combine_payload_hashes("frontendOpenOrders", &hashes),
        })
    }

    async fn read_funding(
        &self,
        cursor: FundingCursor,
    ) -> Result<FundingBatch, ReconciliationError> {
        let mut body = serde_json::json!({
            "type":"userFunding",
            "user":self.execution_account,
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

fn validate_perp_dex_order(order: &[String]) -> Result<(), ReconciliationError> {
    if order.is_empty()
        || order.first().is_none_or(|dex| !dex.is_empty())
        || order.iter().any(|dex| {
            dex.len() > 32
                || (!dex.is_empty()
                    && !dex
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()))
        })
        || order
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != order.len()
    {
        return Err(ReconciliationError::InvalidResponse(
            "invalid production DEX order".into(),
        ));
    }
    Ok(())
}

fn prefixed_asset(dex: &str, coin: String) -> String {
    if dex.is_empty() || coin.contains(':') {
        coin
    } else {
        format!("{dex}:{coin}")
    }
}

fn combine_payload_hashes(kind: &str, hashes: &[PayloadHash]) -> PayloadHash {
    let payload = serde_json::to_vec(&(kind, hashes)).expect("payload hash input serializes");
    hash_payload_bytes(&payload)
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
    // DirectUser is the sole target. A routed address is unrepresentable.
    vault_address: (),
}

impl TryFrom<SignedIocRequest> for ExchangePayload {
    type Error = SubmissionTransportError;

    fn try_from(request: SignedIocRequest) -> Result<Self, Self::Error> {
        validate_decimal(request.limit_price).map_err(SubmissionTransportError::InvalidResponse)?;
        validate_decimal(request.quantity).map_err(SubmissionTransportError::InvalidResponse)?;
        Ok(Self {
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
            vault_address: (),
        })
    }
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
    #[serde(default)]
    start_position: Option<String>,
}

impl UserFillWire {
    fn into_record(self, source_hash: PayloadHash) -> Result<UserFill, ReconciliationError> {
        // Manual/TP fills legitimately lack a CLOID. Retain their exchange
        // identity and economics; reconciliation must resolve their attribution.
        Ok(UserFill {
            cloid: self.cloid.as_deref().map(parse_cloid).transpose()?,
            exchange_order_id: self.oid.to_string(),
            trade_id: self.tid.to_string(),
            asset: self.coin,
            side: self.side,
            price: parse_decimal(&self.px)?,
            quantity: parse_decimal(&self.sz)?,
            closed_pnl: parse_signed_decimal(&self.closed_pnl)?,
            fee: parse_signed_decimal(&self.fee)?,
            fee_token: self.fee_token,
            crossed: self.crossed,
            occurred_at_ms: self.time,
            source_hash,
            position_before: self
                .start_position
                .as_deref()
                .map(parse_signed_decimal)
                .transpose()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpenOrderWire {
    coin: String,
    side: String,
    limit_px: String,
    sz: String,
    orig_sz: String,
    oid: u64,
    cloid: Option<String>,
    reduce_only: bool,
    order_type: String,
    is_trigger: bool,
    is_position_tpsl: bool,
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
    fn captured_live_period_decodes_all_exchange_rows_exactly() {
        let Some(directory) = std::env::var_os("LIVE_PERIOD_CAPTURE") else {
            return;
        };
        let directory = std::path::PathBuf::from(directory);
        let source_hash = PayloadHash([0; 32]);
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("manifest.json")).unwrap())
                .unwrap();
        let wires: Vec<UserFillWire> =
            serde_json::from_slice(&std::fs::read(directory.join("fills.json")).unwrap()).unwrap();
        let mut next_fill = 0;
        let fills = wires
            .into_iter()
            .map(|fill| {
                next_fill = next_fill.max(fill.time.saturating_add(1));
                fill.into_record(source_hash).unwrap()
            })
            .collect::<Vec<_>>();
        let funding_wires: Vec<FundingWire> =
            serde_json::from_slice(&std::fs::read(directory.join("funding.json")).unwrap())
                .unwrap();
        let mut next_funding = 0;
        let funding = funding_wires
            .into_iter()
            .map(|event| {
                next_funding = next_funding.max(event.time.saturating_add(1));
                FundingRecord {
                    event_hash: event.hash,
                    asset: event.delta.coin,
                    signed_usdc_delta: parse_signed_decimal(&event.delta.usdc).unwrap(),
                    occurred_at_ms: event.time,
                    source_hash,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            fills.len() as u64,
            manifest["counts"]["fills"].as_u64().unwrap()
        );
        assert_eq!(
            funding.len() as u64,
            manifest["counts"]["funding"].as_u64().unwrap()
        );
        assert!(fills.len() > 2);
        assert_eq!(fills.iter().filter(|fill| fill.cloid.is_none()).count(), 1);
        assert!(next_fill > 0);
        assert!(next_funding > 0);
        assert_eq!(
            fills.len() as u64,
            manifest["counts"]["fills"].as_u64().unwrap()
        );
        assert_eq!(
            funding.len() as u64,
            manifest["counts"]["funding"].as_u64().unwrap()
        );
        let current: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("current.json")).unwrap())
                .unwrap();
        assert_eq!(current["assetPositions"][0]["position"]["coin"], "BNB");
        assert_eq!(current["assetPositions"][0]["position"]["szi"], "-0.006");
    }

    #[test]
    fn manual_tp_fill_keeps_exchange_identity_without_fabricating_a_cloid() {
        let wire: UserFillWire = serde_json::from_value(serde_json::json!({
            "coin":"HYPE", "px":"84.163", "sz":"0.25", "side":"A",
            "time":1787841431286_u64, "startPosition":"0.25", "dir":"Close Long",
            "closedPnl":"0.6595", "oid":528429565565_u64, "crossed":true,
            "fee":"0.009089", "tid":838440153201631_u64, "feeToken":"USDC"
        }))
        .unwrap();
        let fill = wire.into_record(PayloadHash([0; 32])).unwrap();
        assert_eq!(fill.cloid, None);
        assert_eq!(fill.exchange_order_id, "528429565565");
        assert_eq!(fill.trade_id, "838440153201631");
        assert_eq!(fill.quantity, Decimal::new(25, 2));
        assert_eq!(fill.closed_pnl, Decimal::new(6595, 4));
        assert_eq!(fill.occurred_at_ms, 1787841431286);
    }

    #[test]
    fn direct_user_never_accepts_agent_subaccount_vault_or_missing_roles() {
        let account = "0x2Ae3dD513B342E5162D11B2Bdfae8cAd7B67015B";
        serde_json::from_value::<UserRoleWire>(serde_json::json!({"role":"user"}))
            .unwrap()
            .require_direct_user()
            .unwrap();
        for value in [
            serde_json::json!({"role":"missing"}),
            serde_json::json!({"role":"vault"}),
            serde_json::json!({"role":"subAccount", "data":{"master":account}}),
            serde_json::json!({"role":"agent", "data":{"user":account}}),
            serde_json::json!({"role":"unknown"}),
        ] {
            let role: UserRoleWire = serde_json::from_value(value).unwrap();
            assert_eq!(
                role.require_direct_user(),
                Err(ReconciliationError::IdentityMismatch)
            );
        }
        for malformed in [
            serde_json::json!({}),
            serde_json::json!({"role":null}),
            serde_json::json!({"role":123}),
        ] {
            assert!(serde_json::from_value::<UserRoleWire>(malformed).is_err());
        }
    }

    #[test]
    fn direct_user_ioc_serializes_null_vault_address_without_routing() {
        let request = SignedIocRequest {
            asset_index: 0,
            is_buy: true,
            limit_price: Decimal::from(100),
            quantity: Decimal::ONE,
            reduce_only: false,
            cloid: PlannedCloid([1; 16]),
            nonce: 123,
            signature: Signature {
                r: 1.into(),
                s: 2.into(),
                v: 27,
            },
        };
        let payload = serde_json::to_value(ExchangePayload::try_from(request).unwrap()).unwrap();
        assert_eq!(payload.get("vaultAddress"), Some(&serde_json::Value::Null));
        assert_eq!(payload["action"]["orders"][0]["t"]["limit"]["tif"], "Ioc");
        assert_eq!(
            payload["action"]["orders"][0]["c"],
            cloid_hex(PlannedCloid([1; 16]))
        );
        assert_eq!(payload["nonce"], 123);
    }

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

        let order: OpenOrderWire = serde_json::from_value(serde_json::json!({
            "coin":"BTC","side":"B","limitPx":"100","sz":"0.4","origSz":"1",
            "oid":42,"timestamp":1,"cloid":cloid_hex(cloid),"reduceOnly":false,
            "orderType":"Limit","isTrigger":false,"isPositionTpsl":false
        }))
        .unwrap();
        assert_eq!(order.cloid.as_deref(), Some(cloid_hex(cloid).as_str()));
    }

    #[test]
    fn builder_scoped_account_rows_use_prefixed_asset_identity() {
        assert_eq!(prefixed_asset("", "BTC".into()), "BTC");
        assert_eq!(prefixed_asset("xyz", "AMD".into()), "xyz:AMD");
        assert_eq!(prefixed_asset("xyz", "xyz:AMD".into()), "xyz:AMD");
    }

    #[test]
    fn malformed_production_dex_order_is_rejected() {
        for order in [
            vec![],
            vec!["xyz".to_string()],
            vec![String::new(), "xyz".into(), "xyz".into()],
            vec![String::new(), "XYZ".into()],
            vec![String::new(), "bad-dex".into()],
        ] {
            assert!(validate_perp_dex_order(&order).is_err());
        }
        assert!(validate_perp_dex_order(&[String::new(), "xyz".into()]).is_ok());
    }

    #[test]
    fn typed_trait_surface_has_no_generic_request_operation() {
        fn accepts_transport<T: AuthenticatedExchangeTransport>() {}
        accepts_transport::<HyperliquidMainnetTransport>();
    }
}
