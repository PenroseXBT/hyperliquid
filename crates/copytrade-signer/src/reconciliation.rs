//! Idempotent application of authenticated exchange records to the live ledger.

use crate::transport::{FundingBatch, UserFill, UserFillBatch};
use crate::{ApprovedExecutionIntent, SignerError, SubmissionRegistry};
use copytrade_core::decision::{DecisionId, PlannedCloid, Side, TargetVersion};
use copytrade_core::live_trading::{
    apply_exchange_fill, apply_funding_event, AppliedFillResult, AppliedFundingResult,
    ExchangeFillIdentity, ExchangeOrderId, ExchangeTradeId, FundingEventId,
    LiquidityClassification, LiveTradingState, VerifiedExchangeFill, VerifiedFundingEvent,
};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedExchangeBatch {
    pub fills: Vec<VerifiedExchangeFill>,
    pub funding: Vec<VerifiedFundingEvent>,
}

pub fn apply_authenticated_batches(
    state: &mut LiveTradingState,
    registry: &SubmissionRegistry,
    fills: UserFillBatch,
    funding: FundingBatch,
    reconciled_exchange_equity: rust_decimal::Decimal,
    ledger_path: &Path,
) -> Result<AppliedExchangeBatch, SignerError> {
    let mut next = state.clone();
    let mut applied = AppliedExchangeBatch {
        fills: Vec::new(),
        funding: Vec::new(),
    };
    let mut events = Vec::with_capacity(fills.fills.len() + funding.events.len());
    for fill in fills.fills {
        events.push(ExchangeEvent::Fill(fill));
    }
    for event in funding.events {
        events.push(ExchangeEvent::Funding(event));
    }
    events.sort_by(|left, right| left.key().cmp(&right.key()));
    for pair in events.windows(2) {
        if pair[0].occurred_at() == pair[1].occurred_at()
            && pair[0].asset() == pair[1].asset()
            && pair[0].kind() != pair[1].kind()
        {
            return Err(SignerError::AmbiguousExchangeEventOrdering);
        }
    }
    for event in events {
        match event {
            ExchangeEvent::Fill(fill) => {
                let durable = registry
                    .intent(&cloid_string(fill.cloid))
                    .ok_or(SignerError::UnknownCloid)?;
                let verified = verified_fill(fill, durable, reconciled_exchange_equity)?;
                if matches!(
                    apply_exchange_fill(&mut next, verified.clone())
                        .map_err(|error| SignerError::Ledger(error.to_string()))?,
                    AppliedFillResult::Applied { .. }
                ) {
                    applied.fills.push(verified);
                }
            }
            ExchangeEvent::Funding(event) => {
                let verified = VerifiedFundingEvent {
                    event_id: funding_id(&event.event_hash),
                    asset: event.asset,
                    amount: event.signed_usdc_delta,
                    occurred_at: event.occurred_at_ms,
                    source_hash: event.source_hash,
                };
                if matches!(
                    apply_funding_event(&mut next, verified.clone())
                        .map_err(|error| SignerError::Ledger(error.to_string()))?,
                    AppliedFundingResult::Applied(_)
                ) {
                    applied.funding.push(verified);
                }
            }
        }
    }
    next.advance_fill_cursor(fills.next_cursor.start_time_ms)
        .map_err(|error| SignerError::Ledger(error.to_string()))?;
    next.advance_funding_cursor(funding.next_cursor.start_time_ms)
        .map_err(|error| SignerError::Ledger(error.to_string()))?;
    next.reconcile_equity(reconciled_exchange_equity)
        .map_err(|error| SignerError::Ledger(error.to_string()))?;
    next.save_atomic(ledger_path)
        .map_err(|error| SignerError::Ledger(error.to_string()))?;
    *state = next;
    Ok(applied)
}

fn verified_fill(
    fill: UserFill,
    durable: &ApprovedExecutionIntent,
    exchange_equity: rust_decimal::Decimal,
) -> Result<VerifiedExchangeFill, SignerError> {
    let expected_side = if durable.is_buy { "B" } else { "A" };
    if fill.asset != durable.asset || fill.side != expected_side {
        return Err(SignerError::ExchangeFillMismatch);
    }
    Ok(VerifiedExchangeFill {
        identity: ExchangeFillIdentity {
            exchange_order_id: ExchangeOrderId(fill.exchange_order_id),
            trade_id: ExchangeTradeId(fill.trade_id),
            cloid: fill.cloid,
        },
        decision_id: DecisionId(decode_hex::<32>(&durable.decision_id)?),
        target_version: TargetVersion(durable.target_version),
        root_cloid: decode_cloid(&durable.root_cloid)?,
        parent_cloid: durable
            .parent_cloid
            .as_deref()
            .map(decode_cloid)
            .transpose()?,
        continuation_generation: durable.continuation_generation,
        asset: fill.asset,
        side: if durable.is_buy {
            Side::Buy
        } else {
            Side::Sell
        },
        reduce_only: durable.reduce_only,
        filled_quantity: fill.quantity,
        average_fill_price: fill.price,
        submitted_limit_price: durable.limit_price,
        fee_amount: fill.fee,
        exchange_closed_pnl: fill.closed_pnl,
        fee_asset: fill.fee_token,
        liquidity: if fill.crossed {
            LiquidityClassification::Taker
        } else {
            LiquidityClassification::Maker
        },
        decision_reference_price: durable.decision_reference_price,
        occurred_at: fill.occurred_at_ms,
        decision_timestamp: durable.decision_timestamp_ms,
        exchange_equity_after: exchange_equity,
        source_hash: fill.source_hash,
    })
}

enum ExchangeEvent {
    Fill(UserFill),
    Funding(crate::transport::FundingRecord),
}

impl ExchangeEvent {
    fn key(&self) -> (u64, u8, String) {
        match self {
            Self::Fill(fill) => (fill.occurred_at_ms, 0, fill.trade_id.clone()),
            Self::Funding(event) => (event.occurred_at_ms, 1, event.event_hash.clone()),
        }
    }
    fn occurred_at(&self) -> u64 {
        self.key().0
    }
    fn kind(&self) -> u8 {
        self.key().1
    }
    fn asset(&self) -> &str {
        match self {
            Self::Fill(fill) => &fill.asset,
            Self::Funding(event) => &event.asset,
        }
    }
}

fn funding_id(exchange_event_hash: &str) -> FundingEventId {
    let mut digest = Sha256::new();
    digest.update(b"LIVE/FUNDING_EVENT/V1");
    digest.update((exchange_event_hash.len() as u32).to_be_bytes());
    digest.update(exchange_event_hash.as_bytes());
    FundingEventId(digest.finalize().into())
}

fn decode_cloid(value: &str) -> Result<PlannedCloid, SignerError> {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    Ok(PlannedCloid(decode_hex::<16>(raw)?))
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], SignerError> {
    if value.len() != N * 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SignerError::InvalidIntent);
    }
    let mut bytes = [0; N];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| SignerError::InvalidIntent)?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| SignerError::InvalidIntent)?;
    }
    Ok(bytes)
}

fn cloid_string(cloid: PlannedCloid) -> String {
    cloid.to_hex()
}
