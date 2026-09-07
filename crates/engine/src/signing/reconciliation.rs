//! Idempotent application of authenticated exchange records to the live ledger.

use crate::domain::decision::{DecisionId, PlannedCloid, Side, TargetVersion};
use crate::domain::ledger::LedgerError;
use crate::domain::live_trading::{
    apply_exchange_fill, apply_funding_event, AppliedFillResult, AppliedFundingResult,
    ExchangeFillIdentity, ExchangeOrderId, ExchangeTradeId, FundingEventId,
    LiquidityClassification, LiveTradingState, VerifiedExchangeFill, VerifiedFundingEvent,
};
use crate::signing::transport::{FundingBatch, UserFill, UserFillBatch};
use crate::signing::{ApprovedExecutionIntent, SignerError, SubmissionRegistry};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedExchangeBatch {
    pub fills: Vec<VerifiedExchangeFill>,
    pub funding: Vec<VerifiedFundingEvent>,
    pub external: Vec<crate::domain::live_trading::ExternalFillAccounting>,
}

pub fn apply_authenticated_batches(
    state: &mut LiveTradingState,
    registry: &SubmissionRegistry,
    fills: UserFillBatch,
    funding: FundingBatch,
    positions: &crate::signing::transport::ExchangePositionSnapshot,
    reconciled_exchange_equity: rust_decimal::Decimal,
    ledger_path: &Path,
) -> Result<AppliedExchangeBatch, SignerError> {
    let mut next = state.clone();
    let mut applied = AppliedExchangeBatch {
        fills: Vec::new(),
        funding: Vec::new(),
        external: Vec::new(),
    };
    let mut events = Vec::with_capacity(fills.fills.len() + funding.events.len());
    for fill in fills.fills {
        events.push(ExchangeEvent::Fill(fill));
    }
    for event in funding.events {
        events.push(ExchangeEvent::Funding(event));
    }
    events.sort_by(compare_exchange_events);
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
                if fill.cloid.is_none() {
                    let position_before = fill.position_before.ok_or_else(|| {
                        SignerError::ExchangeTruthMismatch(format!(
                            "UNATTRIBUTED_EXTERNAL_FILL missing startPosition asset={}",
                            fill.asset
                        ))
                    })?;
                    let external = crate::domain::live_trading::ExternalReduction {
                        exchange_order_id: fill.exchange_order_id,
                        trade_id: fill.trade_id,
                        asset: fill.asset,
                        side: match fill.side.as_str() {
                            "B" => Side::Buy,
                            "A" => Side::Sell,
                            _ => return Err(SignerError::ExchangeFillMismatch),
                        },
                        quantity: fill.quantity,
                        price: fill.price,
                        fee: fill.fee,
                        fee_asset: fill.fee_token,
                        closed_pnl: fill.closed_pnl,
                        position_before,
                        occurred_at: fill.occurred_at_ms,
                    };
                    if next
                        .apply_external_reduction(external)
                        .map_err(external_replay_error)?
                    {
                        applied
                            .external
                            .push(next.external_fills().last().unwrap().clone());
                    }
                    continue;
                }
                let cloid = fill.cloid.ok_or_else(|| {
                    SignerError::ExchangeTruthMismatch(format!(
                        "UNATTRIBUTED_EXTERNAL_FILL asset={} oid={} tid={} time={} quantity={} closed_pnl={}",
                        fill.asset, fill.exchange_order_id, fill.trade_id, fill.occurred_at_ms, fill.quantity, fill.closed_pnl
                    ))
                })?;
                let durable = registry.intent(&cloid_string(cloid)).ok_or_else(|| {
                    SignerError::ExchangeTruthMismatch(format!(
                        "exchange fill {} has no durable intent",
                        fill.trade_id
                    ))
                })?;
                let position_before = fill.position_before;
                let verified = verified_fill(fill, durable, reconciled_exchange_equity)?;
                if let Some(previous) = next
                    .verified_fills()
                    .iter()
                    .find(|previous| previous.identity == verified.identity)
                {
                    // Poll boundaries/equity are not the immutable economic
                    // payload. Targeted overlap must not turn them into conflicts.
                    let mut comparable = verified.clone();
                    comparable.source_hash = previous.source_hash;
                    comparable.exchange_equity_after = previous.exchange_equity_after;
                    if &comparable != previous {
                        return Err(SignerError::ExchangeFillMismatch);
                    }
                    continue;
                }
                if position_before
                    .is_some_and(|position| position != next.position(&verified.asset))
                {
                    return Err(SignerError::ExchangeTruthMismatch(
                        "engine fill startPosition differs from reconstructed history".into(),
                    ));
                }
                if matches!(
                    apply_exchange_fill(&mut next, verified.clone())
                        .map_err(external_replay_error)?,
                    AppliedFillResult::Applied { .. }
                ) {
                    applied.fills.push(verified);
                }
            }
            ExchangeEvent::Funding(event) => {
                // Funding hashes may be all-zero for every hourly payment.
                // Match legacy persisted events by economic identity before
                // deriving a new ID; a payload change is still an integrity error.
                if let Some(previous) = next.verified_funding().iter().find(|previous| {
                    previous.asset == event.asset && previous.occurred_at == event.occurred_at_ms
                }) {
                    if previous.amount != event.signed_usdc_delta {
                        return Err(SignerError::Ledger(
                            "funding identity payload conflict".into(),
                        ));
                    }
                    continue;
                }
                let verified = VerifiedFundingEvent {
                    event_id: funding_id(&event.event_hash, &event.asset, event.occurred_at_ms),
                    asset: event.asset,
                    amount: event.signed_usdc_delta,
                    occurred_at: event.occurred_at_ms,
                    source_hash: event.source_hash,
                };
                if matches!(
                    apply_funding_event(&mut next, verified.clone())
                        .map_err(external_replay_error)?,
                    AppliedFundingResult::Applied(_)
                ) {
                    applied.funding.push(verified);
                }
            }
        }
    }
    // A missing historical event must not be skipped by a successful empty poll.
    // Commit fills, positions and cursors together only after exchange convergence.
    next.reconcile_positions(&positions.positions)
        .map_err(|_| {
            SignerError::ExchangeTruthMismatch(format!(
                "ORPHANED_LIVE_POSITION exchange={:?} durable={:?}",
                positions.positions,
                next.positions()
            ))
        })?;
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

// Hyperliquid can split one IOC into multiple fills with the same millisecond.
// Trade ids do not encode their causal order; startPosition does. Apply buys
// from the lowest starting position and sells from the highest so each slice
// sees the position left by its predecessor.
fn compare_exchange_events(left: &ExchangeEvent, right: &ExchangeEvent) -> std::cmp::Ordering {
    let ordinary = left.key().cmp(&right.key());
    let (ExchangeEvent::Fill(left), ExchangeEvent::Fill(right)) = (left, right) else {
        return ordinary;
    };
    if left.occurred_at_ms != right.occurred_at_ms
        || left.asset != right.asset
        || left.side != right.side
    {
        return ordinary;
    }
    match (
        left.position_before,
        right.position_before,
        left.side.as_str(),
    ) {
        (Some(left), Some(right), "B") => left.cmp(&right).then(ordinary),
        (Some(left), Some(right), "A") => right.cmp(&left).then(ordinary),
        _ => ordinary,
    }
}

// A history gap can expose a fill/funding event before its local position exists.
// Only external replay gets this classification; corrupt durable state stays fatal.
fn external_replay_error(error: LedgerError) -> SignerError {
    match error {
        LedgerError::PositionDivergence => {
            SignerError::ExchangeTruthMismatch("PositionDivergence during exchange replay".into())
        }
        error => SignerError::Ledger(error.to_string()),
    }
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
            cloid: fill.cloid.ok_or(SignerError::ExchangeFillMismatch)?,
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
    Funding(crate::signing::transport::FundingRecord),
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

fn funding_id(exchange_event_hash: &str, asset: &str, occurred_at_ms: u64) -> FundingEventId {
    let mut digest = Sha256::new();
    digest.update(b"LIVE/FUNDING_EVENT/V2");
    digest.update((exchange_event_hash.len() as u32).to_be_bytes());
    digest.update(exchange_event_hash.as_bytes());
    digest.update((asset.len() as u32).to_be_bytes());
    digest.update(asset.as_bytes());
    digest.update(occurred_at_ms.to_be_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::PayloadHash;
    use crate::domain::live_trading::LiveTradingState;
    use crate::signing::transport::{
        ExchangePositionSnapshot, FillCursor, FundingBatch, FundingCursor,
    };
    use crate::signing::ApprovedExecutionIntent;
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    use std::str::FromStr;

    fn intent(cloid: &str, is_buy: bool, reduce_only: bool) -> ApprovedExecutionIntent {
        ApprovedExecutionIntent {
            decision_id: "11".repeat(32),
            target_version: 1,
            root_cloid: cloid.into(),
            cloid: cloid.into(),
            parent_cloid: None,
            continuation_generation: 0,
            asset: "CAKE".into(),
            asset_index: 99,
            is_buy,
            reduce_only,
            limit_price: Decimal::from_str("1.8654").unwrap(),
            quantity: Decimal::from_str("6.7").unwrap(),
            risk_projection_hash: "22".repeat(32),
            risk_policy_hash: "33".repeat(32),
            configuration_hash: "44".repeat(32),
            release_manifest_hash: "55".repeat(32),
            decision_reference_price: Decimal::from_str("1.8654").unwrap(),
            decision_timestamp_ms: 10,
            expires_at_mono: 100,
            canonical_intent_hash: "66".repeat(32),
        }
    }

    fn fill(
        cloid: PlannedCloid,
        tid: &str,
        side: &str,
        quantity: &str,
        position_before: &str,
    ) -> UserFill {
        UserFill {
            cloid: Some(cloid),
            exchange_order_id: "532764639295".into(),
            trade_id: tid.into(),
            asset: "CAKE".into(),
            side: side.into(),
            price: Decimal::from_str("1.8651").unwrap(),
            quantity: Decimal::from_str(quantity).unwrap(),
            closed_pnl: Decimal::ZERO,
            fee: Decimal::ZERO,
            fee_token: "USDC".into(),
            crossed: true,
            occurred_at_ms: 20,
            source_hash: PayloadHash([0; 32]),
            position_before: Some(Decimal::from_str(position_before).unwrap()),
        }
    }

    fn funding(cursor: u64) -> FundingBatch {
        FundingBatch {
            events: vec![],
            next_cursor: FundingCursor {
                start_time_ms: cursor,
                end_time_ms: None,
            },
            source_hash: PayloadHash([0; 32]),
        }
    }

    #[test]
    fn same_millisecond_partial_close_follows_exchange_position_chain() {
        let root = tempfile::tempdir().unwrap();
        let ledger_path = root.path().join("live-trading-state.json");
        let open_cloid = PlannedCloid([1; 16]);
        let close_cloid = PlannedCloid([2; 16]);
        let mut registry = SubmissionRegistry::default();
        registry
            .register(intent(&open_cloid.to_hex(), true, false))
            .unwrap();
        registry
            .register(intent(&close_cloid.to_hex(), false, true))
            .unwrap();
        let mut state = LiveTradingState::new(Decimal::from(100), 0).unwrap();
        let mut open_positions = BTreeMap::new();
        open_positions.insert("CAKE".into(), Decimal::from_str("6.7").unwrap());
        apply_authenticated_batches(
            &mut state,
            &registry,
            UserFillBatch {
                fills: vec![fill(open_cloid, "1", "B", "6.7", "0")],
                next_cursor: FillCursor {
                    start_time_ms: 21,
                    end_time_ms: None,
                },
                source_hash: PayloadHash([0; 32]),
            },
            funding(21),
            &ExchangePositionSnapshot {
                positions: open_positions,
                account_equity: Decimal::from(100),
                observed_at_ms: 21,
                source_hash: PayloadHash([0; 32]),
            },
            Decimal::from(100),
            &ledger_path,
        )
        .unwrap();

        // Lexicographic trade-id order is deliberately the opposite of the
        // causal exchange order observed in production.
        let applied = apply_authenticated_batches(
            &mut state,
            &registry,
            UserFillBatch {
                fills: vec![
                    fill(close_cloid, "1048538843103756", "A", "0.7", "0.7"),
                    fill(close_cloid, "769126122173078", "A", "6.0", "6.7"),
                ],
                next_cursor: FillCursor {
                    start_time_ms: 22,
                    end_time_ms: None,
                },
                source_hash: PayloadHash([0; 32]),
            },
            funding(22),
            &ExchangePositionSnapshot {
                positions: BTreeMap::new(),
                account_equity: Decimal::from(100),
                observed_at_ms: 22,
                source_hash: PayloadHash([0; 32]),
            },
            Decimal::from(100),
            &ledger_path,
        )
        .unwrap();
        assert_eq!(applied.fills.len(), 2);
        assert_eq!(state.position("CAKE"), Decimal::ZERO);
    }
}
