//! Durable observer-side production state and idempotent signer-event application.

use crate::mfce::MfcePersistentState;
use copytrade_core::decision::PlannedCloid;
use copytrade_core::ipc::{
    canonical_payload_hash, VerifiedReconciliationEvent, VerifiedReconciliationPayload,
};
use copytrade_core::live_trading::{
    apply_exchange_fill, apply_funding_event, AppliedFillResult, AppliedFundingResult,
    LiveTradingState,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const LEGACY_PRODUCTION_STATE_SCHEMA_VERSION: u32 = 1;
const PRODUCTION_STATE_SCHEMA_VERSION: u32 = 2;
const MAXIMUM_REORDER_WINDOW: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverIntentState {
    Planned,
    Persisted,
    Sent,
    AcceptedBySigner,
    SubmissionUnknown,
    AcknowledgedByExchange,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Superseded,
    Reconciled,
    AuthorizedNotSubmitted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionTradingState {
    schema_version: u32,
    pub live: LiveTradingState,
    /// Observer-only MFCE samples and model strings. The default preserves
    /// compatibility with production-state schema v1 files written before
    /// MFCE1 was embedded.
    #[serde(default)]
    pub mfce: MfcePersistentState,
    #[serde(default)]
    pub mfce_time_high_watermark: u64,
    pub intents: BTreeMap<PlannedCloid, ObserverIntentState>,
    pub highest_contiguous_signer_sequence: u64,
    applied_event_hashes: BTreeMap<u64, [u8; 32]>,
    reorder_buffer: BTreeMap<u64, VerifiedReconciliationEvent>,
    pub blocked_assets: BTreeSet<String>,
    pub assets_requiring_replan: BTreeSet<String>,
    pub position_mismatches: BTreeMap<String, (Decimal, Decimal)>,
    pub equity_mismatch: Option<(Decimal, Decimal)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationApplication {
    Applied { highest_contiguous_sequence: u64 },
    AlreadyApplied { highest_contiguous_sequence: u64 },
    Buffered { missing_sequence: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationApplyError {
    EventHashMismatch,
    SequenceIdentityConflict,
    SequenceRegression,
    ReorderWindowExhausted,
    UnknownIntent,
    Ledger(String),
    Persistence(String),
    SchemaMismatch,
}

impl std::fmt::Display for ReconciliationApplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for ReconciliationApplyError {}

impl ProductionTradingState {
    pub fn new(live: LiveTradingState) -> Self {
        Self {
            schema_version: PRODUCTION_STATE_SCHEMA_VERSION,
            live,
            mfce: MfcePersistentState::default(),
            mfce_time_high_watermark: 0,
            intents: BTreeMap::new(),
            highest_contiguous_signer_sequence: 0,
            applied_event_hashes: BTreeMap::new(),
            reorder_buffer: BTreeMap::new(),
            blocked_assets: BTreeSet::new(),
            assets_requiring_replan: BTreeSet::new(),
            position_mismatches: BTreeMap::new(),
            equity_mismatch: None,
        }
    }

    pub fn save_atomic(&self, path: impl AsRef<Path>) -> Result<(), ReconciliationApplyError> {
        self.validate()?;
        let path = path.as_ref();
        let parent = path.parent().ok_or_else(|| {
            ReconciliationApplyError::Persistence("production state path has no parent".into())
        })?;
        std::fs::create_dir_all(parent)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec(self)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        std::io::Write::write_all(&mut file, &bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        std::fs::rename(&temporary, path)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ReconciliationApplyError> {
        let bytes = std::fs::read(path)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        let encoded: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        let encoded_schema = encoded
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            .and_then(|version| u32::try_from(version).ok())
            .ok_or(ReconciliationApplyError::SchemaMismatch)?;
        if encoded_schema == PRODUCTION_STATE_SCHEMA_VERSION
            && (encoded.get("mfce").is_none() || encoded.get("mfce_time_high_watermark").is_none())
        {
            // Defaults exist only so schema v1 can be decoded. Once a writer
            // advertises v2, omitting either MFCE field is corruption rather
            // than a request to reset durable training state.
            return Err(ReconciliationApplyError::SchemaMismatch);
        }
        let mut state: Self = serde_json::from_value(encoded)
            .map_err(|error| ReconciliationApplyError::Persistence(error.to_string()))?;
        match state.schema_version {
            LEGACY_PRODUCTION_STATE_SCHEMA_VERSION => {
                // Schema v1 predates MFCE. Ignore any unknown fields a
                // noncanonical v1 writer may have supplied and migrate to the
                // bounded empty engine state.
                state.schema_version = PRODUCTION_STATE_SCHEMA_VERSION;
                state.mfce = MfcePersistentState::default();
                state.mfce_time_high_watermark = 0;
            }
            PRODUCTION_STATE_SCHEMA_VERSION => {}
            _ => return Err(ReconciliationApplyError::SchemaMismatch),
        }
        state.validate()?;
        Ok(state)
    }

    fn validate(&self) -> Result<(), ReconciliationApplyError> {
        if self.schema_version != PRODUCTION_STATE_SCHEMA_VERSION
            || self.mfce.validate().is_err()
            || self.mfce_time_high_watermark < self.mfce.time_high_watermark()
            || self.reorder_buffer.len() > MAXIMUM_REORDER_WINDOW
            || self
                .applied_event_hashes
                .keys()
                .next_back()
                .is_some_and(|sequence| *sequence != self.highest_contiguous_signer_sequence)
        {
            return Err(ReconciliationApplyError::SchemaMismatch);
        }
        Ok(())
    }
}

pub fn apply_reconciliation_event(
    state: &mut ProductionTradingState,
    event: VerifiedReconciliationEvent,
) -> Result<ReconciliationApplication, ReconciliationApplyError> {
    let expected_hash = canonical_payload_hash(&event.payload)
        .map_err(|_| ReconciliationApplyError::EventHashMismatch)?;
    if expected_hash != event.identity.event_hash {
        return Err(ReconciliationApplyError::EventHashMismatch);
    }
    let sequence = event.identity.sequence;
    if let Some(existing) = state.applied_event_hashes.get(&sequence) {
        return if *existing == event.identity.event_hash {
            Ok(ReconciliationApplication::AlreadyApplied {
                highest_contiguous_sequence: state.highest_contiguous_signer_sequence,
            })
        } else {
            Err(ReconciliationApplyError::SequenceIdentityConflict)
        };
    }
    if sequence <= state.highest_contiguous_signer_sequence {
        return Err(ReconciliationApplyError::SequenceRegression);
    }
    let next = state
        .highest_contiguous_signer_sequence
        .checked_add(1)
        .ok_or(ReconciliationApplyError::SequenceRegression)?;
    if sequence != next {
        if state.reorder_buffer.len() >= MAXIMUM_REORDER_WINDOW {
            return Err(ReconciliationApplyError::ReorderWindowExhausted);
        }
        if let Some(existing) = state.reorder_buffer.get(&sequence) {
            if existing.identity.event_hash != event.identity.event_hash {
                return Err(ReconciliationApplyError::SequenceIdentityConflict);
            }
        } else {
            state.reorder_buffer.insert(sequence, event);
        }
        return Ok(ReconciliationApplication::Buffered {
            missing_sequence: next,
        });
    }
    apply_contiguous(state, event)?;
    loop {
        let next = state
            .highest_contiguous_signer_sequence
            .checked_add(1)
            .ok_or(ReconciliationApplyError::SequenceRegression)?;
        let Some(event) = state.reorder_buffer.remove(&next) else {
            break;
        };
        apply_contiguous(state, event)?;
    }
    Ok(ReconciliationApplication::Applied {
        highest_contiguous_sequence: state.highest_contiguous_signer_sequence,
    })
}

fn apply_contiguous(
    state: &mut ProductionTradingState,
    event: VerifiedReconciliationEvent,
) -> Result<(), ReconciliationApplyError> {
    let mut next = state.clone();
    let sequence = event.identity.sequence;
    match event.payload {
        VerifiedReconciliationPayload::IntentAccepted { identity, .. } => {
            next.intents.insert(
                identity.planned_cloid,
                ObserverIntentState::AcceptedBySigner,
            );
        }
        VerifiedReconciliationPayload::SubmissionStarted { cloid } => {
            require_intent(&mut next, cloid, ObserverIntentState::Sent)?;
        }
        VerifiedReconciliationPayload::SubmissionUnknown { cloid } => {
            require_intent(&mut next, cloid, ObserverIntentState::SubmissionUnknown)?;
        }
        VerifiedReconciliationPayload::OrderAcknowledged { cloid, .. } => {
            require_intent(
                &mut next,
                cloid,
                ObserverIntentState::AcknowledgedByExchange,
            )?;
        }
        VerifiedReconciliationPayload::OrderRejected { cloid, .. } => {
            require_intent(&mut next, cloid, ObserverIntentState::Rejected)?;
        }
        VerifiedReconciliationPayload::OrderCancelled { cloid, .. } => {
            require_intent(&mut next, cloid, ObserverIntentState::Cancelled)?;
        }
        VerifiedReconciliationPayload::OrderPartiallyFilled { cloid, .. } => {
            require_intent(&mut next, cloid, ObserverIntentState::PartiallyFilled)?;
        }
        VerifiedReconciliationPayload::Fill(fill) => {
            let cloid = fill.identity.cloid;
            let asset = fill.asset.clone();
            match apply_exchange_fill(&mut next.live, fill)
                .map_err(|error| ReconciliationApplyError::Ledger(error.to_string()))?
            {
                AppliedFillResult::Applied { .. } => {
                    require_intent(&mut next, cloid, ObserverIntentState::PartiallyFilled)?;
                    next.assets_requiring_replan.insert(asset);
                }
                AppliedFillResult::AlreadyApplied => {}
            }
        }
        VerifiedReconciliationPayload::Funding(funding) => {
            let asset = funding.asset.clone();
            if matches!(
                apply_funding_event(&mut next.live, funding)
                    .map_err(|error| ReconciliationApplyError::Ledger(error.to_string()))?,
                AppliedFundingResult::Applied(_)
            ) {
                next.assets_requiring_replan.insert(asset);
            }
        }
        VerifiedReconciliationPayload::PositionSnapshot { positions, .. } => {
            next.position_mismatches.clear();
            next.blocked_assets.clear();
            let local = next.live.positions();
            for asset in local.keys().chain(positions.keys()) {
                let local_value = local.get(asset).copied().unwrap_or_default();
                let exchange_value = positions.get(asset).copied().unwrap_or_default();
                if local_value != exchange_value {
                    next.position_mismatches
                        .insert(asset.clone(), (local_value, exchange_value));
                    next.blocked_assets.insert(asset.clone());
                }
            }
        }
        VerifiedReconciliationPayload::EquitySnapshot { equity, .. } => {
            let local = next.live.current_equity();
            next.equity_mismatch = (local != equity).then_some((local, equity));
            if local == equity {
                next.live
                    .reconcile_equity(equity)
                    .map_err(|error| ReconciliationApplyError::Ledger(error.to_string()))?;
            }
        }
        VerifiedReconciliationPayload::Reconciled { cloid } => {
            require_intent(&mut next, cloid, ObserverIntentState::Reconciled)?;
        }
        VerifiedReconciliationPayload::AuthorizedNotSubmitted { cloid } => {
            require_intent(
                &mut next,
                cloid,
                ObserverIntentState::AuthorizedNotSubmitted,
            )?;
        }
    }
    next.applied_event_hashes
        .insert(sequence, event.identity.event_hash);
    next.highest_contiguous_signer_sequence = sequence;
    *state = next;
    Ok(())
}

fn require_intent(
    state: &mut ProductionTradingState,
    cloid: PlannedCloid,
    next: ObserverIntentState,
) -> Result<(), ReconciliationApplyError> {
    if !state.intents.contains_key(&cloid) {
        return Err(ReconciliationApplyError::UnknownIntent);
    }
    state.intents.insert(cloid, next);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::decision::{DecisionId, PayloadHash, PlannedCloid, Side, TargetVersion};
    use copytrade_core::ipc::{
        canonical_payload_hash, ReconciliationEventIdentity, VerifiedReconciliationEvent,
    };
    use copytrade_core::live_trading::{
        ExchangeFillIdentity, ExchangeOrderId, ExchangeTradeId, FundingEventId,
        LiquidityClassification, VerifiedExchangeFill, VerifiedFundingEvent,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMPORARY_STATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temporary_state_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "copytrade-production-state-{name}-{}-{}.json",
            std::process::id(),
            TEMPORARY_STATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn event(sequence: u64, payload: VerifiedReconciliationPayload) -> VerifiedReconciliationEvent {
        VerifiedReconciliationEvent {
            identity: ReconciliationEventIdentity {
                signer_instance_id: [4; 16],
                sequence,
                event_hash: canonical_payload_hash(&payload).unwrap(),
            },
            payload,
        }
    }

    #[test]
    fn replayed_funding_has_exactly_once_effect_and_conflicts_fail_closed() {
        let mut state =
            ProductionTradingState::new(LiveTradingState::new(Decimal::from(100), 0).unwrap());
        apply_exchange_fill(
            &mut state.live,
            VerifiedExchangeFill {
                identity: ExchangeFillIdentity {
                    exchange_order_id: ExchangeOrderId("1".into()),
                    trade_id: ExchangeTradeId("open".into()),
                    cloid: PlannedCloid([8; 16]),
                },
                decision_id: DecisionId([7; 32]),
                target_version: TargetVersion(1),
                root_cloid: PlannedCloid([8; 16]),
                parent_cloid: None,
                continuation_generation: 0,
                asset: "BTC".into(),
                side: Side::Buy,
                reduce_only: false,
                filled_quantity: Decimal::ONE,
                average_fill_price: Decimal::from(100),
                submitted_limit_price: Decimal::from(100),
                fee_amount: Decimal::ZERO,
                exchange_closed_pnl: Decimal::ZERO,
                fee_asset: "USDC".into(),
                liquidity: LiquidityClassification::Taker,
                decision_reference_price: Decimal::from(100),
                occurred_at: 1,
                decision_timestamp: 0,
                exchange_equity_after: Decimal::from(100),
                source_hash: PayloadHash([9; 32]),
            },
        )
        .unwrap();
        let payload = VerifiedReconciliationPayload::Funding(VerifiedFundingEvent {
            event_id: FundingEventId([1; 32]),
            asset: "BTC".into(),
            amount: Decimal::new(-1, 1),
            occurred_at: 1,
            source_hash: PayloadHash([2; 32]),
        });
        let original = event(1, payload);
        assert!(matches!(
            apply_reconciliation_event(&mut state, original.clone()).unwrap(),
            ReconciliationApplication::Applied { .. }
        ));
        assert_eq!(state.live.current_equity(), Decimal::new(999, 1));
        assert!(matches!(
            apply_reconciliation_event(&mut state, original).unwrap(),
            ReconciliationApplication::AlreadyApplied { .. }
        ));
        assert_eq!(state.live.current_equity(), Decimal::new(999, 1));
        let mut conflicting = event(
            1,
            VerifiedReconciliationPayload::Reconciled {
                cloid: PlannedCloid([3; 16]),
            },
        );
        conflicting.identity.event_hash = canonical_payload_hash(&conflicting.payload).unwrap();
        assert_eq!(
            apply_reconciliation_event(&mut state, conflicting),
            Err(ReconciliationApplyError::SequenceIdentityConflict)
        );
    }

    #[test]
    fn sequence_gap_is_buffered_then_drained_contiguously() {
        let mut state =
            ProductionTradingState::new(LiveTradingState::new(Decimal::from(100), 0).unwrap());
        let second = event(
            2,
            VerifiedReconciliationPayload::EquitySnapshot {
                equity: Decimal::from(100),
                observed_at: 2,
            },
        );
        assert_eq!(
            apply_reconciliation_event(&mut state, second).unwrap(),
            ReconciliationApplication::Buffered {
                missing_sequence: 1
            }
        );
        let first = event(
            1,
            VerifiedReconciliationPayload::PositionSnapshot {
                positions: BTreeMap::new(),
                observed_at: 1,
            },
        );
        apply_reconciliation_event(&mut state, first).unwrap();
        assert_eq!(state.highest_contiguous_signer_sequence, 2);
    }

    #[test]
    fn mfce_state_round_trips_atomically_and_legacy_v1_defaults() {
        let path = temporary_state_path("mfce-round-trip");
        let mut state =
            ProductionTradingState::new(LiveTradingState::new(Decimal::from(100), 0).unwrap());
        state.mfce.source_epoch = 41;
        state.mfce_time_high_watermark = 92_000;
        state.save_atomic(&path).unwrap();
        let committed = std::fs::read(&path).unwrap();

        let restored = ProductionTradingState::load(&path).unwrap();
        assert_eq!(restored.mfce, state.mfce);
        assert_eq!(restored.mfce_time_high_watermark, 92_000);

        let mut invalid = state.clone();
        invalid.mfce.schema_version = u32::MAX;
        assert_eq!(
            invalid.save_atomic(&path),
            Err(ReconciliationApplyError::SchemaMismatch)
        );
        assert_eq!(std::fs::read(&path).unwrap(), committed);

        let mut corrupt = serde_json::from_slice::<serde_json::Value>(&committed).unwrap();
        corrupt["mfce"]["schema_version"] = serde_json::json!(u32::MAX);
        std::fs::write(&path, serde_json::to_vec(&corrupt).unwrap()).unwrap();
        assert_eq!(
            ProductionTradingState::load(&path),
            Err(ReconciliationApplyError::SchemaMismatch)
        );

        let mut incomplete_v2 = serde_json::from_slice::<serde_json::Value>(&committed).unwrap();
        incomplete_v2.as_object_mut().unwrap().remove("mfce");
        std::fs::write(&path, serde_json::to_vec(&incomplete_v2).unwrap()).unwrap();
        assert_eq!(
            ProductionTradingState::load(&path),
            Err(ReconciliationApplyError::SchemaMismatch)
        );

        let mut legacy = serde_json::to_value(&state).unwrap();
        {
            let legacy_object = legacy.as_object_mut().unwrap();
            legacy_object.insert(
                "schema_version".into(),
                serde_json::json!(LEGACY_PRODUCTION_STATE_SCHEMA_VERSION),
            );
            legacy_object.remove("mfce");
            legacy_object.remove("mfce_time_high_watermark");
        }
        std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
        let migrated = ProductionTradingState::load(&path).unwrap();
        assert_eq!(migrated.mfce, MfcePersistentState::default());
        assert_eq!(migrated.mfce_time_high_watermark, 0);
        assert_eq!(migrated.schema_version, PRODUCTION_STATE_SCHEMA_VERSION);

        std::fs::remove_file(&path).unwrap();
    }
}
