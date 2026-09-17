//! Authenticated production boundary. Strategy and sizing remain in the engine decision graph.

pub use crate::domain::authorized_intent::{AuthorizedExecutionIntent, PreSigningContext};
use crate::domain::decision::{
    derive_projection_hash, ConfigHash, PlannedCloid, RiskPolicyHash, Side,
};
use crate::domain::execution_floor::validates_rounded_order;
use crate::domain::portfolio_risk::{
    project_and_validate_portfolio, OpenOrderExposure, OpenOrderLifecycle, OrderSide,
    PortfolioProjectionInput,
};
use crate::signing::reconciliation::{apply_authenticated_batches, AppliedExchangeBatch};
use crate::signing::transport::{
    AuthenticatedExchangeTransport, ExchangeOpenOrder, ExchangeOpenOrdersSnapshot,
    ExchangePositionSnapshot, FillCursor, FundingBatch, FundingCursor, OrderObservation,
    SubmissionResponse, SubmissionTransportError, UserFillBatch,
};
use crate::signing::{
    ApiWalletSecret, ApprovedExecutionIntent, NonceAllocator, SignerError, SubmissionRegistry,
    SubmissionState,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationFailure {
    Expired,
    PolicyHashMismatch,
    InvalidIdentity,
    InvalidMarketRules,
    BelowExchangeMinimum,
    CurrentTargetNoLongerRequiresAction,
    ProjectionChanged,
    UnresolvedAsset,
    DuplicateCloid,
    Risk(String),
    Arithmetic,
}

impl std::fmt::Display for AuthorizationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AuthorizationFailure {}

pub fn validate_authorized_intent(
    intent: &AuthorizedExecutionIntent,
    context: &PreSigningContext,
    registry: &SubmissionRegistry,
) -> Result<(), AuthorizationFailure> {
    intent
        .validate_canonical()
        .map_err(|_| AuthorizationFailure::InvalidIdentity)?;
    if context.now_mono > intent.expires_at {
        return Err(AuthorizationFailure::Expired);
    }
    if intent.risk_policy_hash != context.deployed_risk_policy_hash {
        return Err(AuthorizationFailure::PolicyHashMismatch);
    }
    if registry
        .state(&cloid_string(intent.planned_cloid))
        .is_some_and(|state| state != &SubmissionState::AuthorizedNotSubmitted)
    {
        return Err(AuthorizationFailure::DuplicateCloid);
    }
    if !intent.reduce_only && !registry.permits_increase(&intent.asset) {
        return Err(AuthorizationFailure::UnresolvedAsset);
    }
    if intent.asset.is_empty()
        || intent.quantity <= Decimal::ZERO
        || intent.limit_price <= Decimal::ZERO
        || intent.decision_reference_price <= Decimal::ZERO
        || (intent.continuation_generation == 0 && intent.parent_cloid.is_some())
        || (intent.continuation_generation > 0 && intent.parent_cloid.is_none())
    {
        return Err(AuthorizationFailure::InvalidIdentity);
    }
    let rules = context
        .projection_input
        .market_rules
        .get(&intent.asset)
        .ok_or(AuthorizationFailure::InvalidMarketRules)?;
    if intent.quantity % rules.size_step != Decimal::ZERO
        || intent.limit_price % rules.price_tick != Decimal::ZERO
    {
        return Err(AuthorizationFailure::InvalidMarketRules);
    }
    if !validates_rounded_order(
        intent.quantity,
        intent.limit_price,
        context.exchange_minimum_notional,
    )
    .map_err(|_| AuthorizationFailure::Arithmetic)?
    {
        return Err(AuthorizationFailure::BelowExchangeMinimum);
    }
    let projection = project_and_validate_portfolio(&context.projection_input)
        .map_err(|error| AuthorizationFailure::Risk(error.to_string()))?;
    if derive_projection_hash(&projection).map_err(|_| AuthorizationFailure::Arithmetic)?
        != intent.projected_portfolio_hash
    {
        return Err(AuthorizationFailure::ProjectionChanged);
    }
    let required = projection
        .rounded_deltas
        .get(&intent.asset)
        .copied()
        .unwrap_or(Decimal::ZERO);
    let intended = intent
        .quantity
        .checked_mul(rules.mark_price)
        .ok_or(AuthorizationFailure::Arithmetic)?;
    let side_matches = match intent.side {
        Side::Buy => required > Decimal::ZERO,
        Side::Sell => required < Decimal::ZERO,
    };
    if !side_matches || required.abs() != intended {
        return Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction);
    }
    Ok(())
}

fn exchange_open_order_exposure(order: &ExchangeOpenOrder) -> Option<OpenOrderExposure> {
    // Keep the full filled exposure until reductions actually settle. A venue
    // enforced reduce-only order cannot add exposure and earns no risk credit.
    if order.reduce_only {
        return None;
    }
    let side = if order.is_buy {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    Some(OpenOrderExposure {
        asset: order.asset.clone(),
        side: Some(side),
        notional: order.remaining_quantity.checked_mul(order.limit_price),
        lifecycle: OpenOrderLifecycle::Acknowledged,
    })
}

fn validate_exchange_open_orders(
    snapshot: &ExchangeOpenOrdersSnapshot,
    registry: &SubmissionRegistry,
    positions: &ExchangePositionSnapshot,
) -> Result<(), SignerError> {
    let mut observed = BTreeSet::new();
    let mut observed_order_ids = BTreeSet::new();
    for order in &snapshot.orders {
        if !observed_order_ids.insert(&order.exchange_order_id) {
            return Err(SignerError::ExchangeTruthMismatch(
                "duplicate exchange open-order ID".into(),
            ));
        }
        let position = positions
            .positions
            .get(&order.asset)
            .copied()
            .unwrap_or_default();
        let reduces = (order.is_buy && position < Decimal::ZERO)
            || (!order.is_buy && position > Decimal::ZERO);
        let valid_size = order.original_quantity >= order.remaining_quantity
            && order.remaining_quantity >= Decimal::ZERO
            && (order.remaining_quantity > Decimal::ZERO
                || (order.original_quantity == Decimal::ZERO
                    && order.is_trigger
                    && order.is_position_tpsl));
        // An exchange-confirmed external protective order has no engine intent.
        // Retain it without claiming ownership or bypassing managed-order checks.
        // Zero size is valid only for the venue's whole-position TP/SL encoding.
        if order.cloid.is_none()
            && order.reduce_only
            && reduces
            && valid_size
            && order.limit_price > Decimal::ZERO
        {
            continue;
        }
        let cloid = order.cloid.ok_or_else(|| {
            SignerError::ExchangeTruthMismatch(format!(
                "unmanaged open order {} has no CLOID",
                order.exchange_order_id
            ))
        })?;
        let key = cloid_string(cloid);
        if !observed.insert(key.clone()) {
            return Err(SignerError::ExchangeTruthMismatch(
                "duplicate open-order CLOID in exchange snapshot".into(),
            ));
        }
        let intent = registry.intent(&key).ok_or_else(|| {
            SignerError::ExchangeTruthMismatch(format!(
                "unmanaged exchange open order {}",
                order.exchange_order_id
            ))
        })?;
        let (expected_order_id, expected_remaining) = match registry.state(&key) {
            Some(SubmissionState::Acknowledged { order_id }) => (order_id, intent.quantity),
            Some(SubmissionState::PartiallyFilled { order_id, filled }) => (
                order_id,
                intent
                    .quantity
                    .checked_sub(*filled)
                    .ok_or(SignerError::InvalidDecimal)?,
            ),
            _ => {
                return Err(SignerError::ExchangeTruthMismatch(format!(
                    "exchange order {} is open but durable state is not open",
                    order.exchange_order_id
                )))
            }
        };
        if expected_order_id != &order.exchange_order_id
            || intent.asset != order.asset
            || intent.is_buy != order.is_buy
            || intent.limit_price != order.limit_price
            || intent.quantity != order.original_quantity
            || expected_remaining != order.remaining_quantity
            || intent.reduce_only != order.reduce_only
        {
            return Err(SignerError::ExchangeTruthMismatch(format!(
                "exchange open order {} disagrees with durable intent",
                order.exchange_order_id
            )));
        }
    }

    for (cloid, _, state) in registry.entries() {
        if matches!(
            state,
            SubmissionState::Acknowledged { .. } | SubmissionState::PartiallyFilled { .. }
        ) && !observed.contains(cloid)
        {
            return Err(SignerError::ExchangeTruthMismatch(format!(
                "durable open order {cloid} is absent from exchange open orders"
            )));
        }
    }
    Ok(())
}

fn validate_recovery_reduce_only(
    intent: &AuthorizedExecutionIntent,
    positions: &ExchangePositionSnapshot,
    context: &PreSigningContext,
    open_orders: &ExchangeOpenOrdersSnapshot,
) -> Result<(), AuthorizationFailure> {
    if context.now_mono > intent.expires_at {
        return Err(AuthorizationFailure::Expired);
    }
    let rules = context
        .projection_input
        .market_rules
        .get(&intent.asset)
        .ok_or(AuthorizationFailure::InvalidMarketRules)?;
    if !intent.reduce_only
        || rules.size_step <= Decimal::ZERO
        || rules.price_tick <= Decimal::ZERO
        || rules.mark_price <= Decimal::ZERO
        || intent.quantity <= Decimal::ZERO
        || intent.limit_price <= Decimal::ZERO
        || intent.quantity % rules.size_step != Decimal::ZERO
        || intent.limit_price % rules.price_tick != Decimal::ZERO
        || !validates_rounded_order(
            intent.quantity,
            intent.limit_price,
            context.exchange_minimum_notional,
        )
        .map_err(|_| AuthorizationFailure::Arithmetic)?
    {
        return Err(AuthorizationFailure::InvalidMarketRules);
    }
    let position = positions
        .positions
        .get(&intent.asset)
        .copied()
        .unwrap_or(Decimal::ZERO);
    let reduces = match intent.side {
        Side::Buy => position < Decimal::ZERO,
        Side::Sell => position > Decimal::ZERO,
    };
    let reserved = open_orders
        .orders
        .iter()
        .filter(|order| {
            order.asset == intent.asset && order.is_buy == matches!(intent.side, Side::Buy)
        })
        .try_fold(Decimal::ZERO, |sum, order| {
            sum.checked_add(order.remaining_quantity)
        })
        .ok_or(AuthorizationFailure::Arithmetic)?;
    let available = position
        .abs()
        .checked_sub(reserved)
        .ok_or(AuthorizationFailure::Arithmetic)?;
    if !reduces || intent.quantity > available {
        return Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductionSubmissionResult {
    NoAction,
    Acknowledged,
    Filled { order_id: String, quantity: Decimal },
    Rejected { reason: String },
    Unknown,
}

pub struct ProductionSigner<T: AuthenticatedExchangeTransport> {
    transport: T,
    wallet: ApiWalletSecret,
    nonce: NonceAllocator,
    registry: Mutex<SubmissionRegistry>,
    registry_path: PathBuf,
    submission_lock: Mutex<()>,
    startup_reconciled: Mutex<bool>,
    exchange_snapshot: Mutex<Option<ExchangePositionSnapshot>>,
    deployed_risk_policy_hash: RiskPolicyHash,
    deployed_configuration_hash: ConfigHash,
    release_manifest_hash: [u8; 32],
    /// Builder DEXes with open exposure / pending lifecycle. `""` (native)
    /// is always reconciled; HIP-3 DEXes are added as they become active.
    active_dexes: Mutex<BTreeSet<String>>,
}

impl<T: AuthenticatedExchangeTransport> ProductionSigner<T> {
    pub fn new(
        transport: T,
        wallet: ApiWalletSecret,
        registry: SubmissionRegistry,
        registry_path: impl Into<PathBuf>,
        initial_nonce: u64,
        deployed_risk_policy_hash: RiskPolicyHash,
        deployed_configuration_hash: ConfigHash,
        release_manifest_hash: [u8; 32],
    ) -> Result<Self, SignerError> {
        let nonce_floor = initial_nonce.max(registry.next_nonce_floor()?);
        Ok(Self {
            transport,
            wallet,
            nonce: NonceAllocator::new(nonce_floor),
            registry: Mutex::new(registry),
            registry_path: registry_path.into(),
            submission_lock: Mutex::new(()),
            startup_reconciled: Mutex::new(false),
            exchange_snapshot: Mutex::new(None),
            deployed_risk_policy_hash,
            deployed_configuration_hash,
            release_manifest_hash,
            active_dexes: Mutex::new(BTreeSet::new()),
        })
    }

    /// Declare builder DEXes with open exposure / pending lifecycle /
    /// recent execution. Reconciliation aggregates the native DEX plus
    /// every active HIP-3 DEX so a successful HIP-3 order never becomes
    /// an unknown exchange root after restart.
    pub async fn set_active_dexes(&self, dexes: BTreeSet<String>) {
        let mut active = self.active_dexes.lock().await;
        *active = dexes
            .into_iter()
            .filter(|dex| !dex.is_empty() && dex != crate::hip3::EXCLUDED_HIP3_DEX)
            .collect();
    }

    async fn reconciled_dex_list(&self) -> Vec<String> {
        let mut dexes = vec![String::new()];
        let active = self.active_dexes.lock().await;
        let mut extra: Vec<String> = active.iter().cloned().collect();
        extra.sort();
        dexes.extend(extra);
        dexes
    }

    async fn read_aggregated_positions(
        &self,
    ) -> Result<crate::signing::transport::ExchangePositionSnapshot, SignerError> {
        let mut snapshots = Vec::new();
        for dex in self.reconciled_dex_list().await {
            let snapshot = self
                .transport
                .read_positions_for_dex(&dex)
                .await
                .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
            snapshots.push(snapshot);
        }
        crate::signing::transport::merge_position_snapshots(snapshots)
            .map_err(|error| SignerError::Reconciliation(error.to_string()))
    }

    async fn read_aggregated_open_orders(
        &self,
    ) -> Result<crate::signing::transport::ExchangeOpenOrdersSnapshot, SignerError> {
        let mut snapshots = Vec::new();
        for dex in self.reconciled_dex_list().await {
            let snapshot = self
                .transport
                .read_open_orders_for_dex(&dex)
                .await
                .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
            snapshots.push(snapshot);
        }
        Ok(crate::signing::transport::merge_open_order_snapshots(
            snapshots,
        ))
    }

    /// Refresh active DEXes from durable registry intents plus live-state
    /// positions before a reconciliation barrier.
    pub async fn refresh_active_dexes_from_registry_and_positions(
        &self,
        positions: &BTreeMap<String, Decimal>,
    ) {
        let registry = self.registry.lock().await;
        let mut dexes = BTreeSet::new();
        for (_, intent, _) in registry.entries() {
            let dex = crate::hip3::dex_for_market(&intent.asset);
            if !dex.is_empty() {
                dexes.insert(dex.to_string());
            }
        }
        drop(registry);
        for asset in positions.keys() {
            let dex = crate::hip3::dex_for_market(asset);
            if !dex.is_empty() {
                dexes.insert(dex.to_string());
            }
        }
        self.set_active_dexes(dexes).await;
    }

    pub async fn reconcile_startup(
        &self,
        live_state: &mut crate::domain::live_trading::LiveTradingState,
        ledger_path: &Path,
        now_ms: u64,
        required_not_found_confirmations: u32,
        minimum_confirmation_interval_ms: u64,
    ) -> Result<AppliedExchangeBatch, SignerError> {
        *self.startup_reconciled.lock().await = false;
        // Close the event cursor first, then compare the applied ledger with an
        // account-state snapshot taken after that boundary. Concurrent fills
        // fail the exact position comparison and force another reconciliation.
        let mut fills = self
            .read_all_fills(live_state.fill_cursor_ms(), now_ms)
            .await?;
        let mut funding = self
            .read_all_funding(live_state.funding_cursor_ms(), now_ms)
            .await?;
        let states: Vec<_> = self
            .registry
            .lock()
            .await
            .entries()
            .map(|(cloid, _, state)| (cloid.to_string(), state.clone()))
            .collect();
        for (cloid, state) in states {
            match state {
                SubmissionState::Authorized
                | SubmissionState::NonceAllocated { .. }
                | SubmissionState::Signed { .. } => {
                    let mut registry = self.registry.lock().await;
                    registry.transition_at(
                        &cloid,
                        SubmissionState::AuthorizedNotSubmitted,
                        now_ms,
                    )?;
                    registry.persist(&self.registry_path)?;
                }
                SubmissionState::SubmissionStarted { nonce } => {
                    let mut registry = self.registry.lock().await;
                    registry.transition_at(
                        &cloid,
                        SubmissionState::UnknownResult { nonce },
                        now_ms,
                    )?;
                    registry.persist(&self.registry_path)?;
                    drop(registry);
                    let cloid = decode_planned_cloid(&cloid)?;
                    let state = self
                        .reconcile_unknown(
                            cloid,
                            now_ms,
                            required_not_found_confirmations,
                            minimum_confirmation_interval_ms,
                        )
                        .await?;
                    if matches!(state, SubmissionState::UnknownResult { .. }) {
                        return Err(SignerError::StartupNotReconciled);
                    }
                }
                SubmissionState::UnknownResult { .. } => {
                    let cloid = decode_planned_cloid(&cloid)?;
                    let state = self
                        .reconcile_unknown(
                            cloid,
                            now_ms,
                            required_not_found_confirmations,
                            minimum_confirmation_interval_ms,
                        )
                        .await?;
                    if matches!(state, SubmissionState::UnknownResult { .. }) {
                        return Err(SignerError::StartupNotReconciled);
                    }
                }
                SubmissionState::Acknowledged { .. } | SubmissionState::PartiallyFilled { .. } => {
                    let cloid = decode_planned_cloid(&cloid)?;
                    let state = self
                        .reconcile_unknown(
                            cloid,
                            now_ms,
                            required_not_found_confirmations,
                            minimum_confirmation_interval_ms,
                        )
                        .await?;
                    if !matches!(
                        state,
                        SubmissionState::Acknowledged { .. }
                            | SubmissionState::PartiallyFilled { .. }
                            | SubmissionState::Filled { .. }
                            | SubmissionState::Cancelled { .. }
                            | SubmissionState::Rejected { .. }
                    ) {
                        return Err(SignerError::StartupNotReconciled);
                    }
                }
                _ => {}
            }
        }

        // Order resolution is bracketed by a fresh open-order and account
        // snapshot. If an order changes while this barrier runs, one of these
        // exact comparisons fails and the next recovery pass closes the gap.
        // Both snapshots aggregate the native DEX plus every active HIP-3 DEX
        // so builder-perp exposure reconciles exactly like native perps.
        self.refresh_active_dexes_from_registry_and_positions(&live_state.positions())
            .await;
        let open_orders = self.read_aggregated_open_orders().await?;
        let positions = self.read_aggregated_positions().await?;
        *self.exchange_snapshot.lock().await = Some(positions.clone());
        // The clearinghouse response used for positions also carries account
        // equity. Keep the risk snapshot atomic: a second request can observe a
        // different mark price a few milliseconds later and must not force the
        // signer into recovery merely because unrealized PnL moved.
        let account_equity = positions.account_equity;

        self.recover_missing_receipts(live_state, &mut fills, &mut funding, now_ms)
            .await?;
        let applied = {
            let registry = self.registry.lock().await;
            validate_exchange_open_orders(&open_orders, &registry, &positions)?;
            apply_authenticated_batches(
                live_state,
                &registry,
                fills,
                funding,
                &positions,
                account_equity,
                ledger_path,
            )?
        };
        *self.startup_reconciled.lock().await = true;
        Ok(applied)
    }

    pub async fn exchange_snapshot(&self) -> Option<ExchangePositionSnapshot> {
        self.exchange_snapshot.lock().await.clone()
    }

    /// The incremental cursor is not proof that a durable submitted action was
    /// accounted. Only deficient registry receipts may open a bounded lookback.
    async fn recover_missing_receipts(
        &self,
        state: &crate::domain::live_trading::LiveTradingState,
        fills: &mut UserFillBatch,
        funding: &mut FundingBatch,
        now_ms: u64,
    ) -> Result<(), SignerError> {
        let registry = self.registry.lock().await;
        let mut obligations = Vec::new();
        for (cloid, _, status) in registry.entries() {
            let expected = match status {
                SubmissionState::Filled { filled, .. }
                | SubmissionState::Cancelled { filled, .. }
                | SubmissionState::PartiallyFilled { filled, .. } => *filled,
                _ => continue,
            };
            let id = decode_planned_cloid(cloid)?;
            let mut seen = BTreeSet::new();
            let mut accounted = Decimal::ZERO;
            for prior in state
                .verified_fills()
                .iter()
                .filter(|f| f.identity.cloid == id)
            {
                seen.insert((
                    prior.identity.exchange_order_id.0.clone(),
                    prior.identity.trade_id.0.clone(),
                ));
                accounted += prior.filled_quantity;
            }
            for fill in fills.fills.iter().filter(|f| f.cloid == Some(id)) {
                if seen.insert((fill.exchange_order_id.clone(), fill.trade_id.clone())) {
                    accounted += fill.quantity;
                }
            }
            if accounted >= expected {
                continue;
            }
            let start = registry
                .history()
                .iter()
                .filter(|event| {
                    event.cloid == cloid
                        && matches!(event.state, SubmissionState::SubmissionStarted { .. })
                        && event.recorded_at_ms > 0
                })
                .map(|event| event.recorded_at_ms)
                .min()
                .ok_or_else(|| {
                    SignerError::ExchangeTruthMismatch(format!(
                        "missing receipt has no wall-clock submission boundary cloid={cloid}"
                    ))
                })?;
            let start = start.saturating_sub(1_000);
            let end = now_ms.min(start.saturating_add(86_400_000));
            if start >= state.fill_cursor_ms() {
                return Err(SignerError::ExchangeTruthMismatch(format!(
                    "fill receipt not yet visible cloid={cloid}"
                )));
            }
            obligations.push((id, expected, start, end));
        }
        drop(registry);
        if obligations.len() > 16 {
            return Err(SignerError::Reconciliation(
                "targeted receipt recovery exceeds bounded batch".into(),
            ));
        }
        for (cloid, expected, start, end) in obligations {
            let history = self.read_all_fills(start, end).await?;
            let quantity = history
                .fills
                .iter()
                .filter(|f| f.cloid == Some(cloid))
                .try_fold(Decimal::ZERO, |sum, fill| sum.checked_add(fill.quantity))
                .ok_or(SignerError::InvalidIntent)?;
            if quantity < expected {
                return Err(SignerError::ExchangeTruthMismatch(format!(
                    "targeted receipt incomplete cloid={cloid}"
                )));
            }
            // Keep all account events in this bounded interval: filtering out
            // a manual reduction or intervening funding would break chronology.
            fills.fills.extend(history.fills);
            funding
                .events
                .extend(self.read_all_funding(start, end).await?.events);
        }
        // Preserve cursor watermarks from the normal reads, not from lookback.
        let mut unique: std::collections::BTreeMap<_, crate::signing::transport::UserFill> =
            std::collections::BTreeMap::new();
        for fill in fills.fills.drain(..) {
            let key = (
                fill.asset.clone(),
                fill.exchange_order_id.clone(),
                fill.trade_id.clone(),
            );
            if let Some(prior) = unique.get(&key) {
                let mut comparable: crate::signing::transport::UserFill = fill.clone();
                comparable.source_hash = prior.source_hash;
                if &comparable != prior {
                    return Err(SignerError::ExchangeFillMismatch);
                }
            } else {
                unique.insert(key, fill);
            }
        }
        fills.fills = unique.into_values().collect();
        let mut unique: std::collections::BTreeMap<_, crate::signing::transport::FundingRecord> =
            std::collections::BTreeMap::new();
        for event in funding.events.drain(..) {
            let key = (event.asset.clone(), event.occurred_at_ms);
            if let Some(prior) = unique.get(&key) {
                if prior.signed_usdc_delta != event.signed_usdc_delta {
                    return Err(SignerError::Ledger(
                        "funding identity payload conflict".into(),
                    ));
                }
            } else {
                unique.insert(key, event);
            }
        }
        funding.events = unique.into_values().collect();
        Ok(())
    }

    async fn read_all_fills(
        &self,
        start_time_ms: u64,
        end_time_ms: u64,
    ) -> Result<UserFillBatch, SignerError> {
        if start_time_ms > end_time_ms {
            return Ok(UserFillBatch {
                fills: Vec::new(),
                next_cursor: FillCursor {
                    start_time_ms,
                    end_time_ms: Some(end_time_ms),
                },
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            });
        }
        let mut cursor = FillCursor {
            start_time_ms,
            end_time_ms: Some(end_time_ms),
        };
        let mut all = Vec::new();
        let mut last_hash: crate::domain::decision::PayloadHash;
        for _ in 0..10_000 {
            let page = self
                .transport
                .read_fills(cursor)
                .await
                .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
            last_hash = page.source_hash;
            if page.fills.is_empty() {
                return Ok(UserFillBatch {
                    fills: all,
                    next_cursor: FillCursor {
                        start_time_ms: end_time_ms.saturating_add(1),
                        end_time_ms: Some(end_time_ms),
                    },
                    source_hash: last_hash,
                });
            }
            if page.next_cursor.start_time_ms <= cursor.start_time_ms {
                return Err(SignerError::Reconciliation(
                    "fill pagination cursor did not advance".into(),
                ));
            }
            cursor = page.next_cursor;
            all.extend(page.fills);
            if cursor.start_time_ms > end_time_ms {
                return Ok(UserFillBatch {
                    fills: all,
                    next_cursor: cursor,
                    source_hash: last_hash,
                });
            }
        }
        Err(SignerError::Reconciliation(
            "fill pagination limit exceeded".into(),
        ))
    }

    async fn read_all_funding(
        &self,
        start_time_ms: u64,
        end_time_ms: u64,
    ) -> Result<FundingBatch, SignerError> {
        if start_time_ms > end_time_ms {
            return Ok(FundingBatch {
                events: Vec::new(),
                next_cursor: FundingCursor {
                    start_time_ms,
                    end_time_ms: Some(end_time_ms),
                },
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            });
        }
        let mut cursor = FundingCursor {
            start_time_ms,
            end_time_ms: Some(end_time_ms),
        };
        let mut all = Vec::new();
        let mut last_hash: crate::domain::decision::PayloadHash;
        for _ in 0..10_000 {
            let page = self
                .transport
                .read_funding(cursor)
                .await
                .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
            last_hash = page.source_hash;
            if page.events.is_empty() {
                return Ok(FundingBatch {
                    events: all,
                    next_cursor: FundingCursor {
                        start_time_ms: end_time_ms.saturating_add(1),
                        end_time_ms: Some(end_time_ms),
                    },
                    source_hash: last_hash,
                });
            }
            if page.next_cursor.start_time_ms <= cursor.start_time_ms {
                return Err(SignerError::Reconciliation(
                    "funding pagination cursor did not advance".into(),
                ));
            }
            cursor = page.next_cursor;
            all.extend(page.events);
            if cursor.start_time_ms > end_time_ms {
                return Ok(FundingBatch {
                    events: all,
                    next_cursor: cursor,
                    source_hash: last_hash,
                });
            }
        }
        Err(SignerError::Reconciliation(
            "funding pagination limit exceeded".into(),
        ))
    }

    pub async fn submit(
        &self,
        intent: AuthorizedExecutionIntent,
        unix_millis: u64,
    ) -> Result<ProductionSubmissionResult, SignerError> {
        match self.authorize(intent.clone(), unix_millis).await {
            Err(SignerError::NoAction) => return Ok(ProductionSubmissionResult::NoAction),
            result => {
                result?;
            }
        }
        self.submit_authorized(intent, unix_millis).await
    }

    /// Revalidates current exchange state and fsyncs `Authorized` before the
    /// in-process execution boundary may submit the authorized IOC.
    pub async fn authorize(
        &self,
        intent: AuthorizedExecutionIntent,
        now_ms: u64,
    ) -> Result<u64, SignerError> {
        let _serial = self.submission_lock.lock().await;
        intent
            .validate_canonical()
            .map_err(|reason| SignerError::Authorization(reason))?;
        let mut context = intent.authorization.clone();
        context.now_mono = now_ms;
        let fully_reconciled = *self.startup_reconciled.lock().await;
        if !fully_reconciled && !intent.reduce_only {
            return Err(SignerError::StartupNotReconciled);
        }
        if context.deployed_risk_policy_hash != self.deployed_risk_policy_hash
            || intent.risk_policy_hash != self.deployed_risk_policy_hash
        {
            return Err(SignerError::Authorization(
                "deployed risk-policy hash mismatch".into(),
            ));
        }
        if intent.configuration_hash != self.deployed_configuration_hash
            || intent.release_manifest_hash != self.release_manifest_hash
        {
            return Err(SignerError::Authorization(
                "release/configuration hash mismatch".into(),
            ));
        }
        {
            let registry = self.registry.lock().await;
            let key = cloid_string(intent.planned_cloid);
            if registry.state(&key) == Some(&SubmissionState::NoAction) {
                return if registry.intent(&key) == Some(&durable_intent(&intent)) {
                    Err(SignerError::NoAction)
                } else {
                    Err(SignerError::CloidMismatch)
                };
            }
            if !intent.reduce_only
                && registry.has_authorized_not_submitted()
                && registry.state(&key) != Some(&SubmissionState::AuthorizedNotSubmitted)
            {
                return Err(SignerError::StartupNotReconciled);
            }
        }

        // Rebuild committed exposure from current exchange state immediately
        // before authorization. The core projector remains the sole risk formula.
        // Reads aggregate native + active HIP-3 DEXes so builder-perp risk is
        // exact at the signing barrier.
        let positions = self.read_aggregated_positions().await?;
        let open_orders = self.read_aggregated_open_orders().await?;
        context.projection_input.current_equity = positions.account_equity;
        context.projection_input.filled_positions =
            positions_to_notional(&positions, &context.projection_input)?;
        context.projection_input.acknowledged_open_orders.clear();
        context.projection_input.filled_position_state_complete = true;
        context.projection_input.open_order_state_complete = true;

        let mut registry = self.registry.lock().await;
        if let Err(error) = validate_exchange_open_orders(&open_orders, &registry, &positions) {
            if !intent.reduce_only {
                return Err(error);
            }
        }
        context.projection_input.acknowledged_open_orders.extend(
            open_orders
                .orders
                .iter()
                .filter_map(exchange_open_order_exposure),
        );
        let validation = if fully_reconciled {
            validate_authorized_intent(&intent, &context, &registry)
        } else {
            validate_recovery_reduce_only(&intent, &positions, &context, &open_orders)
        };
        if matches!(
            validation,
            Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction
                | AuthorizationFailure::ProjectionChanged)
        ) {
            let durable = durable_intent(&intent);
            let key = cloid_string(intent.planned_cloid);
            if let Some(existing) = registry.intent(&key) {
                if existing != &durable {
                    return Err(SignerError::CloidMismatch);
                }
            } else {
                registry.register(durable)?;
            }
            registry.transition_at(&key, SubmissionState::NoAction, now_ms)?;
            registry.persist(&self.registry_path)?;
            return Err(SignerError::NoAction);
        }
        validation.map_err(|error| SignerError::Authorization(error.to_string()))?;
        let durable = durable_intent(&intent);
        let key = cloid_string(intent.planned_cloid);
        if registry.state(&key) == Some(&SubmissionState::AuthorizedNotSubmitted) {
            if registry.intent(&key) != Some(&durable) {
                return Err(SignerError::CloidMismatch);
            }
            registry.transition_at(&key, SubmissionState::Authorized, now_ms)?;
        } else {
            registry.register(durable)?;
        }
        registry.persist(&self.registry_path)?;

        registry.durable_sequence()
    }

    /// Performs signing and the external submission side effect only for an
    /// intent that was previously durably authorized and acknowledged.
    pub async fn submit_authorized(
        &self,
        intent: AuthorizedExecutionIntent,
        unix_millis: u64,
    ) -> Result<ProductionSubmissionResult, SignerError> {
        let _serial = self.submission_lock.lock().await;
        if !*self.startup_reconciled.lock().await && !intent.reduce_only {
            return Err(SignerError::StartupNotReconciled);
        }
        let mut registry = self.registry.lock().await;
        let key = cloid_string(intent.planned_cloid);
        let persisted = registry.approved_intent(&key)?;
        if persisted != durable_intent(&intent) {
            return Err(SignerError::Authorization(
                "authorized intent differs from durable record".into(),
            ));
        }

        let nonce = self.nonce.next(unix_millis)?;
        transition_and_persist(
            &mut registry,
            &self.registry_path,
            intent.planned_cloid,
            SubmissionState::NonceAllocated { nonce },
            unix_millis,
        )?;
        let request = self.wallet.sign_ioc(&intent, nonce)?;
        transition_and_persist(
            &mut registry,
            &self.registry_path,
            intent.planned_cloid,
            SubmissionState::Signed { nonce },
            unix_millis,
        )?;
        transition_and_persist(
            &mut registry,
            &self.registry_path,
            intent.planned_cloid,
            SubmissionState::SubmissionStarted { nonce },
            unix_millis,
        )?;
        *self.startup_reconciled.lock().await = false;
        drop(registry);

        let response = self.transport.submit_ioc(request).await;
        let mut registry = self.registry.lock().await;
        match response {
            Ok(SubmissionResponse::Acknowledged) => {
                transition_and_persist(
                    &mut registry,
                    &self.registry_path,
                    intent.planned_cloid,
                    SubmissionState::Acknowledged {
                        order_id: cloid_string(intent.planned_cloid),
                    },
                    unix_millis,
                )?;
                Ok(ProductionSubmissionResult::Acknowledged)
            }
            Ok(SubmissionResponse::Filled {
                exchange_order_id,
                filled_quantity,
                ..
            }) => {
                transition_and_persist(
                    &mut registry,
                    &self.registry_path,
                    intent.planned_cloid,
                    SubmissionState::Filled {
                        order_id: exchange_order_id.clone(),
                        filled: filled_quantity,
                    },
                    unix_millis,
                )?;
                Ok(ProductionSubmissionResult::Filled {
                    order_id: exchange_order_id,
                    quantity: filled_quantity,
                })
            }
            Ok(SubmissionResponse::Rejected { reason }) => {
                transition_and_persist(
                    &mut registry,
                    &self.registry_path,
                    intent.planned_cloid,
                    SubmissionState::Rejected {
                        reason: reason.clone(),
                    },
                    unix_millis,
                )?;
                Ok(ProductionSubmissionResult::Rejected { reason })
            }
            Err(
                SubmissionTransportError::UnknownResult
                | SubmissionTransportError::InvalidResponse(_),
            ) => {
                transition_and_persist(
                    &mut registry,
                    &self.registry_path,
                    intent.planned_cloid,
                    SubmissionState::UnknownResult { nonce },
                    unix_millis,
                )?;
                Ok(ProductionSubmissionResult::Unknown)
            }
            Err(SubmissionTransportError::Http(status)) => {
                // A definite pre-acceptance HTTP/validation rejection is durable;
                // ambiguous transport is represented only by UnknownResult.
                let reason = format!("exchange HTTP {status}");
                transition_and_persist(
                    &mut registry,
                    &self.registry_path,
                    intent.planned_cloid,
                    SubmissionState::Rejected {
                        reason: reason.clone(),
                    },
                    unix_millis,
                )?;
                Err(SignerError::Transport(reason))
            }
        }
    }

    pub async fn reconcile_unknown(
        &self,
        cloid: PlannedCloid,
        observed_at_ms: u64,
        required_not_found_confirmations: u32,
        minimum_confirmation_interval_ms: u64,
    ) -> Result<SubmissionState, SignerError> {
        let observation = self
            .transport
            .lookup_order(cloid)
            .await
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        let was_not_found = matches!(observation, OrderObservation::NotFound);
        let key = cloid_string(cloid);
        let mut registry = self.registry.lock().await;
        let state = match observation {
            OrderObservation::NotFound => {
                if !matches!(
                    registry.state(&key),
                    Some(SubmissionState::UnknownResult { .. })
                ) {
                    return Err(SignerError::Reconciliation(
                        "previously observed order is now not found".into(),
                    ));
                }
                let resolved = registry.confirm_not_found(
                    &key,
                    observed_at_ms,
                    required_not_found_confirmations,
                    minimum_confirmation_interval_ms,
                )?;
                registry.persist(&self.registry_path)?;
                if resolved {
                    SubmissionState::ReconciledNotFound
                } else {
                    registry
                        .state(&key)
                        .cloned()
                        .ok_or(SignerError::UnknownCloid)?
                }
            }
            OrderObservation::Open {
                exchange_order_id,
                original_quantity,
                remaining_quantity,
            } => {
                let filled = original_quantity
                    .checked_sub(remaining_quantity)
                    .ok_or(SignerError::InvalidDecimal)?;
                if filled.is_zero() {
                    SubmissionState::Acknowledged {
                        order_id: exchange_order_id,
                    }
                } else {
                    SubmissionState::PartiallyFilled {
                        order_id: exchange_order_id,
                        filled,
                    }
                }
            }
            OrderObservation::Filled {
                exchange_order_id,
                filled_quantity,
            } => SubmissionState::Filled {
                order_id: exchange_order_id,
                filled: filled_quantity,
            },
            OrderObservation::Cancelled {
                exchange_order_id,
                filled_quantity,
                ..
            } => SubmissionState::Cancelled {
                order_id: Some(exchange_order_id),
                filled: filled_quantity,
            },
            OrderObservation::Rejected { reason, .. } => SubmissionState::Rejected { reason },
        };
        if !was_not_found {
            transition_and_persist(
                &mut registry,
                &self.registry_path,
                cloid,
                state.clone(),
                observed_at_ms,
            )?;
        }
        Ok(state)
    }

    pub async fn durable_state(
        &self,
        cloid: PlannedCloid,
    ) -> Result<(SubmissionState, u64), SignerError> {
        let registry = self.registry.lock().await;
        Ok((
            registry
                .state(&cloid_string(cloid))
                .cloned()
                .ok_or(SignerError::UnknownCloid)?,
            registry.durable_sequence()?,
        ))
    }

    pub async fn action_states(
        &self,
    ) -> Result<Vec<(PlannedCloid, Decimal, SubmissionState)>, SignerError> {
        let registry = self.registry.lock().await;
        registry
            .entries()
            .map(|(cloid, intent, state)| {
                Ok((decode_planned_cloid(cloid)?, intent.quantity, state.clone()))
            })
            .collect()
    }

    /// Returns the durable state for an identical CLOID replay. Reusing a
    /// CLOID for different immutable order fields is a hard conflict.
    pub async fn idempotent_state(
        &self,
        intent: &AuthorizedExecutionIntent,
    ) -> Result<Option<SubmissionState>, SignerError> {
        let registry = self.registry.lock().await;
        let key = cloid_string(intent.planned_cloid);
        match registry.intent(&key) {
            None => Ok(None),
            Some(existing) if existing == &durable_intent(intent) => {
                Ok(registry.state(&key).cloned())
            }
            Some(_) => Err(SignerError::CloidMismatch),
        }
    }

    pub async fn is_ready(&self) -> bool {
        *self.startup_reconciled.lock().await
            && !self.registry.lock().await.has_authorized_not_submitted()
    }
}

fn durable_intent(intent: &AuthorizedExecutionIntent) -> ApprovedExecutionIntent {
    ApprovedExecutionIntent {
        decision_id: unprefixed_hex(intent.decision_id.to_hex()),
        target_version: intent.target_version.0,
        root_cloid: cloid_string(intent.root_cloid),
        cloid: cloid_string(intent.planned_cloid),
        parent_cloid: intent.parent_cloid.map(cloid_string),
        continuation_generation: intent.continuation_generation,
        asset: intent.asset.clone(),
        asset_index: intent.asset_index,
        is_buy: matches!(intent.side, Side::Buy),
        reduce_only: intent.reduce_only,
        limit_price: intent.limit_price,
        quantity: intent.quantity,
        risk_projection_hash: unprefixed_hex(intent.projected_portfolio_hash.to_hex()),
        risk_policy_hash: unprefixed_hex(intent.risk_policy_hash.to_hex()),
        configuration_hash: unprefixed_hex(intent.configuration_hash.to_hex()),
        release_manifest_hash: hex32(intent.release_manifest_hash),
        decision_reference_price: intent.decision_reference_price,
        decision_timestamp_ms: intent.decision_timestamp_ms,
        expires_at_mono: intent.expires_at,
        canonical_intent_hash: hex32(intent.canonical_hash),
    }
}

fn transition_and_persist(
    registry: &mut SubmissionRegistry,
    path: &Path,
    cloid: PlannedCloid,
    state: SubmissionState,
    recorded_at_ms: u64,
) -> Result<(), SignerError> {
    registry.transition_at(&cloid_string(cloid), state, recorded_at_ms)?;
    registry.persist(path)
}

fn positions_to_notional(
    snapshot: &ExchangePositionSnapshot,
    input: &PortfolioProjectionInput,
) -> Result<std::collections::BTreeMap<String, Decimal>, SignerError> {
    snapshot
        .positions
        .iter()
        .map(|(asset, quantity)| {
            let mark = input
                .market_rules
                .get(asset)
                .ok_or(SignerError::MissingMarketRules)?
                .mark_price;
            Ok((
                asset.clone(),
                quantity
                    .checked_mul(mark)
                    .ok_or(SignerError::InvalidDecimal)?,
            ))
        })
        .collect()
}

fn cloid_string(cloid: PlannedCloid) -> String {
    cloid.to_hex()
}

fn unprefixed_hex(value: String) -> String {
    value.strip_prefix("0x").unwrap_or(&value).to_string()
}

fn hex32(value: [u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_planned_cloid(value: &str) -> Result<PlannedCloid, SignerError> {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    if raw.len() != 32 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SignerError::InvalidIntent);
    }
    let mut bytes = [0u8; 16];
    for (index, pair) in raw.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).map_err(|_| SignerError::InvalidIntent)?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| SignerError::InvalidIntent)?;
    }
    Ok(PlannedCloid(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::{ConfigHash, DecisionId, ProjectionHash, TargetVersion};
    use crate::domain::portfolio_risk::MarketRules;
    use std::collections::BTreeMap;

    fn context_and_intent() -> (PreSigningContext, AuthorizedExecutionIntent) {
        let mut market_rules = BTreeMap::new();
        market_rules.insert(
            "BTC".into(),
            MarketRules {
                mark_price: Decimal::from(100),
                price_tick: Decimal::ONE,
                size_step: Decimal::new(1, 2),
            },
        );
        let mut targets = BTreeMap::new();
        targets.insert("BTC".into(), Decimal::from(20));
        let input = PortfolioProjectionInput {
            current_equity: Decimal::from(100),
            curve_leverage: Decimal::from(8),
            global_risk_scale: Decimal::new(1, 1),
            max_single_asset_equity_pct: Decimal::new(65, 2),
            max_net_equity_pct: Decimal::new(65, 2),
            filled_positions: BTreeMap::new(),
            filled_position_state_complete: true,
            acknowledged_open_orders: Vec::new(),
            open_order_state_complete: true,
            unconstrained_targets: targets,
            market_rules,
        };
        let hash =
            derive_projection_hash(&project_and_validate_portfolio(&input).unwrap()).unwrap();
        let risk = RiskPolicyHash([7; 32]);
        (
            PreSigningContext {
                now_mono: 100,
                deployed_risk_policy_hash: risk,
                projection_input: input.clone(),
                exchange_minimum_notional: Decimal::from(10),
            },
            AuthorizedExecutionIntent {
                schema_version: crate::domain::authorized_intent::AUTHORIZED_INTENT_SCHEMA_VERSION,
                decision_id: DecisionId([1; 32]),
                target_version: TargetVersion(1),
                root_cloid: PlannedCloid([2; 16]),
                planned_cloid: PlannedCloid([2; 16]),
                parent_cloid: None,
                continuation_generation: 0,
                asset: "BTC".into(),
                asset_index: 0,
                side: Side::Buy,
                quantity: Decimal::new(2, 1),
                limit_price: Decimal::from(100),
                reduce_only: false,
                time_in_force: crate::domain::authorized_intent::TimeInForce::Ioc,
                projected_portfolio_hash: hash,
                risk_policy_hash: risk,
                configuration_hash: ConfigHash([8; 32]),
                market_rules_hash: [10; 32],
                dynamic_floor_policy_hash: [11; 32],
                ioc_policy_hash: [12; 32],
                observer_release_hash: [13; 32],
                signer_release_hash: [14; 32],
                release_manifest_hash: [9; 32],
                decision_reference_price: Decimal::from(100),
                expected_committed_before: Decimal::ZERO,
                expected_committed_after: Decimal::from(20),
                decision_timestamp_ms: 90,
                expires_at: 200,
                authorization: PreSigningContext {
                    now_mono: 100,
                    deployed_risk_policy_hash: risk,
                    projection_input: input.clone(),
                    exchange_minimum_notional: Decimal::from(10),
                },
                canonical_hash: [0; 32],
            }
            .seal()
            .unwrap(),
        )
    }

    struct FlatAccount;
    #[derive(Clone)]
    struct RecordingAccount {
        registry_path: PathBuf,
        fill: std::sync::Arc<std::sync::Mutex<Option<crate::signing::transport::UserFill>>>,
    }

    #[async_trait::async_trait]
    impl AuthenticatedExchangeTransport for RecordingAccount {
        async fn submit_ioc(
            &self,
            request: crate::signing::transport::SignedIocRequest,
        ) -> Result<SubmissionResponse, SubmissionTransportError> {
            let registry = SubmissionRegistry::restore(&self.registry_path).unwrap();
            assert!(matches!(
                registry.state(&request.cloid.to_hex()),
                Some(SubmissionState::SubmissionStarted { .. })
            ));
            let mut fill = self.fill.lock().unwrap();
            assert!(fill.is_none(), "restart cannot submit the same order again");
            *fill = Some(crate::signing::transport::UserFill {
                position_before: Some(Decimal::ZERO),
                cloid: Some(request.cloid),
                exchange_order_id: "oid-1".into(),
                trade_id: "tid-1".into(),
                asset: "BTC".into(),
                side: "B".into(),
                price: request.limit_price,
                quantity: request.quantity,
                closed_pnl: Decimal::ZERO,
                fee: Decimal::new(1, 2),
                fee_token: "USDC".into(),
                crossed: true,
                occurred_at_ms: request.nonce,
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            });
            Ok(SubmissionResponse::Filled {
                exchange_order_id: "oid-1".into(),
                filled_quantity: request.quantity,
                average_fill_price: request.limit_price,
            })
        }
        async fn lookup_order(
            &self,
            _: PlannedCloid,
        ) -> Result<OrderObservation, crate::signing::transport::ReconciliationError> {
            panic!("terminal filled order must not be resubmitted or polled as ambiguous")
        }
        async fn read_fills(
            &self,
            cursor: FillCursor,
        ) -> Result<UserFillBatch, crate::signing::transport::ReconciliationError> {
            Ok(UserFillBatch {
                fills: self
                    .fill
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|f| {
                        f.occurred_at_ms >= cursor.start_time_ms
                            && f.occurred_at_ms <= cursor.end_time_ms.unwrap()
                    })
                    .cloned()
                    .collect(),
                next_cursor: FillCursor {
                    start_time_ms: cursor.end_time_ms.unwrap() + 1,
                    ..cursor
                },
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            })
        }
        async fn read_funding(
            &self,
            cursor: FundingCursor,
        ) -> Result<FundingBatch, crate::signing::transport::ReconciliationError> {
            Ok(FundingBatch {
                events: vec![],
                next_cursor: FundingCursor {
                    start_time_ms: cursor.end_time_ms.unwrap() + 1,
                    ..cursor
                },
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            })
        }
        async fn read_positions(
            &self,
        ) -> Result<ExchangePositionSnapshot, crate::signing::transport::ReconciliationError>
        {
            let mut snapshot = FlatAccount.read_positions().await?;
            if let Some(fill) = &*self.fill.lock().unwrap() {
                snapshot.positions.insert("BTC".into(), fill.quantity);
            }
            Ok(snapshot)
        }
        async fn read_equity(
            &self,
        ) -> Result<
            crate::signing::transport::ExchangeEquitySnapshot,
            crate::signing::transport::ReconciliationError,
        > {
            FlatAccount.read_equity().await
        }
        async fn read_open_orders(
            &self,
        ) -> Result<ExchangeOpenOrdersSnapshot, crate::signing::transport::ReconciliationError>
        {
            FlatAccount.read_open_orders().await
        }
    }

    #[tokio::test]
    async fn hip3_ioc_uses_authoritative_asset_id_and_reconciles_per_dex() {
        use crate::signing::transport::{merge_open_order_snapshots, merge_position_snapshots};
        // Signing serializes the authoritative HIP-3 asset ID (dex order
        // ["", "xyz"] => xyz:NVDA = 110_000) while preserving CLOID.
        let (_, mut intent) = context_and_intent();
        intent.asset = "xyz:NVDA".into();
        intent.asset_index = 110_000;
        intent.quantity = Decimal::new(1, 1);
        let mut market_rules = intent.authorization.projection_input.market_rules.clone();
        market_rules.insert(
            "xyz:NVDA".into(),
            crate::domain::portfolio_risk::MarketRules {
                mark_price: Decimal::from(100),
                price_tick: Decimal::ONE,
                size_step: Decimal::new(1, 2),
            },
        );
        intent.authorization.projection_input.market_rules = market_rules.clone();
        intent.authorization.projection_input.filled_positions = BTreeMap::new();
        intent.authorization.projection_input.unconstrained_targets =
            BTreeMap::from([("xyz:NVDA".into(), Decimal::from(10))]);
        intent.authorization.projection_input.current_equity = Decimal::from(100);
        let projection = crate::domain::portfolio_risk::project_and_validate_portfolio(
            &intent.authorization.projection_input,
        )
        .unwrap();
        intent.projected_portfolio_hash =
            crate::domain::decision::derive_projection_hash(&projection).unwrap();
        let intent = intent.seal().unwrap();
        let wallet = ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap();
        // Direct signing check: wire asset == authoritative HIP-3 ID.
        let request = {
            let signer = ProductionSigner::new(
                FlatAccount,
                ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
                SubmissionRegistry::default(),
                &std::env::temp_dir().join(format!(
                    "hip3-sign-{}-{}",
                    std::process::id(),
                    intent.planned_cloid.to_hex()
                )),
                100,
                intent.risk_policy_hash,
                intent.configuration_hash,
                intent.release_manifest_hash,
            )
            .unwrap();
            let _ = signer;
            wallet
                .sign_ioc_for_test(&intent, 101)
                .expect("hip3 signing must serialize")
        };
        assert_eq!(request.asset_index, 110_000);
        assert_eq!(request.cloid, intent.planned_cloid);
        // Per-DEX aggregation keeps GOLD / xyz:GOLD / foo:GOLD distinct.
        let native = ExchangePositionSnapshot {
            positions: BTreeMap::from([("GOLD".into(), Decimal::ONE)]),
            account_equity: Decimal::from(100),
            observed_at_ms: 1,
            source_hash: crate::domain::decision::PayloadHash([0; 32]),
        };
        let xyz = ExchangePositionSnapshot {
            positions: BTreeMap::from([("xyz:GOLD".into(), Decimal::from(2))]),
            account_equity: Decimal::from(10),
            observed_at_ms: 2,
            source_hash: crate::domain::decision::PayloadHash([0; 32]),
        };
        let merged = merge_position_snapshots(vec![native, xyz]).unwrap();
        assert_eq!(merged.positions.len(), 2);
        assert_eq!(merged.account_equity, Decimal::from(110));
        let empty = merge_open_order_snapshots(vec![]);
        assert!(empty.orders.is_empty());
        let _ = wallet.address_hex();
    }

    #[tokio::test]
    async fn authenticated_ioc_persists_registry_before_post_and_fill_once_across_restart() {
        let root = tempfile::tempdir().unwrap();
        let registry_path = root.path().join("live-submission-registry.json");
        let ledger_path = root.path().join("live-trading-state.json");
        let transport = RecordingAccount {
            registry_path: registry_path.clone(),
            fill: Default::default(),
        };
        let (_, intent) = context_and_intent();
        let wallet = || ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap();
        let signer = ProductionSigner::new(
            transport.clone(),
            wallet(),
            SubmissionRegistry::default(),
            &registry_path,
            100,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        let mut ledger =
            crate::domain::live_trading::LiveTradingState::new(Decimal::from(100), 0).unwrap();
        signer
            .reconcile_startup(&mut ledger, &ledger_path, 99, 2, 1_000)
            .await
            .unwrap();
        assert!(matches!(
            signer.submit(intent.clone(), 100).await.unwrap(),
            ProductionSubmissionResult::Filled { .. }
        ));
        signer
            .reconcile_startup(&mut ledger, &ledger_path, 101, 2, 1_000)
            .await
            .unwrap();
        assert_eq!(ledger.position("BTC"), intent.quantity);
        drop(signer);
        let restarted = ProductionSigner::new(
            transport,
            wallet(),
            SubmissionRegistry::restore(&registry_path).unwrap(),
            &registry_path,
            102,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        let mut ledger = crate::domain::live_trading::LiveTradingState::load(&ledger_path).unwrap();
        restarted
            .reconcile_startup(&mut ledger, &ledger_path, 102, 2, 1_000)
            .await
            .unwrap();
        assert_eq!(ledger.verified_fills().len(), 1);
        assert_eq!(ledger.position("BTC"), intent.quantity);
    }

    #[async_trait::async_trait]
    impl AuthenticatedExchangeTransport for FlatAccount {
        async fn submit_ioc(
            &self,
            _: crate::signing::transport::SignedIocRequest,
        ) -> Result<SubmissionResponse, SubmissionTransportError> {
            panic!("NO_ACTION must never sign or submit")
        }
        async fn lookup_order(
            &self,
            _: PlannedCloid,
        ) -> Result<OrderObservation, crate::signing::transport::ReconciliationError> {
            panic!("no order exists")
        }
        async fn read_fills(
            &self,
            _: FillCursor,
        ) -> Result<UserFillBatch, crate::signing::transport::ReconciliationError> {
            unreachable!()
        }
        async fn read_funding(
            &self,
            _: FundingCursor,
        ) -> Result<FundingBatch, crate::signing::transport::ReconciliationError> {
            unreachable!()
        }
        async fn read_positions(
            &self,
        ) -> Result<ExchangePositionSnapshot, crate::signing::transport::ReconciliationError>
        {
            Ok(ExchangePositionSnapshot {
                positions: BTreeMap::new(),
                account_equity: Decimal::from(100),
                observed_at_ms: 100,
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            })
        }
        async fn read_equity(
            &self,
        ) -> Result<
            crate::signing::transport::ExchangeEquitySnapshot,
            crate::signing::transport::ReconciliationError,
        > {
            Ok(crate::signing::transport::ExchangeEquitySnapshot {
                equity: Decimal::from(100),
                observed_at_ms: 100,
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            })
        }
        async fn read_open_orders(
            &self,
        ) -> Result<ExchangeOpenOrdersSnapshot, crate::signing::transport::ReconciliationError>
        {
            Ok(ExchangeOpenOrdersSnapshot {
                orders: vec![],
                source_hash: crate::domain::decision::PayloadHash([0; 32]),
            })
        }
    }

    #[tokio::test]
    async fn recovery_blocks_all_increases_before_nonce_or_submission() {
        let (_, mut intent) = context_and_intent();
        let path =
            std::env::temp_dir().join(format!("risk-only-never-written-{}", std::process::id()));
        let signer = ProductionSigner::new(
            FlatAccount,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            SubmissionRegistry::default(),
            &path,
            100,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        for asset in ["BTC", "HYPE", "ETH"] {
            intent.asset = asset.into();
            assert_eq!(
                signer.submit(intent.clone().seal().unwrap(), 100).await,
                Err(SignerError::StartupNotReconciled)
            );
        }
        assert_eq!(signer.registry.lock().await.next_nonce_floor().unwrap(), 0);
        assert!(!path.exists());
    }

    #[test]
    fn external_no_cloid_reduce_only_order_is_allowed_only_when_protective() {
        let registry = SubmissionRegistry::default();
        let snapshot_hash = crate::domain::decision::PayloadHash([0; 32]);
        let protective = ExchangeOpenOrder {
            cloid: None,
            exchange_order_id: "541191621726".into(),
            asset: "AERO".into(),
            is_buy: true,
            limit_price: Decimal::from(1),
            original_quantity: Decimal::from(300),
            remaining_quantity: Decimal::from(300),
            reduce_only: true,
            order_type: "Limit".into(),
            is_trigger: false,
            is_position_tpsl: false,
        };
        let orders = ExchangeOpenOrdersSnapshot {
            orders: vec![protective.clone()],
            source_hash: snapshot_hash,
        };
        let short_position = ExchangePositionSnapshot {
            positions: BTreeMap::from([("AERO".into(), Decimal::from(-300))]),
            account_equity: Decimal::from(100),
            observed_at_ms: 1,
            source_hash: snapshot_hash,
        };
        assert_eq!(
            validate_exchange_open_orders(&orders, &registry, &short_position),
            Ok(())
        );

        let flat_position = ExchangePositionSnapshot {
            positions: BTreeMap::new(),
            account_equity: Decimal::from(100),
            observed_at_ms: 1,
            source_hash: snapshot_hash,
        };
        assert!(matches!(
            validate_exchange_open_orders(&orders, &registry, &flat_position),
            Err(SignerError::ExchangeTruthMismatch(reason))
                if reason.contains("has no CLOID")
        ));

        let mut increasing = protective;
        increasing.is_buy = false;
        let increasing_orders = ExchangeOpenOrdersSnapshot {
            orders: vec![increasing],
            source_hash: snapshot_hash,
        };
        assert!(matches!(
            validate_exchange_open_orders(&increasing_orders, &registry, &short_position),
            Err(SignerError::ExchangeTruthMismatch(reason))
                if reason.contains("has no CLOID")
        ));
    }

    #[test]
    fn recovery_reduction_uses_exchange_side_size_and_reserved_capacity() {
        let (context, mut intent) = context_and_intent();
        intent.reduce_only = true;
        let mut positions = ExchangePositionSnapshot {
            positions: BTreeMap::new(),
            account_equity: Decimal::from(100),
            observed_at_ms: 100,
            source_hash: crate::domain::decision::PayloadHash([0; 32]),
        };
        let mut orders = ExchangeOpenOrdersSnapshot {
            orders: vec![],
            source_hash: crate::domain::decision::PayloadHash([0; 32]),
        };
        for (position, reducing_side) in [
            (Decimal::new(25, 2), Side::Sell),
            (Decimal::new(-25, 2), Side::Buy),
        ] {
            positions.positions.insert("BTC".into(), position);
            intent.side = reducing_side;
            intent.quantity = Decimal::new(25, 2);
            assert_eq!(
                validate_recovery_reduce_only(&intent, &positions, &context, &orders),
                Ok(())
            );
            intent.quantity = Decimal::new(26, 2);
            assert_eq!(
                validate_recovery_reduce_only(&intent, &positions, &context, &orders),
                Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction)
            );
            intent.quantity = Decimal::new(25, 2);
            intent.side = if reducing_side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            assert_eq!(
                validate_recovery_reduce_only(&intent, &positions, &context, &orders),
                Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction)
            );
        }
        intent.side = Side::Buy;
        orders
            .orders
            .push(crate::signing::transport::ExchangeOpenOrder {
                cloid: None,
                exchange_order_id: "resting".into(),
                asset: "BTC".into(),
                is_buy: true,
                limit_price: Decimal::from(100),
                original_quantity: Decimal::new(10, 2),
                remaining_quantity: Decimal::new(10, 2),
                reduce_only: true,
                order_type: "Limit".into(),
                is_trigger: false,
                is_position_tpsl: false,
            });
        assert_eq!(
            validate_recovery_reduce_only(&intent, &positions, &context, &orders),
            Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction)
        );
        intent.quantity = Decimal::new(15, 2);
        assert_eq!(
            validate_recovery_reduce_only(&intent, &positions, &context, &orders),
            Ok(())
        );
        positions.positions.clear();
        assert_eq!(
            validate_recovery_reduce_only(&intent, &positions, &context, &orders),
            Err(AuthorizationFailure::CurrentTargetNoLongerRequiresAction)
        );
    }

    #[tokio::test]
    async fn no_action_is_durable_idempotent_unsigned_and_leaves_next_intent_actionable() {
        let (_, mut intent) = context_and_intent();
        intent.quantity = Decimal::new(21, 2);
        intent = intent.seal().unwrap();
        let path =
            std::env::temp_dir().join(format!("no-action-registry-{}.json", std::process::id()));
        let signer = ProductionSigner::new(
            FlatAccount,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            SubmissionRegistry::default(),
            &path,
            100,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        *signer.startup_reconciled.lock().await = true;
        for _ in 0..2 {
            assert_eq!(
                signer.submit(intent.clone(), 100).await.unwrap(),
                ProductionSubmissionResult::NoAction
            );
        }
        let restored = SubmissionRegistry::restore(&path).unwrap();
        assert_eq!(
            restored.state(&intent.planned_cloid.to_hex()),
            Some(&SubmissionState::NoAction)
        );
        assert!(restored.permits_increase("BTC"));
        assert_eq!(restored.next_nonce_floor().unwrap(), 0);
        let (_, mut next) = context_and_intent();
        next.planned_cloid = PlannedCloid([3; 16]);
        next.root_cloid = next.planned_cloid;
        signer.authorize(next.seal().unwrap(), 100).await.unwrap();
        intent.configuration_hash = ConfigHash([99; 32]);
        assert!(matches!(
            signer.authorize(intent.seal().unwrap(), 100).await,
            Err(SignerError::Authorization(_))
        ));
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn stale_projection_is_durable_no_action_before_nonce_or_signing() {
        let (_, mut intent) = context_and_intent();
        intent.projected_portfolio_hash = ProjectionHash([9; 32]);
        let intent = intent.seal().unwrap();
        let path = std::env::temp_dir().join(format!(
            "stale-projection-registry-{}.json",
            std::process::id()
        ));
        let signer = ProductionSigner::new(
            FlatAccount,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            SubmissionRegistry::default(),
            &path,
            100,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        *signer.startup_reconciled.lock().await = true;
        assert_eq!(
            signer.submit(intent.clone(), 100).await.unwrap(),
            ProductionSubmissionResult::NoAction
        );
        let restored = SubmissionRegistry::restore(&path).unwrap();
        assert_eq!(
            restored.state(&intent.planned_cloid.to_hex()),
            Some(&SubmissionState::NoAction)
        );
        assert_eq!(restored.next_nonce_floor().unwrap(), 0);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn authorization_reuses_authoritative_projection_and_rejects_stale_hash() {
        let (context, mut intent) = context_and_intent();
        let registry = SubmissionRegistry::default();
        validate_authorized_intent(&intent, &context, &registry).unwrap();
        intent.projected_portfolio_hash = ProjectionHash([9; 32]);
        intent = intent.seal().unwrap();
        assert_eq!(
            validate_authorized_intent(&intent, &context, &registry),
            Err(AuthorizationFailure::ProjectionChanged)
        );
    }

    #[test]
    fn expired_or_below_minimum_intent_fails_before_registration() {
        let (mut context, mut intent) = context_and_intent();
        let registry = SubmissionRegistry::default();
        context.now_mono = 201;
        assert_eq!(
            validate_authorized_intent(&intent, &context, &registry),
            Err(AuthorizationFailure::Expired)
        );
        context.now_mono = 100;
        intent.limit_price = Decimal::from(10);
        intent = intent.seal().unwrap();
        assert_eq!(
            validate_authorized_intent(&intent, &context, &registry),
            Err(AuthorizationFailure::BelowExchangeMinimum)
        );
    }

    /// DEX-scoped reconciliation must never downgrade UNKNOWN to EMPTY via a
    /// native fallback. A failing `xyz` read is UNKNOWN even when native is
    /// healthy (empty or populated).
    #[derive(Clone)]
    struct DexScopedMock {
        orders: std::collections::BTreeMap<String, Result<Vec<ExchangeOpenOrder>, String>>,
        positions: std::collections::BTreeMap<String, Result<BTreeMap<String, Decimal>, String>>,
    }

    fn mock_order(asset: &str) -> ExchangeOpenOrder {
        ExchangeOpenOrder {
            cloid: None,
            exchange_order_id: "1".into(),
            asset: asset.into(),
            is_buy: true,
            limit_price: Decimal::from(100),
            original_quantity: Decimal::ONE,
            remaining_quantity: Decimal::ONE,
            reduce_only: false,
            order_type: "Limit".into(),
            is_trigger: false,
            is_position_tpsl: false,
        }
    }

    #[async_trait::async_trait]
    impl AuthenticatedExchangeTransport for DexScopedMock {
        async fn submit_ioc(
            &self,
            _: crate::signing::transport::SignedIocRequest,
        ) -> Result<SubmissionResponse, SubmissionTransportError> {
            unreachable!()
        }
        async fn lookup_order(
            &self,
            _: PlannedCloid,
        ) -> Result<OrderObservation, crate::signing::transport::ReconciliationError> {
            unreachable!()
        }
        async fn read_fills(
            &self,
            _: FillCursor,
        ) -> Result<UserFillBatch, crate::signing::transport::ReconciliationError> {
            unreachable!()
        }
        async fn read_positions(
            &self,
        ) -> Result<ExchangePositionSnapshot, crate::signing::transport::ReconciliationError>
        {
            self.read_positions_for_dex("").await
        }
        async fn read_positions_for_dex(
            &self,
            dex: &str,
        ) -> Result<ExchangePositionSnapshot, crate::signing::transport::ReconciliationError>
        {
            match self.positions.get(dex) {
                Some(Ok(positions)) => Ok(ExchangePositionSnapshot {
                    positions: positions.clone(),
                    account_equity: Decimal::from(100),
                    observed_at_ms: 1,
                    source_hash: crate::domain::decision::PayloadHash([0; 32]),
                }),
                Some(Err(_)) => Err(crate::signing::transport::ReconciliationError::Transport(
                    format!("dex {dex} positions UNKNOWN"),
                )),
                None => Ok(ExchangePositionSnapshot {
                    positions: BTreeMap::new(),
                    account_equity: Decimal::from(100),
                    observed_at_ms: 1,
                    source_hash: crate::domain::decision::PayloadHash([0; 32]),
                }),
            }
        }
        async fn read_equity(
            &self,
        ) -> Result<
            crate::signing::transport::ExchangeEquitySnapshot,
            crate::signing::transport::ReconciliationError,
        > {
            unreachable!()
        }
        async fn read_open_orders(
            &self,
        ) -> Result<ExchangeOpenOrdersSnapshot, crate::signing::transport::ReconciliationError>
        {
            self.read_open_orders_for_dex("").await
        }
        async fn read_open_orders_for_dex(
            &self,
            dex: &str,
        ) -> Result<ExchangeOpenOrdersSnapshot, crate::signing::transport::ReconciliationError>
        {
            match self.orders.get(dex) {
                Some(Ok(orders)) => Ok(ExchangeOpenOrdersSnapshot {
                    orders: orders.clone(),
                    source_hash: crate::domain::decision::PayloadHash([0; 32]),
                }),
                Some(Err(_)) => Err(crate::signing::transport::ReconciliationError::Transport(
                    format!("dex {dex} orders UNKNOWN"),
                )),
                None => Ok(ExchangeOpenOrdersSnapshot {
                    orders: Vec::new(),
                    source_hash: crate::domain::decision::PayloadHash([0; 32]),
                }),
            }
        }
        async fn read_funding(
            &self,
            _: FundingCursor,
        ) -> Result<FundingBatch, crate::signing::transport::ReconciliationError> {
            unreachable!()
        }
    }

    async fn aggregated_orders(
        mock: DexScopedMock,
        active: BTreeSet<String>,
    ) -> Result<ExchangeOpenOrdersSnapshot, SignerError> {
        let path = std::env::temp_dir().join(format!(
            "dex-unknown-{}-{}.json",
            std::process::id(),
            active.len()
        ));
        let (_, intent) = context_and_intent();
        let signer = ProductionSigner::new(
            mock,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            SubmissionRegistry::default(),
            &path,
            100,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        signer.set_active_dexes(active).await;
        signer.read_aggregated_open_orders().await
    }

    async fn aggregated_positions(
        mock: DexScopedMock,
        active: BTreeSet<String>,
    ) -> Result<ExchangePositionSnapshot, SignerError> {
        let path = std::env::temp_dir().join(format!(
            "dex-pos-unknown-{}-{}.json",
            std::process::id(),
            active.len()
        ));
        let (_, intent) = context_and_intent();
        let signer = ProductionSigner::new(
            mock,
            ApiWalletSecret::from_private_key(&format!("{:064x}", 1)).unwrap(),
            SubmissionRegistry::default(),
            &path,
            100,
            intent.risk_policy_hash,
            intent.configuration_hash,
            intent.release_manifest_hash,
        )
        .unwrap();
        signer.set_active_dexes(active).await;
        signer.read_aggregated_positions().await
    }

    #[tokio::test]
    async fn dex_scoped_reconciliation_never_downgrades_unknown_to_empty() {
        // DEX success: xyz order visible.
        let mock = DexScopedMock {
            orders: BTreeMap::from([("xyz".into(), Ok(vec![mock_order("xyz:NVDA")]))]),
            positions: BTreeMap::new(),
        };
        let snapshot = aggregated_orders(mock, BTreeSet::from(["xyz".into()]))
            .await
            .unwrap();
        assert_eq!(snapshot.orders.len(), 1);
        assert_eq!(snapshot.orders[0].asset, "xyz:NVDA");

        // DEX empty success: known-empty xyz state.
        let mock = DexScopedMock {
            orders: BTreeMap::from([("xyz".into(), Ok(Vec::new()))]),
            positions: BTreeMap::new(),
        };
        let snapshot = aggregated_orders(mock, BTreeSet::from(["xyz".into()]))
            .await
            .unwrap();
        assert!(snapshot.orders.is_empty());

        // DEX failure + native empty => UNKNOWN (not xyz empty).
        let mock = DexScopedMock {
            orders: BTreeMap::from([
                ("xyz".into(), Err("boom".into())),
                ("".into(), Ok(Vec::new())),
            ]),
            positions: BTreeMap::new(),
        };
        assert!(aggregated_orders(mock, BTreeSet::from(["xyz".into()]))
            .await
            .is_err());

        // DEX failure + native populated => UNKNOWN (never xyz empty, never
        // alias the BTC order into xyz).
        let mock = DexScopedMock {
            orders: BTreeMap::from([
                ("xyz".into(), Err("boom".into())),
                ("".into(), Ok(vec![mock_order("BTC")])),
            ]),
            positions: BTreeMap::new(),
        };
        assert!(aggregated_orders(mock, BTreeSet::from(["xyz".into()]))
            .await
            .is_err());

        // Clearinghouse failure: xyz UNKNOWN even when native healthy.
        let mock = DexScopedMock {
            orders: BTreeMap::new(),
            positions: BTreeMap::from([
                ("xyz".into(), Err("boom".into())),
                (
                    "".into(),
                    Ok(BTreeMap::from([("BTC".into(), Decimal::ONE)])),
                ),
            ]),
        };
        assert!(aggregated_positions(mock, BTreeSet::from(["xyz".into()]))
            .await
            .is_err());
    }
}
