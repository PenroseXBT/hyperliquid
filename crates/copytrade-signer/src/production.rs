//! Authenticated production boundary. Strategy and sizing remain in copytrade-core.

use crate::reconciliation::{apply_authenticated_batches, AppliedExchangeBatch};
use crate::transport::{
    AuthenticatedExchangeTransport, ExchangePositionSnapshot, FillCursor, FundingBatch,
    FundingCursor, OrderObservation, SubmissionResponse, SubmissionTransportError, UserFillBatch,
};
use crate::{
    ApiWalletSecret, ApprovedExecutionIntent, NonceAllocator, SignerError, SubmissionRegistry,
    SubmissionState,
};
pub use copytrade_core::authorized_intent::{AuthorizedExecutionIntent, PreSigningContext};
use copytrade_core::decision::{
    derive_projection_hash, ConfigHash, PlannedCloid, RiskPolicyHash, Side, TargetVersion,
};
use copytrade_core::execution_floor::validates_rounded_order;
use copytrade_core::ipc::IntentIdentity;
use copytrade_core::portfolio_risk::{
    project_and_validate_portfolio, OpenOrderExposure, OpenOrderLifecycle, OrderSide,
    PortfolioProjectionInput,
};
use rust_decimal::Decimal;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductionSubmissionResult {
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
    deployed_risk_policy_hash: RiskPolicyHash,
    deployed_configuration_hash: ConfigHash,
    release_manifest_hash: [u8; 32],
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
            deployed_risk_policy_hash,
            deployed_configuration_hash,
            release_manifest_hash,
        })
    }

    pub async fn reconcile_startup(
        &self,
        live_state: &mut copytrade_core::live_trading::LiveTradingState,
        ledger_path: &Path,
        now_ms: u64,
        required_not_found_confirmations: u32,
        minimum_confirmation_interval_ms: u64,
    ) -> Result<AppliedExchangeBatch, SignerError> {
        *self.startup_reconciled.lock().await = false;
        // Close the event cursor first, then compare the applied ledger with an
        // account-state snapshot taken after that boundary. Concurrent fills
        // fail the exact position comparison and force another reconciliation.
        let fills = self
            .read_all_fills(live_state.fill_cursor_ms(), now_ms)
            .await?;
        let funding = self
            .read_all_funding(live_state.funding_cursor_ms(), now_ms)
            .await?;
        let positions = self
            .transport
            .read_positions()
            .await
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        let equity = self
            .transport
            .read_equity()
            .await
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        if equity.equity != positions.account_equity {
            return Err(SignerError::Reconciliation(
                "position and equity snapshots disagree".into(),
            ));
        }

        let applied = {
            let registry = self.registry.lock().await;
            let applied = apply_authenticated_batches(
                live_state,
                &registry,
                fills,
                funding,
                equity.equity,
                ledger_path,
            )?;
            applied
        };
        live_state
            .reconcile_positions(&positions.positions)
            .map_err(|error| SignerError::Ledger(error.to_string()))?;

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
        *self.startup_reconciled.lock().await = true;
        Ok(applied)
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
                source_hash: copytrade_core::decision::PayloadHash([0; 32]),
            });
        }
        let mut cursor = FillCursor {
            start_time_ms,
            end_time_ms: Some(end_time_ms),
        };
        let mut all = Vec::new();
        let mut last_hash: copytrade_core::decision::PayloadHash;
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
                source_hash: copytrade_core::decision::PayloadHash([0; 32]),
            });
        }
        let mut cursor = FundingCursor {
            start_time_ms,
            end_time_ms: Some(end_time_ms),
        };
        let mut all = Vec::new();
        let mut last_hash: copytrade_core::decision::PayloadHash;
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
        self.authorize(intent.clone(), unix_millis).await?;
        self.submit_authorized(intent, unix_millis).await
    }

    /// Revalidates current exchange state and fsyncs `Authorized` before the
    /// IPC server is allowed to acknowledge durable handoff to the observer.
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
        if !*self.startup_reconciled.lock().await {
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
            if !intent.reduce_only
                && registry.has_authorized_not_submitted()
                && registry.state(&key) != Some(&SubmissionState::AuthorizedNotSubmitted)
            {
                return Err(SignerError::StartupNotReconciled);
            }
        }

        // Rebuild committed exposure from current exchange state immediately
        // before authorization. The core projector remains the sole risk formula.
        let positions = self
            .transport
            .read_positions()
            .await
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        let equity = self
            .transport
            .read_equity()
            .await
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        if equity.equity != positions.account_equity {
            return Err(SignerError::Reconciliation(
                "position and equity snapshots disagree".into(),
            ));
        }
        context.projection_input.current_equity = equity.equity;
        context.projection_input.filled_positions =
            positions_to_notional(&positions, &context.projection_input)?;
        context.projection_input.acknowledged_open_orders.clear();
        context.projection_input.filled_position_state_complete = true;
        context.projection_input.open_order_state_complete = true;

        let mut registry = self.registry.lock().await;
        context.projection_input.acknowledged_open_orders.extend(
            registry
                .entries()
                .filter_map(|(_, durable, state)| {
                    matches!(state, SubmissionState::UnknownResult { .. }).then(|| {
                        let side = if durable.is_buy {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        };
                        durable
                            .quantity
                            .checked_mul(durable.limit_price)
                            .map(|notional| OpenOrderExposure {
                                asset: durable.asset.clone(),
                                side: Some(side),
                                notional: Some(notional),
                                lifecycle: OpenOrderLifecycle::SubmissionUnknown,
                            })
                    })
                })
                .flatten(),
        );
        validate_authorized_intent(&intent, &context, &registry)
            .map_err(|error| SignerError::Authorization(error.to_string()))?;
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
        if !*self.startup_reconciled.lock().await {
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

    pub async fn intent_identity(
        &self,
        cloid: PlannedCloid,
    ) -> Result<IntentIdentity, SignerError> {
        let registry = self.registry.lock().await;
        let intent = registry
            .intent(&cloid_string(cloid))
            .ok_or(SignerError::UnknownCloid)?;
        Ok(IntentIdentity {
            planned_cloid: cloid,
            target_version: TargetVersion(intent.target_version),
            canonical_intent_hash: decode_hex32(&intent.canonical_intent_hash)?,
        })
    }

    pub async fn is_ready(&self) -> bool {
        *self.startup_reconciled.lock().await
            && !self.registry.lock().await.has_authorized_not_submitted()
    }

    pub async fn durable_transitions_after(
        &self,
        sequence: u64,
    ) -> Result<Vec<(u64, crate::SubmissionTransitionRecord)>, SignerError> {
        let registry = self.registry.lock().await;
        let skip: usize = sequence
            .try_into()
            .map_err(|_| SignerError::RegistryCapacityExhausted)?;
        registry
            .history()
            .iter()
            .enumerate()
            .skip(skip)
            .map(|(index, record)| {
                Ok((
                    (index + 1)
                        .try_into()
                        .map_err(|_| SignerError::RegistryCapacityExhausted)?,
                    record.clone(),
                ))
            })
            .collect()
    }
}

fn decode_hex32(value: &str) -> Result<[u8; 32], SignerError> {
    if value.len() != 64 {
        return Err(SignerError::Decode("expected 32-byte hex value".into()));
    }
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|error| SignerError::Decode(error.to_string()))?;
    }
    Ok(output)
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
    use copytrade_core::decision::{ConfigHash, DecisionId, ProjectionHash, TargetVersion};
    use copytrade_core::portfolio_risk::MarketRules;
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
                schema_version: copytrade_core::authorized_intent::AUTHORIZED_INTENT_SCHEMA_VERSION,
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
                time_in_force: copytrade_core::authorized_intent::TimeInForce::Ioc,
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
}
