use crate::domain::decision::{
    derive_planned_cloid, EngineInstanceId, MonotonicTimestamp, PlannedAction, PlannedCloid,
    PlannedCloidInput,
};
use crate::domain::portfolio_risk::Asset;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExchangeOrderRef(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectionReason(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleState {
    DecisionConstructed {
        decision_id: crate::domain::decision::DecisionId,
    },
    RiskValidated {
        decision_id: crate::domain::decision::DecisionId,
    },
    ActionPlanned {
        action: PlannedAction,
    },
    AwaitingFutureSubmission {
        action: PlannedAction,
    },
    SubmissionUnknown {
        action: PlannedAction,
        first_unknown_at: MonotonicTimestamp,
        reconciliation_attempts: u32,
        not_found_confirmations: Vec<MonotonicTimestamp>,
    },
    ReconciledNotFound {
        action: PlannedAction,
    },
    ReconciledOpen {
        action: PlannedAction,
        exchange_order_ref: ExchangeOrderRef,
    },
    ReconciledPartiallyFilled {
        action: PlannedAction,
        exchange_order_ref: ExchangeOrderRef,
        filled_quantity: Decimal,
    },
    ReconciledFilled {
        action: PlannedAction,
        exchange_order_ref: ExchangeOrderRef,
        filled_quantity: Decimal,
    },
    ReconciledCancelled {
        action: PlannedAction,
        exchange_order_ref: Option<ExchangeOrderRef>,
    },
    ReconciledRejected {
        action: PlannedAction,
        reason: RejectionReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    RiskValidated {
        decision_id: crate::domain::decision::DecisionId,
    },
    ActionPlanned {
        action: PlannedAction,
    },
    AwaitFutureSubmission,
    FutureSubmissionUnknown {
        observed_at: MonotonicTimestamp,
    },
    ReconciliationObserved(ReconciliationObservation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationObservation {
    NotFound {
        observed_at: MonotonicTimestamp,
    },
    Open {
        order_ref: ExchangeOrderRef,
        original_quantity: Decimal,
        remaining_quantity: Decimal,
        observed_at: MonotonicTimestamp,
    },
    Filled {
        order_ref: ExchangeOrderRef,
        filled_quantity: Decimal,
        observed_at: MonotonicTimestamp,
    },
    Cancelled {
        order_ref: ExchangeOrderRef,
        filled_quantity: Decimal,
        observed_at: MonotonicTimestamp,
    },
    Rejected {
        reason: RejectionReason,
        observed_at: MonotonicTimestamp,
    },
}

impl ReconciliationObservation {
    fn observed_at(&self) -> MonotonicTimestamp {
        match self {
            Self::NotFound { observed_at }
            | Self::Open { observed_at, .. }
            | Self::Filled { observed_at, .. }
            | Self::Cancelled { observed_at, .. }
            | Self::Rejected { observed_at, .. } => *observed_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationPolicy {
    pub required_not_found_confirmations: u32,
    pub minimum_confirmation_interval_ms: u64,
    pub maximum_attempts: u32,
    pub maximum_unknown_lifetime_ms: u64,
}

impl ReconciliationPolicy {
    pub fn validate(self) -> Result<Self, TransitionError> {
        if self.required_not_found_confirmations == 0
            || self.minimum_confirmation_interval_ms == 0
            || self.maximum_attempts < self.required_not_found_confirmations
            || self.maximum_unknown_lifetime_ms == 0
        {
            return Err(TransitionError::InvalidPolicy);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionError {
    InvalidTransition,
    IdentityMismatch,
    InvalidObservation,
    ReconciliationAttemptLimit,
    UnknownLifetimeExceeded,
    ReconciliationConflict,
    InvalidPolicy,
    RetryGenerationNotAuthorized,
    RetryGenerationOverflow,
}

impl Display for TransitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for TransitionError {}

pub fn apply_lifecycle_event(
    current: &LifecycleState,
    event: LifecycleEvent,
    policy: ReconciliationPolicy,
) -> Result<LifecycleState, TransitionError> {
    let policy = policy.validate()?;
    match (current, event) {
        (
            LifecycleState::DecisionConstructed { decision_id },
            LifecycleEvent::RiskValidated {
                decision_id: event_id,
            },
        ) if *decision_id == event_id => Ok(LifecycleState::RiskValidated {
            decision_id: *decision_id,
        }),
        (
            LifecycleState::RiskValidated { decision_id },
            LifecycleEvent::RiskValidated {
                decision_id: event_id,
            },
        ) if *decision_id == event_id => Ok(current.clone()),
        (
            LifecycleState::RiskValidated { decision_id },
            LifecycleEvent::ActionPlanned { action },
        ) if *decision_id == action.decision_id => Ok(LifecycleState::ActionPlanned { action }),
        (
            LifecycleState::ActionPlanned {
                action: current_action,
            },
            LifecycleEvent::ActionPlanned { action },
        ) if *current_action == action => Ok(current.clone()),
        (LifecycleState::ActionPlanned { action }, LifecycleEvent::AwaitFutureSubmission) => {
            Ok(LifecycleState::AwaitingFutureSubmission {
                action: action.clone(),
            })
        }
        (
            LifecycleState::AwaitingFutureSubmission { .. },
            LifecycleEvent::AwaitFutureSubmission,
        ) => Ok(current.clone()),
        (
            LifecycleState::AwaitingFutureSubmission { action },
            LifecycleEvent::FutureSubmissionUnknown { observed_at },
        ) => Ok(LifecycleState::SubmissionUnknown {
            action: action.clone(),
            first_unknown_at: observed_at,
            reconciliation_attempts: 0,
            not_found_confirmations: Vec::new(),
        }),
        (
            LifecycleState::SubmissionUnknown {
                first_unknown_at, ..
            },
            LifecycleEvent::FutureSubmissionUnknown { observed_at },
        ) if *first_unknown_at == observed_at => Ok(current.clone()),
        (state, LifecycleEvent::ReconciliationObserved(observation)) => {
            apply_reconciliation_observation(state, observation, policy)
        }
        _ => Err(TransitionError::InvalidTransition),
    }
}

fn apply_reconciliation_observation(
    current: &LifecycleState,
    observation: ReconciliationObservation,
    policy: ReconciliationPolicy,
) -> Result<LifecycleState, TransitionError> {
    match current {
        LifecycleState::SubmissionUnknown {
            action,
            first_unknown_at,
            reconciliation_attempts,
            not_found_confirmations,
        } => {
            if observation.observed_at() < *first_unknown_at {
                return Err(TransitionError::InvalidObservation);
            }
            match observation {
                ReconciliationObservation::NotFound { observed_at } => {
                    if observed_at.saturating_sub(*first_unknown_at)
                        > policy.maximum_unknown_lifetime_ms
                    {
                        return Err(TransitionError::UnknownLifetimeExceeded);
                    }
                    if not_found_confirmations.last() == Some(&observed_at) {
                        return Ok(current.clone());
                    }
                    if *reconciliation_attempts >= policy.maximum_attempts {
                        return Err(TransitionError::ReconciliationAttemptLimit);
                    }
                    if let Some(previous) = not_found_confirmations.last() {
                        if observed_at.saturating_sub(*previous)
                            < policy.minimum_confirmation_interval_ms
                        {
                            return Err(TransitionError::InvalidObservation);
                        }
                    }
                    let mut confirmations = not_found_confirmations.clone();
                    confirmations.push(observed_at);
                    if confirmations.len() as u32 >= policy.required_not_found_confirmations {
                        Ok(LifecycleState::ReconciledNotFound {
                            action: action.clone(),
                        })
                    } else {
                        Ok(LifecycleState::SubmissionUnknown {
                            action: action.clone(),
                            first_unknown_at: *first_unknown_at,
                            reconciliation_attempts: reconciliation_attempts + 1,
                            not_found_confirmations: confirmations,
                        })
                    }
                }
                positive => positive_state(action.clone(), positive),
            }
        }
        LifecycleState::ReconciledNotFound { action } => match observation {
            ReconciliationObservation::NotFound { .. } => Ok(current.clone()),
            positive => positive_state(action.clone(), positive),
        },
        LifecycleState::ReconciledOpen {
            action,
            exchange_order_ref,
        } => reconcile_known_positive(current, action, Some(exchange_order_ref), observation),
        LifecycleState::ReconciledPartiallyFilled {
            action,
            exchange_order_ref,
            ..
        }
        | LifecycleState::ReconciledFilled {
            action,
            exchange_order_ref,
            ..
        } => reconcile_known_positive(current, action, Some(exchange_order_ref), observation),
        LifecycleState::ReconciledCancelled {
            action,
            exchange_order_ref,
        } => reconcile_known_positive(current, action, exchange_order_ref.as_ref(), observation),
        LifecycleState::ReconciledRejected { action, reason } => match observation {
            ReconciliationObservation::Rejected {
                reason: observed, ..
            } if *reason == observed => Ok(current.clone()),
            ReconciliationObservation::NotFound { .. } => Ok(current.clone()),
            _ => {
                let _ = action;
                Err(TransitionError::ReconciliationConflict)
            }
        },
        _ => Err(TransitionError::InvalidTransition),
    }
}

fn positive_state(
    action: PlannedAction,
    observation: ReconciliationObservation,
) -> Result<LifecycleState, TransitionError> {
    match observation {
        ReconciliationObservation::Open {
            order_ref,
            original_quantity,
            remaining_quantity,
            ..
        } => {
            if original_quantity <= Decimal::ZERO
                || remaining_quantity < Decimal::ZERO
                || remaining_quantity > original_quantity
            {
                return Err(TransitionError::InvalidObservation);
            }
            let filled = original_quantity
                .checked_sub(remaining_quantity)
                .ok_or(TransitionError::InvalidObservation)?;
            if filled.is_zero() {
                Ok(LifecycleState::ReconciledOpen {
                    action,
                    exchange_order_ref: order_ref,
                })
            } else {
                Ok(LifecycleState::ReconciledPartiallyFilled {
                    action,
                    exchange_order_ref: order_ref,
                    filled_quantity: filled,
                })
            }
        }
        ReconciliationObservation::Filled {
            order_ref,
            filled_quantity,
            ..
        } => {
            validate_filled(filled_quantity)?;
            Ok(LifecycleState::ReconciledFilled {
                action,
                exchange_order_ref: order_ref,
                filled_quantity,
            })
        }
        ReconciliationObservation::Cancelled {
            order_ref,
            filled_quantity,
            ..
        } => {
            if filled_quantity < Decimal::ZERO {
                return Err(TransitionError::InvalidObservation);
            }
            Ok(LifecycleState::ReconciledCancelled {
                action,
                exchange_order_ref: Some(order_ref),
            })
        }
        ReconciliationObservation::Rejected { reason, .. } => {
            Ok(LifecycleState::ReconciledRejected { action, reason })
        }
        ReconciliationObservation::NotFound { .. } => Err(TransitionError::InvalidObservation),
    }
}

fn reconcile_known_positive(
    current: &LifecycleState,
    action: &PlannedAction,
    known_ref: Option<&ExchangeOrderRef>,
    observation: ReconciliationObservation,
) -> Result<LifecycleState, TransitionError> {
    match &observation {
        ReconciliationObservation::NotFound { .. } => return Ok(current.clone()),
        ReconciliationObservation::Open { order_ref, .. }
        | ReconciliationObservation::Filled { order_ref, .. }
        | ReconciliationObservation::Cancelled { order_ref, .. }
            if known_ref.is_some_and(|known| known != order_ref) =>
        {
            return Err(TransitionError::ReconciliationConflict)
        }
        ReconciliationObservation::Rejected { .. } => {
            return Err(TransitionError::ReconciliationConflict)
        }
        _ => {}
    }
    let candidate = positive_state(action.clone(), observation)?;
    if candidate == *current {
        return Ok(current.clone());
    }
    match current {
        LifecycleState::ReconciledOpen { .. } => Ok(candidate),
        LifecycleState::ReconciledPartiallyFilled {
            filled_quantity, ..
        } => match &candidate {
            LifecycleState::ReconciledPartiallyFilled {
                filled_quantity: next,
                ..
            }
            | LifecycleState::ReconciledFilled {
                filled_quantity: next,
                ..
            } if next >= filled_quantity => Ok(candidate),
            LifecycleState::ReconciledCancelled { .. } => Ok(candidate),
            _ => Err(TransitionError::ReconciliationConflict),
        },
        LifecycleState::ReconciledFilled { .. } | LifecycleState::ReconciledCancelled { .. } => {
            Err(TransitionError::ReconciliationConflict)
        }
        _ => Err(TransitionError::InvalidTransition),
    }
}

fn validate_filled(filled: Decimal) -> Result<(), TransitionError> {
    if filled <= Decimal::ZERO {
        return Err(TransitionError::InvalidObservation);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskAuthorization {
    NoIncreasePermitted,
    ReduceOnlyPermitted,
    NormalPlanningPermitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationReadError {
    RateLimited,
    Timeout,
    Transport,
    InvalidResponse,
    Permanent,
}

pub trait ReadOnlyReconciliationSource: Send + Sync {
    fn lookup_by_planned_cloid(
        &self,
        cloid: PlannedCloid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ReconciliationObservation, ReconciliationReadError>>
                + Send
                + '_,
        >,
    >;
}

pub fn plan_reconciled_retry(
    current: &LifecycleState,
    engine_instance_id: EngineInstanceId,
) -> Result<PlannedAction, TransitionError> {
    let LifecycleState::ReconciledNotFound { action } = current else {
        return Err(TransitionError::RetryGenerationNotAuthorized);
    };
    let retry_generation = action
        .retry_generation
        .checked_add(1)
        .ok_or(TransitionError::RetryGenerationOverflow)?;
    let mut retry = action.clone();
    retry.retry_generation = retry_generation;
    retry.planned_cloid = derive_planned_cloid(&PlannedCloidInput {
        engine_instance_id,
        decision_id: action.decision_id,
        target_version: action.target_version,
        asset: action.asset.clone(),
        side: action.side,
        reduce_only: action.reduce_only,
        action_ordinal: action.action_ordinal,
        retry_generation,
    })
    .map_err(|_| TransitionError::RetryGenerationOverflow)?;
    Ok(retry)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    DuplicateCloid,
    CapacityExhausted,
    MissingCloid,
    Transition(TransitionError),
}

#[derive(Debug)]
pub struct LifecycleRegistry {
    by_cloid: BTreeMap<PlannedCloid, LifecycleState>,
    by_asset: BTreeMap<Asset, BTreeSet<PlannedCloid>>,
    blocked_assets: BTreeSet<Asset>,
    maximum_entries: usize,
}

impl LifecycleRegistry {
    pub fn new(maximum_entries: usize) -> Result<Self, RegistryError> {
        if maximum_entries == 0 {
            return Err(RegistryError::CapacityExhausted);
        }
        Ok(Self {
            by_cloid: BTreeMap::new(),
            by_asset: BTreeMap::new(),
            blocked_assets: BTreeSet::new(),
            maximum_entries,
        })
    }

    pub fn insert(&mut self, state: LifecycleState) -> Result<(), RegistryError> {
        let action = state.action().ok_or(RegistryError::MissingCloid)?;
        if self.by_cloid.contains_key(&action.planned_cloid) {
            return Err(RegistryError::DuplicateCloid);
        }
        if self.by_cloid.len() >= self.maximum_entries {
            return Err(RegistryError::CapacityExhausted);
        }
        self.by_asset
            .entry(action.asset.clone())
            .or_default()
            .insert(action.planned_cloid);
        self.by_cloid.insert(action.planned_cloid, state);
        Ok(())
    }

    pub fn apply(
        &mut self,
        cloid: PlannedCloid,
        event: LifecycleEvent,
        policy: ReconciliationPolicy,
    ) -> Result<&LifecycleState, RegistryError> {
        let current = self
            .by_cloid
            .get(&cloid)
            .cloned()
            .ok_or(RegistryError::MissingCloid)?;
        let asset = current
            .action()
            .map(|action| action.asset.clone())
            .ok_or(RegistryError::MissingCloid)?;
        match apply_lifecycle_event(&current, event, policy) {
            Ok(next) => {
                self.by_cloid.insert(cloid, next);
                Ok(self.by_cloid.get(&cloid).expect("inserted lifecycle state"))
            }
            Err(error @ TransitionError::ReconciliationConflict) => {
                self.blocked_assets.insert(asset);
                Err(RegistryError::Transition(error))
            }
            Err(error) => Err(RegistryError::Transition(error)),
        }
    }

    pub fn authorization_for_asset(&self, asset: &str) -> RiskAuthorization {
        if self.blocked_assets.contains(asset)
            || self.by_asset.get(asset).is_some_and(|cloids| {
                cloids.iter().any(|cloid| {
                    matches!(
                        self.by_cloid.get(cloid),
                        Some(LifecycleState::SubmissionUnknown { .. })
                    )
                })
            })
        {
            RiskAuthorization::NoIncreasePermitted
        } else {
            RiskAuthorization::NormalPlanningPermitted
        }
    }

    pub fn compact_terminal(&mut self, maximum_to_remove: usize) -> usize {
        let removable = self
            .by_cloid
            .iter()
            .filter(|(_, state)| state.is_terminal())
            .map(|(cloid, _)| *cloid)
            .take(maximum_to_remove)
            .collect::<Vec<_>>();
        for cloid in &removable {
            if let Some(state) = self.by_cloid.remove(cloid) {
                if let Some(action) = state.action() {
                    if let Some(asset_cloids) = self.by_asset.get_mut(&action.asset) {
                        asset_cloids.remove(cloid);
                        if asset_cloids.is_empty() {
                            self.by_asset.remove(&action.asset);
                        }
                    }
                }
            }
        }
        removable.len()
    }

    pub fn len(&self) -> usize {
        self.by_cloid.len()
    }
}

impl LifecycleState {
    pub fn action(&self) -> Option<&PlannedAction> {
        match self {
            Self::DecisionConstructed { .. } | Self::RiskValidated { .. } => None,
            Self::ActionPlanned { action }
            | Self::AwaitingFutureSubmission { action }
            | Self::SubmissionUnknown { action, .. }
            | Self::ReconciledNotFound { action }
            | Self::ReconciledOpen { action, .. }
            | Self::ReconciledPartiallyFilled { action, .. }
            | Self::ReconciledFilled { action, .. }
            | Self::ReconciledCancelled { action, .. }
            | Self::ReconciledRejected { action, .. } => Some(action),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::ReconciledNotFound { .. }
                | Self::ReconciledFilled { .. }
                | Self::ReconciledCancelled { .. }
                | Self::ReconciledRejected { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::{
        DecisionId, EngineInstanceId, PlannedCloid, Side, TargetVersion,
    };
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn action(asset: &str, cloid_byte: u8) -> PlannedAction {
        PlannedAction {
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(0),
            asset: asset.to_string(),
            side: Side::Buy,
            rounded_notional: decimal("20"),
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
            planned_cloid: PlannedCloid([cloid_byte; 16]),
        }
    }

    fn policy() -> ReconciliationPolicy {
        ReconciliationPolicy {
            required_not_found_confirmations: 2,
            minimum_confirmation_interval_ms: 1_000,
            maximum_attempts: 4,
            maximum_unknown_lifetime_ms: 10_000,
        }
    }

    fn unknown(action: PlannedAction) -> LifecycleState {
        LifecycleState::SubmissionUnknown {
            action,
            first_unknown_at: 100,
            reconciliation_attempts: 0,
            not_found_confirmations: Vec::new(),
        }
    }

    #[test]
    fn engine_side_transitions_are_explicit_and_idempotent() {
        let action = action("BTC", 1);
        let decision = LifecycleState::DecisionConstructed {
            decision_id: action.decision_id,
        };
        assert!(apply_lifecycle_event(
            &decision,
            LifecycleEvent::ReconciliationObserved(ReconciliationObservation::Filled {
                order_ref: ExchangeOrderRef("order-1".to_string()),
                filled_quantity: decimal("1"),
                observed_at: 1,
            }),
            policy(),
        )
        .is_err());
        let validated = apply_lifecycle_event(
            &decision,
            LifecycleEvent::RiskValidated {
                decision_id: action.decision_id,
            },
            policy(),
        )
        .unwrap();
        assert_eq!(
            apply_lifecycle_event(
                &validated,
                LifecycleEvent::RiskValidated {
                    decision_id: action.decision_id,
                },
                policy(),
            )
            .unwrap(),
            validated
        );
        let planned = apply_lifecycle_event(
            &validated,
            LifecycleEvent::ActionPlanned {
                action: action.clone(),
            },
            policy(),
        )
        .unwrap();
        let awaiting =
            apply_lifecycle_event(&planned, LifecycleEvent::AwaitFutureSubmission, policy())
                .unwrap();
        assert!(matches!(
            awaiting,
            LifecycleState::AwaitingFutureSubmission { .. }
        ));
    }

    #[test]
    fn one_not_found_is_insufficient_and_positive_evidence_dominates() {
        let action = action("BTC", 1);
        let first = apply_lifecycle_event(
            &unknown(action.clone()),
            LifecycleEvent::ReconciliationObserved(ReconciliationObservation::NotFound {
                observed_at: 1_000,
            }),
            policy(),
        )
        .unwrap();
        assert!(matches!(first, LifecycleState::SubmissionUnknown { .. }));
        assert_eq!(
            apply_lifecycle_event(
                &first,
                LifecycleEvent::ReconciliationObserved(ReconciliationObservation::NotFound {
                    observed_at: 1_000,
                }),
                policy(),
            )
            .unwrap(),
            first
        );
        let open = apply_lifecycle_event(
            &first,
            LifecycleEvent::ReconciliationObserved(ReconciliationObservation::Open {
                order_ref: ExchangeOrderRef("order-1".to_string()),
                original_quantity: decimal("1"),
                remaining_quantity: decimal("1"),
                observed_at: 1_500,
            }),
            policy(),
        )
        .unwrap();
        assert!(matches!(open, LifecycleState::ReconciledOpen { .. }));

        let second = apply_lifecycle_event(
            &first,
            LifecycleEvent::ReconciliationObserved(ReconciliationObservation::NotFound {
                observed_at: 2_000,
            }),
            policy(),
        )
        .unwrap();
        assert!(matches!(second, LifecycleState::ReconciledNotFound { .. }));
        let late_open = apply_lifecycle_event(
            &second,
            LifecycleEvent::ReconciliationObserved(ReconciliationObservation::Open {
                order_ref: ExchangeOrderRef("order-1".to_string()),
                original_quantity: decimal("1"),
                remaining_quantity: decimal("1"),
                observed_at: 2_100,
            }),
            policy(),
        )
        .unwrap();
        assert!(matches!(late_open, LifecycleState::ReconciledOpen { .. }));
    }

    #[test]
    fn unknown_state_blocks_only_its_asset_and_conflicts_fail_closed() {
        let btc = action("BTC", 1);
        let eth = action("ETH", 2);
        let mut registry = LifecycleRegistry::new(4).unwrap();
        registry.insert(unknown(btc.clone())).unwrap();
        registry
            .insert(LifecycleState::AwaitingFutureSubmission {
                action: eth.clone(),
            })
            .unwrap();
        assert_eq!(
            registry.authorization_for_asset("BTC"),
            RiskAuthorization::NoIncreasePermitted
        );
        assert_eq!(
            registry.authorization_for_asset("ETH"),
            RiskAuthorization::NormalPlanningPermitted
        );

        registry
            .apply(
                btc.planned_cloid,
                LifecycleEvent::ReconciliationObserved(ReconciliationObservation::Filled {
                    order_ref: ExchangeOrderRef("order-1".to_string()),
                    filled_quantity: decimal("1"),
                    observed_at: 1_000,
                }),
                policy(),
            )
            .unwrap();
        let conflict = registry.apply(
            btc.planned_cloid,
            LifecycleEvent::ReconciliationObserved(ReconciliationObservation::Rejected {
                reason: RejectionReason("rejected".to_string()),
                observed_at: 2_000,
            }),
            policy(),
        );
        assert_eq!(
            conflict,
            Err(RegistryError::Transition(
                TransitionError::ReconciliationConflict
            ))
        );
        assert_eq!(
            registry.authorization_for_asset("BTC"),
            RiskAuthorization::NoIncreasePermitted
        );
    }

    #[test]
    fn registry_is_bounded_and_never_evicts_unresolved_entries() {
        let first = action("BTC", 1);
        let second = action("ETH", 2);
        let mut registry = LifecycleRegistry::new(1).unwrap();
        registry.insert(unknown(first.clone())).unwrap();
        assert_eq!(
            registry.insert(unknown(first)),
            Err(RegistryError::DuplicateCloid)
        );
        assert_eq!(
            registry.insert(unknown(second)),
            Err(RegistryError::CapacityExhausted)
        );
        assert_eq!(registry.compact_terminal(10), 0);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn retry_generation_requires_confirmed_absence() {
        let action = action("BTC", 1);
        assert_eq!(
            plan_reconciled_retry(&unknown(action.clone()), EngineInstanceId([2; 16])),
            Err(TransitionError::RetryGenerationNotAuthorized)
        );
        let mut state = unknown(action.clone());
        for observed_at in [1_000, 2_000] {
            state = apply_lifecycle_event(
                &state,
                LifecycleEvent::ReconciliationObserved(ReconciliationObservation::NotFound {
                    observed_at,
                }),
                policy(),
            )
            .unwrap();
        }
        let retry = plan_reconciled_retry(&state, EngineInstanceId([2; 16])).unwrap();
        assert_eq!(retry.retry_generation, 1);
        assert_ne!(retry.planned_cloid, action.planned_cloid);
    }

    #[test]
    fn in_memory_reconciliation_source_exposes_only_lookup() {
        struct Fake;
        impl ReadOnlyReconciliationSource for Fake {
            fn lookup_by_planned_cloid(
                &self,
                _cloid: PlannedCloid,
            ) -> Pin<
                Box<
                    dyn Future<Output = Result<ReconciliationObservation, ReconciliationReadError>>
                        + Send
                        + '_,
                >,
            > {
                Box::pin(async { Ok(ReconciliationObservation::NotFound { observed_at: 1 }) })
            }
        }
        let future = Fake.lookup_by_planned_cloid(PlannedCloid([1; 16]));
        drop(future);
    }
}
