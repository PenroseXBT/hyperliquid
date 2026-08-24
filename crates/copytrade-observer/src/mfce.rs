//! Embedded multi-factor conditional quantile expectancy state and allocation.
//!
//! Gross transition returns are learned from accepted raw source transitions.
//! Execution friction is deliberately absent from labels and predictions; the
//! caller supplies a separately computed live friction estimate to policy
//! evaluation. MFCE owns economic classification and sizing; portfolio risk
//! and accounting invariants remain downstream.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::{Receiver, TryRecvError};

pub const MFCE_STATE_SCHEMA_VERSION: u32 = 1;
pub const MFCE_FEATURE_VERSION: u32 = 1;
pub const MFCE_FEATURE_COUNT: usize = 27;
pub const MFCE_MAX_SAMPLES: usize = 4_096;
// Bound the durable lifecycle map above the configured 375-wallet universe's
// simultaneously active asset surface. This remains a fixed daemon-wide cap;
// flat inactive lifecycles are still deterministically recycled at capacity.
pub const MFCE_MAX_ASSETS: usize = 512;
pub const MFCE_MAX_MODEL_BYTES: usize = 2 * 1_024 * 1_024;
const MFCE_VALIDATION_DENOMINATOR: usize = 5;
// Preserve a full wrapper-minimum chronological prefix after holding out the
// newest fifth for validation: ceil(64 / 0.8) = 80 completed labels.
const MFCE_MIN_TRAINING_SAMPLES: usize = (copytrade_mfce::MIN_TRAINING_ROWS
    * MFCE_VALIDATION_DENOMINATOR
    + (MFCE_VALIDATION_DENOMINATOR - 2))
    / (MFCE_VALIDATION_DENOMINATOR - 1);
const MFCE_RETRAIN_LABEL_INTERVAL: u64 = 32;
const MFCE_MIN_BACKOFF_SAMPLES: usize = 8;
const MFCE_ASSET_SHRINKAGE: f64 = 24.0;
const MFCE_DIRECTION_SHRINKAGE: f64 = 48.0;
const BPS_PER_UNIT_RETURN: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);
const MFCE_EXPLORE_POOL_FRACTION: Decimal = Decimal::from_parts(25, 0, 0, false, 2);
const MFCE_MIN_EXPLORE_PRIOR_FRACTION: Decimal = Decimal::from_parts(15, 0, 0, false, 2);
const MFCE_EXPLORE_CONVICTION_RANGE: Decimal = Decimal::from_parts(10, 0, 0, false, 2);
const MFCE_EXPLORE_RISK_SCALE_BPS: Decimal = Decimal::from_parts(500, 0, 0, false, 0);
const MFCE_SCORE_EPSILON_BPS: Decimal = Decimal::ONE;
const MFCE_BOOTSTRAP_Q10_BPS: Decimal = Decimal::from_parts(100, 0, 0, true, 0);
const MFCE_BOOTSTRAP_Q50_BPS: Decimal = Decimal::ZERO;
const MFCE_BOOTSTRAP_UNCERTAINTY_BPS: Decimal = Decimal::from_parts(100, 0, 0, false, 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfceDirection {
    Long,
    Short,
}

impl MfceDirection {
    pub fn from_signed(value: Decimal) -> Option<Self> {
        if value.is_zero() {
            None
        } else if value.is_sign_positive() {
            Some(Self::Long)
        } else {
            Some(Self::Short)
        }
    }

    pub const fn sign(self) -> i8 {
        match self {
            Self::Long => 1,
            Self::Short => -1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfceTransitionKind {
    Opening,
    Expansion,
    Reversal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceFeatureVector {
    pub version: u32,
    pub values: [Decimal; MFCE_FEATURE_COUNT],
}

impl MfceFeatureVector {
    pub fn new(values: [Decimal; MFCE_FEATURE_COUNT]) -> Self {
        Self {
            version: MFCE_FEATURE_VERSION,
            values,
        }
    }

    fn as_f64(&self) -> Result<Vec<f64>, MfceError> {
        if self.version != MFCE_FEATURE_VERSION {
            return Err(MfceError::InvalidState("feature version mismatch".into()));
        }
        self.values
            .iter()
            .map(|value| {
                value
                    .to_f64()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| MfceError::InvalidState("non-finite feature".into()))
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceTrainingSample {
    pub sample_id: u64,
    pub asset: String,
    pub direction: MfceDirection,
    pub transition_kind: MfceTransitionKind,
    pub opened_at_mono: u64,
    pub completed_at_mono: u64,
    pub features: MfceFeatureVector,
    pub gross_return_bps: Decimal,
    pub lifetime_seconds: Decimal,
    pub admitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceActiveTransition {
    pub transition_id: u64,
    pub entry_midpoint: Decimal,
    pub entry_timestamp_mono: u64,
    pub direction: MfceDirection,
    pub transition_kind: MfceTransitionKind,
    pub proposed_source_exposure: Decimal,
    pub proposed_target_notional: Decimal,
    pub features: MfceFeatureVector,
    pub admitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum MfceAdmissionState {
    Bypass,
    AwaitingLiveBook {
        transition_id: u64,
    },
    Admitted {
        transition_id: u64,
        model_epoch: u64,
        q10_gross_bps: Decimal,
        q50_gross_bps: Decimal,
    },
    Rejected {
        transition_id: u64,
        model_epoch: u64,
        reason: MfceRejectionReason,
        q10_gross_bps: Option<Decimal>,
        q50_gross_bps: Option<Decimal>,
    },
}

impl Default for MfceAdmissionState {
    fn default() -> Self {
        Self::Bypass
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfceRejectionReason {
    NotAcceptedSourceTransition,
    InsufficientPooledSupport,
    MissingLiveBook,
    InvalidPrediction,
    MedianBelowFrictionAndUncertainty,
    StrongNegativeExpectancy,
    AllocationBudgetExhausted,
    TailBudgetExceeded,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceAssetState {
    pub last_observed_source_epoch: u64,
    pub last_raw_source_exposure: Decimal,
    /// Maximum source notional approved by the most recent raw transition.
    /// Non-source recomputations may reduce this target but never expand it.
    #[serde(default)]
    pub approved_target_notional: Decimal,
    /// Net conditional q10 loss rate reserved by the last admission. The
    /// caller applies it to the larger of current and still-approved notional,
    /// so mark drift and partial reductions update dollar tail usage.
    #[serde(default)]
    pub reserved_tail_loss_bps: Decimal,
    pub active: Option<MfceActiveTransition>,
    pub admission: MfceAdmissionState,
    /// Last source transition included in the cumulative decision funnel.
    /// Repricing and book refreshes may reevaluate one transition many times,
    /// but they remain one economic opportunity.
    #[serde(default)]
    pub last_counted_transition_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceValidationSummary {
    pub validation_samples: u64,
    pub candidate_q10_pinball_bps: Decimal,
    pub candidate_q50_pinball_bps: Decimal,
    pub incumbent_q10_pinball_bps: Decimal,
    pub incumbent_q50_pinball_bps: Decimal,
    pub q10_below_label_fraction: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceModelState {
    pub epoch: u64,
    pub trained_through_sample_id: u64,
    pub feature_version: u32,
    pub feature_count: u32,
    pub q10_model: String,
    pub q50_model: String,
    pub q10_sha256: String,
    pub q50_sha256: String,
    pub validation: MfceValidationSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceTrainingAttempt {
    pub attempted_through_sample_id: u64,
    pub activation_sample_id: u64,
    pub candidate_epoch: u64,
    pub incumbent_epoch: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceDecisionCounts {
    pub explore: u64,
    pub exploit: u64,
    pub reject: u64,
    pub allocated: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfcePersistentState {
    pub schema_version: u32,
    pub source_epoch: u64,
    pub next_transition_id: u64,
    pub next_sample_id: u64,
    pub last_retrain_attempt_sample_id: u64,
    pub assets: BTreeMap<String, MfceAssetState>,
    pub samples: VecDeque<MfceTrainingSample>,
    pub incumbent: Option<MfceModelState>,
    #[serde(default)]
    pub pending_training: Option<MfceTrainingAttempt>,
    #[serde(default)]
    pub decision_counts: MfceDecisionCounts,
}

impl Default for MfcePersistentState {
    fn default() -> Self {
        Self {
            schema_version: MFCE_STATE_SCHEMA_VERSION,
            source_epoch: 0,
            next_transition_id: 1,
            next_sample_id: 1,
            last_retrain_attempt_sample_id: 0,
            assets: BTreeMap::new(),
            samples: VecDeque::new(),
            incumbent: None,
            pending_training: None,
            decision_counts: MfceDecisionCounts::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MfceTransitionUpdate {
    pub transition_id: Option<u64>,
    pub completed_sample_id: Option<u64>,
    pub needs_live_book: bool,
    pub effective_target: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MfcePrediction {
    pub model_epoch: u64,
    pub q10_gross_bps: Decimal,
    pub q50_gross_bps: Decimal,
    pub uncertainty_bps: Decimal,
    pub pooled_sample_count: u64,
    pub direction_sample_count: u64,
    pub asset_direction_sample_count: u64,
    pub used_model: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MfceAllocationInput {
    pub prediction: MfcePrediction,
    pub friction_bps: Decimal,
    /// Absolute aggregate copytrade conviction, normalized to [0, 1]. It is
    /// the cold-start allocation prior; it never bypasses downstream risk.
    pub copytrade_conviction: Decimal,
    pub current_position_notional: Decimal,
    pub proposed_position_notional: Decimal,
    pub remaining_tail_loss_budget_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MfceCrossSectionalCandidate {
    pub asset: String,
    pub transition_id: u64,
    pub input: MfceAllocationInput,
    /// Tail dollars already reserved for the incumbent position in this asset.
    /// A replacement allocation consumes only the positive increment.
    pub prior_tail_loss_usd: Decimal,
}

/// Economic state selected by MFCE before downstream portfolio constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MfcePolicyState {
    Exploit,
    Explore,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MfceAllocationDecision {
    pub policy_state: MfcePolicyState,
    pub admitted: bool,
    pub reason: Option<MfceRejectionReason>,
    /// Fraction of the source-authorized risk increase requested by MFCE.
    /// Hard portfolio projection may reduce it further.
    pub allocation_fraction: Decimal,
    /// After-cost median divided by conditional q50-q10 downside width.
    pub opportunity_score: Decimal,
    pub net_q50_bps: Decimal,
    pub conservative_edge_bps: Decimal,
    pub net_q10_bps: Decimal,
    pub required_median_bps: Decimal,
    pub modeled_tail_loss_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MfcePolicyOutput {
    pub asset: String,
    pub transition_id: u64,
    pub model_epoch: u64,
    pub policy_state: MfcePolicyState,
    pub admitted: bool,
    pub reason: Option<MfceRejectionReason>,
    pub q10_gross_bps: Decimal,
    pub q50_gross_bps: Decimal,
    pub friction_bps: Decimal,
    pub uncertainty_bps: Decimal,
    pub used_model: bool,
    pub pooled_sample_count: u64,
    pub direction_sample_count: u64,
    pub asset_direction_sample_count: u64,
    pub net_q10_bps: Decimal,
    pub net_q50_bps: Decimal,
    pub conservative_edge_bps: Decimal,
    pub opportunity_score: Decimal,
    pub allocation_fraction: Decimal,
    pub modeled_tail_loss_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MfceReport {
    pub observed_transitions: u64,
    pub completed_samples: usize,
    pub active_transitions: usize,
    pub awaiting_live_books: usize,
    pub incumbent_epoch: Option<u64>,
    pub trained_through_sample_id: Option<u64>,
    pub labels_since_retrain_attempt: u64,
    pub decision_counts: MfceDecisionCounts,
    pub policy_outputs: Vec<MfcePolicyOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MfceError {
    InvalidState(String),
    Arithmetic,
    Model(String),
}

impl std::fmt::Display for MfceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MfceError {}

struct CompletedTraining {
    attempted_through_sample_id: u64,
    result: Result<Option<MfceModelState>, MfceError>,
}

/// Runtime model handles and the serializable MFCE state they serve.
///
/// LightGBM handles stay on this owning thread. Background workers return only
/// bounded model strings, which are loaded together before state promotion.
pub struct MfceEngine {
    state: MfcePersistentState,
    models: Option<copytrade_mfce::QuantileModelPair>,
    training: Option<Receiver<CompletedTraining>>,
    completed_source_epoch: u64,
    policy_outputs: BTreeMap<String, MfcePolicyOutput>,
}

impl Default for MfceEngine {
    fn default() -> Self {
        Self {
            state: MfcePersistentState::default(),
            models: None,
            training: None,
            completed_source_epoch: 0,
            policy_outputs: BTreeMap::new(),
        }
    }
}

impl MfcePersistentState {
    pub fn time_high_watermark(&self) -> u64 {
        let sample_high_watermark = self.samples.iter().fold(0, |high, sample| {
            high.max(sample.opened_at_mono)
                .max(sample.completed_at_mono)
        });
        self.assets
            .values()
            .filter_map(|asset| asset.active.as_ref())
            .fold(sample_high_watermark, |high, active| {
                high.max(active.entry_timestamp_mono)
            })
    }

    pub fn validate(&self) -> Result<(), MfceError> {
        let counted_decisions = self
            .decision_counts
            .explore
            .checked_add(self.decision_counts.exploit)
            .and_then(|count| count.checked_add(self.decision_counts.reject));
        let admitted_decisions = self
            .decision_counts
            .explore
            .checked_add(self.decision_counts.exploit);
        if self.schema_version != MFCE_STATE_SCHEMA_VERSION
            || self.assets.len() > MFCE_MAX_ASSETS
            || self.samples.len() > MFCE_MAX_SAMPLES
            || self.next_transition_id == 0
            || self.next_sample_id == 0
            || self.last_retrain_attempt_sample_id >= self.next_sample_id
            || counted_decisions.is_none_or(|count| count >= self.next_transition_id)
            || admitted_decisions.is_none_or(|count| self.decision_counts.allocated > count)
        {
            return Err(MfceError::InvalidState(
                "MFCE state bounds are invalid".into(),
            ));
        }
        let mut previous_sample_id = None;
        for sample in &self.samples {
            validate_asset(&sample.asset)?;
            validate_features(&sample.features)?;
            if sample.sample_id == 0
                || previous_sample_id.is_some_and(|previous| sample.sample_id <= previous)
                || sample.completed_at_mono < sample.opened_at_mono
                || sample.lifetime_seconds < Decimal::ZERO
            {
                return Err(MfceError::InvalidState("invalid MFCE sample".into()));
            }
            previous_sample_id = Some(sample.sample_id);
        }
        if previous_sample_id.is_some_and(|id| id >= self.next_sample_id) {
            return Err(MfceError::InvalidState(
                "MFCE sample sequence regressed".into(),
            ));
        }
        for (asset, state) in &self.assets {
            validate_asset(asset)?;
            if state.last_observed_source_epoch > self.source_epoch
                || state.reserved_tail_loss_bps < Decimal::ZERO
                || state
                    .last_counted_transition_id
                    .is_some_and(|transition_id| transition_id >= self.next_transition_id)
                || (state.last_raw_source_exposure.is_zero()
                    && !state.approved_target_notional.is_zero())
                || (!state.approved_target_notional.is_zero()
                    && state.approved_target_notional.is_sign_positive()
                        != state.last_raw_source_exposure.is_sign_positive())
            {
                return Err(MfceError::InvalidState("invalid MFCE asset state".into()));
            }
            if let Some(active) = &state.active {
                validate_features(&active.features)?;
                if active.transition_id == 0
                    || active.transition_id >= self.next_transition_id
                    || active.entry_midpoint <= Decimal::ZERO
                    || active.proposed_source_exposure.is_zero()
                    || active.proposed_target_notional.is_zero()
                    || MfceDirection::from_signed(active.proposed_source_exposure)
                        != Some(active.direction)
                    || MfceDirection::from_signed(active.proposed_target_notional)
                        != Some(active.direction)
                    || active.proposed_source_exposure != state.last_raw_source_exposure
                {
                    return Err(MfceError::InvalidState(
                        "invalid active MFCE transition".into(),
                    ));
                }
            }
            match state.admission {
                MfceAdmissionState::Bypass if state.active.is_some() => {
                    return Err(MfceError::InvalidState(
                        "bypassed MFCE asset has an active transition".into(),
                    ));
                }
                MfceAdmissionState::AwaitingLiveBook { transition_id }
                | MfceAdmissionState::Admitted { transition_id, .. }
                    if state
                        .active
                        .as_ref()
                        .is_none_or(|active| active.transition_id != transition_id) =>
                {
                    return Err(MfceError::InvalidState(
                        "MFCE admission identity is inactive".into(),
                    ));
                }
                MfceAdmissionState::Rejected {
                    transition_id,
                    reason,
                    q10_gross_bps,
                    q50_gross_bps,
                    ..
                } => match state.active.as_ref() {
                    Some(active) if active.transition_id == transition_id => {}
                    Some(_) => {
                        return Err(MfceError::InvalidState(
                            "MFCE rejection identity mismatches its active transition".into(),
                        ));
                    }
                    None if transition_id > 0
                        && transition_id < self.next_transition_id
                        && reason == MfceRejectionReason::NotAcceptedSourceTransition
                        && q10_gross_bps.is_none()
                        && q50_gross_bps.is_none() => {}
                    None => {
                        return Err(MfceError::InvalidState(
                            "inactive MFCE rejection is not an unaccepted source transition".into(),
                        ));
                    }
                },
                MfceAdmissionState::Admitted {
                    q10_gross_bps,
                    q50_gross_bps,
                    ..
                } if q10_gross_bps > q50_gross_bps => {
                    return Err(MfceError::InvalidState(
                        "crossed persisted MFCE admission quantiles".into(),
                    ));
                }
                _ => {}
            }
            if let MfceAdmissionState::Rejected {
                q10_gross_bps,
                q50_gross_bps,
                ..
            } = state.admission
            {
                if q10_gross_bps.is_some() != q50_gross_bps.is_some()
                    || q10_gross_bps
                        .zip(q50_gross_bps)
                        .is_some_and(|(q10, q50)| q10 > q50)
                {
                    return Err(MfceError::InvalidState(
                        "invalid persisted MFCE rejection quantiles".into(),
                    ));
                }
            }
        }
        if let Some(model) = &self.incumbent {
            validate_model_state(model)?;
            if model.trained_through_sample_id >= self.next_sample_id {
                return Err(MfceError::InvalidState(
                    "MFCE model was trained beyond retained sequence".into(),
                ));
            }
        }
        if let Some(attempt) = &self.pending_training {
            let incumbent_epoch = self.incumbent.as_ref().map(|model| model.epoch);
            if attempt.attempted_through_sample_id == 0
                || attempt.attempted_through_sample_id >= self.next_sample_id
                || attempt.activation_sample_id
                    != attempt.attempted_through_sample_id.saturating_add(1)
                || attempt.activation_sample_id > self.next_sample_id
                || attempt.candidate_epoch == 0
                || attempt.incumbent_epoch != incumbent_epoch
                || attempt.candidate_epoch
                    != incumbent_epoch.map_or(1, |epoch| epoch.saturating_add(1))
                || self.last_retrain_attempt_sample_id != attempt.attempted_through_sample_id
            {
                return Err(MfceError::InvalidState(
                    "invalid durable MFCE training attempt".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn note_accepted_source_snapshot(&mut self) -> Result<u64, MfceError> {
        self.source_epoch = self
            .source_epoch
            .checked_add(1)
            .ok_or(MfceError::Arithmetic)?;
        Ok(self.source_epoch)
    }

    pub fn report(&self) -> MfceReport {
        MfceReport {
            observed_transitions: self.next_transition_id.saturating_sub(1),
            completed_samples: self.samples.len(),
            active_transitions: self
                .assets
                .values()
                .filter(|state| state.active.is_some())
                .count(),
            awaiting_live_books: self
                .assets
                .values()
                .filter(|state| {
                    matches!(state.admission, MfceAdmissionState::AwaitingLiveBook { .. })
                })
                .count(),
            incumbent_epoch: self.incumbent.as_ref().map(|model| model.epoch),
            trained_through_sample_id: self
                .incumbent
                .as_ref()
                .map(|model| model.trained_through_sample_id),
            labels_since_retrain_attempt: self
                .next_sample_id
                .saturating_sub(1)
                .saturating_sub(self.last_retrain_attempt_sample_id),
            decision_counts: self.decision_counts,
            policy_outputs: Vec::new(),
        }
    }
}

impl MfceEngine {
    pub fn from_state(state: MfcePersistentState) -> Result<Self, MfceError> {
        state.validate()?;
        let models = state
            .incumbent
            .as_ref()
            .map(|model| {
                let strings = copytrade_mfce::QuantileModelStrings::new(
                    MFCE_FEATURE_COUNT,
                    model.q10_model.clone(),
                    model.q50_model.clone(),
                )
                .map_err(model_error)?;
                copytrade_mfce::QuantileModelPair::from_model_strings(&strings).map_err(model_error)
            })
            .transpose()?;
        Ok(Self {
            completed_source_epoch: state.source_epoch,
            state,
            models,
            training: None,
            policy_outputs: BTreeMap::new(),
        })
    }

    pub fn state(&self) -> &MfcePersistentState {
        &self.state
    }

    pub fn replace_state(&mut self, state: MfcePersistentState) -> Result<(), MfceError> {
        let replacement = Self::from_state(state)?;
        *self = replacement;
        Ok(())
    }

    pub fn note_accepted_source_snapshot(&mut self) -> Result<u64, MfceError> {
        self.state.note_accepted_source_snapshot()
    }

    /// Marks the end of one complete target-universe observation. New assets
    /// appearing without a later accepted source snapshot must not inherit an
    /// old global source epoch merely because a flat lifecycle was evicted.
    pub fn finish_source_observation_cycle(&mut self) {
        self.completed_source_epoch = self.state.source_epoch;
    }

    pub fn report(&self) -> MfceReport {
        let mut report = self.state.report();
        report.policy_outputs = self
            .policy_outputs
            .iter()
            .filter(|(asset, output)| {
                self.state.assets.get(*asset).is_some_and(|state| {
                    state
                        .active
                        .as_ref()
                        .is_some_and(|active| active.transition_id == output.transition_id)
                })
            })
            .map(|(_, output)| output.clone())
            .collect();
        // `AwaitingLiveBook` is also used internally to request a fresh
        // friction evaluation for an already-evaluated transition. Keep that
        // refresh demand out of the external funnel: a transition is truly
        // book-awaiting only until its first complete policy output exists.
        report.awaiting_live_books = self
            .state
            .assets
            .iter()
            .filter(|(asset, state)| {
                let MfceAdmissionState::AwaitingLiveBook { transition_id } = state.admission else {
                    return false;
                };
                !self
                    .policy_outputs
                    .get(*asset)
                    .is_some_and(|output| output.transition_id == transition_id)
            })
            .count();
        report
    }

    /// Assets whose live context should be refreshed. This deliberately
    /// includes evaluated transitions and is therefore broader than the
    /// externally reported `awaiting_live_books` funnel count.
    pub fn awaiting_book_assets(&self) -> impl Iterator<Item = &String> {
        self.state.assets.iter().filter_map(|(asset, state)| {
            matches!(state.admission, MfceAdmissionState::AwaitingLiveBook { .. }).then_some(asset)
        })
    }

    pub fn has_active_transition(&self, asset: &str) -> bool {
        self.state
            .assets
            .get(asset)
            .is_some_and(|state| state.active.is_some())
    }

    pub fn tracked_assets(&self) -> impl Iterator<Item = &String> {
        self.state.assets.keys()
    }

    pub fn previous_source_exposure(&self, asset: &str) -> Decimal {
        self.state
            .assets
            .get(asset)
            .map_or(Decimal::ZERO, |state| state.last_raw_source_exposure)
    }

    pub fn reserved_tail_loss_usd(
        &self,
        asset: &str,
        current_position_notional: Decimal,
    ) -> Result<Decimal, MfceError> {
        let Some(state) = self.state.assets.get(asset) else {
            return Ok(Decimal::ZERO);
        };
        let reserved_notional = if matches!(state.admission, MfceAdmissionState::Admitted { .. }) {
            current_position_notional
                .abs()
                .max(state.approved_target_notional.abs())
        } else {
            current_position_notional.abs()
        };
        reserved_notional
            .checked_mul(state.reserved_tail_loss_bps)
            .and_then(|value| value.checked_div(BPS_PER_UNIT_RETURN))
            .ok_or(MfceError::Arithmetic)
    }

    pub fn observe_raw_source(
        &mut self,
        asset: &str,
        accepted_source_transition: bool,
        raw_source_exposure: Decimal,
        desired_target: Decimal,
        current_target: Decimal,
        midpoint: Decimal,
        now: u64,
        features: MfceFeatureVector,
    ) -> Result<MfceTransitionUpdate, MfceError> {
        validate_asset(asset)?;
        validate_features(&features)?;
        if midpoint <= Decimal::ZERO {
            return Err(MfceError::InvalidState(
                "non-positive transition midpoint".into(),
            ));
        }
        if raw_source_exposure.is_zero() && !self.state.assets.contains_key(asset) {
            return Ok(MfceTransitionUpdate {
                transition_id: None,
                completed_sample_id: None,
                needs_live_book: false,
                effective_target: desired_target,
            });
        }
        if !self.state.assets.contains_key(asset) && self.state.assets.len() >= MFCE_MAX_ASSETS {
            // The daemon has a lifetime bound, not a lifetime asset quota.
            // Deterministically recycle the first completely flat/inactive
            // lifecycle; historical labels remain in the bounded sample ring.
            let evictable = self
                .state
                .assets
                .iter()
                .find(|(_, state)| {
                    state.last_raw_source_exposure.is_zero()
                        && state.approved_target_notional.is_zero()
                        && state.reserved_tail_loss_bps.is_zero()
                        && state.active.is_none()
                        && matches!(state.admission, MfceAdmissionState::Bypass)
                })
                .map(|(asset, _)| asset.clone());
            if let Some(evictable) = evictable {
                self.state.assets.remove(&evictable);
                self.policy_outputs.remove(&evictable);
            } else {
                return Err(MfceError::InvalidState("MFCE asset bound exceeded".into()));
            }
        }

        let source_epoch = self.state.source_epoch;
        let completed_source_epoch = self.completed_source_epoch;
        let state = self
            .state
            .assets
            .entry(asset.to_string())
            .or_insert_with(|| MfceAssetState {
                last_observed_source_epoch: completed_source_epoch,
                ..MfceAssetState::default()
            });
        let previous_exposure = state.last_raw_source_exposure;
        let exposure_changed = previous_exposure != raw_source_exposure;
        state.last_observed_source_epoch = source_epoch;

        if !exposure_changed {
            if current_target.is_zero() {
                state.reserved_tail_loss_bps = Decimal::ZERO;
            }
            // Every execution-time recomputation refreshes friction for the
            // same persisted source transition. This is not a new candidate:
            // the transition identity and gross-label entry stay immutable.
            if source_target_adds_risk(current_target, desired_target) {
                if let Some(active) = state.active.as_ref() {
                    state.admission = MfceAdmissionState::AwaitingLiveBook {
                        transition_id: active.transition_id,
                    };
                }
            }
            return Ok(MfceTransitionUpdate {
                transition_id: transition_id(&state.admission),
                completed_sample_id: None,
                needs_live_book: state.active.is_some()
                    && source_target_adds_risk(current_target, desired_target),
                effective_target: effective_target(
                    &state.admission,
                    state.approved_target_notional,
                    current_target,
                    desired_target,
                ),
            });
        }

        let completed_sample_id = complete_active(
            &mut self.state.samples,
            &mut self.state.next_sample_id,
            asset,
            state.active.take(),
            midpoint,
            now,
        )?;
        state.last_raw_source_exposure = raw_source_exposure;

        // Raw exposure change is the sole candidate trigger, but whether the
        // candidate adds risk is position-aware. A source size reduction can
        // still be a follower opening when the earlier source transition was
        // rejected and the follower remains flat.
        let adds_risk = source_target_adds_risk(current_target, desired_target);
        if !adds_risk {
            state.approved_target_notional = desired_target;
            state.admission = MfceAdmissionState::Bypass;
            return Ok(MfceTransitionUpdate {
                transition_id: None,
                completed_sample_id,
                needs_live_book: false,
                effective_target: desired_target,
            });
        }

        if !accepted_source_transition {
            let transition_id = self.state.next_transition_id;
            self.state.next_transition_id = self
                .state
                .next_transition_id
                .checked_add(1)
                .ok_or(MfceError::Arithmetic)?;
            state.admission = MfceAdmissionState::Rejected {
                transition_id,
                model_epoch: self.state.incumbent.as_ref().map_or(0, |model| model.epoch),
                reason: MfceRejectionReason::NotAcceptedSourceTransition,
                q10_gross_bps: None,
                q50_gross_bps: None,
            };
            state.approved_target_notional = rejected_target(current_target, desired_target);
            return Ok(MfceTransitionUpdate {
                transition_id: Some(transition_id),
                completed_sample_id,
                needs_live_book: false,
                effective_target: rejected_target(current_target, desired_target),
            });
        }

        let direction = MfceDirection::from_signed(raw_source_exposure)
            .ok_or_else(|| MfceError::InvalidState("risk transition has no direction".into()))?;
        let kind = transition_kind(current_target, desired_target);
        let transition_id = self.state.next_transition_id;
        self.state.next_transition_id = self
            .state
            .next_transition_id
            .checked_add(1)
            .ok_or(MfceError::Arithmetic)?;
        state.active = Some(MfceActiveTransition {
            transition_id,
            entry_midpoint: midpoint,
            entry_timestamp_mono: now,
            direction,
            transition_kind: kind,
            proposed_source_exposure: raw_source_exposure,
            proposed_target_notional: desired_target,
            features,
            admitted: false,
        });
        state.approved_target_notional = rejected_target(current_target, desired_target);
        state.admission = MfceAdmissionState::AwaitingLiveBook { transition_id };
        Ok(MfceTransitionUpdate {
            transition_id: Some(transition_id),
            completed_sample_id,
            needs_live_book: true,
            effective_target: rejected_target(current_target, desired_target),
        })
    }

    pub fn update_pending_live_context(
        &mut self,
        asset: &str,
        transition_id: u64,
        liquidity_features: [Decimal; 5],
        proposed_target_notional: Decimal,
    ) -> Result<(), MfceError> {
        let state = self
            .state
            .assets
            .get_mut(asset)
            .ok_or_else(|| MfceError::InvalidState("missing pending MFCE asset".into()))?;
        if !matches!(
            state.admission,
            MfceAdmissionState::AwaitingLiveBook {
                transition_id: pending
            } if pending == transition_id
        ) {
            return Err(MfceError::InvalidState("stale MFCE transition".into()));
        }
        let active = state
            .active
            .as_mut()
            .ok_or_else(|| MfceError::InvalidState("pending MFCE transition is inactive".into()))?;
        if active.transition_id != transition_id {
            return Err(MfceError::InvalidState(
                "MFCE transition identity mismatch".into(),
            ));
        }
        // Preserve every feature captured at the raw transition. A later book
        // response fills only the explicit liquidity-missing/spread/depth
        // slots; it must not reclassify an opening as an expansion or replace
        // the conviction/technical snapshot with post-transition state.
        if active.features.values[10] == Decimal::ONE {
            active.features.values[10..15].copy_from_slice(&liquidity_features);
        }
        validate_features(&active.features)?;
        // The source-authorized cap is immutable for this transition. The
        // latest capacity/desired target is applied ephemerally by
        // `pending_position_notional`; a temporary contraction must not ratchet
        // away an otherwise valid source transition.
        let _ = proposed_target_notional;
        Ok(())
    }

    pub fn pending_position_notional(
        &self,
        asset: &str,
        transition_id: u64,
        desired_target: Decimal,
    ) -> Result<Decimal, MfceError> {
        let state = self
            .state
            .assets
            .get(asset)
            .ok_or_else(|| MfceError::InvalidState("missing pending MFCE asset".into()))?;
        if !matches!(
            state.admission,
            MfceAdmissionState::AwaitingLiveBook {
                transition_id: pending
            } if pending == transition_id
        ) {
            return Err(MfceError::InvalidState("stale MFCE transition".into()));
        }
        let active = state
            .active
            .as_ref()
            .filter(|active| active.transition_id == transition_id)
            .ok_or_else(|| MfceError::InvalidState("pending MFCE transition is inactive".into()))?;
        Ok(cap_to_approved_target(
            active.proposed_target_notional,
            desired_target,
        ))
    }

    pub fn predict_pending(
        &self,
        asset: &str,
        transition_id: u64,
    ) -> Result<MfcePrediction, MfceError> {
        let state = self
            .state
            .assets
            .get(asset)
            .ok_or_else(|| MfceError::InvalidState("missing MFCE asset".into()))?;
        if !matches!(
            state.admission,
            MfceAdmissionState::AwaitingLiveBook {
                transition_id: pending
            } if pending == transition_id
        ) {
            return Err(MfceError::InvalidState(
                "MFCE candidate is not pending".into(),
            ));
        }
        let active = state
            .active
            .as_ref()
            .filter(|active| active.transition_id == transition_id)
            .ok_or_else(|| MfceError::InvalidState("missing active MFCE transition".into()))?;
        self.predict(asset, active.direction, &active.features)
    }

    pub fn record_allocation(
        &mut self,
        asset: &str,
        transition_id: u64,
        decision: &MfceAllocationDecision,
        prediction: &MfcePrediction,
        current_position_notional: Decimal,
    ) -> Result<(), MfceError> {
        let friction_bps = prediction
            .q50_gross_bps
            .checked_sub(decision.net_q50_bps)
            .ok_or(MfceError::Arithmetic)?;
        let mut decision_counts = self.state.decision_counts;
        let state = self
            .state
            .assets
            .get_mut(asset)
            .ok_or_else(|| MfceError::InvalidState("missing MFCE admission asset".into()))?;
        if !matches!(
            state.admission,
            MfceAdmissionState::AwaitingLiveBook {
                transition_id: pending
            } if pending == transition_id
        ) {
            return Err(MfceError::InvalidState(
                "stale MFCE admission result".into(),
            ));
        }
        let is_new_decision = state.last_counted_transition_id != Some(transition_id);
        if is_new_decision {
            match decision.policy_state {
                MfcePolicyState::Explore => {
                    decision_counts.explore = decision_counts
                        .explore
                        .checked_add(1)
                        .ok_or(MfceError::Arithmetic)?;
                }
                MfcePolicyState::Exploit => {
                    decision_counts.exploit = decision_counts
                        .exploit
                        .checked_add(1)
                        .ok_or(MfceError::Arithmetic)?;
                }
                MfcePolicyState::Reject => {
                    decision_counts.reject = decision_counts
                        .reject
                        .checked_add(1)
                        .ok_or(MfceError::Arithmetic)?;
                }
            }
            if decision.admitted && !decision.allocation_fraction.is_zero() {
                decision_counts.allocated = decision_counts
                    .allocated
                    .checked_add(1)
                    .ok_or(MfceError::Arithmetic)?;
            }
        }
        if let Some(active) = state.active.as_mut() {
            if active.transition_id == transition_id {
                active.admitted = decision.admitted;
                if decision.admitted {
                    state.approved_target_notional = allocated_target(
                        current_position_notional,
                        active.proposed_target_notional,
                        decision.allocation_fraction,
                    )?;
                }
            }
        }
        state.admission = if decision.admitted {
            state.reserved_tail_loss_bps = (-decision.net_q10_bps).max(Decimal::ZERO);
            MfceAdmissionState::Admitted {
                transition_id,
                model_epoch: prediction.model_epoch,
                q10_gross_bps: prediction.q10_gross_bps,
                q50_gross_bps: prediction.q50_gross_bps,
            }
        } else {
            if current_position_notional.is_zero() {
                state.reserved_tail_loss_bps = Decimal::ZERO;
            } else {
                state.reserved_tail_loss_bps = state
                    .reserved_tail_loss_bps
                    .max((-decision.net_q10_bps).max(Decimal::ZERO));
            }
            MfceAdmissionState::Rejected {
                transition_id,
                model_epoch: prediction.model_epoch,
                reason: decision
                    .reason
                    .unwrap_or(MfceRejectionReason::InvalidPrediction),
                q10_gross_bps: Some(prediction.q10_gross_bps),
                q50_gross_bps: Some(prediction.q50_gross_bps),
            }
        };
        self.policy_outputs.insert(
            asset.to_string(),
            MfcePolicyOutput {
                asset: asset.to_string(),
                transition_id,
                model_epoch: prediction.model_epoch,
                policy_state: decision.policy_state,
                admitted: decision.admitted,
                reason: decision.reason,
                q10_gross_bps: prediction.q10_gross_bps,
                q50_gross_bps: prediction.q50_gross_bps,
                friction_bps,
                uncertainty_bps: prediction.uncertainty_bps,
                used_model: prediction.used_model,
                pooled_sample_count: prediction.pooled_sample_count,
                direction_sample_count: prediction.direction_sample_count,
                asset_direction_sample_count: prediction.asset_direction_sample_count,
                net_q10_bps: decision.net_q10_bps,
                net_q50_bps: decision.net_q50_bps,
                conservative_edge_bps: decision.conservative_edge_bps,
                opportunity_score: decision.opportunity_score,
                allocation_fraction: decision.allocation_fraction,
                modeled_tail_loss_usd: decision.modeled_tail_loss_usd,
            },
        );
        if is_new_decision {
            state.last_counted_transition_id = Some(transition_id);
        }
        self.state.decision_counts = decision_counts;
        Ok(())
    }

    pub fn reject_pending_without_prediction(
        &mut self,
        asset: &str,
        transition_id: u64,
        reason: MfceRejectionReason,
    ) -> Result<(), MfceError> {
        let model_epoch = self.state.incumbent.as_ref().map_or(0, |model| model.epoch);
        let mut decision_counts = self.state.decision_counts;
        let state = self
            .state
            .assets
            .get_mut(asset)
            .ok_or_else(|| MfceError::InvalidState("missing MFCE rejection asset".into()))?;
        if !matches!(
            state.admission,
            MfceAdmissionState::AwaitingLiveBook {
                transition_id: pending
            } if pending == transition_id
        ) {
            return Err(MfceError::InvalidState("stale MFCE rejection".into()));
        }
        if state.last_counted_transition_id != Some(transition_id) {
            decision_counts.reject = decision_counts
                .reject
                .checked_add(1)
                .ok_or(MfceError::Arithmetic)?;
            state.last_counted_transition_id = Some(transition_id);
        }
        if let Some(active) = state.active.as_mut() {
            if active.transition_id == transition_id {
                active.admitted = false;
            }
        }
        state.admission = MfceAdmissionState::Rejected {
            transition_id,
            model_epoch,
            reason,
            q10_gross_bps: None,
            q50_gross_bps: None,
        };
        self.state.decision_counts = decision_counts;
        Ok(())
    }

    pub fn effective_target(
        &self,
        asset: &str,
        current_target: Decimal,
        desired_target: Decimal,
    ) -> Decimal {
        self.state
            .assets
            .get(asset)
            .map_or(desired_target, |state| {
                effective_target(
                    &state.admission,
                    state.approved_target_notional,
                    current_target,
                    desired_target,
                )
            })
    }

    /// Only an admitted transition may retain the raw source attribution for
    /// unfilled approved risk. Pending/rejected transitions and reduction-only
    /// bypasses may retain only the follower's current position attribution.
    pub fn uses_current_position_attribution(&self, asset: &str) -> bool {
        self.state.assets.get(asset).is_some_and(|state| {
            matches!(
                state.admission,
                MfceAdmissionState::Bypass
                    | MfceAdmissionState::AwaitingLiveBook { .. }
                    | MfceAdmissionState::Rejected { .. }
            )
        })
    }

    pub fn expected_holding_hours(
        &self,
        asset: &str,
        direction: MfceDirection,
        default_hours: Decimal,
    ) -> Decimal {
        let asset_values = self
            .state
            .samples
            .iter()
            .filter(|sample| sample.asset == asset && sample.direction == direction)
            .filter_map(|sample| sample.lifetime_seconds.to_f64())
            .collect::<Vec<_>>();
        let direction_values = self
            .state
            .samples
            .iter()
            .filter(|sample| sample.direction == direction)
            .filter_map(|sample| sample.lifetime_seconds.to_f64())
            .collect::<Vec<_>>();
        let pooled_values = self
            .state
            .samples
            .iter()
            .filter_map(|sample| sample.lifetime_seconds.to_f64())
            .collect::<Vec<_>>();
        let default_seconds = default_hours
            .checked_mul(Decimal::from(3_600))
            .and_then(|value| value.to_f64())
            .unwrap_or(21_600.0);
        let pooled = empirical_quantile(&pooled_values, 0.5).unwrap_or(default_seconds);
        let direction_median = empirical_quantile(&direction_values, 0.5).unwrap_or(pooled);
        let direction_weight = shrinkage_weight(direction_values.len(), MFCE_DIRECTION_SHRINKAGE);
        let direction_backoff = blend(pooled, direction_median, direction_weight);
        let asset_median = empirical_quantile(&asset_values, 0.5).unwrap_or(direction_backoff);
        let asset_weight = shrinkage_weight(asset_values.len(), MFCE_ASSET_SHRINKAGE);
        let seconds = blend(direction_backoff, asset_median, asset_weight).clamp(900.0, 604_800.0);
        Decimal::from_f64(seconds / 3_600.0).unwrap_or(default_hours)
    }

    pub fn poll_training(&mut self) -> Result<bool, MfceError> {
        let Some(receiver) = self.training.as_ref() else {
            return Ok(false);
        };
        let attempt = self.state.pending_training.as_ref().ok_or_else(|| {
            MfceError::InvalidState("runtime MFCE worker has no durable attempt".into())
        })?;
        if self.state.next_sample_id.saturating_sub(1) < attempt.activation_sample_id {
            return Ok(false);
        }
        let result = match receiver.try_recv() {
            Ok(result) => result,
            Err(TryRecvError::Empty) => return Ok(false),
            Err(TryRecvError::Disconnected) => {
                self.training = None;
                return Err(MfceError::Model("MFCE training worker disconnected".into()));
            }
        };
        self.training = None;
        let attempt = self.state.pending_training.take().ok_or_else(|| {
            MfceError::InvalidState("completed MFCE worker lost its durable attempt".into())
        })?;
        if result.attempted_through_sample_id != attempt.attempted_through_sample_id {
            return Err(MfceError::InvalidState(
                "MFCE worker completed the wrong sample prefix".into(),
            ));
        }
        let Some(candidate) = result.result? else {
            return Ok(false);
        };
        if candidate.epoch != attempt.candidate_epoch {
            return Err(MfceError::InvalidState(
                "MFCE candidate identity mismatches its durable attempt".into(),
            ));
        }
        validate_model_state(&candidate)?;
        let strings = copytrade_mfce::QuantileModelStrings::new(
            MFCE_FEATURE_COUNT,
            candidate.q10_model.clone(),
            candidate.q50_model.clone(),
        )
        .map_err(model_error)?;
        let models =
            copytrade_mfce::QuantileModelPair::from_model_strings(&strings).map_err(model_error)?;
        // The runtime pair is fully loaded before either serializable model
        // string is replaced, making q10/q50 promotion one atomic owner action.
        self.models = Some(models);
        self.state.incumbent = Some(candidate);
        Ok(true)
    }

    pub fn maybe_start_training(&mut self) -> Result<bool, MfceError> {
        if self.training.is_some() {
            return Ok(false);
        }
        let attempt = if let Some(attempt) = self.state.pending_training.clone() {
            attempt
        } else {
            if self.state.samples.len() < MFCE_MIN_TRAINING_SAMPLES {
                return Ok(false);
            }
            let latest_sample_id = self.state.next_sample_id.saturating_sub(1);
            let required = if self.state.last_retrain_attempt_sample_id == 0 {
                MFCE_MIN_TRAINING_SAMPLES as u64
            } else {
                MFCE_RETRAIN_LABEL_INTERVAL
            };
            if latest_sample_id.saturating_sub(self.state.last_retrain_attempt_sample_id) < required
            {
                return Ok(false);
            }
            let incumbent_epoch = self.state.incumbent.as_ref().map(|model| model.epoch);
            let attempt = MfceTrainingAttempt {
                attempted_through_sample_id: latest_sample_id,
                activation_sample_id: latest_sample_id
                    .checked_add(1)
                    .ok_or(MfceError::Arithmetic)?,
                candidate_epoch: incumbent_epoch.map_or(1, |epoch| epoch.saturating_add(1)),
                incumbent_epoch,
            };
            self.state.last_retrain_attempt_sample_id = latest_sample_id;
            self.state.pending_training = Some(attempt.clone());
            attempt
        };
        let samples = self
            .state
            .samples
            .iter()
            .filter(|sample| sample.sample_id <= attempt.attempted_through_sample_id)
            .cloned()
            .collect::<Vec<_>>();
        if samples.len() < MFCE_MIN_TRAINING_SAMPLES {
            return Err(MfceError::InvalidState(
                "durable MFCE training prefix is no longer retained".into(),
            ));
        }
        let incumbent = self.state.incumbent.clone();
        if incumbent.as_ref().map(|model| model.epoch) != attempt.incumbent_epoch {
            return Err(MfceError::InvalidState(
                "MFCE incumbent changed during a durable training attempt".into(),
            ));
        }
        let next_epoch = attempt.candidate_epoch;
        let attempted_through_sample_id = attempt.attempted_through_sample_id;
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("mfce-lightgbm-trainer".into())
            .spawn(move || {
                let result = train_candidate(&samples, incumbent.as_ref(), next_epoch);
                let _ = sender.send(CompletedTraining {
                    attempted_through_sample_id,
                    result,
                });
            })
            .map_err(|error| MfceError::Model(error.to_string()))?;
        self.training = Some(receiver);
        Ok(true)
    }

    fn predict(
        &self,
        asset: &str,
        direction: MfceDirection,
        features: &MfceFeatureVector,
    ) -> Result<MfcePrediction, MfceError> {
        validate_features(features)?;
        if self.models.is_none() && self.state.samples.len() < MFCE_MIN_BACKOFF_SAMPLES {
            // Cold start must collect realized copytrade outcomes rather than
            // deadlock behind a model that cannot exist yet. These deliberately
            // conservative prior quantiles only size Explore; they cannot earn
            // Exploit or override the q10 portfolio tail budget.
            return Ok(MfcePrediction {
                model_epoch: 0,
                q10_gross_bps: MFCE_BOOTSTRAP_Q10_BPS,
                q50_gross_bps: MFCE_BOOTSTRAP_Q50_BPS,
                uncertainty_bps: MFCE_BOOTSTRAP_UNCERTAINTY_BPS,
                pooled_sample_count: self.state.samples.len() as u64,
                direction_sample_count: 0,
                asset_direction_sample_count: 0,
                used_model: false,
            });
        }
        let (base_quantiles, used_model) = match &self.models {
            Some(models) => {
                let values = features.as_f64()?;
                let prediction = models.predict_one(&values).map_err(model_error)?;
                (Some((prediction.q10, prediction.q50)), true)
            }
            None => (None, false),
        };
        let conditional = compose_conditional_quantiles(
            self.state.samples.iter(),
            asset,
            direction,
            base_quantiles,
        )?;
        let median_residuals = conditional
            .pooled_labels
            .iter()
            .map(|label| (label - conditional.pooled_q50).abs())
            .collect::<Vec<_>>();
        let robust_scale = empirical_quantile(&median_residuals, 0.50).unwrap_or_default();
        // Direction and asset cohorts are nested views of the pooled rows, not
        // independent evidence. Count each completed transition once and keep
        // a small epistemic prior so identical minimum-support labels cannot
        // collapse uncertainty to zero.
        let effective_support = conditional.pooled_labels.len().max(1);
        let quantile_width = conditional.q50 - conditional.q10;
        let uncertainty = 0.20 * quantile_width
            + robust_scale.max(5.0) / (effective_support as f64).sqrt()
            + if used_model {
                0.0
            } else {
                robust_scale.max(5.0) * 0.25
            };
        Ok(MfcePrediction {
            model_epoch: self.state.incumbent.as_ref().map_or(0, |model| model.epoch),
            q10_gross_bps: decimal(conditional.q10)?,
            q50_gross_bps: decimal(conditional.q50)?,
            uncertainty_bps: decimal(uncertainty.max(0.0))?,
            pooled_sample_count: conditional.pooled_labels.len() as u64,
            direction_sample_count: conditional.direction_sample_count as u64,
            asset_direction_sample_count: conditional.asset_direction_sample_count as u64,
            used_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ConditionalQuantiles {
    q10: f64,
    q50: f64,
    pooled_q50: f64,
    pooled_labels: Vec<f64>,
    direction_sample_count: usize,
    asset_direction_sample_count: usize,
}

/// Applies the exact pooled -> direction -> asset shrinkage path used for
/// every live prediction. Validation calls this with only the chronological
/// training prefix, so no holdout label can enter the served backoff values.
fn compose_conditional_quantiles<'a>(
    samples: impl IntoIterator<Item = &'a MfceTrainingSample>,
    asset: &str,
    direction: MfceDirection,
    base_quantiles: Option<(f64, f64)>,
) -> Result<ConditionalQuantiles, MfceError> {
    let mut pooled_labels = Vec::new();
    let mut direction_labels = Vec::new();
    let mut asset_direction_labels = Vec::new();
    for sample in samples {
        let label = sample
            .gross_return_bps
            .to_f64()
            .filter(|value| value.is_finite())
            .ok_or(MfceError::Arithmetic)?;
        pooled_labels.push(label);
        if sample.direction == direction {
            direction_labels.push(label);
            if sample.asset == asset {
                asset_direction_labels.push(label);
            }
        }
    }
    if pooled_labels.len() < MFCE_MIN_BACKOFF_SAMPLES {
        return Err(MfceError::InvalidState(
            "insufficient pooled MFCE support".into(),
        ));
    }

    let pooled_q10 = empirical_quantile(&pooled_labels, 0.10).ok_or(MfceError::Arithmetic)?;
    let pooled_q50 = empirical_quantile(&pooled_labels, 0.50).ok_or(MfceError::Arithmetic)?;
    let (base_q10, base_q50) = base_quantiles.unwrap_or((pooled_q10, pooled_q50));
    validate_quantile_pair(base_q10, base_q50)?;

    let direction_weight = shrinkage_weight(direction_labels.len(), MFCE_DIRECTION_SHRINKAGE);
    let direction_q10 = empirical_quantile(&direction_labels, 0.10).unwrap_or(pooled_q10);
    let direction_q50 = empirical_quantile(&direction_labels, 0.50).unwrap_or(pooled_q50);
    let q10_direction = blend(base_q10, direction_q10, direction_weight);
    let q50_direction = blend(base_q50, direction_q50, direction_weight);

    let asset_weight = shrinkage_weight(asset_direction_labels.len(), MFCE_ASSET_SHRINKAGE);
    let asset_q10 = empirical_quantile(&asset_direction_labels, 0.10).unwrap_or(q10_direction);
    let asset_q50 = empirical_quantile(&asset_direction_labels, 0.50).unwrap_or(q50_direction);
    let q10 = blend(q10_direction, asset_q10, asset_weight);
    let q50 = blend(q50_direction, asset_q50, asset_weight);
    validate_quantile_pair(q10, q50)?;

    Ok(ConditionalQuantiles {
        q10,
        q50,
        pooled_q50,
        pooled_labels,
        direction_sample_count: direction_labels.len(),
        asset_direction_sample_count: asset_direction_labels.len(),
    })
}

fn validate_quantile_pair(q10: f64, q50: f64) -> Result<(), MfceError> {
    if !q10.is_finite() || !q50.is_finite() {
        Err(MfceError::Model("non-finite MFCE prediction".into()))
    } else if q10 > q50 {
        Err(MfceError::Model(
            "crossed MFCE conditional quantiles".into(),
        ))
    } else {
        Ok(())
    }
}

pub fn evaluate_allocation_policy(
    input: &MfceAllocationInput,
) -> Result<MfceAllocationDecision, MfceError> {
    let requested_increment = requested_risk_increase(
        input.current_position_notional,
        input.proposed_position_notional,
    )?;
    let candidate = MfceCrossSectionalCandidate {
        asset: "single".into(),
        transition_id: 1,
        input: input.clone(),
        prior_tail_loss_usd: Decimal::ZERO,
    };
    let mut decisions = allocate_cross_sectional(
        &[candidate],
        requested_increment,
        input.remaining_tail_loss_budget_usd,
    )?;
    decisions
        .remove("single")
        .ok_or_else(|| MfceError::InvalidState("single MFCE allocation disappeared".into()))
}

/// Ranks one timestamp-consistent transition set and divides remaining hard
/// portfolio capacity among it. Proven Exploit states receive first claim;
/// Explore may borrow what remains, while the shared q10 tail-loss budget and
/// deterministic portfolio projection stay sovereign.
pub fn allocate_cross_sectional(
    candidates: &[MfceCrossSectionalCandidate],
    available_increment_notional: Decimal,
    remaining_tail_loss_budget_usd: Decimal,
) -> Result<BTreeMap<String, MfceAllocationDecision>, MfceError> {
    if available_increment_notional < Decimal::ZERO
        || remaining_tail_loss_budget_usd < Decimal::ZERO
    {
        return Err(MfceError::InvalidState(
            "negative MFCE portfolio allocation capacity".into(),
        ));
    }
    let mut decisions = BTreeMap::new();
    let mut requested = BTreeMap::new();
    for candidate in candidates {
        if decisions.contains_key(&candidate.asset) || candidate.prior_tail_loss_usd < Decimal::ZERO
        {
            return Err(MfceError::InvalidState(
                "duplicate or invalid MFCE cross-sectional candidate".into(),
            ));
        }
        let decision = evaluate_distribution(&candidate.input)?;
        let increment = requested_risk_increase(
            candidate.input.current_position_notional,
            candidate.input.proposed_position_notional,
        )?;
        requested.insert(candidate.asset.clone(), increment);
        decisions.insert(candidate.asset.clone(), decision);
    }

    let mut allocated_increment = BTreeMap::new();
    let mut remaining_notional = available_increment_notional;
    for state in [MfcePolicyState::Exploit, MfcePolicyState::Explore] {
        let mut group = Vec::new();
        for candidate in candidates {
            let decision = &decisions[&candidate.asset];
            if decision.policy_state != state {
                continue;
            }
            let weight = cross_sectional_weight(decision, &candidate.input)?;
            let cap = if state == MfcePolicyState::Explore {
                requested[&candidate.asset]
                    .checked_mul(explore_target_fraction(decision, &candidate.input)?)
                    .ok_or(MfceError::Arithmetic)?
            } else {
                requested[&candidate.asset]
            };
            group.push((candidate.asset.clone(), weight, cap));
        }
        let group_allocations = if state == MfcePolicyState::Explore {
            // Proportional dust across a large cold-start cross-section is not
            // useful exploration. Fund the highest-conviction small probes up
            // to their individual caps, then continue down the stable ranking.
            ranked_capped_allocations(&group, remaining_notional)?
        } else {
            weighted_capped_allocations(&group, remaining_notional)?
        };
        let group_usage = group_allocations
            .values()
            .try_fold(Decimal::ZERO, |total, allocation| {
                total.checked_add(*allocation).ok_or(MfceError::Arithmetic)
            })?;
        remaining_notional = remaining_notional
            .checked_sub(group_usage)
            .ok_or(MfceError::Arithmetic)?;
        for (asset, allocation) in group_allocations {
            allocated_increment.insert(asset, allocation);
        }
    }

    for candidate in candidates {
        let decision = decisions
            .get_mut(&candidate.asset)
            .ok_or_else(|| MfceError::InvalidState("missing MFCE decision".into()))?;
        if decision.policy_state == MfcePolicyState::Reject {
            continue;
        }
        let request = requested[&candidate.asset];
        let allocation = allocated_increment
            .get(&candidate.asset)
            .copied()
            .unwrap_or_default();
        if request.is_zero() || allocation.is_zero() {
            decision.admitted = false;
            decision.reason = Some(MfceRejectionReason::AllocationBudgetExhausted);
            decision.allocation_fraction = Decimal::ZERO;
            decision.modeled_tail_loss_usd = Decimal::ZERO;
            continue;
        }
        decision.allocation_fraction = conservative_ratio(allocation, request)?;
    }

    // Tail capacity is sovereign. Consume it in economic rank order so the
    // best after-cost distributions receive scarce downside capacity first.
    let mut ranked = candidates.iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        let left_decision = &decisions[&left.asset];
        let right_decision = &decisions[&right.asset];
        policy_rank(right_decision.policy_state)
            .cmp(&policy_rank(left_decision.policy_state))
            .then_with(|| {
                right_decision
                    .opportunity_score
                    .cmp(&left_decision.opportunity_score)
            })
            .then_with(|| left.asset.cmp(&right.asset))
            .then_with(|| left.transition_id.cmp(&right.transition_id))
    });
    let mut remaining_tail = remaining_tail_loss_budget_usd;
    for candidate in ranked {
        let decision = decisions
            .get_mut(&candidate.asset)
            .ok_or_else(|| MfceError::InvalidState("missing ranked MFCE decision".into()))?;
        if decision.policy_state == MfcePolicyState::Reject
            || decision.allocation_fraction.is_zero()
        {
            continue;
        }
        let tail_rate_bps = (-decision.net_q10_bps).max(Decimal::ZERO);
        if tail_rate_bps.is_zero() {
            decision.admitted = true;
            continue;
        }
        let maximum_total_tail = candidate
            .prior_tail_loss_usd
            .checked_add(remaining_tail)
            .ok_or(MfceError::Arithmetic)?;
        let maximum_target_notional = maximum_total_tail
            .checked_mul(BPS_PER_UNIT_RETURN)
            .and_then(|value| value.checked_div(tail_rate_bps))
            .ok_or(MfceError::Arithmetic)?;
        let tail_fraction = maximum_allocation_fraction(
            candidate.input.current_position_notional,
            candidate.input.proposed_position_notional,
            maximum_target_notional,
        )?;
        decision.allocation_fraction = decision.allocation_fraction.min(tail_fraction);
        if decision.allocation_fraction.is_zero() {
            reject_decision(decision, MfceRejectionReason::TailBudgetExceeded);
            continue;
        }
        let target = allocated_target(
            candidate.input.current_position_notional,
            candidate.input.proposed_position_notional,
            decision.allocation_fraction,
        )?;
        let total_tail = target
            .abs()
            .checked_mul(tail_rate_bps)
            .and_then(|value| value.checked_div(BPS_PER_UNIT_RETURN))
            .ok_or(MfceError::Arithmetic)?;
        let incremental_tail = total_tail
            .checked_sub(candidate.prior_tail_loss_usd)
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
        remaining_tail = remaining_tail
            .checked_sub(incremental_tail)
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
        decision.modeled_tail_loss_usd = total_tail;
        decision.admitted = true;
    }
    Ok(decisions)
}

fn evaluate_distribution(input: &MfceAllocationInput) -> Result<MfceAllocationDecision, MfceError> {
    if input.friction_bps < Decimal::ZERO
        || !(Decimal::ZERO..=Decimal::ONE).contains(&input.copytrade_conviction)
        || input.prediction.uncertainty_bps < Decimal::ZERO
        || input.proposed_position_notional.is_zero()
        || input.remaining_tail_loss_budget_usd < Decimal::ZERO
        || input.prediction.q10_gross_bps > input.prediction.q50_gross_bps
    {
        return Ok(MfceAllocationDecision {
            policy_state: MfcePolicyState::Reject,
            admitted: false,
            reason: Some(MfceRejectionReason::InvalidPrediction),
            allocation_fraction: Decimal::ZERO,
            opportunity_score: Decimal::MIN,
            net_q50_bps: Decimal::MIN,
            conservative_edge_bps: Decimal::MIN,
            net_q10_bps: Decimal::MIN,
            required_median_bps: Decimal::MAX,
            modeled_tail_loss_usd: Decimal::MAX,
        });
    }
    let required_median_bps = input
        .friction_bps
        .checked_add(input.prediction.uncertainty_bps)
        .ok_or(MfceError::Arithmetic)?;
    let net_q50_bps = input
        .prediction
        .q50_gross_bps
        .checked_sub(input.friction_bps)
        .ok_or(MfceError::Arithmetic)?;
    let conservative_edge_bps = net_q50_bps
        .checked_sub(input.prediction.uncertainty_bps)
        .ok_or(MfceError::Arithmetic)?;
    let net_q10_bps = input
        .prediction
        .q10_gross_bps
        .checked_sub(input.friction_bps)
        .ok_or(MfceError::Arithmetic)?;
    let downside_width_bps = input
        .prediction
        .q50_gross_bps
        .checked_sub(input.prediction.q10_gross_bps)
        .ok_or(MfceError::Arithmetic)?
        .max(MFCE_SCORE_EPSILON_BPS);
    let opportunity_score = conservative_edge_bps
        .max(Decimal::ZERO)
        .checked_div(downside_width_bps)
        .ok_or(MfceError::Arithmetic)?;

    let upper_edge_bps = net_q50_bps
        .checked_add(input.prediction.uncertainty_bps)
        .ok_or(MfceError::Arithmetic)?;
    let (policy_state, reason) = if !input.prediction.used_model {
        // Without a promoted conditional model, every structurally valid
        // source transition explores. Empirical backoff still shrinks size,
        // but it cannot create a cold-start no-trade fixed point.
        (MfcePolicyState::Explore, None)
    } else if upper_edge_bps <= Decimal::ZERO {
        (
            MfcePolicyState::Reject,
            Some(MfceRejectionReason::StrongNegativeExpectancy),
        )
    } else if conservative_edge_bps <= Decimal::ZERO {
        (MfcePolicyState::Explore, None)
    } else {
        (MfcePolicyState::Exploit, None)
    };
    Ok(MfceAllocationDecision {
        policy_state,
        admitted: false,
        reason,
        allocation_fraction: Decimal::ZERO,
        opportunity_score,
        net_q50_bps,
        conservative_edge_bps,
        net_q10_bps,
        required_median_bps,
        modeled_tail_loss_usd: Decimal::ZERO,
    })
}

fn reject_decision(decision: &mut MfceAllocationDecision, reason: MfceRejectionReason) {
    decision.policy_state = MfcePolicyState::Reject;
    decision.admitted = false;
    decision.reason = Some(reason);
    decision.allocation_fraction = Decimal::ZERO;
    decision.modeled_tail_loss_usd = Decimal::ZERO;
}

fn cross_sectional_weight(
    decision: &MfceAllocationDecision,
    input: &MfceAllocationInput,
) -> Result<Decimal, MfceError> {
    let weight = match decision.policy_state {
        MfcePolicyState::Exploit => decision.opportunity_score,
        MfcePolicyState::Explore => explore_target_fraction(decision, input)?,
        MfcePolicyState::Reject => Decimal::ZERO,
    };
    Ok(weight.max(MFCE_SCORE_EPSILON_BPS / BPS_PER_UNIT_RETURN))
}

fn explore_target_fraction(
    decision: &MfceAllocationDecision,
    input: &MfceAllocationInput,
) -> Result<Decimal, MfceError> {
    let conviction_prior = MFCE_MIN_EXPLORE_PRIOR_FRACTION
        .checked_add(
            MFCE_EXPLORE_CONVICTION_RANGE
                .checked_mul(input.copytrade_conviction)
                .ok_or(MfceError::Arithmetic)?,
        )
        .ok_or(MfceError::Arithmetic)?;
    let downside = (-decision.net_q10_bps).max(Decimal::ZERO);
    let risk_denominator = MFCE_EXPLORE_RISK_SCALE_BPS
        .checked_add(input.prediction.uncertainty_bps)
        .and_then(|value| value.checked_add(downside))
        .ok_or(MfceError::Arithmetic)?;
    let risk_multiplier = MFCE_EXPLORE_RISK_SCALE_BPS
        .checked_div(risk_denominator)
        .ok_or(MfceError::Arithmetic)?;
    conviction_prior
        .checked_mul(risk_multiplier)
        .ok_or(MfceError::Arithmetic)
        .map(|fraction| fraction.clamp(Decimal::ZERO, MFCE_EXPLORE_POOL_FRACTION))
}

const fn policy_rank(state: MfcePolicyState) -> u8 {
    match state {
        MfcePolicyState::Exploit => 2,
        MfcePolicyState::Explore => 1,
        MfcePolicyState::Reject => 0,
    }
}

fn requested_risk_increase(current: Decimal, proposed: Decimal) -> Result<Decimal, MfceError> {
    proposed
        .abs()
        .checked_sub(rejected_target(current, proposed).abs())
        .ok_or(MfceError::Arithmetic)
        .map(|value| value.max(Decimal::ZERO))
}

fn maximum_allocation_fraction(
    current: Decimal,
    proposed: Decimal,
    maximum_target_notional: Decimal,
) -> Result<Decimal, MfceError> {
    if maximum_target_notional <= Decimal::ZERO || proposed.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let fraction = if !current.is_zero()
        && current.is_sign_positive() == proposed.is_sign_positive()
        && proposed.abs() > current.abs()
    {
        conservative_ratio(
            maximum_target_notional
                .checked_sub(current.abs())
                .unwrap_or(Decimal::MIN)
                .max(Decimal::ZERO),
            proposed
                .abs()
                .checked_sub(current.abs())
                .ok_or(MfceError::Arithmetic)?,
        )?
    } else {
        conservative_ratio(maximum_target_notional, proposed.abs())?
    };
    Ok(fraction.clamp(Decimal::ZERO, Decimal::ONE))
}

fn conservative_ratio(numerator: Decimal, denominator: Decimal) -> Result<Decimal, MfceError> {
    if numerator <= Decimal::ZERO || denominator <= Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }
    let mut ratio = numerator
        .checked_div(denominator)
        .ok_or(MfceError::Arithmetic)?
        .clamp(Decimal::ZERO, Decimal::ONE);
    if denominator
        .checked_mul(ratio)
        .ok_or(MfceError::Arithmetic)?
        > numerator
    {
        ratio = ratio
            .checked_sub(Decimal::from_parts(1, 0, 0, false, 28))
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
    }
    Ok(ratio)
}

fn ranked_capped_allocations(
    candidates: &[(String, Decimal, Decimal)],
    budget: Decimal,
) -> Result<BTreeMap<String, Decimal>, MfceError> {
    let mut ranked = candidates.to_vec();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let mut remaining = budget.max(Decimal::ZERO);
    let mut output = BTreeMap::new();
    for (asset, _, cap) in ranked {
        let allocation = cap.max(Decimal::ZERO).min(remaining);
        output.insert(asset, allocation);
        remaining = remaining
            .checked_sub(allocation)
            .ok_or(MfceError::Arithmetic)?;
    }
    Ok(output)
}

fn weighted_capped_allocations(
    candidates: &[(String, Decimal, Decimal)],
    budget: Decimal,
) -> Result<BTreeMap<String, Decimal>, MfceError> {
    let mut output = candidates
        .iter()
        .map(|(asset, _, _)| (asset.clone(), Decimal::ZERO))
        .collect::<BTreeMap<_, _>>();
    let mut active = candidates
        .iter()
        .filter(|(_, weight, cap)| *weight > Decimal::ZERO && *cap > Decimal::ZERO)
        .cloned()
        .collect::<Vec<_>>();
    active.sort_by(|left, right| left.0.cmp(&right.0));
    let mut remaining = budget.max(Decimal::ZERO);
    while !active.is_empty() && remaining > Decimal::ZERO {
        let total_weight = active
            .iter()
            .try_fold(Decimal::ZERO, |total, (_, weight, _)| {
                total.checked_add(*weight).ok_or(MfceError::Arithmetic)
            })?;
        if total_weight <= Decimal::ZERO {
            break;
        }
        let mut capped_any = false;
        let mut next = Vec::new();
        let round_budget = remaining;
        for (asset, weight, cap) in active {
            let already = output[&asset];
            let capacity = cap
                .checked_sub(already)
                .ok_or(MfceError::Arithmetic)?
                .max(Decimal::ZERO);
            let share = round_budget
                .checked_mul(weight)
                .and_then(|value| value.checked_div(total_weight))
                .ok_or(MfceError::Arithmetic)?;
            let allocation = share.min(capacity).min(remaining);
            output.insert(
                asset.clone(),
                already
                    .checked_add(allocation)
                    .ok_or(MfceError::Arithmetic)?,
            );
            remaining = remaining
                .checked_sub(allocation)
                .ok_or(MfceError::Arithmetic)?;
            if allocation == capacity {
                capped_any = true;
            } else if capacity > allocation {
                next.push((asset, weight, cap));
            }
        }
        if !capped_any {
            break;
        }
        active = next;
    }
    if remaining > Decimal::ZERO {
        let mut residual_order = candidates.to_vec();
        residual_order
            .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (asset, _, cap) in residual_order {
            let already = output[&asset];
            let capacity = cap
                .checked_sub(already)
                .ok_or(MfceError::Arithmetic)?
                .max(Decimal::ZERO);
            let allocation = capacity.min(remaining);
            output.insert(
                asset,
                already
                    .checked_add(allocation)
                    .ok_or(MfceError::Arithmetic)?,
            );
            remaining = remaining
                .checked_sub(allocation)
                .ok_or(MfceError::Arithmetic)?;
            if remaining.is_zero() {
                break;
            }
        }
    }
    Ok(output)
}

fn allocated_target(
    current: Decimal,
    proposed: Decimal,
    allocation_fraction: Decimal,
) -> Result<Decimal, MfceError> {
    if allocation_fraction <= Decimal::ZERO || proposed.is_zero() {
        return Ok(rejected_target(current, proposed));
    }
    let fraction = allocation_fraction.min(Decimal::ONE);
    if current.is_zero() || current.is_sign_positive() != proposed.is_sign_positive() {
        return proposed.checked_mul(fraction).ok_or(MfceError::Arithmetic);
    }
    let increment = proposed.checked_sub(current).ok_or(MfceError::Arithmetic)?;
    current
        .checked_add(
            increment
                .checked_mul(fraction)
                .ok_or(MfceError::Arithmetic)?,
        )
        .ok_or(MfceError::Arithmetic)
}

/// Returns the position-preserving/reducing target that requires no new MFCE
/// risk allocation. Cross-sectional capacity is measured above this baseline
/// so an incumbent candidate can be ranked again without counting its prior
/// allocation twice.
pub fn allocation_baseline_target(current: Decimal, proposed: Decimal) -> Decimal {
    rejected_target(current, proposed)
}

fn train_candidate(
    samples: &[MfceTrainingSample],
    incumbent: Option<&MfceModelState>,
    epoch: u64,
) -> Result<Option<MfceModelState>, MfceError> {
    if samples.len() < MFCE_MIN_TRAINING_SAMPLES {
        return Ok(None);
    }
    let mut chronological = samples.to_vec();
    chronological.sort_by_key(|sample| (sample.completed_at_mono, sample.sample_id));
    let validation_len = (chronological.len() / MFCE_VALIDATION_DENOMINATOR).max(1);
    let training_len = chronological.len().saturating_sub(validation_len);
    if training_len < copytrade_mfce::MIN_TRAINING_ROWS {
        return Ok(None);
    }
    let (training, validation) = chronological.split_at(training_len);
    let training_features = flatten_features(training)?;
    let training_labels = labels_f64(training)?;
    let candidate_strings = copytrade_mfce::train_quantile_pair(
        &training_features,
        &training_labels,
        None,
        MFCE_FEATURE_COUNT,
    )
    .map_err(model_error)?;
    let candidate = copytrade_mfce::QuantileModelPair::from_model_strings(&candidate_strings)
        .map_err(model_error)?;
    let validation_features = flatten_features(validation)?;
    let validation_labels = labels_f64(validation)?;
    let candidate_raw_predictions = candidate
        .predict(&validation_features)
        .map_err(model_error)?;
    let candidate_predictions =
        match served_validation_predictions(training, validation, Some(&candidate_raw_predictions))
        {
            Ok(predictions) => predictions,
            // A non-finite or crossed candidate pair is not promotable. Keep the
            // incumbent unchanged instead of attempting to repair the quantiles.
            Err(MfceError::Model(_)) => return Ok(None),
            Err(error) => return Err(error),
        };

    let incumbent_raw_predictions = match incumbent {
        Some(model) => {
            let strings = copytrade_mfce::QuantileModelStrings::new(
                MFCE_FEATURE_COUNT,
                model.q10_model.clone(),
                model.q50_model.clone(),
            )
            .map_err(model_error)?;
            let models = copytrade_mfce::QuantileModelPair::from_model_strings(&strings)
                .map_err(model_error)?;
            models.predict(&validation_features).map_err(model_error)?
        }
        None => Vec::new(),
    };
    let incumbent_predictions = match served_validation_predictions(
        training,
        validation,
        if incumbent.is_some() {
            Some(incumbent_raw_predictions.as_slice())
        } else {
            None
        },
    ) {
        Ok(predictions) => predictions,
        Err(MfceError::Model(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let validation_score = chronological_validation_score(
        &validation_labels,
        &candidate_predictions,
        &incumbent_predictions,
    )?;
    let validation_passes = if incumbent.is_some() {
        validation_score.passes_incumbent_comparison(validation.len())
    } else {
        validation_score.passes_cold_start(validation.len())
    };
    if !validation_passes {
        return Ok(None);
    }
    if candidate_strings.q10().len() > MFCE_MAX_MODEL_BYTES
        || candidate_strings.q50().len() > MFCE_MAX_MODEL_BYTES
    {
        return Err(MfceError::Model("MFCE model string bound exceeded".into()));
    }
    let (_, q10_model, q50_model) = candidate_strings.into_parts();
    let trained_through_sample_id = training
        .last()
        .map(|sample| sample.sample_id)
        .ok_or(MfceError::Arithmetic)?;
    Ok(Some(MfceModelState {
        epoch,
        trained_through_sample_id,
        feature_version: MFCE_FEATURE_VERSION,
        feature_count: MFCE_FEATURE_COUNT as u32,
        q10_sha256: sha256_hex(q10_model.as_bytes()),
        q50_sha256: sha256_hex(q50_model.as_bytes()),
        q10_model,
        q50_model,
        validation: MfceValidationSummary {
            validation_samples: validation.len() as u64,
            candidate_q10_pinball_bps: decimal(validation_score.candidate_q10_pinball)?,
            candidate_q50_pinball_bps: decimal(validation_score.candidate_q50_pinball)?,
            incumbent_q10_pinball_bps: decimal(validation_score.incumbent_q10_pinball)?,
            incumbent_q50_pinball_bps: decimal(validation_score.incumbent_q50_pinball)?,
            q10_below_label_fraction: decimal(validation_score.candidate_q10_below_fraction)?,
        },
    }))
}

fn served_validation_predictions(
    training_prefix: &[MfceTrainingSample],
    validation: &[MfceTrainingSample],
    raw_model_predictions: Option<&[copytrade_mfce::QuantilePrediction]>,
) -> Result<Vec<copytrade_mfce::QuantilePrediction>, MfceError> {
    if raw_model_predictions.is_some_and(|predictions| predictions.len() != validation.len()) {
        return Err(MfceError::InvalidState(
            "MFCE validation prediction shape mismatch".into(),
        ));
    }
    validation
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let base_quantiles = raw_model_predictions
                .map(|predictions| (predictions[index].q10, predictions[index].q50));
            let conditional = compose_conditional_quantiles(
                training_prefix.iter(),
                &sample.asset,
                sample.direction,
                base_quantiles,
            )?;
            Ok(copytrade_mfce::QuantilePrediction {
                q10: conditional.q10,
                q50: conditional.q50,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ChronologicalValidationScore {
    candidate_q10_pinball: f64,
    candidate_q50_pinball: f64,
    incumbent_q10_pinball: f64,
    incumbent_q50_pinball: f64,
    candidate_q10_below_fraction: f64,
    candidate_q50_below_fraction: f64,
    incumbent_q10_below_fraction: f64,
}

impl ChronologicalValidationScore {
    fn q10_is_calibrated(self, validation_samples: usize) -> bool {
        if validation_samples == 0 {
            return false;
        }
        let loss_tolerance = 1e-9;
        let finite_sample_tolerance = 1.0 / validation_samples as f64;
        self.candidate_q10_below_fraction <= 0.10 + finite_sample_tolerance + loss_tolerance
    }

    fn passes_cold_start(self, validation_samples: usize) -> bool {
        if validation_samples == 0 || !self.q10_is_calibrated(validation_samples) {
            return false;
        }
        let finite_sample_tolerance = 1.0 / validation_samples as f64;
        let median_tolerance = 0.25 + finite_sample_tolerance;
        self.candidate_q10_pinball.is_finite()
            && self.candidate_q50_pinball.is_finite()
            && self.candidate_q10_pinball >= 0.0
            && self.candidate_q50_pinball >= 0.0
            && (self.candidate_q50_below_fraction - 0.50).abs() <= median_tolerance
    }

    fn passes_incumbent_comparison(self, validation_samples: usize) -> bool {
        if validation_samples == 0 {
            return false;
        }
        let loss_tolerance = 1e-9;
        let finite_sample_tolerance = 1.0 / validation_samples as f64;
        let loss_non_inferior = self.candidate_q10_pinball
            <= self.incumbent_q10_pinball + loss_tolerance
            && self.candidate_q50_pinball <= self.incumbent_q50_pinball + loss_tolerance
            && self.candidate_q10_pinball + self.candidate_q50_pinball + loss_tolerance
                < self.incumbent_q10_pinball + self.incumbent_q50_pinball;
        // For a calibrated lower decile, no more than 10% of labels should
        // fall below q10. One holdout observation is allowed for finite-sample
        // resolution, but the candidate also cannot under-cover the incumbent
        // by more than that same single-observation tolerance.
        let q10_non_inferior = self.candidate_q10_below_fraction
            <= self.incumbent_q10_below_fraction + finite_sample_tolerance + loss_tolerance;
        loss_non_inferior && self.q10_is_calibrated(validation_samples) && q10_non_inferior
    }
}

fn chronological_validation_score(
    labels: &[f64],
    candidate: &[copytrade_mfce::QuantilePrediction],
    incumbent: &[copytrade_mfce::QuantilePrediction],
) -> Result<ChronologicalValidationScore, MfceError> {
    if labels.is_empty() || labels.len() != candidate.len() || labels.len() != incumbent.len() {
        return Err(MfceError::InvalidState(
            "MFCE validation score shape mismatch".into(),
        ));
    }
    for prediction in candidate.iter().chain(incumbent) {
        validate_quantile_pair(prediction.q10, prediction.q50)?;
    }
    let candidate_q10 = candidate.iter().map(|value| value.q10).collect::<Vec<_>>();
    let candidate_q50 = candidate.iter().map(|value| value.q50).collect::<Vec<_>>();
    let incumbent_q10 = incumbent.iter().map(|value| value.q10).collect::<Vec<_>>();
    let incumbent_q50 = incumbent.iter().map(|value| value.q50).collect::<Vec<_>>();
    let below_fraction = |predictions: &[f64]| {
        labels
            .iter()
            .zip(predictions)
            .filter(|(label, prediction)| **label < **prediction)
            .count() as f64
            / labels.len() as f64
    };
    Ok(ChronologicalValidationScore {
        candidate_q10_pinball: pinball_loss(labels, &candidate_q10, 0.10)?,
        candidate_q50_pinball: pinball_loss(labels, &candidate_q50, 0.50)?,
        incumbent_q10_pinball: pinball_loss(labels, &incumbent_q10, 0.10)?,
        incumbent_q50_pinball: pinball_loss(labels, &incumbent_q50, 0.50)?,
        candidate_q10_below_fraction: below_fraction(&candidate_q10),
        candidate_q50_below_fraction: below_fraction(&candidate_q50),
        incumbent_q10_below_fraction: below_fraction(&incumbent_q10),
    })
}

fn complete_active(
    samples: &mut VecDeque<MfceTrainingSample>,
    next_sample_id: &mut u64,
    asset: &str,
    active: Option<MfceActiveTransition>,
    exit_midpoint: Decimal,
    now: u64,
) -> Result<Option<u64>, MfceError> {
    let Some(active) = active else {
        return Ok(None);
    };
    if now < active.entry_timestamp_mono {
        return Err(MfceError::InvalidState("MFCE clock regressed".into()));
    }
    let unsigned_return_bps = exit_midpoint
        .checked_sub(active.entry_midpoint)
        .and_then(|delta| delta.checked_div(active.entry_midpoint))
        .and_then(|fraction| fraction.checked_mul(BPS_PER_UNIT_RETURN))
        .ok_or(MfceError::Arithmetic)?;
    let gross_return_bps = match active.direction {
        MfceDirection::Long => unsigned_return_bps,
        MfceDirection::Short => -unsigned_return_bps,
    };
    let lifetime_seconds = Decimal::from(now.saturating_sub(active.entry_timestamp_mono))
        .checked_div(Decimal::from(1_000))
        .ok_or(MfceError::Arithmetic)?;
    let sample_id = *next_sample_id;
    *next_sample_id = (*next_sample_id)
        .checked_add(1)
        .ok_or(MfceError::Arithmetic)?;
    samples.push_back(MfceTrainingSample {
        sample_id,
        asset: asset.to_string(),
        direction: active.direction,
        transition_kind: active.transition_kind,
        opened_at_mono: active.entry_timestamp_mono,
        completed_at_mono: now,
        features: active.features,
        gross_return_bps,
        lifetime_seconds,
        admitted: active.admitted,
    });
    while samples.len() > MFCE_MAX_SAMPLES {
        samples.pop_front();
    }
    Ok(Some(sample_id))
}

fn effective_target(
    admission: &MfceAdmissionState,
    approved_target: Decimal,
    current_target: Decimal,
    desired_target: Decimal,
) -> Decimal {
    match admission {
        MfceAdmissionState::Admitted { .. } => {
            cap_to_approved_target(approved_target, desired_target)
        }
        MfceAdmissionState::Bypass
        | MfceAdmissionState::AwaitingLiveBook { .. }
        | MfceAdmissionState::Rejected { .. } => rejected_target(current_target, desired_target),
    }
}

fn cap_to_approved_target(approved: Decimal, desired: Decimal) -> Decimal {
    if approved.is_zero()
        || desired.is_zero()
        || approved.is_sign_positive() != desired.is_sign_positive()
    {
        Decimal::ZERO
    } else if desired.abs() <= approved.abs() {
        desired
    } else {
        approved
    }
}

fn rejected_target(current: Decimal, desired: Decimal) -> Decimal {
    if current.is_zero()
        || desired.is_zero()
        || current.is_sign_positive() != desired.is_sign_positive()
    {
        Decimal::ZERO
    } else if desired.abs() < current.abs() {
        desired
    } else {
        current
    }
}

fn transition_id(admission: &MfceAdmissionState) -> Option<u64> {
    match admission {
        MfceAdmissionState::Bypass => None,
        MfceAdmissionState::AwaitingLiveBook { transition_id }
        | MfceAdmissionState::Admitted { transition_id, .. }
        | MfceAdmissionState::Rejected { transition_id, .. } => Some(*transition_id),
    }
}

fn transition_kind(previous: Decimal, desired: Decimal) -> MfceTransitionKind {
    if previous.is_zero() {
        MfceTransitionKind::Opening
    } else if previous.is_sign_positive() != desired.is_sign_positive() {
        MfceTransitionKind::Reversal
    } else {
        MfceTransitionKind::Expansion
    }
}

fn source_target_adds_risk(current: Decimal, desired: Decimal) -> bool {
    !desired.is_zero()
        && (current.is_zero()
            || current.is_sign_positive() != desired.is_sign_positive()
            || desired.abs() > current.abs())
}

fn validate_asset(asset: &str) -> Result<(), MfceError> {
    if asset.is_empty() || asset.len() > 64 {
        Err(MfceError::InvalidState("invalid MFCE asset".into()))
    } else {
        Ok(())
    }
}

fn validate_features(features: &MfceFeatureVector) -> Result<(), MfceError> {
    features.as_f64().map(|_| ())
}

fn validate_model_state(model: &MfceModelState) -> Result<(), MfceError> {
    if model.epoch == 0
        || model.trained_through_sample_id == 0
        || model.feature_version != MFCE_FEATURE_VERSION
        || model.feature_count != MFCE_FEATURE_COUNT as u32
        || model.q10_model.is_empty()
        || model.q50_model.is_empty()
        || model.q10_model.len() > MFCE_MAX_MODEL_BYTES
        || model.q50_model.len() > MFCE_MAX_MODEL_BYTES
        || model.q10_model.as_bytes().contains(&0)
        || model.q50_model.as_bytes().contains(&0)
        || model.q10_sha256 != sha256_hex(model.q10_model.as_bytes())
        || model.q50_sha256 != sha256_hex(model.q50_model.as_bytes())
        || model.validation.validation_samples == 0
        || model.validation.candidate_q10_pinball_bps < Decimal::ZERO
        || model.validation.candidate_q50_pinball_bps < Decimal::ZERO
        || model.validation.incumbent_q10_pinball_bps < Decimal::ZERO
        || model.validation.incumbent_q50_pinball_bps < Decimal::ZERO
        || !(Decimal::ZERO..=Decimal::ONE).contains(&model.validation.q10_below_label_fraction)
    {
        return Err(MfceError::InvalidState("invalid MFCE model state".into()));
    }
    Ok(())
}

fn flatten_features(samples: &[MfceTrainingSample]) -> Result<Vec<f64>, MfceError> {
    let mut values = Vec::with_capacity(samples.len() * MFCE_FEATURE_COUNT);
    for sample in samples {
        values.extend(sample.features.as_f64()?);
    }
    Ok(values)
}

fn labels_f64(samples: &[MfceTrainingSample]) -> Result<Vec<f64>, MfceError> {
    decimals_as_f64(
        &samples
            .iter()
            .map(|sample| sample.gross_return_bps)
            .collect::<Vec<_>>(),
    )
}

fn decimals_as_f64(values: &[Decimal]) -> Result<Vec<f64>, MfceError> {
    values
        .iter()
        .map(|value| {
            value
                .to_f64()
                .filter(|value| value.is_finite())
                .ok_or(MfceError::Arithmetic)
        })
        .collect()
}

fn empirical_quantile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() || !(0.0..=1.0).contains(&quantile) {
        return None;
    }
    let mut sorted = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(f64::total_cmp);
    let position = quantile * (sorted.len().saturating_sub(1) as f64);
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let weight = position - lower as f64;
    Some(sorted[lower] * (1.0 - weight) + sorted[upper] * weight)
}

fn pinball_loss(labels: &[f64], predictions: &[f64], alpha: f64) -> Result<f64, MfceError> {
    if labels.is_empty() || labels.len() != predictions.len() || !(0.0..1.0).contains(&alpha) {
        return Err(MfceError::InvalidState("invalid pinball loss input".into()));
    }
    let total = labels
        .iter()
        .zip(predictions)
        .try_fold(0.0, |sum, (label, prediction)| {
            if !label.is_finite() || !prediction.is_finite() {
                return None;
            }
            let residual = label - prediction;
            Some(
                sum + if residual >= 0.0 {
                    alpha * residual
                } else {
                    (alpha - 1.0) * residual
                },
            )
        })
        .ok_or(MfceError::Arithmetic)?;
    Ok(total / labels.len() as f64)
}

fn shrinkage_weight(count: usize, prior_strength: f64) -> f64 {
    count as f64 / (count as f64 + prior_strength)
}

fn blend(prior: f64, local: f64, local_weight: f64) -> f64 {
    prior * (1.0 - local_weight) + local * local_weight
}

fn decimal(value: f64) -> Result<Decimal, MfceError> {
    Decimal::from_f64(value)
        .filter(|_| value.is_finite())
        .ok_or(MfceError::Arithmetic)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn model_error(error: copytrade_mfce::MfceError) -> MfceError {
    MfceError::Model(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn features(conviction: i64) -> MfceFeatureVector {
        let mut values = [Decimal::ZERO; MFCE_FEATURE_COUNT];
        values[0] = Decimal::from(conviction);
        MfceFeatureVector::new(values)
    }

    fn training_sample(
        sample_id: u64,
        asset: &str,
        direction: MfceDirection,
        gross_return_bps: i64,
    ) -> MfceTrainingSample {
        MfceTrainingSample {
            sample_id,
            asset: asset.to_string(),
            direction,
            transition_kind: MfceTransitionKind::Opening,
            opened_at_mono: sample_id * 1_000,
            completed_at_mono: sample_id * 1_000 + 500,
            features: features(gross_return_bps),
            gross_return_bps: Decimal::from(gross_return_bps),
            lifetime_seconds: Decimal::new(5, 1),
            admitted: sample_id % 2 == 0,
        }
    }

    #[test]
    fn rejected_and_admitted_transitions_use_identical_gross_labels() {
        let run = |admitted: bool| {
            let mut engine = MfceEngine::default();
            engine.note_accepted_source_snapshot().unwrap();
            let opened = engine
                .observe_raw_source(
                    "BTC",
                    true,
                    Decimal::new(5, 1),
                    Decimal::from(50),
                    Decimal::ZERO,
                    Decimal::from(100),
                    1_000,
                    features(5),
                )
                .unwrap();
            let transition_id = opened.transition_id.unwrap();
            if admitted {
                let prediction = MfcePrediction {
                    model_epoch: 0,
                    q10_gross_bps: Decimal::from(10),
                    q50_gross_bps: Decimal::from(20),
                    uncertainty_bps: Decimal::ZERO,
                    pooled_sample_count: 8,
                    direction_sample_count: 8,
                    asset_direction_sample_count: 8,
                    used_model: false,
                };
                engine
                    .record_allocation(
                        "BTC",
                        transition_id,
                        &MfceAllocationDecision {
                            policy_state: MfcePolicyState::Exploit,
                            admitted: true,
                            reason: None,
                            allocation_fraction: Decimal::ONE,
                            opportunity_score: Decimal::ONE,
                            net_q50_bps: Decimal::from(20),
                            conservative_edge_bps: Decimal::from(20),
                            net_q10_bps: Decimal::from(10),
                            required_median_bps: Decimal::ZERO,
                            modeled_tail_loss_usd: Decimal::ZERO,
                        },
                        &prediction,
                        Decimal::ZERO,
                    )
                    .unwrap();
            }
            engine.note_accepted_source_snapshot().unwrap();
            engine
                .observe_raw_source(
                    "BTC",
                    true,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    if admitted {
                        Decimal::from(50)
                    } else {
                        Decimal::ZERO
                    },
                    Decimal::from(110),
                    61_000,
                    features(0),
                )
                .unwrap();
            engine.state.samples.back().cloned().unwrap()
        };

        let rejected = run(false);
        let admitted = run(true);
        assert_eq!(rejected.gross_return_bps, Decimal::from(1_000));
        assert_eq!(rejected.gross_return_bps, admitted.gross_return_bps);
        assert_eq!(rejected.lifetime_seconds, admitted.lifetime_seconds);
        assert!(!rejected.admitted);
        assert!(admitted.admitted);
    }

    #[test]
    fn rejected_reversal_flattens_but_rejected_expansion_holds() {
        assert_eq!(
            rejected_target(Decimal::from(40), Decimal::from(80)),
            Decimal::from(40)
        );
        assert_eq!(
            rejected_target(Decimal::from(40), Decimal::from(-80)),
            Decimal::ZERO
        );
        assert_eq!(
            rejected_target(Decimal::from(40), Decimal::from(20)),
            Decimal::from(20)
        );
    }

    #[test]
    fn only_accepted_source_changes_generate_candidates_and_reductions_bypass() {
        let mut engine = MfceEngine::default();

        let unaccepted = engine
            .observe_raw_source(
                "BTC",
                false,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::ZERO,
                Decimal::from(100),
                1_000,
                features(5),
            )
            .unwrap();
        assert!(!unaccepted.needs_live_book);
        assert_eq!(unaccepted.effective_target, Decimal::ZERO);
        assert!(matches!(
            engine.state.assets["BTC"].admission,
            MfceAdmissionState::Rejected {
                reason: MfceRejectionReason::NotAcceptedSourceTransition,
                ..
            }
        ));

        // Advancing the source epoch without changing exposure cannot create
        // a synthetic candidate. A subsequent accepted exposure expansion can.
        engine.note_accepted_source_snapshot().unwrap();
        let unchanged = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::ZERO,
                Decimal::from(100),
                2_000,
                features(5),
            )
            .unwrap();
        assert!(!unchanged.needs_live_book);
        engine.note_accepted_source_snapshot().unwrap();
        let expansion = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(8, 1),
                Decimal::from(80),
                Decimal::from(40),
                Decimal::from(101),
                3_000,
                features(8),
            )
            .unwrap();
        assert!(expansion.needs_live_book);
        assert_eq!(expansion.effective_target, Decimal::from(40));
        assert_eq!(
            engine.state.assets["BTC"]
                .active
                .as_ref()
                .unwrap()
                .transition_kind,
            MfceTransitionKind::Expansion
        );

        engine.note_accepted_source_snapshot().unwrap();
        let reduction = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(3, 1),
                Decimal::from(30),
                Decimal::from(40),
                Decimal::from(102),
                4_000,
                features(3),
            )
            .unwrap();
        assert!(!reduction.needs_live_book);
        assert_eq!(reduction.effective_target, Decimal::from(30));
        assert!(matches!(
            engine.state.assets["BTC"].admission,
            MfceAdmissionState::Bypass
        ));
        assert_eq!(engine.state.samples.len(), 1);
    }

    #[test]
    fn pending_or_rejected_reversal_flattens_the_incumbent_side() {
        let mut engine = MfceEngine::default();
        engine.note_accepted_source_snapshot().unwrap();
        engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::from(40),
                Decimal::from(100),
                1_000,
                features(5),
            )
            .unwrap();

        engine.note_accepted_source_snapshot().unwrap();
        let reversal = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(-5, 1),
                Decimal::from(-50),
                Decimal::from(40),
                Decimal::from(99),
                2_000,
                features(-5),
            )
            .unwrap();
        let transition_id = reversal.transition_id.unwrap();
        assert!(reversal.needs_live_book);
        assert_eq!(reversal.effective_target, Decimal::ZERO);
        assert_eq!(
            engine.state.assets["BTC"]
                .active
                .as_ref()
                .unwrap()
                .transition_kind,
            MfceTransitionKind::Reversal
        );

        engine
            .reject_pending_without_prediction(
                "BTC",
                transition_id,
                MfceRejectionReason::MedianBelowFrictionAndUncertainty,
            )
            .unwrap();
        assert_eq!(
            engine.effective_target("BTC", Decimal::from(40), Decimal::from(-50)),
            Decimal::ZERO
        );
    }

    #[test]
    fn rejected_opening_then_raw_reduction_is_still_a_follower_opening_candidate() {
        let mut engine = MfceEngine::default();
        engine.note_accepted_source_snapshot().unwrap();
        let first = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(100),
                1_000,
                features(10),
            )
            .unwrap();
        engine
            .reject_pending_without_prediction(
                "BTC",
                first.transition_id.unwrap(),
                MfceRejectionReason::InsufficientPooledSupport,
            )
            .unwrap();

        engine.note_accepted_source_snapshot().unwrap();
        let reduced_source = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::ZERO,
                Decimal::from(101),
                2_000,
                features(5),
            )
            .unwrap();
        assert!(reduced_source.needs_live_book);
        assert_eq!(reduced_source.effective_target, Decimal::ZERO);
        assert_eq!(
            engine.state.assets["BTC"]
                .active
                .as_ref()
                .unwrap()
                .transition_kind,
            MfceTransitionKind::Opening
        );
    }

    #[test]
    fn delayed_book_only_fills_liquidity_slots_and_cannot_expand_transition_notional() {
        let mut engine = MfceEngine::default();
        engine.note_accepted_source_snapshot().unwrap();
        let mut initial = features(5);
        initial.values[6] = Decimal::ONE;
        initial.values[10] = Decimal::ONE;
        initial.values[17] = Decimal::new(75, 2);
        let opened = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::ZERO,
                Decimal::from(100),
                1_000,
                initial.clone(),
            )
            .unwrap();
        let transition_id = opened.transition_id.unwrap();
        assert_eq!(engine.report().awaiting_live_books, 1);
        assert!(engine.report().policy_outputs.is_empty());
        assert_eq!(
            engine
                .pending_position_notional("BTC", transition_id, Decimal::from(80))
                .unwrap(),
            Decimal::from(50)
        );
        engine
            .update_pending_live_context(
                "BTC",
                transition_id,
                [
                    Decimal::ZERO,
                    Decimal::from(2),
                    Decimal::new(15, 1),
                    Decimal::new(12, 1),
                    Decimal::new(1, 1),
                ],
                Decimal::from(80),
            )
            .unwrap();
        let active = engine.state.assets["BTC"].active.as_ref().unwrap();
        assert_eq!(active.features.values[0], initial.values[0]);
        assert_eq!(active.features.values[6], Decimal::ONE);
        assert_eq!(active.features.values[17], initial.values[17]);
        assert_eq!(active.features.values[10], Decimal::ZERO);
        assert_eq!(active.features.values[11], Decimal::from(2));
        assert_eq!(active.proposed_target_notional, Decimal::from(50));

        let prediction = MfcePrediction {
            model_epoch: 0,
            q10_gross_bps: Decimal::from(10),
            q50_gross_bps: Decimal::from(20),
            uncertainty_bps: Decimal::ZERO,
            pooled_sample_count: 8,
            direction_sample_count: 8,
            asset_direction_sample_count: 8,
            used_model: false,
        };
        engine
            .record_allocation(
                "BTC",
                transition_id,
                &MfceAllocationDecision {
                    policy_state: MfcePolicyState::Exploit,
                    admitted: true,
                    reason: None,
                    allocation_fraction: Decimal::ONE,
                    opportunity_score: Decimal::ONE,
                    net_q50_bps: Decimal::from(20),
                    conservative_edge_bps: Decimal::from(20),
                    net_q10_bps: Decimal::from(10),
                    required_median_bps: Decimal::ZERO,
                    modeled_tail_loss_usd: Decimal::ZERO,
                },
                &prediction,
                Decimal::ZERO,
            )
            .unwrap();
        assert_eq!(
            engine.effective_target("BTC", Decimal::from(50), Decimal::from(100)),
            Decimal::from(50)
        );
        assert_eq!(engine.report().observed_transitions, 1);
        assert_eq!(engine.report().decision_counts.exploit, 1);
        assert_eq!(engine.report().decision_counts.allocated, 1);
        assert_eq!(engine.report().awaiting_live_books, 0);
        assert_eq!(engine.report().policy_outputs.len(), 1);

        // Repricing and a fresh book may reevaluate the same transition, but
        // the rolling funnel must continue to count one unique opportunity.
        let recheck = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::ZERO,
                Decimal::from(101),
                2_000,
                initial,
            )
            .unwrap();
        assert_eq!(recheck.transition_id, Some(transition_id));
        assert_eq!(engine.awaiting_book_assets().count(), 1);
        assert_eq!(engine.report().awaiting_live_books, 0);
        assert_eq!(engine.report().policy_outputs.len(), 1);
        engine
            .record_allocation(
                "BTC",
                transition_id,
                &MfceAllocationDecision {
                    policy_state: MfcePolicyState::Exploit,
                    admitted: true,
                    reason: None,
                    allocation_fraction: Decimal::ONE,
                    opportunity_score: Decimal::ONE,
                    net_q50_bps: Decimal::from(20),
                    conservative_edge_bps: Decimal::from(20),
                    net_q10_bps: Decimal::from(10),
                    required_median_bps: Decimal::ZERO,
                    modeled_tail_loss_usd: Decimal::ZERO,
                },
                &prediction,
                Decimal::ZERO,
            )
            .unwrap();
        assert_eq!(engine.report().decision_counts.exploit, 1);
        assert_eq!(engine.report().decision_counts.allocated, 1);
    }

    #[test]
    fn policy_separates_exploit_explore_economic_reject_and_hard_tail_reject() {
        let base = MfcePrediction {
            model_epoch: 1,
            q10_gross_bps: Decimal::from(-21),
            q50_gross_bps: Decimal::from(72),
            uncertainty_bps: Decimal::from(10),
            pooled_sample_count: 64,
            direction_sample_count: 32,
            asset_direction_sample_count: 8,
            used_model: true,
        };
        let exploit = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: base.clone(),
            friction_bps: Decimal::from(14),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        assert_eq!(exploit.policy_state, MfcePolicyState::Exploit);
        assert_eq!(exploit.conservative_edge_bps, Decimal::from(48));
        assert!(exploit.allocation_fraction > Decimal::from_parts(7, 0, 0, false, 1));

        let explore = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: MfcePrediction {
                q10_gross_bps: Decimal::from(-30),
                q50_gross_bps: Decimal::from(36),
                uncertainty_bps: Decimal::from(24),
                ..base.clone()
            },
            friction_bps: Decimal::from(17),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        assert_eq!(explore.policy_state, MfcePolicyState::Explore);
        assert_eq!(explore.conservative_edge_bps, Decimal::from(-5));
        assert!(explore.allocation_fraction <= MFCE_EXPLORE_POOL_FRACTION);
        assert!(explore.allocation_fraction > Decimal::from_parts(2, 0, 0, false, 1));

        let negative = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: MfcePrediction {
                q10_gross_bps: Decimal::from(-144),
                q50_gross_bps: Decimal::from(-19),
                ..base.clone()
            },
            friction_bps: Decimal::from(31),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        assert_eq!(negative.policy_state, MfcePolicyState::Reject);
        assert_eq!(
            negative.reason,
            Some(MfceRejectionReason::StrongNegativeExpectancy)
        );
        let zero_edge = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: MfcePrediction {
                q10_gross_bps: Decimal::from(-20),
                q50_gross_bps: Decimal::from(20),
                ..base.clone()
            },
            friction_bps: Decimal::from(20),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        assert_eq!(zero_edge.policy_state, MfcePolicyState::Explore);

        let tail = evaluate_allocation_policy(&MfceAllocationInput {
            prediction: MfcePrediction {
                q10_gross_bps: Decimal::from(-120),
                q50_gross_bps: Decimal::from(80),
                ..base
            },
            friction_bps: Decimal::from(20),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(10_000),
            remaining_tail_loss_budget_usd: Decimal::ZERO,
        })
        .unwrap();
        assert_eq!(tail.policy_state, MfcePolicyState::Reject);
        assert_eq!(tail.reason, Some(MfceRejectionReason::TailBudgetExceeded));
        assert_eq!(tail.allocation_fraction, Decimal::ZERO);
        assert_eq!(tail.modeled_tail_loss_usd, Decimal::ZERO);
    }

    fn allocation_candidate(
        asset: &str,
        transition_id: u64,
        q10: i64,
        q50: i64,
        friction: i64,
        uncertainty: i64,
        requested: i64,
    ) -> MfceCrossSectionalCandidate {
        MfceCrossSectionalCandidate {
            asset: asset.into(),
            transition_id,
            input: MfceAllocationInput {
                prediction: MfcePrediction {
                    model_epoch: 1,
                    q10_gross_bps: Decimal::from(q10),
                    q50_gross_bps: Decimal::from(q50),
                    uncertainty_bps: Decimal::from(uncertainty),
                    pooled_sample_count: 128,
                    direction_sample_count: 64,
                    asset_direction_sample_count: 16,
                    used_model: true,
                },
                friction_bps: Decimal::from(friction),
                copytrade_conviction: Decimal::new(5, 1),
                current_position_notional: Decimal::ZERO,
                proposed_position_notional: Decimal::from(requested),
                remaining_tail_loss_budget_usd: Decimal::from(1_000),
            },
            prior_tail_loss_usd: Decimal::ZERO,
        }
    }

    #[test]
    fn cross_sectional_allocation_is_order_independent_and_favors_score() {
        let strong = allocation_candidate("BTC", 1, 10, 120, 10, 10, 1_000);
        let weak = allocation_candidate("ETH", 2, 10, 50, 10, 20, 1_000);
        let forward = allocate_cross_sectional(
            &[strong.clone(), weak.clone()],
            Decimal::from(1_000),
            Decimal::from(1_000),
        )
        .unwrap();
        let reverse =
            allocate_cross_sectional(&[weak, strong], Decimal::from(1_000), Decimal::from(1_000))
                .unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(forward["BTC"].policy_state, MfcePolicyState::Exploit);
        assert_eq!(forward["ETH"].policy_state, MfcePolicyState::Exploit);
        assert!(forward["BTC"].allocation_fraction > forward["ETH"].allocation_fraction);
        let total_fraction = forward["BTC"]
            .allocation_fraction
            .checked_add(forward["ETH"].allocation_fraction)
            .unwrap();
        assert!(total_fraction <= Decimal::ONE);
        assert!(total_fraction > Decimal::from_parts(999_999, 0, 0, false, 6));
    }

    #[test]
    fn no_incumbent_explores_negative_backoff_and_conviction_sets_the_prior() {
        let mut low_conviction = allocation_candidate("LOW", 1, -100, -50, 10, 20, 1_000);
        low_conviction.input.prediction.used_model = false;
        low_conviction.input.copytrade_conviction = Decimal::ZERO;
        let mut high_conviction = allocation_candidate("HIGH", 2, -100, -50, 10, 20, 1_000);
        high_conviction.input.prediction.used_model = false;
        high_conviction.input.copytrade_conviction = Decimal::ONE;

        let decisions = allocate_cross_sectional(
            &[low_conviction, high_conviction],
            Decimal::from(10_000),
            Decimal::from(10_000),
        )
        .unwrap();
        assert_eq!(decisions["LOW"].policy_state, MfcePolicyState::Explore);
        assert_eq!(decisions["HIGH"].policy_state, MfcePolicyState::Explore);
        assert!(decisions["LOW"].admitted);
        assert!(decisions["HIGH"].admitted);
        assert!(decisions["HIGH"].allocation_fraction > decisions["LOW"].allocation_fraction);

        let model_backed = allocation_candidate("MODEL", 3, -100, -50, 10, 20, 1_000);
        let decision = allocate_cross_sectional(
            &[model_backed],
            Decimal::from(10_000),
            Decimal::from(10_000),
        )
        .unwrap();
        assert_eq!(decision["MODEL"].policy_state, MfcePolicyState::Reject);
        assert_eq!(
            decision["MODEL"].reason,
            Some(MfceRejectionReason::StrongNegativeExpectancy)
        );
    }

    #[test]
    fn incumbent_candidate_capacity_is_measured_above_its_no_new_risk_baseline() {
        for (current, proposed, expected_baseline) in
            [(0, 100, 0), (40, 100, 40), (40, -100, 0), (100, 40, 40)]
        {
            let current = Decimal::from(current);
            let proposed = Decimal::from(proposed);
            let baseline = allocation_baseline_target(current, proposed);
            assert_eq!(baseline, Decimal::from(expected_baseline));
            let requested = requested_risk_increase(current, proposed).unwrap();
            assert_eq!(
                baseline.abs().checked_add(requested).unwrap(),
                proposed.abs()
            );
        }

        let incumbent = allocation_candidate("BTC", 1, 10, 120, 10, 10, 100);
        let source_capacity = Decimal::from(100);
        let committed_baseline = allocation_baseline_target(
            incumbent.input.current_position_notional,
            incumbent.input.proposed_position_notional,
        )
        .abs();
        let available = source_capacity.checked_sub(committed_baseline).unwrap();
        let decisions =
            allocate_cross_sectional(&[incumbent], available, Decimal::from(1_000)).unwrap();
        assert_eq!(decisions["BTC"].allocation_fraction, Decimal::ONE);
    }

    #[test]
    fn cold_start_explore_can_borrow_idle_gross_but_remains_cross_sectionally_bounded() {
        let candidates = (0..30)
            .map(|index| {
                allocation_candidate(&format!("A{index:02}"), index + 1, -40, 30, 10, 25, 100)
            })
            .collect::<Vec<_>>();
        let decisions =
            allocate_cross_sectional(&candidates, Decimal::from(1_000), Decimal::from(1_000))
                .unwrap();
        let allocated = decisions
            .values()
            .try_fold(Decimal::ZERO, |total, decision| {
                total
                    .checked_add(
                        Decimal::from(100)
                            .checked_mul(decision.allocation_fraction)
                            .unwrap(),
                    )
                    .ok_or(MfceError::Arithmetic)
            });
        let allocated = allocated.unwrap();
        assert!(allocated <= Decimal::from(1_000));
        assert!(allocated > Decimal::from(250));
        assert!(
            decisions
                .values()
                .all(|decision| decision.policy_state == MfcePolicyState::Explore),
            "unexpected decisions: {decisions:?}"
        );
    }

    #[test]
    fn exploit_has_first_claim_and_explore_uses_only_idle_notional_capacity() {
        let exploit = allocation_candidate("PROVEN", 1, -90, 100, 10, 10, 1_000);
        let explore = allocation_candidate("UNKNOWN", 2, -40, 30, 10, 25, 100);
        let decisions = allocate_cross_sectional(
            &[exploit, explore],
            Decimal::from(100),
            Decimal::from(1_000),
        )
        .unwrap();
        assert!(decisions["PROVEN"].admitted);
        assert_eq!(decisions["PROVEN"].allocation_fraction, Decimal::new(1, 1));
        assert!(!decisions["UNKNOWN"].admitted);
        assert_eq!(
            decisions["UNKNOWN"].reason,
            Some(MfceRejectionReason::AllocationBudgetExhausted)
        );
    }

    #[test]
    fn a_single_uncertain_explore_state_is_limited_below_the_pool_ceiling() {
        let candidate = allocation_candidate("MEME", 1, -1_000, 11, 10, 100, 1_000);
        let decisions =
            allocate_cross_sectional(&[candidate], Decimal::from(1_000), Decimal::from(1_000))
                .unwrap();
        let decision = &decisions["MEME"];
        assert_eq!(decision.policy_state, MfcePolicyState::Explore);
        assert!(decision.allocation_fraction > Decimal::ZERO);
        assert!(decision.allocation_fraction < Decimal::from_parts(1, 0, 0, false, 1));
        assert!(decision.allocation_fraction < MFCE_EXPLORE_POOL_FRACTION);
    }

    #[test]
    fn explore_pool_cannot_crowd_out_exploit_and_tail_budget_stays_global() {
        let exploit = allocation_candidate("BTC", 1, -90, 100, 10, 10, 1_000);
        let explore = allocation_candidate("ETH", 2, -90, 30, 10, 25, 1_000);
        let decisions =
            allocate_cross_sectional(&[exploit, explore], Decimal::from(1_000), Decimal::from(15))
                .unwrap();
        assert_eq!(decisions["BTC"].policy_state, MfcePolicyState::Exploit);
        assert_eq!(decisions["ETH"].policy_state, MfcePolicyState::Explore);
        let total_tail = decisions
            .values()
            .try_fold(Decimal::ZERO, |total, decision| {
                total
                    .checked_add(decision.modeled_tail_loss_usd)
                    .ok_or(MfceError::Arithmetic)
            });
        assert!(total_tail.unwrap() <= Decimal::from(15));
        assert!(decisions["BTC"].allocation_fraction > decisions["ETH"].allocation_fraction);
    }

    #[test]
    fn model_allocation_scales_openings_expansions_and_reversals() {
        assert_eq!(
            allocated_target(
                Decimal::ZERO,
                Decimal::from(100),
                MFCE_EXPLORE_POOL_FRACTION,
            )
            .unwrap(),
            Decimal::from(25)
        );
        assert_eq!(
            allocated_target(
                Decimal::from(50),
                Decimal::from(100),
                MFCE_EXPLORE_POOL_FRACTION,
            )
            .unwrap(),
            Decimal::new(625, 1)
        );
        assert_eq!(
            allocated_target(
                Decimal::from(50),
                Decimal::from(-100),
                MFCE_EXPLORE_POOL_FRACTION,
            )
            .unwrap(),
            Decimal::from(-25)
        );
    }

    #[test]
    fn pending_rejected_and_bypass_targets_follow_current_notional_without_buying() {
        let mut engine = MfceEngine::default();
        engine.note_accepted_source_snapshot().unwrap();
        let opening = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::from(50),
                Decimal::from(100),
                1_000,
                features(10),
            )
            .unwrap();
        assert_eq!(opening.effective_target, Decimal::from(50));

        let drifted = engine
            .observe_raw_source(
                "BTC",
                false,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::from(25),
                Decimal::from(100),
                2_000,
                features(10),
            )
            .unwrap();
        assert_eq!(drifted.effective_target, Decimal::from(25));
        engine
            .reject_pending_without_prediction(
                "BTC",
                opening.transition_id.unwrap(),
                MfceRejectionReason::MedianBelowFrictionAndUncertainty,
            )
            .unwrap();
        assert_eq!(
            engine.effective_target("BTC", Decimal::from(20), Decimal::from(100)),
            Decimal::from(20)
        );

        engine.note_accepted_source_snapshot().unwrap();
        let reduction = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(30),
                Decimal::from(50),
                Decimal::from(101),
                3_000,
                features(5),
            )
            .unwrap();
        assert!(!reduction.needs_live_book);
        assert_eq!(
            engine.effective_target("BTC", Decimal::from(20), Decimal::from(30)),
            Decimal::from(20)
        );
    }

    #[test]
    fn first_book_features_and_source_cap_stay_immutable_across_rechecks() {
        let mut engine = MfceEngine::default();
        engine.note_accepted_source_snapshot().unwrap();
        let mut initial = features(10);
        initial.values[10] = Decimal::ONE;
        let opened = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(100),
                1_000,
                initial,
            )
            .unwrap();
        let transition_id = opened.transition_id.unwrap();
        engine
            .update_pending_live_context(
                "BTC",
                transition_id,
                [
                    Decimal::ZERO,
                    Decimal::from(2),
                    Decimal::from(3),
                    Decimal::from(4),
                    Decimal::new(1, 1),
                ],
                Decimal::from(60),
            )
            .unwrap();
        assert_eq!(
            engine
                .pending_position_notional("BTC", transition_id, Decimal::from(100))
                .unwrap(),
            Decimal::from(100)
        );

        engine
            .observe_raw_source(
                "BTC",
                false,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(101),
                2_000,
                features(10),
            )
            .unwrap();
        engine
            .update_pending_live_context(
                "BTC",
                transition_id,
                [
                    Decimal::ZERO,
                    Decimal::from(20),
                    Decimal::from(30),
                    Decimal::from(40),
                    Decimal::new(-1, 1),
                ],
                Decimal::from(100),
            )
            .unwrap();
        engine.note_accepted_source_snapshot().unwrap();
        engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::from(110),
                3_000,
                features(0),
            )
            .unwrap();
        let stored = engine.state.samples.back().unwrap();
        assert_eq!(stored.features.values[11], Decimal::from(2));
        assert_eq!(stored.features.values[12], Decimal::from(3));
        assert_eq!(stored.features.values[14], Decimal::new(1, 1));
    }

    #[test]
    fn flat_lifecycles_are_evicted_without_inheriting_source_authority() {
        let mut engine = MfceEngine::default();
        for index in 0..MFCE_MAX_ASSETS {
            engine
                .state
                .assets
                .insert(format!("A{index:03}"), MfceAssetState::default());
        }
        let update = engine
            .observe_raw_source(
                "NEW",
                false,
                Decimal::ONE,
                Decimal::from(10),
                Decimal::ZERO,
                Decimal::from(100),
                1_000,
                features(1),
            )
            .unwrap();
        assert_eq!(engine.state.assets.len(), MFCE_MAX_ASSETS);
        assert!(!engine.state.assets.contains_key("A000"));
        assert!(!update.needs_live_book);
        assert!(matches!(
            engine.state.assets["NEW"].admission,
            MfceAdmissionState::Rejected {
                reason: MfceRejectionReason::NotAcceptedSourceTransition,
                ..
            }
        ));
    }

    #[test]
    fn minimum_identical_support_keeps_positive_epistemic_uncertainty() {
        let mut engine = MfceEngine::default();
        engine.state.samples = (1..=MFCE_MIN_BACKOFF_SAMPLES as u64)
            .map(|id| training_sample(id, "BTC", MfceDirection::Long, 10))
            .collect();
        engine.state.next_sample_id = MFCE_MIN_BACKOFF_SAMPLES as u64 + 1;
        let prediction = engine
            .predict("BTC", MfceDirection::Long, &features(10))
            .unwrap();
        assert!(prediction.uncertainty_bps > Decimal::ZERO);
        assert_eq!(
            prediction.pooled_sample_count,
            MFCE_MIN_BACKOFF_SAMPLES as u64
        );
    }

    #[test]
    fn empty_engine_serves_a_conservative_exploration_prior() {
        let engine = MfceEngine::default();
        let prediction = engine
            .predict("BTC", MfceDirection::Long, &features(1))
            .unwrap();
        assert!(!prediction.used_model);
        assert_eq!(prediction.pooled_sample_count, 0);
        assert_eq!(prediction.q10_gross_bps, MFCE_BOOTSTRAP_Q10_BPS);
        assert_eq!(prediction.q50_gross_bps, MFCE_BOOTSTRAP_Q50_BPS);
        assert_eq!(prediction.uncertainty_bps, MFCE_BOOTSTRAP_UNCERTAINTY_BPS);
        let decision = evaluate_allocation_policy(&MfceAllocationInput {
            prediction,
            friction_bps: Decimal::from(10),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        assert_eq!(decision.policy_state, MfcePolicyState::Explore);
        assert!(decision.admitted);
        assert!(decision.allocation_fraction > Decimal::ZERO);
    }

    #[test]
    fn tail_reservation_scales_with_mark_and_partial_reduction() {
        let mut engine = MfceEngine::default();
        engine.note_accepted_source_snapshot().unwrap();
        let opened = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(100),
                1_000,
                features(10),
            )
            .unwrap();
        let prediction = MfcePrediction {
            model_epoch: 0,
            q10_gross_bps: Decimal::from(-100),
            q50_gross_bps: Decimal::from(100),
            uncertainty_bps: Decimal::ZERO,
            pooled_sample_count: 8,
            direction_sample_count: 8,
            asset_direction_sample_count: 8,
            used_model: false,
        };
        engine
            .record_allocation(
                "BTC",
                opened.transition_id.unwrap(),
                &MfceAllocationDecision {
                    policy_state: MfcePolicyState::Exploit,
                    admitted: true,
                    reason: None,
                    allocation_fraction: Decimal::ONE,
                    opportunity_score: Decimal::ONE,
                    net_q50_bps: Decimal::from(100),
                    conservative_edge_bps: Decimal::from(100),
                    net_q10_bps: Decimal::from(-100),
                    required_median_bps: Decimal::ZERO,
                    modeled_tail_loss_usd: Decimal::ONE,
                },
                &prediction,
                Decimal::ZERO,
            )
            .unwrap();
        assert_eq!(
            engine.reserved_tail_loss_usd("BTC", Decimal::ZERO).unwrap(),
            Decimal::ONE
        );
        assert_eq!(
            engine
                .reserved_tail_loss_usd("BTC", Decimal::from(200))
                .unwrap(),
            Decimal::from(2)
        );

        engine.note_accepted_source_snapshot().unwrap();
        engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::new(5, 1),
                Decimal::from(50),
                Decimal::from(100),
                Decimal::from(101),
                2_000,
                features(5),
            )
            .unwrap();
        assert_eq!(
            engine
                .reserved_tail_loss_usd("BTC", Decimal::from(50))
                .unwrap(),
            Decimal::new(5, 1)
        );
    }

    #[test]
    fn disconnected_worker_keeps_durable_prefix_and_can_restart() {
        let mut engine = MfceEngine::default();
        engine.state.samples = (1..=81)
            .map(|id| training_sample(id, "BTC", MfceDirection::Long, id as i64))
            .collect();
        engine.state.next_sample_id = 82;
        engine.state.last_retrain_attempt_sample_id = 80;
        engine.state.pending_training = Some(MfceTrainingAttempt {
            attempted_through_sample_id: 80,
            activation_sample_id: 81,
            candidate_epoch: 1,
            incumbent_epoch: None,
        });
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        drop(sender);
        engine.training = Some(receiver);
        assert!(engine.poll_training().is_err());
        assert!(engine.training.is_none());
        assert_eq!(
            engine
                .state
                .pending_training
                .as_ref()
                .unwrap()
                .attempted_through_sample_id,
            80
        );
        assert!(engine.maybe_start_training().unwrap());
    }

    #[test]
    fn chronological_holdout_candidate_promotes_with_distinct_training_cutoff() {
        let samples = (1..=500)
            .map(|id| {
                let mut sample =
                    training_sample(id, "BTC", MfceDirection::Long, ((id - 1) % 100) as i64 - 50);
                sample.features = features(0);
                sample
            })
            .collect::<Vec<_>>();
        let candidate = train_candidate(&samples, None, 1)
            .unwrap()
            .expect("chronologically calibrated candidate");
        assert_eq!(candidate.trained_through_sample_id, 400);

        let mut engine = MfceEngine::default();
        engine.state.samples = samples.into();
        engine.state.next_sample_id = 502;
        engine.state.last_retrain_attempt_sample_id = 500;
        engine.state.pending_training = Some(MfceTrainingAttempt {
            attempted_through_sample_id: 500,
            activation_sample_id: 501,
            candidate_epoch: 1,
            incumbent_epoch: None,
        });
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        sender
            .send(CompletedTraining {
                attempted_through_sample_id: 500,
                result: Ok(Some(candidate)),
            })
            .unwrap();
        engine.training = Some(receiver);

        assert!(engine.poll_training().unwrap());
        let incumbent = engine.state.incumbent.as_ref().unwrap();
        assert_eq!(incumbent.epoch, 1);
        assert_eq!(incumbent.trained_through_sample_id, 400);
        assert_ne!(incumbent.trained_through_sample_id, 500);
        let served = engine
            .predict("BTC", MfceDirection::Long, &features(0))
            .unwrap();
        assert_eq!(served.model_epoch, 1);
        assert!(served.used_model);
    }

    #[test]
    fn sample_ring_is_bounded_and_deterministic() {
        let mut samples = VecDeque::new();
        let mut next = 1;
        for index in 0..=MFCE_MAX_SAMPLES {
            let active = MfceActiveTransition {
                transition_id: index as u64 + 1,
                entry_midpoint: Decimal::from(100),
                entry_timestamp_mono: index as u64,
                direction: MfceDirection::Long,
                transition_kind: MfceTransitionKind::Opening,
                proposed_source_exposure: Decimal::ONE,
                proposed_target_notional: Decimal::ONE,
                features: features(index as i64),
                admitted: false,
            };
            complete_active(
                &mut samples,
                &mut next,
                "BTC",
                Some(active),
                Decimal::from(101),
                index as u64 + 1_000,
            )
            .unwrap();
        }
        assert_eq!(samples.len(), MFCE_MAX_SAMPLES);
        assert_eq!(samples.front().unwrap().sample_id, 2);
        assert_eq!(
            samples.back().unwrap().sample_id,
            MFCE_MAX_SAMPLES as u64 + 1
        );
    }

    #[test]
    fn validation_scores_the_served_pipeline_using_only_training_prefix_backoffs() {
        let training = vec![
            training_sample(1, "BTC", MfceDirection::Long, -40),
            training_sample(2, "BTC", MfceDirection::Long, -20),
            training_sample(3, "BTC", MfceDirection::Long, 10),
            training_sample(4, "ETH", MfceDirection::Long, 30),
            training_sample(5, "ETH", MfceDirection::Long, 50),
            training_sample(6, "SOL", MfceDirection::Short, -10),
            training_sample(7, "SOL", MfceDirection::Short, 20),
            training_sample(8, "DOGE", MfceDirection::Short, 60),
        ];
        let validation_low = vec![training_sample(9, "BTC", MfceDirection::Long, -1_000)];
        let validation_high = vec![training_sample(9, "BTC", MfceDirection::Long, 1_000)];
        let raw = vec![copytrade_mfce::QuantilePrediction {
            q10: -80.0,
            q50: 90.0,
        }];

        let low = served_validation_predictions(&training, &validation_low, Some(&raw)).unwrap();
        let high = served_validation_predictions(&training, &validation_high, Some(&raw)).unwrap();
        let expected = compose_conditional_quantiles(
            training.iter(),
            "BTC",
            MfceDirection::Long,
            Some((-80.0, 90.0)),
        )
        .unwrap();

        assert_eq!(low, high, "holdout labels must not affect served backoffs");
        assert_eq!(low[0].q10, expected.q10);
        assert_eq!(low[0].q50, expected.q50);
        assert_ne!(
            low[0].q10, raw[0].q10,
            "direction/asset shrinkage must be scored"
        );
        assert_ne!(
            low[0].q50, raw[0].q50,
            "direction/asset shrinkage must be scored"
        );
    }

    #[test]
    fn crossed_quantiles_fail_closed_in_served_and_validation_paths() {
        let training = (1..=MFCE_MIN_BACKOFF_SAMPLES as u64)
            .map(|sample_id| {
                training_sample(sample_id, "BTC", MfceDirection::Long, sample_id as i64)
            })
            .collect::<Vec<_>>();
        let validation = vec![training_sample(9, "BTC", MfceDirection::Long, 0)];
        let crossed = vec![copytrade_mfce::QuantilePrediction {
            q10: 10.0,
            q50: 9.0,
        }];

        assert!(matches!(
            served_validation_predictions(&training, &validation, Some(&crossed)),
            Err(MfceError::Model(message)) if message.contains("crossed")
        ));
        assert!(matches!(
            chronological_validation_score(&[0.0], &crossed, &[copytrade_mfce::QuantilePrediction {
                q10: -1.0,
                q50: 1.0,
            }]),
            Err(MfceError::Model(message)) if message.contains("crossed")
        ));
    }

    #[test]
    fn q10_undercoverage_rejects_promotion_despite_better_pinball_loss() {
        let labels = vec![0.0; 16];
        let candidate =
            vec![copytrade_mfce::QuantilePrediction { q10: 1.0, q50: 2.0 }; labels.len()];
        let incumbent = vec![
            copytrade_mfce::QuantilePrediction {
                q10: -100.0,
                q50: 10.0,
            };
            labels.len()
        ];
        let score = chronological_validation_score(&labels, &candidate, &incumbent).unwrap();

        assert!(score.candidate_q10_pinball < score.incumbent_q10_pinball);
        assert!(score.candidate_q50_pinball < score.incumbent_q50_pinball);
        assert_eq!(score.candidate_q10_below_fraction, 1.0);
        assert!(!score.passes_incumbent_comparison(labels.len()));
        assert!(!score.passes_cold_start(labels.len()));
    }

    #[test]
    fn cold_start_promotion_requires_calibration_not_incumbent_superiority() {
        let score = ChronologicalValidationScore {
            candidate_q10_pinball: 10.0,
            candidate_q50_pinball: 10.0,
            incumbent_q10_pinball: 1.0,
            incumbent_q50_pinball: 1.0,
            candidate_q10_below_fraction: 0.10,
            candidate_q50_below_fraction: 0.50,
            incumbent_q10_below_fraction: 0.10,
        };
        assert!(score.passes_cold_start(20));
        assert!(!score.passes_incumbent_comparison(20));

        let uncalibrated = ChronologicalValidationScore {
            candidate_q10_below_fraction: 0.30,
            ..score
        };
        assert!(!uncalibrated.passes_cold_start(20));
    }
}
