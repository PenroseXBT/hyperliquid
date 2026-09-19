//! Embedded multi-factor conditional quantile expectancy state and allocation.
//!
//! Executable after-cost RemainingEdge is learned from frozen decision state.
//! Gross opportunity and realized cost components remain separate diagnostics;
//! the served q10/q50 pair is one atomic net-edge prediction. MFCE owns economic
//! classification and sizing; portfolio risk and accounting remain downstream.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::{Receiver, TryRecvError};

pub const MFCE_STATE_SCHEMA_VERSION: u32 = 4;
pub const MFCE_FEATURE_VERSION: u32 = 3;
pub const MFCE_FEATURE_COUNT: usize = 27;
const MFCE_TRAJECTORY_FEATURE_COUNT: usize = 10;
const MFCE_OBJECTIVE_COUNT: usize = 4;
const MFCE_MODEL_FEATURE_COUNT: usize =
    MFCE_FEATURE_COUNT + 16 + MFCE_TRAJECTORY_FEATURE_COUNT + MFCE_OBJECTIVE_COUNT + 1;
pub const MFCE_MAX_SAMPLES: usize = 4_096;
// Bound the durable lifecycle map above the configured 375-wallet universe's
// simultaneously active asset surface. This remains a fixed daemon-wide cap;
// flat inactive lifecycles are still deterministically recycled at capacity.
pub const MFCE_MAX_ASSETS: usize = 512;
pub const MFCE_MAX_MODEL_BYTES: usize = 2 * 1_024 * 1_024;
const MFCE_VALIDATION_DENOMINATOR: usize = 5;
// Preserve a full wrapper-minimum chronological prefix after holding out the
// newest fifth for validation: ceil(32 / 0.8) = 40 completed labels. The
// chronological promotion gate remains sovereign over every candidate.
const MFCE_MIN_TRAINING_SAMPLES: usize = (crate::MIN_TRAINING_ROWS * MFCE_VALIDATION_DENOMINATOR
    + (MFCE_VALIDATION_DENOMINATOR - 2))
    / (MFCE_VALIDATION_DENOMINATOR - 1);
const MFCE_RETRAIN_LABEL_INTERVAL_COLD: u64 = 8;
const MFCE_RETRAIN_LABEL_INTERVAL_WARM: u64 = 16;
const MFCE_RETRAIN_LABEL_INTERVAL_MATURE: u64 = 32;
const MFCE_MIN_BACKOFF_SAMPLES: usize = 8;
const MFCE_ASSET_SHRINKAGE: f64 = 24.0;
const MFCE_DIRECTION_SHRINKAGE: f64 = 48.0;
const BPS_PER_UNIT_RETURN: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);
// Edge-only: explore pool is unbounded (1.0). Only net_q50 /
// conservative_edge / net_q10 may gate size.
#[cfg(test)]
const MFCE_EXPLORE_POOL_FRACTION: Decimal = Decimal::ONE;
/// Edge-only: information budget is unbounded (1.0). Cold Explore shares
/// full portfolio capacity; diagnostics only.
pub const MFCE_EXPLORE_INFORMATION_FRACTION: Decimal = Decimal::ONE;
const MFCE_MIN_EXPLORE_PRIOR_FRACTION: Decimal = Decimal::from_parts(15, 0, 0, false, 2);
const MFCE_EXPLORE_CONVICTION_RANGE: Decimal = Decimal::from_parts(10, 0, 0, false, 2);
const MFCE_EXPLORE_RISK_SCALE_BPS: Decimal = Decimal::from_parts(500, 0, 0, false, 0);
const MFCE_SCORE_EPSILON_BPS: Decimal = Decimal::ONE;
// Edge-only bootstrap: q10=0, q50=0, unc=0 so conservative==net_q50 at
// 0 labels. Break-even upper edge admits Explore so cold start can buy labels.
const MFCE_BOOTSTRAP_Q10_BPS: Decimal = Decimal::ZERO;
const MFCE_BOOTSTRAP_Q50_BPS: Decimal = Decimal::ZERO;
const MFCE_BOOTSTRAP_UNCERTAINTY_BPS: Decimal = Decimal::ZERO;
// Capital concentration requires the calibrated lower decile itself to clear
// zero after all modeled costs. A one-basis-point floor avoids promoting a
// numerically zero state.
const MFCE_EXPLOIT_Q10_NET_FLOOR_BPS: Decimal = Decimal::ONE;

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
    Continuation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningObjective {
    #[default]
    EntryQuality,
    ContinuationQuality,
    SizeQuality,
    ExitQuality,
}

impl LearningObjective {
    fn one_hot(self) -> [Decimal; MFCE_OBJECTIVE_COUNT] {
        let mut values = [Decimal::ZERO; MFCE_OBJECTIVE_COUNT];
        values[self as usize] = Decimal::ONE;
        values
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionTrajectoryFeatures {
    pub age_hours: Decimal,
    pub position_equity_fraction: Decimal,
    /// Directional mark displacement from average entry, normalized in bps.
    #[serde(alias = "average_entry_price")]
    pub mark_to_entry_bps: Decimal,
    pub unrealized_return_bps: Decimal,
    pub realized_equity_fraction: Decimal,
    pub mfe_bps: Decimal,
    pub mae_bps: Decimal,
    pub drawdown_from_mfe_bps: Decimal,
    pub price_velocity_bps_per_second: Decimal,
    pub current_policy_code: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionLearningContext {
    pub trajectory: Option<PositionTrajectoryFeatures>,
    pub objective: LearningObjective,
    /// Prediction/label horizon is a feature, not a forced exit timer.
    #[serde(default)]
    pub horizon_hours: Decimal,
}

impl PositionTrajectoryFeatures {
    fn values(&self) -> [Decimal; MFCE_TRAJECTORY_FEATURE_COUNT] {
        [
            self.age_hours,
            self.position_equity_fraction,
            self.mark_to_entry_bps,
            self.unrealized_return_bps,
            self.realized_equity_fraction,
            self.mfe_bps,
            self.mae_bps,
            self.drawdown_from_mfe_bps,
            self.price_velocity_bps_per_second,
            self.current_policy_code,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfceFeatureVector {
    pub version: u32,
    pub values: [Decimal; MFCE_FEATURE_COUNT],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<[Decimal; 16]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<PositionLearningContext>,
}

/// Exact two-field feature vector written by the first persisted MFCE layout.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceFeatureVectorV12A {
    pub version: u32,
    pub values: [Decimal; MFCE_FEATURE_COUNT],
}

/// Exact three-field feature vector written after market context was added.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceFeatureVectorV12D {
    pub version: u32,
    pub values: [Decimal; MFCE_FEATURE_COUNT],
    pub context: Option<[Decimal; 16]>,
}

impl MfceFeatureVector {
    pub fn origin(&self) -> crate::delayed::AlphaOrigin {
        match self.context.map(|c| c[11]) {
            Some(v) if v == Decimal::ONE => crate::delayed::AlphaOrigin::Flow,
            Some(v) if v == Decimal::from(2) => crate::delayed::AlphaOrigin::SourceFlowConfluence,
            _ => crate::delayed::AlphaOrigin::Source,
        }
    }
    pub fn new(values: [Decimal; MFCE_FEATURE_COUNT]) -> Self {
        Self {
            version: MFCE_FEATURE_VERSION,
            values,
            context: None,
            position: None,
        }
    }

    pub fn objective(&self) -> LearningObjective {
        self.position
            .as_ref()
            .map_or(LearningObjective::EntryQuality, |position| {
                position.objective
            })
    }

    pub fn set_position_learning(
        &mut self,
        trajectory: Option<PositionTrajectoryFeatures>,
        objective: LearningObjective,
    ) {
        self.position = Some(PositionLearningContext {
            trajectory,
            objective,
            horizon_hours: Decimal::ZERO,
        });
    }

    pub fn set_learning_horizon_ms(&mut self, horizon_ms: u64) {
        let position = self.position.get_or_insert(PositionLearningContext {
            trajectory: None,
            objective: LearningObjective::EntryQuality,
            horizon_hours: Decimal::ZERO,
        });
        position.horizon_hours = Decimal::from(horizon_ms) / Decimal::from(3_600_000u64);
    }

    pub fn snapshot_id(&self) -> Result<[u8; 32], MfceError> {
        validate_features(self)?;
        let canonical =
            serde_json::to_vec(self).map_err(|error| MfceError::InvalidState(error.to_string()))?;
        Ok(Sha256::digest(canonical).into())
    }

    fn as_f64(&self) -> Result<Vec<f64>, MfceError> {
        if self.version != MFCE_FEATURE_VERSION {
            return Err(MfceError::InvalidState("feature version mismatch".into()));
        }
        let context = self.context.unwrap_or([Decimal::ZERO; 16]);
        let trajectory = self
            .position
            .as_ref()
            .and_then(|position| position.trajectory.clone())
            .unwrap_or_default()
            .values();
        let objective = self.objective().one_hot();
        let horizon = self
            .position
            .as_ref()
            .map_or(Decimal::ZERO, |position| position.horizon_hours);
        self.values
            .iter()
            .chain(context.iter())
            .chain(trajectory.iter())
            .chain(objective.iter())
            .chain(std::iter::once(&horizon))
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
    /// Executable after-cost RemainingEdge at the sample's encoded horizon.
    pub remaining_net_edge_bps: Decimal,
    pub lifetime_seconds: Decimal,
    pub admitted: bool,
    /// Trailing in the current positional wire format.
    #[serde(default)]
    pub objective: LearningObjective,
}

/// Exact ten-field gross-return row written by legacy v12 A-C snapshots.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceTrainingSampleV12A {
    pub sample_id: u64,
    pub asset: String,
    pub direction: MfceDirection,
    pub transition_kind: MfceTransitionKind,
    pub opened_at_mono: u64,
    pub completed_at_mono: u64,
    pub features: LegacyMfceFeatureVectorV12A,
    pub gross_return_bps: Decimal,
    pub lifetime_seconds: Decimal,
    pub admitted: bool,
}

/// Exact ten-field gross-return row written by delayed-learning v12 snapshots.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceTrainingSampleV12D {
    pub sample_id: u64,
    pub asset: String,
    pub direction: MfceDirection,
    pub transition_kind: MfceTransitionKind,
    pub opened_at_mono: u64,
    pub completed_at_mono: u64,
    pub features: LegacyMfceFeatureVectorV12D,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum MfceAdmissionState {
    #[default]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfceRejectionReason {
    NotAcceptedSourceTransition,
    InsufficientPooledSupport,
    MissingLiveBook,
    InvalidPrediction,
    // Retained only to decode admissions from older snapshots.
    MedianBelowFrictionAndUncertainty,
    StrongNegativeExpectancy,
    BelowExchangeMinimum,
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
    /// Current target justification survives retirement of an increase action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_origin: Option<crate::delayed::AlphaOrigin>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceActiveTransitionV12A {
    pub transition_id: u64,
    pub entry_midpoint: Decimal,
    pub entry_timestamp_mono: u64,
    pub direction: MfceDirection,
    pub transition_kind: MfceTransitionKind,
    pub proposed_source_exposure: Decimal,
    pub proposed_target_notional: Decimal,
    pub features: LegacyMfceFeatureVectorV12A,
    pub admitted: bool,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceActiveTransitionV12D {
    pub transition_id: u64,
    pub entry_midpoint: Decimal,
    pub entry_timestamp_mono: u64,
    pub direction: MfceDirection,
    pub transition_kind: MfceTransitionKind,
    pub proposed_source_exposure: Decimal,
    pub proposed_target_notional: Decimal,
    pub features: LegacyMfceFeatureVectorV12D,
    pub admitted: bool,
}

/// Exact seven-field asset row used before action origin became durable.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceAssetStateV12A {
    pub last_observed_source_epoch: u64,
    pub last_raw_source_exposure: Decimal,
    pub approved_target_notional: Decimal,
    pub reserved_tail_loss_bps: Decimal,
    pub active: Option<LegacyMfceActiveTransitionV12A>,
    pub admission: MfceAdmissionState,
    pub last_counted_transition_id: Option<u64>,
}

/// Exact eight-field asset row used by delayed-learning v12 snapshots.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfceAssetStateV12D {
    pub last_observed_source_epoch: u64,
    pub last_raw_source_exposure: Decimal,
    pub approved_target_notional: Decimal,
    pub reserved_tail_loss_bps: Decimal,
    pub active: Option<LegacyMfceActiveTransitionV12D>,
    pub admission: MfceAdmissionState,
    pub last_counted_transition_id: Option<u64>,
    pub target_origin: Option<crate::delayed::AlphaOrigin>,
}

impl From<LegacyMfceAssetStateV12A> for MfceAssetState {
    fn from(legacy: LegacyMfceAssetStateV12A) -> Self {
        Self {
            last_observed_source_epoch: legacy.last_observed_source_epoch,
            last_raw_source_exposure: legacy.last_raw_source_exposure,
            approved_target_notional: legacy.approved_target_notional,
            reserved_tail_loss_bps: legacy.reserved_tail_loss_bps,
            active: None,
            admission: MfceAdmissionState::Bypass,
            last_counted_transition_id: legacy.last_counted_transition_id,
            target_origin: None,
        }
    }
}

impl From<LegacyMfceAssetStateV12D> for MfceAssetState {
    fn from(legacy: LegacyMfceAssetStateV12D) -> Self {
        Self {
            last_observed_source_epoch: legacy.last_observed_source_epoch,
            last_raw_source_exposure: legacy.last_raw_source_exposure,
            approved_target_notional: legacy.approved_target_notional,
            reserved_tail_loss_bps: legacy.reserved_tail_loss_bps,
            active: None,
            admission: MfceAdmissionState::Bypass,
            last_counted_transition_id: legacy.last_counted_transition_id,
            target_origin: legacy.target_origin,
        }
    }
}

impl MfceAssetState {
    fn pending(&self, id: u64) -> Result<&MfceActiveTransition, MfceError> {
        if !matches!(self.admission, MfceAdmissionState::AwaitingLiveBook { transition_id } if transition_id == id)
        {
            return Err(MfceError::InvalidState("stale MFCE transition".into()));
        }
        self.active
            .as_ref()
            .filter(|a| a.transition_id == id)
            .ok_or_else(|| MfceError::InvalidState("pending MFCE transition is inactive".into()))
    }
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
    /// Exact durable activation boundary from the training attempt
    /// (`activation_sample_id = attempted_through + 1`). Legacy snapshots
    /// predate this field and deserialize as 0, in which case predict()
    /// falls back to the historical reconstruction.
    #[serde(default)]
    pub activation_sample_id: u64,
    pub feature_version: u32,
    pub feature_count: u32,
    pub q10_model: String,
    pub q50_model: String,
    pub q10_sha256: String,
    pub q50_sha256: String,
    pub validation: MfceValidationSummary,
}

impl MfceModelState {
    fn load_pair(&self) -> Result<crate::QuantileModelPair, MfceError> {
        let strings = crate::QuantileModelStrings::new(
            MFCE_MODEL_FEATURE_COUNT,
            self.q10_model.clone(),
            self.q50_model.clone(),
        )
        .map_err(model_error)?;
        crate::QuantileModelPair::from_model_strings(&strings).map_err(model_error)
    }
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
    #[serde(default)]
    recovered_episodes: Vec<serde_json::Value>,
    pub incumbent: Option<MfceModelState>,
    #[serde(default)]
    pub pending_training: Option<MfceTrainingAttempt>,
    #[serde(default)]
    pub decision_counts: MfceDecisionCounts,
    #[serde(
        default,
        skip_serializing_if = "crate::delayed::DelayedLearning::is_empty"
    )]
    pub delayed: crate::delayed::DelayedLearning,
}

pub trait LegacyMfcePersistentState {
    fn schema_version(&self) -> u32;
    fn migrate(self) -> MfcePersistentState;
}

fn migrate_legacy_state(
    schema_version: u32,
    source_epoch: u64,
    next_transition_id: u64,
    next_sample_id: u64,
    last_retrain_attempt_sample_id: u64,
    assets: BTreeMap<String, MfceAssetState>,
    recovered_episodes: Vec<serde_json::Value>,
    incumbent: Option<MfceModelState>,
    pending_training: Option<MfceTrainingAttempt>,
    decision_counts: MfceDecisionCounts,
    delayed: crate::delayed::DelayedLearning,
) -> MfcePersistentState {
    MfcePersistentState {
        schema_version,
        source_epoch,
        next_transition_id,
        next_sample_id,
        last_retrain_attempt_sample_id,
        assets,
        // Historical rows predict source midpoint return, not follower-side
        // RemainingEdge, so they cannot enter the current training set.
        samples: VecDeque::new(),
        recovered_episodes,
        incumbent,
        pending_training,
        decision_counts,
        delayed,
    }
}

/// Earliest recovered schema-v12 MFCE tuple: nine fields, two-field features.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfcePersistentStateV12A {
    pub schema_version: u32,
    pub source_epoch: u64,
    pub next_transition_id: u64,
    pub next_sample_id: u64,
    pub last_retrain_attempt_sample_id: u64,
    pub assets: BTreeMap<String, LegacyMfceAssetStateV12A>,
    pub samples: VecDeque<LegacyMfceTrainingSampleV12A>,
    pub incumbent: Option<MfceModelState>,
    pub pending_training: Option<MfceTrainingAttempt>,
}

impl LegacyMfcePersistentState for LegacyMfcePersistentStateV12A {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
    fn migrate(self) -> MfcePersistentState {
        migrate_legacy_state(
            self.schema_version,
            self.source_epoch,
            self.next_transition_id,
            self.next_sample_id,
            self.last_retrain_attempt_sample_id,
            self.assets
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            Vec::new(),
            self.incumbent,
            self.pending_training,
            MfceDecisionCounts::default(),
            Default::default(),
        )
    }
}

/// Ten-field schema-v12 MFCE tuple with durable policy counters.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfcePersistentStateV12B {
    pub schema_version: u32,
    pub source_epoch: u64,
    pub next_transition_id: u64,
    pub next_sample_id: u64,
    pub last_retrain_attempt_sample_id: u64,
    pub assets: BTreeMap<String, LegacyMfceAssetStateV12A>,
    pub samples: VecDeque<LegacyMfceTrainingSampleV12A>,
    pub incumbent: Option<MfceModelState>,
    pub pending_training: Option<MfceTrainingAttempt>,
    pub decision_counts: MfceDecisionCounts,
}

impl LegacyMfcePersistentState for LegacyMfcePersistentStateV12B {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
    fn migrate(self) -> MfcePersistentState {
        migrate_legacy_state(
            self.schema_version,
            self.source_epoch,
            self.next_transition_id,
            self.next_sample_id,
            self.last_retrain_attempt_sample_id,
            self.assets
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            Vec::new(),
            self.incumbent,
            self.pending_training,
            self.decision_counts,
            Default::default(),
        )
    }
}

/// Eleven-field schema-v12 MFCE tuple with recovered episode payloads.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyMfcePersistentStateV12C {
    pub schema_version: u32,
    pub source_epoch: u64,
    pub next_transition_id: u64,
    pub next_sample_id: u64,
    pub last_retrain_attempt_sample_id: u64,
    pub assets: BTreeMap<String, LegacyMfceAssetStateV12A>,
    pub samples: VecDeque<LegacyMfceTrainingSampleV12A>,
    pub recovered_episodes: Vec<serde_json::Value>,
    pub incumbent: Option<MfceModelState>,
    pub pending_training: Option<MfceTrainingAttempt>,
    pub decision_counts: MfceDecisionCounts,
}

impl LegacyMfcePersistentState for LegacyMfcePersistentStateV12C {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
    fn migrate(self) -> MfcePersistentState {
        migrate_legacy_state(
            self.schema_version,
            self.source_epoch,
            self.next_transition_id,
            self.next_sample_id,
            self.last_retrain_attempt_sample_id,
            self.assets
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            self.recovered_episodes,
            self.incumbent,
            self.pending_training,
            self.decision_counts,
            Default::default(),
        )
    }
}

macro_rules! legacy_v12_delayed_state {
    ($name:ident, $delayed:ty) => {
        #[doc(hidden)]
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            pub schema_version: u32,
            pub source_epoch: u64,
            pub next_transition_id: u64,
            pub next_sample_id: u64,
            pub last_retrain_attempt_sample_id: u64,
            pub assets: BTreeMap<String, LegacyMfceAssetStateV12D>,
            pub samples: VecDeque<LegacyMfceTrainingSampleV12D>,
            pub recovered_episodes: Vec<serde_json::Value>,
            pub incumbent: Option<MfceModelState>,
            pub pending_training: Option<MfceTrainingAttempt>,
            pub decision_counts: MfceDecisionCounts,
            pub delayed: $delayed,
        }

        impl LegacyMfcePersistentState for $name {
            fn schema_version(&self) -> u32 {
                self.schema_version
            }
            fn migrate(self) -> MfcePersistentState {
                migrate_legacy_state(
                    self.schema_version,
                    self.source_epoch,
                    self.next_transition_id,
                    self.next_sample_id,
                    self.last_retrain_attempt_sample_id,
                    self.assets
                        .into_iter()
                        .map(|(key, value)| (key, value.into()))
                        .collect(),
                    self.recovered_episodes,
                    self.incumbent,
                    self.pending_training,
                    self.decision_counts,
                    self.delayed.migrate(),
                )
            }
        }
    };
}

legacy_v12_delayed_state!(
    LegacyMfcePersistentStateV12D,
    crate::delayed::LegacyDelayedLearningV12A
);
legacy_v12_delayed_state!(
    LegacyMfcePersistentStateV12E,
    crate::delayed::LegacyDelayedLearningV12B
);
legacy_v12_delayed_state!(
    LegacyMfcePersistentStateV12F,
    crate::delayed::LegacyDelayedLearningV12C
);
legacy_v12_delayed_state!(
    LegacyMfcePersistentStateV12G,
    crate::delayed::LegacyDelayedLearningV12D
);

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
            delayed: Default::default(),
            recovered_episodes: Vec::new(),
            incumbent: None,
            pending_training: None,
            decision_counts: MfceDecisionCounts::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MfceTransitionUpdate {
    pub transition_id: Option<u64>,
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
    /// Smallest additional risk unit that can become one exchange-valid IOC.
    /// Zero is valid when an executable close/reversal already supplies the
    /// order floor without requiring a minimum new-risk increment.
    pub minimum_executable_increment: Decimal,
}

/// Economic state selected by MFCE before downstream portfolio constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub modeled_tail_loss_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MfcePolicyOutput {
    pub asset: String,
    pub direction: Option<MfceDirection>,
    pub target_notional: Decimal,
    pub signal_observed_at: u64,
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
    pub delayed_decisions: usize,
    pub dropped_learning_samples: u64,
    pub alpha_performance: BTreeMap<crate::delayed::AlphaOrigin, crate::delayed::AlphaPerformance>,
    pub quality_performance: BTreeMap<
        crate::delayed::AlphaOrigin,
        BTreeMap<LearningObjective, crate::delayed::AlphaPerformance>,
    >,
    pub active_transitions: usize,
    pub awaiting_live_books: usize,
    pub incumbent_epoch: Option<u64>,
    pub trained_through_sample_id: Option<u64>,
    pub labels_since_retrain_attempt: u64,
    pub decision_counts: MfceDecisionCounts,
    pub policy_outputs: Vec<MfcePolicyOutput>,
    pub blockers_removed: bool,
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
#[derive(Default)]
pub struct MfceEngine {
    state: MfcePersistentState,
    models: Option<crate::QuantileModelPair>,
    training: Option<Receiver<CompletedTraining>>,
    completed_source_epoch: u64,
    policy_outputs: BTreeMap<String, MfcePolicyOutput>,
}

/// Owned snapshot of the mutable MFCE planning surface. See
/// `MfceEngine::planning_checkpoint`.
#[derive(Debug, Clone)]
pub struct MfcePlanningCheckpoint {
    state: MfcePersistentState,
    policy_outputs: BTreeMap<String, MfcePolicyOutput>,
    completed_source_epoch: u64,
}

impl MfcePersistentState {
    pub fn time_high_watermark(&self) -> u64 {
        let sample_high_watermark = self.samples.iter().fold(0, |high, sample| {
            high.max(sample.opened_at_mono)
                .max(sample.completed_at_mono)
        });
        let sample_high_watermark = self.delayed.samples.iter().fold(
            sample_high_watermark.max(self.delayed.funding_at.unwrap_or_default()),
            |high, sample| high.max(sample.decision_at).max(sample.funding_start),
        );
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
        if !(1..=MFCE_STATE_SCHEMA_VERSION).contains(&self.schema_version)
            || self.assets.len() > MFCE_MAX_ASSETS
            || self.samples.len() > MFCE_MAX_SAMPLES
            || !self.delayed.valid()
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
            let active_is_continuation = state
                .active
                .as_ref()
                .is_some_and(|active| active.transition_kind == MfceTransitionKind::Continuation);
            if state.last_observed_source_epoch > self.source_epoch
                || state.reserved_tail_loss_bps < Decimal::ZERO
                || state
                    .last_counted_transition_id
                    .is_some_and(|transition_id| transition_id >= self.next_transition_id)
                || (!active_is_continuation
                    && state.last_raw_source_exposure.is_zero()
                    && !state.approved_target_notional.is_zero())
                || (!active_is_continuation
                    && !state.approved_target_notional.is_zero()
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
                    || (active.transition_kind != MfceTransitionKind::Continuation
                        && active.proposed_source_exposure != state.last_raw_source_exposure)
                {
                    return Err(MfceError::InvalidState(
                        format!(
                            "invalid active MFCE transition for {asset}: kind={:?} direction={:?} source={} target={} last_source={} entry={} id={}/{}",
                            active.transition_kind,
                            active.direction,
                            active.proposed_source_exposure,
                            active.proposed_target_notional,
                            state.last_raw_source_exposure,
                            active.entry_midpoint,
                            active.transition_id,
                            self.next_transition_id,
                        ),
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
            delayed_decisions: self.delayed.samples.len(),
            dropped_learning_samples: self.delayed.dropped,
            alpha_performance: self.delayed.performance(),
            quality_performance: self.delayed.quality_performance(),
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
            blockers_removed: true,
        }
    }
}

impl MfceEngine {
    pub fn from_state(mut state: MfcePersistentState) -> Result<Self, MfceError> {
        if !(1..=MFCE_STATE_SCHEMA_VERSION).contains(&state.schema_version) {
            return Err(MfceError::InvalidState(
                "unsupported MFCE state schema".into(),
            ));
        }
        if state.schema_version < MFCE_STATE_SCHEMA_VERSION
            || state
                .incumbent
                .as_ref()
                .is_some_and(|m| m.feature_count != MFCE_MODEL_FEATURE_COUNT as u32)
        {
            // Retain the last follower/source targets, but invalidate every
            // feature-bearing decision and model from the old feature schema.
            // Fresh decisions are reconstructed only from new observations.
            state.samples.clear();
            state.delayed.samples.clear();
            state.delayed.provenance.trajectories.clear();
            state.incumbent = None;
            state.pending_training = None;
            state.last_retrain_attempt_sample_id = 0;
            state.schema_version = MFCE_STATE_SCHEMA_VERSION;
            state.recovered_episodes.clear();
            for asset in state.assets.values_mut() {
                asset.active = None;
                asset.admission = MfceAdmissionState::Bypass;
                asset.reserved_tail_loss_bps = Decimal::ZERO;
            }
        }
        state.validate()?;
        let models = state
            .incumbent
            .as_ref()
            .map(MfceModelState::load_pair)
            .transpose()?;
        Ok(Self {
            completed_source_epoch: state.source_epoch,
            state,
            models,
            training: None,
            policy_outputs: BTreeMap::new(),
        })
    }

    pub fn policy_output(&self, asset: &str) -> Option<&MfcePolicyOutput> {
        self.policy_outputs.get(asset)
    }

    pub fn policy_output_assets(&self) -> Vec<String> {
        self.policy_outputs.keys().cloned().collect()
    }
    pub fn delayed(&self) -> &crate::delayed::DelayedLearning {
        &self.state.delayed
    }
    pub fn delayed_mut(&mut self) -> &mut crate::delayed::DelayedLearning {
        &mut self.state.delayed
    }
    pub fn learn_delayed(
        &mut self,
        asset: &str,
        features: MfceFeatureVector,
        direction: MfceDirection,
        opened_at_mono: u64,
        completed_at_mono: u64,
        remaining_net_edge_bps: Decimal,
    ) {
        let sample_id = self.state.next_sample_id;
        self.state.next_sample_id = sample_id.saturating_add(1);
        self.state.samples.push_back(MfceTrainingSample {
            sample_id,
            asset: asset.into(),
            direction,
            transition_kind: if features.values[6] == Decimal::ONE {
                MfceTransitionKind::Opening
            } else {
                MfceTransitionKind::Expansion
            },
            opened_at_mono,
            completed_at_mono,
            objective: features.objective(),
            features,
            remaining_net_edge_bps,
            lifetime_seconds: Decimal::from(
                completed_at_mono.saturating_sub(opened_at_mono) / 1_000,
            ),
            admitted: false,
        });
        if self.state.samples.len() > MFCE_MAX_SAMPLES {
            self.state.samples.pop_front();
        }
    }

    pub fn state(&self) -> &MfcePersistentState {
        &self.state
    }

    pub fn replace_state(&mut self, state: MfcePersistentState) -> Result<(), MfceError> {
        *self = Self::from_state(state)?;
        Ok(())
    }

    /// Owned checkpoint of the mutable planning surface only. Model handles
    /// and background training handles are read-only during planning and are
    /// intentionally not part of the checkpoint.
    pub fn planning_checkpoint(&self) -> MfcePlanningCheckpoint {
        MfcePlanningCheckpoint {
            state: self.state.clone(),
            policy_outputs: self.policy_outputs.clone(),
            completed_source_epoch: self.completed_source_epoch,
        }
    }

    /// Restores a checkpoint taken by `planning_checkpoint`. Used only to
    /// roll back a planning attempt that advanced MFCE without committing a
    /// coherent DecisionRecord + DecisionSample.
    pub fn restore_planning_checkpoint(&mut self, checkpoint: MfcePlanningCheckpoint) {
        self.state = checkpoint.state;
        self.policy_outputs = checkpoint.policy_outputs;
        self.completed_source_epoch = checkpoint.completed_source_epoch;
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
                self.policy_outputs
                    .get(*asset)
                    .is_none_or(|output| output.transition_id != transition_id)
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

    pub fn has_information_probe_admission(&self, _asset: &str) -> bool {
        // Edge-only: cold-probe budget tracking disabled (diagnostics only).
        // All Explore share full capacity; only edge gates size.
        false
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
        if features.context.is_some() {
            state.target_origin = Some(features.origin());
        }

        if !exposure_changed {
            if let Some(active) = state.active.as_mut() {
                active.features = features;
            }
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

        // A source lifecycle boundary is not an executable learning label.
        state.active = None;
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
            state.approved_target_notional =
                allocation_baseline_target(current_target, desired_target);
            return Ok(MfceTransitionUpdate {
                transition_id: Some(transition_id),
                needs_live_book: false,
                effective_target: allocation_baseline_target(current_target, desired_target),
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
        state.approved_target_notional = allocation_baseline_target(current_target, desired_target);
        state.admission = MfceAdmissionState::AwaitingLiveBook { transition_id };
        Ok(MfceTransitionUpdate {
            transition_id: Some(transition_id),
            needs_live_book: true,
            effective_target: allocation_baseline_target(current_target, desired_target),
        })
    }

    /// Creates a fresh remaining-edge decision for an already-open follower
    /// position. This is an economic continuation observation, not a source
    /// lifecycle boundary and not a fourth policy state.
    #[allow(clippy::too_many_arguments)]
    pub fn observe_position_state(
        &mut self,
        asset: &str,
        raw_source_exposure: Decimal,
        current_target: Decimal,
        proposed_target: Decimal,
        midpoint: Decimal,
        now: u64,
        features: MfceFeatureVector,
    ) -> Result<MfceTransitionUpdate, MfceError> {
        validate_asset(asset)?;
        validate_features(&features)?;
        if current_target.is_zero()
            || proposed_target.is_zero()
            || current_target.is_sign_positive() != proposed_target.is_sign_positive()
            || proposed_target.abs() < current_target.abs()
            || midpoint <= Decimal::ZERO
        {
            return Err(MfceError::InvalidState(
                "invalid position continuation target".into(),
            ));
        }
        let direction = MfceDirection::from_signed(current_target).ok_or_else(|| {
            MfceError::InvalidState("position continuation has no direction".into())
        })?;
        let transition_id = self.state.next_transition_id;
        self.state.next_transition_id =
            transition_id.checked_add(1).ok_or(MfceError::Arithmetic)?;
        self.policy_outputs.remove(asset);
        let state = self.state.assets.entry(asset.to_owned()).or_default();
        state.last_raw_source_exposure = raw_source_exposure;
        state.last_observed_source_epoch = self.state.source_epoch;
        state.target_origin = Some(features.origin());
        state.active = Some(MfceActiveTransition {
            transition_id,
            entry_midpoint: midpoint,
            entry_timestamp_mono: now,
            direction,
            transition_kind: MfceTransitionKind::Continuation,
            // A continuation remains follower-position-side even after its
            // original wallet source disappears. Keep raw source state on the
            // asset, while the active decision direction follows exposure.
            proposed_source_exposure: current_target,
            proposed_target_notional: proposed_target,
            features,
            admitted: false,
        });
        state.approved_target_notional = current_target;
        state.admission = MfceAdmissionState::AwaitingLiveBook { transition_id };
        Ok(MfceTransitionUpdate {
            transition_id: Some(transition_id),
            needs_live_book: true,
            effective_target: current_target,
        })
    }

    pub fn update_pending_live_context(
        &mut self,
        asset: &str,
        transition_id: u64,
        liquidity_features: [Decimal; 5],
    ) -> Result<(), MfceError> {
        let state = self
            .state
            .assets
            .get_mut(asset)
            .ok_or_else(|| MfceError::InvalidState("missing pending MFCE asset".into()))?;
        state.pending(transition_id)?;
        let active = state
            .active
            .as_mut()
            .ok_or_else(|| MfceError::InvalidState("pending MFCE transition is inactive".into()))?;
        // Inference follows the current state; delayed samples own immutable
        // decision-time feature copies independently of this live transition.
        active.features.values[10..15].copy_from_slice(&liquidity_features);
        validate_features(&active.features)?;
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
        let active = state.pending(transition_id)?;
        Ok(cap_to_approved_target(
            active.proposed_target_notional,
            desired_target,
        ))
    }

    /// Predict one point on the RemainingEdge curve. The trained quantiles are
    /// after-cost net edge. We reconstruct gross only for the existing policy
    /// interface, which subtracts the exact same live friction once.
    pub fn predict_pending_at_horizon(
        &self,
        asset: &str,
        transition_id: u64,
        horizon_ms: u64,
        expected_friction_bps: Decimal,
    ) -> Result<MfcePrediction, MfceError> {
        if horizon_ms == 0 || expected_friction_bps < Decimal::ZERO {
            return Err(MfceError::InvalidState(
                "invalid RemainingEdge horizon/friction".into(),
            ));
        }
        let state = self
            .state
            .assets
            .get(asset)
            .ok_or_else(|| MfceError::InvalidState("missing MFCE asset".into()))?;
        let active = state.pending(transition_id)?;
        let mut features = active.features.clone();
        features.set_learning_horizon_ms(horizon_ms);
        let mut prediction = self.predict(asset, active.direction, &features)?;
        // q10/q50 came from one atomic model pair inside `predict`; adding the
        // same friction preserves ordering and their shared generation.
        prediction.q10_gross_bps = prediction
            .q10_gross_bps
            .checked_add(expected_friction_bps)
            .ok_or(MfceError::Arithmetic)?;
        prediction.q50_gross_bps = prediction
            .q50_gross_bps
            .checked_add(expected_friction_bps)
            .ok_or(MfceError::Arithmetic)?;
        if prediction.q10_gross_bps > prediction.q50_gross_bps {
            return Err(MfceError::Model(
                "crossed RemainingEdge q10/q50 pair".into(),
            ));
        }
        Ok(prediction)
    }

    pub fn pending_feature_identity(
        &self,
        asset: &str,
        transition_id: u64,
        horizon_ms: u64,
    ) -> Result<(LearningObjective, [u8; 32]), MfceError> {
        let state = self
            .state
            .assets
            .get(asset)
            .ok_or_else(|| MfceError::InvalidState("missing MFCE asset".into()))?;
        let active = state.pending(transition_id)?;
        let mut features = active.features.clone();
        features.set_learning_horizon_ms(horizon_ms);
        Ok((features.objective(), features.snapshot_id()?))
    }

    pub fn freeze_pending_learning_horizon(
        &mut self,
        asset: &str,
        transition_id: u64,
        horizon_ms: u64,
    ) -> Result<(), MfceError> {
        let state = self
            .state
            .assets
            .get_mut(asset)
            .ok_or_else(|| MfceError::InvalidState("missing pending MFCE asset".into()))?;
        state.pending(transition_id)?;
        let active = state
            .active
            .as_mut()
            .ok_or_else(|| MfceError::InvalidState("pending MFCE transition is inactive".into()))?;
        active.features.set_learning_horizon_ms(horizon_ms);
        validate_features(&active.features)
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
        state.pending(transition_id)?;
        let is_new_decision = state.last_counted_transition_id != Some(transition_id);
        if is_new_decision {
            let count = match decision.policy_state {
                MfcePolicyState::Explore => &mut decision_counts.explore,
                MfcePolicyState::Exploit => &mut decision_counts.exploit,
                MfcePolicyState::Reject => &mut decision_counts.reject,
            };
            *count = count.checked_add(1).ok_or(MfceError::Arithmetic)?;
            if decision.admitted && !decision.allocation_fraction.is_zero() {
                decision_counts.allocated = decision_counts
                    .allocated
                    .checked_add(1)
                    .ok_or(MfceError::Arithmetic)?;
            }
        }
        if let Some(active) = state.active.as_mut() {
            active.admitted = decision.admitted;
            if decision.admitted {
                state.approved_target_notional = allocated_target(
                    current_position_notional,
                    active.proposed_target_notional,
                    decision.allocation_fraction,
                )?;
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
                direction: state.active.as_ref().map(|active| active.direction),
                target_notional: state.approved_target_notional,
                signal_observed_at: state
                    .active
                    .as_ref()
                    .map_or(0, |active| active.entry_timestamp_mono),
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
        state.pending(transition_id)?;
        if state.last_counted_transition_id != Some(transition_id) {
            decision_counts.reject = decision_counts
                .reject
                .checked_add(1)
                .ok_or(MfceError::Arithmetic)?;
            state.last_counted_transition_id = Some(transition_id);
        }
        if let Some(active) = state.active.as_mut() {
            active.admitted = false;
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

    pub fn poll_training(&mut self) -> Result<bool, MfceError> {
        let Some(receiver) = self.training.as_ref() else {
            return Ok(false);
        };
        self.state.pending_training.as_ref().ok_or_else(|| {
            MfceError::InvalidState("runtime MFCE worker has no durable attempt".into())
        })?;
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
        let models = candidate.load_pair()?;
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
            } else if latest_sample_id < 512 {
                MFCE_RETRAIN_LABEL_INTERVAL_COLD
            } else if latest_sample_id < 2_048 {
                MFCE_RETRAIN_LABEL_INTERVAL_WARM
            } else {
                MFCE_RETRAIN_LABEL_INTERVAL_MATURE
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
        let activation_sample_id = attempt.activation_sample_id;
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("mfce-lightgbm-trainer".into())
            .spawn(move || {
                let result = train_candidate(
                    &samples,
                    incumbent.as_ref(),
                    next_epoch,
                    activation_sample_id,
                );
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
        if self
            .state
            .samples
            .iter()
            .filter(|s| s.features.origin() == features.origin())
            .count()
            < MFCE_MIN_BACKOFF_SAMPLES
        {
            // Edge-only cold start: neutral prior (0/0/0) so
            // conservative==net_q50 at 0 labels. Break-even upper edge admits
            // Explore; training still runs in background.
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
        let incumbent_is_eligible = self.state.incumbent.as_ref().is_some_and(|model| {
            if model.activation_sample_id != 0 {
                self.state.next_sample_id >= model.activation_sample_id
            } else {
                // Legacy models predate the durable activation identity.
                model
                    .trained_through_sample_id
                    .checked_add(model.validation.validation_samples)
                    .and_then(|sample| sample.checked_add(1))
                    .is_some_and(|activation_from| self.state.next_sample_id >= activation_from)
            }
        });
        let (base_quantiles, used_model) =
            match self.models.as_ref().filter(|_| incumbent_is_eligible) {
                Some(models) => {
                    let values = features.as_f64()?;
                    let prediction = models.predict_one(&values).map_err(model_error)?;
                    (Some((prediction.q10, prediction.q50)), true)
                }
                None => (None, false),
            };
        let objective_has_support = self
            .state
            .samples
            .iter()
            .filter(|sample| {
                sample.features.origin() == features.origin()
                    && sample.objective == features.objective()
            })
            .count()
            >= MFCE_MIN_BACKOFF_SAMPLES;
        let conditional = compose_conditional_quantiles(
            self.state.samples.iter().filter(|sample| {
                sample.features.origin() == features.origin()
                    && (!objective_has_support || sample.objective == features.objective())
            }),
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
            model_epoch: self
                .state
                .incumbent
                .as_ref()
                .filter(|_| incumbent_is_eligible)
                .map_or(0, |model| model.epoch),
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
            .remaining_net_edge_bps
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
        minimum_executable_increment: Decimal::ZERO,
    };
    let mut decisions = allocate_cross_sectional(
        &[candidate],
        requested_increment,
        input.remaining_tail_loss_budget_usd,
        requested_increment,
    )?;
    decisions
        .remove("single")
        .ok_or_else(|| MfceError::InvalidState("single MFCE allocation disappeared".into()))
}

pub fn remaining_explore_information_budget(
    source_capacity: Decimal,
    _committed_cold_explore: Decimal,
) -> Result<Decimal, MfceError> {
    // Edge-only: information fraction is 1.0; committed cold Explore is
    // diagnostics only and never caps admission.
    if source_capacity < Decimal::ZERO || _committed_cold_explore < Decimal::ZERO {
        return Err(MfceError::InvalidState(
            "negative cold-start Explore capacity".into(),
        ));
    }
    Ok(source_capacity.max(Decimal::ZERO))
}

/// Additional risk represented by a policy-sized candidate before the shared
/// cross-sectional allocator. Callers use this to reject probes whose maximum
/// safe size cannot form an exchange-valid action.
pub fn policy_sized_increment(
    input: &MfceAllocationInput,
    decision: &MfceAllocationDecision,
) -> Result<Decimal, MfceError> {
    requested_risk_increase(
        input.current_position_notional,
        input.proposed_position_notional,
    )?
    .checked_mul(decision.allocation_fraction)
    .ok_or(MfceError::Arithmetic)
}

/// Ranks one timestamp-consistent transition set and divides remaining hard
/// portfolio capacity among it. Proven Exploit states receive first claim;
/// Explore may borrow what remains, while the shared q10 tail-loss budget and
/// deterministic portfolio projection stay sovereign.
pub fn allocate_cross_sectional(
    candidates: &[MfceCrossSectionalCandidate],
    available_increment_notional: Decimal,
    remaining_tail_loss_budget_usd: Decimal,
    remaining_explore_information_notional: Decimal,
) -> Result<BTreeMap<String, MfceAllocationDecision>, MfceError> {
    if available_increment_notional < Decimal::ZERO
        || remaining_tail_loss_budget_usd < Decimal::ZERO
        || remaining_explore_information_notional < Decimal::ZERO
    {
        return Err(MfceError::InvalidState(
            "negative MFCE portfolio allocation capacity".into(),
        ));
    }
    let mut decisions = BTreeMap::new();
    let mut requested = BTreeMap::new();
    for candidate in candidates {
        if decisions.contains_key(&candidate.asset)
            || candidate.prior_tail_loss_usd < Decimal::ZERO
            || candidate.minimum_executable_increment < Decimal::ZERO
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
    // Exploit and conditionally supported model uncertainty receive normal
    // allocator capacity. Unsupported Explore remains an information probe
    // even when an unrelated promoted model exists, so it shares the global
    // cold-information budget.
    for allocation_class in 0..3 {
        let mut group = Vec::new();
        for candidate in candidates {
            let decision = &decisions[&candidate.asset];
            let in_class = match allocation_class {
                0 => decision.policy_state == MfcePolicyState::Exploit,
                1 => {
                    decision.policy_state == MfcePolicyState::Explore
                        && candidate.input.prediction.used_model
                        && has_actionable_conditional_support(&candidate.input.prediction)
                }
                _ => {
                    decision.policy_state == MfcePolicyState::Explore
                        && (!candidate.input.prediction.used_model
                            || !has_actionable_conditional_support(&candidate.input.prediction))
                }
            };
            if !in_class {
                continue;
            }
            let weight = cross_sectional_weight(decision, &candidate.input)?;
            let cap = if decision.policy_state == MfcePolicyState::Explore {
                requested[&candidate.asset]
                    .checked_mul(explore_target_fraction(decision, &candidate.input)?)
                    .ok_or(MfceError::Arithmetic)?
            } else {
                requested[&candidate.asset]
            };
            group.push((
                candidate.asset.clone(),
                weight,
                cap,
                candidate.minimum_executable_increment,
            ));
        }
        let group_capacity = if allocation_class == 2 {
            remaining_notional.min(remaining_explore_information_notional)
        } else {
            remaining_notional
        };
        let group_allocations = if allocation_class != 0 {
            // Buy breadth first: reserve one exchange-valid probe for as many
            // independent ranked states as capacity permits, then distribute
            // residual capital without ever creating sub-floor dust.
            ranked_probe_allocations(&group, group_capacity)?
        } else {
            weighted_capped_allocations(&group, group_capacity)?
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
        if request.is_zero() {
            // HOLD consumes no new allocation. The economic classification is
            // still served and delayed-learning still records this decision.
            decision.admitted = true;
            decision.reason = None;
            decision.allocation_fraction = Decimal::ZERO;
            decision.modeled_tail_loss_usd = candidate.prior_tail_loss_usd;
            continue;
        }
        if allocation.is_zero() {
            decision.admitted = false;
            let maximum = if decision.policy_state == MfcePolicyState::Explore {
                request
                    .checked_mul(explore_target_fraction(decision, &candidate.input)?)
                    .ok_or(MfceError::Arithmetic)?
            } else {
                request
            };
            decision.reason = Some(if maximum < candidate.minimum_executable_increment {
                MfceRejectionReason::BelowExchangeMinimum
            } else {
                MfceRejectionReason::AllocationBudgetExhausted
            });
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
            // Allocation failure is not an economic classification.
            decision.reason = Some(MfceRejectionReason::TailBudgetExceeded);
            continue;
        }
        let incremental_notional = requested[&candidate.asset]
            .checked_mul(decision.allocation_fraction)
            .ok_or(MfceError::Arithmetic)?;
        if incremental_notional < candidate.minimum_executable_increment {
            decision.admitted = false;
            decision.reason = Some(MfceRejectionReason::TailBudgetExceeded);
            decision.allocation_fraction = Decimal::ZERO;
            decision.modeled_tail_loss_usd = Decimal::ZERO;
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
            modeled_tail_loss_usd: Decimal::MAX,
        });
    }
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
    // Edge-only: support and model gates collapsed. Exploit iff net_q10>1,
    // Reject iff upper<0, regardless of support/model. Only net_q50 /
    // conservative_edge / net_q10 / opportunity / uncertainty gate size.
    let (policy_state, reason) = if upper_edge_bps < Decimal::ZERO {
        (
            MfcePolicyState::Reject,
            Some(MfceRejectionReason::StrongNegativeExpectancy),
        )
    } else if net_q10_bps > MFCE_EXPLOIT_Q10_NET_FLOOR_BPS {
        (MfcePolicyState::Exploit, None)
    } else {
        // Positive upper edge but lower decile at/below floor acquires
        // full information in edge-only mode (see explore_target_fraction).
        (MfcePolicyState::Explore, None)
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
        modeled_tail_loss_usd: Decimal::ZERO,
    })
}

fn has_actionable_conditional_support(_prediction: &MfcePrediction) -> bool {
    // Edge-only: support is always actionable. Diagnostics only.
    true
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
    // Edge-only: full size (1.0) whenever conservative edge is positive.
    // Otherwise fall back to conviction/risk sizing clamped to 1.0.
    if decision.conservative_edge_bps > Decimal::ZERO {
        return Ok(Decimal::ONE);
    }
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
        .map(|fraction| fraction.clamp(Decimal::ZERO, Decimal::ONE))
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
        .checked_sub(allocation_baseline_target(current, proposed).abs())
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

fn ranked_probe_allocations(
    candidates: &[(String, Decimal, Decimal, Decimal)],
    budget: Decimal,
) -> Result<BTreeMap<String, Decimal>, MfceError> {
    let mut ranked = candidates.to_vec();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let mut remaining = budget.max(Decimal::ZERO);
    let mut output = candidates
        .iter()
        .map(|(asset, _, _, _)| (asset.clone(), Decimal::ZERO))
        .collect::<BTreeMap<_, _>>();
    let mut selected = Vec::new();
    for (asset, weight, cap, floor) in &ranked {
        let cap = (*cap).max(Decimal::ZERO);
        let floor = (*floor).max(Decimal::ZERO);
        if cap.is_zero() || cap < floor {
            continue;
        }
        let seed = if floor.is_zero() { cap } else { floor };
        if seed > remaining {
            continue;
        }
        output.insert(asset.clone(), seed);
        remaining = remaining.checked_sub(seed).ok_or(MfceError::Arithmetic)?;
        selected.push((asset.clone(), *weight, cap));
    }
    // Breadth is already reserved. Residual capital may now increase those
    // probes in the same stable economic rank without reducing trial count.
    for (asset, _, cap) in selected {
        if remaining.is_zero() {
            break;
        }
        let already = output[&asset];
        let addition = cap
            .checked_sub(already)
            .ok_or(MfceError::Arithmetic)?
            .max(Decimal::ZERO)
            .min(remaining);
        output.insert(
            asset,
            already.checked_add(addition).ok_or(MfceError::Arithmetic)?,
        );
        remaining = remaining
            .checked_sub(addition)
            .ok_or(MfceError::Arithmetic)?;
    }
    Ok(output)
}

fn weighted_capped_allocations(
    candidates: &[(String, Decimal, Decimal, Decimal)],
    budget: Decimal,
) -> Result<BTreeMap<String, Decimal>, MfceError> {
    let mut output = candidates
        .iter()
        .map(|(asset, _, _, _)| (asset.clone(), Decimal::ZERO))
        .collect::<BTreeMap<_, _>>();
    let mut ranked = candidates.to_vec();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let mut remaining = budget.max(Decimal::ZERO);
    let mut active = Vec::new();
    for (asset, weight, cap, floor) in ranked {
        let floor = floor.max(Decimal::ZERO);
        if weight <= Decimal::ZERO || cap <= Decimal::ZERO || cap < floor || floor > remaining {
            continue;
        }
        output.insert(asset.clone(), floor);
        remaining = remaining.checked_sub(floor).ok_or(MfceError::Arithmetic)?;
        active.push((
            asset,
            weight,
            cap.checked_sub(floor).ok_or(MfceError::Arithmetic)?,
        ));
    }
    active.sort_by(|left, right| left.0.cmp(&right.0));
    let mut extras = active
        .iter()
        .map(|(asset, _, _)| (asset.clone(), Decimal::ZERO))
        .collect::<BTreeMap<_, _>>();
    let residual_candidates = active.clone();
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
            let already = extras[&asset];
            let capacity = cap
                .checked_sub(already)
                .ok_or(MfceError::Arithmetic)?
                .max(Decimal::ZERO);
            let share = round_budget
                .checked_mul(weight)
                .and_then(|value| value.checked_div(total_weight))
                .ok_or(MfceError::Arithmetic)?;
            let allocation = share.min(capacity).min(remaining);
            extras.insert(
                asset.clone(),
                already
                    .checked_add(allocation)
                    .ok_or(MfceError::Arithmetic)?,
            );
            remaining = remaining
                .checked_sub(allocation)
                .ok_or(MfceError::Arithmetic)?;
            match allocation.cmp(&capacity) {
                std::cmp::Ordering::Equal => capped_any = true,
                std::cmp::Ordering::Less => next.push((asset, weight, cap)),
                std::cmp::Ordering::Greater => {}
            }
        }
        if !capped_any {
            break;
        }
        active = next;
    }
    if remaining > Decimal::ZERO {
        let mut residual_order = residual_candidates;
        residual_order
            .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (asset, _, cap) in residual_order {
            let already = extras[&asset];
            let capacity = cap
                .checked_sub(already)
                .ok_or(MfceError::Arithmetic)?
                .max(Decimal::ZERO);
            let allocation = capacity.min(remaining);
            extras.insert(
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
    for (asset, extra) in extras {
        let reserved = output[&asset];
        output.insert(
            asset,
            reserved.checked_add(extra).ok_or(MfceError::Arithmetic)?,
        );
    }
    Ok(output)
}

fn allocated_target(
    current: Decimal,
    proposed: Decimal,
    allocation_fraction: Decimal,
) -> Result<Decimal, MfceError> {
    if allocation_fraction <= Decimal::ZERO || proposed.is_zero() {
        return Ok(allocation_baseline_target(current, proposed));
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
    if current == proposed {
        current
    } else {
        cap_to_approved_target(current, proposed)
    }
}

fn train_candidate(
    samples: &[MfceTrainingSample],
    incumbent: Option<&MfceModelState>,
    epoch: u64,
    activation_sample_id: u64,
) -> Result<Option<MfceModelState>, MfceError> {
    if samples.len() < MFCE_MIN_TRAINING_SAMPLES {
        return Ok(None);
    }
    let mut chronological = samples.to_vec();
    chronological.sort_by_key(|sample| (sample.opened_at_mono, sample.sample_id));
    let validation_len = (chronological.len() / MFCE_VALIDATION_DENOMINATOR).max(1);
    let training_len = chronological.len().saturating_sub(validation_len);
    if training_len < crate::MIN_TRAINING_ROWS {
        return Ok(None);
    }
    let (prefix, validation) = chronological.split_at(training_len);
    // Purge overlapping forward windows at the chronological boundary.
    let training = prefix
        .iter()
        .filter(|s| s.completed_at_mono < validation[0].opened_at_mono)
        .cloned()
        .collect::<Vec<_>>();
    if training.len() < crate::MIN_TRAINING_ROWS {
        return Ok(None);
    }
    let training = training.as_slice();
    let training_features = flatten_features(training)?;
    let training_labels = labels_f64(training)?;
    let candidate_strings = crate::train_quantile_pair(
        &training_features,
        &training_labels,
        None,
        MFCE_MODEL_FEATURE_COUNT,
    )
    .map_err(model_error)?;
    let candidate =
        crate::QuantileModelPair::from_model_strings(&candidate_strings).map_err(model_error)?;
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
        Some(model) => model
            .load_pair()?
            .predict(&validation_features)
            .map_err(model_error)?,
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
        activation_sample_id,
        feature_version: MFCE_FEATURE_VERSION,
        feature_count: MFCE_MODEL_FEATURE_COUNT as u32,
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
    raw_model_predictions: Option<&[crate::QuantilePrediction]>,
) -> Result<Vec<crate::QuantilePrediction>, MfceError> {
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
            // Mirror live predict() support selection using the training
            // prefix only, so the gate scores the served prediction.
            let origin_support = training_prefix
                .iter()
                .filter(|s| s.features.origin() == sample.features.origin())
                .count();
            if origin_support < MFCE_MIN_BACKOFF_SAMPLES {
                // Live serves the conservative bootstrap prior here and
                // ignores any raw model output.
                let q10 = MFCE_BOOTSTRAP_Q10_BPS
                    .to_f64()
                    .ok_or(MfceError::Arithmetic)?;
                let q50 = MFCE_BOOTSTRAP_Q50_BPS
                    .to_f64()
                    .ok_or(MfceError::Arithmetic)?;
                return Ok(crate::QuantilePrediction { q10, q50 });
            }
            let objective_has_support = training_prefix
                .iter()
                .filter(|s| {
                    s.features.origin() == sample.features.origin()
                        && s.objective == sample.objective
                })
                .count()
                >= MFCE_MIN_BACKOFF_SAMPLES;
            let conditional = compose_conditional_quantiles(
                training_prefix.iter().filter(|s| {
                    s.features.origin() == sample.features.origin()
                        && (!objective_has_support || s.objective == sample.objective)
                }),
                &sample.asset,
                sample.direction,
                base_quantiles,
            )?;
            Ok(crate::QuantilePrediction {
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
                <= self.incumbent_q10_pinball + self.incumbent_q50_pinball;
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
    candidate: &[crate::QuantilePrediction],
    incumbent: &[crate::QuantilePrediction],
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
        | MfceAdmissionState::Rejected { .. } => {
            allocation_baseline_target(current_target, desired_target)
        }
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
    allocation_baseline_target(current, desired) != desired
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
        || ![MFCE_FEATURE_COUNT as u32, MFCE_MODEL_FEATURE_COUNT as u32]
            .contains(&model.feature_count)
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
    let mut values = Vec::with_capacity(samples.len() * MFCE_MODEL_FEATURE_COUNT);
    for sample in samples {
        values.extend(sample.features.as_f64()?);
    }
    Ok(values)
}

fn labels_f64(samples: &[MfceTrainingSample]) -> Result<Vec<f64>, MfceError> {
    samples
        .iter()
        .map(|sample| {
            sample
                .remaining_net_edge_bps
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
    use std::fmt::Write;

    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
            hex
        })
}

fn model_error(error: crate::MfceError) -> MfceError {
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
        remaining_net_edge_bps: i64,
    ) -> MfceTrainingSample {
        MfceTrainingSample {
            sample_id,
            asset: asset.to_string(),
            direction,
            transition_kind: MfceTransitionKind::Opening,
            opened_at_mono: sample_id * 1_000,
            completed_at_mono: sample_id * 1_000 + 500,
            features: features(remaining_net_edge_bps),
            objective: LearningObjective::EntryQuality,
            remaining_net_edge_bps: Decimal::from(remaining_net_edge_bps),
            lifetime_seconds: Decimal::new(5, 1),
            admitted: sample_id % 2 == 0,
        }
    }

    #[test]
    fn source_lifecycle_without_executable_observations_is_not_a_label() {
        let mut engine = MfceEngine::default();
        for (exposure, price) in [(Decimal::ONE, 100), (Decimal::ZERO, 110)] {
            engine.note_accepted_source_snapshot().unwrap();
            engine
                .observe_raw_source(
                    "BTC",
                    true,
                    exposure,
                    exposure * Decimal::from(50),
                    Decimal::ZERO,
                    Decimal::from(price),
                    price as u64,
                    features(5),
                )
                .unwrap();
        }
        assert!(engine.state.samples.is_empty());
    }

    #[test]
    fn rejected_reversal_flattens_but_rejected_expansion_holds() {
        for (desired, expected) in [(80, 40), (-80, 0), (20, 20)] {
            assert_eq!(
                allocation_baseline_target(Decimal::from(40), Decimal::from(desired)),
                Decimal::from(expected),
            );
        }
    }

    #[test]
    fn only_accepted_source_changes_generate_candidates_and_reductions_bypass() {
        let mut engine = MfceEngine::default();
        // Unaccepted, unchanged-but-accepted, expansion, then reduction.
        for (step, (accepted, exposure, desired, current, price, needs_book, effective)) in [
            (false, 5, 50, 0, 100, false, 0),
            (true, 5, 50, 0, 100, false, 0),
            (true, 8, 80, 40, 101, true, 40),
            (true, 3, 30, 40, 102, false, 30),
        ]
        .into_iter()
        .enumerate()
        {
            if accepted {
                engine.note_accepted_source_snapshot().unwrap();
            }
            let update = engine
                .observe_raw_source(
                    "BTC",
                    accepted,
                    Decimal::new(exposure, 1),
                    Decimal::from(desired),
                    Decimal::from(current),
                    Decimal::from(price),
                    (step as u64 + 1) * 1_000,
                    features(exposure),
                )
                .unwrap();
            assert_eq!(update.needs_live_book, needs_book);
            assert_eq!(update.effective_target, Decimal::from(effective));
            let state = &engine.state.assets["BTC"];
            match step {
                0 | 1 => assert!(matches!(
                    state.admission,
                    MfceAdmissionState::Rejected {
                        reason: MfceRejectionReason::NotAcceptedSourceTransition,
                        ..
                    }
                )),
                2 => assert_eq!(
                    state.active.as_ref().unwrap().transition_kind,
                    MfceTransitionKind::Expansion
                ),
                _ => assert!(matches!(state.admission, MfceAdmissionState::Bypass)),
            }
            assert!(engine.state.samples.is_empty());
        }
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
                Decimal::from(0),
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
                Decimal::from(0),
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
                Decimal::from(0),
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
                Decimal::from(0),
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
    fn exploit_requires_the_after_cost_lower_decile_to_clear_the_floor() {
        let evaluate = |q10| {
            let mut input = allocation_candidate("BTC", 1, q10, 110, 20, 10, 100).input;
            input.copytrade_conviction = Decimal::ONE;
            evaluate_allocation_policy(&input).unwrap()
        };

        let below_floor = evaluate(20);
        assert_eq!(below_floor.policy_state, MfcePolicyState::Explore);
        assert_eq!(below_floor.reason, None);
        assert_eq!(evaluate(21).policy_state, MfcePolicyState::Explore);
        assert_eq!(evaluate(22).policy_state, MfcePolicyState::Exploit);
    }

    #[test]
    fn policy_separates_economic_rejection_from_hard_tail_allocation() {
        let evaluate = |q10, q50, friction, uncertainty, requested, tail| {
            let mut input =
                allocation_candidate("BTC", 1, q10, q50, friction, uncertainty, requested).input;
            input.copytrade_conviction = Decimal::ONE;
            input.remaining_tail_loss_budget_usd = Decimal::from(tail);
            evaluate_allocation_policy(&input).unwrap()
        };
        let exploit = evaluate(30, 92, 14, 10, 1_000, 1_000);
        assert_eq!(exploit.policy_state, MfcePolicyState::Exploit);
        assert_eq!(exploit.conservative_edge_bps, Decimal::from(68));
        assert!(exploit.allocation_fraction > Decimal::new(7, 1));

        let explore = evaluate(-30, 100, 17, 24, 1_000, 1_000);
        assert_eq!(explore.policy_state, MfcePolicyState::Explore);
        assert_eq!(explore.conservative_edge_bps, Decimal::from(59));
        assert!(explore.allocation_fraction <= MFCE_EXPLORE_POOL_FRACTION);
        assert!(explore.allocation_fraction > Decimal::new(2, 1));

        let negative = evaluate(-144, -19, 31, 10, 1_000, 1_000);
        assert_eq!(negative.policy_state, MfcePolicyState::Reject);
        assert_eq!(
            negative.reason,
            Some(MfceRejectionReason::StrongNegativeExpectancy)
        );
        let zero_edge = evaluate(-20, 20, 20, 10, 1_000, 1_000);
        assert_eq!(zero_edge.policy_state, MfcePolicyState::Explore);
        assert_eq!(zero_edge.reason, None);
        let break_even_upper = evaluate(20, 20, 20, 0, 1_000, 1_000);
        assert_eq!(break_even_upper.policy_state, MfcePolicyState::Explore);
        assert_eq!(break_even_upper.reason, None);
        let negative_upper = evaluate(19, 19, 20, 0, 1_000, 1_000);
        assert_eq!(negative_upper.policy_state, MfcePolicyState::Reject);
        assert_eq!(
            negative_upper.reason,
            Some(MfceRejectionReason::StrongNegativeExpectancy)
        );

        for (q10, q50, state) in [
            (-120, 90, MfcePolicyState::Explore),
            (30, 150, MfcePolicyState::Exploit),
        ] {
            let tail = evaluate(q10, q50, 20, 10, 10_000, 0);
            assert_eq!(tail.policy_state, state);
            if state == MfcePolicyState::Explore {
                assert!(!tail.admitted);
                assert_eq!(tail.reason, Some(MfceRejectionReason::TailBudgetExceeded));
                assert_eq!(tail.allocation_fraction, Decimal::ZERO);
                assert_eq!(tail.modeled_tail_loss_usd, Decimal::ZERO);
            } else {
                // A calibrated positive after-cost q10 has no modeled downside
                // claim, so a zero tail-loss budget does not reject it.
                assert!(tail.admitted);
                assert_eq!(tail.reason, None);
            }
        }
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
            minimum_executable_increment: Decimal::ZERO,
        }
    }

    #[test]
    fn cross_sectional_allocation_is_order_independent_and_favors_score() {
        let strong = allocation_candidate("BTC", 1, 40, 120, 10, 10, 1_000);
        let weak = allocation_candidate("ETH", 2, 25, 80, 10, 20, 1_000);
        let forward = allocate_cross_sectional(
            &[strong.clone(), weak.clone()],
            Decimal::from(1_000),
            Decimal::from(1_000),
            Decimal::from(1_000),
        )
        .unwrap();
        let reverse = allocate_cross_sectional(
            &[weak, strong],
            Decimal::from(1_000),
            Decimal::from(1_000),
            Decimal::from(1_000),
        )
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
    fn unsupported_direction_explores_with_one_global_information_budget() {
        // Edge-only: negative upper edge Rejects regardless of support/model.
        // q10=-100,q50=-50,friction=10,unc=20 => net_q50=-60, upper=-40<0.
        let mut low_conviction = allocation_candidate("LOW", 1, -100, -50, 10, 20, 1_000);
        low_conviction.input.prediction.used_model = false;
        low_conviction.input.prediction.direction_sample_count = 1;
        low_conviction.input.prediction.asset_direction_sample_count = 1;
        low_conviction.input.copytrade_conviction = Decimal::ZERO;
        let mut high_conviction = allocation_candidate("HIGH", 2, -100, -50, 10, 20, 1_000);
        high_conviction.input.prediction.used_model = false;
        high_conviction.input.prediction.direction_sample_count = 1;
        high_conviction
            .input
            .prediction
            .asset_direction_sample_count = 1;
        high_conviction.input.copytrade_conviction = Decimal::ONE;

        let decisions = allocate_cross_sectional(
            &[low_conviction, high_conviction],
            Decimal::from(10_000),
            Decimal::from(10_000),
            Decimal::from(10_000),
        )
        .unwrap();
        assert_eq!(decisions["LOW"].policy_state, MfcePolicyState::Reject);
        assert_eq!(decisions["HIGH"].policy_state, MfcePolicyState::Reject);

        let mut model_backed = allocation_candidate("MODEL", 3, -100, -50, 10, 20, 1_000);
        model_backed.input.prediction.direction_sample_count = 1;
        model_backed.input.prediction.asset_direction_sample_count = 0;
        model_backed.minimum_executable_increment = Decimal::TEN;
        let decision = allocate_cross_sectional(
            &[model_backed],
            Decimal::from(10_000),
            Decimal::from(10_000),
            Decimal::from(150),
        )
        .unwrap();
        assert_eq!(decision["MODEL"].policy_state, MfcePolicyState::Reject);

        let supported_negative = allocation_candidate("SUPPORTED", 4, -100, -50, 10, 20, 1_000);
        let decision = allocate_cross_sectional(
            &[supported_negative],
            Decimal::from(10_000),
            Decimal::from(10_000),
            Decimal::from(150),
        )
        .unwrap();
        assert_eq!(decision["SUPPORTED"].policy_state, MfcePolicyState::Reject);
        assert_eq!(
            decision["SUPPORTED"].reason,
            Some(MfceRejectionReason::StrongNegativeExpectancy)
        );
    }

    #[test]
    fn exploit_requires_conditional_direction_support() {
        // Edge-only: support gate collapsed. Exploit iff net_q10>1,
        // regardless of model/support. q10=30,q50=100,friction=10,unc=10
        // gives net_q10=20>1 => Exploit with full size.
        let mut unsupported = allocation_candidate("MODEL", 1, 30, 100, 10, 10, 1_000);
        unsupported.input.prediction.direction_sample_count = 1;
        unsupported.input.prediction.asset_direction_sample_count = 0;
        unsupported.minimum_executable_increment = Decimal::TEN;
        let decisions = allocate_cross_sectional(
            &[unsupported],
            Decimal::from(10_000),
            Decimal::from(10_000),
            Decimal::from(150),
        )
        .unwrap();
        assert_eq!(decisions["MODEL"].policy_state, MfcePolicyState::Exploit);
        assert_eq!(decisions["MODEL"].allocation_fraction, Decimal::ONE);
    }

    #[test]
    fn explore_reserves_exchange_valid_breadth_before_deepening_probes() {
        let candidates = (0..5)
            .map(|index| {
                let mut candidate =
                    allocation_candidate(&format!("A{index}"), index + 1, -100, 20, 10, 20, 100);
                candidate.input.prediction.used_model = false;
                candidate.minimum_executable_increment = Decimal::TEN;
                candidate
            })
            .collect::<Vec<_>>();
        let decisions = allocate_cross_sectional(
            &candidates,
            Decimal::from(30),
            Decimal::from(1_000),
            Decimal::from(30),
        )
        .unwrap();
        let admitted = decisions
            .values()
            .filter(|decision| decision.admitted)
            .collect::<Vec<_>>();
        assert_eq!(admitted.len(), 3);
        assert!(admitted
            .iter()
            .all(|decision| decision.allocation_fraction == Decimal::new(1, 1)));
        assert_eq!(
            decisions
                .values()
                .filter(|decision| {
                    decision.reason == Some(MfceRejectionReason::AllocationBudgetExhausted)
                })
                .count(),
            2
        );
    }

    #[test]
    fn structurally_sub_floor_explore_does_not_consume_allocation() {
        let mut candidate = allocation_candidate("DUST", 1, -100, 20, 10, 20, 20);
        candidate.input.prediction.used_model = false;
        candidate.minimum_executable_increment = Decimal::TEN;
        let decisions = allocate_cross_sectional(
            &[candidate],
            Decimal::from(100),
            Decimal::from(1_000),
            Decimal::from(100),
        )
        .unwrap();
        assert!(!decisions["DUST"].admitted);
        assert_eq!(
            decisions["DUST"].reason,
            Some(MfceRejectionReason::BelowExchangeMinimum)
        );
        assert_eq!(decisions["DUST"].allocation_fraction, Decimal::ZERO);
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

        let incumbent = allocation_candidate("BTC", 1, 30, 120, 10, 10, 100);
        let source_capacity = Decimal::from(100);
        let committed_baseline = allocation_baseline_target(
            incumbent.input.current_position_notional,
            incumbent.input.proposed_position_notional,
        )
        .abs();
        let available = source_capacity.checked_sub(committed_baseline).unwrap();
        let decisions =
            allocate_cross_sectional(&[incumbent], available, Decimal::from(1_000), available)
                .unwrap();
        assert_eq!(decisions["BTC"].allocation_fraction, Decimal::ONE);
    }

    #[test]
    fn epistemically_cold_explore_shares_one_global_information_budget() {
        let candidates = (0..30)
            .map(|index| {
                let mut candidate =
                    allocation_candidate(&format!("A{index:02}"), index + 1, -40, 30, 10, 25, 100);
                candidate.input.prediction.used_model = false;
                candidate
            })
            .collect::<Vec<_>>();
        let decisions = allocate_cross_sectional(
            &candidates,
            Decimal::from(1_000),
            Decimal::from(1_000),
            Decimal::from(150),
        )
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
        assert!(allocated > Decimal::ZERO);
        assert!(allocated <= Decimal::from(150));
        assert!(
            decisions
                .values()
                .all(|decision| decision.policy_state == MfcePolicyState::Explore),
            "unexpected decisions: {decisions:?}"
        );
    }

    #[test]
    fn existing_information_probe_consumes_the_global_budget() {
        // Edge-only: information budget is unbounded (1.0); committed cold
        // Explore is diagnostics only and never caps admission.
        assert_eq!(
            remaining_explore_information_budget(Decimal::from(1_000), Decimal::ZERO).unwrap(),
            Decimal::from(1_000)
        );
        assert_eq!(
            remaining_explore_information_budget(Decimal::from(1_000), Decimal::from(140)).unwrap(),
            Decimal::from(1_000)
        );
        assert_eq!(
            remaining_explore_information_budget(Decimal::from(1_000), Decimal::from(200)).unwrap(),
            Decimal::from(1_000)
        );
    }

    #[test]
    fn exploit_has_first_claim_and_explore_uses_only_idle_notional_capacity() {
        let exploit = allocation_candidate("PROVEN", 1, 30, 100, 10, 10, 1_000);
        let explore = allocation_candidate("UNKNOWN", 2, -40, 65, 10, 25, 100);
        let decisions = allocate_cross_sectional(
            &[exploit, explore],
            Decimal::from(100),
            Decimal::from(1_000),
            Decimal::from(100),
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
        // Edge-only: conservative>0 gives full size (1.0). q10=-1000,q50=140,
        // friction=10,unc=100 => net_q50=130, conservative=30>0 => Explore
        // with allocation 1.0.
        let candidate = allocation_candidate("MEME", 1, -1_000, 140, 10, 100, 1_000);
        let decisions = allocate_cross_sectional(
            &[candidate],
            Decimal::from(1_000),
            Decimal::from(1_000),
            Decimal::from(1_000),
        )
        .unwrap();
        let decision = &decisions["MEME"];
        assert_eq!(decision.policy_state, MfcePolicyState::Explore);
        assert_eq!(decision.allocation_fraction, Decimal::ONE);
    }

    #[test]
    fn explore_pool_cannot_crowd_out_exploit_and_tail_budget_stays_global() {
        let exploit = allocation_candidate("BTC", 1, 30, 100, 10, 10, 1_000);
        let explore = allocation_candidate("ETH", 2, -90, 65, 10, 25, 1_000);
        let decisions = allocate_cross_sectional(
            &[exploit, explore],
            Decimal::from(1_000),
            Decimal::from(15),
            Decimal::from(1_000),
        )
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
        // Edge-only: explore pool is 1.0, so full size on positive edge.
        assert_eq!(
            allocated_target(Decimal::ZERO, Decimal::from(100), Decimal::ONE,).unwrap(),
            Decimal::from(100)
        );
        assert_eq!(
            allocated_target(Decimal::from(50), Decimal::from(100), Decimal::ONE,).unwrap(),
            Decimal::from(100)
        );
        assert_eq!(
            allocated_target(Decimal::from(50), Decimal::from(-100), Decimal::ONE,).unwrap(),
            Decimal::from(-100)
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
    fn refreshed_inference_does_not_ratchet_the_source_cap() {
        let mut engine = MfceEngine::default();
        let transition = engine
            .observe_raw_source(
                "BTC",
                true,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::from(0),
                Decimal::from(100),
                1_000,
                features(5),
            )
            .unwrap()
            .transition_id
            .unwrap();
        for cost in [2, 20] {
            engine
                .update_pending_live_context("BTC", transition, [Decimal::from(cost); 5])
                .unwrap();
            assert_eq!(
                engine
                    .pending_position_notional("BTC", transition, Decimal::from(100))
                    .unwrap(),
                Decimal::from(100)
            );
            assert_eq!(
                engine.state.assets["BTC"]
                    .active
                    .as_ref()
                    .unwrap()
                    .features
                    .values[11],
                Decimal::from(cost)
            );
        }
        assert!(engine.state.samples.is_empty());
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
        // Edge-only bootstrap is neutral (0/0/0). With friction 10, net is
        // negative so upper<0 => Reject (don't trade at a loss).
        let decision = evaluate_allocation_policy(&MfceAllocationInput {
            prediction,
            friction_bps: Decimal::from(10),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        assert_eq!(decision.policy_state, MfcePolicyState::Reject);
        assert_eq!(
            decision.reason,
            Some(MfceRejectionReason::StrongNegativeExpectancy)
        );
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
                Decimal::from(0),
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
    fn first_native_q10_q50_training_attempt_starts_at_forty_net_labels() {
        let mut engine = MfceEngine::default();
        engine.state.samples = (1..MFCE_MIN_TRAINING_SAMPLES as u64)
            .map(|id| training_sample(id, "BTC", MfceDirection::Long, id as i64))
            .collect();
        engine.state.next_sample_id = MFCE_MIN_TRAINING_SAMPLES as u64;
        assert!(!engine.maybe_start_training().unwrap());

        let sample_id = MFCE_MIN_TRAINING_SAMPLES as u64;
        engine.state.samples.push_back(training_sample(
            sample_id,
            "BTC",
            MfceDirection::Long,
            sample_id as i64,
        ));
        engine.state.next_sample_id = sample_id + 1;
        assert_eq!(MFCE_MIN_TRAINING_SAMPLES, 40);
        assert!(engine.maybe_start_training().unwrap());
    }

    #[test]
    fn pending_unpromoted_candidate_cannot_produce_exploit() {
        let mut engine = MfceEngine::default();
        engine.state.samples = (1..=MFCE_MIN_TRAINING_SAMPLES as u64)
            .map(|id| training_sample(id, "BTC", MfceDirection::Long, 25))
            .collect();
        engine.state.next_sample_id = MFCE_MIN_TRAINING_SAMPLES as u64 + 1;
        engine.state.pending_training = Some(MfceTrainingAttempt {
            attempted_through_sample_id: MFCE_MIN_TRAINING_SAMPLES as u64,
            activation_sample_id: MFCE_MIN_TRAINING_SAMPLES as u64 + 1,
            candidate_epoch: 1,
            incumbent_epoch: None,
        });

        let prediction = engine
            .predict("BTC", MfceDirection::Long, &features(0))
            .unwrap();
        assert!(!prediction.used_model);
        assert_eq!(prediction.model_epoch, 0);

        let mut candidate = allocation_candidate("BTC", 1, 30, 120, 10, 10, 100);
        candidate.input.prediction = prediction;
        let decisions = allocate_cross_sectional(
            &[candidate],
            Decimal::from(100),
            Decimal::from(1_000),
            Decimal::from(15),
        )
        .unwrap();
        // Edge-only: Exploit iff net_q10>1 regardless of model/support.
        // Unpromoted prediction with q10=30,q50=120,friction=10 gives
        // net_q10=20>1 => Exploit.
        assert_eq!(decisions["BTC"].policy_state, MfcePolicyState::Exploit);
    }

    #[test]
    fn edge_only_zero_label_explore_on_positive_conservative() {
        // Acceptance: 0-label (used_model=false, counts 0) admits Explore on
        // conservative>0. Manual positive-edge prediction with no support.
        let prediction = MfcePrediction {
            model_epoch: 0,
            q10_gross_bps: Decimal::ZERO,
            q50_gross_bps: Decimal::from(50),
            uncertainty_bps: Decimal::from(10),
            pooled_sample_count: 0,
            direction_sample_count: 0,
            asset_direction_sample_count: 0,
            used_model: false,
        };
        let decision = evaluate_allocation_policy(&MfceAllocationInput {
            prediction,
            friction_bps: Decimal::from(10),
            copytrade_conviction: Decimal::ONE,
            current_position_notional: Decimal::ZERO,
            proposed_position_notional: Decimal::from(1_000),
            remaining_tail_loss_budget_usd: Decimal::from(1_000),
        })
        .unwrap();
        // net_q50=40, conservative=30>0, net_q10=-10<=1 => Explore.
        assert_eq!(decision.policy_state, MfcePolicyState::Explore);
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
        let candidate = train_candidate(&samples, None, 1, 501)
            .unwrap()
            .expect("chronologically calibrated candidate");
        assert_eq!(candidate.trained_through_sample_id, 400);
        assert_eq!(candidate.activation_sample_id, 501);

        let mut engine = MfceEngine::default();
        engine.state.samples = samples.into();
        engine.state.next_sample_id = 501;
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
        assert_eq!(incumbent.activation_sample_id, 501);
        let served = engine
            .predict("BTC", MfceDirection::Long, &features(0))
            .unwrap();
        assert_eq!(served.model_epoch, 1);
        assert!(served.used_model);
    }

    #[test]
    fn sample_ring_is_bounded_and_deterministic() {
        let mut engine = MfceEngine::default();
        for index in 0..=MFCE_MAX_SAMPLES {
            engine.learn_delayed(
                "BTC",
                features(index as i64),
                MfceDirection::Long,
                index as u64,
                index as u64 + 900_000,
                Decimal::ONE,
            );
        }
        assert_eq!(engine.state.samples.len(), MFCE_MAX_SAMPLES);
        assert_eq!(engine.state.samples.front().unwrap().sample_id, 2);
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
        let raw = vec![crate::QuantilePrediction {
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
        let crossed = vec![crate::QuantilePrediction {
            q10: 10.0,
            q50: 9.0,
        }];

        assert!(matches!(
            served_validation_predictions(&training, &validation, Some(&crossed)),
            Err(MfceError::Model(message)) if message.contains("crossed")
        ));
        assert!(matches!(
            chronological_validation_score(&[0.0], &crossed, &[crate::QuantilePrediction {
                q10: -1.0,
                q50: 1.0,
            }]),
            Err(MfceError::Model(message)) if message.contains("crossed")
        ));
    }

    #[test]
    fn q10_undercoverage_rejects_promotion_despite_better_pinball_loss() {
        let labels = vec![0.0; 16];
        let candidate = vec![crate::QuantilePrediction { q10: 1.0, q50: 2.0 }; labels.len()];
        let incumbent = vec![
            crate::QuantilePrediction {
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
