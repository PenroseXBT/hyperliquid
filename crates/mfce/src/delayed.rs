//! Decision-time evidence only. Nothing in this module authorizes an action.
use crate::lifecycle::{
    LearningObjective, LegacyMfceFeatureVectorV12D, MfceDirection, MfceFeatureVector,
    MfcePolicyState, PositionTrajectoryFeatures,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const HORIZONS_MS: [u64; 7] = [
    30_000, 120_000, 300_000, 900_000, 1_800_000, 3_600_000, 14_400_000,
];
pub const HORIZON_COUNT: usize = HORIZONS_MS.len();
pub const HORIZON_5M: usize = 2;
pub const HORIZON_15M: usize = 3;
// 104 concurrently evaluated markets can legitimately carry 32 frozen
// counterfactual states each. The old 1,024 ring guaranteed saturation before
// that breadth was represented, so selected fills competed with loop churn.
pub const MAX_DECISIONS: usize = 4_096;
pub const MAX_PER_MARKET: usize = 32;
const OBSERVATION_GRACE_MS: u64 = 2_000;
// A counterfactual heartbeat is useful for regime coverage, but repeated
// repricing of the same economic state is not a new trial. Material state
// changes bypass this interval; selected executable decisions always retain.
const COUNTERFACTUAL_REFRESH_MS: u64 = 3_600_000;
const BPS: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlphaOrigin {
    #[default]
    Source,
    Flow,
    SourceFlowConfluence,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlphaPerformance {
    pub samples: u64,
    pub wins: u64,
    pub gross_pnl: Decimal,
    pub net_pnl: Decimal,
    pub gains: Decimal,
    pub losses: Decimal,
    pub profit_factor: Option<Decimal>,
    pub win_rate: Option<Decimal>,
    pub max_executable_notional: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionKind {
    Open,
    Add,
    Hold,
    Reduce,
    Exit,
    Reject,
    BudgetConstrained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionProvenance {
    Selected,
    MfceRejected,
    AllocatorDisplaced,
    RiskLimited,
    Unexecuted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictionObservation {
    #[serde(default)]
    pub predicted_q10_gross_edge: Decimal,
    pub predicted_gross_edge: Decimal,
    pub expected_friction: Decimal,
    #[serde(default)]
    pub predicted_q10_net_edge: Decimal,
    pub predicted_net_edge: Decimal,
    #[serde(default)]
    pub horizon_ms: u64,
    #[serde(default)]
    pub model_epoch: u64,
    #[serde(default)]
    pub objective: LearningObjective,
    #[serde(default)]
    pub feature_snapshot_id: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemainingEdgePredictionPoint {
    pub horizon_ms: u64,
    pub model_epoch: u64,
    pub q10_net_bps: Decimal,
    pub q50_net_bps: Decimal,
    pub uncertainty_bps: Decimal,
    pub used_model: bool,
    #[serde(default)]
    pub objective: LearningObjective,
    #[serde(default)]
    pub feature_snapshot_id: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SizeRegretPoint {
    pub additional_quantity: Decimal,
    pub cumulative_additional_quantity: Decimal,
    pub net_edge_bps: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelUnavailableReason {
    MissingDecisionAnchor,
    MissingFundingContinuity,
    MissingExecutableDepth,
    NoTimelyObservation,
    NoLegalLargerSlice,
    MissingLargerExitDepth,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelComponentUnavailable {
    pub gross_executable_movement: Option<LabelUnavailableReason>,
    pub entry_slippage: Option<LabelUnavailableReason>,
    pub exit_slippage: Option<LabelUnavailableReason>,
    pub fees: Option<LabelUnavailableReason>,
    pub funding: Option<LabelUnavailableReason>,
    pub net_remaining_edge: Option<LabelUnavailableReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealizedOutcome {
    pub gross_pnl: Decimal,
    pub fees: Decimal,
    pub slippage: Decimal,
    /// Signed credit, unlike the cost-positive live funding allowance.
    pub funding: Decimal,
    pub net_pnl: Decimal,
}

impl RealizedOutcome {
    /// Actual prices already contain slippage. Keep the statistic, but do not
    /// deduct it from settled cash a second time.
    pub fn settled(
        gross: Decimal,
        fees: Decimal,
        slippage: Decimal,
        funding: Decimal,
    ) -> Option<Self> {
        Some(Self {
            gross_pnl: gross,
            fees,
            slippage,
            funding,
            net_pnl: gross.checked_sub(fees)?.checked_add(funding)?,
        })
    }
    pub fn new(gross: Decimal, fees: Decimal, slippage: Decimal, funding: Decimal) -> Option<Self> {
        Some(Self {
            gross_pnl: gross,
            fees,
            slippage,
            funding,
            net_pnl: gross
                .checked_sub(fees)?
                .checked_sub(slippage)?
                .checked_add(funding)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutableAnchor {
    pub quantity: Decimal,
    pub midpoint: Decimal,
    pub fill_price: Decimal,
    pub fees: Decimal,
    /// False for counterfactual entry/exit quotes; true only after settled fills.
    pub actual: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionSample {
    pub id: u64,
    pub asset: String,
    pub decision_at: u64,
    pub kind: DecisionKind,
    pub origin: AlphaOrigin,
    pub provenance: DecisionProvenance,
    pub mode: Option<MfcePolicyState>,
    pub direction: MfceDirection,
    pub features: MfceFeatureVector,
    pub prediction: Option<PredictionObservation>,
    pub proposed_delta: Decimal,
    pub actual_delta: Decimal,
    /// External association only; PendingAction has no learning fields.
    pub cloid: Option<String>,
    pub anchor: Option<ExecutableAnchor>,
    pub forward: [Option<RealizedOutcome>; HORIZON_COUNT],
    pub pending_mask: u8,
    pub mfe_net_bps: Option<Decimal>,
    pub mae_net_bps: Option<Decimal>,
    pub time_to_mfe_ms: Option<u64>,
    pub entry_regret_bps: [Option<Decimal>; HORIZON_COUNT],
    pub exit_regret_bps: [Option<Decimal>; HORIZON_COUNT],
    pub cost_drag_bps: [Option<Decimal>; HORIZON_COUNT],
    pub funding_credit: Option<Decimal>,
    pub funding_start: u64,
    pub actual_result: Option<RealizedOutcome>,
    /// Unavailable unless a larger legal size was actually depth-evaluated.
    pub size_regret_bps: [Option<Decimal>; HORIZON_COUNT],
    #[serde(default)]
    pub prediction_curve: Vec<RemainingEdgePredictionPoint>,
    #[serde(default)]
    pub larger_anchors: Vec<ExecutableAnchor>,
    #[serde(default)]
    pub size_regret_curve_bps: [Vec<SizeRegretPoint>; HORIZON_COUNT],
    #[serde(default)]
    pub label_unavailable: [Option<LabelUnavailableReason>; HORIZON_COUNT],
    #[serde(default)]
    pub component_unavailable: [LabelComponentUnavailable; HORIZON_COUNT],
    #[serde(default)]
    pub size_regret_unavailable: [Option<LabelUnavailableReason>; HORIZON_COUNT],
    /// Frozen at decision time when this entry follows a recent full exit.
    #[serde(default)]
    pub reentry_after_exit: bool,
    /// Incremental value of retaining exposure instead of exiting now.
    #[serde(default)]
    pub hold_regret_bps: [Option<Decimal>; HORIZON_COUNT],
    /// After-cost value of reopening shortly after the preceding exit.
    #[serde(default)]
    pub reentry_regret_bps: [Option<Decimal>; HORIZON_COUNT],
}

impl DecisionSample {
    pub fn retaining_slice(&self) -> bool {
        matches!(
            self.kind,
            DecisionKind::Hold | DecisionKind::Reduce | DecisionKind::Exit
        ) || (matches!(
            self.kind,
            DecisionKind::Reject | DecisionKind::BudgetConstrained
        ) && self.features.objective() == LearningObjective::ContinuationQuality)
    }
    fn economics(
        &self,
        exit: &ExecutableAnchor,
        entry: &ExecutableAnchor,
    ) -> Option<RealizedOutcome> {
        let sign = Decimal::from(self.direction.sign());
        let quantity = entry.quantity;
        // Retention is incremental from the exit point, including the exit
        // fee saved now. Entry/add gross stays separate from both fill costs.
        let reference = entry.midpoint;
        let entry_slippage = entry
            .fill_price
            .checked_sub(entry.midpoint)?
            .checked_mul(sign)?
            .checked_mul(quantity)?;
        let entry_fee = if self.retaining_slice() {
            -entry.fees
        } else {
            entry.fees
        };
        let gross = exit
            .midpoint
            .checked_sub(reference)?
            .checked_mul(sign)?
            .checked_mul(quantity)?;
        let slippage = exit
            .midpoint
            .checked_sub(exit.fill_price)?
            .checked_mul(sign)?
            .checked_mul(quantity)?
            .checked_add(entry_slippage)?;
        RealizedOutcome::new(
            gross,
            entry_fee.checked_add(exit.fees)?,
            slippage,
            self.funding_credit?
                .checked_mul(entry.quantity)?
                .checked_div(self.anchor.as_ref()?.quantity)?,
        )
    }
    fn incremental_size_economics(
        &self,
        exit: &ExecutableAnchor,
        entry: &ExecutableAnchor,
    ) -> Option<RealizedOutcome> {
        let sign = Decimal::from(self.direction.sign());
        let quantity = entry.quantity;
        let gross = exit
            .midpoint
            .checked_sub(entry.midpoint)?
            .checked_mul(sign)?
            .checked_mul(quantity)?;
        let entry_slippage = entry
            .fill_price
            .checked_sub(entry.midpoint)?
            .checked_mul(sign)?
            .checked_mul(quantity)?;
        let exit_slippage = exit
            .midpoint
            .checked_sub(exit.fill_price)?
            .checked_mul(sign)?
            .checked_mul(quantity)?;
        RealizedOutcome::new(
            gross,
            entry.fees.checked_add(exit.fees)?,
            entry_slippage.checked_add(exit_slippage)?,
            self.funding_credit?
                .checked_mul(quantity)?
                .checked_div(self.anchor.as_ref()?.quantity)?,
        )
    }
    fn bps(&self, value: Decimal) -> Option<Decimal> {
        let anchor = self.anchor.as_ref()?;
        value
            .checked_div(anchor.midpoint.checked_mul(anchor.quantity)?)?
            .checked_mul(BPS)
    }
}

fn component_unavailable(reason: LabelUnavailableReason) -> LabelComponentUnavailable {
    match reason {
        LabelUnavailableReason::MissingFundingContinuity => LabelComponentUnavailable {
            funding: Some(reason),
            net_remaining_edge: Some(reason),
            ..Default::default()
        },
        LabelUnavailableReason::MissingDecisionAnchor => LabelComponentUnavailable {
            gross_executable_movement: Some(reason),
            entry_slippage: Some(reason),
            fees: Some(reason),
            net_remaining_edge: Some(reason),
            ..Default::default()
        },
        LabelUnavailableReason::MissingExecutableDepth
        | LabelUnavailableReason::NoTimelyObservation => LabelComponentUnavailable {
            gross_executable_movement: Some(reason),
            exit_slippage: Some(reason),
            fees: Some(reason),
            net_remaining_edge: Some(reason),
            ..Default::default()
        },
        LabelUnavailableReason::NoLegalLargerSlice
        | LabelUnavailableReason::MissingLargerExitDepth => LabelComponentUnavailable::default(),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeProvenance {
    pub asset: String,
    pub opened_at: u64,
    pub closed_at: Option<u64>,
    pub transitions: Vec<(u64, AlphaOrigin)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled: Option<SettledEpisodeEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettledEpisodeEvidence {
    pub opening_action: String,
    pub external_manual_exit: bool,
    pub outcome: RealizedOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionProvenance {
    pub decision_at: u64,
    pub origin: AlphaOrigin,
    pub episodes: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceHistory {
    pub episodes: BTreeMap<String, EpisodeProvenance>,
    pub actions: BTreeMap<String, ActionProvenance>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trajectories: BTreeMap<String, PositionTrajectory>,
}

const LEGACY_V12_HORIZON_COUNT: usize = 5;

/// Exact three-field prediction written before quantile identity became durable.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyPredictionObservationV12 {
    pub predicted_gross_edge: Decimal,
    pub expected_friction: Decimal,
    pub predicted_net_edge: Decimal,
}

/// Exact 27-field decision row written by the delayed-learning v12 engines.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyDecisionSampleV12 {
    pub id: u64,
    pub asset: String,
    pub decision_at: u64,
    pub kind: DecisionKind,
    pub origin: AlphaOrigin,
    pub provenance: DecisionProvenance,
    pub mode: Option<MfcePolicyState>,
    pub direction: MfceDirection,
    pub features: LegacyMfceFeatureVectorV12D,
    pub prediction: Option<LegacyPredictionObservationV12>,
    pub proposed_delta: Decimal,
    pub actual_delta: Decimal,
    pub cloid: Option<String>,
    pub anchor: Option<ExecutableAnchor>,
    pub larger_anchor: Option<ExecutableAnchor>,
    pub forward: [Option<RealizedOutcome>; LEGACY_V12_HORIZON_COUNT],
    pub pending_mask: u8,
    pub mfe_net_bps: Option<Decimal>,
    pub mae_net_bps: Option<Decimal>,
    pub time_to_mfe_ms: Option<u64>,
    pub entry_regret_bps: [Option<Decimal>; LEGACY_V12_HORIZON_COUNT],
    pub exit_regret_bps: [Option<Decimal>; LEGACY_V12_HORIZON_COUNT],
    pub cost_drag_bps: [Option<Decimal>; LEGACY_V12_HORIZON_COUNT],
    pub funding_credit: Option<Decimal>,
    pub funding_start: u64,
    pub actual_result: Option<RealizedOutcome>,
    pub size_regret_bps: [Option<Decimal>; LEGACY_V12_HORIZON_COUNT],
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyEpisodeProvenanceV12B {
    pub asset: String,
    pub opened_at: u64,
    pub closed_at: Option<u64>,
    pub transitions: Vec<(u64, AlphaOrigin)>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyEpisodeProvenanceV12C {
    pub asset: String,
    pub opened_at: u64,
    pub closed_at: Option<u64>,
    pub transitions: Vec<(u64, AlphaOrigin)>,
    pub settled: Option<SettledEpisodeEvidence>,
}

/// Exact transitional episode wire shape written when absent settled evidence
/// was omitted from the trailing positional field. A single provenance map can
/// therefore contain both four- and five-field episode rows.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyEpisodeProvenanceV12D {
    pub asset: String,
    pub opened_at: u64,
    pub closed_at: Option<u64>,
    pub transitions: Vec<(u64, AlphaOrigin)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settled: Option<SettledEpisodeEvidence>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProvenanceHistoryV12B {
    pub episodes: BTreeMap<String, LegacyEpisodeProvenanceV12B>,
    pub actions: BTreeMap<String, ActionProvenance>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProvenanceHistoryV12C {
    pub episodes: BTreeMap<String, LegacyEpisodeProvenanceV12C>,
    pub actions: BTreeMap<String, ActionProvenance>,
}

/// Exact mixed four-/five-field provenance topology observed in the retained
/// production lineage. Deserialization rejects uniform maps so this adapter
/// cannot absorb either neighboring historical topology.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyProvenanceHistoryV12D {
    pub episodes: BTreeMap<String, LegacyEpisodeProvenanceV12D>,
    pub actions: BTreeMap<String, ActionProvenance>,
}

impl<'de> Deserialize<'de> for LegacyProvenanceHistoryV12D {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (wire_episodes, actions): (
            BTreeMap<String, serde_json::Value>,
            BTreeMap<String, ActionProvenance>,
        ) = Deserialize::deserialize(deserializer)?;
        let mut episodes = BTreeMap::new();
        let mut saw_four = false;
        let mut saw_five = false;
        for (id, wire) in wire_episodes {
            let fields = wire.as_array().ok_or_else(|| {
                serde::de::Error::custom("legacy provenance episode must be positional")
            })?;
            let episode = match fields.len() {
                4 => {
                    saw_four = true;
                    let (asset, opened_at, closed_at, transitions) = serde_json::from_value::<(
                        String,
                        u64,
                        Option<u64>,
                        Vec<(u64, AlphaOrigin)>,
                    )>(wire)
                    .map_err(serde::de::Error::custom)?;
                    LegacyEpisodeProvenanceV12D {
                        asset,
                        opened_at,
                        closed_at,
                        transitions,
                        settled: None,
                    }
                }
                5 => {
                    saw_five = true;
                    let (asset, opened_at, closed_at, transitions, settled) =
                        serde_json::from_value::<(
                            String,
                            u64,
                            Option<u64>,
                            Vec<(u64, AlphaOrigin)>,
                            Option<SettledEpisodeEvidence>,
                        )>(wire)
                        .map_err(serde::de::Error::custom)?;
                    let settled = settled.ok_or_else(|| {
                        serde::de::Error::custom(
                            "five-field legacy provenance episode requires settled evidence",
                        )
                    })?;
                    LegacyEpisodeProvenanceV12D {
                        asset,
                        opened_at,
                        closed_at,
                        transitions,
                        settled: Some(settled),
                    }
                }
                _ => {
                    return Err(serde::de::Error::custom(
                        "legacy provenance episode must have four or five fields",
                    ));
                }
            };
            episodes.insert(id, episode);
        }
        if !(saw_four && saw_five) {
            return Err(serde::de::Error::custom(
                "mixed legacy provenance requires both four- and five-field episodes",
            ));
        }
        Ok(Self { episodes, actions })
    }
}

/// Exact six-field delayed container written before provenance persistence.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyDelayedLearningV12A {
    pub samples: VecDeque<LegacyDecisionSampleV12>,
    pub next_id: u64,
    pub dropped: u64,
    pub flow: BTreeMap<String, MarketFlow>,
    pub turnover: BTreeMap<String, Turnover>,
    pub funding_at: Option<u64>,
}

/// Exact seven-field delayed container with four-field episode provenance.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyDelayedLearningV12B {
    pub samples: VecDeque<LegacyDecisionSampleV12>,
    pub next_id: u64,
    pub dropped: u64,
    pub flow: BTreeMap<String, MarketFlow>,
    pub turnover: BTreeMap<String, Turnover>,
    pub funding_at: Option<u64>,
    pub provenance: LegacyProvenanceHistoryV12B,
}

/// Exact seven-field delayed container with settled episode evidence.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyDelayedLearningV12C {
    pub samples: VecDeque<LegacyDecisionSampleV12>,
    pub next_id: u64,
    pub dropped: u64,
    pub flow: BTreeMap<String, MarketFlow>,
    pub turnover: BTreeMap<String, Turnover>,
    pub funding_at: Option<u64>,
    pub provenance: LegacyProvenanceHistoryV12C,
}

/// Exact seven-field delayed container containing the transitional mixed
/// provenance topology.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyDelayedLearningV12D {
    pub samples: VecDeque<LegacyDecisionSampleV12>,
    pub next_id: u64,
    pub dropped: u64,
    pub flow: BTreeMap<String, MarketFlow>,
    pub turnover: BTreeMap<String, Turnover>,
    pub funding_at: Option<u64>,
    pub provenance: LegacyProvenanceHistoryV12D,
}

impl LegacyDelayedLearningV12A {
    pub(crate) fn migrate(self) -> DelayedLearning {
        DelayedLearning {
            next_id: self.next_id,
            dropped: self.dropped.saturating_add(self.samples.len() as u64),
            flow: self.flow,
            turnover: self.turnover,
            funding_at: self.funding_at,
            ..Default::default()
        }
    }
}

impl LegacyDelayedLearningV12B {
    pub(crate) fn migrate(self) -> DelayedLearning {
        let episodes = self
            .provenance
            .episodes
            .into_iter()
            .map(|(id, episode)| {
                (
                    id,
                    EpisodeProvenance {
                        asset: episode.asset,
                        opened_at: episode.opened_at,
                        closed_at: episode.closed_at,
                        transitions: episode.transitions,
                        settled: None,
                    },
                )
            })
            .collect();
        DelayedLearning {
            next_id: self.next_id,
            dropped: self.dropped.saturating_add(self.samples.len() as u64),
            flow: self.flow,
            turnover: self.turnover,
            funding_at: self.funding_at,
            provenance: ProvenanceHistory {
                episodes,
                actions: self.provenance.actions,
                trajectories: BTreeMap::new(),
            },
            ..Default::default()
        }
    }
}

impl LegacyDelayedLearningV12C {
    pub(crate) fn migrate(self) -> DelayedLearning {
        let episodes = self
            .provenance
            .episodes
            .into_iter()
            .map(|(id, episode)| {
                (
                    id,
                    EpisodeProvenance {
                        asset: episode.asset,
                        opened_at: episode.opened_at,
                        closed_at: episode.closed_at,
                        transitions: episode.transitions,
                        settled: episode.settled,
                    },
                )
            })
            .collect();
        DelayedLearning {
            next_id: self.next_id,
            dropped: self.dropped.saturating_add(self.samples.len() as u64),
            flow: self.flow,
            turnover: self.turnover,
            funding_at: self.funding_at,
            provenance: ProvenanceHistory {
                episodes,
                actions: self.provenance.actions,
                trajectories: BTreeMap::new(),
            },
            ..Default::default()
        }
    }
}

impl LegacyDelayedLearningV12D {
    pub(crate) fn migrate(self) -> DelayedLearning {
        let episodes = self
            .provenance
            .episodes
            .into_iter()
            .map(|(id, episode)| {
                (
                    id,
                    EpisodeProvenance {
                        asset: episode.asset,
                        opened_at: episode.opened_at,
                        closed_at: episode.closed_at,
                        transitions: episode.transitions,
                        settled: episode.settled,
                    },
                )
            })
            .collect();
        DelayedLearning {
            next_id: self.next_id,
            dropped: self.dropped.saturating_add(self.samples.len() as u64),
            flow: self.flow,
            turnover: self.turnover,
            funding_at: self.funding_at,
            provenance: ProvenanceHistory {
                episodes,
                actions: self.provenance.actions,
                trajectories: BTreeMap::new(),
            },
            ..Default::default()
        }
    }
}

impl ProvenanceHistory {
    fn is_empty(&self) -> bool {
        self.episodes.is_empty() && self.actions.is_empty() && self.trajectories.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelayedLearning {
    pub samples: VecDeque<DecisionSample>,
    pub next_id: u64,
    pub dropped: u64,
    pub flow: BTreeMap<String, MarketFlow>,
    pub turnover: BTreeMap<String, Turnover>,
    pub funding_at: Option<u64>,
    #[serde(skip)]
    observed_at: BTreeMap<String, u64>,
    // Additive trailing fields: absent in older snapshots, never reconstructed
    // from today's origin. Opaque engine episode/CLOID strings confer no authority.
    #[serde(default, skip_serializing_if = "ProvenanceHistory::is_empty")]
    pub provenance: ProvenanceHistory,
}

impl DelayedLearning {
    #[allow(clippy::too_many_arguments)]
    pub fn observe_position(
        &mut self,
        asset: &str,
        episode_id: &str,
        opened_at: u64,
        observed_at: u64,
        signed_quantity: Decimal,
        average_entry_price: Decimal,
        realized_pnl: Decimal,
        mark: Decimal,
        equity: Decimal,
        policy: Option<MfcePolicyState>,
    ) -> Option<PositionTrajectoryFeatures> {
        if asset.is_empty()
            || episode_id.is_empty()
            || signed_quantity.is_zero()
            || average_entry_price <= Decimal::ZERO
            || mark <= Decimal::ZERO
            || equity <= Decimal::ZERO
            || observed_at < opened_at
        {
            return None;
        }
        if !self.provenance.trajectories.contains_key(asset)
            && self.provenance.trajectories.len() >= 512
        {
            if let Some(first) = self.provenance.trajectories.keys().next().cloned() {
                self.provenance.trajectories.remove(&first);
            }
        }
        let sign = Decimal::from(if signed_quantity.is_sign_positive() {
            1
        } else {
            -1
        });
        let unrealized_return_bps = mark
            .checked_sub(average_entry_price)?
            .checked_mul(sign)?
            .checked_div(average_entry_price)?
            .checked_mul(BPS)?;
        let trajectory = self
            .provenance
            .trajectories
            .entry(asset.to_owned())
            .or_insert_with(|| PositionTrajectory {
                episode_id: episode_id.to_owned(),
                opened_at,
                last_observed_at: observed_at,
                last_mark: mark,
                mfe_bps: unrealized_return_bps,
                mae_bps: unrealized_return_bps,
            });
        if trajectory.episode_id != episode_id {
            *trajectory = PositionTrajectory {
                episode_id: episode_id.to_owned(),
                opened_at,
                last_observed_at: observed_at,
                last_mark: mark,
                mfe_bps: unrealized_return_bps,
                mae_bps: unrealized_return_bps,
            };
        }
        let elapsed_ms = observed_at.saturating_sub(trajectory.last_observed_at);
        let price_velocity_bps_per_second =
            if elapsed_ms == 0 || trajectory.last_mark <= Decimal::ZERO {
                Decimal::ZERO
            } else {
                mark.checked_sub(trajectory.last_mark)?
                    .checked_mul(sign)?
                    .checked_div(trajectory.last_mark)?
                    .checked_mul(BPS)?
                    .checked_mul(Decimal::from(1_000))?
                    .checked_div(Decimal::from(elapsed_ms))?
            };
        trajectory.mfe_bps = trajectory.mfe_bps.max(unrealized_return_bps);
        trajectory.mae_bps = trajectory.mae_bps.min(unrealized_return_bps);
        trajectory.last_mark = mark;
        trajectory.last_observed_at = observed_at;
        Some(PositionTrajectoryFeatures {
            age_hours: Decimal::from(observed_at.saturating_sub(opened_at))
                .checked_div(Decimal::from(3_600_000))?,
            position_equity_fraction: signed_quantity
                .abs()
                .checked_mul(mark)?
                .checked_div(equity)?,
            mark_to_entry_bps: unrealized_return_bps,
            unrealized_return_bps,
            realized_equity_fraction: realized_pnl.checked_div(equity)?,
            mfe_bps: trajectory.mfe_bps,
            mae_bps: trajectory.mae_bps,
            drawdown_from_mfe_bps: trajectory.mfe_bps.checked_sub(unrealized_return_bps)?,
            price_velocity_bps_per_second,
            current_policy_code: Decimal::from(match policy {
                Some(MfcePolicyState::Exploit) => 2,
                Some(MfcePolicyState::Explore) => 1,
                Some(MfcePolicyState::Reject) => -1,
                None => 0,
            }),
        })
    }

    pub fn retain_open_trajectories(&mut self, assets: &BTreeSet<String>) {
        self.provenance
            .trajectories
            .retain(|asset, _| assets.contains(asset));
    }

    pub fn position_evaluation_due(&self, asset: &str, now: u64, interval_ms: u64) -> bool {
        self.samples
            .iter()
            .rev()
            .find(|sample| {
                sample.asset == asset
                    && sample.features.objective() == LearningObjective::ContinuationQuality
            })
            .is_none_or(|sample| now.saturating_sub(sample.decision_at) >= interval_ms)
    }

    /// A one-second debounce on economically material state movement. The
    /// heartbeat remains a maximum-staleness guard, not the sole trigger.
    pub fn position_feature_change_due(
        &self,
        asset: &str,
        now: u64,
        features: &MfceFeatureVector,
    ) -> bool {
        let Some(previous) = self.samples.iter().rev().find(|sample| {
            sample.asset == asset
                && sample.features.objective() == LearningObjective::ContinuationQuality
        }) else {
            return true;
        };
        if now.saturating_sub(previous.decision_at) < 1_000 {
            return false;
        }
        materially_changed_features(&previous.features, features)
    }
    pub fn observe_episode_origin(
        &mut self,
        id: &str,
        asset: &str,
        opened_at: u64,
        at: u64,
        origin: AlphaOrigin,
    ) {
        let episode = self
            .provenance
            .episodes
            .entry(id.to_owned())
            .or_insert_with(|| EpisodeProvenance {
                asset: asset.into(),
                opened_at,
                ..Default::default()
            });
        if episode.closed_at.is_none()
            && episode
                .transitions
                .last()
                .is_none_or(|(_, previous)| *previous != origin)
        {
            episode.transitions.push((at, origin));
        }
    }

    pub fn position_fill(
        &mut self,
        asset: &str,
        before: Option<&str>,
        after: Option<&str>,
        cloid: &str,
        at: u64,
    ) {
        let origin = self
            .provenance
            .actions
            .get(cloid)
            .map(|action| action.origin);
        if before != after {
            if let Some(previous) = before.and_then(|id| self.provenance.episodes.get_mut(id)) {
                previous.closed_at = Some(at);
            }
            if let Some(id) = after {
                self.provenance
                    .episodes
                    .entry(id.to_owned())
                    .or_insert_with(|| EpisodeProvenance {
                        asset: asset.into(),
                        opened_at: at,
                        closed_at: None,
                        transitions: origin.map(|origin| vec![(at, origin)]).unwrap_or_default(),
                        settled: None,
                    });
            }
        }
        if let Some(action) = self.provenance.actions.get_mut(cloid) {
            action
                .episodes
                .extend(before.into_iter().chain(after).map(str::to_owned));
        }
    }

    /// Same retention boundary as engine economics; never compact open episodes
    /// or the decision attribution of actions attached to those episodes.
    pub fn compact_provenance_before(&mut self, cutoff: u64) {
        self.provenance
            .episodes
            .retain(|_, e| e.closed_at.is_none_or(|at| at >= cutoff));
        self.provenance.actions.retain(|_, a| {
            a.decision_at >= cutoff
                || a.episodes
                    .iter()
                    .any(|id| self.provenance.episodes.contains_key(id))
        });
    }

    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
    pub fn performance(&self) -> BTreeMap<AlphaOrigin, AlphaPerformance> {
        let mut result = BTreeMap::<AlphaOrigin, AlphaPerformance>::new();
        for sample in &self.samples {
            let Some(outcome) = &sample.forward[reporting_horizon_index(sample)] else {
                continue;
            };
            let stats = result.entry(sample.origin).or_default();
            accumulate_performance(stats, sample, outcome);
        }
        result
    }

    pub fn quality_performance(
        &self,
    ) -> BTreeMap<AlphaOrigin, BTreeMap<LearningObjective, AlphaPerformance>> {
        let mut result = BTreeMap::new();
        for sample in &self.samples {
            let Some(outcome) = &sample.forward[reporting_horizon_index(sample)] else {
                continue;
            };
            let stats = result
                .entry(sample.origin)
                .or_insert_with(BTreeMap::new)
                .entry(learning_objective(sample))
                .or_default();
            accumulate_performance(stats, sample, outcome);
        }
        result
    }

    pub fn capture(&mut self, mut sample: DecisionSample) {
        // Freeze issuance attribution even if supervised features are missing or
        // the delayed-observation ring is full. Re-evaluation cannot rewrite it.
        if let Some(cloid) = &sample.cloid {
            self.provenance
                .actions
                .entry(cloid.clone())
                .or_insert_with(|| ActionProvenance {
                    decision_at: sample.decision_at,
                    origin: sample.origin,
                    episodes: BTreeSet::new(),
                });
        }
        if sample.features.context.is_none()
            || sample.features.version != crate::lifecycle::MFCE_FEATURE_VERSION
        {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        sample.reentry_after_exit = matches!(sample.kind, DecisionKind::Open | DecisionKind::Add)
            && self.turnover.get(&sample.asset).is_some_and(|turnover| {
                turnover.last_exit > 0
                    && sample.decision_at >= turnover.last_exit
                    && sample.decision_at.saturating_sub(turnover.last_exit) <= HORIZONS_MS[3]
            });
        // Retire elapsed objectives before enforcing the per-market cap. Short
        // continuation decisions therefore cannot crowd out entry horizons.
        self.expire(sample.decision_at);
        // A selected action is always an independent executable observation.
        // Counterfactuals are retained only when their economic state changes
        // materially, plus one bounded hourly regime heartbeat. CLOID
        // provenance above remains immutable even when its observation is
        // intentionally coalesced here.
        if sample.provenance != DecisionProvenance::Selected
            && self
                .samples
                .iter()
                .rev()
                .find(|previous| {
                    previous.pending_mask != 0
                        && previous.asset == sample.asset
                        && previous.direction == sample.direction
                        && previous.features.objective() == sample.features.objective()
                })
                .is_some_and(|previous| {
                    sample.decision_at.saturating_sub(previous.decision_at)
                        < COUNTERFACTUAL_REFRESH_MS
                        && !materially_changed_decision(previous, &sample)
                })
        {
            return;
        }
        sample.pending_mask = objective_horizon_mask(&sample);
        if sample.provenance != DecisionProvenance::Selected
            && self
                .samples
                .iter()
                .filter(|s| s.asset == sample.asset && s.pending_mask != 0)
                .count()
                >= MAX_PER_MARKET
        {
            // This is intentional bounded counterfactual sampling, not loss of
            // an economically selected observation. The frozen states already
            // in flight remain intact until their horizons settle.
            return;
        }
        if self.samples.len() == MAX_DECISIONS {
            // Real selected decisions own learning capacity. When the ring is
            // full they displace the oldest counterfactual before any completed
            // record; non-selected observations may only evict completed work.
            let replaceable = (sample.provenance == DecisionProvenance::Selected)
                .then(|| {
                    self.samples.iter().position(|s| {
                        s.provenance != DecisionProvenance::Selected && s.pending_mask != 0
                    })
                })
                .flatten()
                .or_else(|| self.samples.iter().position(|s| s.pending_mask == 0));
            if let Some(index) = replaceable {
                self.samples.remove(index);
            } else {
                self.dropped = self.dropped.saturating_add(1);
                return;
            }
        }
        self.next_id = self.next_id.saturating_add(1);
        sample.id = self.next_id;
        self.samples.push_back(sample);
    }
    pub fn markets(&self) -> BTreeSet<String> {
        self.samples
            .iter()
            .filter(|s| s.pending_mask != 0 && s.anchor.is_some())
            .map(|s| s.asset.clone())
            .collect()
    }
    pub fn gap(&mut self) {
        for sample in &mut self.samples {
            if sample.pending_mask != 0 {
                sample.funding_credit = None;
            }
        }
        self.flow.clear();
        self.observed_at.clear();
        self.funding_at = None;
    }
    pub fn funding(&mut self, now: u64, rates: &BTreeMap<String, Decimal>) {
        if self.funding_at.is_some_and(|last| now < last) {
            return;
        }
        for sample in self.samples.iter_mut().filter(|s| s.pending_mask != 0) {
            let start = self.funding_at.unwrap_or(now).max(sample.funding_start);
            if now.saturating_sub(start) > 40_000 {
                sample.funding_credit = None;
            }
            sample.funding_credit = (|| {
                let anchor = sample.anchor.as_ref()?;
                let cost = rates
                    .get(&sample.asset)?
                    .checked_mul(anchor.quantity)?
                    .checked_mul(anchor.midpoint)?
                    .checked_mul(Decimal::from(sample.direction.sign()))?
                    .checked_mul(Decimal::from(now.saturating_sub(start)))?
                    .checked_div(Decimal::from(3_600_000))?;
                sample.funding_credit?.checked_sub(cost)
            })();
        }
        self.funding_at = Some(now);
    }
    /// One bounded scan from the existing actor tick, not one task per sample.
    pub fn expire(&mut self, now: u64) {
        for sample in &mut self.samples {
            for (i, horizon) in HORIZONS_MS.iter().enumerate() {
                if now
                    > sample
                        .decision_at
                        .saturating_add(*horizon)
                        .saturating_add(OBSERVATION_GRACE_MS)
                {
                    if sample.pending_mask & (1 << i) != 0 {
                        let reason = if sample.anchor.is_none() {
                            LabelUnavailableReason::MissingDecisionAnchor
                        } else if sample.funding_credit.is_none() {
                            LabelUnavailableReason::MissingFundingContinuity
                        } else {
                            sample.label_unavailable[i]
                                .unwrap_or(LabelUnavailableReason::NoTimelyObservation)
                        };
                        sample.label_unavailable[i] = Some(reason);
                        sample.component_unavailable[i] = component_unavailable(reason);
                        if sample.size_regret_curve_bps[i].is_empty() {
                            sample.size_regret_unavailable[i] =
                                Some(sample.size_regret_unavailable[i].unwrap_or(
                                    if sample.larger_anchors.is_empty() {
                                        LabelUnavailableReason::NoLegalLargerSlice
                                    } else {
                                        LabelUnavailableReason::MissingLargerExitDepth
                                    },
                                ));
                        }
                    }
                    sample.pending_mask &= !(1 << i);
                }
            }
        }
        let markets = self.markets();
        self.observed_at.retain(|asset, _| markets.contains(asset));
    }
    /// Returns executable after-cost RemainingEdge labels for every enabled
    /// horizon. Horizon is encoded in the frozen features; no horizon is a
    /// forced exit or take-profit timer.
    pub fn observe(
        &mut self,
        asset: &str,
        now: u64,
        mut executable_quote: impl FnMut(MfceDirection, Decimal) -> Option<ExecutableAnchor>,
    ) -> Vec<(MfceFeatureVector, MfceDirection, u64, Decimal)> {
        self.expire(now);
        if self
            .observed_at
            .get(asset)
            .is_some_and(|last| now.saturating_sub(*last) < 1_000)
        {
            return Vec::new();
        }
        self.observed_at.insert(asset.to_string(), now);
        let mut labels = Vec::new();
        let mut quotes = BTreeMap::new();
        let mut quote = |direction, quantity| {
            quotes
                .entry((direction, quantity))
                .or_insert_with(|| executable_quote(direction, quantity))
                .clone()
        };
        for sample in self
            .samples
            .iter_mut()
            .filter(|s| s.asset == asset && s.pending_mask != 0 && now > s.decision_at)
        {
            let due = HORIZONS_MS
                .iter()
                .enumerate()
                .filter(|(i, horizon)| {
                    sample.pending_mask & (1 << i) != 0
                        && now >= sample.decision_at.saturating_add(**horizon)
                })
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if due.is_empty() {
                continue;
            }
            let Some(anchor) = sample.anchor.as_ref() else {
                for i in due {
                    let reason = LabelUnavailableReason::MissingDecisionAnchor;
                    sample.label_unavailable[i] = Some(reason);
                    sample.component_unavailable[i] = component_unavailable(reason);
                    sample.pending_mask &= !(1 << i);
                }
                continue;
            };
            if sample.funding_credit.is_none() {
                for i in due {
                    let reason = LabelUnavailableReason::MissingFundingContinuity;
                    sample.label_unavailable[i] = Some(reason);
                    sample.component_unavailable[i] = component_unavailable(reason);
                    sample.pending_mask &= !(1 << i);
                }
                continue;
            }
            let Some(outcome) = quote(sample.direction, anchor.quantity)
                .as_ref()
                .and_then(|exit| sample.economics(exit, anchor))
            else {
                for i in due {
                    let reason = LabelUnavailableReason::MissingExecutableDepth;
                    sample.label_unavailable[i] = Some(reason);
                    sample.component_unavailable[i] = component_unavailable(reason);
                }
                continue;
            };
            let mut larger_anchors = sample
                .larger_anchors
                .iter()
                .filter(|larger| {
                    if sample.kind == DecisionKind::Hold {
                        larger.quantity > Decimal::ZERO
                    } else {
                        larger.quantity > anchor.quantity
                    }
                })
                .collect::<Vec<_>>();
            larger_anchors.sort_by_key(|larger| larger.quantity);
            let mut size_curve = Vec::new();
            let mut previous_quantity = if sample.kind == DecisionKind::Hold {
                Decimal::ZERO
            } else {
                anchor.quantity
            };
            let mut previous_net = if sample.kind == DecisionKind::Hold {
                Decimal::ZERO
            } else {
                outcome.net_pnl
            };
            for larger in &larger_anchors {
                let Some(exit) = quote(sample.direction, larger.quantity) else {
                    break;
                };
                let Some(total_outcome) = (if sample.kind == DecisionKind::Hold {
                    sample.incremental_size_economics(&exit, larger)
                } else {
                    sample.economics(&exit, larger)
                }) else {
                    break;
                };
                let Some(additional_quantity) = larger.quantity.checked_sub(previous_quantity)
                else {
                    break;
                };
                let Some(cumulative_additional_quantity) =
                    larger
                        .quantity
                        .checked_sub(if sample.kind == DecisionKind::Hold {
                            Decimal::ZERO
                        } else {
                            anchor.quantity
                        })
                else {
                    break;
                };
                let Some(marginal_notional) = additional_quantity.checked_mul(anchor.midpoint)
                else {
                    break;
                };
                let Some(net_edge_bps) = total_outcome
                    .net_pnl
                    .checked_sub(previous_net)
                    .and_then(|net| net.checked_div(marginal_notional))
                    .and_then(|edge| edge.checked_mul(BPS))
                else {
                    break;
                };
                size_curve.push(SizeRegretPoint {
                    additional_quantity,
                    cumulative_additional_quantity,
                    net_edge_bps,
                });
                previous_quantity = larger.quantity;
                previous_net = total_outcome.net_pnl;
            }
            let Some(net) = sample.bps(outcome.net_pnl) else {
                continue;
            };
            if sample.mfe_net_bps.is_none_or(|best| net > best) {
                sample.mfe_net_bps = Some(net);
                sample.time_to_mfe_ms = Some(now - sample.decision_at);
            }
            sample.mae_net_bps = Some(sample.mae_net_bps.map_or(net, |worst| worst.min(net)));
            for (i, horizon) in HORIZONS_MS.iter().enumerate() {
                if sample.pending_mask & (1 << i) == 0
                    || now < sample.decision_at.saturating_add(*horizon)
                {
                    continue;
                }
                sample.cost_drag_bps[i] = sample.bps(outcome.gross_pnl - outcome.net_pnl);
                sample.size_regret_bps[i] = size_curve.iter().map(|point| point.net_edge_bps).max();
                sample.size_regret_curve_bps[i] = size_curve.clone();
                sample.size_regret_unavailable[i] = if size_curve.is_empty() {
                    Some(if larger_anchors.is_empty() {
                        LabelUnavailableReason::NoLegalLargerSlice
                    } else {
                        LabelUnavailableReason::MissingLargerExitDepth
                    })
                } else {
                    None
                };
                if sample.retaining_slice() {
                    sample.exit_regret_bps[i] = Some(net);
                    sample.hold_regret_bps[i] = Some(net);
                } else if sample.provenance == DecisionProvenance::Selected
                    && matches!(sample.kind, DecisionKind::Open | DecisionKind::Add)
                {
                    // The counterfactual is do-not-enter: zero incremental PnL.
                    // Net includes both sides' executable costs, so a negative
                    // value is direct regret for having entered.
                    sample.entry_regret_bps[i] = Some(net);
                    if sample.reentry_after_exit {
                        sample.reentry_regret_bps[i] = Some(net);
                    }
                } else if matches!(
                    sample.provenance,
                    DecisionProvenance::MfceRejected
                        | DecisionProvenance::AllocatorDisplaced
                        | DecisionProvenance::RiskLimited
                        | DecisionProvenance::Unexecuted
                ) {
                    sample.entry_regret_bps[i] = Some(net);
                }
                let mut features = sample.features.clone();
                let trajectory = features
                    .position
                    .as_ref()
                    .and_then(|position| position.trajectory.clone());
                features.set_position_learning(trajectory, learning_objective(sample));
                features.set_learning_horizon_ms(*horizon);
                labels.push((features, sample.direction, sample.decision_at, net));
                sample.forward[i] = Some(outcome.clone());
                sample.label_unavailable[i] = None;
                sample.component_unavailable[i] = LabelComponentUnavailable::default();
                sample.pending_mask &= !(1 << i);
            }
        }
        labels
    }
    pub fn fill(
        &mut self,
        asset: &str,
        cloid: &str,
        now: u64,
        signed_quantity: Decimal,
        price: Decimal,
        fees: Decimal,
        position_before: Decimal,
        reference: Decimal,
        actual: RealizedOutcome,
    ) {
        if signed_quantity.is_zero() {
            return;
        }
        if self.turnover.contains_key(asset) || self.turnover.len() < 512 {
            let state = self.turnover.entry(asset.to_string()).or_default();
            // Compact decayed turnover, not an ever-growing lifetime sum.
            state.notional = state
                .notional
                .checked_mul(
                    Decimal::ONE
                        - Decimal::from(now.saturating_sub(state.last_fill).min(3_600_000))
                            / Decimal::from(3_600_000),
                )
                .unwrap_or_default();
            state.notional = signed_quantity
                .abs()
                .checked_mul(price)
                .and_then(|v| state.notional.checked_add(v))
                .unwrap_or(state.notional);
            state.last_fill = now;
            if position_before.is_zero()
                || position_before.is_sign_positive()
                    != (position_before + signed_quantity).is_sign_positive()
            {
                state.opened_at = now;
            }
            let position_after = position_before + signed_quantity;
            if !position_before.is_zero()
                && (position_after.is_zero()
                    || position_before.is_sign_positive() != position_after.is_sign_positive())
            {
                state.last_exit = now;
            }
        }
        for sample in self.samples.iter_mut().filter(|s| {
            s.cloid.as_deref() == Some(cloid)
                && s.asset == asset
                && now < s.decision_at.saturating_add(HORIZONS_MS[0])
        }) {
            let anchor = sample.anchor.get_or_insert(ExecutableAnchor {
                quantity: Decimal::ZERO,
                midpoint: reference,
                fill_price: price,
                fees: Decimal::ZERO,
                actual: false,
            });
            if !anchor.actual {
                sample.funding_credit = Some(Decimal::ZERO);
                sample.funding_start = now;
            }
            sample.actual_result = Some(match sample.actual_result.take() {
                None => actual.clone(),
                Some(old) => RealizedOutcome {
                    gross_pnl: old.gross_pnl.saturating_add(actual.gross_pnl),
                    fees: old.fees.saturating_add(actual.fees),
                    slippage: old.slippage.saturating_add(actual.slippage),
                    funding: old.funding.saturating_add(actual.funding),
                    net_pnl: old.net_pnl.saturating_add(actual.net_pnl),
                },
            });
            let old = if anchor.actual {
                anchor.quantity
            } else {
                Decimal::ZERO
            };
            let quantity = old + signed_quantity.abs();
            let Some(value) = anchor
                .fill_price
                .checked_mul(old)
                .and_then(|v| price.checked_mul(signed_quantity.abs())?.checked_add(v))
            else {
                continue;
            };
            anchor.fill_price = value / quantity;
            anchor.quantity = quantity;
            anchor.fees = if anchor.actual {
                anchor.fees + fees
            } else {
                fees
            };
            anchor.actual = true;
            sample.actual_delta += signed_quantity;
            // Extrema before actual execution aren't outcomes of the fill.
            sample.mfe_net_bps = None;
            sample.mae_net_bps = None;
            sample.time_to_mfe_ms = None;
        }
    }
    pub fn migrate_actual_cash(&mut self) -> Option<()> {
        for sample in &mut self.samples {
            if let Some(actual) = &mut sample.actual_result {
                actual.net_pnl = actual.net_pnl.checked_add(actual.slippage)?;
            }
        }
        Some(())
    }

    pub fn valid(&self) -> bool {
        self.samples.len() <= MAX_DECISIONS
            && self.flow.len() <= 850
            && self.turnover.len() <= 512
            && self.provenance.trajectories.len() <= 512
            && self
                .provenance
                .trajectories
                .iter()
                .all(|(asset, trajectory)| {
                    !asset.is_empty()
                        && !trajectory.episode_id.is_empty()
                        && trajectory.opened_at <= trajectory.last_observed_at
                        && trajectory.last_mark > Decimal::ZERO
                })
            && self.provenance.episodes.values().all(|episode| {
                episode.settled.as_ref().is_none_or(|settled| {
                    episode
                        .closed_at
                        .is_some_and(|closed| closed >= episode.opened_at)
                        && !episode.asset.is_empty()
                        && !settled.opening_action.is_empty()
                        && settled
                            .outcome
                            .gross_pnl
                            .checked_sub(settled.outcome.fees)
                            .and_then(|value| value.checked_add(settled.outcome.funding))
                            == Some(settled.outcome.net_pnl)
                })
            })
            && self.samples.iter().all(|s| {
                s.id <= self.next_id
                    && s.pending_mask < 128
                    && s.prediction.as_ref().is_none_or(|p| {
                        p.predicted_gross_edge.checked_sub(p.expected_friction)
                            == Some(p.predicted_net_edge)
                            && p.predicted_q10_gross_edge.checked_sub(p.expected_friction)
                                == Some(p.predicted_q10_net_edge)
                            && p.predicted_q10_net_edge <= p.predicted_net_edge
                            && p.objective == s.features.objective()
                            && s.features.snapshot_id() == Ok(p.feature_snapshot_id)
                            && s.prediction_curve.first().is_some_and(|point| {
                                point.horizon_ms == p.horizon_ms
                                    && point.model_epoch == p.model_epoch
                                    && point.objective == p.objective
                                    && point.feature_snapshot_id == p.feature_snapshot_id
                                    && point.q10_net_bps == p.predicted_q10_net_edge
                                    && point.q50_net_bps == p.predicted_net_edge
                            })
                    })
                    && s.prediction_curve.iter().all(|point| {
                        let mut features = s.features.clone();
                        features.set_learning_horizon_ms(point.horizon_ms);
                        point.q10_net_bps <= point.q50_net_bps
                            && point.objective == features.objective()
                            && features.snapshot_id() == Ok(point.feature_snapshot_id)
                            && s.prediction.as_ref().is_none_or(|prediction| {
                                point.model_epoch == prediction.model_epoch
                            })
                    })
                    && s.forward.iter().flatten().all(|o| {
                        o.gross_pnl
                            .checked_sub(o.fees)
                            .and_then(|v| v.checked_sub(o.slippage))
                            .and_then(|v| v.checked_add(o.funding))
                            == Some(o.net_pnl)
                    })
                    && s.actual_result.as_ref().is_none_or(|o| {
                        o.gross_pnl
                            .checked_sub(o.fees)
                            .and_then(|v| v.checked_add(o.funding))
                            == Some(o.net_pnl)
                    })
            })
    }
}

fn learning_objective(sample: &DecisionSample) -> LearningObjective {
    match sample.kind {
        DecisionKind::Open => LearningObjective::EntryQuality,
        DecisionKind::Add => LearningObjective::SizeQuality,
        DecisionKind::Exit => LearningObjective::ExitQuality,
        DecisionKind::Hold | DecisionKind::Reduce => LearningObjective::ContinuationQuality,
        DecisionKind::Reject | DecisionKind::BudgetConstrained => sample.features.objective(),
    }
}

fn relative_change_material(previous: Decimal, next: Decimal, minimum: Decimal) -> bool {
    if previous == next {
        return false;
    }
    let scale = previous.abs().max(next.abs());
    previous
        .checked_sub(next)
        .is_none_or(|difference| difference.abs() >= minimum.max(scale * Decimal::new(5, 2)))
}

fn materially_changed_prediction(
    previous: Option<&PredictionObservation>,
    next: Option<&PredictionObservation>,
) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => {
            previous.model_epoch != next.model_epoch
                || previous.horizon_ms != next.horizon_ms
                || previous.objective != next.objective
                || (previous.predicted_net_edge - next.predicted_net_edge).abs() >= Decimal::from(2)
                || (previous.predicted_q10_net_edge - next.predicted_q10_net_edge).abs()
                    >= Decimal::from(2)
                || (previous.expected_friction - next.expected_friction).abs() >= Decimal::from(2)
        }
        (None, None) => false,
        _ => true,
    }
}

fn materially_changed_features(previous: &MfceFeatureVector, next: &MfceFeatureVector) -> bool {
    if previous.objective() != next.objective() {
        return true;
    }
    let changed = |a: Decimal, b: Decimal, threshold: Decimal| (a - b).abs() >= threshold;
    let pv = &previous.values;
    let nv = &next.values;
    if [0usize, 1, 3]
        .into_iter()
        .any(|i| changed(pv[i], nv[i], Decimal::new(5, 2)))
        || changed(pv[9], nv[9], Decimal::from(5))
        || changed(pv[11], nv[11], Decimal::from(2))
        || changed(pv[13], nv[13], Decimal::new(25, 2))
        || changed(pv[14], nv[14], Decimal::new(15, 2))
        || changed(pv[15], nv[15], Decimal::ONE)
    {
        return true;
    }
    let pc = previous.context.unwrap_or([Decimal::ZERO; 16]);
    let nc = next.context.unwrap_or([Decimal::ZERO; 16]);
    if [1usize, 2, 3, 4, 5, 6, 14, 15]
        .into_iter()
        .any(|i| changed(pc[i], nc[i], Decimal::new(1, 1)))
    {
        return true;
    }
    match (
        previous
            .position
            .as_ref()
            .and_then(|position| position.trajectory.as_ref()),
        next.position
            .as_ref()
            .and_then(|position| position.trajectory.as_ref()),
    ) {
        (Some(old), Some(new)) => {
            changed(
                old.unrealized_return_bps,
                new.unrealized_return_bps,
                Decimal::TEN,
            ) || changed(
                old.drawdown_from_mfe_bps,
                new.drawdown_from_mfe_bps,
                Decimal::TEN,
            ) || changed(
                old.price_velocity_bps_per_second,
                new.price_velocity_bps_per_second,
                Decimal::from(5),
            )
        }
        (None, None) => false,
        _ => true,
    }
}

fn materially_changed_decision(previous: &DecisionSample, next: &DecisionSample) -> bool {
    previous.kind != next.kind
        || previous.origin != next.origin
        || previous.provenance != next.provenance
        || previous.mode != next.mode
        || relative_change_material(previous.proposed_delta, next.proposed_delta, Decimal::ZERO)
        || materially_changed_prediction(previous.prediction.as_ref(), next.prediction.as_ref())
        || materially_changed_features(&previous.features, &next.features)
}

fn objective_horizon_mask(sample: &DecisionSample) -> u8 {
    match learning_objective(sample) {
        // Dense short-end trajectory supervision; these retire by 15 minutes.
        LearningObjective::ContinuationQuality => 0b000_1111,
        // Exit regret retains one slower tail observation without retaining 4h.
        LearningObjective::ExitQuality => 0b010_1111,
        // Entry and re-entry receive a 30-second after-cost label immediately;
        // slower horizons remain on the same frozen observation.
        LearningObjective::EntryQuality => 0b111_1111,
        // Adds also start at 30 seconds and retire after 1h.
        LearningObjective::SizeQuality => 0b011_1111,
    }
}

fn reporting_horizon_index(sample: &DecisionSample) -> usize {
    match learning_objective(sample) {
        LearningObjective::ContinuationQuality | LearningObjective::ExitQuality => HORIZON_5M,
        LearningObjective::EntryQuality | LearningObjective::SizeQuality => HORIZON_15M,
    }
}

fn accumulate_performance(
    stats: &mut AlphaPerformance,
    sample: &DecisionSample,
    outcome: &RealizedOutcome,
) {
    stats.samples += 1;
    stats.wins += u64::from(outcome.net_pnl > Decimal::ZERO);
    stats.gross_pnl = stats.gross_pnl.saturating_add(outcome.gross_pnl);
    stats.net_pnl = stats.net_pnl.saturating_add(outcome.net_pnl);
    stats.gains = stats
        .gains
        .saturating_add(outcome.net_pnl.max(Decimal::ZERO));
    stats.losses = stats
        .losses
        .saturating_add((-outcome.net_pnl).max(Decimal::ZERO));
    stats.profit_factor = stats.gains.checked_div(stats.losses);
    stats.win_rate = Decimal::from(stats.wins).checked_div(Decimal::from(stats.samples));
    stats.max_executable_notional = sample.anchor.as_ref().and_then(|anchor| {
        let notional = anchor.quantity.checked_mul(anchor.midpoint)?;
        Some(
            stats
                .max_executable_notional
                .unwrap_or_default()
                .max(notional),
        )
    });
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turnover {
    pub opened_at: u64,
    pub last_fill: u64,
    pub notional: Decimal,
    #[serde(default)]
    pub last_exit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionTrajectory {
    pub episode_id: String,
    pub opened_at: u64,
    pub last_observed_at: u64,
    pub last_mark: Decimal,
    pub mfe_bps: Decimal,
    pub mae_bps: Decimal,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowBin {
    minute: u64,
    buy: Decimal,
    sell: Decimal,
    trades: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketFlow {
    bins: [FlowBin; 30],
    last_key: (u64, u64),
    #[serde(default)]
    recent_ids: BTreeSet<u64>,
    #[serde(default)]
    incomplete_until: u64,
    pub last_sent_ms: u64,
}

impl MarketFlow {
    /// Called on every raw trade BEFORE wallet provenance classification.
    /// Exchange ordering provides bounded deduplication; old replay rows never
    /// inflate flow. Source fills keep their independent wallet-level dedup.
    pub fn trade(&mut self, time: u64, id: u64, notional: Decimal, buy: bool) {
        if time < self.last_key.0 || notional <= Decimal::ZERO {
            return;
        }
        if time > self.last_key.0 {
            self.recent_ids.clear();
        }
        if self.recent_ids.len() == 1_024 {
            self.incomplete_until = time.saturating_add(1_800_000);
            return;
        }
        if !self.recent_ids.insert(id) {
            return;
        }
        self.last_key = (time, id);
        let minute = time / 60_000;
        let bin = &mut self.bins[minute as usize % 30];
        if bin.minute != minute {
            *bin = FlowBin {
                minute,
                ..FlowBin::default()
            };
        }
        let value = if buy { &mut bin.buy } else { &mut bin.sell };
        if let Some(next) = value.checked_add(notional) {
            *value = next;
            bin.trades = bin.trades.saturating_add(1);
        }
    }
    pub fn features(&self, now: u64) -> Option<[Decimal; 7]> {
        if now < self.incomplete_until
            || now < self.last_key.0
            || now.saturating_sub(self.last_sent_ms) > 40_000
        {
            return None;
        }
        let sum = |minutes| {
            self.bins
                .iter()
                .filter(|b| b.minute <= now / 60_000 && now / 60_000 - b.minute < minutes)
                .try_fold((Decimal::ZERO, Decimal::ZERO, 0u64), |(v, c, n), b| {
                    Some((
                        v.checked_add(b.buy)?.checked_add(b.sell)?,
                        c.checked_add(b.buy)?.checked_sub(b.sell)?,
                        n.saturating_add(b.trades),
                    ))
                })
        };
        let (short, cvd, count) = sum(5)?;
        let (long, long_cvd, _) = sum(30)?;
        let (_, velocity, _) = sum(1)?;
        let previous = self
            .bins
            .iter()
            .find(|b| b.minute + 1 == now / 60_000)
            .map_or(Decimal::ZERO, |b| b.buy - b.sell);
        let persistence = self
            .bins
            .iter()
            .filter(|b| b.minute <= now / 60_000 && now / 60_000 - b.minute < 5)
            .map(|b| match b.buy.cmp(&b.sell) {
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
            })
            .sum::<i32>();
        Some([
            short.checked_mul(Decimal::from(6))?.checked_div(long)?,
            cvd.checked_div(short)?,
            long_cvd.checked_div(long)?,
            Decimal::from(count).checked_div(Decimal::from(300))?,
            velocity.checked_div(short)?,
            velocity.checked_sub(previous)?.checked_div(short)?,
            Decimal::from(persistence) / Decimal::from(5),
        ])
    }
}
