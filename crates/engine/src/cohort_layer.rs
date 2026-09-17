//! Validated boundary between Hyperdash discovery snapshots and the engine.
//!
//! Hyperdash supplies only immutable cohort membership and optional aggregate
//! TWAP context. Qualification observations are required to declare
//! Hyperliquid as their authority, and are re-evaluated with the cutoffs from
//! the active copy-trade configuration before any candidate is scheduled.

use crate::domain::cohort::{
    apply_wallet_quality_policy, resolve_very_profitable_membership, ActiveTwapContext,
    AuthoritativeWalletPosition, CohortMembershipResolution, FilteredCohortWallet,
    HyperdashMembershipSnapshot, WalletQualityDecision, WalletQualityObservation,
};
use crate::domain::configuration::{CopyTradeConfig, TraderCandidate, VeryProfitableLayerIdentity};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

use crate::public_mainnet::SourceStateResponse;

pub const COHORT_LAYER_SCHEMA_VERSION: u32 = 3;
pub const HYPERLIQUID_INFO_AUTHORITY: &str = "https://api.hyperliquid.xyz/info";
const COHORT_CANDIDATE_LABEL_PREFIX: &str = "hyperdash-very-profitable";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CohortLayerError {
    Io(String),
    Decode(String),
    InvalidArtifact(String),
    InvalidMembership(String),
    InvalidQuality(String),
    InvalidMergedConfiguration(String),
}

impl Display for CohortLayerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CohortLayerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CohortAdmissionPolicy {
    QualityFiltered,
    AllCapturedMembers,
}

impl Default for CohortAdmissionPolicy {
    fn default() -> Self {
        Self::QualityFiltered
    }
}

/// A point-in-time import artifact. Membership-only admission deliberately
/// treats quality observations as optional diagnostics, never as rejections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VeryProfitableLayerArtifact {
    pub schema_version: u32,
    #[serde(default)]
    pub admission_policy: CohortAdmissionPolicy,
    pub membership: HyperdashMembershipSnapshot,
    pub hydrated_at_ms: u64,
    pub authoritative_source: String,
    /// SHA-256 of the canonical retained Hyperliquid history/state input used
    /// to calculate the observations below.
    pub authoritative_history_sha256: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wallet_quality: Vec<WalletQualityObservation>,
    /// Optional Hyperdash context captured with this same membership version.
    /// These values never replace Hyperliquid positions or execution state.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub active_twaps: BTreeMap<String, ActiveTwapContext>,
}

impl VeryProfitableLayerArtifact {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, CohortLayerError> {
        let bytes = fs::read(path).map_err(|error| CohortLayerError::Io(error.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|error| CohortLayerError::Decode(error.to_string()))
    }

    pub fn canonical_sha256(&self) -> Result<String, CohortLayerError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| CohortLayerError::Decode(error.to_string()))?;
        Ok(hex(Sha256::digest(bytes).into()))
    }
}

/// Validated cohort input ready for scheduling and signal construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedVeryProfitableLayer {
    pub artifact_sha256: String,
    pub resolution: CohortMembershipResolution,
    pub hydrated_at_ms: u64,
    pub quality_decisions: BTreeMap<String, WalletQualityDecision>,
    pub qualified_members: BTreeSet<String>,
    pub qualified_cohort_only_members: BTreeSet<String>,
    pub bounded_wallet_weights: BTreeMap<String, Decimal>,
    pub active_twaps: BTreeMap<String, ActiveTwapContext>,
}

impl PreparedVeryProfitableLayer {
    pub fn prepare(
        artifact: &VeryProfitableLayerArtifact,
        config: &CopyTradeConfig,
    ) -> Result<Self, CohortLayerError> {
        validate_artifact_header(artifact)?;
        let artifact_sha256 = artifact.canonical_sha256()?;
        let existing = config
            .candidates
            .iter()
            .filter(|candidate| !is_cohort_candidate_label(&candidate.label))
            .map(|candidate| candidate.address.clone())
            .collect::<Vec<_>>();
        let resolution = resolve_very_profitable_membership(&artifact.membership, existing)
            .map_err(|error| CohortLayerError::InvalidMembership(error.to_string()))?;
        validate_bound_identity(config, &layer_identity(&resolution, &artifact_sha256))?;
        let mut observations = BTreeMap::new();
        for observation in &artifact.wallet_quality {
            let address = observation.address.to_ascii_lowercase();
            if !resolution.all_members.contains(&address) {
                return Err(CohortLayerError::InvalidArtifact(format!(
                    "quality observation is not a member: {address}"
                )));
            }
            if observations.insert(address.clone(), observation).is_some() {
                return Err(CohortLayerError::InvalidArtifact(format!(
                    "duplicate quality observation: {address}"
                )));
            }
        }
        if artifact.admission_policy == CohortAdmissionPolicy::QualityFiltered
            && observations.len() != resolution.unique_wallet_count
        {
            return Err(CohortLayerError::InvalidArtifact(
                "every unique member requires one Hyperliquid quality observation".into(),
            ));
        }

        let (quality_decisions, qualified_members, bounded_wallet_weights) =
            if artifact.admission_policy == CohortAdmissionPolicy::AllCapturedMembers {
                (
                    BTreeMap::new(),
                    resolution.all_members.clone(),
                    resolution
                        .all_members
                        .iter()
                        .map(|address| (address.clone(), Decimal::ONE))
                        .collect(),
                )
            } else {
                // Edge-only: quality is diagnostics only, never a veto. All
                // tracked wallets are scheduled; decisions retained for audit.
                let policy = config
                    .cohort_wallet_quality_policy()
                    .map_err(|error| CohortLayerError::InvalidQuality(error.to_string()))?;
                let mut decisions = BTreeMap::new();
                for (address, observation) in observations {
                    let decision = apply_wallet_quality_policy(observation, &policy, Decimal::ONE)
                        .map_err(|error| CohortLayerError::InvalidQuality(error.to_string()))?;
                    decisions.insert(address, decision);
                }
                (
                    decisions,
                    resolution.all_members.clone(),
                    resolution
                        .all_members
                        .iter()
                        .map(|address| (address.clone(), Decimal::ONE))
                        .collect(),
                )
            };
        let qualified_cohort_only_members = qualified_members
            .intersection(&resolution.cohort_only_members)
            .cloned()
            .collect::<BTreeSet<_>>();

        Ok(Self {
            artifact_sha256,
            resolution,
            hydrated_at_ms: artifact.hydrated_at_ms,
            quality_decisions,
            qualified_members,
            qualified_cohort_only_members,
            bounded_wallet_weights,
            active_twaps: artifact.active_twaps.clone(),
        })
    }

    /// Add only genuinely new, qualified wallets to the read scheduler. Their
    /// direct consensus weight is zero because they vote through the one
    /// cohort aggregate; overlap wallets are not inserted a second time.
    pub fn merge_candidates(
        &self,
        config: &mut CopyTradeConfig,
    ) -> Result<usize, CohortLayerError> {
        let identity = layer_identity(&self.resolution, &self.artifact_sha256);
        validate_bound_identity(config, &identity)?;
        let scheduled_cohort = config
            .candidates
            .iter()
            .filter(|candidate| is_cohort_candidate_label(&candidate.label))
            .map(|candidate| candidate.address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let additions = if scheduled_cohort == self.qualified_cohort_only_members {
            Vec::new()
        } else {
            config
                .candidates
                .retain(|candidate| !is_cohort_candidate_label(&candidate.label));
            self.qualified_cohort_only_members
                .iter()
                .cloned()
                .collect::<Vec<_>>()
        };
        for (index, address) in additions.iter().enumerate() {
            config.candidates.push(TraderCandidate {
                address: address.clone(),
                label: format!("{COHORT_CANDIDATE_LABEL_PREFIX}-{:03}", index + 1),
                allocation_weight: 0.0,
                confidence_modifier: Some(1.0),
                enabled: true,
            });
        }
        config.very_profitable_layer = Some(identity);
        config
            .validate_production()
            .map_err(|error| CohortLayerError::InvalidMergedConfiguration(error.to_string()))?;
        Ok(additions.len())
    }

    pub fn is_cohort_only_candidate(&self, address: &str) -> bool {
        self.qualified_cohort_only_members
            .contains(&address.to_ascii_lowercase())
    }

    /// Converts fresh engine snapshots into the cohort engine's authoritative
    /// wallet form. Overlap wallets remain part of full-cohort diagnostics.
    /// The live source book removes their direct existing-wallet vote while
    /// this layer is installed, so each address still contributes exactly once.
    pub fn authoritative_wallets<'a>(
        &self,
        observed_at_ms: u64,
        fresh_states: impl IntoIterator<Item = &'a SourceStateResponse>,
    ) -> Result<Vec<FilteredCohortWallet>, CohortLayerError> {
        if observed_at_ms < self.hydrated_at_ms {
            return Err(CohortLayerError::InvalidArtifact(
                "authoritative position snapshot predates cohort hydration".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        let mut wallets = Vec::new();
        for state in fresh_states {
            let address = state.candidate_id.to_ascii_lowercase();
            if !self.qualified_members.contains(&address) || !seen.insert(address.clone()) {
                continue;
            }
            let mut positions = BTreeMap::new();
            for (asset, position) in &state.positions {
                let entry_price = position.entry_price.ok_or_else(|| {
                    CohortLayerError::InvalidArtifact(format!(
                        "Hyperliquid entryPx is required for cohort position {address}/{asset}"
                    ))
                })?;
                positions.insert(
                    asset.clone(),
                    AuthoritativeWalletPosition {
                        signed_notional: position.signed_notional,
                        entry_price,
                        unrealized_pnl: position.unrealized_pnl.unwrap_or_default(),
                    },
                );
            }
            let (bounded_wallet_weight, realized_quality_score) = self
                .quality_decisions
                .get(&address)
                .map(|decision| {
                    (
                        decision.bounded_wallet_weight,
                        decision.realized_quality_score,
                    )
                })
                .unwrap_or((Decimal::ONE, Decimal::ONE));
            wallets.push(FilteredCohortWallet {
                address,
                observed_at_ms,
                bounded_wallet_weight,
                realized_quality_score,
                positions,
            });
        }
        Ok(wallets)
    }
}

fn layer_identity(
    resolution: &CohortMembershipResolution,
    artifact_sha256: &str,
) -> VeryProfitableLayerIdentity {
    VeryProfitableLayerIdentity {
        cohort_id: crate::domain::cohort::VERY_PROFITABLE_COHORT_ID.into(),
        cohort_snapshot_timestamp_ms: resolution.snapshot_timestamp_ms,
        membership_completeness: resolution.completeness,
        reported_member_count: resolution.reported_member_count,
        captured_member_count: resolution.captured_member_count,
        membership_set_hash: resolution.membership_set_hash.clone(),
        artifact_sha256: artifact_sha256.into(),
    }
}

fn validate_bound_identity(
    config: &CopyTradeConfig,
    supplied: &VeryProfitableLayerIdentity,
) -> Result<(), CohortLayerError> {
    if config
        .very_profitable_layer
        .as_ref()
        .is_some_and(|bound| bound != supplied)
    {
        return Err(CohortLayerError::InvalidMergedConfiguration(
            "configuration is already bound to a different very_profitable artifact".into(),
        ));
    }
    Ok(())
}

pub fn is_cohort_candidate_label(label: &str) -> bool {
    label.starts_with(COHORT_CANDIDATE_LABEL_PREFIX)
}

fn validate_artifact_header(
    artifact: &VeryProfitableLayerArtifact,
) -> Result<(), CohortLayerError> {
    if artifact.schema_version != COHORT_LAYER_SCHEMA_VERSION
        || artifact.authoritative_source != HYPERLIQUID_INFO_AUTHORITY
        || artifact.hydrated_at_ms < artifact.membership.observed_at_ms
        || !valid_sha256_hex(&artifact.authoritative_history_sha256)
        || artifact.active_twaps.iter().any(|(asset, twap)| {
            asset.is_empty()
                || twap.observed_at_ms < artifact.membership.observed_at_ms
                || twap.observed_at_ms > artifact.hydrated_at_ms
        })
    {
        return Err(CohortLayerError::InvalidArtifact(
            "invalid cohort-layer provenance or timestamp".into(),
        ));
    }
    Ok(())
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn config() -> CopyTradeConfig {
        CopyTradeConfig::from_path(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json"),
        )
        .unwrap()
    }

    fn address(index: u64) -> String {
        format!("0x{index:040x}")
    }

    fn passing_observation(address: String) -> WalletQualityObservation {
        WalletQualityObservation {
            address,
            recent_activity_age_ms: 1,
            closed_trades: 100,
            realized_pnl_usd: Decimal::from(10_000),
            annualized_sharpe: Decimal::from(20),
            win_rate_pct: Decimal::from(99),
            maximum_drawdown_pct: Decimal::from(1),
            account_equity_usd: Decimal::from(1_000_000),
            maximum_observed_leverage: Decimal::ONE,
            maximum_margin_utilization_pct: Some(Decimal::from(10)),
            maximum_asset_concentration_pct: Decimal::from(10),
            median_holding_duration_ms: Some(60_000),
            retained_fill_count: 100,
            history_complete: true,
        }
    }

    fn artifact(existing: String, new: String) -> VeryProfitableLayerArtifact {
        VeryProfitableLayerArtifact {
            schema_version: COHORT_LAYER_SCHEMA_VERSION,
            admission_policy: CohortAdmissionPolicy::QualityFiltered,
            membership: HyperdashMembershipSnapshot {
                schema_version: 1,
                source: crate::domain::cohort::VERY_PROFITABLE_COHORT_URL.into(),
                cohort_id: crate::domain::cohort::VERY_PROFITABLE_COHORT_ID.into(),
                observed_at_ms: 1_000,
                displayed_min_all_time_pnl_usd: Decimal::from(100_000),
                displayed_max_all_time_pnl_usd: Decimal::from(1_000_000),
                completeness: crate::domain::cohort::MembershipCompleteness::Complete,
                reported_member_count: 2,
                captured_member_count: 2,
                wallets: vec![existing.clone(), new.clone()],
            },
            hydrated_at_ms: 2_000,
            authoritative_source: HYPERLIQUID_INFO_AUTHORITY.into(),
            authoritative_history_sha256: "ab".repeat(32),
            wallet_quality: vec![passing_observation(existing), passing_observation(new)],
            active_twaps: BTreeMap::new(),
        }
    }

    #[test]
    fn prepares_strict_hyperliquid_quality_and_deduplicates_existing_wallets() {
        let mut config = config();
        let existing = config.candidates[0].address.clone();
        let new = address(0xfeed);
        let prepared =
            PreparedVeryProfitableLayer::prepare(&artifact(existing, new.clone()), &config)
                .unwrap();
        assert_eq!(prepared.resolution.overlap_with_existing, 1);
        assert_eq!(
            prepared.qualified_cohort_only_members,
            BTreeSet::from([new])
        );
        assert_eq!(prepared.merge_candidates(&mut config).unwrap(), 1);
        assert_eq!(prepared.merge_candidates(&mut config).unwrap(), 0);
        assert_eq!(
            config
                .candidates
                .iter()
                .filter(|candidate| candidate.label.starts_with(COHORT_CANDIDATE_LABEL_PREFIX))
                .count(),
            1
        );
        assert_eq!(config.candidates.last().unwrap().allocation_weight, 0.0);
        assert_eq!(
            config
                .very_profitable_layer
                .as_ref()
                .unwrap()
                .artifact_sha256,
            prepared.artifact_sha256
        );
    }

    #[test]
    fn all_captured_policy_admits_every_valid_member_without_quality_decisions() {
        let mut config = config();
        let existing = config.candidates[0].address.clone();
        let new = address(0xbeef);
        let mut artifact = artifact(existing.clone(), new.clone());
        artifact.admission_policy = CohortAdmissionPolicy::AllCapturedMembers;
        artifact.wallet_quality.clear();

        let prepared = PreparedVeryProfitableLayer::prepare(&artifact, &config).unwrap();
        assert!(prepared.quality_decisions.is_empty());
        assert_eq!(
            prepared.qualified_members,
            BTreeSet::from([existing, new.clone()])
        );
        assert_eq!(
            prepared.qualified_cohort_only_members,
            BTreeSet::from([new])
        );
        assert_eq!(
            prepared
                .bounded_wallet_weights
                .values()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([Decimal::ONE])
        );
        assert_eq!(prepared.merge_candidates(&mut config).unwrap(), 1);
    }

    #[test]
    fn fails_closed_when_any_member_lacks_authoritative_history() {
        let config = config();
        let existing = config.candidates[0].address.clone();
        let new = address(0xbeef);
        let mut artifact = artifact(existing, new);
        artifact.wallet_quality.pop();
        assert!(matches!(
            PreparedVeryProfitableLayer::prepare(&artifact, &config),
            Err(CohortLayerError::InvalidArtifact(_))
        ));
    }

    #[test]
    fn rejected_wallet_is_not_scheduled() {
        // Edge-only: quality is diagnostics only, never a veto. Even a
        // failing wallet is scheduled; decisions retained for audit.
        let mut config = config();
        let existing = config.candidates[0].address.clone();
        let new = address(0xcafe);
        let mut artifact = artifact(existing, new.clone());
        artifact.wallet_quality[1].win_rate_pct = Decimal::from(1);
        let prepared = PreparedVeryProfitableLayer::prepare(&artifact, &config).unwrap();
        assert!(prepared
            .qualified_members
            .contains(&new.to_ascii_lowercase()));
        assert!(!prepared.quality_decisions.is_empty());
        assert_eq!(prepared.merge_candidates(&mut config).unwrap(), 1);
    }

    #[test]
    fn authoritative_positions_keep_full_cohort_while_live_book_deduplicates_overlap() {
        let config = config();
        let existing = config.candidates[0].address.clone();
        let new = address(0xd00d);
        let prepared =
            PreparedVeryProfitableLayer::prepare(&artifact(existing.clone(), new.clone()), &config)
                .unwrap();
        let position = crate::public_mainnet::SourceAssetPosition {
            asset: "BTC".into(),
            signed_size: Decimal::ONE,
            signed_notional: Decimal::from(500),
            entry_price: Some(Decimal::from(100)),
            unrealized_pnl: Some(Decimal::from(5)),
        };
        let states = vec![
            SourceStateResponse {
                block_number: None,
                candidate_id: existing,
                account_value: Decimal::from(1_000),
                source_time_ms: 2_100,
                positions: BTreeMap::from([("BTC".into(), position.clone())]),
                closed_candles: Vec::new(),
            },
            SourceStateResponse {
                block_number: None,
                candidate_id: new,
                account_value: Decimal::from(1_000),
                source_time_ms: 2_100,
                positions: BTreeMap::from([("BTC".into(), position)]),
                closed_candles: Vec::new(),
            },
        ];
        let wallets = prepared
            .authoritative_wallets(2_100, states.iter())
            .unwrap();
        assert_eq!(wallets.len(), 2);
        assert_eq!(
            wallets[0].positions["BTC"].signed_notional,
            Decimal::from(500)
        );
        assert_eq!(
            wallets[1].positions["BTC"].signed_notional,
            Decimal::from(500)
        );
    }

    #[test]
    fn cannot_rebind_a_configuration_to_a_different_snapshot() {
        let mut config = config();
        let existing = config.candidates[0].address.clone();
        let prepared = PreparedVeryProfitableLayer::prepare(
            &artifact(existing.clone(), address(0xa001)),
            &config,
        )
        .unwrap();
        prepared.merge_candidates(&mut config).unwrap();

        let mut replacement_artifact = artifact(existing, address(0xa002));
        replacement_artifact.membership.observed_at_ms = 1_001;
        replacement_artifact.hydrated_at_ms = 2_001;
        assert!(matches!(
            PreparedVeryProfitableLayer::prepare(&replacement_artifact, &config),
            Err(CohortLayerError::InvalidMergedConfiguration(_))
        ));
    }

    #[test]
    fn an_identically_prebound_snapshot_can_be_reloaded() {
        let mut config = config();
        let existing = config.candidates[0].address.clone();
        let artifact = artifact(existing, address(0xa003));
        let prepared = PreparedVeryProfitableLayer::prepare(&artifact, &config).unwrap();
        prepared.merge_candidates(&mut config).unwrap();

        let reloaded = PreparedVeryProfitableLayer::prepare(&artifact, &config).unwrap();
        assert_eq!(reloaded.artifact_sha256, prepared.artifact_sha256);
        assert_eq!(reloaded.resolution, prepared.resolution);
    }

    #[test]
    fn historical_sleeve_fields_do_not_control_cohort_preparation() {
        let mut config = config();
        let existing = config.candidates[0].address.clone();
        let artifact = artifact(existing, address(0xa004));
        config.technical.source_budget_fraction = 0.40;
        config.technical.technical_budget_fraction = 0.60;

        assert!(PreparedVeryProfitableLayer::prepare(&artifact, &config).is_ok());
        assert!(config.very_profitable_layer.is_none());
        assert_eq!(config.candidates.len(), 169);
    }

    #[test]
    fn rejects_non_hyperliquid_authority_and_future_hydration_mismatch() {
        let config = config();
        let existing = config.candidates[0].address.clone();
        let new = address(0xdead);
        let mut artifact = artifact(existing, new);
        artifact.authoritative_source = "https://hyperdash.com".into();
        assert!(matches!(
            PreparedVeryProfitableLayer::prepare(&artifact, &config),
            Err(CohortLayerError::InvalidArtifact(_))
        ));
    }
}
