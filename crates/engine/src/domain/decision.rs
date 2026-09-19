use crate::domain::allocation::{allocate_sparse_portfolio, SparseAllocationInput};
use crate::domain::configuration::{CopyTradeConfig, GlobalRiskConfig};
use crate::domain::consensus::{bounded_additive_consensus, ConsensusInput};
use crate::domain::execution_floor::{
    execution_floor_for_asset, AssetExecutionFloor, ExecutionFloorPolicy,
};
use crate::domain::portfolio_risk::{
    decimal_from_f64, project_and_validate_portfolio, Asset, PortfolioProjection,
    PortfolioProjectionInput, RiskViolation,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

macro_rules! byte_id {
    ($name:ident, $length:expr) => {
        #[derive(
            Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
        )]
        pub struct $name(pub [u8; $length]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }

            pub fn to_hex(self) -> String {
                hex_bytes(&self.0)
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&hex_bytes(&self.0))
            }
        }
    };
}

byte_id!(SnapshotSetId, 32);
byte_id!(DecisionId, 32);
byte_id!(PlannedCloid, 16);
byte_id!(EngineInstanceId, 16);
byte_id!(ConfigHash, 32);
byte_id!(RiskPolicyHash, 32);
byte_id!(MarketSnapshotId, 32);
byte_id!(PayloadHash, 32);
byte_id!(EligibilityHash, 32);
byte_id!(TargetHash, 32);
byte_id!(ProjectionHash, 32);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct TargetVersion(pub u64);

pub type MonotonicTimestamp = u64;

const SNAPSHOT_DOMAIN: &[u8] = b"HL1E/SNAPSHOT_SET/V1";
const ELIGIBILITY_DOMAIN: &[u8] = b"HL1E/ELIGIBILITY/V1";
const DECISION_DOMAIN: &[u8] = b"HL1E/DECISION/V1";
const CLOID_DOMAIN: &[u8] = b"HL1E/PLANNED_CLOID/V1";
const TARGET_DOMAIN: &[u8] = b"HL1E/TARGET_VECTOR/V1";
const CONFIG_DOMAIN: &[u8] = b"HL1E/CONFIG/V1";
const RISK_DOMAIN: &[u8] = b"HL1E/RISK_POLICY/V1";
const MARKET_DOMAIN: &[u8] = b"HL1E/MARKET_SNAPSHOT/V1";
const PAYLOAD_DOMAIN: &[u8] = b"HL1E/PAYLOAD/V1";
const PROJECTION_DOMAIN: &[u8] = b"LIVE/PORTFOLIO_PROJECTION/V1";

pub fn derive_projection_hash(
    projection: &PortfolioProjection,
) -> Result<ProjectionHash, IdentityError> {
    let mut canonical = Canonical::new(PROJECTION_DOMAIN);
    encode_decimal_map(&mut canonical, &projection.constrained_targets)?;
    encode_decimal_map(&mut canonical, &projection.proposed_deltas)?;
    encode_decimal_map(&mut canonical, &projection.rounded_deltas)?;
    canonical.length(projection.projected_ranges.len())?;
    for (asset, range) in &projection.projected_ranges {
        canonical.string(asset)?;
        canonical.string(&range.minimum.normalize().to_string())?;
        canonical.string(&range.maximum.normalize().to_string())?;
    }
    canonical.string(&projection.maximum_projected_gross.normalize().to_string())?;
    canonical.string(&projection.minimum_projected_net.normalize().to_string())?;
    canonical.string(&projection.maximum_projected_net.normalize().to_string())?;
    Ok(ProjectionHash(canonical.finish32()))
}

fn encode_decimal_map(
    canonical: &mut Canonical,
    values: &BTreeMap<Asset, Decimal>,
) -> Result<(), IdentityError> {
    canonical.length(values.len())?;
    for (asset, value) in values {
        canonical.string(asset)?;
        canonical.string(&value.normalize().to_string())?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    DuplicateCandidate(String),
    EligibilityMismatch,
    InvalidInput(String),
    ArithmeticOverflow(&'static str),
    Risk(RiskViolation),
}

impl Display for IdentityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for IdentityError {}

impl From<RiskViolation> for IdentityError {
    fn from(error: RiskViolation) -> Self {
        Self::Risk(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSetMember {
    pub candidate_id: String,
    pub accepted_sequence: u64,
    pub payload_hash: PayloadHash,
    pub received_at_mono: MonotonicTimestamp,
    pub valid_until_mono: MonotonicTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    Disabled,
    Quarantined,
    Stale,
    InvalidPayload,
    ZeroWeight,
    MissingSnapshot,
}

impl ExclusionReason {
    fn code(self) -> u8 {
        match self {
            Self::Disabled => 0,
            Self::Quarantined => 1,
            Self::Stale => 2,
            Self::InvalidPayload => 3,
            Self::ZeroWeight => 4,
            Self::MissingSnapshot => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceEligibilitySummary {
    pub active_ids: BTreeSet<String>,
    pub excluded: BTreeMap<String, ExclusionReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionIdentityInput {
    pub engine_instance_id: EngineInstanceId,
    pub decision_sequence: u64,
    pub config_hash: ConfigHash,
    pub risk_policy_hash: RiskPolicyHash,
    pub snapshot_set_id: SnapshotSetId,
    pub eligibility_hash: EligibilityHash,
    pub market_snapshot_id: MarketSnapshotId,
    pub target_version: TargetVersion,
}

pub fn derive_snapshot_set_id(
    members: &[SnapshotSetMember],
) -> Result<SnapshotSetId, IdentityError> {
    let mut members = members.to_vec();
    members.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
    for pair in members.windows(2) {
        if pair[0].candidate_id == pair[1].candidate_id {
            return Err(IdentityError::DuplicateCandidate(
                pair[0].candidate_id.clone(),
            ));
        }
    }
    let mut canonical = Canonical::new(SNAPSHOT_DOMAIN);
    canonical.length(members.len())?;
    for member in members {
        if member.received_at_mono > member.valid_until_mono {
            return Err(IdentityError::InvalidInput(format!(
                "invalid freshness interval for {}",
                member.candidate_id
            )));
        }
        canonical.string(&member.candidate_id)?;
        canonical.u64(member.accepted_sequence);
        canonical.bytes(member.payload_hash.as_bytes());
        canonical.u64(member.received_at_mono);
        canonical.u64(member.valid_until_mono);
    }
    Ok(SnapshotSetId(canonical.finish32()))
}

pub fn derive_eligibility_hash(
    summary: &SourceEligibilitySummary,
) -> Result<EligibilityHash, IdentityError> {
    if summary
        .active_ids
        .iter()
        .any(|candidate| summary.excluded.contains_key(candidate))
    {
        return Err(IdentityError::EligibilityMismatch);
    }
    let mut canonical = Canonical::new(ELIGIBILITY_DOMAIN);
    canonical.length(summary.active_ids.len())?;
    for candidate in &summary.active_ids {
        canonical.string(candidate)?;
    }
    canonical.length(summary.excluded.len())?;
    for (candidate, reason) in &summary.excluded {
        canonical.string(candidate)?;
        canonical.u8(reason.code());
    }
    Ok(EligibilityHash(canonical.finish32()))
}

pub fn derive_decision_id(input: &DecisionIdentityInput) -> DecisionId {
    let mut canonical = Canonical::new(DECISION_DOMAIN);
    canonical.bytes(input.engine_instance_id.as_bytes());
    canonical.u64(input.decision_sequence);
    canonical.bytes(input.config_hash.as_bytes());
    canonical.bytes(input.risk_policy_hash.as_bytes());
    canonical.bytes(input.snapshot_set_id.as_bytes());
    canonical.bytes(input.eligibility_hash.as_bytes());
    canonical.bytes(input.market_snapshot_id.as_bytes());
    canonical.u64(input.target_version.0);
    DecisionId(canonical.finish32())
}

pub fn hash_config_bytes(bytes: &[u8]) -> ConfigHash {
    ConfigHash(domain_hash(CONFIG_DOMAIN, bytes))
}

pub fn derive_config_hash(config: &CopyTradeConfig) -> Result<ConfigHash, IdentityError> {
    Ok(ConfigHash(hash_canonical_value(
        CONFIG_DOMAIN,
        &serde_json::to_value(config)
            .map_err(|error| IdentityError::InvalidInput(error.to_string()))?,
    )?))
}

pub fn derive_risk_policy_hash(risk: &GlobalRiskConfig) -> Result<RiskPolicyHash, IdentityError> {
    Ok(RiskPolicyHash(hash_canonical_value(
        RISK_DOMAIN,
        &serde_json::to_value(risk)
            .map_err(|error| IdentityError::InvalidInput(error.to_string()))?,
    )?))
}

pub fn hash_risk_policy_bytes(bytes: &[u8]) -> RiskPolicyHash {
    RiskPolicyHash(domain_hash(RISK_DOMAIN, bytes))
}

pub fn hash_market_snapshot_bytes(bytes: &[u8]) -> MarketSnapshotId {
    MarketSnapshotId(domain_hash(MARKET_DOMAIN, bytes))
}

pub fn hash_payload_bytes(bytes: &[u8]) -> PayloadHash {
    PayloadHash(domain_hash(PAYLOAD_DOMAIN, bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousTargetState {
    pub version: TargetVersion,
    pub target_hash: TargetHash,
}

pub fn canonical_target_hash(
    targets: &BTreeMap<Asset, Decimal>,
) -> Result<TargetHash, IdentityError> {
    let mut canonical = Canonical::new(TARGET_DOMAIN);
    canonical.length(targets.len())?;
    for (asset, value) in targets {
        canonical.string(asset)?;
        canonical.decimal(*value);
    }
    Ok(TargetHash(canonical.finish32()))
}

pub fn resolve_target_version(
    previous: Option<&PreviousTargetState>,
    targets: &BTreeMap<Asset, Decimal>,
) -> Result<(TargetVersion, TargetHash), IdentityError> {
    let current_hash = canonical_target_hash(targets)?;
    let version = match previous {
        None => TargetVersion(0),
        Some(previous) if previous.target_hash == current_hash => previous.version,
        Some(previous) => TargetVersion(
            previous
                .version
                .0
                .checked_add(1)
                .ok_or(IdentityError::ArithmeticOverflow("target version"))?,
        ),
    };
    Ok((version, current_hash))
}

pub fn next_decision_sequence(current: u64) -> Result<u64, IdentityError> {
    current
        .checked_add(1)
        .ok_or(IdentityError::ArithmeticOverflow("decision sequence"))
}

#[derive(Debug, Clone)]
pub struct DecisionConstructionInput {
    pub engine_instance_id: EngineInstanceId,
    pub decision_sequence: u64,
    pub config_hash: ConfigHash,
    pub risk_policy_hash: RiskPolicyHash,
    pub snapshot_members: Vec<SnapshotSetMember>,
    pub eligibility: SourceEligibilitySummary,
    pub market_snapshot_id: MarketSnapshotId,
    pub consensus_inputs: BTreeMap<Asset, Vec<ConsensusInput>>,
    pub maximum_source_exposure: f64,
    pub source_snapshot_max_age_ms: u64,
    pub execution_floor_policy: ExecutionFloorPolicy,
    pub slot_rank_hysteresis: Decimal,
    pub projection_input: PortfolioProjectionInput,
    pub previous_target: Option<PreviousTargetState>,
    pub created_at_mono: MonotonicTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionRecord {
    pub decision_id: DecisionId,
    pub decision_sequence: u64,
    pub target_version: TargetVersion,
    pub target_hash: TargetHash,
    pub config_hash: ConfigHash,
    pub risk_policy_hash: RiskPolicyHash,
    pub snapshot_set_id: SnapshotSetId,
    pub eligibility_hash: EligibilityHash,
    pub market_snapshot_id: MarketSnapshotId,
    pub consensus: BTreeMap<Asset, Decimal>,
    pub unconstrained_targets: BTreeMap<Asset, Decimal>,
    pub retained_below_minimum: BTreeMap<Asset, Decimal>,
    pub micro_slots: BTreeMap<Asset, MicroPositionSlot>,
    pub projection: PortfolioProjection,
    pub created_at_mono: MonotonicTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotLifecycle {
    ExitPending,
    RiskReductionPending,
    ExistingContinuation,
    NewPositionAdmitted,
    RetainedBelowMinimum,
    RetainedForCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroPositionSlot {
    pub asset: Asset,
    pub desired_notional: Decimal,
    pub admitted_notional: Decimal,
    pub filled_notional: Decimal,
    pub committed_notional: Decimal,
    pub residual_notional: Decimal,
    pub conviction: Decimal,
    pub agreement_weight: Decimal,
    pub execution_floor: AssetExecutionFloor,
    pub target_version: TargetVersion,
    pub lifecycle: SlotLifecycle,
}

pub fn construct_decision(
    mut input: DecisionConstructionInput,
) -> Result<DecisionRecord, IdentityError> {
    let member_ids = input
        .snapshot_members
        .iter()
        .map(|member| member.candidate_id.clone())
        .collect::<BTreeSet<_>>();
    if member_ids != input.eligibility.active_ids {
        return Err(IdentityError::EligibilityMismatch);
    }
    let snapshot_set_id = derive_snapshot_set_id(&input.snapshot_members)?;
    let eligibility_hash = derive_eligibility_hash(&input.eligibility)?;
    let mut consensus = BTreeMap::new();
    let mut agreement_weights = BTreeMap::new();
    for (asset, inputs) in &mut input.consensus_inputs {
        inputs.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
        for pair in inputs.windows(2) {
            if pair[0].candidate_id == pair[1].candidate_id {
                return Err(IdentityError::DuplicateCandidate(
                    pair[0].candidate_id.clone(),
                ));
            }
        }
        let result = bounded_additive_consensus(
            inputs,
            input.maximum_source_exposure,
            input.source_snapshot_max_age_ms,
        )
        .map_err(|error| IdentityError::InvalidInput(error.to_string()))?;
        consensus.insert(
            asset.clone(),
            decimal_from_f64(result.exposure, "consensus exposure")?,
        );
        let agreement = inputs
            .iter()
            .filter(|candidate| {
                candidate.enabled
                    && !candidate.quarantined
                    && candidate.snapshot_age_ms <= input.source_snapshot_max_age_ms
                    && candidate.source_exposure != 0.0
                    && candidate.source_exposure.is_sign_positive()
                        == result.exposure.is_sign_positive()
            })
            .try_fold(Decimal::ZERO, |sum, candidate| {
                let weight = decimal_from_f64(
                    candidate.allocation_weight * candidate.confidence_modifier,
                    "agreement weight",
                )?;
                sum.checked_add(weight)
                    .ok_or(IdentityError::ArithmeticOverflow("agreement weight"))
            })?;
        agreement_weights.insert(asset.clone(), agreement);
    }

    let scale = input
        .projection_input
        .current_equity
        .checked_mul(input.projection_input.curve_leverage)
        .and_then(|value| value.checked_mul(input.projection_input.global_risk_scale))
        .ok_or(IdentityError::ArithmeticOverflow("target scale"))?;
    let mut unconstrained_targets = BTreeMap::new();
    for (asset, exposure) in &consensus {
        unconstrained_targets.insert(
            asset.clone(),
            scale
                .checked_mul(*exposure)
                .ok_or(IdentityError::ArithmeticOverflow("unconstrained target"))?,
        );
    }
    let execution_floors = unconstrained_targets
        .keys()
        .chain(input.projection_input.filled_positions.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|asset| {
            let raw = unconstrained_targets
                .get(&asset)
                .copied()
                .unwrap_or_default();
            let filled = input
                .projection_input
                .filled_positions
                .get(&asset)
                .copied()
                .unwrap_or_default();
            let side = if raw >= filled { Side::Buy } else { Side::Sell };
            let rules = input
                .projection_input
                .market_rules
                .get(&asset)
                .ok_or_else(|| {
                    IdentityError::Risk(RiskViolation::MissingMarketRules(asset.clone()))
                })?;
            Ok((
                asset,
                execution_floor_for_asset(side, rules, input.execution_floor_policy)
                    .map_err(|error| IdentityError::InvalidInput(error.to_string()))?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, IdentityError>>()?;
    let allocation = allocate_sparse_portfolio(&SparseAllocationInput {
        raw_targets: &unconstrained_targets,
        convictions: &consensus,
        agreement_weights: &agreement_weights,
        execution_floors: &execution_floors,
        filled_positions: &input.projection_input.filled_positions,
        current_equity: input.projection_input.current_equity,
        curve_leverage: input.projection_input.curve_leverage,
        global_risk_scale: input.projection_input.global_risk_scale,
        max_single_asset_equity_pct: input.projection_input.max_single_asset_equity_pct,
        max_net_equity_pct: input.projection_input.max_net_equity_pct,
        rank_hysteresis: input.slot_rank_hysteresis,
    })
    .map_err(|error| IdentityError::InvalidInput(error.to_string()))?;
    input.projection_input.unconstrained_targets = allocation.admitted_targets;
    let projection = project_and_validate_portfolio(&input.projection_input)?;
    let (target_version, target_hash) = resolve_target_version(
        input.previous_target.as_ref(),
        &projection.constrained_targets,
    )?;
    let decision_id = derive_decision_id(&DecisionIdentityInput {
        engine_instance_id: input.engine_instance_id,
        decision_sequence: input.decision_sequence,
        config_hash: input.config_hash,
        risk_policy_hash: input.risk_policy_hash,
        snapshot_set_id,
        eligibility_hash,
        market_snapshot_id: input.market_snapshot_id,
        target_version,
    });
    let micro_slots = execution_floors
        .iter()
        .map(|(asset, floor)| -> Result<_, IdentityError> {
            let desired = unconstrained_targets
                .get(asset)
                .copied()
                .unwrap_or_default();
            let admitted = projection
                .constrained_targets
                .get(asset)
                .copied()
                .unwrap_or_default();
            let filled = input
                .projection_input
                .filled_positions
                .get(asset)
                .copied()
                .unwrap_or_default();
            let lifecycle = slot_lifecycle(
                desired,
                admitted,
                filled,
                allocation.retained_below_minimum.contains_key(asset),
            );
            let residual = admitted
                .checked_sub(filled)
                .ok_or(IdentityError::ArithmeticOverflow("micro slot residual"))?;
            Ok((
                asset.clone(),
                MicroPositionSlot {
                    asset: asset.clone(),
                    desired_notional: desired,
                    admitted_notional: admitted,
                    filled_notional: filled,
                    committed_notional: filled,
                    residual_notional: residual,
                    conviction: consensus.get(asset).copied().unwrap_or_default(),
                    agreement_weight: agreement_weights.get(asset).copied().unwrap_or_default(),
                    execution_floor: *floor,
                    target_version,
                    lifecycle,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(DecisionRecord {
        decision_id,
        decision_sequence: input.decision_sequence,
        target_version,
        target_hash,
        config_hash: input.config_hash,
        risk_policy_hash: input.risk_policy_hash,
        snapshot_set_id,
        eligibility_hash,
        market_snapshot_id: input.market_snapshot_id,
        consensus,
        unconstrained_targets,
        retained_below_minimum: allocation.retained_below_minimum,
        micro_slots,
        projection,
        created_at_mono: input.created_at_mono,
    })
}

fn slot_lifecycle(
    desired: Decimal,
    admitted: Decimal,
    filled: Decimal,
    retained_below_minimum: bool,
) -> SlotLifecycle {
    if retained_below_minimum {
        SlotLifecycle::RetainedBelowMinimum
    } else if !filled.is_zero() && admitted.is_zero() {
        SlotLifecycle::ExitPending
    } else if !filled.is_zero() && admitted.abs() < filled.abs() {
        SlotLifecycle::RiskReductionPending
    } else if !filled.is_zero() {
        SlotLifecycle::ExistingContinuation
    } else if !admitted.is_zero() {
        SlotLifecycle::NewPositionAdmitted
    } else {
        let _ = desired;
        SlotLifecycle::RetainedForCapacity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    fn code(self) -> u8 {
        match self {
            Self::Buy => 0,
            Self::Sell => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedAction {
    pub decision_id: DecisionId,
    pub target_version: TargetVersion,
    pub asset: Asset,
    pub side: Side,
    pub rounded_notional: Decimal,
    pub reduce_only: bool,
    pub action_ordinal: u32,
    pub retry_generation: u32,
    pub planned_cloid: PlannedCloid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedCloidInput {
    pub engine_instance_id: EngineInstanceId,
    pub decision_id: DecisionId,
    pub target_version: TargetVersion,
    pub asset: Asset,
    pub side: Side,
    pub reduce_only: bool,
    pub action_ordinal: u32,
    pub retry_generation: u32,
}

pub fn derive_planned_cloid(input: &PlannedCloidInput) -> Result<PlannedCloid, IdentityError> {
    if input.asset.is_empty() {
        return Err(IdentityError::InvalidInput(
            "planned action asset must not be empty".to_string(),
        ));
    }
    let mut canonical = Canonical::new(CLOID_DOMAIN);
    canonical.bytes(input.engine_instance_id.as_bytes());
    canonical.bytes(input.decision_id.as_bytes());
    canonical.u64(input.target_version.0);
    canonical.string(&input.asset)?;
    canonical.u8(input.side.code());
    canonical.u8(u8::from(input.reduce_only));
    canonical.u32(input.action_ordinal);
    canonical.u32(input.retry_generation);
    let full = canonical.finish32();
    let mut cloid = [0_u8; 16];
    cloid.copy_from_slice(&full[..16]);
    Ok(PlannedCloid(cloid))
}

pub fn construct_planned_actions(
    engine_instance_id: EngineInstanceId,
    decision: &DecisionRecord,
    reduce_only_by_asset: &BTreeMap<Asset, bool>,
) -> Result<Vec<PlannedAction>, IdentityError> {
    let mut actions = Vec::new();
    for (asset, delta) in &decision.projection.rounded_deltas {
        let reduce_only = reduce_only_by_asset.get(asset).copied().unwrap_or(false);
        let actionable_delta = if delta.is_zero() && reduce_only {
            decision
                .projection
                .proposed_deltas
                .get(asset)
                .copied()
                .unwrap_or_default()
        } else {
            *delta
        };
        if actionable_delta.is_zero() {
            continue;
        }
        let action_ordinal = u32::try_from(actions.len())
            .map_err(|_| IdentityError::ArithmeticOverflow("action ordinal"))?;
        let side = if actionable_delta > Decimal::ZERO {
            Side::Buy
        } else {
            Side::Sell
        };
        let retry_generation = 0;
        let cloid_input = PlannedCloidInput {
            engine_instance_id,
            decision_id: decision.decision_id,
            target_version: decision.target_version,
            asset: asset.clone(),
            side,
            reduce_only,
            action_ordinal,
            retry_generation,
        };
        actions.push(PlannedAction {
            decision_id: decision.decision_id,
            target_version: decision.target_version,
            asset: asset.clone(),
            side,
            // For a reduction rounded to zero at target projection this is a
            // trigger notional. The existing reduce-only book-time planner
            // derives the exact closable quantity from committed quantity.
            rounded_notional: actionable_delta.abs(),
            reduce_only: cloid_input.reduce_only,
            action_ordinal,
            retry_generation,
            planned_cloid: derive_planned_cloid(&cloid_input)?,
        });
    }
    Ok(actions)
}

fn domain_hash(domain: &[u8], payload: &[u8]) -> [u8; 32] {
    let mut canonical = Canonical::new(domain);
    canonical.bytes(payload);
    canonical.finish32()
}

fn hash_canonical_value(
    domain: &[u8],
    value: &serde_json::Value,
) -> Result<[u8; 32], IdentityError> {
    fn encode(value: &serde_json::Value, output: &mut Canonical) -> Result<(), IdentityError> {
        match value {
            serde_json::Value::Null => output.u8(0),
            serde_json::Value::Bool(value) => {
                output.u8(1);
                output.u8(u8::from(*value));
            }
            serde_json::Value::Number(value) => {
                output.u8(2);
                if let Some(value) = value.as_i64() {
                    output.u8(0);
                    output.bytes(&value.to_be_bytes());
                } else if let Some(value) = value.as_u64() {
                    output.u8(1);
                    output.u64(value);
                } else if let Some(value) = value.as_f64() {
                    if !value.is_finite() {
                        return Err(IdentityError::InvalidInput(
                            "non-finite canonical number".to_string(),
                        ));
                    }
                    output.u8(2);
                    output.u64(value.to_bits());
                } else {
                    return Err(IdentityError::InvalidInput(
                        "unrepresentable canonical number".to_string(),
                    ));
                }
            }
            serde_json::Value::String(value) => {
                output.u8(3);
                output.string(value)?;
            }
            serde_json::Value::Array(values) => {
                output.u8(4);
                output.length(values.len())?;
                for value in values {
                    encode(value, output)?;
                }
            }
            serde_json::Value::Object(values) => {
                output.u8(5);
                output.length(values.len())?;
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_by(|left, right| left.0.cmp(right.0));
                for (key, value) in entries {
                    output.string(key)?;
                    encode(value, output)?;
                }
            }
        }
        Ok(())
    }

    let mut canonical = Canonical::new(domain);
    encode(value, &mut canonical)?;
    Ok(canonical.finish32())
}

struct Canonical {
    bytes: Vec<u8>,
}

impl Canonical {
    fn new(domain: &[u8]) -> Self {
        let mut value = Self { bytes: Vec::new() };
        value.bytes(domain);
        value
    }

    fn finish32(self) -> [u8; 32] {
        Sha256::digest(self.bytes).into()
    }

    fn length(&mut self, length: usize) -> Result<(), IdentityError> {
        self.u32(
            u32::try_from(length)
                .map_err(|_| IdentityError::ArithmeticOverflow("canonical length"))?,
        );
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), IdentityError> {
        self.length(value.len())?;
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn decimal(&mut self, value: Decimal) {
        let normalized = value.normalize();
        self.bytes
            .extend_from_slice(&normalized.mantissa().to_be_bytes());
        self.u32(normalized.scale());
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("0x");
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::portfolio_risk::MarketRules;
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn consensus_input(candidate: &str, exposure: f64) -> ConsensusInput {
        ConsensusInput {
            candidate_id: candidate.to_string(),
            allocation_weight: 1.0,
            confidence_modifier: 1.0,
            source_exposure: exposure,
            enabled: true,
            quarantined: false,
            snapshot_age_ms: 0,
        }
    }

    fn construction_input() -> DecisionConstructionInput {
        let candidate = "candidate-001".to_string();
        DecisionConstructionInput {
            engine_instance_id: EngineInstanceId([1; 16]),
            decision_sequence: 7,
            config_hash: hash_config_bytes(b"config-v1"),
            risk_policy_hash: hash_risk_policy_bytes(b"risk-v1"),
            snapshot_members: vec![SnapshotSetMember {
                candidate_id: candidate.clone(),
                accepted_sequence: 11,
                payload_hash: hash_payload_bytes(b"btc:+0.1"),
                received_at_mono: 1_000,
                valid_until_mono: 41_000,
            }],
            eligibility: SourceEligibilitySummary {
                active_ids: [candidate.clone()].into_iter().collect(),
                excluded: BTreeMap::new(),
            },
            market_snapshot_id: hash_market_snapshot_bytes(b"BTC:100"),
            consensus_inputs: [("BTC".to_string(), vec![consensus_input(&candidate, 0.10)])]
                .into_iter()
                .collect(),
            maximum_source_exposure: 1.0,
            source_snapshot_max_age_ms: 40_000,
            execution_floor_policy: ExecutionFloorPolicy {
                exchange_minimum_notional: decimal("10"),
                rounding_buffer: decimal("0.2"),
                closeability_margin: decimal("0.5"),
                maximum_slippage_fraction: decimal("0.0004"),
            },
            slot_rank_hysteresis: decimal("0.02"),
            projection_input: PortfolioProjectionInput {
                current_equity: decimal("1000"),
                curve_leverage: decimal("8"),
                global_risk_scale: decimal("0.025"),
                max_single_asset_equity_pct: decimal("0.65"),
                max_net_equity_pct: decimal("0.65"),
                filled_positions: BTreeMap::new(),
                filled_position_state_complete: true,
                acknowledged_open_orders: Vec::new(),
                open_order_state_complete: true,
                unconstrained_targets: BTreeMap::new(),
                market_rules: [(
                    "BTC".to_string(),
                    MarketRules {
                        mark_price: decimal("100"),
                        price_tick: decimal("0.1"),
                        size_step: decimal("0.001"),
                    },
                )]
                .into_iter()
                .collect(),
                gross_cap_override: None,
                held_manual_assets: BTreeSet::new(),
            },
            previous_target: None,
            created_at_mono: 1_000,
        }
    }

    #[test]
    fn canonical_snapshot_and_decision_identity_ignore_insertion_order() {
        let mut members = construction_input().snapshot_members;
        members.push(SnapshotSetMember {
            candidate_id: "candidate-000".to_string(),
            accepted_sequence: 1,
            payload_hash: hash_payload_bytes(b"other"),
            received_at_mono: 1,
            valid_until_mono: 2,
        });
        let first = derive_snapshot_set_id(&members).unwrap();
        members.reverse();
        assert_eq!(first, derive_snapshot_set_id(&members).unwrap());

        let first = construct_decision(construction_input()).unwrap();
        let second = construct_decision(construction_input()).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn material_inputs_change_their_identity_domains() {
        let base = construction_input();
        let snapshot = derive_snapshot_set_id(&base.snapshot_members).unwrap();
        let mut changed_members = base.snapshot_members.clone();
        changed_members[0].accepted_sequence += 1;
        assert_ne!(snapshot, derive_snapshot_set_id(&changed_members).unwrap());

        let decision = construct_decision(base.clone()).unwrap();
        let mut changed = base;
        changed.config_hash = hash_config_bytes(b"config-v2");
        assert_ne!(
            decision.decision_id,
            construct_decision(changed).unwrap().decision_id
        );
        assert_ne!(snapshot.as_bytes(), decision.decision_id.as_bytes());
    }

    #[test]
    fn target_version_changes_only_for_a_material_target_change() {
        let first = construct_decision(construction_input()).unwrap();
        let mut refreshed = construction_input();
        refreshed.decision_sequence = 8;
        refreshed.snapshot_members[0].accepted_sequence = 12;
        refreshed.previous_target = Some(PreviousTargetState {
            version: first.target_version,
            target_hash: first.target_hash,
        });
        let same_target = construct_decision(refreshed).unwrap();
        assert_ne!(first.decision_id, same_target.decision_id);
        assert_eq!(same_target.target_version, TargetVersion(0));

        let mut changed = construction_input();
        changed.consensus_inputs.get_mut("BTC").unwrap()[0].source_exposure = 0.20;
        changed.previous_target = Some(PreviousTargetState {
            version: first.target_version,
            target_hash: first.target_hash,
        });
        assert_eq!(
            construct_decision(changed).unwrap().target_version,
            TargetVersion(1)
        );
        assert!(resolve_target_version(
            Some(&PreviousTargetState {
                version: TargetVersion(u64::MAX),
                target_hash: TargetHash([0; 32]),
            }),
            &first.projection.constrained_targets,
        )
        .is_err());
        assert!(next_decision_sequence(u64::MAX).is_err());
    }

    #[test]
    fn anime_technical_neutralization_emits_existing_reduce_only_flatten() {
        let mut input = construction_input();
        input.consensus_inputs = BTreeMap::from([(
            "ANIME".to_string(),
            vec![consensus_input("candidate-001", 0.0)],
        )]);
        input.projection_input.filled_positions =
            BTreeMap::from([("ANIME".to_string(), decimal("36.97484"))]);
        input.projection_input.market_rules = BTreeMap::from([(
            "ANIME".to_string(),
            MarketRules {
                mark_price: decimal("0.00259"),
                price_tick: decimal("0.000001"),
                size_step: decimal("1"),
            },
        )]);

        let decision = construct_decision(input).unwrap();
        assert_eq!(
            decision.micro_slots["ANIME"].admitted_notional,
            Decimal::ZERO
        );
        assert_eq!(
            decision.projection.rounded_deltas["ANIME"],
            decimal("-36.97484")
        );
        let actions = construct_planned_actions(
            EngineInstanceId([1; 16]),
            &decision,
            &BTreeMap::from([("ANIME".to_string(), true)]),
        )
        .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].side, Side::Sell);
        assert!(actions[0].reduce_only);
    }

    #[test]
    fn planned_cloids_are_replayable_and_sensitive_to_every_action_dimension() {
        let decision = construct_decision(construction_input()).unwrap();
        let base = PlannedCloidInput {
            engine_instance_id: EngineInstanceId([1; 16]),
            decision_id: decision.decision_id,
            target_version: decision.target_version,
            asset: "BTC".to_string(),
            side: Side::Buy,
            reduce_only: false,
            action_ordinal: 0,
            retry_generation: 0,
        };
        let first = derive_planned_cloid(&base).unwrap();
        assert_eq!(first, derive_planned_cloid(&base).unwrap());
        let variants = [
            PlannedCloidInput {
                asset: "ETH".to_string(),
                ..base.clone()
            },
            PlannedCloidInput {
                side: Side::Sell,
                ..base.clone()
            },
            PlannedCloidInput {
                action_ordinal: 1,
                ..base.clone()
            },
            PlannedCloidInput {
                retry_generation: 1,
                ..base.clone()
            },
        ];
        assert!(variants
            .iter()
            .all(|variant| derive_planned_cloid(variant).unwrap() != first));
    }

    #[test]
    fn projection_rounding_cannot_erase_a_reduce_only_trigger() {
        let mut decision = construct_decision(construction_input()).unwrap();
        decision
            .projection
            .proposed_deltas
            .insert("BTC".into(), decimal("-0.09"));
        decision
            .projection
            .rounded_deltas
            .insert("BTC".into(), Decimal::ZERO);

        let actions = construct_planned_actions(
            EngineInstanceId([1; 16]),
            &decision,
            &BTreeMap::from([("BTC".to_string(), true)]),
        )
        .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].side, Side::Sell);
        assert!(actions[0].reduce_only);
        assert_eq!(actions[0].rounded_notional, decimal("0.09"));

        assert!(
            construct_planned_actions(EngineInstanceId([1; 16]), &decision, &BTreeMap::new(),)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn all_169_sources_and_opposing_signals_are_deterministic() {
        let mut input = construction_input();
        input.snapshot_members.clear();
        input.eligibility.active_ids.clear();
        let mut sources = Vec::new();
        for index in 0..169 {
            let candidate = format!("candidate-{index:03}");
            input.eligibility.active_ids.insert(candidate.clone());
            input.snapshot_members.push(SnapshotSetMember {
                candidate_id: candidate.clone(),
                accepted_sequence: 1,
                payload_hash: hash_payload_bytes(candidate.as_bytes()),
                received_at_mono: 1,
                valid_until_mono: 40_001,
            });
            sources.push(consensus_input(
                &candidate,
                if index % 2 == 0 { 0.1 } else { -0.1 },
            ));
        }
        input.consensus_inputs.insert("BTC".to_string(), sources);
        let first = construct_decision(input.clone()).unwrap();
        input.snapshot_members.reverse();
        input.consensus_inputs.get_mut("BTC").unwrap().reverse();
        assert_eq!(first, construct_decision(input).unwrap());
    }

    #[test]
    fn fixed_replay_vector_v1() {
        let decision = construct_decision(construction_input()).unwrap();
        let actions =
            construct_planned_actions(EngineInstanceId([1; 16]), &decision, &BTreeMap::new())
                .unwrap();
        assert_eq!(
            decision.snapshot_set_id.to_hex(),
            "0x5dd1d746e7ac9aa38f58b85cae3690ee217ea5f36d1307262c61bff76230497b"
        );
        assert_eq!(
            decision.eligibility_hash.to_hex(),
            "0xed9b277a50cbd26a13758909f2fc97c3f9dda43608d182515947fa0e7d5edc74"
        );
        assert_eq!(
            decision.target_hash.to_hex(),
            "0x77622c9b4eb45004872a09eeffecbfa5b1ec6c944e5d6cfaa8d7dc66c6e31f8d"
        );
        assert_eq!(
            decision.decision_id.to_hex(),
            "0x440a766e10a96b5d19d44e5713ebc4f82f4cd21cebb80e12aac7f4686e73ba72"
        );
        assert_eq!(
            actions[0].planned_cloid.to_hex(),
            "0x289e187bea2d80417adbabbf1087b059"
        );
        assert_eq!(actions[0].rounded_notional, decimal("20"));
        assert_eq!(actions[0].retry_generation, 0);
    }
}
