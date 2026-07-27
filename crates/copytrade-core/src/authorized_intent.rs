//! Signer-free immutable execution envelope produced by the authoritative core.

use crate::decision::{
    ConfigHash, DecisionId, MonotonicTimestamp, PlannedCloid, ProjectionHash, RiskPolicyHash, Side,
    TargetVersion,
};
use crate::portfolio_risk::PortfolioProjectionInput;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const AUTHORIZED_INTENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Ioc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedExecutionIntent {
    pub schema_version: u16,
    pub decision_id: DecisionId,
    pub target_version: TargetVersion,
    pub root_cloid: PlannedCloid,
    pub planned_cloid: PlannedCloid,
    pub parent_cloid: Option<PlannedCloid>,
    pub continuation_generation: u32,
    pub asset: String,
    pub asset_index: u32,
    pub side: Side,
    pub quantity: Decimal,
    pub limit_price: Decimal,
    pub reduce_only: bool,
    pub time_in_force: TimeInForce,
    pub projected_portfolio_hash: ProjectionHash,
    pub risk_policy_hash: RiskPolicyHash,
    pub configuration_hash: ConfigHash,
    pub market_rules_hash: [u8; 32],
    pub dynamic_floor_policy_hash: [u8; 32],
    pub ioc_policy_hash: [u8; 32],
    pub observer_release_hash: [u8; 32],
    pub signer_release_hash: [u8; 32],
    pub release_manifest_hash: [u8; 32],
    pub decision_reference_price: Decimal,
    pub expected_committed_before: Decimal,
    pub expected_committed_after: Decimal,
    pub decision_timestamp_ms: u64,
    pub expires_at: MonotonicTimestamp,
    /// Immutable authorization material constructed by core. The signer
    /// refreshes only exchange-owned fields before re-running the projector.
    pub authorization: PreSigningContext,
    /// SHA-256 of the canonical MessagePack encoding with this field zeroed.
    pub canonical_hash: [u8; 32],
}

impl AuthorizedExecutionIntent {
    pub fn compute_canonical_hash(&self) -> Result<[u8; 32], String> {
        let mut canonical = self.clone();
        canonical.canonical_hash = [0; 32];
        let bytes = rmp_serde::to_vec(&canonical).map_err(|error| error.to_string())?;
        Ok(Sha256::digest(bytes).into())
    }

    pub fn seal(mut self) -> Result<Self, String> {
        self.canonical_hash = self.compute_canonical_hash()?;
        Ok(self)
    }

    pub fn validate_canonical(&self) -> Result<(), String> {
        if self.schema_version != AUTHORIZED_INTENT_SCHEMA_VERSION {
            return Err("unsupported authorized intent schema".into());
        }
        if self.time_in_force != TimeInForce::Ioc {
            return Err("authorized execution must be IOC".into());
        }
        if self.quantity <= Decimal::ZERO
            || self.limit_price <= Decimal::ZERO
            || self.decision_reference_price <= Decimal::ZERO
            || self.expires_at == 0
        {
            return Err("invalid authorized intent financial field".into());
        }
        if self.canonical_hash != self.compute_canonical_hash()? {
            return Err("authorized intent canonical hash mismatch".into());
        }
        Ok(())
    }
}

/// Complete signer authorization material.  The signer may refresh exchange
/// positions/open orders and re-run the authoritative projector, but it may
/// not alter the target vector, market rules, or economic parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreSigningContext {
    pub now_mono: MonotonicTimestamp,
    pub deployed_risk_policy_hash: RiskPolicyHash,
    pub projection_input: PortfolioProjectionInput,
    pub exchange_minimum_notional: Decimal,
}
