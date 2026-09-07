use crate::domain::decision::{SnapshotSetId, TargetVersion};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::Path;

const TARGET_LEDGER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualTargetState {
    pub raw_desired_notional: Decimal,
    pub admitted_target_notional: Decimal,
    pub filled_notional: Decimal,
    pub acknowledged_open_notional: Decimal,
    pub unknown_result_notional: Decimal,
    pub executable_residual: Decimal,
    pub target_version: TargetVersion,
    pub latest_source_set_id: SnapshotSetId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualTargetLedger {
    schema_version: u32,
    assets: BTreeMap<String, VirtualTargetState>,
}

impl Default for VirtualTargetLedger {
    fn default() -> Self {
        Self {
            schema_version: TARGET_LEDGER_SCHEMA_VERSION,
            assets: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStateError {
    ArithmeticOverflow(&'static str),
    Persistence(String),
    SchemaMismatch,
}

impl Display for TargetStateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for TargetStateError {}

impl VirtualTargetLedger {
    #[allow(clippy::too_many_arguments)]
    pub fn replace_absolute_targets(
        &mut self,
        raw_desired: &BTreeMap<String, Decimal>,
        admitted_targets: &BTreeMap<String, Decimal>,
        filled: &BTreeMap<String, Decimal>,
        acknowledged_open: &BTreeMap<String, Decimal>,
        unknown_result: &BTreeMap<String, Decimal>,
        target_version: TargetVersion,
        source_set_id: SnapshotSetId,
    ) -> Result<(), TargetStateError> {
        let assets = self
            .assets
            .keys()
            .chain(raw_desired.keys())
            .chain(admitted_targets.keys())
            .chain(filled.keys())
            .chain(acknowledged_open.keys())
            .chain(unknown_result.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut replacement = BTreeMap::new();
        for asset in assets {
            let raw = raw_desired.get(&asset).copied().unwrap_or_default();
            let admitted = admitted_targets.get(&asset).copied().unwrap_or_default();
            let filled = filled.get(&asset).copied().unwrap_or_default();
            let open = acknowledged_open.get(&asset).copied().unwrap_or_default();
            let unknown = unknown_result.get(&asset).copied().unwrap_or_default();
            let committed = filled
                .checked_add(open)
                .and_then(|value| value.checked_add(unknown))
                .ok_or(TargetStateError::ArithmeticOverflow("committed exposure"))?;
            let residual = admitted
                .checked_sub(committed)
                .ok_or(TargetStateError::ArithmeticOverflow("target residual"))?;
            replacement.insert(
                asset,
                VirtualTargetState {
                    raw_desired_notional: raw,
                    admitted_target_notional: admitted,
                    filled_notional: filled,
                    acknowledged_open_notional: open,
                    unknown_result_notional: unknown,
                    executable_residual: residual,
                    target_version,
                    latest_source_set_id: source_set_id,
                },
            );
        }
        self.assets = replacement;
        Ok(())
    }

    pub fn get(&self, asset: &str) -> Option<&VirtualTargetState> {
        self.assets.get(asset)
    }

    pub fn assets(&self) -> &BTreeMap<String, VirtualTargetState> {
        &self.assets
    }

    pub fn admitted_targets(&self) -> BTreeMap<String, Decimal> {
        self.assets
            .iter()
            .map(|(asset, state)| (asset.clone(), state.admitted_target_notional))
            .collect()
    }

    pub fn latest_target_version(&self) -> Option<TargetVersion> {
        let mut versions = self.assets.values().map(|state| state.target_version);
        let first = versions.next()?;
        versions.all(|version| version == first).then_some(first)
    }

    pub fn retained_below_minimum(&self, minimum_notional: Decimal) -> BTreeMap<String, Decimal> {
        self.assets
            .iter()
            .filter_map(|(asset, state)| {
                let raw_gap = state.raw_desired_notional.checked_sub(
                    state
                        .filled_notional
                        .checked_add(state.acknowledged_open_notional)?
                        .checked_add(state.unknown_result_notional)?,
                )?;
                (!raw_gap.is_zero() && raw_gap.abs() < minimum_notional)
                    .then(|| (asset.clone(), raw_gap))
            })
            .collect()
    }

    pub fn save_atomic(&self, path: impl AsRef<Path>) -> Result<(), TargetStateError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| TargetStateError::Persistence(error.to_string()))?;
        }
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec(self)
            .map_err(|error| TargetStateError::Persistence(error.to_string()))?;
        std::fs::write(&temporary, bytes)
            .map_err(|error| TargetStateError::Persistence(error.to_string()))?;
        std::fs::rename(&temporary, path)
            .map_err(|error| TargetStateError::Persistence(error.to_string()))
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, TargetStateError> {
        let bytes = std::fs::read(path)
            .map_err(|error| TargetStateError::Persistence(error.to_string()))?;
        let ledger = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| TargetStateError::Persistence(error.to_string()))?;
        if ledger.schema_version != TARGET_LEDGER_SCHEMA_VERSION {
            return Err(TargetStateError::SchemaMismatch);
        }
        Ok(ledger)
    }

    pub fn validate_integrity(&self) -> Result<(), TargetStateError> {
        if self.schema_version != TARGET_LEDGER_SCHEMA_VERSION {
            return Err(TargetStateError::SchemaMismatch);
        }
        for state in self.assets.values() {
            let committed = state
                .filled_notional
                .checked_add(state.acknowledged_open_notional)
                .and_then(|value| value.checked_add(state.unknown_result_notional))
                .ok_or(TargetStateError::ArithmeticOverflow("committed exposure"))?;
            let expected = state
                .admitted_target_notional
                .checked_sub(committed)
                .ok_or(TargetStateError::ArithmeticOverflow("target residual"))?;
            if expected != state.executable_residual {
                return Err(TargetStateError::Persistence(
                    "target residual invariant mismatch".into(),
                ));
            }
        }
        if !self.assets.is_empty() && self.latest_target_version().is_none() {
            return Err(TargetStateError::Persistence(
                "target versions are inconsistent".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::derive_snapshot_set_id;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn source_set() -> SnapshotSetId {
        derive_snapshot_set_id(&[]).unwrap()
    }

    fn replace(ledger: &mut VirtualTargetLedger, desired: &str) {
        ledger
            .replace_absolute_targets(
                &[("ETH".into(), d(desired))].into_iter().collect(),
                &[("ETH".into(), d(desired))].into_iter().collect(),
                &BTreeMap::new(),
                &BTreeMap::new(),
                &BTreeMap::new(),
                TargetVersion(1),
                source_set(),
            )
            .unwrap();
    }

    #[test]
    fn absolute_progression_never_blindly_accumulates() {
        let mut ledger = VirtualTargetLedger::default();
        replace(&mut ledger, "5");
        assert_eq!(ledger.get("ETH").unwrap().executable_residual, d("5"));
        replace(&mut ledger, "9.5");
        assert_eq!(ledger.get("ETH").unwrap().executable_residual, d("9.5"));
        replace(&mut ledger, "12.5");
        assert_eq!(ledger.get("ETH").unwrap().executable_residual, d("12.5"));
    }

    #[test]
    fn target_retreat_replaces_prior_intent() {
        let mut ledger = VirtualTargetLedger::default();
        replace(&mut ledger, "8");
        replace(&mut ledger, "4");
        replace(&mut ledger, "0");
        assert_eq!(ledger.get("ETH").unwrap().raw_desired_notional, d("0"));
        assert_eq!(ledger.get("ETH").unwrap().executable_residual, d("0"));
    }

    #[test]
    fn below_minimum_state_survives_restart() {
        let mut ledger = VirtualTargetLedger::default();
        replace(&mut ledger, "9.5");
        let path = std::env::temp_dir().join(format!(
            "copytrade-target-state-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        ledger.save_atomic(&path).unwrap();
        let loaded = VirtualTargetLedger::load(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(loaded, ledger);
        assert_eq!(loaded.retained_below_minimum(d("12"))["ETH"], d("9.5"));
    }
}
