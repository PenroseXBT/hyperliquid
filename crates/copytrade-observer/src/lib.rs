#![forbid(unsafe_code)]

pub mod cohort_layer;
pub mod ingestion;
pub mod ipc_client;
pub mod live_shadow;
pub mod mfce;
pub mod production_state;
pub mod profitability;
pub mod public_mainnet;
pub mod qualification;
pub mod qualification_evidence;
pub mod railway_aggregate;
// The legacy observer-side reconciliation journal is intentionally not part of
// production. The signer owns the sole authoritative fill/funding ledger.
pub mod replay;
pub mod state_root;

use cohort_layer::{PreparedVeryProfitableLayer, VeryProfitableLayerArtifact};
use copytrade_core::CopyTradeConfig;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::Path;

#[cfg(doctest)]
mod compile_fail_proofs;

pub use copytrade_core::portfolio_risk::{
    project_and_validate_portfolio, Asset, ExposureRange, MarketRules, OpenOrderExposure,
    OpenOrderLifecycle, OrderSide, PortfolioProjection, PortfolioProjectionInput, RiskViolation,
    SignedNotional,
};

pub const OBSERVER_ARCHITECTURE_VERSION: &str = "HL1C-1";
pub const FORBIDDEN_DEPENDENCY_PACKAGES: &[&str] = &[
    "hyperliquid_rust_sdk",
    "ethers",
    "ethers-signers",
    "dotenv",
    "k256",
    "secp256k1",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverBuildManifest {
    pub architecture_version: &'static str,
    pub package_name: &'static str,
    pub package_version: &'static str,
    pub configuration_schema_version: u32,
    pub candidate_count: usize,
    pub configuration_fingerprint: String,
}

impl Display for ObserverBuildManifest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "architecture={} package={} version={} schema={} candidates={} config={}",
            self.architecture_version,
            self.package_name,
            self.package_version,
            self.configuration_schema_version,
            self.candidate_count,
            self.configuration_fingerprint
        )
    }
}

#[derive(Debug)]
pub struct ObserverCoreState {
    config: CopyTradeConfig,
    build_manifest: ObserverBuildManifest,
    very_profitable_layer: Option<PreparedVeryProfitableLayer>,
}

impl ObserverCoreState {
    pub fn load(config_path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        Self::load_with_very_profitable_layer(config_path, None::<&Path>)
    }

    pub fn load_with_very_profitable_layer(
        config_path: impl AsRef<Path>,
        layer_path: Option<impl AsRef<Path>>,
    ) -> Result<Self, Box<dyn Error>> {
        let mut config = CopyTradeConfig::from_path(config_path)?;
        let very_profitable_layer = match layer_path {
            Some(path) => {
                let artifact = VeryProfitableLayerArtifact::from_path(path)?;
                let prepared = PreparedVeryProfitableLayer::prepare(&artifact, &config)?;
                prepared.merge_candidates(&mut config)?;
                Some(prepared)
            }
            None if config.very_profitable_layer.is_some() => {
                return Err(
                    "configuration binds a very_profitable layer but no artifact was supplied"
                        .into(),
                );
            }
            None => None,
        };
        let build_manifest = ObserverBuildManifest {
            architecture_version: OBSERVER_ARCHITECTURE_VERSION,
            package_name: env!("CARGO_PKG_NAME"),
            package_version: env!("CARGO_PKG_VERSION"),
            configuration_schema_version: config.schema_version,
            candidate_count: config.candidates.len(),
            configuration_fingerprint: config.deterministic_fingerprint()?,
        };
        Ok(Self {
            config,
            build_manifest,
            very_profitable_layer,
        })
    }

    pub fn config(&self) -> &CopyTradeConfig {
        &self.config
    }

    pub fn build_manifest(&self) -> &ObserverBuildManifest {
        &self.build_manifest
    }

    pub fn very_profitable_layer(&self) -> Option<&PreparedVeryProfitableLayer> {
        self.very_profitable_layer.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json")
    }

    #[test]
    fn observer_loads_validated_core_state_without_runtime_clients() {
        let state = ObserverCoreState::load(config_path()).unwrap();
        assert_eq!(state.config().candidates.len(), 169);
        assert_eq!(
            state.build_manifest().architecture_version,
            OBSERVER_ARCHITECTURE_VERSION
        );
    }

    #[test]
    fn manifest_is_reproducible_for_the_same_configuration() {
        let first = ObserverCoreState::load(config_path()).unwrap();
        let second = ObserverCoreState::load(config_path()).unwrap();
        assert_eq!(first.build_manifest(), second.build_manifest());
    }

    #[test]
    fn hl1b_projection_boundary_is_reachable_without_execution_types() {
        let _boundary: fn(&PortfolioProjectionInput) -> Result<PortfolioProjection, RiskViolation> =
            project_and_validate_portfolio;
    }
}
