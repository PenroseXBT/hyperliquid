use crate::domain::configuration::CopyTradeConfig;
use crate::domain::decision::{derive_config_hash, derive_risk_policy_hash, IdentityError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseManifest {
    #[serde(default)]
    pub deployment_scope: String,
    #[serde(default)]
    pub source_tree_clean: bool,
    #[serde(default)]
    pub git_commit_required: bool,
    pub manifest_schema_version: u32,
    pub architecture_version: String,
    pub qualification_stage: String,
    pub git_commit: String,
    pub git_tree_state: String,
    pub source_tree_sha256: String,
    #[serde(default)]
    pub authoritative_source_files: BTreeMap<String, String>,
    pub binary_sha256: String,
    pub configuration_sha256: String,
    pub risk_policy_sha256: String,
    pub configuration_schema_version: u32,
    pub test_report_sha256: String,
    pub risk_policy_version: String,
    pub global_risk_scale: String,
    #[serde(default)]
    pub candidate_configuration_sha256: Option<String>,
    #[serde(default)]
    pub runtime_configuration_sha256: Option<String>,
    #[serde(default)]
    pub observer_binary_sha256: Option<String>,
    #[serde(default)]
    pub signer_binary_sha256: Option<String>,
    #[serde(default)]
    pub market_rule_policy_sha256: Option<String>,
    #[serde(default)]
    pub dynamic_floor_policy_sha256: Option<String>,
    #[serde(default)]
    pub ioc_pricing_policy_sha256: Option<String>,
    #[serde(default)]
    pub signer_protocol_schema_version: Option<u32>,
    #[serde(default)]
    pub signer_state_schema_version: Option<u32>,
    #[serde(default)]
    pub ipc_policy_sha256: Option<String>,
    #[serde(default)]
    pub systemd_units_sha256: Option<String>,
    #[serde(default)]
    pub linux_topology_policy_sha256: Option<String>,
    #[serde(default)]
    pub authorization_schema_version: Option<u32>,
    #[serde(default)]
    pub reconciliation_event_schema_version: Option<u32>,
}

#[derive(Debug)]
pub enum ReleaseError {
    Io(String),
    Identity(IdentityError),
    InvalidGitCommit,
    InvalidSourceTreeHash,
    Serialize(String),
}

impl Display for ReleaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for ReleaseError {}

impl From<IdentityError> for ReleaseError {
    fn from(error: IdentityError) -> Self {
        Self::Identity(error)
    }
}

pub fn create_release_manifest(
    config: &CopyTradeConfig,
    binary_path: impl AsRef<Path>,
    test_report_path: impl AsRef<Path>,
    git_commit: &str,
    git_tree_state: &str,
    source_tree_sha256: &str,
) -> Result<ReleaseManifest, ReleaseError> {
    if git_commit.len() != 40 || !git_commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ReleaseError::InvalidGitCommit);
    }
    if !matches!(git_tree_state, "clean" | "dirty")
        || source_tree_sha256.len() != 64
        || !source_tree_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ReleaseError::InvalidSourceTreeHash);
    }
    Ok(ReleaseManifest {
        manifest_schema_version: 1,
        deployment_scope: "local-only".to_string(),
        source_tree_clean: git_tree_state == "clean",
        git_commit_required: false,
        architecture_version: crate::ENGINE_ARCHITECTURE_VERSION.to_string(),
        qualification_stage: "LOCAL_BUILD".to_string(),
        git_commit: git_commit.to_ascii_lowercase(),
        git_tree_state: git_tree_state.to_string(),
        source_tree_sha256: source_tree_sha256.to_ascii_lowercase(),
        authoritative_source_files: BTreeMap::new(),
        binary_sha256: sha256_file(binary_path)?,
        configuration_sha256: derive_config_hash(config)?.to_hex(),
        risk_policy_sha256: derive_risk_policy_hash(&config.global_risk)?.to_hex(),
        configuration_schema_version: config.schema_version,
        test_report_sha256: sha256_file(test_report_path)?,
        risk_policy_version: "HL1B-RISK-V1".to_string(),
        global_risk_scale: config.global_risk.global_risk_scale.to_string(),
        candidate_configuration_sha256: None,
        runtime_configuration_sha256: None,
        observer_binary_sha256: None,
        signer_binary_sha256: None,
        market_rule_policy_sha256: None,
        dynamic_floor_policy_sha256: None,
        ioc_pricing_policy_sha256: None,
        signer_protocol_schema_version: None,
        signer_state_schema_version: None,
        ipc_policy_sha256: None,
        systemd_units_sha256: None,
        linux_topology_policy_sha256: None,
        authorization_schema_version: None,
        reconciliation_event_schema_version: None,
    })
}

pub fn canonical_manifest_json(manifest: &ReleaseManifest) -> Result<String, ReleaseError> {
    serde_json::to_string_pretty(manifest)
        .map(|mut json| {
            json.push('\n');
            json
        })
        .map_err(|error| ReleaseError::Serialize(error.to_string()))
}

pub fn sha256_file(path: impl AsRef<Path>) -> Result<String, ReleaseError> {
    let bytes = std::fs::read(path).map_err(|error| ReleaseError::Io(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_reproducible_and_rejects_unversioned_commit() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let artifact = root.join("Cargo.toml");
        let first = create_release_manifest(
            &config,
            &artifact,
            &artifact,
            "0123456789abcdef0123456789abcdef01234567",
            "clean",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let second = create_release_manifest(
            &config,
            &artifact,
            &artifact,
            "0123456789abcdef0123456789abcdef01234567",
            "clean",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(
            canonical_manifest_json(&first).unwrap(),
            canonical_manifest_json(&second).unwrap()
        );
        assert!(create_release_manifest(
            &config,
            &artifact,
            &artifact,
            "dirty",
            "clean",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .is_err());
    }
}
