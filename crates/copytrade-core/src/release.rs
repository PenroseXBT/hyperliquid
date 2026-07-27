use crate::configuration::CopyTradeConfig;
use crate::decision::{derive_config_hash, derive_risk_policy_hash, IdentityError};
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
        architecture_version: "HL1C-1".to_string(),
        qualification_stage: "HL1J".to_string(),
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

pub struct ProductionReleaseInputs<'a> {
    pub observer_binary_path: &'a Path,
    pub signer_binary_path: &'a Path,
    pub test_report_path: &'a Path,
    pub market_rule_policy_path: &'a Path,
    pub dynamic_floor_policy_path: &'a Path,
    pub ioc_pricing_policy_path: &'a Path,
    pub ipc_policy_path: &'a Path,
    pub systemd_units_hash_path: &'a Path,
    pub linux_topology_policy_path: &'a Path,
    pub git_commit: &'a str,
    pub git_tree_state: &'a str,
    pub source_tree_sha256: &'a str,
    pub source_file_hashes_path: &'a Path,
    pub runtime_configuration_path: &'a Path,
    pub signer_protocol_schema_version: u32,
    pub signer_state_schema_version: u32,
    pub authorization_schema_version: u32,
    pub reconciliation_event_schema_version: u32,
}

pub fn create_production_release_manifest(
    config: &CopyTradeConfig,
    input: ProductionReleaseInputs<'_>,
) -> Result<ReleaseManifest, ReleaseError> {
    if input.signer_protocol_schema_version == 0
        || input.signer_state_schema_version == 0
        || input.authorization_schema_version == 0
        || input.reconciliation_event_schema_version == 0
    {
        return Err(ReleaseError::InvalidSourceTreeHash);
    }
    let mut manifest = create_release_manifest(
        config,
        input.observer_binary_path,
        input.test_report_path,
        input.git_commit,
        input.git_tree_state,
        input.source_tree_sha256,
    )?;
    manifest.manifest_schema_version = 2;
    manifest.architecture_version = "COPYTRADE-PRODUCTION-V1".into();
    manifest.qualification_stage = "PRODUCTION_RELEASE".into();
    manifest.authoritative_source_files = parse_source_file_hashes(input.source_file_hashes_path)?;
    manifest.candidate_configuration_sha256 = Some(manifest.configuration_sha256.clone());
    manifest.runtime_configuration_sha256 = Some(sha256_file(input.runtime_configuration_path)?);
    manifest.observer_binary_sha256 = Some(manifest.binary_sha256.clone());
    manifest.signer_binary_sha256 = Some(sha256_file(input.signer_binary_path)?);
    manifest.market_rule_policy_sha256 = Some(sha256_file(input.market_rule_policy_path)?);
    manifest.dynamic_floor_policy_sha256 = Some(sha256_file(input.dynamic_floor_policy_path)?);
    manifest.ioc_pricing_policy_sha256 = Some(sha256_file(input.ioc_pricing_policy_path)?);
    manifest.signer_protocol_schema_version = Some(input.signer_protocol_schema_version);
    manifest.signer_state_schema_version = Some(input.signer_state_schema_version);
    manifest.ipc_policy_sha256 = Some(sha256_file(input.ipc_policy_path)?);
    manifest.systemd_units_sha256 = Some(sha256_file(input.systemd_units_hash_path)?);
    manifest.linux_topology_policy_sha256 = Some(sha256_file(input.linux_topology_policy_path)?);
    manifest.authorization_schema_version = Some(input.authorization_schema_version);
    manifest.reconciliation_event_schema_version = Some(input.reconciliation_event_schema_version);
    Ok(manifest)
}

fn parse_source_file_hashes(path: &Path) -> Result<BTreeMap<String, String>, ReleaseError> {
    let text =
        std::fs::read_to_string(path).map_err(|error| ReleaseError::Io(error.to_string()))?;
    let mut files = BTreeMap::new();
    for line in text.lines() {
        let (hash, file) = line
            .split_once("  ")
            .ok_or(ReleaseError::InvalidSourceTreeHash)?;
        if hash.len() != 64
            || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || file.is_empty()
            || files
                .insert(file.to_string(), hash.to_ascii_lowercase())
                .is_some()
        {
            return Err(ReleaseError::InvalidSourceTreeHash);
        }
    }
    if files.is_empty() {
        return Err(ReleaseError::InvalidSourceTreeHash);
    }
    Ok(files)
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
        let artifact = root.join("fixtures/hl1e-replay-v1.json");
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

    #[test]
    fn production_manifest_allows_dirty_local_tree_and_binds_binaries_sources_and_policies() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        let artifact = root.join("fixtures/hl1e-replay-v1.json");
        let source_hashes = root.join("fixtures/source-file-hashes-v1.txt");
        let policy = root.join("policy/production-ioc-pricing-v1.json");
        let manifest = create_production_release_manifest(
            &config,
            ProductionReleaseInputs {
                observer_binary_path: &artifact,
                signer_binary_path: &artifact,
                test_report_path: &artifact,
                market_rule_policy_path: &policy,
                dynamic_floor_policy_path: &policy,
                ioc_pricing_policy_path: &policy,
                ipc_policy_path: &policy,
                systemd_units_hash_path: &policy,
                linux_topology_policy_path: &policy,
                git_commit: "0123456789abcdef0123456789abcdef01234567",
                git_tree_state: "clean",
                source_tree_sha256:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                source_file_hashes_path: &source_hashes,
                runtime_configuration_path: &policy,
                signer_protocol_schema_version: 1,
                signer_state_schema_version: 1,
                authorization_schema_version: 1,
                reconciliation_event_schema_version: 1,
            },
        )
        .unwrap();
        assert_eq!(manifest.manifest_schema_version, 2);
        assert_eq!(manifest.global_risk_scale, "0.1");
        assert_eq!(manifest.signer_protocol_schema_version, Some(1));
        assert_eq!(manifest.signer_state_schema_version, Some(1));
        assert!(manifest.ipc_policy_sha256.is_some());
        assert!(manifest.signer_binary_sha256.is_some());
        let dirty = create_production_release_manifest(
            &config,
            ProductionReleaseInputs {
                observer_binary_path: &artifact,
                signer_binary_path: &artifact,
                test_report_path: &artifact,
                market_rule_policy_path: &policy,
                dynamic_floor_policy_path: &policy,
                ioc_pricing_policy_path: &policy,
                ipc_policy_path: &policy,
                systemd_units_hash_path: &policy,
                linux_topology_policy_path: &policy,
                git_commit: "0123456789abcdef0123456789abcdef01234567",
                git_tree_state: "dirty",
                source_tree_sha256:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                source_file_hashes_path: &source_hashes,
                runtime_configuration_path: &policy,
                signer_protocol_schema_version: 1,
                signer_state_schema_version: 1,
                authorization_schema_version: 1,
                reconciliation_event_schema_version: 1,
            },
        )
        .unwrap();
        assert_eq!(dirty.deployment_scope, "local-only");
        assert!(!dirty.source_tree_clean);
        assert!(!dirty.git_commit_required);
    }
}
