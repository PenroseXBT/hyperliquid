use copytrade_core::configuration::CopyTradeConfig;
use copytrade_core::release::{
    canonical_manifest_json, create_production_release_manifest, ProductionReleaseInputs,
};
use std::error::Error;
use std::path::Path;

fn main() {
    if let Err(error) = run() {
        eprintln!("production manifest generation failed closed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 19 {
        return Err("expected: CONFIG OBSERVER SIGNER TEST_REPORT MARKET_POLICY FLOOR_POLICY IOC_POLICY IPC_POLICY SYSTEMD_HASHES TOPOLOGY_POLICY GIT_COMMIT GIT_TREE_STATE SOURCE_TREE_SHA256 SOURCE_FILE_HASHES RUNTIME_CONFIGURATION PROTOCOL_VERSION STATE_VERSION AUTHORIZATION_VERSION RECONCILIATION_VERSION".into());
    }
    let config = CopyTradeConfig::from_path(&args[0])?;
    let manifest = create_production_release_manifest(
        &config,
        ProductionReleaseInputs {
            observer_binary_path: Path::new(&args[1]),
            signer_binary_path: Path::new(&args[2]),
            test_report_path: Path::new(&args[3]),
            market_rule_policy_path: Path::new(&args[4]),
            dynamic_floor_policy_path: Path::new(&args[5]),
            ioc_pricing_policy_path: Path::new(&args[6]),
            ipc_policy_path: Path::new(&args[7]),
            systemd_units_hash_path: Path::new(&args[8]),
            linux_topology_policy_path: Path::new(&args[9]),
            git_commit: &args[10],
            git_tree_state: &args[11],
            source_tree_sha256: &args[12],
            source_file_hashes_path: Path::new(&args[13]),
            runtime_configuration_path: Path::new(&args[14]),
            signer_protocol_schema_version: args[15].parse()?,
            signer_state_schema_version: args[16].parse()?,
            authorization_schema_version: args[17].parse()?,
            reconciliation_event_schema_version: args[18].parse()?,
        },
    )?;
    print!("{}", canonical_manifest_json(&manifest)?);
    Ok(())
}
