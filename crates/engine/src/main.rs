use engine::domain::release::{canonical_manifest_json, create_release_manifest};
use engine::runtime::{run_continuous_daemon, RuntimeOptions};
use engine::state_root::unsigned_persistence_schema_sha256;
use engine::{EngineState, FORBIDDEN_DEPENDENCY_PACKAGES};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::process;
#[derive(Debug, PartialEq, Eq)]
struct Arguments {
    config: PathBuf,
    very_profitable_layer: Option<PathBuf>,
    request_policy: PathBuf,
    test_report: PathBuf,
    git_commit: Option<String>,
    git_tree_state: Option<String>,
    source_tree_sha256: Option<String>,
    transport_policy: PathBuf,
    release_binary: Option<PathBuf>,
    output: PathBuf,
    state_root: Option<PathBuf>,
    source_backfill_from: Option<PathBuf>,
    operation: Operation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Initialize,
    ValidateConfig,
    PrintEffectiveConfig,
    PrintBuildManifest,
    CheckDependencyPolicy,
    PrintPersistenceSchemaHash,
    ReleaseManifest,
    Continuous,
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("engine failed closed: {error}");
        process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn Error>> {
    let arguments = parse_arguments(env::args().skip(1))?;
    let state = EngineState::load_with_very_profitable_layer(
        &arguments.config,
        arguments.very_profitable_layer.as_ref(),
    )?;
    match arguments.operation {
        Operation::Initialize => {
            println!("engine initialized: {}", state.build_manifest())
        }
        Operation::ValidateConfig => {
            println!(
                "engine configuration valid: schema={} candidates={} config={}",
                state.config().schema_version,
                state.config().candidates.len(),
                state.build_manifest().configuration_fingerprint
            )
        }
        Operation::PrintEffectiveConfig => {
            println!("{}", serde_json::to_string_pretty(state.config())?)
        }
        Operation::PrintBuildManifest => println!("{}", state.build_manifest()),
        Operation::CheckDependencyPolicy => {
            println!(
                "engine dependency denylist loaded: {}",
                FORBIDDEN_DEPENDENCY_PACKAGES.join(",")
            )
        }
        Operation::PrintPersistenceSchemaHash => {
            println!("{}", unsigned_persistence_schema_sha256())
        }
        Operation::ReleaseManifest => run_release_manifest(
            &state,
            &arguments.test_report,
            arguments.git_commit.as_deref(),
            arguments.git_tree_state.as_deref(),
            arguments.source_tree_sha256.as_deref(),
            arguments.release_binary.as_deref(),
        )?,
        Operation::Continuous => {
            run_continuous_daemon(RuntimeOptions {
                config_path: arguments.config,
                very_profitable_layer_path: arguments.very_profitable_layer,
                request_policy_path: arguments.request_policy,
                transport_policy_path: arguments.transport_policy,
                output: arguments.output,
                state_root: arguments.state_root,
                source_backfill_from: arguments.source_backfill_from,
            })
            .await?;
        }
    }
    Ok(())
}
fn parse_arguments(arguments: impl Iterator<Item = String>) -> Result<Arguments, Box<dyn Error>> {
    let mut config = PathBuf::from("config/copytrade.json");
    let mut very_profitable_layer = None;
    let mut request_policy = PathBuf::from("config/read-api-policy.json");
    let mut test_report = PathBuf::from("target/production-release/test-report.txt");
    let mut git_commit = None;
    let mut git_tree_state = None;
    let mut source_tree_sha256 = None;
    let mut transport_policy = PathBuf::from("config/public-mainnet-transport.json");
    let mut release_binary = None;
    let mut output = PathBuf::from("target/engine");
    let mut state_root = None;
    let mut source_backfill_from = None;
    let mut operation = Operation::Initialize;
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                let path = arguments.next().ok_or("--config requires a path")?;
                config = PathBuf::from(path);
            }
            "--very-profitable-layer" => {
                very_profitable_layer = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or("--very-profitable-layer requires a path")?,
                ));
            }
            "--request-policy" => {
                let path = arguments.next().ok_or("--request-policy requires a path")?;
                request_policy = PathBuf::from(path);
            }

            "--test-report" => {
                let path = arguments.next().ok_or("--test-report requires a path")?;
                test_report = PathBuf::from(path);
            }
            "--git-commit" => {
                git_commit = Some(arguments.next().ok_or("--git-commit requires a value")?);
            }
            "--git-tree-state" => {
                git_tree_state = Some(
                    arguments
                        .next()
                        .ok_or("--git-tree-state requires a value")?,
                );
            }
            "--source-tree-sha256" => {
                source_tree_sha256 = Some(
                    arguments
                        .next()
                        .ok_or("--source-tree-sha256 requires a value")?,
                );
            }
            "--transport-policy" => {
                transport_policy = PathBuf::from(
                    arguments
                        .next()
                        .ok_or("--transport-policy requires a path")?,
                );
            }

            "--release-binary" => {
                release_binary = Some(PathBuf::from(
                    arguments.next().ok_or("--release-binary requires a path")?,
                ));
            }

            "--output" => {
                output = PathBuf::from(arguments.next().ok_or("--output requires a path")?);
            }
            "--state-root" => {
                state_root = Some(PathBuf::from(
                    arguments.next().ok_or("--state-root requires a path")?,
                ));
            }
            "--source-backfill-from" => {
                source_backfill_from = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or("--source-backfill-from requires a path")?,
                ));
            }
            "release-manifest" => set_operation(&mut operation, Operation::ReleaseManifest)?,
            "continuous" => set_operation(&mut operation, Operation::Continuous)?,
            "--validate-config" => set_operation(&mut operation, Operation::ValidateConfig)?,
            "--print-effective-config" => {
                set_operation(&mut operation, Operation::PrintEffectiveConfig)?
            }
            "--print-build-manifest" => {
                set_operation(&mut operation, Operation::PrintBuildManifest)?
            }
            "--check-dependency-policy" => {
                set_operation(&mut operation, Operation::CheckDependencyPolicy)?
            }
            "--print-persistence-schema-hash" => {
                set_operation(&mut operation, Operation::PrintPersistenceSchemaHash)?
            }
            _ => return Err(format!("unsupported engine argument: {argument}").into()),
        }
    }
    if release_binary.is_some() && operation != Operation::ReleaseManifest {
        return Err("--release-binary is supported only by release-manifest".into());
    }
    if source_backfill_from.is_some() && operation != Operation::Continuous {
        return Err("--source-backfill-from is supported only by continuous".into());
    }
    Ok(Arguments {
        config,
        very_profitable_layer,
        request_policy,

        test_report,
        git_commit,
        git_tree_state,
        source_tree_sha256,
        transport_policy,

        release_binary,

        output,
        state_root,
        source_backfill_from,
        operation,
    })
}
fn run_release_manifest(
    state: &EngineState,
    test_report: &PathBuf,
    git_commit: Option<&str>,
    git_tree_state: Option<&str>,
    source_tree_sha256: Option<&str>,
    release_binary: Option<&std::path::Path>,
) -> Result<(), Box<dyn Error>> {
    let git_commit = git_commit.ok_or("--git-commit is required for release-manifest")?;
    let git_tree_state =
        git_tree_state.ok_or("--git-tree-state is required for release-manifest")?;
    let source_tree_sha256 =
        source_tree_sha256.ok_or("--source-tree-sha256 is required for release-manifest")?;
    let executable = match release_binary {
        Some(path) => path.to_path_buf(),
        None => std::env::current_exe()?,
    };
    let manifest = create_release_manifest(
        state.config(),
        executable,
        test_report,
        git_commit,
        git_tree_state,
        source_tree_sha256,
    )?;
    print!("{}", canonical_manifest_json(&manifest)?);
    Ok(())
}
fn set_operation(current: &mut Operation, requested: Operation) -> Result<(), Box<dyn Error>> {
    if *current != Operation::Initialize {
        return Err("only one engine operation may be selected".into());
    }
    *current = requested;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn engine_cli_defaults_to_read_only_operations() {
        for forbidden in [
            "--approve-agent",
            "--copytrade",
            "--submit-order",
            "--cancel-order",
            "--withdraw",
            "--transfer",
            "--private-key",
            "--key-file",
        ] {
            assert!(parse_arguments([forbidden.to_string()].into_iter()).is_err());
        }
    }
    #[test]
    fn parser_accepts_explicit_config_and_validation() {
        let parsed = parse_arguments(
            [
                "--config".to_string(),
                "config/copytrade.json".to_string(),
                "--validate-config".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::ValidateConfig);
        assert_eq!(
            parsed.request_policy,
            PathBuf::from("config/read-api-policy.json")
        );
        assert!(parsed.git_commit.is_none());
        assert!(parsed.git_tree_state.is_none());
        assert!(parsed.source_tree_sha256.is_none());
    }
    #[test]
    fn parser_accepts_versioned_very_profitable_layer() {
        let parsed = parse_arguments(
            [
                "--very-profitable-layer".to_string(),
                "data/very-profitable-layer.json".to_string(),
                "--validate-config".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(
            parsed.very_profitable_layer,
            Some(PathBuf::from("data/very-profitable-layer.json"))
        );
    }
    #[test]
    fn parser_accepts_effective_config_export_for_release_tooling() {
        let parsed = parse_arguments(["--print-effective-config".to_string()].into_iter()).unwrap();
        assert_eq!(parsed.operation, Operation::PrintEffectiveConfig);
    }
    #[test]
    fn continuous_parser_accepts_state_root_without_a_duration() {
        let parsed = parse_arguments(
            [
                "continuous".to_string(),
                "--state-root".to_string(),
                "/data/su6-continuous/state".to_string(),
                "--output".to_string(),
                "/data/su6-continuous/runtime".to_string(),
                "--source-backfill-from".to_string(),
                "/data/system-v1/source-state.sqlite".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::Continuous);
        assert_eq!(
            parsed.state_root,
            Some(PathBuf::from("/data/su6-continuous/state"))
        );
        assert_eq!(
            parsed.source_backfill_from,
            Some(PathBuf::from("/data/system-v1/source-state.sqlite"))
        );
        assert!(parse_arguments(
            [
                "--validate-config".to_string(),
                "--source-backfill-from".to_string(),
                "/data/system-v1/source-state.sqlite".to_string(),
            ]
            .into_iter(),
        )
        .is_err());
    }
    #[test]
    fn release_manifest_requires_explicit_commit() {
        let parsed = parse_arguments(
            [
                "release-manifest".to_string(),
                "--git-commit".to_string(),
                "0123456789abcdef0123456789abcdef01234567".to_string(),
                "--git-tree-state".to_string(),
                "dirty".to_string(),
                "--source-tree-sha256".to_string(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::ReleaseManifest);
        assert!(parsed.git_commit.is_some());
        assert_eq!(parsed.git_tree_state.as_deref(), Some("dirty"));
        assert!(parsed.source_tree_sha256.is_some());
        assert!(parsed.release_binary.is_none());
    }
    #[test]
    fn release_manifest_accepts_only_an_explicit_release_binary_override() {
        let parsed = parse_arguments(
            [
                "release-manifest".to_string(),
                "--release-binary".to_string(),
                "target/linux-release/engine".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::ReleaseManifest);
        assert_eq!(
            parsed.release_binary,
            Some(PathBuf::from("target/linux-release/engine"))
        );
        assert!(parse_arguments(
            [
                "qualify-profitability".to_string(),
                "--release-binary".to_string(),
                "target/linux-release/engine".to_string(),
            ]
            .into_iter(),
        )
        .is_err());
    }
}
