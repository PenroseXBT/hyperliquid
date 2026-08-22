#[cfg(feature = "research-cli")]
use copytrade_core::planning_fixture::{construct_plan_from_fixture, execute_fixture_shadow};
use copytrade_core::release::{canonical_manifest_json, create_release_manifest};
#[cfg(feature = "research-cli")]
use copytrade_core::scheduler::{
    BudgetClass, Clock, MonotonicClock, ReadOnlyDataSource, ReadOnlySchedulerConfig,
    ReadRequestKind, RequestKey, RequestPriority, RequestScheduler, RequestSubject,
    ScheduleOutcome, ScheduledReadRequest,
};
#[cfg(feature = "research-cli")]
use copytrade_observer::public_mainnet::{HyperliquidPublicTransport, PublicTransportPolicy};
#[cfg(feature = "research-cli")]
use copytrade_observer::qualification::{finalize_qualification, run_qualification};
use copytrade_observer::qualification::{
    parse_duration_seconds, run_continuous_daemon, QualificationOptions,
};
#[cfg(feature = "research-cli")]
use copytrade_observer::railway_aggregate::aggregate_railway_window;
#[cfg(feature = "research-cli")]
use copytrade_observer::replay::replay_density_rungs_with_layer;
use copytrade_observer::state_root::unsigned_persistence_schema_sha256;
use copytrade_observer::{ObserverCoreState, FORBIDDEN_DEPENDENCY_PACKAGES};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::process;
#[cfg(feature = "research-cli")]
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
struct Arguments {
    config: PathBuf,
    very_profitable_layer: Option<PathBuf>,
    request_policy: PathBuf,
    fixture: PathBuf,
    test_report: PathBuf,
    git_commit: Option<String>,
    git_tree_state: Option<String>,
    source_tree_sha256: Option<String>,
    transport_policy: PathBuf,
    release_manifest: PathBuf,
    release_binary: Option<PathBuf>,
    isolation_report: PathBuf,
    output: PathBuf,
    state_root: Option<PathBuf>,
    duration_seconds: Option<u64>,
    minimum_observation_seconds: Option<u64>,
    minimum_closed_episodes: Option<usize>,
    frozen_identity_sha256: Option<String>,
    bundle: Option<PathBuf>,
    journal: Option<PathBuf>,
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
    #[cfg(feature = "research-cli")]
    Observe,
    #[cfg(feature = "research-cli")]
    Plan,
    #[cfg(feature = "research-cli")]
    Shadow,
    ReleaseManifest,
    #[cfg(feature = "research-cli")]
    QualifyMainnet,
    #[cfg(feature = "research-cli")]
    QualifyTransport,
    #[cfg(feature = "research-cli")]
    QualifyProfitability,
    #[cfg(feature = "research-cli")]
    QualifyMicroDensity,
    #[cfg(feature = "research-cli")]
    FinalizeQualification,
    #[cfg(feature = "research-cli")]
    AggregateQualification,
    #[cfg(feature = "research-cli")]
    ReplayDensity,
    #[cfg(feature = "research-cli")]
    AuditCandidates,
    Continuous,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("copytrade-observer failed closed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let arguments = parse_arguments(env::args().skip(1))?;
    let state = ObserverCoreState::load_with_very_profitable_layer(
        &arguments.config,
        arguments.very_profitable_layer.as_ref(),
    )?;
    match arguments.operation {
        Operation::Initialize => println!(
            "HL1 signer-free observer initialized: {}",
            state.build_manifest()
        ),
        Operation::ValidateConfig => println!(
            "observer configuration valid: schema={} candidates={} config={}",
            state.config().schema_version,
            state.config().candidates.len(),
            state.build_manifest().configuration_fingerprint
        ),
        Operation::PrintEffectiveConfig => {
            println!("{}", serde_json::to_string_pretty(state.config())?)
        }
        Operation::PrintBuildManifest => println!("{}", state.build_manifest()),
        Operation::CheckDependencyPolicy => println!(
            "observer dependency denylist loaded: {}",
            FORBIDDEN_DEPENDENCY_PACKAGES.join(",")
        ),
        Operation::PrintPersistenceSchemaHash => {
            println!("{}", unsigned_persistence_schema_sha256())
        }
        #[cfg(feature = "research-cli")]
        Operation::Observe => run_read_only_observation(&state, &arguments.request_policy)?,
        #[cfg(feature = "research-cli")]
        Operation::Plan => run_inert_plan(&state, &arguments.fixture)?,
        #[cfg(feature = "research-cli")]
        Operation::Shadow => run_deterministic_shadow(&state, &arguments.fixture)?,
        Operation::ReleaseManifest => run_release_manifest(
            &state,
            &arguments.test_report,
            arguments.git_commit.as_deref(),
            arguments.git_tree_state.as_deref(),
            arguments.source_tree_sha256.as_deref(),
            arguments.release_binary.as_deref(),
        )?,
        #[cfg(feature = "research-cli")]
        Operation::QualifyMainnet => {
            run_qualification(QualificationOptions {
                config_path: arguments.config,
                very_profitable_layer_path: arguments.very_profitable_layer,
                request_policy_path: arguments.request_policy,
                transport_policy_path: arguments.transport_policy,
                release_manifest_path: arguments.release_manifest,
                isolation_report_path: arguments.isolation_report,
                output: arguments.output,
                state_root: arguments.state_root,
                duration_seconds: arguments.duration_seconds.ok_or("--duration is required")?,
                transport_gate: false,
                profitability_gate: false,
                micro_density_gate: false,
                continuous: false,
            })
            .await?;
        }
        #[cfg(feature = "research-cli")]
        Operation::QualifyTransport => {
            run_qualification(QualificationOptions {
                config_path: arguments.config,
                very_profitable_layer_path: arguments.very_profitable_layer,
                request_policy_path: arguments.request_policy,
                transport_policy_path: arguments.transport_policy,
                release_manifest_path: arguments.release_manifest,
                isolation_report_path: arguments.isolation_report,
                output: arguments.output,
                state_root: arguments.state_root,
                duration_seconds: arguments.duration_seconds.ok_or("--duration is required")?,
                transport_gate: true,
                profitability_gate: false,
                micro_density_gate: false,
                continuous: false,
            })
            .await?;
        }
        #[cfg(feature = "research-cli")]
        Operation::QualifyProfitability => {
            run_qualification(QualificationOptions {
                config_path: arguments.config,
                very_profitable_layer_path: arguments.very_profitable_layer,
                request_policy_path: arguments.request_policy,
                transport_policy_path: arguments.transport_policy,
                release_manifest_path: arguments.release_manifest,
                isolation_report_path: arguments.isolation_report,
                output: arguments.output,
                state_root: arguments.state_root,
                duration_seconds: arguments.duration_seconds.ok_or("--duration is required")?,
                transport_gate: false,
                profitability_gate: true,
                micro_density_gate: false,
                continuous: false,
            })
            .await?;
        }
        #[cfg(feature = "research-cli")]
        Operation::QualifyMicroDensity => {
            run_qualification(QualificationOptions {
                config_path: arguments.config,
                very_profitable_layer_path: arguments.very_profitable_layer,
                request_policy_path: arguments.request_policy,
                transport_policy_path: arguments.transport_policy,
                release_manifest_path: arguments.release_manifest,
                isolation_report_path: arguments.isolation_report,
                output: arguments.output,
                state_root: arguments.state_root,
                duration_seconds: arguments.duration_seconds.ok_or("--duration is required")?,
                transport_gate: false,
                profitability_gate: false,
                micro_density_gate: true,
                continuous: false,
            })
            .await?;
        }
        #[cfg(feature = "research-cli")]
        Operation::FinalizeQualification => {
            let summary = finalize_qualification(
                &arguments.bundle.ok_or("--bundle is required")?,
                &arguments.isolation_report,
            )?;
            println!(
                "hl1k_passed={} elapsed_seconds={}",
                summary.passed, summary.monotonic_elapsed_seconds
            );
        }
        #[cfg(feature = "research-cli")]
        Operation::AggregateQualification => {
            let summary = aggregate_railway_window(
                &arguments.bundle.ok_or("--bundle is required")?,
                &arguments.output,
                arguments
                    .frozen_identity_sha256
                    .as_deref()
                    .ok_or("--frozen-identity-sha256 is required")?,
                arguments
                    .minimum_observation_seconds
                    .ok_or("--minimum-observation-seconds is required")?,
                arguments
                    .minimum_closed_episodes
                    .ok_or("--minimum-closed-episodes is required")?,
            )?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        #[cfg(feature = "research-cli")]
        Operation::ReplayDensity => {
            let report = replay_density_rungs_with_layer(
                &arguments.config,
                arguments.very_profitable_layer.as_deref(),
                &arguments.transport_policy,
                arguments.journal.as_ref().ok_or("--journal is required")?,
            )?;
            if let Some(parent) = arguments.output.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut bytes = serde_json::to_vec_pretty(&report)?;
            bytes.push(b'\n');
            std::fs::write(&arguments.output, bytes)?;
            println!(
                "P4H.4 unsigned replay written: {}",
                arguments.output.display()
            );
        }
        #[cfg(feature = "research-cli")]
        Operation::AuditCandidates => {
            run_candidate_audit(&state, &arguments.transport_policy, &arguments.output).await?
        }
        Operation::Continuous => {
            run_continuous_daemon(QualificationOptions {
                config_path: arguments.config,
                very_profitable_layer_path: arguments.very_profitable_layer,
                request_policy_path: arguments.request_policy,
                transport_policy_path: arguments.transport_policy,
                release_manifest_path: arguments.release_manifest,
                isolation_report_path: arguments.isolation_report,
                output: arguments.output,
                state_root: arguments.state_root,
                duration_seconds: 0,
                transport_gate: false,
                profitability_gate: false,
                micro_density_gate: false,
                continuous: true,
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
    let mut fixture = PathBuf::from("fixtures/accepted-snapshots.json");
    let mut test_report = PathBuf::from("target/hl1j-test-report.txt");
    let mut git_commit = None;
    let mut git_tree_state = None;
    let mut source_tree_sha256 = None;
    let mut transport_policy = PathBuf::from("config/public-mainnet-transport.json");
    let mut release_manifest = PathBuf::from("target/hl1j/release-manifest.json");
    let mut release_binary = None;
    let mut isolation_report = PathBuf::from("target/hl1c/isolation-report.txt");
    let mut output = PathBuf::from("target/hl1k");
    let mut state_root = None;
    let mut duration_seconds = None;
    let mut minimum_observation_seconds = None;
    let mut minimum_closed_episodes = None;
    let mut frozen_identity_sha256 = None;
    let mut bundle = None;
    let mut journal = None;
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
            "--fixture" => {
                let path = arguments.next().ok_or("--fixture requires a path")?;
                fixture = PathBuf::from(path);
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
                )
            }
            "--release-manifest" => {
                release_manifest = PathBuf::from(
                    arguments
                        .next()
                        .ok_or("--release-manifest requires a path")?,
                )
            }
            "--release-binary" => {
                release_binary = Some(PathBuf::from(
                    arguments.next().ok_or("--release-binary requires a path")?,
                ))
            }
            "--isolation-report" => {
                isolation_report = PathBuf::from(
                    arguments
                        .next()
                        .ok_or("--isolation-report requires a path")?,
                )
            }
            "--output" => {
                output = PathBuf::from(arguments.next().ok_or("--output requires a path")?)
            }
            "--state-root" => {
                state_root = Some(PathBuf::from(
                    arguments.next().ok_or("--state-root requires a path")?,
                ))
            }
            "--duration" => {
                duration_seconds = Some(parse_duration_seconds(
                    &arguments.next().ok_or("--duration requires a value")?,
                )?)
            }
            "--minimum-observation-seconds" => {
                minimum_observation_seconds = Some(
                    arguments
                        .next()
                        .ok_or("--minimum-observation-seconds requires a value")?
                        .parse()?,
                )
            }
            "--minimum-closed-episodes" => {
                minimum_closed_episodes = Some(
                    arguments
                        .next()
                        .ok_or("--minimum-closed-episodes requires a value")?
                        .parse()?,
                )
            }
            "--frozen-identity-sha256" => {
                frozen_identity_sha256 = Some(
                    arguments
                        .next()
                        .ok_or("--frozen-identity-sha256 requires a value")?,
                )
            }
            "--bundle" => {
                bundle = Some(PathBuf::from(
                    arguments.next().ok_or("--bundle requires a path")?,
                ))
            }
            "--journal" => {
                journal = Some(PathBuf::from(
                    arguments.next().ok_or("--journal requires a path")?,
                ))
            }
            #[cfg(feature = "research-cli")]
            "observe" => set_operation(&mut operation, Operation::Observe)?,
            #[cfg(feature = "research-cli")]
            "plan" => set_operation(&mut operation, Operation::Plan)?,
            #[cfg(feature = "research-cli")]
            "shadow" => set_operation(&mut operation, Operation::Shadow)?,
            "release-manifest" => set_operation(&mut operation, Operation::ReleaseManifest)?,
            #[cfg(feature = "research-cli")]
            "qualify-mainnet" => set_operation(&mut operation, Operation::QualifyMainnet)?,
            #[cfg(feature = "research-cli")]
            "qualify-transport" => set_operation(&mut operation, Operation::QualifyTransport)?,
            #[cfg(feature = "research-cli")]
            "qualify-profitability" => {
                set_operation(&mut operation, Operation::QualifyProfitability)?
            }
            #[cfg(feature = "research-cli")]
            "qualify-micro-density" => {
                set_operation(&mut operation, Operation::QualifyMicroDensity)?
            }
            #[cfg(feature = "research-cli")]
            "finalize-qualification" => {
                set_operation(&mut operation, Operation::FinalizeQualification)?
            }
            #[cfg(feature = "research-cli")]
            "aggregate-qualification" => {
                set_operation(&mut operation, Operation::AggregateQualification)?
            }
            #[cfg(feature = "research-cli")]
            "replay-density" => set_operation(&mut operation, Operation::ReplayDensity)?,
            #[cfg(feature = "research-cli")]
            "audit-candidates" => set_operation(&mut operation, Operation::AuditCandidates)?,
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
            _ => return Err(format!("unsupported observer argument: {argument}").into()),
        }
    }
    if release_binary.is_some() && operation != Operation::ReleaseManifest {
        return Err("--release-binary is supported only by release-manifest".into());
    }
    Ok(Arguments {
        config,
        very_profitable_layer,
        request_policy,
        fixture,
        test_report,
        git_commit,
        git_tree_state,
        source_tree_sha256,
        transport_policy,
        release_manifest,
        release_binary,
        isolation_report,
        output,
        state_root,
        duration_seconds,
        minimum_observation_seconds,
        minimum_closed_episodes,
        frozen_identity_sha256,
        bundle,
        journal,
        operation,
    })
}

fn run_release_manifest(
    state: &ObserverCoreState,
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

#[cfg(feature = "research-cli")]
fn run_inert_plan(state: &ObserverCoreState, fixture_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let plan = construct_plan_from_fixture(state.config(), fixture_path)?;
    println!(
        "planned_only=true submission_capable=false decision_id={} snapshot_set_id={} target_version={} actions={}",
        plan.decision.decision_id,
        plan.decision.snapshot_set_id,
        plan.decision.target_version.0,
        plan.actions.len()
    );
    for action in plan.actions {
        println!(
            "planned_action ordinal={} asset={} side={:?} notional={} reduce_only={} retry_generation={} cloid={}",
            action.action_ordinal,
            action.asset,
            action.side,
            action.rounded_notional,
            action.reduce_only,
            action.retry_generation,
            action.planned_cloid
        );
    }
    Ok(())
}

#[cfg(feature = "research-cli")]
fn run_deterministic_shadow(
    state: &ObserverCoreState,
    fixture_path: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let plan = construct_plan_from_fixture(state.config(), fixture_path)?;
    let executions = execute_fixture_shadow(&plan, state.config().taker_fee_bps)?;
    println!(
        "shadow_only=true planned_only=true submission_capable=false scenario=expected executions={}",
        executions.len()
    );
    for execution in executions {
        println!(
            "shadow_execution id={} asset={} filled_quantity={} average_price={} unfilled_ioc={} fees={} funding={} slippage={}",
            execution.shadow_execution_id,
            execution.action.asset,
            execution.modeled_filled_quantity,
            execution
                .modeled_average_fill_price
                .map_or_else(|| "none".to_string(), |value| value.to_string()),
            execution.unfilled_ioc_remainder,
            execution.fees,
            execution.funding,
            execution.slippage
        );
    }
    Ok(())
}

fn set_operation(current: &mut Operation, requested: Operation) -> Result<(), Box<dyn Error>> {
    if *current != Operation::Initialize {
        return Err("only one observer operation may be selected".into());
    }
    *current = requested;
    Ok(())
}

#[cfg(feature = "research-cli")]
fn run_read_only_observation(
    state: &ObserverCoreState,
    policy_path: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let scheduler_config = ReadOnlySchedulerConfig::from_path(policy_path)?;
    let clock = MonotonicClock::default();
    let scheduler = RequestScheduler::new(
        clock.clone(),
        scheduler_config.api.clone(),
        scheduler_config.retry,
    )?;
    let created_at = clock.now_ms();
    let expires_at = created_at
        .checked_add(scheduler_config.freshness.configured_max_age_ms)
        .ok_or("source request expiration overflow")?;
    let mut enqueued = 0_usize;
    let mut rejected = 0_usize;
    for candidate in state
        .config()
        .candidates
        .iter()
        .filter(|candidate| candidate.enabled)
    {
        let request = scheduler_config.api.request(
            RequestKey {
                subject: RequestSubject::Candidate(candidate.address.clone()),
                kind: ReadRequestKind::SourceState,
            },
            RequestPriority::Normal,
            BudgetClass::SourcePolling,
            Some(copytrade_core::scheduler::SourceTier::Inactive),
            created_at,
            created_at,
            expires_at,
            0,
        )?;
        match scheduler.schedule(request) {
            ScheduleOutcome::Enqueued => enqueued += 1,
            _ => rejected += 1,
        }
    }
    let health = scheduler.health();
    println!(
        "HL1D read-only scheduler initialized: candidates={} enqueued={} rejected={} pending={} capacity={} normal_weight={} reserved_weight={} concurrency={}",
        state.config().candidates.len(),
        enqueued,
        rejected,
        health.pending,
        health.queue_capacity,
        health.available_normal_weight,
        health.available_total_weight.saturating_sub(health.available_normal_weight),
        health.available_concurrency
    );
    Ok(())
}

#[cfg(feature = "research-cli")]
async fn run_candidate_audit(
    state: &ObserverCoreState,
    transport_policy_path: &PathBuf,
    output: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let policy: PublicTransportPolicy =
        serde_json::from_slice(&std::fs::read(transport_policy_path)?)?;
    let clock = MonotonicClock::default();
    let transport = HyperliquidPublicTransport::new(clock.clone(), policy)
        .map_err(|error| format!("public transport: {error:?}"))?;
    let now = clock.now_ms();
    let metadata = ScheduledReadRequest {
        key: RequestKey {
            subject: RequestSubject::Market,
            kind: ReadRequestKind::ExchangeMetadata,
        },
        kind: ReadRequestKind::ExchangeMetadata,
        candidate_id: None,
        source_tier: None,
        weight: 20,
        priority: RequestPriority::High,
        budget_class: BudgetClass::Normal,
        created_at: now,
        not_before: now,
        expires_at: now.checked_add(30_000).ok_or("audit expiry overflow")?,
        attempt: 0,
    };
    transport
        .execute(metadata.clone())
        .await
        .map_err(|error| format!("metadata audit request: {error:?}"))?;
    transport
        .take_accepted(&metadata)
        .ok_or("metadata audit response missing")?;

    let mut tasks = tokio::task::JoinSet::new();
    let concurrency = Arc::new(tokio::sync::Semaphore::new(32));
    for candidate in state
        .config()
        .candidates
        .iter()
        .filter(|value| value.enabled)
    {
        let transport = transport.clone();
        let concurrency = Arc::clone(&concurrency);
        let candidate = candidate.address.to_ascii_lowercase();
        let created_at = clock.now_ms();
        tasks.spawn(async move {
            let _permit = concurrency
                .acquire_owned()
                .await
                .map_err(|_| copytrade_core::scheduler::ReadFailure::Permanent)?;
            let request = ScheduledReadRequest {
                key: RequestKey {
                    subject: RequestSubject::Candidate(candidate.clone()),
                    kind: ReadRequestKind::SourceState,
                },
                kind: ReadRequestKind::SourceState,
                candidate_id: Some(candidate),
                source_tier: Some(copytrade_core::scheduler::SourceTier::Inactive),
                weight: 2,
                priority: RequestPriority::Normal,
                budget_class: BudgetClass::SourcePolling,
                created_at,
                not_before: created_at,
                expires_at: created_at.saturating_add(30_000),
                attempt: 0,
            };
            let result = transport.execute(request.clone()).await;
            if result.is_ok() {
                let _ = transport.take_accepted(&request);
            }
            result
        });
    }
    while let Some(result) = tasks.join_next().await {
        let _ = result?;
    }
    let audit = transport.candidate_audit_snapshot();
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut bytes = serde_json::to_vec_pretty(&audit)?;
    bytes.push(b'\n');
    std::fs::write(output, bytes)?;
    println!(
        "candidate_audit_complete=true configured={} validated={} rejected={} output={}",
        audit.len(),
        audit.values().filter(|value| value.ever_validated).count(),
        audit.values().filter(|value| !value.ever_validated).count(),
        output.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observer_cli_contains_only_read_only_operations() {
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
        assert_eq!(
            parsed.fixture,
            PathBuf::from("fixtures/accepted-snapshots.json")
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
    #[cfg(feature = "research-cli")]
    fn observer_accepts_only_modeled_read_only_observation() {
        let parsed = parse_arguments(
            [
                "observe".to_string(),
                "--request-policy".to_string(),
                "policy.json".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::Observe);
        assert_eq!(parsed.request_policy, PathBuf::from("policy.json"));
    }

    #[test]
    #[cfg(feature = "research-cli")]
    fn profitability_parser_accepts_persistent_state_root() {
        let parsed = parse_arguments(
            [
                "qualify-profitability".to_string(),
                "--state-root".to_string(),
                "/data/su6-forward/state".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::QualifyProfitability);
        assert_eq!(
            parsed.state_root,
            Some(PathBuf::from("/data/su6-forward/state"))
        );
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
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::Continuous);
        assert_eq!(parsed.duration_seconds, None);
        assert_eq!(
            parsed.state_root,
            Some(PathBuf::from("/data/su6-continuous/state"))
        );
    }

    #[test]
    #[cfg(feature = "research-cli")]
    fn observer_accepts_inert_fixture_planning() {
        let parsed = parse_arguments(
            [
                "plan".to_string(),
                "--fixture".to_string(),
                "fixture.json".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::Plan);
        assert_eq!(parsed.fixture, PathBuf::from("fixture.json"));
    }

    #[test]
    #[cfg(feature = "research-cli")]
    fn observer_accepts_deterministic_shadow_fixture() {
        let parsed = parse_arguments(["shadow".to_string()].into_iter()).unwrap();
        assert_eq!(parsed.operation, Operation::Shadow);
    }

    #[test]
    #[cfg(feature = "research-cli")]
    fn observer_accepts_only_recorded_public_density_replay() {
        let parsed = parse_arguments(
            [
                "replay-density".to_string(),
                "--journal".to_string(),
                "recorded-public-events.jsonl".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::ReplayDensity);
        assert_eq!(
            parsed.journal,
            Some(PathBuf::from("recorded-public-events.jsonl"))
        );
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
                "target/linux-release/copytrade-observer".to_string(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.operation, Operation::ReleaseManifest);
        assert_eq!(
            parsed.release_binary,
            Some(PathBuf::from("target/linux-release/copytrade-observer"))
        );

        assert!(parse_arguments(
            [
                "qualify-profitability".to_string(),
                "--release-binary".to_string(),
                "target/linux-release/copytrade-observer".to_string(),
            ]
            .into_iter(),
        )
        .is_err());
    }
}
