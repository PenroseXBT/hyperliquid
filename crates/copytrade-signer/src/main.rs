#![forbid(unsafe_code)]

use copytrade_core::decision::{ConfigHash, RiskPolicyHash};
use copytrade_core::ipc::{
    read_framed, write_framed, IntentIdentity, IpcPolicy, ObserverRequest, ProcessRole,
    ProductionHandshake, SignerRejection, SignerResponse, VerifiedReconciliationPayload,
    IPC_STATE_SCHEMA_VERSION, OBSERVER_MESSAGE_TYPE, SIGNER_MESSAGE_TYPE,
};
use copytrade_core::live_trading::LiveTradingState;
use copytrade_core::release::{sha256_file, ReleaseManifest};
use copytrade_signer::outbox::DurableReconciliationOutbox;
use copytrade_signer::production::ProductionSigner;
use copytrade_signer::transport::HyperliquidMainnetTransport;
use copytrade_signer::{ApiWalletSecret, SignerError, SubmissionRegistry, SubmissionState};
use rust_decimal::Decimal;
use std::error::Error;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio::net::{UnixListener, UnixStream};

#[derive(Debug)]
struct Arguments {
    socket: PathBuf,
    ipc_policy: PathBuf,
    secret_file: PathBuf,
    master_account: String,
    registry: PathBuf,
    ledger: PathBuf,
    outbox: PathBuf,
    release_manifest: PathBuf,
    initial_equity: Decimal,
    initial_nonce: u64,
    request_timeout_ms: u64,
    expected_observer_uid: u32,
    expected_observer_gid: u32,
    expected_observer_sha256: Option<String>,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_io()
        .enable_time()
        .thread_name("hype-signer")
        .build()
        .unwrap_or_else(|error| {
            eprintln!("copytrade-signer runtime failed closed: {error}");
            std::process::exit(1);
        });
    if let Err(error) = runtime.block_on(run()) {
        eprintln!("copytrade-signer failed closed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let arguments = parse_arguments(std::env::args().skip(1))?;
    let policy: IpcPolicy = serde_json::from_slice(&std::fs::read(&arguments.ipc_policy)?)?;
    policy.validate()?;
    let manifest: ReleaseManifest =
        serde_json::from_slice(&std::fs::read(&arguments.release_manifest)?)?;
    verify_release(&manifest, &std::env::current_exe()?, &arguments.ipc_policy)?;
    let risk_hash = RiskPolicyHash(parse_hash32(&manifest.risk_policy_sha256)?);
    let configuration_hash = ConfigHash(parse_hash32(&manifest.configuration_sha256)?);
    let release_manifest_hash = parse_hash32(&sha256_file(&arguments.release_manifest)?)?;
    let signer_binary_hash = parse_hash32(&sha256_file(std::env::current_exe()?)?)?;
    let ipc_policy_hash = parse_hash32(&sha256_file(&arguments.ipc_policy)?)?;

    let wallet = ApiWalletSecret::load_from_file(&arguments.secret_file).await?;
    let transport =
        HyperliquidMainnetTransport::new(arguments.master_account, arguments.request_timeout_ms)?;
    let registry = if arguments.registry.exists() {
        SubmissionRegistry::restore(&arguments.registry)?
    } else {
        SubmissionRegistry::default()
    };
    let mut live_state = if arguments.ledger.exists() {
        LiveTradingState::load(&arguments.ledger)?
    } else {
        LiveTradingState::new(arguments.initial_equity, current_unix_millis()?)?
    };
    let signer = ProductionSigner::new(
        transport,
        wallet,
        registry,
        &arguments.registry,
        arguments.initial_nonce,
        risk_hash,
        configuration_hash,
        release_manifest_hash,
    )?;
    let mut signer_instance_id = [0u8; 16];
    signer_instance_id.copy_from_slice(&release_manifest_hash[..16]);
    let mut outbox = if arguments.outbox.exists() {
        DurableReconciliationOutbox::load(&arguments.outbox)?
    } else {
        DurableReconciliationOutbox::new(signer_instance_id, policy.maximum_pending_events)?
    };
    signer
        .reconcile_startup(
            &mut live_state,
            &arguments.ledger,
            current_unix_millis()?,
            2,
            1_000,
        )
        .await?;
    sync_outbox(&signer, &live_state, &mut outbox).await?;
    outbox.save_atomic(&arguments.outbox)?;

    let listener = bind_private_socket(&arguments.socket)?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        if let Err(error) = verify_peer(
            &stream,
            arguments.expected_observer_uid,
            arguments.expected_observer_gid,
            arguments.expected_observer_sha256.as_deref(),
        ) {
            eprintln!("rejected signer IPC peer: {error}");
            continue;
        }
        let mut protocol_authenticated = false;
        while let Ok(frame) =
            read_framed::<ObserverRequest, _>(&mut stream, OBSERVER_MESSAGE_TYPE, &policy).await
        {
            let sequence = frame.sequence;
            let response = match frame.message {
                ObserverRequest::Handshake(peer) => {
                    if protocol_authenticated {
                        break;
                    }
                    if peer.process_role != ProcessRole::Observer
                        || peer.protocol_version != policy.protocol_version
                        || peer.state_schema_version != IPC_STATE_SCHEMA_VERSION
                        || peer.source_tree_sha256 != manifest.source_tree_sha256
                        || peer.ipc_policy_hash != ipc_policy_hash
                        || peer.peer_nonce.is_some()
                        || arguments
                            .expected_observer_sha256
                            .as_deref()
                            .is_some_and(|expected| {
                                parse_hash32(expected).ok() != Some(peer.release_binary_hash)
                            })
                    {
                        break;
                    }
                    protocol_authenticated = true;
                    SignerResponse::Handshake(ProductionHandshake {
                        protocol_version: policy.protocol_version,
                        process_role: ProcessRole::Signer,
                        release_binary_hash: signer_binary_hash,
                        source_tree_sha256: manifest.source_tree_sha256.clone(),
                        ipc_policy_hash,
                        state_schema_version: IPC_STATE_SCHEMA_VERSION,
                        nonce: handshake_nonce()?,
                        peer_nonce: Some(peer.nonce),
                    })
                }
                ObserverRequest::Submit(intent) => {
                    if !protocol_authenticated {
                        break;
                    }
                    let cloid = intent.planned_cloid;
                    let identity = intent_identity(&intent);
                    if validate_release_bound_intent(
                        &intent,
                        &manifest,
                        signer_binary_hash,
                        arguments.expected_observer_sha256.as_deref(),
                    )
                    .is_err()
                    {
                        SignerResponse::Rejected {
                            cloid,
                            reason: SignerRejection::PolicyMismatch,
                        }
                    } else {
                        match signer.idempotent_state(&intent).await? {
                            Some(SubmissionState::AuthorizedNotSubmitted) | None => match signer
                                .authorize(intent.clone(), current_unix_millis()?)
                                .await
                            {
                                Ok(_) => {
                                    // Authorization is fsynced before this acknowledgment. If the
                                    // peer disconnects here, no exchange side effect is attempted.
                                    if write_response(
                                        &mut stream,
                                        &policy,
                                        sequence,
                                        &SignerResponse::Accepted {
                                            identity: identity.clone(),
                                            durable_sequence: signer.durable_state(cloid).await?.1,
                                            durable_state: "authorized".into(),
                                        },
                                    )
                                    .await
                                    .is_err()
                                    {
                                        break;
                                    }
                                    if let Err(error) = signer
                                        .submit_authorized(intent, current_unix_millis()?)
                                        .await
                                    {
                                        eprintln!(
                                            "durably recorded signer submission outcome: {error}"
                                        );
                                    }
                                    // The signer alone applies fills/funding and reconciles actual
                                    // positions. Failure leaves readiness false and blocks exposure.
                                    if let Err(error) = signer
                                        .reconcile_startup(
                                            &mut live_state,
                                            &arguments.ledger,
                                            current_unix_millis()?,
                                            2,
                                            1_000,
                                        )
                                        .await
                                    {
                                        eprintln!("post-submit reconciliation required: {error}");
                                    }
                                    sync_outbox(&signer, &live_state, &mut outbox).await?;
                                    outbox.save_atomic(&arguments.outbox)?;
                                    continue;
                                }
                                Err(error) => SignerResponse::Rejected {
                                    cloid,
                                    reason: signer_rejection(&error),
                                },
                            },
                            Some(state) => {
                                let (_, durable_sequence) = signer.durable_state(cloid).await?;
                                SignerResponse::Accepted {
                                    identity,
                                    durable_sequence,
                                    durable_state: format!("{state:?}"),
                                }
                            }
                        }
                    }
                }
                ObserverRequest::Reconcile(cloid) => {
                    if !protocol_authenticated {
                        break;
                    }
                    let state = signer.durable_state(cloid).await.map(|value| value.0);
                    match state {
                        Ok(
                            SubmissionState::UnknownResult { .. }
                            | SubmissionState::Acknowledged { .. }
                            | SubmissionState::PartiallyFilled { .. },
                        ) => match signer
                            .reconcile_unknown(cloid, current_unix_millis()?, 2, 1_000)
                            .await
                        {
                            Ok(_) => {
                                let _ = signer
                                    .reconcile_startup(
                                        &mut live_state,
                                        &arguments.ledger,
                                        current_unix_millis()?,
                                        2,
                                        1_000,
                                    )
                                    .await;
                                let (_, durable_sequence) = signer.durable_state(cloid).await?;
                                SignerResponse::Accepted {
                                    identity: intent_identity_from_durable(cloid, &signer).await?,
                                    durable_sequence,
                                    durable_state: "reconciled".into(),
                                }
                            }
                            Err(error) => SignerResponse::Rejected {
                                cloid,
                                reason: signer_rejection(&error),
                            },
                        },
                        Ok(state) => {
                            let durable_sequence = signer.durable_state(cloid).await?.1;
                            SignerResponse::Accepted {
                                identity: intent_identity_from_durable(cloid, &signer).await?,
                                durable_sequence,
                                durable_state: format!("{state:?}"),
                            }
                        }
                        Err(SignerError::UnknownCloid) => SignerResponse::NotFound { cloid },
                        Err(error) => SignerResponse::Rejected {
                            cloid,
                            reason: signer_rejection(&error),
                        },
                    }
                }
                ObserverRequest::Status => {
                    if !protocol_authenticated {
                        break;
                    }
                    let reconciled = signer.is_ready().await;
                    SignerResponse::Status {
                        ready: reconciled,
                        reconciled,
                        highest_event_sequence: signer.durable_transitions_after(0).await?.len()
                            as u64,
                    }
                }
                ObserverRequest::ReplayFrom {
                    highest_contiguous_sequence,
                } => {
                    if !protocol_authenticated {
                        break;
                    }
                    sync_outbox(&signer, &live_state, &mut outbox).await?;
                    outbox.save_atomic(&arguments.outbox)?;
                    SignerResponse::ReconciliationEvents(
                        outbox.events_after(highest_contiguous_sequence),
                    )
                }
                ObserverRequest::AcknowledgeEvents {
                    highest_contiguous_sequence,
                } => {
                    if !protocol_authenticated {
                        break;
                    }
                    outbox.acknowledge(highest_contiguous_sequence)?;
                    outbox.save_atomic(&arguments.outbox)?;
                    SignerResponse::EventsAcknowledged {
                        highest_contiguous_sequence,
                    }
                }
            };
            if write_response(&mut stream, &policy, sequence, &response)
                .await
                .is_err()
            {
                break;
            }
        }
    }
}

async fn write_response(
    stream: &mut UnixStream,
    policy: &IpcPolicy,
    sequence: u64,
    response: &SignerResponse,
) -> Result<(), copytrade_core::ipc::IpcError> {
    write_framed(stream, SIGNER_MESSAGE_TYPE, sequence, response, policy).await
}

fn bind_private_socket(path: &Path) -> Result<UnixListener, Box<dyn Error>> {
    let mut inherited = listenfd::ListenFd::from_env();
    if let Some(listener) = inherited.take_unix_listener(0)? {
        listener.set_nonblocking(true)?;
        return Ok(UnixListener::from_std(listener)?);
    }
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_socket() {
            return Err("signer socket path exists and is not a socket".into());
        }
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
    Ok(listener)
}

fn verify_peer(
    stream: &UnixStream,
    expected_uid: u32,
    expected_gid: u32,
    expected_executable_sha256: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let credentials = stream.peer_cred()?;
    if credentials.uid() != expected_uid || credentials.gid() != expected_gid {
        return Err("SO_PEERCRED uid/gid mismatch".into());
    }
    #[cfg(target_os = "linux")]
    if let Some(expected) = expected_executable_sha256 {
        use std::os::unix::fs::MetadataExt;
        let pid = credentials.pid().ok_or("peer pid unavailable")?;
        let path = format!("/proc/{pid}/exe");
        let executable = std::fs::OpenOptions::new().read(true).open(&path)?;
        let fd_metadata = executable.metadata()?;
        let proc_metadata = std::fs::metadata(&path)?;
        if fd_metadata.ino() != proc_metadata.ino() || fd_metadata.dev() != proc_metadata.dev() {
            return Err("peer executable identity changed during verification".into());
        }
        if sha256_open_file(&executable)? != expected {
            return Err("peer executable hash mismatch".into());
        }
    }
    #[cfg(not(target_os = "linux"))]
    if expected_executable_sha256.is_some() {
        return Err("peer executable hash enforcement requires Linux".into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn sha256_open_file(file: &std::fs::File) -> Result<String, Box<dyn Error>> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    let mut file = file.try_clone()?;
    file.seek(std::io::SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn handshake_nonce() -> Result<[u8; 32], Box<dyn Error>> {
    use std::io::Read;
    let mut nonce = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    Ok(nonce)
}

fn intent_identity(
    intent: &copytrade_core::authorized_intent::AuthorizedExecutionIntent,
) -> IntentIdentity {
    IntentIdentity {
        planned_cloid: intent.planned_cloid,
        target_version: intent.target_version,
        canonical_intent_hash: intent.canonical_hash,
    }
}

async fn intent_identity_from_durable<
    T: copytrade_signer::transport::AuthenticatedExchangeTransport,
>(
    cloid: copytrade_core::decision::PlannedCloid,
    signer: &ProductionSigner<T>,
) -> Result<IntentIdentity, Box<dyn Error>> {
    signer.intent_identity(cloid).await.map_err(Into::into)
}

fn signer_rejection(error: &SignerError) -> SignerRejection {
    match error {
        SignerError::DuplicateCloid | SignerError::CloidMismatch => SignerRejection::DuplicateCloid,
        SignerError::RegistryCapacityExhausted => SignerRejection::CapacityExhausted,
        SignerError::StartupNotReconciled => SignerRejection::NotReady,
        SignerError::Authorization(reason) if reason.contains("expired") => {
            SignerRejection::Expired
        }
        SignerError::Authorization(reason) if reason.contains("policy") => {
            SignerRejection::PolicyMismatch
        }
        SignerError::Authorization(reason) if reason.contains("projection") => {
            SignerRejection::RiskChanged
        }
        SignerError::Authorization(reason) => SignerRejection::InvalidIntent(reason.clone()),
        other => SignerRejection::Internal(other.to_string()),
    }
}

fn validate_release_bound_intent(
    intent: &copytrade_core::authorized_intent::AuthorizedExecutionIntent,
    manifest: &ReleaseManifest,
    signer_binary_hash: [u8; 32],
    expected_observer_hash: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    intent.validate_canonical()?;
    if intent.signer_release_hash != signer_binary_hash
        || expected_observer_hash
            .map(parse_hash32)
            .transpose()?
            .is_some_and(|expected| intent.observer_release_hash != expected)
        || manifest
            .market_rule_policy_sha256
            .as_deref()
            .map(parse_hash32)
            .transpose()?
            != Some(intent.market_rules_hash)
        || manifest
            .dynamic_floor_policy_sha256
            .as_deref()
            .map(parse_hash32)
            .transpose()?
            != Some(intent.dynamic_floor_policy_hash)
        || manifest
            .ioc_pricing_policy_sha256
            .as_deref()
            .map(parse_hash32)
            .transpose()?
            != Some(intent.ioc_policy_hash)
    {
        return Err("authorized intent release-policy binding mismatch".into());
    }
    Ok(())
}

fn verify_release(
    manifest: &ReleaseManifest,
    executable: &Path,
    ipc_policy: &Path,
) -> Result<(), Box<dyn Error>> {
    if manifest.manifest_schema_version < 2
        || manifest.deployment_scope != "local-only"
        || manifest.source_tree_clean != (manifest.git_tree_state == "clean")
        || manifest.git_commit_required
        || manifest.qualification_stage != "PRODUCTION_RELEASE"
        || manifest.signer_protocol_schema_version != Some(1)
        || manifest.global_risk_scale != "0.1"
        || manifest.signer_binary_sha256.as_deref() != Some(&sha256_file(executable)?)
        || manifest.ipc_policy_sha256.as_deref() != Some(&sha256_file(ipc_policy)?)
    {
        return Err("production release manifest does not match signer artifact/policy".into());
    }
    Ok(())
}

fn parse_arguments(arguments: impl Iterator<Item = String>) -> Result<Arguments, Box<dyn Error>> {
    let mut arguments = arguments;
    if arguments.next().as_deref() != Some("serve") {
        return Err("copytrade-signer requires the serve subcommand".into());
    }
    let mut values = std::collections::BTreeMap::new();
    while let Some(key) = arguments.next() {
        if !key.starts_with("--")
            || values
                .insert(key, arguments.next().ok_or("missing value")?)
                .is_some()
        {
            return Err("invalid or duplicate signer argument".into());
        }
    }
    let take = |key: &str| {
        values
            .get(key)
            .cloned()
            .ok_or_else(|| format!("{key} is required"))
    };
    Ok(Arguments {
        socket: take("--socket")?.into(),
        ipc_policy: take("--ipc-policy")?.into(),
        secret_file: values
            .get("--api-wallet-secret-file")
            .cloned()
            .or_else(|| std::env::var("HYPERLIQUID_API_WALLET_PRIVATE_KEY_FILE").ok())
            .ok_or("API-wallet secret file is required")?
            .into(),
        master_account: values
            .get("--master-account")
            .cloned()
            .or_else(|| std::env::var("HYPERLIQUID_MASTER_ACCOUNT").ok())
            .ok_or("master account is required")?,
        registry: take("--registry")?.into(),
        ledger: take("--ledger")?.into(),
        outbox: take("--outbox")?.into(),
        release_manifest: take("--manifest")?.into(),
        initial_equity: Decimal::from_str(&take("--initial-equity")?)?,
        initial_nonce: take("--initial-nonce")?.parse()?,
        request_timeout_ms: take("--request-timeout-ms")?.parse()?,
        expected_observer_uid: take("--expected-observer-uid")?.parse()?,
        expected_observer_gid: take("--expected-observer-gid")?.parse()?,
        expected_observer_sha256: values.get("--expected-observer-sha256").cloned(),
    })
}

async fn sync_outbox<T: copytrade_signer::transport::AuthenticatedExchangeTransport>(
    signer: &ProductionSigner<T>,
    live: &LiveTradingState,
    outbox: &mut DurableReconciliationOutbox,
) -> Result<(), Box<dyn Error>> {
    for (sequence, transition) in signer.durable_transitions_after(0).await? {
        let cloid = parse_cloid(&transition.cloid)?;
        let payload = match transition.state {
            SubmissionState::Authorized => VerifiedReconciliationPayload::IntentAccepted {
                identity: signer.intent_identity(cloid).await?,
                durable_sequence: sequence,
            },
            SubmissionState::AuthorizedNotSubmitted => {
                VerifiedReconciliationPayload::AuthorizedNotSubmitted { cloid }
            }
            SubmissionState::SubmissionStarted { .. } => {
                VerifiedReconciliationPayload::SubmissionStarted { cloid }
            }
            SubmissionState::UnknownResult { .. } => {
                VerifiedReconciliationPayload::SubmissionUnknown { cloid }
            }
            SubmissionState::Acknowledged { order_id } => {
                VerifiedReconciliationPayload::OrderAcknowledged { cloid, order_id }
            }
            SubmissionState::PartiallyFilled { order_id, filled } => {
                VerifiedReconciliationPayload::OrderPartiallyFilled {
                    cloid,
                    order_id,
                    filled_quantity: filled,
                }
            }
            SubmissionState::Filled { .. } | SubmissionState::ReconciledNotFound => {
                VerifiedReconciliationPayload::Reconciled { cloid }
            }
            SubmissionState::Cancelled { filled, .. } => {
                VerifiedReconciliationPayload::OrderCancelled {
                    cloid,
                    filled_quantity: filled,
                }
            }
            SubmissionState::Rejected { reason } => {
                VerifiedReconciliationPayload::OrderRejected { cloid, reason }
            }
            SubmissionState::NonceAllocated { .. } | SubmissionState::Signed { .. } => continue,
        };
        outbox.append_once(format!("transition:{sequence}"), payload)?;
    }
    for fill in live.verified_fills() {
        outbox.append_once(
            format!(
                "fill:{}:{}",
                fill.identity.exchange_order_id.0, fill.identity.trade_id.0
            ),
            VerifiedReconciliationPayload::Fill(fill.clone()),
        )?;
    }
    for funding in live.verified_funding() {
        outbox.append_once(
            format!("funding:{}", hex_bytes(&funding.event_id.0)),
            VerifiedReconciliationPayload::Funding(funding.clone()),
        )?;
    }
    Ok(())
}

fn parse_cloid(value: &str) -> Result<copytrade_core::decision::PlannedCloid, Box<dyn Error>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 32 {
        return Err("invalid CLOID".into());
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(copytrade_core::decision::PlannedCloid(bytes))
}

fn hex_bytes(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hash32(value: &str) -> Result<[u8; 32], Box<dyn Error>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid risk policy hash".into());
    }
    let mut result = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        result[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(result)
}

fn current_unix_millis() -> Result<u64, Box<dyn Error>> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
