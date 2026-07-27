//! Durable, signer-free observer handoff over a private Unix-domain socket.

use copytrade_core::authorized_intent::AuthorizedExecutionIntent;
use copytrade_core::decision::PlannedCloid;
use copytrade_core::ipc::{
    encode_frame, read_framed, write_framed, IpcError, IpcPolicy, ObserverRequest, ProcessRole,
    ProductionHandshake, SignerResponse, VerifiedReconciliationEvent, IPC_STATE_SCHEMA_VERSION,
    OBSERVER_MESSAGE_TYPE, SIGNER_MESSAGE_TYPE,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

const HANDOFF_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverIntentState {
    Planned,
    Persisted,
    Sent,
    AcceptedBySigner,
    SubmissionUnknown,
    AcknowledgedByExchange,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected { reason: String },
    Superseded,
    Reconciled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingHandoff {
    pub intent: AuthorizedExecutionIntent,
    pub state: ObserverIntentState,
    pub observer_sequence: u64,
    pub canonical_frame: Vec<u8>,
    pub canonical_frame_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableHandoffQueue {
    schema_version: u32,
    maximum_pending_intents: usize,
    #[serde(default)]
    maximum_pending_risk_reducing: usize,
    #[serde(default)]
    maximum_pending_exposure_increasing: usize,
    next_sequence: u64,
    by_cloid: BTreeMap<PlannedCloid, PendingHandoff>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ObserverIpcError {
    Io(String),
    Codec(IpcError),
    CapacityExhausted,
    DuplicateConflict,
    UnknownCloid,
    UnexpectedResponse,
    PeerMismatch,
    HandshakeMismatch,
    InvalidState,
    SequenceOverflow,
}

impl Display for ObserverIpcError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for ObserverIpcError {}
impl From<IpcError> for ObserverIpcError {
    fn from(error: IpcError) -> Self {
        Self::Codec(error)
    }
}

impl DurableHandoffQueue {
    pub fn new(maximum_pending_intents: usize) -> Result<Self, ObserverIpcError> {
        if maximum_pending_intents == 0 {
            return Err(ObserverIpcError::CapacityExhausted);
        }
        Ok(Self {
            schema_version: HANDOFF_SCHEMA_VERSION,
            maximum_pending_intents,
            maximum_pending_risk_reducing: maximum_pending_intents,
            maximum_pending_exposure_increasing: maximum_pending_intents,
            next_sequence: 1,
            by_cloid: BTreeMap::new(),
        })
    }

    pub fn with_lanes(
        maximum_pending_risk_reducing: usize,
        maximum_pending_exposure_increasing: usize,
    ) -> Result<Self, ObserverIpcError> {
        let maximum_pending_intents = maximum_pending_risk_reducing
            .checked_add(maximum_pending_exposure_increasing)
            .ok_or(ObserverIpcError::CapacityExhausted)?;
        if maximum_pending_risk_reducing == 0 || maximum_pending_exposure_increasing == 0 {
            return Err(ObserverIpcError::CapacityExhausted);
        }
        Ok(Self {
            schema_version: HANDOFF_SCHEMA_VERSION,
            maximum_pending_intents,
            maximum_pending_risk_reducing,
            maximum_pending_exposure_increasing,
            next_sequence: 1,
            by_cloid: BTreeMap::new(),
        })
    }

    pub fn restore(path: &Path, expected_capacity: usize) -> Result<Self, ObserverIpcError> {
        let bytes = std::fs::read(path).map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        let state: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        if state.schema_version != HANDOFF_SCHEMA_VERSION
            || state.maximum_pending_intents != expected_capacity
            || state.by_cloid.len() > state.maximum_pending_intents
            || state.next_sequence == 0
        {
            return Err(ObserverIpcError::InvalidState);
        }
        Ok(state)
    }

    pub fn enqueue(&mut self, intent: AuthorizedExecutionIntent) -> Result<u64, ObserverIpcError> {
        let cloid = intent.planned_cloid;
        if let Some(existing) = self.by_cloid.get(&cloid) {
            return if existing.intent == intent {
                Ok(existing.observer_sequence)
            } else {
                Err(ObserverIpcError::DuplicateConflict)
            };
        }
        intent
            .validate_canonical()
            .map_err(|_| ObserverIpcError::DuplicateConflict)?;
        if self.unresolved_len() >= self.maximum_pending_intents {
            return Err(ObserverIpcError::CapacityExhausted);
        }
        let lane_count = self
            .by_cloid
            .values()
            .filter(|entry| {
                entry.intent.reduce_only == intent.reduce_only
                    && !matches!(
                        entry.state,
                        ObserverIntentState::Rejected { .. }
                            | ObserverIntentState::Superseded
                            | ObserverIntentState::Reconciled
                    )
            })
            .count();
        let lane_capacity = if intent.reduce_only {
            self.maximum_pending_risk_reducing
        } else {
            self.maximum_pending_exposure_increasing
        };
        if lane_count >= lane_capacity {
            return Err(ObserverIpcError::CapacityExhausted);
        }
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(ObserverIpcError::SequenceOverflow)?;
        let request = ObserverRequest::Submit(intent.clone());
        let canonical_frame =
            encode_frame(OBSERVER_MESSAGE_TYPE, sequence, &request, 16 * 1024 * 1024)?;
        let canonical_frame_hash = copytrade_core::ipc::canonical_payload_hash(&request)?;
        self.by_cloid.insert(
            cloid,
            PendingHandoff {
                intent,
                state: ObserverIntentState::Persisted,
                observer_sequence: sequence,
                canonical_frame,
                canonical_frame_hash,
            },
        );
        Ok(sequence)
    }

    pub fn mark_disconnect(&mut self, cloid: PlannedCloid) -> Result<(), ObserverIpcError> {
        let pending = self
            .by_cloid
            .get_mut(&cloid)
            .ok_or(ObserverIpcError::UnknownCloid)?;
        if matches!(pending.state, ObserverIntentState::Sent) {
            pending.state = ObserverIntentState::SubmissionUnknown;
        }
        Ok(())
    }

    pub fn mark_accepted(&mut self, cloid: PlannedCloid) -> Result<(), ObserverIpcError> {
        let pending = self
            .by_cloid
            .get_mut(&cloid)
            .ok_or(ObserverIpcError::UnknownCloid)?;
        pending.state = ObserverIntentState::AcceptedBySigner;
        Ok(())
    }

    pub fn mark_rejected(
        &mut self,
        cloid: PlannedCloid,
        reason: String,
    ) -> Result<(), ObserverIpcError> {
        self.by_cloid
            .get_mut(&cloid)
            .ok_or(ObserverIpcError::UnknownCloid)?
            .state = ObserverIntentState::Rejected { reason };
        Ok(())
    }

    pub fn pending(&self, cloid: PlannedCloid) -> Option<&PendingHandoff> {
        self.by_cloid.get(&cloid)
    }

    pub fn unresolved_len(&self) -> usize {
        self.by_cloid
            .values()
            .filter(|entry| {
                !matches!(
                    entry.state,
                    ObserverIntentState::AcceptedBySigner
                        | ObserverIntentState::Rejected { .. }
                        | ObserverIntentState::Superseded
                        | ObserverIntentState::Reconciled
                )
            })
            .count()
    }

    pub fn persist(&self, path: &Path) -> Result<(), ObserverIpcError> {
        persist_json_fsync(path, self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentDispatchState {
    AcceptedBySigner { durable_sequence: u64 },
    RetainedForReplay,
    CapacityUnavailableTargetRetained,
    Rejected { reason: String },
}

#[derive(Debug)]
pub enum IntentDispatchError {
    Queue(ObserverIpcError),
}

impl Display for IntentDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for IntentDispatchError {}

/// Durable observer-to-signer handoff. Delivery is at least once; the stable
/// CLOID and canonical intent hash provide exactly-once effects.
pub struct ProductionIntentDispatcher {
    queue: tokio::sync::Mutex<DurableHandoffQueue>,
    queue_path: PathBuf,
    client: tokio::sync::Mutex<ObserverSignerClient>,
}

#[derive(Clone)]
pub struct ProductionDispatchHandle {
    dispatcher: std::sync::Arc<ProductionIntentDispatcher>,
    wake: std::sync::Arc<tokio::sync::Notify>,
}

impl ProductionIntentDispatcher {
    pub fn new(
        queue: DurableHandoffQueue,
        queue_path: PathBuf,
        client: ObserverSignerClient,
    ) -> Self {
        Self {
            queue: tokio::sync::Mutex::new(queue),
            queue_path,
            client: tokio::sync::Mutex::new(client),
        }
    }

    pub async fn persist_and_dispatch_intent(
        &self,
        intent: AuthorizedExecutionIntent,
    ) -> Result<IntentDispatchState, IntentDispatchError> {
        let cloid = self.persist_intent(intent).await?;
        self.dispatch_persisted(cloid).await
    }

    pub fn spawn_bounded_worker(self: &std::sync::Arc<Self>) -> ProductionDispatchHandle {
        let wake = std::sync::Arc::new(tokio::sync::Notify::new());
        let worker = self.clone();
        let worker_wake = wake.clone();
        tokio::spawn(async move {
            loop {
                while let Some(cloid) = worker.next_dispatchable().await {
                    let _ = worker.dispatch_persisted(cloid).await;
                }
                tokio::select! {
                    _ = worker_wake.notified() => {},
                    _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {},
                }
            }
        });
        ProductionDispatchHandle {
            dispatcher: self.clone(),
            wake,
        }
    }

    async fn persist_intent(
        &self,
        intent: AuthorizedExecutionIntent,
    ) -> Result<PlannedCloid, IntentDispatchError> {
        intent
            .validate_canonical()
            .map_err(|_| IntentDispatchError::Queue(ObserverIpcError::DuplicateConflict))?;
        let cloid = intent.planned_cloid;
        let mut queue = self.queue.lock().await;
        queue.enqueue(intent)?;
        // The exact framed bytes are already retained in the queue. fsync the
        // complete queue before the first socket write.
        queue.persist(&self.queue_path)?;
        Ok(cloid)
    }

    async fn next_dispatchable(&self) -> Option<PlannedCloid> {
        let queue = self.queue.lock().await;
        queue
            .by_cloid
            .iter()
            .filter(|(_, pending)| {
                matches!(
                    pending.state,
                    ObserverIntentState::Persisted
                        | ObserverIntentState::Sent
                        | ObserverIntentState::SubmissionUnknown
                )
            })
            .min_by_key(|(_, pending)| (!pending.intent.reduce_only, pending.observer_sequence))
            .map(|(cloid, _)| *cloid)
    }

    async fn dispatch_persisted(
        &self,
        cloid: PlannedCloid,
    ) -> Result<IntentDispatchState, IntentDispatchError> {
        let mut queue = self.queue.lock().await;
        let mut client = self.client.lock().await;
        match client.handoff(&mut queue, &self.queue_path, cloid).await {
            Ok(SignerResponse::Accepted {
                durable_sequence, ..
            }) => Ok(IntentDispatchState::AcceptedBySigner { durable_sequence }),
            Ok(SignerResponse::Rejected { reason, .. }) => Ok(IntentDispatchState::Rejected {
                reason: format!("{reason:?}"),
            }),
            Ok(_) => Err(IntentDispatchError::Queue(
                ObserverIpcError::UnexpectedResponse,
            )),
            Err(error) => {
                let _ = queue.mark_disconnect(cloid);
                let _ = queue.persist(&self.queue_path);
                let _ = error;
                Ok(IntentDispatchState::RetainedForReplay)
            }
        }
    }

    pub async fn reconcile_into(
        &self,
        state: &mut crate::production_state::ProductionTradingState,
        state_path: &Path,
    ) -> Result<u64, IntentDispatchError> {
        let mut client = self.client.lock().await;
        let events = client
            .replay_events(state.highest_contiguous_signer_sequence)
            .await?;
        for event in events {
            let authorized_not_submitted = match &event.payload {
                copytrade_core::ipc::VerifiedReconciliationPayload::AuthorizedNotSubmitted {
                    cloid,
                } => Some(*cloid),
                _ => None,
            };
            crate::production_state::apply_reconciliation_event(state, event).map_err(|error| {
                IntentDispatchError::Queue(ObserverIpcError::Io(error.to_string()))
            })?;
            state.save_atomic(state_path).map_err(|error| {
                IntentDispatchError::Queue(ObserverIpcError::Io(error.to_string()))
            })?;
            if let Some(cloid) = authorized_not_submitted {
                let mut queue = self.queue.lock().await;
                let pending = queue
                    .by_cloid
                    .get_mut(&cloid)
                    .ok_or(IntentDispatchError::Queue(ObserverIpcError::UnknownCloid))?;
                pending.state = ObserverIntentState::Persisted;
                queue.persist(&self.queue_path)?;
            }
        }
        let highest = state.highest_contiguous_signer_sequence;
        client.acknowledge_events(highest).await?;
        Ok(highest)
    }
}

impl ProductionDispatchHandle {
    pub async fn persist_and_queue(
        &self,
        intent: AuthorizedExecutionIntent,
    ) -> Result<IntentDispatchState, IntentDispatchError> {
        let reduce_only = intent.reduce_only;
        match self.dispatcher.persist_intent(intent).await {
            Ok(_) => {}
            Err(IntentDispatchError::Queue(ObserverIpcError::CapacityExhausted))
                if !reduce_only =>
            {
                return Ok(IntentDispatchState::CapacityUnavailableTargetRetained)
            }
            Err(error) => return Err(error),
        }
        self.wake.notify_one();
        Ok(IntentDispatchState::RetainedForReplay)
    }
}

impl From<ObserverIpcError> for IntentDispatchError {
    fn from(value: ObserverIpcError) -> Self {
        Self::Queue(value)
    }
}

pub struct ObserverSignerClient {
    stream: UnixStream,
    policy: IpcPolicy,
    expected_signer_uid: Option<u32>,
    expected_signer_gid: Option<u32>,
    verified_peer: Option<VerifiedPeerIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPeerIdentity {
    pub pid: Option<i32>,
    pub uid: u32,
    pub gid: u32,
    pub executable_hash: Option<[u8; 32]>,
}

impl ObserverSignerClient {
    pub async fn connect(
        path: impl AsRef<Path>,
        policy: IpcPolicy,
        expected_signer_uid: Option<u32>,
        expected_signer_gid: Option<u32>,
    ) -> Result<Self, ObserverIpcError> {
        policy.validate()?;
        let stream = UnixStream::connect(path)
            .await
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        let credentials = stream
            .peer_cred()
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        if expected_signer_uid.is_some_and(|uid| credentials.uid() != uid)
            || expected_signer_gid.is_some_and(|gid| credentials.gid() != gid)
        {
            return Err(ObserverIpcError::PeerMismatch);
        }
        Ok(Self {
            stream,
            policy,
            expected_signer_uid,
            expected_signer_gid,
            verified_peer: None,
        })
    }

    pub async fn connect_authenticated(
        path: impl AsRef<Path>,
        policy: IpcPolicy,
        expected_signer_uid: u32,
        expected_signer_gid: u32,
        expected_signer_hash: [u8; 32],
        local_handshake: ProductionHandshake,
    ) -> Result<Self, ObserverIpcError> {
        let mut client = Self::connect(
            path,
            policy,
            Some(expected_signer_uid),
            Some(expected_signer_gid),
        )
        .await?;
        let credentials = client
            .stream
            .peer_cred()
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        let executable_hash = verify_peer_executable(credentials.pid(), expected_signer_hash)?;
        client.verified_peer = Some(VerifiedPeerIdentity {
            pid: credentials.pid(),
            uid: credentials.uid(),
            gid: credentials.gid(),
            executable_hash: Some(executable_hash),
        });
        let nonce = local_handshake.nonce;
        write_framed(
            &mut client.stream,
            OBSERVER_MESSAGE_TYPE,
            0,
            &ObserverRequest::Handshake(local_handshake.clone()),
            &client.policy,
        )
        .await?;
        let response = read_framed::<SignerResponse, _>(
            &mut client.stream,
            SIGNER_MESSAGE_TYPE,
            &client.policy,
        )
        .await?;
        match response.message {
            SignerResponse::Handshake(peer)
                if peer.process_role == ProcessRole::Signer
                    && peer.protocol_version == client.policy.protocol_version
                    && peer.state_schema_version == IPC_STATE_SCHEMA_VERSION
                    && peer.release_binary_hash == expected_signer_hash
                    && peer.source_tree_sha256 == local_handshake.source_tree_sha256
                    && peer.ipc_policy_hash == local_handshake.ipc_policy_hash
                    && peer.peer_nonce == Some(nonce) =>
            {
                Ok(client)
            }
            _ => Err(ObserverIpcError::HandshakeMismatch),
        }
    }

    pub async fn handoff(
        &mut self,
        queue: &mut DurableHandoffQueue,
        queue_path: &Path,
        cloid: PlannedCloid,
    ) -> Result<SignerResponse, ObserverIpcError> {
        let pending = queue
            .pending(cloid)
            .ok_or(ObserverIpcError::UnknownCloid)?
            .clone();
        if matches!(pending.state, ObserverIntentState::SubmissionUnknown) {
            write_framed(
                &mut self.stream,
                OBSERVER_MESSAGE_TYPE,
                pending.observer_sequence,
                &ObserverRequest::Reconcile(cloid),
                &self.policy,
            )
            .await?;
        } else if matches!(
            pending.state,
            ObserverIntentState::Persisted | ObserverIntentState::Sent
        ) {
            self.stream
                .write_all(&pending.canonical_frame)
                .await
                .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
            queue.by_cloid.get_mut(&cloid).unwrap().state = ObserverIntentState::Sent;
            queue.persist(queue_path)?;
        } else {
            return Err(ObserverIpcError::InvalidState);
        }
        let mut reply = tokio::time::timeout(
            std::time::Duration::from_millis(self.policy.acknowledgment_timeout_ms),
            read_framed::<SignerResponse, _>(&mut self.stream, SIGNER_MESSAGE_TYPE, &self.policy),
        )
        .await
        .map_err(|_| ObserverIpcError::Codec(IpcError::Timeout))??
        .message;
        if matches!(reply, SignerResponse::NotFound { cloid: response } if response == cloid)
            && matches!(pending.state, ObserverIntentState::SubmissionUnknown)
        {
            // Reconciliation proved the original CLOID absent. Replay the
            // exact fsynced frame; never mint a replacement identity.
            self.stream
                .write_all(&pending.canonical_frame)
                .await
                .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
            reply = tokio::time::timeout(
                std::time::Duration::from_millis(self.policy.acknowledgment_timeout_ms),
                read_framed::<SignerResponse, _>(
                    &mut self.stream,
                    SIGNER_MESSAGE_TYPE,
                    &self.policy,
                ),
            )
            .await
            .map_err(|_| ObserverIpcError::Codec(IpcError::Timeout))??
            .message;
        }
        match &reply {
            SignerResponse::Accepted { identity, .. }
                if identity.planned_cloid == cloid
                    && identity.target_version == pending.intent.target_version
                    && identity.canonical_intent_hash == pending.intent.canonical_hash =>
            {
                queue.mark_accepted(cloid)?;
            }
            SignerResponse::Rejected {
                cloid: response,
                reason,
            } if *response == cloid => {
                queue.mark_rejected(cloid, format!("{reason:?}"))?;
            }
            _ => return Err(ObserverIpcError::UnexpectedResponse),
        }
        queue.persist(queue_path)?;
        Ok(reply)
    }

    pub async fn status(&mut self, sequence: u64) -> Result<(bool, bool), ObserverIpcError> {
        write_framed(
            &mut self.stream,
            OBSERVER_MESSAGE_TYPE,
            sequence,
            &ObserverRequest::Status,
            &self.policy,
        )
        .await?;
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(self.policy.acknowledgment_timeout_ms),
            read_framed::<SignerResponse, _>(&mut self.stream, SIGNER_MESSAGE_TYPE, &self.policy),
        )
        .await
        .map_err(|_| ObserverIpcError::Codec(IpcError::Timeout))??;
        match response.message {
            SignerResponse::Status {
                ready, reconciled, ..
            } => Ok((ready, reconciled)),
            _ => Err(ObserverIpcError::UnexpectedResponse),
        }
    }

    pub async fn replay_events(
        &mut self,
        highest_contiguous_sequence: u64,
    ) -> Result<Vec<VerifiedReconciliationEvent>, ObserverIpcError> {
        write_framed(
            &mut self.stream,
            OBSERVER_MESSAGE_TYPE,
            highest_contiguous_sequence,
            &ObserverRequest::ReplayFrom {
                highest_contiguous_sequence,
            },
            &self.policy,
        )
        .await?;
        match read_framed::<SignerResponse, _>(&mut self.stream, SIGNER_MESSAGE_TYPE, &self.policy)
            .await?
            .message
        {
            SignerResponse::ReconciliationEvents(events) => Ok(events),
            _ => Err(ObserverIpcError::UnexpectedResponse),
        }
    }

    pub async fn acknowledge_events(
        &mut self,
        highest_contiguous_sequence: u64,
    ) -> Result<(), ObserverIpcError> {
        write_framed(
            &mut self.stream,
            OBSERVER_MESSAGE_TYPE,
            highest_contiguous_sequence,
            &ObserverRequest::AcknowledgeEvents {
                highest_contiguous_sequence,
            },
            &self.policy,
        )
        .await?;
        match read_framed::<SignerResponse, _>(&mut self.stream, SIGNER_MESSAGE_TYPE, &self.policy)
            .await?
            .message
        {
            SignerResponse::EventsAcknowledged {
                highest_contiguous_sequence: value,
            } if value == highest_contiguous_sequence => Ok(()),
            _ => Err(ObserverIpcError::UnexpectedResponse),
        }
    }

    pub fn configured_peer(&self) -> (Option<u32>, Option<u32>) {
        (self.expected_signer_uid, self.expected_signer_gid)
    }
}

fn verify_peer_executable(
    pid: Option<i32>,
    expected_hash: [u8; 32],
) -> Result<[u8; 32], ObserverIpcError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let pid = pid.ok_or(ObserverIpcError::PeerMismatch)?;
        let path = PathBuf::from(format!("/proc/{pid}/exe"));
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        let fd_metadata = file
            .metadata()
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        let proc_metadata =
            std::fs::metadata(&path).map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        if fd_metadata.ino() != proc_metadata.ino() || fd_metadata.dev() != proc_metadata.dev() {
            return Err(ObserverIpcError::PeerMismatch);
        }
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(0))
            .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
        let mut hasher = sha2::Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = std::io::Read::read(&mut file, &mut buffer)
                .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
            if read == 0 {
                break;
            }
            use sha2::Digest;
            hasher.update(&buffer[..read]);
        }
        use sha2::Digest;
        let actual: [u8; 32] = hasher.finalize().into();
        if actual != expected_hash {
            return Err(ObserverIpcError::PeerMismatch);
        }
        Ok(actual)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, expected_hash);
        Err(ObserverIpcError::PeerMismatch)
    }
}

fn persist_json_fsync(path: &Path, value: &impl Serialize) -> Result<(), ObserverIpcError> {
    let parent = path.parent().ok_or(ObserverIpcError::InvalidState)?;
    std::fs::create_dir_all(parent).map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    let temporary = PathBuf::from(format!("{}.tmp", path.display()));
    let mut bytes =
        serde_json::to_vec(value).map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    file.write_all(&bytes)
        .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    file.sync_all()
        .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    std::fs::rename(&temporary, path).map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    OpenOptions::new()
        .read(true)
        .open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ObserverIpcError::Io(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::authorized_intent::{
        AuthorizedExecutionIntent, PreSigningContext, TimeInForce, AUTHORIZED_INTENT_SCHEMA_VERSION,
    };
    use copytrade_core::decision::{
        ConfigHash, DecisionId, PlannedCloid, ProjectionHash, RiskPolicyHash, Side, TargetVersion,
    };
    use copytrade_core::portfolio_risk::PortfolioProjectionInput;
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;

    fn intent(cloid: u8) -> AuthorizedExecutionIntent {
        AuthorizedExecutionIntent {
            schema_version: AUTHORIZED_INTENT_SCHEMA_VERSION,
            decision_id: DecisionId([1; 32]),
            target_version: TargetVersion(1),
            root_cloid: PlannedCloid([cloid; 16]),
            planned_cloid: PlannedCloid([cloid; 16]),
            parent_cloid: None,
            continuation_generation: 0,
            asset: "BTC".into(),
            asset_index: 0,
            side: Side::Buy,
            quantity: Decimal::ONE,
            limit_price: Decimal::ONE,
            reduce_only: false,
            time_in_force: TimeInForce::Ioc,
            projected_portfolio_hash: ProjectionHash([2; 32]),
            risk_policy_hash: RiskPolicyHash([3; 32]),
            configuration_hash: ConfigHash([4; 32]),
            market_rules_hash: [6; 32],
            dynamic_floor_policy_hash: [7; 32],
            ioc_policy_hash: [8; 32],
            observer_release_hash: [9; 32],
            signer_release_hash: [10; 32],
            release_manifest_hash: [5; 32],
            decision_reference_price: Decimal::ONE,
            expected_committed_before: Decimal::ZERO,
            expected_committed_after: Decimal::ONE,
            decision_timestamp_ms: 1,
            expires_at: 10,
            authorization: PreSigningContext {
                now_mono: 1,
                deployed_risk_policy_hash: RiskPolicyHash([3; 32]),
                projection_input: PortfolioProjectionInput {
                    current_equity: Decimal::ONE,
                    curve_leverage: Decimal::ONE,
                    global_risk_scale: Decimal::ONE,
                    max_single_asset_equity_pct: Decimal::ONE,
                    max_net_equity_pct: Decimal::ONE,
                    filled_positions: BTreeMap::new(),
                    acknowledged_open_orders: vec![],
                    unconstrained_targets: BTreeMap::new(),
                    market_rules: BTreeMap::new(),
                    filled_position_state_complete: true,
                    open_order_state_complete: true,
                },
                exchange_minimum_notional: Decimal::ONE,
            },
            canonical_hash: [0; 32],
        }
        .seal()
        .unwrap()
    }

    #[test]
    fn queue_is_bounded_idempotent_and_disconnect_requires_reconciliation() {
        let mut queue = DurableHandoffQueue::new(1).unwrap();
        let first = intent(1);
        assert_eq!(queue.enqueue(first.clone()).unwrap(), 1);
        assert_eq!(queue.enqueue(first).unwrap(), 1);
        let mut conflicting = intent(1);
        conflicting.quantity = Decimal::from(2);
        conflicting = conflicting.seal().unwrap();
        assert_eq!(
            queue.enqueue(conflicting),
            Err(ObserverIpcError::DuplicateConflict)
        );
        assert_eq!(
            queue.enqueue(intent(2)),
            Err(ObserverIpcError::CapacityExhausted)
        );
        queue.mark_disconnect(PlannedCloid([1; 16])).unwrap();
        assert!(matches!(
            queue.pending(PlannedCloid([1; 16])).unwrap().state,
            ObserverIntentState::Persisted
        ));
    }
}
