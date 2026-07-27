//! Signer event receiver and durable observer-side reconciliation journal.

use copytrade_core::ipc::{
    read_framed, write_framed, IpcPolicy, ObserverToSigner, SignerToObserver,
    OBSERVER_MESSAGE_TYPE, SIGNER_MESSAGE_TYPE,
};
use copytrade_core::live_trading::{apply_exchange_fill, apply_funding_event, LiveTradingState};
#[cfg(target_os = "linux")]
use copytrade_core::release::sha256_file;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tokio::net::{UnixListener, UnixStream};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalRecord {
    sequence: u64,
    previous_hash: [u8; 32],
    payload_hash: [u8; 32],
    event_hash: [u8; 32],
    event: SignerToObserver,
}

#[derive(Debug)]
pub enum ReconciliationIpcError {
    Io(String),
    Protocol(String),
    PeerMismatch,
    SequenceViolation,
    Ledger(String),
}
impl Display for ReconciliationIpcError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for ReconciliationIpcError {}

pub struct ReconciliationReceiver {
    listener: UnixListener,
    policy: IpcPolicy,
    expected_signer_uid: u32,
    expected_signer_gid: u32,
    expected_signer_sha256: Option<String>,
    journal_path: PathBuf,
    ledger_path: PathBuf,
    last_sequence: u64,
    last_payload_hash: [u8; 32],
    chain_head: [u8; 32],
}

impl ReconciliationReceiver {
    pub fn bind(
        socket: &Path,
        policy: IpcPolicy,
        expected_signer_uid: u32,
        expected_signer_gid: u32,
        expected_signer_sha256: Option<String>,
        journal_path: PathBuf,
        ledger_path: PathBuf,
    ) -> Result<Self, ReconciliationIpcError> {
        policy
            .validate()
            .map_err(|error| ReconciliationIpcError::Protocol(error.to_string()))?;
        if socket.exists() {
            let metadata = std::fs::symlink_metadata(socket)
                .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
            if !metadata.file_type().is_socket() {
                return Err(ReconciliationIpcError::Io(
                    "reconciliation path is not a socket".into(),
                ));
            }
            std::fs::remove_file(socket)
                .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
        }
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
        }
        let listener = UnixListener::bind(socket)
            .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o660))
            .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
        let (last_sequence, last_payload_hash, chain_head) =
            load_and_verify_journal(&journal_path)?;
        Ok(Self {
            listener,
            policy,
            expected_signer_uid,
            expected_signer_gid,
            expected_signer_sha256,
            journal_path,
            ledger_path,
            last_sequence,
            last_payload_hash,
            chain_head,
        })
    }

    pub async fn accept(&self) -> Result<UnixStream, ReconciliationIpcError> {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
        verify_peer(
            &stream,
            self.expected_signer_uid,
            self.expected_signer_gid,
            self.expected_signer_sha256.as_deref(),
        )?;
        Ok(stream)
    }

    pub async fn receive_and_apply_one(
        &mut self,
        stream: &mut UnixStream,
        live_state: &mut LiveTradingState,
    ) -> Result<SignerToObserver, ReconciliationIpcError> {
        let frame = read_framed::<SignerToObserver, _>(stream, SIGNER_MESSAGE_TYPE, &self.policy)
            .await
            .map_err(|error| ReconciliationIpcError::Protocol(error.to_string()))?;
        if frame.sequence == self.last_sequence && frame.payload_hash == self.last_payload_hash {
            write_event_ack(stream, &self.policy, frame.sequence, frame.payload_hash).await?;
            return Ok(frame.message);
        }
        if frame.sequence
            != self
                .last_sequence
                .checked_add(1)
                .ok_or(ReconciliationIpcError::SequenceViolation)?
        {
            return Err(ReconciliationIpcError::SequenceViolation);
        }
        let event = frame.message;
        let mut next = live_state.clone();
        match &event {
            SignerToObserver::ExchangeFill { fill } => {
                apply_exchange_fill(&mut next, fill.clone())
                    .map_err(|error| ReconciliationIpcError::Ledger(error.to_string()))?;
            }
            SignerToObserver::FundingEvent { funding } => {
                apply_funding_event(&mut next, funding.clone())
                    .map_err(|error| ReconciliationIpcError::Ledger(error.to_string()))?;
            }
            _ => {}
        }
        next.save_atomic(&self.ledger_path)
            .map_err(|error| ReconciliationIpcError::Ledger(error.to_string()))?;
        let bytes = rmp_serde::to_vec(&(frame.sequence, self.chain_head, &event))
            .map_err(|error| ReconciliationIpcError::Protocol(error.to_string()))?;
        let event_hash: [u8; 32] = Sha256::digest(bytes).into();
        append_record_fsync(
            &self.journal_path,
            &JournalRecord {
                sequence: frame.sequence,
                previous_hash: self.chain_head,
                payload_hash: frame.payload_hash,
                event_hash,
                event: event.clone(),
            },
        )?;
        self.last_sequence = frame.sequence;
        self.last_payload_hash = frame.payload_hash;
        self.chain_head = event_hash;
        *live_state = next;
        write_event_ack(stream, &self.policy, frame.sequence, frame.payload_hash).await?;
        Ok(event)
    }
}

async fn write_event_ack(
    stream: &mut UnixStream,
    policy: &IpcPolicy,
    sequence: u64,
    payload_hash: [u8; 32],
) -> Result<(), ReconciliationIpcError> {
    write_framed(
        stream,
        OBSERVER_MESSAGE_TYPE,
        sequence,
        &ObserverToSigner::AcknowledgeEvent {
            sequence,
            payload_hash,
        },
        policy,
    )
    .await
    .map_err(|error| ReconciliationIpcError::Protocol(error.to_string()))
}

fn load_and_verify_journal(
    path: &Path,
) -> Result<(u64, [u8; 32], [u8; 32]), ReconciliationIpcError> {
    if !path.exists() {
        return Ok((0, [0; 32], [0; 32]));
    }
    let text = std::fs::read_to_string(path)
        .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
    let mut sequence = 0u64;
    let mut payload_hash = [0; 32];
    let mut chain_head = [0; 32];
    for line in text.lines() {
        let record: JournalRecord = serde_json::from_str(line)
            .map_err(|error| ReconciliationIpcError::Protocol(error.to_string()))?;
        if record.sequence
            != sequence
                .checked_add(1)
                .ok_or(ReconciliationIpcError::SequenceViolation)?
            || record.previous_hash != chain_head
        {
            return Err(ReconciliationIpcError::SequenceViolation);
        }
        let bytes = rmp_serde::to_vec(&(record.sequence, record.previous_hash, &record.event))
            .map_err(|error| ReconciliationIpcError::Protocol(error.to_string()))?;
        let expected: [u8; 32] = Sha256::digest(bytes).into();
        if expected != record.event_hash {
            return Err(ReconciliationIpcError::SequenceViolation);
        }
        sequence = record.sequence;
        payload_hash = record.payload_hash;
        chain_head = record.event_hash;
    }
    Ok((sequence, payload_hash, chain_head))
}

fn append_record_fsync(path: &Path, record: &JournalRecord) -> Result<(), ReconciliationIpcError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
    }
    let mut bytes = serde_json::to_vec(record)
        .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
    bytes.push(b'\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
    use std::io::Write;
    file.write_all(&bytes)
        .and_then(|_| file.sync_data())
        .map_err(|error| ReconciliationIpcError::Io(error.to_string()))
}

fn verify_peer(
    stream: &UnixStream,
    uid: u32,
    gid: u32,
    hash: Option<&str>,
) -> Result<(), ReconciliationIpcError> {
    let credentials = stream
        .peer_cred()
        .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
    if credentials.uid() != uid || credentials.gid() != gid {
        return Err(ReconciliationIpcError::PeerMismatch);
    }
    #[cfg(target_os = "linux")]
    if let Some(expected) = hash {
        let pid = credentials
            .pid()
            .ok_or(ReconciliationIpcError::PeerMismatch)?;
        let executable = std::fs::read_link(format!("/proc/{pid}/exe"))
            .map_err(|error| ReconciliationIpcError::Io(error.to_string()))?;
        if sha256_file(executable).map_err(|error| ReconciliationIpcError::Io(error.to_string()))?
            != expected
        {
            return Err(ReconciliationIpcError::PeerMismatch);
        }
    }
    #[cfg(not(target_os = "linux"))]
    if hash.is_some() {
        return Err(ReconciliationIpcError::PeerMismatch);
    }
    Ok(())
}
