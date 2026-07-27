//! Canonical signer IPC domain and frame encoding. No network or signing code.

use crate::authorized_intent::AuthorizedExecutionIntent;
use crate::decision::{PlannedCloid, TargetVersion};
use crate::live_trading::{VerifiedExchangeFill, VerifiedFundingEvent};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const IPC_MAGIC: [u8; 4] = *b"HYPA";
pub const IPC_PROTOCOL_VERSION: u16 = 1;
pub const IPC_HEADER_BYTES: usize = 52;
pub const OBSERVER_MESSAGE_TYPE: u16 = 1;
pub const SIGNER_MESSAGE_TYPE: u16 = 2;
pub const IPC_STATE_SCHEMA_VERSION: u32 = 2;

pub type BinaryHash = [u8; 32];
pub type PolicyHash = [u8; 32];
pub type HandshakeNonce = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRole {
    Observer,
    Signer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionHandshake {
    pub protocol_version: u16,
    pub process_role: ProcessRole,
    pub release_binary_hash: BinaryHash,
    pub source_tree_sha256: String,
    pub ipc_policy_hash: PolicyHash,
    pub state_schema_version: u32,
    pub nonce: HandshakeNonce,
    pub peer_nonce: Option<HandshakeNonce>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentIdentity {
    pub planned_cloid: PlannedCloid,
    pub target_version: TargetVersion,
    pub canonical_intent_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationEventIdentity {
    pub signer_instance_id: [u8; 16],
    pub sequence: u64,
    pub event_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerifiedReconciliationPayload {
    IntentAccepted {
        identity: IntentIdentity,
        durable_sequence: u64,
    },
    SubmissionStarted {
        cloid: PlannedCloid,
    },
    SubmissionUnknown {
        cloid: PlannedCloid,
    },
    OrderAcknowledged {
        cloid: PlannedCloid,
        order_id: String,
    },
    OrderRejected {
        cloid: PlannedCloid,
        reason: String,
    },
    OrderCancelled {
        cloid: PlannedCloid,
        filled_quantity: Decimal,
    },
    OrderPartiallyFilled {
        cloid: PlannedCloid,
        order_id: String,
        filled_quantity: Decimal,
    },
    Fill(VerifiedExchangeFill),
    Funding(VerifiedFundingEvent),
    PositionSnapshot {
        positions: BTreeMap<String, Decimal>,
        observed_at: u64,
    },
    EquitySnapshot {
        equity: Decimal,
        observed_at: u64,
    },
    Reconciled {
        cloid: PlannedCloid,
    },
    AuthorizedNotSubmitted {
        cloid: PlannedCloid,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedReconciliationEvent {
    pub identity: ReconciliationEventIdentity,
    pub payload: VerifiedReconciliationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IpcPolicy {
    pub protocol_version: u16,
    pub maximum_frame_bytes: usize,
    pub maximum_pending_intents: usize,
    pub maximum_pending_risk_reducing: usize,
    pub maximum_pending_exposure_increasing: usize,
    pub maximum_pending_events: usize,
    pub maximum_reorder_window: usize,
    pub write_timeout_ms: u64,
    pub acknowledgment_timeout_ms: u64,
}

impl IpcPolicy {
    pub fn validate(&self) -> Result<(), IpcError> {
        if self.protocol_version != IPC_PROTOCOL_VERSION
            || self.maximum_frame_bytes < IPC_HEADER_BYTES
            || self.maximum_frame_bytes > 16 * 1024 * 1024
            || self.maximum_pending_intents == 0
            || self.maximum_pending_risk_reducing == 0
            || self.maximum_pending_exposure_increasing == 0
            || self.maximum_pending_events == 0
            || self.maximum_reorder_window == 0
            || self
                .maximum_pending_risk_reducing
                .checked_add(self.maximum_pending_exposure_increasing)
                != Some(self.maximum_pending_intents)
            || self.write_timeout_ms == 0
            || self.acknowledgment_timeout_ms == 0
        {
            return Err(IpcError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObserverRequest {
    Handshake(ProductionHandshake),
    Submit(AuthorizedExecutionIntent),
    Reconcile(PlannedCloid),
    ReplayFrom { highest_contiguous_sequence: u64 },
    AcknowledgeEvents { highest_contiguous_sequence: u64 },
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignerRejection {
    InvalidIntent(String),
    DuplicateCloid,
    Expired,
    PolicyMismatch,
    RiskChanged,
    NotReady,
    CapacityExhausted,
    Internal(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SignerResponse {
    Handshake(ProductionHandshake),
    Accepted {
        identity: IntentIdentity,
        durable_sequence: u64,
        durable_state: String,
    },
    Rejected {
        cloid: PlannedCloid,
        reason: SignerRejection,
    },
    NotFound {
        cloid: PlannedCloid,
    },
    Status {
        ready: bool,
        reconciled: bool,
        highest_event_sequence: u64,
    },
    ReconciliationEvent(VerifiedReconciliationEvent),
    ReconciliationEvents(Vec<VerifiedReconciliationEvent>),
    EventsAcknowledged {
        highest_contiguous_sequence: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFrame<T> {
    pub sequence: u64,
    pub payload_hash: [u8; 32],
    pub message: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpcError {
    InvalidPolicy,
    FrameTooLarge,
    Truncated,
    InvalidMagic,
    UnsupportedVersion,
    UnexpectedMessageType,
    PayloadHashMismatch,
    Encode(String),
    Decode(String),
    Io(String),
    Timeout,
}

pub async fn read_framed<T: DeserializeOwned, R: AsyncRead + Unpin>(
    reader: &mut R,
    expected_message_type: u16,
    policy: &IpcPolicy,
) -> Result<DecodedFrame<T>, IpcError> {
    policy.validate()?;
    let mut header = [0u8; IPC_HEADER_BYTES];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| IpcError::Io(error.to_string()))?;
    let payload_length =
        u32::from_be_bytes(header[16..20].try_into().map_err(|_| IpcError::Truncated)?) as usize;
    let total = IPC_HEADER_BYTES
        .checked_add(payload_length)
        .ok_or(IpcError::FrameTooLarge)?;
    if total > policy.maximum_frame_bytes {
        return Err(IpcError::FrameTooLarge);
    }
    let mut frame = Vec::with_capacity(total);
    frame.extend_from_slice(&header);
    frame.resize(total, 0);
    reader
        .read_exact(&mut frame[IPC_HEADER_BYTES..])
        .await
        .map_err(|error| IpcError::Io(error.to_string()))?;
    decode_frame(&frame, expected_message_type, policy.maximum_frame_bytes)
}

pub async fn write_framed<T: Serialize, W: AsyncWrite + Unpin>(
    writer: &mut W,
    message_type: u16,
    sequence: u64,
    message: &T,
    policy: &IpcPolicy,
) -> Result<(), IpcError> {
    policy.validate()?;
    let frame = encode_frame(message_type, sequence, message, policy.maximum_frame_bytes)?;
    tokio::time::timeout(
        Duration::from_millis(policy.write_timeout_ms),
        writer.write_all(&frame),
    )
    .await
    .map_err(|_| IpcError::Timeout)?
    .map_err(|error| IpcError::Io(error.to_string()))?;
    Ok(())
}

impl Display for IpcError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for IpcError {}

pub fn encode_frame<T: Serialize>(
    message_type: u16,
    sequence: u64,
    message: &T,
    maximum_frame_bytes: usize,
) -> Result<Vec<u8>, IpcError> {
    let payload =
        rmp_serde::to_vec(message).map_err(|error| IpcError::Encode(error.to_string()))?;
    let total = IPC_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(IpcError::FrameTooLarge)?;
    if total > maximum_frame_bytes || payload.len() > u32::MAX as usize {
        return Err(IpcError::FrameTooLarge);
    }
    let digest = Sha256::digest(&payload);
    let mut frame = Vec::with_capacity(total);
    frame.extend_from_slice(&IPC_MAGIC);
    frame.extend_from_slice(&IPC_PROTOCOL_VERSION.to_be_bytes());
    frame.extend_from_slice(&message_type.to_be_bytes());
    frame.extend_from_slice(&sequence.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&digest);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_frame<T: DeserializeOwned>(
    bytes: &[u8],
    expected_message_type: u16,
    maximum_frame_bytes: usize,
) -> Result<DecodedFrame<T>, IpcError> {
    if bytes.len() < IPC_HEADER_BYTES {
        return Err(IpcError::Truncated);
    }
    if bytes.len() > maximum_frame_bytes {
        return Err(IpcError::FrameTooLarge);
    }
    if bytes[..4] != IPC_MAGIC {
        return Err(IpcError::InvalidMagic);
    }
    if u16::from_be_bytes([bytes[4], bytes[5]]) != IPC_PROTOCOL_VERSION {
        return Err(IpcError::UnsupportedVersion);
    }
    if u16::from_be_bytes([bytes[6], bytes[7]]) != expected_message_type {
        return Err(IpcError::UnexpectedMessageType);
    }
    let sequence = u64::from_be_bytes(bytes[8..16].try_into().map_err(|_| IpcError::Truncated)?);
    let length =
        u32::from_be_bytes(bytes[16..20].try_into().map_err(|_| IpcError::Truncated)?) as usize;
    if bytes.len()
        != IPC_HEADER_BYTES
            .checked_add(length)
            .ok_or(IpcError::FrameTooLarge)?
    {
        return Err(IpcError::Truncated);
    }
    let payload = &bytes[IPC_HEADER_BYTES..];
    if Sha256::digest(payload).as_slice() != &bytes[20..52] {
        return Err(IpcError::PayloadHashMismatch);
    }
    let payload_hash = bytes[20..52].try_into().map_err(|_| IpcError::Truncated)?;
    let message =
        rmp_serde::from_slice(payload).map_err(|error| IpcError::Decode(error.to_string()))?;
    Ok(DecodedFrame {
        sequence,
        payload_hash,
        message,
    })
}

pub fn canonical_payload_hash<T: Serialize>(message: &T) -> Result<[u8; 32], IpcError> {
    let payload =
        rmp_serde::to_vec(message).map_err(|error| IpcError::Encode(error.to_string()))?;
    Ok(Sha256::digest(payload).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_frame_replays_and_rejects_tampering() {
        let message = ObserverRequest::Status;
        let first = encode_frame(OBSERVER_MESSAGE_TYPE, 7, &message, 4096).unwrap();
        let second = encode_frame(OBSERVER_MESSAGE_TYPE, 7, &message, 4096).unwrap();
        assert_eq!(first, second);
        let decoded: DecodedFrame<ObserverRequest> =
            decode_frame(&first, OBSERVER_MESSAGE_TYPE, 4096).unwrap();
        assert_eq!(decoded.sequence, 7);
        assert_eq!(decoded.message, message);
        let mut damaged = first;
        *damaged.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_frame::<ObserverRequest>(&damaged, OBSERVER_MESSAGE_TYPE, 4096),
            Err(IpcError::PayloadHashMismatch)
        );
    }

    #[test]
    fn policy_and_frame_bounds_fail_closed() {
        let policy = IpcPolicy {
            protocol_version: IPC_PROTOCOL_VERSION,
            maximum_frame_bytes: 51,
            maximum_pending_intents: 1,
            maximum_pending_risk_reducing: 1,
            maximum_pending_exposure_increasing: 0,
            maximum_pending_events: 1,
            maximum_reorder_window: 1,
            write_timeout_ms: 1,
            acknowledgment_timeout_ms: 1,
        };
        assert_eq!(policy.validate(), Err(IpcError::InvalidPolicy));
        assert_eq!(
            encode_frame(OBSERVER_MESSAGE_TYPE, 1, &ObserverRequest::Status, 52),
            Err(IpcError::FrameTooLarge)
        );
    }
}
