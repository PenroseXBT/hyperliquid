use crate::replay::{derive_replay_event_hash, replay_hash_hex, ReplayEvent, ReplayPayload};
use copytrade_core::release::ReleaseManifest;
use copytrade_core::scheduler::{ReadRequestKind, SourceTier};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunHeader {
    pub stage: String,
    pub run_id: String,
    pub binary_sha256: String,
    pub source_tree_sha256: String,
    pub git_commit: String,
    pub git_tree_state: String,
    pub configuration_sha256: String,
    pub risk_policy_sha256: String,
    pub read_policy_sha256: String,
    pub transport_policy_sha256: String,
    pub start_wall_clock_utc_ms: u64,
    pub start_monotonic_ms: u64,
    pub expected_minimum_duration_seconds: u64,
    pub candidate_count: usize,
    pub process_id: u32,
    pub host_identity: String,
    pub unsigned: bool,
    pub planned_only: bool,
    pub submission_capable: bool,
    pub api_wallet_present: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointPayload {
    pub sequence: u64,
    pub monotonic_elapsed_ms: u64,
    pub accepted_source_snapshots: u64,
    pub stale_source_snapshots: u64,
    pub rejected_source_snapshots: u64,
    pub active_source_count: usize,
    pub total_weight_consumed: u64,
    pub reserved_weight_consumed: u64,
    pub pending_queue: usize,
    pub successor_queue: usize,
    pub in_flight: usize,
    pub rate_limited_responses: u64,
    pub retry_count: u64,
    pub decision_count: u64,
    pub projection_violations: u64,
    pub shadow_execution_count: u64,
    pub open_episode_count: u64,
    pub closed_episode_count: u64,
    pub persistence_healthy: bool,
    pub response_rejected_as_stale: u64,
    pub accepted_snapshot_expired_by_age: BTreeMap<SourceTier, u64>,
    pub candidate_never_refreshed: BTreeMap<SourceTier, u64>,
    pub request_expired_in_queue: BTreeMap<SourceTier, u64>,
    pub request_superseded_pending: BTreeMap<SourceTier, u64>,
    pub request_superseded_in_flight: BTreeMap<SourceTier, u64>,
    pub dispatch_count_by_tier: BTreeMap<SourceTier, u64>,
    pub accepted_count_by_tier: BTreeMap<SourceTier, u64>,
    pub queue_wait_ms_by_tier: BTreeMap<SourceTier, LatencyEvidence>,
    pub transport_latency_ms_by_kind: BTreeMap<ReadRequestKind, LatencyEvidence>,
    pub fresh_candidates_by_tier: BTreeMap<SourceTier, u64>,
    pub oldest_pending_age_by_tier: BTreeMap<SourceTier, u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyEvidence {
    pub samples: u64,
    pub total_ms: u64,
    pub maximum_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainedCheckpoint {
    pub previous_hash: String,
    pub payload: CheckpointPayload,
    pub checkpoint_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSummary {
    pub stage: String,
    pub passed: bool,
    pub failure_reason: Option<String>,
    pub secondary_reason: Option<String>,
    pub unsigned: bool,
    pub planned_only: bool,
    pub submission_capable: bool,
    pub api_wallet_present: bool,
    pub candidate_count: usize,
    pub monotonic_elapsed_seconds: u64,
    pub process_restarts: u32,
    pub risk_invariant_violations: u64,
    pub mutation_requests: u64,
    pub key_file_opens: u64,
    pub closed_shadow_episodes: u64,
    pub closed_source_episodes: u64,
    pub event_chain_verified: bool,
    pub all_candidates_scheduled: bool,
    pub unreconciled_shadow_intents: u64,
    pub isolation_pre_run_passed: bool,
    pub isolation_post_run_passed: bool,
    #[serde(default)]
    pub evidence_verified: bool,
    #[serde(default)]
    pub profitability_gate_passed: bool,
    #[serde(default)]
    pub operational_gate_passed: bool,
    #[serde(default)]
    pub accounting_gate_passed: bool,
    #[serde(default)]
    pub profitability_signal_positive: bool,
    #[serde(default)]
    pub sharpe_target_proven: bool,
}

#[derive(Debug)]
pub enum EvidenceError {
    Io(String),
    Serialize(String),
    InvalidChain,
    ExistingOutput,
}

impl Display for EvidenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for EvidenceError {}

pub struct EvidenceBundle {
    root: PathBuf,
    checkpoints: File,
    chain_head: [u8; 32],
    next_sequence: u64,
    replay_events: File,
    next_replay_sequence: u64,
    replay_chain_head: [u8; 32],
}

impl EvidenceBundle {
    pub fn create(
        root: impl AsRef<Path>,
        release_manifest_path: impl AsRef<Path>,
        qualification_config: &serde_json::Value,
        header: &RunHeader,
        isolation_report_path: impl AsRef<Path>,
    ) -> Result<Self, EvidenceError> {
        let root = root.as_ref();
        std::fs::create_dir(root).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                EvidenceError::ExistingOutput
            } else {
                EvidenceError::Io(error.to_string())
            }
        })?;
        copy_file(release_manifest_path, root.join("release-manifest.json"))?;
        copy_file(isolation_report_path, root.join("isolation-check-report"))?;
        write_json_atomic(root.join("qualification-config.json"), qualification_config)?;
        write_json_atomic(root.join("run-header.json"), header)?;
        for name in [
            "observer.stdout.log",
            "observer.stderr.log",
            "scheduler-summary.json",
            "freshness-summary.json",
            "shadow-accounting-summary.json",
            "episode-summary.json",
        ] {
            File::create(root.join(name)).map_err(|error| EvidenceError::Io(error.to_string()))?;
        }
        let checkpoints = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(root.join("periodic-checkpoints.jsonl"))
            .map_err(|error| EvidenceError::Io(error.to_string()))?;
        let replay_events = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(root.join("replay-events.jsonl"))
            .map_err(|error| EvidenceError::Io(error.to_string()))?;
        Ok(Self {
            root: root.to_path_buf(),
            checkpoints,
            chain_head: [0; 32],
            next_sequence: 0,
            replay_events,
            next_replay_sequence: 0,
            replay_chain_head: [0; 32],
        })
    }

    pub fn append_checkpoint(
        &mut self,
        mut payload: CheckpointPayload,
    ) -> Result<String, EvidenceError> {
        self.replay_events
            .flush()
            .and_then(|_| self.replay_events.sync_data())
            .map_err(|error| EvidenceError::Io(error.to_string()))?;
        payload.sequence = self.next_sequence;
        let canonical = serde_json::to_vec(&payload)
            .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
        let mut hash = Sha256::new();
        hash.update(self.chain_head);
        hash.update(b"|");
        hash.update(canonical);
        let current: [u8; 32] = hash.finalize().into();
        let checkpoint = ChainedCheckpoint {
            previous_hash: hex(self.chain_head),
            payload,
            checkpoint_hash: hex(current),
        };
        serde_json::to_writer(&mut self.checkpoints, &checkpoint)
            .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
        self.checkpoints
            .write_all(b"\n")
            .and_then(|_| self.checkpoints.sync_data())
            .map_err(|error| EvidenceError::Io(error.to_string()))?;
        self.chain_head = current;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(EvidenceError::InvalidChain)?;
        std::fs::write(
            self.root.join("event-chain-head.txt"),
            format!("{}\n", hex(current)),
        )
        .map_err(|error| EvidenceError::Io(error.to_string()))?;
        Ok(hex(current))
    }

    pub fn write_terminal(&self, summary: &TerminalSummary) -> Result<(), EvidenceError> {
        write_json_atomic(self.root.join("terminal-summary.json"), summary)
    }

    pub fn write_summary<T: Serialize + ?Sized>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), EvidenceError> {
        write_json_atomic(self.root.join(name), value)
    }

    pub fn append_log(&self, stderr: bool, message: &str) -> Result<(), EvidenceError> {
        let name = if stderr {
            "observer.stderr.log"
        } else {
            "observer.stdout.log"
        };
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.root.join(name))
            .map_err(|error| EvidenceError::Io(error.to_string()))?;
        writeln!(file, "{message}")
            .and_then(|_| file.sync_data())
            .map_err(|error| EvidenceError::Io(error.to_string()))
    }

    pub fn append_replay_event(
        &mut self,
        observed_at_mono: u64,
        payload: ReplayPayload,
    ) -> Result<(), EvidenceError> {
        let current = derive_replay_event_hash(
            self.replay_chain_head,
            self.next_replay_sequence,
            observed_at_mono,
            &payload,
        )
        .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
        let event = ReplayEvent {
            sequence: self.next_replay_sequence,
            observed_at_mono,
            payload,
            previous_hash: replay_hash_hex(self.replay_chain_head),
            event_hash: replay_hash_hex(current),
        };
        serde_json::to_writer(&mut self.replay_events, &event)
            .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
        self.replay_events
            .write_all(b"\n")
            .map_err(|error| EvidenceError::Io(error.to_string()))?;
        self.next_replay_sequence = self
            .next_replay_sequence
            .checked_add(1)
            .ok_or(EvidenceError::InvalidChain)?;
        self.replay_chain_head = current;
        Ok(())
    }

    pub fn flush_replay_events(&mut self) -> Result<(), EvidenceError> {
        self.replay_events
            .flush()
            .and_then(|_| self.replay_events.sync_data())
            .and_then(|_| {
                std::fs::write(
                    self.root.join("replay-event-chain-head.txt"),
                    format!("{}\n", replay_hash_hex(self.replay_chain_head)),
                )
            })
            .map_err(|error| EvidenceError::Io(error.to_string()))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

pub fn verify_checkpoint_chain(path: impl AsRef<Path>) -> Result<String, EvidenceError> {
    let file = File::open(path).map_err(|error| EvidenceError::Io(error.to_string()))?;
    let mut previous = [0_u8; 32];
    let mut expected_sequence = 0_u64;
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| EvidenceError::Io(error.to_string()))?;
        let checkpoint: ChainedCheckpoint = serde_json::from_str(&line)
            .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
        if checkpoint.payload.sequence != expected_sequence
            || checkpoint.previous_hash != hex(previous)
        {
            return Err(EvidenceError::InvalidChain);
        }
        let canonical = serde_json::to_vec(&checkpoint.payload)
            .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
        let mut hash = Sha256::new();
        hash.update(previous);
        hash.update(b"|");
        hash.update(canonical);
        let current: [u8; 32] = hash.finalize().into();
        if checkpoint.checkpoint_hash != hex(current) {
            return Err(EvidenceError::InvalidChain);
        }
        previous = current;
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(EvidenceError::InvalidChain)?;
    }
    if expected_sequence == 0 {
        return Err(EvidenceError::InvalidChain);
    }
    Ok(hex(previous))
}

pub fn load_release_manifest(path: impl AsRef<Path>) -> Result<ReleaseManifest, EvidenceError> {
    let bytes = std::fs::read(path).map_err(|error| EvidenceError::Io(error.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|error| EvidenceError::Serialize(error.to_string()))
}

pub fn sha256_file(path: impl AsRef<Path>) -> Result<String, EvidenceError> {
    let bytes = std::fs::read(path).map_err(|error| EvidenceError::Io(error.to_string()))?;
    Ok(hex(Sha256::digest(bytes).into()))
}

fn copy_file(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<(), EvidenceError> {
    std::fs::copy(from, to)
        .map(|_| ())
        .map_err(|error| EvidenceError::Io(error.to_string()))
}

fn write_json_atomic(
    path: impl AsRef<Path>,
    value: &(impl Serialize + ?Sized),
) -> Result<(), EvidenceError> {
    let path = path.as_ref();
    let temporary = path.with_extension("tmp");
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| EvidenceError::Serialize(error.to_string()))?;
    bytes.push(b'\n');
    std::fs::write(&temporary, bytes)
        .and_then(|_| std::fs::rename(&temporary, path))
        .map_err(|error| EvidenceError::Io(error.to_string()))
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_chain_detects_editing_and_reordering() {
        let root = std::env::temp_dir().join(format!("hl1k-evidence-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let path = root.join("checkpoints.jsonl");
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .unwrap();
        let mut previous = [0_u8; 32];
        for sequence in 0..2 {
            let payload = CheckpointPayload {
                sequence,
                persistence_healthy: true,
                ..Default::default()
            };
            let canonical = serde_json::to_vec(&payload).unwrap();
            let mut hash = Sha256::new();
            hash.update(previous);
            hash.update(b"|");
            hash.update(canonical);
            let current: [u8; 32] = hash.finalize().into();
            serde_json::to_writer(
                &mut file,
                &ChainedCheckpoint {
                    previous_hash: hex(previous),
                    payload,
                    checkpoint_hash: hex(current),
                },
            )
            .unwrap();
            writeln!(file).unwrap();
            previous = current;
        }
        drop(file);
        assert_eq!(verify_checkpoint_chain(&path).unwrap(), hex(previous));
        let mut text = std::fs::read_to_string(&path).unwrap();
        text = text.replacen("true", "false", 1);
        std::fs::write(&path, text).unwrap();
        assert!(verify_checkpoint_chain(&path).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
