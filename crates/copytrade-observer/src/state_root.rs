use crate::live_shadow::{LiveShadowEngine, SnapshotIdentity, UNSIGNED_SNAPSHOT_SCHEMA_VERSION};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const INITIALIZATION_MARKER_SCHEMA_VERSION: u32 = 1;
const LOCK_FILE_NAME: &str = ".observer-state.lock";
const MARKER_FILE_NAME: &str = "initialized.json";
const SNAPSHOT_FILE_NAME: &str = "unsigned-observer-state.msgpack";
const MARKER_TEMP_FILE_NAME: &str = "initialized.tmp";
const SNAPSHOT_TEMP_FILE_NAME: &str = "unsigned-observer-state.tmp";

pub const UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR: &str = concat!(
    "copytrade-observer-unsigned-persistence-v8\n",
    "lock=.observer-state.lock:exclusive-os-lock:lifetime\n",
    "marker=initialized.json:",
    "InitializationMarker{schema_version:u32,snapshot_schema_version:u32,generation:u64,",
    "identity:SnapshotIdentity}\n",
    "snapshot=unsigned-observer-state.msgpack:canonical-messagepack:",
    "UnsignedSnapshotEnvelope{schema_version:u32,generation:u64,identity:SnapshotIdentity,",
    "payload:UnsignedObserverState,checksum_sha256:[u8;32]}\n",
    "identity=SnapshotIdentity{source_tree_sha256:String,observer_binary_sha256:String,",
    "configuration_sha256:String,risk_policy_sha256:String}\n",
    "payload=UnsignedObserverState{ledger:DualLedger,target_ledger:VirtualTargetLedger,",
    "technical_engine:TechnicalEngine{canonical_candle_identity:(asset,interval,open_ms,close_ms),",
    "technical_funnel:TechnicalFunnel},",
    "technical_decision_state:BTreeMap<String,TechnicalDecisionUniquenessState>,",
    "very_profitable_engine:VeryProfitableCohortEngine,",
    "very_profitable_layer_artifact_sha256:Option<String>,decision_sequence:u64,",
    "continuations:BTreeMap<String,ContinuationIntent>,",
    "accrued_funding:BTreeMap<String,Decimal>,last_mids:Option<MarketSnapshotResponse>,",
    "mfce:MfcePersistentState{schema_version:u32,source_epoch:u64,",
    "next_transition_id:u64,next_sample_id:u64,last_retrain_attempt_sample_id:u64,",
    "assets:BTreeMap<String,MfceAssetState>:max=512,",
    "samples:VecDeque<MfceTrainingSample>:max=4096,",
    "decision_counts:MfceDecisionCounts{explore:u64,exploit:u64,reject:u64,allocated:u64},",
    "incumbent:Option<MfceModelState{q10_model:String:max_bytes=2097152,",
    "q50_model:String:max_bytes=2097152}>},",
    "mfce_time_high_watermark:Timestamp,",
    "ledger_time_high_watermark:Timestamp,executions:Vec<ShadowActionAccounting>,",
    "equity_buckets:Vec<EquityReturnBucket>,last_bucket:Option<EquityBoundary>,",
    "micro_density:MicroDensityCounters}\n",
    "ledger_episode=PortfolioEpisode{entry_notional:Decimal,exit_notional:Decimal,",
    "realized_pnl:Decimal,fees:Decimal,funding:Decimal,slippage:Decimal,",
    "attribution_residual:Decimal,net_pnl:Decimal}\n",
    "ledger_archive=BTreeMap<asset,EpisodeTotals{closed_count:u64,entry_notional:Decimal,",
    "exit_notional:Decimal,gross_pnl:Decimal,fees:Decimal,funding:Decimal,slippage:Decimal,",
    "net_pnl:Decimal,gains:Decimal,losses:Decimal}>\n",
    "checksum=sha256(canonical-messagepack((schema_version,generation,identity,payload)))\n",
    "migration=v7-source-ewma-to-v8-empty-mfce:checksum-first:ledger-preserving\n",
    "commit=temp-write,file-fsync,atomic-rename,directory-fsync,marker-update,directory-fsync\n",
);

pub fn unsigned_persistence_schema_sha256() -> String {
    Sha256::digest(UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateRootStartup {
    Initialize,
    Restore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializationMarker {
    schema_version: u32,
    snapshot_schema_version: u32,
    generation: u64,
    identity: SnapshotIdentity,
}

pub struct UnsignedStateRoot {
    root: PathBuf,
    snapshot_path: PathBuf,
    marker_path: PathBuf,
    identity: SnapshotIdentity,
    marker: Option<InitializationMarker>,
    _lock_file: File,
}

impl UnsignedStateRoot {
    pub fn acquire(root: impl AsRef<Path>, identity: &SnapshotIdentity) -> Result<Self, String> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .map_err(|error| format!("failed to create unsigned state root: {error}"))?;
        let lock_path = root.join(LOCK_FILE_NAME);
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| format!("failed to open unsigned state-root lock: {error}"))?;
        lock_file
            .try_lock_exclusive()
            .map_err(|error| format!("unsigned state root is already owned: {error}"))?;

        let marker_path = root.join(MARKER_FILE_NAME);
        let snapshot_path = root.join(SNAPSHOT_FILE_NAME);
        let marker_exists = marker_path.is_file();
        let snapshot_exists = snapshot_path.is_file();
        let mut unexpected = Vec::new();
        let mut recoverable_temporary = Vec::new();
        for entry in std::fs::read_dir(&root)
            .map_err(|error| format!("failed to inspect unsigned state root: {error}"))?
        {
            let entry =
                entry.map_err(|error| format!("failed to inspect unsigned state root: {error}"))?;
            let name = entry.file_name();
            if name == LOCK_FILE_NAME || name == MARKER_FILE_NAME || name == SNAPSHOT_FILE_NAME {
                continue;
            }
            if name == MARKER_TEMP_FILE_NAME || name == SNAPSHOT_TEMP_FILE_NAME {
                recoverable_temporary.push(entry.path());
            } else {
                unexpected.push(entry.path());
            }
        }

        if !unexpected.is_empty() {
            return Err(format!(
                "unsigned state root contains unexpected artifacts: {unexpected:?}"
            ));
        }
        if !recoverable_temporary.is_empty() && !(marker_exists && snapshot_exists) {
            return Err(
                "unsigned state root contains uncommitted state without a complete marker/snapshot pair"
                    .into(),
            );
        }

        let marker = match (marker_exists, snapshot_exists) {
            (false, false) => None,
            (true, true) => {
                for temporary in recoverable_temporary {
                    std::fs::remove_file(&temporary).map_err(|error| {
                        format!(
                            "failed to remove stale unsigned state temporary {}: {error}",
                            temporary.display()
                        )
                    })?;
                }
                sync_directory(&root)?;
                let marker = read_marker(&marker_path)?;
                validate_marker(&marker, identity)?;
                Some(marker)
            }
            (true, false) => {
                return Err("unsigned state marker exists without a snapshot".into());
            }
            (false, true) => {
                return Err(
                    "unsigned state snapshot exists without an initialization marker".into(),
                );
            }
        };

        Ok(Self {
            root,
            snapshot_path,
            marker_path,
            identity: identity.clone(),
            marker,
            _lock_file: lock_file,
        })
    }

    pub fn startup(&self) -> StateRootStartup {
        if self.marker.is_some() {
            StateRootStartup::Restore
        } else {
            StateRootStartup::Initialize
        }
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    pub fn initialize(&mut self, engine: &mut LiveShadowEngine) -> Result<u64, String> {
        if self.marker.is_some() {
            return Err("unsigned state root is already initialized".into());
        }
        let generation = engine
            .persist_unsigned_state(&self.snapshot_path, &self.identity)
            .map_err(|error| error.to_string())?;
        if generation != 0 {
            return Err("generation-zero initialization produced a nonzero generation".into());
        }
        self.write_marker(generation)?;
        Ok(generation)
    }

    pub fn restore(&mut self, engine: &mut LiveShadowEngine) -> Result<u64, String> {
        let marker = self
            .marker
            .as_ref()
            .ok_or("unsigned state root is not initialized")?;
        let marker_generation = marker.generation;
        let snapshot_generation = engine
            .restore_unsigned_state(&self.snapshot_path, &self.identity)
            .map_err(|error| error.to_string())?;
        if snapshot_generation < marker_generation {
            return Err(format!(
                "unsigned snapshot generation rollback: marker={marker_generation} snapshot={snapshot_generation}"
            ));
        }
        if snapshot_generation > marker_generation {
            // The snapshot is renamed and directory-synced before its marker is
            // updated. A crash in that interval is the sole safe mismatch: the
            // checksummed newer snapshot is authoritative and heals the marker.
            self.write_marker(snapshot_generation)?;
        }
        Ok(snapshot_generation)
    }

    pub fn persist(&mut self, engine: &mut LiveShadowEngine) -> Result<u64, String> {
        if self.marker.is_none() {
            return Err("unsigned state root is not initialized".into());
        }
        let generation = engine
            .persist_unsigned_state(&self.snapshot_path, &self.identity)
            .map_err(|error| error.to_string())?;
        self.write_marker(generation)?;
        Ok(generation)
    }

    fn write_marker(&mut self, generation: u64) -> Result<(), String> {
        let marker = InitializationMarker {
            schema_version: INITIALIZATION_MARKER_SCHEMA_VERSION,
            snapshot_schema_version: UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            generation,
            identity: self.identity.clone(),
        };
        let bytes = serde_json::to_vec(&marker)
            .map_err(|error| format!("failed to encode unsigned state marker: {error}"))?;
        let temporary = self.root.join(MARKER_TEMP_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| format!("failed to create unsigned state marker: {error}"))?;
        file.write_all(&bytes)
            .map_err(|error| format!("failed to write unsigned state marker: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("failed to sync unsigned state marker: {error}"))?;
        std::fs::rename(&temporary, &self.marker_path)
            .map_err(|error| format!("failed to commit unsigned state marker: {error}"))?;
        sync_directory(&self.root)?;
        self.marker = Some(marker);
        Ok(())
    }
}

fn read_marker(path: &Path) -> Result<InitializationMarker, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("failed to read unsigned state marker: {error}"))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid unsigned state marker: {error}"))
}

fn validate_marker(
    marker: &InitializationMarker,
    expected_identity: &SnapshotIdentity,
) -> Result<(), String> {
    if marker.schema_version != INITIALIZATION_MARKER_SCHEMA_VERSION
        || marker.snapshot_schema_version != UNSIGNED_SNAPSHOT_SCHEMA_VERSION
    {
        return Err("unsupported unsigned state marker schema".into());
    }
    if &marker.identity != expected_identity {
        return Err("unsigned state marker identity mismatch".into());
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync unsigned state directory: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::CopyTradeConfig;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

    fn identity(suffix: &str) -> SnapshotIdentity {
        SnapshotIdentity {
            source_tree_sha256: format!("source-{suffix}"),
            observer_binary_sha256: format!("observer-{suffix}"),
            configuration_sha256: format!("configuration-{suffix}"),
            risk_policy_sha256: format!("risk-{suffix}"),
        }
    }

    fn temporary_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "copytrade-observer-state-root-{name}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn fixture_engine() -> LiveShadowEngine {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        LiveShadowEngine::new(
            config,
            b"state-root-fixture",
            "state-root-run",
            40_000,
            80_000,
        )
        .unwrap()
    }

    #[test]
    fn descriptor_identifies_schema_v8_and_the_mfce_artifact_bounds() {
        assert!(UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR
            .starts_with("copytrade-observer-unsigned-persistence-v8\n"));
        assert!(UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR
            .contains("samples:VecDeque<MfceTrainingSample>:max=4096"));
        assert!(UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR.contains("max_bytes=2097152"));
        assert!(!UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR
            .contains("source_expectancy:BTreeMap<String,SourceExpectancyState>"));
    }

    #[test]
    fn lock_is_exclusive_for_the_owner_lifetime() {
        let root = temporary_root("exclusive");
        let identity = identity("exclusive");
        let owner = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        assert_eq!(owner.startup(), StateRootStartup::Initialize);
        assert!(UnsignedStateRoot::acquire(&root, &identity).is_err());
        drop(owner);
        assert!(UnsignedStateRoot::acquire(&root, &identity).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_matrix_fails_closed_for_partial_or_unexpected_state() {
        let identity = identity("matrix");

        let marker_only = temporary_root("marker-only");
        std::fs::create_dir_all(&marker_only).unwrap();
        std::fs::write(marker_only.join(MARKER_FILE_NAME), b"{}").unwrap();
        assert!(UnsignedStateRoot::acquire(&marker_only, &identity).is_err());
        std::fs::remove_dir_all(marker_only).unwrap();

        let snapshot_only = temporary_root("snapshot-only");
        std::fs::create_dir_all(&snapshot_only).unwrap();
        std::fs::write(snapshot_only.join(SNAPSHOT_FILE_NAME), b"snapshot").unwrap();
        assert!(UnsignedStateRoot::acquire(&snapshot_only, &identity).is_err());
        std::fs::remove_dir_all(snapshot_only).unwrap();

        let unexpected = temporary_root("unexpected");
        std::fs::create_dir_all(&unexpected).unwrap();
        std::fs::write(unexpected.join("old-state.json"), b"state").unwrap();
        assert!(UnsignedStateRoot::acquire(&unexpected, &identity).is_err());
        std::fs::remove_dir_all(unexpected).unwrap();
    }

    #[test]
    fn newer_valid_snapshot_heals_marker_but_snapshot_rollback_is_rejected() {
        let root = temporary_root("generation");
        let identity = identity("generation");
        let mut engine = fixture_engine();
        let mut state_root = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        assert_eq!(state_root.initialize(&mut engine).unwrap(), 0);
        let generation_zero = std::fs::read(state_root.snapshot_path()).unwrap();

        // Simulate power loss after snapshot rename/fsync and before marker update.
        assert_eq!(
            engine
                .persist_unsigned_state(state_root.snapshot_path(), &identity)
                .unwrap(),
            1
        );
        drop(state_root);

        let mut restored = fixture_engine();
        let mut state_root = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        assert_eq!(state_root.restore(&mut restored).unwrap(), 1);
        assert_eq!(
            read_marker(&root.join(MARKER_FILE_NAME))
                .unwrap()
                .generation,
            1
        );
        drop(state_root);

        // A marker ahead of its snapshot can only be rollback/corruption.
        std::fs::write(root.join(SNAPSHOT_FILE_NAME), generation_zero).unwrap();
        let mut rejected = fixture_engine();
        let mut state_root = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        assert!(state_root.restore(&mut rejected).is_err());
        drop(state_root);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn marker_identity_mismatch_is_rejected_before_snapshot_restore() {
        let root = temporary_root("identity");
        let original = identity("original");
        let mut engine = fixture_engine();
        let mut state_root = UnsignedStateRoot::acquire(&root, &original).unwrap();
        state_root.initialize(&mut engine).unwrap();
        drop(state_root);

        assert!(UnsignedStateRoot::acquire(&root, &identity("different")).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
