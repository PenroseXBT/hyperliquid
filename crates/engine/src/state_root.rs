use crate::decision::{
    DecisionEngine, SnapshotIdentity, LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
    UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const INITIALIZATION_MARKER_SCHEMA_VERSION: u32 = 1;
// Preserve legacy file/schema names: package consolidation must not bypass
// the existing root lock or silently create a fresh accounting state.
const LOCK_FILE_NAME: &str = ".observer-state.lock";
const MARKER_FILE_NAME: &str = "initialized.json";
const SNAPSHOT_FILE_NAME: &str = "unsigned-observer-state.msgpack";
const MARKER_TEMP_FILE_NAME: &str = "initialized.tmp";
const SNAPSHOT_TEMP_FILE_NAME: &str = "unsigned-observer-state.tmp";
const LIVE_FILES: &[&str] = &[
    "live-account-identity.json",
    "live-account-identity.tmp",
    "live-submission-registry.json",
    "live-submission-registry.tmp",
    "live-trading-state.json",
    "live-trading-state.tmp",
    // Retired write-only execution journal files may remain on a retained
    // production volume. They are accepted as inert compatibility artifacts
    // and are never opened by the engine.
    "live-submission-registry.sqlite",
    "live-submission-registry.sqlite-wal",
    "live-submission-registry.sqlite-shm",
];

pub const UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR: &str = concat!(
    "engine-unsigned-persistence-v13\n",
    "root=directory:no-symlinks;entries=explicit-name-and-type\n",
    "live=directory:no-symlinks;files=live-account-identity,live-submission-registry,",
    "live-trading-state:json-or-tmp:regular-files:no-symlinks\n",
    "retired-execution-journal=live/live-submission-registry.sqlite:ignored-regular-files:no-symlinks\n",
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
    "pending:BTreeMap<String,PendingAction{action:PlannedAction,",
    "root_planned_cloid:String,remaining_order_quantity:Decimal,",
    "component_remaining:BTreeMap<String,Decimal>,mfce_lineage:optional-trailing}>,",
    "continuations:BTreeMap<String,ContinuationIntent>,",
    "accrued_funding:BTreeMap<String,Decimal>,last_mids:Option<MarketSnapshotResponse>,",
    "history_complete_through_ms:Option<u64>,",
    "mfce:MfcePersistentState{schema_version:u32,source_epoch:u64,",
    "next_transition_id:u64,next_sample_id:u64,last_retrain_attempt_sample_id:u64,",
    "assets:BTreeMap<String,MfceAssetState>:max=512,",
    "samples:VecDeque<MfceTrainingSample>:max=4096,",
    "delayed:optional-trailing:decisions=4096,per-market-counterfactual=32,selected-priority,flow-markets=850,flow-bins=30,",
    "horizons-ms=[30000,120000,300000,900000,1800000,3600000,14400000];net-remaining-edge-model-features=58,",
    "regret=entry,exit,hold,reentry,size:observed-executable-depth-only,",
    "decision_counts:MfceDecisionCounts{explore:u64,exploit:u64,reject:u64,allocated:u64},",
    "incumbent:Option<MfceModelState{q10_model:String:max_bytes=2097152,",
    "q50_model:String:max_bytes=2097152}>},",
    "mfce_time_high_watermark:Timestamp,",
    "ledger_time_high_watermark:Timestamp,executions:Vec<SettledActionAccounting{mfce_lineage:optional-trailing}>,",
    "equity_buckets:Vec<EquityReturnBucket>,last_bucket:Option<EquityBoundary>,",
    "micro_density:MicroDensityCounters,waiting_market_rules:BTreeSet<String>:optional-trailing-empty-omitted}\n",
    "durable_time=unix-start-anchor+process-monotonic-elapsed;runtime-anchor-not-persisted\n",
    "ledger_episode=PortfolioEpisode{entry_notional:Decimal,exit_notional:Decimal,",
    "realized_pnl:Decimal,fees:Decimal,funding:Decimal,slippage:Decimal,",
    "attribution_residual:Decimal,net_pnl:Decimal}\n",
    "ledger-cash=v3:actual-fill-gross-fees-funding-cost;signed-slippage-diagnostic-only;v2-migration\n",
    "recovered-executions=optional-trailing:verified-engine-fills,external-reductions-with-episode-id\n",
    "episode-economics=optional-trailing:opening-action,external-manual-exit,settled-cash;no-synthetic-label\n",
    "ledger_archive=BTreeMap<asset,EpisodeTotals{closed_count:u64,entry_notional:Decimal,",
    "exit_notional:Decimal,gross_pnl:Decimal,fees:Decimal,funding:Decimal,slippage:Decimal,",
    "net_pnl:Decimal,gains:Decimal,losses:Decimal}>\n",
    "checksum=sha256(canonical-messagepack((schema_version,generation,identity,payload)))\n",
    "migration=v12-exact-positional-wire+typed-checksum;mfce-v1-gross-episode-model-reset-preserves-targets\n",
    "commit=temp-write,file-fsync,atomic-rename,directory-fsync,marker-update,directory-fsync\n",
);

pub fn unsigned_persistence_schema_sha256() -> String {
    Sha256::digest(UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn persisted_snapshot_identity(root: impl AsRef<Path>) -> Result<SnapshotIdentity, String> {
    let path = root.as_ref().join(MARKER_FILE_NAME);
    let marker: InitializationMarker =
        serde_json::from_slice(&std::fs::read(&path).map_err(|error| {
            format!("failed to read state marker for offline evaluation: {error}")
        })?)
        .map_err(|error| format!("invalid state marker for offline evaluation: {error}"))?;
    Ok(marker.identity)
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
        require_path_type(&root, true)?;
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
            if name == "live" {
                require_path_type(&entry.path(), true)?;
                for live in std::fs::read_dir(entry.path())
                    .map_err(|error| format!("failed to inspect live state root: {error}"))?
                {
                    let live = live.map_err(|error| error.to_string())?;
                    if !LIVE_FILES.iter().any(|name| live.file_name() == *name) {
                        return Err(format!(
                            "live state root contains unexpected artifact: {:?}",
                            live.path()
                        ));
                    }
                    require_path_type(&live.path(), false)?;
                }
                continue;
            }
            if name == LOCK_FILE_NAME || name == MARKER_FILE_NAME || name == SNAPSHOT_FILE_NAME {
                require_path_type(&entry.path(), false)?;
                continue;
            }
            if name == MARKER_TEMP_FILE_NAME || name == SNAPSHOT_TEMP_FILE_NAME {
                require_path_type(&entry.path(), false)?;
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
        // Validate types before opening: even the lock must never follow a symlink.
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(root.join(LOCK_FILE_NAME))
            .map_err(|error| format!("failed to open unsigned state-root lock: {error}"))?;
        lock_file
            .try_lock_exclusive()
            .map_err(|error| format!("unsigned state root is already owned: {error}"))?;
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

    pub fn initialize(&mut self, engine: &mut DecisionEngine) -> Result<u64, String> {
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

    pub fn restore(&mut self, engine: &mut DecisionEngine) -> Result<u64, String> {
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

    pub fn persist(&mut self, engine: &mut DecisionEngine) -> Result<u64, String> {
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

fn require_path_type(path: &Path, directory: bool) -> Result<(), String> {
    let kind = std::fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?
        .file_type();
    if (directory && kind.is_dir()) || (!directory && kind.is_file()) {
        Ok(())
    } else {
        Err(format!(
            "state path has unexpected type (symlinks forbidden): {}",
            path.display()
        ))
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
        || ![
            LEGACY_UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
            UNSIGNED_SNAPSHOT_SCHEMA_VERSION,
        ]
        .contains(&marker.snapshot_schema_version)
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
    use crate::domain::CopyTradeConfig;
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
            "engine-state-root-{name}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn fixture_engine() -> DecisionEngine {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = CopyTradeConfig::from_path(root.join("config/copytrade.json")).unwrap();
        DecisionEngine::new(
            config,
            b"state-root-fixture",
            "state-root-run",
            40_000,
            80_000,
        )
        .unwrap()
    }

    #[test]
    fn descriptor_identifies_current_schema_and_the_mfce_artifact_bounds() {
        assert!(
            UNSIGNED_PERSISTENCE_SCHEMA_DESCRIPTOR.starts_with("engine-unsigned-persistence-v13\n")
        );
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
    fn production_live_directory_survives_first_boot_and_restart_without_cleanup() {
        let root = temporary_root("live-restart");
        let identity = identity("live-restart");
        let mut owner = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        owner.initialize(&mut fixture_engine()).unwrap();
        std::fs::create_dir(root.join("live")).unwrap();
        drop(owner);
        let mut owner = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        assert_eq!(owner.startup(), StateRootStartup::Restore);
        owner.restore(&mut fixture_engine()).unwrap();
        for name in LIVE_FILES {
            std::fs::write(root.join("live").join(name), b"preserved").unwrap();
        }
        drop(owner);
        let mut owner = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        owner.restore(&mut fixture_engine()).unwrap();
        for name in LIVE_FILES {
            assert_eq!(
                std::fs::read(root.join("live").join(name)).unwrap(),
                b"preserved"
            );
        }
        drop(owner);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_execution_sqlite_is_inert_and_cannot_change_engine_state() {
        let root = temporary_root("retired-execution-sqlite");
        let identity = identity("retired-execution-sqlite");
        let mut expected = fixture_engine();
        let expected_fingerprint = expected.persistence_semantic_fingerprint();
        let mut owner = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        owner.initialize(&mut expected).unwrap();
        std::fs::create_dir(root.join("live")).unwrap();
        let retired = root.join("live/live-submission-registry.sqlite");
        std::fs::write(&retired, b"arbitrary retired journal bytes").unwrap();
        drop(owner);

        let mut restored = fixture_engine();
        let mut owner = UnsignedStateRoot::acquire(&root, &identity).unwrap();
        owner.restore(&mut restored).unwrap();
        assert_eq!(
            restored.persistence_semantic_fingerprint(),
            expected_fingerprint
        );
        assert_eq!(
            std::fs::read(&retired).unwrap(),
            b"arbitrary retired journal bytes"
        );
        drop(owner);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_state_rejects_unknown_entries_and_wrong_types() {
        for name in [
            "live",
            LOCK_FILE_NAME,
            MARKER_FILE_NAME,
            SNAPSHOT_FILE_NAME,
            MARKER_TEMP_FILE_NAME,
            SNAPSHOT_TEMP_FILE_NAME,
        ] {
            let root = temporary_root("wrong-type");
            std::fs::create_dir_all(&root).unwrap();
            if name == "live" {
                std::fs::write(root.join(name), b"not a directory").unwrap();
            } else {
                std::fs::create_dir(root.join(name)).unwrap();
            }
            assert!(UnsignedStateRoot::acquire(&root, &identity("types")).is_err());
            std::fs::remove_dir_all(root).unwrap();
        }
        for name in LIVE_FILES.iter().copied().chain(["unknown.json"]) {
            let root = temporary_root("live-entry");
            std::fs::create_dir_all(root.join("live")).unwrap();
            if name == "unknown.json" {
                std::fs::write(root.join("live").join(name), b"unknown").unwrap();
            } else {
                std::fs::create_dir(root.join("live").join(name)).unwrap();
            }
            assert!(UnsignedStateRoot::acquire(&root, &identity("types")).is_err());
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn live_state_and_known_root_artifacts_never_follow_symlinks() {
        use std::os::unix::fs::symlink;
        for name in [
            "live",
            LOCK_FILE_NAME,
            MARKER_FILE_NAME,
            SNAPSHOT_FILE_NAME,
            MARKER_TEMP_FILE_NAME,
            SNAPSHOT_TEMP_FILE_NAME,
        ]
        .into_iter()
        .map(str::to_owned)
        .chain(LIVE_FILES.iter().map(|name| format!("live/{name}")))
        {
            let root = temporary_root("symlink");
            let outside = temporary_root("outside");
            std::fs::create_dir_all(root.join("live")).unwrap();
            std::fs::write(&outside, b"untouched").unwrap();
            if name == "live" {
                std::fs::remove_dir(root.join("live")).unwrap();
            }
            symlink(&outside, root.join(name)).unwrap();
            assert!(UnsignedStateRoot::acquire(&root, &identity("symlinks")).is_err());
            assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
            std::fs::remove_dir_all(root).unwrap();
            std::fs::remove_file(outside).unwrap();
        }
        let root = temporary_root("root-symlink");
        let outside = temporary_root("outside-dir");
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, &root).unwrap();
        assert!(UnsignedStateRoot::acquire(&root, &identity("symlinks")).is_err());
        assert!(!outside.join(LOCK_FILE_NAME).exists());
        std::fs::remove_file(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
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
