use copytrade_core::ipc::{
    canonical_payload_hash, ReconciliationEventIdentity, VerifiedReconciliationEvent,
    VerifiedReconciliationPayload,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;

const OUTBOX_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableReconciliationOutbox {
    schema_version: u32,
    signer_instance_id: [u8; 16],
    next_sequence: u64,
    highest_acknowledged_sequence: u64,
    source_keys: BTreeSet<String>,
    events: Vec<VerifiedReconciliationEvent>,
    maximum_events: usize,
}

impl DurableReconciliationOutbox {
    pub fn new(signer_instance_id: [u8; 16], maximum_events: usize) -> Result<Self, String> {
        if maximum_events == 0 {
            return Err("outbox capacity must be nonzero".into());
        }
        Ok(Self {
            schema_version: OUTBOX_SCHEMA_VERSION,
            signer_instance_id,
            next_sequence: 1,
            highest_acknowledged_sequence: 0,
            source_keys: BTreeSet::new(),
            events: Vec::new(),
            maximum_events,
        })
    }

    pub fn append_once(
        &mut self,
        source_key: String,
        payload: VerifiedReconciliationPayload,
    ) -> Result<Option<u64>, String> {
        if self.source_keys.contains(&source_key) {
            return Ok(None);
        }
        if self.events.len() >= self.maximum_events {
            return Err("reconciliation outbox capacity exhausted".into());
        }
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or("reconciliation sequence overflow")?;
        let event_hash = canonical_payload_hash(&payload).map_err(|error| error.to_string())?;
        self.events.push(VerifiedReconciliationEvent {
            identity: ReconciliationEventIdentity {
                signer_instance_id: self.signer_instance_id,
                sequence,
                event_hash,
            },
            payload,
        });
        self.source_keys.insert(source_key);
        Ok(Some(sequence))
    }

    pub fn events_after(&self, sequence: u64) -> Vec<VerifiedReconciliationEvent> {
        self.events
            .iter()
            .filter(|event| event.identity.sequence > sequence)
            .cloned()
            .collect()
    }

    pub fn acknowledge(&mut self, sequence: u64) -> Result<(), String> {
        let maximum = self.next_sequence.saturating_sub(1);
        if sequence < self.highest_acknowledged_sequence || sequence > maximum {
            return Err("invalid reconciliation acknowledgment".into());
        }
        self.highest_acknowledged_sequence = sequence;
        Ok(())
    }

    pub fn highest_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }

    pub fn save_atomic(&self, path: impl AsRef<Path>) -> Result<(), String> {
        let path = path.as_ref();
        let parent = path.parent().ok_or("outbox path has no parent")?;
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        std::io::Write::write_all(&mut file, &bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| error.to_string())?;
        std::fs::rename(&temporary, path).map_err(|error| error.to_string())?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let state: Self =
            serde_json::from_slice(&std::fs::read(path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        if state.schema_version != OUTBOX_SCHEMA_VERSION
            || state.maximum_events == 0
            || state.events.len() > state.maximum_events
            || state.events.iter().enumerate().any(|(index, event)| {
                event.identity.sequence != (index as u64 + 1)
                    || canonical_payload_hash(&event.payload).ok()
                        != Some(event.identity.event_hash)
            })
        {
            return Err("invalid reconciliation outbox".into());
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::decision::PlannedCloid;

    #[test]
    fn outbox_replay_has_stable_identity_and_contiguous_ack() {
        let mut outbox = DurableReconciliationOutbox::new([7; 16], 4).unwrap();
        let payload = VerifiedReconciliationPayload::Reconciled {
            cloid: PlannedCloid([1; 16]),
        };
        assert_eq!(
            outbox.append_once("a".into(), payload.clone()).unwrap(),
            Some(1)
        );
        assert_eq!(outbox.append_once("a".into(), payload).unwrap(), None);
        assert_eq!(outbox.events_after(0).len(), 1);
        outbox.acknowledge(1).unwrap();
        assert!(outbox.acknowledge(0).is_err());
    }
}
