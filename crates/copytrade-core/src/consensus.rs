use crate::decision::PayloadHash;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct ConsensusInput {
    pub candidate_id: String,
    pub allocation_weight: f64,
    pub confidence_modifier: f64,
    pub source_exposure: f64,
    pub enabled: bool,
    pub quarantined: bool,
    pub snapshot_age_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConsensusResult {
    pub exposure: f64,
    pub contribution_by_candidate: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceExposureState {
    pub accepted_sequence: u64,
    pub payload_hash: PayloadHash,
    pub exposures: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceExposureUpdate {
    Inserted,
    FreshnessOnly,
    Replaced,
    RejectedOlderOrEqual,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceExposureBook {
    states: BTreeMap<String, SourceExposureState>,
}

impl SourceExposureBook {
    pub fn accept(
        &mut self,
        candidate_id: String,
        incoming: SourceExposureState,
    ) -> SourceExposureUpdate {
        let mut current = self.states.remove(&candidate_id);
        let outcome = accept_source_exposure_state(&mut current, incoming);
        if let Some(state) = current {
            self.states.insert(candidate_id, state);
        }
        outcome
    }

    pub fn get(&self, candidate_id: &str) -> Option<&SourceExposureState> {
        self.states.get(candidate_id)
    }
}

pub fn accept_source_exposure_state(
    current: &mut Option<SourceExposureState>,
    incoming: SourceExposureState,
) -> SourceExposureUpdate {
    let Some(existing) = current else {
        *current = Some(incoming);
        return SourceExposureUpdate::Inserted;
    };
    if incoming.accepted_sequence <= existing.accepted_sequence {
        return SourceExposureUpdate::RejectedOlderOrEqual;
    }
    if incoming.payload_hash == existing.payload_hash {
        existing.accepted_sequence = incoming.accepted_sequence;
        return SourceExposureUpdate::FreshnessOnly;
    }
    *existing = incoming;
    SourceExposureUpdate::Replaced
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusError(String);

impl Display for ConsensusError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConsensusError {}

pub fn bounded_additive_consensus(
    inputs: &[ConsensusInput],
    max_source_exposure: f64,
    source_snapshot_max_age_ms: u64,
) -> Result<ConsensusResult, ConsensusError> {
    if !max_source_exposure.is_finite() || max_source_exposure < 0.0 {
        return Err(ConsensusError(
            "max_source_exposure must be finite and non-negative".to_string(),
        ));
    }
    if source_snapshot_max_age_ms == 0 {
        return Err(ConsensusError(
            "source_snapshot_max_age_ms must be positive".to_string(),
        ));
    }

    let mut contributions = BTreeMap::<String, f64>::new();
    for input in inputs {
        if !input.allocation_weight.is_finite() || input.allocation_weight < 0.0 {
            return Err(ConsensusError(format!(
                "invalid allocation_weight for {}",
                input.candidate_id
            )));
        }
        if !input.confidence_modifier.is_finite()
            || !(0.0..=1.0).contains(&input.confidence_modifier)
        {
            return Err(ConsensusError(format!(
                "invalid confidence_modifier for {}",
                input.candidate_id
            )));
        }
        if !input.source_exposure.is_finite() {
            return Err(ConsensusError(format!(
                "non-finite source exposure for {}",
                input.candidate_id
            )));
        }
        if !input.enabled
            || input.quarantined
            || input.snapshot_age_ms > source_snapshot_max_age_ms
            || input.allocation_weight == 0.0
        {
            continue;
        }

        if contributions.contains_key(&input.candidate_id) {
            return Err(ConsensusError(format!(
                "duplicate candidate contribution for {}",
                input.candidate_id
            )));
        }
        let contribution = input.allocation_weight
            * input.confidence_modifier
            * input
                .source_exposure
                .clamp(-max_source_exposure, max_source_exposure);
        if !contribution.is_finite() {
            return Err(ConsensusError(format!(
                "non-finite source contribution for {}",
                input.candidate_id
            )));
        }
        contributions.insert(input.candidate_id.clone(), contribution);
    }

    if contributions.is_empty() {
        return Ok(ConsensusResult {
            exposure: 0.0,
            contribution_by_candidate: BTreeMap::new(),
        });
    }
    let unsaturated = contributions.values().try_fold(0.0_f64, |sum, value| {
        let next = sum + value;
        next.is_finite().then_some(next)
    });
    let Some(unsaturated) = unsaturated else {
        return Err(ConsensusError(
            "consensus exposure is non-finite".to_string(),
        ));
    };
    Ok(ConsensusResult {
        exposure: unsaturated.clamp(-1.0, 1.0),
        contribution_by_candidate: contributions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: usize, exposure: f64) -> ConsensusInput {
        ConsensusInput {
            candidate_id: format!("candidate-{id:03}"),
            allocation_weight: 1.0,
            confidence_modifier: 1.0,
            source_exposure: exposure,
            enabled: true,
            quarantined: false,
            snapshot_age_ms: 0,
        }
    }

    #[test]
    fn same_side_sources_add_then_saturate() {
        let one = bounded_additive_consensus(&[source(0, 0.10)], 1.0, 40_000).unwrap();
        assert!((one.exposure - 0.10).abs() < 1e-12);

        let two =
            bounded_additive_consensus(&[source(0, 0.10), source(1, 0.10)], 1.0, 40_000).unwrap();
        assert!((two.exposure - 0.20).abs() < 1e-12);

        let inputs = (0..169).map(|id| source(id, 0.10)).collect::<Vec<_>>();
        let result = bounded_additive_consensus(&inputs, 1.0, 40_000).unwrap();
        assert_eq!(result.exposure, 1.0);
    }

    #[test]
    fn opposing_sources_net_and_confidence_attenuates() {
        let mut low_confidence = source(1, 0.10);
        low_confidence.confidence_modifier = 0.10;
        let attenuated = bounded_additive_consensus(&[low_confidence], 1.0, 40_000).unwrap();
        assert!((attenuated.exposure - 0.01).abs() < 1e-12);

        let opposed =
            bounded_additive_consensus(&[source(1, 0.10), source(2, -0.10)], 1.0, 40_000).unwrap();
        assert!(opposed.exposure.abs() < 1e-12);
    }

    #[test]
    fn inactive_inputs_are_absent_from_both_numerator_and_denominator() {
        let active = source(1, 0.10);
        let mut stale = source(2, -0.90);
        stale.snapshot_age_ms = 40_001;
        let mut quarantined = source(3, -0.90);
        quarantined.quarantined = true;
        let result =
            bounded_additive_consensus(&[active, stale, quarantined], 1.0, 40_000).unwrap();
        assert!((result.exposure - 0.10).abs() < 1e-12);
        assert_eq!(result.contribution_by_candidate.len(), 1);
    }

    #[test]
    fn invalid_numbers_fail_closed() {
        let mut input = source(1, f64::NAN);
        assert!(bounded_additive_consensus(&[input.clone()], 1.0, 40_000).is_err());
        input.source_exposure = 0.1;
        input.confidence_modifier = f64::INFINITY;
        assert!(bounded_additive_consensus(&[input], 1.0, 40_000).is_err());
    }

    #[test]
    fn duplicate_candidate_in_one_decision_fails_closed() {
        assert!(
            bounded_additive_consensus(&[source(1, 0.1), source(1, 0.1)], 1.0, 40_000).is_err()
        );
    }

    #[test]
    fn identical_snapshot_refreshes_never_accumulate_exposure() {
        use crate::decision::hash_payload_bytes;
        use std::str::FromStr;

        let payload_hash = hash_payload_bytes(b"BTC:+0.10");
        let exposures: BTreeMap<String, Decimal> =
            [("BTC".to_string(), Decimal::from_str("0.10").unwrap())]
                .into_iter()
                .collect();
        let mut current = None;
        for sequence in 1..=1_000 {
            let outcome = accept_source_exposure_state(
                &mut current,
                SourceExposureState {
                    accepted_sequence: sequence,
                    payload_hash,
                    exposures: exposures.clone(),
                },
            );
            assert_eq!(
                outcome,
                if sequence == 1 {
                    SourceExposureUpdate::Inserted
                } else {
                    SourceExposureUpdate::FreshnessOnly
                }
            );
        }
        let state = current.unwrap();
        assert_eq!(state.accepted_sequence, 1_000);
        assert_eq!(state.exposures, exposures);
        let result = bounded_additive_consensus(&[source(1, 0.10)], 1.0, 40_000).unwrap();
        assert!((result.exposure - 0.10).abs() < 1e-12);
    }
}
