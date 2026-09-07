use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};

const CONFIDENCE_LEVELS: [Decimal; 7] = [
    Decimal::ZERO,
    Decimal::from_parts(25, 0, 0, false, 3),
    Decimal::from_parts(5, 0, 0, false, 2),
    Decimal::from_parts(10, 0, 0, false, 2),
    Decimal::from_parts(25, 0, 0, false, 2),
    Decimal::from_parts(50, 0, 0, false, 2),
    Decimal::ONE,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateState {
    Warmup,
    Active,
    Decayed,
    Quarantined,
    IntegrityBlocked,
    ManualDenylist,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateConfidence {
    pub state: CandidateState,
    pub confidence_modifier: Decimal,
    pub closed_episodes: u64,
    pub active_days: u32,
    pub last_rung_change_episode: u64,
    pub consecutive_reentry_windows: u32,
}

impl CandidateConfidence {
    pub fn new(base_confidence: Decimal) -> Result<Self, ConfidenceError> {
        if !CONFIDENCE_LEVELS.contains(&base_confidence) || base_confidence.is_zero() {
            return Err(ConfidenceError::InvalidConfidenceLevel);
        }
        Ok(Self {
            state: CandidateState::Warmup,
            confidence_modifier: base_confidence,
            closed_episodes: 0,
            active_days: 0,
            last_rung_change_episode: 0,
            consecutive_reentry_windows: 0,
        })
    }

    pub fn contributes_to_consensus(&self) -> bool {
        matches!(
            self.state,
            CandidateState::Warmup | CandidateState::Active | CandidateState::Decayed
        ) && self.confidence_modifier > Decimal::ZERO
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConfidenceReview {
    pub closed_episodes: u64,
    pub active_days: u32,
    pub net_expectancy: f64,
    pub annualized_sharpe: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceOutcome {
    GatheringData,
    Held,
    IncreasedOneRung,
    ReducedOneRung,
    Quarantined,
    ReenteredWarmup,
    IntegrityBlocked,
    ManualDenylisted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfidenceError {
    InvalidConfidenceLevel,
    NonFiniteMetric,
    EpisodeCountRegressed,
    PermanentlyBlocked,
}

impl Display for ConfidenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for ConfidenceError {}

pub fn apply_confidence_review(
    candidate: &mut CandidateConfidence,
    review: ConfidenceReview,
) -> Result<ConfidenceOutcome, ConfidenceError> {
    if matches!(
        candidate.state,
        CandidateState::IntegrityBlocked | CandidateState::ManualDenylist
    ) {
        return Err(ConfidenceError::PermanentlyBlocked);
    }
    if !review.net_expectancy.is_finite() || !review.annualized_sharpe.is_finite() {
        return Err(ConfidenceError::NonFiniteMetric);
    }
    if review.closed_episodes < candidate.closed_episodes {
        return Err(ConfidenceError::EpisodeCountRegressed);
    }
    candidate.closed_episodes = review.closed_episodes;
    candidate.active_days = review.active_days;
    if candidate.state == CandidateState::Quarantined {
        return Ok(ConfidenceOutcome::Quarantined);
    }
    if review.closed_episodes < 50 {
        candidate.state = CandidateState::Warmup;
        return Ok(ConfidenceOutcome::GatheringData);
    }
    let rung_eligible = review
        .closed_episodes
        .saturating_sub(candidate.last_rung_change_episode)
        >= 25;
    if review.closed_episodes < 100 || review.active_days < 14 {
        if review.net_expectancy < 0.0 && rung_eligible {
            reduce_one_rung(candidate);
            candidate.last_rung_change_episode = review.closed_episodes;
            return Ok(ConfidenceOutcome::ReducedOneRung);
        }
        candidate.state = state_for_confidence(candidate.confidence_modifier);
        return Ok(ConfidenceOutcome::Held);
    }
    if review.annualized_sharpe < 0.0 {
        candidate.state = CandidateState::Quarantined;
        candidate.confidence_modifier = Decimal::ZERO;
        candidate.consecutive_reentry_windows = 0;
        candidate.last_rung_change_episode = review.closed_episodes;
        return Ok(ConfidenceOutcome::Quarantined);
    }
    if !rung_eligible {
        candidate.state = state_for_confidence(candidate.confidence_modifier);
        return Ok(ConfidenceOutcome::Held);
    }
    if review.annualized_sharpe >= 15.0 {
        increase_one_rung(candidate);
        candidate.last_rung_change_episode = review.closed_episodes;
        Ok(ConfidenceOutcome::IncreasedOneRung)
    } else if review.annualized_sharpe < 10.0 {
        reduce_one_rung(candidate);
        candidate.last_rung_change_episode = review.closed_episodes;
        Ok(ConfidenceOutcome::ReducedOneRung)
    } else {
        candidate.state = state_for_confidence(candidate.confidence_modifier);
        Ok(ConfidenceOutcome::Held)
    }
}

pub fn observe_quarantine_reentry_window(
    candidate: &mut CandidateConfidence,
    annualized_sharpe: f64,
) -> Result<ConfidenceOutcome, ConfidenceError> {
    if candidate.state != CandidateState::Quarantined {
        return Ok(ConfidenceOutcome::Held);
    }
    if !annualized_sharpe.is_finite() {
        return Err(ConfidenceError::NonFiniteMetric);
    }
    if annualized_sharpe >= 15.0 {
        candidate.consecutive_reentry_windows = candidate
            .consecutive_reentry_windows
            .checked_add(1)
            .ok_or(ConfidenceError::EpisodeCountRegressed)?;
    } else {
        candidate.consecutive_reentry_windows = 0;
    }
    if candidate.consecutive_reentry_windows >= 3 {
        candidate.state = CandidateState::Warmup;
        candidate.confidence_modifier = CONFIDENCE_LEVELS[1];
        candidate.consecutive_reentry_windows = 0;
        candidate.last_rung_change_episode = candidate.closed_episodes;
        Ok(ConfidenceOutcome::ReenteredWarmup)
    } else {
        Ok(ConfidenceOutcome::Quarantined)
    }
}

pub fn block_integrity(candidate: &mut CandidateConfidence) -> ConfidenceOutcome {
    candidate.state = CandidateState::IntegrityBlocked;
    candidate.confidence_modifier = Decimal::ZERO;
    ConfidenceOutcome::IntegrityBlocked
}

pub fn manual_denylist(candidate: &mut CandidateConfidence) -> ConfidenceOutcome {
    candidate.state = CandidateState::ManualDenylist;
    candidate.confidence_modifier = Decimal::ZERO;
    ConfidenceOutcome::ManualDenylisted
}

fn increase_one_rung(candidate: &mut CandidateConfidence) {
    let index = confidence_index(candidate.confidence_modifier).unwrap_or(0);
    candidate.confidence_modifier = CONFIDENCE_LEVELS[(index + 1).min(CONFIDENCE_LEVELS.len() - 1)];
    candidate.state = state_for_confidence(candidate.confidence_modifier);
}

fn reduce_one_rung(candidate: &mut CandidateConfidence) {
    let index = confidence_index(candidate.confidence_modifier).unwrap_or(1);
    let next = index.saturating_sub(1);
    candidate.confidence_modifier = CONFIDENCE_LEVELS[next];
    if candidate.confidence_modifier.is_zero() {
        candidate.state = CandidateState::Quarantined;
    } else {
        candidate.state = CandidateState::Decayed;
    }
}

fn confidence_index(value: Decimal) -> Option<usize> {
    CONFIDENCE_LEVELS.iter().position(|level| *level == value)
}

fn state_for_confidence(value: Decimal) -> CandidateState {
    if value == Decimal::ONE {
        CandidateState::Active
    } else if value.is_zero() {
        CandidateState::Quarantined
    } else {
        CandidateState::Decayed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_uses_closed_episodes_not_fill_count() {
        let mut candidate = CandidateConfidence::new(Decimal::ONE).unwrap();
        assert_eq!(
            apply_confidence_review(
                &mut candidate,
                ConfidenceReview {
                    closed_episodes: 49,
                    active_days: 30,
                    net_expectancy: -1.0,
                    annualized_sharpe: -10.0,
                }
            )
            .unwrap(),
            ConfidenceOutcome::GatheringData
        );
        assert_eq!(candidate.confidence_modifier, Decimal::ONE);
    }

    #[test]
    fn preliminary_can_reduce_but_never_promote() {
        let mut candidate = CandidateConfidence::new(Decimal::ONE).unwrap();
        apply_confidence_review(
            &mut candidate,
            ConfidenceReview {
                closed_episodes: 50,
                active_days: 7,
                net_expectancy: -0.01,
                annualized_sharpe: 20.0,
            },
        )
        .unwrap();
        assert_eq!(candidate.confidence_modifier, Decimal::new(50, 2));
        assert_eq!(candidate.state, CandidateState::Decayed);
    }

    #[test]
    fn mature_policy_moves_at_most_one_rung_per_25_episodes() {
        let mut candidate = CandidateConfidence::new(Decimal::new(10, 2)).unwrap();
        let review = ConfidenceReview {
            closed_episodes: 100,
            active_days: 14,
            net_expectancy: 0.1,
            annualized_sharpe: 15.0,
        };
        assert_eq!(
            apply_confidence_review(&mut candidate, review).unwrap(),
            ConfidenceOutcome::IncreasedOneRung
        );
        assert_eq!(candidate.confidence_modifier, Decimal::new(25, 2));
        let mut too_soon = review;
        too_soon.closed_episodes = 124;
        assert_eq!(
            apply_confidence_review(&mut candidate, too_soon).unwrap(),
            ConfidenceOutcome::Held
        );
        assert_eq!(candidate.confidence_modifier, Decimal::new(25, 2));
    }

    #[test]
    fn negative_mature_sharpe_quarantines_and_three_windows_reenter() {
        let mut candidate = CandidateConfidence::new(Decimal::ONE).unwrap();
        assert_eq!(
            apply_confidence_review(
                &mut candidate,
                ConfidenceReview {
                    closed_episodes: 100,
                    active_days: 14,
                    net_expectancy: -1.0,
                    annualized_sharpe: -0.01,
                }
            )
            .unwrap(),
            ConfidenceOutcome::Quarantined
        );
        assert!(!candidate.contributes_to_consensus());
        for _ in 0..2 {
            assert_eq!(
                observe_quarantine_reentry_window(&mut candidate, 15.0).unwrap(),
                ConfidenceOutcome::Quarantined
            );
        }
        assert_eq!(
            observe_quarantine_reentry_window(&mut candidate, 15.0).unwrap(),
            ConfidenceOutcome::ReenteredWarmup
        );
        assert_eq!(candidate.confidence_modifier, Decimal::new(25, 3));
    }

    #[test]
    fn integrity_and_operator_blocks_are_permanent() {
        let mut candidate = CandidateConfidence::new(Decimal::ONE).unwrap();
        block_integrity(&mut candidate);
        assert!(apply_confidence_review(
            &mut candidate,
            ConfidenceReview {
                closed_episodes: 100,
                active_days: 14,
                net_expectancy: 1.0,
                annualized_sharpe: 20.0,
            }
        )
        .is_err());
        let mut candidate = CandidateConfidence::new(Decimal::ONE).unwrap();
        manual_denylist(&mut candidate);
        assert_eq!(candidate.confidence_modifier, Decimal::ZERO);
    }
}
