use crate::live_shadow::{ExecutableDensitySummary, LiveShadowEngine};
use crate::public_mainnet::{AcceptedPublicResponse, PublicPayload, PublicTransportPolicy};
use copytrade_core::configuration::CopyTradeConfig;
use copytrade_core::scheduler::{ReadRequestKind, SourceTier, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplayPayload {
    RunContext {
        binary_sha256: String,
        configuration_sha256: String,
        risk_policy_sha256: String,
        read_policy_sha256: String,
        transport_policy_sha256: String,
    },
    AcceptedResponse {
        response: AcceptedPublicResponse,
    },
    DecisionTick,
    EquityBoundary,
    TierSet {
        candidate_id: String,
        tier: SourceTier,
    },
    FreshnessDecision {
        request_kind: ReadRequestKind,
        subject: String,
        requested_at_mono: Timestamp,
        expires_at_mono: Timestamp,
        attempt: u32,
        outcome: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayEvent {
    pub sequence: u64,
    pub observed_at_mono: Timestamp,
    pub payload: ReplayPayload,
    pub previous_hash: String,
    pub event_hash: String,
}

pub fn derive_replay_event_hash(
    previous: [u8; 32],
    sequence: u64,
    observed_at_mono: Timestamp,
    payload: &ReplayPayload,
) -> Result<[u8; 32], serde_json::Error> {
    let canonical = serde_json::to_vec(&(sequence, observed_at_mono, payload))?;
    let mut hash = Sha256::new();
    hash.update(previous);
    hash.update(b"|");
    hash.update(canonical);
    Ok(hash.finalize().into())
}

pub fn replay_hash_hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Clone, Serialize)]
pub struct P4h4ReplayReport {
    pub stage: &'static str,
    pub unsigned: bool,
    pub submission_capable: bool,
    pub input_event_count: usize,
    pub accepted_source_events: usize,
    pub accepted_market_events: usize,
    pub accepted_book_events: usize,
    pub decision_ticks: usize,
    pub summaries: Vec<ExecutableDensitySummary>,
}

pub fn replay_density_rungs(
    config_path: impl AsRef<Path>,
    transport_policy_path: impl AsRef<Path>,
    journal_path: impl AsRef<Path>,
) -> Result<P4h4ReplayReport, Box<dyn Error>> {
    let journal_path = journal_path.as_ref();
    let base_config = CopyTradeConfig::from_path(config_path)?;
    let transport_policy: PublicTransportPolicy =
        serde_json::from_slice(&std::fs::read(transport_policy_path)?)?;
    transport_policy
        .validate()
        .map_err(|error| format!("{error:?}"))?;
    let active_freshness = transport_policy
        .source_deadline_ms(SourceTier::Active)
        .ok_or("active freshness overflow")?;
    let inactive_freshness = transport_policy
        .source_deadline_ms(SourceTier::Inactive)
        .ok_or("inactive freshness overflow")?;
    let events = load_replay_events(journal_path).map_err(|error| {
        format!(
            "recorded public replay journal is required; synthetic or mock replacement is forbidden: {error}"
        )
    })?;
    let accepted_source_events = events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                ReplayPayload::AcceptedResponse {
                    response: AcceptedPublicResponse {
                        request_kind: ReadRequestKind::SourceState,
                        ..
                    }
                }
            )
        })
        .count();
    let accepted_market_events = events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                ReplayPayload::AcceptedResponse {
                    response: AcceptedPublicResponse {
                        request_kind: ReadRequestKind::MarketMids
                            | ReadRequestKind::ExchangeMetadata,
                        ..
                    }
                }
            )
        })
        .count();
    let accepted_book_events = events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                ReplayPayload::AcceptedResponse {
                    response: AcceptedPublicResponse {
                        request_kind: ReadRequestKind::OrderBook,
                        ..
                    }
                }
            )
        })
        .count();
    let decision_ticks = events
        .iter()
        .filter(|event| matches!(event.payload, ReplayPayload::DecisionTick))
        .count();
    if accepted_source_events == 0
        || accepted_market_events == 0
        || accepted_book_events == 0
        || decision_ticks == 0
    {
        return Err("replay journal lacks source, market, book, or decision events".into());
    }

    let journal_hash = Sha256::digest(std::fs::read(journal_path)?);
    let mut summaries = Vec::new();
    for rung in [0.025, 0.050, 0.100] {
        let mut config = base_config.clone();
        config.global_risk.global_risk_scale = rung;
        config.validate_production()?;
        let mut seed = Vec::from(journal_hash.as_slice());
        seed.extend_from_slice(&rung.to_bits().to_be_bytes());
        let mut engine = LiveShadowEngine::new(
            config,
            &seed,
            format!("P4H4-{rung:.3}"),
            active_freshness,
            inactive_freshness,
        )?;
        for event in &events {
            match &event.payload {
                ReplayPayload::TierSet { candidate_id, tier } => {
                    engine.set_source_tier(candidate_id, *tier)
                }
                ReplayPayload::AcceptedResponse { response } => {
                    if let PublicPayload::SourceState(source) = &response.payload {
                        if let Some(tier) = response.source_tier {
                            engine.set_source_tier(&source.candidate_id, tier);
                        }
                    }
                    engine.ingest(response.clone(), event.observed_at_mono)?;
                }
                ReplayPayload::DecisionTick => {
                    engine.construct_next_decision(event.observed_at_mono)?;
                }
                ReplayPayload::EquityBoundary => {
                    engine.record_equity_boundary(event.observed_at_mono)?;
                }
                ReplayPayload::RunContext { .. } | ReplayPayload::FreshnessDecision { .. } => {}
            }
        }
        summaries.push(engine.executable_density_summary()?);
    }
    Ok(P4h4ReplayReport {
        stage: "P4H.4",
        unsigned: true,
        submission_capable: false,
        input_event_count: events.len(),
        accepted_source_events,
        accepted_market_events,
        accepted_book_events,
        decision_ticks,
        summaries,
    })
}

fn load_replay_events(path: impl AsRef<Path>) -> Result<Vec<ReplayEvent>, Box<dyn Error>> {
    let file = File::open(path)?;
    let mut events = Vec::new();
    let mut previous = [0_u8; 32];
    for (expected, line) in BufReader::new(file).lines().enumerate() {
        let event: ReplayEvent = serde_json::from_str(&line?)?;
        let calculated = derive_replay_event_hash(
            previous,
            event.sequence,
            event.observed_at_mono,
            &event.payload,
        )?;
        if event.sequence != expected as u64
            || event.previous_hash != replay_hash_hex(previous)
            || event.event_hash != replay_hash_hex(calculated)
        {
            return Err("replay event sequence is missing or reordered".into());
        }
        previous = calculated;
        events.push(event);
    }
    if events.is_empty() {
        return Err("replay journal is empty".into());
    }
    Ok(events)
}

pub fn verify_replay_event_chain(path: impl AsRef<Path>) -> Result<String, Box<dyn Error>> {
    let events = load_replay_events(path)?;
    events
        .last()
        .map(|event| event.event_hash.clone())
        .ok_or_else(|| "replay journal is empty".into())
}
