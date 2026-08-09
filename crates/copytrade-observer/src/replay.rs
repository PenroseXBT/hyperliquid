use crate::live_shadow::{
    ExecutableDensitySummary, LiveShadowEngine, TechnicalDecisionOutcome, TechnicalDecisionReason,
    TechnicalDecisionRecord,
};
use crate::public_mainnet::{AcceptedPublicResponse, PublicPayload, PublicTransportPolicy};
use crate::ObserverCoreState;
use copytrade_core::cohort::{AttributionTag, CohortIndicatorRecord};
use copytrade_core::decision::{derive_config_hash, derive_risk_policy_hash};
use copytrade_core::scheduler::{ReadRequestKind, SourceTier, Timestamp};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub const REPLAY_CHAIN_VERSION: u32 = 2;
pub const REPLAY_CANONICALIZATION: &str = "canonical_json_value_v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplayPayload {
    RunContext {
        replay_chain_version: u32,
        canonicalization: String,
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
    /// An immutable, as-observed cohort decision record.
    ///
    /// The membership timestamp, membership-set hash, and attribution tags are
    /// journaled with the indicator instead of being recomputed from the
    /// cohort's latest membership during replay.
    CohortIndicatorSnapshot {
        indicator: CohortIndicatorRecord,
    },
    /// An immutable, material per-asset technical target change.
    TechnicalDecisionSnapshot {
        decision: TechnicalDecisionRecord,
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

pub fn replay_event_preimage_v2(
    sequence: u64,
    observed_at_mono: Timestamp,
    payload: Value,
    previous_hash: String,
) -> Value {
    serde_json::json!({
        "sequence": sequence,
        "observed_at_mono": observed_at_mono,
        "payload": payload,
        "previous_hash": previous_hash,
    })
}

pub fn derive_replay_event_hash_v2(preimage: &Value) -> Result<[u8; 32], serde_json::Error> {
    // serde_json::Map is key-sorted without preserve_order. Serializing a
    // Value therefore gives one recursive, whitespace-free representation
    // independent of domain-type round-trip behavior.
    Ok(Sha256::digest(serde_json::to_vec(preimage)?).into())
}

#[derive(Debug, Clone, Serialize)]
pub struct CohortReplaySummary {
    pub indicator_record_count: usize,
    pub independent_root_count: usize,
    pub attribution_tag_counts: BTreeMap<AttributionTag, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TechnicalReplaySummary {
    pub decision_record_count: usize,
    pub asset_count: usize,
    pub latest_target_version_by_asset: BTreeMap<String, u64>,
    pub outcome_counts: BTreeMap<TechnicalDecisionOutcome, usize>,
    pub reason_counts: BTreeMap<TechnicalDecisionReason, usize>,
    pub archetype_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TechnicalRungReplaySummary {
    pub global_risk_scale: f64,
    pub summary: TechnicalReplaySummary,
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
    pub cohort: CohortReplaySummary,
    pub recorded_technical: TechnicalReplaySummary,
    pub regenerated_technical_by_rung: Vec<TechnicalRungReplaySummary>,
    pub summaries: Vec<ExecutableDensitySummary>,
}

pub fn replay_density_rungs(
    config_path: impl AsRef<Path>,
    transport_policy_path: impl AsRef<Path>,
    journal_path: impl AsRef<Path>,
) -> Result<P4h4ReplayReport, Box<dyn Error>> {
    replay_density_rungs_with_layer(config_path, None, transport_policy_path, journal_path)
}

pub fn replay_density_rungs_with_layer(
    config_path: impl AsRef<Path>,
    very_profitable_layer_path: Option<&Path>,
    transport_policy_path: impl AsRef<Path>,
    journal_path: impl AsRef<Path>,
) -> Result<P4h4ReplayReport, Box<dyn Error>> {
    let journal_path = journal_path.as_ref();
    let observer_state = ObserverCoreState::load_with_very_profitable_layer(
        config_path,
        very_profitable_layer_path,
    )?;
    let base_config = observer_state.config().clone();
    let very_profitable_layer = observer_state.very_profitable_layer().cloned();
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
    validate_replay_context(&events, &base_config, &transport_policy)?;
    let recorded_cohort_indicators = events
        .iter()
        .filter_map(|event| match &event.payload {
            ReplayPayload::CohortIndicatorSnapshot { indicator } => Some(indicator.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let recorded_technical_decisions = events
        .iter()
        .filter_map(|event| match &event.payload {
            ReplayPayload::TechnicalDecisionSnapshot { decision } => Some(decision.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match (
        &very_profitable_layer,
        recorded_cohort_indicators.is_empty(),
    ) {
        (None, false) => {
            return Err(
                "replay journal contains cohort decisions but no very_profitable layer was supplied"
                    .into(),
            );
        }
        (Some(layer), _) => {
            for indicator in &recorded_cohort_indicators {
                if indicator.cohort_snapshot_timestamp_ms != layer.resolution.snapshot_timestamp_ms
                    || indicator.membership_set_hash != layer.resolution.membership_set_hash
                {
                    return Err(
                        "recorded cohort decision does not match the supplied membership snapshot"
                            .into(),
                    );
                }
            }
        }
        (None, true) => {}
    }
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
    let cohort = summarize_cohort_events(&events)?;
    let recorded_technical = summarize_technical_events(&events)?;
    if accepted_source_events == 0
        || accepted_market_events == 0
        || accepted_book_events == 0
        || decision_ticks == 0
    {
        return Err("replay journal lacks source, market, book, or decision events".into());
    }

    let journal_hash = Sha256::digest(std::fs::read(journal_path)?);
    let mut summaries = Vec::new();
    let mut regenerated_technical_by_rung = Vec::new();
    let mut compared_recorded_technical = recorded_technical_decisions.is_empty();
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
        if let Some(layer) = very_profitable_layer.clone() {
            engine.install_very_profitable_layer(layer)?;
        }
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
                ReplayPayload::RunContext { .. }
                | ReplayPayload::FreshnessDecision { .. }
                | ReplayPayload::CohortIndicatorSnapshot { .. }
                | ReplayPayload::TechnicalDecisionSnapshot { .. } => {}
            }
        }
        if very_profitable_layer.is_some()
            && engine.cohort_indicator_records() != recorded_cohort_indicators
        {
            return Err(
                "regenerated cohort indicators differ from the immutable recorded decisions".into(),
            );
        }
        if !recorded_technical_decisions.is_empty()
            && (rung - base_config.global_risk.global_risk_scale).abs() <= f64::EPSILON
        {
            if engine.technical_decision_records() != recorded_technical_decisions {
                return Err(
                    "regenerated technical decisions differ from the immutable recorded decisions"
                        .into(),
                );
            }
            compared_recorded_technical = true;
        }
        regenerated_technical_by_rung.push(TechnicalRungReplaySummary {
            global_risk_scale: rung,
            summary: summarize_technical_records(engine.technical_decision_records())?,
        });
        summaries.push(engine.executable_density_summary()?);
    }
    if !compared_recorded_technical {
        return Err("recorded technical decisions have no matching configured replay rung".into());
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
        cohort,
        recorded_technical,
        regenerated_technical_by_rung,
        summaries,
    })
}

fn validate_replay_context(
    events: &[ReplayEvent],
    config: &copytrade_core::configuration::CopyTradeConfig,
    transport_policy: &PublicTransportPolicy,
) -> Result<(), Box<dyn Error>> {
    let expected_config = derive_config_hash(config)?.to_hex();
    let expected_risk = derive_risk_policy_hash(&config.global_risk)?.to_hex();
    let expected_transport = replay_hash_hex(
        Sha256::digest(serde_json::to_vec(&serde_json::to_value(
            transport_policy,
        )?)?)
        .into(),
    );
    let mut observed = 0usize;
    for event in events {
        let ReplayPayload::RunContext {
            configuration_sha256,
            risk_policy_sha256,
            transport_policy_sha256,
            ..
        } = &event.payload
        else {
            continue;
        };
        observed += 1;
        if configuration_sha256 != &expected_config
            || risk_policy_sha256 != &expected_risk
            || transport_policy_sha256 != &expected_transport
        {
            return Err("replay RunContext does not match the effective strategy identity".into());
        }
    }
    if observed == 0 {
        return Err("replay journal has no RunContext identity".into());
    }
    Ok(())
}

fn summarize_cohort_events(events: &[ReplayEvent]) -> Result<CohortReplaySummary, Box<dyn Error>> {
    let mut indicator_record_count = 0usize;
    let mut independent_roots = BTreeSet::new();
    let mut attribution_tag_counts = [
        AttributionTag::ExistingWalletSourceOnly,
        AttributionTag::VeryProfitableCohortOnly,
        AttributionTag::BothSourceSetsAgreeing,
        AttributionTag::SourceSetsDisagreeing,
        AttributionTag::TechnicalOnly,
        AttributionTag::SourceTechnicalHybrid,
    ]
    .into_iter()
    .map(|tag| (tag, 0usize))
    .collect::<BTreeMap<_, _>>();
    let mut membership_hashes_by_timestamp = BTreeMap::<u64, String>::new();

    for event in events {
        let ReplayPayload::CohortIndicatorSnapshot { indicator } = &event.payload else {
            continue;
        };
        validate_cohort_indicator(indicator, &mut membership_hashes_by_timestamp)?;
        indicator_record_count += 1;
        if indicator.independent_execution_root {
            independent_roots.insert((indicator.asset.clone(), indicator.target_version));
        }
        for tag in &indicator.attribution {
            *attribution_tag_counts.entry(*tag).or_insert(0) += 1;
        }
    }

    Ok(CohortReplaySummary {
        indicator_record_count,
        independent_root_count: independent_roots.len(),
        attribution_tag_counts,
    })
}

fn summarize_technical_events(
    events: &[ReplayEvent],
) -> Result<TechnicalReplaySummary, Box<dyn Error>> {
    let mut decision_record_count = 0usize;
    let mut latest_target_version_by_asset = BTreeMap::<String, u64>::new();
    let mut outcome_counts = [
        TechnicalDecisionOutcome::Admitted,
        TechnicalDecisionOutcome::RetainedBelowDynamicFloor,
        TechnicalDecisionOutcome::CloseFirstStaged,
        TechnicalDecisionOutcome::RiskOrCapacityConstrained,
        TechnicalDecisionOutcome::OffsetBySource,
        TechnicalDecisionOutcome::Neutralized,
    ]
    .into_iter()
    .map(|outcome| (outcome, 0usize))
    .collect::<BTreeMap<_, _>>();
    let mut reason_counts = [
        TechnicalDecisionReason::InitialActivation,
        TechnicalDecisionReason::DirectionReversal,
        TechnicalDecisionReason::MaterialIncrease,
        TechnicalDecisionReason::MaterialReduction,
        TechnicalDecisionReason::SignalNeutralized,
    ]
    .into_iter()
    .map(|reason| (reason, 0usize))
    .collect::<BTreeMap<_, _>>();
    let mut archetype_counts = BTreeMap::<String, usize>::new();

    for event in events {
        let ReplayPayload::TechnicalDecisionSnapshot { decision } = &event.payload else {
            continue;
        };
        validate_technical_decision(
            decision,
            event.observed_at_mono,
            &mut latest_target_version_by_asset,
        )?;
        decision_record_count += 1;
        *outcome_counts.entry(decision.outcome).or_insert(0) += 1;
        *reason_counts.entry(decision.reason).or_insert(0) += 1;
        *archetype_counts
            .entry(decision.archetype.as_str().into())
            .or_insert(0) += 1;
    }

    Ok(TechnicalReplaySummary {
        decision_record_count,
        asset_count: latest_target_version_by_asset.len(),
        latest_target_version_by_asset,
        outcome_counts,
        reason_counts,
        archetype_counts,
    })
}

fn summarize_technical_records(
    records: &[TechnicalDecisionRecord],
) -> Result<TechnicalReplaySummary, Box<dyn Error>> {
    let events = records
        .iter()
        .enumerate()
        .map(|(sequence, decision)| ReplayEvent {
            sequence: sequence as u64,
            observed_at_mono: decision.observed_at_mono,
            payload: ReplayPayload::TechnicalDecisionSnapshot {
                decision: decision.clone(),
            },
            previous_hash: String::new(),
            event_hash: String::new(),
        })
        .collect::<Vec<_>>();
    summarize_technical_events(&events)
}

fn validate_technical_decision(
    decision: &TechnicalDecisionRecord,
    event_observed_at_mono: Timestamp,
    latest_target_version_by_asset: &mut BTreeMap<String, u64>,
) -> Result<(), Box<dyn Error>> {
    let combined = decision
        .source_target
        .checked_add(decision.technical_target)
        .ok_or("technical decision target arithmetic overflow")?;
    let order_delta = decision
        .admitted_target
        .checked_sub(decision.committed_position_notional)
        .ok_or("technical decision order delta arithmetic overflow")?;
    let meets_floor = decision.exchange_rounded_notional.abs() >= decision.dynamic_floor_notional;
    if decision.asset.is_empty()
        || decision.observed_at_mono != event_observed_at_mono
        || decision.candle_close_ms == 0
        || decision.target_version == 0
        || !(-Decimal::ONE..=Decimal::ONE).contains(&decision.score)
        || decision.dynamic_floor_notional < Decimal::ZERO
        || combined != decision.combined_target
        || order_delta != decision.order_delta_notional
        || meets_floor != decision.order_delta_meets_execution_floor
        || (!decision.exchange_rounded_notional.is_zero()
            && !decision.order_delta_notional.is_zero()
            && decision.exchange_rounded_notional.is_sign_positive()
                != decision.order_delta_notional.is_sign_positive())
        || (decision.score.is_zero() != decision.technical_target.is_zero())
        || (!decision.score.is_zero()
            && decision.score.is_sign_positive() != decision.technical_target.is_sign_positive())
    {
        return Err("invalid as-observed technical decision record".into());
    }
    if let Some(previous) = latest_target_version_by_asset.get(&decision.asset) {
        if decision.target_version != previous.saturating_add(1) {
            return Err("technical target versions are missing, duplicated, or reordered".into());
        }
    }
    latest_target_version_by_asset.insert(decision.asset.clone(), decision.target_version);
    Ok(())
}

fn validate_cohort_indicator(
    indicator: &CohortIndicatorRecord,
    membership_hashes_by_timestamp: &mut BTreeMap<u64, String>,
) -> Result<(), Box<dyn Error>> {
    if indicator.asset.is_empty()
        || indicator.cohort_snapshot_timestamp_ms == 0
        || !valid_sha256_hex(&indicator.membership_set_hash)
        || indicator.wallet_count_after_filtering > indicator.wallet_count_before_filtering
        || indicator.overlap_with_existing > indicator.wallet_count_before_filtering
    {
        return Err("invalid as-observed cohort indicator record".into());
    }
    match membership_hashes_by_timestamp.get(&indicator.cohort_snapshot_timestamp_ms) {
        Some(existing) if existing != &indicator.membership_set_hash => {
            return Err(
                "cohort membership snapshot timestamp was assigned conflicting membership hashes"
                    .into(),
            );
        }
        Some(_) => {}
        None => {
            membership_hashes_by_timestamp.insert(
                indicator.cohort_snapshot_timestamp_ms,
                indicator.membership_set_hash.clone(),
            );
        }
    }
    Ok(())
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn load_replay_events(path: impl AsRef<Path>) -> Result<Vec<ReplayEvent>, Box<dyn Error>> {
    let file = File::open(path)?;
    let mut events = Vec::new();
    let mut expected_previous = replay_hash_hex([0_u8; 32]);
    for (expected, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        let mut persisted: Value = serde_json::from_str(&line)?;
        let stored_event_hash = persisted
            .as_object_mut()
            .and_then(|object| object.remove("event_hash"))
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or("replay event is missing a string event_hash")?;
        let calculated = replay_hash_hex(derive_replay_event_hash_v2(&persisted)?);
        let event: ReplayEvent = serde_json::from_str(&line)?;
        if expected == 0 {
            let ReplayPayload::RunContext {
                replay_chain_version,
                canonicalization,
                ..
            } = &event.payload
            else {
                return Err("replay journal must begin with RunContext".into());
            };
            if *replay_chain_version != REPLAY_CHAIN_VERSION
                || canonicalization != REPLAY_CANONICALIZATION
            {
                return Err(format!(
                    "unsupported replay chain format: version {replay_chain_version}, canonicalization {canonicalization}"
                )
                .into());
            }
        }
        if event.sequence != expected as u64 {
            return Err(format!(
                "replay sequence mismatch at physical line {}: expected {}, stored {}",
                expected + 1,
                expected,
                event.sequence
            )
            .into());
        }
        if event.event_hash != stored_event_hash || stored_event_hash != calculated {
            return Err(format!(
                "replay payload hash mismatch at physical line {} sequence {}: stored {}, recomputed {}",
                expected + 1,
                event.sequence,
                stored_event_hash,
                calculated
            )
            .into());
        }
        if event.previous_hash != expected_previous {
            return Err(format!(
                "replay previous-hash mismatch at physical line {} sequence {}: stored {}, expected {}",
                expected + 1,
                event.sequence,
                event.previous_hash,
                expected_previous
            )
            .into());
        }
        expected_previous = stored_event_hash;
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

#[cfg(test)]
mod tests {
    use super::*;
    use copytrade_core::cohort::CohortDecisionReason;
    use rust_decimal::Decimal;

    fn indicator(
        asset: &str,
        membership_timestamp_ms: u64,
        membership_hash: &str,
        target_version: u64,
        independent_root: bool,
        attribution: impl IntoIterator<Item = AttributionTag>,
    ) -> CohortIndicatorRecord {
        CohortIndicatorRecord {
            asset: asset.into(),
            cohort_snapshot_timestamp_ms: membership_timestamp_ms,
            membership_set_hash: membership_hash.into(),
            wallet_count_before_filtering: 12,
            wallet_count_after_filtering: 8,
            overlap_with_existing: 3,
            long_notional: Decimal::from(100),
            short_notional: Decimal::from(20),
            long_trader_count: 6,
            short_trader_count: 2,
            impulse_5m: Decimal::new(1, 1),
            impulse_15m: Decimal::new(2, 1),
            impulse_1h: Decimal::new(3, 1),
            active_twap_direction: 1,
            active_twap_remaining_quantity: Decimal::from(5),
            weighted_wallet_quality: Decimal::new(8, 1),
            dominant_wallet_percentage: Decimal::from(25),
            cohort_score: Decimal::new(4, 1),
            chase_distance_r: Some(Decimal::new(1, 1)),
            cost_estimate_fraction: Decimal::new(1, 3),
            existing_wallet_source_target: Decimal::new(1, 1),
            very_profitable_cohort_target: Decimal::new(1, 1),
            source_target: Decimal::new(2, 1),
            technical_target: Decimal::new(3, 1),
            combined_target: Decimal::new(5, 1),
            target_version,
            independent_execution_root: independent_root,
            reasons: [CohortDecisionReason::EligibleLong].into_iter().collect(),
            attribution: attribution.into_iter().collect(),
        }
    }

    fn replay_event(sequence: u64, indicator: CohortIndicatorRecord) -> ReplayEvent {
        ReplayEvent {
            sequence,
            observed_at_mono: sequence + 1,
            payload: ReplayPayload::CohortIndicatorSnapshot { indicator },
            previous_hash: String::new(),
            event_hash: String::new(),
        }
    }

    fn technical_decision(
        asset: &str,
        observed_at_mono: u64,
        target_version: u64,
        technical_target: Decimal,
        outcome: TechnicalDecisionOutcome,
        reason: TechnicalDecisionReason,
    ) -> TechnicalDecisionRecord {
        let source_target = Decimal::from(10);
        TechnicalDecisionRecord {
            asset: asset.into(),
            observed_at_mono,
            candle_close_ms: 1_725_000_000_000,
            regime: copytrade_core::technical::MarketRegime::Range,
            archetype: copytrade_core::technical::SignalArchetype::RangeMeanReversion,
            score: if technical_target.is_zero() {
                Decimal::ZERO
            } else if technical_target.is_sign_positive() {
                Decimal::new(8, 1)
            } else {
                -Decimal::new(8, 1)
            },
            raw_technical_target_notional: technical_target,
            leveraged_technical_target_notional: technical_target,
            post_grs_technical_target_notional: technical_target,
            post_source_netting_notional: source_target + technical_target,
            post_risk_cap_notional: source_target + technical_target,
            exchange_rounded_notional: technical_target,
            committed_position_notional: source_target,
            order_delta_notional: technical_target,
            order_delta_meets_execution_floor: technical_target.abs() >= Decimal::from(12),
            source_target,
            technical_target,
            combined_target: source_target + technical_target,
            raw_target: source_target + technical_target,
            admitted_target: source_target + technical_target,
            dynamic_floor_notional: Decimal::from(12),
            target_version,
            outcome,
            reason,
        }
    }

    fn technical_replay_event(sequence: u64, decision: TechnicalDecisionRecord) -> ReplayEvent {
        ReplayEvent {
            sequence,
            observed_at_mono: decision.observed_at_mono,
            payload: ReplayPayload::TechnicalDecisionSnapshot { decision },
            previous_hash: String::new(),
            event_hash: String::new(),
        }
    }

    #[test]
    fn old_replay_payloads_remain_deserializable() {
        let payload: ReplayPayload = serde_json::from_str(r#"{"kind":"decision_tick"}"#).unwrap();
        assert!(matches!(payload, ReplayPayload::DecisionTick));
    }

    #[test]
    fn technical_payload_round_trip_preserves_material_target_decision() {
        let payload = ReplayPayload::TechnicalDecisionSnapshot {
            decision: technical_decision(
                "BTC",
                42,
                7,
                Decimal::from(20),
                TechnicalDecisionOutcome::Admitted,
                TechnicalDecisionReason::MaterialIncrease,
            ),
        };
        let encoded = serde_json::to_vec(&payload).unwrap();
        let decoded: ReplayPayload = serde_json::from_slice(&encoded).unwrap();
        let ReplayPayload::TechnicalDecisionSnapshot { decision } = decoded else {
            panic!("technical payload changed variant during round trip");
        };
        assert_eq!(decision.asset, "BTC");
        assert_eq!(decision.target_version, 7);
        assert_eq!(decision.combined_target, Decimal::from(30));
        assert_eq!(decision.reason, TechnicalDecisionReason::MaterialIncrease);
    }

    #[test]
    fn technical_summary_counts_only_material_versions_and_rejects_version_gaps() {
        let first = technical_replay_event(
            0,
            technical_decision(
                "BTC",
                10,
                4,
                Decimal::from(20),
                TechnicalDecisionOutcome::Admitted,
                TechnicalDecisionReason::InitialActivation,
            ),
        );
        let second = technical_replay_event(
            1,
            technical_decision(
                "BTC",
                20,
                5,
                Decimal::from(25),
                TechnicalDecisionOutcome::Admitted,
                TechnicalDecisionReason::MaterialIncrease,
            ),
        );
        let summary = summarize_technical_events(&[first.clone(), second]).unwrap();
        assert_eq!(summary.decision_record_count, 2);
        assert_eq!(summary.asset_count, 1);
        assert_eq!(summary.latest_target_version_by_asset["BTC"], 5);
        assert_eq!(
            summary.reason_counts[&TechnicalDecisionReason::MaterialIncrease],
            1
        );

        let gap = technical_replay_event(
            2,
            technical_decision(
                "BTC",
                30,
                7,
                Decimal::from(30),
                TechnicalDecisionOutcome::Admitted,
                TechnicalDecisionReason::MaterialIncrease,
            ),
        );
        assert!(summarize_technical_events(&[first, gap]).is_err());
    }

    #[test]
    fn cohort_payload_round_trip_preserves_observed_membership_and_attribution() {
        let membership_hash = "ab".repeat(32);
        let payload = ReplayPayload::CohortIndicatorSnapshot {
            indicator: indicator(
                "BTC",
                1_725_000_000_000,
                &membership_hash,
                7,
                true,
                [
                    AttributionTag::BothSourceSetsAgreeing,
                    AttributionTag::SourceTechnicalHybrid,
                ],
            ),
        };
        let encoded = serde_json::to_vec(&payload).unwrap();
        let decoded: ReplayPayload = serde_json::from_slice(&encoded).unwrap();
        let ReplayPayload::CohortIndicatorSnapshot { indicator } = decoded else {
            panic!("cohort payload changed variant during round trip");
        };
        assert_eq!(indicator.cohort_snapshot_timestamp_ms, 1_725_000_000_000);
        assert_eq!(indicator.membership_set_hash, membership_hash);
        assert_eq!(
            indicator.attribution,
            [
                AttributionTag::BothSourceSetsAgreeing,
                AttributionTag::SourceTechnicalHybrid,
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn su6r1_r2_fixture_reproduces_first_signed_zero_hash_mismatch() {
        let events = include_str!("../../../fixtures/su6r1-r2-first-replay-hash-mismatch.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<ReplayEvent>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 3);
        let previous = &events[0];
        let failing = &events[1];
        assert_eq!(failing.sequence, 4_578);
        assert_eq!(failing.previous_hash, previous.event_hash);

        let recomputed = replay_hash_hex(
            derive_replay_event_hash(
                decode_hex_hash_for_test(&failing.previous_hash),
                failing.sequence,
                failing.observed_at_mono,
                &failing.payload,
            )
            .unwrap(),
        );
        assert_eq!(
            failing.event_hash,
            "8642d7eeb3d815c81a7922759153dd4cb9528a0841ab5de8d5d97edd40b20c86"
        );
        assert_eq!(
            recomputed,
            "db627bfbc8b268b118474c00897bee6ca8bdbc1543712a64502f7c24ba890cea"
        );
        assert_ne!(failing.event_hash, recomputed);

        let ReplayPayload::CohortIndicatorSnapshot { indicator } = &failing.payload else {
            panic!("fixture changed event type");
        };
        assert!(indicator.cohort_score.is_zero());
        assert_eq!(indicator.cohort_score.to_string(), "0");
        assert!(
            include_str!("../../../fixtures/su6r1-r2-first-replay-hash-mismatch.jsonl")
                .lines()
                .nth(1)
                .unwrap()
                .contains("\"cohort_score\":\"-0\"")
        );
    }

    #[test]
    fn su6r1_r2_raw_fixture_hashes_and_v2_mutations_are_detected() {
        let line = include_str!("../../../fixtures/su6r1-r2-first-replay-hash-mismatch.jsonl")
            .lines()
            .nth(1)
            .unwrap();
        let event: ReplayEvent = serde_json::from_str(line).unwrap();
        let payload_start = line.find("\"payload\":").unwrap() + 10;
        let payload_end = line.rfind(",\"previous_hash\":").unwrap();
        let raw_payload = &line.as_bytes()[payload_start..payload_end];
        let mut old_preimage = Vec::new();
        old_preimage.extend_from_slice(
            format!("[{},{} ,", event.sequence, event.observed_at_mono)
                .replace(" ,", ",")
                .as_bytes(),
        );
        old_preimage.extend_from_slice(raw_payload);
        old_preimage.extend_from_slice(b"]");
        let mut old_hash = Sha256::new();
        old_hash.update(decode_hex_hash_for_test(&event.previous_hash));
        old_hash.update(b"|");
        old_hash.update(old_preimage);
        assert_eq!(
            replay_hash_hex(old_hash.finalize().into()),
            event.event_hash
        );

        let raw: Value = serde_json::from_str(line).unwrap();
        let payload = raw["payload"].clone();
        let previous = replay_hash_hex([0; 32]);
        let preimage =
            replay_event_preimage_v2(0, event.observed_at_mono, payload, previous.clone());
        let original = derive_replay_event_hash_v2(&preimage).unwrap();

        let mut payload_mutation = preimage.clone();
        payload_mutation["payload"]["indicator"]["target_version"] = 13.into();
        assert_ne!(
            derive_replay_event_hash_v2(&payload_mutation).unwrap(),
            original
        );

        let mut previous_mutation = preimage;
        previous_mutation["previous_hash"] = "ff".repeat(32).into();
        assert_ne!(
            derive_replay_event_hash_v2(&previous_mutation).unwrap(),
            original
        );
        assert_ne!(previous_mutation["previous_hash"], previous);
    }

    fn decode_hex_hash_for_test(value: &str) -> [u8; 32] {
        let mut decoded = [0_u8; 32];
        for (index, byte) in decoded.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
        }
        decoded
    }

    fn persisted_v2_event(
        sequence: u64,
        observed_at_mono: u64,
        payload: Value,
        previous_hash: String,
    ) -> Value {
        let mut event =
            replay_event_preimage_v2(sequence, observed_at_mono, payload, previous_hash);
        let event_hash = replay_hash_hex(derive_replay_event_hash_v2(&event).unwrap());
        event
            .as_object_mut()
            .unwrap()
            .insert("event_hash".into(), event_hash.into());
        event
    }

    #[test]
    fn v2_stored_value_chain_verifies_while_semantic_decode_normalizes_signed_zero() {
        let root = std::env::temp_dir().join(format!(
            "copytrade-replay-v2-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("replay-events.jsonl");
        let context = persisted_v2_event(
            0,
            1,
            serde_json::json!({
                "kind": "run_context",
                "replay_chain_version": REPLAY_CHAIN_VERSION,
                "canonicalization": REPLAY_CANONICALIZATION,
                "binary_sha256": "00",
                "configuration_sha256": "11",
                "risk_policy_sha256": "22",
                "read_policy_sha256": "33",
                "transport_policy_sha256": "44"
            }),
            replay_hash_hex([0; 32]),
        );
        let context_hash = context["event_hash"].as_str().unwrap().to_owned();
        let raw_fixture: Value = serde_json::from_str(
            include_str!("../../../fixtures/su6r1-r2-first-replay-hash-mismatch.jsonl")
                .lines()
                .nth(1)
                .unwrap(),
        )
        .unwrap();
        let cohort = persisted_v2_event(2 - 1, 2, raw_fixture["payload"].clone(), context_hash);
        let expected_head = cohort["event_hash"].as_str().unwrap().to_owned();
        std::fs::write(
            &path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&context).unwrap(),
                serde_json::to_string(&cohort).unwrap()
            ),
        )
        .unwrap();

        assert_eq!(verify_replay_event_chain(&path).unwrap(), expected_head);
        let decoded = load_replay_events(&path).unwrap();
        let ReplayPayload::CohortIndicatorSnapshot { indicator } = &decoded[1].payload else {
            panic!("fixture changed event type");
        };
        assert_eq!(indicator.cohort_score.to_string(), "0");

        let mut payload_mutation = cohort.clone();
        payload_mutation["payload"]["indicator"]["target_version"] = 999.into();
        std::fs::write(
            &path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&context).unwrap(),
                serde_json::to_string(&payload_mutation).unwrap()
            ),
        )
        .unwrap();
        assert!(verify_replay_event_chain(&path)
            .unwrap_err()
            .to_string()
            .contains("payload hash mismatch"));

        let mut link_mutation = cohort;
        link_mutation["previous_hash"] = "ff".repeat(32).into();
        link_mutation.as_object_mut().unwrap().remove("event_hash");
        let mutated_hash = replay_hash_hex(derive_replay_event_hash_v2(&link_mutation).unwrap());
        link_mutation
            .as_object_mut()
            .unwrap()
            .insert("event_hash".into(), mutated_hash.into());
        std::fs::write(
            &path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&context).unwrap(),
                serde_json::to_string(&link_mutation).unwrap()
            ),
        )
        .unwrap();
        assert!(verify_replay_event_chain(&path)
            .unwrap_err()
            .to_string()
            .contains("previous-hash mismatch"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v2_verifier_fails_closed_on_unsupported_chain_format() {
        let root = std::env::temp_dir().join(format!(
            "copytrade-replay-unsupported-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("replay-events.jsonl");
        let event = persisted_v2_event(
            0,
            1,
            serde_json::json!({
                "kind": "run_context",
                "replay_chain_version": 99,
                "canonicalization": REPLAY_CANONICALIZATION,
                "binary_sha256": "00",
                "configuration_sha256": "11",
                "risk_policy_sha256": "22",
                "read_policy_sha256": "33",
                "transport_policy_sha256": "44"
            }),
            replay_hash_hex([0; 32]),
        );
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&event).unwrap()),
        )
        .unwrap();
        let error = verify_replay_event_chain(&path).unwrap_err().to_string();
        assert!(error.contains("unsupported replay chain format"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cohort_summary_counts_records_unique_roots_and_recorded_tags() {
        let membership_hash = "01".repeat(32);
        let events = vec![
            replay_event(
                0,
                indicator(
                    "BTC",
                    100,
                    &membership_hash,
                    4,
                    true,
                    [
                        AttributionTag::VeryProfitableCohortOnly,
                        AttributionTag::SourceTechnicalHybrid,
                    ],
                ),
            ),
            replay_event(
                1,
                indicator(
                    "BTC",
                    100,
                    &membership_hash,
                    4,
                    true,
                    [AttributionTag::VeryProfitableCohortOnly],
                ),
            ),
            replay_event(
                2,
                indicator(
                    "ETH",
                    100,
                    &membership_hash,
                    0,
                    true,
                    [AttributionTag::TechnicalOnly],
                ),
            ),
        ];
        let summary = summarize_cohort_events(&events).unwrap();
        assert_eq!(summary.indicator_record_count, 3);
        assert_eq!(summary.independent_root_count, 2);
        assert_eq!(
            summary
                .attribution_tag_counts
                .get(&AttributionTag::VeryProfitableCohortOnly),
            Some(&2)
        );
        assert_eq!(
            summary
                .attribution_tag_counts
                .get(&AttributionTag::SourceTechnicalHybrid),
            Some(&1)
        );
        assert_eq!(
            summary
                .attribution_tag_counts
                .get(&AttributionTag::TechnicalOnly),
            Some(&1)
        );
        assert_eq!(
            summary
                .attribution_tag_counts
                .get(&AttributionTag::SourceSetsDisagreeing),
            Some(&0)
        );
    }

    #[test]
    fn one_observed_membership_timestamp_cannot_be_reclassified() {
        let events = vec![
            replay_event(
                0,
                indicator(
                    "BTC",
                    100,
                    &"01".repeat(32),
                    0,
                    false,
                    [AttributionTag::VeryProfitableCohortOnly],
                ),
            ),
            replay_event(
                1,
                indicator(
                    "ETH",
                    100,
                    &"02".repeat(32),
                    0,
                    false,
                    [AttributionTag::ExistingWalletSourceOnly],
                ),
            ),
        ];
        assert!(summarize_cohort_events(&events).is_err());
    }
}
