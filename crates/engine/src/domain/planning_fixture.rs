use crate::domain::configuration::CopyTradeConfig;
use crate::domain::consensus::ConsensusInput;
use crate::domain::decision::{
    construct_decision, construct_planned_actions, derive_config_hash, derive_risk_policy_hash,
    hash_market_snapshot_bytes, hash_payload_bytes, DecisionConstructionInput, DecisionRecord,
    EngineInstanceId, ExclusionReason, IdentityError, PlannedAction, SnapshotSetMember,
    SourceEligibilitySummary,
};
use crate::domain::execution_floor::ExecutionFloorPolicy;
use crate::domain::ioc::{
    execute_fixture_ioc, DepthLevel, ExecutableBook, ExecutionFill, FixtureExecutionInput,
    LatencyScenario,
};
use crate::domain::portfolio_risk::{MarketRules, PortfolioProjectionInput};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanningFixture {
    schema_version: u32,
    engine_instance_byte: u8,
    decision_sequence: u64,
    created_at_mono: u64,
    follower_equity: String,
    curve_leverage: String,
    markets: BTreeMap<String, FixtureMarket>,
    sources: Vec<FixtureSource>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureMarket {
    mark_price: String,
    price_tick: String,
    size_step: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureSource {
    candidate_id: String,
    accepted_sequence: u64,
    received_at_mono: u64,
    valid_until_mono: u64,
    allocation_weight: f64,
    confidence_modifier: f64,
    exclusion_reason: Option<ExclusionReason>,
    exposures: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct FixturePlan {
    pub decision: DecisionRecord,
    pub actions: Vec<PlannedAction>,
    pub market_rules: BTreeMap<String, MarketRules>,
}

#[derive(Debug)]
pub enum FixtureError {
    Io(String),
    Parse(String),
    Invalid(String),
    Identity(IdentityError),
}

impl Display for FixtureError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for FixtureError {}

impl From<IdentityError> for FixtureError {
    fn from(error: IdentityError) -> Self {
        Self::Identity(error)
    }
}

pub fn construct_plan_from_fixture(
    config: &CopyTradeConfig,
    fixture_path: impl AsRef<Path>,
) -> Result<FixturePlan, FixtureError> {
    let bytes = std::fs::read(fixture_path).map_err(|error| FixtureError::Io(error.to_string()))?;
    let fixture = serde_json::from_slice::<PlanningFixture>(&bytes)
        .map_err(|error| FixtureError::Parse(error.to_string()))?;
    if fixture.schema_version != 1 || fixture.markets.is_empty() || fixture.sources.is_empty() {
        return Err(FixtureError::Invalid(
            "fixture requires schema 1, markets, and sources".to_string(),
        ));
    }
    let configured = config
        .candidates
        .iter()
        .map(|candidate| candidate.address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut eligibility = SourceEligibilitySummary::default();
    let mut members = Vec::new();
    let mut consensus_inputs = BTreeMap::<String, Vec<ConsensusInput>>::new();
    let mut source_ids = BTreeSet::new();
    for source in fixture.sources {
        let normalized = source.candidate_id.to_ascii_lowercase();
        if !configured.contains(&normalized) || !source_ids.insert(normalized.clone()) {
            return Err(FixtureError::Invalid(format!(
                "unknown or duplicate fixture source {}",
                source.candidate_id
            )));
        }
        if let Some(reason) = source.exclusion_reason {
            eligibility.excluded.insert(normalized, reason);
            continue;
        }
        eligibility.active_ids.insert(normalized.clone());
        let payload = canonical_exposure_bytes(&source.exposures)?;
        members.push(SnapshotSetMember {
            candidate_id: normalized.clone(),
            accepted_sequence: source.accepted_sequence,
            payload_hash: hash_payload_bytes(&payload),
            received_at_mono: source.received_at_mono,
            valid_until_mono: source.valid_until_mono,
        });
        for (asset, exposure) in source.exposures {
            if !fixture.markets.contains_key(&asset) {
                return Err(FixtureError::Invalid(format!(
                    "missing fixture market for {asset}"
                )));
            }
            consensus_inputs
                .entry(asset)
                .or_default()
                .push(ConsensusInput {
                    candidate_id: normalized.clone(),
                    allocation_weight: source.allocation_weight,
                    confidence_modifier: source.confidence_modifier,
                    source_exposure: exposure,
                    enabled: true,
                    quarantined: false,
                    snapshot_age_ms: 0,
                });
        }
    }
    let market_bytes = canonical_market_bytes(&fixture.markets)?;
    let market_rules = fixture
        .markets
        .into_iter()
        .map(|(asset, market)| {
            Ok((
                asset,
                MarketRules {
                    mark_price: parse_decimal(&market.mark_price)?,
                    price_tick: parse_decimal(&market.price_tick)?,
                    size_step: parse_decimal(&market.size_step)?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, FixtureError>>()?;
    let risk = &config.global_risk;
    let decision = construct_decision(DecisionConstructionInput {
        engine_instance_id: EngineInstanceId([fixture.engine_instance_byte; 16]),
        decision_sequence: fixture.decision_sequence,
        config_hash: derive_config_hash(config)?,
        risk_policy_hash: derive_risk_policy_hash(risk)?,
        snapshot_members: members,
        eligibility,
        market_snapshot_id: hash_market_snapshot_bytes(&market_bytes),
        consensus_inputs,
        maximum_source_exposure: risk.max_source_exposure,
        source_snapshot_max_age_ms: risk.source_snapshot_max_age_ms,
        execution_floor_policy: ExecutionFloorPolicy {
            exchange_minimum_notional: Decimal::from_f64(risk.min_order_notional_usd)
                .ok_or_else(|| FixtureError::Invalid("invalid minimum notional".to_string()))?,
            rounding_buffer: Decimal::from_f64(risk.order_rounding_buffer_usd)
                .ok_or_else(|| FixtureError::Invalid("invalid rounding buffer".to_string()))?,
            closeability_margin: Decimal::from_f64(risk.closeability_margin_usd)
                .ok_or_else(|| FixtureError::Invalid("invalid closeability margin".to_string()))?,
            maximum_slippage_fraction: Decimal::from_f64(
                config.execution.max_slippage_bps / 10_000.0,
            )
            .ok_or_else(|| FixtureError::Invalid("invalid slippage".to_string()))?,
        },
        slot_rank_hysteresis: Decimal::from_f64(risk.slot_rank_hysteresis)
            .ok_or_else(|| FixtureError::Invalid("invalid rank hysteresis".to_string()))?,
        projection_input: PortfolioProjectionInput {
            current_equity: parse_decimal(&fixture.follower_equity)?,
            curve_leverage: parse_decimal(&fixture.curve_leverage)?,
            global_risk_scale: Decimal::from_f64(risk.global_risk_scale)
                .ok_or_else(|| FixtureError::Invalid("invalid GRS".to_string()))?,
            max_single_asset_equity_pct: Decimal::from_f64(risk.max_single_asset_equity_pct)
                .ok_or_else(|| FixtureError::Invalid("invalid asset cap".to_string()))?,
            max_net_equity_pct: Decimal::from_f64(risk.max_net_equity_pct)
                .ok_or_else(|| FixtureError::Invalid("invalid net cap".to_string()))?,
            filled_positions: BTreeMap::new(),
            filled_position_state_complete: true,
            acknowledged_open_orders: Vec::new(),
            open_order_state_complete: true,
            unconstrained_targets: BTreeMap::new(),
            market_rules: market_rules.clone(),
        },
        previous_target: None,
        created_at_mono: fixture.created_at_mono,
    })?;
    let actions = construct_planned_actions(
        EngineInstanceId([fixture.engine_instance_byte; 16]),
        &decision,
        &BTreeMap::new(),
    )?;
    Ok(FixturePlan {
        decision,
        actions,
        market_rules,
    })
}

pub fn execute_fixture_shadow(
    plan: &FixturePlan,
    taker_fee_bps: f64,
) -> Result<Vec<ExecutionFill>, FixtureError> {
    let fee_rate = Decimal::from_f64(taker_fee_bps / 10_000.0)
        .ok_or_else(|| FixtureError::Invalid("invalid taker fee".to_string()))?;
    let spread_fraction = Decimal::new(1, 3);
    let limit_fraction = Decimal::new(2, 3);
    plan.actions
        .iter()
        .map(|action| {
            let market = plan.market_rules.get(&action.asset).ok_or_else(|| {
                FixtureError::Invalid(format!("missing market for {}", action.asset))
            })?;
            let midpoint = market.mark_price;
            let quantity = action
                .rounded_notional
                .checked_div(midpoint)
                .ok_or_else(|| FixtureError::Invalid("shadow quantity overflow".to_string()))?;
            let spread = midpoint
                .checked_mul(spread_fraction)
                .ok_or_else(|| FixtureError::Invalid("shadow spread overflow".to_string()))?;
            let limit_buffer = midpoint
                .checked_mul(limit_fraction)
                .ok_or_else(|| FixtureError::Invalid("shadow limit overflow".to_string()))?;
            let decision_snapshot = ExecutableBook {
                snapshot_id: hash_market_snapshot_bytes(
                    format!("{}:decision:{}", action.asset, midpoint).as_bytes(),
                ),
                observed_at_mono: plan.decision.created_at_mono,
                midpoint,
                bids: vec![DepthLevel {
                    price: midpoint - spread,
                    quantity,
                }],
                asks: vec![DepthLevel {
                    price: midpoint + spread,
                    quantity,
                }],
            };
            let evaluation_snapshot = ExecutableBook {
                snapshot_id: hash_market_snapshot_bytes(
                    format!("{}:expected:{}", action.asset, midpoint).as_bytes(),
                ),
                observed_at_mono: plan.decision.created_at_mono + 50,
                ..decision_snapshot.clone()
            };
            execute_fixture_ioc(&FixtureExecutionInput {
                action: action.clone(),
                decision_timestamp_mono: plan.decision.created_at_mono,
                decision_market_snapshot: decision_snapshot,
                evaluation_market_snapshot: evaluation_snapshot,
                latency_scenario: LatencyScenario::Expected,
                configured_latency_ms: 50,
                proposed_limit_price: match action.side {
                    crate::domain::decision::Side::Buy => midpoint + limit_buffer,
                    crate::domain::decision::Side::Sell => midpoint - limit_buffer,
                },
                rounded_quantity: quantity,
                taker_fee_rate: fee_rate,
                funding_attribution: Decimal::ZERO,
                position_before: Decimal::ZERO,
            })
            .map_err(|error| FixtureError::Invalid(error.to_string()))
        })
        .collect()
}

fn parse_decimal(value: &str) -> Result<Decimal, FixtureError> {
    Decimal::from_str(value).map_err(|error| FixtureError::Parse(error.to_string()))
}

fn canonical_exposure_bytes(exposures: &BTreeMap<String, f64>) -> Result<Vec<u8>, FixtureError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(exposures.len() as u32).to_be_bytes());
    for (asset, exposure) in exposures {
        push_string(&mut bytes, asset)?;
        if !exposure.is_finite() {
            return Err(FixtureError::Invalid("non-finite exposure".to_string()));
        }
        bytes.extend_from_slice(&exposure.to_bits().to_be_bytes());
    }
    Ok(bytes)
}

fn canonical_market_bytes(
    markets: &BTreeMap<String, FixtureMarket>,
) -> Result<Vec<u8>, FixtureError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(markets.len() as u32).to_be_bytes());
    for (asset, market) in markets {
        push_string(&mut bytes, asset)?;
        push_string(&mut bytes, &market.mark_price)?;
        push_string(&mut bytes, &market.price_tick)?;
        push_string(&mut bytes, &market.size_step)?;
    }
    Ok(bytes)
}

fn push_string(output: &mut Vec<u8>, value: &str) -> Result<(), FixtureError> {
    let length = u32::try_from(value.len())
        .map_err(|_| FixtureError::Invalid("fixture string too long".to_string()))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}
