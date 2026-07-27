#![forbid(unsafe_code)]

pub mod allocation;
pub mod authorized_intent;
pub mod confidence;
pub mod configuration;
pub mod consensus;
pub mod decision;
pub mod deployment_equity;
pub mod execution_floor;
pub mod exit_planning;
pub mod ipc;
pub mod ledger;
pub mod lifecycle;
pub mod live_trading;
pub mod planning_fixture;
pub mod portfolio_risk;
pub mod release;
pub mod scheduler;
pub mod shadow;
pub mod target_state;
pub mod technical;

pub use allocation::{
    allocate_sparse_portfolio, AllocationError, SparseAllocation, SparseAllocationInput,
};

pub use configuration::{
    CopyTradeConfig, GlobalRiskConfig, LeverageCurvePoint, TraderCandidate,
    LIVE_CONFIG_SCHEMA_VERSION,
};
pub use consensus::{
    accept_source_exposure_state, bounded_additive_consensus, ConsensusInput, ConsensusResult,
    SourceExposureBook, SourceExposureState, SourceExposureUpdate,
};
pub use deployment_equity::{calculate_deployment_equity, DeploymentEquity, DeploymentEquityError};
pub use execution_floor::{
    execution_floor_for_asset, validates_rounded_order, AssetExecutionFloor, ExecutionFloorError,
    ExecutionFloorPolicy,
};
pub use target_state::{TargetStateError, VirtualTargetLedger, VirtualTargetState};
pub use technical::{
    CandleAcceptance, CandleInterval, ClosedCandle, CostEstimate, MarketRegime, SignalArchetype,
    TechnicalEngine, TechnicalError, TechnicalState, TechnicalStrategyConfig, TechnicalTarget,
};
