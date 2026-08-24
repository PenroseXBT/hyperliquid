use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub type Timestamp = u64;
pub type CandidateId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadRequestKind {
    SourceState,
    ExpandedSourceState,
    SourceFills,
    MarketMids,
    ExchangeMetadata,
    OrderBook,
    FollowerState,
    FollowerOpenOrders,
}

impl ReadRequestKind {
    pub const REQUIRED: [Self; 8] = [
        Self::SourceState,
        Self::ExpandedSourceState,
        Self::SourceFills,
        Self::MarketMids,
        Self::ExchangeMetadata,
        Self::OrderBook,
        Self::FollowerState,
        Self::FollowerOpenOrders,
    ];

    fn permits_reserved_budget(self) -> bool {
        matches!(self, Self::FollowerState | Self::FollowerOpenOrders)
    }

    pub fn is_source_state(self) -> bool {
        matches!(self, Self::SourceState | Self::ExpandedSourceState)
    }

    pub fn uses_source_polling_budget(self) -> bool {
        self.is_source_state() || self == Self::SourceFills
    }

    fn is_replaceable_refresh(self) -> bool {
        matches!(
            self,
            Self::SourceState
                | Self::ExpandedSourceState
                | Self::MarketMids
                | Self::ExchangeMetadata
                | Self::OrderBook
                | Self::FollowerState
                | Self::FollowerOpenOrders
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequestSubject {
    Candidate(CandidateId),
    CandidateFillRange {
        candidate: CandidateId,
        start_time_ms: Timestamp,
        end_time_ms: Timestamp,
    },
    Follower(String),
    Asset(String),
    Market,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestKey {
    pub subject: RequestSubject,
    pub kind: ReadRequestKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetClass {
    Normal,
    SourcePolling,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTier {
    Active,
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledReadRequest {
    pub key: RequestKey,
    pub kind: ReadRequestKind,
    pub candidate_id: Option<CandidateId>,
    pub source_tier: Option<SourceTier>,
    pub weight: u32,
    pub priority: RequestPriority,
    pub budget_class: BudgetClass,
    pub created_at: Timestamp,
    pub not_before: Timestamp,
    pub expires_at: Timestamp,
    pub attempt: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadApiPolicy {
    pub total_weight_per_window: u32,
    pub reserved_weight_per_window: u32,
    /// Portion of the noncritical allowance reserved for source-state reads.
    pub source_polling_weight_per_window: u32,
    pub window_ms: u64,
    pub max_concurrency: usize,
    pub queue_capacity: usize,
    pub endpoint_weights: BTreeMap<ReadRequestKind, u32>,
    pub source_tier_dispatch_targets_per_window: BTreeMap<SourceTier, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyError(String);

impl Display for PolicyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for PolicyError {}

impl ReadApiPolicy {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        let bytes = std::fs::read(path).map_err(|error| PolicyError(error.to_string()))?;
        let policy = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| PolicyError(error.to_string()))?;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.total_weight_per_window == 0 {
            return Err(PolicyError("total weight must be positive".to_string()));
        }
        if self.reserved_weight_per_window >= self.total_weight_per_window {
            return Err(PolicyError(
                "reserved weight must be below total weight".to_string(),
            ));
        }
        if self.window_ms == 0 || self.max_concurrency == 0 || self.queue_capacity == 0 {
            return Err(PolicyError(
                "window, concurrency, and queue capacity must be positive".to_string(),
            ));
        }
        let normal_capacity = self
            .total_weight_per_window
            .checked_sub(self.reserved_weight_per_window)
            .ok_or_else(|| PolicyError("normal budget underflow".to_string()))?;
        if self.source_polling_weight_per_window == 0
            || self.source_polling_weight_per_window >= normal_capacity
        {
            return Err(PolicyError(
                "source polling budget must be positive and below normal capacity".to_string(),
            ));
        }
        for tier in [SourceTier::Active, SourceTier::Inactive] {
            if self
                .source_tier_dispatch_targets_per_window
                .get(&tier)
                .copied()
                .unwrap_or(0)
                == 0
            {
                return Err(PolicyError(format!(
                    "missing positive dispatch target for {tier:?}"
                )));
            }
        }
        for kind in ReadRequestKind::REQUIRED {
            let weight = self
                .endpoint_weights
                .get(&kind)
                .ok_or_else(|| PolicyError(format!("missing endpoint weight for {kind:?}")))?;
            if *weight == 0 {
                return Err(PolicyError(format!(
                    "endpoint weight must be positive for {kind:?}"
                )));
            }
            if *weight > normal_capacity {
                return Err(PolicyError(format!(
                    "normal request {kind:?} exceeds nonreserved capacity"
                )));
            }
        }
        u128::from(self.total_weight_per_window)
            .checked_mul(u128::from(self.window_ms))
            .ok_or_else(|| PolicyError("budget duration overflow".to_string()))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn request(
        &self,
        key: RequestKey,
        priority: RequestPriority,
        budget_class: BudgetClass,
        source_tier: Option<SourceTier>,
        created_at: Timestamp,
        not_before: Timestamp,
        expires_at: Timestamp,
        attempt: u32,
    ) -> Result<ScheduledReadRequest, PolicyError> {
        if budget_class == BudgetClass::Critical && !key.kind.permits_reserved_budget() {
            return Err(PolicyError(format!(
                "reserved budget is not permitted for {:?}",
                key.kind
            )));
        }
        if key.kind.uses_source_polling_budget() != (budget_class == BudgetClass::SourcePolling) {
            return Err(PolicyError(
                "source_state requires the source polling budget class".to_string(),
            ));
        }
        let weight = *self
            .endpoint_weights
            .get(&key.kind)
            .ok_or_else(|| PolicyError(format!("missing endpoint weight for {:?}", key.kind)))?;
        let candidate_id = match &key.subject {
            RequestSubject::Candidate(candidate) => Some(candidate.clone()),
            RequestSubject::CandidateFillRange { candidate, .. } => Some(candidate.clone()),
            RequestSubject::Follower(_) | RequestSubject::Asset(_) | RequestSubject::Market => None,
        };
        if key.kind.uses_source_polling_budget() != source_tier.is_some() {
            return Err(PolicyError(
                "source_state requires exactly one source tier".to_string(),
            ));
        }
        Ok(ScheduledReadRequest {
            kind: key.kind,
            key,
            candidate_id,
            source_tier,
            weight,
            priority,
            budget_class,
            created_at,
            not_before,
            expires_at,
            attempt,
        })
    }
}

pub trait Clock: Clone + Send + Sync + 'static {
    fn now_ms(&self) -> Timestamp;
}

#[derive(Debug, Clone, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    pub fn new(now_ms: Timestamp) -> Self {
        Self(Arc::new(AtomicU64::new(now_ms)))
    }

    pub fn set(&self, now_ms: Timestamp) {
        self.0.store(now_ms, AtomicOrdering::SeqCst);
    }

    pub fn advance(&self, delta_ms: u64) -> Result<(), PolicyError> {
        self.0
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |current| {
                current.checked_add(delta_ms)
            })
            .map(|_| ())
            .map_err(|_| PolicyError("manual clock overflow".to_string()))
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> Timestamp {
        self.0.load(AtomicOrdering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct MonotonicClock {
    epoch: Instant,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl Clock for MonotonicClock {
    fn now_ms(&self) -> Timestamp {
        self.epoch.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDecision {
    Charged,
    Insufficient,
    Overweight,
}

#[derive(Debug)]
pub struct WeightedTokenBucket {
    total_capacity_units: u128,
    normal_capacity_units: u128,
    source_capacity_units: u128,
    enrichment_capacity_units: u128,
    total_units: u128,
    normal_units: u128,
    source_units: u128,
    enrichment_units: u128,
    total_refill_per_ms: u128,
    normal_refill_per_ms: u128,
    source_refill_per_ms: u128,
    enrichment_refill_per_ms: u128,
    window_ms: u64,
    last_refill_ms: Timestamp,
    accounting_window_started_ms: Timestamp,
    total_consumed_in_window: u32,
    normal_consumed_in_window: u32,
    source_consumed_in_window: u32,
    enrichment_consumed_in_window: u32,
}

impl WeightedTokenBucket {
    pub fn new(policy: &ReadApiPolicy, now_ms: Timestamp) -> Result<Self, PolicyError> {
        policy.validate()?;
        let window = u128::from(policy.window_ms);
        let normal_weight = policy
            .total_weight_per_window
            .checked_sub(policy.reserved_weight_per_window)
            .ok_or_else(|| PolicyError("normal budget underflow".to_string()))?;
        let total_capacity_units = u128::from(policy.total_weight_per_window)
            .checked_mul(window)
            .ok_or_else(|| PolicyError("total budget overflow".to_string()))?;
        let normal_capacity_units = u128::from(normal_weight)
            .checked_mul(window)
            .ok_or_else(|| PolicyError("normal budget overflow".to_string()))?;
        let source_weight = policy.source_polling_weight_per_window;
        let enrichment_weight = normal_weight
            .checked_sub(source_weight)
            .ok_or_else(|| PolicyError("enrichment budget underflow".to_string()))?;
        let source_capacity_units = u128::from(source_weight)
            .checked_mul(window)
            .ok_or_else(|| PolicyError("source budget overflow".to_string()))?;
        let enrichment_capacity_units = u128::from(enrichment_weight)
            .checked_mul(window)
            .ok_or_else(|| PolicyError("enrichment budget overflow".to_string()))?;
        Ok(Self {
            total_capacity_units,
            normal_capacity_units,
            source_capacity_units,
            enrichment_capacity_units,
            total_units: total_capacity_units,
            normal_units: normal_capacity_units,
            source_units: source_capacity_units,
            enrichment_units: enrichment_capacity_units,
            total_refill_per_ms: u128::from(policy.total_weight_per_window),
            normal_refill_per_ms: u128::from(normal_weight),
            source_refill_per_ms: u128::from(source_weight),
            enrichment_refill_per_ms: u128::from(enrichment_weight),
            window_ms: policy.window_ms,
            last_refill_ms: now_ms,
            accounting_window_started_ms: now_ms,
            total_consumed_in_window: 0,
            normal_consumed_in_window: 0,
            source_consumed_in_window: 0,
            enrichment_consumed_in_window: 0,
        })
    }

    pub fn try_charge(
        &mut self,
        class: BudgetClass,
        weight: u32,
        now_ms: Timestamp,
    ) -> BudgetDecision {
        self.refill(now_ms);
        let requested = match u128::from(weight).checked_mul(u128::from(self.window_ms)) {
            Some(value) => value,
            None => return BudgetDecision::Overweight,
        };
        if requested > self.total_capacity_units
            || (class != BudgetClass::Critical && requested > self.normal_capacity_units)
            || (class == BudgetClass::SourcePolling && requested > self.source_capacity_units)
            || (class == BudgetClass::Normal && requested > self.enrichment_capacity_units)
        {
            return BudgetDecision::Overweight;
        }
        let requested_weight = weight;
        let total_capacity_weight = (self.total_capacity_units / u128::from(self.window_ms)) as u32;
        let normal_capacity_weight =
            (self.normal_capacity_units / u128::from(self.window_ms)) as u32;
        if self
            .total_consumed_in_window
            .checked_add(requested_weight)
            .is_none_or(|consumed| consumed > total_capacity_weight)
            || (class != BudgetClass::Critical
                && self
                    .normal_consumed_in_window
                    .checked_add(requested_weight)
                    .is_none_or(|consumed| consumed > normal_capacity_weight))
            || (class == BudgetClass::SourcePolling
                && self
                    .source_consumed_in_window
                    .checked_add(requested_weight)
                    .is_none_or(|consumed| {
                        consumed > (self.source_capacity_units / u128::from(self.window_ms)) as u32
                    }))
            || (class == BudgetClass::Normal
                && self
                    .enrichment_consumed_in_window
                    .checked_add(requested_weight)
                    .is_none_or(|consumed| {
                        consumed
                            > (self.enrichment_capacity_units / u128::from(self.window_ms)) as u32
                    }))
        {
            return BudgetDecision::Insufficient;
        }
        if requested > self.total_units
            || (class != BudgetClass::Critical && requested > self.normal_units)
            || (class == BudgetClass::SourcePolling && requested > self.source_units)
            || (class == BudgetClass::Normal && requested > self.enrichment_units)
        {
            return BudgetDecision::Insufficient;
        }
        self.total_units -= requested;
        self.total_consumed_in_window += requested_weight;
        if class != BudgetClass::Critical {
            self.normal_units -= requested;
            self.normal_consumed_in_window += requested_weight;
        }
        match class {
            BudgetClass::SourcePolling => {
                self.source_units -= requested;
                self.source_consumed_in_window += requested_weight;
            }
            BudgetClass::Normal => {
                self.enrichment_units -= requested;
                self.enrichment_consumed_in_window += requested_weight;
            }
            BudgetClass::Critical => {}
        }
        BudgetDecision::Charged
    }

    pub fn available_total_weight(&mut self, now_ms: Timestamp) -> u32 {
        self.refill(now_ms);
        let token_weight = (self.total_units / u128::from(self.window_ms)) as u32;
        let capacity_weight = (self.total_capacity_units / u128::from(self.window_ms)) as u32;
        token_weight.min(capacity_weight.saturating_sub(self.total_consumed_in_window))
    }

    pub fn available_normal_weight(&mut self, now_ms: Timestamp) -> u32 {
        self.refill(now_ms);
        let token_weight = (self.normal_units / u128::from(self.window_ms)) as u32;
        let capacity_weight = (self.normal_capacity_units / u128::from(self.window_ms)) as u32;
        token_weight.min(capacity_weight.saturating_sub(self.normal_consumed_in_window))
    }

    fn refund(&mut self, class: BudgetClass, weight: u32, now_ms: Timestamp) {
        self.refill(now_ms);
        let units = u128::from(weight).saturating_mul(u128::from(self.window_ms));
        self.total_units = self
            .total_units
            .saturating_add(units)
            .min(self.total_capacity_units);
        self.total_consumed_in_window = self.total_consumed_in_window.saturating_sub(weight);
        if class != BudgetClass::Critical {
            self.normal_units = self
                .normal_units
                .saturating_add(units)
                .min(self.normal_capacity_units);
            self.normal_consumed_in_window = self.normal_consumed_in_window.saturating_sub(weight);
        }
        match class {
            BudgetClass::SourcePolling => {
                self.source_units = self
                    .source_units
                    .saturating_add(units)
                    .min(self.source_capacity_units);
                self.source_consumed_in_window =
                    self.source_consumed_in_window.saturating_sub(weight);
            }
            BudgetClass::Normal => {
                self.enrichment_units = self
                    .enrichment_units
                    .saturating_add(units)
                    .min(self.enrichment_capacity_units);
                self.enrichment_consumed_in_window =
                    self.enrichment_consumed_in_window.saturating_sub(weight);
            }
            BudgetClass::Critical => {}
        }
    }

    fn refill(&mut self, now_ms: Timestamp) {
        let elapsed = now_ms.saturating_sub(self.last_refill_ms);
        if elapsed == 0 {
            return;
        }
        self.total_units = self
            .total_units
            .saturating_add(u128::from(elapsed).saturating_mul(self.total_refill_per_ms))
            .min(self.total_capacity_units);
        self.normal_units = self
            .normal_units
            .saturating_add(u128::from(elapsed).saturating_mul(self.normal_refill_per_ms))
            .min(self.normal_capacity_units);
        self.source_units = self
            .source_units
            .saturating_add(u128::from(elapsed).saturating_mul(self.source_refill_per_ms))
            .min(self.source_capacity_units);
        self.enrichment_units = self
            .enrichment_units
            .saturating_add(u128::from(elapsed).saturating_mul(self.enrichment_refill_per_ms))
            .min(self.enrichment_capacity_units);
        self.last_refill_ms = now_ms;
        if now_ms.saturating_sub(self.accounting_window_started_ms) >= self.window_ms {
            self.accounting_window_started_ms = now_ms;
            self.total_consumed_in_window = 0;
            self.normal_consumed_in_window = 0;
            self.source_consumed_in_window = 0;
            self.enrichment_consumed_in_window = 0;
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub initial_backoff_ms: u64,
    pub maximum_backoff_ms: u64,
    pub maximum_attempts: u32,
    pub maximum_lifetime_ms: u64,
    pub jitter_bps: u32,
    pub deterministic_seed: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlySchedulerConfig {
    pub api: ReadApiPolicy,
    pub retry: RetryPolicy,
    pub freshness: FreshnessPolicy,
}

impl ReadOnlySchedulerConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        let bytes = std::fs::read(path).map_err(|error| PolicyError(error.to_string()))?;
        let config = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| PolicyError(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), PolicyError> {
        self.api.validate()?;
        self.retry.validate()?;
        self.freshness.validate()?;
        Ok(())
    }
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.initial_backoff_ms == 0
            || self.maximum_backoff_ms < self.initial_backoff_ms
            || self.maximum_attempts == 0
            || self.maximum_lifetime_ms == 0
            || self.jitter_bps > 10_000
        {
            return Err(PolicyError("invalid retry policy".to_string()));
        }
        Ok(())
    }

    fn delay_ms(&self, request: &ScheduledReadRequest, failure: &ReadFailure) -> Option<u64> {
        if matches!(
            failure,
            ReadFailure::InvalidResponse | ReadFailure::Permanent
        ) {
            return None;
        }
        if let ReadFailure::RateLimited {
            retry_after_ms: Some(delay),
        } = failure
        {
            return (*delay > 0).then_some(*delay);
        }
        let shift = request.attempt.min(63);
        let exponential = self
            .initial_backoff_ms
            .checked_mul(1_u64.checked_shl(shift).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX)
            .min(self.maximum_backoff_ms);
        let jitter_span = u128::from(exponential) * u128::from(self.jitter_bps) / 10_000;
        if jitter_span == 0 {
            return Some(exponential.max(1));
        }
        let hash = stable_request_hash(&request.key, request.attempt + 1, self.deterministic_seed);
        let width = jitter_span.saturating_mul(2).saturating_add(1);
        let offset = u128::from(hash) % width;
        let base = u128::from(exponential).saturating_sub(jitter_span);
        Some(base.saturating_add(offset).min(u128::from(u64::MAX)) as u64)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadFailure {
    RateLimited { retry_after_ms: Option<u64> },
    Timeout,
    Transport,
    Server,
    InvalidResponse,
    Permanent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    pub received_at: Timestamp,
    pub valid_until: Timestamp,
    pub payload_valid: bool,
    /// Provider weight actually incurred after a response-sized endpoint was
    /// decoded. Requests reserve their policy maximum before dispatch.
    pub actual_weight: u32,
}

pub trait ReadOnlyDataSource: Send + Sync {
    fn execute(
        &self,
        request: ScheduledReadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ReadResponse, ReadFailure>> + Send + '_>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleOutcome {
    Enqueued,
    ReplacedOlder,
    SuccessorRecorded,
    ShedReplaceable,
    RejectedOlder,
    RejectedQueueFull,
    RejectedInvalid,
    RejectedOverweight,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionOutcome {
    CompletedFresh,
    CompletedButStale,
    Superseded,
    RetryScheduled,
    RetryExhausted,
    InvalidResponse,
    PermanentFailure,
}

#[derive(Debug, Default)]
struct QueueState {
    pending: BTreeMap<RequestKey, ScheduledReadRequest>,
    in_flight: BTreeSet<RequestKey>,
    successors: BTreeMap<RequestKey, ScheduledReadRequest>,
    expired_in_queue: BTreeMap<SourceTier, u64>,
    superseded_pending: BTreeMap<SourceTier, u64>,
    superseded_in_flight: BTreeMap<SourceTier, u64>,
    dispatch_count: BTreeMap<SourceTier, u64>,
    dispatch_window_count: BTreeMap<SourceTier, u32>,
    dispatch_window_started_ms: Timestamp,
}

impl QueueState {
    fn queued_len(&self) -> usize {
        self.pending.len() + self.successors.len()
    }

    fn purge_expired(&mut self, now_ms: Timestamp) {
        let expired = self
            .pending
            .values()
            .chain(self.successors.values())
            .filter(|request| request.expires_at <= now_ms)
            .filter_map(|request| request.source_tier)
            .collect::<Vec<_>>();
        for tier in expired {
            *self.expired_in_queue.entry(tier).or_default() += 1;
        }
        self.pending
            .retain(|_, request| request.expires_at > now_ms);
        self.successors
            .retain(|_, request| request.expires_at > now_ms);
    }

    fn release(&mut self, key: &RequestKey) -> bool {
        self.in_flight.remove(key);
        if let Some(successor) = self.successors.remove(key) {
            self.pending.insert(key.clone(), successor);
            true
        } else {
            false
        }
    }
}

impl ScheduledReadRequest {
    fn may_shed_under_backpressure(&self) -> bool {
        self.kind.is_replaceable_refresh() && self.priority != RequestPriority::Critical
    }
}

pub struct ScheduledDispatch {
    request: ScheduledReadRequest,
    permit: Option<OwnedSemaphorePermit>,
    queue: Arc<Mutex<QueueState>>,
    active: bool,
}

impl ScheduledDispatch {
    pub fn request(&self) -> &ScheduledReadRequest {
        &self.request
    }

    fn release(&mut self) -> bool {
        if !self.active {
            return false;
        }
        let successor = self
            .queue
            .lock()
            .expect("scheduler queue mutex poisoned")
            .release(&self.request.key);
        self.permit.take();
        self.active = false;
        successor
    }
}

impl Drop for ScheduledDispatch {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct RequestScheduler<C: Clock> {
    clock: C,
    policy: ReadApiPolicy,
    budget: Mutex<WeightedTokenBucket>,
    concurrency: Arc<Semaphore>,
    queue: Arc<Mutex<QueueState>>,
    retry_policy: RetryPolicy,
}

impl<C: Clock> RequestScheduler<C> {
    pub fn new(
        clock: C,
        policy: ReadApiPolicy,
        retry_policy: RetryPolicy,
    ) -> Result<Self, PolicyError> {
        policy.validate()?;
        retry_policy.validate()?;
        let budget = WeightedTokenBucket::new(&policy, clock.now_ms())?;
        Ok(Self {
            clock,
            concurrency: Arc::new(Semaphore::new(policy.max_concurrency)),
            queue: Arc::new(Mutex::new(QueueState::default())),
            budget: Mutex::new(budget),
            policy,
            retry_policy,
        })
    }

    pub fn schedule(&self, request: ScheduledReadRequest) -> ScheduleOutcome {
        let now_ms = self.clock.now_ms();
        if let Some(outcome) = self.validate_request(&request, now_ms) {
            return outcome;
        }
        let mut queue = self.queue.lock().expect("scheduler queue mutex poisoned");
        queue.purge_expired(now_ms);
        if now_ms.saturating_sub(queue.dispatch_window_started_ms) >= self.policy.window_ms {
            queue.dispatch_window_started_ms = now_ms;
            queue.dispatch_window_count.clear();
        }

        if queue.in_flight.contains(&request.key) {
            if let Some(existing) = queue.successors.get(&request.key) {
                if !is_newer(&request, existing) {
                    return ScheduleOutcome::RejectedOlder;
                }
                let existing_tier = existing.source_tier;
                queue.successors.insert(request.key.clone(), request);
                if let Some(tier) = existing_tier {
                    *queue.superseded_in_flight.entry(tier).or_default() += 1;
                }
                return ScheduleOutcome::SuccessorRecorded;
            }
            if !make_queue_room(&mut queue, self.policy.queue_capacity, &request) {
                if request.may_shed_under_backpressure() {
                    return ScheduleOutcome::ShedReplaceable;
                }
                return ScheduleOutcome::RejectedQueueFull;
            }
            if let Some(tier) = request.source_tier {
                *queue.superseded_in_flight.entry(tier).or_default() += 1;
            }
            queue.successors.insert(request.key.clone(), request);
            return ScheduleOutcome::SuccessorRecorded;
        }

        if let Some(existing) = queue.pending.get(&request.key) {
            if !is_newer(&request, existing) {
                return ScheduleOutcome::RejectedOlder;
            }
            let existing_tier = existing.source_tier;
            queue.pending.insert(request.key.clone(), request);
            if let Some(tier) = existing_tier {
                *queue.superseded_pending.entry(tier).or_default() += 1;
            }
            return ScheduleOutcome::ReplacedOlder;
        }

        if !make_queue_room(&mut queue, self.policy.queue_capacity, &request) {
            if request.may_shed_under_backpressure() {
                return ScheduleOutcome::ShedReplaceable;
            }
            return ScheduleOutcome::RejectedQueueFull;
        }
        queue.pending.insert(request.key.clone(), request);
        ScheduleOutcome::Enqueued
    }

    pub fn begin_next(&self) -> Option<ScheduledDispatch> {
        let permit = Arc::clone(&self.concurrency).try_acquire_owned().ok()?;
        let now_ms = self.clock.now_ms();
        let mut queue = self.queue.lock().expect("scheduler queue mutex poisoned");
        queue.purge_expired(now_ms);
        let ordered = queue
            .pending
            .values()
            .filter(|request| request.not_before <= now_ms)
            .cloned()
            .collect::<Vec<_>>();
        let mut ordered = ordered;
        ordered.sort_by(|left, right| {
            dispatch_order_with_fairness(
                left,
                right,
                &queue.dispatch_window_count,
                &self.policy.source_tier_dispatch_targets_per_window,
            )
        });

        let mut chosen = None;
        for request in ordered {
            let decision = self
                .budget
                .lock()
                .expect("scheduler budget mutex poisoned")
                .try_charge(request.budget_class, request.weight, now_ms);
            if decision == BudgetDecision::Charged {
                chosen = Some(request);
                break;
            }
        }
        let request = chosen?;
        queue.pending.remove(&request.key);
        queue.in_flight.insert(request.key.clone());
        if let Some(tier) = request.source_tier {
            *queue.dispatch_count.entry(tier).or_default() += 1;
            *queue.dispatch_window_count.entry(tier).or_default() += 1;
        }
        Some(ScheduledDispatch {
            request,
            permit: Some(permit),
            queue: Arc::clone(&self.queue),
            active: true,
        })
    }

    pub fn finish(
        &self,
        mut dispatch: ScheduledDispatch,
        result: Result<ReadResponse, ReadFailure>,
    ) -> ExecutionOutcome {
        let request = dispatch.request.clone();
        let actual_weight = result
            .as_ref()
            .ok()
            .map_or(request.weight, |response| response.actual_weight);
        let weight_valid = actual_weight > 0 && actual_weight <= request.weight;
        if weight_valid && actual_weight < request.weight {
            self.budget
                .lock()
                .expect("scheduler budget mutex poisoned")
                .refund(
                    request.budget_class,
                    request.weight - actual_weight,
                    self.clock.now_ms(),
                );
        }
        if dispatch.release() {
            return ExecutionOutcome::Superseded;
        }
        let now_ms = self.clock.now_ms();
        match result {
            Ok(response) => {
                if !weight_valid {
                    ExecutionOutcome::InvalidResponse
                } else if response.payload_valid
                    && response.received_at >= request.created_at
                    && response.received_at <= response.valid_until
                    && now_ms <= response.valid_until
                    && now_ms < request.expires_at
                {
                    ExecutionOutcome::CompletedFresh
                } else if response.payload_valid {
                    ExecutionOutcome::CompletedButStale
                } else {
                    ExecutionOutcome::InvalidResponse
                }
            }
            Err(ReadFailure::InvalidResponse) => ExecutionOutcome::InvalidResponse,
            Err(ReadFailure::Permanent) => ExecutionOutcome::PermanentFailure,
            Err(failure) => self.schedule_retry(request, failure, now_ms),
        }
    }

    pub async fn execute_next<D: ReadOnlyDataSource>(
        &self,
        data_source: &D,
    ) -> Option<ExecutionOutcome> {
        let dispatch = self.begin_next()?;
        let request = dispatch.request.clone();
        let result = data_source.execute(request).await;
        Some(self.finish(dispatch, result))
    }

    pub fn health(&self) -> SchedulerHealth {
        let now_ms = self.clock.now_ms();
        let queue = self.queue.lock().expect("scheduler queue mutex poisoned");
        let mut budget = self.budget.lock().expect("scheduler budget mutex poisoned");
        SchedulerHealth {
            pending: queue.pending.len(),
            successors: queue.successors.len(),
            in_flight: queue.in_flight.len(),
            queue_capacity: self.policy.queue_capacity,
            available_total_weight: budget.available_total_weight(now_ms),
            available_normal_weight: budget.available_normal_weight(now_ms),
            available_concurrency: self.concurrency.available_permits(),
            expired_in_queue_by_tier: queue.expired_in_queue.clone(),
            superseded_pending_by_tier: queue.superseded_pending.clone(),
            superseded_in_flight_by_tier: queue.superseded_in_flight.clone(),
            dispatch_count_by_tier: queue.dispatch_count.clone(),
            oldest_pending_age_ms_by_tier: oldest_pending_age(&queue, now_ms),
        }
    }

    fn validate_request(
        &self,
        request: &ScheduledReadRequest,
        now_ms: Timestamp,
    ) -> Option<ScheduleOutcome> {
        let Some(policy_weight) = self.policy.endpoint_weights.get(&request.kind) else {
            return Some(ScheduleOutcome::RejectedInvalid);
        };
        if request.kind != request.key.kind
            || request.weight == 0
            || request.weight != *policy_weight
            || request.not_before < request.created_at
            || request.expires_at <= request.created_at
            || (request.budget_class == BudgetClass::Critical
                && !request.kind.permits_reserved_budget())
            || (request.kind.uses_source_polling_budget()
                != (request.budget_class == BudgetClass::SourcePolling))
            || request.candidate_id
                != match &request.key.subject {
                    RequestSubject::Candidate(candidate) => Some(candidate.clone()),
                    RequestSubject::CandidateFillRange { candidate, .. } => Some(candidate.clone()),
                    RequestSubject::Follower(_)
                    | RequestSubject::Asset(_)
                    | RequestSubject::Market => None,
                }
            || request.kind.uses_source_polling_budget() != request.source_tier.is_some()
        {
            return Some(ScheduleOutcome::RejectedInvalid);
        }
        if request.expires_at <= now_ms {
            return Some(ScheduleOutcome::Expired);
        }
        let normal_capacity =
            self.policy.total_weight_per_window - self.policy.reserved_weight_per_window;
        if request.weight > self.policy.total_weight_per_window
            || (request.budget_class != BudgetClass::Critical && request.weight > normal_capacity)
        {
            return Some(ScheduleOutcome::RejectedOverweight);
        }
        None
    }

    fn schedule_retry(
        &self,
        mut request: ScheduledReadRequest,
        failure: ReadFailure,
        now_ms: Timestamp,
    ) -> ExecutionOutcome {
        let next_attempt = match request.attempt.checked_add(1) {
            Some(attempt) if attempt < self.retry_policy.maximum_attempts => attempt,
            _ => return ExecutionOutcome::RetryExhausted,
        };
        let elapsed = now_ms.saturating_sub(request.created_at);
        if elapsed >= self.retry_policy.maximum_lifetime_ms {
            return ExecutionOutcome::RetryExhausted;
        }
        let Some(delay) = self.retry_policy.delay_ms(&request, &failure) else {
            return match failure {
                ReadFailure::InvalidResponse => ExecutionOutcome::InvalidResponse,
                _ => ExecutionOutcome::PermanentFailure,
            };
        };
        let Some(not_before) = now_ms.checked_add(delay) else {
            return ExecutionOutcome::RetryExhausted;
        };
        if not_before <= now_ms
            || not_before >= request.expires_at
            || not_before.saturating_sub(request.created_at) > self.retry_policy.maximum_lifetime_ms
        {
            return ExecutionOutcome::RetryExhausted;
        }
        request.attempt = next_attempt;
        request.not_before = not_before;
        match self.schedule(request) {
            ScheduleOutcome::Enqueued
            | ScheduleOutcome::ReplacedOlder
            | ScheduleOutcome::SuccessorRecorded
            | ScheduleOutcome::RejectedOlder
            | ScheduleOutcome::ShedReplaceable => ExecutionOutcome::RetryScheduled,
            _ => ExecutionOutcome::RetryExhausted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerHealth {
    pub pending: usize,
    pub successors: usize,
    pub in_flight: usize,
    pub queue_capacity: usize,
    pub available_total_weight: u32,
    pub available_normal_weight: u32,
    pub available_concurrency: usize,
    pub expired_in_queue_by_tier: BTreeMap<SourceTier, u64>,
    pub superseded_pending_by_tier: BTreeMap<SourceTier, u64>,
    pub superseded_in_flight_by_tier: BTreeMap<SourceTier, u64>,
    pub dispatch_count_by_tier: BTreeMap<SourceTier, u64>,
    pub oldest_pending_age_ms_by_tier: BTreeMap<SourceTier, u64>,
}

fn oldest_pending_age(queue: &QueueState, now_ms: Timestamp) -> BTreeMap<SourceTier, u64> {
    let mut ages: BTreeMap<SourceTier, u64> = BTreeMap::new();
    for request in queue.pending.values().chain(queue.successors.values()) {
        if let Some(tier) = request.source_tier {
            let age = now_ms.saturating_sub(request.created_at);
            ages.entry(tier)
                .and_modify(|current| *current = (*current).max(age))
                .or_insert(age);
        }
    }
    ages
}

fn is_newer(incoming: &ScheduledReadRequest, existing: &ScheduledReadRequest) -> bool {
    incoming.created_at > existing.created_at
        || (incoming.created_at == existing.created_at && incoming.attempt > existing.attempt)
}

fn make_queue_room(
    queue: &mut QueueState,
    capacity: usize,
    incoming: &ScheduledReadRequest,
) -> bool {
    if queue.queued_len() < capacity {
        return true;
    }
    if incoming.may_shed_under_backpressure() && shed_stale_replaceable(queue, incoming) {
        return true;
    }
    let pending = queue
        .pending
        .iter()
        .filter(|(_, request)| request.priority != RequestPriority::Critical)
        .map(|(key, request)| (false, key.clone(), request.clone()));
    let successors = queue
        .successors
        .iter()
        .filter(|(_, request)| request.priority != RequestPriority::Critical)
        .map(|(key, request)| (true, key.clone(), request.clone()));
    let Some((is_successor, key, lowest)) = pending
        .chain(successors)
        .min_by(|(_, _, left), (_, _, right)| compare_value(left, right))
    else {
        return false;
    };
    if compare_value(incoming, &lowest) != Ordering::Greater {
        return false;
    }
    if is_successor {
        queue.successors.remove(&key);
    } else {
        queue.pending.remove(&key);
    }
    true
}

fn shed_stale_replaceable(queue: &mut QueueState, incoming: &ScheduledReadRequest) -> bool {
    let pending = queue
        .pending
        .iter()
        .filter(|(_, request)| {
            request.may_shed_under_backpressure() && request.priority <= incoming.priority
        })
        .map(|(key, request)| (false, key.clone(), request.clone()));
    let successors = queue
        .successors
        .iter()
        .filter(|(_, request)| {
            request.may_shed_under_backpressure() && request.priority <= incoming.priority
        })
        .map(|(key, request)| (true, key.clone(), request.clone()));
    let Some((is_successor, key, _)) = pending
        .chain(successors)
        .min_by(|(_, _, left), (_, _, right)| compare_shed_candidate(left, right))
    else {
        return false;
    };
    if is_successor {
        queue.successors.remove(&key);
    } else {
        queue.pending.remove(&key);
    }
    true
}

fn compare_shed_candidate(left: &ScheduledReadRequest, right: &ScheduledReadRequest) -> Ordering {
    left.priority
        .cmp(&right.priority)
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.expires_at.cmp(&right.expires_at))
        .then_with(|| left.key.cmp(&right.key))
}

fn compare_value(left: &ScheduledReadRequest, right: &ScheduledReadRequest) -> Ordering {
    left.priority
        .cmp(&right.priority)
        .then_with(|| right.expires_at.cmp(&left.expires_at))
        .then_with(|| right.created_at.cmp(&left.created_at))
}

fn dispatch_order_with_fairness(
    left: &ScheduledReadRequest,
    right: &ScheduledReadRequest,
    counts: &BTreeMap<SourceTier, u32>,
    targets: &BTreeMap<SourceTier, u32>,
) -> Ordering {
    let priority = right.priority.cmp(&left.priority);
    if priority != Ordering::Equal {
        return priority;
    }
    if let (Some(left_tier), Some(right_tier)) = (left.source_tier, right.source_tier) {
        let left_count = u64::from(counts.get(&left_tier).copied().unwrap_or(0));
        let right_count = u64::from(counts.get(&right_tier).copied().unwrap_or(0));
        let left_target = u64::from(targets[&left_tier]);
        let right_target = u64::from(targets[&right_tier]);
        let fairness = left_count
            .saturating_mul(right_target)
            .cmp(&right_count.saturating_mul(left_target));
        if fairness != Ordering::Equal {
            return fairness;
        }
    }
    left.expires_at
        .cmp(&right.expires_at)
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.key.cmp(&right.key))
}

fn stable_request_hash(key: &RequestKey, attempt: u32, seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64 ^ seed;
    let text = format!("{key:?}:{attempt}");
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSnapshot<T> {
    pub candidate_id: CandidateId,
    pub payload: T,
    pub requested_at: Timestamp,
    pub received_at: Timestamp,
    pub source_observed_at: Option<Timestamp>,
    pub valid_until: Timestamp,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FreshnessPolicy {
    pub configured_max_age_ms: u64,
    pub maximum_future_skew_ms: u64,
}

impl FreshnessPolicy {
    pub fn validate(self) -> Result<Self, PolicyError> {
        if self.configured_max_age_ms == 0 {
            return Err(PolicyError(
                "snapshot maximum age must be positive".to_string(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotAcceptance {
    Accepted,
    InvalidPayload,
    InvalidTimestamps,
    Expired,
    FutureDated,
    OutOfOrder,
}

#[derive(Debug)]
pub struct SnapshotStore<T> {
    policy: FreshnessPolicy,
    accepted: BTreeMap<CandidateId, SourceSnapshot<T>>,
}

impl<T> SnapshotStore<T> {
    pub fn new(policy: FreshnessPolicy) -> Result<Self, PolicyError> {
        Ok(Self {
            policy: policy.validate()?,
            accepted: BTreeMap::new(),
        })
    }

    pub fn accept(
        &mut self,
        snapshot: SourceSnapshot<T>,
        now_ms: Timestamp,
        payload_valid: bool,
    ) -> SnapshotAcceptance {
        if !payload_valid {
            return SnapshotAcceptance::InvalidPayload;
        }
        if snapshot.received_at < snapshot.requested_at
            || snapshot.valid_until < snapshot.received_at
        {
            return SnapshotAcceptance::InvalidTimestamps;
        }
        if now_ms > snapshot.valid_until
            || now_ms.saturating_sub(snapshot.received_at) > self.policy.configured_max_age_ms
        {
            return SnapshotAcceptance::Expired;
        }
        let maximum_future = match now_ms.checked_add(self.policy.maximum_future_skew_ms) {
            Some(value) => value,
            None => return SnapshotAcceptance::InvalidTimestamps,
        };
        if snapshot.requested_at > maximum_future || snapshot.received_at > maximum_future {
            return SnapshotAcceptance::FutureDated;
        }
        if let Some(observed_at) = snapshot.source_observed_at {
            if observed_at > maximum_future {
                return SnapshotAcceptance::FutureDated;
            }
            if now_ms.saturating_sub(observed_at) > self.policy.configured_max_age_ms {
                return SnapshotAcceptance::Expired;
            }
        }
        if let Some(current) = self.accepted.get(&snapshot.candidate_id) {
            if snapshot.sequence <= current.sequence
                || snapshot.received_at < current.received_at
                || snapshot.source_observed_at < current.source_observed_at
            {
                return SnapshotAcceptance::OutOfOrder;
            }
        }
        self.accepted
            .insert(snapshot.candidate_id.clone(), snapshot);
        SnapshotAcceptance::Accepted
    }

    pub fn fresh(&self, candidate_id: &str, now_ms: Timestamp) -> Option<&SourceSnapshot<T>> {
        let snapshot = self.accepted.get(candidate_id)?;
        (now_ms <= snapshot.valid_until
            && now_ms.saturating_sub(snapshot.received_at) <= self.policy.configured_max_age_ms)
            .then_some(snapshot)
    }

    /// Latest validated snapshot, including one that is no longer eligible for
    /// trading consensus. Scheduling tiers may use this only as a cadence hint.
    pub fn latest(&self, candidate_id: &str) -> Option<&SourceSnapshot<T>> {
        self.accepted.get(candidate_id)
    }

    pub fn len(&self) -> usize {
        self.accepted.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accepted.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(total: u32, reserve: u32, concurrency: usize, queue: usize) -> ReadApiPolicy {
        ReadApiPolicy {
            total_weight_per_window: total,
            reserved_weight_per_window: reserve,
            source_polling_weight_per_window: (total - reserve).saturating_sub(2).max(1),
            window_ms: 60_000,
            max_concurrency: concurrency,
            queue_capacity: queue,
            endpoint_weights: ReadRequestKind::REQUIRED
                .into_iter()
                .map(|kind| {
                    (
                        kind,
                        if kind == ReadRequestKind::ExchangeMetadata {
                            20
                        } else {
                            2
                        },
                    )
                })
                .collect(),
            source_tier_dispatch_targets_per_window: [
                (SourceTier::Active, 300),
                (SourceTier::Inactive, 69),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy {
            initial_backoff_ms: 1_000,
            maximum_backoff_ms: 8_000,
            maximum_attempts: 3,
            maximum_lifetime_ms: 20_000,
            jitter_bps: 1_000,
            deterministic_seed: 7,
        }
    }

    fn key(index: usize) -> RequestKey {
        RequestKey {
            subject: RequestSubject::Candidate(format!("candidate-{index:03}")),
            kind: ReadRequestKind::SourceState,
        }
    }

    fn request(
        policy: &ReadApiPolicy,
        index: usize,
        created: u64,
        expires: u64,
    ) -> ScheduledReadRequest {
        policy
            .request(
                key(index),
                RequestPriority::Normal,
                BudgetClass::SourcePolling,
                Some(SourceTier::Active),
                created,
                created,
                expires,
                0,
            )
            .unwrap()
    }

    #[test]
    fn policy_validation_fails_closed() {
        let mut invalid = policy(1_200, 360, 20, 256);
        invalid
            .endpoint_weights
            .remove(&ReadRequestKind::SourceState);
        assert!(invalid.validate().is_err());
        let mut invalid = policy(1_200, 1_200, 20, 256);
        assert!(invalid.validate().is_err());
        invalid.reserved_weight_per_window = 360;
        invalid.max_concurrency = 0;
        assert!(invalid.validate().is_err());
        assert!(policy(1_200, 360, 20, 256)
            .request(
                key(1),
                RequestPriority::Critical,
                BudgetClass::Critical,
                Some(SourceTier::Active),
                0,
                0,
                1_000,
                0,
            )
            .is_err());
    }

    #[test]
    fn weighted_budget_preserves_reserve_and_caps_burst_credit() {
        let policy = policy(100, 30, 2, 10);
        let mut bucket = WeightedTokenBucket::new(&policy, 0).unwrap();
        assert_eq!(
            bucket.try_charge(BudgetClass::Normal, 2, 0),
            BudgetDecision::Charged
        );
        assert_eq!(
            bucket.try_charge(BudgetClass::Normal, 1, 0),
            BudgetDecision::Insufficient
        );
        assert_eq!(
            bucket.try_charge(BudgetClass::SourcePolling, 68, 0),
            BudgetDecision::Charged
        );
        assert_eq!(
            bucket.try_charge(BudgetClass::SourcePolling, 1, 0),
            BudgetDecision::Insufficient
        );
        assert_eq!(
            bucket.try_charge(BudgetClass::Critical, 30, 0),
            BudgetDecision::Charged
        );
        assert_eq!(
            bucket.try_charge(BudgetClass::Critical, 1, 0),
            BudgetDecision::Insufficient
        );
        assert_eq!(bucket.available_total_weight(6_000_000), 100);
        assert_eq!(bucket.available_normal_weight(6_000_000), 70);
    }

    #[test]
    fn response_sized_weight_refunds_only_the_unused_reservation() {
        let mut policy = policy(900, 120, 1, 10);
        policy
            .endpoint_weights
            .insert(ReadRequestKind::SourceState, 120);
        let scheduler =
            RequestScheduler::new(ManualClock::new(0), policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 1, 0, 1_000));
        let dispatch = scheduler.begin_next().unwrap();
        assert_eq!(
            scheduler.finish(
                dispatch,
                Ok(ReadResponse {
                    received_at: 0,
                    valid_until: 1_000,
                    payload_valid: true,
                    actual_weight: 20,
                }),
            ),
            ExecutionOutcome::CompletedFresh
        );
        let health = scheduler.health();
        assert_eq!(health.available_total_weight, 880);
        assert_eq!(health.available_normal_weight, 760);
    }

    #[test]
    fn queue_deduplicates_supersedes_and_stays_bounded() {
        let policy = policy(1_200, 360, 2, 2);
        let clock = ManualClock::new(10);
        let scheduler = RequestScheduler::new(clock, policy.clone(), retry_policy()).unwrap();
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 10, 1_000)),
            ScheduleOutcome::Enqueued
        );
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 10, 1_000)),
            ScheduleOutcome::RejectedOlder
        );
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 11, 1_000)),
            ScheduleOutcome::ReplacedOlder
        );
        assert_eq!(
            scheduler.schedule(request(&policy, 2, 10, 1_000)),
            ScheduleOutcome::Enqueued
        );
        let mut low_value = request(&policy, 3, 12, 1_000);
        low_value.priority = RequestPriority::Low;
        assert_eq!(
            scheduler.schedule(low_value),
            ScheduleOutcome::ShedReplaceable
        );
        let mut high_value = request(&policy, 3, 12, 1_000);
        high_value.priority = RequestPriority::High;
        assert_eq!(scheduler.schedule(high_value), ScheduleOutcome::Enqueued);
        assert_eq!(scheduler.health().pending, 2);
    }

    #[test]
    fn replaceable_refresh_backpressure_sheds_without_capacity_growth() {
        let policy = policy(1_200, 360, 1, 2);
        let clock = ManualClock::new(10);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 10, 1_000)),
            ScheduleOutcome::Enqueued
        );
        assert_eq!(
            scheduler.schedule(request(&policy, 2, 11, 1_000)),
            ScheduleOutcome::Enqueued
        );
        assert_eq!(
            scheduler.schedule(request(&policy, 3, 12, 1_000)),
            ScheduleOutcome::Enqueued
        );
        assert_eq!(scheduler.health().pending, 2);
        clock.set(12);
        let mut dispatched = Vec::new();
        for _ in 0..2 {
            let dispatch = scheduler.begin_next().unwrap();
            dispatched.push(dispatch.request().key.clone());
            drop(dispatch);
        }
        assert!(!dispatched.contains(&key(1)));
        assert!(dispatched.contains(&key(2)));
        assert!(dispatched.contains(&key(3)));
    }

    #[test]
    fn critical_work_still_fails_closed_when_queue_cannot_preserve_it() {
        let policy = policy(1_200, 360, 1, 1);
        let scheduler =
            RequestScheduler::new(ManualClock::new(10), policy.clone(), retry_policy()).unwrap();
        let critical = |id: &str, created_at| {
            policy
                .request(
                    RequestKey {
                        subject: RequestSubject::Follower(id.to_string()),
                        kind: ReadRequestKind::FollowerState,
                    },
                    RequestPriority::Critical,
                    BudgetClass::Critical,
                    None,
                    created_at,
                    created_at,
                    1_000,
                    0,
                )
                .unwrap()
        };
        assert_eq!(
            scheduler.schedule(critical("follower-a", 10)),
            ScheduleOutcome::Enqueued
        );
        assert_eq!(
            scheduler.schedule(critical("follower-b", 11)),
            ScheduleOutcome::RejectedQueueFull
        );
        assert_eq!(scheduler.health().pending, 1);
    }

    #[test]
    fn concurrency_and_successor_are_independently_bounded() {
        let policy = policy(1_200, 360, 1, 10);
        let clock = ManualClock::new(10);
        let scheduler = RequestScheduler::new(clock, policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 1, 10, 1_000));
        let dispatch = scheduler.begin_next().unwrap();
        assert!(scheduler.begin_next().is_none());
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 11, 1_000)),
            ScheduleOutcome::SuccessorRecorded
        );
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 12, 1_000)),
            ScheduleOutcome::SuccessorRecorded
        );
        assert_eq!(scheduler.health().successors, 1);
        drop(dispatch);
        assert_eq!(scheduler.health().in_flight, 0);
        assert_eq!(scheduler.health().pending, 1);
    }

    #[test]
    fn expired_work_never_dispatches() {
        let policy = policy(1_200, 360, 1, 10);
        let clock = ManualClock::new(10);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 1, 10, 20));
        clock.set(20);
        assert!(scheduler.begin_next().is_none());
        assert_eq!(scheduler.health().pending, 0);
    }

    #[test]
    fn rate_limit_retry_is_delayed_bounded_and_supersedable() {
        let policy = policy(1_200, 360, 1, 10);
        let clock = ManualClock::new(10);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 1, 10, 30_000));
        let dispatch = scheduler.begin_next().unwrap();
        assert_eq!(
            scheduler.finish(
                dispatch,
                Err(ReadFailure::RateLimited {
                    retry_after_ms: Some(2_000),
                })
            ),
            ExecutionOutcome::RetryScheduled
        );
        assert!(scheduler.begin_next().is_none());
        assert_eq!(
            scheduler.schedule(request(&policy, 1, 11, 30_000)),
            ScheduleOutcome::ReplacedOlder
        );
        clock.advance(2_000).unwrap();
        let dispatch = scheduler.begin_next().unwrap();
        assert_eq!(dispatch.request().created_at, 11);
    }

    #[test]
    fn transport_failures_release_permits_and_exhaust_retries() {
        let policy = policy(1_200, 360, 1, 10);
        let clock = ManualClock::new(0);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 1, 0, 30_000));
        for attempt in 0..3 {
            let dispatch = scheduler.begin_next().unwrap();
            let outcome = scheduler.finish(dispatch, Err(ReadFailure::Timeout));
            if attempt < 2 {
                assert_eq!(outcome, ExecutionOutcome::RetryScheduled);
                clock.advance(8_000).unwrap();
            } else {
                assert_eq!(outcome, ExecutionOutcome::RetryExhausted);
            }
            assert_eq!(scheduler.health().available_concurrency, 1);
        }
    }

    #[test]
    fn every_terminal_and_cancellation_path_releases_concurrency() {
        let policy = policy(1_200, 360, 1, 10);
        for (index, result) in [
            Ok(ReadResponse {
                received_at: 0,
                valid_until: 1_000,
                payload_valid: true,
                actual_weight: 1,
            }),
            Ok(ReadResponse {
                received_at: 0,
                valid_until: 1_000,
                payload_valid: false,
                actual_weight: 1,
            }),
            Err(ReadFailure::Permanent),
        ]
        .into_iter()
        .enumerate()
        {
            let scheduler =
                RequestScheduler::new(ManualClock::new(0), policy.clone(), retry_policy()).unwrap();
            scheduler.schedule(request(&policy, index, 0, 1_000));
            let dispatch = scheduler.begin_next().unwrap();
            scheduler.finish(dispatch, result);
            assert_eq!(scheduler.health().available_concurrency, 1);
            assert_eq!(scheduler.health().in_flight, 0);
        }

        let scheduler =
            RequestScheduler::new(ManualClock::new(0), policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 9, 0, 1_000));
        drop(scheduler.begin_next().unwrap());
        assert_eq!(scheduler.health().available_concurrency, 1);
        assert_eq!(scheduler.health().in_flight, 0);
    }

    #[test]
    fn panic_unwind_releases_dispatch_permit_and_inflight_key() {
        let policy = policy(1_200, 360, 1, 10);
        let scheduler =
            RequestScheduler::new(ManualClock::new(0), policy.clone(), retry_policy()).unwrap();
        scheduler.schedule(request(&policy, 1, 0, 1_000));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _dispatch = scheduler.begin_next().unwrap();
            panic!("injected transport panic");
        }));
        assert!(result.is_err());
        assert_eq!(scheduler.health().available_concurrency, 1);
        assert_eq!(scheduler.health().in_flight, 0);
    }

    #[test]
    fn snapshots_reject_stale_future_invalid_and_out_of_order_data() {
        let mut store = SnapshotStore::new(FreshnessPolicy {
            configured_max_age_ms: 40_000,
            maximum_future_skew_ms: 1_000,
        })
        .unwrap();
        let snapshot = SourceSnapshot {
            candidate_id: "candidate-001".to_string(),
            payload: 10,
            requested_at: 90,
            received_at: 100,
            source_observed_at: Some(100),
            valid_until: 1_000,
            sequence: 2,
        };
        assert_eq!(
            store.accept(snapshot.clone(), 100, true),
            SnapshotAcceptance::Accepted
        );
        let mut older = snapshot.clone();
        older.sequence = 1;
        assert_eq!(
            store.accept(older, 100, true),
            SnapshotAcceptance::OutOfOrder
        );
        let mut future = snapshot.clone();
        future.sequence = 3;
        future.source_observed_at = Some(1_101);
        assert_eq!(
            store.accept(future, 100, true),
            SnapshotAcceptance::FutureDated
        );
        let mut future_receipt = snapshot.clone();
        future_receipt.sequence = 3;
        future_receipt.requested_at = 1_101;
        future_receipt.received_at = 1_101;
        future_receipt.valid_until = 2_000;
        assert_eq!(
            store.accept(future_receipt, 100, true),
            SnapshotAcceptance::FutureDated
        );
        let mut invalid = snapshot.clone();
        invalid.sequence = 3;
        assert_eq!(
            store.accept(invalid, 100, false),
            SnapshotAcceptance::InvalidPayload
        );
        assert_eq!(store.len(), 1);
        assert!(store.fresh("candidate-001", 1_001).is_none());
    }

    #[test]
    fn all_169_candidates_remain_bounded_under_load_scenarios() {
        let policy = policy(738, 200, 20, 169);
        let clock = ManualClock::new(0);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        for index in 0..169 {
            assert_eq!(
                scheduler.schedule(request(&policy, index, 0, 120_000)),
                ScheduleOutcome::Enqueued
            );
        }
        assert_eq!(scheduler.health().pending, 169);

        for index in 0..169 {
            assert_eq!(
                scheduler.schedule(request(&policy, index, 1, 120_000)),
                ScheduleOutcome::ReplacedOlder
            );
        }
        assert_eq!(scheduler.health().pending, 169);

        clock.set(1);
        let mut active = Vec::new();
        for _ in 0..20 {
            active.push(scheduler.begin_next().unwrap());
        }
        assert!(scheduler.begin_next().is_none());
        for dispatch in active {
            assert_eq!(
                scheduler.finish(dispatch, Err(ReadFailure::Timeout)),
                ExecutionOutcome::RetryScheduled
            );
        }
        assert!(scheduler.health().pending <= 169);
        assert!(scheduler.health().successors <= 169);
        assert!(scheduler.health().in_flight <= 20);

        clock.advance(60_000).unwrap();
        assert!(scheduler.health().available_total_weight <= 738);
        assert!(scheduler.health().available_normal_weight <= 538);
    }

    #[test]
    fn mixed_active_and_inactive_tiers_fit_the_documented_169_source_budget() {
        let policy = policy(1_200, 360, 20, 256);
        let clock = ManualClock::new(0);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        let mut completed = 0;
        for cycle in 0..3 {
            let now = cycle * 20_000;
            clock.set(now);
            for index in 0..100 {
                assert_eq!(
                    scheduler.schedule(request(&policy, index, now, 60_000)),
                    ScheduleOutcome::Enqueued
                );
            }
            if cycle == 0 {
                for index in 100..169 {
                    assert_eq!(
                        scheduler.schedule(request(&policy, index, now, 60_000)),
                        ScheduleOutcome::Enqueued
                    );
                }
            }
            while let Some(dispatch) = scheduler.begin_next() {
                let actual_weight = dispatch.request().weight;
                assert_eq!(
                    scheduler.finish(
                        dispatch,
                        Ok(ReadResponse {
                            received_at: now,
                            valid_until: 60_000,
                            payload_valid: true,
                            actual_weight,
                        })
                    ),
                    ExecutionOutcome::CompletedFresh
                );
                completed += 1;
            }
        }
        assert_eq!(completed, 369);
        let health = scheduler.health();
        assert_eq!(health.available_normal_weight, 102);
        assert_eq!(health.available_total_weight, 462);
        assert_eq!(health.pending, 0);
    }

    #[test]
    fn sustained_rate_limiting_never_creates_immediate_or_unbounded_retries() {
        let policy = policy(1_200, 360, 20, 169);
        let clock = ManualClock::new(0);
        let scheduler =
            RequestScheduler::new(clock.clone(), policy.clone(), retry_policy()).unwrap();
        for index in 0..169 {
            scheduler.schedule(request(&policy, index, 0, 20_000));
        }

        let mut executions = 0;
        for round in 0..3 {
            clock.set(round * 1_000);
            while let Some(dispatch) = scheduler.begin_next() {
                let outcome = scheduler.finish(
                    dispatch,
                    Err(ReadFailure::RateLimited {
                        retry_after_ms: Some(1_000),
                    }),
                );
                assert_eq!(
                    outcome,
                    if round < 2 {
                        ExecutionOutcome::RetryScheduled
                    } else {
                        ExecutionOutcome::RetryExhausted
                    }
                );
                executions += 1;
            }
            assert!(scheduler.health().pending <= 169);
            assert!(scheduler.health().in_flight <= 20);
        }
        assert_eq!(executions, 419);
        // Two enrichment units remain isolated and cannot be borrowed by source retries.
        assert_eq!(scheduler.health().available_normal_weight, 2);
        assert!(scheduler.begin_next().is_none());
    }

    #[test]
    fn weighted_source_tiers_are_both_dispatched_without_starvation() {
        let policy = policy(1_200, 360, 1, 256);
        let clock = ManualClock::new(0);
        let scheduler = RequestScheduler::new(clock, policy.clone(), retry_policy()).unwrap();
        for index in 0..100 {
            assert_eq!(
                scheduler.schedule(request(&policy, index, 0, 60_000)),
                ScheduleOutcome::Enqueued
            );
        }
        for index in 100..169 {
            let mut request = request(&policy, index, 0, 60_000);
            request.source_tier = Some(SourceTier::Inactive);
            assert_eq!(scheduler.schedule(request), ScheduleOutcome::Enqueued);
        }
        let mut first_twenty = BTreeMap::new();
        for _ in 0..20 {
            let dispatch = scheduler.begin_next().unwrap();
            *first_twenty
                .entry(dispatch.request().source_tier.unwrap())
                .or_insert(0_u32) += 1;
            drop(dispatch);
        }
        assert!(first_twenty.get(&SourceTier::Active).copied().unwrap_or(0) >= 15);
        assert!(
            first_twenty
                .get(&SourceTier::Inactive)
                .copied()
                .unwrap_or(0)
                >= 3
        );
        let health = scheduler.health();
        assert_eq!(health.dispatch_count_by_tier.values().sum::<u64>(), 20);
    }

    #[test]
    fn deterministic_seed_and_clock_reproduce_dispatch_order() {
        fn trace() -> Vec<RequestKey> {
            let policy = policy(1_200, 360, 3, 10);
            let scheduler =
                RequestScheduler::new(ManualClock::new(10), policy.clone(), retry_policy())
                    .unwrap();
            for index in [3, 1, 2] {
                scheduler.schedule(request(&policy, index, 10, 1_000));
            }
            (0..3)
                .map(|_| scheduler.begin_next().unwrap().request().key.clone())
                .collect()
        }
        assert_eq!(trace(), trace());
    }
}
