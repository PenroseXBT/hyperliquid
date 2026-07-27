use copytrade_core::decision::PayloadHash;
use copytrade_core::scheduler::SourceTier;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type WallClockTimestamp = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodeStage {
    NotAttempted,
    AddressValidated,
    HttpValidated,
    BodyHashed,
    SchemaDecoded,
    NumericsValidated,
    MetadataResolved,
    SnapshotAccepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestionFailureClassification {
    TransportFailure,
    AccountGenuinelyAbsent,
    UnsupportedValidPayloadVariant,
    MissingRequiredEquity,
    InvalidNumericValue,
    SchemaLayoutMismatch,
    StaleFutureTimestamp,
    AssetMetadataDependencyMissing,
    OtherExplicitDecodeFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateDisposition {
    ValidZeroExposure,
    ValidNonzeroExposure,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestionFailure {
    pub classification: IngestionFailureClassification,
    pub stage: DecodeStage,
    pub reason: String,
}

impl IngestionFailure {
    pub fn new(
        classification: IngestionFailureClassification,
        stage: DecodeStage,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            classification,
            stage,
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateAuditState {
    pub candidate_id: String,
    pub configured_tier: SourceTier,
    pub request_count: u64,
    pub accepted_count: u64,
    pub rejected_count: u64,
    pub first_failure_time: Option<WallClockTimestamp>,
    pub last_failure_time: Option<WallClockTimestamp>,
    pub last_success_time: Option<WallClockTimestamp>,
    pub last_http_status: Option<u16>,
    pub http_status_history: Vec<u16>,
    pub last_payload_hash: Option<PayloadHash>,
    pub payload_hash_history: Vec<PayloadHash>,
    pub last_decode_stage: DecodeStage,
    pub last_failure: Option<IngestionFailure>,
    pub ever_validated: bool,
    pub last_account_value: Option<Decimal>,
    pub last_position_count: Option<usize>,
    pub disposition: CandidateDisposition,
}

impl CandidateAuditState {
    pub fn new(candidate_id: String, configured_tier: SourceTier) -> Self {
        Self {
            candidate_id,
            configured_tier,
            request_count: 0,
            accepted_count: 0,
            rejected_count: 0,
            first_failure_time: None,
            last_failure_time: None,
            last_success_time: None,
            last_http_status: None,
            http_status_history: Vec::new(),
            last_payload_hash: None,
            payload_hash_history: Vec::new(),
            last_decode_stage: DecodeStage::NotAttempted,
            last_failure: None,
            ever_validated: false,
            last_account_value: None,
            last_position_count: None,
            disposition: CandidateDisposition::Rejected,
        }
    }

    pub fn record_request(&mut self) {
        self.request_count = self.request_count.saturating_add(1);
        self.last_decode_stage = DecodeStage::AddressValidated;
    }

    pub fn record_http(&mut self, status: u16) {
        self.last_http_status = Some(status);
        push_bounded(&mut self.http_status_history, status);
        self.last_decode_stage = DecodeStage::HttpValidated;
    }

    pub fn record_payload(&mut self, hash: PayloadHash) {
        self.last_payload_hash = Some(hash);
        push_bounded(&mut self.payload_hash_history, hash);
        self.last_decode_stage = DecodeStage::BodyHashed;
    }

    pub fn record_success(
        &mut self,
        at: WallClockTimestamp,
        account_value: Decimal,
        position_count: usize,
    ) {
        self.accepted_count = self.accepted_count.saturating_add(1);
        self.last_success_time = Some(at);
        self.last_decode_stage = DecodeStage::SnapshotAccepted;
        self.last_failure = None;
        self.ever_validated = true;
        self.last_account_value = Some(account_value);
        self.last_position_count = Some(position_count);
        self.disposition = if position_count == 0 {
            CandidateDisposition::ValidZeroExposure
        } else {
            CandidateDisposition::ValidNonzeroExposure
        };
    }

    pub fn record_failure(&mut self, at: WallClockTimestamp, failure: IngestionFailure) {
        self.rejected_count = self.rejected_count.saturating_add(1);
        self.first_failure_time.get_or_insert(at);
        self.last_failure_time = Some(at);
        self.last_decode_stage = failure.stage;
        self.last_failure = Some(failure);
    }
}

pub type CandidateAuditSnapshot = BTreeMap<String, CandidateAuditState>;

fn push_bounded<T>(values: &mut Vec<T>, value: T) {
    const MAX_HISTORY: usize = 64;
    if values.len() == MAX_HISTORY {
        values.remove(0);
    }
    values.push(value);
}
