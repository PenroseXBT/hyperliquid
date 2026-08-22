#![forbid(unsafe_code)]

pub mod production;
pub mod reconciliation;
pub mod transport;

use ethers::contract::{Eip712, EthAbiType};
use ethers::core::k256::{
    ecdsa::signature::digest::{FixedOutput, FixedOutputReset, HashMarker, Reset, Update},
    elliptic_curve::{generic_array::GenericArray, FieldBytes},
    Secp256k1,
};
use ethers::prelude::k256::sha2::{
    self,
    digest::{Digest, Output, OutputSizeUser},
};
use ethers::signers::LocalWallet;
use ethers::types::{transaction::eip712::Eip712 as _, Signature, H256, U256};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedExecutionIntent {
    pub decision_id: String,
    pub target_version: u64,
    pub root_cloid: String,
    pub cloid: String,
    pub parent_cloid: Option<String>,
    pub continuation_generation: u32,
    pub asset: String,
    pub asset_index: u32,
    pub is_buy: bool,
    pub reduce_only: bool,
    pub limit_price: Decimal,
    pub quantity: Decimal,
    pub risk_projection_hash: String,
    pub risk_policy_hash: String,
    pub configuration_hash: String,
    pub release_manifest_hash: String,
    pub decision_reference_price: Decimal,
    pub decision_timestamp_ms: u64,
    pub expires_at_mono: u64,
    pub canonical_intent_hash: String,
}

impl ApprovedExecutionIntent {
    pub fn validate(&self) -> Result<(), SignerError> {
        if self.decision_id.len() != 64
            || !is_lower_hex(&self.decision_id)
            || self.risk_projection_hash.len() != 64
            || !is_lower_hex(&self.risk_projection_hash)
            || self.risk_policy_hash.len() != 64
            || !is_lower_hex(&self.risk_policy_hash)
            || self.configuration_hash.len() != 64
            || !is_lower_hex(&self.configuration_hash)
            || self.release_manifest_hash.len() != 64
            || !is_lower_hex(&self.release_manifest_hash)
            || self.canonical_intent_hash.len() != 64
            || !is_lower_hex(&self.canonical_intent_hash)
            || !valid_cloid(&self.root_cloid)
            || !valid_cloid(&self.cloid)
            || self
                .parent_cloid
                .as_deref()
                .is_some_and(|value| !valid_cloid(value))
            || self.asset.is_empty()
            || self.limit_price <= Decimal::ZERO
            || self.quantity <= Decimal::ZERO
            || self.decision_reference_price <= Decimal::ZERO
            || self.expires_at_mono == 0
            || self.limit_price.scale() > 8
            || self.quantity.scale() > 8
        {
            return Err(SignerError::InvalidIntent);
        }
        if self.continuation_generation == 0 && self.parent_cloid.is_some() {
            return Err(SignerError::InvalidContinuation);
        }
        if self.continuation_generation > 0 && self.parent_cloid.is_none() {
            return Err(SignerError::InvalidContinuation);
        }
        Ok(())
    }
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_cloid(value: &str) -> bool {
    value.len() == 34 && value.starts_with("0x") && is_lower_hex(&value[2..])
}

#[derive(Debug)]
pub struct NonceAllocator {
    next: AtomicU64,
}

impl NonceAllocator {
    pub fn new(initial: u64) -> Self {
        Self {
            next: AtomicU64::new(initial),
        }
    }

    pub fn next(&self, unix_millis: u64) -> Result<u64, SignerError> {
        loop {
            let observed = self.next.load(Ordering::Acquire);
            let nonce = observed.max(unix_millis);
            let following = nonce.checked_add(1).ok_or(SignerError::NonceOverflow)?;
            if self
                .next
                .compare_exchange(observed, following, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(nonce);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionState {
    Authorized,
    AuthorizedNotSubmitted,
    NonceAllocated {
        nonce: u64,
    },
    Signed {
        nonce: u64,
    },
    SubmissionStarted {
        nonce: u64,
    },
    Acknowledged {
        order_id: String,
    },
    PartiallyFilled {
        order_id: String,
        filled: Decimal,
    },
    Filled {
        order_id: String,
        filled: Decimal,
    },
    Cancelled {
        order_id: Option<String>,
        filled: Decimal,
    },
    Rejected {
        reason: String,
    },
    UnknownResult {
        nonce: u64,
    },
    ReconciledNotFound,
}

impl SubmissionState {
    fn unresolved(&self) -> bool {
        matches!(
            self,
            Self::Authorized
                | Self::AuthorizedNotSubmitted
                | Self::NonceAllocated { .. }
                | Self::Signed { .. }
                | Self::SubmissionStarted { .. }
                | Self::Acknowledged { .. }
                | Self::PartiallyFilled { .. }
                | Self::UnknownResult { .. }
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubmissionRegistry {
    by_cloid: BTreeMap<String, (ApprovedExecutionIntent, SubmissionState)>,
    unknown_assets: BTreeSet<String>,
    not_found_confirmations: BTreeMap<String, Vec<u64>>,
    maximum_entries: usize,
    #[serde(default)]
    history: Vec<SubmissionTransitionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionTransitionRecord {
    pub cloid: String,
    pub state: SubmissionState,
    pub recorded_at_ms: u64,
}

impl Default for SubmissionRegistry {
    fn default() -> Self {
        Self::with_capacity(10_000).expect("default registry capacity is valid")
    }
}

impl SubmissionRegistry {
    pub fn with_capacity(maximum_entries: usize) -> Result<Self, SignerError> {
        if maximum_entries == 0 {
            return Err(SignerError::InvalidConfiguration);
        }
        Ok(Self {
            by_cloid: BTreeMap::new(),
            unknown_assets: BTreeSet::new(),
            not_found_confirmations: BTreeMap::new(),
            maximum_entries,
            history: Vec::new(),
        })
    }

    pub fn register(&mut self, intent: ApprovedExecutionIntent) -> Result<(), SignerError> {
        intent.validate()?;
        if self.by_cloid.contains_key(&intent.cloid) {
            return Err(SignerError::DuplicateCloid);
        }
        if !intent.reduce_only && self.unknown_assets.contains(&intent.asset) {
            return Err(SignerError::AssetBlockedByUnknownResult);
        }
        if self.by_cloid.len() >= self.maximum_entries {
            return Err(SignerError::RegistryCapacityExhausted);
        }
        let cloid = intent.cloid.clone();
        self.by_cloid
            .insert(cloid.clone(), (intent, SubmissionState::Authorized));
        self.history.push(SubmissionTransitionRecord {
            cloid,
            state: SubmissionState::Authorized,
            recorded_at_ms: 0,
        });
        Ok(())
    }

    pub fn state(&self, cloid: &str) -> Option<&SubmissionState> {
        self.by_cloid.get(cloid).map(|(_, state)| state)
    }

    pub fn approved_intent(&self, cloid: &str) -> Result<ApprovedExecutionIntent, SignerError> {
        let (intent, state) = self.by_cloid.get(cloid).ok_or(SignerError::UnknownCloid)?;
        if !matches!(
            state,
            SubmissionState::Authorized | SubmissionState::AuthorizedNotSubmitted
        ) {
            return Err(SignerError::InvalidTransition);
        }
        Ok(intent.clone())
    }

    pub fn transition(&mut self, cloid: &str, next: SubmissionState) -> Result<(), SignerError> {
        self.transition_at(cloid, next, 0)
    }

    pub fn transition_at(
        &mut self,
        cloid: &str,
        next: SubmissionState,
        recorded_at_ms: u64,
    ) -> Result<(), SignerError> {
        let (intent, current) = self
            .by_cloid
            .get_mut(cloid)
            .ok_or(SignerError::UnknownCloid)?;
        if current == &next {
            return Ok(());
        }
        if let (Some(current_order), Some(next_order)) =
            (state_order_id(current), state_order_id(&next))
        {
            if current_order != cloid && current_order != next_order {
                return Err(SignerError::InvalidTransition);
            }
        }
        if let (
            SubmissionState::Acknowledged {
                order_id: current_order,
            },
            SubmissionState::Acknowledged {
                order_id: next_order,
            },
        ) = (&*current, &next)
        {
            if current_order != cloid || next_order == cloid {
                return Err(SignerError::InvalidTransition);
            }
        }
        if let (
            SubmissionState::PartiallyFilled {
                order_id: current_order,
                filled: current_filled,
            },
            SubmissionState::PartiallyFilled {
                order_id: next_order,
                filled: next_filled,
            },
        ) = (&*current, &next)
        {
            if current_order != next_order || next_filled < current_filled {
                return Err(SignerError::InvalidTransition);
            }
        }
        if !valid_transition(current, &next) {
            return Err(SignerError::InvalidTransition);
        }
        if matches!(next, SubmissionState::UnknownResult { .. }) {
            self.unknown_assets.insert(intent.asset.clone());
        }
        if matches!(
            next,
            SubmissionState::Filled { .. }
                | SubmissionState::Cancelled { .. }
                | SubmissionState::Rejected { .. }
                | SubmissionState::ReconciledNotFound
        ) {
            self.unknown_assets.remove(&intent.asset);
            self.not_found_confirmations.remove(cloid);
        }
        *current = next;
        self.history.push(SubmissionTransitionRecord {
            cloid: cloid.to_string(),
            state: current.clone(),
            recorded_at_ms,
        });
        Ok(())
    }

    pub fn intent(&self, cloid: &str) -> Option<&ApprovedExecutionIntent> {
        self.by_cloid.get(cloid).map(|(intent, _)| intent)
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&str, &ApprovedExecutionIntent, &SubmissionState)> {
        self.by_cloid
            .iter()
            .map(|(cloid, (intent, state))| (cloid.as_str(), intent, state))
    }

    pub fn has_authorized_not_submitted(&self) -> bool {
        self.by_cloid
            .values()
            .any(|(_, state)| state == &SubmissionState::AuthorizedNotSubmitted)
    }

    pub fn history(&self) -> &[SubmissionTransitionRecord] {
        &self.history
    }

    pub fn durable_sequence(&self) -> Result<u64, SignerError> {
        self.history
            .len()
            .try_into()
            .map_err(|_| SignerError::RegistryCapacityExhausted)
    }

    pub fn next_nonce_floor(&self) -> Result<u64, SignerError> {
        self.history
            .iter()
            .filter_map(|event| match event.state {
                SubmissionState::NonceAllocated { nonce }
                | SubmissionState::Signed { nonce }
                | SubmissionState::SubmissionStarted { nonce }
                | SubmissionState::UnknownResult { nonce } => Some(nonce),
                _ => None,
            })
            .max()
            .map_or(Ok(0), |nonce| {
                nonce.checked_add(1).ok_or(SignerError::NonceOverflow)
            })
    }

    pub fn permits_increase(&self, asset: &str) -> bool {
        !self.unknown_assets.contains(asset)
    }

    pub fn confirm_not_found(
        &mut self,
        cloid: &str,
        observed_at_ms: u64,
        required_confirmations: u32,
        minimum_interval_ms: u64,
    ) -> Result<bool, SignerError> {
        if required_confirmations < 2 || minimum_interval_ms == 0 {
            return Err(SignerError::InvalidConfiguration);
        }
        let (_, state) = self.by_cloid.get(cloid).ok_or(SignerError::UnknownCloid)?;
        if !matches!(state, SubmissionState::UnknownResult { .. }) {
            return Err(SignerError::InvalidTransition);
        }
        let confirmations = self
            .not_found_confirmations
            .entry(cloid.to_string())
            .or_default();
        if confirmations
            .last()
            .is_some_and(|last| observed_at_ms < last.saturating_add(minimum_interval_ms))
        {
            return Err(SignerError::ConfirmationTooSoon);
        }
        confirmations.push(observed_at_ms);
        if confirmations.len() < required_confirmations as usize {
            return Ok(false);
        }
        let (intent, state) = self
            .by_cloid
            .get_mut(cloid)
            .ok_or(SignerError::UnknownCloid)?;
        *state = SubmissionState::ReconciledNotFound;
        self.unknown_assets.remove(&intent.asset);
        self.not_found_confirmations.remove(cloid);
        Ok(true)
    }

    pub fn unresolved_count(&self) -> usize {
        self.by_cloid
            .values()
            .filter(|(_, state)| state.unresolved())
            .count()
    }

    pub fn persist(&self, path: &Path) -> Result<(), SignerError> {
        let bytes =
            serde_json::to_vec(self).map_err(|error| SignerError::Encode(error.to_string()))?;
        let parent = path.parent().ok_or(SignerError::InvalidConfiguration)?;
        std::fs::create_dir_all(parent)
            .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        let temporary = path.with_extension("tmp");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        std::io::Write::write_all(&mut file, &bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        std::fs::rename(&temporary, path)
            .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        Ok(())
    }

    pub fn restore(path: &Path) -> Result<Self, SignerError> {
        let bytes =
            std::fs::read(path).map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        let registry: Self = serde_json::from_slice(&bytes)
            .map_err(|error| SignerError::Decode(error.to_string()))?;
        if registry.maximum_entries == 0 || registry.by_cloid.len() > registry.maximum_entries {
            return Err(SignerError::InvalidConfiguration);
        }
        for (cloid, (intent, _)) in &registry.by_cloid {
            intent.validate()?;
            if cloid != &intent.cloid {
                return Err(SignerError::CloidMismatch);
            }
        }
        let expected_unknown: BTreeSet<_> = registry
            .by_cloid
            .values()
            .filter_map(|(intent, state)| {
                matches!(state, SubmissionState::UnknownResult { .. }).then(|| intent.asset.clone())
            })
            .collect();
        if registry.unknown_assets != expected_unknown {
            return Err(SignerError::InvalidExchangeState);
        }
        for (cloid, (_, state)) in &registry.by_cloid {
            let Some(last) = registry
                .history
                .iter()
                .rev()
                .find(|event| &event.cloid == cloid)
            else {
                return Err(SignerError::InvalidExchangeState);
            };
            if &last.state != state {
                return Err(SignerError::InvalidExchangeState);
            }
        }
        Ok(registry)
    }
}

fn state_order_id(state: &SubmissionState) -> Option<&str> {
    match state {
        SubmissionState::Acknowledged { order_id }
        | SubmissionState::PartiallyFilled { order_id, .. }
        | SubmissionState::Filled { order_id, .. } => Some(order_id),
        SubmissionState::Cancelled {
            order_id: Some(order_id),
            ..
        } => Some(order_id),
        _ => None,
    }
}

fn valid_transition(current: &SubmissionState, next: &SubmissionState) -> bool {
    matches!(
        (current, next),
        (
            SubmissionState::Authorized,
            SubmissionState::NonceAllocated { .. }
        ) | (
            SubmissionState::Authorized,
            SubmissionState::AuthorizedNotSubmitted
        ) | (
            SubmissionState::NonceAllocated { .. },
            SubmissionState::AuthorizedNotSubmitted
        ) | (
            SubmissionState::Signed { .. },
            SubmissionState::AuthorizedNotSubmitted
        ) | (
            SubmissionState::AuthorizedNotSubmitted,
            SubmissionState::NonceAllocated { .. }
        ) | (
            SubmissionState::AuthorizedNotSubmitted,
            SubmissionState::Authorized
        ) | (
            SubmissionState::AuthorizedNotSubmitted,
            SubmissionState::Rejected { .. }
        ) | (
            SubmissionState::Authorized,
            SubmissionState::Rejected { .. }
        ) | (
            SubmissionState::NonceAllocated { .. },
            SubmissionState::Rejected { .. }
        ) | (
            SubmissionState::Signed { .. },
            SubmissionState::Rejected { .. }
        ) | (
            SubmissionState::NonceAllocated { .. },
            SubmissionState::Signed { .. }
        ) | (
            SubmissionState::Signed { .. },
            SubmissionState::SubmissionStarted { .. }
        ) | (
            SubmissionState::SubmissionStarted { .. },
            SubmissionState::Acknowledged { .. }
        ) | (
            SubmissionState::SubmissionStarted { .. },
            SubmissionState::UnknownResult { .. }
        ) | (
            SubmissionState::SubmissionStarted { .. },
            SubmissionState::Filled { .. }
        ) | (
            SubmissionState::SubmissionStarted { .. },
            SubmissionState::Rejected { .. }
        ) | (
            SubmissionState::Acknowledged { .. },
            SubmissionState::Acknowledged { .. }
        ) | (
            SubmissionState::Acknowledged { .. },
            SubmissionState::PartiallyFilled { .. }
        ) | (
            SubmissionState::Acknowledged { .. },
            SubmissionState::Filled { .. }
        ) | (
            SubmissionState::Acknowledged { .. },
            SubmissionState::Cancelled { .. }
        ) | (
            SubmissionState::Acknowledged { .. },
            SubmissionState::Rejected { .. }
        ) | (
            SubmissionState::PartiallyFilled { .. },
            SubmissionState::PartiallyFilled { .. }
        ) | (
            SubmissionState::PartiallyFilled { .. },
            SubmissionState::Filled { .. }
        ) | (
            SubmissionState::PartiallyFilled { .. },
            SubmissionState::Cancelled { .. }
        ) | (
            SubmissionState::UnknownResult { .. },
            SubmissionState::Acknowledged { .. }
        ) | (
            SubmissionState::UnknownResult { .. },
            SubmissionState::PartiallyFilled { .. }
        ) | (
            SubmissionState::UnknownResult { .. },
            SubmissionState::Filled { .. }
        ) | (
            SubmissionState::UnknownResult { .. },
            SubmissionState::Cancelled { .. }
        ) | (
            SubmissionState::UnknownResult { .. },
            SubmissionState::Rejected { .. }
        )
    )
}

pub struct ApiWalletSecret {
    wallet: LocalWallet,
}

impl ApiWalletSecret {
    pub fn from_private_key(secret: &str) -> Result<Self, SignerError> {
        let wallet =
            LocalWallet::from_str(secret.trim()).map_err(|_| SignerError::InvalidSecret)?;
        Ok(Self { wallet })
    }

    pub async fn load_from_file(path: &Path) -> Result<Self, SignerError> {
        validate_secret_file(path)?;
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|error| SignerError::SecretIo(error.to_string()))?;
        let text = std::str::from_utf8(&bytes).map_err(|_| SignerError::InvalidSecret)?;
        Self::from_private_key(text)
    }

    fn sign_ioc(
        &self,
        intent: &production::AuthorizedExecutionIntent,
        nonce: u64,
    ) -> Result<transport::SignedIocRequest, SignerError> {
        let cloid = intent.planned_cloid.to_hex();
        let action = OrderAction {
            orders: vec![WireOrder {
                asset: intent.asset_index,
                is_buy: matches!(intent.side, copytrade_core::decision::Side::Buy),
                limit_px: canonical_decimal(intent.limit_price)?,
                size: canonical_decimal(intent.quantity)?,
                reduce_only: intent.reduce_only,
                order_type: OrderType {
                    limit: LimitOrder { tif: "Ioc" },
                },
                cloid,
            }],
            grouping: "na",
        };
        let signature = sign_mainnet_action(&self.wallet, action_hash(&action, nonce)?)?;
        Ok(transport::SignedIocRequest {
            asset_index: intent.asset_index,
            is_buy: matches!(intent.side, copytrade_core::decision::Side::Buy),
            limit_price: intent.limit_price,
            quantity: intent.quantity,
            reduce_only: intent.reduce_only,
            cloid: intent.planned_cloid,
            nonce,
            signature,
        })
    }
}

#[cfg(unix)]
fn validate_secret_file(path: &Path) -> Result<(), SignerError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| SignerError::SecretIo(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.mode() & 0o077 != 0 {
        return Err(SignerError::UnsafeSecretPermissions);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_secret_file(path: &Path) -> Result<(), SignerError> {
    if !path.is_file() {
        return Err(SignerError::UnsafeSecretPermissions);
    }
    Ok(())
}

fn canonical_decimal(value: Decimal) -> Result<String, SignerError> {
    if value <= Decimal::ZERO || value.scale() > 8 {
        return Err(SignerError::InvalidDecimal);
    }
    Ok(value.normalize().to_string())
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename = "order")]
struct OrderAction {
    orders: Vec<WireOrder>,
    grouping: &'static str,
}

#[derive(Debug, Serialize)]
struct WireOrder {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    limit_px: String,
    #[serde(rename = "s")]
    size: String,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: OrderType,
    #[serde(rename = "c")]
    cloid: String,
}

#[derive(Debug, Serialize)]
struct OrderType {
    limit: LimitOrder,
}

#[derive(Debug, Serialize)]
struct LimitOrder {
    tif: &'static str,
}

fn action_hash(action: &OrderAction, nonce: u64) -> Result<H256, SignerError> {
    let mut bytes =
        rmp_serde::to_vec_named(action).map_err(|error| SignerError::Encode(error.to_string()))?;
    bytes.extend(nonce.to_be_bytes());
    bytes.push(0);
    Ok(H256(ethers::utils::keccak256(bytes)))
}

#[derive(Debug, Eip712, Clone, EthAbiType)]
#[eip712(
    name = "Exchange",
    version = "1",
    chain_id = 1337,
    verifying_contract = "0x0000000000000000000000000000000000000000"
)]
struct Agent {
    source: String,
    connection_id: H256,
}

fn sign_mainnet_action(
    wallet: &LocalWallet,
    connection_id: H256,
) -> Result<Signature, SignerError> {
    let encoded = Agent {
        source: "a".to_string(),
        connection_id,
    }
    .encode_eip712()
    .map_err(|error| SignerError::Signing(error.to_string()))?;
    let hash = H256::from(encoded);
    let (signature, recovery) = wallet
        .signer()
        .sign_digest_recoverable(Sha256Proxy::from(hash))
        .map_err(|error| SignerError::Signing(error.to_string()))?;
    let r_bytes: FieldBytes<Secp256k1> = signature.r().into();
    let s_bytes: FieldBytes<Secp256k1> = signature.s().into();
    Ok(Signature {
        r: U256::from_big_endian(r_bytes.as_slice()),
        s: U256::from_big_endian(s_bytes.as_slice()),
        v: u8::from(recovery) as u64 + 27,
    })
}

type Sha256Proxy = ProxyDigest<sha2::Sha256>;

#[derive(Clone)]
enum ProxyDigest<D: Digest> {
    Proxy(Output<D>),
    Digest(D),
}

impl<D: Digest + Clone> From<H256> for ProxyDigest<D>
where
    GenericArray<u8, <D as OutputSizeUser>::OutputSize>: Copy,
{
    fn from(value: H256) -> Self {
        Self::Proxy(*GenericArray::from_slice(value.as_bytes()))
    }
}

impl<D: Digest> Default for ProxyDigest<D> {
    fn default() -> Self {
        Self::Digest(D::new())
    }
}

impl<D: Digest> Update for ProxyDigest<D> {
    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Digest(digest) => digest.update(data),
            Self::Proxy(_) => unreachable!("proxy digest is already finalized"),
        }
    }
}

impl<D: Digest> HashMarker for ProxyDigest<D> {}
impl<D: Digest> Reset for ProxyDigest<D> {
    fn reset(&mut self) {
        *self = Self::default();
    }
}
impl<D: Digest> OutputSizeUser for ProxyDigest<D> {
    type OutputSize = <D as OutputSizeUser>::OutputSize;
}
impl<D: Digest> FixedOutput for ProxyDigest<D> {
    fn finalize_into(self, out: &mut GenericArray<u8, Self::OutputSize>) {
        match self {
            Self::Digest(digest) => *out = digest.finalize(),
            Self::Proxy(output) => *out = output,
        }
    }
}
impl<D: Digest> FixedOutputReset for ProxyDigest<D> {
    fn finalize_into_reset(&mut self, out: &mut Output<Self>) {
        Digest::finalize_into(std::mem::take(self), out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignerError {
    InvalidIntent,
    InvalidContinuation,
    InvalidDecimal,
    InvalidConfiguration,
    DuplicateCloid,
    UnknownCloid,
    AssetBlockedByUnknownResult,
    InvalidTransition,
    NonceOverflow,
    UnsafeSecretPermissions,
    InvalidSecret,
    SecretIo(String),
    Encode(String),
    Signing(String),
    Transport(String),
    ExchangeHttp(u16),
    Decode(String),
    UnknownSubmissionResult { nonce: u64 },
    UnexpectedExchangeResponse,
    IocUnexpectedlyResting(u64),
    ConfirmationTooSoon,
    CloidMismatch,
    RegistryCapacityExhausted,
    RegistryIo(String),
    StartupNotReconciled,
    Authorization(String),
    Reconciliation(String),
    MissingMarketRules,
    InvalidExchangeState,
    Ledger(String),
    ExchangeFillMismatch,
    AmbiguousExchangeEventOrdering,
}

impl Display for SignerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for SignerError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(asset: &str, cloid_byte: char) -> ApprovedExecutionIntent {
        ApprovedExecutionIntent {
            decision_id: "11".repeat(32),
            target_version: 1,
            root_cloid: format!("0x{}", cloid_byte.to_string().repeat(32)),
            cloid: format!("0x{}", cloid_byte.to_string().repeat(32)),
            parent_cloid: None,
            continuation_generation: 0,
            asset: asset.to_string(),
            asset_index: 0,
            is_buy: true,
            reduce_only: false,
            limit_price: Decimal::from(100),
            quantity: Decimal::ONE,
            risk_projection_hash: "22".repeat(32),
            risk_policy_hash: "33".repeat(32),
            configuration_hash: "44".repeat(32),
            release_manifest_hash: "55".repeat(32),
            decision_reference_price: Decimal::from(100),
            decision_timestamp_ms: 1,
            expires_at_mono: 10_000,
            canonical_intent_hash: "66".repeat(32),
        }
    }

    #[test]
    fn nonce_is_atomic_monotonic_and_time_floored() {
        let allocator = NonceAllocator::new(0);
        assert_eq!(allocator.next(100).unwrap(), 100);
        assert_eq!(allocator.next(90).unwrap(), 101);
        assert_eq!(allocator.next(200).unwrap(), 200);
    }

    #[test]
    fn duplicate_cloid_and_unknown_asset_fail_closed() {
        let mut registry = SubmissionRegistry::default();
        let first = intent("BTC", 'a');
        registry.register(first.clone()).unwrap();
        assert_eq!(registry.register(first), Err(SignerError::DuplicateCloid));
        registry
            .transition(
                &format!("0x{}", "a".repeat(32)),
                SubmissionState::NonceAllocated { nonce: 1 },
            )
            .unwrap();
        registry
            .transition(
                &format!("0x{}", "a".repeat(32)),
                SubmissionState::Signed { nonce: 1 },
            )
            .unwrap();
        registry
            .transition(
                &format!("0x{}", "a".repeat(32)),
                SubmissionState::SubmissionStarted { nonce: 1 },
            )
            .unwrap();
        registry
            .transition(
                &format!("0x{}", "a".repeat(32)),
                SubmissionState::UnknownResult { nonce: 1 },
            )
            .unwrap();
        assert!(!registry.permits_increase("BTC"));
        assert_eq!(
            registry.register(intent("BTC", 'b')),
            Err(SignerError::AssetBlockedByUnknownResult)
        );
        let cloid = format!("0x{}", "a".repeat(32));
        assert!(!registry.confirm_not_found(&cloid, 1_000, 2, 100).unwrap());
        assert_eq!(
            registry.confirm_not_found(&cloid, 1_050, 2, 100),
            Err(SignerError::ConfirmationTooSoon)
        );
        assert!(registry.confirm_not_found(&cloid, 1_100, 2, 100).unwrap());
        assert!(registry.permits_increase("BTC"));
    }

    #[test]
    fn durable_transition_history_restores_nonce_floor_and_rejects_tampering() {
        let root = std::env::temp_dir().join(format!(
            "copytrade-registry-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let mut registry = SubmissionRegistry::default();
        let intent = intent("BTC", 'c');
        let cloid = intent.cloid.clone();
        registry.register(intent).unwrap();
        registry
            .transition_at(&cloid, SubmissionState::NonceAllocated { nonce: 77 }, 1)
            .unwrap();
        registry
            .transition_at(&cloid, SubmissionState::Signed { nonce: 77 }, 2)
            .unwrap();
        registry.persist(&root).unwrap();
        let restored = SubmissionRegistry::restore(&root).unwrap();
        assert_eq!(restored.next_nonce_floor().unwrap(), 78);
        let mut json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&root).unwrap()).unwrap();
        json["history"] = serde_json::json!([]);
        std::fs::write(&root, serde_json::to_vec(&json).unwrap()).unwrap();
        assert!(SubmissionRegistry::restore(&root).is_err());
        let _ = std::fs::remove_file(root);
    }

    #[test]
    fn exact_decimal_wire_has_no_binary_float_conversion() {
        assert_eq!(
            canonical_decimal(Decimal::from_str("10.23000000").unwrap()).unwrap(),
            "10.23"
        );
        assert_eq!(
            canonical_decimal(Decimal::from_str("0.000000001").unwrap()),
            Err(SignerError::InvalidDecimal)
        );
    }

    #[test]
    fn ioc_wire_is_canonical_and_contains_no_mutation_other_than_order() {
        let action = OrderAction {
            orders: vec![WireOrder {
                asset: 1,
                is_buy: true,
                limit_px: "100.25".to_string(),
                size: "0.125".to_string(),
                reduce_only: true,
                order_type: OrderType {
                    limit: LimitOrder { tif: "Ioc" },
                },
                cloid: format!("0x{}", "a".repeat(32)),
            }],
            grouping: "na",
        };
        assert_eq!(
            serde_json::to_value(action).unwrap(),
            serde_json::json!({
                "type":"order",
                "orders":[{"a":1,"b":true,"p":"100.25","s":"0.125","r":true,"t":{"limit":{"tif":"Ioc"}},"c":format!("0x{}", "a".repeat(32))}],
                "grouping":"na"
            })
        );
    }
}
