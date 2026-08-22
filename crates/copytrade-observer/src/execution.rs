//! One execution switch shared by shadow and live operation.

use copytrade_core::authorized_intent::AuthorizedExecutionIntent;
use copytrade_core::decision::{ConfigHash, PlannedCloid, RiskPolicyHash};
use copytrade_core::live_trading::LiveTradingState;
use copytrade_signer::production::{ProductionSigner, ProductionSubmissionResult};
use copytrade_signer::reconciliation::AppliedExchangeBatch;
use copytrade_signer::transport::{AuthenticatedExchangeTransport, HyperliquidMainnetTransport};
use copytrade_signer::{ApiWalletSecret, SignerError, SubmissionRegistry, SubmissionState};
use rust_decimal::Decimal;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

pub const EXECUTION_MODE_ENV: &str = "SU6_EXECUTION_MODE";
pub const API_WALLET_SECRET_ENV: &str = "HYPERLIQUID_API_WALLET_SECRET";
pub const MASTER_ACCOUNT_ENV: &str = "HYPERLIQUID_MASTER_ACCOUNT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Shadow,
    Live,
}

impl ExecutionMode {
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.unwrap_or("shadow") {
            "shadow" => Ok(Self::Shadow),
            "live" => Ok(Self::Live),
            _ => Err("SU6_EXECUTION_MODE must be shadow or live".into()),
        }
    }

    pub fn from_environment() -> Result<Self, String> {
        Self::parse(std::env::var(EXECUTION_MODE_ENV).ok().as_deref())
    }
}

pub struct LiveExecutionSettings {
    pub master_account: String,
    private_key: String,
}

impl LiveExecutionSettings {
    pub fn load_if_live_with(
        mode: ExecutionMode,
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<Self>, String> {
        if mode == ExecutionMode::Shadow {
            return Ok(None);
        }
        let master_account = lookup(MASTER_ACCOUNT_ENV)
            .filter(|value| !value.trim().is_empty())
            .ok_or("live mode requires HYPERLIQUID_MASTER_ACCOUNT")?;
        let private_key = lookup(API_WALLET_SECRET_ENV)
            .filter(|value| !value.trim().is_empty())
            .ok_or("live mode requires HYPERLIQUID_API_WALLET_SECRET")?;
        Ok(Some(Self {
            master_account,
            private_key,
        }))
    }

    pub fn load_if_live(mode: ExecutionMode) -> Result<Option<Self>, String> {
        Self::load_if_live_with(mode, |name| std::env::var(name).ok())
    }

    fn into_wallet(self) -> Result<(String, ApiWalletSecret), SignerError> {
        let wallet = ApiWalletSecret::from_private_key(&self.private_key)?;
        Ok((self.master_account, wallet))
    }
}

pub struct LiveExecutionRuntime<T: AuthenticatedExchangeTransport> {
    signer: ProductionSigner<T>,
    state: LiveTradingState,
    ledger_path: PathBuf,
    active_cloids: BTreeSet<PlannedCloid>,
    next_reconciliation_ms: u64,
}

pub struct LiveTerminalResolution {
    pub cloid: PlannedCloid,
    pub original_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub rejected: bool,
}

pub struct LiveExecutionUpdate {
    pub applied: AppliedExchangeBatch,
    pub terminal: Vec<LiveTerminalResolution>,
}

impl LiveExecutionRuntime<HyperliquidMainnetTransport> {
    pub async fn initialize(
        settings: LiveExecutionSettings,
        state_root: &Path,
        risk_policy_hash: RiskPolicyHash,
        configuration_hash: ConfigHash,
        now_ms: u64,
    ) -> Result<Self, SignerError> {
        let (master_account, wallet) = settings.into_wallet()?;
        let transport = HyperliquidMainnetTransport::new(master_account, 10_000)
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        Self::initialize_with_transport(
            transport,
            wallet,
            state_root,
            risk_policy_hash,
            configuration_hash,
            now_ms,
        )
        .await
    }
}

impl<T: AuthenticatedExchangeTransport + Clone> LiveExecutionRuntime<T> {
    pub async fn initialize_with_transport(
        transport: T,
        wallet: ApiWalletSecret,
        state_root: &Path,
        risk_policy_hash: RiskPolicyHash,
        configuration_hash: ConfigHash,
        now_ms: u64,
    ) -> Result<Self, SignerError> {
        std::fs::create_dir_all(state_root)
            .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        let registry_path = state_root.join("live-submission-registry.json");
        let ledger_path = state_root.join("live-trading-state.json");
        let registry = if registry_path.exists() {
            SubmissionRegistry::restore(&registry_path)?
        } else {
            SubmissionRegistry::default()
        };
        let state = if ledger_path.exists() {
            LiveTradingState::load(&ledger_path)
                .map_err(|error| SignerError::Ledger(error.to_string()))?
        } else {
            let equity = transport
                .read_equity()
                .await
                .map_err(|error| SignerError::Reconciliation(error.to_string()))?
                .equity;
            LiveTradingState::new(equity, now_ms)
                .map_err(|error| SignerError::Ledger(error.to_string()))?
        };
        let signer = ProductionSigner::new(
            transport,
            wallet,
            registry,
            &registry_path,
            now_ms,
            risk_policy_hash,
            configuration_hash,
            [0; 32],
        )?;
        let mut runtime = Self {
            signer,
            state,
            ledger_path,
            active_cloids: BTreeSet::new(),
            next_reconciliation_ms: now_ms,
        };
        runtime.reconcile(now_ms).await?;
        runtime.active_cloids.extend(
            runtime
                .signer
                .action_states()
                .await?
                .into_iter()
                .map(|(cloid, _, _)| cloid),
        );
        Ok(runtime)
    }

    pub async fn submit(
        &mut self,
        intent: AuthorizedExecutionIntent,
        unix_ms: u64,
    ) -> Result<(ProductionSubmissionResult, LiveExecutionUpdate), SignerError> {
        let authorization_now = intent.authorization.now_mono;
        let cloid = intent.planned_cloid;
        self.signer
            .authorize(intent.clone(), authorization_now)
            .await?;
        self.active_cloids.insert(cloid);
        let result = self.signer.submit_authorized(intent, unix_ms).await?;
        let update = self.reconcile_update(unix_ms).await?;
        Ok((result, update))
    }

    pub async fn reconcile(&mut self, now_ms: u64) -> Result<AppliedExchangeBatch, SignerError> {
        self.signer
            .reconcile_startup(&mut self.state, &self.ledger_path, now_ms, 2, 1_000)
            .await
    }

    pub async fn reconcile_if_due(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<LiveExecutionUpdate>, SignerError> {
        if self.active_cloids.is_empty() || now_ms < self.next_reconciliation_ms {
            return Ok(None);
        }
        self.reconcile_update(now_ms).await.map(Some)
    }

    async fn reconcile_update(&mut self, now_ms: u64) -> Result<LiveExecutionUpdate, SignerError> {
        let applied = self.reconcile(now_ms).await?;
        self.next_reconciliation_ms = now_ms.saturating_add(1_000);
        let mut terminal = Vec::new();
        for (cloid, original_quantity, state) in self.signer.action_states().await? {
            if !self.active_cloids.contains(&cloid) {
                continue;
            }
            let resolution = match state {
                SubmissionState::Filled { filled, .. } => Some((filled, false)),
                SubmissionState::Cancelled { filled, .. } => Some((filled, false)),
                SubmissionState::Rejected { .. } | SubmissionState::ReconciledNotFound => {
                    Some((Decimal::ZERO, true))
                }
                _ => None,
            };
            if let Some((filled_quantity, rejected)) = resolution {
                self.active_cloids.remove(&cloid);
                terminal.push(LiveTerminalResolution {
                    cloid,
                    original_quantity,
                    filled_quantity,
                    rejected,
                });
            }
        }
        Ok(LiveExecutionUpdate { applied, terminal })
    }

    pub fn state(&self) -> &LiveTradingState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_is_default_and_never_reads_live_secrets() {
        assert_eq!(ExecutionMode::parse(None).unwrap(), ExecutionMode::Shadow);
        let mut reads = 0;
        let settings = LiveExecutionSettings::load_if_live_with(ExecutionMode::Shadow, |_| {
            reads += 1;
            panic!("shadow mode must not inspect live secret configuration")
        })
        .unwrap();
        assert!(settings.is_none());
        assert_eq!(reads, 0);
    }

    #[test]
    fn live_requires_exactly_the_external_account_and_secret_inputs() {
        assert!(LiveExecutionSettings::load_if_live_with(ExecutionMode::Live, |_| None).is_err());
        let settings =
            LiveExecutionSettings::load_if_live_with(ExecutionMode::Live, |name| match name {
                MASTER_ACCOUNT_ENV => Some("0x1111111111111111111111111111111111111111".into()),
                API_WALLET_SECRET_ENV => Some(format!("0x{}", "11".repeat(32))),
                _ => None,
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            settings.master_account,
            "0x1111111111111111111111111111111111111111"
        );
        settings.into_wallet().unwrap();
    }

    #[test]
    fn invalid_execution_mode_fails_closed() {
        assert!(ExecutionMode::parse(Some("paper")).is_err());
    }
}
