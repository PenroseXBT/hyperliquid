//! Authenticated production execution and restart reconciliation.

use crate::domain::authorized_intent::AuthorizedExecutionIntent;
use crate::domain::decision::{ConfigHash, PlannedCloid, RiskPolicyHash};
use crate::domain::live_trading::{LiveTradingState, ReconciliationMode};
use crate::signing::production::{ProductionSigner, ProductionSubmissionResult};
use crate::signing::reconciliation::AppliedExchangeBatch;
use crate::signing::transport::{
    AuthenticatedExchangeTransport, ExchangePositionSnapshot, HyperliquidMainnetTransport,
};
use crate::signing::{ApiWalletSecret, SignerError, SubmissionRegistry, SubmissionState};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

pub const API_WALLET_SECRET_ENV: &str = "HYPERLIQUID_API_WALLET_SECRET";
pub const EXECUTION_ACCOUNT_ENV: &str = "HYPERLIQUID_EXECUTION_ACCOUNT";

pub struct LiveExecutionSettings {
    pub execution_account: String,
    private_key: String,
}

impl LiveExecutionSettings {
    pub fn load_with(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let execution_account = lookup(EXECUTION_ACCOUNT_ENV)
            .filter(|value| !value.trim().is_empty())
            .ok_or("production requires HYPERLIQUID_EXECUTION_ACCOUNT")?;
        let private_key = lookup(API_WALLET_SECRET_ENV)
            .filter(|value| !value.trim().is_empty())
            .ok_or("production requires HYPERLIQUID_API_WALLET_SECRET")?;
        Ok(Self {
            execution_account,
            private_key,
        })
    }

    pub fn load() -> Result<Self, String> {
        Self::load_with(|name| std::env::var(name).ok())
    }

    fn into_wallet(self) -> Result<(String, ApiWalletSecret), SignerError> {
        let wallet = ApiWalletSecret::from_private_key(&self.private_key)?;
        if !self
            .execution_account
            .eq_ignore_ascii_case(&wallet.address_hex())
        {
            return Err(SignerError::Authorization(
                "DirectUser signer must equal HYPERLIQUID_EXECUTION_ACCOUNT".into(),
            ));
        }
        Ok((wallet.address_hex(), wallet))
    }
}

pub struct LiveExecutionRuntime<T: AuthenticatedExchangeTransport> {
    signer: ProductionSigner<T>,
    state: LiveTradingState,
    ledger_path: PathBuf,
    active_cloids: BTreeSet<PlannedCloid>,
    next_reconciliation_ms: u64,
    mode: ReconciliationMode,
    exchange_snapshot: Option<ExchangePositionSnapshot>,
    recovery_reason: Option<String>,
}

pub struct LiveTerminalResolution {
    pub cloid: PlannedCloid,
    pub original_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub rejected: bool,
    pub no_action: bool,
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
    ) -> Result<(Self, Option<LiveExecutionUpdate>), SignerError> {
        let (execution_account, wallet) = settings.into_wallet()?;
        let transport = HyperliquidMainnetTransport::new(execution_account.clone(), 10_000)
            .map_err(|error| SignerError::Reconciliation(error.to_string()))?;
        transport
            .verify_direct_user()
            .await
            .map_err(|error| match error {
                crate::signing::transport::ReconciliationError::IdentityMismatch => {
                    SignerError::Authorization(
                        "DirectUser execution account must have Hyperliquid role user".into(),
                    )
                }
                _ => SignerError::Reconciliation(format!("DirectUser verification: {error}")),
            })?;
        eprintln!(
            "execution_identity_verified=true topology=DirectUser account={} signer={} vaultAddress=null",
            execution_account,
            wallet.address_hex(),
        );
        verify_live_identity(state_root, &execution_account, &wallet.address_hex())?;
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

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct LiveAccountIdentity {
    schema_version: u32,
    economic_account: String,
    signer: String,
}

fn verify_live_identity(
    state_root: &Path,
    economic_account: &str,
    signer: &str,
) -> Result<(), SignerError> {
    let path = state_root.join("live-account-identity.json");
    let expected = LiveAccountIdentity {
        schema_version: 1,
        economic_account: economic_account.to_ascii_lowercase(),
        signer: signer.to_ascii_lowercase(),
    };
    if path.exists() {
        let actual: LiveAccountIdentity = serde_json::from_slice(
            &std::fs::read(&path).map_err(|error| SignerError::RegistryIo(error.to_string()))?,
        )
        .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
        return (actual == expected)
            .then_some(())
            .ok_or(SignerError::InvalidExchangeState);
    }
    std::fs::create_dir_all(state_root)
        .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
    let temporary = path.with_extension("tmp");
    let bytes = serde_json::to_vec(&expected)
        .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
    let mut file = std::fs::File::create(&temporary)
        .map_err(|error| SignerError::RegistryIo(error.to_string()))?;
    std::io::Write::write_all(&mut file, &bytes)
        .and_then(|_| file.sync_all())
        .and_then(|_| std::fs::rename(&temporary, &path))
        .and_then(|_| std::fs::File::open(state_root)?.sync_all())
        .map_err(|error| SignerError::RegistryIo(error.to_string()))
}

impl<T: AuthenticatedExchangeTransport + Clone> LiveExecutionRuntime<T> {
    pub async fn registered_cloids(&self) -> Result<BTreeSet<PlannedCloid>, SignerError> {
        Ok(self
            .signer
            .action_states()
            .await?
            .into_iter()
            .map(|(cloid, _, _)| cloid)
            .collect())
    }

    pub fn install_perp_dex_order(&self, order: Vec<String>) -> Result<(), SignerError> {
        self.signer.install_perp_dex_order(order)
    }

    pub async fn initialize_with_transport(
        transport: T,
        wallet: ApiWalletSecret,
        state_root: &Path,
        risk_policy_hash: RiskPolicyHash,
        configuration_hash: ConfigHash,
        now_ms: u64,
    ) -> Result<(Self, Option<LiveExecutionUpdate>), SignerError> {
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
                .read_positions()
                .await
                .map_err(|error| SignerError::Reconciliation(error.to_string()))?
                .account_equity;
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
            mode: ReconciliationMode::RiskOnly,
            exchange_snapshot: None,
            recovery_reason: None,
        };
        runtime.active_cloids.extend(
            runtime
                .signer
                .action_states()
                .await?
                .into_iter()
                .map(|(cloid, _, _)| cloid),
        );
        let startup_update = match runtime.reconcile_update(now_ms).await {
            Ok(update) => {
                // Engine attribution must also converge before leaving risk-only.
                Some(LiveExecutionUpdate {
                    applied: AppliedExchangeBatch {
                        fills: runtime.state.verified_fills().to_vec(),
                        funding: runtime.state.verified_funding().to_vec(),
                        external: runtime.state.external_fills().to_vec(),
                    },
                    terminal: update.terminal,
                })
            }
            Err(error) if recoverable_reconciliation_error(&error) => {
                // No partial startup truth is handed to the engine. The next
                // successful authenticated reconciliation returns the complete
                // verified history and applies it transactionally.
                runtime.next_reconciliation_ms = now_ms.saturating_add(1_000);
                None
            }
            Err(error) => return Err(error),
        };
        Ok((runtime, startup_update))
    }

    pub async fn submit(
        &mut self,
        intent: AuthorizedExecutionIntent,
        unix_ms: u64,
    ) -> Result<(ProductionSubmissionResult, LiveExecutionUpdate), SignerError> {
        if self.recovery_only() && !intent.reduce_only {
            return Err(SignerError::StartupNotReconciled);
        }
        let authorization_now = intent.authorization.now_mono;
        let cloid = intent.planned_cloid;
        match self
            .signer
            .authorize(intent.clone(), authorization_now)
            .await
        {
            Err(SignerError::NoAction) => {
                return Ok((
                    ProductionSubmissionResult::NoAction,
                    LiveExecutionUpdate {
                        applied: AppliedExchangeBatch {
                            fills: Vec::new(),
                            funding: Vec::new(),
                            external: Vec::new(),
                        },
                        terminal: vec![LiveTerminalResolution {
                            cloid,
                            original_quantity: intent.quantity,
                            filled_quantity: Decimal::ZERO,
                            rejected: false,
                            no_action: true,
                        }],
                    },
                ))
            }
            result => {
                result?;
            }
        }
        self.active_cloids.insert(cloid);
        let result = self.signer.submit_authorized(intent, unix_ms).await?;
        let update = self.reconcile_update(unix_ms).await?;
        Ok((result, update))
    }

    pub async fn reconcile(&mut self, now_ms: u64) -> Result<AppliedExchangeBatch, SignerError> {
        let result = self
            .signer
            .reconcile_startup(&mut self.state, &self.ledger_path, now_ms, 2, 1_000)
            .await;
        self.exchange_snapshot = self.signer.exchange_snapshot().await;
        if let Err(error) = &result {
            if recoverable_reconciliation_error(error) {
                self.defer_recovery(now_ms);
                let reason = error.to_string();
                if self.recovery_reason.as_ref() != Some(&reason) {
                    eprintln!("execution_state=RISK_ONLY reason={reason} exchange_positions={:?} durable_positions={:?}",
                        self.exchange_snapshot.as_ref().map(|snapshot| &snapshot.positions), self.state.positions());
                }
                self.recovery_reason = Some(reason);
            }
        }
        result
    }

    pub async fn reconcile_if_due(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<LiveExecutionUpdate>, SignerError> {
        if now_ms < self.next_reconciliation_ms {
            return Ok(None);
        }
        self.reconcile_update(now_ms).await.map(Some)
    }

    async fn reconcile_update(&mut self, now_ms: u64) -> Result<LiveExecutionUpdate, SignerError> {
        let newly_applied = self.reconcile(now_ms).await?;
        let applied = if self.recovery_only() {
            AppliedExchangeBatch {
                fills: self.state.verified_fills().to_vec(),
                funding: self.state.verified_funding().to_vec(),
                external: self.state.external_fills().to_vec(),
            }
        } else {
            newly_applied
        };
        self.next_reconciliation_ms =
            now_ms.saturating_add(if self.recovery_only() || !self.active_cloids.is_empty() {
                1_000
            } else {
                30_000
            });
        let mut terminal = Vec::new();
        for (cloid, original_quantity, state) in self.signer.action_states().await? {
            if !self.active_cloids.contains(&cloid) {
                continue;
            }
            let resolution = match state {
                SubmissionState::NoAction => Some((Decimal::ZERO, false)),
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
                    no_action: state == SubmissionState::NoAction,
                });
            }
        }
        Ok(LiveExecutionUpdate { applied, terminal })
    }

    pub fn state(&self) -> &LiveTradingState {
        &self.state
    }

    pub fn exchange_snapshot(&self) -> Option<&ExchangePositionSnapshot> {
        self.exchange_snapshot.as_ref()
    }

    pub fn defer_recovery(&mut self, now_ms: u64) {
        self.mode = ReconciliationMode::RiskOnly;
        self.next_reconciliation_ms = now_ms.saturating_add(1_000);
    }

    pub fn leave_recovery_only(&mut self) {
        if self.recovery_only() {
            eprintln!("execution_state=NORMAL reconciliation_converged=true");
        }
        self.mode = ReconciliationMode::Normal;
        self.recovery_reason = None;
    }

    pub fn recovery_only(&self) -> bool {
        self.mode == ReconciliationMode::RiskOnly
    }
}

pub fn recoverable_reconciliation_error(error: &SignerError) -> bool {
    matches!(
        error,
        SignerError::Reconciliation(_)
            | SignerError::StartupNotReconciled
            | SignerError::Transport(_)
            | SignerError::ExchangeHttp(_)
            | SignerError::UnknownSubmissionResult { .. }
            | SignerError::ConfirmationTooSoon
            | SignerError::ExchangeTruthMismatch(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_requires_exactly_the_external_account_and_secret_inputs() {
        assert!(LiveExecutionSettings::load_with(|_| None).is_err());
        let fixture_key = format!("0x{}", "11".repeat(32));
        let expected = ApiWalletSecret::from_private_key(&fixture_key)
            .unwrap()
            .address_hex();
        let settings = LiveExecutionSettings::load_with(|name| match name {
            EXECUTION_ACCOUNT_ENV => Some(expected.clone()),
            API_WALLET_SECRET_ENV => Some(fixture_key.clone()),
            _ => None,
        })
        .unwrap();
        assert_eq!(settings.execution_account, expected);
        let (account, wallet) = settings.into_wallet().unwrap();
        assert_eq!(account, wallet.address_hex());
        assert!(
            LiveExecutionSettings::load_with(|name| match name {
                "HYPERLIQUID_MASTER_ACCOUNT" => Some(expected.clone()),
                API_WALLET_SECRET_ENV => Some(fixture_key.clone()),
                _ => None,
            })
            .is_err(),
            "obsolete master variable must not silently select an account"
        );
    }

    #[test]
    fn direct_user_derives_the_signer_and_rejects_any_other_economic_account() {
        let fixture_key = format!("0x{}", "11".repeat(32));
        let derived = ApiWalletSecret::from_private_key(&fixture_key)
            .unwrap()
            .address_hex();
        let matching = LiveExecutionSettings {
            execution_account: format!("0x{}", derived[2..].to_ascii_uppercase()),
            private_key: fixture_key.clone(),
        };
        assert_eq!(matching.into_wallet().unwrap().0, derived);
        for execution_account in [
            "0x2Ae3dD513B342E5162D11B2Bdfae8cAd7B67015B",
            "0x0000000000000000000000000000000000000000",
            "malformed",
        ] {
            let settings = LiveExecutionSettings {
                execution_account: execution_account.into(),
                private_key: fixture_key.clone(),
            };
            assert!(matches!(
                settings.into_wallet(),
                Err(SignerError::Authorization(_))
            ));
        }
    }

    #[test]
    fn only_exchange_reconciliation_uncertainty_enters_recovery_only() {
        assert!(recoverable_reconciliation_error(
            &SignerError::StartupNotReconciled
        ));
        assert!(recoverable_reconciliation_error(&SignerError::Transport(
            "temporary".into()
        )));
        assert!(recoverable_reconciliation_error(
            &SignerError::ExchangeTruthMismatch("orphan position".into())
        ));
        assert!(!recoverable_reconciliation_error(
            &SignerError::InvalidSecret
        ));
        assert!(!recoverable_reconciliation_error(
            &SignerError::ExchangeFillMismatch
        ));
        assert!(!recoverable_reconciliation_error(
            &SignerError::Authorization("DirectUser signer/account mismatch".into())
        ));
    }
}
