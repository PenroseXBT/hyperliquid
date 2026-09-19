use crate::domain::scheduler::RequestSubject;
use crate::public_mainnet::{SourceAssetPosition, SourceStateResponse};
use crate::streaming::PublicTrade;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Transaction};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::str::FromStr;

const SCHEMA_VERSION: i64 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFillRecord {
    coin: String,
    side: String,
    px: Decimal,
    sz: Decimal,
    time: u64,
    tid: u64,
    #[serde(default)]
    oid: Option<u64>,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    start_position: Option<Decimal>,
    #[serde(default)]
    closed_pnl: Option<Decimal>,
    #[serde(default)]
    fee: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFillPage {
    pub wallet: String,
    pub start_ms: u64,
    pub end_ms: u64,
    fills: Vec<SourceFillRecord>,
}

impl SourceFillPage {
    pub fn parse(subject: &RequestSubject, bytes: &[u8]) -> Result<Self, String> {
        let RequestSubject::SourceHistory {
            candidate,
            start_ms,
            end_ms,
        } = subject
        else {
            return Err("source fill history requires a bounded wallet interval".into());
        };
        let mut fills: Vec<SourceFillRecord> =
            serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        if start_ms > end_ms
            || end_ms - start_ms > 86_400_000
            || fills.len() > 2000
            || fills.iter().any(|f| {
                f.time < *start_ms
                    || f.time > *end_ms
                    || f.coin.is_empty()
                    || !matches!(f.side.as_str(), "A" | "B")
                    || f.px <= Decimal::ZERO
                    || f.sz <= Decimal::ZERO
            })
        {
            return Err("invalid source fill page".into());
        }
        fills.sort_by(|a, b| (a.time, &a.coin, a.tid).cmp(&(b.time, &b.coin, b.tid)));
        if fills.windows(2).any(|pair| {
            (pair[0].time, &pair[0].coin, pair[0].tid) == (pair[1].time, &pair[1].coin, pair[1].tid)
        }) {
            return Err("duplicate source fill identity within page".into());
        }
        Ok(Self {
            wallet: candidate.to_ascii_lowercase(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            fills,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceContinuityStatus {
    pub durable_baselines: usize,
    pub live_state_confirmed: usize,
    pub live_state_recovering: usize,
    pub history_contiguous: usize,
    pub history_catching_up: usize,
    pub history_gapped: usize,
    pub scan_commits: u64,
    pub last_scan_commit_ms: u128,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Hip3SourceActivity {
    pub markets: BTreeMap<String, Hip3SourceMarketActivity>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Hip3SourceMarketActivity {
    pub source_fills: u64,
    pub source_wallets: BTreeSet<String>,
}

/// Single-writer durable source baseline store. Durable baselines survive a
/// process generation; current-generation confirmation deliberately does not.
pub struct SourceStateStore {
    connection: Connection,
    process_generation: i64,
    wallets: BTreeSet<String>,
    scan_pending: BTreeMap<String, (SourceStateResponse, bool, bool)>,
    pub scan_commits: u64,
    pub last_scan_commit_ms: u128,
}

impl SourceStateStore {
    pub fn open(
        path: impl AsRef<Path>,
        cohort: &str,
        wallets: impl IntoIterator<Item = String>,
    ) -> Result<Self, String> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or("source-state database path has no parent")?;
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create source-state database parent: {error}"))?;
        let mut connection = Connection::open(path)
            .map_err(|error| format!("open source-state database: {error}"))?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|error| format!("configure source-state busy timeout: {error}"))?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|error| format!("enable source-state WAL: {error}"))?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(|error| format!("enable durable source-state commits: {error}"))?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| format!("enable source-state foreign keys: {error}"))?;

        let wallets = wallets
            .into_iter()
            .map(|wallet| wallet.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let transaction = connection
            .transaction()
            .map_err(|error| format!("begin source-state initialization: {error}"))?;
        initialize_schema(&transaction)?;
        let process_generation = next_process_generation(&transaction)?;
        transaction
            .execute("UPDATE source_wallet SET enabled = 0", [])
            .map_err(|error| format!("disable prior source cohort: {error}"))?;
        for wallet in &wallets {
            transaction
                .execute(
                    "INSERT INTO source_wallet (
                         wallet_address, cohort, enabled, baseline_generation
                     ) VALUES (?1, ?2, 1, 0)
                     ON CONFLICT(wallet_address) DO UPDATE SET
                         cohort = excluded.cohort,
                         enabled = 1",
                    params![wallet, cohort],
                )
                .map_err(|error| format!("register source wallet: {error}"))?;
            transaction
                .execute(
                    "INSERT INTO source_recovery (
                         wallet_address, durable_baseline, live_confirmed,
                         stream_gap, process_generation, confirmed_at_ms
                     ) VALUES (?1, 0, 0, 0, ?2, NULL)
                     ON CONFLICT(wallet_address) DO UPDATE SET
                         live_confirmed = 0,
                         stream_gap = 0,
                         process_generation = excluded.process_generation,
                         confirmed_at_ms = NULL",
                    params![wallet, process_generation],
                )
                .map_err(|error| format!("initialize source recovery generation: {error}"))?;
            transaction.execute("INSERT OR IGNORE INTO source_history_cursor (wallet_address,history_state) VALUES (?1,'unknown')", [&wallet])
                .map_err(|e| e.to_string())?;
        }
        transaction
            .commit()
            .map_err(|error| format!("commit source-state initialization: {error}"))?;
        Ok(Self {
            connection,
            process_generation,
            wallets,
            scan_pending: BTreeMap::new(),
            scan_commits: 0,
            last_scan_commit_ms: 0,
        })
    }

    pub fn process_generation(&self) -> u64 {
        u64::try_from(self.process_generation).unwrap_or_default()
    }

    pub fn load_baselines(&self) -> Result<Vec<SourceStateResponse>, String> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT w.wallet_address, w.account_value, w.last_exchange_time_ms,
                        w.state_hash, w.block_number
                 FROM source_wallet w
                 JOIN source_recovery r USING(wallet_address)
                 WHERE w.enabled = 1 AND r.durable_baseline = 1
                 ORDER BY w.wallet_address",
            )
            .map_err(|error| format!("prepare source baseline restore: {error}"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })
            .map_err(|error| format!("query source baselines: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("read source baselines: {error}"))?;
        drop(statement);

        let mut restored = Vec::with_capacity(rows.len());
        for (wallet, account_value, source_time_ms, expected_hash, block_number) in rows {
            let source_time_ms = decode_u64(source_time_ms, "source exchange time")?;
            let mut positions = BTreeMap::new();
            let mut position_statement = self
                .connection
                .prepare(
                    "SELECT coin, signed_size, signed_notional, entry_price, unrealized_pnl
                     FROM source_position WHERE wallet_address = ?1 ORDER BY coin",
                )
                .map_err(|error| format!("prepare source positions: {error}"))?;
            let position_rows = position_statement
                .query_map([&wallet], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(|error| format!("query source positions: {error}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("read source positions: {error}"))?;
            for (coin, signed_size, signed_notional, entry_price, unrealized_pnl) in position_rows {
                let position = SourceAssetPosition {
                    asset: coin.clone(),
                    signed_size: parse_decimal(&signed_size)?,
                    signed_notional: parse_decimal(&signed_notional)?,
                    entry_price: entry_price.as_deref().map(parse_decimal).transpose()?,
                    unrealized_pnl: unrealized_pnl.as_deref().map(parse_decimal).transpose()?,
                };
                positions.insert(coin, position);
            }
            let state = SourceStateResponse {
                block_number: block_number
                    .map(|b| decode_u64(b, "snapshot block"))
                    .transpose()?,
                candidate_id: wallet,
                account_value: parse_decimal(&account_value)?,
                source_time_ms,
                positions,
                closed_candles: Vec::new(),
            };
            if expected_hash.as_slice() != state_hash(&state)?.as_slice() {
                return Err(format!(
                    "source-state checksum mismatch for {}",
                    state.candidate_id
                ));
            }
            restored.push(state);
        }
        Ok(restored)
    }

    /// Trade updates and cohort commits share this connection; no second writer task.
    pub fn persist(
        &mut self,
        state: &SourceStateResponse,
        live_confirmed: bool,
        stream_gap: bool,
    ) -> Result<(), String> {
        let tx = self.connection.transaction().map_err(|e| e.to_string())?;
        write_source_state(
            &tx,
            self.process_generation,
            state,
            live_confirmed,
            stream_gap,
        )?;
        tx.commit().map_err(|e| e.to_string())?;
        if let Some(staged) = self
            .scan_pending
            .get_mut(&state.candidate_id.to_ascii_lowercase())
        {
            if state.source_time_ms >= staged.0.source_time_ms {
                *staged = (state.clone(), live_confirmed, stream_gap);
            }
        }
        Ok(())
    }

    /// Stage network results without holding a SQLite transaction open. Once
    /// every enabled wallet is present, commit the entire cohort atomically.
    /// Intervening trade writes replace staged states, preventing regression.
    pub fn stage_scan(
        &mut self,
        state: &SourceStateResponse,
        live_confirmed: bool,
        stream_gap: bool,
    ) -> Result<(), String> {
        let wallet = state.candidate_id.to_ascii_lowercase();
        if !self.wallets.contains(&wallet) {
            return Err("scan wallet is not enabled".into());
        }
        self.scan_pending
            .insert(wallet, (state.clone(), live_confirmed, stream_gap));
        if self.scan_pending.len() != self.wallets.len() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let tx = self.connection.transaction().map_err(|e| e.to_string())?;
        for (state, confirmed, gap) in self.scan_pending.values() {
            write_source_state(&tx, self.process_generation, state, *confirmed, *gap)?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        self.scan_commits += 1;
        self.last_scan_commit_ms = started.elapsed().as_millis();
        eprintln!(
            "event=source_scan_committed wallets={} elapsed_ms={} generation={}",
            self.scan_pending.len(),
            self.last_scan_commit_ms,
            self.scan_commits
        );
        self.scan_pending.clear();
        Ok(())
    }

    /// Establishes the first provable boundary for a newly tracked wallet.
    /// History before this boundary is intentionally not synthesized.
    pub fn continuity_status(&self) -> Result<SourceContinuityStatus, String> {
        self.connection
            .query_row(
                "SELECT
                    SUM(CASE WHEN r.durable_baseline = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN r.live_confirmed = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN r.live_confirmed = 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN h.history_state = 'contiguous' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN h.history_state IN ('unknown','catching_up') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN h.history_state = 'gapped_unrecoverable' THEN 1 ELSE 0 END)
                 FROM source_wallet w
                 JOIN source_recovery r USING(wallet_address)
                 JOIN source_history_cursor h USING(wallet_address)
                 WHERE w.enabled = 1",
                [],
                |row| {
                    Ok(SourceContinuityStatus {
                        durable_baselines: row.get::<_, i64>(0)?.max(0) as usize,
                        live_state_confirmed: row.get::<_, i64>(1)?.max(0) as usize,
                        live_state_recovering: row.get::<_, i64>(2)?.max(0) as usize,
                        history_contiguous: row.get::<_, i64>(3)?.max(0) as usize,
                        history_catching_up: row.get::<_, i64>(4)?.max(0) as usize,
                        history_gapped: row.get::<_, i64>(5)?.max(0) as usize,
                        scan_commits: self.scan_commits,
                        last_scan_commit_ms: self.last_scan_commit_ms,
                    })
                },
            )
            .map_err(|error| format!("read source continuity status: {error}"))
    }

    pub fn hip3_source_activity(&self) -> Result<Hip3SourceActivity, String> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT f.coin, f.wallet_address, COUNT(*)
                 FROM source_fill_event f
                 JOIN source_wallet w USING(wallet_address)
                 WHERE w.enabled = 1 AND instr(f.coin, ':') > 0
                 GROUP BY f.coin, f.wallet_address
                 ORDER BY f.coin, f.wallet_address",
            )
            .map_err(|error| format!("prepare HIP-3 source activity query: {error}"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|error| format!("query HIP-3 source activity: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("read HIP-3 source activity: {error}"))?;
        let mut activity = Hip3SourceActivity::default();
        for (coin, wallet, count) in rows {
            let count = u64::try_from(count)
                .map_err(|_| "HIP-3 source fill count is negative".to_string())?;
            let market = activity.markets.entry(coin).or_default();
            market.source_fills = market
                .source_fills
                .checked_add(count)
                .ok_or("HIP-3 source fill count overflow")?;
            market.source_wallets.insert(wallet);
        }
        Ok(activity)
    }

    pub fn mark_stream_gap(&mut self) -> Result<(), String> {
        self.scan_pending.clear();
        self.connection
            .execute(
                "UPDATE source_recovery SET
                     live_confirmed = 0,
                     stream_gap = 1,
                     process_generation = ?1,
                     confirmed_at_ms = NULL",
                [self.process_generation],
            )
            .map_err(|error| format!("persist source stream gap: {error}"))?;
        Ok(())
    }

    /// Low-priority history is independent of live baseline admission. A
    /// retained boundary/anchor is never advanced by public stream events.
    pub fn history_request(
        &self,
        wallet: &str,
        now: u64,
    ) -> Result<Option<RequestSubject>, String> {
        let row: Option<(Option<i64>,Option<i64>,Option<i64>,bool)> = self.connection.query_row(
            "SELECT h.contiguous_through_ms,h.last_fill_time_ms,w.last_exchange_time_ms,h.unrecoverable_history_gap FROM source_wallet w JOIN source_history_cursor h USING(wallet_address) WHERE w.wallet_address=?1 AND w.enabled=1",
            [wallet], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(|e| e.to_string())?;
        let Some((through, last, baseline, gapped)) = row else {
            return Ok(None);
        };
        let Some(boundary) = through.or(last).or(baseline) else {
            return Ok(None);
        };
        let boundary = decode_u64(boundary, "source history boundary")?;
        if boundary >= now {
            return Ok(None);
        }
        let anchor = last
            .map(|n| decode_u64(n, "source history anchor"))
            .transpose()?
            .unwrap_or(boundary);
        let start_ms = if !gapped && boundary.saturating_sub(anchor) < 86_400_000 {
            anchor.min(boundary)
        } else {
            boundary
        };
        Ok(Some(RequestSubject::SourceHistory {
            candidate: wallet.to_string(),
            start_ms,
            end_ms: now.min(start_ms.saturating_add(86_400_000)),
        }))
    }

    pub fn persist_trade(
        &mut self,
        trade: &PublicTrade,
        observed_at_ms: u64,
    ) -> Result<(), String> {
        if trade.buyer == trade.seller {
            return Ok(());
        }
        let tx = self.connection.transaction().map_err(|e| e.to_string())?;
        for (wallet, side) in [(&trade.buyer, "B"), (&trade.seller, "A")] {
            let enabled: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM source_wallet WHERE wallet_address=?1 AND enabled=1)", [wallet], |r| r.get(0)).map_err(|e|e.to_string())?;
            if !enabled {
                continue;
            }
            journal_source_fill(
                &tx,
                wallet,
                &SourceFillRecord {
                    coin: trade.coin.clone(),
                    side: side.into(),
                    px: trade.price,
                    sz: trade.size,
                    time: trade.time_ms,
                    tid: trade.trade_id,
                    oid: None,
                    hash: Some(trade.hash.clone()),
                    start_position: None,
                    closed_pnl: None,
                    fee: None,
                },
                observed_at_ms,
                "live_stream",
            )?;
        }
        tx.commit().map_err(|e| e.to_string())
    }

    pub fn persist_history(
        &mut self,
        page: &SourceFillPage,
        observed_at_ms: u64,
    ) -> Result<(), String> {
        let tx = self.connection.transaction().map_err(|e| e.to_string())?;
        let (through,anchor_time,anchor_key): (Option<i64>,Option<i64>,Option<String>) = tx.query_row(
            "SELECT h.contiguous_through_ms,h.last_fill_time_ms,h.last_fill_event_key FROM source_history_cursor h JOIN source_wallet w USING(wallet_address) WHERE wallet_address=?1 AND enabled=1",
            [&page.wallet], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|e.to_string())?;
        let mut anchor_found = anchor_key.is_none();
        let mut last_key = None;
        for fill in &page.fills {
            let key =
                journal_source_fill(&tx, &page.wallet, fill, observed_at_ms, "history_catchup")?;
            anchor_found |= anchor_key.as_ref() == Some(&key);
            last_key = Some(key);
        }
        let full = page.fills.len() == 2000;
        let last_time = page.fills.last().map(|f| f.time);
        let stalled = full && last_time == Some(page.start_ms);
        // Keep the last timestamp inclusive on a full page: multiple fills can
        // share a millisecond. Never skip an unpageable timestamp silently.
        let next = if stalled {
            page.start_ms.saturating_add(1).min(page.end_ms)
        } else if full {
            last_time.unwrap()
        } else {
            page.end_ms
        };
        let missing_anchor = anchor_time.is_some() && !anchor_found;
        let gap = stalled || missing_anchor;
        let boundary = through
            .unwrap_or(0)
            .max(encode_u64(next, "source history boundary")?);
        tx.execute("UPDATE source_history_cursor SET contiguous_through_ms=?2, last_fill_time_ms=CASE WHEN ?3 IS NOT NULL AND ?3>=COALESCE(last_fill_time_ms,0) THEN ?3 ELSE last_fill_time_ms END, last_fill_event_key=CASE WHEN ?3 IS NOT NULL AND ?3>=COALESCE(last_fill_time_ms,0) THEN ?4 ELSE last_fill_event_key END, unrecoverable_history_gap=MAX(unrecoverable_history_gap,?5), history_state=CASE WHEN unrecoverable_history_gap=1 OR ?5=1 THEN 'gapped_unrecoverable' WHEN ?6=1 THEN 'catching_up' ELSE 'contiguous' END WHERE wallet_address=?1",
            params![page.wallet,boundary,last_time.map(|n|encode_u64(n,"source fill time")).transpose()?,last_key,i64::from(gap),i64::from(full && !stalled)]).map_err(|e|e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
}

fn write_source_state(
    transaction: &Transaction<'_>,
    process_generation: i64,
    state: &SourceStateResponse,
    live_confirmed: bool,
    stream_gap: bool,
) -> Result<(), String> {
    let mut canonical = state.clone();
    canonical.candidate_id = canonical.candidate_id.to_ascii_lowercase();
    canonical.closed_candles.clear();
    let state_hash = state_hash(&canonical)?;
    let source_time_ms = encode_u64(canonical.source_time_ms, "source exchange time")?;
    let incoming_block_number = canonical
        .block_number
        .map(|block| encode_u64(block, "snapshot block"))
        .transpose()?;
    let updated = transaction
        .execute(
            "UPDATE source_wallet SET
                     account_value = ?2,
                     last_exchange_time_ms = ?3,
                     last_stream_time_ms = MAX(COALESCE(last_stream_time_ms, 0), ?3),
                     baseline_generation = baseline_generation + 1,
                     state_hash = ?4, block_number = ?5
                 WHERE wallet_address = ?1 AND enabled = 1
                   AND (last_exchange_time_ms IS NULL OR last_exchange_time_ms <= ?3)
                   AND (block_number IS NULL OR (?5 IS NOT NULL AND block_number <= ?5))",
            params![
                canonical.candidate_id,
                canonical.account_value.to_string(),
                source_time_ms,
                state_hash.as_slice(),
                incoming_block_number,
            ],
        )
        .map_err(|error| format!("update durable source wallet: {error}"))?;
    if updated != 1 {
        let current = transaction
            .query_row(
                "SELECT enabled, last_exchange_time_ms, block_number
                     FROM source_wallet WHERE wallet_address = ?1",
                params![canonical.candidate_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| format!("read durable source wallet ordering: {error}"))?;
        let Some((enabled, last_exchange_time_ms, stored_block_number)) = current else {
            return Err(format!(
                "source wallet is not configured: {}",
                canonical.candidate_id
            ));
        };
        if enabled != 1 {
            return Err(format!(
                "source wallet is disabled: {}",
                canonical.candidate_id
            ));
        }
        let timestamp_regressed =
            last_exchange_time_ms.is_some_and(|stored| stored > source_time_ms);
        let block_regressed = match (stored_block_number, incoming_block_number) {
            (Some(_), None) => true,
            (Some(stored), Some(incoming)) => stored > incoming,
            _ => false,
        };
        if timestamp_regressed || block_regressed {
            return Ok(());
        }
        return Err(format!(
            "source wallet ordering update was not applied: {}",
            canonical.candidate_id
        ));
    }
    transaction
        .execute(
            "DELETE FROM source_position WHERE wallet_address = ?1
                 AND coin NOT IN (SELECT value FROM json_each(?2))",
            params![
                canonical.candidate_id,
                serde_json::to_string(&canonical.positions.keys().collect::<Vec<_>>())
                    .map_err(|e| e.to_string())?
            ],
        )
        .map_err(|error| format!("replace durable source positions: {error}"))?;
    for (coin, position) in &canonical.positions {
        transaction
            .execute(
                "INSERT INTO source_position (
                         wallet_address, coin, signed_size, signed_notional,
                         entry_price, unrealized_pnl, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(wallet_address,coin) DO UPDATE SET
                         signed_size=excluded.signed_size, signed_notional=excluded.signed_notional,
                         entry_price=excluded.entry_price, unrealized_pnl=excluded.unrealized_pnl,
                         updated_at_ms=excluded.updated_at_ms
                     WHERE source_position.signed_size IS NOT excluded.signed_size
                        OR source_position.signed_notional IS NOT excluded.signed_notional
                        OR source_position.entry_price IS NOT excluded.entry_price
                        OR source_position.unrealized_pnl IS NOT excluded.unrealized_pnl",
                params![
                    canonical.candidate_id,
                    coin,
                    position.signed_size.to_string(),
                    position.signed_notional.to_string(),
                    position.entry_price.map(|value| value.to_string()),
                    position.unrealized_pnl.map(|value| value.to_string()),
                    source_time_ms,
                ],
            )
            .map_err(|error| format!("write durable source position: {error}"))?;
    }
    transaction
        .execute(
            "UPDATE source_recovery SET
                     durable_baseline = 1,
                     live_confirmed = ?2,
                     stream_gap = ?3,
                     process_generation = ?4,
                     confirmed_at_ms = CASE WHEN ?2 = 1 THEN ?5 ELSE NULL END
                 WHERE wallet_address = ?1",
            params![
                canonical.candidate_id,
                i64::from(live_confirmed),
                i64::from(stream_gap),
                process_generation,
                source_time_ms,
            ],
        )
        .map_err(|error| format!("update source recovery state: {error}"))?;
    Ok(())
}

/// Summary of a validated one-time SQLite backfill import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBackfillSummary {
    pub wallets: usize,
    pub durable_baselines: usize,
    pub fill_events: u64,
    pub history_cursors: usize,
}

/// Outcome of attempting a backfill import before `SourceStateStore::open`.
///
/// `SkippedDestinationExists` is the idempotent steady state: the destination
/// already exists, so every restart keeps using its own durable data and never
/// re-copies the old root, which would wipe fills accrued since the import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBackfillOutcome {
    Imported(SourceBackfillSummary),
    SkippedDestinationExists,
}

fn require_regular_file_no_symlink(path: &Path, field: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{field} is not accessible: {error}"))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(format!(
            "{field} must never be a symlink: {}",
            path.display()
        ));
    }
    if !file_type.is_file() {
        return Err(format!(
            "{field} must be a regular file: {}",
            path.display()
        ));
    }
    Ok(())
}

fn dest_sidecar_exists(dest: &Path) -> Result<bool, String> {
    let dest_string = dest.to_string_lossy().into_owned();
    let mut exists = false;
    for suffix in [
        String::new(),
        "-wal".to_string(),
        "-shm".to_string(),
        "-journal".to_string(),
    ] {
        let candidate = if suffix.is_empty() {
            dest.to_path_buf()
        } else {
            Path::new(&format!("{dest_string}{suffix}")).to_path_buf()
        };
        match std::fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "source backfill destination must never be a symlink: {}",
                    candidate.display()
                ));
            }
            Ok(_) => exists = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "inspect source backfill destination {}: {error}",
                    candidate.display()
                ));
            }
        }
    }
    Ok(exists)
}

/// One-time validated import of an old root's `source-state.sqlite` into a
/// fresh `DATA_ROOT`.
///
/// Must be called before `SourceStateStore::open` on the destination. Uses
/// `VACUUM INTO` so the copy includes WAL content as one consistent snapshot
/// instead of a raw file copy that can drop uncheckpointed fills. Validates
/// schema version and every durable baseline checksum; on validation failure
/// the partial destination is removed and an error is returned so the caller
/// fails closed instead of starting with zero baselines silently.
pub fn import_source_backfill(
    dest: impl AsRef<Path>,
    src: impl AsRef<Path>,
) -> Result<SourceBackfillOutcome, String> {
    let dest = dest.as_ref();
    let src = src.as_ref();
    if dest_sidecar_exists(dest)? {
        return Ok(SourceBackfillOutcome::SkippedDestinationExists);
    }
    require_regular_file_no_symlink(src, "source backfill file")?;
    if src == dest {
        return Err("source backfill file must not equal the destination database".into());
    }
    let dest_parent = dest
        .parent()
        .ok_or("source backfill destination has no parent")?;
    std::fs::create_dir_all(dest_parent)
        .map_err(|error| format!("create source backfill destination parent: {error}"))?;
    let parent_type = std::fs::symlink_metadata(dest_parent)
        .map_err(|error| format!("inspect source backfill destination parent: {error}"))?
        .file_type();
    if parent_type.is_symlink() || !parent_type.is_dir() {
        return Err(format!(
            "source backfill destination parent must be a real directory: {}",
            dest_parent.display()
        ));
    }

    let src_connection =
        Connection::open(src).map_err(|error| format!("open source backfill file: {error}"))?;
    src_connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|error| format!("configure source backfill busy timeout: {error}"))?;
    let schema_version: i64 = src_connection
        .query_row(
            "SELECT schema_version FROM source_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("source backfill is not a source-state database: {error}"))?;
    if schema_version != SCHEMA_VERSION {
        return Err(format!(
            "source backfill schema mismatch: expected {SCHEMA_VERSION}, found {schema_version}"
        ));
    }
    let dest_string = dest.to_string_lossy().into_owned();
    let escaped = dest_string.replace('\'', "''");
    src_connection
        .execute_batch(&format!("VACUUM INTO '{escaped}'"))
        .map_err(|error| format!("copy source backfill snapshot: {error}"))?;
    drop(src_connection);

    let validation = (|| -> Result<SourceBackfillSummary, String> {
        let connection = Connection::open_with_flags(dest, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| format!("open copied source backfill: {error}"))?;
        let version: i64 = connection
            .query_row(
                "SELECT schema_version FROM source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|error| format!("read copied source backfill schema: {error}"))?;
        if version != SCHEMA_VERSION {
            return Err(format!(
                "copied source backfill schema mismatch: expected {SCHEMA_VERSION}, found {version}"
            ));
        }
        let wallets: i64 = connection
            .query_row("SELECT count(*) FROM source_wallet", [], |row| row.get(0))
            .map_err(|error| format!("count copied source wallets: {error}"))?;
        let history_cursors: i64 = connection
            .query_row("SELECT count(*) FROM source_history_cursor", [], |row| {
                row.get(0)
            })
            .map_err(|error| format!("count copied source history cursors: {error}"))?;
        let fill_events: i64 = connection
            .query_row("SELECT count(*) FROM source_fill_event", [], |row| {
                row.get(0)
            })
            .map_err(|error| format!("count copied source fills: {error}"))?;
        let mut baseline_statement = connection
            .prepare(
                "SELECT w.wallet_address, w.account_value, w.last_exchange_time_ms, w.state_hash, w.block_number
                 FROM source_wallet w
                 JOIN source_recovery r USING(wallet_address)
                 WHERE r.durable_baseline = 1
                 ORDER BY w.wallet_address",
            )
            .map_err(|error| format!("prepare copied baseline validation: {error}"))?;
        let baseline_rows = baseline_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })
            .map_err(|error| format!("query copied baselines: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("read copied baselines: {error}"))?;
        drop(baseline_statement);
        for (wallet, account_value, source_time_ms, expected_hash, block_number) in &baseline_rows {
            let source_time_ms = decode_u64(*source_time_ms, "source exchange time")?;
            let mut positions = BTreeMap::new();
            let mut position_statement = connection
                .prepare(
                    "SELECT coin, signed_size, signed_notional, entry_price, unrealized_pnl
                     FROM source_position WHERE wallet_address = ?1 ORDER BY coin",
                )
                .map_err(|error| format!("prepare copied positions: {error}"))?;
            let position_rows = position_statement
                .query_map([wallet], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(|error| format!("query copied positions: {error}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("read copied positions: {error}"))?;
            for (coin, signed_size, signed_notional, entry_price, unrealized_pnl) in position_rows {
                positions.insert(
                    coin.clone(),
                    SourceAssetPosition {
                        asset: coin,
                        signed_size: parse_decimal(&signed_size)?,
                        signed_notional: parse_decimal(&signed_notional)?,
                        entry_price: entry_price.as_deref().map(parse_decimal).transpose()?,
                        unrealized_pnl: unrealized_pnl.as_deref().map(parse_decimal).transpose()?,
                    },
                );
            }
            let state = SourceStateResponse {
                block_number: block_number
                    .map(|block| decode_u64(block, "snapshot block"))
                    .transpose()?,
                candidate_id: wallet.clone(),
                account_value: parse_decimal(account_value)?,
                source_time_ms,
                positions,
                closed_candles: Vec::new(),
            };
            if expected_hash.as_slice() != state_hash(&state)?.as_slice() {
                return Err(format!("source backfill checksum mismatch for {wallet}"));
            }
        }
        Ok(SourceBackfillSummary {
            wallets: usize::try_from(wallets.max(0)).unwrap_or_default(),
            durable_baselines: baseline_rows.len(),
            fill_events: u64::try_from(fill_events.max(0)).unwrap_or_default(),
            history_cursors: usize::try_from(history_cursors.max(0)).unwrap_or_default(),
        })
    })();
    match validation {
        Ok(summary) => Ok(SourceBackfillOutcome::Imported(summary)),
        Err(error) => {
            let _ = std::fs::remove_file(dest);
            let _ = std::fs::remove_file(format!("{dest_string}-wal"));
            let _ = std::fs::remove_file(format!("{dest_string}-shm"));
            let _ = std::fs::remove_file(format!("{dest_string}-journal"));
            Err(error)
        }
    }
}

fn journal_source_fill(
    tx: &Transaction<'_>,
    wallet: &str,
    fill: &SourceFillRecord,
    observed: u64,
    provenance: &str,
) -> Result<String, String> {
    let previous: Option<(String,i64,String,String,String,Option<String>,Option<String>,Option<String>,Option<String>)> = tx.query_row(
        "SELECT event_key,exchange_ms,side,px,sz,oid,start_position,closed_pnl,fee FROM source_fill_event WHERE wallet_address=?1 AND coin=?2 AND tid=?3 AND exchange_ms=?4",params![wallet,fill.coin,fill.tid.to_string(),encode_u64(fill.time,"source fill time")?],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional().map_err(|e|e.to_string())?;
    let key = if let Some((key, time, side, px, sz, oid, start, pnl, fee)) = previous {
        if decode_u64(time, "source fill time")? != fill.time
            || !matches!(
                (side.as_str(), fill.side.as_str()),
                ("buy" | "B", "B") | ("sell" | "A", "A")
            )
            || parse_decimal(&px)? != fill.px
            || parse_decimal(&sz)? != fill.sz
        {
            return Err("conflicting source fill identity".into());
        }
        if oid
            .as_ref()
            .zip(fill.oid)
            .is_some_and(|(old, new)| old != &new.to_string())
        {
            return Err("conflicting source order identity".into());
        }
        for (old, new) in [
            (start, fill.start_position),
            (pnl, fill.closed_pnl),
            (fee, fill.fee),
        ] {
            if let (Some(old), Some(new)) = (old, new) {
                if parse_decimal(&old)? != new {
                    return Err("conflicting source fill economics".into());
                }
            }
        }
        key
    } else {
        format!("{}:{}:{}", fill.time, fill.coin, fill.tid)
    };
    tx.execute("INSERT INTO source_fill_event (wallet_address,event_key,exchange_ms,tid,oid,tx_hash,coin,side,px,sz,start_position,closed_pnl,fee,observed_at_ms,provenance) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15) ON CONFLICT(wallet_address,event_key) DO UPDATE SET oid=COALESCE(source_fill_event.oid,excluded.oid),start_position=COALESCE(source_fill_event.start_position,excluded.start_position),closed_pnl=COALESCE(source_fill_event.closed_pnl,excluded.closed_pnl),fee=COALESCE(source_fill_event.fee,excluded.fee)",
        params![wallet,key,encode_u64(fill.time,"source fill time")?,fill.tid.to_string(),fill.oid.map(|n|n.to_string()),fill.hash,fill.coin,if fill.side == "B" { "buy" } else { "sell" },fill.px.to_string(),fill.sz.to_string(),fill.start_position.map(|v|v.to_string()),fill.closed_pnl.map(|v|v.to_string()),fill.fee.map(|v|v.to_string()),encode_u64(observed,"source fill observation")?,provenance]).map_err(|e|e.to_string())?;
    Ok(key)
}

fn initialize_schema(transaction: &Transaction<'_>) -> Result<(), String> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS source_meta (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 schema_version INTEGER NOT NULL,
                 process_generation INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO source_meta VALUES (1, 1, 0);
             CREATE TABLE IF NOT EXISTS source_wallet (
                 wallet_address TEXT PRIMARY KEY,
                 cohort TEXT NOT NULL,
                 enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                 account_value TEXT,
                 last_exchange_time_ms INTEGER,
                 last_stream_time_ms INTEGER,
                 baseline_generation INTEGER NOT NULL,
                 state_hash BLOB
             );
             CREATE TABLE IF NOT EXISTS source_position (
                 wallet_address TEXT NOT NULL REFERENCES source_wallet(wallet_address) ON DELETE CASCADE,
                 coin TEXT NOT NULL,
                 signed_size TEXT NOT NULL,
                 signed_notional TEXT NOT NULL,
                 entry_price TEXT,
                 unrealized_pnl TEXT,
                 updated_at_ms INTEGER NOT NULL,
                 PRIMARY KEY(wallet_address, coin)
             );
             CREATE TABLE IF NOT EXISTS source_recovery (
                 wallet_address TEXT PRIMARY KEY REFERENCES source_wallet(wallet_address) ON DELETE CASCADE,
                 durable_baseline INTEGER NOT NULL CHECK(durable_baseline IN (0, 1)),
                 live_confirmed INTEGER NOT NULL CHECK(live_confirmed IN (0, 1)),
                 stream_gap INTEGER NOT NULL CHECK(stream_gap IN (0, 1)),
                 process_generation INTEGER NOT NULL,
                 confirmed_at_ms INTEGER
             );
             CREATE TABLE IF NOT EXISTS source_fill_event (
                 wallet_address TEXT NOT NULL REFERENCES source_wallet(wallet_address),event_key TEXT NOT NULL,
                 exchange_ms INTEGER NOT NULL,tid TEXT,oid TEXT,tx_hash TEXT,coin TEXT NOT NULL,side TEXT NOT NULL,
                 px TEXT NOT NULL,sz TEXT NOT NULL,start_position TEXT,closed_pnl TEXT,fee TEXT,observed_at_ms INTEGER NOT NULL,
                 provenance TEXT NOT NULL CHECK(provenance IN ('live_stream','history_catchup')),PRIMARY KEY(wallet_address,event_key)
             );
             CREATE INDEX IF NOT EXISTS source_fill_event_time ON source_fill_event(wallet_address,exchange_ms,event_key);
             CREATE INDEX IF NOT EXISTS source_fill_event_identity ON source_fill_event(wallet_address,coin,tid);
             CREATE TABLE IF NOT EXISTS source_history_cursor (
                 wallet_address TEXT PRIMARY KEY REFERENCES source_wallet(wallet_address),last_fill_time_ms INTEGER,
                 last_fill_event_key TEXT,contiguous_through_ms INTEGER,history_state TEXT NOT NULL
                 CHECK(history_state IN ('unknown','catching_up','contiguous','gapped_unrecoverable')),
                 unrecoverable_history_gap INTEGER NOT NULL DEFAULT 0 CHECK(unrecoverable_history_gap IN (0,1))
             );
",
        )
        .map_err(|error| format!("initialize source-state schema: {error}"))?;
    let mut version = transaction
        .query_row(
            "SELECT schema_version FROM source_meta WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("read source-state schema version: {error}"))?;
    if version == 1 {
        transaction.execute_batch("ALTER TABLE source_wallet ADD COLUMN block_number INTEGER; UPDATE source_meta SET schema_version=2 WHERE singleton=1;").map_err(|e|e.to_string())?;
        version = 2;
    }
    if version != SCHEMA_VERSION {
        return Err(format!(
            "source-state schema mismatch: expected {SCHEMA_VERSION}, found {version}"
        ));
    }
    Ok(())
}

fn next_process_generation(transaction: &Transaction<'_>) -> Result<i64, String> {
    let current = transaction
        .query_row(
            "SELECT process_generation FROM source_meta WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("read source process generation: {error}"))?;
    let next = current
        .checked_add(1)
        .ok_or("source process generation overflow")?;
    transaction
        .execute(
            "UPDATE source_meta SET process_generation = ?1 WHERE singleton = 1",
            [next],
        )
        .map_err(|error| format!("advance source process generation: {error}"))?;
    Ok(next)
}

fn state_hash(state: &SourceStateResponse) -> Result<[u8; 32], String> {
    let encoded = serde_json::to_vec(state)
        .map_err(|error| format!("encode durable source state: {error}"))?;
    Ok(Sha256::digest(encoded).into())
}

fn parse_decimal(value: &str) -> Result<Decimal, String> {
    let mut parsed = Decimal::from_str(value)
        .map_err(|error| format!("invalid durable source decimal: {error}"))?;
    // Decimal parsing drops the sign of zero; preserve the exact durable hash.
    if parsed.is_zero() && value.starts_with('-') {
        parsed.set_sign_negative(true);
    }
    Ok(parsed)
}

fn encode_u64(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{field} exceeds SQLite integer range"))
}

fn decode_u64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{field} is negative"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::DecisionEngine;
    use crate::domain::scheduler::{ReadRequestKind, SourceTier};
    use crate::public_mainnet::{AcceptedPublicResponse, PublicPayload};
    use crate::streaming::StreamingSourceBook;

    fn page(wallet: &str, start: u64, end: u64, fills: serde_json::Value) -> SourceFillPage {
        SourceFillPage::parse(
            &RequestSubject::SourceHistory {
                candidate: wallet.into(),
                start_ms: start,
                end_ms: end,
            },
            &serde_json::to_vec(&fills).unwrap(),
        )
        .unwrap()
    }

    fn fill(time: u64, tid: u64, side: &str) -> serde_json::Value {
        serde_json::json!({"coin":"xyz:AMD","px":"100","sz":"0.25","side":side,"time":time,"tid":tid,"oid":tid+10,"hash":"0xabc","startPosition":"0","closedPnl":"0","fee":"0.01"})
    }

    #[test]
    fn hip3_source_activity_summarizes_enabled_prefixed_fills() {
        let root = tempfile::tempdir().unwrap();
        let first = wallet('a');
        let second = wallet('b');
        let disabled = wallet('c');
        let mut store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "hip3",
            [first.clone(), second.clone()],
        )
        .unwrap();
        store
            .persist_history(
                &page(
                    &first,
                    100,
                    300,
                    serde_json::json!([
                        fill(150, 1, "B"),
                        {"coin":"BTC","px":"100","sz":"0.25","side":"B","time":160,"tid":2,"oid":12,"hash":"0xabc","startPosition":"0","closedPnl":"0","fee":"0.01"}
                    ]),
                ),
                301,
            )
            .unwrap();
        store
            .persist_history(
                &page(&second, 100, 300, serde_json::json!([fill(170, 3, "A")])),
                302,
            )
            .unwrap();
        drop(store);

        let mut store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "hip3",
            [first.clone(), second.clone(), disabled.clone()],
        )
        .unwrap();
        store
            .persist_history(
                &page(&disabled, 100, 300, serde_json::json!([fill(180, 4, "B")])),
                303,
            )
            .unwrap();
        drop(store);

        let store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "hip3",
            [first.clone(), second.clone()],
        )
        .unwrap();
        let activity = store.hip3_source_activity().unwrap();
        assert_eq!(activity.markets.len(), 1);
        assert_eq!(activity.markets["xyz:AMD"].source_fills, 2);
        assert_eq!(
            activity.markets["xyz:AMD"].source_wallets,
            BTreeSet::from([first, second])
        );
    }

    #[test]
    fn effective_frozen_cohort_sql_restart_preserves_history_and_admits_wallets_independently() {
        let config_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../railway-frozen");
        let loaded = crate::EngineState::load_with_very_profitable_layer(
            config_root.join("copytrade.json"),
            Some(config_root.join("very-profitable-layer.json")),
        )
        .unwrap();
        let wallets: BTreeSet<_> = loaded
            .config()
            .candidates
            .iter()
            .filter(|c| c.enabled)
            .map(|c| c.address.to_ascii_lowercase())
            .collect();
        // Pinned to the frozen production cohort: any unintended change to
        // railway-frozen/copytrade.json must fail here, not in production.
        let cohort = wallets.len();
        // Pinned to the effective production cohort: 169 config candidates
        // plus qualified layer members merged at load (246 total). Any
        // unintended change to the frozen config or layer must fail here.
        let digest = Sha256::digest(format!(
            "{}\n",
            wallets.iter().cloned().collect::<Vec<_>>().join("\n")
        ));
        assert_eq!(
            format!("{digest:x}"),
            "76d64097084d46fc7deab8aa4b055c7f83402996ba458bab420e119e9654b32c"
        );
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source-state.sqlite");
        let first = wallets.first().unwrap();
        let mut store = SourceStateStore::open(&path, "frozen-cohort", wallets.clone()).unwrap();
        for wallet in &wallets {
            store
                .persist(&state(wallet.clone(), 100), true, false)
                .unwrap();
        }
        store
            .persist_history(
                &page(first, 100, 200, serde_json::json!([fill(150, 1, "B")])),
                201,
            )
            .unwrap();
        let baselines = store.load_baselines().unwrap();
        let history_before: String = store.connection.query_row("SELECT group_concat(wallet_address||':'||COALESCE(last_fill_time_ms,0)||':'||COALESCE(last_fill_event_key,'')||':'||COALESCE(contiguous_through_ms,0)||':'||history_state||':'||unrecoverable_history_gap,'|') FROM (SELECT * FROM source_history_cursor ORDER BY wallet_address)",[],|r|r.get(0)).unwrap();
        drop(store);
        for generation in [2, 3] {
            let mut store = SourceStateStore::open(
                &path,
                "frozen-cohort",
                wallets.iter().map(|w| w.to_ascii_uppercase()),
            )
            .unwrap();
            assert_eq!(store.process_generation(), generation);
            assert_eq!(store.load_baselines().unwrap(), baselines);
            assert_eq!(
                store.continuity_status().unwrap(),
                SourceContinuityStatus {
                    durable_baselines: cohort,
                    live_state_confirmed: 0,
                    live_state_recovering: cohort,
                    history_contiguous: 1,
                    history_catching_up: cohort - 1,
                    history_gapped: 0,
                    scan_commits: 0,
                    last_scan_commit_ms: 0
                }
            );
            let history_after: String = store.connection.query_row("SELECT group_concat(wallet_address||':'||COALESCE(last_fill_time_ms,0)||':'||COALESCE(last_fill_event_key,'')||':'||COALESCE(contiguous_through_ms,0)||':'||history_state||':'||unrecoverable_history_gap,'|') FROM (SELECT * FROM source_history_cursor ORDER BY wallet_address)",[],|r|r.get(0)).unwrap();
            assert_eq!(history_after, history_before);
            let counts: (i64,i64,i64,i64) = store.connection.query_row("SELECT (SELECT count(*) FROM source_wallet WHERE enabled=1),(SELECT count(*) FROM source_position),(SELECT count(*) FROM source_history_cursor),(SELECT count(*) FROM source_fill_event)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
            assert_eq!(counts, (cohort as i64, cohort as i64, cohort as i64, 1));
            let mut engine = DecisionEngine::new(
                loaded.config().clone(),
                b"cohort-restart",
                "run",
                40_000,
                80_000,
            )
            .unwrap();
            engine
                .install_very_profitable_layer(loaded.very_profitable_layer().unwrap().clone())
                .unwrap();
            engine.enable_source_stream_mode();
            engine.enter_live_recovery_only(); // Qualification cannot place trades.
            let mut book = StreamingSourceBook::new(wallets.clone()).unwrap();
            for baseline in store.load_baselines().unwrap() {
                book.restore_durable_baseline(baseline).unwrap();
            }
            book.begin_recovery();
            assert_eq!(book.hydrated_count(), cohort);
            assert_eq!(engine.live_confirmed_source_count(), 0);
            for baseline in baselines.iter().take(cohort - 1) {
                let baseline = book.install_baseline(baseline.clone()).unwrap();
                engine
                    .ingest(
                        AcceptedPublicResponse {
                            request_kind: ReadRequestKind::ExpandedSourceState,
                            source_tier: Some(SourceTier::Inactive),
                            subject: baseline.candidate_id.clone(),
                            requested_at_mono: 300,
                            received_at_mono: 301,
                            valid_until_mono: 80_301,
                            payload: PublicPayload::SourceState(baseline.clone()),
                        },
                        301,
                    )
                    .unwrap();
                assert!(engine
                    .admit_reconciled_source_wallet(
                        &baseline.candidate_id,
                        baseline.source_time_ms,
                        301
                    )
                    .unwrap());
                store.persist(&baseline, true, false).unwrap();
            }
            assert_eq!(engine.active_source_count(301), cohort - 1);
            assert_eq!(engine.pending_source_wallets().len(), 1);
            assert_eq!(
                store.continuity_status().unwrap().live_state_confirmed,
                cohort - 1
            );
            assert!(engine.take_prepared_authorized_intents().is_empty());
        }
    }

    #[test]
    fn source_trade_history_overlap_preserves_buyer_seller_and_enriches_without_duplicates() {
        let root = tempfile::tempdir().unwrap();
        let buyer = wallet('1');
        let seller = wallet('2');
        let mut store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "test",
            [buyer.clone(), seller.clone()],
        )
        .unwrap();
        let trade = PublicTrade {
            coin: "xyz:AMD".into(),
            price: Decimal::from(100),
            size: Decimal::new(25, 2),
            time_ms: 150,
            trade_id: 1,
            aggressor_buy: false,
            hash: "0xabc".into(),
            buyer: buyer.clone(),
            seller: seller.clone(),
        };
        store.persist_trade(&trade, 151).unwrap();
        store.persist_trade(&trade, 152).unwrap();
        for (wallet, side) in [(&buyer, "B"), (&seller, "A")] {
            let batch = page(wallet, 100, 200, serde_json::json!([fill(150, 1, side)]));
            store.persist_history(&batch, 201).unwrap();
            store.persist_history(&batch, 202).unwrap();
            let (stored_side, fee, oid): (String, String, String) = store
                .connection
                .query_row(
                    "SELECT side,fee,oid FROM source_fill_event WHERE wallet_address=?1",
                    [wallet],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            assert_eq!(
                (stored_side.as_str(), fee.as_str(), oid.as_str()),
                (if side == "B" { "buy" } else { "sell" }, "0.01", "11")
            );
            // a8bc briefly wrote wire-side A/B into the retained buy/sell
            // journal. Both encodings identify the same immutable fill.
            store
                .connection
                .execute(
                    "UPDATE source_fill_event SET side=?2 WHERE wallet_address=?1",
                    params![wallet, side],
                )
                .unwrap();
            store.persist_history(&batch, 203).unwrap();
            store.persist_trade(&trade, 204).unwrap();
        }
        assert_eq!(
            store
                .connection
                .query_row("SELECT count(*) FROM source_fill_event", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        let mut conflicting = fill(150, 1, "B");
        conflicting["px"] = serde_json::json!("101");
        assert!(store
            .persist_history(
                &page(&buyer, 100, 200, serde_json::json!([conflicting])),
                203
            )
            .is_err());
    }

    #[test]
    fn bounded_history_keeps_full_page_boundary_and_missing_retention_anchor_explicit() {
        let root = tempfile::tempdir().unwrap();
        let wallet = wallet('1');
        let mut store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "test",
            [wallet.clone()],
        )
        .unwrap();
        store
            .persist(&state(wallet.clone(), 100), true, false)
            .unwrap();
        store
            .persist_history(
                &page(&wallet, 100, 200, serde_json::json!([fill(150, 1, "B")])),
                201,
            )
            .unwrap();
        let subject = store.history_request(&wallet, 3000).unwrap().unwrap();
        let records: Vec<_> = (1..=2000).map(|i| fill(149 + i, i, "B")).collect();
        let batch =
            SourceFillPage::parse(&subject, &serde_json::to_vec(&records).unwrap()).unwrap();
        store.persist_history(&batch, 3001).unwrap();
        let next = store.history_request(&wallet, 4000).unwrap().unwrap();
        assert!(matches!(
            next,
            RequestSubject::SourceHistory { start_ms: 2149, .. }
        ));
        // Retention loss must not be relabeled as contiguous history, and it
        // must not revoke the independently confirmed current baseline.
        store
            .persist_history(
                &page(
                    &wallet,
                    2149,
                    4000,
                    serde_json::json!([fill(3000, 3000, "B")]),
                ),
                4001,
            )
            .unwrap();
        let status: String = store
            .connection
            .query_row("SELECT history_state FROM source_history_cursor", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(status, "gapped_unrecoverable");
        assert_eq!(store.continuity_status().unwrap().live_state_confirmed, 1);
        assert!(SourceFillPage::parse(&subject, b"[{\"coin\":\"xyz:AMD\"}]").is_err());
        // A full page at one timestamp cannot be paged without loss. Record
        // that gap and advance only past that millisecond, never to now.
        let records: Vec<_> = (4000..6000).map(|tid| fill(4000, tid, "B")).collect();
        store
            .persist_history(&page(&wallet, 4000, 5000, serde_json::json!(records)), 5001)
            .unwrap();
        assert!(matches!(
            store.history_request(&wallet, 6000).unwrap().unwrap(),
            RequestSubject::SourceHistory { start_ms: 4001, .. }
        ));
        assert_eq!(store.continuity_status().unwrap().history_gapped, 1);
    }

    #[test]
    fn source_sql_exact_decimals_nulls_timestamps_and_restart_reconstruction() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source-state.sqlite");
        let wallet = wallet('3');
        let expected = SourceStateResponse {
            block_number: None,
            candidate_id: wallet.clone(),
            account_value: Decimal::from_str("123456789012345.1234567890123").unwrap(),
            source_time_ms: 1_725_123_456_789,
            positions: BTreeMap::from([
                (
                    "LONG".into(),
                    SourceAssetPosition {
                        asset: "LONG".into(),
                        signed_size: Decimal::from_str("0.00000001").unwrap(),
                        signed_notional: Decimal::from_str("1.234567890123").unwrap(),
                        entry_price: None,
                        unrealized_pnl: Some(Decimal::ZERO),
                    },
                ),
                (
                    "SHORT".into(),
                    SourceAssetPosition {
                        asset: "SHORT".into(),
                        signed_size: Decimal::from_str("-999999.00000001").unwrap(),
                        signed_notional: Decimal::from_str("-987654321.123456789").unwrap(),
                        entry_price: Some(Decimal::from_str("0.00000001").unwrap()),
                        unrealized_pnl: Some(Decimal::from_str("-0.000000000000000001").unwrap()),
                    },
                ),
            ]),
            closed_candles: Vec::new(),
        };
        let mut store = SourceStateStore::open(&path, "test", [wallet.clone()]).unwrap();
        store.persist(&expected, true, false).unwrap();
        let null_and_zero: (i64, String, String) = store
            .connection
            .query_row(
                "SELECT entry_price IS NULL, unrealized_pnl, typeof(signed_size) FROM source_position WHERE wallet_address=?1 AND coin='LONG'",
                [&wallet],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(null_and_zero, (1, "0".into(), "text".into()));
        drop(store);

        let reopened = SourceStateStore::open(&path, "test", [wallet.clone()]).unwrap();
        assert_eq!(reopened.load_baselines().unwrap(), vec![expected]);
        assert_eq!(reopened.process_generation(), 2);
        assert_eq!(
            reopened.continuity_status().unwrap(),
            SourceContinuityStatus {
                durable_baselines: 1,
                live_state_confirmed: 0,
                live_state_recovering: 1,
                history_contiguous: 0,
                history_catching_up: 1,
                history_gapped: 0,
                scan_commits: 0,
                last_scan_commit_ms: 0,
            }
        );
        assert!(matches!(
            reopened
                .history_request(&wallet, 1_725_123_556_789)
                .unwrap(),
            Some(RequestSubject::SourceHistory {
                start_ms: 1_725_123_456_789,
                ..
            })
        ));
    }

    #[test]
    fn source_fill_sql_round_trip_is_exact_ordered_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let wallet = wallet('4');
        let mut store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "test",
            [wallet.clone()],
        )
        .unwrap();
        let records = serde_json::json!([
            {
                "coin":"TINY", "px":"0.00000001", "sz":"0.00000001",
                "side":"B", "time":101, "tid":2, "oid":12,
                "hash":"0x2", "startPosition":"-0.00000001",
                "closedPnl":"-0.000000000000000001", "fee":"0.000000000000000001"
            },
            {
                "coin":"TINY", "px":"999999999.123456789", "sz":"1.00000001",
                "side":"A", "time":100, "tid":1, "hash":"0x1",
                "closedPnl":"0", "fee":"-0.00000001"
            }
        ]);
        let page = page(&wallet, 100, 200, records);
        store.persist_history(&page, 201).unwrap();
        store.persist_history(&page, 999).unwrap();
        let mut statement = store.connection.prepare(
            "SELECT exchange_ms,tid,side,px,sz,oid,start_position,closed_pnl,fee,observed_at_ms FROM source_fill_event WHERE wallet_address=?1 ORDER BY exchange_ms,event_key"
        ).unwrap();
        let rows = statement
            .query_map([&wallet], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (&rows[0].0, rows[0].1.as_str(), rows[0].2.as_str()),
            (&100, "1", "sell")
        );
        assert_eq!(
            (&rows[1].0, rows[1].1.as_str(), rows[1].2.as_str()),
            (&101, "2", "buy")
        );
        assert_eq!(
            parse_decimal(&rows[0].3).unwrap(),
            Decimal::from_str("999999999.123456789").unwrap()
        );
        assert_eq!(
            parse_decimal(&rows[0].4).unwrap(),
            Decimal::from_str("1.00000001").unwrap()
        );
        assert_eq!(rows[0].5, None);
        assert_eq!(rows[0].6, None);
        assert_eq!(
            parse_decimal(rows[0].7.as_deref().unwrap()).unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            parse_decimal(rows[0].8.as_deref().unwrap()).unwrap(),
            Decimal::from_str("-0.00000001").unwrap()
        );
        assert_eq!(rows[0].9, 201);
        assert_eq!(rows[1].5.as_deref(), Some("12"));
        assert_eq!(
            parse_decimal(rows[1].6.as_deref().unwrap()).unwrap(),
            Decimal::from_str("-0.00000001").unwrap()
        );
        assert_eq!(
            parse_decimal(rows[1].7.as_deref().unwrap()).unwrap(),
            Decimal::from_str("-0.000000000000000001").unwrap()
        );
        assert_eq!(
            parse_decimal(rows[1].8.as_deref().unwrap()).unwrap(),
            Decimal::from_str("0.000000000000000001").unwrap()
        );
        assert_eq!(rows[1].9, 201);
    }

    #[test]
    fn cohort_commit_is_atomic_and_preserves_intervening_trades() {
        let root = tempfile::tempdir().unwrap();
        let wallets: Vec<_> = (0..375).map(|i| format!("0x{i:040x}")).collect();
        let mut store =
            SourceStateStore::open(root.path().join("source.sqlite"), "test", wallets.clone())
                .unwrap();
        for wallet in &wallets[..374] {
            store
                .stage_scan(&state(wallet.clone(), 100), true, false)
                .unwrap();
        }
        assert_eq!(store.scan_commits, 0);
        assert!(store.load_baselines().unwrap().is_empty());
        let mut trade = state(wallets[0].clone(), 200);
        trade.positions.get_mut("BTC").unwrap().signed_size = Decimal::ONE;
        store.persist(&trade, true, false).unwrap();
        // A failure at the last wallet must roll back the other 374 scan writes.
        store.connection.execute_batch(&format!(
            "CREATE TRIGGER reject_last BEFORE UPDATE ON source_wallet WHEN NEW.wallet_address='{}' BEGIN SELECT RAISE(ABORT,'injected scan failure'); END;",wallets[374]
        )).unwrap();
        let last = state(wallets[374].clone(), 100);
        assert!(store
            .stage_scan(&last, true, false)
            .unwrap_err()
            .contains("injected scan failure"));
        assert_eq!(store.load_baselines().unwrap(), vec![trade.clone()]);
        assert_eq!(store.scan_commits, 0);
        store
            .connection
            .execute_batch("DROP TRIGGER reject_last")
            .unwrap();
        store.stage_scan(&last, true, false).unwrap();
        let restored = store.load_baselines().unwrap();
        assert_eq!(restored.len(), 375);
        assert_eq!(restored[0], trade);
        assert_eq!(store.scan_commits, 1);
        assert_eq!(store.continuity_status().unwrap().live_state_confirmed, 375);
        let rows: (i64, i64, i64) = store
            .connection
            .query_row(
                "SELECT COUNT(*),MIN(updated_at_ms),MAX(updated_at_ms) FROM source_position",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(rows, (375, 100, 200));
        store
            .stage_scan(&state(wallets[0].clone(), 300), true, false)
            .unwrap();
        store.mark_stream_gap().unwrap();
        assert!(store.scan_pending.is_empty());
    }

    #[test]
    fn cohort_commit_waits_for_competing_writer_without_blocking_wal_reader() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.sqlite");
        let wallets: Vec<_> = (0..375).map(|i| format!("0x{i:040x}")).collect();
        let mut store = SourceStateStore::open(&path, "test", wallets.clone()).unwrap();
        let timeout: i64 = store
            .connection
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        let mode: String = store
            .connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!((timeout, mode.as_str()), (5000, "wal"));
        for wallet in &wallets[..374] {
            store
                .stage_scan(&state(wallet.clone(), 100), true, false)
                .unwrap();
        }
        let contender = Connection::open(&path).unwrap();
        contender
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE source_meta SET process_generation=process_generation;",
            )
            .unwrap();
        // This is a genuinely separate connection holding SQLite's write lock.
        let reader = Connection::open(&path).unwrap();
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM source_wallet", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 375);
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(150));
            contender.execute_batch("COMMIT").unwrap();
        });
        let started = std::time::Instant::now();
        store
            .stage_scan(&state(wallets[374].clone(), 100), true, false)
            .unwrap();
        release.join().unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(100));
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(store.scan_commits, 1);
        assert_eq!(store.load_baselines().unwrap().len(), 375);
    }

    #[test]
    fn unchanged_position_rows_are_not_rewritten_and_closures_remove_rows() {
        for scale in 0..=28 {
            for negative in [false, true] {
                let mut value = Decimal::new(0, scale);
                value.set_sign_negative(negative);
                let wire = value.to_string();
                assert_eq!(parse_decimal(&wire).unwrap().to_string(), wire);
            }
        }
        let root = tempfile::tempdir().unwrap();
        let wallet = wallet('a');
        let mut store =
            SourceStateStore::open(root.path().join("source.sqlite"), "test", [wallet.clone()])
                .unwrap();
        let mut snapshot = state(wallet, 100);
        snapshot.block_number = Some(10);
        snapshot.positions.get_mut("BTC").unwrap().unrealized_pnl = Some(-Decimal::new(0, 6));
        store.persist(&snapshot, true, false).unwrap();
        assert!(!snapshot.positions.is_empty());
        snapshot.source_time_ms = 200;
        snapshot.block_number = Some(11);
        store.persist(&snapshot, true, false).unwrap();
        for (time, block) in [(199, 12), (201, 9)] {
            let mut stale = snapshot.clone();
            stale.source_time_ms = time;
            stale.block_number = Some(block);
            store.persist(&stale, true, false).unwrap();
        }
        let mut missing_block = snapshot.clone();
        missing_block.source_time_ms = 201;
        missing_block.block_number = None;
        store.persist(&missing_block, true, false).unwrap();
        assert_eq!(store.load_baselines().unwrap()[0], snapshot);
        let updated: i64 = store
            .connection
            .query_row("SELECT MAX(updated_at_ms) FROM source_position", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(updated, 100);
        assert_eq!(store.load_baselines().unwrap()[0].source_time_ms, 200);
        snapshot.positions.clear();
        store.persist(&snapshot, true, false).unwrap();
        let count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM source_position", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn source_sql_transaction_rolls_back_and_schema_cannot_become_accounting_authority() {
        let root = tempfile::tempdir().unwrap();
        let wallet = wallet('5');
        let mut store = SourceStateStore::open(
            root.path().join("source-state.sqlite"),
            "test",
            [wallet.clone()],
        )
        .unwrap();
        store
            .persist(&state(wallet.clone(), 100), true, false)
            .unwrap();
        store
            .persist_history(
                &page(&wallet, 100, 300, serde_json::json!([fill(250, 1, "B")])),
                301,
            )
            .unwrap();
        let before: (i64, i64, Option<String>) = store.connection.query_row(
            "SELECT (SELECT count(*) FROM source_fill_event), contiguous_through_ms, last_fill_event_key FROM source_history_cursor WHERE wallet_address=?1",
            [&wallet],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        let mut conflicting = fill(250, 1, "B");
        conflicting["px"] = serde_json::json!("101");
        assert!(store
            .persist_history(
                &page(
                    &wallet,
                    100,
                    300,
                    serde_json::json!([fill(160, 2, "B"), conflicting])
                ),
                302,
            )
            .is_err());
        let after: (i64, i64, Option<String>) = store.connection.query_row(
            "SELECT (SELECT count(*) FROM source_fill_event), contiguous_through_ms, last_fill_event_key FROM source_history_cursor WHERE wallet_address=?1",
            [&wallet],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(after, before);

        let tables = store.connection.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            tables,
            vec![
                "source_fill_event",
                "source_history_cursor",
                "source_meta",
                "source_position",
                "source_recovery",
                "source_wallet",
            ]
        );
        assert!(tables.iter().all(|name| ![
            "execution",
            "ledger",
            "accounting",
            "pending_action",
            "mfce",
            "label",
            "learning"
        ]
        .iter()
        .any(|forbidden| name.contains(forbidden))));
    }

    fn wallet(byte: char) -> String {
        format!("0x{}", byte.to_string().repeat(40))
    }

    fn state(wallet: String, source_time_ms: u64) -> SourceStateResponse {
        SourceStateResponse {
            block_number: None,
            candidate_id: wallet,
            account_value: Decimal::from(100),
            source_time_ms,
            positions: BTreeMap::from([(
                "BTC".into(),
                SourceAssetPosition {
                    asset: "BTC".into(),
                    signed_size: Decimal::new(25, 2),
                    signed_notional: Decimal::from(25_000),
                    entry_price: Some(Decimal::from(100_000)),
                    unrealized_pnl: Some(Decimal::from(5)),
                },
            )]),
            closed_candles: Vec::new(),
        }
    }

    fn test_root(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "copytrade-source-state-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn restores_durable_baseline_but_resets_generation_confirmation() {
        let root = test_root("restore");
        let path = root.join("source-state.sqlite");
        let first = wallet('1');
        let second = wallet('2');
        let mut store =
            SourceStateStore::open(&path, "375", [first.clone(), second.clone()]).unwrap();
        assert_eq!(store.process_generation(), 1);
        assert_eq!(
            store
                .connection
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        store
            .persist(&state(first.clone(), 1234), true, false)
            .unwrap();
        // Reopening a v1 database must preserve its old canonical state hashes.
        store.connection.execute_batch("ALTER TABLE source_wallet DROP COLUMN block_number; UPDATE source_meta SET schema_version=1").unwrap();
        drop(store);

        let store = SourceStateStore::open(&path, "375", [first.clone(), second]).unwrap();
        assert_eq!(store.process_generation(), 2);
        assert_eq!(
            store.load_baselines().unwrap(),
            vec![state(first.clone(), 1234)]
        );
        let live_confirmed = store
            .connection
            .query_row(
                "SELECT live_confirmed FROM source_recovery WHERE durable_baseline = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(live_confirmed, 0);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_corrupt_durable_baseline() {
        let root = test_root("checksum");
        let path = root.join("source-state.sqlite");
        let first = wallet('1');
        let mut store = SourceStateStore::open(&path, "375", [first.clone()]).unwrap();
        store.persist(&state(first, 100), true, false).unwrap();
        store
            .connection
            .execute("UPDATE source_wallet SET state_hash = X'00'", [])
            .unwrap();
        assert!(store
            .load_baselines()
            .unwrap_err()
            .contains("checksum mismatch"));
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
