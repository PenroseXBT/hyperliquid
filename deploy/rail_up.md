# Railway Deployment Guide — SU6 Continuous Engine (rail_up)

Live, authenticated deployment of the frozen `engine continuous` daemon on Railway.
Read this top-to-bottom before touching the service.

Production generation: `SYSTEM_V1`. Single permanent state root: `SU6_DATA_ROOT=/data/system-v1`.
There are no V12G/V13/schema-specific production roots. Serialization/schema versions
remain internal and future persistence bumps keep using `/data/system-v1`.

## 1. What gets deployed

| Piece | Source in repo | Path in container |
|---|---|---|
| Engine binary (frozen) | `railway-frozen/engine` + `engine.sha256` | `/app/bin/engine` |
| Wrapper / supervisor | `scripts/run_su6_railway.sh` | `/app/bin/run_su6_railway.sh` |
| Copytrade config (frozen) | `railway-frozen/copytrade.json` | `/app/config/copytrade.json` |
| Cohort layer (frozen) | `railway-frozen/very-profitable-layer.json` | `/app/config/very-profitable-layer.json` |
| Read/API policy (frozen) | `railway-frozen/read-api-policy.json` | `/app/policy/read-api-policy.json` |
| Transport policy (frozen) | `railway-frozen/public-mainnet-transport.json` | `/app/policy/public-mainnet-transport.json` |
| Build | `Dockerfile.railway` (`debian:bookworm-slim` + `bash`, `ca-certificates`, `coreutils`, `tini`) | — |
| Service definition | `railway.toml` | — |

Runtime command (do not change without a new frozen build):

```sh
python3 /app/bin/hl_bot.py supervise
# which supervises:
# /app/bin/run_su6_railway.sh
# which execs:
# /app/bin/engine continuous \
#   --config /app/config/copytrade.json \
#   --very-profitable-layer /app/config/very-profitable-layer.json \
#   --request-policy /app/policy/read-api-policy.json \
#   --transport-policy /app/policy/public-mainnet-transport.json \
#   --output "$DATA_ROOT/runtime" \
#   --state-root "$DATA_ROOT/state"
```

Build-time verification in `Dockerfile.railway` already does:

```sh
sha256sum --check /app/bin/engine.sha256
/app/bin/engine --config /app/config/copytrade.json \
  --very-profitable-layer /app/config/very-profitable-layer.json \
  --print-persistence-schema-hash >/dev/null
```

If either fails, the image never ships.

Key service semantics from `railway.toml`:

- `builder = "DOCKERFILE"`, `dockerfilePath = "Dockerfile.railway"`
- `region = "asia-southeast1-eqsg3a"`, `numReplicas = 1` — never scale to >1. Two writers corrupt the volume.
- `restartPolicyType = "ALWAYS"`, `restartPolicyMaxRetries = 100` — the supervisor keeps telemetry online while the engine restarts under a pause. A fatal engine exit logs `supervisor_restart_required=true` and the supervisor re-verifies before unpausing; boot verification (not a stopped container) is the capital-safety latch.
- `drainingSeconds = 90`, `overlapSeconds = 0` — Railway sends `SIGTERM`, `tini` forwards it, the wrapper forwards `SIGINT` to the engine for a graceful drain (up to 90 s). No zero-downtime overlap.

## 2. Prerequisites

1. Railway account + a project with one service pointed at this repo/branch.
2. A Railway **Volume** attached to that service.
3. A Hyperliquid **DirectUser** account (spot/perp, `user` role, `vaultAddress=null`):
   - Funded with the equity you intend to risk.
   - An API-wallet private key for that exact account (hex, `0x…`, 32 bytes).
   - The engine enforces `HYPERLIQUID_EXECUTION_ACCOUNT == derived address of HYPERLIQUID_API_WALLET_SECRET`. Any mismatch exits fatal.
4. `railway` CLI (optional but recommended) or use the Railway dashboard:
   ```sh
   npm i -g @railway/cli
   railway login
   railway link   # pick project + su6 service
   ```

## 3. Required variables

Set these on the Railway service (Dashboard → Service → Variables, or `railway variables --set`).

| Variable | Required | Example | Notes |
|---|---|---|---|
| `SU6_DATA_ROOT` | **you set** | `/data/system-v1` | Single permanent SYSTEM_V1 root. Must be an absolute path; default is `/data/system-v1`. Requires a Railway volume mounted at `/data`. |
| `HYPERLIQUID_EXECUTION_ACCOUNT` | **you set** | `0xabc…` | Checksummed or lowercase hex. Must equal the API-wallet signer address. Case-insensitive compare, persisted lowercase. |
| `HYPERLIQUID_API_WALLET_SECRET` | **you set, secret** | `0x…64 hex…` | Raw API-wallet private key. Never commit, never log, never put in `railway-frozen/`. Mark as secret/encrypted in Railway. |

No other env is read by the live path (`crates/engine/src/execution.rs`, `runtime.rs`). `HYPERLIQUID_MASTER_ACCOUNT` is obsolete and ignored — setting it instead of `HYPERLIQUID_EXECUTION_ACCOUNT` fails closed.

### Volume layout (created automatically)

```
/data/system-v1/                # $SU6_DATA_ROOT (SYSTEM_V1, single permanent root)
  state/                        # --state-root → UnsignedStateRoot + live/
    live/
      live-account-identity.json  # economic_account+signer latch (schema_version 1)
      live-submission-registry.json
      live-trading-state.json
  runtime/                      # --output (ephemeral diagnostics)
  failure/
    current-run.meta            # run_id, previous_run_id, started_at, pids, binary_sha256
    last-exit.meta              # persisted only on unexpected exit
    last-exit.stderr            # tail (≤1 MiB) of process.stderr
    process.stderr              # current run stderr (cap 1 MiB on persist)
  source-state.sqlite           # durable source baselines (sibling of state/)
```

Rules enforced by `run_su6_railway.sh`: `umask 077`, no symlinks anywhere on that path, only the six `failure/` filenames above may exist. Anything else → `invalid_runtime_directory` / `invalid_failure_artifact` and immediate exit.

V1 starts clean: no MFCE training rows, delayed DecisionSamples, incumbent q10/q50 model,
pending training attempt, old pending action ownership, or historical lifecycle/provenance
state are carried over. Prior volume contents are left untouched — never copied, migrated,
or deleted by V1 startup. A clean local root does not imply a clean exchange account:
startup always performs authenticated exchange reconciliation (positions/orders/fills/account
truth) before strategy execution authority, fails closed on authenticated exposure, and
preserves signer/reconciliation ownership invariants.

> **Identity latch:** the first successful start writes `live-account-identity.json`. Changing `HYPERLIQUID_*` afterward without wiping the volume fails with `InvalidExchangeState`. That is intentional — it prevents trading the wrong account's equity. To rotate accounts, stop the service, wipe `/data/system-v1`, then redeploy.

## 4. Deploy steps

### A. Freeze the artifact (maintainer step — skip if `railway-frozen/` is already the release you want)

```sh
bash scripts/build_production_release.sh
# runs: cargo fmt --check, cargo check, cargo test, boundary check,
#       git diff --check, cargo build --locked --release -p engine
# writes: target/production-release/{test-report.txt,isolation-report.txt,engine.sha256}

```sh
bash scripts/build_production_release.sh
# runs: fmt --check, check, test, boundary check, git diff --check,
#       musl release build → target/x86_64-unknown-linux-musl/release/engine
# writes: target/production-release/{test-report.txt,isolation-report.txt,engine.sha256}

cp target/x86_64-unknown-linux-musl/release/engine railway-frozen/engine
cp target/production-release/engine.sha256 railway-frozen/engine.sha256
cp config/copytrade.json railway-frozen/copytrade.json
# + very-profitable-layer.json, read-api-policy.json, public-mainnet-transport.json
# The .sha256 line references /app/bin/engine (container path), so compare
# hashes explicitly locally; `sha256sum -c` only passes inside the image:
shasum -a 256 railway-frozen/engine | cut -d ' ' -f 1
cut -d ' ' -f 1 railway-frozen/engine.sha256
# the two hashes must match
./target/x86_64-unknown-linux-musl/release/engine --config railway-frozen/copytrade.json \
  --very-profitable-layer railway-frozen/very-profitable-layer.json \
  --print-persistence-schema-hash
```

Commit the result. The container only trusts what is in `railway-frozen/`.

### B. Create / link the Railway service

Dashboard path:

1. New Project → Deploy from GitHub repo → select this repo/branch.
2. Service Settings → Build → Builder: `Dockerfile`, Path: `Dockerfile.railway`.
3. Settings → Deploy → Start Command: `python3 /app/bin/hl_bot.py supervise`, Region: `asia-southeast1-eqsg3a` (or keep `railway.toml` as source of truth).
4. Add a Volume (e.g. 5–10 GB) mounted at `/data`. This provides the permanent `/data/system-v1` root.
5. Variables → add `SU6_DATA_ROOT=/data/system-v1`, `HYPERLIQUID_EXECUTION_ACCOUNT`, `HYPERLIQUID_API_WALLET_SECRET` (secret).
6. Deploy.

CLI path:

```sh
railway link
railway volume add          # follow prompts, mount at /data
railway variables --set "SU6_DATA_ROOT=/data/system-v1"
railway variables --set "HYPERLIQUID_EXECUTION_ACCOUNT=0xYourAccount"
railway variables --set "HYPERLIQUID_API_WALLET_SECRET=0xYourApiWalletSecret"
railway up                  # builds Dockerfile.railway, runs startCommand
```

### C. Verify a healthy start

Watch Deploy Logs. Healthy sequence:

```
engine_binary_sha256=<64 hex>
continuous_engine_start=true run_id=… previous_run_id=none data_root=/data/system-v1 execution=authenticated_live
execution_identity_verified=true topology=DirectUser account=0x… signer=0x… vaultAddress=null
source_cohort_loaded=<N> restored_source_baselines=<M> source_sqlite_available=true live_confirmations_reset=true
```

Then periodic status/equity lines with `execution_state=NORMAL` once reconciliation converges. `execution_state=RISK_ONLY reason=…` at startup or after a gap is normal — it means reduce-only until exchange truth converges; do not restart for it.

Check the wrapper also persisted `failure/current-run.meta`:

```sh
railway run cat /data/system-v1/failure/current-run.meta
# run_id=… previous_run_id=… started_at=… wrapper_pid=… engine_pid=… binary_sha256=…
```

### D. Normal operations

- **Graceful stop:** Stop / Redeploy from dashboard (sends `SIGTERM`). Expect `continuous_engine_stopped_by_operator=true exit=…` and exit 0. Wait for drain (≤90 s) before wiping the volume.
- **Config change:** never edit `/app/config` or `/app/policy` live. Edit repo source → re-freeze (`§4A`) → commit → redeploy. The new image re-verifies sha + schema hash at build.
- **One replica only.** Do not add replicas, doublers, or a second service sharing the same `SU6_DATA_ROOT` (`/data/system-v1`).

## 5. When it stops (expected behavior)

`restartPolicyType = ALWAYS` with Railway backoff means an unexpected engine exit is supervised, not silent. Logs will show one of:

```
continuous_engine_unexpected_exit=true exit=<code> supervisor_restart_required=true
continuous_engine_exited_without_operator_request=true   # exit 0 without SIGINT/SIGTERM → normalized to 1
predecessor_diagnostic_persistence_failed=true
```

The supervisor pauses the engine, keeps telemetry online, and requires boot verification before unpausing — that gate (not a stopped container) is the capital-safety latch.

Recovery procedure:

1. Do **not** hit Restart yet. Dump the persisted diagnostics first:
   ```sh
   bash scripts/live_watch.sh fail
   # === CURRENT RUN ===  failure/current-run.meta
   # === LAST EXIT ===    failure/last-exit.meta (only on unexpected exit)
   # === LAST STDERR ===  tail of failure/last-exit.stderr (≤20 KiB)
   ```
   (`railway logs --deployment <failed-deployment-id>` also reprints `predecessor_exit_*` on the next start.)
2. Match against the troubleshooting table below. Fix the root cause (usually secret rotation, volume full, Hyperliquid outage, or a frozen-config rejection).
3. Only then: Dashboard → Redeploy / Restart. The next start logs `previous_run_id=…` + `predecessor_exit_metadata_begin` + `predecessor_exit_stderr_begin` so you can confirm what it recovered from.
4. If the failure was an account change or corrupted `state/`, stop the service, wipe `/data/system-v1`, re-verify variables, and deploy fresh. The identity latch will re-initialize.

## 6. Troubleshooting

| Symptom / log | Cause | Fix |
|---|---|---|
| `invalid SU6_DATA_ROOT` | Bad root | Use the single permanent root `/data/system-v1`. |
| `invalid_runtime_directory=` / `invalid_failure_artifact=` | Symlink or stray file in volume | `railway run ls -laR /data/system-v1`; remove symlinks/strays; keep only `state/ runtime/ failure/` + the six `failure/` files. |
| `production requires HYPERLIQUID_EXECUTION_ACCOUNT` / `…_API_WALLET_SECRET` | Missing/empty variable | Set both variables (secret for the key), redeploy. |
| `DirectUser signer must equal HYPERLIQUID_EXECUTION_ACCOUNT` | Secret does not derive to that account | Paste the API-wallet key for exactly that account; check for whitespace/newline. |
| `DirectUser execution account must have Hyperliquid role user` / `IdentityMismatch` | Account is vault/proxy or wrong network role | Use a plain DirectUser account; `vaultAddress` must be null. |
| `InvalidExchangeState` (after `verify_live_identity`) | Variables changed but volume still latched to old account | Stop, wipe `/data/system-v1`, redeploy — or restore the original variables. |
| `execution_state=RISK_ONLY reason=…` persists | Exchange reconciliation uncertain (gap, rate-limit, deploy during outage) | Wait; it self-recovers to `NORMAL`. Restarting resets reconciliation — avoid it. |
| `source_database_open_failed=true nonfatal=true` / `source_*_failed=true nonfatal=true` | SQLite/baseline issue | Nonfatal; engine continues on REST+stream. Investigate disk space if repeated. |
| Build fails on `sha256sum --check` | `railway-frozen/engine` and `.sha256` out of sync | Re-run `§4A`, commit both files together. |
| Build fails on `--print-persistence-schema-hash` | Frozen config incompatible with binary | Re-freeze all four JSONs from the same commit as the binary. |
| Deploy loops / OOM | Volume full or undersized instance | Grow volume, ensure 1 replica. Prior roots are left untouched; V1 never migrates or deletes them. |

## 7. Security checklist

- `HYPERLIQUID_API_WALLET_SECRET` is a Railway **secret variable** only. It must never appear in git, `railway-frozen/`, logs, or screenshots. The engine logs only `engine_binary_sha256` and addresses, never the key.
- Prefer a dedicated API wallet (not the master key) with only the needed permissions, funded with limited equity (`starting_equity_usd`, `global_risk_scale`, `max_account_drawdown_pct` in `copytrade.json` are your on-chain guardrails — review before deploy).
- `railway-frozen/copytrade.json` currently enables `unlimited_mode`, 8.75× max leverage curve, 98% win-rate / 50-trade vetting gates, Sharpe gates. Treat any edit as a risk change: re-freeze, re-review, redeploy.
- Limit Railway project access; rotate the API wallet after any suspected leak (remember: rotation requires wiping `/data/system-v1` because of the identity latch).

## 8. Live monitoring — two commands, no extra stack

Operational loop: `deploy → hlwatch → hlfail (only when something looks wrong) → diagnose before restart`.
No Prometheus, Grafana, dashboard project, or extra daemon.

```sh
# 1. Everyday live view (filtered; Railway retains the full logs)
bash scripts/live_watch.sh watch
# equivalent unfiltered:
bash scripts/live_watch.sh logs
# or, after `source scripts/live_watch.sh`: hlwatch / hllogs

# 2. One failure dump when the service stops or looks suspicious
bash scripts/live_watch.sh fail
# or, after sourcing: hlfail
```

`watch` tails `railway logs --deployment latest` filtered to what matters:
`execution_state=`, `unexpected_exit`, `RISK_ONLY`, `ERROR`/`WARN`/`failed=true`/`fatal`/`panic`,
`IdentityMismatch`/`InvalidExchangeState`, `source_*failed`, `submission`/`fill`/`order`/`position`, `MFCE`, `HIP-3`.
Watch for `execution_state=NORMAL` vs stuck `RISK_ONLY`, plus actual orders/fills/MFCE/HIP-3/warnings.
`fail` prints exactly what `run_su6_railway.sh` persists: `current-run.meta`, `last-exit.meta`, `last-exit.stderr`.

## 9. Quick reference

```sh
# link + inspect
railway link && railway status
railway variables                                   # verify SU6_DATA_ROOT + 2 HYPERLIQUID vars present
railway volume list                                 # verify volume mounted at /data

# deploy
railway up
bash scripts/live_watch.sh watch

# failure inspection
bash scripts/live_watch.sh fail

# unfiltered logs when needed
bash scripts/live_watch.sh logs

# local preflight (before pushing)
bash scripts/build_production_release.sh
shasum -a 256 railway-frozen/engine | cut -d ' ' -f 1
cut -d ' ' -f 1 railway-frozen/engine.sha256
# the two hashes must match (`sha256sum -c` only passes inside the image,
# where the /app/bin/engine path exists)
```

Files of record: `railway.toml`, `Dockerfile.railway`, `scripts/run_su6_railway.sh`, `railway-frozen/*`, `crates/engine/src/execution.rs` (`HYPERLIQUID_*`), `crates/engine/src/main.rs` + `runtime.rs` (`continuous --state-root`).
