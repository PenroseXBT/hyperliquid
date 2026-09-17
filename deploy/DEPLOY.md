# Deploy guide (first-time setup only)

Creates the Railway service once. For freezing a release see
[FREEZE.md](./FREEZE.md). For ops, monitoring, and recovery,
[rail_up.md](../rail_up.md) is canonical — nothing here duplicates it.

Source of truth: `railway.toml:1-17`,
`scripts/run_su6_railway.sh:18-28`, [rail_up.md](../rail_up.md) §§2-4B.

## Prereqs

- Railway account with a project to host the service.
- Hyperliquid DirectUser account, funded with the equity you intend to risk,
  plus its API-wallet private key (hex, `0x…`, 32 bytes).
- `railway` CLI (optional; dashboard works for everything below).

## Create the service

Dashboard path:

1. New Project → Deploy from GitHub repo → select this repo/branch.
2. Service Settings → Build → Builder: `DOCKERFILE`, Path: `Dockerfile.railway`.
3. Settings → Deploy → Start Command: `/app/bin/run_su6_railway.sh`,
   Region: `asia-southeast1-eqsg3a` (or keep `railway.toml` as source of truth).
   Keep 1 replica — never scale to >1.
4. Add a Volume (e.g. 5–10 GB) mounted at `/data`. This provides the
   permanent `/data/system-v1` root.
5. Variables → add the three variables in the table below.
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

## Variables (only these three)

| Variable | Required | Example | Notes |
|---|---|---|---|
| `SU6_DATA_ROOT` | you set | `/data/system-v1` | Single permanent root. Requires the volume mounted at `/data`. |
| `HYPERLIQUID_EXECUTION_ACCOUNT` | you set | `0xabc…` | Must equal the API-wallet signer address. |
| `HYPERLIQUID_API_WALLET_SECRET` | you set, secret | `0x…64 hex…` | Raw API-wallet key. Never commit. Mark secret in Railway. |

## Confirm first boot

```sh
railway up
railway run cat /data/system-v1/failure/current-run.meta
# run_id=… previous_run_id=… started_at=… wrapper_pid=… engine_pid=… binary_sha256=…
```

Healthy-start log lines (see [rail_up.md](../rail_up.md) §4C for the full
sequence):

```text
engine_binary_sha256=<64 hex>
continuous_engine_start=true run_id=… previous_run_id=none data_root=/data/system-v1 execution=authenticated_live
```

## Non-goals

- Config changes → [FREEZE.md](./FREEZE.md) (never edit `/app/config` or
  `/app/policy` live).
- Stop / redeploy / monitor / recovery → [rail_up.md](../rail_up.md).
