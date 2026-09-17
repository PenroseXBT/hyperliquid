# Freeze guide (maintainer) — `railway-frozen/`

How to cut a new frozen release artifact set. For first boot see
[DEPLOY.md](./DEPLOY.md). For ops, monitoring, and recovery,
[rail_up.md](../rail_up.md) is canonical.

Source of truth: `scripts/build_production_release.sh:10-18`,
`Dockerfile.railway:13-14,23-29`, `.gitignore:34-37`,
[rail_up.md](../rail_up.md) §4A.

## Frozen set (6 files)

| Source in repo | Frozen path | Path in container (`Dockerfile.railway:13-19`) |
|---|---|---|
| `target/release/engine` (from `cargo build --locked --release -p engine`) | `railway-frozen/engine` | `/app/bin/engine` |
| `target/production-release/engine.sha256` (rewritten to container path, see below) | `railway-frozen/engine.sha256` | `/app/bin/engine.sha256` |
| `config/copytrade.json` | `railway-frozen/copytrade.json` | `/app/config/copytrade.json` |
| Reviewed layer file, copied by hand (no generator) | `railway-frozen/very-profitable-layer.json` | `/app/config/very-profitable-layer.json` |
| `config/read-api-policy.json` | `railway-frozen/read-api-policy.json` | `/app/policy/read-api-policy.json` |
| `config/public-mainnet-transport.json` | `railway-frozen/public-mainnet-transport.json` | `/app/policy/public-mainnet-transport.json` |

The wrapper `scripts/run_su6_railway.sh` is copied live from the repo at
image build (`Dockerfile.railway:15`); it is not part of the frozen set.
Only the six files above are trusted inside the container.

## Prereqs

- Clean `git status` (frozen output must match one commit).
- `cargo`, `shasum`/`sha256sum` available.
- The reviewed `very-profitable-layer.json` file at hand (see manual-copy rule).

## Procedure

```sh
bash scripts/build_production_release.sh
# runs: cargo fmt --check, cargo check, cargo test, boundary check,
#       git diff --check, cargo build --locked --release -p engine
# writes: target/production-release/{test-report.txt,isolation-report.txt,engine.sha256}

cp target/release/engine railway-frozen/engine
cp target/production-release/engine.sha256 railway-frozen/engine.sha256
cp config/copytrade.json railway-frozen/copytrade.json
cp <reviewed-layer-path> railway-frozen/very-profitable-layer.json  # manual copy, see below
cp config/read-api-policy.json railway-frozen/read-api-policy.json
cp config/public-mainnet-transport.json railway-frozen/public-mainnet-transport.json
```

The frozen sha file must record the **container path** `/app/bin/engine`,
because `Dockerfile.railway:23` runs `sha256sum --check
/app/bin/engine.sha256` inside the image. After the `cp`, rewrite the path
field so the committed file keeps the form:

```sh
# railway-frozen/engine.sha256 contains one line:
# <64 hex>  /app/bin/engine
```

Local smoke test (the `-c` form only passes inside the image where
`/app/bin/engine` exists; locally compare hashes explicitly):

```sh
shasum -a 256 railway-frozen/engine | cut -d ' ' -f 1
cut -d ' ' -f 1 railway-frozen/engine.sha256
# the two hashes must match
./target/release/engine --config railway-frozen/copytrade.json \
  --very-profitable-layer railway-frozen/very-profitable-layer.json \
  --print-persistence-schema-hash
# same check Dockerfile.railway:25-29 runs at build time; a failure there never ships
```

## Manual-copy rule for `very-profitable-layer.json`

There is no generator to document. The reviewed file is the source of
truth; copy it by hand into `railway-frozen/`. Never generate in place.

When freezing, record provenance alongside the commit (commit message or
release note): where the reviewed file came from, plus
`authoritative_history_sha256` and `hydrated_at_ms` (cf.
`railway-frozen/very-profitable-layer.json:3-6`).

Layer + binary + all four JSONs commit together. Never commit a layer
without the binary and configs from the same release commit.

## `policy/` directory — reference only

Versioned reference defaults, explicitly **not** copied into the image:

- `policy/production-dynamic-floor-v1.json`
- `policy/production-ioc-pricing-v1.json`
- `policy/production-ipc-v1.json`
- `policy/production-market-rules-v1.json`
- `policy/hl1c-forbidden-artifact-patterns.txt`

They have zero references in `crates/`, `scripts/run_su6_railway.sh`,
`Dockerfile.railway`, and `railway.toml` (`Dockerfile.railway:18-19` only
COPYs the two frozen policies). Do not copy them into `railway-frozen/`.
Promoting one to frozen status requires a `Dockerfile.railway` + wrapper +
guide update. No Dockerfile/code change in this task.

## Commit + fail table

Commit the six frozen files together. The container only trusts what is in
`railway-frozen/`.

| Failure | Cause | Fix |
|---|---|---|
| Image build fails on `sha256sum --check` | `railway-frozen/engine` and `.sha256` out of sync, or sha line does not reference `/app/bin/engine` | Re-freeze binary + sha together, keep container path, commit both |
| Image build fails on `--print-persistence-schema-hash` | Frozen JSONs incompatible with binary | All four JSONs must be from the same commit as the binary; re-freeze |

Next: first boot in [DEPLOY.md](./DEPLOY.md); daily ops in
[rail_up.md](../rail_up.md).
