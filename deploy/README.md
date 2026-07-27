# Production host deployment

Create two system users, `hype-observer` and `hype-signer`, and the shared
`hype-arb` group. The signer socket is `/run/hype-arb/signer.sock`, owned by
`hype-signer:hype-arb` with mode `0660`.

The observer receives only public configuration and has no access to signer
state or secrets. The signer owns `/var/lib/hype-arb/signer`, authenticated
exchange access, and the API-wallet key.

`/etc/hype-arb/signer-secret.env` may contain only paths and public identity:

```text
HYPERLIQUID_API_WALLET_PRIVATE_KEY_FILE=/etc/hype-arb/api-wallet.key
HYPERLIQUID_MASTER_ACCOUNT=0x...
```

The key file must be a regular, non-symlink file with mode `0600`. Start the
signer first and require a ready/reconciled status before enabling observer
handoff. Use normal systemd CPU and NUMA affinity defaults.
