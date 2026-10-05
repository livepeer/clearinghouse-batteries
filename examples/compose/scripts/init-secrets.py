#!/usr/bin/env python3
"""Create service secrets once. Supply the wallet and RPC URL separately."""
import os
from pathlib import Path
import secrets

os.umask(0o077)
root = Path(__file__).resolve().parents[1]
target = root / "secrets"
target.mkdir(mode=0o700, exist_ok=True)
if any(target.iterdir()):
    raise SystemExit("secrets/ must be empty; refusing to replace credentials")
operator, webhook, reader, producer = (secrets.token_hex(32) for _ in range(4))
template = (root.parents[1] / "creds.example.toml").read_text()
for value in (operator, webhook, reader, producer):
    template = template.replace('secret = ""', f'secret = "{value}"', 1)
for name, value in {
    "clearinghouse.toml": template,
    "operator-token": operator,
    "webhook-headers": f"Livepeer-Clearinghouse-Token:{webhook}",
    "kafka-password": producer,
    "kafka-username": "producer",
    "webhook-url": "http://127.0.0.1:8080/v1/signer/authorize",
}.items():
    (target / name).write_text(value)
print("Created secrets/. Add rpc-url, keystore.json, and keystore-password (mode 0600).")
