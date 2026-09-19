# Deployment security

The clearinghouse controls signing authorization and maintains financial
accounting state. Treat the service, its database, and its credentials as
security-sensitive.

## Network exposure

- Bind the authorization webhook and embedded Kafka broker to loopback whenever
  they run on the same host as the signer.
- Do not expose either service directly to the public internet.
- If the webhook must cross a network, place it behind a TLS-terminating proxy
  or an authenticated tunnel. Prefer mutual TLS and restrict source addresses
  with a firewall.
- `--unsafe-http-bind` only permits a non-loopback bind. It does not enable TLS
  or otherwise secure the connection.
- Kafka connections are unencrypted and unauthenticated. Keep Kafka on loopback
  or a trusted private network. Use an authenticated encrypted tunnel when
  traffic must cross hosts.
- Keep SQLite on local storage. Do not place the database on a network
  filesystem.

## Credentials

- Generate `CLEARINGHOUSE_WEBHOOK_TOKEN` with a cryptographically secure random
  generator. Use at least 32 random bytes.
- Supply secrets through the deployment platform's secret-management mechanism.
  Do not place them in source control, command-line arguments, container images,
  or ordinary configuration files.
- Ensure proxies and request logs do not record the
  `Livepeer-Clearinghouse-Token` or `Authorization` headers.
- API-key creation displays the key only once. Store it immediately in a secret
  manager, avoid capturing it in shell history or CI logs, and revoke keys that
  may have been disclosed.
- Use a separate API key for each gateway or deployment so credentials can be
  rotated or revoked independently.

## Process and filesystem isolation

- Run the clearinghouse as a dedicated, unprivileged operating-system user.
- Restrict the state directory to that user, preferably with directory mode
  `0700` and a process umask of `0077`.
- Keep the accounting and embedded Kafka databases on reliable local storage.
  Treat database files, WAL files, configuration, logs, and backups as
  sensitive.
- Permit management commands only to trusted operators. These commands directly
  modify grants, allocations, API keys, sessions, and accounting state.
- Run at most one active accounting service and one active on-chain listener for
  each accounting database. When using containers or an orchestrator, use a
  single replica and a recreate-style deployment rather than an overlapping
  rolling update.

## Operational safeguards

- Run the service under a supervisor that restarts it after failure and delivers
  `SIGTERM` for graceful shutdown.
- Monitor accounting lag, quarantined events, negative allocation balances,
  database growth, available disk space, RPC failures, and process restarts.
- Accounting is asynchronous. Prolonged Kafka lag can allow an allocation to
  exceed its budget, so stop or isolate signing when accounting is unhealthy or
  materially behind.
- Protect `/livez` and `/readyz` from unnecessary public exposure. Use `/readyz`
  for traffic admission, but monitor accounting progress separately.
- Back up the accounting and embedded Kafka databases as one consistent recovery
  set. Include associated WAL files when using filesystem copies.
- Encrypt backups, store them separately from the deployment, and regularly test
  restoration.
- Before upgrading, stop the active service, take a verified backup, apply
  migrations, and then start the new version. Do not run `migrate down` against
  a production database.
- Keep the host, clearinghouse binary, reverse proxy, and supporting services
  patched. Deploy only reviewed build artifacts.

## On-chain listener

- Set `ChainID` in production to detect an incorrectly configured RPC endpoint.
- Use a reputable HTTPS RPC provider and protect any provider credentials.
- Use a separate accounting database for each chain, TicketBroker, and signer
  set.
- Choose confirmation and reorganization-lookback settings conservatively, and
  monitor the listener's distance from the confirmed chain head.
