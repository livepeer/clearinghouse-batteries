# Deployment security

## Network

- Put the webhook, management API, and embedded Kafka behind terminating TLS
  proxies. Use trusted certificates and mutual TLS where supported.
- Bind plaintext listeners to loopback, or use a trusted private network between
  proxy and clearinghouse. Restrict source addresses with a firewall; do not
  expose plaintext listeners to the internet.
- Kafka needs a TLS TCP proxy; accounting can connect locally. SASL and HTTP
  credentials are still required. `--unsafe-http-bind` only permits a bind;
  it adds no TLS or access control.
- Health routes require no credentials. Restrict their exposure; `/readyz`
  checks the database and shutdown state, not accounting progress.

## Credentials

- Generate a distinct secret per service credential from at least 32 random
  bytes, encoded as hex or base64. Mount the credentials file through a secret
  manager, set `CredsFile` to its path, and allow only the service user to read it.
- Keep secrets out of source control, command-line arguments, container images,
  and ordinary configuration files. Do not log `Livepeer-Clearinghouse-Token`,
  `Authorization`, or API-key creation responses.
- API-key secrets are shown once. Store them immediately, use a separate key
  per gateway or deployment, and revoke disclosed keys.
- Management permissions cover all resources of a type. Wildcards include future
  actions; review them before upgrades. After changing credentials or permissions,
  update affected clients or broker users and restart clearinghouse.

## Host and database

- Use a dedicated unprivileged user, state-directory mode `0700`, and umask `0077`.
  Protect database/WAL files, configuration, logs, and backups.
- Keep SQLite on reliable local storage. Limit management API and CLI access to
  trusted operators; CLI commands modify the database directly.
- Run one accounting service and one on-chain listener per database. Use a single
  replica and recreate deployments to avoid overlapping workers.

## Operations

- Use a supervisor that restarts failures and sends `SIGTERM` for shutdown.
  Keep the host, binary, proxy, and dependencies patched; deploy reviewed builds.
- Monitor accounting lag, quarantined events, negative balances, database growth,
  free disk space, RPC failures, and restarts. Delayed accounting can exceed
  budgets; stop or isolate signing when accounting falls behind or fails.
- Back up accounting and Kafka databases as one consistent set, including WAL
  files for filesystem copies. Encrypt backups, store them separately, and test
  restoration.
- Stop the service and take a verified backup before upgrades; apply migrations
  before restarting. Do not use `migrate down` in production.

## On-chain listener

- Set `ChainID` to detect the wrong RPC chain. Use a trusted HTTPS RPC provider
  and protect its credentials.
- Use a separate database for each chain, TicketBroker, and signer set.
- Choose confirmations and reorganization lookback conservatively; monitor
  progress against the confirmed chain head.
