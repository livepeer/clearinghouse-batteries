# Livepeer grants clearinghouse

The clearinghouse manages budgets for Livepeer remote signing. It provides:

- grant and allocation management;
- API-key authorization for gateways;
- accounting for issued tickets;
- optional on-chain payment reporting.

Management commands operate on a local SQLite database. There is no management
HTTP API yet.

## How accounting works

A grant provides a budget that can be divided into allocations. Gateways use API
keys associated with those allocations to request signing authorization.

The accounting service uses Kafka as a durable event log of issued tickets. It
validates signer events against authorized sessions and records their charges in
the internal ledger. The authorization webhook uses the available allocation
balance to decide whether signing may continue.

Accounting is asynchronous: authorization does not reserve funds, so delayed
events can take an allocation negative before further signing is stopped. Monitor
accounting lag, quarantined usage, and negative balances.

The optional on-chain listener records payments and escrow activity and matches
settlements to signing sessions. These reports do not charge a grant a second time.

## Quick start

Build the executable, initialize the database, and create a funded grant:

```sh
make build

export CLEARINGHOUSE_DB_PATH=/var/lib/clearinghouse/accounting.db

./bin/clearinghouse migrate up
./bin/clearinghouse grant create \
  --name 'Developer grants' \
  --sponsor Livepeer \
  --amount-eth 1 \
  --status active
```

Copy the returned grant ID, then create an allocation and API key:

```sh
./bin/clearinghouse api-key create \
  --grant-id GRANT_ID \
  --name gateway \
  --amount-eth 0.1
```

The response contains `allocation_id`, key `id`, and `api_key`. Store the API key
securely: its secret is shown only once.

Start the authorization webhook, embedded Kafka broker, and accounting service:

```sh
export CLEARINGHOUSE_WEBHOOK_TOKEN='a-long-random-shared-token'

./bin/clearinghouse serve \
  --enable-auth-webhook \
  --enable-kafka \
  --enable-accounting \
  --kafka-data-dir /var/lib/clearinghouse/kafka
```

### Connect go-livepeer

Add the generated API key to the gateway's remote-signer headers:

```text
-remoteSignerHeaders "Authorization:Bearer lpg_..."
```

Configure the remote signer to use the clearinghouse webhook and Kafka topic:

```text
-remoteSignerWebhookUrl http://127.0.0.1:8080/v1/signer/authorize
-remoteSignerWebhookHeaders "Livepeer-Clearinghouse-Token:a-long-random-shared-token"
-monitor
-kafkaBootstrapServers 127.0.0.1:9092
-kafkaGatewayTopic livepeer-signing
```

The shared webhook token authenticates the signer. The gateway API key belongs in
the nested `Authorization` header sent by the signer, not in the outer webhook
request.

### Inspect usage and balances

After signing requests, inspect the recorded usage and account balances:

```sh
./bin/clearinghouse usage list
./bin/clearinghouse ledger report
```

## How-to guides

### Run the services

`serve` can run any combination of these components:

- `--enable-auth-webhook` — signer authorization HTTP server;
- `--enable-kafka` — embedded Kafka broker;
- `--enable-accounting` — accounting service;
- `--enable-onchain-listener` — on-chain payment listener.

Enable at least one component. Enable both `--enable-kafka` and
`--enable-accounting` to use the embedded broker with the accounting service;
the connection is configured automatically. The broker can also run independently.

To run the accounting service with an external Kafka broker:

```sh
./bin/clearinghouse serve \
  --enable-accounting \
  --kafka-brokers broker1:9092,broker2:9092
```

Use `--kafka-topic` to select the topic configured on the signer. External broker
addresses cannot be combined with `--enable-kafka`.

Run at most one accounting service and one on-chain listener per accounting
database. Keep SQLite on a local filesystem; do not share it over a network
filesystem.

In general, keep every listener bound to localhost. Cross-network access usually
requires a terminating TLS proxy in front of the clearinghouse; Cloudflare
Tunnels are a good option.

Kafka connections use plaintext. Keep brokers on a trusted loopback or private
network and restrict access with a firewall.

### Enable on-chain reporting

On Arbitrum One, the listener derives the chain ID from the RPC provider and
discovers Livepeer's registered TicketBroker automatically:

```sh
./bin/clearinghouse serve \
  --enable-onchain-listener \
  --rpc-url https://YOUR_RPC \
  --signer-addresses 0xYOUR_SIGNER
```

Use `--start-block` to include earlier activity; otherwise a new database starts
at the current RPC head. The listener resumes from its saved progress on restart.
Your RPC provider must support historical queries for the selected starting point.

Use a separate accounting database for each chain, TicketBroker, and signer set.

Inspect the results with:

```sh
./bin/clearinghouse settlement list
./bin/clearinghouse escrow report
./bin/clearinghouse escrow activity
```

### Create an allocation with its own API keys

To manage an allocation separately from its keys, create the allocation first:

```sh
./bin/clearinghouse allocation create \
  --grant-id GRANT_ID \
  --name 'Example app' \
  --beneficiary 'Example developer' \
  --amount-eth 0.1

./bin/clearinghouse api-key create \
  --allocation-id ALLOCATION_ID \
  --name gateway
```

Allocation creation and funding accept `--amount-eth all` to use the grant's full
current unallocated balance.

### Back up and maintain the database

Stop all clearinghouse processes and management commands before taking a simple
filesystem backup. Copy the accounting database, including its WAL/SHM files, and
the complete embedded Kafka data directory together. Restore them as one consistent
set. For external Kafka, coordinate event-log retention and recovery with the
broker operator.

Backups of the SQLite accounting database can also be managed with Litestream.

Before applying database migrations, stop the services and take a backup. Run
`./bin/clearinghouse migrate up`, then restart the services.

## Reference

### Configuration

Settings can come from command-line flags, environment variables, a configuration
file, or defaults. Precedence is:

```text
flags > environment > configuration file > defaults
```

Run `./bin/clearinghouse serve --help` or a management command with `--help` to see
its flags, environment variables, and defaults. Every command accepts `--db-path`
and `--config-file`.

Both TOML and JSON configuration files are supported. Start with
[`config.example.toml`](config.example.toml) or
[`config.example.json`](config.example.json). Configuration keys use the field
names shown in those examples. Duration values in configuration files are integer
nanoseconds; duration flags and environment variables accept values such as `5s`.

The accounting service can be enabled with `CLEARINGHOUSE_ENABLE_ACCOUNTING`;
`CLEARINGHOUSE_KAFKA_BROKERS` accepts comma-separated broker addresses.
Configuration files use `EnableAccounting` and a `KafkaBrokers` array.

HTTP defaults to `127.0.0.1:8080` and Kafka to `127.0.0.1:9092`.

For on-chain reporting, `--chain-id` optionally checks the RPC provider's chain ID.
`--ticket-broker` overrides discovery and is required on chains without a known
Livepeer Controller. Listener defaults are 64 confirmations, a 5-second poll
interval, 2,000 blocks per batch, and a 256-block reorg lookback.

Keep secrets such as `CLEARINGHOUSE_WEBHOOK_TOKEN` in the environment or a secret
manager rather than a checked-in configuration file.

### Amounts and allocation states

CLI amounts are exact decimal ETH values with up to 18 fractional digits. Signs,
exponents, and additional precision are rejected. Amount-bearing JSON fields use
`*_eth` names and canonical decimal values. Only allocations accept
`--amount-eth all`; grant creation and funding do not.

Grant creation defaults to `draft`. Allocation creation defaults to `active`, or
`exhausted` when it receives zero funding. Funding an exhausted allocation
reactivates it; explicitly paused allocations remain paused. Revoked allocations
and closed grants cannot be reopened.

### HTTP endpoints

The signer authorization endpoint is `POST /v1/signer/authorize`. Authorization
decisions use an inner status: 200 allows signing, 401 rejects the key, 402
indicates an exhausted allocation, and 403 indicates an inactive or invalid
session. Invalid webhook tokens return HTTP 401.

HTTP-enabled processes also expose `GET /healthz` and `GET /readyz`.

### Commands

```text
migrate up|down|status
grant create|list|show|set-status|fund
allocation create|list|show|set-status|fund|revoke
api-key create|list|revoke
session list|show|revoke
ledger report
settlement list
escrow report|activity
usage list
```

Use `--id` for show, revoke, status, and funding commands. Creation commands also
accept RFC3339 `--starts-at` and `--ends-at` values plus JSON `--metadata`.

Services that use the accounting database apply pending migrations at startup.
Before running management or report commands against a new database, run
`migrate up`. Ordinary management commands do not apply migrations.

`migrate down` rolls back the latest migration and can destroy accounting data.
Use it only on a disposable database or after a verified backup.

## Development

Building requires Go 1.27.1, CGO, and a C compiler.

```sh
make build
make check
```

`make check` runs the race-enabled tests, `go vet`, and a production build. Tests
use local fixtures and do not require a live chain or production credentials.
