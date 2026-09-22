# Livepeer grants clearinghouse

The clearinghouse manages budgets for Livepeer remote signing. It provides:

- grant and allocation management;
- API-key authorization for gateways;
- accounting for issued tickets;
- on-chain payment reporting.

All bookkeeping is done with a local SQLite database.

## How accounting works

A grant provides a budget that can be divided into allocations. Gateways use API
keys associated with those allocations to request signing authorization.

The signer uses Kafka as a durable event log of issued tickets. The accounting
service validates events against authorized sessions and records their charges
in the internal ledger. The authorization webhook uses the available allocation
balance to decide whether signing may continue.

Accounting is asynchronous: authorization does not reserve funds, so delayed
events can take an allocation negative before further signing is stopped. Monitor
accounting lag, quarantined events, and negative balances.

The optional on-chain listener records payments and escrow (deposit + reserve)
activity and matches settlement to signing sessions. Settlement does not charge
a grant a second time.

## Quick start

Build the executable, initialize the database, and create a funded grant:

```sh
make build

./bin/clearinghouse migrate up
./bin/clearinghouse grant create \
  --name 'Developer grants' \
  --sponsor Livepeer \
  --amount-eth 1 \
  --status active
```

By default, the SQLite accounting and embedded Kafka databases are stored in
the current working directory as `clearinghouse.db` and `minikafka_*.db`.
Kafka databases are always stored beside the accounting database selected by
`--db-path`. The explicit migration is needed because the management command
that follows does not apply migrations; `serve` applies pending migrations
automatically.

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
  --enable-auth-webhook :8080 \
  --enable-kafka \
  --enable-accounting
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

### Inspect balances

After signing requests, inspect the account balances:

```sh
./bin/clearinghouse ledger report
```

## How-to guides

### Run the services

`serve` can run any combination of these components:

- `--enable-auth-webhook `:PORT` — signer authorization HTTP server;
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
  --kafka-broker broker:9092
```

Use `--kafka-topic` to select the topic configured on the signer. External broker
addresses cannot be combined with `--enable-kafka`.

Run at most one accounting service and one on-chain listener per accounting
database. Keep SQLite on a local filesystem; do not share it over a network
filesystem.

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

## Reference

### Configuration

Run `./bin/clearinghouse serve --help` or a management command with `--help` to see
its flags, environment variables, and defaults. Every command accepts `--db-path`
and `--config-file`. Options marked with an environment variable in --help can
also be configured through the environment.

Configuration precedence:

```text
flags > environment > configuration file > defaults
```

Both TOML and JSON configuration files are supported. Start with
[`config.example.toml`](config.example.toml) or
[`config.example.json`](config.example.json). Configuration keys use the field
names shown in those examples.

Secrets can be provided directly through the environment or via a local mounted
secret file.

| Secret | Direct environment | File environment | File flag | Configuration key |
| --- | --- | --- | --- | --- |
| Authorization webhook token | `CLEARINGHOUSE_WEBHOOK_TOKEN` | `CLEARINGHOUSE_WEBHOOK_TOKEN_FILE` | `--webhook-token-file` | `WebhookTokenFile` |
| On-chain RPC URL | `CLEARINGHOUSE_RPC_URL` | `CLEARINGHOUSE_RPC_URL_FILE` | `--rpc-url-file` | `RPCURLFile` |

Direct secret flags and configuration values are not accepted. Secret file
contents are used verbatim, so avoid a trailing newline unless it is part of the
secret.

### Commands

Run `./bin/clearinghouse --help` to list available commands. Run
`./bin/clearinghouse <command> --help` for its subcommands, options,
environment variables, and defaults.

- `serve` — Run the authorization, Kafka, accounting, and on-chain services.
- `grant` — Create and manage grant budgets.
- `allocation` — Divide grants into allocations and manage their funding and status.
- `api-key` — Create, list, and revoke API keys.
- `session` — Inspect and revoke payment sessions.
- `usage` — Inspect applied and quarantined Kafka events.
- `ledger` — Report exact accounting balances.
- `settlement` — Inspect treasury redemptions and session attribution.
- `escrow` — Report on-chain balances and payment activity.
- `migrate` — Apply, inspect, or roll back database migrations.

### Amounts and allocation states

CLI amounts are exact decimal ETH values with up to 18 fractional digits. Signs,
exponents, and additional precision are rejected. ETH-denominated JSON fields use
`*_eth` names and decimal values. Only allocations accept
`--amount-eth all`; grant creation and funding do not.

Grant creation defaults to `draft`. Allocation creation defaults to `active`, or
`exhausted` when it receives zero funding. Funding an exhausted allocation
reactivates it; explicitly paused allocations remain paused. Revoked allocations
and closed grants cannot be reopened.

### Security

See [SECURITY.md](SECURITY.md) for deployment security recommendations.

### Database Management

#### Back-Ups

There are several options for database back-up:

1. Use Litestream.

2. Stop all clearinghouse processes and management commands. Copy the accounting
   and Kafka databases, including their WAL/SHM files. Restore everything as one
   consistent set.

3. Use Litestream.

Validate backups regularly.

#### Migrations

Long-lived services that use the accounting database (eg, via `serve`) apply
pending migrations at startup.

For short-lived management or report commands against a new database, run
`migrate up` first.

`migrate down` rolls back the latest migration and can destroy accounting data.
Use it only on a disposable database or after a verified backup.


### HTTP endpoints and Binding

The `--enable-auth-webhook` flag surfaces these endpoints:

* `POST /v1/signer/authorize` Remote signer authorization
* `GET /livez` Returns 200 OK whenever the server can answer
* `GET /readyz` Returns 503 if the server is shutting down or the database is unavailable, 200 otherwise.

The authorization webhook has no default bind and requires at least a port.
`--enable-auth-webhook :8080` enables it on `127.0.0.1:8080`. Bind addresses
must be IPv4 or bracketed IPv6 literals. Non-loopback and wildcard addresses
additionally require `--unsafe-http-bind`. Cross-network access usually
requires a terminating TLS proxy in front of the clearinghouse.

A webhook token is also required; specify via CLEARINGHOUSE_WEBHOOK_TOKEN and
configure go-livepeer as indicated in [Connect go-livepeer](#Connect-go-livepeer).

## Development

Building requires Go 1.27.1, CGO, and a C compiler.

```sh
make build
make check
```

`make check` runs the race-enabled tests, `go vet`, and a production build. Tests
use local fixtures and do not require a live chain or production credentials.
