# Livepeer grants clearinghouse

The clearinghouse manages grants, usage accounting, and API keys for Livepeer
network payments. It tracks account usage and balances, issued tickets, and network
settlement.

## How accounting works

Grants provide usage credits. Allocations define budgets within a grant, and
each allocation can have multiple API keys. Payers (Livepeer gateways or remote
signers) use these keys to authorize network payments.

The clearinghouse authorization webhook checks the API key and the allocation's
available balance. Payers log issued tickets to Kafka, and the accounting service
validates those events and charges the allocation for usage.

Authorization does not reserve funds. Delayed accounting can take an allocation
negative before signing stops. Monitor accounting lag and quarantined events.

The optional on-chain listener records payments and escrow (deposit + reserve)
activity. It matches settlements to sessions but does not charge for usage again.

## Quick start

This starts the webhook, embedded Kafka broker, and accounting service. Run the
HTTP and Kafka listeners behind terminating TLS proxies.

Build, initialize the database, and create a funded grant:

```sh
make build
./bin/clearinghouse migrate up
./bin/clearinghouse grant create \
  --name 'Developer grants' \
  --sponsor Livepeer \
  --amount-eth 1 \
  --status active
```

Use the returned grant ID to create an allocation and API key:

```sh
./bin/clearinghouse api-key create \
  --grant-id GRANT_ID \
  --name gateway \
  --amount-eth 0.1
```

The response includes `allocation_id`, key `id`, and `api_key`. Store the key;
the CLI will not show its secret again.

Copy [`creds.example.toml`](creds.example.toml), replace each empty secret with a
distinct random value, and restrict file access to the clearinghouse process.
Remove the `operator` entry if you do not need the management API, then start:

```sh
./bin/clearinghouse serve \
  --creds-file /run/secrets/clearinghouse-creds.toml \
  --enable-auth-webhook :8080 \
  --enable-kafka \
  --kafka-advertised-addr clearinghouse.example.com:9093 \
  --enable-accounting
```

Use a certificate trusted by clients and configure the proxy endpoints below.
Replace the hostname with yours. The Kafka proxy terminates TLS and forwards
TCP traffic; accounting connects to the broker locally.

| Client endpoint | Local listener |
| --- | --- |
| `https://clearinghouse.example.com` | `http://127.0.0.1:8080` |
| `clearinghouse.example.com:9093` (Kafka over TLS) | `127.0.0.1:9092` |

### Connect go-livepeer

Set the gateway's remote-signer header:

```text
-remoteSignerHeaders "Authorization:Bearer lpg_..."
```

Configure the remote signer:

```text
-remoteSignerWebhookUrl https://clearinghouse.example.com/v1/signer/authorize
-remoteSignerWebhookHeaders "Livepeer-Clearinghouse-Token:SIGNER_SECRET"
-monitor
-kafkaBootstrapServers clearinghouse.example.com:9093
-kafkaGatewayTopic livepeer-signing
```

Use the `signer` entry's secret for `SIGNER_SECRET`. Through your secret manager,
set go-livepeer's `LP_KAFKAUSER` to `producer` and `LP_KAFKAPASSWORD` to the
`producer` entry's secret. The producer uses SASL/PLAIN over TLS.

The outer webhook header authenticates the signer. The gateway API key is sent
in the request body's nested `Authorization` header.

After accounting applies ticket events, inspect balances:

```sh
./bin/clearinghouse ledger report
```

See the [Docker Compose sample](examples/compose/README.md) for Clearinghouse +
Livepeer Node signer, TLS termination, and a database backup/restore drill.

## How-to guides

### Run the services

Enable one or more components:

- `--enable-auth-webhook :PORT` — signer authorization;
- `--enable-management-api :PORT` — account management HTTP API;
- `--enable-kafka` — embedded Kafka broker;
- `--enable-accounting` — ticket accounting;
- `--enable-onchain-listener` — on-chain reporting.

With both `--enable-kafka` and `--enable-accounting`, the local broker connection
is automatic. The broker defaults to `127.0.0.1:9092` and topic
`livepeer-signing`. Producers connect through its TLS proxy.

For an external broker, omit `--enable-kafka`:

```sh
./bin/clearinghouse serve \
  --creds-file /run/secrets/clearinghouse-creds.toml \
  --enable-accounting \
  --kafka-broker broker:9092
```

Set `--kafka-topic` to match the signer. External accounting uses plaintext TCP;
configure the broker's users and permissions separately. If go-livepeer shares
this broker, provide TLS for its producer and a private plaintext connection
for accounting, both using the same cluster and topic.

### Enable on-chain reporting

On Arbitrum One, the listener derives the chain ID from the RPC provider and
discovers Livepeer's registered TicketBroker:

```sh
./bin/clearinghouse serve \
  --enable-onchain-listener \
  --rpc-url-file /run/secrets/clearinghouse-rpc-url \
  --signer-addresses 0xYOUR_SIGNER
```

A new database starts at the current RPC head unless `--start-block` is set;
restarts resume saved progress. The RPC provider must support historical queries
from that block. Use a separate database for each chain, TicketBroker, and signer
set.

```sh
./bin/clearinghouse settlement list
./bin/clearinghouse escrow report
./bin/clearinghouse escrow activity
```

### Create an allocation with its own API keys

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

Allocation creation and funding accept `--amount-eth all` to use the grant's
full unallocated balance.

## Reference

### Service credentials

Service credentials authenticate management clients, signers, and Kafka clients.
Gateway API keys authorize signing against allocations and are created separately.
Local CLI commands use the database directly and need no service credentials.

`--creds-file` is required for the management API, webhook, or embedded Kafka.
Use [`creds.example.toml`](creds.example.toml) or
[`creds.example.json`](creds.example.json); omit unused entries. Each entry has
one `management`, `webhook`, or `kafka` block, a unique `id` (1–64 ASCII letters,
digits, `_`, or `-`), and a distinct nonempty `secret`. Unknown fields are rejected.

Generate secrets with at least 32 random bytes, for example `openssl rand -hex 32`.
For HTTP, send the secret as-is in `Livepeer-Clearinghouse-Token`; the `id` is not
sent. HTTP secrets must be valid header values without leading or trailing
spaces or tabs. The TOML/JSON file may end with a newline.

After changing secrets or permissions, update affected clients or broker users
and restart clearinghouse.

#### Management permissions

`allow` lists use `resource.action`. A permission covers all resources of that
type; `resource.*` includes current and future actions. Unknown permissions are
rejected. For reporting access, allow only the needed `read` permissions.

| Resource | Actions |
| --- | --- |
| `grants` | `read`, `create`, `fund`, `status` |
| `allocations` | `read`, `create`, `fund`, `status`, `revoke` |
| `api_keys` | `read`, `create`, `revoke` |
| `sessions` | `read`, `revoke` |
| `settlements`, `usage`, `ledger`, `escrow` | `read` |

Creating a grant or allocation with nonzero funding needs both `create` and
`fund`; allocation creation with `all` also needs `fund`. Zero or omitted
amounts need only `create`.

API-key creation with `grant_id` needs `api_keys.create`, `allocations.create`,
and `allocations.fund`, even for zero funding. With an existing `allocation_id`,
it needs only `api_keys.create`.

#### Webhook and Kafka credentials

Webhook entries set `authorize = true` to authenticate the signer.

Kafka entries use `username`, `allow` (`read` or `write`), and `secret` as the
password. Usernames must be unique, including after SCRAM normalization.
Embedded Kafka requires separate read and write users, limits access to
`--kafka-topic`, and accepts SASL/PLAIN and SASL/SCRAM-SHA-512.

Embedded accounting requires exactly one read credential with `accounting = true`.
The accounting client uses SCRAM-SHA-512. For external accounting this credential
is optional; without it, the client uses no SASL authentication. External broker
permissions must be configured on the broker.

### Configuration and commands

Run `./bin/clearinghouse --help` or `<command> --help` for commands, flags,
environment variables, and defaults. Every command accepts `--db-path` and
`--config-file`. Configuration precedence is:

```text
flags > environment > configuration file > defaults
```

Start with [`config.example.toml`](config.example.toml) or
[`config.example.json`](config.example.json). Set the credentials file path with
`--creds-file`, `CLEARINGHOUSE_CREDS_FILE`, or `CredsFile`.

For embedded Kafka behind a proxy such as TLS, set `--kafka-advertised-addr`,
`CLEARINGHOUSE_KAFKA_ADVERTISED_ADDR`, or `KafkaAdvertisedAddr` to the client-facing
`host:port`; when empty, it defaults to `KafkaBind`.

Supply the RPC URL via `CLEARINGHOUSE_RPC_URL`, or a file selected by
`--rpc-url-file`, `CLEARINGHOUSE_RPC_URL_FILE`, or `RPCURLFile`. Direct RPC URL
flags and configuration values are rejected. The URL file is read verbatim;
write the URL without a trailing newline.

The accounting database defaults to `clearinghouse.db` in the working directory.
Embedded Kafka's `minikafka_*.db` files are stored beside the database selected
by `--db-path`.

| Command | Purpose |
| --- | --- |
| `serve` | Run services. |
| `grant`, `allocation` | Manage budgets, funding, and status. |
| `api-key`, `session` | Create or inspect API keys; inspect sessions; revoke either. |
| `usage`, `ledger` | Inspect applied/quarantined events and accounting balances. |
| `settlement`, `escrow` | Inspect redemptions, session attribution, balances, and payment activity. |
| `migrate` | Apply, inspect, or roll back migrations. |

### Amounts and states

Amounts use `--amount-usd` or `--amount-eth` (`amount_usd` / `amount_eth` in the
API): exact decimals with up to 18 fractional digits. New grants default to
USD $0 if the amount is omitted. Allocations inherit their grant's currency.
Allocation creation and funding accept `all`. Signer events require an ETH fee;
USD allocations also require a USD fee. Missing required fees or invalid supplied
fees are quarantined. On-chain escrow and settlements remain in ETH.

API and CLI reports use decimal `*_usd` or `*_eth` strings for numeric amounts.
Allocation reads include `available_*` for the remaining balance, which may be
negative, and `spent_*` for recorded usage, including late charges after revocation.
Empty amounts return `""`, null amounts return `null`, and failed amount conversions
return `"[error]"`. Query the database directly to inspect the stored values.

Grants default to `draft`. Allocations default to `active`, or `exhausted` with
zero funding. Exhaustion is automatic; funding reactivates exhausted allocations
but leaves paused ones paused. Closed grants and revoked allocations cannot be
reopened.

Allocation status updates accept `active` or `paused`. To revoke and return
unused funds, use `allocation revoke` or `POST /v1/allocations/{id}/revoke`
(`allocations.revoke` permission). Revocation is not a status update.

### Database management

`serve` applies pending migrations. Before running CLI management or reports on
a new database or after upgrading, run `migrate up`. `migrate down` can destroy
accounting data; use it only on disposable databases or after a verified backup.

For backups, use Litestream or stop all clearinghouse processes and management
commands and copy the accounting and Kafka databases, including WAL/SHM files,
as one consistent set. Test restoration regularly.

### HTTP listeners

Both HTTP listeners require an explicit port: `:8080` binds to `127.0.0.1:8080`.
Use IPv4 or bracketed IPv6 literals. Non-loopback and wildcard binds require
`--unsafe-http-bind`, which adds no TLS or access control. The webhook and
management API must use different ports; expose them through TLS proxies.

See [API.md](API.md) for the webhook, management, and health endpoints.

## Development

Builds require Go 1.27.3, CGO, and a C compiler for SQLite. `make check` runs
race-enabled tests, `go vet`, Staticcheck, a vulnerability scan, and a production
build. Tests use local fixtures without a live chain or production credentials.
Run `./bin/clearinghouse --version` to inspect the version and source commit.

To release, set [VERSION](VERSION) to the next `MAJOR.MINOR.PATCH`, merge it to
`main`, and run [Release](.github/workflows/release.yml) from GitHub Actions.
Retry failures with **Re-run failed jobs** until artifacts expire after 7d, or
rerun all jobs.

See [SECURITY.md](SECURITY.md) for deployment security.

### Metrics

Enable a private Prometheus endpoint with `--enable-metrics :9094`. See the
[monitoring examples](examples/monitoring/README.md) for metric semantics,
scraping configuration, alert guidance, and a Grafana dashboard.
