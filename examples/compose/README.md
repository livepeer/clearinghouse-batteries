# Clearinghouse + Livepeer Node signer

A single-host sample with embedded Kafka, accounting, persistent SQLite, a durable
signer outbox, Litestream, and three Cloudflare Quick Tunnels. No host ports are
published. Clearinghouse, signer, and tunnels share a container network namespace;
HTTP origins and Kafka bind loopback. The Node producer and accounting consumer use
**SCRAM-SHA-512 without TLS (SASL_PLAINTEXT)**. `kafka://` explicitly disables TLS.
The embedded broker also supports SASL/PLAIN; it is inaccessible outside this
namespace. Metrics remain private on port 8938.

## Start

Install Docker Engine with Compose v2 and Python 3. Clone `livepeer/node` beside
this checkout (`../node`), or set `NODE_SOURCE` and `SIGNER_DOCKERFILE` in `.env`
(the latter may be an absolute path to this sample's `Dockerfile.signer`). Builds
use the selected local sources; record both commit IDs and working-tree changes
when preparing a release. Container dependencies have versioned tags.

```sh
cd examples/compose
python3 scripts/init-secrets.py
# Copy an encrypted geth keystore and its exact password into:
# secrets/keystore.json, secrets/keystore-password
# Write an Arbitrum One RPC URL with NO trailing newline to secrets/rpc-url.
chmod 600 secrets/*
docker compose build
docker compose up -d
docker compose ps
docker compose logs webhook-tunnel management-tunnel signer-tunnel
```

Service secrets are generated once and mounted as files. Keep `secrets/` in a
separate encrypted backup: SQLite replicas do not include the wallet, RPC URL,
operator token, webhook token, or Kafka credentials. Local Compose bind mounts
preserve host permissions; the signer requires owner-only wallet/password files.
The sample runs as root inside its containers so these mounts remain readable.

Fund the signer account's TicketBroker deposit and reserve before paid requests.
Signer `/readyz` on private port 8938 checks chain funds, price policy and outbox;
Kafka publication failures are reported in `/metrics`. Clearinghouse `/readyz`
is an HTTP/database health check; also inspect accounting logs and checkpoint
progress. Restart policies supervise process exits, not unhealthy containers.

The log of each tunnel contains its `https://…trycloudflare.com` URL. Use the
**signer** URL as the gateway's remote signer, with the allocation API key in its
`Authorization: Bearer lpg_…` header. The signer calls the **private** webhook and
forwards that authorization inside the webhook body. Public webhook clients also
need the webhook token. Public management clients need `Livepeer-Clearinghouse-Token`
with the contents of `secrets/operator-token`.

Create a grant and allocation/key (store the returned API key securely):

```sh
docker compose exec clearinghouse clearinghouse --db-path /data/clearinghouse.db \
  grant create --name Sample --sponsor Livepeer --amount-eth 1 --status active
docker compose exec clearinghouse clearinghouse --db-path /data/clearinghouse.db \
  api-key create --grant-id GRANT_ID --name gateway --amount-eth 0.1
```

Quick Tunnels have random URLs that change after recreation. They are public test
endpoints, with no SLA, a 200-request concurrency limit, and no SSE support; use
named tunnels/access policies for long-lived deployments. Keep gateway URLs in
sync after restarts. When recreating Clearinghouse, recreate the signer and
tunnels with it (`docker compose down` then `docker compose up -d`) so all join
the same new namespace. [Cloudflare's Quick Tunnel documentation](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/)
describes these limits. The signer publishes payment events to Kafka; it does not
itself run an orchestrator or SDK runner.

Optional chain reporting: set `EnableOnchainListener`, `RPCURLFile`, and
`SignerAddresses` in `clearinghouse.toml`. A new database starts at the current
head unless `StartBlock` is set; recovery resumes the saved checkpoint. Custom
chains require explicit Node network/chain/controller and oracle configuration.

## Back up and rehearse recovery

Litestream continuously replicates four databases: Clearinghouse (balances,
sessions, accounting/chain checkpoints), Kafka metadata, Kafka's signing partition,
and the signer outbox. This fixed topic has one partition. Update the configuration
and drill's database list if that topology changes; the drill rejects unexpected
SQLite files. Named volumes keep SQLite/WAL locks inside the Docker host.

The default `backup` volume demonstrates local replicas, **not host-loss recovery**.
For off-host backups use the supplied GCS overlay: put a restricted GCS credential
in `secrets/gcs.json`, set `REPLICA_ROOT=gs://BUCKET/UNIQUE_PREFIX` in `.env`, and use
both files for every command. The identity needs object read/write/list/delete for
that prefix (retention deletes old objects). Never reuse a prefix across independent
stacks. [Litestream GCS setup](https://litestream.io/guides/gcs/) documents ADC.

```sh
export COMPOSE_FILE=compose.yaml:compose.gcs.yaml # omit for local replicas
# No concurrent CLI writes during this drill. It stops public ingress and writers.
bash scripts/backup-drill.sh
```

The drill stops tunnels, signer, then Clearinghouse; forces and waits for replica
sync; stops Litestream; restores into a new `restored/` directory; and checks SQLite
integrity, foreign keys, schema and every table's contents against the stopped
source, including balances, Kafka offsets, chain checkpoints and outbox rows.
It fails on missing backups or any mismatch and leaves writers stopped.
Live replicas alone are not an atomic multi-database checkpoint: restoring the
independent latest positions after a crash can leave accounting ahead of Kafka.
Preserve the verified `restored/` recovery set off-host after each successful drill.
Its databases contain sensitive account data. Replication RPO is asynchronous;
restore only a verified set when cross-database consistency is required.

Rehearse migration and restart in a **new Compose project**, preserving the source:

```sh
# With GCS: first change REPLICA_ROOT to a NEW recovery prefix. This prevents
# the recovery stack from modifying the original stack's replica history.
docker compose -p clearinghouse-recovery run --rm --no-deps inspect install
docker compose -p clearinghouse-recovery run --rm --no-deps clearinghouse \
  --db-path /data/clearinghouse.db migrate up
docker compose -p clearinghouse-recovery up -d
docker compose -p clearinghouse-recovery ps
docker compose -p clearinghouse-recovery exec clearinghouse clearinghouse \
  --db-path /data/clearinghouse.db ledger report
# Check new management/webhook URLs, signer readiness and accounting logs.
# Make an authenticated request, confirm the saved key still works, and verify
# new events advance checkpoints without charging previously applied events again.
```

`inspect install` refuses a nonempty destination. Recovery copies only restored
SQLite files with mode 0600 into the new volume, never stale source WAL/SHM or
Litestream metadata. Owner-only mode is required by the signer outbox.
Litestream reconstructs committed WAL contents during restore. A real recovery
also needs the separately saved secrets and the original signer key. Keep the
source and old images until validation succeeds. An upgrade can be rehearsed with
newly built images against this recovery project before switching ingress.

Stop with `docker compose down`; named volumes survive. `down -v` permanently
removes that project's live databases **and its local replicas**. Delete a disposable
recovery project only after inspecting its project name. Restore the original
`REPLICA_ROOT` before resuming the source with `docker compose up -d`; subsequent
replica writes mean “latest” no longer refers to the drill's checkpoint, so retain `restored/` independently.

This sample covers deployment and recovery work in
[naap-ops #74](https://github.com/livepeer/naap-ops/issues/74) and
[#80](https://github.com/livepeer/naap-ops/issues/80), and supports the Node runbook
and backup work in [#60](https://github.com/livepeer/naap-ops/issues/60) and
[#35](https://github.com/livepeer/naap-ops/issues/35).

## Verification

Rehearsed on 2026-10-04 in a disposable Ubuntu 24.04 GCP VM (Docker 29.1.3,
Compose 2.40.3), using Clearinghouse `6bdbdc0` and Node `37b15c0` plus this
sample. The `e2e-scope` geth fixture supplied the isolated chain and wallet;
Node used that chain's Controller and a fixed test exchange rate.

Verified all three public HTTPS endpoints, HTTP credential rejection, grant/key
creation and webhook authorization, SCRAM delivery, wrong-password retention,
duplicate accounting without a second charge, and restoration of all four
SQLite databases with exact table comparisons. A second drill restored a pending
outbox event into fresh volumes, ran `migrate up`, restarted both services,
reauthorized with the saved API key, and delivered the replay with unchanged
balances and one Kafka checkpoint advance. Installation into a nonempty volume
was rejected. The producer tests injected labeled fixture events into the signer
outbox; they did not run a complete paid SDK job.

The GCS overlay was syntax-checked; GCS transport was not exercised. The old geth
fixture substitutes Clique coinbase in RPC headers, so continuous on-chain
listener coverage was disabled after its ancestry checks rejected those headers.
The saved chain checkpoint was nevertheless preserved by backup and recovery.
This exercised the current schema's migration command, not a version upgrade.
