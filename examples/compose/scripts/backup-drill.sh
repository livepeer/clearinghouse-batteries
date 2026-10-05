#!/usr/bin/env bash
# Quiesce, sync each replica, restore into an empty directory, compare all tables.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -e restored ]]; then
  echo 'Move restored/ aside before running another drill.' >&2; exit 1
fi
mkdir -m 700 restored
# Stop ingress before writers. No CLI/database mutations during this barrier.
docker compose stop webhook-tunnel management-tunnel signer-tunnel
docker compose stop signer
docker compose stop clearinghouse
for db in clearinghouse.db minikafka_+meta.db minikafka_livepeer-signing_0.db signer-events.sqlite; do
  docker compose exec -T litestream litestream sync -wait -timeout 60 "/data/$db"
done
docker compose stop litestream
for db in clearinghouse.db minikafka_+meta.db minikafka_livepeer-signing_0.db signer-events.sqlite; do
  docker compose run --rm --no-deps restore restore -config /etc/litestream.yml \
    -o "/restore/$db" "/data/$db"
done
docker compose run --rm --no-deps inspect compare
# Deliberately leave writers stopped: these replicas are a consistent recovery set.
echo 'Verified recovery set in restored/. Follow README.md to rehearse restart.'
