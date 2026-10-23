#!/bin/sh
set -eu

version=$1
commit=$2
work=$(mktemp -d)
pid=
watchdog=
cleanup() {
  if [ -n "$watchdog" ]; then kill "$watchdog" 2>/dev/null || true; fi
  if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT
cd "$work"

if command -v go >/dev/null 2>&1; then
  echo 'Smoke environment unexpectedly contains Go' >&2
  exit 1
fi
test "$(clearinghouse --version)" = "clearinghouse version $version ($commit)"
clearinghouse --help > help
clearinghouse completion bash > completion
test -s help && test -s completion
clearinghouse migrate up --db-path accounts.db > migrations.json
clearinghouse migrate status --db-path accounts.db > status.json
grant=$(clearinghouse grant create --db-path accounts.db --name smoke --amount-eth 1 --status active |
  sed -n 's/.*"id": "\([^"]*\)".*/\1/p')
test -n "$grant"
clearinghouse api-key create --db-path accounts.db --grant-id "$grant" --name smoke --amount-eth 0.1 > key.json
grep -q '"api_key": "lpg_' key.json
clearinghouse ledger report --db-path accounts.db > ledger.json

cat > creds.json <<'EOF'
{"credentials":[
  {"id":"operator","secret":"smoke-operator","management":{"allow":["grants.read"]}},
  {"id":"signer","secret":"smoke-signer","webhook":{"authorize":true}},
  {"id":"reader","secret":"smoke-reader","kafka":{"username":"reader","allow":["read"],"accounting":true}},
  {"id":"writer","secret":"smoke-writer","kafka":{"username":"writer","allow":["write"]}}
]}
EOF
chmod 600 creds.json
clearinghouse serve --db-path accounts.db --creds-file creds.json \
  --enable-auth-webhook :18080 --enable-management-api :18081 \
  --enable-kafka --kafka-bind 127.0.0.1:19092 --enable-accounting > service.log 2>&1 &
pid=$!

http() {
  if command -v curl >/dev/null 2>&1; then
    curl --fail --silent --show-error --max-time 2 "$@"
  else
    wget -q -T 2 -O - "$@"
  fi
}
ready=false
attempt=0
while [ "$attempt" -lt 30 ]; do
  if http http://127.0.0.1:18080/readyz 2>/dev/null && http http://127.0.0.1:18081/readyz 2>/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  attempt=$((attempt + 1))
  sleep 1
done
if [ "$ready" != true ]; then
  cat service.log >&2
  exit 1
fi
http http://127.0.0.1:18080/livez
http http://127.0.0.1:18081/livez
if http http://127.0.0.1:18081/v1/grants 2>/dev/null; then
  echo 'Management API accepted an unauthenticated request' >&2
  exit 1
fi
http --header 'Livepeer-Clearinghouse-Token: smoke-operator' http://127.0.0.1:18081/v1/grants > grants.json
grep -q "$grant" grants.json

kill -TERM "$pid"
(sleep 10; kill -KILL "$pid" 2>/dev/null || true) &
watchdog=$!
if ! wait "$pid"; then
  cat service.log >&2
  exit 1
fi
pid=
kill "$watchdog" 2>/dev/null || true
watchdog=
echo 'Version, CLI, SQLite, credentials, Kafka, accounting, readiness, and shutdown passed without Go.'
