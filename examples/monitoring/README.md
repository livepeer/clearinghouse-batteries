# Clearinghouse monitoring

Enable `--enable-metrics :9094`, `CLEARINGHOUSE_ENABLE_METRICS=:9094`, or
`EnableMetrics = ":9094"`. Metrics are disabled by default. Keep this unauthenticated
port private; non-loopback binds require `--unsafe-http-bind` and network access
controls. The endpoint does not change `/readyz`.

Use `prometheus.yml` as a scrape example (15 seconds). Give components sharing a
database the same `clearinghouse` label and distinct `instance` labels; scrape each
process once. Import `dashboard.json` and select a Prometheus datasource.
Accounting owns budget/quarantine gauges; chain listeners own settlement/escrow
gauges. Broker-only operation never opens the accounting database.

## Reading metrics

Application names use `livepeer_clearinghouse_`. Database, Kafka and file snapshots
refresh every 15 seconds with five-second deadlines; chain snapshots follow polls.
`collector_success == 0` means data is unavailable, not zero. Last-success timestamps
are absent before first success and retained after failure. Timeouts count as
errors; intentional shutdown preserves the previous snapshot.

Counters reset on restart. Financial gauges reflect committed state and can
decrease after funding changes or reorgs. Exact integer totals are converted to
approximate ETH/USD samples; keep currencies separate. Overdraw is summed before
aggregation, so positive allocations cannot conceal negative ones. Chain totals
start at `chain_start_block`; escrow includes its opening snapshot.

Database gauges use bounded reads of summaries updated atomically with source
records, even when metrics are disabled.

Kafka lag measures the durable checkpoint. Zero lag does not establish billing:
check quarantined/ignored outcomes. Quarantines are retained audit records, not a
retry queue; use usage/ledger reports to investigate. Same-offset replay is not
new usage; duplicate payloads at new offsets are counted separately. Apply delay
measures completed events, not the oldest pending record. Future/missing event
timestamps are excluded and counted.

`authorization_decisions_total` records reasons `kafka_lag_exceeded` and
`kafka_lag_unavailable` with decision `503`. See [Kafka lag](../../README.md#configure-kafka-lag)
for enforcement and configuration.

## Alerts

`alerts.rules.json` provides invariant-based examples. Adapt the target-down job
selector. Underpayment and ambiguity alerts intentionally remain active for
retained audit findings; investigate rather than treating them as transient rates.
Timing/liquidity policies below are **unset**; calibrate them from production data
and configure them explicitly in your alert provisioning system.

| Policy | Signal (prefix application names as above) |
| --- | --- |
| Accounting backlog/stall duration | Nonzero `accounting_lag_records`; unchanged durable checkpoint while behind |
| Apply-delay threshold | p99 `accounting_apply_delay_seconds` |
| Collector freshness | Age of `collector_last_success_timestamp_seconds`, gated on enabled components |
| Chain freshness | Age of `chain_last_successful_poll_timestamp_seconds` and `chain_head_timestamp_seconds` |
| Chain backlog/duration | `chain_lag_blocks`, already accounting for confirmations |
| Unmatched duration | Age of `settlement_oldest_unmatched_timestamp_seconds` while canonical unmatched count is positive |
| Deposit/reserve minimums | `escrow_min_balance_eth` per bucket; totals can hide an empty signer |
| HTTP failure ratio/duration | Application 5xx / requests, excluding health traffic, with a traffic floor |

Business denials differ from server failures. Idle traffic is normal; successful
caught-up chain polls still refresh health. Signed usage fees need not equal
probabilistic redemptions. Scrape the signer outbox too: it reveals events that
have not reached Kafka. Obtain free disk, restart, expected-target and backup
monitoring from their owning infrastructure. Fatal errors can exit before a scrape.

Validate with `promtool check rules alerts.rules.json` and
`promtool test rules alerts.test.yml`.
