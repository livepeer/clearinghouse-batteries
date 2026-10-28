package store

import (
	"context"
	"database/sql"
	"math/big"

	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Summary updates use the source transaction, even when collection is disabled.
func changeMetricCount(ctx context.Context, tx *sql.Tx, kind, first, second string, delta int64) error {
	if delta == 0 {
		return nil
	}
	if delta < 0 {
		r, err := tx.ExecContext(ctx, `UPDATE metric_counts SET value=value+? WHERE kind=? AND label1=? AND label2=?`, delta, kind, first, second)
		return affected(r, err)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO metric_counts VALUES (?,?,?,?) ON CONFLICT(kind,label1,label2) DO UPDATE SET value=value+excluded.value`, kind, first, second, delta)
	return err
}

func changeMetricAmount(ctx context.Context, tx *sql.Tx, name, currency string, delta *big.Int) error {
	if delta.Sign() == 0 {
		return nil
	}
	var value string
	if err := tx.QueryRowContext(ctx, `SELECT amount_units FROM metric_amounts WHERE name=? AND currency=?`, name, currency).Scan(&value); err != nil {
		return err
	}
	n, err := signedBalance(value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE metric_amounts SET amount_units=? WHERE name=? AND currency=?`, n.Add(n, delta).String(), name, currency)
	return err
}

func changeInventory(ctx context.Context, tx *sql.Tx, kind, currency, old, next string) error {
	if old == next {
		return nil
	}
	if old != "" {
		if err := changeMetricCount(ctx, tx, kind, currency, old, -1); err != nil {
			return err
		}
	}
	return changeMetricCount(ctx, tx, kind, currency, next, 1)
}

func boolCount(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// MetricSnapshot reads one consistent deferred, read-only transaction on a separate pool.
func MetricSnapshot(ctx context.Context, db *sql.DB, accounting, chain bool) (out []prometheus.Metric, err error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Real bounded reads verify the expected application tables, including HTTP-only monitoring.
	var populated int
	if err = tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT id FROM grants LIMIT 1)+EXISTS(SELECT id FROM grant_allocations LIMIT 1)+
 EXISTS(SELECT id FROM api_keys LIMIT 1)+EXISTS(SELECT id FROM payment_sessions LIMIT 1)+
 EXISTS(SELECT id FROM usage_events LIMIT 1)+EXISTS(SELECT next_position FROM ingestion_checkpoints LIMIT 1)+
 EXISTS(SELECT balance_units FROM account_balances LIMIT 1)+EXISTS(SELECT id FROM settlements LIMIT 1)`).Scan(&populated); err != nil {
		return nil, err
	}

	gauge := func(name string, n float64, labels []string, values ...string) {
		out = append(out, metrics.Gauge(name, n, labels, values...))
	}
	query := func(q string, visit func(*sql.Rows) error) error {
		rows, e := tx.QueryContext(ctx, q)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			if e = visit(rows); e != nil {
				return e
			}
		}
		return rows.Err()
	}
	if !accounting && !chain {
		return out, tx.Commit()
	}
	type bucket struct{ kind, first, second string }
	counts := map[bucket]int64{}
	if err = query(`SELECT kind,label1,label2,value FROM metric_counts`, func(r *sql.Rows) error {
		var b bucket
		var n int64
		if err := r.Scan(&b.kind, &b.first, &b.second, &n); err != nil {
			return err
		}
		counts[b] = n
		return nil
	}); err != nil {
		return nil, err
	}
	amounts := map[string]float64{}
	if err = query(`SELECT name,currency,amount_units FROM metric_amounts`, func(r *sql.Rows) error {
		var name, currency, value string
		if err := r.Scan(&name, &currency, &value); err != nil {
			return err
		}
		if _, err := signedBalance(value); err != nil {
			return err
		}
		amounts[name+"_"+currency] = MetricAmount(value)
		return nil
	}); err != nil {
		return nil, err
	}
	if accounting {
		for _, currency := range []string{"eth", "usd"} {
			gauge("allocations_overdrawn", float64(counts[bucket{"allocations_overdrawn", currency, ""}]), []string{"currency"}, currency)
			for _, kind := range []string{"grant_unallocated", "allocation_available", "allocation_spent", "allocation_overdraw"} {
				name := kind + "_" + currency
				gauge(name, amounts[name], nil)
			}
			largest, err := metricExtreme(ctx, tx, "allocation_available", currency, false)
			if err != nil {
				return nil, err
			}
			value := 0.
			if largest != nil {
				value = MetricAmount(largest.Neg(largest).String())
			}
			gauge("allocation_largest_overdraw_"+currency, value, nil)
		}
		for b, n := range counts {
			if n == 0 {
				continue
			}
			switch b.kind {
			case "quarantines":
				gauge("quarantined_events", float64(n), []string{"reason"}, b.first)
			case "grants", "allocations":
				gauge(b.kind, float64(n), []string{"currency", "status"}, b.first, b.second)
			}
		}
	}
	if chain {
		for _, status := range []string{"settled", "orphaned"} {
			for _, match := range []string{"matched", "unmatched", "ambiguous"} {
				gauge("settlements", float64(counts[bucket{"settlements", status, match}]), []string{"status", "match_status"}, status, match)
			}
		}
		for _, name := range []string{"settlement_face_value", "settlement_paid", "settlement_shortfall"} {
			gauge(name+"_eth", amounts[name+"_eth"], nil)
		}
		gauge("settlements_underpaid", float64(counts[bucket{"settlements_underpaid", "", ""}]), nil)
		var oldest sql.NullInt64
		if err := tx.QueryRowContext(ctx, oldestUnmatchedQuery).Scan(&oldest); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if oldest.Valid && oldest.Int64 > 0 {
			gauge("settlement_oldest_unmatched_timestamp_seconds", float64(oldest.Int64)/1000, nil)
		}
		for _, name := range []string{"deposit", "reserve"} {
			minimum, err := metricExtreme(ctx, tx, "escrow_"+name, "eth", true)
			if err != nil {
				return nil, err
			}
			if minimum == nil {
				continue
			}
			gauge("escrow_balance_eth", amounts["escrow_"+name+"_eth"], []string{"bucket"}, name)
			gauge("escrow_min_balance_eth", MetricAmount(minimum.String()), []string{"bucket"}, name)
			gauge("escrow_unfunded_signers", float64(counts[bucket{"escrow_unfunded_signers", name, ""}]), []string{"bucket"}, name)
		}
		var start sql.NullInt64
		if err = tx.QueryRowContext(ctx, chainStartQuery).Scan(&start); err != nil {
			return nil, err
		}
		if start.Valid {
			gauge("chain_start_block", float64(start.Int64), nil)
		}
	}
	return out, tx.Commit()
}

const oldestUnmatchedQuery = `SELECT created_at_ms FROM settlements WHERE status='settled' AND match_status='unmatched' ORDER BY created_at_ms LIMIT 1`
const chainStartQuery = `SELECT min(block_number)+1 FROM escrow_snapshots`
const negativeExtremeQuery = `SELECT balance_units FROM metric_balances WHERE account_type=? AND currency=? AND substr(balance_units,1,1)='-' ORDER BY length(balance_units) DESC,balance_units DESC LIMIT 1`
const nonnegativeExtremeQuery = `SELECT balance_units FROM metric_balances WHERE account_type=? AND currency=? AND substr(balance_units,1,1)<>'-' ORDER BY length(balance_units),balance_units LIMIT 1`

// Negative values order by descending magnitude, without a bounded numeric cast.
// A minimum falls back to the smallest nonnegative value if no negative exists.
func metricExtreme(ctx context.Context, tx *sql.Tx, account, currency string, minimum bool) (*big.Int, error) {
	queries := []string{negativeExtremeQuery}
	if minimum {
		queries = append(queries, nonnegativeExtremeQuery)
	}
	for _, query := range queries {
		var value string
		err := tx.QueryRowContext(ctx, query, account, currency).Scan(&value)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		return signedBalance(value)
	}
	return nil, nil
}
