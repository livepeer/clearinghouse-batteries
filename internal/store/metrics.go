package store

import (
	"context"
	"database/sql"
	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"math/big"
)

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
	if accounting {
		amounts := map[string]*big.Int{}
		counts := map[string]int{}
		maxima := map[string]*big.Int{}
		err = query(`SELECT b.account_type,b.currency,b.balance_units FROM account_balances b LEFT JOIN grant_allocations a ON a.id=b.account_id WHERE b.account_type='grant_unallocated' OR (b.account_type IN ('allocation_available','allocation_spent') AND a.currency=b.currency)`, func(r *sql.Rows) error {
			var kind, currency, value string
			if e := r.Scan(&kind, &currency, &value); e != nil {
				return e
			}
			n, e := signedBalance(value)
			if e != nil {
				return e
			}
			key := kind + "_" + currency
			if amounts[key] == nil {
				amounts[key] = new(big.Int)
			}
			amounts[key].Add(amounts[key], n)
			if kind == "allocation_available" && n.Sign() < 0 {
				counts[currency]++
				n.Neg(n)
				k := "allocation_overdraw_" + currency
				if amounts[k] == nil {
					amounts[k] = new(big.Int)
				}
				amounts[k].Add(amounts[k], n)
				if maxima[currency] == nil || n.Cmp(maxima[currency]) > 0 {
					maxima[currency] = new(big.Int).Set(n)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, currency := range []string{"eth", "usd"} {
			gauge("allocations_overdrawn", float64(counts[currency]), []string{"currency"}, currency)
			for _, kind := range []string{"grant_unallocated", "allocation_available", "allocation_spent", "allocation_overdraw"} {
				key := kind + "_" + currency
				n := amounts[key]
				if n == nil {
					n = new(big.Int)
				}
				gauge(key, MetricAmount(n.String()), nil)
			}
			n := maxima[currency]
			if n == nil {
				n = new(big.Int)
			}
			gauge("allocation_largest_overdraw_"+currency, MetricAmount(n.String()), nil)
		}
		quarantines := map[string]int64{}
		err = query(`SELECT error,count(*) FROM usage_events WHERE status='quarantined' GROUP BY error`, func(r *sql.Rows) error {
			var reason string
			var n int64
			if e := r.Scan(&reason, &n); e != nil {
				return e
			}
			quarantines[QuarantineReason(reason)] += n
			return nil
		})
		if err != nil {
			return nil, err
		}
		for reason, n := range quarantines {
			gauge("quarantined_events", float64(n), []string{"reason"}, reason)
		}
		for _, item := range []struct{ table, name string }{{"grants", "grants"}, {"grant_allocations", "allocations"}} {
			err = query("SELECT currency,status,count(*) FROM "+item.table+" GROUP BY currency,status", func(r *sql.Rows) error {
				var c, s string
				var n int64
				if e := r.Scan(&c, &s, &n); e != nil {
					return e
				}
				gauge(item.name, float64(n), []string{"currency", "status"}, c, s)
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	if chain {
		totals := map[string]*big.Int{"settlement_face_value_eth": new(big.Int), "settlement_paid_eth": new(big.Int), "settlement_shortfall_eth": new(big.Int)}
		counts := map[string]int{}
		oldest := int64(0)
		underpaid := 0
		err = query(`SELECT status,match_status,face_value_wei,paid_amount_wei,created_at_ms FROM settlements`, func(r *sql.Rows) error {
			var status, match, face, paid string
			var created int64
			if e := r.Scan(&status, &match, &face, &paid, &created); e != nil {
				return e
			}
			counts[status+":"+match]++
			if status == "settled" {
				f, e := Amount(face)
				if e != nil {
					return e
				}
				p, e := Amount(paid)
				if e != nil {
					return e
				}
				totals["settlement_face_value_eth"].Add(totals["settlement_face_value_eth"], f)
				totals["settlement_paid_eth"].Add(totals["settlement_paid_eth"], p)
				if f.Cmp(p) > 0 {
					underpaid++
					totals["settlement_shortfall_eth"].Add(totals["settlement_shortfall_eth"], new(big.Int).Sub(f, p))
				}
				if match == "unmatched" && (oldest == 0 || created < oldest) {
					oldest = created
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, s := range []string{"settled", "orphaned"} {
			for _, m := range []string{"matched", "unmatched", "ambiguous"} {
				gauge("settlements", float64(counts[s+":"+m]), []string{"status", "match_status"}, s, m)
			}
		}
		for name, n := range totals {
			gauge(name, MetricAmount(n.String()), nil)
		}
		gauge("settlements_underpaid", float64(underpaid), nil)
		if oldest > 0 {
			gauge("settlement_oldest_unmatched_timestamp_seconds", float64(oldest)/1000, nil)
		}
		for _, bucket := range []string{"deposit", "reserve"} {
			total := new(big.Int)
			var minimum *big.Int
			empty := 0
			err = query(`SELECT coalesce(b.balance_units,'0') FROM escrow_snapshots s LEFT JOIN account_balances b ON b.account_type='escrow_`+bucket+`' AND b.currency='eth' AND b.account_id=s.chain_id||':'||s.contract_address||':'||s.sender`, func(r *sql.Rows) error {
				var value string
				if e := r.Scan(&value); e != nil {
					return e
				}
				n, e := signedBalance(value)
				if e != nil {
					return e
				}
				total.Add(total, n)
				if minimum == nil || n.Cmp(minimum) < 0 {
					minimum = new(big.Int).Set(n)
				}
				if n.Sign() == 0 {
					empty++
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			if minimum != nil {
				gauge("escrow_balance_eth", MetricAmount(total.String()), []string{"bucket"}, bucket)
				gauge("escrow_min_balance_eth", MetricAmount(minimum.String()), []string{"bucket"}, bucket)
				gauge("escrow_unfunded_signers", float64(empty), []string{"bucket"}, bucket)
			}
		}
		var start sql.NullInt64
		if err = tx.QueryRowContext(ctx, `SELECT min(block_number)+1 FROM escrow_snapshots`).Scan(&start); err != nil {
			return nil, err
		}
		if start.Valid {
			gauge("chain_start_block", float64(start.Int64), nil)
		}
	}
	return out, tx.Commit()
}
