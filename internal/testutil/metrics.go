package testutil

import (
	"database/sql"
	"math/big"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// AssertMetricSnapshot compares every published database sample with a full-source oracle.
func AssertMetricSnapshot(t testing.TB, db *store.Store) {
	t.Helper()
	got, err := store.MetricSnapshot(t.Context(), db.DB, true, true)
	require.NoError(t, err)
	want := ReferenceMetricSnapshot(t, db.DB)
	samples := func(items []prometheus.Metric) []string {
		m := metrics.New("test", "test")
		m.Snapshot("db", items)
		families, err := m.Registry.Gather()
		require.NoError(t, err)
		var out []string
		for _, family := range families {
			if strings.HasPrefix(family.GetName(), metrics.Prefix) {
				out = append(out, family.String())
			}
		}
		return out
	}
	require.Equal(t, samples(want), samples(got))
}

// MetricValue requires a matching sample; unrelated runtime metrics cannot satisfy assertions.
func MetricValue(t testing.TB, m *metrics.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	value, found := findMetric(t, m, name, labels)
	if !found {
		t.Fatalf("missing metric %s with labels %v", name, labels)
	}
	return value
}
func HasMetric(t testing.TB, m *metrics.Metrics, name string, selectors ...map[string]string) bool {
	t.Helper()
	_, found := findMetric(t, m, name, selectors...)
	return found
}
func findMetric(t testing.TB, m *metrics.Metrics, name string, selectors ...map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != metrics.Prefix+name {
			continue
		}
	samples:
		for _, sample := range family.Metric {
			for _, selector := range selectors {
				for key, value := range selector {
					found := false
					for _, label := range sample.Label {
						if label.GetName() == key && label.GetValue() == value {
							found = true
							break
						}
					}
					if !found {
						continue samples
					}
				}
			}
			if sample.Counter != nil {
				return sample.Counter.GetValue(), true
			}
			return sample.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// ReferenceMetricSnapshot independently calculates all database gauges from source tables.
func ReferenceMetricSnapshot(t testing.TB, db *sql.DB) (out []prometheus.Metric) {
	t.Helper()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()
	gauge := func(name string, n float64, labels []string, values ...string) {
		out = append(out, metrics.Gauge(name, n, labels, values...))
	}
	query := func(q string, visit func(*sql.Rows)) {
		t.Helper()
		rows, err := tx.QueryContext(ctx, q)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			visit(rows)
		}
		require.NoError(t, rows.Err())
	}
	amounts := map[string]*big.Int{}
	overdrawn := map[string]int{}
	maxima := map[string]*big.Int{}
	query(`SELECT b.account_type,b.currency,b.balance_units FROM account_balances b LEFT JOIN grant_allocations a ON a.id=b.account_id WHERE b.account_type='grant_unallocated' OR (b.account_type IN ('allocation_available','allocation_spent') AND a.currency=b.currency)`, func(r *sql.Rows) {
		var kind, currency, value string
		require.NoError(t, r.Scan(&kind, &currency, &value))
		n := referenceBalance(t, value)
		key := kind + "_" + currency
		if amounts[key] == nil {
			amounts[key] = new(big.Int)
		}
		amounts[key].Add(amounts[key], n)
		if kind == "allocation_available" && n.Sign() < 0 {
			overdrawn[currency]++
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
	})
	for _, currency := range []string{"eth", "usd"} {
		gauge("allocations_overdrawn", float64(overdrawn[currency]), []string{"currency"}, currency)
		for _, kind := range []string{"grant_unallocated", "allocation_available", "allocation_spent", "allocation_overdraw"} {
			key := kind + "_" + currency
			n := amounts[key]
			if n == nil {
				n = new(big.Int)
			}
			gauge(key, store.MetricAmount(n.String()), nil)
		}
		n := maxima[currency]
		if n == nil {
			n = new(big.Int)
		}
		gauge("allocation_largest_overdraw_"+currency, store.MetricAmount(n.String()), nil)
	}
	quarantines := map[string]int64{}
	query(`SELECT error,count(*) FROM usage_events WHERE status='quarantined' GROUP BY error`, func(r *sql.Rows) {
		var reason string
		var n int64
		require.NoError(t, r.Scan(&reason, &n))
		quarantines[store.QuarantineReason(reason)] += n
	})
	for reason, n := range quarantines {
		gauge("quarantined_events", float64(n), []string{"reason"}, reason)
	}
	for _, item := range []struct{ table, name string }{{"grants", "grants"}, {"grant_allocations", "allocations"}} {
		query("SELECT currency,status,count(*) FROM "+item.table+" GROUP BY currency,status", func(r *sql.Rows) {
			var c, s string
			var n int64
			require.NoError(t, r.Scan(&c, &s, &n))
			gauge(item.name, float64(n), []string{"currency", "status"}, c, s)
		})
	}
	totals := map[string]*big.Int{"settlement_face_value_eth": new(big.Int), "settlement_paid_eth": new(big.Int), "settlement_shortfall_eth": new(big.Int)}
	counts := map[string]int{}
	oldest := int64(0)
	underpaid := 0
	query(`SELECT status,match_status,face_value_wei,paid_amount_wei,created_at_ms FROM settlements`, func(r *sql.Rows) {
		var status, match, face, paid string
		var created int64
		require.NoError(t, r.Scan(&status, &match, &face, &paid, &created))
		counts[status+":"+match]++
		if status == "settled" {
			f, e := store.Amount(face)
			require.NoError(t, e)
			p, e := store.Amount(paid)
			require.NoError(t, e)
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
	})
	for _, s := range []string{"settled", "orphaned"} {
		for _, m := range []string{"matched", "unmatched", "ambiguous"} {
			gauge("settlements", float64(counts[s+":"+m]), []string{"status", "match_status"}, s, m)
		}
	}
	for name, n := range totals {
		gauge(name, store.MetricAmount(n.String()), nil)
	}
	gauge("settlements_underpaid", float64(underpaid), nil)
	if oldest > 0 {
		gauge("settlement_oldest_unmatched_timestamp_seconds", float64(oldest)/1000, nil)
	}
	for _, bucket := range []string{"deposit", "reserve"} {
		total := new(big.Int)
		var minimum *big.Int
		empty := 0
		query(`SELECT coalesce(b.balance_units,'0') FROM escrow_snapshots s LEFT JOIN account_balances b ON b.account_type='escrow_`+bucket+`' AND b.currency='eth' AND b.account_id=s.chain_id||':'||s.contract_address||':'||s.sender`, func(r *sql.Rows) {
			var value string
			require.NoError(t, r.Scan(&value))
			n := referenceBalance(t, value)
			total.Add(total, n)
			if minimum == nil || n.Cmp(minimum) < 0 {
				minimum = new(big.Int).Set(n)
			}
			if n.Sign() == 0 {
				empty++
			}
		})
		if minimum != nil {
			gauge("escrow_balance_eth", store.MetricAmount(total.String()), []string{"bucket"}, bucket)
			gauge("escrow_min_balance_eth", store.MetricAmount(minimum.String()), []string{"bucket"}, bucket)
			gauge("escrow_unfunded_signers", float64(empty), []string{"bucket"}, bucket)
		}
	}
	var start sql.NullInt64
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT min(block_number)+1 FROM escrow_snapshots`).Scan(&start))
	if start.Valid {
		gauge("chain_start_block", float64(start.Int64), nil)
	}
	require.NoError(t, tx.Commit())
	return out
}

func referenceBalance(t testing.TB, value string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok, "invalid balance %q", value)
	require.Equal(t, value, n.String())
	return n
}
