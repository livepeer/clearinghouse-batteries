package store_test

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/internal/units"
	"github.com/livepeer/clearinghouse/migrations"
	"github.com/stretchr/testify/require"
)

func metricSummaryState(t testing.TB, db *store.Store) map[string][]map[string]any {
	t.Helper()
	out := map[string][]map[string]any{}
	for _, query := range []struct{ table, suffix string }{
		{"metric_amounts", "ORDER BY name,currency"},
		{"metric_counts", "WHERE value>0 ORDER BY kind,label1,label2"},
		{"metric_balances", "ORDER BY account_type,source_id,currency"},
	} {
		rows, err := db.Rows(t.Context(), "SELECT * FROM "+query.table+" "+query.suffix)
		require.NoError(t, err)
		out[query.table] = rows
	}
	return out
}

func assertMetricSummaryRebuild(t testing.TB, db *store.Store) {
	t.Helper()
	before := metricSummaryState(t, db)
	require.NoError(t, migrations.Down(t.Context(), db.DB))
	require.NoError(t, migrations.Up(t.Context(), db.DB))
	require.Equal(t, before, metricSummaryState(t, db))
	testutil.AssertMetricSnapshot(t, db)
}

func TestFinancialSnapshotDoesNotNetAwayOverdraw(t *testing.T) {
	for _, currency := range []string{"eth", "usd"} {
		t.Run(currency, func(t *testing.T) {
			f := testutil.NewCurrency(t, "1000000000000000000", currency)
			ctx := t.Context()
			require.NoError(t, f.DB.FundCurrency(ctx, "grant", f.Grant, "9000000000000000000", currency))
			_, err := f.DB.Create(ctx, "allocation", store.Create{GrantID: f.Grant, Name: "positive", Amount: "9000000000000000000", Currency: currency})
			require.NoError(t, err)
			raw := f.Event(t, "overdraw", "2000000000000000000", testutil.PM)
			if currency == "usd" {
				var event map[string]any
				require.NoError(t, json.Unmarshal(raw, &event))
				event["data"].(map[string]any)["computed_fee_usd"] = "2"
				raw, err = json.Marshal(event)
				require.NoError(t, err)
			}
			require.NoError(t, f.DB.Ingest(ctx, "events", 0, 0, raw))
			snapshot, err := store.MetricSnapshot(ctx, f.DB.DB, true, false)
			require.NoError(t, err)
			m := metrics.New("test", "commit")
			m.Snapshot("db", snapshot)
			require.Equal(t, 1., testutil.MetricValue(t, m, "allocation_overdraw_"+currency, nil))
		})
	}
}

func TestChargedCountersAccumulateExactUnits(t *testing.T) {
	for _, currency := range []string{"eth", "usd"} {
		t.Run(currency, func(t *testing.T) {
			unit := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
			large := new(big.Int).Mul(new(big.Int).Lsh(big.NewInt(1), 53), unit)
			budget := new(big.Int).Add(new(big.Int).Set(large), new(big.Int).Mul(big.NewInt(1000), unit))
			f := testutil.NewCurrency(t, budget.String(), currency)
			m := metrics.New("test", "commit")
			f.DB.Metrics = m
			event := func(id string, fee *big.Int) []byte {
				raw := f.Event(t, id, fee.String(), testutil.PM)
				if currency == "usd" {
					var ev map[string]any
					require.NoError(t, json.Unmarshal(raw, &ev))
					decimal, e := units.UnitsToDecimal(fee.String())
					require.NoError(t, e)
					ev["data"].(map[string]any)["computed_fee_usd"] = decimal
					raw, e = json.Marshal(ev)
					require.NoError(t, e)
				}
				return raw
			}
			raw := event("large", large)
			_, err := f.DB.DB.Exec(`CREATE TRIGGER rollback_usage BEFORE INSERT ON ingestion_checkpoints BEGIN SELECT RAISE(ABORT,'failure'); END`)
			require.NoError(t, err)
			require.Error(t, f.DB.Ingest(t.Context(), "events", 0, 0, raw))
			for _, name := range []string{"accounting_charged_" + currency + "_total", "usage_events_total", "accounting_tickets_total"} {
				require.False(t, testutil.HasMetric(t, m, name))
			}
			_, err = f.DB.DB.Exec(`DROP TRIGGER rollback_usage`)
			require.NoError(t, err)
			require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, 0, raw))
			require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, 0, raw))
			require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, 1, raw))
			require.Equal(t, 1., testutil.MetricValue(t, m, "accounting_tickets_total", nil))
			for _, outcome := range []string{"applied", "duplicate"} {
				require.Equal(t, 1., testutil.MetricValue(t, m, "usage_events_total", map[string]string{"topic": "events", "partition": "0", "outcome": outcome, "reason": "none"}))
			}
			for i := 0; i < 100; i++ {
				require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, int64(i+2), event(fmt.Sprint(i), unit)))
			}
			want, _ := new(big.Rat).SetFrac(new(big.Int).Add(large, new(big.Int).Mul(big.NewInt(100), unit)), unit).Float64()
			require.Equal(t, want, testutil.MetricValue(t, m, "accounting_charged_"+currency+"_total", nil))
			require.Equal(t, 101., testutil.MetricValue(t, m, "accounting_tickets_total", nil))
		})
	}
}

func TestMetricSummaryLifecycle(t *testing.T) {
	for _, currency := range []string{"eth", "usd"} {
		t.Run(currency, func(t *testing.T) {
			f := testutil.NewCurrency(t, "100", currency)
			ctx := t.Context()
			check := func(err error) {
				t.Helper()
				require.NoError(t, err)
				testutil.AssertMetricSnapshot(t, f.DB)
			}
			event := func(id, amount string) []byte {
				raw := f.Event(t, id, amount, testutil.PM)
				if currency == "usd" {
					var ev map[string]any
					require.NoError(t, json.Unmarshal(raw, &ev))
					decimal, err := units.UnitsToDecimal(amount)
					require.NoError(t, err)
					ev["data"].(map[string]any)["computed_fee_usd"] = decimal
					raw, err = json.Marshal(ev)
					require.NoError(t, err)
				}
				return raw
			}
			check(f.DB.Fund(ctx, "grant", f.Grant, "500"))
			other, err := f.DB.Create(ctx, "allocation", store.Create{Name: "other", GrantID: f.Grant, Amount: "200"})
			check(err)
			raw := event("overdraw", "150")
			check(f.DB.Ingest(ctx, "events", 0, 0, raw))
			check(f.DB.Ingest(ctx, "events", 0, 0, raw))
			check(f.DB.Ingest(ctx, "events", 0, 1, raw))
			check(f.DB.Fund(ctx, "allocation", f.Allocation, "100"))
			for _, status := range []string{"paused", "active", "revoked", "revoked"} {
				check(f.DB.SetStatus(ctx, "allocation", f.Allocation, status))
			}
			check(f.DB.Ingest(ctx, "events", 0, 2, event("late", "100")))
			check(f.DB.SetStatus(ctx, "allocation", other, "revoked"))
			for _, status := range []string{"paused", "active", "closed"} {
				check(f.DB.SetStatus(ctx, "grant", f.Grant, status))
			}
			check(f.DB.Ingest(ctx, "events", 0, 3, []byte("{bad")))
			check(f.DB.Ingest(ctx, "events", 0, 3, []byte("{bad")))
			check(f.DB.Ingest(ctx, "events", 0, 4, []byte(`{"id":"ignored","type":"other","data":{}}`)))
			assertMetricSummaryRebuild(t, f.DB)
			reopened, err := store.Open(ctx, f.Path, false)
			require.NoError(t, err)
			defer reopened.Close()
			testutil.AssertMetricSnapshot(t, reopened)
		})
	}
}
