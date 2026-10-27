package store_test

import (
	"encoding/json"
	"fmt"
	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/internal/units"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

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
