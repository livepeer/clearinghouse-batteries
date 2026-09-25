package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestUSDAllocationChargesExactEventFee(t *testing.T) {
	f := testutil.NewCurrency(t, "1000000000000000000", "")
	ctx := context.Background()
	var currency string
	require.NoError(t, f.DB.DB.QueryRow(`SELECT currency FROM grant_allocations WHERE id=?`, f.Allocation).Scan(&currency))
	require.Equal(t, "usd", currency)
	_, err := f.DB.Create(ctx, "allocation", store.Create{Name: "wrong", GrantID: f.Grant, Amount: "1", Currency: "eth"})
	require.ErrorContains(t, err, "currency")
	require.ErrorContains(t, f.DB.FundCurrency(ctx, "allocation", f.Allocation, "1", "eth"), "currency")
	event := func(id string, fee any) []byte {
		t.Helper()
		var envelope map[string]any
		require.NoError(t, json.Unmarshal(f.Event(t, id, "1000000000000000", testutil.PM), &envelope))
		data := envelope["data"].(map[string]any)
		if fee != nil {
			data["computed_fee_usd"] = fee
		}
		encoded, err := json.Marshal(envelope)
		require.NoError(t, err)
		return encoded
	}
	invalid := []any{nil, json.RawMessage("null"), "", "invalid", "0.0000000000000000001", "-1", "1e0", json.Number("0.25"), true}
	var status string
	for offset, fee := range invalid {
		require.NoError(t, f.DB.Ingest(ctx, "usd", 0, int64(offset), event(store.ID(), fee)))
		require.NoError(t, f.DB.DB.QueryRow(`SELECT status FROM usage_events WHERE topic='usd' AND offset=?`, offset).Scan(&status))
		require.Equal(t, "quarantined", status)
	}
	fee := "0.750000000000000001"
	require.NoError(t, f.DB.Ingest(ctx, "usd", 0, int64(len(invalid)), event("paid-usd", fee)))
	var saved string
	require.NoError(t, f.DB.DB.QueryRow(`SELECT status,computed_fee_usd FROM usage_events WHERE event_id='paid-usd'`).Scan(&status, &saved))
	require.Equal(t, "applied", status)
	require.Equal(t, fee, saved)
	available, err := store.BalanceCurrency(ctx, f.DB.DB, "allocation_available", f.Allocation, "usd")
	require.NoError(t, err)
	require.Equal(t, "249999999999999999", available.String())
	spent, err := store.BalanceCurrency(ctx, f.DB.DB, "allocation_spent", f.Allocation, "usd")
	require.NoError(t, err)
	require.Equal(t, "750000000000000001", spent.String())
	require.NoError(t, f.DB.Ingest(ctx, "usd", 0, int64(len(invalid)+1), event("zero-usd", "0")))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT status,computed_fee_usd FROM usage_events WHERE event_id='zero-usd'`).Scan(&status, &saved))
	require.Equal(t, "applied", status)
	require.Equal(t, "0", saved)
	require.NoError(t, f.DB.Ingest(ctx, "usd", 0, int64(len(invalid)+2), event("overdraw-usd", "0.3")))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT status FROM grant_allocations WHERE id=?`, f.Allocation).Scan(&status))
	require.Equal(t, "exhausted", status)
	decision, err := f.DB.Authorize(ctx, f.Request)
	require.NoError(t, err)
	require.Equal(t, 402, decision.Status)
}
