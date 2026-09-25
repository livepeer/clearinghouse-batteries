package app

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestUSDManagementInputsAndOutput(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(context.Background(), f.DB)
	grant := managementObject(t, managementRequest(t, handler, "POST", "/v1/grants", "application/json", `{"name":"USD grant","amount_usd":"2","status":"active"}`), http.StatusCreated)
	grantID := grant["id"].(string)
	allocation := managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations", "application/json", `{"name":"USD allocation","grant_id":"`+grantID+`","amount_usd":"1.25"}`), http.StatusCreated)
	allocationID := allocation["id"].(string)
	row := managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+allocationID, "", ""), http.StatusOK)
	require.Equal(t, "usd", row["currency"])
	require.Equal(t, "1.25", row["allocated_usd"])
	require.NotContains(t, row, "allocated_eth")
	managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations/"+allocationID+"/fund", "application/json", `{"amount_usd":"all"}`), http.StatusOK)
	row = managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+allocationID, "", ""), http.StatusOK)
	require.Equal(t, "2", row["allocated_usd"])
	managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations/"+allocationID+"/fund", "application/json", `{"amount_eth":"1"}`), http.StatusBadRequest)
	managementObject(t, managementRequest(t, handler, "POST", "/v1/grants", "application/json", `{"name":"bad","amount_usd":"1","amount_eth":"1"}`), http.StatusBadRequest)
	managementObject(t, managementRequest(t, handler, "POST", "/v1/grants", "application/json", `{"name":"bad","amount_usd":"0.0000000000000000001"}`), http.StatusBadRequest)
	keyGrant := managementObject(t, managementRequest(t, handler, "POST", "/v1/grants", "application/json", `{"name":"key grant","amount_usd":"1"}`), http.StatusCreated)["id"].(string)
	key := managementObject(t, managementRequest(t, handler, "POST", "/v1/api-keys", "application/json", `{"name":"USD key","grant_id":"`+keyGrant+`","amount_usd":"0.5"}`), http.StatusCreated)
	keyAllocation := managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+key["allocation_id"].(string), "", ""), http.StatusOK)
	require.Equal(t, "0.5", keyAllocation["allocated_usd"])
	ethDefault := managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations", "application/json", `{"name":"inherited ETH","grant_id":"`+f.Grant+`"}`), http.StatusCreated)
	ethRow := managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+ethDefault["id"].(string), "", ""), http.StatusOK)
	require.Equal(t, "eth", ethRow["currency"])
	require.Equal(t, "0", ethRow["allocated_eth"])
}

func TestUSDCLIFlagsAndDefault(t *testing.T) {
	t.Setenv("CLEARINGHOUSE_DB_PATH", filepath.Join(t.TempDir(), "accounts.db"))
	cli(t, "migrate", "up")
	grant := object(t, cli(t, "grant", "create", "--name", "USD grant", "--amount-usd", "2", "--status", "active"))["id"]
	allocation := object(t, cli(t, "allocation", "create", "--name", "USD allocation", "--grant-id", grant, "--amount-usd", "1"))["id"]
	cli(t, "allocation", "fund", "--id", allocation, "--amount-usd", "0.5")
	require.Contains(t, cli(t, "allocation", "show", "--id", allocation), `"allocated_usd": "1.5"`)
	require.Contains(t, cli(t, "ledger", "report"), `"balance_usd": "1.5"`)
	defaultGrant := object(t, cli(t, "grant", "create", "--name", "zero default"))["id"]
	require.Contains(t, cli(t, "grant", "show", "--id", defaultGrant), `"currency": "usd"`)
}

func TestUsageCurrencyOutput(t *testing.T) {
	for _, currency := range []string{"usd", "eth"} {
		t.Run(currency, func(t *testing.T) {
			f := testutil.NewCurrency(t, "1000000000000000000", currency)
			t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
			ctx := context.Background()
			var envelope map[string]any
			require.NoError(t, json.Unmarshal(f.Event(t, "paid", "10", testutil.PM), &envelope))
			envelope["data"].(map[string]any)["computed_fee_usd"] = "0.2500"
			raw, err := json.Marshal(envelope)
			require.NoError(t, err)
			require.NoError(t, f.DB.Ingest(ctx, "usage", 0, 0, raw))
			envelope["id"] = "quarantined"
			envelope["data"].(map[string]any)["computed_fee_usd"] = "-1"
			raw, err = json.Marshal(envelope)
			require.NoError(t, err)
			require.NoError(t, f.DB.Ingest(ctx, "usage", 0, 1, raw))
			response := managementRequest(t, managementHandler(ctx, f.DB), "GET", "/v1/usage", "", "")
			rows := managementArray(t, response, http.StatusOK)
			require.Len(t, rows, 2)
			require.JSONEq(t, response.Body.String(), cli(t, "usage", "list"))
			for _, item := range rows {
				row := item.(map[string]any)
				if row["event_id"] == "paid" {
					require.Equal(t, currency, row["currency"])
					require.Equal(t, "0.00000000000000001", row["computed_fee_eth"])
					require.Equal(t, "0.25", row["computed_fee_usd"])
				} else {
					require.Equal(t, "quarantined", row["status"])
					require.Nil(t, row["currency"])
				}
			}
		})
	}
}
