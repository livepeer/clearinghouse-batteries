package app

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/internal/units"
	"github.com/stretchr/testify/require"
)

func TestUSDManagementInputsAndOutput(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(context.Background(), f.DB, testRegistry(t))
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
			require.NoError(t, f.DB.Ingest(ctx, "usage", 0, 1, raw)) // Identical replay has an empty fee.
			envelope["id"] = "quarantined"
			envelope["data"].(map[string]any)["computed_fee"] = "invalid"
			raw, err = json.Marshal(envelope)
			require.NoError(t, err)
			require.NoError(t, f.DB.Ingest(ctx, "usage", 0, 2, raw))
			response := managementRequest(t, managementHandler(ctx, f.DB, testRegistry(t)), "GET", "/v1/usage", "", "")
			rows, next := managementPage(t, response, http.StatusOK)
			require.Len(t, rows, 3)
			require.Empty(t, next)
			items, err := json.Marshal(rows)
			require.NoError(t, err)
			require.JSONEq(t, string(items), cli(t, "usage", "list"))
			for i, want := range []map[string]any{
				{"status": "applied", "computed_fee_eth": "0.00000000000000001", "computed_fee_usd": "0.25", "currency": currency},
				{"status": "duplicate", "computed_fee_eth": "", "computed_fee_usd": nil, "currency": nil},
				{"status": "quarantined", "computed_fee_eth": "[error]", "computed_fee_usd": "0.25", "currency": nil},
			} {
				require.Subset(t, rows[i], want)
			}
		})
	}
}

func TestAllocationBalanceReads(t *testing.T) {
	for _, currency := range []string{"usd", "eth"} {
		t.Run(currency, func(t *testing.T) {
			f := testutil.NewCurrency(t, "1000000000000000001", currency)
			t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
			ctx := t.Context()
			handler := managementHandler(ctx, f.DB, managementRegistry(t, []string{"allocations.read"}))
			zero, err := f.DB.Create(ctx, "allocation", store.Create{Name: "empty", GrantID: f.Grant, Amount: "0"})
			require.NoError(t, err)

			check := func(id, allocated, available, spent, status string) {
				t.Helper()
				row := managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+id, "", "", "limited-secret"), http.StatusOK)
				require.Equal(t, id, row["id"])
				require.Equal(t, f.Grant, row["grant_id"])
				require.Equal(t, currency, row["currency"])
				require.Equal(t, status, row["status"])
				require.Equal(t, allocated, row["allocated_"+currency])
				require.Equal(t, available, row["available_"+currency])
				require.Equal(t, spent, row["spent_"+currency])
				for _, key := range []string{"allocated", "available", "spent"} {
					require.NotContains(t, row, key+"_units")
					otherCurrency := "eth"
					if currency == "eth" {
						otherCurrency = "usd"
					}
					require.NotContains(t, row, key+"_"+otherCurrency)
				}

				response := managementRequest(t, handler, "GET", "/v1/allocations", "", "", "limited-secret")
				list, next := managementPage(t, response, http.StatusOK)
				require.Len(t, list, 2)
				require.Empty(t, next)
				var listed map[string]any
				for _, candidate := range list {
					if candidate["id"] == id {
						listed = candidate
					}
					if candidate["id"] == zero {
						require.Equal(t, "0", candidate["available_"+currency])
						require.Equal(t, "0", candidate["spent_"+currency])
					}
				}
				require.Equal(t, row, listed)
				items, err := json.Marshal(list)
				require.NoError(t, err)
				require.JSONEq(t, string(items), cli(t, "allocation", "list"))
				var shown []map[string]any
				require.NoError(t, json.Unmarshal([]byte(cli(t, "allocation", "show", "--id", id)), &shown))
				require.Equal(t, []map[string]any{row}, shown)
			}

			fund := func(amount string) {
				t.Helper()
				value, err := units.DecimalToUnits(amount, strings.ToUpper(currency))
				require.NoError(t, err)
				require.NoError(t, f.DB.FundCurrency(ctx, "grant", f.Grant, value, currency))
				require.NoError(t, f.DB.FundCurrency(ctx, "allocation", f.Allocation, value, currency))
			}
			var offset int64
			spend := func(amount string) {
				t.Helper()
				value, err := units.DecimalToUnits(amount, strings.ToUpper(currency))
				require.NoError(t, err)
				var envelope map[string]any
				require.NoError(t, json.Unmarshal(f.Event(t, store.ID(), value, testutil.PM), &envelope))
				envelope["data"].(map[string]any)["computed_fee_usd"] = amount
				raw, err := json.Marshal(envelope)
				require.NoError(t, err)
				require.NoError(t, f.DB.Ingest(ctx, "usage", 0, offset, raw))
				offset++
			}

			check(zero, "0", "0", "0", "exhausted")
			check(f.Allocation, "1.000000000000000001", "1.000000000000000001", "0", "active")
			fund("0.5")
			check(f.Allocation, "1.500000000000000001", "1.500000000000000001", "0", "active")
			spend("0.4")
			check(f.Allocation, "1.500000000000000001", "1.100000000000000001", "0.4", "active")
			spend("1.100000000000000001")
			check(f.Allocation, "1.500000000000000001", "0", "1.500000000000000001", "exhausted")
			spend("0.25")
			check(f.Allocation, "1.500000000000000001", "-0.25", "1.750000000000000001", "exhausted")
			fund("0.75")
			check(f.Allocation, "2.250000000000000001", "0.5", "1.750000000000000001", "active")
			require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "revoked"))
			check(f.Allocation, "1.750000000000000001", "0", "1.750000000000000001", "revoked")
			spend("0.125")
			check(f.Allocation, "1.750000000000000001", "-0.125", "1.875000000000000001", "revoked")
		})
	}
}
