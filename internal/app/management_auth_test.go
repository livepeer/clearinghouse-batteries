package app

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func managementRegistry(t *testing.T, allow []string) *serviceauth.Registry {
	t.Helper()
	permissions, err := json.Marshal(allow)
	require.NoError(t, err)
	return testRegistry(t, `{"id":"limited","secret":"limited-secret","management":{"allow":`+string(permissions)+`}}`,
		`{"id":"signer","secret":"signer-secret","webhook":{"authorize":true}}`)
}

// This table is independent of route registration, so a wrong permission or a
// missing route fails both the exact-permission and resource-wildcard cases.
func TestEveryManagementRouteRequiresItsPermission(t *testing.T) {
	for _, route := range []struct {
		method, path, permission, body string
		status                         int
	}{
		{"GET", "/v1/grants", "grants.read", "", 200},
		{"GET", "/v1/grants/{grant}", "grants.read", "", 200},
		{"POST", "/v1/grants", "grants.create", `{"name":"empty grant"}`, 201},
		{"POST", "/v1/grants/{grant}/fund", "grants.fund", `{"amount_eth":"0.000000000000000001"}`, 200},
		{"PATCH", "/v1/grants/{grant}/status", "grants.status", `{"status":"paused"}`, 200},
		{"GET", "/v1/allocations", "allocations.read", "", 200},
		{"GET", "/v1/allocations/{allocation}", "allocations.read", "", 200},
		{"POST", "/v1/allocations", "allocations.create", `{"name":"empty allocation","grant_id":"{grant}"}`, 201},
		{"POST", "/v1/allocations/{allocation}/fund", "allocations.fund", `{"amount_eth":"0.000000000000000001"}`, 200},
		{"PATCH", "/v1/allocations/{allocation}/status", "allocations.status", `{"status":"paused"}`, 200},
		{"POST", "/v1/allocations/{allocation}/revoke", "allocations.revoke", "", 200},
		{"GET", "/v1/api-keys", "api_keys.read", "", 200},
		{"POST", "/v1/api-keys", "api_keys.create", `{"name":"key","allocation_id":"{allocation}"}`, 201},
		{"POST", "/v1/api-keys/{key}/revoke", "api_keys.revoke", "", 200},
		{"GET", "/v1/sessions", "sessions.read", "", 200},
		{"GET", "/v1/sessions/{session}", "sessions.read", "", 200},
		{"POST", "/v1/sessions/{session}/revoke", "sessions.revoke", "", 200},
		{"GET", "/v1/settlements", "settlements.read", "", 200},
		{"GET", "/v1/usage", "usage.read", "", 200},
		{"GET", "/v1/ledger/report", "ledger.read", "", 200},
		{"GET", "/v1/escrow/report", "escrow.read", "", 200},
		{"GET", "/v1/escrow/activity", "escrow.read", "", 200},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			resource, _, _ := strings.Cut(route.permission, ".")
			unrelated := "usage.read"
			if route.permission == unrelated {
				unrelated = "grants.read"
			}
			if route.permission == "allocations.status" {
				unrelated = "allocations.revoke"
			} else if route.permission == "allocations.revoke" {
				unrelated = "allocations.status"
			}
			for _, credential := range []struct {
				name, permission, token string
				status                  int
			}{
				{"missing", route.permission, "", 401},
				{"invalid", route.permission, "wrong", 401},
				{"wrong integration", route.permission, "signer-secret", 401},
				{"unrelated", unrelated, "limited-secret", 403},
				{"exact", route.permission, "limited-secret", route.status},
				{"wildcard", resource + ".*", "limited-secret", route.status},
			} {
				t.Run(credential.name, func(t *testing.T) {
					f := testutil.New(t, "100")
					require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "75"))
					replace := strings.NewReplacer("{grant}", f.Grant, "{allocation}", f.Allocation, "{key}", f.KeyID, "{session}", f.Session)
					handler := managementHandler(t.Context(), f.DB, managementRegistry(t, []string{credential.permission}))
					response := managementRequest(t, handler, route.method, replace.Replace(route.path), "application/json", replace.Replace(route.body), credential.token)
					require.Equal(t, credential.status, response.Code, response.Body.String())
					require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
					if route.method == "GET" {
						response = managementRequest(t, handler, "HEAD", replace.Replace(route.path), "", "", credential.token)
						require.Equal(t, credential.status, response.Code)
					}
				})
			}
		})
	}
}

func TestCreationFundingPermissions(t *testing.T) {
	for _, creation := range []struct{ resource, currency, amount, units string }{
		{"grants", "usd", "", "0"}, {"grants", "eth", "", "0"},
		{"grants", "usd", "0.000", "0"}, {"grants", "eth", "0.000", "0"},
		{"grants", "usd", "0.000000000000000001", "1"}, {"grants", "eth", "0.000000000000000001", "1"},
		{"allocations", "usd", "", "0"}, {"allocations", "eth", "", "0"},
		{"allocations", "usd", "0.000", "0"}, {"allocations", "eth", "0.000", "0"},
		{"allocations", "usd", "0.000000000000000001", "1"}, {"allocations", "eth", "0.000000000000000001", "1"},
		{"allocations", "usd", "all", "75"}, {"allocations", "eth", "all", "75"},
	} {
		resource, unrelated := creation.resource, "grants.*"
		if resource == "grants" {
			unrelated = "allocations.*"
		}
		for _, format := range []string{"json", "form", "multipart"} {
			for _, credential := range []struct {
				permissions []string
				canFund     bool
			}{
				{[]string{resource + ".create"}, false},
				{[]string{resource + ".create", unrelated}, false},
				{[]string{resource + ".create", resource + ".fund"}, true},
				{[]string{resource + ".*"}, true},
			} {
				t.Run(strings.Join([]string{resource, creation.currency, format, creation.amount, strings.Join(credential.permissions, ",")}, "/"), func(t *testing.T) {
					f := testutil.NewCurrency(t, "100", creation.currency)
					require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "75"))
					fields := map[string]string{"name": "created"}
					if resource == "allocations" {
						fields["grant_id"] = f.Grant
					}
					if creation.amount != "" {
						fields["amount_"+creation.currency] = creation.amount
					}
					var contentType, body string
					switch format {
					case "json":
						data, err := json.Marshal(fields)
						require.NoError(t, err)
						contentType, body = "application/json", string(data)
					case "form":
						values := url.Values{}
						for key, value := range fields {
							values.Set(key, value)
						}
						contentType, body = "application/x-www-form-urlencoded", values.Encode()
					case "multipart":
						contentType, body = managementForm(t, fields)
					}
					kind := strings.TrimSuffix(resource, "s")
					before, err := f.DB.List(t.Context(), kind, store.ListOptions{})
					require.NoError(t, err)
					balances, err := f.DB.Report(t.Context())
					require.NoError(t, err)
					handler := managementHandler(t.Context(), f.DB, managementRegistry(t, credential.permissions))
					response := managementRequest(t, handler, "POST", "/v1/"+resource, contentType, body, "limited-secret")
					if !credential.canFund && creation.units != "0" {
						require.Equal(t, http.StatusForbidden, response.Code)
						after, err := f.DB.List(t.Context(), kind, store.ListOptions{})
						require.NoError(t, err)
						require.Equal(t, before, after)
						afterBalances, err := f.DB.Report(t.Context())
						require.NoError(t, err)
						require.Equal(t, balances, afterBalances)
						return
					}
					result := managementObject(t, response, http.StatusCreated)
					rows, err := f.DB.List(t.Context(), kind, store.ListOptions{ID: result["id"].(string)})
					require.NoError(t, err)
					column := "total_units"
					if resource == "allocations" {
						column = "allocated_units"
					}
					require.Equal(t, creation.units, rows[0][column])
				})
			}
		}
	}
}

func TestAPIKeyGrantShortcutNeedsAllocationPermissions(t *testing.T) {
	for _, tc := range []struct {
		permissions  []string
		status, rows int
	}{
		{[]string{"api_keys.*"}, 403, 1},
		{[]string{"api_keys.create", "allocations.create"}, 403, 1},
		{[]string{"api_keys.create", "allocations.fund"}, 403, 1},
		{[]string{"allocations.*"}, 403, 1},
		{[]string{"api_keys.create", "allocations.create", "allocations.fund"}, 201, 2},
		{[]string{"api_keys.*", "allocations.*"}, 201, 2},
	} {
		t.Run(strings.Join(tc.permissions, ","), func(t *testing.T) {
			f := testutil.New(t, "100")
			handler := managementHandler(t.Context(), f.DB, managementRegistry(t, tc.permissions))
			body := `{"grant_id":"` + f.Grant + `","name":"shortcut","amount_eth":"0"}`
			response := managementRequest(t, handler, "POST", "/v1/api-keys", "application/json", body, "limited-secret")
			var allocations, keys int
			require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grant_allocations`).Scan(&allocations))
			require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM api_keys`).Scan(&keys))
			require.Equal(t, tc.status, response.Code, response.Body.String())
			require.Equal(t, tc.rows, allocations)
			require.Equal(t, tc.rows, keys)
		})
	}
}
