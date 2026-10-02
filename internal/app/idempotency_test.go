package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func keyedManagementRequest(t *testing.T, handler http.Handler, path, contentType, body string, keys []string, tokens ...string) *httptest.ResponseRecorder {
	t.Helper()
	return managementRequest(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header["Idempotency-Key"] = keys
		handler.ServeHTTP(w, r)
	}), "POST", path, contentType, body, tokens...)
}

func idempotencyJSON(t *testing.T, fields map[string]string) string {
	t.Helper()
	body, err := json.Marshal(fields)
	require.NoError(t, err)
	return string(body)
}

func TestManagementIdempotencyReplay(t *testing.T) {
	for _, currency := range []string{"usd", "eth"} {
		for _, tc := range []struct {
			name, path, body string
			status           int
			permissions      []string
		}{
			{"allocation create", "/v1/allocations", `{"grant_id":"{grant}","name":"created","amount_{currency}":"1"}`, 201, []string{"allocations.create", "allocations.fund"}},
			{"allocation create all", "/v1/allocations", `{"grant_id":"{grant}","name":"created","amount_{currency}":"all"}`, 201, []string{"allocations.create", "allocations.fund"}},
			{"grant fund", "/v1/grants/{grant}/fund", `{"amount_{currency}":"1"}`, 200, []string{"grants.fund"}},
			{"allocation fund", "/v1/allocations/{allocation}/fund", `{"amount_{currency}":"1"}`, 200, []string{"allocations.fund"}},
			{"allocation fund all", "/v1/allocations/{allocation}/fund", `{"amount_{currency}":"all"}`, 200, []string{"allocations.fund"}},
			{"allocation API key", "/v1/api-keys", `{"allocation_id":"{allocation}","name":"key"}`, 201, []string{"api_keys.create"}},
			{"grant API key", "/v1/api-keys", `{"grant_id":"{grant}","name":"key","amount_{currency}":"1"}`, 201, []string{"api_keys.create", "allocations.create", "allocations.fund"}},
			{"grant API key all", "/v1/api-keys", `{"grant_id":"{grant}","name":"key","amount_{currency}":"all"}`, 201, []string{"api_keys.create", "allocations.create", "allocations.fund"}},
		} {
			t.Run(currency+"/"+tc.name, func(t *testing.T) {
				f := testutil.NewCurrency(t, "100000000000000000000", currency)
				require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "100000000000000000000"))
				replace := strings.NewReplacer("{grant}", f.Grant, "{allocation}", f.Allocation, "{currency}", currency)
				path, body := replace.Replace(tc.path), replace.Replace(tc.body)
				handler := managementHandler(t.Context(), f.DB, testRegistry(t))
				first := keyedManagementRequest(t, handler, path, "application/json", body, []string{"request"})
				created := managementObject(t, first, tc.status)
				if currency == "eth" && !strings.Contains(body, `"all"`) {
					before := f.ManagementState(t)
					require.Equal(t, 401, keyedManagementRequest(t, handler, path, "application/json", body, []string{"request"}, "").Code)
					for missing := range tc.permissions {
						permissions := append([]string{"usage.read"}, tc.permissions[:missing]...)
						permissions = append(permissions, tc.permissions[missing+1:]...)
						limited := managementHandler(t.Context(), f.DB, managementRegistry(t, permissions))
						require.Equal(t, 403, keyedManagementRequest(t, limited, path, "application/json", body, []string{"request"}, "limited-secret").Code)
					}
					require.Equal(t, before, f.ManagementState(t))
				}
				// Replay must survive funding, closure, and revocation without undoing them.
				require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "7"))
				for _, allocation := range f.ManagementState(t)["grant_allocations"] {
					require.NoError(t, f.DB.SetStatus(t.Context(), "allocation", allocation["id"].(string), "revoked"))
				}
				if path == "/v1/api-keys" {
					require.Contains(t, created["api_key"], "lpg_")
					require.NoError(t, f.DB.SetStatus(t.Context(), "api-key", created["id"].(string), "revoked"))
				}
				require.NoError(t, f.DB.SetStatus(t.Context(), "grant", f.Grant, "closed"))
				before := f.ManagementState(t)
				limited := managementHandler(t.Context(), f.DB, managementRegistry(t, tc.permissions))
				replay := keyedManagementRequest(t, limited, path, "application/json", body, []string{"request"}, "limited-secret")
				managementObject(t, replay, tc.status)
				require.Equal(t, first.Body.String(), replay.Body.String())
				require.Equal(t, before, f.ManagementState(t))
			})
		}
	}
}

func TestManagementIdempotencyGrantNamespaceAndExactFields(t *testing.T) {
	f := testutil.New(t, "100000000000000000000")
	require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "100000000000000000000"))
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	post := func(path, body, key string, status int) map[string]any {
		return managementObject(t, keyedManagementRequest(t, handler, path, "application/json", body, []string{key}), status)
	}
	fields := map[string]string{"grant_id": f.Grant, "name": "created", "amount_eth": "1"}
	created := post("/v1/allocations", fmt.Sprintf(`{"name":"created","grant_id":%q,"amount_eth":"1"}`, f.Grant), "shared", 201)
	before := f.ManagementState(t)
	values := url.Values{}
	for key, value := range fields {
		values.Set(key, value)
	}
	multipartType, multipartBody := managementForm(t, fields)
	for _, format := range []struct{ contentType, body string }{
		{"application/json", strings.Replace(idempotencyJSON(t, fields), `"1"`, `"\u0031"`, 1)},
		{"application/x-www-form-urlencoded", values.Encode()},
		{multipartType, multipartBody},
	} {
		replay := keyedManagementRequest(t, handler, "/v1/allocations", format.contentType, format.body, []string{"shared"})
		require.Equal(t, created, managementObject(t, replay, 201))
	}
	for _, changed := range []map[string]string{
		{"grant_id": f.Grant, "name": "other", "amount_eth": "1"},
		{"grant_id": f.Grant, "name": "created", "amount_eth": "1.0"},
		{"grant_id": f.Grant, "name": "created", "amount_eth": "1", "metadata": ""},
		{"grant_id": f.Grant, "name": "created", "amount_eth": "1", "status": "active"},
		{"grant_id": f.Grant, "name": "created", "amount_usd": "1"},
	} {
		post("/v1/allocations", idempotencyJSON(t, changed), "shared", 409)
	}
	// Identical fields and target, but a different operation.
	post("/v1/api-keys", idempotencyJSON(t, fields), "shared", 409)
	require.Equal(t, before, f.ManagementState(t))
	fundPath, fundBody := "/v1/allocations/"+f.Allocation+"/fund", `{"amount_eth":"1"}`
	post(fundPath, fundBody, "fund", 200)
	before = f.ManagementState(t)
	post("/v1/grants/"+f.Grant+"/fund", fundBody, "fund", 409)
	post("/v1/allocations/"+created["id"].(string)+"/fund", fundBody, "fund", 409)
	post(fundPath, `{"amount_eth":"2"}`, "fund", 409)
	require.Equal(t, before, f.ManagementState(t))
	other, err := f.DB.Create(t.Context(), "grant", store.Create{Name: "other", Amount: "100000000000000000000", Currency: "eth"})
	require.NoError(t, err)
	fields["grant_id"] = other
	require.NotEqual(t, created["id"], post("/v1/allocations", idempotencyJSON(t, fields), "shared", 201)["id"])
	require.Len(t, f.ManagementState(t)["management_idempotency"], 3)
}

func TestManagementIdempotencyHeaderAndOptionalBehavior(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	path, body := "/v1/grants/"+f.Grant+"/fund", `{"amount_eth":"0.000000000000000001"}`
	before := f.ManagementState(t)
	for _, keys := range [][]string{
		{""}, {" \t"}, {"a b"}, {"a\tb"}, {"a\x00b"}, {"é"}, {"a+"}, {"a/"}, {"a."},
		{"a", "b"}, {"a", "a"}, {strings.Repeat("x", 257)},
	} {
		managementObject(t, keyedManagementRequest(t, handler, path, "application/json", body, keys), 400)
	}
	require.Equal(t, before, f.ManagementState(t))
	for _, key := range []string{"a", strings.Repeat("x", 256), "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-", "Key", "key", "YQ=="} {
		managementObject(t, keyedManagementRequest(t, handler, path, "application/json", body, []string{key}), 200)
	}
	for range 2 {
		managementObject(t, keyedManagementRequest(t, handler, path, "application/json", body, nil), 200)
	}
	after := f.ManagementState(t)
	require.Len(t, after["ledger_transactions"], len(before["ledger_transactions"])+8)
	require.Len(t, after["management_idempotency"], 6)
}

func TestManagementIdempotencyFailuresLeaveKeyAvailable(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	for i, tc := range []struct {
		contentType, body string
		status            int
	}{
		{"application/json", fmt.Sprintf(`{"grant_id":%q,"name":"created","amount_eth":"0.000000000000000001"}`, f.Grant), 409},
		{"application/json", fmt.Sprintf(`{"grant_id":%q,"name":""}`, f.Grant), 400},
		{"application/json", `{"grant_id":"missing","name":"created"}`, 404},
		{"application/x-www-form-urlencoded", "grant_id=" + f.Grant + "&name=%FF", 400},
	} {
		key := []string{fmt.Sprint(i)}
		managementObject(t, keyedManagementRequest(t, handler, "/v1/allocations", tc.contentType, tc.body, key), tc.status)
		body := fmt.Sprintf(`{"grant_id":%q,"name":"corrected"}`, f.Grant)
		managementObject(t, keyedManagementRequest(t, handler, "/v1/allocations", "application/json", body, key), 201)
	}
	require.Len(t, f.ManagementState(t)["management_idempotency"], 4)
}
