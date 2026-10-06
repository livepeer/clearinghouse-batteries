package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func managementRequest(t *testing.T, handler http.Handler, method, path, contentType, body string, tokens ...string) *httptest.ResponseRecorder {
	t.Helper()
	token := testOperatorToken
	if len(tokens) > 0 {
		token = tokens[0]
	}
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		r.Header.Set(serviceauth.Header, token)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func managementObject(t *testing.T, response *httptest.ResponseRecorder, status int) map[string]any {
	t.Helper()
	require.Equal(t, status, response.Code, response.Body.String())
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	var value map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &value))
	return value
}

func managementForm(t *testing.T, fields map[string]string) (string, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range fields {
		require.NoError(t, writer.WriteField(key, value))
	}
	require.NoError(t, writer.Close())
	return writer.FormDataContentType(), body.String()
}

func TestManagementRoutes(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(context.Background(), f.DB, testRegistry(t))
	grant := managementObject(t, managementRequest(t, handler, "POST", "/v1/grants", "application/json", `{"name":"HTTP grant","amount_eth":"1.000000000000000001","status":"active","metadata":"not JSON: {bad}"}`), 201)
	grantID := grant["id"].(string)
	row := managementObject(t, managementRequest(t, handler, "GET", "/v1/grants/"+grantID, "", ""), 200)
	require.Equal(t, "1.000000000000000001", row["total_eth"])
	require.Equal(t, "not JSON: {bad}", row["metadata"])
	require.NotContains(t, row, "total_wei")
	require.Len(t, managementArray(t, managementRequest(t, handler, "GET", "/v1/grants", "", ""), 200), 2)

	contentType, form := managementForm(t, map[string]string{"grant_id": grantID, "name": "HTTP allocation", "amount_eth": "0.5", "metadata": "opaque allocation\n{bad"})
	allocation := managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations", contentType, form), 201)
	allocationID := allocation["id"].(string)
	row = managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+allocationID, "", ""), 200)
	require.Equal(t, "0.5", row["allocated_eth"])
	require.Equal(t, "opaque allocation\n{bad", row["metadata"])
	require.Len(t, managementArray(t, managementRequest(t, handler, "GET", "/v1/allocations", "", ""), 200), 2)
	require.Equal(t, "", managementObject(t, managementRequest(t, handler, "GET", "/v1/grants/"+f.Grant, "", ""), 200)["metadata"])
	require.Equal(t, "", managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+f.Allocation, "", ""), 200)["metadata"])

	fund := url.Values{"amount_eth": {"0.5"}}.Encode()
	managementObject(t, managementRequest(t, handler, "POST", "/v1/grants/"+grantID+"/fund", "application/x-www-form-urlencoded", fund), 200)
	managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations/"+allocationID+"/fund", "application/json", `{"amount_eth":"all"}`), 200)
	row = managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+allocationID, "", ""), 200)
	require.Equal(t, "1.500000000000000001", row["allocated_eth"])
	require.Equal(t, "paused", managementObject(t, managementRequest(t, handler, "PATCH", "/v1/grants/"+grantID+"/status", "application/json", `{"status":"paused"}`), 200)["status"])
	require.Equal(t, "paused", managementObject(t, managementRequest(t, handler, "PATCH", "/v1/allocations/"+allocationID+"/status", "application/x-www-form-urlencoded", "status=paused"), 200)["status"])
	require.Equal(t, "active", managementObject(t, managementRequest(t, handler, "PATCH", "/v1/allocations/"+allocationID+"/status", "application/json", `{"status":"active"}`), 200)["status"])

	createdKey := managementObject(t, managementRequest(t, handler, "POST", "/v1/api-keys", "application/json", fmt.Sprintf(`{"allocation_id":%q,"name":"HTTP key"}`, allocationID)), 201)
	secret := createdKey["api_key"].(string)
	require.True(t, strings.HasPrefix(secret, "lpg_"))
	keyList := managementRequest(t, handler, "GET", "/v1/api-keys", "", "")
	require.Equal(t, 200, keyList.Code)
	require.NotContains(t, keyList.Body.String(), secret)
	require.NotContains(t, keyList.Body.String(), "secret_hash")

	contentType, form = managementForm(t, map[string]string{"grant_id": grantID, "name": "Empty key", "amount_eth": "all"})
	keyForGrant := managementObject(t, managementRequest(t, handler, "POST", "/v1/api-keys", contentType, form), 201)
	require.NotEmpty(t, keyForGrant["allocation_id"])
	managementObject(t, managementRequest(t, handler, "POST", "/v1/api-keys/"+createdKey["id"].(string)+"/revoke", "", ""), 200)

	require.NotEmpty(t, managementArray(t, managementRequest(t, handler, "GET", "/v1/sessions", "", ""), 200))
	require.Equal(t, f.Session, managementObject(t, managementRequest(t, handler, "GET", "/v1/sessions/"+f.Session, "", ""), 200)["id"])
	require.NoError(t, f.DB.Ingest(context.Background(), "test", 0, 0, f.Event(t, "http-usage", "7", testutil.PM)))
	managementObject(t, managementRequest(t, handler, "POST", "/v1/sessions/"+f.Session+"/revoke", "", ""), 200)
	require.Equal(t, "revoked", managementObject(t, managementRequest(t, handler, "GET", "/v1/sessions/"+f.Session, "", ""), 200)["status"])

	stream := "42161:" + testutil.Contract + ":" + testutil.Sender
	require.NoError(t, f.DB.BootstrapChain(context.Background(), stream, store.Block{Number: -1}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "12", Reserve: "3"}}))
	for _, path := range []string{"/v1/settlements", "/v1/usage", "/v1/ledger/report", "/v1/escrow/report", "/v1/escrow/activity"} {
		managementArray(t, managementRequest(t, handler, "GET", path, "", ""), 200)
	}
	require.Contains(t, managementRequest(t, handler, "GET", "/v1/usage", "", "").Body.String(), `"computed_fee_eth":"0.000000000000000007"`)
	require.Contains(t, managementRequest(t, handler, "GET", "/v1/escrow/report", "", "").Body.String(), `"total_balance_eth":"0.000000000000000015"`)
	require.Contains(t, managementRequest(t, handler, "GET", "/v1/ledger/report", "", "").Body.String(), `"balance_eth"`)
	managementObject(t, managementRequest(t, handler, "POST", "/v1/allocations/"+allocationID+"/revoke", "", ""), 200)
	require.Equal(t, "revoked", managementObject(t, managementRequest(t, handler, "GET", "/v1/allocations/"+allocationID, "", ""), 200)["status"])
	managementObject(t, managementRequest(t, handler, "PATCH", "/v1/allocations/"+allocationID+"/status", "application/json", `{"status":"active"}`), 409)
	managementObject(t, managementRequest(t, handler, "GET", "/v1/grants/"+grantID, "", ""), 200)
}

func TestAPIKeyItemRoute(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	created := managementObject(t, managementRequest(t, handler, "POST", "/v1/api-keys", "application/json", fmt.Sprintf(`{"allocation_id":%q,"name":"gateway"}`, f.Allocation)), http.StatusCreated)
	id, secret := created["id"].(string), created["api_key"].(string)
	path := "/v1/api-keys/" + id
	var createdAt int64
	require.NoError(t, f.DB.DB.QueryRow(`SELECT created_at_ms FROM api_keys WHERE id=?`, id).Scan(&createdAt))
	require.Greater(t, createdAt, int64(0))
	want := map[string]any{
		"id": id, "allocation_id": f.Allocation, "name": "gateway", "prefix": "lpg_" + id,
		"created_at_ms": float64(createdAt), "last_used_at_ms": nil, "revoked_at_ms": nil,
	}
	check := func() {
		t.Helper()
		response := managementRequest(t, handler, "GET", path, "", "")
		row := managementObject(t, response, http.StatusOK)
		require.Equal(t, want, row)
		require.NotContains(t, row, "api_key")
		require.NotContains(t, row, "secret_hash")
		require.NotContains(t, response.Body.String(), secret)
	}
	check()

	request := f.Request
	request.Headers = http.Header{"Authorization": []string{"Bearer " + secret}}
	state := *f.Request.State
	state.StateID = "api-key-item"
	request.State = &state
	decision, err := f.DB.Authorize(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, decision.Status)
	var lastUsed int64
	require.NoError(t, f.DB.DB.QueryRow(`SELECT last_used_at_ms FROM api_keys WHERE id=?`, id).Scan(&lastUsed))
	require.Greater(t, lastUsed, int64(0))
	want["last_used_at_ms"] = float64(lastUsed)
	check()

	managementObject(t, managementRequest(t, handler, "POST", path+"/revoke", "", ""), http.StatusOK)
	var revokedAt int64
	require.NoError(t, f.DB.DB.QueryRow(`SELECT revoked_at_ms FROM api_keys WHERE id=?`, id).Scan(&revokedAt))
	require.Greater(t, revokedAt, int64(0))
	want["revoked_at_ms"] = float64(revokedAt)
	check()

	missing := managementObject(t, managementRequest(t, handler, "GET", "/v1/api-keys/missing", "", ""), http.StatusNotFound)
	require.Equal(t, map[string]any{"error": "resource not found"}, missing)
}

func managementArray(t *testing.T, response *httptest.ResponseRecorder, status int) []any {
	t.Helper()
	require.Equal(t, status, response.Code, response.Body.String())
	var value []any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &value))
	return value
}

func TestManagementErrors(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(context.Background(), f.DB, testRegistry(t))
	balancesBefore, err := f.DB.Report(t.Context())
	require.NoError(t, err)
	revokedType, revokedBody := managementForm(t, map[string]string{"status": "revoked"})
	exhaustedType, exhaustedBody := managementForm(t, map[string]string{"status": "exhausted"})
	for _, tc := range []struct {
		method, path, contentType, body string
		status                          int
	}{
		{"GET", "/v1/grants/missing", "", "", 404},
		{"GET", "/v1/allocations/missing", "", "", 404},
		{"POST", "/v1/allocations", "application/json", `{"name":"missing","grant_id":"missing","amount_eth":"1"}`, 404},
		{"POST", "/v1/allocations", "application/json", fmt.Sprintf(`{"name":"too much","grant_id":%q,"amount_eth":"1"}`, f.Grant), 409},
		{"POST", "/v1/grants", "application/json", `{"name":"bad","amount_eth":"1e2"}`, 400},
		{"POST", "/v1/grants", "application/json", `{"name":"bad","metadata":{"nested":"object"}}`, 400},
		{"POST", "/v1/grants", "application/json", `{"name":"bad","amount_eth":1}`, 400},
		{"POST", "/v1/grants", "application/json", `{"name":"bad","amount_eth":null}`, 400},
		{"POST", "/v1/grants", "application/json", `null`, 400},
		{"POST", "/v1/grants", "application/json", `{"name":"bad"} {}`, 400},
		{"POST", "/v1/grants", "application/json", `{"name":"bad","extra":"value"}`, 400},
		{"POST", "/v1/grants", "application/json", `{"name":`, 400},
		{"POST", "/v1/grants", "text/plain", "name=bad", 415},
		{"POST", "/v1/grants", "application/x-www-form-urlencoded", "name=a&name=b", 400},
		{"POST", "/v1/grants", "application/json", `{"name":"bad","starts_at":"tomorrow"}`, 400},
		{"POST", "/v1/api-keys", "application/json", `{"name":"key"}`, 400},
		{"PATCH", "/v1/grants/" + f.Grant + "/status", "application/json", `{"status":"invalid"}`, 400},
		{"PATCH", "/v1/allocations/" + f.Allocation + "/status", "application/json", `{"status":"revoked"}`, 400},
		{"PATCH", "/v1/allocations/" + f.Allocation + "/status", "application/json", `{"status":"exhausted"}`, 400},
		{"PATCH", "/v1/allocations/" + f.Allocation + "/status", "application/x-www-form-urlencoded", "status=revoked", 400},
		{"PATCH", "/v1/allocations/" + f.Allocation + "/status", "application/x-www-form-urlencoded", "status=exhausted", 400},
		{"PATCH", "/v1/allocations/" + f.Allocation + "/status", revokedType, revokedBody, 400},
		{"PATCH", "/v1/allocations/" + f.Allocation + "/status", exhaustedType, exhaustedBody, 400},
		{"POST", "/v1/allocations/" + f.Allocation + "/fund", "application/json", `{"amount_eth":"1"}`, 409},
		{"POST", "/v1/api-keys/missing/revoke", "", "", 404},
		{"POST", "/v1/sessions/missing/revoke", "", "", 404},
		{"POST", "/v1/grants", "application/json", `{"name":"` + strings.Repeat("x", managementBodyLimit) + `"}`, 413},
	} {
		response := managementRequest(t, handler, tc.method, tc.path, tc.contentType, tc.body)
		managementObject(t, response, tc.status)
	}
	balancesAfter, err := f.DB.Report(t.Context())
	require.NoError(t, err)
	require.Equal(t, balancesBefore, balancesAfter)
	decision, err := f.DB.Authorize(t.Context(), f.Request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, decision.Status)
	require.Equal(t, 405, managementRequest(t, handler, "DELETE", "/v1/grants/"+f.Grant, "", "").Code)
}

func TestManagementBindValidation(t *testing.T) {
	for _, address := range []string{":8081", "127.0.0.2:8081", "[::1]:8081"} {
		require.NoError(t, (ServeParams{EnableManagementAPI: mustHTTPBind(t, address), CredsFile: "creds.json"}).Validate())
	}
	for _, address := range []string{"0.0.0.0:8081", "[::]:8081", "10.0.0.1:8081"} {
		p := ServeParams{EnableManagementAPI: mustHTTPBind(t, address), CredsFile: "creds.json"}
		require.ErrorContains(t, p.Validate(), "--unsafe-http-bind")
		p.UnsafeHTTPBind = true
		require.NoError(t, p.Validate())
	}
	p := ServeParams{EnableManagementAPI: mustHTTPBind(t, ":8081"), EnableAuthWebhook: mustHTTPBind(t, "[::1]:8081"), CredsFile: "creds.json"}
	require.ErrorContains(t, p.Validate(), "separate TCP ports")
	p.EnableAuthWebhook = mustHTTPBind(t, ":8080")
	require.NoError(t, p.Validate())
}

func TestManagementServerStartup(t *testing.T) {
	for _, combined := range []bool{false, true} {
		t.Run(fmt.Sprint(combined), func(t *testing.T) {
			managementBind := testutil.Port(t)
			p := ServeParams{Common: Common{DBPath: filepath.Join(t.TempDir(), "management.db")}, EnableManagementAPI: mustHTTPBind(t, managementBind), CredsFile: testCredsFile(t)}
			if combined {
				webhookBind := testutil.Port(t)
				for webhookBind == managementBind {
					webhookBind = testutil.Port(t)
				}
				p.EnableAuthWebhook = mustHTTPBind(t, webhookBind)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, p) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Error("management server shutdown timed out")
				}
			})
			client := &http.Client{Timeout: time.Second}
			base := "http://" + managementBind
			testutil.Eventually(t, func() bool {
				response, err := client.Get(base + "/readyz")
				if err != nil {
					return false
				}
				response.Body.Close()
				return response.StatusCode == 200
			})
			request, err := http.NewRequest("POST", base+"/v1/grants", strings.NewReader(`{"name":"over HTTP","amount_eth":"1"}`))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Livepeer-Clearinghouse-Token", testOperatorToken)
			response, err := client.Do(request)
			require.NoError(t, err)
			response.Body.Close()
			require.Equal(t, 201, response.StatusCode)
			if combined {
				webhookBase := "http://" + p.EnableAuthWebhook.String()
				response, err := client.Get(webhookBase + "/readyz")
				require.NoError(t, err)
				response.Body.Close()
				require.Equal(t, 200, response.StatusCode)
				response, err = client.Get(webhookBase + "/v1/grants")
				require.NoError(t, err)
				response.Body.Close()
				require.Equal(t, 404, response.StatusCode)
				response, err = client.Post(base+"/v1/signer/authorize", "application/json", strings.NewReader(`{}`))
				require.NoError(t, err)
				response.Body.Close()
				require.Equal(t, 404, response.StatusCode)
			}
		})
	}
}

func TestManagementConfigPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serve.toml")
	require.NoError(t, os.WriteFile(path, []byte("EnableManagementAPI = \":7101\"\n"), 0600))
	read := func(args ...string) ServeParams {
		t.Helper()
		var got ServeParams
		cmd := command[ServeParams]("serve", "test", func(p *ServeParams, _ *cobra.Command) error {
			got = *p
			return nil
		})
		cmd.SetArgs(args)
		require.NoError(t, cmd.Execute())
		return got
	}
	require.Equal(t, "127.0.0.1:7101", read("--config-file", path).EnableManagementAPI.String())
	jsonPath := filepath.Join(t.TempDir(), "serve.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"EnableManagementAPI":":7104"}`), 0600))
	require.Equal(t, "127.0.0.1:7104", read("--config-file", jsonPath).EnableManagementAPI.String())
	t.Setenv("CLEARINGHOUSE_ENABLE_MANAGEMENT_API", ":7102")
	require.Equal(t, "127.0.0.1:7102", read("--config-file", path).EnableManagementAPI.String())
	require.Equal(t, "127.0.0.1:7103", read("--config-file", path, "--enable-management-api", ":7103").EnableManagementAPI.String())
}
