package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSignerWebhook(t *testing.T) {
	f := testutil.New(t, "100")
	h := Handler(f.DB, "signer-token")
	ctx := context.Background()
	call := func(token string, body any) (int, store.Decision) {
		t.Helper()
		b, err := json.Marshal(body)
		require.NoError(t, err)
		r := httptest.NewRequest("POST", "/v1/signer/authorize", bytes.NewReader(b))
		r.Header.Set("Livepeer-Clearinghouse-Token", token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var d store.Decision
		if w.Code == 200 {
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &d))
		}
		return w.Code, d
	}
	code, d := call("signer-token", f.Request)
	if code != 200 || d.Status != 200 || d.AuthID != f.Session || d.Expiry != 0 {
		t.Fatal(code, d)
	}
	f.Request.State.AuthID = f.Session
	_, d = call("signer-token", f.Request)
	if d.AuthID != f.Session {
		t.Fatal(d)
	}
	code, _ = call("wrong", f.Request)
	if code != 401 {
		t.Fatal(code)
	}
	rawWrongHeader, _ := json.Marshal(f.Request)
	wrongHeader := httptest.NewRequest("POST", "/v1/signer/authorize", bytes.NewReader(rawWrongHeader))
	wrongHeader.Header.Set("X-Clearinghouse-Token", "signer-token")
	wrongHeaderResponse := httptest.NewRecorder()
	h.ServeHTTP(wrongHeaderResponse, wrongHeader)
	if wrongHeaderResponse.Code != 401 {
		t.Fatal("unsupported token header accepted", wrongHeaderResponse.Code)
	}
	code, _ = call("signer-token", map[string]any{"headers": map[string]any{}})
	if code != 400 {
		t.Fatal(code)
	}
	copyReq := f.Request
	copyReq.Headers = http.Header{"Authorization": []string{"Bearer wrong"}}
	code, d = call("signer-token", copyReq)
	if code != 200 || d.Status != 401 {
		t.Fatal(code, d)
	}
	// Outer Authorization must not substitute for the gateway's nested credential.
	raw, _ := json.Marshal(store.AuthRequest{State: f.Request.State})
	r := httptest.NewRequest("POST", "/", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+f.Key)
	r.Header.Set("Livepeer-Clearinghouse-Token", "signer-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &d))
	if d.Status != 401 {
		t.Fatal(d)
	}
	require.NoError(t, f.DB.SetStatus(ctx, "session", f.Session, "revoked"))
	_, d = call("signer-token", f.Request)
	if d.Status != 403 {
		t.Fatal(d)
	}
	f.Request.State = &store.RemoteState{StateID: "new-state", App: "test-app", Type: "live", OrchestratorAddress: testutil.Orch}
	_, d = call("signer-token", f.Request)
	if d.Status != 200 {
		t.Fatal(d)
	}
	require.NoError(t, f.DB.SetStatus(ctx, "api-key", f.KeyID, "revoked"))
	_, d = call("signer-token", f.Request)
	if d.Status != 401 {
		t.Fatal(d)
	}
}

func TestExpiryPauseAndNoSecrets(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	_, err := f.DB.DB.Exec(`UPDATE grants SET ends_at_ms=?`, time.Now().Add(-time.Second).UnixMilli())
	require.NoError(t, err)
	d, err := f.DB.Authorize(ctx, f.Request)
	require.NoError(t, err)
	if d.Status != 403 {
		t.Fatal(d)
	}
	_, err = f.DB.DB.Exec(`UPDATE grants SET ends_at_ms=NULL`)
	require.NoError(t, err)
	require.NoError(t, f.DB.SetStatus(ctx, "grant", f.Grant, "paused"))
	d, err = f.DB.Authorize(ctx, f.Request)
	require.NoError(t, err)
	if d.Status != 403 {
		t.Fatal(d)
	}
	keys, err := f.DB.List(ctx, "api-key", "")
	require.NoError(t, err)
	b, err := json.Marshal(keys)
	require.NoError(t, err)
	if strings.Contains(string(b), f.Key) || strings.Contains(string(b), "secret_hash") {
		t.Fatal("secret exposed in listing")
	}
	var hash []byte
	require.NoError(t, f.DB.DB.QueryRow(`SELECT secret_hash FROM api_keys`).Scan(&hash))
	if len(hash) != 32 {
		t.Fatal(len(hash))
	}
}
