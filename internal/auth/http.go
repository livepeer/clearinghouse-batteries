package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/livepeer/clearinghouse/internal/store"
)

func Handler(db *store.Store, token string) http.Handler {
	expected := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := sha256.Sum256([]byte(r.Header.Get("Livepeer-Clearinghouse-Token")))
		if token == "" || subtle.ConstantTimeCompare(expected[:], provided[:]) != 1 {
			http.Error(w, "invalid clearinghouse token", 401)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		var body store.AuthRequest
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "invalid webhook body", 400)
			return
		}
		var extra any
		if dec.Decode(&extra) != io.EOF {
			http.Error(w, "expected one JSON object", 400)
			return
		}
		if body.State == nil || body.State.StateID == "" || len(body.State.StateID) > 256 || len(body.State.App) > 4096 || len(body.State.AuthID) > 256 {
			http.Error(w, "invalid state", 400)
			return
		}
		if _, err := store.Address(body.State.OrchestratorAddress); err != nil {
			http.Error(w, "invalid orchestrator address", 400)
			return
		}
		decision, err := db.Authorize(r.Context(), body)
		if err != nil {
			slog.Error("authorize failed", "error", err)
			http.Error(w, "authorization unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(decision)
	})
}
