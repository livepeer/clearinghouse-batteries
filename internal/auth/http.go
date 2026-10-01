package auth

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"log/slog"
	"net/http"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
)

func Handler(db *store.Store, registry *serviceauth.Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !registry.Require(w, r, "webhook", "authorize") {
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		var body store.AuthRequest
		if err := jsonv2.UnmarshalRead(r.Body, &body, json.DefaultOptionsV1()); err != nil {
			http.Error(w, "invalid webhook body", 400)
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
