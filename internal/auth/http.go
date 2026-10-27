package auth

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"log/slog"
	"net/http"
	"strconv"

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
			db.Metrics.Error("authorization", "authorize", "unavailable")
			slog.Error("authorize failed", "error", err)
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		reason := "allowed"
		switch decision.Reason {
		case "invalid API key":
			reason = "invalid_key"
		case "grant or allocation is inactive":
			reason = "inactive"
		case "allocation exhausted":
			reason = "exhausted"
		case "insufficient allocation balance":
			reason = "insufficient_balance"
		case "invalid USD payment fee", "invalid payment fee":
			reason = "invalid_fee"
		case "unknown auth_id":
			reason = "unknown_auth"
		case "session is revoked or binding does not match":
			reason = "session_binding"
		}
		db.Metrics.Add("authorization_decisions_total", 1, []string{"decision", "reason"}, strconv.Itoa(decision.Status), reason)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(decision)
	})
}
