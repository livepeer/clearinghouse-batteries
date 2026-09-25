package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"

	"github.com/livepeer/clearinghouse/internal/store"
)

const managementBodyLimit = 1 << 20

type managementRequestError struct {
	status  int
	message string
}

func (e managementRequestError) Error() string { return e.message }

func badManagementRequest(message string) error {
	return managementRequestError{http.StatusBadRequest, message}
}

func managementHandler(ctx context.Context, db *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil || db.DB.PingContext(r.Context()) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	for _, resource := range []struct {
		path, kind string
		item       bool
	}{
		{"grants", "grant", true},
		{"allocations", "allocation", true},
		{"api-keys", "api-key", false},
		{"sessions", "session", true},
		{"settlements", "settlement", false},
		{"usage", "usage", false},
	} {
		path, kind := resource.path, resource.kind
		mux.HandleFunc("GET /v1/"+path, func(w http.ResponseWriter, r *http.Request) {
			managementResult(w, http.StatusOK, func() (any, error) { return db.List(r.Context(), kind, "") })
		})
		if resource.item {
			mux.HandleFunc("GET /v1/"+path+"/{id}", func(w http.ResponseWriter, r *http.Request) {
				managementResult(w, http.StatusOK, func() (any, error) {
					rows, err := db.List(r.Context(), kind, r.PathValue("id"))
					if err != nil {
						return nil, err
					}
					return rows[0], nil
				})
			})
		}
	}
	for _, resource := range []struct{ path, kind string }{{"grants", "grant"}, {"allocations", "allocation"}} {
		path, kind := resource.path, resource.kind
		mux.HandleFunc("POST /v1/"+path, func(w http.ResponseWriter, r *http.Request) {
			managementResult(w, http.StatusCreated, func() (any, error) {
				fields, err := managementFields(w, r, "name", "amount_usd", "amount_eth", "sponsor", "beneficiary", "grant_id", "status", "metadata", "starts_at", "ends_at")
				if err != nil {
					return nil, err
				}
				if kind == "grant" && (fields["beneficiary"] != "" || fields["grant_id"] != "") {
					return nil, badManagementRequest("beneficiary and grant_id are only valid for allocations")
				}
				if kind == "allocation" && (fields["sponsor"] != "" || fields["grant_id"] == "") {
					return nil, badManagementRequest("allocations require grant_id and do not accept sponsor")
				}
				amount, currency, err := inputAmount(fields["amount_usd"], fields["amount_eth"], true, kind == "allocation")
				if err != nil {
					return nil, badManagementRequest(err.Error())
				}
				starts, err := parseTime(fields["starts_at"])
				if err != nil {
					return nil, badManagementRequest("invalid starts_at: expected RFC3339")
				}
				ends, err := parseTime(fields["ends_at"])
				if err != nil {
					return nil, badManagementRequest("invalid ends_at: expected RFC3339")
				}
				id, err := db.Create(r.Context(), kind, store.Create{
					Name: fields["name"], Sponsor: fields["sponsor"], Beneficiary: fields["beneficiary"], GrantID: fields["grant_id"],
					Amount: amount, Currency: currency, Status: fields["status"], Metadata: fields["metadata"], Starts: starts, Ends: ends,
				})
				return map[string]string{"id": id}, err
			})
		})
		mux.HandleFunc("POST /v1/"+path+"/{id}/fund", func(w http.ResponseWriter, r *http.Request) {
			managementResult(w, http.StatusOK, func() (any, error) {
				fields, err := managementFields(w, r, "amount_usd", "amount_eth")
				if err != nil {
					return nil, err
				}
				amount, currency, err := inputAmount(fields["amount_usd"], fields["amount_eth"], false, kind == "allocation")
				if err != nil {
					return nil, badManagementRequest(err.Error())
				}
				id := r.PathValue("id")
				return map[string]string{"id": id}, db.FundCurrency(r.Context(), kind, id, amount, currency)
			})
		})
		mux.HandleFunc("PATCH /v1/"+path+"/{id}/status", func(w http.ResponseWriter, r *http.Request) {
			managementResult(w, http.StatusOK, func() (any, error) {
				fields, err := managementFields(w, r, "status")
				if err != nil {
					return nil, err
				}
				id, status := r.PathValue("id"), fields["status"]
				return map[string]string{"id": id, "status": status}, db.SetStatus(r.Context(), kind, id, status)
			})
		})
	}
	mux.HandleFunc("POST /v1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		managementResult(w, http.StatusCreated, func() (any, error) {
			fields, err := managementFields(w, r, "allocation_id", "grant_id", "name", "amount_usd", "amount_eth")
			if err != nil {
				return nil, err
			}
			allocation, grant := fields["allocation_id"], fields["grant_id"]
			usd, eth := fields["amount_usd"], fields["amount_eth"]
			if (allocation == "") == (grant == "") {
				return nil, badManagementRequest("specify exactly one of allocation_id or grant_id")
			}
			if allocation != "" {
				if usd != "" || eth != "" {
					return nil, badManagementRequest("amount is only valid with grant_id")
				}
				id, key, err := db.CreateKey(r.Context(), allocation, fields["name"])
				return map[string]string{"allocation_id": allocation, "id": id, "api_key": key}, err
			}
			amount, currency, err := inputAmount(usd, eth, false, true)
			if err != nil {
				return nil, badManagementRequest(err.Error())
			}
			allocation, id, key, err := db.CreateKeyForGrantCurrency(r.Context(), grant, fields["name"], amount, currency)
			return map[string]string{"allocation_id": allocation, "id": id, "api_key": key}, err
		})
	})
	for _, resource := range []struct{ path, kind string }{{"allocations", "allocation"}, {"api-keys", "api-key"}, {"sessions", "session"}} {
		path, kind := resource.path, resource.kind
		mux.HandleFunc("POST /v1/"+path+"/{id}/revoke", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			managementResult(w, http.StatusOK, func() (any, error) {
				return map[string]string{"id": id, "status": "revoked"}, db.SetStatus(r.Context(), kind, id, "revoked")
			})
		})
	}
	mux.HandleFunc("GET /v1/ledger/report", func(w http.ResponseWriter, r *http.Request) {
		managementResult(w, http.StatusOK, func() (any, error) { return db.Report(r.Context()) })
	})
	mux.HandleFunc("GET /v1/escrow/report", func(w http.ResponseWriter, r *http.Request) {
		managementResult(w, http.StatusOK, func() (any, error) { return db.EscrowReport(r.Context()) })
	})
	mux.HandleFunc("GET /v1/escrow/activity", func(w http.ResponseWriter, r *http.Request) {
		managementResult(w, http.StatusOK, func() (any, error) { return db.EscrowActivity(r.Context()) })
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func managementFields(w http.ResponseWriter, r *http.Request, allowed ...string) (map[string]string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, managementBodyLimit)
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, managementRequestError{http.StatusUnsupportedMediaType, "expected JSON or form content type"}
	}
	values := map[string]string{}
	switch mediaType {
	case "application/json":
		var raw map[string]json.RawMessage
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&raw); err != nil {
			return nil, managementDecodeError(err)
		}
		if raw == nil {
			return nil, badManagementRequest("expected one JSON object")
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			if err == nil {
				return nil, badManagementRequest("expected one JSON object")
			}
			return nil, managementDecodeError(err)
		}
		for key, value := range raw {
			if len(value) == 0 || value[0] != '"' {
				return nil, badManagementRequest(fmt.Sprintf("%s must be a string", key))
			}
			var decoded string
			if err := json.Unmarshal(value, &decoded); err != nil {
				return nil, badManagementRequest("invalid JSON string")
			}
			values[key] = decoded
		}
	case "application/x-www-form-urlencoded":
		if err := r.ParseForm(); err != nil {
			return nil, managementDecodeError(err)
		}
		if err := copyManagementForm(values, r.PostForm); err != nil {
			return nil, err
		}
	case "multipart/form-data":
		if err := r.ParseMultipartForm(managementBodyLimit); err != nil {
			return nil, managementDecodeError(err)
		}
		defer r.MultipartForm.RemoveAll()
		if len(r.MultipartForm.File) > 0 {
			return nil, badManagementRequest("file uploads are not supported")
		}
		if err := copyManagementForm(values, r.MultipartForm.Value); err != nil {
			return nil, err
		}
	default:
		return nil, managementRequestError{http.StatusUnsupportedMediaType, "expected JSON or form content type"}
	}
	permitted := make(map[string]bool, len(allowed))
	for _, field := range allowed {
		permitted[field] = true
	}
	for field := range values {
		if !permitted[field] {
			return nil, badManagementRequest("unknown field: " + field)
		}
	}
	return values, nil
}

func copyManagementForm(dst map[string]string, form map[string][]string) error {
	for field, values := range form {
		if len(values) != 1 {
			return badManagementRequest("expected one value for " + field)
		}
		dst[field] = values[0]
	}
	return nil
}

func managementDecodeError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return managementRequestError{http.StatusRequestEntityTooLarge, "request body too large"}
	}
	return badManagementRequest("invalid request body")
}

func managementResult(w http.ResponseWriter, status int, fn func() (any, error)) {
	result, err := fn()
	if err != nil {
		managementErrorResponse(w, err)
		return
	}
	result, err = displayETH(result)
	if err != nil {
		managementErrorResponse(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

func managementErrorResponse(w http.ResponseWriter, err error) {
	status, message := http.StatusInternalServerError, "internal server error"
	var requestErr managementRequestError
	switch {
	case errors.As(err, &requestErr):
		status, message = requestErr.status, requestErr.message
	case errors.Is(err, sql.ErrNoRows):
		status, message = http.StatusNotFound, "resource not found"
	case errors.Is(err, store.ErrInvalidManagementInput):
		status, message = http.StatusBadRequest, err.Error()
	case errors.Is(err, store.ErrManagementConflict):
		status, message = http.StatusConflict, err.Error()
	default:
		slog.Error("management request failed", "error", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": strings.TrimSpace(message)})
}
