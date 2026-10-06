package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
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

func managementHandler(ctx context.Context, db *store.Store, registry *serviceauth.Registry) http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern, permission string, status int, fn func(http.ResponseWriter, *http.Request) (any, error)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if registry.Require(w, r, "management", permission) {
				result, err := fn(w, r)
				managementResult(w, status, result, err)
			}
		})
	}
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
		query      []string
	}{
		{"grants", "grant", true, nil},
		{"allocations", "allocation", true, []string{"grant_id"}},
		{"api-keys", "api-key", true, []string{"grant_id", "allocation_id"}},
		{"sessions", "session", true, []string{"grant_id", "allocation_id"}},
		{"settlements", "settlement", false, []string{"grant_id", "allocation_id"}},
		{"usage", "usage", false, []string{"grant_id", "allocation_id", "manifest_id"}},
	} {
		path, kind := resource.path, resource.kind
		handle("GET /v1/"+path, strings.ReplaceAll(path, "-", "_")+".read", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
			options, err := managementListQuery(r, kind, resource.query...)
			if err != nil {
				return nil, err
			}
			return managementList(r.Context(), db, kind, options)
		})
		if resource.item {
			handle("GET /v1/"+path+"/{id}", strings.ReplaceAll(path, "-", "_")+".read", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
				rows, err := db.List(r.Context(), kind, store.ListOptions{ID: r.PathValue("id")})
				if err != nil {
					return nil, err
				}
				return rows[0], nil
			})
		}
	}
	for _, resource := range []struct{ path, kind string }{{"grants", "grant"}, {"allocations", "allocation"}} {
		path, kind := resource.path, resource.kind
		handle("POST /v1/"+path, path+".create", http.StatusCreated, func(w http.ResponseWriter, r *http.Request) (any, error) {
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
			if amount != "0" && registry.Check(r, "management", path+".fund") != http.StatusOK {
				return nil, managementRequestError{http.StatusForbidden, "Forbidden"}
			}
			starts, err := parseTime(fields["starts_at"])
			if err != nil {
				return nil, badManagementRequest("invalid starts_at: expected RFC3339")
			}
			ends, err := parseTime(fields["ends_at"])
			if err != nil {
				return nil, badManagementRequest("invalid ends_at: expected RFC3339")
			}
			p := store.Create{
				Name: fields["name"], Sponsor: fields["sponsor"], Beneficiary: fields["beneficiary"], GrantID: fields["grant_id"],
				Amount: amount, Currency: currency, Status: fields["status"], Metadata: fields["metadata"], Starts: starts, Ends: ends,
			}
			if kind == "allocation" {
				request, err := managementIdempotency(r, fields)
				if err != nil {
					return nil, err
				}
				return db.CreateAllocationIdempotent(r.Context(), p, request)
			}
			id, err := db.Create(r.Context(), kind, p)
			return map[string]string{"id": id}, err
		})
		handle("POST /v1/"+path+"/{id}/fund", path+".fund", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
			fields, err := managementFields(w, r, "amount_usd", "amount_eth")
			if err != nil {
				return nil, err
			}
			amount, currency, err := inputAmount(fields["amount_usd"], fields["amount_eth"], false, kind == "allocation")
			if err != nil {
				return nil, badManagementRequest(err.Error())
			}
			request, err := managementIdempotency(r, fields)
			if err != nil {
				return nil, err
			}
			return db.FundCurrencyIdempotent(r.Context(), kind, r.PathValue("id"), amount, currency, request)
		})
		handle("PATCH /v1/"+path+"/{id}/status", path+".status", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
			fields, err := managementFields(w, r, "status")
			if err != nil {
				return nil, err
			}
			id, status := r.PathValue("id"), fields["status"]
			if kind == "allocation" && status == "revoked" {
				return nil, badManagementRequest("use POST /v1/allocations/{id}/revoke to revoke an allocation")
			}
			return map[string]string{"id": id, "status": status}, db.SetStatus(r.Context(), kind, id, status)
		})
	}
	handle("POST /v1/api-keys", "api_keys.create", http.StatusCreated, func(w http.ResponseWriter, r *http.Request) (any, error) {
		fields, err := managementFields(w, r, "allocation_id", "grant_id", "name", "amount_usd", "amount_eth")
		if err != nil {
			return nil, err
		}
		allocation, grant := fields["allocation_id"], fields["grant_id"]
		usd, eth := fields["amount_usd"], fields["amount_eth"]
		if (allocation == "") == (grant == "") {
			return nil, badManagementRequest("specify exactly one of allocation_id or grant_id")
		}
		if grant != "" && registry.Check(r, "management", "allocations.create", "allocations.fund") != http.StatusOK {
			return nil, managementRequestError{http.StatusForbidden, "Forbidden"}
		}
		request, err := managementIdempotency(r, fields)
		if err != nil {
			return nil, err
		}
		if allocation != "" {
			if usd != "" || eth != "" {
				return nil, badManagementRequest("amount is only valid with grant_id")
			}
			return db.CreateKeyIdempotent(r.Context(), allocation, fields["name"], request)
		}
		amount, currency, err := inputAmount(usd, eth, false, true)
		if err != nil {
			return nil, badManagementRequest(err.Error())
		}
		return db.CreateKeyForGrantCurrencyIdempotent(r.Context(), grant, fields["name"], amount, currency, request)
	})
	for _, resource := range []struct{ path, kind string }{{"allocations", "allocation"}, {"api-keys", "api-key"}, {"sessions", "session"}} {
		path, kind := resource.path, resource.kind
		handle("POST /v1/"+path+"/{id}/revoke", strings.ReplaceAll(path, "-", "_")+".revoke", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
			id := r.PathValue("id")
			return map[string]string{"id": id, "status": "revoked"}, db.SetStatus(r.Context(), kind, id, "revoked")
		})
	}
	handle("GET /v1/ledger/report", "ledger.read", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
		return db.Report(r.Context())
	})
	handle("GET /v1/escrow/report", "escrow.read", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
		return db.EscrowReport(r.Context())
	})
	handle("GET /v1/escrow/activity", "escrow.read", http.StatusOK, func(w http.ResponseWriter, r *http.Request) (any, error) {
		return db.EscrowActivity(r.Context())
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func managementIdempotency(r *http.Request, fields map[string]string) (store.Idempotency, error) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) == 0 {
		return store.Idempotency{}, nil
	}
	if len(values) != 1 {
		return store.Idempotency{}, badManagementRequest("expected exactly one Idempotency-Key")
	}
	if err := store.ValidateIdempotencyKey(values[0]); err != nil {
		return store.Idempotency{}, err
	}
	// Hash decoded fields without normalizing values or adding defaults. The
	// route pattern and target distinguish operations sharing a grant namespace.
	parameters, err := jsonv2.Marshal(struct {
		Method, Route, Target string
		Fields                map[string]string
	}{r.Method, r.Pattern, r.PathValue("id"), fields}, json.DefaultOptionsV1(), jsontext.AllowInvalidUTF8(false))
	if err != nil {
		return store.Idempotency{}, badManagementRequest("request fields must be valid UTF-8")
	}
	return store.Idempotency{Key: values[0], Fingerprint: sha256.Sum256(parameters)}, nil
}

func managementQuery(r *http.Request, allowed ...string) (url.Values, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, badManagementRequest("invalid query parameters")
	}
	for name, entries := range query {
		if !slices.Contains(allowed, name) {
			return nil, badManagementRequest("unknown query parameter: " + name)
		}
		if len(entries) != 1 {
			return nil, badManagementRequest("expected one value for " + name)
		}
		if entries[0] == "" {
			return nil, badManagementRequest(name + " must not be empty")
		}
	}
	return query, nil
}

func managementFields(w http.ResponseWriter, r *http.Request, allowed ...string) (map[string]string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, managementBodyLimit)
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, managementRequestError{http.StatusUnsupportedMediaType, "expected JSON or form content type"}
	}
	values := map[string]string{}
	var form map[string][]string
	switch mediaType {
	case "application/json":
		var raw map[string]*string
		if err := jsonv2.UnmarshalRead(r.Body, &raw, json.DefaultOptionsV1()); err != nil {
			return nil, managementDecodeError(err)
		}
		if raw == nil {
			return nil, badManagementRequest("expected one JSON object")
		}
		for key, value := range raw {
			if value == nil {
				return nil, badManagementRequest(fmt.Sprintf("%s must be a string", key))
			}
			values[key] = *value
		}
	case "application/x-www-form-urlencoded":
		if err := r.ParseForm(); err != nil {
			return nil, managementDecodeError(err)
		}
		form = r.PostForm
	case "multipart/form-data":
		if err := r.ParseMultipartForm(managementBodyLimit); err != nil {
			return nil, managementDecodeError(err)
		}
		defer r.MultipartForm.RemoveAll()
		if len(r.MultipartForm.File) > 0 {
			return nil, badManagementRequest("file uploads are not supported")
		}
		form = r.MultipartForm.Value
	default:
		return nil, managementRequestError{http.StatusUnsupportedMediaType, "expected JSON or form content type"}
	}
	for field, entries := range form {
		if len(entries) != 1 {
			return nil, badManagementRequest("expected one value for " + field)
		}
		values[field] = entries[0]
	}
	for field := range values {
		if !slices.Contains(allowed, field) {
			return nil, badManagementRequest("unknown field: " + field)
		}
	}
	return values, nil
}

func managementDecodeError(err error) error {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return managementRequestError{http.StatusRequestEntityTooLarge, "request body too large"}
	}
	return badManagementRequest("invalid request body")
}

func managementResult(w http.ResponseWriter, status int, result any, err error) {
	if err == nil {
		if response, ok := result.(store.ManagementResponse); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(response.Status)
			_, _ = w.Write(response.Body)
			return
		}
		result, err = displayETH(result)
	}
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
	requestErr, isRequest := errors.AsType[managementRequestError](err)
	switch {
	case isRequest:
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
