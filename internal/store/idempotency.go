package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Idempotency identifies one management request within a grant. Fingerprint
// covers the operation, target, and exact decoded request fields. An empty Key
// disables replay storage.
type Idempotency struct {
	Key         string
	Fingerprint [32]byte
}

// ValidateIdempotencyKey accepts 1-256 base64url characters, including '='.
func ValidateIdempotencyKey(key string) error {
	if len(key) == 0 || len(key) > 256 || strings.ContainsFunc(key, func(c rune) bool {
		return !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '=')
	}) {
		return invalidInput("Idempotency-Key must be 1-256 characters using only A-Z, a-z, 0-9, '_', '-' or '='")
	}
	return nil
}

// ManagementResponse is the original successful response, including its JSON
// encoding. API-key creation responses contain secrets and must not be logged.
type ManagementResponse struct {
	Status int
	Body   []byte
}

// writeIdempotent serializes lookup, mutation, and response storage in the same
// immediate transaction, including when separate Store connections compete.
func (s *Store) writeIdempotent(ctx context.Context, scopeKind, scopeID string, status int, request Idempotency, mutate func(*sql.Tx) (map[string]string, error)) (ManagementResponse, error) {
	if request.Key != "" {
		if err := ValidateIdempotencyKey(request.Key); err != nil {
			return ManagementResponse{}, err
		}
	}
	var response ManagementResponse
	err := s.Write(ctx, func(tx *sql.Tx) error {
		grant := scopeID
		if request.Key != "" {
			if scopeKind == "allocation" {
				if err := tx.QueryRowContext(ctx, `SELECT grant_id FROM grant_allocations WHERE id=?`, scopeID).Scan(&grant); err != nil {
					return err
				}
			}
			var fingerprint []byte
			err := tx.QueryRowContext(ctx, `SELECT fingerprint,response_status,response_json FROM management_idempotency WHERE grant_id=? AND idempotency_key=?`, grant, request.Key).Scan(&fingerprint, &response.Status, &response.Body)
			if err == nil {
				if !bytes.Equal(fingerprint, request.Fingerprint[:]) {
					return stateConflict("idempotency key was already used with different parameters or operation")
				}
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		result, err := mutate(tx)
		if err != nil {
			return err
		}
		body, err := json.Marshal(result)
		if err != nil {
			return err
		}
		response = ManagementResponse{Status: status, Body: append(body, '\n')}
		if request.Key == "" {
			return nil
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO management_idempotency(grant_id,idempotency_key,fingerprint,response_status,response_json,created_at_ms) VALUES (?,?,?,?,?,?)`, grant, request.Key, request.Fingerprint[:], response.Status, string(response.Body), time.Now().UnixMilli())
		return err
	})
	if err != nil {
		return ManagementResponse{}, err
	}
	return response, nil
}

func (s *Store) CreateAllocationIdempotent(ctx context.Context, p Create, request Idempotency) (ManagementResponse, error) {
	if err := prepareCreate(&p); err != nil {
		return ManagementResponse{}, err
	}
	return s.writeIdempotent(ctx, "grant", p.GrantID, http.StatusCreated, request, func(tx *sql.Tx) (map[string]string, error) {
		id, err := createAllocation(ctx, tx, p)
		return map[string]string{"id": id}, err
	})
}

func (s *Store) FundCurrencyIdempotent(ctx context.Context, kind, id, value, currency string, request Idempotency) (ManagementResponse, error) {
	if !oneOf(kind, "grant", "allocation") {
		return ManagementResponse{}, errors.New("unknown resource")
	}
	return s.writeIdempotent(ctx, kind, id, http.StatusOK, request, func(tx *sql.Tx) (map[string]string, error) {
		return map[string]string{"id": id}, fundCurrency(ctx, tx, kind, id, value, currency)
	})
}

func (s *Store) CreateKeyIdempotent(ctx context.Context, allocation, name string, request Idempotency) (ManagementResponse, error) {
	if strings.TrimSpace(name) == "" {
		return ManagementResponse{}, invalidInput("name is required")
	}
	return s.writeIdempotent(ctx, "allocation", allocation, http.StatusCreated, request, func(tx *sql.Tx) (map[string]string, error) {
		id, key, err := createKey(ctx, tx, allocation, name)
		return map[string]string{"allocation_id": allocation, "id": id, "api_key": key}, err
	})
}

func (s *Store) CreateKeyForGrantCurrencyIdempotent(ctx context.Context, grant, name, amount, currency string, request Idempotency) (ManagementResponse, error) {
	p := Create{Name: name, GrantID: grant, Amount: amount, Currency: currency}
	if err := prepareCreate(&p); err != nil {
		return ManagementResponse{}, err
	}
	return s.writeIdempotent(ctx, "grant", grant, http.StatusCreated, request, func(tx *sql.Tx) (map[string]string, error) {
		allocation, id, key, err := createKeyForGrant(ctx, tx, p)
		return map[string]string{"allocation_id": allocation, "id": id, "api_key": key}, err
	})
}
