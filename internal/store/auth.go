package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// RemoteState mirrors the current signer webhook's state; additional signer fields are ignored.
type RemoteState struct {
	StateID             string
	PMSessionID         string
	OrchestratorAddress string
	App                 string
	Type                string
	AuthID              string
}
type AuthRequest struct {
	Headers http.Header  `json:"headers"`
	State   *RemoteState `json:"state"`
}
type Decision struct {
	Status int    `json:"status"`
	Reason string `json:"reason,omitempty"`
	Expiry int64  `json:"expiry"`
	AuthID string `json:"auth_id,omitempty"`
}

func (s *Store) Authorize(ctx context.Context, req AuthRequest) (Decision, error) {
	if req.State == nil || req.State.StateID == "" {
		return Decision{}, errors.New("signer state is required")
	}
	denied := Decision{Status: 401, Reason: "invalid API key"}
	var bearer string
	for k, v := range req.Headers {
		if strings.EqualFold(k, "Authorization") {
			if bearer != "" || len(v) != 1 {
				return denied, nil
			}
			bearer = v[0]
		}
	}
	parts := strings.Fields(bearer)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return denied, nil
	}
	keyID, ok := apiKeyID(parts[1])
	if !ok {
		return denied, nil
	}
	hash := sha256.Sum256([]byte(parts[1]))
	snapshot, err := readAuthorization(ctx, s.DB, keyID, req.State.StateID)
	if err != nil {
		return Decision{}, err
	}
	now := time.Now().UnixMilli()
	decision, err := snapshot.decide(req.State, keyID, hash, now)
	if err != nil || decision.Status != 200 {
		return decision, err
	}
	if decision.AuthID == "" {
		// A session may have appeared or permissions may have changed since the
		// initial read. Revalidate everything under the immediate write lock.
		err = s.Write(ctx, func(tx *sql.Tx) error {
			snapshot, err = readAuthorization(ctx, tx, keyID, req.State.StateID)
			if err != nil {
				return err
			}
			now = time.Now().UnixMilli()
			decision, err = snapshot.decide(req.State, keyID, hash, now)
			if err != nil || decision.Status != 200 || decision.AuthID != "" {
				return err
			}
			id := ID()
			orch, _ := Address(req.State.OrchestratorAddress)
			if _, err := tx.ExecContext(ctx, `INSERT INTO payment_sessions VALUES (?,?,?,?,?,?,?, 'active',?,?)`, id, snapshot.allocation, keyID, req.State.StateID, req.State.App, req.State.Type, orch, now, now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE api_keys SET last_used_at_ms=? WHERE id=?`, now, keyID); err != nil {
				return err
			}
			decision.AuthID = id
			snapshot.lastSeen = now
			snapshot.lastUsed = sql.NullInt64{Int64: now, Valid: true}
			return nil
		})
		if err != nil || decision.Status != 200 {
			return decision, err
		}
	}
	cutoff := now - time.Minute.Milliseconds()
	if snapshot.lastSeen <= cutoff || !snapshot.lastUsed.Valid || snapshot.lastUsed.Int64 <= cutoff {
		// Activity timestamps are approximate observability data after creation.
		// Conditional updates also suppress competing requests' stale touches.
		if err := s.touchAuthorization(ctx, keyID, decision.AuthID, now); err != nil {
			slog.Warn("authorization activity touch failed", "session", decision.AuthID, "error", err)
		}
	}
	return decision, nil
}

type authorizationSnapshot struct {
	saved                                               []byte
	allocation, allocationStatus, grantStatus, balance  string
	revoked, start, end, grantStart, grantEnd, lastUsed sql.NullInt64
	id, key, app, paymentType, orchestrator, status     string
	lastSeen                                            int64
}

// A single SELECT supplies a consistent snapshot without BEGIN IMMEDIATE,
// allowing authorization reads while another connection holds a WAL write lock.
func readAuthorization(ctx context.Context, q querier, key, state string) (authorizationSnapshot, error) {
	var a authorizationSnapshot
	err := q.QueryRowContext(ctx, `
SELECT k.secret_hash,k.allocation_id,k.revoked_at_ms,k.last_used_at_ms,
 a.status,g.status,a.starts_at_ms,a.ends_at_ms,g.starts_at_ms,g.ends_at_ms,
 coalesce(b.balance_wei,'0'),coalesce(s.id,''),coalesce(s.api_key_id,''),
 coalesce(s.app,''),coalesce(s.payment_type,''),coalesce(s.orchestrator,''),
 coalesce(s.status,''),coalesce(s.last_seen_at_ms,0)
FROM api_keys k JOIN grant_allocations a ON a.id=k.allocation_id
 JOIN grants g ON g.id=a.grant_id
 LEFT JOIN account_balances b ON b.account_type='allocation_available' AND b.account_id=a.id
 LEFT JOIN payment_sessions s ON s.state_id=?
WHERE k.id=?`, state, key).Scan(&a.saved, &a.allocation, &a.revoked, &a.lastUsed,
		&a.allocationStatus, &a.grantStatus, &a.start, &a.end, &a.grantStart, &a.grantEnd,
		&a.balance, &a.id, &a.key, &a.app, &a.paymentType, &a.orchestrator, &a.status, &a.lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return a, nil
	}
	return a, err
}

func (a authorizationSnapshot) decide(state *RemoteState, key string, hash [32]byte, now int64) (Decision, error) {
	if subtle.ConstantTimeCompare(hash[:], a.saved) != 1 || a.revoked.Valid {
		return Decision{Status: 401, Reason: "invalid API key"}, nil
	}
	if a.grantStatus != "active" || !oneOf(a.allocationStatus, "active", "exhausted") || !inWindow(now, a.start, a.end) || !inWindow(now, a.grantStart, a.grantEnd) {
		return Decision{Status: 403, Reason: "grant or allocation is inactive"}, nil
	}
	balance, err := signedBalance(a.balance)
	if err != nil {
		return Decision{}, err
	}
	if a.allocationStatus == "exhausted" || balance.Sign() <= 0 {
		return Decision{Status: 402, Reason: "allocation exhausted"}, nil
	}
	orch, err := Address(state.OrchestratorAddress)
	if err != nil {
		return Decision{}, err
	}
	if a.id == "" {
		if state.AuthID != "" {
			return Decision{Status: 403, Reason: "unknown auth_id"}, nil
		}
		return Decision{Status: 200}, nil // Caller must create the session transactionally.
	}
	if a.key != key || a.app != state.App || a.paymentType != state.Type || a.orchestrator != orch || a.status != "active" || (state.AuthID != "" && state.AuthID != a.id) {
		return Decision{Status: 403, Reason: "session is revoked or binding does not match"}, nil
	}
	return Decision{Status: 200, AuthID: a.id}, nil
}

func (s *Store) touchAuthorization(ctx context.Context, key, session string, now int64) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		cutoff := now - time.Minute.Milliseconds()
		if _, err := tx.ExecContext(ctx, `UPDATE payment_sessions SET last_seen_at_ms=? WHERE id=? AND last_seen_at_ms<=?`, now, session, cutoff); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE api_keys SET last_used_at_ms=? WHERE id=? AND (last_used_at_ms IS NULL OR last_used_at_ms<=?)`, now, key, cutoff)
		return err
	})
}

func apiKeyID(key string) (string, bool) {
	const prefix = "lpg_"
	const encodedUUIDLength = 22
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	payload := key[len(prefix):]
	if len(payload) <= encodedUUIDLength+1 || payload[encodedUUIDLength] != '_' {
		return "", false
	}
	id := payload[:encodedUUIDLength]
	decoded, err := base64.RawURLEncoding.DecodeString(id)
	return id, err == nil && len(decoded) == 16 && decoded[6]>>4 == 7 && decoded[8]>>6 == 2
}

func inWindow(now int64, start, end sql.NullInt64) bool {
	return (!start.Valid || now >= start.Int64) && (!end.Valid || now < end.Int64)
}
