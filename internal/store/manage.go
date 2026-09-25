package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

type Create struct {
	Name, Sponsor, Beneficiary, GrantID, Amount, Currency, Status, Metadata string
	Starts, Ends                                                            *int64
}

var ErrInvalidManagementInput = errors.New("invalid management input")
var ErrManagementConflict = errors.New("management conflict")

type managementError struct {
	kind    error
	message string
}

func (e managementError) Error() string { return e.message }
func (e managementError) Unwrap() error { return e.kind }

func invalidInput(message string) error {
	return managementError{ErrInvalidManagementInput, message}
}

func stateConflict(message string) error {
	return managementError{ErrManagementConflict, message}
}

func (s *Store) Create(ctx context.Context, kind string, p Create) (string, error) {
	if err := prepareCreate(&p); err != nil {
		return "", err
	}
	var id string
	err := s.Write(ctx, func(tx *sql.Tx) error {
		var err error
		switch kind {
		case "grant":
			id, err = createGrant(ctx, tx, p)
		case "allocation":
			id, err = createAllocation(ctx, tx, p)
		default:
			err = errors.New("unknown resource")
		}
		return err
	})
	return id, err
}

func prepareCreate(p *Create) error {
	if strings.TrimSpace(p.Name) == "" {
		return invalidInput("name is required")
	}
	if p.Starts != nil && p.Ends != nil && *p.Ends <= *p.Starts {
		return invalidInput("ends must follow starts")
	}
	return nil
}

func createGrant(ctx context.Context, tx *sql.Tx, p Create) (string, error) {
	if p.Currency == "" {
		p.Currency = "usd"
	}
	if !oneOf(p.Currency, "usd", "eth") {
		return "", invalidInput("currency must be usd or eth")
	}
	n, err := Amount(p.Amount)
	if err != nil {
		return "", err
	}
	id := ID()
	if p.Status == "" {
		p.Status = "draft"
	}
	if !oneOf(p.Status, "draft", "active", "paused") {
		return "", invalidInput("initial grant status must be draft, active, or paused")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO grants VALUES (?,?,?,?,?,?,?,?,?,?)`, id, p.Name, p.Sponsor, n.String(), p.Currency, p.Starts, p.Ends, p.Status, p.Metadata, time.Now().UnixMilli()); err != nil {
		return "", err
	}
	return id, TransferCurrency(ctx, tx, "create:"+id, "grant funding", "grant", id, "grant_funding_source", id, "grant_unallocated", id, p.Currency, n)
}

func allocationAmount(value string, available *big.Int) (*big.Int, error) {
	if value == "all" {
		return new(big.Int).Set(available), nil
	}
	return Amount(value)
}

func createAllocation(ctx context.Context, tx *sql.Tx, p Create) (string, error) {
	var status, currency string
	if err := tx.QueryRowContext(ctx, `SELECT status,currency FROM grants WHERE id=?`, p.GrantID).Scan(&status, &currency); err != nil {
		return "", err
	}
	if p.Currency != "" && p.Currency != currency {
		return "", invalidInput("allocation currency must match grant currency")
	}
	if status == "closed" {
		return "", stateConflict("grant is closed")
	}
	available, err := BalanceCurrency(ctx, tx, "grant_unallocated", p.GrantID, currency)
	if err != nil {
		return "", err
	}
	n, err := allocationAmount(p.Amount, available)
	if err != nil {
		return "", err
	}
	if available.Cmp(n) < 0 {
		return "", stateConflict("insufficient unallocated grant balance")
	}
	if p.Status == "" {
		p.Status = "active"
	}
	if !oneOf(p.Status, "active", "paused") {
		return "", invalidInput("initial allocation status must be active or paused")
	}
	if p.Status == "active" && n.Sign() == 0 {
		p.Status = "exhausted"
	}
	id := ID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO grant_allocations VALUES (?,?,?,?,?,?,?,?,?,?,?)`, id, p.GrantID, p.Name, p.Beneficiary, n.String(), currency, p.Starts, p.Ends, p.Status, p.Metadata, time.Now().UnixMilli()); err != nil {
		return "", err
	}
	return id, TransferCurrency(ctx, tx, "create:"+id, "grant allocation", "allocation", id, "grant_unallocated", p.GrantID, "allocation_available", id, currency, n)
}

func (s *Store) Fund(ctx context.Context, kind, id, value string) error {
	return s.FundCurrency(ctx, kind, id, value, "")
}

func (s *Store) FundCurrency(ctx context.Context, kind, id, value, currency string) error {
	if kind == "grant" {
		n, err := Amount(value)
		if err != nil {
			return err
		}
		if n.Sign() == 0 {
			return invalidInput("funding must be positive")
		}
		return s.fundGrant(ctx, id, n, currency)
	}
	if kind != "allocation" {
		return errors.New("unknown resource")
	}
	return s.Write(ctx, func(tx *sql.Tx) error {
		var old, status, grant, actualCurrency string
		if err := tx.QueryRowContext(ctx, `SELECT allocated_units,status,grant_id,currency FROM grant_allocations WHERE id=?`, id).Scan(&old, &status, &grant, &actualCurrency); err != nil {
			return err
		}
		if currency != "" && currency != actualCurrency {
			return invalidInput("funding currency must match allocation currency")
		}
		if status == "revoked" {
			return stateConflict("allocation is revoked")
		}
		var gs string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM grants WHERE id=?`, grant).Scan(&gs); err != nil {
			return err
		}
		if gs == "closed" {
			return stateConflict("grant is closed")
		}
		available, err := BalanceCurrency(ctx, tx, "grant_unallocated", grant, actualCurrency)
		if err != nil {
			return err
		}
		n, err := allocationAmount(value, available)
		if err != nil {
			return err
		}
		if n.Sign() == 0 {
			return invalidInput("funding must be positive")
		}
		if available.Cmp(n) < 0 {
			return stateConflict("insufficient unallocated grant balance")
		}
		total, err := Amount(old)
		if err != nil {
			return err
		}
		total.Add(total, n)
		if err := TransferCurrency(ctx, tx, "fund:"+ID(), "allocation funding", "allocation", id, "grant_unallocated", grant, "allocation_available", id, actualCurrency, n); err != nil {
			return err
		}
		bal, err := BalanceCurrency(ctx, tx, "allocation_available", id, actualCurrency)
		if err != nil {
			return err
		}
		if status == "exhausted" && bal.Sign() > 0 {
			status = "active"
		}
		_, err = tx.ExecContext(ctx, `UPDATE grant_allocations SET allocated_units=?,status=? WHERE id=?`, total.String(), status, id)
		return err
	})
}

func (s *Store) fundGrant(ctx context.Context, id string, n *big.Int, currency string) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		var old, status, actualCurrency string
		if err := tx.QueryRowContext(ctx, `SELECT total_units,status,currency FROM grants WHERE id=?`, id).Scan(&old, &status, &actualCurrency); err != nil {
			return err
		}
		if currency != "" && currency != actualCurrency {
			return invalidInput("funding currency must match grant currency")
		}
		if status == "closed" {
			return stateConflict("grant is closed")
		}
		total, err := Amount(old)
		if err != nil {
			return err
		}
		total.Add(total, n)
		if _, err := tx.ExecContext(ctx, `UPDATE grants SET total_units=? WHERE id=?`, total.String(), id); err != nil {
			return err
		}
		return TransferCurrency(ctx, tx, "fund:"+ID(), "grant funding", "grant", id, "grant_funding_source", id, "grant_unallocated", id, actualCurrency, n)
	})
}

func (s *Store) SetStatus(ctx context.Context, kind, id, status string) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		switch kind {
		case "grant":
			if !oneOf(status, "draft", "active", "paused", "closed") {
				return invalidInput("invalid grant status")
			}
			var old string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM grants WHERE id=?`, id).Scan(&old); err != nil {
				return err
			}
			if old == "closed" && status != old {
				return stateConflict("closed grant cannot be reopened")
			}
			_, err := tx.ExecContext(ctx, `UPDATE grants SET status=? WHERE id=?`, status, id)
			return err
		case "allocation":
			if !oneOf(status, "active", "paused", "exhausted", "revoked") {
				return invalidInput("invalid allocation status")
			}
			var old, grant, allocated, currency string
			if err := tx.QueryRowContext(ctx, `SELECT status,grant_id,allocated_units,currency FROM grant_allocations WHERE id=?`, id).Scan(&old, &grant, &allocated, &currency); err != nil {
				return err
			}
			if old == "revoked" {
				if status == old {
					return nil
				}
				return stateConflict("revoked allocation cannot be reopened")
			}
			bal, err := BalanceCurrency(ctx, tx, "allocation_available", id, currency)
			if err != nil {
				return err
			}
			if status == "active" && bal.Sign() <= 0 {
				return stateConflict("allocation has no available balance")
			}
			if status == "revoked" && bal.Sign() > 0 {
				if err := TransferCurrency(ctx, tx, "revoke:"+id, "return unused allocation", "allocation", id, "allocation_available", id, "grant_unallocated", grant, currency, bal); err != nil {
					return err
				}
				n, err := Amount(allocated)
				if err != nil {
					return err
				}
				n.Sub(n, bal)
				allocated = n.String()
			}
			_, err = tx.ExecContext(ctx, `UPDATE grant_allocations SET status=?,allocated_units=? WHERE id=?`, status, allocated, id)
			return err
		case "api-key":
			result, err := tx.ExecContext(ctx, `UPDATE api_keys SET revoked_at_ms=coalesce(revoked_at_ms,?) WHERE id=?`, time.Now().UnixMilli(), id)
			return affected(result, err)
		case "session":
			result, err := tx.ExecContext(ctx, `UPDATE payment_sessions SET status='revoked' WHERE id=?`, id)
			return affected(result, err)
		}
		return errors.New("unknown resource")
	})
}

func affected(r sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
func oneOf(s string, values ...string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}

func (s *Store) CreateKey(ctx context.Context, allocation, name string) (string, string, error) {
	if strings.TrimSpace(name) == "" {
		return "", "", invalidInput("name is required")
	}
	record, err := newKeyRecord(name)
	if err != nil {
		return "", "", err
	}
	err = s.Write(ctx, func(tx *sql.Tx) error {
		return insertKey(ctx, tx, allocation, record)
	})
	return record.id, record.key, err
}

// CreateKeyForGrant atomically creates a default allocation and an API key for it.
func (s *Store) CreateKeyForGrant(ctx context.Context, grant, name, amount string) (string, string, string, error) {
	return s.CreateKeyForGrantCurrency(ctx, grant, name, amount, "")
}

func (s *Store) CreateKeyForGrantCurrency(ctx context.Context, grant, name, amount, currency string) (string, string, string, error) {
	p := Create{Name: name, GrantID: grant, Amount: amount, Currency: currency}
	if err := prepareCreate(&p); err != nil {
		return "", "", "", err
	}
	record, err := newKeyRecord(name)
	if err != nil {
		return "", "", "", err
	}
	var allocation string
	err = s.Write(ctx, func(tx *sql.Tx) error {
		var err error
		allocation, err = createAllocation(ctx, tx, p)
		if err != nil {
			return err
		}
		return insertKey(ctx, tx, allocation, record)
	})
	return allocation, record.id, record.key, err
}

type keyRecord struct {
	id, name, prefix, key string
	hash                  [32]byte
}

func newKeyRecord(name string) (keyRecord, error) {
	id := ID()
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return keyRecord{}, err
	}
	prefix := "lpg_" + id
	key := prefix + "_" + base64.RawURLEncoding.EncodeToString(secret[:])
	return keyRecord{id: id, name: name, prefix: prefix, key: key, hash: sha256.Sum256([]byte(key))}, nil
}

func insertKey(ctx context.Context, tx *sql.Tx, allocation string, record keyRecord) error {
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM grant_allocations WHERE id=?`, allocation).Scan(&status); err != nil {
		return err
	}
	if status == "revoked" {
		return stateConflict("allocation is revoked")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO api_keys(id,allocation_id,name,prefix,secret_hash,created_at_ms) VALUES (?,?,?,?,?,?)`, record.id, allocation, record.name, record.prefix, record.hash[:], time.Now().UnixMilli())
	return err
}

func (s *Store) List(ctx context.Context, kind, id string) ([]map[string]any, error) {
	queries := map[string]string{
		"grant": `SELECT * FROM grants`, "allocation": `SELECT * FROM grant_allocations`,
		"api-key": `SELECT id,allocation_id,name,prefix,created_at_ms,last_used_at_ms,revoked_at_ms FROM api_keys`,
		"session": `SELECT * FROM payment_sessions`, "settlement": `SELECT * FROM settlements`,
		"usage": `SELECT id,event_id,topic,partition,offset,status,error,computed_fee_wei,computed_fee_usd,created_at_ms,
 (SELECT a.currency FROM payment_sessions s JOIN grant_allocations a ON a.id=s.allocation_id WHERE s.id=usage_events.payment_session_id) AS currency
 FROM usage_events`,
	}
	q, ok := queries[kind]
	if !ok {
		return nil, fmt.Errorf("unknown resource %q", kind)
	}
	args := []any{}
	if id != "" {
		q += " WHERE id=?"
		args = append(args, id)
	}
	q += " ORDER BY created_at_ms,id"
	rows, err := s.Rows(ctx, q, args...)
	if err == nil && id != "" && len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return rows, err
}
