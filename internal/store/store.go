// Package store owns the clearinghouse's SQLite transactions and exact currency arithmetic.
package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"uuid"

	"github.com/livepeer/clearinghouse/migrations"
	_ "github.com/mattn/go-sqlite3"
)

type Store struct{ DB *sql.DB }

func Open(ctx context.Context, path string, migrate bool) (*Store, error) {
	if path == "" {
		return nil, errors.New("db path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if migrate {
		if err := os.MkdirAll(filepath.Dir(abs), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(abs, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		f.Close()
	} else if _, err := os.Stat(abs); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Set("_foreign_keys", "on")
	q.Set("_journal_mode", "WAL")
	q.Set("_synchronous", "FULL")
	q.Set("_busy_timeout", "5000")
	q.Set("_txlock", "immediate")
	q.Set("mode", "rw")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if migrate {
		if err := migrations.Up(ctx, db); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Store{db}, nil
}

func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) Write(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func ID() string {
	id := uuid.NewV7()
	return base64.RawURLEncoding.EncodeToString(id[:])
}

// Amount accepts canonical unsigned integer units without a floating-point conversion.
func Amount(value string) (*big.Int, error) {
	if value == "" || len(value) > 256 || (len(value) > 1 && value[0] == '0') {
		return nil, errors.New("amount must be a canonical unsigned decimal integer (at most 256 digits)")
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return nil, errors.New("amount must be an unsigned integer")
		}
	}
	n, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, errors.New("invalid amount")
	}
	return n, nil
}

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func Balance(ctx context.Context, q querier, account, owner string) (*big.Int, error) {
	return BalanceCurrency(ctx, q, account, owner, "eth")
}

func BalanceCurrency(ctx context.Context, q querier, account, owner, currency string) (*big.Int, error) {
	if currency != "usd" && currency != "eth" {
		return nil, errors.New("invalid balance currency")
	}
	var value string
	err := q.QueryRowContext(ctx, `SELECT balance_units FROM account_balances WHERE account_type=? AND account_id=? AND currency=?`, account, owner, currency).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return new(big.Int), nil
	}
	if err != nil {
		return nil, err
	}
	return signedBalance(value)
}

func signedBalance(value string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(value, 10)
	if !ok || n.String() != value {
		return nil, errors.New("invalid canonical signed balance")
	}
	return n, nil
}

// Transfer is the only posting primitive: two equal, opposite entries, committed with their source record.
func Transfer(ctx context.Context, tx *sql.Tx, key, reason, refType, refID, debit, debitID, credit, creditID string, n *big.Int) error {
	return TransferCurrency(ctx, tx, key, reason, refType, refID, debit, debitID, credit, creditID, "eth", n)
}

func TransferCurrency(ctx context.Context, tx *sql.Tx, key, reason, refType, refID, debit, debitID, credit, creditID, currency string, n *big.Int) error {
	if currency != "usd" && currency != "eth" {
		return errors.New("invalid transfer currency")
	}
	if n.Sign() < 0 {
		return errors.New("negative transfer")
	}
	if n.Sign() == 0 {
		return nil
	}
	id, now := ID(), time.Now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions VALUES (?,?,?,?,?,?)`, id, key, reason, refType, refID, now); err != nil {
		return err
	}
	for _, e := range []struct{ account, owner, dir string }{{debit, debitID, "debit"}, {credit, creditID, "credit"}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries VALUES (?,?,?,?,?,?,?,?)`, ID(), id, e.account, e.owner, e.dir, n.String(), currency, now); err != nil {
			return err
		}
		balance, err := BalanceCurrency(ctx, tx, e.account, e.owner, currency)
		if err != nil {
			return err
		}
		if e.dir == "debit" {
			balance.Sub(balance, n)
		} else {
			balance.Add(balance, n)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO account_balances VALUES (?,?,?,?) ON CONFLICT(account_type,account_id,currency) DO UPDATE SET balance_units=excluded.balance_units`, e.account, e.owner, currency, balance.String()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Rows(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range values {
			ptr[i] = &values[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, c := range cols {
			if b, ok := values[i].([]byte); ok {
				row[c] = string(b)
			} else {
				row[c] = values[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) Report(ctx context.Context) ([]map[string]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT account_type,account_id,currency,balance_units FROM account_balances ORDER BY account_type,account_id,currency`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]string{}
	for rows.Next() {
		var a, id, currency, v string
		if err := rows.Scan(&a, &id, &currency, &v); err != nil {
			return nil, err
		}
		if _, err := signedBalance(v); err != nil {
			return nil, err
		}
		out = append(out, map[string]string{"account_type": a, "account_id": id, "currency": currency, "balance_units": v})
	}
	return out, rows.Err()
}

func (s *Store) Checkpoint(ctx context.Context, source, stream string) (int64, string, bool, error) {
	var next int64
	var hash string
	err := s.DB.QueryRowContext(ctx, `SELECT next_position,block_hash FROM ingestion_checkpoints WHERE source=? AND stream=?`, source, stream).Scan(&next, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, nil
	}
	return next, hash, err == nil, err
}
func SetCheckpoint(ctx context.Context, tx *sql.Tx, source, stream string, next int64, hash string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO ingestion_checkpoints VALUES (?,?,?,?) ON CONFLICT(source,stream) DO UPDATE SET next_position=excluded.next_position,block_hash=excluded.block_hash`, source, stream, next, hash)
	return err
}

func Address(v string) (string, error) {
	if len(v) != 42 || !strings.HasPrefix(v, "0x") {
		return "", errors.New("expected a 0x-prefixed 20-byte address")
	}
	for _, c := range strings.ToLower(v[2:]) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", errors.New("invalid address")
		}
	}
	return strings.ToLower(v), nil
}
