// Package testutil holds shared integration fixtures, never production data.
package testutil

import (
	"context"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/store"
)

const Orch = "0x1111111111111111111111111111111111111111"
const Sender = "0x2222222222222222222222222222222222222222"
const Contract = "0x3333333333333333333333333333333333333333"
const PM = "0x0000000000000000000000000000000000000000000000000000000000000001"

type Fixture struct {
	DB                                           *store.Store
	Path, Grant, Allocation, KeyID, Key, Session string
	Request                                      store.AuthRequest
}

func New(t *testing.T, amount string) *Fixture {
	t.Helper()
	ctx := context.Background()
	f := &Fixture{Path: filepath.Join(t.TempDir(), "accounting.db")}
	var err error
	f.DB, err = store.Open(ctx, f.Path, true)
	Must(t, err)
	t.Cleanup(func() { f.DB.Close() })
	f.Grant, err = f.DB.Create(ctx, "grant", store.Create{Name: "test", Amount: amount, Status: "active"})
	Must(t, err)
	f.Allocation, err = f.DB.Create(ctx, "allocation", store.Create{Name: "test allocation", GrantID: f.Grant, Amount: amount})
	Must(t, err)
	f.KeyID, f.Key, err = f.DB.CreateKey(ctx, f.Allocation, "test key")
	Must(t, err)
	f.Request = store.AuthRequest{Headers: http.Header{"Authorization": []string{"Bearer " + f.Key}}, State: &store.RemoteState{StateID: "state-1", PMSessionID: PM, OrchestratorAddress: Orch, App: "test-app", Type: "live"}}
	d, err := f.DB.Authorize(ctx, f.Request)
	Must(t, err)
	if d.Status != 200 {
		t.Fatalf("auth: %+v", d)
	}
	f.Session = d.AuthID
	t.Cleanup(func() { AssertBalances(t, f.DB) })
	return f
}

// AssertBalances independently rebuilds the complete balance map from the ledger.
// Fixture cleanup runs this across management, usage, settlement and reorg tests.
func AssertBalances(t *testing.T, db *store.Store) {
	t.Helper()
	rows, err := db.DB.Query(`SELECT account_type,account_id,direction,amount_wei FROM ledger_entries`)
	Must(t, err)
	type account struct{ kind, id string }
	want := map[account]*big.Int{}
	for rows.Next() {
		var a account
		var direction, value string
		Must(t, rows.Scan(&a.kind, &a.id, &direction, &value))
		n, ok := new(big.Int).SetString(value, 10)
		if !ok {
			t.Fatalf("invalid ledger amount %q", value)
		}
		if want[a] == nil {
			want[a] = new(big.Int)
		}
		switch direction {
		case "credit":
			want[a].Add(want[a], n)
		case "debit":
			want[a].Sub(want[a], n)
		default:
			t.Fatalf("invalid ledger direction %q", direction)
		}
	}
	Must(t, rows.Err())
	Must(t, rows.Close())
	rows, err = db.DB.Query(`SELECT account_type,account_id,balance_wei FROM account_balances`)
	Must(t, err)
	defer rows.Close()
	for rows.Next() {
		var a account
		var value string
		Must(t, rows.Scan(&a.kind, &a.id, &value))
		if want[a] == nil || want[a].String() != value {
			t.Errorf("balance %v: cached %s, ledger %v", a, value, want[a])
		}
		delete(want, a)
	}
	Must(t, rows.Err())
	for a, value := range want {
		t.Errorf("missing materialized balance %v: ledger %s", a, value)
	}
}

// Event follows monitor.GatewayEvent and the existing remote_signer.go map exactly.
func (f *Fixture) Event(t *testing.T, id, fee, pm string) []byte {
	t.Helper()
	now := time.Now().UTC()
	b, err := json.Marshal(map[string]any{"id": id, "type": "create_signed_ticket", "timestamp": "1800000000000", "gateway": "signer.test", "data": map[string]any{
		"session_id": "state-1", "session_status": "new", "app": "test-app", "pipeline": "live", "request_id": "request-" + id, "orch_address": Orch, "orch_url": "https://orch.test", "manifest_id": "manifest-1", "pm_session_id": pm, "current_time": now, "current_time_unix": now.UnixMilli(), "previous_time": now.Add(-10 * time.Second), "previous_time_unix": now.Add(-10 * time.Second).UnixMilli(), "billable_secs": 10, "pixels": 0, "session_balance": "0", "computed_fee": fee, "cost": "1.0000000000", "sequence_number": 0, "num_tickets": 1, "auth_id": f.Session,
	}})
	Must(t, err)
	return b
}
func Must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func Port(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Must(t, err)
	addr := l.Addr().String()
	Must(t, l.Close())
	return addr
}
func Eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
