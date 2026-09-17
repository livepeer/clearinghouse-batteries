package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/migrations"
	"github.com/stretchr/testify/require"
)

func TestIDIsBase64URLUUIDv7(t *testing.T) {
	id := store.ID()
	if len(id) != 22 {
		t.Fatalf("ID length = %d, want 22", len(id))
	}
	raw, err := base64.RawURLEncoding.DecodeString(id)
	require.NoError(t, err)
	if len(raw) != 16 {
		t.Fatalf("decoded ID length = %d, want 16", len(raw))
	}
	if version := raw[6] >> 4; version != 7 {
		t.Fatalf("UUID version = %d, want 7", version)
	}
	if variant := raw[8] >> 6; variant != 2 {
		t.Fatalf("UUID variant = %d, want 2", variant)
	}
}

func TestAuthorizeAcceptsUnderscoreInBase64URLKeyID(t *testing.T) {
	f := testutil.New(t, "100")
	raw := bytes.Repeat([]byte{0xff}, 16)
	raw[6] = 0x7f
	raw[8] = 0xbf
	id := base64.RawURLEncoding.EncodeToString(raw)
	key := "lpg_" + id + "_secret"
	hash := sha256.Sum256([]byte(key))
	_, err := f.DB.DB.Exec(`INSERT INTO api_keys(id,allocation_id,name,prefix,secret_hash,created_at_ms) VALUES (?,?,?,?,?,?)`, id, f.Allocation, "underscore", "lpg_"+id, hash[:], 0)
	require.NoError(t, err)
	req := store.AuthRequest{
		Headers: http.Header{"Authorization": []string{"Bearer " + key}},
		State:   &store.RemoteState{StateID: "state-underscore", OrchestratorAddress: testutil.Orch, App: "test-app", Type: "live"},
	}
	decision, err := f.DB.Authorize(context.Background(), req)
	require.NoError(t, err)
	if decision.Status != 200 {
		t.Fatalf("auth: %+v", decision)
	}
}

func TestMigrationsConstraintsAndRoundTrip(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	for _, q := range []string{`UPDATE ledger_entries SET amount_wei='1'`, `DELETE FROM ledger_entries`, `DELETE FROM ledger_transactions`, `UPDATE ledger_transactions SET reason='changed'`, `INSERT INTO api_keys(id,allocation_id,name,prefix,secret_hash,created_at_ms) VALUES ('bad','missing','bad','bad',zeroblob(32),0)`} {
		if _, err := f.DB.DB.ExecContext(ctx, q); err == nil {
			t.Fatalf("constraint allowed %s", q)
		}
	}
	for _, q := range []string{`UPDATE grants SET total_wei='01'`, `UPDATE grants SET metadata='{bad'`, `UPDATE grant_allocations SET status='unknown'`, `UPDATE payment_sessions SET orchestrator='0xgggggggggggggggggggggggggggggggggggggggg'`, `UPDATE api_keys SET secret_hash=zeroblob(31)`} {
		if _, err := f.DB.DB.ExecContext(ctx, q); err == nil {
			t.Fatalf("check allowed %s", q)
		}
	}
	for _, index := range []string{"allocations_grant", "keys_allocation", "sessions_allocation", "usage_status", "signing_match", "ledger_account", "ledger_transaction", "settlements_match", "settlements_block", "ticket_broker_events_block", "ticket_broker_events_sender"} {
		var n int
		require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?`, index).Scan(&n))
		if n != 1 {
			t.Fatalf("missing index %s", index)
		}
	}
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	list, err := migrations.List(ctx, f.DB.DB)
	require.NoError(t, err)
	if len(list) != 1 || !list[0].Applied {
		t.Fatal(list)
	}
	require.NoError(t, migrations.Down(ctx, f.DB.DB))
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grants`).Scan(&count))
	if count != 0 {
		t.Fatal(count)
	}
}

func TestEscrowReportUsesArbitraryWidthAmounts(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	amount := strings.Repeat("9", 200)
	stream := "42161:" + testutil.Contract + ":" + testutil.Sender
	require.NoError(t, f.DB.BootstrapChain(ctx, stream, store.Block{Number: -1}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: amount, Reserve: amount}}))
	report, err := f.DB.EscrowReport(ctx)
	require.NoError(t, err)
	want, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		t.Fatal("invalid fixture")
	}
	want.Mul(want, big.NewInt(2))
	if len(report) != 1 || report[0]["deposit_balance_wei"] != amount || report[0]["reserve_balance_wei"] != amount || report[0]["total_balance_wei"] != want.String() {
		t.Fatal(report)
	}
	assertBalanced(t, f.DB)
}

func TestEscrowSnapshotIsAHardReorgBoundary(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	stream := "42161:" + testutil.Contract + ":" + testutil.Sender
	hash := "0x" + strings.Repeat("a", 64)
	require.NoError(t, f.DB.BootstrapChain(ctx, stream, store.Block{Number: 5, Hash: hash}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "1", Reserve: "2"}}))
	err := f.DB.Rewind(ctx, stream, "42161", testutil.Contract, store.Block{Number: 4, Hash: "0x" + strings.Repeat("b", 64)})
	if !errors.Is(err, store.ErrEscrowSnapshotReorg) {
		t.Fatal(err)
	}
	next, gotHash, found, err := f.DB.Checkpoint(ctx, "chain", stream)
	require.NoError(t, err)
	if !found || next != 6 || gotHash != hash {
		t.Fatal(next, gotHash, found)
	}
}

func TestIngestExactMoneyReplayQuarantineOverdraw(t *testing.T) {
	const budget = "100000000000000000000000000000000000000000000000000000000000000000000"
	f := testutil.New(t, budget)
	ctx := context.Background()
	raw := f.Event(t, "event-1", budget, testutil.PM)
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, raw))
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, raw))
	require.NoError(t, f.DB.Ingest(ctx, "test", 1, raw))
	require.NoError(t, f.DB.Ingest(ctx, "test", 2, []byte(`{bad`)))
	require.NoError(t, f.DB.Ingest(ctx, "test", 3, f.Event(t, "event-2", "17", testutil.PM)))
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "-17" {
		t.Fatal(bal)
	}
	d, err := f.DB.Authorize(ctx, f.Request)
	require.NoError(t, err)
	if d.Status != 402 {
		t.Fatal(d)
	}
	var signed int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&signed))
	if signed != 2 {
		t.Fatal(signed)
	}
	rows, err := f.DB.Rows(ctx, `SELECT status FROM usage_events ORDER BY offset`)
	require.NoError(t, err)
	if rows[1]["status"] != "duplicate" || rows[2]["status"] != "quarantined" {
		t.Fatal(rows)
	}
	next, _, _, err := f.DB.Checkpoint(ctx, "kafka", "test")
	require.NoError(t, err)
	if next != 4 {
		t.Fatal(next)
	}
	if err := f.DB.Ingest(ctx, "test", 5, raw); err == nil {
		t.Fatal("gap accepted")
	}
	// A new connection resumes from the durable accounting checkpoint.
	db, err := store.Open(ctx, f.Path, false)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ingest(ctx, "test", 3, f.Event(t, "event-2", "17", testutil.PM)))
	assertBalanced(t, f.DB)
}

func TestQuarantineBindingsAndConflictingID(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	valid := f.Event(t, "event", "10", testutil.PM)
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, valid))
	variants := [][]byte{[]byte(strings.ReplaceAll(string(valid), `"10"`, `"20"`)), []byte(strings.ReplaceAll(string(f.Event(t, "unknown", "10", testutil.PM)), f.Session, "unknown")), []byte(strings.ReplaceAll(string(f.Event(t, "binding", "10", testutil.PM)), "state-1", "state-other")), []byte(strings.ReplaceAll(string(f.Event(t, "missing", "10", testutil.PM)), f.Session, ""))}
	for i, raw := range variants {
		require.NoError(t, f.DB.Ingest(ctx, "test", int64(i+1), raw))
	}
	var quarantined int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM usage_events WHERE status='quarantined'`).Scan(&quarantined))
	if quarantined != 4 {
		t.Fatal(quarantined)
	}
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "90" {
		t.Fatal(bal)
	}
}

func TestConcurrentFundingCannotOverallocate(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "100"))
	var wg sync.WaitGroup
	var success atomic.Int32
	errs := make(chan error, 12)
	for i := range 12 {
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err != nil {
				errs <- err
				return
			}
			defer db.Close()
			_, err = db.Create(ctx, "allocation", store.Create{Name: fmt.Sprint(i), GrantID: f.Grant, Amount: "30"})
			if err == nil {
				success.Add(1)
			} else if !strings.Contains(err.Error(), "insufficient") {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if success.Load() != 3 {
		t.Fatal(success.Load())
	}
	bal, err := store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if bal.String() != "10" {
		t.Fatal(bal)
	}
	assertBalanced(t, f.DB)
}

func TestAllAllocationCreationAndFunding(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()

	zeroGrant, err := f.DB.Create(ctx, "grant", store.Create{Name: "empty", Amount: "0"})
	require.NoError(t, err)
	zeroAllocation, err := f.DB.Create(ctx, "allocation", store.Create{Name: "empty allocation", GrantID: zeroGrant, Amount: "all"})
	require.NoError(t, err)
	var allocated, status string
	require.NoError(t, f.DB.DB.QueryRow(`SELECT allocated_wei,status FROM grant_allocations WHERE id=?`, zeroAllocation).Scan(&allocated, &status))
	if allocated != "0" || status != "exhausted" {
		t.Fatal(allocated, status)
	}

	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "75"))
	allocation, err := f.DB.Create(ctx, "allocation", store.Create{Name: "everything", GrantID: f.Grant, Amount: "all"})
	require.NoError(t, err)
	require.NoError(t, f.DB.DB.QueryRow(`SELECT allocated_wei,status FROM grant_allocations WHERE id=?`, allocation).Scan(&allocated, &status))
	if allocated != "75" || status != "active" {
		t.Fatal(allocated, status)
	}
	if err := f.DB.Fund(ctx, "allocation", allocation, "all"); err == nil {
		t.Fatal("funded allocation with an empty grant balance")
	}
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "25"))
	require.NoError(t, f.DB.Fund(ctx, "allocation", allocation, "all"))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT allocated_wei FROM grant_allocations WHERE id=?`, allocation).Scan(&allocated))
	if allocated != "100" {
		t.Fatal(allocated)
	}
	assertBalanced(t, f.DB)
}

func TestConcurrentAllUsesOneTransactionalBalance(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "100"))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err == nil {
				defer db.Close()
				_, err = db.Create(ctx, "allocation", store.Create{Name: "all-" + fmt.Sprint(i), GrantID: f.Grant, Amount: "all"})
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	rows, err := f.DB.Rows(ctx, `SELECT allocated_wei,status FROM grant_allocations WHERE name LIKE 'all-%'`)
	require.NoError(t, err)
	var funded int
	for _, row := range rows {
		switch row["allocated_wei"] {
		case "100":
			funded++
			if row["status"] != "active" {
				t.Fatal(row)
			}
		case "0":
			if row["status"] != "exhausted" {
				t.Fatal(row)
			}
		default:
			t.Fatal(row)
		}
	}
	if len(rows) != 8 || funded != 1 {
		t.Fatal(rows)
	}

	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "60"))
	var fundingSuccess atomic.Int32
	errs = make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err == nil {
				defer db.Close()
				err = db.Fund(ctx, "allocation", f.Allocation, "all")
				if err == nil {
					fundingSuccess.Add(1)
				} else if strings.Contains(err.Error(), "funding must be positive") {
					err = nil
				}
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	if fundingSuccess.Load() != 1 {
		t.Fatal(fundingSuccess.Load())
	}
	assertBalanced(t, f.DB)
}

func TestCreateKeyForGrantIsAtomicAndUsesAllocationDefaults(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "50"))
	var before int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grant_allocations`).Scan(&before))
	_, err := f.DB.DB.Exec(`CREATE TRIGGER fail_auto_key BEFORE INSERT ON api_keys BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	require.NoError(t, err)
	if _, _, _, err := f.DB.CreateKeyForGrant(ctx, f.Grant, "rolled back", "10"); err == nil {
		t.Fatal("expected key insertion failure")
	}
	var after int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grant_allocations`).Scan(&after))
	if after != before {
		t.Fatal("allocation was not rolled back", before, after)
	}
	remaining, err := store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if remaining.String() != "50" {
		t.Fatal(remaining)
	}
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_auto_key`)
	require.NoError(t, err)

	allocation, keyID, key, err := f.DB.CreateKeyForGrant(ctx, f.Grant, "gateway", "all")
	require.NoError(t, err)
	if allocation == "" || keyID == "" || !strings.HasPrefix(key, "lpg_"+keyID+"_") {
		t.Fatal(allocation, keyID, key)
	}
	var name, beneficiary, amount, metadata, allocationStatus string
	var starts, ends any
	require.NoError(t, f.DB.DB.QueryRow(`SELECT name,beneficiary,allocated_wei,metadata,starts_at_ms,ends_at_ms,status FROM grant_allocations WHERE id=?`, allocation).Scan(&name, &beneficiary, &amount, &metadata, &starts, &ends, &allocationStatus))
	if name != "gateway" || beneficiary != "" || amount != "50" || metadata != "{}" || starts != nil || ends != nil || allocationStatus != "active" {
		t.Fatal(name, beneficiary, amount, metadata, starts, ends, allocationStatus)
	}
	assertBalanced(t, f.DB)
}

func TestRevocationReturnsOnlyUnspentAndLateChargeIsRecorded(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, f.Event(t, "one", "40", testutil.PM)))
	require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "revoked"))
	require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "revoked"))
	require.NoError(t, f.DB.Ingest(ctx, "test", 1, f.Event(t, "late", "20", testutil.PM)))
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "-20" {
		t.Fatal(bal)
	}
	bal, err = store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if bal.String() != "60" {
		t.Fatal(bal)
	}
	if err := f.DB.SetStatus(ctx, "allocation", f.Allocation, "active"); err == nil {
		t.Fatal("revoked allocation reopened")
	}
}

func assertBalanced(t *testing.T, db *store.Store) {
	t.Helper()
	rows, err := db.DB.Query(`SELECT transaction_id,count(*),count(DISTINCT amount_wei),count(DISTINCT direction) FROM ledger_entries GROUP BY transaction_id`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id string
		var n, a, d int
		require.NoError(t, rows.Scan(&id, &n, &a, &d))
		if n != 2 || a != 1 || d != 2 {
			t.Fatalf("unbalanced %s", id)
		}
	}
	require.NoError(t, rows.Err())
}

func TestConcurrentSpendsNeverLoseCharges(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		raw := f.Event(t, fmt.Sprint(i), "10", testutil.PM)
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err == nil {
				defer db.Close()
				err = db.Ingest(ctx, fmt.Sprint(i), 0, raw)
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	b, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if b.String() != "-60" {
		t.Fatal(b)
	}
	assertBalanced(t, f.DB)
}
