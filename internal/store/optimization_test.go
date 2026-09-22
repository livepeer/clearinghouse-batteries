package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestBalancesHandleLargeAmountsAndOverdraw(t *testing.T) {
	amount := strings.Repeat("9", 200)
	f := testutil.New(t, amount)
	ctx := context.Background()
	n, _ := new(big.Int).SetString(amount, 10)
	n.Add(n, big.NewInt(1))
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 0, f.Event(t, "overdraw", n.String(), testutil.PM)))
	b, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if b.String() != "-1" {
		t.Fatal(b)
	}
	b, err = store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if b.Sign() != 0 {
		t.Fatal(b)
	}
	testutil.AssertBalances(t, f.DB)
	// Delayed usage must continue charging an already overdrawn allocation.
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 1, f.Event(t, "delayed", "2", testutil.PM)))
	b, err = store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if b.String() != "-3" {
		t.Fatal(b)
	}
}

func TestBalanceUpdateFailureRollsBackUsageAndFunding(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	before, err := f.DB.Report(ctx)
	require.NoError(t, err)
	_, err = f.DB.DB.Exec(`CREATE TRIGGER fail_balance BEFORE UPDATE ON account_balances BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	require.NoError(t, err)
	raw := f.Event(t, "retry", "20", testutil.PM)
	if err := f.DB.Ingest(ctx, "events", 0, 0, raw); err == nil {
		t.Fatal("usage survived balance update failure")
	}
	if err := f.DB.Fund(ctx, "grant", f.Grant, "50"); err == nil {
		t.Fatal("funding survived balance update failure")
	}
	var events, auths, checkpoints int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM usage_events`).Scan(&events))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&auths))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM ingestion_checkpoints`).Scan(&checkpoints))
	var total string
	require.NoError(t, f.DB.DB.QueryRow(`SELECT total_wei FROM grants WHERE id=?`, f.Grant).Scan(&total))
	after, err := f.DB.Report(ctx)
	require.NoError(t, err)
	if events != 0 || auths != 0 || checkpoints != 0 || total != "100" || !reflect.DeepEqual(before, after) {
		t.Fatal("partial transaction survived", events, auths, checkpoints, total, after)
	}
	testutil.AssertBalances(t, f.DB)
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_balance`)
	require.NoError(t, err)
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 0, raw))
}

func activity(t *testing.T, db *store.Store, session, key string) (int64, int64) {
	t.Helper()
	var seen, used int64
	require.NoError(t, db.DB.QueryRow(`SELECT s.last_seen_at_ms,k.last_used_at_ms FROM payment_sessions s JOIN api_keys k ON k.id=? WHERE s.id=?`, key, session).Scan(&seen, &used))
	return seen, used
}

func TestExistingAuthorizationReadsWhileWritesAreBlocked(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	beforeSeen, beforeUsed := activity(t, f.DB, f.Session, f.KeyID)
	other, err := store.Open(ctx, f.Path, false)
	require.NoError(t, err)
	defer other.Close()
	tx, err := other.DB.BeginTx(ctx, nil) // BEGIN IMMEDIATE holds the writer lock.
	require.NoError(t, err)
	defer tx.Rollback()
	for range 3 {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		decision, err := f.DB.Authorize(readCtx, f.Request)
		cancel()
		require.NoError(t, err)
		if decision.Status != 200 || decision.AuthID != f.Session {
			t.Fatal(decision)
		}
	}
	seen, used := activity(t, f.DB, f.Session, f.KeyID)
	if seen != beforeSeen || used != beforeUsed {
		t.Fatal("recent activity was rewritten")
	}
}

func TestActivityTouchThrottleAndFailure(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	_, err := f.DB.DB.Exec(`
UPDATE payment_sessions SET last_seen_at_ms=1;
UPDATE api_keys SET last_used_at_ms=1;
CREATE TABLE touches (kind TEXT);
CREATE TRIGGER count_session_touch AFTER UPDATE OF last_seen_at_ms ON payment_sessions BEGIN INSERT INTO touches VALUES ('session'); END;
CREATE TRIGGER count_key_touch AFTER UPDATE OF last_used_at_ms ON api_keys BEGIN INSERT INTO touches VALUES ('key'); END;`)
	require.NoError(t, err)
	results := make(chan error, 5)
	start := make(chan struct{})
	for range 5 {
		go func() {
			<-start
			d, err := f.DB.Authorize(ctx, f.Request)
			if err == nil && (d.Status != 200 || d.AuthID != f.Session) {
				err = fmt.Errorf("unexpected decision: %+v", d)
			}
			results <- err
		}()
	}
	close(start)
	for range 5 {
		require.NoError(t, <-results)
	}
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM touches`).Scan(&count))
	seen, used := activity(t, f.DB, f.Session, f.KeyID)
	if count != 2 || seen != used || seen <= 1 {
		t.Fatal("timestamps were not throttled", count, seen, used)
	}
	_, err = f.DB.DB.Exec(`
UPDATE payment_sessions SET last_seen_at_ms=1;
UPDATE api_keys SET last_used_at_ms=1;
CREATE TRIGGER fail_touch BEFORE UPDATE OF last_used_at_ms ON api_keys BEGIN SELECT RAISE(ABORT,'fixture failure'); END;`)
	require.NoError(t, err)
	d, err := f.DB.Authorize(ctx, f.Request)
	require.NoError(t, err)
	if d.Status != 200 {
		t.Fatal("touch failure denied authorization", d)
	}
	seen, used = activity(t, f.DB, f.Session, f.KeyID)
	if seen != 1 || used != 1 {
		t.Fatal("partial touch transaction", seen, used)
	}
}

func TestNewSessionRecordsExactActivity(t *testing.T) {
	f := testutil.New(t, "100")
	state := *f.Request.State
	state.StateID = "another-state"
	req := f.Request
	req.State = &state
	before := time.Now().UnixMilli()
	d, err := f.DB.Authorize(context.Background(), req)
	after := time.Now().UnixMilli()
	require.NoError(t, err)
	if d.Status != 200 {
		t.Fatal(d)
	}
	seen, used := activity(t, f.DB, d.AuthID, f.KeyID)
	var created int64
	require.NoError(t, f.DB.DB.QueryRow(`SELECT created_at_ms FROM payment_sessions WHERE id=?`, d.AuthID).Scan(&created))
	if created < before || created > after || seen != created || used != created {
		t.Fatal(created, seen, used, before, after)
	}
}

func TestConcurrentSessionCreationRevalidates(t *testing.T) {
	f := testutil.New(t, "100")
	state := *f.Request.State
	state.StateID = "concurrent-state"
	req := f.Request
	req.State = &state
	type result struct {
		decision store.Decision
		err      error
	}
	results := make(chan result, 10)
	start := make(chan struct{})
	for range 10 {
		go func() {
			<-start
			d, err := f.DB.Authorize(context.Background(), req)
			results <- result{d, err}
		}()
	}
	close(start)
	var session string
	for range 10 {
		r := <-results
		require.NoError(t, r.err)
		if r.decision.Status != 200 || r.decision.AuthID == "" {
			t.Fatal(r.decision)
		}
		if session != "" && session != r.decision.AuthID {
			t.Fatal("concurrent requests created different sessions")
		}
		session = r.decision.AuthID
	}
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM payment_sessions WHERE state_id=?`, state.StateID).Scan(&count))
	if count != 1 {
		t.Fatal("expected one session", count)
	}
}

func TestCachedBalanceCanonicalConstraint(t *testing.T) {
	f := testutil.New(t, "100")
	for _, value := range []string{"", "-", "-0", "+1", "01", "-01", "1.0", "1e3", " 1", "-1x"} {
		if _, err := f.DB.DB.Exec(`UPDATE account_balances SET balance_wei=?`, value); err == nil {
			t.Fatalf("accepted noncanonical balance %q", value)
		}
	}
	// Verify the cache does not impose the per-event 256-digit amount limit.
	large := strings.Repeat("9", 256)
	n, _ := new(big.Int).SetString(large, 10)
	require.NoError(t, f.DB.Write(context.Background(), func(tx *sql.Tx) error {
		for _, key := range []string{"large1", "large2"} {
			if err := store.Transfer(context.Background(), tx, key, "test", "test", key, "treasury_cash", "large", "escrow_deposit", "large", n); err != nil {
				return err
			}
		}
		return nil
	}))
	testutil.AssertBalances(t, f.DB)
}
