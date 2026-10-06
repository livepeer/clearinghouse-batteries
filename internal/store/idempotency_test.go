package store_test

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

type idempotentMutation struct {
	name                     string
	allocations, keys, posts int
	run                      func(*store.Store, store.Idempotency) (store.ManagementResponse, error)
}

func idempotentMutations(t *testing.T, f *testutil.Fixture) []idempotentMutation {
	return []idempotentMutation{
		{"allocation create all", 1, 0, 1, func(db *store.Store, request store.Idempotency) (store.ManagementResponse, error) {
			return db.CreateAllocationIdempotent(t.Context(), store.Create{Name: "created", GrantID: f.Grant, Amount: "all"}, request)
		}},
		{"grant fund", 0, 0, 1, func(db *store.Store, request store.Idempotency) (store.ManagementResponse, error) {
			return db.FundCurrencyIdempotent(t.Context(), "grant", f.Grant, "10", "eth", request)
		}},
		{"allocation fund all", 0, 0, 1, func(db *store.Store, request store.Idempotency) (store.ManagementResponse, error) {
			return db.FundCurrencyIdempotent(t.Context(), "allocation", f.Allocation, "all", "eth", request)
		}},
		{"allocation API key", 0, 1, 0, func(db *store.Store, request store.Idempotency) (store.ManagementResponse, error) {
			return db.CreateKeyIdempotent(t.Context(), f.Allocation, "created key", request)
		}},
		{"grant API key all", 1, 1, 1, func(db *store.Store, request store.Idempotency) (store.ManagementResponse, error) {
			return db.CreateKeyForGrantCurrencyIdempotent(t.Context(), f.Grant, "created key", "all", "eth", request)
		}},
	}
}

type idempotentResult struct {
	response store.ManagementResponse
	err      error
}

func concurrentIdempotent(t *testing.T, f *testutil.Fixture, count int, run func(*store.Store, int) (store.ManagementResponse, error)) []idempotentResult {
	t.Helper()
	connections := make([]*store.Store, count)
	for i := range connections {
		db, err := store.Open(t.Context(), f.Path, false)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		connections[i] = db
	}
	// Every connection is open before any worker can begin its transaction.
	start := make(chan struct{})
	results := make([]idempotentResult, count)
	var workers sync.WaitGroup
	for i, db := range connections {
		workers.Go(func() {
			<-start
			results[i].response, results[i].err = run(db, i)
		})
	}
	close(start)
	workers.Wait()
	for _, db := range connections {
		require.NoError(t, db.Close())
	}
	return results
}

func TestIdempotentMutations(t *testing.T) {
	for index, testCase := range idempotentMutations(t, &testutil.Fixture{}) {
		t.Run(testCase.name, func(t *testing.T) {
			f := testutil.New(t, "100")
			require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "100"))
			tc := idempotentMutations(t, f)[index]
			request := store.Idempotency{Key: "request", Fingerprint: sha256.Sum256([]byte(tc.name))}
			before := f.ManagementState(t)
			tables := []string{"management_idempotency"}
			if tc.posts > 0 {
				tables = append(tables, "ledger_transactions")
			}
			if tc.keys > 0 {
				tables = append(tables, "api_keys")
			}
			for _, table := range tables {
				_, err := f.DB.DB.Exec("CREATE TRIGGER fail_idempotency BEFORE INSERT ON " + table + " BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
				require.NoError(t, err)
				response, err := tc.run(f.DB, request)
				require.ErrorContains(t, err, "fixture failure", table)
				require.Empty(t, response)
				require.Equal(t, before, f.ManagementState(t), table)
				_, err = f.DB.DB.Exec("DROP TRIGGER fail_idempotency")
				require.NoError(t, err)
			}
			// The failed key remains available; simultaneous retries commit once.
			results := concurrentIdempotent(t, f, 8, func(db *store.Store, _ int) (store.ManagementResponse, error) {
				return tc.run(db, request)
			})
			for _, result := range results {
				require.NoError(t, result.err)
				require.Equal(t, results[0].response, result.response)
			}
			after := f.ManagementState(t)
			for table, increase := range map[string]int{
				"grant_allocations": tc.allocations, "api_keys": tc.keys,
				"ledger_transactions": tc.posts, "ledger_entries": 2 * tc.posts, "management_idempotency": 1,
			} {
				require.Len(t, after[table], len(before[table])+increase, table)
			}
			if tc.name == "grant API key all" {
				require.NoError(t, f.DB.Close())
				reopened, err := store.Open(t.Context(), f.Path, false)
				require.NoError(t, err)
				f.DB = reopened
				replay, err := tc.run(f.DB, request)
				require.NoError(t, err)
				require.Equal(t, results[0].response, replay)
				require.Equal(t, after, f.ManagementState(t))
			}
		})
	}
}

func TestConcurrentIdempotencyParameterConflict(t *testing.T) {
	f := testutil.New(t, "100")
	before := f.ManagementState(t)
	results := concurrentIdempotent(t, f, 2, func(db *store.Store, i int) (store.ManagementResponse, error) {
		amount := fmt.Sprint(i + 1)
		return db.FundCurrencyIdempotent(t.Context(), "grant", f.Grant, amount, "eth",
			store.Idempotency{Key: "race", Fingerprint: sha256.Sum256([]byte(amount))})
	})
	successes := 0
	for _, result := range results {
		if result.err == nil {
			successes++
		} else {
			require.ErrorIs(t, result.err, store.ErrManagementConflict)
		}
	}
	require.Equal(t, 1, successes)
	after := f.ManagementState(t)
	require.Len(t, after["ledger_transactions"], len(before["ledger_transactions"])+1)
	require.Len(t, after["management_idempotency"], 1)
}

func TestIdempotencyRejectsInvalidKey(t *testing.T) {
	f := testutil.New(t, "100")
	before := f.ManagementState(t)
	_, err := f.DB.FundCurrencyIdempotent(t.Context(), "grant", f.Grant, "10", "eth", store.Idempotency{Key: "invalid+key"})
	require.ErrorIs(t, err, store.ErrInvalidManagementInput)
	require.Equal(t, before, f.ManagementState(t))
}
