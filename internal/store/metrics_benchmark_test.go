package store_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/migrations"
	"github.com/stretchr/testify/require"
)

// Each dataset is seeded as legacy data, then upgraded before timing reads.
func BenchmarkMetricSnapshot(b *testing.B) {
	for _, dataset := range []struct {
		kind string
		rows int
	}{
		{"settlements", 10000}, {"settlements", 100000}, {"settlements", 1000000}, {"settlements", 3000000},
		{"quarantines", 10000}, {"quarantines", 1000000},
		{"accounts", 1000}, {"accounts", 100000},
		{"signers", 100}, {"signers", 10000},
	} {
		b.Run(fmt.Sprintf("%s=%d", dataset.kind, dataset.rows), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "scale.db")
			db, err := store.Open(b.Context(), path, true)
			require.NoError(b, err)
			defer db.Close()
			require.NoError(b, migrations.Down(b.Context(), db.DB))
			seedMetricDataset(b, db, dataset.kind, dataset.rows)
			_, err = db.DB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
			require.NoError(b, err)
			before, err := os.Stat(path)
			require.NoError(b, err)
			start := time.Now()
			require.NoError(b, migrations.Up(b.Context(), db.DB))
			backfill := time.Since(start)
			_, err = db.DB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
			require.NoError(b, err)
			after, err := os.Stat(path)
			require.NoError(b, err)
			b.Logf("backfill=%s added_database_bytes=%d", backfill, after.Size()-before.Size())
			reader, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_query_only=1&_txlock=deferred&_busy_timeout=1000")
			require.NoError(b, err)
			defer reader.Close()
			reader.SetMaxOpenConns(1)
			for _, collector := range []string{"scan", "summary"} {
				b.Run(collector, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if collector == "scan" {
							testutil.ReferenceMetricSnapshot(b, reader)
						} else {
							_, err := store.MetricSnapshot(b.Context(), reader, true, true)
							require.NoError(b, err)
						}
					}
					b.ReportMetric(float64(backfill.Microseconds())/1000, "backfill-ms")
					b.ReportMetric(float64(after.Size()-before.Size()), "projection-B")
				})
			}
		})
	}
}

func seedMetricDataset(t testing.TB, db *store.Store, kind string, n int) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.DB.ExecContext(t.Context(), query, args...)
		require.NoError(t, err)
	}
	const numbers = `WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM nums WHERE x<?) `
	switch kind {
	case "settlements":
		exec(numbers+`INSERT INTO settlements(id,chain_id,contract_address,tx_hash,log_index,block_number,block_hash,sender,recipient,face_value_wei,paid_amount_wei,deposit_paid_wei,reserve_paid_wei,win_probability,sender_nonce,recipient_rand,pm_session_id,aux_data,match_status,status,created_at_ms,settled_at_ms)
 SELECT printf('settlement-%09d',x),'42161',?,?,x,x,?,?,?,'1000000000000000000',CASE WHEN x%5=0 THEN '750000000000000000' ELSE '1000000000000000000' END,CASE WHEN x%5=0 THEN '750000000000000000' ELSE '1000000000000000000' END,'0','1','0','0',?,'0x','unmatched',CASE WHEN x%100=0 THEN 'orphaned' ELSE 'settled' END,1800000000000+x,1800000000000+x FROM nums`, n, testutil.Contract, testutil.PM, testutil.PM, testutil.Sender, testutil.Orch, testutil.PM)
	case "quarantines":
		exec(numbers+`INSERT INTO usage_events(id,topic,partition,offset,raw_payload,status,error,created_at_ms) SELECT printf('usage-%09d',x),'probe',0,x,zeroblob(1024),'quarantined','malformed envelope or missing event id/type/data',1800000000000+x FROM nums`, n)
	case "accounts":
		exec(numbers+`INSERT INTO grants(id,name,total_units,currency,status,created_at_ms) SELECT printf('grant-%09d',x),'probe','1000000000000000000',CASE WHEN x%2=0 THEN 'eth' ELSE 'usd' END,'active',x FROM nums`, n)
		exec(`INSERT INTO grant_allocations(id,grant_id,name,allocated_units,currency,status,created_at_ms) SELECT 'allocation-'||id,id,'probe','1000000000000000000',currency,'active',seq FROM grants`)
		exec(`INSERT INTO account_balances SELECT 'grant_unallocated',id,currency,'1000000000000000000' FROM grants`)
		exec(`INSERT INTO account_balances SELECT 'allocation_available',id,currency,CASE WHEN seq%10=0 THEN '-1000000000000000000' ELSE '1000000000000000000' END FROM grant_allocations`)
		exec(`INSERT INTO account_balances SELECT 'allocation_spent',id,currency,'1000000000000000000' FROM grant_allocations`)
	case "signers":
		exec(numbers+`INSERT INTO escrow_snapshots SELECT printf('snapshot-%09d',x),'chain','42161',?,printf('0x%040x',x),-1,'','0','0',x FROM nums`, n, testutil.Contract)
		exec(`INSERT INTO account_balances SELECT 'escrow_deposit',chain_id||':'||contract_address||':'||sender,'eth',CASE WHEN created_at_ms%2=0 THEN '1000000000000000000' ELSE '0' END FROM escrow_snapshots`)
	}
}
