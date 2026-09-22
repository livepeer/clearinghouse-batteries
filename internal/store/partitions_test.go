package store_test

import (
	"context"
	"math"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestPartitionCheckpointFailureRollsBackAccounting(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 0, f.Event(t, "zero", "10", testutil.PM)))
	require.NoError(t, f.DB.Ingest(ctx, "events", 1, 0, f.Event(t, "one", "20", testutil.PM)))
	_, err := f.DB.DB.Exec(`CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON ingestion_checkpoints
 WHEN NEW.source='kafka' AND NEW.stream='events:1'
 BEGIN SELECT RAISE(ABORT,'checkpoint failure'); END`)
	require.NoError(t, err)
	raw := f.Event(t, "retry", "5", testutil.PM)
	require.ErrorContains(t, f.DB.Ingest(ctx, "events", 1, 1, raw), "checkpoint failure")
	for _, partition := range []int{0, 1} {
		next, _, found, err := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("events", partition))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, int64(1), next)
	}
	var usages, authorizations int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM usage_events`).Scan(&usages))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&authorizations))
	require.Equal(t, 2, usages)
	require.Equal(t, 2, authorizations)
	balance, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	require.Equal(t, "70", balance.String())
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_checkpoint`)
	require.NoError(t, err)
	require.NoError(t, f.DB.Ingest(ctx, "events", 1, 1, raw))
	// Partition 1's progress cannot permit a gap in partition 0.
	require.ErrorContains(t, f.DB.Ingest(ctx, "events", 0, 2, raw), "offset gap")
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 1, raw))
	balance, err = store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	require.Equal(t, "65", balance.String())
}

func TestInvalidKafkaPartition(t *testing.T) {
	f := testutil.New(t, "100")
	for _, partition := range []int{-1, math.MaxInt32 + 1} {
		require.ErrorContains(t, f.DB.Ingest(context.Background(), "events", partition, 0, []byte(`{}`)), "invalid Kafka partition")
	}
}
