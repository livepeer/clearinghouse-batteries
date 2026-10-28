package store

import (
	"math/big"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsageManifestQueryPlan(t *testing.T) {
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "manifest.db"), true)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	for _, after := range []int64{0, 10} {
		query, args, err := listQuery("usage", ListOptions{ManifestID: "manifest-1", AfterSeq: after, Limit: 101})
		require.NoError(t, err)
		plan, err := db.Rows(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
		require.NoError(t, err)
		var manifestPlan string
		for _, step := range plan {
			detail := step["detail"].(string)
			require.NotRegexp(t, `(?i)\b(?:TEMP(?:ORARY)?\s+B-TREE|SORT)\b`, detail)
			if strings.Contains(strings.ToLower(detail), "usage_manifest") {
				manifestPlan = detail
			}
		}
		// Check constraints on this index, allowing whitespace and rowid aliases to vary.
		require.Regexp(t, `(?i)\bmanifest_id\s*=\s*\?`, manifestPlan)
		if after > 0 {
			require.Regexp(t, `(?i)\b(?:seq|rowid|_rowid_|oid)\s*>\s*\?`, manifestPlan)
		}
	}
}

func TestManifestFilterRequiresUsageResource(t *testing.T) {
	for _, kind := range []string{"grant", "allocation", "api-key", "session", "settlement"} {
		_, _, err := listQuery(kind, ListOptions{ManifestID: "manifest-1"})
		require.ErrorIs(t, err, ErrInvalidManagementInput)
	}
}

func TestMetricQueryPlans(t *testing.T) {
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "metrics.db"), true)
	require.NoError(t, err)
	defer db.Close()
	for _, test := range []struct {
		query, index string
		args         []any
	}{
		{negativeExtremeQuery, "metric_balances_negative", []any{"allocation_available", "eth"}},
		{nonnegativeExtremeQuery, "metric_balances_nonnegative", []any{"escrow_deposit", "eth"}},
		{oldestUnmatchedQuery, "settlements_oldest_unmatched", nil},
		{chainStartQuery, "escrow_snapshots_block", nil},
	} {
		t.Run(test.index, func(t *testing.T) {
			rows, err := db.Rows(t.Context(), "EXPLAIN QUERY PLAN "+test.query, test.args...)
			require.NoError(t, err)
			var details string
			for _, row := range rows {
				details += row["detail"].(string) + "\n"
			}
			require.Contains(t, details, test.index)
			require.NotContains(t, details, "TEMP B-TREE")
		})
	}
}

func TestMetricExtremaExactDecimalOrdering(t *testing.T) {
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "extrema.db"), true)
	require.NoError(t, err)
	defer db.Close()
	values := []string{"0", "1", "2", "9", "10", "11", "99", "100", strings.Repeat("9", 256), "1" + strings.Repeat("0", 256)}
	for _, value := range append([]string{}, values[1:]...) {
		values = append(values, "-"+value)
	}
	for _, value := range values {
		_, err := db.DB.Exec(`INSERT INTO metric_balances VALUES ('escrow_deposit',?,'eth',?,?)`, value, value, value)
		require.NoError(t, err)
	}
	_, err = db.DB.Exec(`INSERT INTO metric_balances VALUES ('escrow_deposit','unrelated','usd','other','-999999')`)
	require.NoError(t, err)
	sort.Slice(values, func(i, j int) bool {
		a, _ := new(big.Int).SetString(values[i], 10)
		b, _ := new(big.Int).SetString(values[j], 10)
		return a.Cmp(b) < 0
	})
	for _, want := range values {
		tx, err := db.DB.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		n, err := metricExtreme(t.Context(), tx, "escrow_deposit", "eth", true)
		require.NoError(t, err)
		require.NotNil(t, n)
		require.Equal(t, want, n.String())
		require.NoError(t, tx.Commit())
		_, err = db.DB.Exec(`DELETE FROM metric_balances WHERE source_id=? AND currency='eth'`, want)
		require.NoError(t, err)
	}
	tx, err := db.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	n, err := metricExtreme(t.Context(), tx, "escrow_deposit", "eth", true)
	require.NoError(t, err)
	require.Nil(t, n)
}
