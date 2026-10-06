package store

import (
	"path/filepath"
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
