package migrations_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/migrations"
	"github.com/stretchr/testify/require"
)

func TestFailedMetadataWriteRollsBackEntireMigration(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "migration.db"), true)
	require.NoError(t, err)
	defer db.Close()
	pending, err := migrations.List(ctx, db.DB)
	require.NoError(t, err)
	require.NotEmpty(t, pending)
	for range pending {
		require.NoError(t, migrations.Down(ctx, db.DB))
	}
	_, err = db.DB.Exec(`CREATE TRIGGER fail_metadata BEFORE INSERT ON migrations BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	require.NoError(t, err)
	if err := migrations.Up(ctx, db.DB); err == nil {
		t.Fatal("expected failure")
	}
	var n int
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('grants','account_balances')`).Scan(&n))
	if n != 0 {
		t.Fatal("failed migration left schema changes")
	}
	_, err = db.DB.Exec(`DROP TRIGGER fail_metadata`)
	require.NoError(t, err)
	require.NoError(t, migrations.Up(ctx, db.DB))
	status, err := migrations.List(ctx, db.DB)
	require.NoError(t, err)
	require.Len(t, status, len(pending))
	for i, item := range status {
		require.Equal(t, pending[i].Filename, item.Filename)
		require.Equal(t, pending[i].SHA256, item.SHA256)
		require.Len(t, item.SHA256, 64)
		require.NotNil(t, item.AppliedAtMS)
		require.True(t, item.Applied)
		var appliedAt int64
		require.NoError(t, db.DB.QueryRow(`SELECT applied_at_ms FROM migrations WHERE filename=?`, item.Filename).Scan(&appliedAt))
		require.Positive(t, appliedAt)
		require.Equal(t, *item.AppliedAtMS, appliedAt)
	}
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM migrations`).Scan(&n))
	require.Equal(t, len(status), n)
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM account_balances`).Scan(&n))
	if n != 0 {
		t.Fatal("new database has unexpected balances", n)
	}
	for range pending {
		require.NoError(t, migrations.Down(ctx, db.DB))
	}
	status, err = migrations.List(ctx, db.DB)
	require.NoError(t, err)
	require.Len(t, status, len(pending))
	for _, item := range status {
		require.False(t, item.Applied)
		require.Nil(t, item.AppliedAtMS)
	}
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM migrations`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND name <> 'migrations'`).Scan(&n))
	require.Zero(t, n, "migration down left schema objects")
	require.NoError(t, migrations.Up(ctx, db.DB))
	_, err = db.DB.Exec(`INSERT INTO migrations(filename,sha256,applied_at_ms) VALUES ('999_unknown.sql',?,0)`, strings.Repeat("0", 64))
	require.NoError(t, err)
	if _, err := migrations.List(ctx, db.DB); err == nil {
		t.Fatal("accepted unknown migration filename")
	}
}

func TestMigrationMetadataMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column string
		value  string
		want   string
	}{
		{name: "filename", column: "filename", value: "001_other.sql", want: "unknown migration"},
		{name: "checksum", column: "sha256", value: strings.Repeat("0", 64), want: "checksum mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "migration.db"), true)
			require.NoError(t, err)
			defer db.Close()
			status, err := migrations.List(ctx, db.DB)
			require.NoError(t, err)
			require.NotEmpty(t, status)

			_, err = db.DB.Exec(`UPDATE migrations SET `+tc.column+`=? WHERE filename=?`, tc.value, status[0].Filename)
			require.NoError(t, err)
			_, err = migrations.List(ctx, db.DB)
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, migrations.Up(ctx, db.DB), tc.want)
		})
	}
}
