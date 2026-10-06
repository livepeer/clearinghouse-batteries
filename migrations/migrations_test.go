package migrations_test

// These tests should exercise the migrator itself, not app contents

import (
	"database/sql"
	"testing"

	"github.com/livepeer/clearinghouse/migrations"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func migrationDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?_foreign_keys=on")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestMigrationRoundTrip(t *testing.T) {
	ctx, db := t.Context(), migrationDB(t)
	pending, err := migrations.List(ctx, db)
	require.NoError(t, err)
	require.NotEmpty(t, pending)
	for _, item := range pending {
		require.NotEmpty(t, item.Filename)
		require.Len(t, item.SHA256, 64)
		require.False(t, item.Applied)
		require.Nil(t, item.AppliedAtMS)
	}
	require.NoError(t, migrations.Up(ctx, db))
	applied, err := migrations.List(ctx, db)
	require.NoError(t, err)
	require.Len(t, applied, len(pending))
	for i, item := range applied {
		require.NotNil(t, item.AppliedAtMS)
		require.Positive(t, *item.AppliedAtMS)
		want := pending[i]
		want.Applied, want.AppliedAtMS = true, item.AppliedAtMS
		require.Equal(t, want, item)
	}
	// Applying an already current catalog leaves its metadata unchanged.
	require.NoError(t, migrations.Up(ctx, db))
	again, err := migrations.List(ctx, db)
	require.NoError(t, err)
	require.Equal(t, applied, again)
	for i := len(applied) - 1; i >= 0; i-- {
		require.NoError(t, migrations.Down(ctx, db))
		applied[i] = pending[i]
		status, err := migrations.List(ctx, db)
		require.NoError(t, err)
		require.Equal(t, applied, status)
	}
	require.NoError(t, migrations.Down(ctx, db))
	require.NoError(t, migrations.Up(ctx, db))
}

func TestFailedMetadataWriteRollsBackEntireMigration(t *testing.T) {
	ctx, db := t.Context(), migrationDB(t)
	// Initialize the migrator's metadata table without applying any migrations.
	require.NoError(t, migrations.Down(ctx, db))
	pending, err := migrations.List(ctx, db)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TRIGGER fail_metadata BEFORE INSERT ON migrations BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	require.NoError(t, err)
	var before, after int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&before))
	require.ErrorContains(t, migrations.Up(ctx, db), "fixture failure")
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&after))
	require.Equal(t, before, after, "failed migration left schema changes")
	status, err := migrations.List(ctx, db)
	require.NoError(t, err)
	require.Equal(t, pending, status)
	_, err = db.Exec(`DROP TRIGGER fail_metadata`)
	require.NoError(t, err)
	require.NoError(t, migrations.Up(ctx, db))
	status, err = migrations.List(ctx, db)
	require.NoError(t, err)
	for _, item := range status {
		require.True(t, item.Applied)
	}
}

func TestMigrationMetadataMismatch(t *testing.T) {
	for _, tc := range []struct{ name, query, want string }{
		{"filename", `UPDATE migrations SET filename='renamed.sql' WHERE filename=?`, "unknown migration"},
		{"checksum", `UPDATE migrations SET sha256=lower(hex(zeroblob(32))) WHERE filename=?`, "checksum mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := t.Context(), migrationDB(t)
			require.NoError(t, migrations.Up(ctx, db))
			status, err := migrations.List(ctx, db)
			require.NoError(t, err)
			require.NotEmpty(t, status)
			_, err = db.Exec(tc.query, status[0].Filename)
			require.NoError(t, err)
			_, err = migrations.List(ctx, db)
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, migrations.Up(ctx, db), tc.want)
			require.ErrorContains(t, migrations.Down(ctx, db), tc.want)
		})
	}
}
