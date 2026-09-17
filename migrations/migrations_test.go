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
	require.NoError(t, migrations.Down(ctx, db.DB))
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
	if len(status) != 1 || status[0].Version != 1 || status[0].Filename != "001_initial.sql" || len(status[0].SHA256) != 64 || status[0].AppliedAtMS == nil || !status[0].Applied {
		t.Fatal(status)
	}
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM account_balances`).Scan(&n))
	if n != 0 {
		t.Fatal("new database has unexpected balances", n)
	}
	var appliedAt int64
	require.NoError(t, db.DB.QueryRow(`SELECT applied_at_ms FROM migrations WHERE version=1`).Scan(&appliedAt))
	if appliedAt <= 0 {
		t.Fatal(appliedAt)
	}
	require.NoError(t, migrations.Down(ctx, db.DB))
	if status, err := migrations.List(ctx, db.DB); err != nil || len(status) != 1 || status[0].Applied {
		t.Fatal(status, err)
	}
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='account_balances'`).Scan(&n))
	if n != 0 {
		t.Fatal("migration down left balance table")
	}
	require.NoError(t, migrations.Up(ctx, db.DB))
	_, err = db.DB.Exec(`INSERT INTO migrations(version,filename,sha256,applied_at_ms) VALUES (999,'999_unknown.sql',?,0)`, strings.Repeat("0", 64))
	require.NoError(t, err)
	if _, err := migrations.List(ctx, db.DB); err == nil {
		t.Fatal("accepted unknown schema version")
	}
}

func TestMigrationMetadataMismatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		column     string
		value      string
		want       string
		upMustFail bool
	}{
		{name: "filename", column: "filename", value: "001_other.sql", want: "filename mismatch"},
		{name: "checksum", column: "sha256", value: strings.Repeat("0", 64), want: "checksum mismatch", upMustFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := store.Open(ctx, filepath.Join(t.TempDir(), "migration.db"), true)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.DB.Exec(`UPDATE migrations SET `+tc.column+`=? WHERE version=1`, tc.value)
			require.NoError(t, err)
			_, err = migrations.List(ctx, db.DB)
			require.ErrorContains(t, err, tc.want)
			if tc.upMustFail {
				require.ErrorContains(t, migrations.Up(ctx, db.DB), tc.want)
			}
		})
	}
}
