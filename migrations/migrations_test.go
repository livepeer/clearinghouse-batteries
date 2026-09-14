package migrations_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/migrations"
)

func TestFailedMetadataWriteRollsBackEntireMigration(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "migration.db"), true)
	testutil.Must(t, err)
	defer db.Close()
	testutil.Must(t, migrations.Down(ctx, db.DB))
	_, err = db.DB.Exec(`CREATE TRIGGER fail_metadata BEFORE INSERT ON migrations BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	testutil.Must(t, err)
	if err := migrations.Up(ctx, db.DB); err == nil {
		t.Fatal("expected failure")
	}
	var n int
	testutil.Must(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('grants','account_balances')`).Scan(&n))
	if n != 0 {
		t.Fatal("failed migration left schema changes")
	}
	_, err = db.DB.Exec(`DROP TRIGGER fail_metadata`)
	testutil.Must(t, err)
	testutil.Must(t, migrations.Up(ctx, db.DB))
	status, err := migrations.List(ctx, db.DB)
	testutil.Must(t, err)
	if len(status) != 1 || status[0].Version != 1 || status[0].Filename != "001_initial.sql" || len(status[0].SHA256) != 64 || status[0].AppliedAtMS == nil || !status[0].Applied {
		t.Fatal(status)
	}
	testutil.Must(t, db.DB.QueryRow(`SELECT count(*) FROM account_balances`).Scan(&n))
	if n != 0 {
		t.Fatal("new database has unexpected balances", n)
	}
	var appliedAt int64
	testutil.Must(t, db.DB.QueryRow(`SELECT applied_at_ms FROM migrations WHERE version=1`).Scan(&appliedAt))
	if appliedAt <= 0 {
		t.Fatal(appliedAt)
	}
	testutil.Must(t, migrations.Down(ctx, db.DB))
	if status, err := migrations.List(ctx, db.DB); err != nil || len(status) != 1 || status[0].Applied {
		t.Fatal(status, err)
	}
	testutil.Must(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='account_balances'`).Scan(&n))
	if n != 0 {
		t.Fatal("migration down left balance table")
	}
	testutil.Must(t, migrations.Up(ctx, db.DB))
	_, err = db.DB.Exec(`INSERT INTO migrations(version,filename,sha256,applied_at_ms) VALUES (999,'999_unknown.sql',?,0)`, strings.Repeat("0", 64))
	testutil.Must(t, err)
	if _, err := migrations.List(ctx, db.DB); err == nil {
		t.Fatal("accepted unknown schema version")
	}
}

func TestMigrationFilenameMismatch(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "migration.db"), true)
	testutil.Must(t, err)
	defer db.Close()
	_, err = db.DB.Exec(`UPDATE migrations SET filename='001_other.sql' WHERE version=1`)
	testutil.Must(t, err)
	if _, err := migrations.List(ctx, db.DB); err == nil || !strings.Contains(err.Error(), "filename mismatch") {
		t.Fatal(err)
	}
}

func TestMigrationChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "migration.db"), true)
	testutil.Must(t, err)
	defer db.Close()
	_, err = db.DB.Exec(`UPDATE migrations SET sha256=? WHERE version=1`, strings.Repeat("0", 64))
	testutil.Must(t, err)
	if _, err := migrations.List(ctx, db.DB); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatal(err)
	}
	if err := migrations.Up(ctx, db.DB); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatal(err)
	}
}
