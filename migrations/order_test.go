package migrations

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"testing/fstest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestFilenameOrderingAndHistory(t *testing.T) {
	original := files
	files = fstest.MapFS{
		"009_create.sql":  {Data: []byte("-- UP\nCREATE TABLE migration_order (value INTEGER NOT NULL CHECK(value>=0)); INSERT INTO migration_order VALUES (0);\n-- DOWN\nDROP TABLE migration_order;")},
		"1000_first.sql":  {Data: []byte("-- UP\nUPDATE migration_order SET value=value+1;\n-- DOWN\nUPDATE migration_order SET value=value-1;")},
		"1000_second.sql": {Data: []byte("-- UP\nUPDATE migration_order SET value=value*2;\n-- DOWN\nUPDATE migration_order SET value=value/2;")},
		"999_last.sql":    {Data: []byte("-- UP\nUPDATE migration_order SET value=value+10;\n-- DOWN\nUPDATE migration_order SET value=value-10;")},
	}
	t.Cleanup(func() { files = original })
	db := openTestDB(t)
	ctx := context.Background()

	status, err := List(ctx, db)
	require.NoError(t, err)
	var names []string
	for _, item := range status {
		names = append(names, item.Filename)
		require.False(t, item.Applied)
	}
	require.Equal(t, []string{"009_create.sql", "1000_first.sql", "1000_second.sql", "999_last.sql"}, names)
	require.NoError(t, Up(ctx, db))
	require.NoError(t, Up(ctx, db))
	var value int
	require.NoError(t, db.QueryRow(`SELECT value FROM migration_order`).Scan(&value))
	require.Equal(t, 12, value)
	status, err = List(ctx, db)
	require.NoError(t, err)
	for _, item := range status {
		require.True(t, item.Applied)
	}

	// A missing file in the applied sequence is a gap even though numeric prefixes may have gaps.
	_, err = db.Exec(`DELETE FROM migrations WHERE filename='1000_first.sql'`)
	require.NoError(t, err)
	_, err = List(ctx, db)
	require.ErrorContains(t, err, "gap before 1000_second.sql")
	require.ErrorContains(t, Up(ctx, db), "gap before 1000_second.sql")
	require.ErrorContains(t, Down(ctx, db), "gap before 1000_second.sql")
	_, err = db.Exec(`INSERT INTO migrations(filename,sha256,applied_at_ms) VALUES (?,?,?)`, status[1].Filename, status[1].SHA256, *status[1].AppliedAtMS)
	require.NoError(t, err)

	for _, want := range []int{2, 1, 0} {
		require.NoError(t, Down(ctx, db))
		require.NoError(t, db.QueryRow(`SELECT value FROM migration_order`).Scan(&value))
		require.Equal(t, want, value)
	}
	require.NoError(t, Down(ctx, db))
	require.NoError(t, Down(ctx, db))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='migration_order'`).Scan(&value))
	require.Zero(t, value)
	status, err = List(ctx, db)
	require.NoError(t, err)
	for _, item := range status {
		require.False(t, item.Applied)
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "migration.db")+"?_foreign_keys=on")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}
