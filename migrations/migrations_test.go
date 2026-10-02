package migrations

import (
	"database/sql"
	"testing"
	"testing/fstest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func migrationDB(t *testing.T) (*sql.DB, fstest.MapFS) {
	t.Helper()
	fixtures := fstest.MapFS{
		"001_create.sql": {Data: []byte("-- UP\nCREATE TABLE items (value INTEGER);\n-- DOWN\nDROP TABLE items;")},
		"002_expand.sql": {Data: []byte("-- UP\nALTER TABLE items ADD COLUMN note TEXT DEFAULT 'added';\n-- DOWN\nALTER TABLE items DROP COLUMN note;")},
	}
	original := files
	files = fixtures
	t.Cleanup(func() { files = original })
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db, fixtures
}

func TestMigrationLifecycle(t *testing.T) {
	db, fixtures := migrationDB(t)
	ctx := t.Context()
	list := func() []Status {
		status, err := List(ctx, db)
		require.NoError(t, err)
		return status
	}
	next := fixtures["002_expand.sql"]
	delete(fixtures, "002_expand.sql")
	pending := list()
	require.Len(t, pending, 1)
	require.False(t, pending[0].Applied)
	require.Nil(t, pending[0].AppliedAtMS)
	require.NoError(t, Up(ctx, db))
	_, err := db.Exec("INSERT INTO items VALUES (7)")
	require.NoError(t, err)
	first := list()[0]
	require.Equal(t, 1, first.Version)
	require.Equal(t, "001_create.sql", first.Filename)
	require.Len(t, first.SHA256, 64)
	require.True(t, first.Applied)
	require.NotNil(t, first.AppliedAtMS)
	require.Positive(t, *first.AppliedAtMS)

	// A later release adds another migration to the existing database.
	fixtures["002_expand.sql"] = next
	pending = list()
	require.Len(t, pending, 2)
	require.Equal(t, first, pending[0])
	require.False(t, pending[1].Applied)
	_, err = db.Exec("CREATE TRIGGER fail_metadata BEFORE INSERT ON migrations BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
	require.NoError(t, err)
	require.ErrorContains(t, Up(ctx, db), "fixture failure")
	require.Equal(t, pending, list())
	var note string
	require.ErrorContains(t, db.QueryRow("SELECT note FROM items").Scan(&note), "no such column")
	_, err = db.Exec("DROP TRIGGER fail_metadata")
	require.NoError(t, err)
	require.NoError(t, Up(ctx, db))
	applied := list()
	require.Equal(t, first, applied[0])
	require.True(t, applied[1].Applied)
	var value int
	require.NoError(t, db.QueryRow("SELECT value,note FROM items").Scan(&value, &note))
	require.Equal(t, 7, value)
	require.Equal(t, "added", note)
	require.NoError(t, Up(ctx, db))
	require.Equal(t, applied, list())

	require.NoError(t, Down(ctx, db))
	require.Equal(t, pending, list())
	require.NoError(t, db.QueryRow("SELECT value FROM items").Scan(&value))
	require.Equal(t, 7, value)
	require.ErrorContains(t, db.QueryRow("SELECT note FROM items").Scan(&note), "no such column")
	require.NoError(t, Down(ctx, db))
	require.NoError(t, Down(ctx, db))
	for _, item := range list() {
		require.False(t, item.Applied)
	}
	require.ErrorContains(t, db.QueryRow("SELECT value FROM items").Scan(&value), "no such table")
	require.NoError(t, Up(ctx, db))
}

func TestMigrationMetadataMismatch(t *testing.T) {
	for _, tc := range []struct{ name, query, want string }{
		{"filename", "UPDATE migrations SET filename='001_other.sql' WHERE version=1", "filename mismatch"},
		{"checksum", "UPDATE migrations SET sha256=lower(hex(zeroblob(32))) WHERE version=1", "checksum mismatch"},
		{"unknown version", "UPDATE migrations SET version=3 WHERE version=2", "unknown migration version"},
		{"history gap", "DELETE FROM migrations WHERE version=1", "history has a gap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := migrationDB(t)
			require.NoError(t, Up(t.Context(), db))
			_, err := db.Exec(tc.query)
			require.NoError(t, err)
			_, err = List(t.Context(), db)
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, Up(t.Context(), db), tc.want)
			require.ErrorContains(t, Down(t.Context(), db), tc.want)
		})
	}
}
