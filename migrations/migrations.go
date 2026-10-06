// Package migrations applies embedded SQL files using the repository's UP/DOWN format.
package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
)

//go:embed *.sql
var embeddedFiles embed.FS

var files fs.FS = embeddedFiles

var migrationName = regexp.MustCompile(`^[0-9]{3,}_[a-z0-9_]+\.sql$`)

type Status struct {
	Filename    string `json:"filename"`
	SHA256      string `json:"sha256"`
	AppliedAtMS *int64 `json:"applied_at_ms"`
	Applied     bool   `json:"applied"`
}

type migration struct {
	Status
	up, down string
}

func initTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS migrations (
 filename TEXT NOT NULL PRIMARY KEY,
 sha256 TEXT NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
 applied_at_ms INTEGER NOT NULL
) STRICT`)
	return err
}

func catalog() ([]migration, error) {
	// ReadDir returns entries in lexical filename order.
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, err
	}
	out := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if !migrationName.MatchString(entry.Name()) {
			return nil, fmt.Errorf("invalid migration filename %s", entry.Name())
		}
		contents, err := fs.ReadFile(files, entry.Name())
		if err != nil {
			return nil, err
		}
		up, down, err := sections(entry.Name(), contents)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(contents)
		out = append(out, migration{Status: Status{Filename: entry.Name(), SHA256: hex.EncodeToString(hash[:])}, up: up, down: down})
	}
	return out, nil
}

func List(ctx context.Context, db *sql.DB) ([]Status, error) {
	specs, err := catalog()
	if err != nil {
		return nil, err
	}
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='migrations'`).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		out := make([]Status, len(specs))
		for i := range specs {
			out[i] = specs[i].Status
		}
		return out, nil
	}
	known := make(map[string]Status, len(specs))
	for _, item := range specs {
		known[item.Filename] = item.Status
	}
	rows, err := db.QueryContext(ctx, `SELECT filename,sha256,applied_at_ms FROM migrations ORDER BY filename`)
	if err != nil {
		return nil, fmt.Errorf("read migration metadata: %w", err)
	}
	defer rows.Close()
	applied := map[string]Status{}
	for rows.Next() {
		var saved Status
		var appliedAt int64
		if err := rows.Scan(&saved.Filename, &saved.SHA256, &appliedAt); err != nil {
			return nil, err
		}
		saved.AppliedAtMS = &appliedAt
		saved.Applied = true
		expected, ok := known[saved.Filename]
		if !ok {
			return nil, fmt.Errorf("database has unknown migration %s", saved.Filename)
		}
		if saved.SHA256 != expected.SHA256 {
			return nil, fmt.Errorf("migration %s checksum mismatch", saved.Filename)
		}
		if _, duplicate := applied[saved.Filename]; duplicate {
			return nil, fmt.Errorf("duplicate migration %s", saved.Filename)
		}
		applied[saved.Filename] = saved
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	missing := false
	out := make([]Status, len(specs))
	for i := range specs {
		out[i] = specs[i].Status
		if saved, ok := applied[out[i].Filename]; ok {
			out[i].Applied = true
			out[i].AppliedAtMS = saved.AppliedAtMS
		}
		if !out[i].Applied {
			missing = true
		} else if missing {
			return nil, fmt.Errorf("database migration history has a gap before %s", out[i].Filename)
		}
	}
	return out, nil
}

func sections(name string, contents []byte) (string, string, error) {
	s := strings.TrimSpace(string(contents))
	up, down, ok := strings.Cut(s, "-- DOWN")
	if !ok || !strings.HasPrefix(up, "-- UP") || strings.Contains(down, "-- DOWN") {
		return "", "", fmt.Errorf("invalid UP/DOWN migration %s", name)
	}
	return strings.TrimPrefix(up, "-- UP"), down, nil
}

func Up(ctx context.Context, db *sql.DB) error {
	if err := initTable(ctx, db); err != nil {
		return err
	}
	list, err := List(ctx, db)
	if err != nil {
		return err
	}
	specs, err := catalog()
	if err != nil {
		return err
	}
	for i, status := range list {
		if status.Applied {
			continue
		}
		if err := apply(ctx, db, specs[i], false); err != nil {
			return err
		}
	}
	return nil
}

func Down(ctx context.Context, db *sql.DB) error {
	if err := initTable(ctx, db); err != nil {
		return err
	}
	list, err := List(ctx, db)
	if err != nil {
		return err
	}
	specs, err := catalog()
	if err != nil {
		return err
	}
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Applied {
			return apply(ctx, db, specs[i], true)
		}
	}
	return nil
}

func apply(ctx context.Context, db *sql.DB, item migration, down bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var present int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM migrations WHERE filename=?`, item.Filename).Scan(&present); err != nil {
		return err
	}
	if (!down && present != 0) || (down && present == 0) {
		return tx.Commit()
	}
	script := item.up
	if down {
		script = item.down
	}
	if _, err = tx.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("migration %s: %w", item.Filename, err)
	}
	if down {
		_, err = tx.ExecContext(ctx, `DELETE FROM migrations WHERE filename=?`, item.Filename)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO migrations(filename,sha256,applied_at_ms) VALUES (?,?,CAST(unixepoch('subsec')*1000 AS INTEGER))`, item.Filename, item.SHA256)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}
