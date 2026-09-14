// Package migrations applies embedded, versioned SQL files in the repository's UP/DOWN format.
package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//go:embed *.sql
var files embed.FS

var migrationName = regexp.MustCompile(`^([0-9]{3,})_[a-z0-9_]+\.sql$`)

type Status struct {
	Version     int    `json:"version"`
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
 version INTEGER PRIMARY KEY CHECK(version > 0),
 filename TEXT NOT NULL UNIQUE,
 sha256 TEXT NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
 applied_at_ms INTEGER NOT NULL
) STRICT`)
	return err
}

func catalog() ([]migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	out := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		matches := migrationName.FindStringSubmatch(entry.Name())
		if matches == nil {
			return nil, fmt.Errorf("invalid migration filename %s", entry.Name())
		}
		version, err := strconv.Atoi(matches[1])
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration version in %s", entry.Name())
		}
		contents, err := files.ReadFile(entry.Name())
		if err != nil {
			return nil, err
		}
		up, down, err := sections(entry.Name(), contents)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(contents)
		out = append(out, migration{Status: Status{Version: version, Filename: entry.Name(), SHA256: hex.EncodeToString(hash[:])}, up: up, down: down})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, item := range out {
		if item.Version != i+1 {
			return nil, fmt.Errorf("migration versions must be contiguous from 1: found %d at %s", item.Version, item.Filename)
		}
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
	rows, err := db.QueryContext(ctx, `SELECT version,filename,sha256,applied_at_ms FROM migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("read migration metadata: %w", err)
	}
	defer rows.Close()
	applied := map[int]Status{}
	for rows.Next() {
		var saved Status
		var appliedAt int64
		if err := rows.Scan(&saved.Version, &saved.Filename, &saved.SHA256, &appliedAt); err != nil {
			return nil, err
		}
		saved.AppliedAtMS = &appliedAt
		saved.Applied = true
		if saved.Version < 1 || saved.Version > len(specs) {
			return nil, fmt.Errorf("database has unknown migration version %d (%s)", saved.Version, saved.Filename)
		}
		expected := specs[saved.Version-1].Status
		if saved.Filename != expected.Filename {
			return nil, fmt.Errorf("migration version %d filename mismatch: database has %s, application expects %s", saved.Version, saved.Filename, expected.Filename)
		}
		if saved.SHA256 != expected.SHA256 {
			return nil, fmt.Errorf("migration %s checksum mismatch", saved.Filename)
		}
		if _, duplicate := applied[saved.Version]; duplicate {
			return nil, fmt.Errorf("duplicate migration version %d", saved.Version)
		}
		applied[saved.Version] = saved
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	missing := false
	out := make([]Status, len(specs))
	for i := range specs {
		out[i] = specs[i].Status
		if saved, ok := applied[out[i].Version]; ok {
			out[i].Applied = true
			out[i].AppliedAtMS = saved.AppliedAtMS
		}
		if !out[i].Applied {
			missing = true
		} else if missing {
			return nil, fmt.Errorf("database migration history has a gap before version %d", out[i].Version)
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
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM migrations WHERE version=?`, item.Version).Scan(&present); err != nil {
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
		_, err = tx.ExecContext(ctx, `DELETE FROM migrations WHERE version=?`, item.Version)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO migrations(version,filename,sha256,applied_at_ms) VALUES (?,?,?,CAST(unixepoch('subsec')*1000 AS INTEGER))`, item.Version, item.Filename, item.SHA256)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}
