// Package storage implements the Satellite Poller's persistent state using SQLite.
package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed 001_init.sql
var initialMigration string

// Open opens the SQLite database, applies connection settings, and runs migrations.
// Use a path on the Kubernetes Local PV, for example:
// /var/lib/satellite-poller/state.db
func Open(ctx context.Context, path string) (*sql.DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("SQLite database path is required")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, fmt.Errorf("create SQLite directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}

	// SQLite supports one writer at a time. A single connection avoids
	// connection-local PRAGMA inconsistencies and serializes this process's writes.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite database: %w", err)
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("apply SQLite setting %q: %w", pragma, err)
		}
	}

	if err := Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Migrate applies the embedded initial schema once. SQLite DDL is executed
// inside a transaction so a failed migration does not leave a partial schema.
func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("SQLite database is nil")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	var applied int
	err = tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM schema_migrations WHERE version = 1",
	).Scan(&applied)
	if err != nil {
		return fmt.Errorf("check migration version: %w", err)
	}
	if applied > 0 {
		return tx.Commit()
	}

	// The migration contains simple DDL statements separated by semicolons.
	for _, statement := range strings.Split(initialMigration, ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute initial migration: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO schema_migrations(version, applied_at) VALUES(1, CURRENT_TIMESTAMP)",
	); err != nil {
		return fmt.Errorf("record initial migration: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
