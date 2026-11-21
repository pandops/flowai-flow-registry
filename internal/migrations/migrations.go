// Package migrations creates and evolves flow-registry's own schema only.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

//go:embed sql/*.sql
var files embed.FS

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Apply migrates the named schema; it never touches another schema. The
// deployment pre-creates the schema and gives the role no CREATE privilege
// on the database, so CREATE SCHEMA runs only when the schema is missing.
func Apply(ctx context.Context, db *sql.DB, schema string) error {
	if !schemaName.MatchString(schema) {
		return fmt.Errorf("migrations: invalid schema name %q", schema)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema).Scan(&exists); err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	if !exists {
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+quote(schema)); err != nil {
			return fmt.Errorf("migrations: create schema %s: %w", schema, err)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	defer tx.Rollback()
	for _, q := range []string{
		"SET LOCAL search_path TO " + quote(schema),
		"SELECT pg_advisory_xact_lock(hashtext('flow-registry migrations'))",
		"CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())",
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	names, err := files.ReadDir("sql")
	if err != nil {
		return err
	}
	var versions []string
	for _, n := range names {
		versions = append(versions, n.Name())
	}
	sort.Strings(versions)
	for _, v := range versions {
		var done bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", v).Scan(&done); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
		if done {
			continue
		}
		body, err := files.ReadFile("sql/" + v)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("migrations: %s: %w", v, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", v); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	return tx.Commit()
}

func quote(ident string) string { return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"` }
