package store

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations
var migrationsFS embed.FS

// Store wraps the connection pool. All state lives in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies pending incremental migrations in filename order. It is
// safe to run repeatedly: applied versions are recorded in
// schema_migrations and skipped. Each migration runs in its own
// transaction. 0001 uses IF NOT EXISTS throughout, so databases created by
// the original (bcc7c845) schema simply no-op through it and then receive
// the incremental upgrades.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
		   version    INT PRIMARY KEY,
		   name       TEXT NOT NULL,
		   applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		 )`); err != nil {
		return fmt.Errorf("migrate: bootstrap schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("migrate: read dir: %w", err)
	}
	names := []string{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		version, err := migrationVersion(name)
		if err != nil {
			return err
		}
		var applied bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
			version).Scan(&applied); err != nil {
			return fmt.Errorf("migrate: check %s: %w", name, err)
		}
		if applied {
			continue
		}
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("migrate: read %s: %w", name, err)
		}
		if err := s.inTx(ctx, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, version, name)
			return err
		}); err != nil {
			return fmt.Errorf("migrate: apply %s: %w", name, err)
		}
	}
	return nil
}

// migrationVersion parses the numeric prefix of "0002_something.sql".
func migrationVersion(name string) (int, error) {
	base, _, found := strings.Cut(name, "_")
	if !found {
		return 0, fmt.Errorf("migrate: bad migration filename %q", name)
	}
	v, err := strconv.Atoi(base)
	if err != nil {
		return 0, fmt.Errorf("migrate: bad migration filename %q: %w", name, err)
	}
	return v, nil
}

// inTx runs fn inside a transaction, committing on success.
func (s *Store) inTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Pool exposes the underlying pool (tests, ops tooling).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
