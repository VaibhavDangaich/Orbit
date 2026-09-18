// Package store is orbit's data-access layer: the only place that knows
// SQL exists. internal/job stays free of any Postgres import -- domain
// logic and persistence are separate packages on purpose, so the domain
// types can be unit-tested with zero database, and swapping storage later
// (unlikely here, but the point stands) touches only this package.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pooled connection to Postgres. All of its methods take a
// context.Context as their first argument -- Go convention, not a
// stylistic choice: ctx is how a caller propagates "give up after 3
// seconds" or "the incoming HTTP request was cancelled, stop working" down
// through every layer to the actual network call. Compare it to an
// AbortController's signal in JS fetch -- same idea, but woven through the
// entire standard library (DB drivers, net/http, exec.Command all accept
// one) instead of being fetch-specific.
type Store struct {
	pool *pgxpool.Pool
}

// DefaultDevDSN matches deploy/compose/docker-compose.yml. It's the single
// source of truth for local-dev connection details -- cmd/scheduler,
// cmd/worker, and the store's own tests all fall back to this if
// ORBIT_DATABASE_URL / ORBIT_TEST_DATABASE_URL isn't set. Duplicating this
// string in three places is exactly how we ended up debugging a port
// mismatch earlier (localhost:5432 colliding with a native Postgres
// install) -- one constant instead means a future port change can't drift.
const DefaultDevDSN = "postgres://scheduler:scheduler@localhost:5433/scheduler?sslmode=disable"

// New opens a connection pool to Postgres and verifies it's reachable.
//
// A pool, not a single connection: opening a fresh TCP connection plus
// doing Postgres's auth handshake costs real time (multiple round trips),
// so paying that cost per-query would be disastrous under load. pgxpool
// keeps a bounded set of already-authenticated connections open and hands
// them out to goroutines that need one, returning them to the pool when
// done -- the same idea as an HTTP keep-alive pool, applied to SQL
// connections.
//
// The bound matters for a system-design reason, not just performance:
// Postgres itself has a hard cap on total concurrent connections
// (max_connections, often 100). If every service instance opened
// connections without limit, a traffic spike could exhaust that cap and
// take the database down for every OTHER service sharing it too. Setting
// MaxConns here is us deciding, up front, how much of that shared budget
// this service is allowed to consume.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	cfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pool: %w", err)
	}

	// NewWithConfig doesn't actually dial anything -- it's lazy. Ping
	// forces one real connection attempt now, so New() fails fast with a
	// clear error if Postgres is down, instead of the caller finding out
	// three unrelated function calls later on the first real query.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close releases every pooled connection. Callers should defer this right
// after New succeeds.
func (s *Store) Close() {
	s.pool.Close()
}

// Ping reports whether Postgres is currently reachable, acquiring a
// connection from the pool and round-tripping to the server rather than
// just reporting that a pool object exists.
//
// This backs the readiness probe. Postgres is the one dependency whose
// loss stops both binaries from doing anything useful at all: a scheduler
// cannot materialize runs and a worker cannot claim or complete them. The
// other dependencies deliberately do NOT gate readiness -- see the
// comment on metrics.Serve for why.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// testLockKey is the arbitrary, fixed key every caller of TruncateForTest
// agrees on -- Postgres advisory locks are keyed by a plain int64, no
// registry involved, just "use the same number."
const testLockKey = 851917

// TruncateForTest wipes job_runs and jobs, resets their ID sequences, and
// serializes against every OTHER test using this same call -- including
// tests in a different PACKAGE, like cmd/api's. That last part is the
// reason this exists at all: `go test ./...` runs different packages'
// test binaries as genuinely concurrent OS processes, not goroutines in
// one process, so two packages' tests truncating and asserting against
// the same live shared table race exactly like two goroutines would
// without a mutex -- one process's insert can land in the middle of
// another's truncate-then-assert window. A session-scoped Postgres
// advisory lock (pg_advisory_lock) is the cross-process mutex: it blocks
// until this caller is the only holder, tied to the one connection
// acquired here rather than returned to the pool, so it stays held for
// the whole test, not just this TRUNCATE statement -- the race this
// defends against is "another process's insert happens mid-test", not
// just "two truncates happen at once".
//
// The returned unlock func must be called when the test is done --
// callers register it with t.Cleanup rather than this function taking
// testing.TB itself, which would mean a non-test file importing
// "testing".
func (s *Store) TruncateForTest(ctx context.Context) (unlock func(), err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: truncate for test: acquire: %w", err)
	}

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", testLockKey); err != nil {
		conn.Release()
		return nil, fmt.Errorf("store: truncate for test: advisory lock: %w", err)
	}
	unlock = func() {
		// A fresh, unbounded context: this runs from t.Cleanup, after
		// the test's own ctx may already be cancelled, and unlocking is
		// exactly the kind of cleanup that still needs to happen even
		// then.
		conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testLockKey)
		conn.Release()
	}

	if _, err := conn.Exec(ctx, "TRUNCATE TABLE job_runs, jobs RESTART IDENTITY CASCADE"); err != nil {
		unlock()
		return nil, fmt.Errorf("store: truncate for test: %w", err)
	}

	return unlock, nil
}
