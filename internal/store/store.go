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
