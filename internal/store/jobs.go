package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// ErrNotFound is returned by lookups that find no matching row. It's a
// "sentinel error" -- a specific, comparable value callers check for with
// errors.Is(err, store.ErrNotFound), the same way you'd check
// err.code === 'ENOENT' in Node instead of just testing err.message.
// pgx's own "no rows" signal (pgx.ErrNoRows) is a store-package detail;
// wrapping it into our own sentinel keeps that detail from leaking into
// every caller.
var ErrNotFound = errors.New("store: not found")

// CreateJob inserts a new job and returns it with server-generated fields
// (ID, CreatedAt, UpdatedAt) filled in.
//
// The INSERT carries a RETURNING clause instead of doing a separate SELECT
// afterwards -- one round trip to Postgres instead of two, and no window
// where a concurrent process could see or modify the row between our
// insert and our read-back.
func (s *Store) CreateJob(ctx context.Context, j job.Job) (job.Job, error) {
	const q = `
		INSERT INTO jobs (tenant_id, name, schedule, payload, enabled, max_attempts, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at, updated_at`

	err := s.pool.QueryRow(ctx, q,
		j.TenantID, j.Name, j.Schedule.String(), j.Payload, j.Enabled, j.MaxAttempts, j.NextRunAt,
	).Scan(&j.ID, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return job.Job{}, fmt.Errorf("store: create job: %w", err)
	}

	return j, nil
}

// GetJob loads a job by ID, or returns ErrNotFound if no such row exists.
func (s *Store) GetJob(ctx context.Context, id job.ID) (job.Job, error) {
	const q = `
		SELECT id, tenant_id, name, schedule, payload, enabled, max_attempts, next_run_at, created_at, updated_at
		FROM jobs
		WHERE id = $1`

	var j job.Job
	var scheduleSpec string

	err := s.pool.QueryRow(ctx, q, id).Scan(
		&j.ID, &j.TenantID, &j.Name, &scheduleSpec, &j.Payload, &j.Enabled, &j.MaxAttempts, &j.NextRunAt, &j.CreatedAt, &j.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("store: get job: %w", err)
	}

	// Schedule's fields are unexported (internal/job/schedule.go) -- store
	// can't construct one by hand even though it's in a different package.
	// The only door in is ParseSchedule, which re-validates the string on
	// the way back out of the database. That's deliberate: it means a
	// value can never exist as a Go Schedule unless it already passed
	// validation once, whether it just came from a user or from a
	// round-trip through Postgres.
	sched, err := job.ParseSchedule(scheduleSpec)
	if err != nil {
		return job.Job{}, fmt.Errorf("store: get job: stored schedule %q is invalid: %w", scheduleSpec, err)
	}
	j.Schedule = sched

	return j, nil
}
