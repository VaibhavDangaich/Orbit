package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// MaterializeDueRuns finds up to limit enabled jobs due at or before now,
// inserts one pending Run for each (collapsing any missed occurrences via
// job.Schedule.FastForward), and advances each job's next_run_at. It
// returns how many runs were actually created.
//
// FOR UPDATE SKIP LOCKED on the jobs scan means multiple scheduler
// instances could call this concurrently and each would lock a disjoint
// set of due jobs, rather than blocking on each other or double-processing
// the same one. To be precise about what that buys us, since it's a
// common interview trip-up: this lock is NOT what prevents duplicate runs
// -- the UNIQUE (job_id, scheduled_for) constraint on job_runs is that
// backstop, and it holds even without this lock. What the lock actually
// prevents is a subtler bug: two schedulers both reading the same stale
// next_run_at and one clobbering the other's advance of it (a lost
// update). We don't have multiple scheduler instances until the leader
// election phase, but the query is already safe for when we do.
func (s *Store) MaterializeDueRuns(ctx context.Context, now time.Time, limit int) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: materialize: begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	rows, err := tx.Query(ctx, `
		SELECT id, schedule, next_run_at
		FROM jobs
		WHERE enabled AND next_run_at <= $1
		ORDER BY next_run_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`,
		now, limit,
	)
	if err != nil {
		return 0, fmt.Errorf("store: materialize: select due jobs: %w", err)
	}

	type dueJob struct {
		id          job.ID
		scheduleStr string
		nextRunAt   time.Time
	}
	var due []dueJob
	for rows.Next() {
		var d dueJob
		if err := rows.Scan(&d.id, &d.scheduleStr, &d.nextRunAt); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: materialize: scan: %w", err)
		}
		due = append(due, d)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: materialize: rows: %w", err)
	}
	rows.Close()

	created := 0
	for _, d := range due {
		sched, err := job.ParseSchedule(d.scheduleStr)
		if err != nil {
			return created, fmt.Errorf("store: materialize: job %d has invalid schedule %q: %w", d.id, d.scheduleStr, err)
		}

		fireAt, next, skipped := sched.FastForward(d.nextRunAt, now)
		if skipped > 0 {
			// ponytail: log.Printf, not a metric -- fine for a single
			// process; swap for a real counter in the observability phase
			// once something is actually scraping this.
			log.Printf("job %d missed %d occurrence(s), firing once for %s", d.id, skipped, fireAt)
		}

		tag, err := tx.Exec(ctx, `
			INSERT INTO job_runs (job_id, scheduled_for)
			VALUES ($1, $2)
			ON CONFLICT (job_id, scheduled_for) DO NOTHING`,
			d.id, fireAt,
		)
		if err != nil {
			return created, fmt.Errorf("store: materialize: insert run for job %d: %w", d.id, err)
		}
		if tag.RowsAffected() > 0 {
			created++
		}

		if _, err := tx.Exec(ctx,
			`UPDATE jobs SET next_run_at = $1, updated_at = now() WHERE id = $2`,
			next, d.id,
		); err != nil {
			return created, fmt.Errorf("store: materialize: advance job %d: %w", d.id, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return created, fmt.Errorf("store: materialize: commit: %w", err)
	}
	return created, nil
}

// ClaimRuns lets a worker check out up to limit pending runs, marking each
// running under a lease that expires after `lease`. If the worker dies
// before finishing, the lease eventually expires -- but reclaiming an
// expired lease isn't implemented yet; that lands with FailRun in the next
// step, since both need the same "has this run exhausted its retries"
// check against the job's max_attempts.
//
// FOR UPDATE SKIP LOCKED is the entire safety mechanism here, and it's
// worth being precise about what it does: every worker process runs this
// exact query concurrently against the same job_runs table. Postgres
// guarantees that if worker A already holds the lock on a row (because
// it's inside this same query, mid-transaction), worker B's scan simply
// skips that row and moves on to the next candidate instead of blocking
// behind it. No two workers can ever have the same row locked at once, so
// no two can ever claim the same run -- with no external lock service, no
// leader, and no coordination between the workers at all.
func (s *Store) ClaimRuns(ctx context.Context, workerID string, lease time.Duration, limit int) ([]job.Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: claim: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, job_id, scheduled_for, attempt
		FROM job_runs
		WHERE status = 'pending'
		ORDER BY scheduled_for
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: claim: select: %w", err)
	}

	var claimed []job.Run
	for rows.Next() {
		var r job.Run
		if err := rows.Scan(&r.ID, &r.JobID, &r.ScheduledFor, &r.Attempt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: claim: scan: %w", err)
		}
		claimed = append(claimed, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: claim: rows: %w", err)
	}
	rows.Close()

	expiresAt := time.Now().Add(lease)
	for _, r := range claimed {
		if _, err := tx.Exec(ctx, `
			UPDATE job_runs
			SET status = 'running', claimed_by = $1, claim_expires_at = $2, started_at = now()
			WHERE id = $3`,
			workerID, expiresAt, r.ID,
		); err != nil {
			return nil, fmt.Errorf("store: claim: update run %d: %w", r.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: claim: commit: %w", err)
	}

	for i := range claimed {
		claimed[i].Status = job.RunRunning
		claimed[i].ClaimedBy = &workerID
		claimed[i].ClaimExpiresAt = &expiresAt
	}
	return claimed, nil
}
