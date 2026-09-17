package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// ErrStale is returned by CompleteRun/FailRun when the update didn't
// match any row -- meaning the calling worker is no longer the run's
// lease holder (someone else reclaimed it after the lease expired) or the
// run was already finished. See the fencing note on CompleteRun.
var ErrStale = errors.New("store: run not claimed by this worker (lease expired or already finished)")

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

// CompleteRun marks a run succeeded -- but only if workerID is still its
// current lease holder.
//
// The WHERE clause (claimed_by = $2 AND status = 'running') is called
// "fencing," and it's the actual answer to a question every leader-
// election or lease-based design gets asked: a worker's lease can expire
// for reasons that have nothing to do with it being dead -- a long GC
// pause, a slow network blip -- and it might come back and try to report
// success *after* another worker already reclaimed and finished its run.
// Without this check, whichever one writes last wins, silently. With it,
// the database only accepts the write from whoever the CURRENT lease
// holder is; a stale worker's write matches zero rows and is rejected.
// The safety doesn't come from the lease timer being accurate -- it comes
// from this conditional update being the only door in.
func (s *Store) CompleteRun(ctx context.Context, runID job.RunID, workerID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'succeeded', finished_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'`,
		runID, workerID,
	)
	if err != nil {
		return fmt.Errorf("store: complete run %d: %w", runID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStale
	}
	return nil
}

// FailRun reports that workerID's execution of runID failed with errMsg.
// Same fencing as CompleteRun (WHERE claimed_by = $2 AND status =
// 'running'): a stale worker's failure report is rejected exactly like a
// stale success report would be.
//
// The retry decision -- Job.MaxAttempts is fetched via a join, not passed
// in by the caller, so a worker can never accidentally under- or
// over-report it: if Run.Attempt hasn't reached it yet, the SAME row goes
// back to 'pending' with Attempt incremented (per the contract documented
// on job.Run -- a second INSERT for this (job_id, scheduled_for) would
// violate the unique constraint, so retries must reuse the row). Otherwise
// it becomes terminally 'failed'. The returned status tells the caller
// which branch happened, for logging.
func (s *Store) FailRun(ctx context.Context, runID job.RunID, workerID string, errMsg string) (job.RunStatus, error) {
	var status job.RunStatus
	err := s.pool.QueryRow(ctx, `
		UPDATE job_runs AS jr
		SET
			status           = CASE WHEN jr.attempt < j.max_attempts THEN 'pending' ELSE 'failed' END,
			attempt          = CASE WHEN jr.attempt < j.max_attempts THEN jr.attempt + 1 ELSE jr.attempt END,
			claimed_by       = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE jr.claimed_by END,
			claim_expires_at = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE jr.claim_expires_at END,
			started_at       = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE jr.started_at END,
			finished_at      = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE now() END,
			error            = $3
		FROM jobs AS j
		WHERE jr.id = $1 AND jr.claimed_by = $2 AND jr.status = 'running' AND j.id = jr.job_id
		RETURNING jr.status`,
		runID, workerID, errMsg,
	).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrStale
	}
	if err != nil {
		return "", fmt.Errorf("store: fail run %d: %w", runID, err)
	}
	return status, nil
}

// ReapExpiredLeases finds every run still marked 'running' whose lease has
// expired -- meaning the worker holding it never called CompleteRun or
// FailRun, most likely because it crashed -- and applies the exact same
// retry-vs-terminal decision FailRun does, with a synthetic error message.
//
// This is what closes the gap ClaimRuns left open: a dead worker's run
// would otherwise sit at status='running' forever, since nothing would
// ever call FailRun on its behalf. Nothing here is a new idea -- it's the
// same CASE logic as FailRun, just triggered by a timeout instead of an
// explicit report. In a running system this gets called on a timer
// (alongside MaterializeDueRuns, once cmd/scheduler exists) rather than
// on demand.
func (s *Store) ReapExpiredLeases(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs AS jr
		SET
			status           = CASE WHEN jr.attempt < j.max_attempts THEN 'pending' ELSE 'failed' END,
			attempt          = CASE WHEN jr.attempt < j.max_attempts THEN jr.attempt + 1 ELSE jr.attempt END,
			claimed_by       = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE jr.claimed_by END,
			claim_expires_at = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE jr.claim_expires_at END,
			started_at       = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE jr.started_at END,
			finished_at      = CASE WHEN jr.attempt < j.max_attempts THEN NULL ELSE now() END,
			error            = 'lease expired: worker did not report completion'
		FROM jobs AS j
		WHERE jr.status = 'running' AND jr.claim_expires_at < now() AND j.id = jr.job_id`,
	)
	if err != nil {
		return 0, fmt.Errorf("store: reap expired leases: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
