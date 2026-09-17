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

		var newRunID job.RunID
		err = tx.QueryRow(ctx, `
			INSERT INTO job_runs (job_id, scheduled_for)
			VALUES ($1, $2)
			ON CONFLICT (job_id, scheduled_for) DO NOTHING
			RETURNING id`,
			d.id, fireAt,
		).Scan(&newRunID)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Already materialized by an earlier tick (or another
			// scheduler instance) -- not an error, just nothing new to
			// dispatch. next_run_at still needs to advance below
			// regardless of which branch we took here.
		case err != nil:
			return created, fmt.Errorf("store: materialize: insert run for job %d: %w", d.id, err)
		default:
			if _, err := tx.Exec(ctx, `INSERT INTO outbox (run_id) VALUES ($1)`, newRunID); err != nil {
				return created, fmt.Errorf("store: materialize: outbox insert for run %d: %w", newRunID, err)
			}
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
// before finishing, ReapExpiredLeases eventually reclaims it.
//
// This batch/poll-based claim is what cmd/worker used before the Kafka
// migration; the Kafka consume path uses the single-run ClaimRun instead,
// since Kafka already tells it exactly which run to work on. ClaimRuns
// stays in use as cmd/worker's periodic reconciliation sweep -- a
// deliberate second path to the same pending runs, not dead code: if the
// outbox/Kafka path is ever down or drops a message, ClaimRuns is what
// still finds and processes the run instead of it sitting unclaimed
// forever.
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
//
// When the run goes back to 'pending', this also writes an outbox row in
// the SAME transaction -- without it, a retried run would be invisible to
// the Kafka consume path: nothing else creates an outbox entry for a
// retry, so no worker would ever be told this run is workable again, and
// it would sit as 'pending' until cmd/worker's periodic ClaimRuns sweep
// eventually found it (correct, just slow -- see ClaimRuns' doc comment).
func (s *Store) FailRun(ctx context.Context, runID job.RunID, workerID string, errMsg string) (job.RunStatus, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("store: fail run %d: begin: %w", runID, err)
	}
	defer tx.Rollback(ctx)

	var status job.RunStatus
	err = tx.QueryRow(ctx, `
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

	if status == job.RunPending {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (run_id) VALUES ($1)`, runID); err != nil {
			return "", fmt.Errorf("store: fail run %d: outbox insert: %w", runID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("store: fail run %d: commit: %w", runID, err)
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
// explicit report, and it writes the same kind of outbox row FailRun does
// for whichever reaped runs went back to 'pending' (not the ones that hit
// terminal 'failed' -- nothing needs to dispatch those).
func (s *Store) ReapExpiredLeases(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: reap expired leases: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
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
		WHERE jr.status = 'running' AND jr.claim_expires_at < now() AND j.id = jr.job_id
		RETURNING jr.id, jr.status`,
	)
	if err != nil {
		return 0, fmt.Errorf("store: reap expired leases: %w", err)
	}

	var reaped int
	var retriedIDs []job.RunID
	for rows.Next() {
		var id job.RunID
		var status job.RunStatus
		if err := rows.Scan(&id, &status); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: reap expired leases: scan: %w", err)
		}
		reaped++
		if status == job.RunPending {
			retriedIDs = append(retriedIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: reap expired leases: rows: %w", err)
	}
	rows.Close()

	for _, id := range retriedIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (run_id) VALUES ($1)`, id); err != nil {
			return 0, fmt.Errorf("store: reap expired leases: outbox insert for run %d: %w", id, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: reap expired leases: commit: %w", err)
	}
	return reaped, nil
}

// ClaimRun claims one SPECIFIC run by ID, for the Kafka consume path --
// unlike ClaimRuns' batch scan, which discovers candidates by polling,
// the caller here already knows exactly which run to work on because
// Kafka told it.
//
// ok=false means the run wasn't claimable: either another worker already
// has it, or it already finished. Both are ordinary outcomes here, not
// errors -- a duplicate Kafka delivery (expected under at-least-once
// delivery) looks exactly like "someone already handled this," and the
// caller's correct response in either case is the same: commit the Kafka
// offset and move on, nothing left to do for this message.
func (s *Store) ClaimRun(ctx context.Context, runID job.RunID, workerID string, lease time.Duration) (job.Run, bool, error) {
	expiresAt := time.Now().Add(lease)
	var r job.Run
	err := s.pool.QueryRow(ctx, `
		UPDATE job_runs
		SET status = 'running', claimed_by = $1, claim_expires_at = $2, started_at = now()
		WHERE id = $3 AND status = 'pending'
		RETURNING id, job_id, scheduled_for, attempt`,
		workerID, expiresAt, runID,
	).Scan(&r.ID, &r.JobID, &r.ScheduledFor, &r.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Run{}, false, nil
	}
	if err != nil {
		return job.Run{}, false, fmt.Errorf("store: claim run %d: %w", runID, err)
	}
	r.Status = job.RunRunning
	r.ClaimedBy = &workerID
	r.ClaimExpiresAt = &expiresAt
	return r, true, nil
}

// DispatchOutbox drains up to limit undispatched outbox rows, calling
// publish for each and marking it dispatched on success.
//
// publish is a plain function, not an interface -- this package never
// imports internal/queue (or anything Kafka-specific) at all; the caller
// (cmd/scheduler) passes queue.Publisher.Publish in directly. A one-method
// interface would say the same thing with more ceremony; Go's http.
// HandlerFunc is the standard-library example of this same idiom.
//
// jobID comes along via a JOIN to job_runs rather than being denormalized
// onto outbox itself -- outbox.run_id already has a foreign key into
// job_runs, so the join is always consistent, and this query runs on a
// poll interval against small batches, not a latency-critical request
// path, so there's no real cost to reading job_id this way instead of
// duplicating it. publish uses jobID to pick a Kafka partition (see
// queue.HashBalancer) so a job's runs consistently land on the same
// partition over time.
//
// If publish fails partway through a batch, the whole transaction rolls
// back -- including the dispatched_at marks already written for earlier
// rows in this same batch. Those get republished on the next call. That's
// a duplicate publish, not a bug: the outbox pattern is only ever "at
// least once," and ClaimRun's fencing (WHERE status = 'pending') is what
// makes a duplicate harmless downstream. See migrations/0002_outbox.up.sql
// for the fuller version of this argument.
func (s *Store) DispatchOutbox(ctx context.Context, limit int, publish func(context.Context, job.RunID, job.ID) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: dispatch outbox: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// FOR UPDATE OF o -- not a bare FOR UPDATE -- locks only the outbox
	// rows this transaction is about to mark dispatched, not the joined
	// job_runs rows too. We only ever READ job_id here; locking job_runs
	// as well would needlessly serialize against ClaimRun's concurrent
	// UPDATEs on those same rows for no benefit.
	rows, err := tx.Query(ctx, `
		SELECT o.id, o.run_id, jr.job_id
		FROM outbox o
		JOIN job_runs jr ON jr.id = o.run_id
		WHERE o.dispatched_at IS NULL
		ORDER BY o.id
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("store: dispatch outbox: select: %w", err)
	}

	type outboxRow struct {
		id    int64
		runID job.RunID
		jobID job.ID
	}
	var pending []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.runID, &r.jobID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: dispatch outbox: scan: %w", err)
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: dispatch outbox: rows: %w", err)
	}
	rows.Close()

	dispatched := 0
	for _, r := range pending {
		if err := publish(ctx, r.runID, r.jobID); err != nil {
			return dispatched, fmt.Errorf("store: dispatch outbox: publish run %d: %w", r.runID, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET dispatched_at = now() WHERE id = $1`, r.id); err != nil {
			return dispatched, fmt.Errorf("store: dispatch outbox: mark run %d dispatched: %w", r.runID, err)
		}
		dispatched++
	}

	if err := tx.Commit(ctx); err != nil {
		return dispatched, fmt.Errorf("store: dispatch outbox: commit: %w", err)
	}
	return dispatched, nil
}
