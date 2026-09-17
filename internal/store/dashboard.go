// dashboard.go holds the read-only queries cmd/tui uses. They're kept out
// of jobs.go/runs.go on purpose: those files are the write path (creating
// jobs, claiming/completing/failing runs) and this is a reporting path that
// will never need a transaction or a fencing check. Same containment
// principle either way -- cmd/tui never sees a raw SQL string, it only ever
// calls a Store method, exactly like cmd/scheduler and cmd/worker do.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// JobSummary is the read-only projection of a Job that cmd/tui displays.
// It's a separate type from job.Job (not job.Job itself) because the
// dashboard doesn't need Payload or the timestamps, and because ListJobs
// reads schedule as the string already stored in Postgres rather than
// paying ParseSchedule's validation cost for a value nothing here executes.
type JobSummary struct {
	ID        job.ID
	TenantID  string
	Name      string
	Schedule  string
	Enabled   bool
	NextRunAt time.Time
}

// ListJobs returns up to limit jobs, ordered by ID. The jobs table is the
// set of job *definitions*, not their execution history (that's job_runs) --
// for a dashboard, it's reasonable to expect this to stay small (tens to
// low thousands of rows) even in a busy deployment, so a plain LIMIT is
// sufficient to keep this bounded; no pagination needed yet.
func (s *Store) ListJobs(ctx context.Context, limit int) ([]JobSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, name, schedule, enabled, next_run_at
		FROM jobs
		ORDER BY id
		LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()

	var out []JobSummary
	for rows.Next() {
		var j JobSummary
		if err := rows.Scan(&j.ID, &j.TenantID, &j.Name, &j.Schedule, &j.Enabled, &j.NextRunAt); err != nil {
			return nil, fmt.Errorf("store: list jobs: scan: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list jobs: rows: %w", err)
	}
	return out, nil
}

// RunStatusCounts is how many recent runs are in each terminal/non-terminal
// state, per the job.RunStatus constants.
type RunStatusCounts struct {
	Pending   int
	Running   int
	Succeeded int
	Failed    int
}

// RunStatusCounts groups the most recent `window` runs by status.
//
// Scoping choice, stated explicitly per the containment doc convention:
// this counts the last `window` runs by ID (an inner query does
// ORDER BY id DESC LIMIT window before the GROUP BY), NOT "runs in the
// last N minutes" and NOT "every run ever." A time-window WHERE clause
// (e.g. created_at > now() - interval) would still have to scan every row
// older than the cutoff to rule it out, since job_runs has no index on
// created_at -- cost grows with total table size forever. Ordering by id
// DESC LIMIT window instead walks the primary key's btree backwards and
// stops after `window` rows, so the query costs the same whether job_runs
// has a thousand rows or a hundred million. The tradeoff: "recent" here
// means "the last N runs across all jobs," not "the last N minutes" --
// fine for an at-a-glance dashboard, and exactly the same bounded-scan
// reasoning as the partial indexes in migrations/0001_init.up.sql.
func (s *Store) RunStatusCounts(ctx context.Context, window int) (RunStatusCounts, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, count(*)
		FROM (
			SELECT status
			FROM job_runs
			ORDER BY id DESC
			LIMIT $1
		) recent
		GROUP BY status`,
		window,
	)
	if err != nil {
		return RunStatusCounts{}, fmt.Errorf("store: run status counts: %w", err)
	}
	defer rows.Close()

	var c RunStatusCounts
	for rows.Next() {
		var status job.RunStatus
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return RunStatusCounts{}, fmt.Errorf("store: run status counts: scan: %w", err)
		}
		switch status {
		case job.RunPending:
			c.Pending = n
		case job.RunRunning:
			c.Running = n
		case job.RunSucceeded:
			c.Succeeded = n
		case job.RunFailed:
			c.Failed = n
		}
	}
	if err := rows.Err(); err != nil {
		return RunStatusCounts{}, fmt.Errorf("store: run status counts: rows: %w", err)
	}
	return c, nil
}
