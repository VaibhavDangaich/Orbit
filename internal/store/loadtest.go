// loadtest.go holds the one read-only query cmd/loadtest needs: every run
// belonging to a given tenant, timestamps included. Same containment
// principle as dashboard.go -- a reporting path, kept separate from the
// write path in jobs.go/runs.go, and cmd/loadtest never sees a raw SQL
// string, only this Store method.
package store

import (
	"context"
	"fmt"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// RunsByTenantPrefix returns every run whose job's tenant_id starts with
// prefix, in creation order. A PREFIX, not an exact tenant match, because
// cmd/loadtest deliberately spreads one measurement run across MANY
// distinct tenant IDs (to stay under internal/ratelimit's per-tenant cap
// and measure real system throughput instead of one tenant's rate limit)
// while still needing to gather that whole run's results back as one set
// -- the prefix is the shared "this batch" identifier, the suffix is the
// rate limiter's own per-tenant bucket key. Scoped at all (not "every run
// in the table") specifically so a load test's own numbers aren't
// contaminated by whatever else is using the same shared dev database at
// the time -- this repo has hit that exact contamination problem more
// than once.
func (s *Store) RunsByTenantPrefix(ctx context.Context, prefix string) ([]job.Run, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.id, r.job_id, r.scheduled_for, r.status, r.attempt,
		       r.started_at, r.finished_at, r.created_at
		FROM job_runs r
		JOIN jobs j ON j.id = r.job_id
		WHERE j.tenant_id LIKE $1
		ORDER BY r.id`,
		prefix+"%",
	)
	if err != nil {
		return nil, fmt.Errorf("store: runs by tenant prefix: %w", err)
	}
	defer rows.Close()

	var out []job.Run
	for rows.Next() {
		var r job.Run
		if err := rows.Scan(&r.ID, &r.JobID, &r.ScheduledFor, &r.Status, &r.Attempt,
			&r.StartedAt, &r.FinishedAt, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: runs by tenant: scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: runs by tenant: rows: %w", err)
	}
	return out, nil
}
