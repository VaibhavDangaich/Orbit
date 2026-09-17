package store

import (
	"context"
	"testing"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

func TestListJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	sched, err := job.ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, err := s.CreateJob(ctx, job.Job{
			TenantID:    "tenant-1",
			Name:        "job",
			Schedule:    sched,
			Payload:     []byte(`{}`),
			Enabled:     true,
			MaxAttempts: 3,
			NextRunAt:   time.Now().UTC().Truncate(time.Microsecond),
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
	}

	all, err := s.ListJobs(ctx, 10)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("ListJobs: got %d jobs, want 3", len(all))
	}
	if all[0].Schedule != "@every 30s" {
		t.Errorf("Schedule = %q, want %q", all[0].Schedule, "@every 30s")
	}
	if !all[0].Enabled {
		t.Errorf("Enabled = false, want true")
	}

	limited, err := s.ListJobs(ctx, 2)
	if err != nil {
		t.Fatalf("ListJobs(limit=2): %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("ListJobs(limit=2): got %d jobs, want 2", len(limited))
	}
}

// TestRunStatusCounts inserts job_runs rows directly rather than driving
// them through MaterializeDueRuns/ClaimRun/CompleteRun -- this test only
// cares that RunStatusCounts groups and windows correctly, and the store
// package's own tests (newTestStore truncates job_runs too) already touch
// s.pool directly for setup, so there's no containment rule being bent
// here that isn't already established in store_test.go.
func TestRunStatusCounts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	sched, err := job.ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}
	j, err := s.CreateJob(ctx, job.Job{
		TenantID:    "tenant-1",
		Name:        "job",
		Schedule:    sched,
		Payload:     []byte(`{}`),
		Enabled:     true,
		MaxAttempts: 3,
		NextRunAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	insertRun := func(status job.RunStatus, scheduledFor time.Time) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO job_runs (job_id, scheduled_for, status)
			VALUES ($1, $2, $3)`,
			j.ID, scheduledFor, status,
		); err != nil {
			t.Fatalf("insert run (status=%s): %v", status, err)
		}
	}

	now := time.Now().UTC()
	insertRun(job.RunPending, now)
	insertRun(job.RunPending, now.Add(time.Second))
	insertRun(job.RunRunning, now.Add(2*time.Second))
	insertRun(job.RunSucceeded, now.Add(3*time.Second))
	insertRun(job.RunSucceeded, now.Add(4*time.Second))
	insertRun(job.RunSucceeded, now.Add(5*time.Second))
	insertRun(job.RunFailed, now.Add(6*time.Second))

	counts, err := s.RunStatusCounts(ctx, 100)
	if err != nil {
		t.Fatalf("RunStatusCounts: %v", err)
	}
	want := RunStatusCounts{Pending: 2, Running: 1, Succeeded: 3, Failed: 1}
	if counts != want {
		t.Errorf("RunStatusCounts(window=100) = %+v, want %+v", counts, want)
	}

	// Windowing: with only the 3 most recent runs in scope (by insertion/id
	// order), only the last 3 rows inserted above count -- succeeded,
	// succeeded, failed.
	windowed, err := s.RunStatusCounts(ctx, 3)
	if err != nil {
		t.Fatalf("RunStatusCounts(window=3): %v", err)
	}
	wantWindowed := RunStatusCounts{Succeeded: 2, Failed: 1}
	if windowed != wantWindowed {
		t.Errorf("RunStatusCounts(window=3) = %+v, want %+v", windowed, wantWindowed)
	}
}
