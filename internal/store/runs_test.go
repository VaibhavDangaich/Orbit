package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

func mustCreateJob(t *testing.T, s *Store, nextRunAt time.Time) job.Job {
	t.Helper()
	return mustCreateJobWithMaxAttempts(t, s, nextRunAt, 3)
}

func mustCreateJobWithMaxAttempts(t *testing.T, s *Store, nextRunAt time.Time, maxAttempts int) job.Job {
	t.Helper()
	sched, err := job.ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}
	created, err := s.CreateJob(context.Background(), job.Job{
		TenantID:    "tenant-1",
		Name:        fmt.Sprintf("job-%d", time.Now().UnixNano()),
		Schedule:    sched,
		Payload:     []byte(`{}`),
		Enabled:     true,
		MaxAttempts: maxAttempts,
		NextRunAt:   nextRunAt,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	return created
}

func TestMaterializeDueRuns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	in := mustCreateJob(t, s, now)

	created, err := s.MaterializeDueRuns(ctx, now, 10)
	if err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}

	after, err := s.GetJob(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	wantNext := now.Add(30 * time.Second)
	if !after.NextRunAt.Equal(wantNext) {
		t.Errorf("NextRunAt = %v, want %v", after.NextRunAt, wantNext)
	}

	// Calling it again immediately must NOT create a second run: `now`
	// hasn't advanced past the job's new next_run_at, so the job simply
	// isn't due yet. This is the "did we accidentally create a duplicate"
	// check, not the misfire check below.
	createdAgain, err := s.MaterializeDueRuns(ctx, now, 10)
	if err != nil {
		t.Fatalf("MaterializeDueRuns (second call): %v", err)
	}
	if createdAgain != 0 {
		t.Fatalf("second call created = %d, want 0", createdAgain)
	}
}

func TestMaterializeDueRunsCollapsesMisfires(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	// Simulate a job whose schedule has been due since 5 minutes ago --
	// as if the scheduler had been offline this whole time.
	longOverdue := now.Add(-5 * time.Minute)
	in := mustCreateJob(t, s, longOverdue)

	created, err := s.MaterializeDueRuns(ctx, now, 10)
	if err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}
	// Exactly one run, not ten -- the whole point of FastForward.
	if created != 1 {
		t.Fatalf("created = %d, want 1 (misfires must collapse to one run)", created)
	}

	after, err := s.GetJob(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !after.NextRunAt.After(now) {
		t.Errorf("NextRunAt = %v, want something after %v", after.NextRunAt, now)
	}
}

// TestClaimRunsNoDoubleClaim is the actual proof this design works: several
// workers race for the same single pending run, and exactly one may win.
//
// `go func() { ... }()` starts a goroutine -- a function running
// concurrently with the rest of the program, scheduled by the Go runtime
// onto OS threads (not 1:1; thousands of goroutines can run on a handful
// of threads). It's conceptually close to firing off a Promise in JS, but
// unlike an async function, a goroutine isn't awaited by default -- so we
// need sync.WaitGroup: wg.Add(n) records "n goroutines are in flight,"
// each calls wg.Done() when finished, and wg.Wait() blocks the test until
// that count hits zero. It's a manual Promise.all with no return values.
func TestClaimRunsNoDoubleClaim(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	mustCreateJob(t, s, now)
	if _, err := s.MaterializeDueRuns(ctx, now, 10); err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}

	const numWorkers = 5
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex // guards claimedRuns below -- see note at the bottom
		claimedRuns []job.Run
	)

	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runs, err := s.ClaimRuns(ctx, workerID, 30*time.Second, 1)
			if err != nil {
				t.Errorf("ClaimRuns(%s): %v", workerID, err)
				return
			}
			if len(runs) == 0 {
				return
			}
			mu.Lock()
			claimedRuns = append(claimedRuns, runs...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(claimedRuns) != 1 {
		t.Fatalf("claimed by %d workers, want exactly 1 (got %+v)", len(claimedRuns), claimedRuns)
	}
}

// Why the mutex: every goroutine above shares the same claimedRuns slice.
// Go's race detector (go test -race) flags concurrent map/slice writes
// even when they don't happen to collide during a given run, because
// "didn't crash this time" isn't the same as "safe." The mutex serializes
// access to the slice; it has nothing to do with the database locking
// this test is actually verifying.

func TestCompleteRun(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	mustCreateJob(t, s, now)
	if _, err := s.MaterializeDueRuns(ctx, now, 10); err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}
	runs, err := s.ClaimRuns(ctx, "worker-1", 30*time.Second, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ClaimRuns: runs=%v err=%v", runs, err)
	}

	if err := s.CompleteRun(ctx, runs[0].ID, "worker-1"); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}

	var status job.RunStatus
	if err := s.pool.QueryRow(ctx, `SELECT status FROM job_runs WHERE id = $1`, runs[0].ID).Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != job.RunSucceeded {
		t.Errorf("status = %q, want %q", status, job.RunSucceeded)
	}
}

// TestCompleteRunFencing is the actual proof of the fencing claim: a
// worker that is no longer the lease holder must not be able to complete
// a run someone else now owns.
func TestCompleteRunFencing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	mustCreateJob(t, s, now)
	if _, err := s.MaterializeDueRuns(ctx, now, 10); err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}
	runs, err := s.ClaimRuns(ctx, "worker-1", 30*time.Second, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ClaimRuns: runs=%v err=%v", runs, err)
	}

	// A different worker (never claimed this run) tries to complete it.
	err = s.CompleteRun(ctx, runs[0].ID, "worker-2")
	if !errors.Is(err, ErrStale) {
		t.Fatalf("CompleteRun by non-owner: err = %v, want ErrStale", err)
	}

	// The real owner's completion must still succeed afterwards -- the
	// rejected attempt must not have mutated the row at all.
	if err := s.CompleteRun(ctx, runs[0].ID, "worker-1"); err != nil {
		t.Fatalf("CompleteRun by real owner: %v", err)
	}
}

func TestFailRunRetriesThenTerminates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	// MaxAttempts=2 keeps this test to two rounds instead of three.
	mustCreateJobWithMaxAttempts(t, s, now, 2)
	if _, err := s.MaterializeDueRuns(ctx, now, 10); err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}

	// Round 1: attempt=1 < max_attempts=2 -> goes back to pending.
	runs, err := s.ClaimRuns(ctx, "worker-1", 30*time.Second, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ClaimRuns (round 1): runs=%v err=%v", runs, err)
	}
	status, err := s.FailRun(ctx, runs[0].ID, "worker-1", "connection refused")
	if err != nil {
		t.Fatalf("FailRun (round 1): %v", err)
	}
	if status != job.RunPending {
		t.Fatalf("status after round 1 = %q, want %q (should still have attempts left)", status, job.RunPending)
	}

	// Round 2: a (possibly different) worker claims the retried run.
	// attempt is now 2, so attempt < max_attempts (2 < 2) is false ->
	// this failure must be terminal.
	runs, err = s.ClaimRuns(ctx, "worker-2", 30*time.Second, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ClaimRuns (round 2): runs=%v err=%v", runs, err)
	}
	if runs[0].Attempt != 2 {
		t.Fatalf("Attempt = %d, want 2", runs[0].Attempt)
	}
	status, err = s.FailRun(ctx, runs[0].ID, "worker-2", "connection refused")
	if err != nil {
		t.Fatalf("FailRun (round 2): %v", err)
	}
	if status != job.RunFailed {
		t.Fatalf("status after round 2 = %q, want %q (attempts exhausted)", status, job.RunFailed)
	}
}

func TestReapExpiredLeases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	mustCreateJob(t, s, now)
	if _, err := s.MaterializeDueRuns(ctx, now, 10); err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}

	// A lease so short it's already expired by the time we check it --
	// simulating a worker that claimed a run and then died.
	runs, err := s.ClaimRuns(ctx, "worker-1", 1*time.Millisecond, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ClaimRuns: runs=%v err=%v", runs, err)
	}
	time.Sleep(10 * time.Millisecond)

	reaped, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1", reaped)
	}

	// MaxAttempts defaults to 3 in mustCreateJob, attempt was 1 -> retried,
	// not terminally failed.
	var status job.RunStatus
	var claimedBy *string
	if err := s.pool.QueryRow(ctx, `SELECT status, claimed_by FROM job_runs WHERE id = $1`, runs[0].ID).Scan(&status, &claimedBy); err != nil {
		t.Fatalf("query: %v", err)
	}
	if status != job.RunPending {
		t.Errorf("status = %q, want %q", status, job.RunPending)
	}
	if claimedBy != nil {
		t.Errorf("claimed_by = %v, want nil (cleared on retry)", *claimedBy)
	}
}

func TestReapExpiredLeasesIgnoresActiveLeases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	mustCreateJob(t, s, now)
	if _, err := s.MaterializeDueRuns(ctx, now, 10); err != nil {
		t.Fatalf("MaterializeDueRuns: %v", err)
	}
	if _, err := s.ClaimRuns(ctx, "worker-1", 30*time.Second, 1); err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}

	reaped, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped = %d, want 0 (lease still active)", reaped)
	}
}
