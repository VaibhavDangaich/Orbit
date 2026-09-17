// cmd/worker claims pending Runs and executes them. Any number of these
// can run at once, against the same Postgres, with zero coordination
// between them -- that's what internal/store's FOR UPDATE SKIP LOCKED
// claim query buys us. Try it: run two of these in separate terminals
// pointed at the same database and watch them split the work.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
	"github.com/vaibhavdangaich/orbit/internal/store"
)

func main() {
	workerID := envOr("ORBIT_WORKER_ID", defaultWorkerID())
	log.SetPrefix(fmt.Sprintf("[worker %s] ", workerID))

	dsn := envOr("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	pollInterval := envDurationOr("ORBIT_POLL_INTERVAL", 2*time.Second)
	lease := envDurationOr("ORBIT_LEASE", 30*time.Second)
	batchSize := envIntOr("ORBIT_BATCH_SIZE", 10)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := store.New(startupCtx, dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer s.Close()

	log.Printf("started: poll_interval=%s lease=%s batch_size=%d", pollInterval, lease, batchSize)
	run(ctx, s, workerID, pollInterval, lease, batchSize)
	log.Printf("stopped")
}

func run(ctx context.Context, s *store.Store, workerID string, pollInterval, lease time.Duration, batchSize int) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick(ctx, s, workerID, lease, batchSize)
		}
	}
}

func tick(ctx context.Context, s *store.Store, workerID string, lease time.Duration, batchSize int) {
	runs, err := s.ClaimRuns(ctx, workerID, lease, batchSize)
	if err != nil {
		log.Printf("claim runs: %v", err)
		return
	}

	for _, r := range runs {
		runOne(ctx, s, workerID, r)
	}
}

func runOne(ctx context.Context, s *store.Store, workerID string, r job.Run) {
	// A real Payload isn't in hand yet -- ClaimRuns returns job_runs
	// columns only, not the parent job's payload. Fetching it here with a
	// second query is the simplest correct thing to do today; if this
	// join shows up as a hot path later (batch_size climbing into the
	// thousands), it's the kind of thing an index or a JOIN in ClaimRuns
	// itself would fix.
	j, err := s.GetJob(ctx, r.JobID)
	if err != nil {
		log.Printf("run %d: load job %d: %v", r.ID, r.JobID, err)
		return
	}

	execErr := execute(ctx, j.Payload)
	if execErr == nil {
		if err := s.CompleteRun(ctx, r.ID, workerID); err != nil && !errors.Is(err, store.ErrStale) {
			log.Printf("run %d: complete: %v", r.ID, err)
		}
		return
	}

	status, err := s.FailRun(ctx, r.ID, workerID, execErr.Error())
	if err != nil && !errors.Is(err, store.ErrStale) {
		log.Printf("run %d: fail: %v", r.ID, err)
		return
	}
	if status == job.RunPending {
		log.Printf("run %d: failed (%v), will retry", r.ID, execErr)
	} else {
		log.Printf("run %d: failed (%v), attempts exhausted", r.ID, execErr)
	}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func envIntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}
