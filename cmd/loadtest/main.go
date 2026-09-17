// cmd/loadtest seeds a burst of jobs, waits for the running scheduler(s)
// and worker(s) to process them, and reports P50/P95/P99 latency broken
// into three real segments rather than one end-to-end blob:
//
//	scheduler lag   = created_at   - scheduled_for   (materialize delay)
//	dispatch+deliver = started_at  - created_at       (outbox tick + Kafka)
//	execution        = finished_at - started_at       (execute() itself)
//
// Not k6: k6 measures request/response latency over a network protocol,
// and nothing in orbit's own due-to-completed path is an HTTP request --
// it spans a Postgres poll, an outbox dispatch, and a Kafka hop. Using k6
// here would mean either building cmd/api (a whole deferred phase) just to
// give it something to call, or pointing it at /metrics, which measures
// nothing about the actual workload. A plain Go client seeding jobs
// through the same Store every other binary uses, then reading the
// results back the same way, is the honest tool for an async pipeline.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
	"github.com/vaibhavdangaich/orbit/internal/store"
)

func main() {
	dsn := envOr("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	n := envIntOr("ORBIT_LOADTEST_JOBS", 1000)
	// Spread across many tenants, not one: internal/ratelimit caps each
	// tenant at ORBIT_RATE_LIMIT_PER_TENANT (default 10/s). Seeding all N
	// jobs under a single tenant would measure that cap, not the
	// scheduler/worker pipeline's real throughput -- a single-tenant run
	// is its own, separate, useful measurement (see README), not this
	// one's default.
	numTenants := envIntOr("ORBIT_LOADTEST_TENANTS", 20)
	timeout := envDurationOr("ORBIT_LOADTEST_TIMEOUT", 3*time.Minute)
	// Printed alongside the results, not just used -- the whole point of
	// stating these is that ORBIT_BATCH_SIZE/ORBIT_POLL_INTERVAL bound
	// materialize throughput BY CONSTRUCTION (batch_size runs picked up
	// per poll_interval). A P99 quoted without them is unfalsifiable.
	pollInterval := envDurationOr("ORBIT_POLL_INTERVAL", 5*time.Second)
	batchSize := envIntOr("ORBIT_BATCH_SIZE", 50)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := store.New(startupCtx, dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer s.Close()

	batchPrefix := fmt.Sprintf("loadtest-%d-", time.Now().UnixNano())
	log.Printf("seeding %d jobs across %d tenants under batch %s (all due now -- a burst, not a steady rate)", n, numTenants, batchPrefix)
	if err := seed(ctx, s, batchPrefix, numTenants, n); err != nil {
		log.Fatalf("seed: %v", err)
	}

	log.Printf("waiting up to %s for all %d runs to reach a terminal state (poll_interval=%s batch_size=%d -- theoretical materialize ceiling %.1f runs/sec)",
		timeout, n, pollInterval, batchSize, float64(batchSize)/pollInterval.Seconds())

	runs, waited, err := waitForTerminal(ctx, s, batchPrefix, n, timeout)
	if err != nil {
		log.Printf("warning: %v -- reporting on what completed anyway", err)
	}
	log.Printf("done waiting after %s", waited.Round(time.Millisecond))

	report(batchPrefix, n, runs)
}

func seed(ctx context.Context, s *store.Store, batchPrefix string, numTenants, n int) error {
	sched, err := job.ParseSchedule("@every 1h") // long enough it won't refire mid-test
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		_, err := s.CreateJob(ctx, job.Job{
			TenantID:    fmt.Sprintf("%st%d", batchPrefix, i%numTenants),
			Name:        fmt.Sprintf("loadtest-%d", i),
			Schedule:    sched,
			Payload:     []byte(`{"message":"loadtest"}`),
			Enabled:     true,
			MaxAttempts: 3,
			NextRunAt:   now, // due immediately -- this IS the burst
		})
		if err != nil {
			return fmt.Errorf("create job %d: %w", i, err)
		}
	}
	return nil
}

// waitForTerminal polls RunsByTenant until every seeded job has a
// materialized, terminal run, or timeout elapses. Polling Postgres every
// 500ms, not subscribing to anything -- this is a one-shot measurement
// tool run by hand, not a long-lived service; a plain poll loop is the
// right amount of machinery for that.
func waitForTerminal(ctx context.Context, s *store.Store, batchPrefix string, n int, timeout time.Duration) ([]job.Run, time.Duration, error) {
	deadline := time.Now().Add(timeout)
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		runs, err := s.RunsByTenantPrefix(ctx, batchPrefix)
		if err != nil {
			return nil, time.Since(start), fmt.Errorf("runs by tenant: %w", err)
		}
		terminal := 0
		for _, r := range runs {
			if r.Status == job.RunSucceeded || r.Status == job.RunFailed {
				terminal++
			}
		}
		if terminal >= n {
			return runs, time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return runs, time.Since(start), fmt.Errorf("timed out: %d/%d runs terminal (%d materialized so far)", terminal, n, len(runs))
		}
		select {
		case <-ctx.Done():
			return runs, time.Since(start), ctx.Err()
		case <-ticker.C:
		}
	}
}

func report(batchPrefix string, seeded int, runs []job.Run) {
	var schedLag, dispatchLag, execTime, total []time.Duration
	succeeded, failed, notMaterialized, retried := 0, 0, seeded-len(runs), 0

	for _, r := range runs {
		switch r.Status {
		case job.RunSucceeded:
			succeeded++
		case job.RunFailed:
			failed++
		}
		if r.Attempt > 1 {
			retried++
		}
		if r.StartedAt == nil || r.FinishedAt == nil {
			continue // never claimed, or claimed but not yet terminal
		}
		schedLag = append(schedLag, r.CreatedAt.Sub(r.ScheduledFor))
		dispatchLag = append(dispatchLag, r.StartedAt.Sub(r.CreatedAt))
		execTime = append(execTime, r.FinishedAt.Sub(*r.StartedAt))
		total = append(total, r.FinishedAt.Sub(r.ScheduledFor))
	}

	fmt.Println()
	fmt.Println("=== orbit load test report ===")
	fmt.Printf("batch:               %s\n", batchPrefix)
	fmt.Printf("seeded:              %d\n", seeded)
	fmt.Printf("materialized:        %d\n", len(runs))
	fmt.Printf("succeeded:           %d\n", succeeded)
	fmt.Printf("failed (exhausted):  %d\n", failed)
	fmt.Printf("never materialized:  %d\n", notMaterialized)
	fmt.Printf("retried at least once: %d\n", retried)
	fmt.Println()

	if notMaterialized > 0 || succeeded+failed != seeded {
		fmt.Printf("INVARIANT VIOLATED: %d of %d seeded jobs never reached a terminal run -- this is a real loss, not a rounding note.\n", seeded-succeeded-failed, seeded)
	} else {
		fmt.Printf("Invariant holds: all %d seeded jobs reached a terminal run. No loss.\n", seeded)
	}
	fmt.Println()

	fmt.Printf("%-28s %10s %10s %10s %10s\n", "segment", "p50", "p95", "p99", "max")
	printPercentiles("scheduler lag (created-scheduled)", schedLag)
	printPercentiles("dispatch+delivery (started-created)", dispatchLag)
	printPercentiles("execution (finished-started)", execTime)
	printPercentiles("end-to-end (finished-scheduled)", total)
}

func printPercentiles(label string, d []time.Duration) {
	if len(d) == 0 {
		fmt.Printf("%-28s %10s %10s %10s %10s\n", label, "n/a", "n/a", "n/a", "n/a")
		return
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	fmt.Printf("%-28s %10s %10s %10s %10s\n", label,
		percentile(sorted, 50).Round(time.Millisecond),
		percentile(sorted, 95).Round(time.Millisecond),
		percentile(sorted, 99).Round(time.Millisecond),
		sorted[len(sorted)-1].Round(time.Millisecond),
	)
}

// percentile expects sorted ascending. Nearest-rank, not interpolated --
// simple, and the sample sizes here (hundreds to low thousands) don't
// need interpolation's extra precision to be a defensible number.
func percentile(sorted []time.Duration, p int) time.Duration {
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
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
