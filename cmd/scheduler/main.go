// cmd/scheduler is orbit's scheduling loop: it turns due Jobs into pending
// Runs and reclaims leases abandoned by dead workers. It owns no HTTP
// server and no worker logic -- see cmd/worker for the process that
// actually executes a Run.
//
// `package main` + `func main()` is Go's entry-point convention: any
// directory whose package is "main" and that has a main() function
// compiles to an executable via `go build`; every other package we've
// written so far (job, store) is a library, imported but never run
// directly.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/store"
)

func main() {
	log.SetPrefix("[scheduler] ")

	dsn := envOr("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	pollInterval := envDurationOr("ORBIT_POLL_INTERVAL", 5*time.Second)
	batchSize := envIntOr("ORBIT_BATCH_SIZE", 50)

	// signal.NotifyContext returns a context that's cancelled the moment
	// this process receives SIGINT (Ctrl+C) or SIGTERM (what `docker stop`
	// and Kubernetes send before killing a pod). Everything downstream
	// that accepts a context -- our store methods included -- can check
	// ctx.Done() and stop cleanly instead of being killed mid-write.
	// `defer stop()` releases the signal handler on the way out so a
	// second Ctrl+C isn't needed to actually exit once we return.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := store.New(startupCtx, dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer s.Close()

	log.Printf("started: poll_interval=%s batch_size=%d", pollInterval, batchSize)
	run(ctx, s, pollInterval, batchSize)
	log.Printf("stopped")
}

// run is the actual loop, pulled out of main so it's callable without a
// real process -- not exercised by a test yet, but this is the shape that
// makes it possible to add one later without restructuring anything.
func run(ctx context.Context, s *store.Store, pollInterval time.Duration, batchSize int) {
	// time.Ticker delivers a tick on a channel every pollInterval, which
	// is what lets this loop wait on EITHER a tick OR shutdown at the same
	// time via select -- a plain time.Sleep(pollInterval) loop would block
	// through a shutdown signal until the sleep finished.
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick(ctx, s, batchSize)
		}
	}
}

func tick(ctx context.Context, s *store.Store, batchSize int) {
	created, err := s.MaterializeDueRuns(ctx, time.Now().UTC(), batchSize)
	if err != nil {
		log.Printf("materialize due runs: %v", err)
	} else if created > 0 {
		log.Printf("materialized %d run(s)", created)
	}

	reaped, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		log.Printf("reap expired leases: %v", err)
	} else if reaped > 0 {
		log.Printf("reaped %d expired lease(s)", reaped)
	}
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
