// cmd/worker executes Runs. It has two independent paths to the same
// work, running as two goroutines in every worker process:
//
//   - consumeLoop: the primary path. Consumes "run ready" messages from
//     Kafka as part of a consumer group -- Kafka's partition assignment
//     is what replaces the old manual SKIP LOCKED polling as the
//     mechanism spreading work across however many workers are running.
//     Before claiming the run it names, this path checks the run's
//     tenant against internal/ratelimit; see handleRunID for why that
//     check has to happen before ClaimRun, not after.
//   - sweepLoop: a slow (default 30s) reconciliation pass using
//     store.ClaimRuns, the original batch SKIP LOCKED scan from before
//     Kafka existed. If the outbox/Kafka path is ever down, or drops a
//     message, this is what still finds and processes the run -- instead
//     of it sitting unclaimed forever. Not a fallback that only matters
//     in theory: it's the same code this worker used exclusively before
//     this step, now demoted to a safety net instead of deleted. It's
//     also, as of rate limiting, the mechanism that eventually picks up a
//     run that consumeLoop deferred because its tenant was over quota --
//     sweepLoop doesn't itself check the rate limit, so a severely
//     throttled tenant's runs still make forward progress, just on this
//     slower interval instead of instant Kafka redelivery.
//
// Any number of these can run at once, against the same Postgres and
// Kafka consumer group, with zero coordination between them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
	"github.com/vaibhavdangaich/orbit/internal/queue"
	"github.com/vaibhavdangaich/orbit/internal/ratelimit"
	"github.com/vaibhavdangaich/orbit/internal/store"
)

func main() {
	workerID := envOr("ORBIT_WORKER_ID", defaultWorkerID())
	log.SetPrefix(fmt.Sprintf("[worker %s] ", workerID))

	dsn := envOr("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	lease := envDurationOr("ORBIT_LEASE", 30*time.Second)
	sweepInterval := envDurationOr("ORBIT_SWEEP_INTERVAL", 30*time.Second)
	sweepBatchSize := envIntOr("ORBIT_BATCH_SIZE", 10)
	kafkaBrokers := strings.Split(envOr("ORBIT_KAFKA_BROKERS", strings.Join(queue.DefaultDevBrokers, ",")), ",")
	kafkaGroup := envOr("ORBIT_KAFKA_GROUP", "orbit-workers")
	redisAddr := envOr("ORBIT_REDIS_ADDR", ratelimit.DefaultDevAddr)
	rateLimitPerTenant := envIntOr("ORBIT_RATE_LIMIT_PER_TENANT", 10)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := store.New(startupCtx, dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer s.Close()

	consumer := queue.NewConsumer(kafkaBrokers, kafkaGroup, queue.RunsTopic)
	defer consumer.Close()

	rlStartupCtx, rlCancel := context.WithTimeout(ctx, 10*time.Second)
	limiter, err := ratelimit.New(rlStartupCtx, redisAddr, rateLimitPerTenant)
	rlCancel()
	if err != nil {
		log.Fatalf("connect to redis: %v", err)
	}
	defer limiter.Close()

	log.Printf("started: lease=%s sweep_interval=%s kafka_group=%s rate_limit_per_tenant=%d/s", lease, sweepInterval, kafkaGroup, rateLimitPerTenant)

	// Two independent loops, one process. `var wg sync.WaitGroup` is a
	// counter: wg.Add(1) before each goroutine starts, wg.Done() when it
	// exits, wg.Wait() blocks until that count hits zero. Both loops
	// share ctx, so the same SIGTERM that cancels one cancels both --
	// wg.Wait() is just what lets main() know they've actually finished
	// cleaning up before the process exits, instead of exiting out from
	// under them.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumeLoop(ctx, s, consumer, limiter, workerID, lease)
	}()
	go func() {
		defer wg.Done()
		sweepLoop(ctx, s, workerID, lease, sweepInterval, sweepBatchSize)
	}()
	wg.Wait()

	log.Printf("stopped")
}

// consumeLoop is the primary path: block on the next Kafka message, claim
// the specific run it names, execute it, report the outcome, then commit
// the offset -- in that order, so a crash between fetch and commit means
// Kafka redelivers the message rather than silently losing it.
func consumeLoop(ctx context.Context, s *store.Store, consumer *queue.Consumer, limiter *ratelimit.Limiter, workerID string, lease time.Duration) {
	for {
		if ctx.Err() != nil {
			return
		}

		runID, commit, err := consumer.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("consume: %v", err)
			continue
		}

		handleRunID(ctx, s, limiter, workerID, lease, runID)

		// Committed unconditionally -- including when handleRunID
		// deferred the run for being rate-limited. See handleRunID's
		// comment on why that's the correct thing to do here, not a bug.
		if err := commit(ctx); err != nil {
			log.Printf("run %d: commit kafka offset: %v", runID, err)
		}
	}
}

func handleRunID(ctx context.Context, s *store.Store, limiter *ratelimit.Limiter, workerID string, lease time.Duration, runID job.RunID) {
	// Rate limiting has to be a pre-claim gate, checked with a read-only
	// lookup (GetRunTenant) BEFORE ClaimRun is ever called -- not
	// something enforced after claiming by routing a throttled run
	// through FailRun. FailRun increments Attempt; a bursty but otherwise
	// healthy tenant could get its legitimate jobs terminally failed by
	// MaxAttempts purely from repeated backpressure, never from an actual
	// execution failure. Checking first means a rate-limited run's
	// Attempt is never touched at all.
	tenantID, err := s.GetRunTenant(ctx, runID)
	if err != nil {
		log.Printf("run %d: get tenant: %v", runID, err)
		return
	}

	allowed, err := limiter.Allow(ctx, tenantID)
	if err != nil {
		// Fail OPEN, not closed: if Redis is unreachable, treating every
		// run as rate-limited would silently stop the entire Kafka fast
		// path for every tenant, pushing all traffic onto the much
		// slower sweep -- a Redis outage becoming a scheduler-wide
		// slowdown is a worse failure mode than temporarily running
		// without rate limiting. Log it loudly and proceed as allowed.
		log.Printf("run %d: rate limit check for tenant %q: %v (failing open)", runID, tenantID, err)
		allowed = true
	}
	if !allowed {
		// Do NOT call ClaimRun. The run stays exactly 'pending' in
		// Postgres, as if this Kafka message never arrived, and
		// sweepLoop's periodic store.ClaimRuns scan is what eventually
		// picks it back up once the tenant's bucket has refilled. This
		// is a deliberate, accepted tradeoff, not an oversight: a
		// severely rate-limited tenant's runs are retried on
		// ORBIT_SWEEP_INTERVAL's cadence (default 30s), not near-instant
		// Kafka redelivery. consumeLoop still commits this message's
		// Kafka offset right after this call returns, so this partition
		// doesn't stall behind one throttled tenant -- any other
		// tenants' messages queued behind it keep flowing.
		log.Printf("run %d: tenant %q rate-limited, deferring to sweep", runID, tenantID)
		return
	}

	r, ok, err := s.ClaimRun(ctx, runID, workerID, lease)
	if err != nil {
		log.Printf("run %d: claim: %v", runID, err)
		return
	}
	if !ok {
		// Not an error -- a duplicate Kafka delivery (expected under
		// at-least-once) or the sweep already got to it first look
		// identical from here: someone already has (or finished) this
		// run, so there's nothing left for this message to do.
		log.Printf("run %d: already claimed or finished, skipping", runID)
		return
	}
	runOne(ctx, s, workerID, r)
}

// sweepLoop is the reconciliation path: periodically claim whatever's
// still pending via the batch SKIP LOCKED scan, same as every worker did
// before Kafka existed.
func sweepLoop(ctx context.Context, s *store.Store, workerID string, lease, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runs, err := s.ClaimRuns(ctx, workerID, lease, batchSize)
			if err != nil {
				log.Printf("sweep: claim runs: %v", err)
				continue
			}
			if len(runs) > 0 {
				log.Printf("sweep: claimed %d run(s) the kafka path missed", len(runs))
			}
			for _, r := range runs {
				runOne(ctx, s, workerID, r)
			}
		}
	}
}

func runOne(ctx context.Context, s *store.Store, workerID string, r job.Run) {
	// A real Payload isn't in hand yet -- ClaimRun(s) returns job_runs
	// columns only, not the parent job's payload. Fetching it here with a
	// second query is the simplest correct thing to do today; if this
	// join shows up as a hot path later, it's the kind of thing a JOIN in
	// the claim query itself would fix.
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
