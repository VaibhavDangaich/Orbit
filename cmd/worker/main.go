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
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/vaibhavdangaich/orbit/internal/env"
	"github.com/vaibhavdangaich/orbit/internal/job"
	"github.com/vaibhavdangaich/orbit/internal/metrics"
	"github.com/vaibhavdangaich/orbit/internal/queue"
	"github.com/vaibhavdangaich/orbit/internal/ratelimit"
	"github.com/vaibhavdangaich/orbit/internal/store"
	"github.com/vaibhavdangaich/orbit/internal/tracing"
)

// tracer is cmd/worker's own named tracer -- spans started here (claim,
// execute) show up nested under the consumer span internal/queue.Consumer.Next
// started for this message, because they're all given the same ctx chain.
var tracer = otel.Tracer("orbit/worker")

func main() {
	workerID := env.Or("ORBIT_WORKER_ID", defaultWorkerID())
	log.SetPrefix(fmt.Sprintf("[worker %s] ", workerID))

	dsn := env.Or("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	lease := env.DurationOr("ORBIT_LEASE", 30*time.Second)
	sweepInterval := env.DurationOr("ORBIT_SWEEP_INTERVAL", 30*time.Second)
	sweepBatchSize := env.IntOr("ORBIT_BATCH_SIZE", 10)
	kafkaBrokers := strings.Split(env.Or("ORBIT_KAFKA_BROKERS", strings.Join(queue.DefaultDevBrokers, ",")), ",")
	kafkaGroup := env.Or("ORBIT_KAFKA_GROUP", "orbit-workers")
	redisAddr := env.Or("ORBIT_REDIS_ADDR", ratelimit.DefaultDevAddr)
	rateLimitPerTenant := env.IntOr("ORBIT_RATE_LIMIT_PER_TENANT", 10)
	metricsAddr := env.Or("ORBIT_METRICS_ADDR", ":9102")
	otlpEndpoint := env.Or("ORBIT_OTLP_ENDPOINT", "localhost:4317")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Same non-fatal treatment as metrics.Serve just below: an unreachable
	// Jaeger shouldn't stop this worker from claiming and executing runs.
	shutdownTracing, err := tracing.Init(ctx, "orbit-worker", otlpEndpoint)
	if err != nil {
		log.Printf("tracing: %v (continuing without spans)", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			log.Printf("tracing: shutdown: %v", err)
		}
	}()

	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := store.New(startupCtx, dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer s.Close()

	// After store.New, not before: the readiness probe pings Postgres
	// through s, so the server can only be started once s exists. Nothing
	// is lost by waiting -- a worker that cannot reach Postgres exits on
	// the line above rather than lingering to be scraped.
	//
	// A failed bind here is logged, not fatal -- running several worker
	// replicas on one machine for a local demo means they'd all try this
	// same default port unless given distinct ORBIT_METRICS_ADDR values,
	// and a worker whose metrics port lost that race should still claim
	// and execute jobs correctly. See internal/metrics.Serve's comment.
	if err := metrics.Serve(metricsAddr, s.Ping); err != nil {
		log.Printf("metrics: %v (continuing without /metrics or health endpoints)", err)
	}

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

		msgCtx, runID, commit, err := consumer.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("consume: %v", err)
			continue
		}

		// msgCtx, not ctx: it carries the consumer span Next started as a
		// child of whatever the producer injected into this message's
		// headers. Passing it (not the loop's own ctx) down through claim,
		// execute, and report is what makes those show up nested under
		// that span in Jaeger instead of as orphaned, trace-less work.
		handleRunID(msgCtx, s, limiter, workerID, lease, runID)

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
		metrics.RunsRateLimited.Inc()
		return
	}

	claimCtx, claimSpan := tracer.Start(ctx, "orbit.worker claim_run")
	r, ok, err := s.ClaimRun(claimCtx, runID, workerID, lease)
	claimSpan.SetAttributes(attribute.Bool("orbit.claimed", ok))
	if err != nil {
		claimSpan.RecordError(err)
		claimSpan.SetStatus(codes.Error, "claim failed")
	}
	claimSpan.End()
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

	execCtx, execSpan := tracer.Start(ctx, "orbit.worker execute")
	start := time.Now()
	execErr := execute(execCtx, j.Payload)
	metrics.ExecutionDuration.Observe(time.Since(start).Seconds())
	if execErr != nil {
		execSpan.RecordError(execErr)
		execSpan.SetStatus(codes.Error, "execute failed")
	}
	execSpan.End()

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
