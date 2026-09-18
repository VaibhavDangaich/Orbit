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
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/election"
	"github.com/vaibhavdangaich/orbit/internal/metrics"
	"github.com/vaibhavdangaich/orbit/internal/queue"
	"github.com/vaibhavdangaich/orbit/internal/store"
	"github.com/vaibhavdangaich/orbit/internal/tracing"
)

func main() {
	nodeID := envOr("ORBIT_NODE_ID", defaultNodeID())
	log.SetPrefix(fmt.Sprintf("[scheduler %s] ", nodeID))

	dsn := envOr("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	pollInterval := envDurationOr("ORBIT_POLL_INTERVAL", 5*time.Second)
	batchSize := envIntOr("ORBIT_BATCH_SIZE", 50)
	etcdEndpoints := strings.Split(envOr("ORBIT_ETCD_ENDPOINTS", "localhost:2379"), ",")
	electionKey := envOr("ORBIT_ELECTION_KEY", "/orbit/scheduler-leader/")
	electionTTL := envDurationOr("ORBIT_ELECTION_TTL", 10*time.Second)
	kafkaBrokers := strings.Split(envOr("ORBIT_KAFKA_BROKERS", strings.Join(queue.DefaultDevBrokers, ",")), ",")
	kafkaPartitions := envIntOr("ORBIT_KAFKA_PARTITIONS", 3)
	metricsAddr := envOr("ORBIT_METRICS_ADDR", ":9101")
	otlpEndpoint := envOr("ORBIT_OTLP_ENDPOINT", "localhost:4317")

	// signal.NotifyContext returns a context that's cancelled the moment
	// this process receives SIGINT (Ctrl+C) or SIGTERM (what `docker stop`
	// and Kubernetes send before killing a pod). Everything downstream
	// that accepts a context -- our store methods included -- can check
	// ctx.Done() and stop cleanly instead of being killed mid-write.
	// `defer stop()` releases the signal handler on the way out so a
	// second Ctrl+C isn't needed to actually exit once we return.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Same non-fatal treatment as metrics.Serve below: an unreachable
	// Jaeger shouldn't stop this scheduler from campaigning and ticking.
	shutdownTracing, err := tracing.Init(ctx, "orbit-scheduler", otlpEndpoint)
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

	elStartupCtx, elCancel := context.WithTimeout(ctx, 10*time.Second)
	el, err := election.New(elStartupCtx, etcdEndpoints, electionKey, electionTTL)
	elCancel()
	if err != nil {
		log.Fatalf("connect to etcd: %v", err)
	}
	defer el.Close()

	// EnsureTopic runs on every startup, not as a one-time manual step --
	// idempotent infrastructure setup that happens to live in code instead
	// of a runbook. Partition count matters for the demo: a topic
	// created with the default single partition would mean only ONE
	// worker in a consumer group ever gets messages, no matter how many
	// workers are running -- Kafka partitions, not consumer count, are
	// the unit of parallelism.
	topicCtx, topicCancel := context.WithTimeout(ctx, 10*time.Second)
	err = queue.EnsureTopic(topicCtx, kafkaBrokers, queue.RunsTopic, kafkaPartitions)
	topicCancel()
	if err != nil {
		log.Fatalf("ensure kafka topic: %v", err)
	}

	publisher := queue.NewPublisher(kafkaBrokers, queue.RunsTopic)
	defer publisher.Close()

	// A failed bind here is logged, not fatal -- running multiple
	// scheduler replicas on one machine for a local demo means they'd all
	// try this same default port unless given distinct ORBIT_METRICS_ADDR
	// values, and a scheduler whose metrics port lost that race should
	// still campaign, tick, and dispatch correctly.
	if err := metrics.Serve(metricsAddr, s.Ping); err != nil {
		log.Printf("metrics: %v (continuing without /metrics or health endpoints)", err)
	}

	log.Printf("started: poll_interval=%s batch_size=%d election_ttl=%s kafka_partitions=%d", pollInterval, batchSize, electionTTL, kafkaPartitions)
	runWithLeaderElection(ctx, el, nodeID, s, publisher, pollInterval, batchSize)
	log.Printf("stopped")
}

// runWithLeaderElection blocks as a follower until this process wins the
// leadership campaign, runs the tick loop for as long as it holds
// leadership, and returns once the process is shutting down.
func runWithLeaderElection(ctx context.Context, el *election.Election, nodeID string, s *store.Store, publisher *queue.Publisher, pollInterval time.Duration, batchSize int) {
	log.Printf("campaigning for leadership")
	if err := el.Campaign(ctx, nodeID); err != nil {
		if ctx.Err() != nil {
			return // shutting down while still a follower -- not an error
		}
		log.Fatalf("campaign: %v", err)
	}
	log.Printf("elected leader")
	metrics.LeaderStatus.Set(1)

	// If our own etcd session dies mid-leadership (lease expired because
	// we lost connectivity to etcd for longer than electionTTL), cancel
	// leaderCtx so the tick loop below stops immediately -- exactly like
	// a SIGTERM would, just triggered by a different signal.
	leaderCtx, cancelLeader := context.WithCancel(ctx)
	defer cancelLeader()
	go func() {
		select {
		case <-el.Done():
			cancelLeader()
		case <-leaderCtx.Done():
		}
	}()

	run(leaderCtx, s, publisher, pollInterval, batchSize)
	metrics.LeaderStatus.Set(0)

	if ctx.Err() != nil {
		// Real shutdown: resign cleanly instead of just disappearing, so
		// the rest of the fleet doesn't have to wait out a full TTL to
		// notice we're gone and start their own campaign.
		resignCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := el.Resign(resignCtx); err != nil {
			log.Printf("resign: %v", err)
		} else {
			log.Printf("resigned leadership")
		}
		cancel()
		return
	}

	// leaderCtx ended but the top-level ctx didn't -- the only other
	// trigger for that is our own session dying. We deliberately don't
	// try to recover in-process (open a fresh session, re-campaign): a
	// process that just discovered it silently lost its lease is in an
	// uncertain state, and exiting lets whatever supervises it (systemd,
	// Kubernetes) restart it cleanly rather than us reasoning our way
	// back to a known-good state from inside the failure.
	log.Fatalf("etcd session lost -- exiting for a clean restart")
}

func defaultNodeID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// run is the actual loop, pulled out of main so it's callable without a
// real process -- not exercised by a test yet, but this is the shape that
// makes it possible to add one later without restructuring anything.
func run(ctx context.Context, s *store.Store, publisher *queue.Publisher, pollInterval time.Duration, batchSize int) {
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
			tick(ctx, s, publisher, batchSize)
		}
	}
}

func tick(ctx context.Context, s *store.Store, publisher *queue.Publisher, batchSize int) {
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

	dispatched, err := s.DispatchOutbox(ctx, batchSize, publisher.Publish)
	if err != nil {
		log.Printf("dispatch outbox: %v", err)
	} else if dispatched > 0 {
		log.Printf("dispatched %d run(s) to kafka", dispatched)
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
