// Package metrics is orbit's only place that imports Prometheus's client
// library -- same containment principle as internal/store for Postgres,
// internal/election for etcd, internal/queue for Kafka, and
// internal/ratelimit for Redis.
//
// One real difference from those packages: this one is imported BY
// several others (store, cmd/scheduler, cmd/worker) rather than wrapping
// one thing those others call through a narrow interface. That's
// deliberate, not an inconsistency: a metric is a global, write-only
// counter with no connection lifecycle to manage (no Close(), no pool
// size to tune) -- prometheus's own client library registers everything
// to a process-wide default registry by design. Reaching for it directly
// from business logic is the same shape as importing "log" or "context",
// not a violation of "one package per infra dependency" -- that rule is
// about stateful clients (a DB pool, a Kafka writer) that need contained
// lifecycle management, which a counter simply doesn't have.
package metrics

import (
	"fmt"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// RunsMaterialized counts every run MaterializeDueRuns actually
	// creates -- not every tick, only the ticks that found real work.
	RunsMaterialized = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orbit_runs_materialized_total",
		Help: "Total runs created by MaterializeDueRuns.",
	})

	// ScheduleMisfires counts occurrences Schedule.FastForward collapsed
	// away -- i.e. how far behind the scheduler has fallen. This fulfills
	// a comment left in MaterializeDueRuns during the phase-1 build
	// ("swap for a real counter in the observability phase") rather than
	// leaving that promise unresolved now that this phase has arrived.
	ScheduleMisfires = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orbit_schedule_misfires_total",
		Help: "Total missed schedule occurrences collapsed by FastForward.",
	})

	// RunsDispatched counts successful Kafka publishes via the outbox.
	RunsDispatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orbit_runs_dispatched_total",
		Help: "Total runs successfully published to Kafka via the outbox.",
	})

	// RunsClaimed is labeled by which path did the claiming: "kafka" (the
	// primary consume path, ClaimRun) or "sweep" (the reconciliation
	// safety net, ClaimRuns). Watching these two label values apart in a
	// dashboard is a direct, live view of "reconciliation as a safety
	// net, not a religion" -- the sweep counter should normally sit near
	// zero and only climb when the Kafka path is actually missing work.
	RunsClaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orbit_runs_claimed_total",
		Help: "Total runs claimed, labeled by claim path.",
	}, []string{"path"})

	// RunsCompleted is labeled by outcome: "succeeded", "retried" (sent
	// back to pending with attempts remaining), or "failed" (terminal).
	RunsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orbit_runs_completed_total",
		Help: "Total runs that reached an outcome, labeled by result.",
	}, []string{"status"})

	// RunsRateLimited counts runs deferred to the sweep because their
	// tenant was over budget -- see cmd/worker's handleRunID. This is the
	// live counterpart to the "31 explicit rate-limited log lines" proof
	// from the rate-limiting phase; now it's a number you can graph
	// instead of grepping logs for.
	RunsRateLimited = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orbit_runs_rate_limited_total",
		Help: "Total runs deferred because their tenant was over its rate limit.",
	})

	// LeaderStatus is 1 on the scheduler instance that currently holds
	// leadership, 0 on every follower. Scraping multiple scheduler
	// replicas on distinct ports and graphing this gauge is what makes a
	// failover visible as it happens: one line drops to 0, another rises
	// to 1, at the same moment the logs say "elected leader."
	LeaderStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "orbit_leader_status",
		Help: "1 if this scheduler instance currently holds leadership, 0 otherwise.",
	})

	// ExecutionDuration times execute(), the actual payload-running step
	// -- not the whole claim-execute-report cycle. prometheus.DefBuckets
	// (5ms to 10s) is a reasonable default for a job scheduler where most
	// payloads are expected to run in well under a second.
	ExecutionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "orbit_run_execution_duration_seconds",
		Help:    "Time spent executing a run's payload.",
		Buckets: prometheus.DefBuckets,
	})
)

// Serve starts a background HTTP server exposing Prometheus's standard
// /metrics endpoint on addr, returning immediately.
//
// Binding is checked synchronously (net.Listen, not a bare
// http.ListenAndServe) so a port conflict is reported to the caller
// instead of silently swallowed inside a goroutine. Callers are expected
// to log a returned error and keep running without a working /metrics
// endpoint, not treat it as fatal -- the same "a peripheral concern
// shouldn't take down the core loop" reasoning behind the rate limiter
// failing open on a Redis outage. This matters concretely here: running
// several worker replicas on one machine for a local demo means they'd
// all try the same default ORBIT_METRICS_ADDR unless given distinct
// values, and a scheduler or worker whose metrics port lost that race
// should still schedule and execute jobs correctly.
func Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go http.Serve(ln, mux)
	return nil
}
