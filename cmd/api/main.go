// cmd/api is orbit's only write path into the jobs table that isn't a raw
// SQL statement. Every other phase of this project ran against jobs
// inserted by hand via psql -- correct for development, not something a
// real system hands its users. This is that gap closed: create a job,
// look one up, list them, over plain HTTP/JSON.
//
// It owns no scheduling logic and no execution logic -- see cmd/scheduler
// and cmd/worker for those. Its entire job is validating a request,
// computing the one field a caller shouldn't set directly (NextRunAt,
// derived from the schedule), and calling into internal/store, the same
// way cmd/scheduler and cmd/worker do. No new database access lives here
// that store.CreateJob/GetJob didn't already have.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/env"
	"github.com/vaibhavdangaich/orbit/internal/metrics"
	"github.com/vaibhavdangaich/orbit/internal/store"
	"github.com/vaibhavdangaich/orbit/internal/tracing"
)

func main() {
	dsn := env.Or("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	addr := env.Or("ORBIT_API_ADDR", ":8080")
	metricsAddr := env.Or("ORBIT_METRICS_ADDR", ":9103")
	otlpEndpoint := env.Or("ORBIT_OTLP_ENDPOINT", "localhost:4317")

	log.SetPrefix("[api] ")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Same non-fatal treatment as cmd/scheduler and cmd/worker: an
	// unreachable Jaeger shouldn't stop this service from accepting
	// requests.
	shutdownTracing, err := tracing.Init(ctx, "orbit-api", otlpEndpoint)
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

	// A failed bind here is logged, not fatal -- same reasoning as
	// cmd/scheduler/cmd/worker: losing the metrics/health port shouldn't
	// stop this process from doing its actual job.
	if err := metrics.Serve(metricsAddr, s.Ping); err != nil {
		log.Printf("metrics: %v (continuing without /metrics or health endpoints)", err)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           newMux(s),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("started: addr=%s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("stopped")
}
