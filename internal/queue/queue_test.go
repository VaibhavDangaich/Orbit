package queue

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// TestMain installs a real (but non-exporting) TracerProvider and the W3C
// propagator before any test runs. Production code gets both from
// internal/tracing.Init at startup; without this, otel.Tracer falls back
// to the package-default no-op tracer, whose spans carry an invalid
// SpanContext and whose Inject/Extract do nothing -- silently defeating
// the exact header round-trip this package's tests exist to check,
// without any test actually failing to compile or panicking.
func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

func testBrokers() []string {
	if v := os.Getenv("ORBIT_TEST_KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return DefaultDevBrokers
}

func TestPublishConsumeRoundTrip(t *testing.T) {
	brokers := testBrokers()
	// 60s, not 10s: this one context has to cover creating a topic,
	// publishing, AND a brand-new consumer group's first rebalance
	// against a partition it has never read. That last part is the slow,
	// variable one -- a cold group has to find the coordinator, join, and
	// get its assignment before Next can return anything. On a laptop
	// that fits in 10s; on a CI runner sharing a box it intermittently
	// does not, which showed up as "Next: fetch: context deadline
	// exceeded" on runs whose Go code was byte-identical to runs that
	// passed.
	//
	// Raising it does not weaken the test. Nothing here asserts on
	// latency -- the assertions are that the RunID round-trips and the
	// trace context survives the headers. A timeout is the backstop for
	// "this will never complete", and 60s still fails fast enough to be
	// useful while no longer failing on a slow rebalance.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A fresh, unique TOPIC per test run -- not just a fresh consumer
	// group. A fresh group still starts from the earliest offset on a
	// SHARED topic, which means it has to read past every message every
	// previous run of this test ever published before reaching its own.
	// That backlog only ever grows, since Kafka topics are durable and
	// nothing here deletes old messages -- an earlier version of this
	// test learned that the hard way once HashBalancer made every run
	// hash to the same partition, concentrating months of accumulated
	// test messages onto one partition. A throwaway topic has zero
	// history: the first message its consumer sees is guaranteed to be
	// the one this test just published. Same underlying lesson as the
	// Postgres TRUNCATE in store_test.go, just applied to a system where
	// "truncate" isn't a real operation -- give the test its own
	// sandboxed resource instead.
	topic := fmt.Sprintf("orbit.runs.test.%d", time.Now().UnixNano())

	if err := EnsureTopic(ctx, brokers, topic, 3); err != nil {
		t.Skipf("kafka not reachable at %v (start it: docker compose -f deploy/compose/docker-compose.yml up -d kafka): %v", brokers, err)
	}

	pub := NewPublisher(brokers, topic)
	t.Cleanup(func() { pub.Close() })

	con := NewConsumer(brokers, fmt.Sprintf("test-%d", time.Now().UnixNano()), topic)
	t.Cleanup(func() { con.Close() })

	wantRunID := job.RunID(time.Now().UnixNano())
	if err := pub.Publish(ctx, wantRunID, job.ID(1)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msgCtx, gotRunID, commit, err := con.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if gotRunID != wantRunID {
		t.Fatalf("RunID = %d, want %d", gotRunID, wantRunID)
	}
	// The whole point of the header carrier: msgCtx should carry a real
	// span, extracted from headers Publish injected into a completely
	// separate message send -- not just whatever ctx this test happened to
	// call Next with.
	if sc := trace.SpanFromContext(msgCtx).SpanContext(); !sc.IsValid() {
		t.Fatalf("Next: returned context has no valid span; header propagation didn't round-trip")
	}
	if err := commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
