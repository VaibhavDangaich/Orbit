package queue

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

func testBrokers() []string {
	if v := os.Getenv("ORBIT_TEST_KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return DefaultDevBrokers
}

func TestPublishConsumeRoundTrip(t *testing.T) {
	brokers := testBrokers()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	gotRunID, commit, err := con.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if gotRunID != wantRunID {
		t.Fatalf("RunID = %d, want %d", gotRunID, wantRunID)
	}
	if err := commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
