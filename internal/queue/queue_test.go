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

	if err := EnsureTopic(ctx, brokers, 3); err != nil {
		t.Skipf("kafka not reachable at %v (start it: docker compose -f deploy/compose/docker-compose.yml up -d kafka): %v", brokers, err)
	}

	pub := NewPublisher(brokers)
	t.Cleanup(func() { pub.Close() })

	// A fresh, unique group ID per test run so this test doesn't inherit
	// committed offsets from a previous run and skip the message it just
	// published -- the Kafka-consumer-group equivalent of the Postgres
	// TRUNCATE we added to store_test.go for the same underlying reason.
	groupID := fmt.Sprintf("test-%d", time.Now().UnixNano())
	con := NewConsumer(brokers, groupID)
	t.Cleanup(func() { con.Close() })

	wantRunID := job.RunID(time.Now().UnixNano())
	if err := pub.Publish(ctx, wantRunID); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// A fresh consumer group with no committed offset starts from the
	// EARLIEST message still in the topic -- which, since the topic is a
	// durable log that outlives any single test run, includes messages
	// published by every previous run of this same test. That's the
	// right default for production (a new worker group shouldn't skip
	// already-published, not-yet-committed work), so it's not something
	// to change on Consumer itself -- just read past the stale ones here
	// until the message this test actually published shows up.
	var found bool
	for i := 0; i < 100 && !found; i++ {
		gotRunID, commit, err := con.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if err := commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
		found = gotRunID == wantRunID
	}
	if !found {
		t.Fatalf("never saw RunID %d after 100 messages", wantRunID)
	}
}
