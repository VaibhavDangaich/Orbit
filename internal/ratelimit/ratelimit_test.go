package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// newTestLimiter mirrors internal/store's newTestStore and internal/queue's
// testBrokers: skip cleanly with a clear message if the dependency isn't
// reachable, rather than failing opaquely.
func newTestLimiter(t *testing.T, ratePerSecond int) *Limiter {
	t.Helper()

	addr := os.Getenv("ORBIT_TEST_REDIS_ADDR")
	if addr == "" {
		addr = DefaultDevAddr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l, err := New(ctx, addr, ratePerSecond)
	if err != nil {
		t.Skipf("redis not reachable at %s (start it: docker compose -f deploy/compose/docker-compose.yml up -d redis): %v", addr, err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// testTenant gives every test its own disposable tenant ID instead of a
// shared one -- there's no TRUNCATE equivalent for a handful of Redis
// hash keys, so a unique key per test run is how this avoids the same
// cross-run state leak TestPublishConsumeRoundTrip hit with a shared
// Kafka topic (see internal/queue/queue_test.go).
func testTenant(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestAllowRejectsOnceExhausted(t *testing.T) {
	const capacity = 2
	l := newTestLimiter(t, capacity)
	ctx := context.Background()
	tenant := testTenant(t)

	for i := 0; i < capacity; i++ {
		ok, err := l.Allow(ctx, tenant)
		if err != nil {
			t.Fatalf("Allow request %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("Allow request %d: got false, want true (bucket starts full at capacity %d)", i, capacity)
		}
	}

	// The bucket is now empty; refilling even one token at 2/sec takes
	// 500ms, and the two prior calls above took nowhere near that -- this
	// request must be rejected.
	ok, err := l.Allow(ctx, tenant)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Fatal("Allow after exhausting the bucket: got true, want false")
	}
}

func TestAllowRefillsOverTime(t *testing.T) {
	const rate = 5 // one token every 200ms
	l := newTestLimiter(t, rate)
	ctx := context.Background()
	tenant := testTenant(t)

	for i := 0; i < rate; i++ {
		ok, err := l.Allow(ctx, tenant)
		if err != nil || !ok {
			t.Fatalf("Allow request %d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, err := l.Allow(ctx, tenant); err != nil {
		t.Fatalf("Allow: %v", err)
	} else if ok {
		t.Fatal("bucket should already be exhausted")
	}

	// Comfortably more than one refill interval (200ms) at this rate.
	time.Sleep(300 * time.Millisecond)

	ok, err := l.Allow(ctx, tenant)
	if err != nil {
		t.Fatalf("Allow after waiting for refill: %v", err)
	}
	if !ok {
		t.Fatal("Allow after waiting for refill: got false, want true (bucket should have refilled at least one token)")
	}
}

// TestAllowNoOverAdmitConcurrent mirrors internal/store's
// TestClaimRunsNoDoubleClaim: don't just assert the bucket behaves, prove
// it under real concurrency. numGoroutines all race to spend from a
// bucket that starts with exactly one token -- the atomic Lua script must
// ensure exactly one of them sees allowed=true, no matter how their
// requests interleave at Redis. A rate of 1/sec means the bucket can't
// legitimately refill a second token mid-test (this whole test runs in
// low single-digit milliseconds locally), so "exactly one admitted" is
// unambiguous: any second admit could only come from the
// read-modify-write race this script's atomicity is supposed to close.
func TestAllowNoOverAdmitConcurrent(t *testing.T) {
	const numGoroutines = 20

	l := newTestLimiter(t, 1)
	ctx := context.Background()
	tenant := testTenant(t)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := l.Allow(ctx, tenant)
			if err != nil {
				t.Errorf("Allow: %v", err)
				return
			}
			if ok {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 1 {
		t.Fatalf("allowed = %d, want exactly 1 (the atomic script must not let more than the bucket's capacity through under concurrency)", allowed)
	}
}
