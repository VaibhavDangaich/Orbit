// Package ratelimit is orbit's only place that knows Redis exists -- the
// same containment principle internal/store applies to Postgres,
// internal/election to etcd, and internal/queue to Kafka. It does exactly
// one thing: a per-tenant token bucket, shared across every worker replica
// via Redis so the limit means what it says for the whole fleet, not just
// one process.
//
// Why Redis and not an in-memory bucket: an in-memory counter only sees
// requests that land on ITS process. With N worker replicas, each running
// its own private bucket, a tenant configured for 10 req/s could actually
// get up to 10*N req/s in aggregate -- N buckets, none aware of the
// others exist. Redis gives every replica the same shared counter to
// check against, which is the only way "10 req/s for this tenant" holds
// across a fleet instead of per process.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultDevAddr matches deploy/compose/docker-compose.yml. Same
// centralize-the-dev-connection-string lesson as store.DefaultDevDSN and
// queue.DefaultDevBrokers -- this project has already shipped one real bug
// (a Postgres port mismatch) from that string living in more than one
// place and drifting; one constant per dependency is how that stays fixed.
//
// Host port 6380, not the Redis default 6379: this machine already has a
// native Redis listening on 6379 (confirmed with `lsof -nP -iTCP:6379
// -sTCP:LISTEN` before picking this port) -- the exact same class of
// collision that moved Postgres to 5433 and Kafka to 19092 earlier in
// this project.
const DefaultDevAddr = "localhost:6380"

// keyPrefix namespaces every key this package writes, so a `redis-cli
// KEYS *` against a shared Redis instance can't be confused for anything
// else that might use the same instance.
const keyPrefix = "orbit:ratelimit:"

// idleTTL bounds how long a tenant's bucket key survives with no traffic.
// Without this, every tenant that has EVER made one request leaves a
// Redis key behind forever. Losing the key costs nothing correctness-wise
// -- the next request just starts from a full bucket, indistinguishable
// from a brand new tenant -- so letting idle keys expire is free memory
// hygiene, not a shortcut that trades away correctness.
const idleTTL = 10 * time.Minute

// tokenBucketScript is the entire rate-limit decision, expressed as one
// atomic Redis command.
//
// Why this has to be a single script and not separate read/decide/write
// calls: picture two worker processes both handling a message for the
// same tenant at nearly the same instant. If each one ran "GET the
// current token count, decide whether to admit, SET the new count" as
// three separate round trips, both could read the SAME (not-yet-updated)
// count, both decide "yes, there's a token available," and both admit --
// the bucket just let through one more request than it had room for. This
// is a textbook check-then-act race, the same shape of bug
// internal/store's fencing exists to close for a claimed row, just here
// for a counter instead. EVAL runs this whole script as one indivisible
// operation against Redis's single-threaded command execution -- no other
// client's command can interleave in the middle of it -- so "read the
// bucket, refill it for elapsed time, spend a token if one's available,
// write it back" always happens as if it were the only thing touching
// this key, no matter how many workers call Allow concurrently.
//
// KEYS[1] = bucket key (one per tenant)
// ARGV[1] = capacity, which doubles as the refill rate in tokens/second
//
//	(see Limiter.ratePerSecond -- one number, not separate burst/
//	refill knobs, is the whole configured shape of this bucket)
//
// ARGV[2] = now, unix milliseconds
// ARGV[3] = idle TTL, seconds
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'updated_at')
local tokens = tonumber(bucket[1])
local updatedAt = tonumber(bucket[2])

if tokens == nil then
	-- No bucket yet for this tenant -- start full, same as any tenant
	-- that's been idle long enough for its key to expire.
	tokens = capacity
	updatedAt = now
end

local elapsedSeconds = math.max(0, now - updatedAt) / 1000
tokens = math.min(capacity, tokens + elapsedSeconds * capacity)

local allowed = 0
if tokens >= 1 then
	tokens = tokens - 1
	allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'updated_at', now)
redis.call('EXPIRE', key, ttl)

return allowed
`)

// Limiter enforces a per-tenant token bucket, backed by Redis so the limit
// holds across every worker replica sharing the same tenant.
type Limiter struct {
	client        *redis.Client
	ratePerSecond int
}

// New connects to Redis and verifies it's reachable before returning.
//
// ratePerSecond is applied uniformly to every tenant's bucket -- there is
// deliberately no per-tenant override and no separate burst-size knob. A
// per-tenant, database-configurable limit is a real feature some system
// eventually needs, but this project has consistently avoided building
// abstraction ahead of an actual, current need (see cmd/worker/execute.go's
// single-function executor for the same stance applied elsewhere); one
// env var (ORBIT_RATE_LIMIT_PER_TENANT) is exactly the amount of
// configurability this system currently has a real reason to want.
// ratePerSecond doubling as both the bucket's capacity and its refill
// rate means a tenant can burst up to one second's worth of its own
// budget -- the simplest bucket shape that's still a genuine token
// bucket (smooth refill) rather than a fixed window (hard reset every
// interval, with its own well-known edge-of-window burst problem).
func New(ctx context.Context, addr string, ratePerSecond int) (*Limiter, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})

	// Same reasoning as store.New's Ping: fail fast at startup with one
	// clear error, instead of the caller finding out three unrelated
	// calls later, on the first real Allow, that Redis was never
	// reachable to begin with.
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ratelimit: ping: %w", err)
	}

	return &Limiter{client: client, ratePerSecond: ratePerSecond}, nil
}

// Close releases the underlying Redis connection(s).
func (l *Limiter) Close() error {
	return l.client.Close()
}

// Allow reports whether tenantID may proceed right now, atomically
// spending one token from its bucket if so. false means the tenant is
// currently over its configured rate -- the caller must treat that as
// backpressure, not a failure (see cmd/worker's handleRunID, which is
// what this is for: gating a run BEFORE it's claimed, so a rate-limited
// run never burns one of its real retry attempts).
func (l *Limiter) Allow(ctx context.Context, tenantID string) (bool, error) {
	now := time.Now().UnixMilli()
	key := keyPrefix + tenantID

	res, err := tokenBucketScript.Run(ctx, l.client, []string{key}, l.ratePerSecond, now, int(idleTTL.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("ratelimit: allow %q: %w", tenantID, err)
	}
	return res == 1, nil
}
