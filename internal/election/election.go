// Package election is orbit's only place that knows etcd exists -- the
// same containment principle as internal/store for Postgres. cmd/scheduler
// talks to this package's small API; it never touches an etcd client
// directly.
//
// Why this exists at all, given internal/store.MaterializeDueRuns already
// uses FOR UPDATE SKIP LOCKED to make multiple concurrent schedulers SAFE:
// safety and efficiency are different problems. Without leader election,
// running 3 scheduler replicas for redundancy means all 3 hammer Postgres
// with the same due-jobs query every tick, fighting over the same rows via
// SKIP LOCKED -- correct, but wasteful, and impossible to reason about
// ("which instance did this?"). Leader election makes exactly one replica
// do the work at any moment, with the other two sitting idle, ready to
// take over within one election-session TTL if the leader dies. The data
// layer is what makes that handoff SAFE even in the ugly case (a
// partitioned old leader that thinks it's still leader keeps firing for a
// few seconds); leader election is what makes the common case efficient
// and unambiguous.
package election

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Election coordinates leadership among any number of processes racing on
// the same etcd key.
type Election struct {
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
}

// New connects to etcd and opens a session under key.
//
// A "session" here is backed by an etcd lease with the given ttl: etcd
// starts a countdown, and the client library spawns a background
// goroutine that sends a keepalive well before it expires, over and over,
// for as long as the process is alive and can reach etcd. If the process
// crashes, hangs, or loses network to etcd, keepalives stop, and the lease
// -- and everything tied to it, including any election this process has
// won -- expires automatically after ttl. That auto-expiry is the entire
// failure-detection mechanism: nothing has to explicitly notice a leader
// died, etcd just stops hearing from it.
//
// ttl is a real tradeoff, not a knob to set-and-forget: too short and
// ordinary network jitter causes needless leadership flapping; too long
// and a genuinely dead leader's replacement takes that long to take over.
func New(ctx context.Context, endpoints []string, key string, ttl time.Duration) (*Election, error) {
	// Deliberately NOT passing ctx as clientv3.Config.Context: that field
	// is the client's long-lived background context, used for the whole
	// client's lifetime -- including the session keepalive loop we start
	// below. ctx here is the caller's short, startup-bounded context (see
	// cmd/scheduler's elStartupCtx), which gets cancelled seconds after
	// this function returns. Wiring it in as the client's root context
	// killed the keepalive loop almost immediately after each session
	// started, which looked like "elected leader" instantly followed by
	// "session lost" in the very first end-to-end run of this code --
	// the client's background operations need to keep running for the
	// whole process lifetime, long after this ctx expires.
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("election: connect: %w", err)
	}

	// A real round trip against etcd, bounded by the caller's ctx, so New
	// fails fast with a clear error if etcd is unreachable -- same reason
	// store.New() calls Ping. This is the one place ctx is used; nothing
	// below needs it.
	if _, err := client.Get(ctx, key); err != nil {
		client.Close()
		return nil, fmt.Errorf("election: connectivity check: %w", err)
	}

	session, err := concurrency.NewSession(client, concurrency.WithTTL(int(ttl.Seconds())))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("election: open session: %w", err)
	}

	return &Election{
		client:   client,
		session:  session,
		election: concurrency.NewElection(session, key),
	}, nil
}

// Campaign blocks until this process becomes leader, ctx is cancelled, or
// an error occurs. Multiple processes calling Campaign on the same key
// (via their own Election, own session) queue up: etcd resolves the race
// by revision order, so exactly one wins at a time, and it's etcd doing
// that arbitration -- not any coordination between our processes, which
// never talk to each other directly.
func (e *Election) Campaign(ctx context.Context, nodeID string) error {
	return e.election.Campaign(ctx, nodeID)
}

// Resign gives up leadership immediately, without waiting for the session
// to expire -- used for a clean shutdown, so a replica that's exiting on
// purpose doesn't make the rest of the fleet wait out a full TTL before
// noticing.
func (e *Election) Resign(ctx context.Context) error {
	return e.election.Resign(ctx)
}

// Done reports when this process's session has ended -- meaning its lease
// expired (etcd connectivity was lost for longer than the TTL) and any
// leadership it held is already gone from etcd's perspective, whether or
// not this process has noticed yet. cmd/scheduler treats this as an
// unconditional signal to stop doing leader-only work immediately.
func (e *Election) Done() <-chan struct{} {
	return e.session.Done()
}

// Close releases the session (resigning any held leadership as a side
// effect) and closes the underlying etcd client.
func (e *Election) Close() error {
	if err := e.session.Close(); err != nil {
		e.client.Close()
		return fmt.Errorf("election: close session: %w", err)
	}
	return e.client.Close()
}
