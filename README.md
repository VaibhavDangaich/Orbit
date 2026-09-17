# orbit

A distributed job scheduler — the same class of problem Kubernetes' CronJob controller, Temporal, and Airflow solve — built from scratch in Go to demonstrate the mechanics most portfolio projects wave their hands at: leader election, exactly-the-right-amount-of-execution under crashes, safe concurrent claiming with zero external coordination, and message-queue migration done correctly.

No AI, no CRUD, no web frontend. This is a systems project, and it's built to be defended in an interview, not just demoed.

## What it actually does

Define a job with a schedule (`@every 30s`) and a payload. `orbit` fires it on time, retries it if it fails, never fires it twice for the same instant, survives a crashed scheduler or a crashed worker without losing or duplicating work, and spreads execution across as many workers as you run — all coordinated through Postgres, etcd, and Kafka, with no single point of manual intervention.

## Architecture

```
                    ┌─────────────┐
                    │    etcd     │  leader election
                    │ (KRaft-like │  (only 1 scheduler
                    │  consensus) │   does work at a time)
                    └──────┬──────┘
                           │ campaign / lease
                    ┌──────▼──────┐
        ┌──────────▶│  scheduler  │◀──────────┐  (N replicas, 1 active)
        │           │  (leader)   │           │
        │           └──────┬──────┘           │
        │                  │ materialize due   │
        │                  │ runs, reap dead   │
        │                  │ leases, dispatch  │
        │                  ▼                   │
   ┌────┴────┐      ┌─────────────┐            │
   │ Postgres│◀────▶│   outbox    │───publish──▶│  Kafka
   │(source  │      │ (atomic w/  │    (hash-   │ (3 partitions,
   │of truth)│      │ the state   │  partitioned│  consumer group)
   └────┬────┘      │  change)    │  by JobID)  │
        │            └─────────────┘            │
        │  claim / fence / complete             │
        ▼                                       ▼
   ┌─────────────────────────────────────────────────┐
   │  worker (N replicas)                             │
   │   • consumeLoop: primary path, via Kafka         │
   │   • sweepLoop: periodic reconciliation safety net │
   └───────────────────────────────────────────────────┘
```

## System design concepts, and where they actually live

Every one of these is implemented and covered by a test that proves the property, not just a comment asserting it.

| Concept | Where | What it actually proves |
|---|---|---|
| **Idempotent, exactly-once-enough execution** | `migrations/0001_init.up.sql` (`UNIQUE (job_id, scheduled_for)`) | The database, not application code, refuses a duplicate firing |
| **Safe concurrent claiming, zero coordination** | `internal/store/runs.go` (`ClaimRuns`, `SELECT ... FOR UPDATE SKIP LOCKED`) | 5 concurrent goroutines race for 1 row; exactly 1 wins, proven under `-race` |
| **Fencing** | `internal/store/runs.go` (`CompleteRun`, `FailRun`) | A worker whose lease expired can't overwrite a result that isn't its to write — proven in `TestCompleteRunFencing` |
| **Misfire collapsing** | `internal/job/schedule.go` (`Schedule.FastForward`) | A scheduler down for an hour fires once on restart, not 120 times |
| **Leader election** | `internal/election` (etcd session/campaign/resign) | Only one scheduler replica does work; graceful failover is sub-second, crash failover is bounded by the election TTL — both measured live |
| **Transactional outbox** | `migrations/0002_outbox.up.sql`, `store.DispatchOutbox` | Solves the dual-write problem: a Postgres commit and a Kafka publish aren't atomic, so the outbox row is |
| **Consistent hashing** | `internal/hashring` | Adding a 6th node to a 5-node ring remaps ~16% of keys (measured), not the ~83% a naive `hash % N` would |
| **Partition-aware message routing** | `internal/queue/balancer.go` (`HashBalancer`) | A job's runs consistently land on the same Kafka partition (and, in steady state, the same worker) — verified live: 27/27 job-A firings via Kafka went to one worker, 28/28 job-B firings went to a different one, zero mixing. The single exception (1 job-A run landing on job-B's worker) came from the reconciliation sweep, which bypasses Kafka and therefore partition affinity entirely — expected, not a bug |
| **Reconciliation as a safety net, not a religion** | `cmd/worker` (`sweepLoop`) | If the outbox/Kafka path is ever down, the original batch-poll claim path still finds the work |

## Getting started

Requires Docker and Go 1.26+.

```bash
# 1. Start Postgres, etcd, and Kafka
docker compose -f deploy/compose/docker-compose.yml up -d

# 2. Apply the schema
docker exec -i compose-postgres-1 psql -U scheduler -d scheduler < migrations/0001_init.up.sql
docker exec -i compose-postgres-1 psql -U scheduler -d scheduler < migrations/0002_outbox.up.sql

# 3. Seed a job (there's no API yet -- see Roadmap)
docker exec -i compose-postgres-1 psql -U scheduler -d scheduler -c \
  "INSERT INTO jobs (tenant_id, name, schedule, payload, enabled, max_attempts, next_run_at) \
   VALUES ('demo', 'hello', '@every 10s', '{\"message\":\"hello from orbit\"}', true, 3, now());"

# 4. Run the scheduler and one or more workers, in separate terminals
go run ./cmd/scheduler
go run ./cmd/worker
```

Run a second `go run ./cmd/worker` in another terminal and watch work split across both. Run a second `go run ./cmd/scheduler` and only one will log "elected leader" — kill it and watch the other take over.

### Configuration

Both binaries are configured entirely by environment variables (no config file, no flags) — the standard pattern for anything meant to run in a container.

**`cmd/scheduler`**

| Variable | Default | Meaning |
|---|---|---|
| `ORBIT_NODE_ID` | `<hostname>-<pid>` | Identity used in leader election |
| `ORBIT_DATABASE_URL` | local dev Postgres | Postgres connection string |
| `ORBIT_POLL_INTERVAL` | `5s` | How often the leader ticks (materialize, reap, dispatch) |
| `ORBIT_BATCH_SIZE` | `50` | Max rows processed per tick |
| `ORBIT_ETCD_ENDPOINTS` | `localhost:2379` | Comma-separated etcd endpoints |
| `ORBIT_ELECTION_KEY` | `/orbit/scheduler-leader/` | etcd key the leader election runs on |
| `ORBIT_ELECTION_TTL` | `10s` | Leader session TTL — the failover-speed/flapping-sensitivity tradeoff |
| `ORBIT_KAFKA_BROKERS` | `localhost:19092` | Comma-separated Kafka brokers |
| `ORBIT_KAFKA_PARTITIONS` | `3` | Partition count for topic creation |

**`cmd/worker`**

| Variable | Default | Meaning |
|---|---|---|
| `ORBIT_WORKER_ID` | `<hostname>-<pid>` | Identity used for lease claims |
| `ORBIT_DATABASE_URL` | local dev Postgres | Postgres connection string |
| `ORBIT_LEASE` | `30s` | How long a claimed run is held before it's considered abandoned |
| `ORBIT_SWEEP_INTERVAL` | `30s` | How often the reconciliation sweep runs |
| `ORBIT_BATCH_SIZE` | `10` | Max rows the sweep claims per pass |
| `ORBIT_KAFKA_BROKERS` | `localhost:19092` | Comma-separated Kafka brokers |
| `ORBIT_KAFKA_GROUP` | `orbit-workers` | Consumer group — all workers should share this |

## Project structure

```
cmd/
  scheduler/     leader-elected loop: materialize due runs, reap dead leases, dispatch to Kafka
  worker/        claims + executes runs, via Kafka (primary) and a periodic sweep (safety net)
internal/
  job/           pure domain logic (Schedule, Job, Run) -- no database, no infra, fully unit-tested
  store/         the only package that knows Postgres exists
  election/      the only package that knows etcd exists
  queue/         the only package that knows Kafka exists
  hashring/      consistent hashing, standalone and fully tested on its own
migrations/      versioned SQL, golang-migrate-compatible naming
deploy/compose/  local dev infrastructure (Postgres, etcd, Kafka)
```

Every `internal/` package is a hard boundary, not a convention: the Go compiler itself blocks any package outside this module from importing it. Each infra dependency (Postgres, etcd, Kafka) is contained to exactly one package that owns it; nothing else in the codebase imports a driver directly.

## Design decisions worth knowing before an interview asks about them

- **`SKIP LOCKED` isn't what prevents duplicate runs.** The `UNIQUE (job_id, scheduled_for)` constraint does that, on its own, even with zero other locking. What `SKIP LOCKED` prevents is a subtler bug: two schedulers racing to advance the same job's `next_run_at` and one silently undoing the other's progress.
- **Leader election is about efficiency, not safety.** The system is already correct with multiple scheduler replicas running unelected — `SKIP LOCKED` alone prevents duplicate work. Election exists so N replicas don't all redundantly hammer Postgres with the same query every tick, and so there's one unambiguous owner instead of a free-for-all.
- **"Exactly once" is never claimed anywhere in this codebase.** The real guarantee is at-least-once delivery with idempotent, fenced execution — which composes to "effectively once" in practice. Claiming true exactly-once is the kind of overstatement a good interviewer will immediately probe.
- **The outbox pattern exists because a DB commit and a Kafka publish aren't atomic.** Publishing inside the transaction risks a phantom message for a row that gets rolled back; publishing after commit risks a crash silently losing the dispatch. The outbox row, written atomically with the state change, is what closes that gap — at the cost of being only ever "at least once," which is exactly why the fencing above has to exist regardless of whether Kafka is involved.
- **Consistent hashing and Kafka consumer groups solve different problems.** Kafka's group coordinator already rebalances partitions across live workers on its own. The hash ring's job is narrower: giving a job's runs a *stable* partition over time (and minimal disruption if the topic is ever repartitioned) — not replacing worker-level load balancing, which Kafka already does.

## Known limitations

Stated explicitly rather than glossed over:

- **No API yet** — jobs are inserted directly via SQL. A `cmd/api` service is the natural next step.
- **`cmd/worker` has a per-run N+1 query** (`GetJob` after every claim, to fetch the payload) — fine at current batch sizes, a known candidate for folding into the claim query itself if it ever becomes a hot path.
- **No pluggable executor** — `cmd/worker/execute.go` is a single function, not an `Executor` interface with a registry, because there's exactly one kind of job so far. Building the abstraction before a second kind exists would be solving a problem this system doesn't have yet.
- **No rate limiting, no observability stack, no Kubernetes manifests yet** — all on the roadmap below.

## Roadmap

- [ ] Redis-backed per-tenant rate limiting
- [ ] OpenTelemetry tracing + Prometheus/Grafana
- [ ] Kubernetes deployment manifests
- [ ] Load testing (k6) with published P50/P95/P99 numbers, plus chaos testing (kill -9 everything, prove no loss)
- [ ] A terminal dashboard (`bubbletea`) for live job/run/leader/worker visibility

## Testing

```bash
go test ./... -race
```

Every package with infrastructure dependencies (`internal/store`, `internal/queue`) skips cleanly with a clear message if Postgres/Kafka isn't running, rather than failing opaquely. `internal/job` and `internal/hashring` are pure logic and need nothing running at all.

Correctness claims in this README aren't just asserted — the ones involving real concurrency or real infrastructure (fencing, leader failover, partition stickiness, zero-duplication under retries) were verified against live Postgres, etcd, and Kafka, not just unit-tested in isolation.
