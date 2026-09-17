-- The transactional outbox: solves the "dual write" problem that shows up
-- the moment a Postgres commit needs to also notify an external system
-- (Kafka). Publishing to Kafka directly inside the same transaction that
-- inserts a job_runs row doesn't work -- if the transaction later rolls
-- back, we've told Kafka about a run that doesn't exist; publishing AFTER
-- commit doesn't work either -- a crash between commit and publish means
-- the run exists in Postgres but nothing ever tells a worker about it,
-- and workers no longer poll Postgres once Kafka is in the picture.
--
-- The outbox row is written in the SAME transaction as the job_runs
-- change that requires dispatching (see store.MaterializeDueRuns,
-- store.FailRun, store.ReapExpiredLeases) -- so it's atomic with that
-- change by construction. A separate step (store.DispatchOutbox) then
-- drains undispatched rows and publishes them to Kafka, marking each
-- dispatched_at on success. If that step crashes between publishing and
-- marking, the row gets published again on the next pass -- a duplicate,
-- which is exactly why the outbox pattern is only ever "at least once,"
-- never "exactly once." That's fine here specifically because ClaimRun's
-- fencing (WHERE status = 'pending') already makes a duplicate dispatch
-- harmless.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES job_runs (id) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatched_at TIMESTAMPTZ
);

-- DispatchOutbox's poll query: "give me undispatched rows." A partial
-- index, like jobs(next_run_at) WHERE enabled -- once a row is
-- dispatched, it never needs to be found by this query again, so there's
-- no reason to keep it in the index.
CREATE INDEX idx_outbox_undispatched ON outbox (id) WHERE dispatched_at IS NULL;
