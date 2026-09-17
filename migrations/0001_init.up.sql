-- jobs: the recurring definition. One row per job, mutated in place only
-- for scheduling bookkeeping (next_run_at, updated_at) -- never for
-- execution history, which lives entirely in job_runs.
CREATE TABLE jobs (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    name         TEXT NOT NULL,
    schedule     TEXT NOT NULL,              -- e.g. "@every 30s"; see internal/job.Schedule
    payload      JSONB NOT NULL DEFAULT '{}',
    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    max_attempts INT NOT NULL DEFAULT 3,
    next_run_at  TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The scheduler's core poll query is "which enabled jobs are due right
-- now" -- WHERE enabled AND next_run_at <= now(). A partial index (only
-- indexing enabled rows) keeps that scan fast without wasting index space
-- on disabled jobs, which never appear in that query.
CREATE INDEX idx_jobs_next_run_at ON jobs (next_run_at) WHERE enabled;

-- job_runs: one row per firing. status/attempt/claim fields track a single
-- run through its lease lifecycle; see internal/job.Run's doc comment for
-- the full retry contract.
CREATE TABLE job_runs (
    id               BIGSERIAL PRIMARY KEY,
    job_id           BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    scheduled_for    TIMESTAMPTZ NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    attempt          INT NOT NULL DEFAULT 1,
    claimed_by       TEXT,
    claim_expires_at TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The duplicate-firing guardrail: the database, not app code, refuses
    -- a second run for the same job at the same logical instant.
    CONSTRAINT uq_job_runs_job_scheduled_for UNIQUE (job_id, scheduled_for)
);

-- Supports two queries we'll add in later steps: the worker's claim query
-- (status = 'pending') and the reaper's expired-lease sweep
-- (status = 'running' AND claim_expires_at < now()).
CREATE INDEX idx_job_runs_status_claim ON job_runs (status, claim_expires_at);
