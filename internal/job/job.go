package job

import "time"

// ID identifies a Job. It's a *distinct* type from RunID, even though both
// are backed by int64 underneath -- Go calls this a "defined type" (as
// opposed to a type alias, written `type ID = int64`, which would be fully
// interchangeable with int64). Because ID and RunID are distinct types, the
// compiler rejects code that passes a RunID where an ID is expected, even
// though both are "just numbers." That's a real bug class in schedulers
// (mixing up a job's ID with one of its run's IDs) caught at compile time
// for the price of two one-line type declarations.
type ID int64

// RunID identifies a single Run -- one firing of a Job.
type RunID int64

// RunStatus is the lifecycle state of a Run. Go has no enum keyword; the
// idiom is a defined string type plus a const block. We use strings (not
// iota-based ints) because these values round-trip through Postgres and
// log lines -- "running" in a log beats "2".
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// Job is a recurring definition: what to run, and how often. It does not
// track individual firings -- that's Run's job. Splitting the two is what
// makes "did this already fire for this instant?" answerable via a unique
// constraint instead of a mutated status column; see migrations/001_init.sql.
type Job struct {
	ID          ID
	TenantID    string
	Name        string
	Schedule    Schedule
	Payload     []byte // raw JSON; decoding it is the executor's job, not ours
	Enabled     bool
	MaxAttempts int // retries per Run before it's left in RunFailed for good
	NextRunAt   time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Run is one scheduled firing of a Job.
//
// ScheduledFor is the *logical* time the job was due -- not the wall-clock
// time a worker happened to pick it up. Those two drift apart under load or
// after a scheduler outage, and ScheduledFor (not "now") is what the unique
// constraint on (job_id, scheduled_for) is keyed on: it's what makes a
// retried or re-materialized run collide with itself instead of duplicating.
//
// Nullability convention: every field below that maps to a nullable
// Postgres column is a Go pointer (or, for Payload-like blobs, a nil
// slice) -- nil in Go means NULL in SQL, with no separate sentinel values
// like empty strings. TenantID/Name on Job are NOT NULL columns, so they
// stay plain strings. This is a rule, not a per-field judgment call, so
// the mapping in internal/store never has to guess.
//
// ClaimedBy/ClaimExpiresAt implement a lease: a worker "checks out" a run
// for a bounded time. If it dies mid-job, the lease expires and a reaper
// (added once we have workers to reap) puts the run back up for grabs.
//
// Retry semantics: Attempt starts at 1 on insert. When a worker reports
// failure and Attempt < Job.MaxAttempts, internal/store increments Attempt
// and resets this SAME row to RunPending (clearing ClaimedBy/StartedAt/
// FinishedAt) rather than inserting a new Run -- a second INSERT for the
// same (JobID, ScheduledFor) would hit the unique constraint. Only once
// Attempt reaches MaxAttempts does a failure become terminal.
type Run struct {
	ID             RunID
	JobID          ID
	ScheduledFor   time.Time
	Status         RunStatus
	Attempt        int
	ClaimedBy      *string
	ClaimExpiresAt *time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	Error          *string
	CreatedAt      time.Time
}
