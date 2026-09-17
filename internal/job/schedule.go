// Package job holds the scheduler's core domain logic: what a Schedule is
// and when it should next fire. It has no dependency on Postgres, Kafka,
// or anything else "infra" — that separation is deliberate (see README
// notes in later phases on why domain logic stays infra-free).
package job

import (
	"fmt"
	"strings"
	"time"
)

// Schedule represents a parsed, validated recurrence rule.
// The zero value is not valid — always construct one via ParseSchedule.
type Schedule struct {
	spec  string        // the original spec string, kept for logging/debugging
	every time.Duration // how often the job should run
}

// ParseSchedule parses a spec string into a Schedule.
//
// Today it only supports interval specs of the form "@every <duration>",
// e.g. "@every 30s" or "@every 5m". Go's time.ParseDuration understands
// units like "ns", "us", "ms", "s", "m", "h".
//
// A later step adds standard 5-field cron syntax ("*/5 * * * *") alongside
// this — ParseSchedule is the single place that decides which format a
// spec string is using.
func ParseSchedule(spec string) (Schedule, error) {
	const prefix = "@every "

	if !strings.HasPrefix(spec, prefix) {
		return Schedule{}, fmt.Errorf("job: unsupported schedule spec %q", spec)
	}

	durStr := strings.TrimPrefix(spec, prefix)
	d, err := time.ParseDuration(durStr)
	if err != nil {
		return Schedule{}, fmt.Errorf("job: invalid duration in spec %q: %w", spec, err)
	}

	if d <= 0 {
		return Schedule{}, fmt.Errorf("job: schedule interval must be positive, got %s", d)
	}

	return Schedule{spec: spec, every: d}, nil
}

// NextRun returns the next time this schedule should fire, given the last
// time it fired (or the job's creation time, if it has never run).
func (s Schedule) NextRun(last time.Time) time.Time {
	return last.Add(s.every)
}

// String implements fmt.Stringer so Schedule prints nicely in logs.
func (s Schedule) String() string {
	return s.spec
}
