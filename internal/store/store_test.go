package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
)

// defaultTestDSN matches deploy/compose/docker-compose.yml. Override with
// ORBIT_TEST_DATABASE_URL if you're running Postgres somewhere else.
const defaultTestDSN = "postgres://scheduler:scheduler@localhost:5433/scheduler?sslmode=disable"

// newTestStore is a small test helper -- unexported, lives only in _test.go
// files, and never ships in the built binary. This is Go's version of a
// Jest beforeEach/test fixture, just written as a plain function.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	dsn := os.Getenv("ORBIT_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultTestDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not reachable at %s (start it: docker compose -f deploy/compose/docker-compose.yml up -d): %v", dsn, err)
	}
	t.Cleanup(s.Close)

	return s
}

func TestCreateAndGetJob(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	sched, err := job.ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	in := job.Job{
		TenantID:    "tenant-1",
		Name:        "send-welcome-email",
		Schedule:    sched,
		Payload:     []byte(`{"template":"welcome"}`),
		Enabled:     true,
		MaxAttempts: 3,
		NextRunAt:   time.Now().UTC().Truncate(time.Microsecond),
	}

	created, err := s.CreateJob(ctx, in)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.ID == 0 {
		t.Fatal("CreateJob: expected a non-zero ID to be assigned")
	}

	got, err := s.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if got.Name != in.Name {
		t.Errorf("Name = %q, want %q", got.Name, in.Name)
	}
	if got.Schedule.String() != in.Schedule.String() {
		t.Errorf("Schedule = %q, want %q", got.Schedule, in.Schedule)
	}
	if !got.NextRunAt.Equal(in.NextRunAt) {
		t.Errorf("NextRunAt = %v, want %v", got.NextRunAt, in.NextRunAt)
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.GetJob(ctx, job.ID(999_999_999))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetJob: err = %v, want ErrNotFound", err)
	}
}
