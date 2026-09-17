package main

import (
	"testing"
	"time"

	"github.com/vaibhavdangaich/orbit/internal/job"
	"github.com/vaibhavdangaich/orbit/internal/store"
)

func TestNextRunLabel(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		next time.Time
		want string
	}{
		{"due now", now, "now"},
		{"future", now.Add(5 * time.Second), "in 5s"},
		{"future minutes", now.Add(90 * time.Second), "in 1m30s"},
		{"overdue", now.Add(-12 * time.Second), "12s ago"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextRunLabel(tt.next, now)
			if got != tt.want {
				t.Errorf("nextRunLabel(%v, now) = %q, want %q", tt.next, got, tt.want)
			}
		})
	}
}

func TestBuildJobRows(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	jobs := []store.JobSummary{
		{ID: job.ID(1), TenantID: "t1", Name: "hello", Schedule: "@every 10s", Enabled: true, NextRunAt: now.Add(10 * time.Second)},
		{ID: job.ID(2), TenantID: "t2", Name: "world", Schedule: "@every 1m", Enabled: false, NextRunAt: now.Add(-3 * time.Second)},
	}

	rows := buildJobRows(jobs, now)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	want0 := []string{"1", "t1", "hello", "@every 10s", "yes", "in 10s"}
	for i, v := range want0 {
		if rows[0][i] != v {
			t.Errorf("row 0 col %d = %q, want %q", i, rows[0][i], v)
		}
	}

	want1 := []string{"2", "t2", "world", "@every 1m", "no", "3s ago"}
	for i, v := range want1 {
		if rows[1][i] != v {
			t.Errorf("row 1 col %d = %q, want %q", i, rows[1][i], v)
		}
	}
}
