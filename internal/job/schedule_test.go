package job

import (
	"testing"
	"time"
)

func TestParseSchedule(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{name: "valid 30 seconds", spec: "@every 30s", wantErr: false},
		{name: "valid 5 minutes", spec: "@every 5m", wantErr: false},
		{name: "missing @every prefix", spec: "*/5 * * * *", wantErr: true},
		{name: "zero duration", spec: "@every 0s", wantErr: true},
		{name: "negative duration", spec: "@every -5s", wantErr: true},
		{name: "unparseable duration", spec: "@every soon", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSchedule(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseSchedule(%q) error = %v, wantErr %v", tt.spec, err, tt.wantErr)
			}
		})
	}
}

func TestScheduleNextRun(t *testing.T) {
	sched, err := ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	last := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	want := last.Add(30 * time.Second)

	got := sched.NextRun(last)
	if !got.Equal(want) {
		t.Fatalf("NextRun() = %v, want %v", got, want)
	}
}
