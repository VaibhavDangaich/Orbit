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

func TestScheduleFastForward(t *testing.T) {
	sched, err := ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t0 := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		dueAt       time.Time
		now         time.Time
		wantFireAt  time.Time
		wantNext    time.Time
		wantSkipped int
	}{
		{
			name:        "on time, no misfire",
			dueAt:       t0,
			now:         t0,
			wantFireAt:  t0,
			wantNext:    t0.Add(30 * time.Second),
			wantSkipped: 0,
		},
		{
			name:        "one period missed",
			dueAt:       t0,
			now:         t0.Add(45 * time.Second),
			wantFireAt:  t0.Add(30 * time.Second),
			wantNext:    t0.Add(60 * time.Second),
			wantSkipped: 1,
		},
		{
			name:        "long outage, ten periods missed",
			dueAt:       t0,
			now:         t0.Add(300 * time.Second),
			wantFireAt:  t0.Add(300 * time.Second),
			wantNext:    t0.Add(330 * time.Second),
			wantSkipped: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fireAt, next, skipped := sched.FastForward(tt.dueAt, tt.now)
			if !fireAt.Equal(tt.wantFireAt) {
				t.Errorf("fireAt = %v, want %v", fireAt, tt.wantFireAt)
			}
			if !next.Equal(tt.wantNext) {
				t.Errorf("next = %v, want %v", next, tt.wantNext)
			}
			if skipped != tt.wantSkipped {
				t.Errorf("skipped = %d, want %d", skipped, tt.wantSkipped)
			}
		})
	}
}
