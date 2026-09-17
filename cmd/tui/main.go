// cmd/tui is orbit's terminal dashboard: a read-only, single-screen view of
// job definitions and recent run outcomes, polling internal/store on a
// timer. It never writes anything -- no job creation, no run cancellation --
// on purpose: this phase's job is live visibility, not an admin console.
// Building job-editing UI now would be scope creep ahead of an actual need,
// the same restraint this project already applied to deferring a pluggable
// executor until a second job kind exists.
//
// Like cmd/scheduler and cmd/worker, it never touches SQL directly -- every
// query lives in internal/store (see internal/store/dashboard.go), and it's
// configured entirely by environment variables, no flags or config file.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/vaibhavdangaich/orbit/internal/store"
)

func main() {
	dsn := envOr("ORBIT_DATABASE_URL", store.DefaultDevDSN)
	refreshInterval := envDurationOr("ORBIT_TUI_REFRESH_INTERVAL", 2*time.Second)
	jobLimit := envIntOr("ORBIT_TUI_JOB_LIMIT", 50)
	runWindow := envIntOr("ORBIT_TUI_RUN_WINDOW", 500)

	startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	s, err := store.New(startupCtx, dsn)
	cancel()
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer s.Close()

	m := newModel(s, refreshInterval, jobLimit, runWindow)

	// No tea.WithAltScreen(): this is meant to run comfortably alongside
	// terminal-recording tools and doesn't need a separate screen buffer
	// for a single always-on dashboard view.
	if _, err := tea.NewProgram(m).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "orbit-tui: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func envIntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}
