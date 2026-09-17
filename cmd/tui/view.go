package main

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"

	"github.com/vaibhavdangaich/orbit/internal/store"
)

// jobColumns is the fixed column layout for the jobs table. Widths are
// generous enough for realistic values (tenant/job names, an "@every 1h30m"
// style schedule) without being wide enough to force horizontal scrolling
// in a normal terminal window.
func jobColumns() []table.Column {
	return []table.Column{
		{Title: "ID", Width: 5},
		{Title: "TENANT", Width: 14},
		{Title: "NAME", Width: 24},
		{Title: "SCHEDULE", Width: 16},
		{Title: "ENABLED", Width: 8},
		{Title: "NEXT RUN", Width: 16},
	}
}

// buildJobRows converts store.JobSummary rows into table.Row values. Pulled
// out of the tea.Model so it's a plain, deterministic function of (jobs,
// now) -- testable without spinning up a bubbletea program or a terminal.
func buildJobRows(jobs []store.JobSummary, now time.Time) []table.Row {
	rows := make([]table.Row, len(jobs))
	for i, j := range jobs {
		enabled := "yes"
		if !j.Enabled {
			enabled = "no"
		}
		rows[i] = table.Row{
			fmt.Sprintf("%d", j.ID),
			j.TenantID,
			j.Name,
			j.Schedule,
			enabled,
			nextRunLabel(j.NextRunAt, now),
		}
	}
	return rows
}

// nextRunLabel renders a job's next_run_at relative to now, e.g. "in 5s" or
// "12s ago" (overdue -- worth noticing at a glance, since it means the
// scheduler is falling behind). time.Duration's own String() already
// formats "5s" / "1m30s" the way we want; the only work here is rounding to
// whole seconds (sub-second precision is just noise on a screen that
// refreshes every couple of seconds) and picking "in"/"ago".
func nextRunLabel(next, now time.Time) string {
	d := next.Sub(now).Round(time.Second)
	if d < 0 {
		return fmt.Sprintf("%s ago", -d)
	}
	if d == 0 {
		return "now"
	}
	return fmt.Sprintf("in %s", d)
}

// Dark-terminal-native palette: muted backgrounds, one accent color per run
// status so the count row scans like a legend. Colors are 256-color-safe
// ANSI approximations (lipgloss falls back gracefully on true-color
// terminals it can't detect).
var (
	colorAccent    = lipgloss.Color("39")  // blue
	colorMuted     = lipgloss.Color("245") // grey
	colorPending   = lipgloss.Color("214") // amber
	colorRunning   = lipgloss.Color("39")  // blue
	colorSucceeded = lipgloss.Color("42")  // green
	colorFailed    = lipgloss.Color("203") // red

	helpStyle = lipgloss.NewStyle().Foreground(colorMuted)

	errStyle = lipgloss.NewStyle().Foreground(colorFailed).Bold(true)

	countLabelStyle = lipgloss.NewStyle().Foreground(colorMuted)
)

// statBox renders one "LABEL\nN" tile for the status-count row, colored by
// status so pending/running/succeeded/failed are distinguishable without
// reading the label.
func statBox(label string, n int, color lipgloss.Color) string {
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(color).
		Padding(0, 2).
		Align(lipgloss.Center)
	return box.Render(fmt.Sprintf("%s\n%s", countLabelStyle.Render(label), lipgloss.NewStyle().Bold(true).Foreground(color).Render(fmt.Sprintf("%d", n))))
}

// renderCounts lays out the four status boxes side by side, labeled with
// the window they're scoped to (see store.RunStatusCounts' doc comment for
// why it's "last N runs" and not a time window).
func renderCounts(c store.RunStatusCounts, window int) string {
	boxes := lipgloss.JoinHorizontal(lipgloss.Top,
		statBox("PENDING", c.Pending, colorPending),
		statBox("RUNNING", c.Running, colorRunning),
		statBox("SUCCEEDED", c.Succeeded, colorSucceeded),
		statBox("FAILED", c.Failed, colorFailed),
	)
	caption := countLabelStyle.Render(fmt.Sprintf("run status -- last %d runs", window))
	return lipgloss.JoinVertical(lipgloss.Left, boxes, caption)
}
