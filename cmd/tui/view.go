package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"

	"github.com/vaibhavdangaich/orbit/internal/store"
)

// jobColumnWeights is the proportional layout for the jobs table --
// fractions of the available width, not fixed character counts. A fixed
// set of widths (the original design) summed to more characters than an
// 80-column terminal actually has, silently clipping the last column.
// Weights scale to whatever width the terminal reports, so the table
// always fits instead of being tuned for one specific size.
var jobColumnWeights = []struct {
	title string
	frac  float64
	min   int
}{
	{"ID", 0.06, 4},
	{"TENANT", 0.16, 10},
	{"NAME", 0.26, 14},
	{"SCHEDULE", 0.16, 12},
	{"ENABLED", 0.10, 7},
	{"NEXT RUN", 0.26, 12},
}

// computeJobColumns turns jobColumnWeights into concrete table.Columns for
// a given terminal width. Called once at startup with a sane default and
// again on every tea.WindowSizeMsg, so resizing the terminal keeps the
// table correctly proportioned instead of the widths being decided once
// and never revisited.
func computeJobColumns(width int) []table.Column {
	// bubbles/table's own rendering (padding between cells, borders) eats
	// a handful of characters beyond the sum of declared column widths --
	// reserving a margin here is what actually fixes the clipping, not
	// just picking smaller numbers by hand.
	avail := width - 10
	if avail < 60 {
		avail = 60
	}

	cols := make([]table.Column, len(jobColumnWeights))
	for i, w := range jobColumnWeights {
		colWidth := int(float64(avail) * w.frac)
		if colWidth < w.min {
			colWidth = w.min
		}
		cols[i] = table.Column{Title: w.title, Width: colWidth}
	}
	return cols
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

// Dark-terminal-native palette: color is reserved for signal, not
// decoration. colorAccent is used in exactly two places -- the divider
// rule and the table's selected-row indicator -- both functional, not
// applied to the banner or headers as decoration. Everything else is
// plain white or muted gray; colorFailed exists for exactly one purpose
// (see renderCounts), not as one more shade in a per-status rainbow.
// Colors are 256-color-safe ANSI approximations (lipgloss falls back
// gracefully on true-color terminals it can't detect).
var (
	colorAccent = lipgloss.Color("39")  // blue -- divider + selection only
	colorMuted  = lipgloss.Color("245") // grey -- labels, captions, help text
	colorFailed = lipgloss.Color("203") // red -- reserved for actual failures

	helpStyle = lipgloss.NewStyle().Foreground(colorMuted)

	errStyle = lipgloss.NewStyle().Foreground(colorFailed).Bold(true)

	countLabelStyle = lipgloss.NewStyle().Foreground(colorMuted)

	dividerStyle = lipgloss.NewStyle().Foreground(colorAccent)
)

// renderDivider draws a single accent-colored rule spanning width -- the
// one deliberate spot of color in the header block, doing a real job
// (separating the logo from the data) rather than decorating it.
func renderDivider(width int) string {
	if width < 1 {
		width = 1
	}
	return dividerStyle.Render(strings.Repeat("─", width))
}

// statFieldWidth is the fixed column width each of the four run-status
// tiles gets in the borderless stats row -- wide enough for "SUCCEEDED"
// (the longest label) plus breathing room.
const statFieldWidth = 16

// renderCounts lays out the four run-status counts as two aligned rows --
// labels, then numbers -- with no borders or boxes around them. The
// earlier design put each count in its own bordered, individually-colored
// tile; four different colors for four routine states (most of which
// don't mean anything is wrong) read as noisy rather than informative.
// This keeps the same at-a-glance scannability through alignment and
// weight (bold numbers under small muted labels) instead of borders and
// color variety.
//
// PENDING/RUNNING/SUCCEEDED are always plain muted/bold-white -- none of
// them signal a problem, so none of them get a color that implies one.
// FAILED is the one number that turns red, and only once it's actually
// nonzero: a red tile for "0 failures" would be a false alarm every time
// the dashboard is healthy, which is worse than no color at all.
func renderCounts(c store.RunStatusCounts, window int) string {
	header := countLabelStyle.Render(fmt.Sprintf("RUN STATUS · last %d runs", window))

	failedColor := lipgloss.Color("255")
	if c.Failed > 0 {
		failedColor = colorFailed
	}

	stats := []struct {
		label string
		n     int
		color lipgloss.Color
	}{
		{"PENDING", c.Pending, lipgloss.Color("255")},
		{"RUNNING", c.Running, lipgloss.Color("255")},
		{"SUCCEEDED", c.Succeeded, lipgloss.Color("255")},
		{"FAILED", c.Failed, failedColor},
	}

	var labelRow, numberRow strings.Builder
	for _, s := range stats {
		labelRow.WriteString(countLabelStyle.Width(statFieldWidth).Render(s.label))
		numberRow.WriteString(lipgloss.NewStyle().Bold(true).Foreground(s.color).Width(statFieldWidth).Render(fmt.Sprintf("%d", s.n)))
	}

	return lipgloss.JoinVertical(lipgloss.Left, header, labelRow.String(), numberRow.String())
}
