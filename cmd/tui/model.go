package main

import (
	"context"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/vaibhavdangaich/orbit/internal/store"
)

// model is orbit-dashboard's single tea.Model. It's intentionally one
// screen, no navigation between views -- see cmd/tui/main.go's doc comment
// for why: this phase's scope is "show live state," not "build an admin
// console."
type model struct {
	s      *store.Store
	table  table.Model
	counts store.RunStatusCounts
	err    error

	refreshInterval time.Duration
	jobLimit        int
	runWindow       int
}

// dataMsg carries the result of one poll of the store. err is non-nil if
// either query failed (e.g. Postgres briefly unreachable) -- the dashboard
// keeps showing the last good data underneath a one-line error rather than
// blanking the screen, since a transient hiccup shouldn't make the whole
// TUI look broken.
type dataMsg struct {
	jobs   []store.JobSummary
	counts store.RunStatusCounts
	err    error
}

type tickMsg time.Time

func newModel(s *store.Store, refreshInterval time.Duration, jobLimit, runWindow int) model {
	t := table.New(
		table.WithColumns(jobColumns()),
		table.WithFocused(true),
	)
	t.SetStyles(tableStyles())

	return model{
		s:               s,
		table:           t,
		refreshInterval: refreshInterval,
		jobLimit:        jobLimit,
		runWindow:       runWindow,
	}
}

func tableStyles() table.Styles {
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(colorMuted).
		BorderBottom(true).
		Bold(true).
		Foreground(colorAccent)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("0")).
		Background(colorAccent).
		Bold(false)
	return s
}

// fetch queries the store once and reports the result as a dataMsg. It's a
// tea.Cmd -- bubbletea runs it in its own goroutine and delivers whatever
// it returns back into Update, which is how a blocking Postgres round trip
// stays off the render loop.
func (m model) fetch() tea.Msg {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobs, err := m.s.ListJobs(ctx, m.jobLimit)
	if err != nil {
		return dataMsg{err: err}
	}
	counts, err := m.s.RunStatusCounts(ctx, m.runWindow)
	if err != nil {
		return dataMsg{err: err}
	}
	return dataMsg{jobs: jobs, counts: counts}
}

func (m model) tick() tea.Cmd {
	return tea.Tick(m.refreshInterval, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.fetch, m.tick())
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.table.SetWidth(msg.Width)
		// Leave room for the title, the count boxes, the caption line, and
		// the help line above the table -- a fixed budget rather than a
		// perfectly reactive layout, which is plenty for a single-screen
		// dashboard.
		h := msg.Height - 10
		if h < 3 {
			h = 3
		}
		m.table.SetHeight(h)

	case tickMsg:
		return m, tea.Batch(m.fetch, m.tick())

	case dataMsg:
		m.err = msg.err
		if msg.err == nil {
			m.table.SetRows(buildJobRows(msg.jobs, time.Now()))
			m.counts = msg.counts
		}
		return m, nil
	}

	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m model) View() string {
	title := titleStyle.Render("orbit dashboard")
	counts := renderCounts(m.counts, m.runWindow)
	help := helpStyle.Render("q / ctrl+c: quit  •  ↑/↓: scroll jobs  •  refreshes every " + m.refreshInterval.String())

	sections := []string{title, "", counts, "", m.table.View(), "", help}
	if m.err != nil {
		sections = append(sections, "", errStyle.Render("error refreshing: "+m.err.Error()))
	}

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}
