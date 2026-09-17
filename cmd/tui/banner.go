package main

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// bannerLines is "ORBIT" rendered as ASCII art (figlet, "slant" font),
// baked in at compile time rather than generated at runtime. The project
// name never changes, so there's nothing to gain from pulling in a
// runtime ASCII-art library just to redraw the same six lines every
// startup -- this is a plain string constant, zero new dependencies.
//
// "slant" over the earlier "doom" font specifically because its letterforms
// lean forward on their own -- a genuine italic look baked into the glyph
// shapes, rather than depending on the terminal font's italic rendering
// (which box-drawing-style ASCII art doesn't always support well).
var bannerLines = []string{
	`   ____  ____  ____  __________`,
	`  / __ \/ __ \/ __ )/  _/_  __/`,
	` / / / / /_/ / __  |/ /  / /   `,
	`/ /_/ / _, _/ /_/ // /  / /    `,
	`\____/_/ |_/_____/___/ /_/     `,
}

// bannerStyle renders the banner in plain bold white -- not the accent
// color. Establishing a clear hierarchy is the point: the banner is the
// single largest, boldest thing on screen, so it doesn't also need to be
// the one colored thing to draw the eye. colorAccent is reserved entirely
// for renderDivider and the table's selected-row highlight -- exactly two
// places, both functional (a real dividing line, a real selection
// indicator), not decoration layered onto the logo.
var bannerStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255"))

func renderBanner() string {
	return bannerStyle.Render(strings.Join(bannerLines, "\n"))
}

var taglineStyle = lipgloss.NewStyle().Foreground(colorMuted).Italic(true)
