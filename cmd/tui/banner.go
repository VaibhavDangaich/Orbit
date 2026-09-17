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

// bannerStyle renders the whole banner in one flat, bold accent color --
// no gradient. A per-line color gradient looked showy rather than clean;
// one solid color also ties the banner visually to the rest of the
// dashboard, which already uses colorAccent for the RUNNING status box and
// the table header.
var bannerStyle = lipgloss.NewStyle().Bold(true).Foreground(colorAccent)

func renderBanner() string {
	return bannerStyle.Render(strings.Join(bannerLines, "\n"))
}

var taglineStyle = lipgloss.NewStyle().Foreground(colorMuted).Italic(true)
