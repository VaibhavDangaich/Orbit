package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// bannerLines is "ORBIT" rendered as ASCII art (figlet, "doom" font),
// baked in at compile time rather than generated at runtime. The project
// name never changes, so there's nothing to gain from pulling in a
// runtime ASCII-art library just to redraw the same six lines every
// startup -- this is a plain string constant, zero new dependencies.
var bannerLines = []string{
	` _________________ _____ _____ `,
	`|  _  | ___ \ ___ \_   _|_   _|`,
	`| | | | |_/ / |_/ / | |   | |  `,
	`| | | |    /| ___ \ | |   | |  `,
	`\ \_/ / |\ \| |_/ /_| |_  | |  `,
	` \___/\_| \_\____/ \___/  \_/  `,
}

// bannerFrom/bannerTo are the gradient's endpoints: a bright cyan-blue at
// the top fading to violet at the bottom.
var (
	bannerFrom = [3]int{0, 191, 255}  // deep sky blue
	bannerTo   = [3]int{124, 58, 237} // violet
)

// renderBanner draws the logo with a top-to-bottom color gradient -- one
// interpolated hex color per line -- rather than one flat color. lipgloss
// has no built-in gradient primitive for plain multi-line strings, so this
// interpolates RGB components by hand across the banner's line count; six
// lines is few enough that the per-line color steps read as a smooth
// gradient rather than a handful of visible bands.
func renderBanner() string {
	lines := make([]string, len(bannerLines))
	steps := len(bannerLines) - 1

	for i, line := range bannerLines {
		t := float64(i) / float64(steps)
		r := lerp(bannerFrom[0], bannerTo[0], t)
		g := lerp(bannerFrom[1], bannerTo[1], t)
		b := lerp(bannerFrom[2], bannerTo[2], t)

		style := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(hexColor(r, g, b)))
		lines[i] = style.Render(line)
	}

	return strings.Join(lines, "\n")
}

func lerp(a, b int, t float64) int {
	return a + int(float64(b-a)*t)
}

func hexColor(r, g, b int) string {
	return fmt.Sprintf("#%02x%02x%02x", r, g, b)
}

var taglineStyle = lipgloss.NewStyle().Foreground(colorMuted).Italic(true)
