package tui

import "github.com/charmbracelet/lipgloss"

// Small palette per ADR 0004 Notes. ANSI 256 codes; truecolor terminals
// upscale automatically. The screen has one accent (117 cyan) for focus
// signals (selected-row gutter, filter pill); everything else lives in
// neutral greys so status colours in the detail panel pop.
var (
	statusStyles = map[string]lipgloss.Style{
		"saved":      lipgloss.NewStyle().Foreground(lipgloss.Color("33")),  // blue
		"applied":    lipgloss.NewStyle().Foreground(lipgloss.Color("220")), // yellow
		"assessment": lipgloss.NewStyle().Foreground(lipgloss.Color("214")), // orange — adjacent to applied/interview
		"interview":  lipgloss.NewStyle().Foreground(lipgloss.Color("177")), // purple
		"offer":      lipgloss.NewStyle().Foreground(lipgloss.Color("42")),  // green
		"rejected":   lipgloss.NewStyle().Foreground(lipgloss.Color("244")), // grey
		"declined":   lipgloss.NewStyle().Foreground(lipgloss.Color("244")), // grey — terminal
		"withdrawn":  lipgloss.NewStyle().Foreground(lipgloss.Color("244")), // grey
	}

	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("231"))

	helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))

	errStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	infoStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))

	pillStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("117"))

	detailLabel = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))

	ruleStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	gutterStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("117"))

	detailBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1)

	modalBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("33")).
			Padding(1, 2)
)

func styleStatus(s string) string {
	if st, ok := statusStyles[s]; ok {
		return st.Render(s)
	}
	return s
}

var tagBadgeBackgrounds = []lipgloss.Color{
	lipgloss.Color("24"),
	lipgloss.Color("29"),
	lipgloss.Color("53"),
	lipgloss.Color("58"),
	lipgloss.Color("60"),
	lipgloss.Color("88"),
	lipgloss.Color("94"),
	lipgloss.Color("96"),
}

func renderTagBadge(tag string) string {
	bg := tagBadgeBackgrounds[tagColorIndex(tag)]
	return lipgloss.NewStyle().
		Foreground(lipgloss.Color("231")).
		Background(bg).
		Render("[" + tag + "]")
}

func tagColorIndex(tag string) int {
	if len(tagBadgeBackgrounds) == 0 {
		return 0
	}
	sum := 0
	for _, r := range tag {
		sum = (sum*31 + int(r)) % len(tagBadgeBackgrounds)
	}
	return sum
}
