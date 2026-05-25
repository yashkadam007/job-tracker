package tui

import "github.com/charmbracelet/lipgloss"

// Small palette per ADR 0004 Notes. ANSI 256 codes; truecolor terminals
// upscale automatically. The screen has one accent (117 cyan) for focus
// signals (selected-row gutter, filter pill); everything else lives in
// neutral greys so status colours in the detail panel pop.
var (
	statusStyles = map[string]lipgloss.Style{
		"saved":     lipgloss.NewStyle().Foreground(lipgloss.Color("33")),  // blue
		"applied":   lipgloss.NewStyle().Foreground(lipgloss.Color("220")), // yellow
		"interview": lipgloss.NewStyle().Foreground(lipgloss.Color("177")), // purple
		"offer":     lipgloss.NewStyle().Foreground(lipgloss.Color("42")),  // green
		"rejected":  lipgloss.NewStyle().Foreground(lipgloss.Color("244")), // grey
		"withdrawn": lipgloss.NewStyle().Foreground(lipgloss.Color("244")), // grey
	}

	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("231"))

	helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))

	errStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))

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
