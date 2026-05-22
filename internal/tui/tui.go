// Package tui is the Bubble Tea desktop frontend for session-based job
// triage. It owns no state of its own: every render is a fresh
// jobclient.Reader.List against Postgres, and every mutation is a Kafka
// event via jobclient.Publisher. See ADR 0004.
//
// Architecture (one parent Model):
//
//   list view    — bubbles/table of jobs with status pill + search box
//   detail view  — selected job's metadata + pending reminder
//   new modal    — three-step prompt (URL → title → company) on `n`
//
// All async work is wrapped in tea.Cmds (see cmds.go) so a slow tailnet
// never blocks the UI.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/jackc/pgx/v5/pgxpool"

	"job-tracker/internal/events"
	"job-tracker/internal/jobclient"
)

// Config bundles wired dependencies. Build in main(), pass to New.
type Config struct {
	Publisher *jobclient.Publisher
	Reader    *jobclient.Reader
	Pool      *pgxpool.Pool
}

// mode enumerates which sub-UI owns keystrokes right now. list is the
// default; modeNew owns input while the new-job form is open; modeSearch
// owns input while typing into the search box.
type mode int

const (
	modeList mode = iota
	modeNew
	modeSearch
)

// newStep tracks which field of the new-job form the user is filling.
type newStep int

const (
	stepURL newStep = iota
	stepTitle
	stepCompany
)

// Model is the parent Bubble Tea model. Holds the full job set in
// `jobs` (most recent server snapshot) and a `view` slice that's
// `jobs` filtered by the active search query.
type Model struct {
	cfg Config

	jobs   []jobclient.Job // last server snapshot
	view   []jobclient.Job // jobs filtered by search query
	tbl    table.Model
	width  int
	height int

	statusFilter *events.JobStatus // nil = any status

	mode mode

	// new-job modal
	newStep   newStep
	newInput  textinput.Model
	newURL    string
	newTitle  string

	// search
	search     textinput.Model
	searchTerm string

	loading bool
	err     string
}

// New constructs a Model ready to be passed to tea.NewProgram. The
// initial List is dispatched from Init so the program is interactive
// while the first query is in flight.
func New(cfg Config) Model {
	tbl := table.New(
		table.WithColumns(defaultColumns(80)),
		table.WithFocused(true),
		table.WithHeight(15),
	)
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("231")).
		Background(lipgloss.Color("57")).
		Bold(true)
	tbl.SetStyles(s)

	ni := textinput.New()
	ni.CharLimit = 1024
	ni.Width = 60

	si := textinput.New()
	si.CharLimit = 100
	si.Width = 40
	si.Prompt = "/ "

	return Model{
		cfg:      cfg,
		tbl:      tbl,
		newInput: ni,
		search:   si,
		loading:  true,
	}
}

func defaultColumns(width int) []table.Column {
	// status • title • company • last_event
	// last_event is fixed; status is small; title/company share the rest.
	statusW := 10
	lastW := 18
	rest := width - statusW - lastW - 6 // account for padding/borders
	if rest < 30 {
		rest = 30
	}
	titleW := rest * 6 / 10
	companyW := rest - titleW
	return []table.Column{
		{Title: "status", Width: statusW},
		{Title: "title", Width: titleW},
		{Title: "company", Width: companyW},
		{Title: "last event", Width: lastW},
	}
}

func (m Model) Init() tea.Cmd {
	return listJobsCmd(m.cfg.Reader, m.statusFilter)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.tbl.SetColumns(defaultColumns(m.width))
		// Reserve 8 rows for header, pill, detail, help, error.
		h := m.height - 14
		if h < 5 {
			h = 5
		}
		m.tbl.SetHeight(h)
		return m, nil

	case jobsLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.err = "list: " + msg.err.Error()
			return m, clearErrAfter(4 * time.Second)
		}
		m.jobs = msg.jobs
		m.applyFilter()
		return m, nil

	case statusChangedMsg:
		if msg.err != nil {
			m.err = "status change: " + msg.err.Error()
			return m, tea.Batch(
				listJobsCmd(m.cfg.Reader, m.statusFilter),
				clearErrAfter(4*time.Second),
			)
		}
		// Reconcile against the real store. The optimistic row update
		// already happened on keypress.
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)

	case snoozedMsg:
		if msg.err != nil {
			m.err = "snooze: " + msg.err.Error()
			return m, clearErrAfter(4 * time.Second)
		}
		return m, nil

	case submittedMsg:
		if msg.err != nil {
			m.err = "submit: " + msg.err.Error()
			return m, clearErrAfter(4 * time.Second)
		}
		// Re-query so the new row appears once the Store consumer has
		// processed the event. There's a small race here — if the
		// reload arrives before the consumer commits, the row is
		// briefly absent. Acceptable.
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)

	case clearErrMsg:
		m.err = ""
		return m, nil

	case errMsg:
		m.err = msg.err.Error()
		return m, clearErrAfter(4 * time.Second)

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.mode {
	case modeNew:
		return m.handleNewKey(msg)
	case modeSearch:
		return m.handleSearchKey(msg)
	}

	// modeList
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "n":
		m.mode = modeNew
		m.newStep = stepURL
		m.newURL = ""
		m.newTitle = ""
		m.newInput.SetValue("")
		m.newInput.Placeholder = "https://…"
		m.newInput.Focus()
		return m, textinput.Blink
	case "/":
		m.mode = modeSearch
		m.search.Focus()
		m.search.SetValue(m.searchTerm)
		return m, textinput.Blink
	case "esc":
		if m.searchTerm != "" {
			m.searchTerm = ""
			m.applyFilter()
		}
		return m, nil
	case "f":
		// Cycle the status filter pill through (any, saved, applied,
		// interview, offer, rejected, withdrawn).
		m.statusFilter = nextStatusFilter(m.statusFilter)
		m.loading = true
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)
	case "r":
		// Lowercase r = "rejected" hotkey on the selected row.
		return m.applyStatus(events.StatusRejected)
	case "a":
		return m.applyStatus(events.StatusApplied)
	case "i":
		return m.applyStatus(events.StatusInterview)
	case "o":
		return m.applyStatus(events.StatusOffer)
	case "w":
		return m.applyStatus(events.StatusWithdrawn)
	case "S":
		// shift-s — back to saved. Lowercase s is snooze.
		return m.applyStatus(events.StatusSaved)
	case "s":
		job, ok := m.selectedJob()
		if !ok {
			return m, nil
		}
		return m, snoozeCmd(m.cfg.Pool, job.JobID)
	case "R":
		// shift-r — explicit reload.
		m.loading = true
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)
	}

	var cmd tea.Cmd
	m.tbl, cmd = m.tbl.Update(msg)
	return m, cmd
}

func (m Model) handleNewKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = modeList
		m.newInput.Blur()
		return m, nil
	case "enter":
		val := strings.TrimSpace(m.newInput.Value())
		if val == "" {
			return m, nil
		}
		switch m.newStep {
		case stepURL:
			m.newURL = val
			m.newStep = stepTitle
			m.newInput.SetValue("")
			m.newInput.Placeholder = "Senior Software Engineer"
			return m, nil
		case stepTitle:
			m.newTitle = val
			m.newStep = stepCompany
			m.newInput.SetValue("")
			m.newInput.Placeholder = "Acme Corp"
			return m, nil
		case stepCompany:
			cmd := submitCmd(m.cfg.Publisher, m.newURL, m.newTitle, val)
			m.mode = modeList
			m.newInput.Blur()
			return m, cmd
		}
	}
	var cmd tea.Cmd
	m.newInput, cmd = m.newInput.Update(msg)
	return m, cmd
}

func (m Model) handleSearchKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = modeList
		m.search.Blur()
		return m, nil
	case "enter":
		m.searchTerm = strings.TrimSpace(m.search.Value())
		m.mode = modeList
		m.search.Blur()
		m.applyFilter()
		return m, nil
	}
	var cmd tea.Cmd
	m.search, cmd = m.search.Update(msg)
	return m, cmd
}

// applyStatus is the optimistic-update path: mutate the in-memory row
// immediately so the keystroke feels instant, fire the event, and let
// the follow-up List reconcile against the real store state.
func (m Model) applyStatus(s events.JobStatus) (tea.Model, tea.Cmd) {
	job, ok := m.selectedJob()
	if !ok {
		return m, nil
	}
	// Mutate both jobs and view (view is just a filtered alias).
	for i := range m.jobs {
		if m.jobs[i].JobID == job.JobID {
			m.jobs[i].Status = s
			break
		}
	}
	m.applyFilter()
	return m, changeStatusCmd(m.cfg.Publisher, job.JobID, s)
}

// selectedJob returns the job under the cursor, if any. The table's
// cursor is into the filtered view.
func (m Model) selectedJob() (jobclient.Job, bool) {
	if len(m.view) == 0 {
		return jobclient.Job{}, false
	}
	i := m.tbl.Cursor()
	if i < 0 || i >= len(m.view) {
		return jobclient.Job{}, false
	}
	return m.view[i], true
}

// applyFilter re-derives `view` from `jobs` using the current search
// term (client-side, per ADR 0004 v1) and refreshes the table rows.
// Server-side filter on status already happened in the List call.
func (m *Model) applyFilter() {
	term := strings.ToLower(m.searchTerm)
	m.view = m.view[:0]
	for _, j := range m.jobs {
		if term == "" || matchesSearch(j, term) {
			m.view = append(m.view, j)
		}
	}
	rows := make([]table.Row, 0, len(m.view))
	for _, j := range m.view {
		rows = append(rows, table.Row{
			styleStatus(string(j.Status)),
			truncate(j.Title, 60),
			truncate(j.Company, 30),
			j.LastEventAt.Local().Format("Jan 02 15:04"),
		})
	}
	m.tbl.SetRows(rows)
}

func matchesSearch(j jobclient.Job, term string) bool {
	if strings.Contains(strings.ToLower(j.Title), term) {
		return true
	}
	if strings.Contains(strings.ToLower(j.Company), term) {
		return true
	}
	if strings.Contains(strings.ToLower(j.URL), term) {
		return true
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

// nextStatusFilter advances the status pill: any → saved → applied →
// interview → offer → rejected → withdrawn → any.
func nextStatusFilter(cur *events.JobStatus) *events.JobStatus {
	order := []events.JobStatus{
		events.StatusSaved, events.StatusApplied, events.StatusInterview,
		events.StatusOffer, events.StatusRejected, events.StatusWithdrawn,
	}
	if cur == nil {
		s := order[0]
		return &s
	}
	for i, s := range order {
		if s == *cur {
			if i == len(order)-1 {
				return nil
			}
			next := order[i+1]
			return &next
		}
	}
	return nil
}

// View renders top-to-bottom: title, status pill, table, detail box,
// help line, error line. The new-job modal overlays everything.
func (m Model) View() string {
	if m.mode == modeNew {
		return m.viewNew()
	}

	var b strings.Builder
	b.WriteString(titleStyle.Render("jobtracker — desktop triage"))
	b.WriteString("\n")
	b.WriteString(m.pill())
	b.WriteString("\n\n")
	b.WriteString(m.tbl.View())
	b.WriteString("\n\n")
	b.WriteString(m.viewDetail())
	b.WriteString("\n")
	b.WriteString(m.viewHelp())
	if m.err != "" {
		b.WriteString("\n")
		b.WriteString(errStyle.Render(m.err))
	}
	return b.String()
}

func (m Model) pill() string {
	left := "filter: any"
	if m.statusFilter != nil {
		left = "filter: " + string(*m.statusFilter)
	}
	pill := pillStyle.Render(left)
	if m.mode == modeSearch {
		return pill + "  " + m.search.View()
	}
	if m.searchTerm != "" {
		return pill + "  " + helpStyle.Render(fmt.Sprintf("search: %q", m.searchTerm))
	}
	if m.loading {
		return pill + "  " + helpStyle.Render("loading…")
	}
	return pill + "  " + helpStyle.Render(fmt.Sprintf("%d jobs", len(m.view)))
}

func (m Model) viewDetail() string {
	job, ok := m.selectedJob()
	if !ok {
		return detailBox.Width(m.detailWidth()).Render(helpStyle.Render("no job selected"))
	}
	lines := []string{
		fmt.Sprintf("%s  %s", styleStatus(string(job.Status)), titleStyle.Render(job.Title)),
		detailLabel.Render("company: ") + job.Company,
		detailLabel.Render("url:     ") + truncate(job.URL, m.detailWidth()-12),
		detailLabel.Render("last:    ") + job.LastEventAt.Local().Format(time.RFC822),
	}
	if job.Location != "" {
		lines = append(lines, detailLabel.Render("loc:     ")+job.Location)
	}
	if len(job.TechTags) > 0 {
		lines = append(lines, detailLabel.Render("tags:    ")+strings.Join(job.TechTags, ", "))
	}
	return detailBox.Width(m.detailWidth()).Render(strings.Join(lines, "\n"))
}

func (m Model) detailWidth() int {
	w := m.width - 2
	if w < 40 {
		return 40
	}
	return w
}

func (m Model) viewHelp() string {
	keys := []string{
		"a=applied", "i=interview", "o=offer", "r=rejected", "w=withdrawn",
		"S=saved", "s=snooze1d", "n=new", "/=search", "f=filter", "R=reload",
		"q=quit",
	}
	return helpStyle.Render(strings.Join(keys, "  "))
}

func (m Model) viewNew() string {
	var label string
	switch m.newStep {
	case stepURL:
		label = "new job — URL"
	case stepTitle:
		label = fmt.Sprintf("new job — title  (url=%s)", truncate(m.newURL, 50))
	case stepCompany:
		label = fmt.Sprintf("new job — company  (%s)", truncate(m.newTitle, 50))
	}
	body := titleStyle.Render(label) + "\n\n" + m.newInput.View() + "\n\n" +
		helpStyle.Render("enter=next  esc=cancel")
	return modalBox.Render(body)
}
