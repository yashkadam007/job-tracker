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
	"errors"
	"fmt"
	"sort"
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

	// AdminEndpoints is the list of consumer-side admin /skip-count
	// endpoints surfaced by ADR 0006. The status panel fetches each
	// one on entry. Empty slice = panel renders a "not configured"
	// hint instead of trying.
	AdminEndpoints []AdminEndpoint
}

// AdminEndpoint names a consumer's /skip-count surface.
type AdminEndpoint struct {
	Name string // "store", "scheduler"
	URL  string // "http://homeserver:9090/skip-count"
}

// mode enumerates which sub-UI owns keystrokes right now. list is the
// default; modeNew owns input while the new-job form is open; modeSearch
// owns input while typing into the search box.
type mode int

const (
	modeList mode = iota
	modeNew
	modeSearch
	modeStatus
	modeEdit
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

	// new-job modal. Field values persist across a submit so a
	// validation failure can reopen the form with the same input and
	// focus the offending step.
	newStep    newStep
	newInput   textinput.Model
	newURL     string
	newTitle   string
	newCompany string
	// newErr is the producer-side validation message, rendered as a
	// red banner above the form. Sticky — cleared on next successful
	// submit, modal-close, or modal-open.
	newErr string

	// Company autocomplete state for stepCompany (ADR 0010). companies
	// is the full server snapshot loaded once on modeNew entry; matched
	// is the case-insensitive substring filter against the current
	// textinput value; companyPick is the index into matched.
	companies         []jobclient.Company
	companyMatched    []jobclient.Company
	companyPick       int

	// search
	search     textinput.Model
	searchTerm string

	// status panel (ADR 0006). Lazy-fetched on entry to modeStatus.
	statusLoading bool
	statusResults []skipCountResult
	statusFetched time.Time

	// edit modal (ADR 0011). editJob is the snapshot taken when the
	// modal opens — the modal never re-fetches. editCursor indexes
	// editFields; editing=true means an inline editor (textinput or
	// enum cycler) owns input. The edit* pointers stage individual
	// field changes; nil = "no change for this field", non-nil = "set
	// to dereferenced value" (dereferenced zero = clear-to-NULL).
	editJob          jobclient.Job
	editCursor       int
	editing          bool
	editInput        textinput.Model
	editEnumIdx      int
	editErr          string
	editURL          *string
	editTitle        *string
	editWorkMode     *events.WorkMode
	editLocation     *string
	editSource       *events.Source
	editTechTags     *[]string
	editCustomTags   *[]string
	editPriority     *int
	editExpectedComp *float64
	editNote         string

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
		Background(lipgloss.Color("237"))
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
	// statusW = 11 leaves one space past the widest status ("interview"=9)
	// so column truncation can't bleed into the title column.
	// lastW = 16 matches the "2006-01-02 15:04" format.
	// Reserve 2 chars on the left for the gutter overlay added in tableView.
	statusW := 11
	lastW := 16
	rest := width - statusW - lastW - 6 - 2
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

// fmtWhen is the single source of truth for date display. ISO ordering
// sorts mentally and matches between list and detail.
func fmtWhen(t time.Time) string {
	return t.Local().Format("2006-01-02 15:04")
}

// padRight pads s with trailing spaces to display width n. Used for
// status cells so the column is always exactly the visible width we
// asked for — no ANSI in cells means no width-accounting surprises.
func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func (m Model) Init() tea.Cmd {
	return listJobsCmd(m.cfg.Reader, m.statusFilter)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.tbl.SetColumns(defaultColumns(m.width))
		m.setTableHeight()
		return m, nil

	case jobsLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.err = "list: " + msg.err.Error()
			return m, clearErrAfter(15 * time.Second)
		}
		m.jobs = msg.jobs
		m.applyFilter()
		return m, nil

	case statusChangedMsg:
		if msg.err != nil {
			m.err = "status change: " + msg.err.Error()
			return m, tea.Batch(
				listJobsCmd(m.cfg.Reader, m.statusFilter),
				clearErrAfter(15*time.Second),
			)
		}
		// Reconcile against the real store. The optimistic row update
		// already happened on keypress.
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)

	case snoozedMsg:
		if msg.err != nil {
			m.err = "snooze: " + msg.err.Error()
			return m, clearErrAfter(15 * time.Second)
		}
		return m, nil

	case submittedMsg:
		if msg.err != nil {
			if jobclient.IsValidationError(msg.err) {
				// Producer-side rejection (ADR 0005): reopen the form,
				// banner with the validator's message, focus the
				// offending field. No override — user must correct or
				// cancel.
				m.mode = modeNew
				m.newErr = msg.err.Error()
				m.newStep = stepForValidationError(msg.err)
				m.newInput.SetValue(currentStepValue(&m, m.newStep))
				m.newInput.Focus()
				return m, textinput.Blink
			}
			m.err = "submit: " + msg.err.Error()
			return m, clearErrAfter(15 * time.Second)
		}
		// Successful publish — reset modal scratch state so the next /n
		// starts clean.
		m.newURL, m.newTitle, m.newCompany, m.newErr = "", "", "", ""
		// Re-query so the new row appears once the Store consumer has
		// processed the event. There's a small race here — if the
		// reload arrives before the consumer commits, the row is
		// briefly absent. Acceptable.
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)

	case editedMsg:
		if msg.err != nil {
			if jobclient.IsValidationError(msg.err) {
				// Producer-side rejection: reopen the modal so the
				// operator can correct. Staged edits are kept (the
				// edit* pointers live on Model until enterEditMode
				// resets them).
				m.mode = modeEdit
				m.editErr = msg.err.Error()
				m.editing = false
				return m, nil
			}
			m.err = "edit: " + msg.err.Error()
			return m, tea.Batch(
				listJobsCmd(m.cfg.Reader, m.statusFilter),
				clearErrAfter(15*time.Second),
			)
		}
		return m, listJobsCmd(m.cfg.Reader, m.statusFilter)

	case clearErrMsg:
		m.err = ""
		return m, nil

	case companiesLoadedMsg:
		// Silent on error — the autocomplete is a nice-to-have, not a
		// blocker. The operator can still type any string and submit.
		if msg.err == nil {
			m.companies = msg.companies
			m.recomputeCompanyMatches()
		}
		return m, nil

	case skipCountsLoadedMsg:
		m.statusLoading = false
		m.statusResults = msg.results
		m.statusFetched = time.Now()
		return m, nil

	case errMsg:
		m.err = msg.err.Error()
		return m, clearErrAfter(15 * time.Second)

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
	case modeStatus:
		return m.handleStatusKey(msg)
	case modeEdit:
		return m.handleEditKey(msg)
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
		m.newCompany = ""
		m.newErr = ""
		m.newInput.SetValue("")
		m.newInput.Placeholder = "https://…"
		m.newInput.Focus()
		m.companyMatched = nil
		m.companyPick = 0
		return m, tea.Batch(textinput.Blink, listCompaniesCmd(m.cfg.Reader))
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
	case "H":
		// ADR 0006 status panel — fetches each consumer's /skip-count
		// and surfaces non-zero values. Toggle on H; esc returns to list.
		m.mode = modeStatus
		m.statusLoading = true
		m.statusResults = nil
		return m, fetchSkipCountsCmd(m.cfg.AdminEndpoints)
	case "e":
		job, ok := m.selectedJob()
		if !ok {
			return m, nil
		}
		m.enterEditMode(job)
		return m, nil
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
		m.newErr = ""
		return m, nil
	case "tab", "shift+tab":
		// Tab/Shift+Tab cycles the company autocomplete (ADR 0010).
		// Picking a suggestion fills the input with the canonical
		// company name; Enter then submits that exact string. Only
		// active on stepCompany.
		if m.newStep == stepCompany && len(m.companyMatched) > 0 {
			delta := 1
			if msg.String() == "shift+tab" {
				delta = -1
			}
			m.companyPick = (m.companyPick + delta + len(m.companyMatched)) % len(m.companyMatched)
			m.newInput.SetValue(m.companyMatched[m.companyPick].Name)
			m.newInput.CursorEnd()
			return m, nil
		}
	case "enter":
		val := strings.TrimSpace(m.newInput.Value())
		if val == "" {
			return m, nil
		}
		switch m.newStep {
		case stepURL:
			m.newURL = val
			m.newStep = stepTitle
			m.newInput.SetValue(m.newTitle)
			m.newInput.Placeholder = "Senior Software Engineer"
			return m, nil
		case stepTitle:
			m.newTitle = val
			m.newStep = stepCompany
			m.newInput.SetValue(m.newCompany)
			m.newInput.Placeholder = "Acme Corp"
			m.recomputeCompanyMatches()
			return m, nil
		case stepCompany:
			m.newCompany = val
			cmd := submitCmd(m.cfg.Publisher, m.newURL, m.newTitle, m.newCompany)
			m.mode = modeList
			m.newInput.Blur()
			return m, cmd
		}
	}
	var cmd tea.Cmd
	m.newInput, cmd = m.newInput.Update(msg)
	if m.newStep == stepCompany {
		m.recomputeCompanyMatches()
	}
	return m, cmd
}

// recomputeCompanyMatches filters the loaded companies list against the
// current stepCompany input. Case-insensitive substring, capped at 8
// suggestions so the overlay never grows past the modal. Resets the
// highlighted pick to the top of the new match set.
func (m *Model) recomputeCompanyMatches() {
	const maxSuggestions = 8
	q := strings.ToLower(strings.TrimSpace(m.newInput.Value()))
	m.companyMatched = m.companyMatched[:0]
	if q == "" {
		m.companyPick = 0
		return
	}
	for _, c := range m.companies {
		if strings.Contains(strings.ToLower(c.Name), q) {
			m.companyMatched = append(m.companyMatched, c)
			if len(m.companyMatched) >= maxSuggestions {
				break
			}
		}
	}
	m.companyPick = 0
}

// stepForValidationError maps a producer-side validation sentinel back
// to the form field that produced it, so the modal can reopen with
// focus on the offending step. Any unrecognised sentinel falls back to
// stepURL — the earliest step — so the user retraces from the top.
func stepForValidationError(err error) newStep {
	switch {
	case errors.Is(err, jobclient.ErrMissingURL),
		errors.Is(err, jobclient.ErrInvalidURL):
		return stepURL
	case errors.Is(err, jobclient.ErrMissingTitle):
		return stepTitle
	case errors.Is(err, jobclient.ErrMissingCompany):
		return stepCompany
	}
	return stepURL
}

// currentStepValue returns the in-model value for the given step, so
// the textinput can be pre-populated when the form reopens after a
// validation failure.
func currentStepValue(m *Model, s newStep) string {
	switch s {
	case stepURL:
		return m.newURL
	case stepTitle:
		return m.newTitle
	case stepCompany:
		return m.newCompany
	}
	return ""
}

// handleStatusKey owns input while the ADR 0006 skip-count panel is
// open. esc/q returns to the list; H or r re-runs the fetch.
func (m Model) handleStatusKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q", "H":
		m.mode = modeList
		return m, nil
	case "r", "R":
		m.statusLoading = true
		m.statusResults = nil
		return m, fetchSkipCountsCmd(m.cfg.AdminEndpoints)
	}
	return m, nil
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
		// Status is plain text in the list (coloured in the detail
		// panel). Mixing ANSI escapes with bubbles/table column
		// truncation produced the title-column bleed.
		rows = append(rows, table.Row{
			padRight(string(j.Status), 9),
			truncate(j.Title, 60),
			truncate(j.Company, 30),
			fmtWhen(j.LastEventAt),
		})
	}
	m.tbl.SetRows(rows)
	m.setTableHeight()
}

// setTableHeight shrinks the table to the row count when rows fit, so a
// small result set doesn't strand the detail panel at the bottom of the
// terminal. Reserves 14 rows for the surrounding chrome (title, pill,
// three rules, detail block, three-line help, error line).
//
// SetHeight in bubbles/table v1.0.0 sets viewport.Height to
// (h - headersView.Height). Our header has a bottom border, so its
// height is 2 — meaning the desired height must include those 2 lines
// or rows get clipped (e.g. asking for 3 yielded a 1-row viewport).
func (m *Model) setTableHeight() {
	if m.height == 0 {
		return
	}
	const headerH = 2
	maxH := m.height - 14
	if maxH < headerH+1 {
		maxH = headerH + 1
	}
	rows := len(m.view)
	if rows < 1 {
		rows = 1
	}
	h := rows + headerH
	if h > maxH {
		h = maxH
	}
	m.tbl.SetHeight(h)
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
	if m.mode == modeStatus {
		return m.viewStatus()
	}
	if m.mode == modeEdit {
		return m.viewEdit()
	}

	w := m.width
	if w < 40 {
		w = 40
	}
	rule := ruleStyle.Render(strings.Repeat("─", w))

	var top strings.Builder
	top.WriteString(" " + titleStyle.Render("jobtracker — desktop triage"))
	top.WriteString("\n")
	top.WriteString(m.pill())
	top.WriteString("\n")
	top.WriteString(rule)
	top.WriteString("\n")
	top.WriteString(m.tableView())

	var bottom strings.Builder
	bottom.WriteString(m.viewDetail())
	bottom.WriteString("\n")
	bottom.WriteString(m.viewHelp())
	if m.err != "" {
		bottom.WriteString("\n")
		bottom.WriteString(errStyle.Render(m.err))
	}

	topStr := top.String()
	bottomStr := bottom.String()

	// Pin the detail+help block to the bottom of the terminal. With a
	// small result set the table shrinks, and without filler the bottom
	// block rides up just below it; padding pushes it back down.
	if m.height > 0 {
		used := lipgloss.Height(topStr) + lipgloss.Height(bottomStr)
		if pad := m.height - used - 1; pad > 0 {
			return topStr + "\n" + strings.Repeat("\n", pad) + bottomStr
		}
	}
	return topStr + "\n\n" + bottomStr
}

// tableView wraps tbl.View() with a left gutter: a subtle cyan "▌" on
// the selected row, blank on every other. Done as post-processing
// because bubbles/table v0 has no per-row prefix hook — wrapping each
// rendered line is cheaper than forking the widget.
func (m Model) tableView() string {
	raw := m.tbl.View()
	lines := strings.Split(raw, "\n")
	// Header (titles) + border line = 2 lines before body, given
	// Header.BorderBottom(true) in New().
	const headerLines = 2
	selRow := -1
	if len(m.view) > 0 {
		selRow = headerLines + m.tbl.Cursor()
	}
	for i := range lines {
		if i == selRow && i >= 0 && i < len(lines) {
			lines[i] = gutterStyle.Render("▌") + " " + lines[i]
		} else {
			lines[i] = "  " + lines[i]
		}
	}
	return strings.Join(lines, "\n")
}

func (m Model) pill() string {
	left := "filter: any"
	if m.statusFilter != nil {
		left = "filter: " + string(*m.statusFilter)
	}
	pill := pillStyle.Render("[" + left + "]")
	suffix := helpStyle.Render(fmt.Sprintf("%d jobs", len(m.view)))
	switch {
	case m.mode == modeSearch:
		suffix = m.search.View()
	case m.searchTerm != "":
		suffix = helpStyle.Render(fmt.Sprintf("search: %q", m.searchTerm))
	case m.loading:
		suffix = helpStyle.Render("loading…")
	}
	return " " + pill + "  " + suffix
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
		detailLabel.Render("last:    ") + fmtWhen(job.LastEventAt),
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
		"S=saved", "s=snooze1d", "n=new", "e=edit", "/=search", "f=filter", "R=reload",
		"H=health", "q=quit",
	}
	return helpStyle.Render(strings.Join(keys, "  "))
}

// viewStatus renders the ADR 0006 skip-count panel. Each consumer's
// row shows its total and the by-class breakdown; any non-zero value
// is rendered in red to flag "go look at the logs".
func (m Model) viewStatus() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("consumer health — skip counts (ADR 0006)"))
	b.WriteString("\n\n")

	if len(m.cfg.AdminEndpoints) == 0 {
		b.WriteString(helpStyle.Render("no admin endpoints configured — set JOB_TRACKER_ADMIN_ENDPOINTS"))
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("esc=back  r=reload"))
		return b.String()
	}
	if m.statusLoading {
		b.WriteString(helpStyle.Render("fetching…"))
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("esc=back"))
		return b.String()
	}

	for _, r := range m.statusResults {
		b.WriteString(titleStyle.Render(r.endpoint.Name))
		b.WriteString("  ")
		b.WriteString(helpStyle.Render(r.endpoint.URL))
		b.WriteString("\n")
		switch {
		case r.err != nil:
			b.WriteString(errStyle.Render("  error: " + r.err.Error()))
			b.WriteString("\n")
		default:
			b.WriteString("  total: ")
			b.WriteString(countStyle(r.total).Render(fmt.Sprintf("%d", r.total)))
			b.WriteString("\n")
			// Stable ordering — iterate sorted keys
			keys := make([]string, 0, len(r.byClass))
			for k := range r.byClass {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := r.byClass[k]
				b.WriteString("    ")
				b.WriteString(detailLabel.Render(fmt.Sprintf("%-22s", k)))
				b.WriteString(countStyle(v).Render(fmt.Sprintf("%d", v)))
				b.WriteString("\n")
			}
		}
		b.WriteString("\n")
	}

	if !m.statusFetched.IsZero() {
		b.WriteString(helpStyle.Render("fetched " + m.statusFetched.Local().Format(time.RFC822)))
		b.WriteString("\n")
	}
	b.WriteString(helpStyle.Render("esc=back  r=reload"))
	return b.String()
}

// countStyle picks the colour for a count value: red when non-zero
// (the operator-attention signal per ADR 0006), grey when zero.
func countStyle(n int) lipgloss.Style {
	if n > 0 {
		return errStyle
	}
	return helpStyle
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
	var banner string
	if m.newErr != "" {
		banner = errStyle.Render(m.newErr) + "\n\n"
	}
	help := "enter=next  esc=cancel"
	suggestions := ""
	if m.newStep == stepCompany {
		help = "enter=submit  tab=cycle  esc=cancel"
		suggestions = m.viewCompanySuggestions()
	}
	body := banner + titleStyle.Render(label) + "\n\n" + m.newInput.View() + suggestions + "\n\n" +
		helpStyle.Render(help)
	return modalBox.Render(body)
}

// viewCompanySuggestions renders the matched-companies list under the
// stepCompany input. Empty when there are no matches — the modal then
// looks identical to the URL/title steps. The highlighted row is the
// one Tab will fill into the input next. Tag badges are deferred per
// the ADR 0010 resolved notes.
func (m Model) viewCompanySuggestions() string {
	if len(m.companyMatched) == 0 {
		return ""
	}
	var b strings.Builder
	for i, c := range m.companyMatched {
		b.WriteString("\n")
		if i == m.companyPick {
			b.WriteString(gutterStyle.Render("▌") + " " + c.Name)
		} else {
			b.WriteString("  " + c.Name)
		}
	}
	return b.String()
}
