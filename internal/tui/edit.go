package tui

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"job-tracker/internal/events"
	"job-tracker/internal/jobclient"
)

// modeEdit owns input while the edit modal is open. Field navigation is
// up/down; Enter focuses an inline editor for the highlighted field;
// Ctrl+S saves the staged changes as one JobEdited (plus one
// JobNoteAdded if the note line is filled). Esc-from-editor returns to
// the field list; Esc-from-field-list cancels the modal.

// editFieldKind discriminates the per-field interaction. Mirrors the
// per-field interaction table in ADR 0011.
type editFieldKind int

const (
	editKindRequiredText editFieldKind = iota // url, title — cannot be cleared
	editKindText                              // location
	editKindEnum                              // work_mode, source
	editKindNumber                            // priority, expected_comp
	editKindTags                              // tech_tags, custom_tags
	editKindNote                              // virtual "new note" appender
)

// editField describes one row in the modal's field list.
type editField struct {
	key     string // matches the JobEdited / column name
	label   string // display label in the list
	kind    editFieldKind
	options []string // for enums; first option is the explicit "(unset)" / clear
}

// editFields is the v1 surface (ADR 0011 Assumptions). Adding a field
// later is a TUI-only commit; the event already carries the rest.
var editFields = []editField{
	{key: "url", label: "url", kind: editKindRequiredText},
	{key: "title", label: "title", kind: editKindRequiredText},
	{key: "work_mode", label: "work mode", kind: editKindEnum,
		options: []string{"(unset)", "onsite", "hybrid", "remote"}},
	{key: "location", label: "location", kind: editKindText},
	{key: "source", label: "source", kind: editKindEnum,
		options: []string{"(unset)", "linkedin", "indeed", "referral", "company_site", "recruiter", "other"}},
	{key: "tech_tags", label: "tech tags", kind: editKindTags},
	{key: "custom_tags", label: "custom tags", kind: editKindTags},
	{key: "priority", label: "priority", kind: editKindNumber},
	{key: "expected_comp", label: "expected comp", kind: editKindNumber},
	{key: "note", label: "+ new note", kind: editKindNote},
}

// enterEditMode initialises modal state from the selected job. Called
// from the list-view 'e' keypress. Snapshots the job so the modal
// renders against a stable view even if a later List arrives.
func (m *Model) enterEditMode(job jobclient.Job) {
	m.mode = modeEdit
	m.editJob = job
	m.editCursor = 0
	m.editing = false
	m.editErr = ""
	m.editURL = nil
	m.editTitle = nil
	m.editWorkMode = nil
	m.editLocation = nil
	m.editSource = nil
	m.editTechTags = nil
	m.editCustomTags = nil
	m.editPriority = nil
	m.editExpectedComp = nil
	m.editNote = ""
}

// handleEditKey routes keystrokes while the edit modal is open.
func (m Model) handleEditKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.editing {
		return m.handleEditFieldKey(msg)
	}
	switch msg.String() {
	case "esc":
		m.mode = modeList
		return m, nil
	case "ctrl+s":
		return m.publishEdit()
	case "up", "k":
		if m.editCursor > 0 {
			m.editCursor--
		}
		return m, nil
	case "down", "j":
		if m.editCursor < len(editFields)-1 {
			m.editCursor++
		}
		return m, nil
	case "enter":
		f := editFields[m.editCursor]
		switch f.kind {
		case editKindEnum:
			m.editing = true
			m.editEnumIdx = m.currentEnumIndex(f)
			return m, nil
		case editKindRequiredText, editKindText, editKindNumber, editKindTags, editKindNote:
			m.editing = true
			m.editInput = newEditInput(m.currentFieldString(f))
			m.editInput.Focus()
			return m, textinput.Blink
		}
	}
	return m, nil
}

// handleEditFieldKey handles keystrokes while an individual field is
// being edited. esc returns to the field list without commiting.
func (m Model) handleEditFieldKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	f := editFields[m.editCursor]
	if f.kind == editKindEnum {
		switch msg.String() {
		case "esc":
			m.editing = false
			return m, nil
		case "left", "h":
			n := len(f.options)
			m.editEnumIdx = (m.editEnumIdx - 1 + n) % n
			return m, nil
		case "right", "l":
			m.editEnumIdx = (m.editEnumIdx + 1) % len(f.options)
			return m, nil
		case "enter":
			m.commitEnum(f)
			m.editing = false
			return m, nil
		}
		return m, nil
	}

	switch msg.String() {
	case "esc":
		m.editing = false
		m.editInput.Blur()
		return m, nil
	case "enter":
		val := strings.TrimSpace(m.editInput.Value())
		if err := m.commitTextField(f, val); err != nil {
			m.editErr = err.Error()
			return m, nil
		}
		m.editErr = ""
		m.editing = false
		m.editInput.Blur()
		return m, nil
	}
	var cmd tea.Cmd
	m.editInput, cmd = m.editInput.Update(msg)
	return m, cmd
}

// newEditInput returns a textinput pre-populated with v.
func newEditInput(v string) textinput.Model {
	ti := textinput.New()
	ti.CharLimit = 1024
	ti.Width = 60
	ti.SetValue(v)
	ti.CursorEnd()
	return ti
}

// currentEnumIndex returns the index in f.options of the current value
// of the enum field (staged value if any, else the job's value). Falls
// back to 0 ("(unset)") if no match.
func (m Model) currentEnumIndex(f editField) int {
	current := m.currentFieldString(f)
	if current == "" {
		return 0 // "(unset)"
	}
	for i, opt := range f.options {
		if opt == current {
			return i
		}
	}
	return 0
}

// currentFieldString returns the field's current string value — staged
// edit if any, otherwise the snapshot from the job row. Tag fields
// come back as a comma-joined list; numeric fields as decimal.
func (m Model) currentFieldString(f editField) string {
	switch f.key {
	case "url":
		if m.editURL != nil {
			return *m.editURL
		}
		return m.editJob.URL
	case "title":
		if m.editTitle != nil {
			return *m.editTitle
		}
		return m.editJob.Title
	case "work_mode":
		if m.editWorkMode != nil {
			return string(*m.editWorkMode)
		}
		return string(m.editJob.WorkMode)
	case "location":
		if m.editLocation != nil {
			return *m.editLocation
		}
		return m.editJob.Location
	case "source":
		if m.editSource != nil {
			return string(*m.editSource)
		}
		return string(m.editJob.Source)
	case "tech_tags":
		if m.editTechTags != nil {
			return strings.Join(*m.editTechTags, ", ")
		}
		return strings.Join(m.editJob.TechTags, ", ")
	case "custom_tags":
		if m.editCustomTags != nil {
			return strings.Join(*m.editCustomTags, ", ")
		}
		return strings.Join(m.editJob.CustomTags, ", ")
	case "priority":
		if m.editPriority != nil {
			if *m.editPriority == 0 {
				return ""
			}
			return strconv.Itoa(*m.editPriority)
		}
		if m.editJob.Priority != nil {
			return strconv.Itoa(*m.editJob.Priority)
		}
		return ""
	case "expected_comp":
		if m.editExpectedComp != nil {
			if *m.editExpectedComp == 0 {
				return ""
			}
			return strconv.FormatFloat(*m.editExpectedComp, 'f', -1, 64)
		}
		if m.editJob.ExpectedComp != nil {
			return strconv.FormatFloat(*m.editJob.ExpectedComp, 'f', -1, 64)
		}
		return ""
	case "note":
		return m.editNote
	}
	return ""
}

// commitEnum stores the enum field's currently-highlighted option onto
// the staged-edit pointers. The "(unset)" option stages a zero-value
// pointer (clear-to-NULL).
func (m *Model) commitEnum(f editField) {
	chosen := f.options[m.editEnumIdx]
	switch f.key {
	case "work_mode":
		var v events.WorkMode
		if chosen != "(unset)" {
			v = events.WorkMode(chosen)
		}
		m.editWorkMode = &v
	case "source":
		var v events.Source
		if chosen != "(unset)" {
			v = events.Source(chosen)
		}
		m.editSource = &v
	}
}

// commitTextField parses and stores a textinput value onto the
// staged-edit pointers. Returns an error message suitable for display
// in the modal banner; the caller stays in editing mode on error.
func (m *Model) commitTextField(f editField, val string) error {
	switch f.key {
	case "url":
		if val == "" {
			return fmt.Errorf("url cannot be cleared")
		}
		m.editURL = &val
	case "title":
		if val == "" {
			return fmt.Errorf("title cannot be cleared")
		}
		m.editTitle = &val
	case "location":
		v := val
		m.editLocation = &v
	case "priority":
		if val == "" {
			z := 0
			m.editPriority = &z
			return nil
		}
		n, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("priority: expected integer, got %q", val)
		}
		if n < 1 || n > 5 {
			return fmt.Errorf("priority: %d; allowed 1-5", n)
		}
		m.editPriority = &n
	case "expected_comp":
		if val == "" {
			z := 0.0
			m.editExpectedComp = &z
			return nil
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return fmt.Errorf("expected_comp: expected number, got %q", val)
		}
		if v <= 0 {
			return fmt.Errorf("expected_comp: must be > 0, got %v", v)
		}
		m.editExpectedComp = &v
	case "tech_tags":
		ts := splitTags(val)
		m.editTechTags = &ts
	case "custom_tags":
		ts := splitTags(val)
		m.editCustomTags = &ts
	case "note":
		m.editNote = val
	}
	return nil
}

// splitTags splits a comma list and trims whitespace. Empty input
// returns an empty slice — the ApplyEdited path writes '{}' to the
// column (clear-to-empty).
func splitTags(v string) []string {
	if strings.TrimSpace(v) == "" {
		return []string{}
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

// publishEdit assembles the staged edits into a JobEdited and dispatches
// the publish command (plus a JobNoteAdded if the note line is filled).
// Returns to the list view on dispatch; the result lands as editedMsg.
func (m Model) publishEdit() (tea.Model, tea.Cmd) {
	ev := events.JobEdited{
		JobID:        m.editJob.JobID,
		URL:          m.editURL,
		Title:        m.editTitle,
		WorkMode:     m.editWorkMode,
		Location:     m.editLocation,
		Source:       m.editSource,
		TechTags:     m.editTechTags,
		CustomTags:   m.editCustomTags,
		Priority:     m.editPriority,
		ExpectedComp: m.editExpectedComp,
	}
	note := strings.TrimSpace(m.editNote)
	if !hasStagedEdit(ev) && note == "" {
		m.editErr = "no fields changed — esc to cancel"
		return m, nil
	}
	cmd := editJobAndNoteCmd(m.cfg.Publisher, ev, note)
	m.mode = modeList
	return m, cmd
}

// hasStagedEdit reports whether the JobEdited has any non-nil field.
// Mirrors validateEdited's empty-edit guard but cheaper than going
// through Publisher just to find out.
func hasStagedEdit(ev events.JobEdited) bool {
	return ev.URL != nil ||
		ev.Title != nil ||
		ev.WorkMode != nil ||
		ev.Location != nil ||
		ev.Source != nil ||
		ev.TechTags != nil ||
		ev.CustomTags != nil ||
		ev.Priority != nil ||
		ev.ExpectedComp != nil
}

// viewEdit renders the modal. List of fields with current values; the
// active field shows an inline textinput (or the cycling enum
// preview). A trailing help line names the keybinds for the current
// state.
func (m Model) viewEdit() string {
	var b strings.Builder
	if m.editErr != "" {
		b.WriteString(errStyle.Render(m.editErr))
		b.WriteString("\n\n")
	}
	b.WriteString(titleStyle.Render(fmt.Sprintf("edit — %s @ %s", truncate(m.editJob.Title, 40), truncate(m.editJob.Company, 30))))
	b.WriteString("\n\n")

	for i, f := range editFields {
		selected := i == m.editCursor
		marker := "  "
		if selected {
			marker = gutterStyle.Render("▌") + " "
		}
		label := detailLabel.Render(fmt.Sprintf("%-15s ", f.label+":"))
		valStr := m.renderFieldValue(f, selected && m.editing)
		b.WriteString(marker + label + valStr + "\n")
	}

	b.WriteString("\n")
	if m.editing {
		f := editFields[m.editCursor]
		if f.kind == editKindEnum {
			b.WriteString(helpStyle.Render("←/→ cycle  enter=accept  esc=back"))
		} else {
			b.WriteString(helpStyle.Render("enter=accept  esc=back"))
		}
	} else {
		b.WriteString(helpStyle.Render("↑/↓ field  enter=edit  ctrl+s=save  esc=cancel"))
	}
	return modalBox.Render(b.String())
}

// renderFieldValue produces the right-hand cell for a single field
// row. While editing, the active field shows the textinput (or the
// cycling enum preview); other fields show their staged-or-current
// value with a "(changed)" tag when the operator has touched them.
func (m Model) renderFieldValue(f editField, active bool) string {
	if active {
		if f.kind == editKindEnum {
			return "← " + titleStyle.Render(f.options[m.editEnumIdx]) + " →"
		}
		return m.editInput.View()
	}
	val := m.currentFieldString(f)
	if val == "" {
		val = helpStyle.Render("(unset)")
	}
	if m.fieldStaged(f.key) {
		val = val + "  " + helpStyle.Render("(changed)")
	}
	return val
}

// fieldStaged reports whether the operator has staged a change for
// the named field on this modal session.
func (m Model) fieldStaged(key string) bool {
	switch key {
	case "url":
		return m.editURL != nil
	case "title":
		return m.editTitle != nil
	case "work_mode":
		return m.editWorkMode != nil
	case "location":
		return m.editLocation != nil
	case "source":
		return m.editSource != nil
	case "tech_tags":
		return m.editTechTags != nil
	case "custom_tags":
		return m.editCustomTags != nil
	case "priority":
		return m.editPriority != nil
	case "expected_comp":
		return m.editExpectedComp != nil
	case "note":
		return strings.TrimSpace(m.editNote) != ""
	}
	return false
}
