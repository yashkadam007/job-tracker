package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"job-tracker/internal/events"
	"job-tracker/internal/jobclient"
)

// Async ops are wrapped as tea.Cmds so a slow tailnet never blocks the
// UI thread. Each command returns a typed message that Update routes.
// Per-op contexts get a bounded timeout — the Bubble Tea Program owns
// the parent ctx (set in main via tea.WithContext) so SIGINT unblocks
// long DB calls too.

type jobsLoadedMsg struct {
	jobs []jobclient.Job
	err  error
}

type statusChangedMsg struct {
	jobID  string
	status events.JobStatus
	err    error
}

type snoozedMsg struct {
	jobID string
	err   error
}

type submittedMsg struct {
	job events.JobSubmitted
	err error
}

// editedMsg is delivered after editJobAndNoteCmd finishes. err is the
// first non-nil error from the Edit publish and (optionally) the note
// publish — the modal renders it verbatim and reopens for correction
// if it's a validation error.
type editedMsg struct {
	jobID string
	err   error
}

type companiesLoadedMsg struct {
	companies []jobclient.Company
	err       error
}

type errMsg struct{ err error }

// clearErrMsg is delivered on a timer to clear a transient error.
type clearErrMsg struct{}

// listJobsCmd re-queries the catalog. Called on startup and after every
// mutation to reconcile optimistic UI with the real store state.
func listJobsCmd(reader *jobclient.Reader, statusFilter *events.JobStatus) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		jobs, err := reader.List(ctx, jobclient.ListFilter{
			Status:  statusFilter,
			Limit:   500,
			OrderBy: "last_event_at",
		})
		return jobsLoadedMsg{jobs: jobs, err: err}
	}
}

// listCompaniesCmd loads the companies list for the new-job modal's
// stepCompany autocomplete (ADR 0010). Cheap — fired once on modeNew
// entry, not on every keystroke.
func listCompaniesCmd(reader *jobclient.Reader) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cs, err := reader.ListCompanies(ctx)
		return companiesLoadedMsg{companies: cs, err: err}
	}
}

// changeStatusCmd publishes job.status.changed. The Store consumer may
// reject an invalid transition; that surfaces as a different status on
// the next List, not as an error here.
func changeStatusCmd(pub *jobclient.Publisher, jobID string, status events.JobStatus) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := pub.ChangeStatus(ctx, events.JobStatusChanged{
			EventID:   uuid.NewString(),
			JobID:     jobID,
			Status:    status,
			ChangedAt: time.Now().UTC(),
		})
		return statusChangedMsg{jobID: jobID, status: status, err: err}
	}
}

// submitCmd publishes job.submitted for a brand-new job.
func submitCmd(pub *jobclient.Publisher, url, title, company string) tea.Cmd {
	ev := events.JobSubmitted{
		EventID:     uuid.NewString(),
		JobID:       uuid.NewString(),
		URL:         url,
		Title:       title,
		Company:     company,
		Status:      events.StatusSaved,
		SubmittedAt: time.Now().UTC(),
	}
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := pub.Submit(ctx, ev)
		return submittedMsg{job: ev, err: err}
	}
}

// editJobAndNoteCmd publishes one JobEdited (the sparse field-edit
// event from ADR 0011) plus, if note is non-empty, one JobNoteAdded.
// Both events share the same timestamp so the timeline groups them.
// Validation runs producer-side inside Publisher.Edit / Publisher.AddNote;
// returns the first non-nil error so the modal can surface it.
func editJobAndNoteCmd(pub *jobclient.Publisher, ev events.JobEdited, note string) tea.Cmd {
	ev.EventID = uuid.NewString()
	ev.EditedAt = time.Now().UTC()
	hasEdit := ev.URL != nil || ev.Title != nil || ev.WorkMode != nil ||
		ev.Location != nil || ev.Source != nil || ev.TechTags != nil ||
		ev.CustomTags != nil || ev.Priority != nil || ev.ExpectedComp != nil
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Empty-edit guard at the Publisher (ErrEmptyEdit) means we
		// only publish the field-edit event when something actually
		// changed. A note-only save (the operator added a note but
		// touched no fields) still publishes the JobNoteAdded below.
		if hasEdit {
			if err := pub.Edit(ctx, ev); err != nil {
				return editedMsg{jobID: ev.JobID, err: err}
			}
		}
		if strings.TrimSpace(note) != "" {
			nev := events.JobNoteAdded{
				EventID:   uuid.NewString(),
				JobID:     ev.JobID,
				Body:      note,
				CreatedAt: ev.EditedAt,
			}
			if err := pub.AddNote(ctx, nev); err != nil {
				return editedMsg{jobID: ev.JobID, err: err}
			}
		}
		return editedMsg{jobID: ev.JobID}
	}
}

// snoozeCmd inserts a fresh reminder 24h out — same shape as the bot's
// snooze callback (ADR 0003 Notes / ADR 0004). Direct INSERT rather
// than an event because reminders are scheduler-local state, not part
// of the job event log.
func snoozeCmd(pool *pgxpool.Pool, jobID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		due := time.Now().UTC().Add(24 * time.Hour)
		_, err := pool.Exec(ctx,
			`INSERT INTO reminders (job_id, kind, due_at) VALUES ($1, $2, $3)`,
			jobID, string(events.ReminderFollowupSaved), due)
		return snoozedMsg{jobID: jobID, err: err}
	}
}

// clearErrAfter returns a Cmd that fires clearErrMsg after d. Used to
// auto-dismiss the transient error line.
func clearErrAfter(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(time.Time) tea.Msg { return clearErrMsg{} })
}

// skipCountResult is one row of the status panel — the result of
// fetching a single consumer's /skip-count endpoint (ADR 0006).
type skipCountResult struct {
	endpoint AdminEndpoint
	total    int
	byClass  map[string]int
	err      error
}

type skipCountsLoadedMsg struct {
	results []skipCountResult
}

// fetchSkipCountsCmd hits every configured admin /skip-count endpoint
// in parallel with a short timeout. The TUI is allowed to render a
// stale view if one consumer is unreachable — that itself is a useful
// signal to the operator. Results come back in the same order as the
// configured endpoints so the panel is stable across refreshes.
func fetchSkipCountsCmd(endpoints []AdminEndpoint) tea.Cmd {
	return func() tea.Msg {
		results := make([]skipCountResult, len(endpoints))
		var wg sync.WaitGroup
		for i, ep := range endpoints {
			wg.Add(1)
			go func(i int, ep AdminEndpoint) {
				defer wg.Done()
				results[i] = fetchSkipCount(ep)
			}(i, ep)
		}
		wg.Wait()
		return skipCountsLoadedMsg{results: results}
	}
}

func fetchSkipCount(ep AdminEndpoint) skipCountResult {
	out := skipCountResult{endpoint: ep}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.URL, nil)
	if err != nil {
		out.err = err
		return out
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		out.err = err
		return out
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		out.err = fmt.Errorf("http %d", resp.StatusCode)
		return out
	}
	var body struct {
		Count   int            `json:"count"`
		ByClass map[string]int `json:"by_class"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		out.err = err
		return out
	}
	out.total = body.Count
	out.byClass = body.ByClass
	return out
}
