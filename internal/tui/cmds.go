package tui

import (
	"context"
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
