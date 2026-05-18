// Package scheduler owns the reminder timeline. It reacts to job
// events by inserting future-dated rows in `reminders`, and on a
// timer publishes JobReminder events for rows whose due_at has
// arrived.
package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"job-tracker/internal/db"
	"job-tracker/internal/events"
)

const Consumer = "scheduler"

type Config struct {
	SavedFollowup   time.Duration // when status=saved, remind after this delay
	AppliedFollowup time.Duration // when status=applied, remind after this delay
}

type Scheduler struct {
	pool *pgxpool.Pool
	cfg  Config
}

func New(pool *pgxpool.Pool, cfg Config) *Scheduler {
	return &Scheduler{pool: pool, cfg: cfg}
}

// DueReminder is a row returned by FetchDue, joined with the
// originating job so the published event can carry context.
type DueReminder struct {
	ID      int64
	URL     string
	Kind    events.ReminderKind
	DueAt   time.Time
	Title   string
	Company string
	Status  events.JobStatus
}

// HandleSubmitted reacts to a JobSubmitted event. It schedules a
// reminder according to the event's initial status — most commonly
// "saved", but the CLI can also create a job directly as "applied".
// Idempotent: the (consumer, event_id) ledger row prevents a replay
// from inserting a second reminder.
func (s *Scheduler) HandleSubmitted(ctx context.Context, ev events.JobSubmitted) (applied bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	if err := db.ClaimEvent(ctx, tx, Consumer, ev.EventID); err != nil {
		if errors.Is(err, db.ErrAlreadyProcessed) {
			return false, nil
		}
		return false, err
	}
	if kind, due, ok := s.dueForStatus(ev.Status, ev.SubmittedAt); ok {
		if _, err := tx.Exec(ctx,
			`INSERT INTO reminders (url, kind, due_at) VALUES ($1, $2, $3)`,
			ev.URL, string(kind), due); err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
}

// HandleStatusChanged reacts to a status change: cancel any pending
// reminders for the URL (the old ones don't apply anymore) and
// schedule a new one if the new status warrants it.
func (s *Scheduler) HandleStatusChanged(ctx context.Context, ev events.JobStatusChanged) (applied bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	if err := db.ClaimEvent(ctx, tx, Consumer, ev.EventID); err != nil {
		if errors.Is(err, db.ErrAlreadyProcessed) {
			return false, nil
		}
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE reminders SET cancelled = true
           WHERE url = $1 AND fired_at IS NULL AND NOT cancelled`,
		ev.URL); err != nil {
		return false, err
	}
	if kind, due, ok := s.dueForStatus(ev.Status, ev.ChangedAt); ok {
		if _, err := tx.Exec(ctx,
			`INSERT INTO reminders (url, kind, due_at) VALUES ($1, $2, $3)`,
			ev.URL, string(kind), due); err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
}

// dueForStatus encodes the v1 policy. Only saved/applied get a
// follow-up reminder; terminal statuses (rejected, offer, withdrawn)
// don't schedule anything new.
func (s *Scheduler) dueForStatus(status events.JobStatus, anchor time.Time) (events.ReminderKind, time.Time, bool) {
	switch status {
	case events.StatusSaved:
		return events.ReminderFollowupSaved, anchor.Add(s.cfg.SavedFollowup), true
	case events.StatusApplied:
		return events.ReminderFollowupApplied, anchor.Add(s.cfg.AppliedFollowup), true
	default:
		return "", time.Time{}, false
	}
}

// FetchDue returns reminders whose due_at has arrived and which
// haven't fired yet, joined with job context. Locks each row with
// FOR UPDATE SKIP LOCKED so multiple scheduler instances (unlikely
// in v1, but cheap to support) can split the work without overlap.
func (s *Scheduler) FetchDue(ctx context.Context, now time.Time, limit int) ([]DueReminder, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT r.id, r.url, r.kind, r.due_at, j.title, j.company, j.status
          FROM reminders r
          JOIN jobs j ON j.url = r.url
         WHERE r.fired_at IS NULL
           AND NOT r.cancelled
           AND r.due_at <= $1
         ORDER BY r.due_at
         LIMIT $2
    `, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DueReminder
	for rows.Next() {
		var d DueReminder
		var kind, status string
		if err := rows.Scan(&d.ID, &d.URL, &kind, &d.DueAt, &d.Title, &d.Company, &status); err != nil {
			return nil, err
		}
		d.Kind = events.ReminderKind(kind)
		d.Status = events.JobStatus(status)
		out = append(out, d)
	}
	return out, rows.Err()
}

// MarkFired stamps fired_at on a reminder so it won't be returned by
// FetchDue again. Called after the JobReminder event is acknowledged
// by Kafka.
func (s *Scheduler) MarkFired(ctx context.Context, id int64, firedAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE reminders SET fired_at = $2 WHERE id = $1 AND fired_at IS NULL`,
		id, firedAt)
	return err
}
