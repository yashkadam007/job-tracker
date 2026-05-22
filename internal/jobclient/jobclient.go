// Package jobclient is the shared frontend client for the job-tracker
// event/read paths. Two narrow types live here:
//
//   - Publisher  — long-lived kgo wrapper. Frontends call Submit /
//     ChangeStatus / AddNote / RecordInterview; it owns topic names,
//     partition key, ack policy, and JSON encoding.
//   - Reader     — read-only pgxpool wrapper. Frontends call List / Get
//     / PendingReminders against the same schema the Store/Scheduler
//     consumers maintain.
//
// Frontends construct one of each at startup in main() and reuse them
// across all subcommands. Writes only flow through Kafka events; Reader
// never mutates Postgres (per ADR 0002).
package jobclient

import (
	"time"

	"job-tracker/internal/events"
)

// Job is the read-side projection of a row in `jobs`. Field names follow
// the column names so a future HTTP read API can wrap Reader 1:1.
type Job struct {
	JobID        string
	URL          string
	Title        string
	Company      string
	Status       events.JobStatus
	FirstSeenAt  time.Time
	LastEventAt  time.Time

	WorkMode    events.WorkMode
	Location    string
	Seniority   events.Seniority
	Source      events.Source
	TechTags    []string
	Description string
	Deadline    *time.Time

	CompMin      *float64
	CompMax      *float64
	CompCurrency string
	CompEquity   string
	CompBonus    string

	ResumeVersion      string
	CoverLetterVersion string
	Referral           string
	RecruiterName      string
	RecruiterEmail     string
	RecruiterPhone     string

	Priority   *int
	CustomTags []string
}

// ListFilter narrows the result set for Reader.List. All fields
// optional. Status nil means "any status". OrderBy is an allowlisted
// column name; the empty string means "default" (last_event_at). Order
// is always DESC in v1 — no direction knob.
type ListFilter struct {
	Status  *events.JobStatus
	Limit   int
	OrderBy string
}

// PendingReminder pairs an unfired, uncancelled reminder with its
// originating job. Used by frontends rendering an "upcoming follow-ups"
// view. At most one row per job — the earliest pending reminder.
type PendingReminder struct {
	JobID   string
	URL     string
	Title   string
	Company string
	Status  events.JobStatus
	Kind    events.ReminderKind
	DueAt   time.Time
}
