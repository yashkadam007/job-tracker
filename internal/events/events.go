// Package events defines the topic names and message schemas exchanged
// over Kafka. Every service in the project imports this package so the
// wire format stays in lockstep.
package events

import "time"

// Topic names. Convention: dot-separated, past tense ("submitted",
// "changed") because Kafka topics carry events that already happened.
const (
	TopicJobSubmitted     = "job.submitted"
	TopicJobStatusChanged = "job.status.changed"
	TopicJobReminder      = "job.reminder"
)

type JobStatus string

const (
	StatusSaved     JobStatus = "saved"
	StatusApplied   JobStatus = "applied"
	StatusInterview JobStatus = "interview"
	StatusRejected  JobStatus = "rejected"
	StatusOffer     JobStatus = "offer"
	StatusWithdrawn JobStatus = "withdrawn"
)

// JobSubmitted is published when a job is first added to the tracker.
type JobSubmitted struct {
	EventID     string    `json:"event_id"`
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	Company     string    `json:"company"`
	Status      JobStatus `json:"status"`
	SubmittedAt time.Time `json:"submitted_at"`
}

// JobStatusChanged is published when an existing job's status changes
// (e.g. saved → applied).
type JobStatusChanged struct {
	EventID   string    `json:"event_id"`
	URL       string    `json:"url"`
	Status    JobStatus `json:"status"`
	ChangedAt time.Time `json:"changed_at"`
}

// ReminderKind labels why a reminder fired so the Notifier can choose
// wording / channel later.
type ReminderKind string

const (
	ReminderFollowupSaved   ReminderKind = "followup_saved"
	ReminderFollowupApplied ReminderKind = "followup_applied"
)

// JobReminder is emitted by the Reminder Scheduler when a scheduled
// reminder comes due. EventID is deterministic ("reminder-<row id>")
// so a crash-and-replay between publish and mark-as-fired turns into
// a duplicate the Notifier safely ignores.
type JobReminder struct {
	EventID  string       `json:"event_id"`
	URL      string       `json:"url"`
	Kind     ReminderKind `json:"kind"`
	DueAt    time.Time    `json:"due_at"`
	Title    string       `json:"title"`
	Company  string       `json:"company"`
	Status   JobStatus    `json:"status"`
	FiredAt  time.Time    `json:"fired_at"`
}
