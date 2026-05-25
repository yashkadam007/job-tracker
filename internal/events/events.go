// Package events defines the topic names and message schemas exchanged
// over Kafka. Every service in the project imports this package so the
// wire format stays in lockstep.
//
// Two rules: field names match Postgres column names (snake_case both
// sides), and payloads describe what happened — never what to do next.
//
// Job identity is `job_id` (producer-generated UUID), not URL. URLs
// rename, redirect, and rot; the job_id is stable for the job's
// lifetime and is the partition key on every job-scoped topic.
package events

import "time"

// Topic names. Convention: dot-separated, past tense ("submitted",
// "changed", "added", "recorded") because Kafka topics carry events
// that already happened.
const (
	TopicJobSubmitted          = "job.submitted"
	TopicJobStatusChanged      = "job.status.changed"
	TopicJobReminder           = "job.reminder"
	TopicJobNoteAdded          = "job.note.added"
	TopicJobInterviewRecorded  = "job.interview.recorded"
	TopicJobEdited             = "job.edited"
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

type WorkMode string

const (
	WorkModeOnsite WorkMode = "onsite"
	WorkModeHybrid WorkMode = "hybrid"
	WorkModeRemote WorkMode = "remote"
)

type Seniority string

const (
	SeniorityIntern    Seniority = "intern"
	SeniorityJunior    Seniority = "junior"
	SeniorityMid       Seniority = "mid"
	SenioritySenior    Seniority = "senior"
	SeniorityStaff     Seniority = "staff"
	SeniorityPrincipal Seniority = "principal"
)

type Source string

const (
	SourceLinkedIn    Source = "linkedin"
	SourceIndeed      Source = "indeed"
	SourceReferral    Source = "referral"
	SourceCompanySite Source = "company_site"
	SourceRecruiter   Source = "recruiter"
	SourceOther       Source = "other"
)

type InterviewRound string

const (
	RoundPhoneScreen   InterviewRound = "phone_screen"
	RoundTechnical     InterviewRound = "technical"
	RoundBehavioral    InterviewRound = "behavioral"
	RoundSystemDesign  InterviewRound = "system_design"
	RoundOnsite        InterviewRound = "onsite"
	RoundFinal         InterviewRound = "final"
	RoundOther         InterviewRound = "other"
)

type InterviewOutcome string

const (
	OutcomePassed    InterviewOutcome = "passed"
	OutcomeFailed    InterviewOutcome = "failed"
	OutcomeNoShow    InterviewOutcome = "no_show"
	OutcomePending   InterviewOutcome = "pending"
	OutcomeWithdrawn InterviewOutcome = "withdrawn"
)

// JobSubmitted is published when a job is first added to the tracker.
// All metadata fields are optional — only JobID/URL/Title/Company/Status
// are required at submit time. Optional fields use omitempty so terse
// submits stay terse on the wire.
type JobSubmitted struct {
	EventID     string    `json:"event_id"`
	JobID       string    `json:"job_id"`
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	Company     string    `json:"company"`
	Status      JobStatus `json:"status"`
	SubmittedAt time.Time `json:"submitted_at"`

	// Posting metadata.
	WorkMode    WorkMode  `json:"work_mode,omitempty"`
	Location    string    `json:"location,omitempty"`
	Seniority   Seniority `json:"seniority,omitempty"`
	Source      Source    `json:"source,omitempty"`
	TechTags    []string  `json:"tech_tags,omitempty"`
	Description string    `json:"description,omitempty"`
	Deadline    *time.Time `json:"deadline,omitempty"`

	// Compensation.
	CompMin      *float64 `json:"comp_min,omitempty"`
	CompMax      *float64 `json:"comp_max,omitempty"`
	CompCurrency string   `json:"comp_currency,omitempty"`
	CompEquity   string   `json:"comp_equity,omitempty"`
	CompBonus    string   `json:"comp_bonus,omitempty"`
	// ExpectedComp is the operator's quoted number (ADR 0011) — distinct
	// from CompMin/CompMax (the posting's advertised range). Shares
	// CompCurrency.
	ExpectedComp *float64 `json:"expected_comp,omitempty"`

	// Application details.
	ResumeVersion      string `json:"resume_version,omitempty"`
	CoverLetterVersion string `json:"cover_letter_version,omitempty"`
	Referral           string `json:"referral,omitempty"`
	RecruiterName      string `json:"recruiter_name,omitempty"`
	RecruiterEmail     string `json:"recruiter_email,omitempty"`
	RecruiterPhone     string `json:"recruiter_phone,omitempty"`

	// Personal scaffolding.
	Priority   *int     `json:"priority,omitempty"`
	CustomTags []string `json:"custom_tags,omitempty"`
}

// JobEdited is published when the operator edits one or more mutable
// fields on an existing job (ADR 0011). Sparse-by-default: every
// editable field is a pointer. nil = no change for this field; non-nil
// = set the column to the dereferenced value. Dereferenced zero values
// mean "clear to NULL" (or '{}' for text[] columns) — the Store
// consumer translates accordingly.
//
// URL and Title are settable-only. Both columns are NOT NULL (and URL
// is UNIQUE), so a pointer to an empty string is rejected by the
// Publisher with ErrMissingURL / ErrMissingTitle — the "zero means
// clear" rule applies to the other seven fields only.
type JobEdited struct {
	EventID  string    `json:"event_id"`
	JobID    string    `json:"job_id"`
	EditedAt time.Time `json:"edited_at"`

	URL          *string   `json:"url,omitempty"`
	Title        *string   `json:"title,omitempty"`
	WorkMode     *WorkMode `json:"work_mode,omitempty"`
	Location     *string   `json:"location,omitempty"`
	Source       *Source   `json:"source,omitempty"`
	TechTags     *[]string `json:"tech_tags,omitempty"`
	CustomTags   *[]string `json:"custom_tags,omitempty"`
	Priority     *int      `json:"priority,omitempty"`
	ExpectedComp *float64  `json:"expected_comp,omitempty"`
}

// JobStatusChanged is published when an existing job's status changes
// (e.g. saved → applied).
type JobStatusChanged struct {
	EventID   string    `json:"event_id"`
	JobID     string    `json:"job_id"`
	Status    JobStatus `json:"status"`
	ChangedAt time.Time `json:"changed_at"`
}

// JobNoteAdded is a free-text note pinned to a job. Append-only — there
// is no edit or delete event for v1.
type JobNoteAdded struct {
	EventID   string    `json:"event_id"`
	JobID     string    `json:"job_id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

// JobInterviewRecorded carries the current state of an interview round.
// The same topic handles "scheduled" and "completed/updated" — the
// store upserts on InterviewID with a COALESCE pattern so a partial
// update doesn't wipe earlier fields.
//
// InterviewID is producer-generated (UUID) and stable across events,
// so a "complete" event upserts the same row as the earlier "schedule".
type JobInterviewRecorded struct {
	EventID      string            `json:"event_id"`
	InterviewID  string            `json:"interview_id"`
	JobID        string            `json:"job_id"`
	Round        InterviewRound    `json:"round,omitempty"`
	ScheduledAt  *time.Time        `json:"scheduled_at,omitempty"`
	CompletedAt  *time.Time        `json:"completed_at,omitempty"`
	Outcome      InterviewOutcome  `json:"outcome,omitempty"`
	Interviewers []string          `json:"interviewers,omitempty"`
	Notes        string            `json:"notes,omitempty"`
	RecordedAt   time.Time         `json:"recorded_at"`
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
//
// URL is carried as descriptive metadata for the Notifier's display
// (it's denormalised from jobs.url at fetch time); identity is JobID.
type JobReminder struct {
	EventID  string       `json:"event_id"`
	JobID    string       `json:"job_id"`
	URL      string       `json:"url"`
	Kind     ReminderKind `json:"kind"`
	DueAt    time.Time    `json:"due_at"`
	Title    string       `json:"title"`
	Company  string       `json:"company"`
	Status   JobStatus    `json:"status"`
	FiredAt  time.Time    `json:"fired_at"`
}
