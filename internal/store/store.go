// Package store is the Store consumer's persistence layer. It exposes
// idempotent Apply* methods that the consumer calls per event.
//
// Status transitions land in two places: jobs.status (the denormalised
// "what is it now?" cache) and job_status_history (the source of truth
// for "when did it become that?"). Both writes happen in the same
// transaction as the processed_events claim, so a crash mid-write
// rolls back cleanly and the next delivery re-runs them atomically.
package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"job-tracker/internal/db"
	"job-tracker/internal/events"
)

// Consumer name used to namespace this service's ledger entries in
// processed_events.
const Consumer = "store"

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ApplySubmitted upserts a job row, writes the initial status-history
// row, and marks the event processed in one transaction. Returns
// applied=false if the event was a duplicate.
//
// Re-submits overwrite every column from the event — JobSubmitted is
// treated as a full snapshot, not a sparse patch. (A future JobEdited
// event would carry the COALESCE-style partial-update semantics; see
// ADR 0001 "Open question" notes.)
func (s *Store) ApplySubmitted(ctx context.Context, ev events.JobSubmitted) (applied bool, err error) {
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

	techTags := ev.TechTags
	if techTags == nil {
		techTags = []string{}
	}
	customTags := ev.CustomTags
	if customTags == nil {
		customTags = []string{}
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO jobs (
            job_id, url, title, company, status, first_seen_at, last_event_at,
            work_mode, location, seniority, source, tech_tags, description, deadline,
            comp_min, comp_max, comp_currency, comp_equity, comp_bonus,
            resume_version, cover_letter_version, referral,
            recruiter_name, recruiter_email, recruiter_phone,
            priority, custom_tags
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $6,
            $7, $8, $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18,
            $19, $20, $21,
            $22, $23, $24,
            $25, $26
        )
        ON CONFLICT (job_id) DO UPDATE SET
            url                  = EXCLUDED.url,
            title                = EXCLUDED.title,
            company              = EXCLUDED.company,
            status               = EXCLUDED.status,
            last_event_at        = EXCLUDED.last_event_at,
            work_mode            = EXCLUDED.work_mode,
            location             = EXCLUDED.location,
            seniority            = EXCLUDED.seniority,
            source               = EXCLUDED.source,
            tech_tags            = EXCLUDED.tech_tags,
            description          = EXCLUDED.description,
            deadline             = EXCLUDED.deadline,
            comp_min             = EXCLUDED.comp_min,
            comp_max             = EXCLUDED.comp_max,
            comp_currency        = EXCLUDED.comp_currency,
            comp_equity          = EXCLUDED.comp_equity,
            comp_bonus           = EXCLUDED.comp_bonus,
            resume_version       = EXCLUDED.resume_version,
            cover_letter_version = EXCLUDED.cover_letter_version,
            referral             = EXCLUDED.referral,
            recruiter_name       = EXCLUDED.recruiter_name,
            recruiter_email      = EXCLUDED.recruiter_email,
            recruiter_phone      = EXCLUDED.recruiter_phone,
            priority             = EXCLUDED.priority,
            custom_tags          = EXCLUDED.custom_tags
    `,
		ev.JobID, ev.URL, ev.Title, ev.Company, string(ev.Status), ev.SubmittedAt,
		nullableEnum(string(ev.WorkMode)), nullableStr(ev.Location),
		nullableEnum(string(ev.Seniority)), nullableEnum(string(ev.Source)),
		techTags, nullableStr(ev.Description), ev.Deadline,
		ev.CompMin, ev.CompMax,
		nullableStr(ev.CompCurrency), nullableStr(ev.CompEquity), nullableStr(ev.CompBonus),
		nullableStr(ev.ResumeVersion), nullableStr(ev.CoverLetterVersion), nullableStr(ev.Referral),
		nullableStr(ev.RecruiterName), nullableStr(ev.RecruiterEmail), nullableStr(ev.RecruiterPhone),
		ev.Priority, customTags,
	)
	if err != nil {
		return false, err
	}

	// Initial status-history row. event_id UNIQUE makes the replay path
	// a single no-op insert (no second round trip needed).
	if _, err := tx.Exec(ctx, `
        INSERT INTO job_status_history (job_id, status, changed_at, event_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING
    `, ev.JobID, string(ev.Status), ev.SubmittedAt, ev.EventID); err != nil {
		return false, err
	}

	return true, tx.Commit(ctx)
}

// ApplyStatusChanged updates the status of an existing job and appends
// a history row. missing=true means the job_id wasn't in the table
// (status arrived before submit).
func (s *Store) ApplyStatusChanged(ctx context.Context, ev events.JobStatusChanged) (applied bool, missing bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback(ctx)

	if err := db.ClaimEvent(ctx, tx, Consumer, ev.EventID); err != nil {
		if errors.Is(err, db.ErrAlreadyProcessed) {
			return false, false, nil
		}
		return false, false, err
	}

	ct, err := tx.Exec(ctx, `
        UPDATE jobs
           SET status        = $2,
               last_event_at = $3
         WHERE job_id = $1
    `, ev.JobID, string(ev.Status), ev.ChangedAt)
	if err != nil {
		return false, false, err
	}
	if ct.RowsAffected() == 0 {
		// Don't write a history row for a job we don't have; the FK
		// would reject it anyway. Let the caller log and move on.
		return true, true, tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `
        INSERT INTO job_status_history (job_id, status, changed_at, event_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING
    `, ev.JobID, string(ev.Status), ev.ChangedAt, ev.EventID); err != nil {
		return false, false, err
	}

	return true, false, tx.Commit(ctx)
}

// ApplyNoteAdded appends a note to a job's timeline. missing=true if
// the job_id doesn't exist in jobs.
func (s *Store) ApplyNoteAdded(ctx context.Context, ev events.JobNoteAdded) (applied bool, missing bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback(ctx)

	if err := db.ClaimEvent(ctx, tx, Consumer, ev.EventID); err != nil {
		if errors.Is(err, db.ErrAlreadyProcessed) {
			return false, false, nil
		}
		return false, false, err
	}

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE job_id = $1)`, ev.JobID).Scan(&exists); err != nil {
		return false, false, err
	}
	if !exists {
		return true, true, tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `
        INSERT INTO job_notes (job_id, body, created_at, event_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING
    `, ev.JobID, ev.Body, ev.CreatedAt, ev.EventID); err != nil {
		return false, false, err
	}
	return true, false, tx.Commit(ctx)
}

// ApplyInterviewRecorded upserts an interview row keyed by interview_id.
// Uses COALESCE on the partial-update path so a follow-up event that
// only sets, say, completed_at and outcome doesn't wipe round/
// scheduled_at/interviewers from the schedule event that preceded it.
// missing=true if the job_id doesn't exist in jobs.
func (s *Store) ApplyInterviewRecorded(ctx context.Context, ev events.JobInterviewRecorded) (applied bool, missing bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback(ctx)

	if err := db.ClaimEvent(ctx, tx, Consumer, ev.EventID); err != nil {
		if errors.Is(err, db.ErrAlreadyProcessed) {
			return false, false, nil
		}
		return false, false, err
	}

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE job_id = $1)`, ev.JobID).Scan(&exists); err != nil {
		return false, false, err
	}
	if !exists {
		return true, true, tx.Commit(ctx)
	}

	// round is NOT NULL in the table. On insert it must be present; on
	// update it stays untouched via COALESCE. The CHECK constraint
	// rejects an insert with NULL round.
	var interviewers any = ev.Interviewers
	if ev.Interviewers == nil {
		interviewers = nil
	}

	if _, err := tx.Exec(ctx, `
        INSERT INTO job_interviews (
            interview_id, job_id, round, scheduled_at, completed_at, outcome, interviewers, notes, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, COALESCE($7, '{}'::text[]), $8, now()
        )
        ON CONFLICT (interview_id) DO UPDATE SET
            round        = COALESCE(EXCLUDED.round,        job_interviews.round),
            scheduled_at = COALESCE(EXCLUDED.scheduled_at, job_interviews.scheduled_at),
            completed_at = COALESCE(EXCLUDED.completed_at, job_interviews.completed_at),
            outcome      = COALESCE(EXCLUDED.outcome,      job_interviews.outcome),
            interviewers = COALESCE($7,                    job_interviews.interviewers),
            notes        = COALESCE(EXCLUDED.notes,        job_interviews.notes),
            updated_at   = now()
    `,
		ev.InterviewID, ev.JobID,
		nullableEnum(string(ev.Round)),
		ev.ScheduledAt, ev.CompletedAt,
		nullableEnum(string(ev.Outcome)),
		interviewers,
		nullableStr(ev.Notes),
	); err != nil {
		return false, false, err
	}

	return true, false, tx.Commit(ctx)
}

// nullableStr maps "" to a SQL NULL. The CHECK-constrained enum columns
// (work_mode, seniority, source, round, outcome) only accept a fixed
// set of values, so the empty string would fail the constraint —
// callers must use nullableEnum for those instead.
func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullableEnum is nullableStr renamed to make the intent obvious at the
// call site for enum-typed columns.
func nullableEnum(s string) any {
	if s == "" {
		return nil
	}
	return s
}
