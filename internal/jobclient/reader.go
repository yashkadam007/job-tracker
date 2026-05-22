package jobclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"job-tracker/internal/events"
)

// Reader is the read-only Postgres surface for frontends. It owns the
// pool abstraction so the SQL surface stays inside this package — the
// pool is intentionally not exposed.
type Reader struct {
	pool *pgxpool.Pool
}

// NewReader wraps an existing pool. Callers still own the pool's
// lifecycle (db.Connect → defer pool.Close()); Reader does not close
// the pool on its own.
func NewReader(pool *pgxpool.Pool) *Reader {
	return &Reader{pool: pool}
}

// orderColumns is the OrderBy allowlist. Anything else falls back to
// the default (last_event_at DESC) so a caller can't drop arbitrary
// SQL into the ORDER BY clause.
var orderColumns = map[string]string{
	"":               "last_event_at",
	"last_event_at":  "last_event_at",
	"first_seen_at":  "first_seen_at",
}

const defaultListLimit = 100

const jobColumns = `
    job_id, url, title, company, status, first_seen_at, last_event_at,
    work_mode, location, seniority, source, tech_tags, description, deadline,
    comp_min, comp_max, comp_currency, comp_equity, comp_bonus,
    resume_version, cover_letter_version, referral,
    recruiter_name, recruiter_email, recruiter_phone,
    priority, custom_tags
`

// List returns jobs matching filter, most-recent first. Status nil
// returns every status. Limit ≤ 0 falls back to defaultListLimit.
func (r *Reader) List(ctx context.Context, f ListFilter) ([]Job, error) {
	col, ok := orderColumns[f.OrderBy]
	if !ok {
		col = orderColumns[""]
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}

	query := `SELECT ` + jobColumns + ` FROM jobs`
	var args []any
	if f.Status != nil {
		query += ` WHERE status = $1`
		args = append(args, string(*f.Status))
	}
	query += fmt.Sprintf(` ORDER BY %s DESC LIMIT $%d`, col, len(args)+1)
	args = append(args, limit)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Get returns the job whose url matches. Returns ErrNotFound when no
// row exists.
func (r *Reader) Get(ctx context.Context, url string) (Job, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE url = $1`, url)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, err
	}
	return j, nil
}

// PendingReminders returns one row per job that has an unfired,
// uncancelled reminder — the earliest such reminder per job. Useful
// for "upcoming follow-ups" views.
func (r *Reader) PendingReminders(ctx context.Context) ([]PendingReminder, error) {
	// DISTINCT ON keeps the row with the lowest due_at per job_id. The
	// LEFT JOIN of jobs → reminders is filtered to pending reminders;
	// jobs without pending reminders are dropped by the WHERE clause.
	rows, err := r.pool.Query(ctx, `
        SELECT DISTINCT ON (j.job_id)
               j.job_id, j.url, j.title, j.company, j.status,
               r.kind, r.due_at
          FROM jobs j
     LEFT JOIN reminders r ON r.job_id = j.job_id
                          AND r.fired_at IS NULL
                          AND NOT r.cancelled
         WHERE r.id IS NOT NULL
      ORDER BY j.job_id, r.due_at
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingReminder
	for rows.Next() {
		var p PendingReminder
		var status, kind string
		if err := rows.Scan(&p.JobID, &p.URL, &p.Title, &p.Company, &status, &kind, &p.DueAt); err != nil {
			return nil, err
		}
		p.Status = events.JobStatus(status)
		p.Kind = events.ReminderKind(kind)
		out = append(out, p)
	}
	return out, rows.Err()
}

// scannable is satisfied by both *pgx.Rows iterators and single-row
// QueryRow results — both expose Scan(...).
type scannable interface {
	Scan(dest ...any) error
}

func scanJob(s scannable) (Job, error) {
	var (
		j           Job
		status      string
		workMode    *string
		location    *string
		seniority   *string
		source      *string
		description *string

		compCurrency *string
		compEquity   *string
		compBonus    *string

		resumeVersion      *string
		coverLetterVersion *string
		referral           *string
		recruiterName      *string
		recruiterEmail     *string
		recruiterPhone     *string
	)
	if err := s.Scan(
		&j.JobID, &j.URL, &j.Title, &j.Company, &status, &j.FirstSeenAt, &j.LastEventAt,
		&workMode, &location, &seniority, &source, &j.TechTags, &description, &j.Deadline,
		&j.CompMin, &j.CompMax, &compCurrency, &compEquity, &compBonus,
		&resumeVersion, &coverLetterVersion, &referral,
		&recruiterName, &recruiterEmail, &recruiterPhone,
		&j.Priority, &j.CustomTags,
	); err != nil {
		return Job{}, err
	}
	j.Status = events.JobStatus(status)
	j.WorkMode = events.WorkMode(derefStr(workMode))
	j.Location = derefStr(location)
	j.Seniority = events.Seniority(derefStr(seniority))
	j.Source = events.Source(derefStr(source))
	j.Description = derefStr(description)
	j.CompCurrency = derefStr(compCurrency)
	j.CompEquity = derefStr(compEquity)
	j.CompBonus = derefStr(compBonus)
	j.ResumeVersion = derefStr(resumeVersion)
	j.CoverLetterVersion = derefStr(coverLetterVersion)
	j.Referral = derefStr(referral)
	j.RecruiterName = derefStr(recruiterName)
	j.RecruiterEmail = derefStr(recruiterEmail)
	j.RecruiterPhone = derefStr(recruiterPhone)
	return j, nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
