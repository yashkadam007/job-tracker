// Package store is the Store consumer's persistence layer. It exposes
// idempotent Apply* methods that the consumer calls per event.
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

// ApplySubmitted upserts a job row and marks the event processed in
// one transaction. Returns applied=false if the event was a duplicate.
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

	// On URL conflict, refresh title/company/status to whatever the
	// latest submit said. last_event_at advances; first_seen_at sticks.
	_, err = tx.Exec(ctx, `
        INSERT INTO jobs (url, title, company, status, first_seen_at, last_event_at)
        VALUES ($1, $2, $3, $4, $5, $5)
        ON CONFLICT (url) DO UPDATE SET
            title         = EXCLUDED.title,
            company       = EXCLUDED.company,
            status        = EXCLUDED.status,
            last_event_at = EXCLUDED.last_event_at
    `, ev.URL, ev.Title, ev.Company, string(ev.Status), ev.SubmittedAt)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// ApplyStatusChanged updates the status of an existing job. missing=true
// means the URL wasn't in the table (status arrived before submit).
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
         WHERE url = $1
    `, ev.URL, string(ev.Status), ev.ChangedAt)
	if err != nil {
		return false, false, err
	}
	return true, ct.RowsAffected() == 0, tx.Commit(ctx)
}
