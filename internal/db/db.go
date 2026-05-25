// Package db owns the Postgres connection pool and the cross-consumer
// idempotency helper. Schema is owned by the migrations directory
// (internal/db/migrations) and applied out-of-band by `make migrate-up`
// — Connect no longer mutates the database (ADR 0009).
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pgx pool against dsn. The schema must already have
// been applied by `make migrate-up`; the first query against a
// pre-migration DB will fail loudly ("column X does not exist"), which
// is the operator's signal to run the migration.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool: %w", err)
	}
	return pool, nil
}

// ErrAlreadyProcessed signals that a consumer has already applied the
// given event_id. Callers treat this as a successful no-op so the
// Kafka offset still advances past the duplicate.
var ErrAlreadyProcessed = errors.New("event already processed")

// ClaimEvent records (consumer, event_id) in processed_events. Must
// run inside the same transaction as the business write so they
// commit (or roll back) together. Returns ErrAlreadyProcessed on
// duplicate.
func ClaimEvent(ctx context.Context, tx pgx.Tx, consumer, eventID string) error {
	ct, err := tx.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumer, eventID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrAlreadyProcessed
	}
	return nil
}
