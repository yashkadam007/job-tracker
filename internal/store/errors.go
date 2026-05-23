package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// Consumer-side error sentinels. ADR 0006: the Store consumer loop
// branches into three paths (retry / skip / crash) by matching with
// errors.Is against these. Apply* methods wrap their internal errors
// with %w so the loop can classify without type assertions.
var (
	// ErrDecode — json.Unmarshal failure on the record value. Permanent.
	ErrDecode = errors.New("store: decode failure")
	// ErrUnknownTopic — record arrived on a topic the consumer doesn't
	// know how to handle. Permanent (deploy-time schema drift).
	ErrUnknownTopic = errors.New("store: unknown topic")
	// ErrInfraUnavailable — pgx connection error, context-deadline on
	// the DB call, Kafka transient. Transient — retry indefinitely.
	ErrInfraUnavailable = errors.New("store: infra unavailable")
	// ErrConstraintViolation — Postgres CHECK or FK violation that the
	// producer-side validator (ADR 0005) should have caught. Permanent,
	// and a signal that producer/consumer validation has drifted.
	ErrConstraintViolation = errors.New("store: constraint violation")
)

// ErrorClass is the typed result of Classify — one of four values that
// map 1:1 to a branch in the consumer-loop switch.
type ErrorClass int

const (
	ClassNone ErrorClass = iota
	ClassDecode
	ClassUnknownTopic
	ClassInfra
	ClassConstraint
	ClassUnexpected
)

// String returns the short name used in the /skip-count JSON payload
// and in the structured-log "class" field.
func (c ErrorClass) String() string {
	switch c {
	case ClassNone:
		return "none"
	case ClassDecode:
		return "decode"
	case ClassUnknownTopic:
		return "unknown_topic"
	case ClassInfra:
		return "infra_unavailable"
	case ClassConstraint:
		return "constraint_violation"
	default:
		return "unexpected"
	}
}

// SkipClasses is the set of class names that the admin /skip-count
// endpoint always emits — keeps the JSON shape stable for the TUI even
// before any skips happen.
var SkipClasses = []string{
	ClassDecode.String(),
	ClassUnknownTopic.String(),
	ClassConstraint.String(),
}

// Classify maps a wrapped error to its consumer-loop branch. Decode /
// unknown-topic / constraint are permanent (skip + log + counter);
// infra is retried; ClassUnexpected fails fast (the loop log.Fatalfs).
func Classify(err error) ErrorClass {
	switch {
	case err == nil:
		return ClassNone
	case errors.Is(err, ErrDecode):
		return ClassDecode
	case errors.Is(err, ErrUnknownTopic):
		return ClassUnknownTopic
	case errors.Is(err, ErrInfraUnavailable):
		return ClassInfra
	case errors.Is(err, ErrConstraintViolation):
		return ClassConstraint
	default:
		return ClassUnexpected
	}
}

// wrapDBError tags a pgx/pgconn error with the right sentinel. Used by
// the Apply* methods so callers branch with errors.Is. Returns the
// original error unchanged if it doesn't match a known shape.
func wrapDBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// Caller-side cancellation isn't really an infra error, but the
		// loop's retry behaviour is still right: bail and let ctx.Err()
		// propagate. Tagging it as infra preserves uniform handling.
		return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// SQLSTATE class 23 = integrity constraint violation
		// (NOT NULL, FK, UNIQUE, CHECK). Any of those at the consumer
		// means producer validation drifted (ADR 0005).
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "23" {
			return fmt.Errorf("%w: %w", ErrConstraintViolation, err)
		}
		// Other PgError shapes (admin shutdown 57P01, etc.) are
		// treated as infra so the loop retries instead of skipping.
		return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
	}

	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
	}

	// Network-level pgx errors (broken connection, dial failure) don't
	// expose a typed handle in pgx/v5 — fall back to ErrInfraUnavailable
	// for anything else so we err on the side of "retry, don't skip".
	return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
}
