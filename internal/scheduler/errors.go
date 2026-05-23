package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// Consumer-side error sentinels. Same shape as internal/store/errors.go
// per ADR 0006 — the scheduler's failure mode is less severe (missed
// reminder, not missed row) but it runs the same classify-and-branch
// loop so the TUI status panel can treat both endpoints identically.
var (
	ErrDecode              = errors.New("scheduler: decode failure")
	ErrUnknownTopic        = errors.New("scheduler: unknown topic")
	ErrInfraUnavailable    = errors.New("scheduler: infra unavailable")
	ErrConstraintViolation = errors.New("scheduler: constraint violation")
)

type ErrorClass int

const (
	ClassNone ErrorClass = iota
	ClassDecode
	ClassUnknownTopic
	ClassInfra
	ClassConstraint
	ClassUnexpected
)

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

// SkipClasses is the set of class names always present in /skip-count.
var SkipClasses = []string{
	ClassDecode.String(),
	ClassUnknownTopic.String(),
	ClassConstraint.String(),
}

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

// wrapDBError tags a pgx/pgconn error with the right sentinel.
func wrapDBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "23" {
			return fmt.Errorf("%w: %w", ErrConstraintViolation, err)
		}
		return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
	}

	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
	}

	return fmt.Errorf("%w: %w", ErrInfraUnavailable, err)
}
