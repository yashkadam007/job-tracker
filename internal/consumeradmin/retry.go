package consumeradmin

import (
	"context"
	"log"
	"time"
)

// infraBackoff is the ADR 0006 schedule for ErrInfraUnavailable:
// 100ms, 250ms, 500ms, 1s, 2s, 5s, 10s — then 10s indefinitely.
// No upper bound on attempts. A multi-hour Postgres outage keeps the
// consumer alive and resumes cleanly when the service returns.
var infraBackoff = []time.Duration{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

// RetryInfra calls fn repeatedly until it returns nil, ctx is cancelled,
// or shouldRetry(err) returns false. Logs each retry with the attempt
// count and elapsed delay so a stuck consumer is visible in tail -f.
//
// shouldRetry decides whether a returned error is still in the infra
// class — for anything else the caller wants to break out and let the
// classification switch handle it.
func RetryInfra(ctx context.Context, label string, shouldRetry func(error) bool, fn func() error) error {
	attempt := 0
	for {
		err := fn()
		if err == nil {
			if attempt > 0 {
				log.Printf("%s: recovered after %d retries", label, attempt)
			}
			return nil
		}
		if !shouldRetry(err) {
			return err
		}
		delay := infraBackoff[len(infraBackoff)-1]
		if attempt < len(infraBackoff) {
			delay = infraBackoff[attempt]
		}
		attempt++
		log.Printf("%s: infra error (attempt %d, retry in %s): %v", label, attempt, delay, err)

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}
