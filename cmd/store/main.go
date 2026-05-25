// store — Kafka consumer that writes job events to Postgres.
//
// Joins consumer group "store" and subscribes to job.submitted and
// job.status.changed. A separate consumer group ("scheduler") reads
// the same topics independently for reminder bookkeeping.
//
// Idempotency: each Postgres write happens in the same transaction
// as an INSERT into processed_events. Duplicate deliveries become
// no-ops, so manual offset commit + at-least-once delivery is safe.
//
// Error handling (ADR 0006): the per-record handle() is wrapped in a
// classify-and-branch switch. Infra errors retry indefinitely with
// capped backoff; permanent errors skip + structured-log + counter;
// anything unclassified fails fast (log.Fatalf).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/config"
	"job-tracker/internal/consumeradmin"
	"job-tracker/internal/db"
	"job-tracker/internal/events"
	"job-tracker/internal/store"
)

const adminAddr = "0.0.0.0:9090"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, config.DSN(""))
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()
	st := store.New(pool)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers("")...),
		kgo.ConsumerGroup(store.Consumer),
		kgo.ConsumeTopics(
			events.TopicJobSubmitted,
			events.TopicJobStatusChanged,
			events.TopicJobNoteAdded,
			events.TopicJobInterviewRecorded,
		),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer cl.Close()

	counter := consumeradmin.NewSkipCounter()
	admin := consumeradmin.NewServer(adminAddr, counter, store.SkipClasses)
	go func() {
		if err := admin.Run(ctx); err != nil {
			log.Printf("store: admin server exited: %v", err)
		}
	}()

	log.Printf("store: consuming %s, %s, %s, %s (group=%s) admin=%s",
		events.TopicJobSubmitted, events.TopicJobStatusChanged,
		events.TopicJobNoteAdded, events.TopicJobInterviewRecorded,
		store.Consumer, adminAddr)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch error topic=%s partition=%d: %v", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if err := processRecord(ctx, st, counter, r); err != nil {
				// processRecord only returns non-nil on context
				// cancellation during an infra retry — the next
				// PollFetches loop will see ctx.Err() and return.
				return
			}
			cl.MarkCommitRecords(r)
		})

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := cl.CommitMarkedOffsets(commitCtx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
		cancel()
	}
}

// processRecord applies the ADR 0006 classification policy to one
// record. Returns nil when the record has been resolved (success,
// permanent-skip, or crash) and the caller should mark it for commit.
// Returns ctx.Err() when an infra retry is interrupted by shutdown —
// the caller does not commit and the next session re-delivers the
// record.
func processRecord(ctx context.Context, st *store.Store, counter *consumeradmin.SkipCounter, r *kgo.Record) error {
	label := fmt.Sprintf("store handle topic=%s offset=%d", r.Topic, r.Offset)

	err := consumeradmin.RetryInfra(ctx, label,
		func(e error) bool { return store.Classify(e) == store.ClassInfra },
		func() error { return safeHandle(ctx, st, r) },
	)

	// Shutdown mid-retry: RetryInfra returns ctx.Err() raw, so don't
	// classify it as "unexpected" and crash. The outer loop will see
	// ctx.Err() on the next PollFetches and exit.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	switch class := store.Classify(err); class {
	case store.ClassNone:
		return nil
	case store.ClassDecode, store.ClassUnknownTopic, store.ClassConstraint:
		consumeradmin.LogSkip(r.Topic, r.Partition, r.Offset, class.String(), err, r.Value)
		counter.Inc(class.String())
		return nil
	case store.ClassInfra:
		// Shouldn't happen: RetryInfra only returns infra errors via
		// ctx.Err() (handled above). Treat as a no-op safely.
		return err
	default:
		// ClassUnexpected — fail fast per ADR 0006 default branch. The
		// container restart-counts the operator's signal that the
		// classification missed a case.
		consumeradmin.LogSkip(r.Topic, r.Partition, r.Offset, class.String(), err, r.Value)
		log.Fatalf("store: unclassified error, crashing: %v", err)
		return err
	}
}

// safeHandle wraps handle() with defer/recover so a panic in a domain
// method becomes a structured-log + crash (ADR 0006 Notes "Panic
// recovery in the consumer loop") instead of an unstructured stack.
func safeHandle(ctx context.Context, st *store.Store, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			panicErr := fmt.Errorf("store: panic: %v\n%s", p, debug.Stack())
			consumeradmin.LogSkip(r.Topic, r.Partition, r.Offset, "panic", panicErr, r.Value)
			log.Fatalf("store: recovered panic, crashing: %v", p)
		}
	}()
	return handle(ctx, st, r)
}

func handle(ctx context.Context, st *store.Store, r *kgo.Record) error {
	switch r.Topic {
	case events.TopicJobSubmitted:
		var ev events.JobSubmitted
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("%w: %w", store.ErrDecode, err)
		}
		applied, err := st.ApplySubmitted(ctx, ev)
		if err != nil {
			return err
		}
		if applied {
			log.Printf("submitted: %s @ %s (%s)", ev.Title, ev.Company, ev.Status)
		} else {
			log.Printf("submitted: dup event_id=%s skipped", ev.EventID)
		}
	case events.TopicJobStatusChanged:
		var ev events.JobStatusChanged
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("%w: %w", store.ErrDecode, err)
		}
		applied, missing, err := st.ApplyStatusChanged(ctx, ev)
		if err != nil {
			return err
		}
		switch {
		case !applied:
			log.Printf("status: dup event_id=%s skipped", ev.EventID)
		case missing:
			log.Printf("status: no job for job_id=%s (status event before submit?)", ev.JobID)
		default:
			log.Printf("status: %s → %s", ev.JobID, ev.Status)
		}
	case events.TopicJobNoteAdded:
		var ev events.JobNoteAdded
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("%w: %w", store.ErrDecode, err)
		}
		applied, missing, err := st.ApplyNoteAdded(ctx, ev)
		if err != nil {
			return err
		}
		switch {
		case !applied:
			log.Printf("note: dup event_id=%s skipped", ev.EventID)
		case missing:
			log.Printf("note: no job for job_id=%s (note before submit?)", ev.JobID)
		default:
			log.Printf("note: added for %s", ev.JobID)
		}
	case events.TopicJobInterviewRecorded:
		var ev events.JobInterviewRecorded
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("%w: %w", store.ErrDecode, err)
		}
		applied, missing, err := st.ApplyInterviewRecorded(ctx, ev)
		if err != nil {
			return err
		}
		switch {
		case !applied:
			log.Printf("interview: dup event_id=%s skipped", ev.EventID)
		case missing:
			log.Printf("interview: no job for job_id=%s (interview before submit?)", ev.JobID)
		default:
			log.Printf("interview: %s job_id=%s", ev.InterviewID, ev.JobID)
		}
	default:
		return fmt.Errorf("%w: %s", store.ErrUnknownTopic, r.Topic)
	}
	return nil
}

