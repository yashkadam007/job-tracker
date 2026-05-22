// store — Kafka consumer that writes job events to Postgres.
//
// Joins consumer group "store" and subscribes to job.submitted and
// job.status.changed. A separate consumer group ("scheduler") reads
// the same topics independently for reminder bookkeeping.
//
// Idempotency: each Postgres write happens in the same transaction
// as an INSERT into processed_events. Duplicate deliveries become
// no-ops, so manual offset commit + at-least-once delivery is safe.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/db"
	"job-tracker/internal/events"
	"job-tracker/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, dsn())
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()
	st := store.New(pool)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
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

	log.Printf("store: consuming %s, %s, %s, %s (group=%s)",
		events.TopicJobSubmitted, events.TopicJobStatusChanged,
		events.TopicJobNoteAdded, events.TopicJobInterviewRecorded, store.Consumer)

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
			if err := handle(ctx, st, r); err != nil {
				log.Printf("handle error topic=%s offset=%d: %v", r.Topic, r.Offset, err)
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

func handle(ctx context.Context, st *store.Store, r *kgo.Record) error {
	switch r.Topic {
	case events.TopicJobSubmitted:
		var ev events.JobSubmitted
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return err
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
			return err
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
			return err
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
			return err
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
		return errors.New("unknown topic: " + r.Topic)
	}
	return nil
}

func brokers() []string {
	b := os.Getenv("KAFKA_BOOTSTRAP")
	if b == "" {
		b = "localhost:9092"
	}
	return strings.Split(b, ",")
}

func dsn() string {
	d := os.Getenv("DATABASE_URL")
	if d == "" {
		d = "postgres://jobtracker:jobtracker@localhost:5432/jobtracker?sslmode=disable"
	}
	return d
}
