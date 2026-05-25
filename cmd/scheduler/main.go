// scheduler — Reminder Scheduler.
//
// Two responsibilities:
//
//  1. Consume job.submitted and job.status.changed (consumer group
//     "scheduler", independent of Store) and insert future-dated
//     rows in the `reminders` table.
//
//  2. On a ticker, scan `reminders` for rows whose due_at has
//     arrived, publish a JobReminder event for each to the
//     job.reminder topic, then mark them fired.
//
// Note that two services (Store and Scheduler) are reading the same
// Kafka topics. Because they're in *different* consumer groups, each
// gets the full stream — that's how Kafka does fan-out.
//
// Error handling (ADR 0006): same shape as cmd/store — classify each
// per-record error, retry infra indefinitely, structured-log + count
// permanent skips, fail fast on the unclassified default.
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/consumeradmin"
	"job-tracker/internal/db"
	"job-tracker/internal/events"
	"job-tracker/internal/scheduler"
)

const adminAddr = "0.0.0.0:9091"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, dsn())
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	tzName := envString("REMINDER_TZ", "Asia/Kolkata")
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		log.Fatalf("REMINDER_TZ=%q: %v", tzName, err)
	}
	snapHour := envInt("REMINDER_HOUR", 9)
	sch := scheduler.New(pool, scheduler.Config{
		SavedFollowup:   envDuration("REMINDER_SAVED", 7*24*time.Hour),
		AppliedFollowup: envDuration("REMINDER_APPLIED", 14*24*time.Hour),
		SnapHour:        snapHour,
		Location:        loc,
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(scheduler.Consumer),
		kgo.ConsumeTopics(events.TopicJobSubmitted, events.TopicJobStatusChanged),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer cl.Close()

	// A second, separate client for producing JobReminder events.
	// Mixing consumer + producer on one client is fine but keeping
	// them separate makes lifecycles easier to reason about.
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(10*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer prod.Close()

	counter := consumeradmin.NewSkipCounter()
	admin := consumeradmin.NewServer(adminAddr, counter, scheduler.SkipClasses)
	go func() {
		if err := admin.Run(ctx); err != nil {
			log.Printf("scheduler: admin server exited: %v", err)
		}
	}()

	pollInterval := envDuration("REMINDER_POLL", 30*time.Second)
	log.Printf("scheduler: group=%s, saved=%s, applied=%s, poll=%s, snap=%dh %s, admin=%s",
		scheduler.Consumer, envDuration("REMINDER_SAVED", 7*24*time.Hour),
		envDuration("REMINDER_APPLIED", 14*24*time.Hour), pollInterval,
		snapHour, loc, adminAddr)

	// Tick loop: independent goroutine that fires due reminders.
	go runTicker(ctx, sch, prod, pollInterval)

	// Consumer loop: same shape as Store.
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
			if err := processRecord(ctx, sch, counter, r); err != nil {
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

func processRecord(ctx context.Context, sch *scheduler.Scheduler, counter *consumeradmin.SkipCounter, r *kgo.Record) error {
	label := fmt.Sprintf("scheduler handle topic=%s offset=%d", r.Topic, r.Offset)

	err := consumeradmin.RetryInfra(ctx, label,
		func(e error) bool { return scheduler.Classify(e) == scheduler.ClassInfra },
		func() error { return safeHandle(ctx, sch, r) },
	)

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	switch class := scheduler.Classify(err); class {
	case scheduler.ClassNone:
		return nil
	case scheduler.ClassDecode, scheduler.ClassUnknownTopic, scheduler.ClassConstraint:
		consumeradmin.LogSkip(r.Topic, r.Partition, r.Offset, class.String(), err, r.Value)
		counter.Inc(class.String())
		return nil
	case scheduler.ClassInfra:
		return err
	default:
		consumeradmin.LogSkip(r.Topic, r.Partition, r.Offset, class.String(), err, r.Value)
		log.Fatalf("scheduler: unclassified error, crashing: %v", err)
		return err
	}
}

func safeHandle(ctx context.Context, sch *scheduler.Scheduler, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			panicErr := fmt.Errorf("scheduler: panic: %v\n%s", p, debug.Stack())
			consumeradmin.LogSkip(r.Topic, r.Partition, r.Offset, "panic", panicErr, r.Value)
			log.Fatalf("scheduler: recovered panic, crashing: %v", p)
		}
	}()
	return handle(ctx, sch, r)
}

func handle(ctx context.Context, sch *scheduler.Scheduler, r *kgo.Record) error {
	switch r.Topic {
	case events.TopicJobSubmitted:
		var ev events.JobSubmitted
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("%w: %w", scheduler.ErrDecode, err)
		}
		applied, err := sch.HandleSubmitted(ctx, ev)
		if err != nil {
			return err
		}
		if applied {
			log.Printf("scheduled reminder for %s (%s)", ev.JobID, ev.Status)
		}
	case events.TopicJobStatusChanged:
		var ev events.JobStatusChanged
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("%w: %w", scheduler.ErrDecode, err)
		}
		if _, err := sch.HandleStatusChanged(ctx, ev); err != nil {
			return err
		}
		log.Printf("reminders updated for %s (%s)", ev.JobID, ev.Status)
	default:
		return fmt.Errorf("%w: %s", scheduler.ErrUnknownTopic, r.Topic)
	}
	return nil
}

func runTicker(ctx context.Context, sch *scheduler.Scheduler, prod *kgo.Client, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := fireDue(ctx, sch, prod); err != nil {
				log.Printf("fire due: %v", err)
			}
		}
	}
}

// fireDue runs one pass: fetch due reminders, publish a JobReminder
// for each, then mark fired. Publish-before-mark is deliberate — if
// we crash in between, the next pass republishes the same event_id
// ("reminder-<id>"), and the Notifier's claim against processed_events
// (consumer="notifier", see ADR 0007) turns the duplicate into a
// no-op.
func fireDue(ctx context.Context, sch *scheduler.Scheduler, prod *kgo.Client) error {
	due, err := sch.FetchDue(ctx, time.Now().UTC(), 100)
	if err != nil {
		return err
	}
	for _, d := range due {
		now := time.Now().UTC()
		ev := events.JobReminder{
			EventID: fmt.Sprintf("reminder-%d", d.ID),
			JobID:   d.JobID,
			URL:     d.URL,
			Kind:    d.Kind,
			DueAt:   d.DueAt,
			Title:   d.Title,
			Company: d.Company,
			Status:  d.Status,
			FiredAt: now,
		}
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		rec := &kgo.Record{
			Topic: events.TopicJobReminder,
			Key:   []byte(d.JobID),
			Value: body,
		}
		publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = prod.ProduceSync(publishCtx, rec).FirstErr()
		cancel()
		if err != nil {
			return fmt.Errorf("produce reminder id=%d: %w", d.ID, err)
		}
		if err := sch.MarkFired(ctx, d.ID, now); err != nil {
			return fmt.Errorf("mark fired id=%d: %w", d.ID, err)
		}
		log.Printf("fired reminder id=%d job_id=%s url=%s kind=%s", d.ID, d.JobID, d.URL, d.Kind)
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

// envDuration reads <key>_SECONDS as integer seconds, falling back to
// the default. Keeps env config readable without pulling in a parser.
func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key + "_SECONDS")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
