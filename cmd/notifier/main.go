// notifier — consumes job.reminder and "delivers" each reminder.
//
// v1: writes to stdout and (if configured) sends a Telegram message
// with inline-keyboard buttons (Applied / Rejected / Snooze 1d). The
// bot service handles the button callbacks; notifier stays one-way.
//
// Dedup (ADR 0007): each event_id is claimed in processed_events
// (consumer="notifier") before delivery. The Scheduler's fireDue is
// publish-then-mark with no atomicity, so a Scheduler crash can
// republish the same reminder; the claim turns the duplicate into a
// no-op.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/db"
	"job-tracker/internal/events"
	"job-tracker/internal/telegram"
)

const consumerGroup = "notifier"

var (
	tg     *telegram.Client
	chatID string
	pool   *pgxpool.Pool
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if token := os.Getenv("TELEGRAM_BOT_TOKEN"); token != "" {
		tg = telegram.New(token)
		chatID = os.Getenv("TELEGRAM_CHAT_ID")
	}

	var err error
	pool, err = db.Connect(ctx, dsn())
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(events.TopicJobReminder),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Auto-commit is fine: dedup lives in processed_events, not in
		// the Kafka offset. A duplicate redelivery hits the claim's
		// ON CONFLICT and is skipped.
	)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer cl.Close()

	log.Printf("notifier: consuming %s (group=%s)", events.TopicJobReminder, consumerGroup)

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
			var ev events.JobReminder
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				log.Printf("decode error offset=%d: %v", r.Offset, err)
				return
			}
			if ev.EventID == "" {
				log.Printf("skip: empty event_id offset=%d job_id=%s", r.Offset, ev.JobID)
				return
			}
			claimed, err := claim(ctx, ev.EventID)
			if err != nil {
				log.Printf("claim error offset=%d event_id=%s: %v", r.Offset, ev.EventID, err)
				return
			}
			if !claimed {
				log.Printf("skip duplicate offset=%d event_id=%s", r.Offset, ev.EventID)
				return
			}
			deliver(ctx, ev)
		})
	}
}

// claim records (consumer, event_id) in processed_events. Returns
// true if this is the first time we've seen the event, false on
// duplicate. Mirrors internal/bot.claimUpdate — no transaction is
// needed because the Notifier has no business write to bundle with.
func claim(ctx context.Context, eventID string) (bool, error) {
	ct, err := pool.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumerGroup, eventID)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() > 0, nil
}

func deliver(ctx context.Context, ev events.JobReminder) {
	var prompt string
	switch ev.Kind {
	case events.ReminderFollowupSaved:
		prompt = "Still interested? Apply or drop it."
	case events.ReminderFollowupApplied:
		prompt = "Time to follow up — any response yet?"
	default:
		prompt = "Reminder."
	}
	log.Printf("REMINDER  %s — %s @ %s (%s, due %s) :: %s",
		ev.Kind, ev.Title, ev.Company, ev.Status, ev.DueAt.Format(time.RFC3339), prompt)

	if tg == nil || chatID == "" {
		return
	}
	msg := fmt.Sprintf("🔔 %s — %s @ %s\nStatus: %s\nDue: %s\n%s",
		ev.Kind, ev.Title, ev.Company, ev.Status, ev.DueAt.Format(time.RFC3339), prompt)

	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := tg.SendMessage(sendCtx, chatID, msg, telegram.SendMessageOptions{
		ReplyMarkup: telegram.ReminderKeyboard(ev.JobID),
	}); err != nil {
		log.Printf("telegram: %v", err)
	}
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
