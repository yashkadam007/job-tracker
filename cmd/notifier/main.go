// notifier — consumes job.reminder and "delivers" each reminder.
//
// v1: writes to stdout and (if configured) sends a Telegram message
// with inline-keyboard buttons (Applied / Rejected / Snooze 1d). The
// bot service handles the button callbacks; notifier stays one-way.
//
// Notifier doesn't touch Postgres; it relies on the Scheduler's
// deterministic event IDs ("reminder-<id>") and Kafka's offset
// tracking to avoid double-delivery on restart.
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

	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/events"
	"job-tracker/internal/telegram"
)

const consumerGroup = "notifier"

var (
	tg     *telegram.Client
	chatID string
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if token := os.Getenv("TELEGRAM_BOT_TOKEN"); token != "" {
		tg = telegram.New(token)
		chatID = os.Getenv("TELEGRAM_CHAT_ID")
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(events.TopicJobReminder),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Auto-commit is fine here: the side effect (printing or
		// sending Telegram) is naturally idempotent enough for v1 — if
		// we crash after delivery but before commit, the worst case is
		// one duplicate notification when the consumer restarts.
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
			deliver(ctx, ev)
		})
	}
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
