// notifier — consumes job.reminder and "delivers" each reminder.
//
// v1: writes to stdout. Future scope: Telegram, email.
//
// Notifier doesn't touch Postgres; it relies on the Scheduler's
// deterministic event IDs ("reminder-<id>") and Kafka's offset
// tracking to avoid double-delivery on restart.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/events"
)

const consumerGroup = "notifier"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(events.TopicJobReminder),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Auto-commit is fine here: the side effect (printing or, later,
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
			deliver(ev)
		})
	}
}

func deliver(ev events.JobReminder) {
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

	msg := fmt.Sprintf("🔔 %s — %s @ %s\nStatus: %s\nDue: %s\n%s",
		ev.Kind, ev.Title, ev.Company, ev.Status, ev.DueAt.Format(time.RFC3339), prompt)
	if err := sendTelegram(msg); err != nil {
		log.Printf("telegram: %v", err)
	}
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

func sendTelegram(text string) error {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	if token == "" || chatID == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]string{"chat_id": chatID, "text": text})
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
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
