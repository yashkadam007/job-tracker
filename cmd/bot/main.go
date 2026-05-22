// bot — Telegram two-way interface for capture (/add) and status
// updates (inline buttons, /applied <n>, …). Companion to notifier:
// notifier is one-way out, bot is the inbound half. See ADR 0003.
//
// Wire-up only; the long-poll loop, command grammar, callback
// handling, and /add URL fetch live in internal/bot.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"job-tracker/internal/bot"
	"job-tracker/internal/db"
	"job-tracker/internal/jobclient"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, dsn())
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	pub, err := jobclient.NewPublisher(brokers())
	if err != nil {
		log.Fatalf("publisher: %v", err)
	}
	defer func() {
		if err := pub.Close(); err != nil {
			log.Printf("publisher close: %v", err)
		}
	}()
	reader := jobclient.NewReader(pool)

	b, err := bot.New(bot.Config{
		Token:     os.Getenv("TELEGRAM_BOT_TOKEN"),
		ChatID:    os.Getenv("TELEGRAM_CHAT_ID"),
		Publisher: pub,
		Reader:    reader,
		Pool:      pool,
	})
	if err != nil {
		log.Fatalf("bot: %v", err)
	}

	if err := b.Run(ctx); err != nil {
		log.Fatalf("bot run: %v", err)
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
