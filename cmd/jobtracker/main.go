// jobtracker — desktop TUI for session-based job triage. Reads the
// catalog directly from the home-server's Postgres and publishes
// status-changes / new-submits as Kafka events through the shared
// `internal/jobclient` library. No daemon, no API tier, no local
// cache — every launch reads fresh state. See ADR 0004.
//
// Config (env, namespaced because it lives in the user's shell rc):
//
//   JOB_TRACKER_DATABASE_URL    postgres DSN reachable over Tailscale/LAN
//   JOB_TRACKER_KAFKA_BOOTSTRAP comma-separated bootstrap brokers
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"

	"job-tracker/internal/db"
	"job-tracker/internal/jobclient"
	"job-tracker/internal/tui"
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

	m := tui.New(tui.Config{
		Publisher:      pub,
		Reader:         reader,
		Pool:           pool,
		AdminEndpoints: adminEndpoints(),
	})

	prog := tea.NewProgram(m, tea.WithAltScreen(), tea.WithContext(ctx))
	if _, err := prog.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "jobtracker: %v\n", err)
		os.Exit(1)
	}
}

// brokers reads JOB_TRACKER_KAFKA_BOOTSTRAP (comma-separated). Namespaced
// rather than KAFKA_BOOTSTRAP because the TUI's config lives in the
// user's ~/.zshrc — a global namespace shared with other tools.
func brokers() []string {
	b := os.Getenv("JOB_TRACKER_KAFKA_BOOTSTRAP")
	if b == "" {
		b = "localhost:9092"
	}
	return strings.Split(b, ",")
}

func dsn() string {
	d := os.Getenv("JOB_TRACKER_DATABASE_URL")
	if d == "" {
		d = "postgres://jobtracker:jobtracker@localhost:5432/jobtracker?sslmode=disable"
	}
	return d
}

// adminEndpoints surfaces the consumer-side /skip-count endpoints
// introduced in ADR 0006. Defaults to the localhost ports exposed by
// compose; override with JOB_TRACKER_ADMIN_HOST when the consumers
// live on the home server reached over Tailscale.
//
// Format: host:port[,host:port,…] paired with consumer names in
// fixed order (store, scheduler). For the simple single-host case,
// just set JOB_TRACKER_ADMIN_HOST to the tailnet hostname.
func adminEndpoints() []tui.AdminEndpoint {
	host := os.Getenv("JOB_TRACKER_ADMIN_HOST")
	if host == "" {
		host = "localhost"
	}
	return []tui.AdminEndpoint{
		{Name: "store", URL: fmt.Sprintf("http://%s:9090/skip-count", host)},
		{Name: "scheduler", URL: fmt.Sprintf("http://%s:9091/skip-count", host)},
	}
}
