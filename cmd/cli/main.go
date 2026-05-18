// jobs — small CLI that publishes events to Kafka. This is the first
// "producer" in the system. It does no DB work; the Store consumer
// listens on the same topics and is responsible for persistence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/events"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "add":
		cmdAdd(os.Args[2:])
	case "status":
		cmdStatus(os.Args[2:])
	case "ensure-topics":
		cmdEnsureTopics()
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  jobs ensure-topics")
	fmt.Fprintln(os.Stderr, "  jobs add --url <u> --title <t> --company <c> [--status saved]")
	fmt.Fprintln(os.Stderr, "  jobs status <url> <new-status>")
}

// brokers reads KAFKA_BOOTSTRAP (comma-separated) or defaults to the
// host-side listener exposed by compose.yml.
func brokers() []string {
	b := os.Getenv("KAFKA_BOOTSTRAP")
	if b == "" {
		b = "localhost:9092"
	}
	return strings.Split(b, ",")
}

// cmdEnsureTopics creates the topics if they don't already exist. Run
// this once after starting the cluster. Kafka *can* auto-create topics
// on first publish, but explicit creation lets us set partition and
// replication factor up front.
func cmdEnsureTopics() {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	topics := []string{
		events.TopicJobSubmitted,
		events.TopicJobStatusChanged,
		events.TopicJobReminder,
	}

	// 1 partition, 1 replica — fine for a single-broker dev cluster.
	// Increase partitions later if you want parallel consumers in the
	// same consumer group.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := adm.CreateTopics(ctx, 1, 1, nil, topics...)
	if err != nil {
		log.Fatalf("create topics: %v", err)
	}
	for _, r := range resp.Sorted() {
		switch {
		case r.Err == nil:
			fmt.Printf("created: %s\n", r.Topic)
		case errors.Is(r.Err, kerr.TopicAlreadyExists):
			fmt.Printf("exists:  %s\n", r.Topic)
		default:
			fmt.Printf("error:   %s — %v\n", r.Topic, r.Err)
		}
	}
}

func cmdAdd(args []string) {
	fs := flag.NewFlagSet("add", flag.ExitOnError)
	url := fs.String("url", "", "job posting URL (required)")
	title := fs.String("title", "", "job title (required)")
	company := fs.String("company", "", "company name (required)")
	status := fs.String("status", string(events.StatusSaved), "initial status")
	_ = fs.Parse(args)

	if *url == "" || *title == "" || *company == "" {
		fs.Usage()
		os.Exit(2)
	}

	ev := events.JobSubmitted{
		EventID:     uuid.NewString(),
		URL:         *url,
		Title:       *title,
		Company:     *company,
		Status:      events.JobStatus(*status),
		SubmittedAt: time.Now().UTC(),
	}
	// Partition key = URL. Two events for the same URL land on the same
	// partition, so Kafka preserves their order — Store will see
	// "submitted" before any "status changed".
	publish(events.TopicJobSubmitted, ev.URL, ev)
	fmt.Printf("published: %s @ %s (%s)\n", ev.Title, ev.Company, ev.Status)
}

func cmdStatus(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: jobs status <url> <new-status>")
		os.Exit(2)
	}
	ev := events.JobStatusChanged{
		EventID:   uuid.NewString(),
		URL:       args[0],
		Status:    events.JobStatus(args[1]),
		ChangedAt: time.Now().UTC(),
	}
	publish(events.TopicJobStatusChanged, ev.URL, ev)
	fmt.Printf("status changed: %s → %s\n", ev.URL, ev.Status)
}

// publish serializes v to JSON and sends a single record synchronously.
// RequiredAcks=AllISRAcks means the broker waits until every in-sync
// replica has the record before acking — the strongest durability the
// cluster can offer.
func publish(topic, key string, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
	}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		log.Fatalf("publish: %v", err)
	}
}
