package jobclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"job-tracker/internal/events"
)

// Publisher wraps a long-lived kgo client. One per frontend process —
// construct in main(), defer Close(). Owns topic names, partition key
// (job_id, so two events for the same job land on the same partition
// and Kafka preserves their order), ack policy (AllISRAcks), and JSON
// encoding.
type Publisher struct {
	cl *kgo.Client
}

// NewPublisher dials brokers and returns a ready Publisher. ProducerLinger(0)
// matches the original CLI behaviour — responsiveness over batching, since
// interactive frontends publish one record at a time.
func NewPublisher(brokers []string) (*Publisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, fmt.Errorf("jobclient: kafka client: %w", err)
	}
	return &Publisher{cl: cl}, nil
}

// Close performs a graceful shutdown: flush any in-flight produces
// under a bounded timeout before releasing the underlying kgo client.
// Safe to call once per Publisher; subsequent calls are no-ops.
func (p *Publisher) Close() error {
	if p.cl == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Flush returns when every buffered record has been acked, errored,
	// or the context expires. Either way we then release the client.
	flushErr := p.cl.Flush(ctx)
	p.cl.Close()
	p.cl = nil
	return flushErr
}

// Submit publishes a JobSubmitted event. The event is validated first;
// on validation failure no Kafka produce is attempted (ADR 0005).
func (p *Publisher) Submit(ctx context.Context, ev events.JobSubmitted) error {
	if err := validateSubmitted(ev); err != nil {
		return err
	}
	return p.produce(ctx, events.TopicJobSubmitted, ev.JobID, ev)
}

// ChangeStatus publishes a JobStatusChanged event.
func (p *Publisher) ChangeStatus(ctx context.Context, ev events.JobStatusChanged) error {
	if err := validateStatusChanged(ev); err != nil {
		return err
	}
	return p.produce(ctx, events.TopicJobStatusChanged, ev.JobID, ev)
}

// AddNote publishes a JobNoteAdded event.
func (p *Publisher) AddNote(ctx context.Context, ev events.JobNoteAdded) error {
	if err := validateNoteAdded(ev); err != nil {
		return err
	}
	return p.produce(ctx, events.TopicJobNoteAdded, ev.JobID, ev)
}

// RecordInterview publishes a JobInterviewRecorded event. The same
// topic carries both "schedule" and "complete/update" shapes — the
// Store upserts on interview_id with a COALESCE pattern.
func (p *Publisher) RecordInterview(ctx context.Context, ev events.JobInterviewRecorded) error {
	if err := validateInterviewRecorded(ev); err != nil {
		return err
	}
	return p.produce(ctx, events.TopicJobInterviewRecorded, ev.JobID, ev)
}

func (p *Publisher) produce(ctx context.Context, topic, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("jobclient: marshal %s: %w", topic, err)
	}
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
	}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("jobclient: publish %s: %w", topic, err)
	}
	return nil
}
