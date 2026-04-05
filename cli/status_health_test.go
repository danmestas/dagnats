// cli/status_health_test.go
// Integration tests for the --detail collectors: queue health, DLQ summary,
// and engine lag. Methodology: each test starts an embedded NATS server with
// all streams, exercises the collector, and asserts on returned values.
package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCollectQueueHealth(t *testing.T) {
	nc := dagnatstest.Server(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Create a consumer on TASK_QUEUES to simulate a worker.
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	_, err = stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject: "task.test-task.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
		},
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	result := collectQueueHealth(ctx, js)

	// Positive: must find the test-task consumer.
	found := false
	for _, q := range result {
		if q.Task == "test-task" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected test-task in queue health results")
	}

	// Negative: no task name should be empty.
	for _, q := range result {
		if q.Task == "" {
			t.Fatal("task name must not be empty")
		}
	}
}

func TestCollectDLQSummary(t *testing.T) {
	nc := dagnatstest.Server(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	// Publish a dead letter message.
	_, err = js.Publish(
		ctx, "dead.test-task.abc123", []byte("failed"),
	)
	if err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

	result := collectDLQSummary(ctx, js)

	// Positive: total must be at least 1.
	if result.Total < 1 {
		t.Fatalf("expected total >= 1, got %d", result.Total)
	}

	// Positive: per-task map must contain test-task entry.
	if len(result.ByTask) == 0 {
		t.Fatal("expected non-empty by-task map")
	}

	// Negative: oldest must not be nil since we published.
	if result.Oldest == nil {
		t.Fatal("expected oldest timestamp to be set")
	}
}

func TestCollectEngineLag(t *testing.T) {
	nc := dagnatstest.Server(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	result := collectEngineLag(ctx, js)

	// Positive: lag values must be >= 0 (uint64 guarantees this
	// but we verify the struct is populated).
	if result.HistoryLagSeconds < 0 {
		t.Fatal("history lag seconds must be >= 0")
	}

	// Negative: scheduled timers should be 0 with no published
	// timer messages.
	if result.ScheduledTimers != 0 {
		t.Fatalf(
			"expected 0 scheduled timers, got %d",
			result.ScheduledTimers,
		)
	}
}

func TestPrintQueueHealth(t *testing.T) {
	queues := []queueHealth{
		{
			Task:        "greet",
			Pending:     5,
			InFlight:    2,
			Redelivered: 1,
			AckWaitMS:   30000,
		},
	}

	output := captureOutput(func() {
		printQueueHealth(queues)
	})

	// Positive: output must contain task name.
	if !strings.Contains(output, "greet") {
		t.Fatalf("expected 'greet' in output, got: %s", output)
	}

	// Negative: must not contain error text.
	if strings.Contains(output, "error") {
		t.Fatalf("unexpected error in output: %s", output)
	}
}

func TestPrintDLQSummary(t *testing.T) {
	now := time.Now().UTC()
	summary := dlqSummary{
		Total:  3,
		Oldest: &now,
		Newest: &now,
		ByTask: map[string]uint64{"test-task": 3},
	}

	output := captureOutput(func() {
		printDLQSummary(summary)
	})

	// Positive: output must contain total count.
	if !strings.Contains(output, "3") {
		t.Fatalf("expected '3' in output, got: %s", output)
	}

	// Negative: empty summary should say "none".
	emptyOutput := captureOutput(func() {
		printDLQSummary(dlqSummary{
			ByTask: map[string]uint64{},
		})
	})
	if !strings.Contains(emptyOutput, "none") {
		t.Fatalf(
			"expected 'none' for empty DLQ, got: %s", emptyOutput,
		)
	}
}

func TestPrintEngineLag(t *testing.T) {
	lag := engineLag{
		HistoryLagMessages: 10,
		HistoryLagSeconds:  1.5,
		ScheduledTimers:    2,
	}

	output := captureOutput(func() {
		printEngineLag(lag)
	})

	// Positive: output must contain lag info.
	if !strings.Contains(output, "10") {
		t.Fatalf("expected '10' in output, got: %s", output)
	}

	// Negative: must not contain error text.
	if strings.Contains(output, "error") {
		t.Fatalf("unexpected error in output: %s", output)
	}
}
