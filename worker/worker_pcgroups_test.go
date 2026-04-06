// worker/worker_pcgroups_test.go
// Tests for pcgroups-based elastic consumer group subscriptions.
// Methodology: real embedded NATS server, publish tasks, verify
// delivery through elastic consumer groups.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWorker_ElasticConsume(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var mu sync.Mutex
	received := make([]string, 0, 2)

	w := NewWorker(nc,
		WithPartitions(2),
	)
	w.Handle("compile", func(ctx TaskContext) error {
		mu.Lock()
		received = append(received, ctx.StepID())
		mu.Unlock()
		return ctx.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	// Publish two tasks
	for _, stepID := range []string{"a", "b"} {
		payload := protocol.TaskPayload{
			TaskID: "run-1." + stepID,
			RunID:  "run-1",
			StepID: stepID,
			Input:  json.RawMessage(`"data"`),
		}
		data, _ := json.Marshal(payload)
		_, pubErr := js.Publish(
			context.Background(),
			"task.compile.run-1",
			data,
		)
		if pubErr != nil {
			t.Fatalf("Publish %s: %v", stepID, pubErr)
		}
	}

	// Wait for delivery with bounded timeout
	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timeout: %d/2 messages received",
				len(received))
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Positive: both messages received
	mu.Lock()
	if len(received) != 2 {
		t.Errorf("received %d, want 2", len(received))
	}
	mu.Unlock()

	// Negative: no extras after drain
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	if len(received) != 2 {
		t.Errorf("extra messages: %d", len(received)-2)
	}
	mu.Unlock()
}

func TestWorker_Singleton(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var count atomic.Int32
	w := NewWorker(nc,
		WithPartitions(4),
	)
	w.HandleSingleton("deploy", func(ctx TaskContext) error {
		count.Add(1)
		return ctx.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		TaskID: "run-1.deploy",
		RunID:  "run-1",
		StepID: "deploy",
		Input:  json.RawMessage(`"data"`),
	}
	data, _ := json.Marshal(payload)
	_, pubErr := js.Publish(
		context.Background(),
		"task.deploy.run-1",
		data,
	)
	if pubErr != nil {
		t.Fatalf("Publish: %v", pubErr)
	}

	deadline := time.After(15 * time.Second)
	for count.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for singleton task")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Positive: processed exactly once
	if count.Load() != 1 {
		t.Errorf("count = %d, want 1", count.Load())
	}

	// Negative: no extra deliveries after drain
	time.Sleep(500 * time.Millisecond)
	if count.Load() != 1 {
		t.Errorf("extra deliveries: count = %d", count.Load())
	}
}

func TestHandleSingletonPanicsOnEmptyTaskType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc,
		WithPartitions(4),
	)
	defer func() {
		r := recover()
		// Positive: panics on empty taskType
		if r == nil {
			t.Fatal("expected panic for empty taskType")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: panic message is specific
		if msg != "HandleSingleton: taskType must not be empty" {
			t.Fatalf("panic = %q, want taskType message", msg)
		}
	}()
	w.HandleSingleton("", func(ctx TaskContext) error {
		return nil
	})
}

func TestHandleSingletonPanicsOnNilHandler(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc,
		WithPartitions(4),
	)
	defer func() {
		r := recover()
		// Positive: panics on nil handler
		if r == nil {
			t.Fatal("expected panic for nil handler")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: panic message mentions handler
		if msg != "HandleSingleton: handler must not be nil" {
			t.Fatalf("panic = %q, want handler message", msg)
		}
	}()
	w.HandleSingleton("deploy", nil)
}

func TestWithPartitionsBounds(t *testing.T) {
	// Positive: valid partitions accepted
	opt := WithPartitions(4)
	if opt == nil {
		t.Fatal("expected non-nil option")
	}

	// Negative: negative panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for negative partitions")
		}
	}()
	WithPartitions(-1)
}

func TestWithPartitionsUpperBound(t *testing.T) {
	// Negative: >256 panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for partitions > 256")
		}
	}()
	WithPartitions(257)
}
