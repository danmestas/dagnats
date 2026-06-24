// engine/dispatch_nonce_test.go
// Regression tests for the per-dispatch nonce (#380): EVERY path that builds
// a TaskPayload for a worker dispatch must stamp a non-empty DispatchNonce,
// otherwise a granted control-plane handler is over-denied by the server's
// VerifyDispatch ("dispatch proof required"). The agent-loop Continue
// re-enqueue (PublishIteration) and the timer-driven retry republish paths
// (rate_retry / retry_after / retry_backoff / sticky-fallback) are the ones
// that regressed. Methodology: a capturing JetStream mock records the
// published bytes; we unmarshal the TaskPayload and assert the nonce is set.
package engine

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// capturingJS records the bytes of every PublishMsg so a test can inspect
// the marshalled TaskPayload.
type capturingJS struct {
	jetstream.JetStream
	mu   sync.Mutex
	last []byte
}

func (c *capturingJS) PublishMsg(
	_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	c.mu.Lock()
	c.last = append([]byte(nil), msg.Data...)
	c.mu.Unlock()
	return &jetstream.PubAck{Stream: "TASK_QUEUES"}, nil
}

func (c *capturingJS) lastPayload(t *testing.T) protocol.TaskPayload {
	t.Helper()
	c.mu.Lock()
	data := c.last
	c.mu.Unlock()
	if len(data) == 0 {
		t.Fatal("no message was published")
	}
	var p protocol.TaskPayload
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("unmarshal published TaskPayload: %v", err)
	}
	return p
}

func newCapturingPublisher(js *capturingJS) *TaskPublisher {
	return &TaskPublisher{
		js:     js,
		pub:    natsutil.NewTracingPublisherJSOnly(js),
		tracer: tracenoop.NewTracerProvider().Tracer("test"),
		metrics: pubMetrics{
			stepEnqueue:      &noopCounter{},
			taskConcAcquired: &noopCounter{},
			taskConcRejected: &noopCounter{},
		},
	}
}

// TestPublishIterationStampsNonce proves the agent-loop Continue re-enqueue
// stamps a fresh nonce so a granted agent-loop step can still call the
// control plane after iterating.
func TestPublishIterationStampsNonce(t *testing.T) {
	js := &capturingJS{}
	tp := newCapturingPublisher(js)
	step := dag.StepDef{ID: "loop", Task: "agent", Type: dag.StepTypeAgentLoop}

	// Pass an empty nonce to prove the defensive choke-point mint: even if a
	// caller forgets to thread one, the payload still carries a nonce.
	if err := tp.PublishIteration(
		context.Background(), "run-1", step, []byte(`{}`), 3, "wf", "",
	); err != nil {
		t.Fatalf("PublishIteration: %v", err)
	}
	p := js.lastPayload(t)
	if p.DispatchNonce == "" {
		t.Fatal("PublishIteration must stamp a non-empty DispatchNonce")
	}
	// Negative space: iteration index is preserved (not clobbered).
	if p.Iteration != 3 {
		t.Fatalf("Iteration = %d, want 3", p.Iteration)
	}
}

// TestRepublishTaskStampsNonce proves the retry_after / retry_backoff timer
// republish path carries the nonce threaded through the TimerMessage.
func TestRepublishTaskStampsNonce(t *testing.T) {
	js := &capturingJS{}
	st := &SleepTimer{
		js: js, tp: natsutil.NewTracingPublisherJSOnly(js),
	}
	tm := TimerMessage{
		Action:        TimerActionRetryAfter,
		RunID:         "run-1",
		StepID:        "step-1",
		TaskType:      "my-task",
		Input:         []byte(`{}`),
		Attempt:       1,
		DispatchNonce: "nonce-from-schedule",
	}
	st.republishTask(tm, "retry_after")

	p := js.lastPayload(t)
	if p.DispatchNonce != "nonce-from-schedule" {
		t.Fatalf("republishTask nonce = %q, want carried-through value",
			p.DispatchNonce)
	}
}

// TestFireRateRetryStampsNonce proves the rate-retry (and sticky-fallback,
// which reuses this action) republish path carries the nonce.
func TestFireRateRetryStampsNonce(t *testing.T) {
	js := &capturingJS{}
	st := &SleepTimer{
		js: js, tp: natsutil.NewTracingPublisherJSOnly(js),
	}
	tm := TimerMessage{
		Action:        TimerActionRateRetry,
		RunID:         "run-1",
		StepID:        "step-1",
		TaskType:      "my-task",
		Input:         []byte(`{}`),
		DispatchNonce: "nonce-from-schedule",
	}
	st.fireRateRetry(tm)

	p := js.lastPayload(t)
	if p.DispatchNonce != "nonce-from-schedule" {
		t.Fatalf("fireRateRetry nonce = %q, want carried-through value",
			p.DispatchNonce)
	}
}
