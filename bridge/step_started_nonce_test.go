// bridge/step_started_nonce_test.go
// Methodology: a task carrying a dispatch nonce is published to the
// task stream, a poller claims it over HTTP, and the history stream is
// read back. The step.started the bridge publishes on hand-out must
// carry that nonce (#814), mirroring the native worker, so the engine
// can ignore a step.started for a dispatch the step has already left.
// Negative space: step.started is not fenced, so it carries no
// iteration (no full identity).
package bridge

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPollStepStartedCarriesDispatchNonce(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	const runID, nonce = "run-nonce", "dispatch-nonce-1"
	payload := protocol.TaskPayload{
		RunID: runID, StepID: "step-nonce", DispatchNonce: nonce,
		Input: json.RawMessage(`{"x":1}`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.Publish(context.Background(), "task.echo."+runID, data); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	tasks := postPoll(t, ts, "echo", 1, 3000)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	msg := fetchHistoryMsg(t, nc, runID, protocol.EventStepStarted, 2*time.Second)
	var started protocol.Event
	if err := json.Unmarshal(msg.Data(), &started); err != nil {
		t.Fatalf("unmarshal step.started: %v", err)
	}
	if started.DispatchNonce != nonce {
		t.Fatalf("step.started dispatch_nonce = %q, want %q", started.DispatchNonce, nonce)
	}
	if started.HasIdentity() || started.AttemptNumber != 1 {
		t.Fatalf("step.started = attempt %d identity=%v, want attempt 1 without a fenced identity",
			started.AttemptNumber, started.HasIdentity())
	}
}
