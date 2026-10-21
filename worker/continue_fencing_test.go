// worker/continue_fencing_test.go
// The worker side of fenced step events (#801): Continue carries the
// checkpointed state and the dispatch identity under a stable msg-id,
// refuses a state over the engine's cap before publishing, and
// LoadCheckpoint prefers a pause marker in the checkpoints bucket over
// the payload state until a Continue consumes it.
//
// Methodology: a taskContext built from a TaskPayload the way the
// Worker builds one, against an embedded NATS server where an event
// must be read back. Every wait is bounded.
package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// contextForPayload builds a taskContext for payload on nc (nil for a
// context that must never publish).
func contextForPayload(
	t *testing.T, nc *nats.Conn, payload protocol.TaskPayload, cpKV jetstream.KeyValue,
) *taskContext {
	t.Helper()
	var js jetstream.JetStream
	if nc != nil {
		var err error
		js, err = jetstream.New(nc)
		if err != nil {
			t.Fatalf("jetstream.New: %v", err)
		}
	}
	tr := tracenoop.NewTracerProvider().Tracer("test")
	_, span := tr.Start(context.Background(), "test")
	return newTaskContext(
		nc, tr, js, payload, context.Background(), span,
		&testJetstreamMsg{}, cpKV, nil,
	)
}

// readContinue returns the first step.continue on runID's history.
func readContinue(t *testing.T, nc *nats.Conn, runID string) (protocol.Event, string) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no history event: %v", err)
	}
	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	return evt, msg.Header.Get("Nats-Msg-Id")
}

func TestContinue_CarriesCheckpointedStateAndIdentity(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	tc := contextForPayload(t, nc, protocol.TaskPayload{
		RunID: "run-cont", StepID: "s", Iteration: 2, Attempt: 3,
		DispatchNonce: "nonce-2", State: []byte(`{"turn":2}`),
	}, nil)
	if err := tc.Checkpoint([]byte(`{"turn":3}`)); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := tc.Continue([]byte(`"out"`)); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	evt, msgID := readContinue(t, nc, "run-cont")
	if !evt.HasIdentity() || *evt.Iteration != 2 || evt.AttemptNumber != 3 ||
		evt.DispatchNonce != "nonce-2" {
		t.Fatalf("event identity = (%v, %d, %q), want (2, 3, nonce-2)",
			evt.Iteration, evt.AttemptNumber, evt.DispatchNonce)
	}
	if want := "run-cont.s.step.continue.3.2"; msgID != want {
		t.Fatalf("msg-id = %q, want %q", msgID, want)
	}
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(payload.State) != `{"turn":3}` || string(payload.Output) != `"out"` {
		t.Fatalf("payload = state %s output %s, want the checkpointed state and output",
			payload.State, payload.Output)
	}
}

func TestContinue_WithoutCheckpointCarriesPayloadStateForward(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	tc := contextForPayload(t, nc, protocol.TaskPayload{
		RunID: "run-fwd", StepID: "s", Iteration: 1, Attempt: 1, State: []byte(`{"turn":1}`),
	}, nil)
	if err := tc.Continue(nil); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	evt, _ := readContinue(t, nc, "run-fwd")
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(payload.State) != `{"turn":1}` {
		t.Fatalf("state = %s, want the payload state carried forward", payload.State)
	}
	if len(payload.Output) != 0 {
		t.Fatalf("output = %s, want none", payload.Output)
	}
}

func TestContinue_OverCapStateFailsBeforePublishing(t *testing.T) {
	tc := contextForPayload(t, nil, protocol.TaskPayload{
		RunID: "run-cap", StepID: "s", StateMaxBytes: 64,
	}, nil)
	if err := tc.Checkpoint([]byte(strings.Repeat("x", 100))); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	err := tc.Continue(nil)
	if err == nil || !strings.Contains(err.Error(), "100 bytes") ||
		!strings.Contains(err.Error(), "64 byte cap") {
		t.Fatalf("Continue error = %v, want one naming 100 bytes and the 64 byte cap", err)
	}
	// Without a cap on the payload the protocol default applies.
	tc = contextForPayload(t, nil, protocol.TaskPayload{RunID: "run-cap", StepID: "s"}, nil)
	if err := tc.Checkpoint(make([]byte, protocol.LoopStateMaxBytesDefault+1)); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	err = tc.Continue(nil)
	if err == nil || !strings.Contains(err.Error(), "262144 byte cap") {
		t.Fatalf("Continue error = %v, want the default cap", err)
	}
}

func TestLoadCheckpoint_PauseMarkerShadowsPayloadStateUntilContinue(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc, natsutil.WithKVBuckets(
		natsutil.KVConfig{Bucket: "checkpoints"},
	)); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cpKV, err := js.KeyValue(context.Background(), "checkpoints")
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	payload := protocol.TaskPayload{
		RunID: "run-pm", StepID: "s", Iteration: 1, Attempt: 1, State: []byte(`{"turn":1}`),
	}
	paused := contextForPayload(t, nc, payload, cpKV)
	if err := paused.Pause("wait", time.Second); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	// The redelivery sees the marker, not the committed state.
	resumed := contextForPayload(t, nc, payload, cpKV)
	got, err := resumed.LoadCheckpoint()
	if err != nil || !strings.Contains(string(got), "pause_resume") {
		t.Fatalf("LoadCheckpoint after pause = (%s, %v), want the pause marker", got, err)
	}
	if err := resumed.Continue(nil); err != nil {
		t.Fatalf("Continue: %v", err)
	}

	// The marker is consumed: the next iteration sees committed state.
	next := contextForPayload(t, nc, payload, cpKV)
	got, err = next.LoadCheckpoint()
	if err != nil || string(got) != `{"turn":1}` {
		t.Fatalf("LoadCheckpoint after continue = (%s, %v), want the payload state", got, err)
	}
	if _, err := cpKV.Get(context.Background(), "run-pm.s"); err == nil {
		t.Fatal("pause marker still in the checkpoints bucket after Continue")
	}
}
