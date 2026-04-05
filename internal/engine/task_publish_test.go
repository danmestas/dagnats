// internal/engine/task_publish_test.go
// Tests for collectReadyMessages and atomic fan-out.
// Methodology: pure unit tests for collectReadyMessages,
// real embedded NATS server for integration tests.
package engine

import (
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCollectReadyMessages(t *testing.T) {
	steps := []dag.StepDef{
		{ID: "a", Task: "compile"},
		{ID: "b", Task: "test"},
	}
	run := &dag.WorkflowRun{
		RunID: "run-1",
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusQueued, Attempts: 0},
			"b": {Status: dag.StepStatusQueued, Attempts: 1},
		},
	}
	msgs, err := collectReadyMessages("run-1", steps, run)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Positive: correct count
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	// Positive: correct subject
	if msgs[0].Subject != "task.compile.run-1" {
		t.Errorf(
			"msg[0] subject = %q, want task.compile.run-1",
			msgs[0].Subject,
		)
	}
	// Positive: correct attempt in payload
	var p protocol.TaskPayload
	if err := json.Unmarshal(msgs[1].Data, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Attempt != 1 {
		t.Errorf("msg[1] attempt = %d, want 1", p.Attempt)
	}
	// Negative: empty steps produces empty slice
	empty, err := collectReadyMessages("run-1", nil, run)
	if err != nil {
		t.Fatalf("empty steps error: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty steps: got %d msgs, want 0", len(empty))
	}
}

func TestEnqueueReadySteps_AtomicPublish(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "test", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "compile"},
			{ID: "b", Task: "test"},
		},
	}
	run := &dag.WorkflowRun{
		RunID:      "run-1",
		WorkflowID: "test",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusPending},
			"b": {Status: dag.StepStatusPending},
		},
	}
	err = enqueueReadySteps(jsLegacy, js, wfDef, run)
	if err != nil {
		t.Fatalf("enqueueReadySteps: %v", err)
	}
	// Positive: steps marked queued
	if run.Steps["a"].Status != dag.StepStatusQueued {
		t.Errorf(
			"step a: %v, want queued",
			run.Steps["a"].Status,
		)
	}
	// Positive: messages in stream
	info, err := jsLegacy.StreamInfo("TASK_QUEUES")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if info.State.Msgs != 2 {
		t.Errorf("stream msgs = %d, want 2", info.State.Msgs)
	}
}
