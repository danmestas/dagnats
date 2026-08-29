// internal/engine/task_publish_test.go
// Tests for collectReadyMessages.
// Methodology: pure unit tests, no NATS required.
package engine

import (
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
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
	msgs, err := collectReadyMessages(
		"run-1", steps, run, nil, "deploy-pipeline",
	)
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
	// Positive: correct attempt and workflow_name in payload (#503)
	var p protocol.TaskPayload
	if err := json.Unmarshal(msgs[1].Data, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Attempt != 1 {
		t.Errorf("msg[1] attempt = %d, want 1", p.Attempt)
	}
	if p.WorkflowName != "deploy-pipeline" {
		t.Errorf(
			"msg[1] WorkflowName = %q, want %q",
			p.WorkflowName, "deploy-pipeline",
		)
	}
	// Negative: empty steps produces empty slice
	empty, err := collectReadyMessages(
		"run-1", nil, run, nil, "deploy-pipeline",
	)
	if err != nil {
		t.Fatalf("empty steps error: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty steps: got %d msgs, want 0", len(empty))
	}
}

// TestCollectReadyMessages_Metadata verifies that StepDef.Metadata is copied
// into TaskPayload.Metadata so workers receive static per-step config without
// a KV round-trip.
//
// Methodology: pure unit tests — no NATS required. collectReadyMessages is the
// single path that builds TaskPayload from a StepDef; we confirm the field
// threads through by marshalling the resulting nats.Msg data.
// Positive: a step with Metadata produces the same map in TaskPayload.
// Negative: a step without Metadata yields nil/empty Metadata in TaskPayload.
func TestCollectReadyMessages_Metadata(t *testing.T) {
	steps := []dag.StepDef{
		{
			ID:   "with-meta",
			Task: "dagger.call",
			Metadata: map[string]string{
				"module": ".",
				"call":   "test",
			},
		},
		{
			ID:   "no-meta",
			Task: "dagger.check",
		},
	}
	run := &dag.WorkflowRun{
		RunID: "run-meta",
		Steps: map[string]dag.StepState{
			"with-meta": {Status: dag.StepStatusQueued, Attempts: 0},
			"no-meta":   {Status: dag.StepStatusQueued, Attempts: 0},
		},
	}
	msgs, err := collectReadyMessages(
		"run-meta", steps, run, nil, "meta-workflow",
	)
	if err != nil {
		t.Fatalf("collectReadyMessages: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	// Unmarshal both messages; order matches the steps slice.
	var p0 protocol.TaskPayload
	if err := json.Unmarshal(msgs[0].Data, &p0); err != nil {
		t.Fatalf("unmarshal msgs[0]: %v", err)
	}
	var p1 protocol.TaskPayload
	if err := json.Unmarshal(msgs[1].Data, &p1); err != nil {
		t.Fatalf("unmarshal msgs[1]: %v", err)
	}

	// Positive: the step with Metadata has the map in the payload.
	if p0.Metadata == nil {
		t.Fatal("msgs[0].Metadata is nil; expected map from StepDef.Metadata")
	}
	if p0.Metadata["call"] != "test" {
		t.Errorf(`Metadata["call"] = %q, want "test"`, p0.Metadata["call"])
	}
	if p0.Metadata["module"] != "." {
		t.Errorf(`Metadata["module"] = %q, want "."`, p0.Metadata["module"])
	}

	// Negative: a step without Metadata produces nil/empty Metadata in the payload.
	if len(p1.Metadata) != 0 {
		t.Errorf("msgs[1].Metadata should be empty/nil, got %v", p1.Metadata)
	}
}
