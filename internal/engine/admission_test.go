// internal/engine/admission_test.go
// Tests for the admitRun pipeline: priority ordering and
// singleton lock enforcement.
// Uses real embedded NATS server.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestPriorityAffectsPendingOrder(t *testing.T) {
	// With concurrency=1, start two runs:
	// run-low (offset=0) first, then run-high (offset=300).
	// run-low should be running, run-high pending with offset.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "prio-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "echo",
				Type: dag.StepTypeNormal,
			},
		},
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Priority: &dag.PriorityConfig{
			Key:   "tier",
			Rules: map[string]int{"high": 300},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	if _, err := defKV.Put(wfDef.Name, defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start run-low (no priority match = offset 0)
	startAdmissionRun(t, js, wfDef, "run-low",
		[]byte(`{"tier":"free"}`))
	time.Sleep(300 * time.Millisecond)

	// Start run-high (priority match = offset 300)
	startAdmissionRun(t, js, wfDef, "run-high",
		[]byte(`{"tier":"high"}`))
	time.Sleep(300 * time.Millisecond)

	// Positive: run-low is running
	lowRun, err := orch.store.Load(context.Background(), "run-low")
	if err != nil {
		t.Fatalf("load run-low: %v", err)
	}
	if lowRun.Status != dag.RunStatusRunning {
		t.Fatalf("run-low status = %s, want running",
			lowRun.Status)
	}

	// run-high should be pending with priority offset
	highRun, err := orch.store.Load(context.Background(), "run-high")
	if err != nil {
		t.Fatalf("load run-high: %v", err)
	}
	if highRun.Status != dag.RunStatusPending {
		t.Fatalf("run-high status = %s, want pending",
			highRun.Status)
	}

	// Positive: run-high has priority offset
	if highRun.PriorityOffset != 300 {
		t.Fatalf("PriorityOffset = %d, want 300",
			highRun.PriorityOffset)
	}
}

func TestSingletonSkipMode(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "singleton-skip-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "echo",
				Type: dag.StepTypeNormal,
			},
		},
		Singleton: &dag.SingletonConfig{
			Mode: dag.SingletonModeSkip,
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	if _, err := defKV.Put(wfDef.Name, defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start first run
	startAdmissionRun(t, js, wfDef, "run-first", nil)
	time.Sleep(300 * time.Millisecond)

	// Start second run -- should be skipped
	startAdmissionRun(t, js, wfDef, "run-second", nil)
	time.Sleep(300 * time.Millisecond)

	// Positive: first run is running
	firstRun, err := orch.store.Load(context.Background(), "run-first")
	if err != nil {
		t.Fatalf("load run-first: %v", err)
	}
	if firstRun.Status != dag.RunStatusRunning {
		t.Fatalf("run-first = %s, want running",
			firstRun.Status)
	}

	// Negative: second run should not exist (skipped)
	_, err = orch.store.Load(context.Background(), "run-second")
	if err == nil {
		t.Fatal(
			"run-second should not exist (singleton skip)",
		)
	}
}

func TestSingletonCancelMode(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "cancel-singleton",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "slow-task",
				Type: dag.StepTypeNormal,
			},
		},
		Singleton: &dag.SingletonConfig{
			Mode: dag.SingletonModeCancel,
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	if _, err := defKV.Put(wfDef.Name, defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start first run
	startAdmissionRun(t, js, wfDef, "run-1", nil)
	time.Sleep(300 * time.Millisecond)

	// Start second run -- should cancel first
	startAdmissionRun(t, js, wfDef, "run-2", nil)
	time.Sleep(300 * time.Millisecond)

	// Positive: run-2 should be running
	run2, err := orch.store.Load(context.Background(), "run-2")
	if err != nil {
		t.Fatalf("load run-2: %v", err)
	}
	if run2.Status != dag.RunStatusRunning {
		t.Errorf("run-2 status = %s, want running",
			run2.Status)
	}

	// Positive: run-1 should be cancelled
	run1, err := orch.store.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("load run-1: %v", err)
	}
	if run1.Status != dag.RunStatusCancelled {
		t.Errorf("run-1 status = %s, want cancelled",
			run1.Status)
	}
}

// startAdmissionRun publishes a workflow.started event with
// the new-format payload containing both def and input.
func startAdmissionRun(
	t *testing.T, js nats.JetStreamContext,
	wfDef dag.WorkflowDef, runID string,
	input []byte,
) {
	t.Helper()
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal wfDef: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"workflow_def": json.RawMessage(defData),
		"input":        json.RawMessage(input),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := js.Publish(
		evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish event: %v", err)
	}
}
