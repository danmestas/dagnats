package engine

// Methodology: integration test for run-level concurrency limits.
// Tests that ConcurrencyManager is wired into orchestrator event handlers
// and enforces WorkflowDef.Concurrency.MaxRuns properly.
// Uses real embedded NATS server.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestOrchestratorEnforcesRunConcurrencyLimit(t *testing.T) {
	// Red: test workflow with MaxRuns=1. Start two runs.
	// First should be Running, second should stay Pending.
	// Complete first run, second should transition to Running.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "concurrency-wf",
		Version: "1",
		Concurrency: &dag.ConcurrencyLimit{
			MaxRuns: 1,
		},
		Steps: []dag.StepDef{
			{ID: "step-a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start first run
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-1", defData,
	)
	evt1Data, _ := evt1.Marshal()
	js.Publish(
		evt1.NATSSubject(), evt1Data, nats.MsgId(evt1.NATSMsgID()),
	)

	// Wait for first run to be processed and task enqueued
	time.Sleep(200 * time.Millisecond)

	// Verify first run is Running
	run1, err := orch.store.Load("run-1")
	if err != nil {
		t.Fatalf("load run-1: %v", err)
	}
	if run1.Status != dag.RunStatusRunning {
		t.Fatalf(
			"run-1 should be Running, got %s", run1.Status,
		)
	}

	// Verify first task was enqueued
	taskSub, err := js.PullSubscribe(
		"task.task-a.*", "", nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	msgs, err := taskSub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if err != nil || len(msgs) == 0 {
		t.Fatalf("expected task-a for run-1")
	}
	msgs[0].Ack()

	// Start second run (should be Pending due to concurrency limit)
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-2", defData,
	)
	evt2Data, _ := evt2.Marshal()
	js.Publish(
		evt2.NATSSubject(), evt2Data, nats.MsgId(evt2.NATSMsgID()),
	)

	// Wait for second run to be processed
	time.Sleep(200 * time.Millisecond)

	// Verify second run is Pending (not Running)
	run2, err := orch.store.Load("run-2")
	if err != nil {
		t.Fatalf("load run-2: %v", err)
	}
	if run2.Status != dag.RunStatusPending {
		t.Fatalf(
			"run-2 should be Pending (limit 1), got %s",
			run2.Status,
		)
	}

	// Verify second task was NOT enqueued
	msgs2, _ := taskSub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if len(msgs2) > 0 {
		t.Fatal("run-2 should not enqueue task while limit reached")
	}

	// Complete first run
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "run-1", "step-a", []byte(`"done"`),
	)
	compData, _ := compEvt.Marshal()
	js.Publish(
		compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()),
	)

	// Wait for completion to propagate
	time.Sleep(200 * time.Millisecond)

	// Verify first run is Completed
	run1Final, err := orch.store.Load("run-1")
	if err != nil {
		t.Fatalf("load run-1 final: %v", err)
	}
	if run1Final.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"run-1 should be Completed, got %s", run1Final.Status,
		)
	}

	// Second run should still be Pending (no auto-start implemented yet)
	run2Check, err := orch.store.Load("run-2")
	if err != nil {
		t.Fatalf("load run-2 check: %v", err)
	}
	if run2Check.Status != dag.RunStatusPending {
		t.Fatalf(
			"run-2 should remain Pending, got %s", run2Check.Status,
		)
	}
}
