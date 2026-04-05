package engine

// Methodology: integration test for run-level concurrency limits.
// Tests that ConcurrencyManager is wired into orchestrator event handlers
// and enforces WorkflowDef.Concurrency.MaxRuns properly.
// Uses real embedded NATS server.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestOrchestratorConcurrencySecondRunPends(t *testing.T) {
	// Methodology: MaxRuns=1. Start two runs. Second Pends.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "conc-wf", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{
			{ID: "sa", Task: "ta",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	publishEvt(js, protocol.EventWorkflowStarted,
		"cr-1", defData)
	time.Sleep(200 * time.Millisecond)

	r1, _ := orch.store.Load(context.Background(), "cr-1")
	if r1.Status != dag.RunStatusRunning {
		t.Fatalf("run-1 = %v, want Running", r1.Status)
	}

	publishEvt(js, protocol.EventWorkflowStarted,
		"cr-2", defData)
	time.Sleep(200 * time.Millisecond)

	r2, _ := orch.store.Load(context.Background(), "cr-2")
	if r2.Status != dag.RunStatusPending {
		t.Fatalf("run-2 = %v, want Pending", r2.Status)
	}
}

func TestOrchestratorConcurrencyAutoStart(t *testing.T) {
	// Methodology: MaxRuns=1. Complete run-1, run-2 auto-starts.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "conc-wf2", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{
			{ID: "sa", Task: "ta",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	publishEvt(js, protocol.EventWorkflowStarted,
		"ca-1", defData)
	time.Sleep(200 * time.Millisecond)

	publishEvt(js, protocol.EventWorkflowStarted,
		"ca-2", defData)
	time.Sleep(200 * time.Millisecond)

	// Complete run-1.
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "ca-1", "sa",
		[]byte(`"done"`))
	cd, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), cd,
		nats.MsgId(compEvt.NATSMsgID()))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r2, err := orch.store.Load(context.Background(), "ca-2")
		if err == nil &&
			r2.Status == dag.RunStatusRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run-2 should auto-start after run-1 completion")
}

// publishEvt is a helper for concurrency tests.
func publishEvt(
	js nats.JetStreamContext,
	evtType protocol.EventType,
	runID string,
	payload []byte,
) {
	evt := protocol.NewWorkflowEvent(evtType, runID, payload)
	data, _ := evt.Marshal()
	js.Publish(evt.NATSSubject(), data,
		nats.MsgId(evt.NATSMsgID()))
}

func TestOrchestratorCancelReleasesConcurrencySlot(t *testing.T) {
	// Methodology: with MaxRuns=1, start run-1, queue run-2 as
	// Pending. Cancel run-1. Verify run-2 auto-starts.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "cancel-conc", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start run-1.
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cc-run-1", defData)
	d1, _ := evt1.Marshal()
	js.Publish(evt1.NATSSubject(), d1,
		nats.MsgId(evt1.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Start run-2 (should be Pending).
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cc-run-2", defData)
	d2, _ := evt2.Marshal()
	js.Publish(evt2.NATSSubject(), d2,
		nats.MsgId(evt2.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	run2, _ := orch.store.Load(context.Background(), "cc-run-2")
	if run2.Status != dag.RunStatusPending {
		t.Fatalf("run-2 = %v, want Pending", run2.Status)
	}

	// Cancel run-1.
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cc-run-1", nil)
	cd, _ := cancelEvt.Marshal()
	js.Publish(cancelEvt.NATSSubject(), cd,
		nats.MsgId(cancelEvt.NATSMsgID()))

	// Wait for run-2 to auto-start.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r2, err := orch.store.Load(context.Background(), "cc-run-2")
		if err == nil && r2.Status == dag.RunStatusRunning {
			// Positive: run-1 cancelled.
			r1, _ := orch.store.Load(context.Background(), "cc-run-1")
			if r1.Status != dag.RunStatusCancelled {
				t.Fatalf("run-1 = %v, want Cancelled",
					r1.Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run-2 should auto-start after run-1 cancelled")
}

func TestOrchestratorStepFailReleasesConcurrency(t *testing.T) {
	// Methodology: with MaxRuns=1, start run-1 with a step that
	// fails permanently (no retries). Verify the slot is released
	// and a queued run-2 auto-starts.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "fail-conc", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "fail-t",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start run-1.
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "fc-run-1", defData)
	d1, _ := evt1.Marshal()
	js.Publish(evt1.NATSSubject(), d1,
		nats.MsgId(evt1.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Start run-2 (should be Pending).
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "fc-run-2", defData)
	d2, _ := evt2.Marshal()
	js.Publish(evt2.NATSSubject(), d2,
		nats.MsgId(evt2.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Fail run-1 permanently.
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "fc-run-1", "s1",
		[]byte(`"permanent"`))
	fd, _ := failEvt.Marshal()
	js.Publish(failEvt.NATSSubject(), fd,
		nats.MsgId(failEvt.NATSMsgID()))

	// Wait for run-1 to fail and run-2 to auto-start.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r1, e1 := orch.store.Load(context.Background(), "fc-run-1")
		r2, e2 := orch.store.Load(context.Background(), "fc-run-2")
		if e1 == nil && r1.Status == dag.RunStatusFailed &&
			e2 == nil && r2.Status == dag.RunStatusRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run-1 should fail and run-2 should auto-start")
}

func TestOrchestratorCompletionReleasesConcurrency(
	t *testing.T,
) {
	// Methodology: with MaxRuns=1, start two runs. Complete
	// run-1. Verify the completion code releases concurrency
	// and auto-starts run-2.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "comp-conc", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "comp-t",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start run-1.
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cc2-run-1", defData)
	d1, _ := evt1.Marshal()
	js.Publish(evt1.NATSSubject(), d1,
		nats.MsgId(evt1.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Start run-2 (queued as Pending).
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cc2-run-2", defData)
	d2, _ := evt2.Marshal()
	js.Publish(evt2.NATSSubject(), d2,
		nats.MsgId(evt2.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Complete run-1.
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "cc2-run-1", "s1",
		[]byte(`"done"`))
	cd, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), cd,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for auto-start of run-2.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r2, err := orch.store.Load(context.Background(), "cc2-run-2")
		if err == nil && r2.Status == dag.RunStatusRunning {
			// Positive: run-1 completed successfully.
			r1, _ := orch.store.Load(context.Background(), "cc2-run-1")
			if r1.Status != dag.RunStatusCompleted {
				t.Fatalf("run-1 = %v, want Completed",
					r1.Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run-2 should auto-start after run-1 completion")
}

func TestOrchestratorConcurrencyNoPendingRuns(t *testing.T) {
	// Methodology: with MaxRuns=1, start one run and complete it.
	// No second run queued. Verify startNextPendingRun returns
	// without error when no pending runs exist.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "nopend-conc", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start and complete a single run.
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "np-run-1", defData)
	d, _ := evt.Marshal()
	js.Publish(evt.NATSSubject(), d,
		nats.MsgId(evt.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "np-run-1", "s1",
		[]byte(`"done"`))
	cd, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), cd,
		nats.MsgId(compEvt.NATSMsgID()))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := orch.store.Load(context.Background(), "np-run-1")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Positive: run completed without error even with no
	// pending runs to auto-start.
	t.Fatalf("run should complete even with no pending runs")
}

func TestOrchestratorConcurrencyWithTimeout(t *testing.T) {
	// Methodology: with concurrency+timeout, verify pending run
	// gets timeout set when transitioning to Running.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "timeout-conc", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Timeout:     5 * time.Second,
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start run-1 (gets slot).
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "tc-run-1", defData)
	d1, _ := evt1.Marshal()
	js.Publish(evt1.NATSSubject(), d1,
		nats.MsgId(evt1.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Start run-2 (queued as Pending).
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "tc-run-2", defData)
	d2, _ := evt2.Marshal()
	js.Publish(evt2.NATSSubject(), d2,
		nats.MsgId(evt2.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Complete run-1.
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "tc-run-1", "s1",
		[]byte(`"done"`))
	cd, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), cd,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for run-2 to transition.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r2, err := orch.store.Load(context.Background(), "tc-run-2")
		if err == nil && r2.Status == dag.RunStatusRunning {
			// Positive: run-2 is now Running.
			// Positive: deadline is set.
			if r2.Deadline == nil {
				t.Fatal("deadline should be set on run-2")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run-2 should auto-start with deadline after run-1")
}

func TestOrchestratorFailedLoopReleasesConcurrency(t *testing.T) {
	// Methodology: with MaxRuns=1, start an agent-loop run that
	// exceeds MaxIterations. Verify concurrency slot is released
	// and a queued run auto-starts.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "loop-conc", Version: "1",
		Concurrency: &dag.ConcurrencyLimit{MaxRuns: 1},
		Steps: []dag.StepDef{{
			ID: "loop", Task: "loop-t",
			Type:   dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 1}),
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start run-1.
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "lc-run-1", defData)
	d1, _ := evt1.Marshal()
	js.Publish(evt1.NATSSubject(), d1,
		nats.MsgId(evt1.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Start run-2 (queued as Pending).
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "lc-run-2", defData)
	d2, _ := evt2.Marshal()
	js.Publish(evt2.NATSSubject(), d2,
		nats.MsgId(evt2.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	// Send continue to trip MaxIterations on run-1.
	cont := protocol.NewStepEvent(
		protocol.EventStepContinue, "lc-run-1", "loop", nil)
	cd, _ := cont.Marshal()
	js.Publish(cont.NATSSubject(), cd,
		nats.MsgId(cont.NATSMsgID()))

	// Wait for run-1 to fail and run-2 to auto-start.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r1, err1 := orch.store.Load(context.Background(), "lc-run-1")
		r2, err2 := orch.store.Load(context.Background(), "lc-run-2")
		if err1 == nil && r1.Status == dag.RunStatusFailed &&
			err2 == nil && r2.Status == dag.RunStatusRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(
		"run-1 should fail and run-2 should auto-start")
}
