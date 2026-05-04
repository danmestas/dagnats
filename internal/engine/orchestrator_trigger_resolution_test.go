// internal/engine/orchestrator_trigger_resolution_test.go
// Verifies the orchestrator resolves a registered WorkflowDef from
// workflow_defs KV when a workflow.started event arrives carrying a
// TriggerEnvelope payload (i.e. trigger-fired). Methodology: red-green
// TDD with embedded NATS, a real Orchestrator, and a TriggerEnvelope-
// shaped payload published manually so the engine path is exercised
// without bringing up the trigger service. End-to-end tests for each
// trigger type live in separate files.
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

// triggerEnvelope mirrors internal/trigger.TriggerEnvelope on the wire.
// Engine code cannot import the trigger package, so this test redeclares
// the shape locally — the field names and JSON tags are the contract.
type triggerEnvelope struct {
	Trigger    string          `json:"trigger"`
	Source     string          `json:"source"`
	WorkflowID string          `json:"workflow_id"`
	Timestamp  time.Time       `json:"timestamp"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// TestOrchestrator_ResolvesDefFromTriggerEnvelope exercises #167:
// when workflow.started arrives with a TriggerEnvelope payload (no
// embedded WorkflowDef) the orchestrator must look up the registered
// def by WorkflowID and dispatch the first task.
func TestOrchestrator_ResolvesDefFromTriggerEnvelope(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "trig-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-trig-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	envelope := triggerEnvelope{
		Trigger:    "cron",
		Source:     "trig-1",
		WorkflowID: "trig-wf",
		Timestamp:  time.Now().UTC(),
	}
	envBytes := mustMarshal(t, envelope)
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "trig-wf-run-1", envBytes,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal evt: %v", err)
	}
	mustPublish(t, js, evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()))

	sub, err := js.PullSubscribe("task.task-trig-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("engine did not dispatch task from trigger envelope: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	deadline := time.Now().Add(2 * time.Second)
	var run dag.WorkflowRun
	var loaded bool
	for time.Now().Before(deadline) && !loaded {
		r, loadErr := orch.store.Load(context.Background(), "trig-wf-run-1")
		if loadErr == nil && r.WorkflowID == "trig-wf" {
			run = r
			loaded = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !loaded {
		t.Fatal("expected snapshot for run-1 within 2s")
	}
	if run.Status != dag.RunStatusRunning {
		t.Fatalf("Status = %q, want Running", run.Status)
	}
	if len(run.Input) == 0 {
		t.Fatal("expected envelope in run.Input, got empty")
	}
	var roundTrip triggerEnvelope
	if err := json.Unmarshal(run.Input, &roundTrip); err != nil {
		t.Fatalf("Input is not a TriggerEnvelope: %v", err)
	}
	if roundTrip.Trigger != "cron" || roundTrip.Source != "trig-1" {
		t.Fatalf("envelope round-trip wrong: %+v", roundTrip)
	}
}

// TestOrchestrator_TriggerEnvelopeMissingWorkflowFails verifies the
// engine fails the run cleanly (no crash) when a trigger envelope
// references a workflow name that has no registered definition.
func TestOrchestrator_TriggerEnvelopeMissingWorkflowFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	envelope := triggerEnvelope{
		Trigger:    "cron",
		Source:     "trig-missing",
		WorkflowID: "no-such-workflow",
		Timestamp:  time.Now().UTC(),
	}
	envBytes := mustMarshal(t, envelope)
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "missing-run-1", envBytes,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal evt: %v", err)
	}
	mustPublish(t, js, evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()))

	deadline := time.Now().Add(5 * time.Second)
	var sawFailed bool
	for time.Now().Before(deadline) && !sawFailed {
		r, loadErr := orch.store.Load(
			context.Background(), "missing-run-1",
		)
		if loadErr == nil && r.Status == dag.RunStatusFailed {
			sawFailed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawFailed {
		t.Fatal("expected RunStatusFailed snapshot for missing-workflow run")
	}

	// Negative: engine still serves a valid event after the missing one.
	wfDef := dag.WorkflowDef{
		Name: "still-alive-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-still-alive", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))
	follow := triggerEnvelope{
		Trigger:    "cron",
		Source:     "trig-ok",
		WorkflowID: "still-alive-wf",
		Timestamp:  time.Now().UTC(),
	}
	followBytes := mustMarshal(t, follow)
	followEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "still-alive-run", followBytes,
	)
	followData, mErr := followEvt.Marshal()
	if mErr != nil {
		t.Fatalf("marshal follow evt: %v", mErr)
	}
	mustPublish(t, js, followEvt.NATSSubject(), followData,
		nats.MsgId(followEvt.NATSMsgID()))

	sub, err := js.PullSubscribe("task.task-still-alive.*", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, fErr := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if fErr != nil {
		t.Fatalf("engine wedged after missing-workflow event: %v", fErr)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task after recovery, got %d", len(msgs))
	}
}
