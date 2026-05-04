// internal/engine/orchestrator_resilience_test.go
// Verifies the orchestrator does not crash on malformed workflow.started
// events or panicking handlers. Methodology: red-green TDD. Each test
// asserts a positive outcome (engine still serves valid events) and a
// negative property (the bad event is recorded as a failure rather than
// crashing the process).
package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// TestOrchestrator_EmptyWorkflowDefDoesNotCrash exercises issue #166:
// a workflow.started event whose embedded WorkflowDef has zero steps
// must produce a RunStatusFailed snapshot and leave the engine alive
// to process subsequent events. Pre-fix the dag.NewWorkflowRun
// invariant panic kills the consumer goroutine and the process exits.
func TestOrchestrator_EmptyWorkflowDefDoesNotCrash(t *testing.T) {
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

	emptyDef := dag.WorkflowDef{Name: "empty-wf", Version: "1"}
	emptyData := mustMarshal(t, emptyDef)
	badEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "bad-run", emptyData,
	)
	badData, err := badEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal bad evt: %v", err)
	}
	mustPublish(t, js, badEvt.NATSSubject(), badData,
		nats.MsgId(badEvt.NATSMsgID()))

	deadline := time.Now().Add(5 * time.Second)
	var failedRun dag.WorkflowRun
	var sawFailed bool
	for time.Now().Before(deadline) && !sawFailed {
		r, loadErr := orch.store.Load(context.Background(), "bad-run")
		if loadErr == nil && r.Status == dag.RunStatusFailed {
			failedRun = r
			sawFailed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawFailed {
		t.Fatalf("expected RunStatusFailed snapshot for bad-run within 5s")
	}
	if failedRun.WorkflowID != "empty-wf" {
		t.Fatalf("WorkflowID = %q, want %q",
			failedRun.WorkflowID, "empty-wf")
	}

	validDef := dag.WorkflowDef{
		Name: "valid-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	validData := mustMarshal(t, validDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, validDef.Name, validData)
	validEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "good-run", validData,
	)
	goodData, err := validEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal good evt: %v", err)
	}
	mustPublish(t, js, validEvt.NATSSubject(), goodData,
		nats.MsgId(validEvt.NATSMsgID()))

	sub, err := js.PullSubscribe("task.task-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("engine did not process valid event after bad event: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task after good run, got %d", len(msgs))
	}
}

// TestOrchestrator_DispatchRecoversFromHandlerPanic verifies that
// dispatchEvent converts handler panics into errors instead of letting
// them escape the goroutine. We invoke dispatchEvent directly with an
// event whose Payload is nil — handleWorkflowStarted asserts Payload
// non-nil via panic. Pre-fix the panic propagates through the
// safeDispatch wrapper. Post-fix dispatchEvent's defer recover converts
// it to a "handler panic" error.
func TestOrchestrator_DispatchRecoversFromHandlerPanic(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	evt := protocol.Event{
		RunID:   "panic-run",
		Type:    protocol.EventWorkflowStarted,
		Payload: nil,
	}

	err := safeDispatch(orch, evt)
	if err == nil {
		t.Fatal("dispatchEvent returned nil on panicking handler; want error")
	}
	if !strings.Contains(err.Error(), "handler panic") {
		t.Fatalf("error %q does not contain \"handler panic\" — "+
			"likely the panic escaped dispatchEvent", err.Error())
	}

	js, _ := nc.JetStream()
	validDef := dag.WorkflowDef{
		Name: "post-panic-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, validDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, validDef.Name, defData)
	validEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "post-panic-run", defData,
	)
	goodData, mErr := validEvt.Marshal()
	if mErr != nil {
		t.Fatalf("marshal: %v", mErr)
	}
	mustPublish(t, js, validEvt.NATSSubject(), goodData,
		nats.MsgId(validEvt.NATSMsgID()))

	sub, err := js.PullSubscribe("task.task-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, fErr := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if fErr != nil {
		t.Fatalf("engine wedged after handler panic: %v", fErr)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task after post-panic run, got %d", len(msgs))
	}
}

// safeDispatch wraps Orchestrator.dispatchEvent in a recover so the test
// itself does not crash when production code is unfixed. Returns a
// distinct error string so the test can tell whether dispatchEvent
// recovered internally (post-fix, error contains "handler panic") or
// the panic escaped (pre-fix, error contains "wrapper caught").
func safeDispatch(o *Orchestrator, evt protocol.Event) (err error) {
	if o == nil {
		panic("safeDispatch: orchestrator must not be nil")
	}
	if evt.Type == "" {
		panic("safeDispatch: evt.Type must not be empty")
	}
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("test wrapper caught panic that escaped dispatchEvent")
		}
	}()
	return o.dispatchEvent(context.Background(), evt)
}
