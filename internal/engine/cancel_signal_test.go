// internal/engine/cancel_signal_test.go
// Pins #802 on the engine side: cancelling a run publishes the core NATS
// signal dagnats.cancel.{runID}, and what a still-running handler
// publishes afterwards is not accepted.
//
// Methodology: real embedded NATS and orchestrator. The test subscribes
// to the cancel subject before cancelling, then plays the late handler
// by publishing step.completed for the cancelled run. Negative space: a
// run that was not cancelled gets no signal, and the late completion
// leaves the step cancelled and the run cancelled. Waits are bounded.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCancel_PublishesSignalAndDropsLateEvents(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue(context.Background(), "workflow_defs")
	wfDef := dag.WorkflowDef{
		Name: "cancel-sig", Version: "1",
		Steps: []dag.StepDef{{ID: "s1", Task: "slow", Type: dag.StepTypeNormal}},
	}
	defData := mustMarshal(t, wfDef)
	for _, key := range []string{
		"cancel-sig", dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)),
	} {
		if _, err := defKV.Put(context.Background(), key, defData); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	signals := make(chan string, 4)
	sub, err := nc.Subscribe("dagnats.cancel.*", func(m *nats.Msg) {
		signals <- m.Subject
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	publishTestEvent(t, js, protocol.EventWorkflowStarted, "sig-run", defData)
	waitForRunStatus(t, orch.store, "sig-run", dag.RunStatusRunning, 5*time.Second)
	select {
	case s := <-signals:
		t.Fatalf("signal %q before any cancel", s)
	case <-time.After(200 * time.Millisecond):
	}

	publishTestEvent(t, js, protocol.EventWorkflowCancelled, "sig-run", nil)
	waitForRunStatus(t, orch.store, "sig-run", dag.RunStatusCancelled, 5*time.Second)
	select {
	case s := <-signals:
		if s != protocol.CancelSubject("sig-run") {
			t.Fatalf("signal subject = %q", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no cancel signal published")
	}

	// The handler that ignored the signal finishes anyway.
	publishTestEvent(t, js, protocol.EventStepCompleted, "sig-run", nil, "s1")
	time.Sleep(500 * time.Millisecond)
	run, err := orch.store.Load(context.Background(), "sig-run")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if run.Status != dag.RunStatusCancelled {
		t.Fatalf("run status = %v, want cancelled", run.Status)
	}
	if got := run.Steps["s1"].Status; got != dag.StepStatusCancelled {
		t.Fatalf("late completion moved step to %v, want cancelled", got)
	}
}

// publishTestEvent publishes a workflow event, or a step event when a
// step id is given, to the history stream.
func publishTestEvent(
	t *testing.T, js jetstream.JetStream, typ protocol.EventType,
	runID string, payload []byte, stepID ...string,
) {
	t.Helper()
	evt := protocol.NewWorkflowEvent(typ, runID, payload)
	if len(stepID) == 1 {
		evt = protocol.NewStepEvent(typ, runID, stepID[0], payload)
	}
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	if _, err := js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}
}
