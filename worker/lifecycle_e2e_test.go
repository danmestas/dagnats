// worker/lifecycle_e2e_test.go
// End-to-end tests joining a real orchestrator with a real worker.
// Verifies the complete lifecycle event sequence appears in history
// in the correct order with non-decreasing timestamps, and that
// retries via NAK increment the attempt counter both in events and
// in engine state.
// Methodology: register a workflow, start a run, register a worker
// that completes, drain the history stream end-to-end, assert the
// expected sequence and field values.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEndToEnd_LifecycleEventsFire(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "e2e-life", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "lifecycle-task", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	w := NewWorker(nc)
	w.Handle("lifecycle-task", func(tc TaskContext) error {
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-e2e1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	sub, err := js.SubscribeSync("history.run-e2e1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	type record struct {
		typ     protocol.EventType
		attempt int
		ts      time.Time
	}
	var seq []record
	want := map[protocol.EventType]bool{
		protocol.EventWorkflowStarted:   false,
		protocol.EventStepQueued:        false,
		protocol.EventStepStarted:       false,
		protocol.EventStepCompleted:     false,
		protocol.EventWorkflowCompleted: false,
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		seq = append(seq, record{evt.Type, evt.AttemptNumber, evt.Timestamp})
		if _, ok := want[evt.Type]; ok {
			want[evt.Type] = true
		}
		allSeen := true
		for _, v := range want {
			if !v {
				allSeen = false
				break
			}
		}
		if allSeen {
			break
		}
	}
	for typ, seen := range want {
		if !seen {
			t.Fatalf("expected event %q in history, missing. seq=%v", typ, seq)
		}
	}

	wantOrder := []protocol.EventType{
		protocol.EventWorkflowStarted,
		protocol.EventStepQueued,
		protocol.EventStepStarted,
		protocol.EventStepCompleted,
		protocol.EventWorkflowCompleted,
	}
	idx := 0
	for _, r := range seq {
		if idx < len(wantOrder) && r.typ == wantOrder[idx] {
			idx++
		}
	}
	if idx != len(wantOrder) {
		t.Fatalf("event order mismatch — got=%v, want subsequence=%v", seq, wantOrder)
	}

	var prev time.Time
	for _, r := range seq {
		switch r.typ {
		case protocol.EventStepQueued,
			protocol.EventStepStarted,
			protocol.EventStepCompleted:
			if !prev.IsZero() && r.ts.Before(prev) {
				t.Fatalf("timestamps not non-decreasing: %v before %v in seq=%v", r.ts, prev, seq)
			}
			prev = r.ts
		}
	}

	for _, r := range seq {
		if (r.typ == protocol.EventStepQueued || r.typ == protocol.EventStepStarted) && r.attempt != 1 {
			t.Fatalf("event %q AttemptNumber = %d, want 1", r.typ, r.attempt)
		}
	}
}

func TestEndToEnd_AttemptsVisibleDuringRun(t *testing.T) {
	// Methodology: this is the original #137 bug repro. Start a long
	// task (handler blocks on a channel), then sample run state during
	// the block. Assert Status=Running, Attempts=1 — proving the fix.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "e2e-attempts", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "long-task", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	handlerStarted := make(chan struct{}, 1)
	handlerProceed := make(chan struct{})
	w := NewWorker(nc)
	w.Handle("long-task", func(tc TaskContext) error {
		handlerStarted <- struct{}{}
		<-handlerProceed
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-att-vis", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked within 10s")
	}

	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(3 * time.Second)
	var observed dag.StepState
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "run-att-vis")
		if err == nil {
			observed = run.Steps["a"]
			if observed.Status == dag.StepStatusRunning && observed.Attempts == 1 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if observed.Status != dag.StepStatusRunning {
		t.Fatalf("Status = %v, want Running (#137 repro)", observed.Status)
	}
	if observed.Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1 (#137 repro)", observed.Attempts)
	}

	close(handlerProceed)
}

func TestEndToEnd_RetryViaNakIncrementsAttempts(t *testing.T) {
	// Methodology: handler errors on the first call, succeeds on
	// the second. Assert run final state has Attempts=2 and the
	// history stream contains both step.started events with distinct
	// AttemptNumber values.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "e2e-retry", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "retry-task", Type: dag.StepTypeNormal, Retries: 3},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	w := NewWorker(nc)
	w.Handle("retry-task", func(tc TaskContext) error {
		n := calls.Add(1)
		if n == 1 {
			return fmt.Errorf("transient on attempt %d", n)
		}
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rt", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	deadline := time.After(20 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("calls = %d, want 2 within 20s", calls.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

	store := enginepkg.NewSnapshotStore(jsNew)
	endDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(endDeadline) {
		run, err := store.Load(context.Background(), "run-rt")
		if err == nil && run.Steps["a"].Status == dag.StepStatusCompleted {
			if run.Steps["a"].Attempts != 2 {
				t.Fatalf("final Attempts = %d, want 2", run.Steps["a"].Attempts)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	sub, _ := js.SubscribeSync("history.run-rt", nats.DeliverAll())
	attemptsSeen := make(map[int]bool)
	histDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(histDeadline) && !(attemptsSeen[1] && attemptsSeen[2]) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepStarted {
			attemptsSeen[evt.AttemptNumber] = true
		}
	}
	if !attemptsSeen[1] {
		t.Fatal("expected step.started attempt 1, missing")
	}
	if !attemptsSeen[2] {
		t.Fatal("expected step.started attempt 2, missing")
	}
}
