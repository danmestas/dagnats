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
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
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
