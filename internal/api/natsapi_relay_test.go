// natsapi_relay_test.go
// Tests for the engine relay a program writes step events and journal
// records through (#885): api.p.<name>.events and
// api.p.<name>.journal.append. The relay publishes only for a step whose
// task is under "<name>." and only with the step's current dispatch
// nonce, so one program cannot forge another program's step events or
// journal records, and an event without dispatch identity is refused
// whatever legacy_step_events says. A JetStream rejection (the journal's
// compare-and-set) comes back typed.
// Methodology: real embedded NATS, orchestrator and NATSAPI. A run is
// started with no worker so its step stays dispatched; the test reads
// the step's nonce as root, the way a worker gets it in its task.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// dispatchedStep starts notify.relay (one step, task notify.work, no
// worker) and returns its run ID and the step's dispatch nonce.
func dispatchedStep(t *testing.T, nc *nats.Conn) (runID, nonce string) {
	t.Helper()
	wb := dag.NewWorkflow("notify.relay")
	wb.Task("work", "notify.work")
	registerScoped(t, nc, "notify", wb)
	runID = replyField(t, startScoped(t, nc, "notify", "notify.relay"), "run_id")
	if runID == "" {
		t.Fatal("no run started")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data := scopedRequest(t, nc, "api.runs.get", []byte(runID))
		var run dag.WorkflowRun
		if err := json.Unmarshal(data, &run); err == nil && run.Steps["work"].DispatchNonce != "" {
			return runID, run.Steps["work"].DispatchNonce
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("step of run %s was never dispatched", runID)
	return "", ""
}

func relayClient(t *testing.T, nc *nats.Conn, program string) *natsutil.TracingPublisher {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return natsutil.NewTracingPublisher(nc, js).WithRelay("api.p." + program + ".")
}

func startedEvent(runID, nonce string) *protocol.Event {
	evt := protocol.NewStepEvent(protocol.EventStepStarted, runID, "work", nil)
	evt.AttemptNumber = 1
	evt.DispatchNonce = nonce
	return &evt
}

func publishEvent(t *testing.T, tp *natsutil.TracingPublisher, evt *protocol.Event) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := nats.NewMsg(evt.NATSSubject())
	msg.Header.Set(jetstream.MsgIDHeader, evt.NATSMsgID())
	_, err := tp.JSPublishMsgEvent(ctx, msg, evt)
	return err
}

func TestRelayEventsOnlyForOwnStepWithItsNonce(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")

	if err := publishEvent(t, notify, startedEvent(runID, nonce)); err != nil {
		t.Fatalf("own step.started refused: %v", err)
	}
	refusals := map[string]func() error{
		"another program": func() error {
			return publishEvent(t, relayClient(t, nc, "artifact"), startedEvent(runID, nonce))
		},
		"wrong nonce": func() error {
			return publishEvent(t, notify, startedEvent(runID, "not-the-nonce"))
		},
		"no identity": func() error {
			evt := protocol.NewStepEvent(protocol.EventStepCompleted, runID, "work", nil)
			return publishEvent(t, notify, &evt)
		},
		"engine-only type": func() error {
			evt := startedEvent(runID, nonce)
			evt.Type = protocol.EventWorkflowCompleted
			return publishEvent(t, notify, evt)
		},
		"subject for another run": func() error {
			evt := startedEvent(runID, nonce)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			msg := nats.NewMsg("history.someotherrun")
			msg.Header.Set(jetstream.MsgIDHeader, evt.NATSMsgID())
			_, err := notify.JSPublishMsgEvent(ctx, msg, evt)
			return err
		},
	}
	for name, attempt := range refusals {
		if err := attempt(); err == nil {
			t.Errorf("%s: relayed", name)
		}
	}
}

func journalRecord(runID, nonce string, expectLast uint64) *nats.Msg {
	msg := nats.NewMsg(natsutil.JournalSubject(runID, "work"))
	msg.Header.Set(jetstream.MsgIDHeader, runID+".work.1.0.1.1.0")
	msg.Header.Set("Nats-Expected-Last-Subject-Sequence", fmtUint(expectLast))
	msg.Header.Set("Dagnats-Journal-Type", "tool.result")
	msg.Header.Set("Dagnats-Dispatch-Nonce", nonce)
	msg.Data = []byte(`{"ok":true}`)
	return msg
}

func fmtUint(v uint64) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestRelayJournalOnlyForOwnStepWithItsNonce(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := notify.JSPublishMsg(ctx, journalRecord(runID, nonce, 0))
	if err != nil || ack == nil || ack.Sequence == 0 || ack.Stream != natsutil.AgentJournalStream {
		t.Fatalf("own journal append = %+v, %v", ack, err)
	}
	// The compare-and-set still works through the relay, and its
	// rejection comes back as the JetStream API error the journal reads.
	_, err = notify.JSPublishMsg(ctx, journalRecord(runID, nonce, 0))
	var apiErr *jetstream.APIError
	wrongLast := errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
	if !wrongLast {
		t.Fatalf("stale CAS err = %v, want a wrong-last-sequence API error", err)
	}
	if _, err := relayClient(t, nc, "artifact").JSPublishMsg(ctx,
		journalRecord(runID, nonce, ack.Sequence)); err == nil {
		t.Fatal("another program appended to notify's journal")
	}
	forged := journalRecord(runID, "forged", ack.Sequence)
	if _, err := notify.JSPublishMsg(ctx, forged); err == nil {
		t.Fatal("a journal record with the wrong nonce was appended")
	}
	second := journalRecord(runID, nonce, ack.Sequence)
	if _, err := notify.JSPublishMsg(ctx, second); err != nil {
		t.Fatalf("own second append at the right sequence: %v", err)
	}
}

func TestRelayDropsUnlistedHeaders(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	evt := startedEvent(runID, nonce)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := nats.NewMsg(evt.NATSSubject())
	msg.Header.Set(jetstream.MsgIDHeader, evt.NATSMsgID())
	msg.Header.Set("Nats-Expected-Stream", "SOMETHING_ELSE")
	msg.Header.Set("Nats-Rollup", "sub")
	if _, err := notify.JSPublishMsgEvent(ctx, msg, evt); err != nil {
		t.Fatalf("relay with extra headers: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("history stream: %v", err)
	}
	stored, err := stream.GetLastMsgForSubject(ctx, evt.NATSSubject())
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	for key := range stored.Header {
		if strings.HasPrefix(key, "Nats-Expected") || key == "Nats-Rollup" {
			t.Fatalf("relay passed header %s through", key)
		}
	}
	if stored.Header.Get(jetstream.MsgIDHeader) != evt.NATSMsgID() {
		t.Fatal("relay dropped the dedup id")
	}
}

// TestRelayRefusesForgedSpawn: the engine spawns a child run from a
// workflow.spawn event on history.<run> (internal/engine/
// spawn_handlers.go). Relayed, it would start any workflow, another
// program's included, bypassing registration's confinement, so it is
// refused whatever child it names and with a valid nonce.
func TestRelayRefusesForgedSpawn(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	for _, child := range []string{"artifact.x", "notify.relay"} {
		payload, err := json.Marshal(map[string]any{
			"child_run_id":   "forged" + strings.ReplaceAll(child, ".", ""),
			"child_workflow": child, "parent_step_id": "work",
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		evt := protocol.NewStepEvent(protocol.EventWorkflowSpawn, runID, "work", payload)
		evt.DispatchNonce = nonce
		iteration := 0
		evt.Iteration, evt.AttemptNumber = &iteration, 1
		if err := publishEvent(t, notify, &evt); err == nil {
			t.Errorf("a forged workflow.spawn of %s was relayed", child)
		}
	}
}

// TestRelayRefusesEveryEngineEventType: only the four events a worker
// publishes pass; every other history event type is the engine's own
// (spawn, cancel, approval, wait, sleep, compensation, child results,
// planner materialization, input) and is refused even with identity
// and the step's current nonce.
func TestRelayRefusesEveryEngineEventType(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	engineTypes := []protocol.EventType{
		protocol.EventWorkflowStarted, protocol.EventStepQueued,
		protocol.EventStepMapStarted, protocol.EventStepMapCompleted,
		protocol.EventStepMapInstanceCompleted, protocol.EventStepSleepStarted,
		protocol.EventStepSleepCompleted, protocol.EventStepWaitStarted,
		protocol.EventStepWaitMatched, protocol.EventStepWaitTimeout,
		protocol.EventWorkflowCompleted, protocol.EventWorkflowFailed,
		protocol.EventWorkflowSpawn, protocol.EventWorkflowChildCompleted,
		protocol.EventWorkflowChildFailed, protocol.EventWorkflowCancelled,
		protocol.EventStepCancelled, protocol.EventCompensateStarted,
		protocol.EventCompensateStepCompleted, protocol.EventCompensateFailed,
		protocol.EventCompensateCompleted, protocol.EventApprovalRequested,
		protocol.EventApprovalGranted, protocol.EventApprovalRejected,
		protocol.EventApprovalExpired, protocol.EventPlannerMaterialized,
		protocol.EventStepInputQueued, protocol.EventStepWaitElapsed,
	}
	for _, eventType := range engineTypes {
		if relayedEventTypes[eventType] {
			t.Fatalf("%s is in the relay allowlist", eventType)
		}
		evt := protocol.NewStepEvent(eventType, runID, "work", nil)
		evt.DispatchNonce = nonce
		iteration := 0
		evt.Iteration, evt.AttemptNumber = &iteration, 1
		if err := publishEvent(t, notify, &evt); err == nil {
			t.Errorf("%s was relayed", eventType)
		}
	}
	if len(relayedEventTypes) != 4 {
		t.Fatalf("relay allowlist has %d types, want the 4 worker events", len(relayedEventTypes))
	}
}
