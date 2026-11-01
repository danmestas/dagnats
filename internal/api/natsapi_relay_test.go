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
	"github.com/danmestas/dagnats/journal"
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
	conn := programConn(t, nc, program)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return natsutil.NewTracingPublisher(conn, js).WithRelay("api.p." + program + ".")
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
	refusals := map[string]struct {
		code    string
		attempt func() error
	}{
		"another program": {natsutil.RefusalNotYours, func() error {
			return publishEvent(t, relayClient(t, nc, "artifact"), startedEvent(runID, nonce))
		}},
		"wrong nonce": {natsutil.RefusalNotYours, func() error {
			return publishEvent(t, notify, startedEvent(runID, "not-the-nonce"))
		}},
		"no identity": {natsutil.RefusalIdentity, func() error {
			evt := protocol.NewStepEvent(protocol.EventStepCompleted, runID, "work", nil)
			return publishEvent(t, notify, &evt)
		}},
		"engine-only type": {natsutil.RefusalEventType, func() error {
			evt := startedEvent(runID, nonce)
			evt.Type = protocol.EventWorkflowCompleted
			return publishEvent(t, notify, evt)
		}},
		"subject for another run": {natsutil.RefusalSubject, func() error {
			evt := startedEvent(runID, nonce)
			msg := nats.NewMsg("history.someotherrun")
			msg.Header.Set(jetstream.MsgIDHeader, evt.NATSMsgID())
			_, err := notify.JSPublishMsgEvent(context.Background(), msg, evt)
			return err
		}},
		"foreign dedup id": {natsutil.RefusalMsgID, func() error {
			evt := startedEvent(runID, nonce)
			msg := nats.NewMsg(evt.NATSSubject())
			msg.Header.Set(jetstream.MsgIDHeader, "otherrun.work.step.started.1")
			_, err := notify.JSPublishMsgEvent(context.Background(), msg, evt)
			return err
		}},
	}
	for name, tc := range refusals {
		if got := refusalCode(tc.attempt()); got != tc.code {
			t.Errorf("%s: refusal %q, want %q", name, got, tc.code)
		}
	}
}

// refusalCode is the relay check that refused err, or "" when it was
// not a relay refusal.
func refusalCode(err error) string {
	var refusal *natsutil.RelayRefusal
	if errors.As(err, &refusal) {
		return refusal.Code
	}
	return ""
}

func journalRecord(runID, nonce string, expectLast uint64) *nats.Msg {
	return journalRecordWithID(runID, nonce, expectLast, runID+".work.1.0.1.1.0")
}

func journalRecordWithID(runID, nonce string, expectLast uint64, msgID string) *nats.Msg {
	msg := nats.NewMsg(natsutil.JournalSubject(runID, "work"))
	msg.Header.Set(jetstream.MsgIDHeader, msgID)
	msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmtUint(expectLast))
	msg.Header.Set(journal.HeaderType, "tool.result")
	msg.Header.Set(journal.HeaderAttempt, "1")
	msg.Header.Set(journal.HeaderIteration, "0")
	msg.Header.Set(journal.HeaderNonce, nonce)
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
	next := ack.Sequence
	refusals := map[string]struct {
		code string
		tp   *natsutil.TracingPublisher
		msg  *nats.Msg
	}{
		"another program": {natsutil.RefusalNotYours, relayClient(t, nc, "artifact"),
			journalRecord(runID, nonce, next)},
		"forged nonce": {natsutil.RefusalNotYours, notify, journalRecord(runID, "forged", next)},
		// H1: a dedup id another step would use next. Dedup is stream-wide,
		// so admitting it would make that step's real append a Duplicate.
		"foreign msg id": {natsutil.RefusalMsgID, notify,
			journalRecordWithID(runID, nonce, next, "otherrun.otherstep.1.0.1.1.0")},
		"wrong iteration": {natsutil.RefusalMsgID, notify,
			journalRecordWithID(runID, nonce, next, runID+".work.1.7.1.1.0")},
		"short msg id": {natsutil.RefusalMsgID, notify,
			journalRecordWithID(runID, nonce, next, runID+".work.1.0")},
	}
	for name, tc := range refusals {
		if _, err := tc.tp.JSPublishMsg(ctx, tc.msg); refusalCode(err) != tc.code {
			t.Errorf("%s: err %v, want refusal %q", name, err, tc.code)
		}
	}
	second := journalRecordWithID(runID, nonce, next, runID+".work.1.0.1.1.1")
	if _, err := notify.JSPublishMsg(ctx, second); err != nil {
		t.Fatalf("own second append at the right sequence: %v", err)
	}
}

func TestRelayJournalViewOnlyOwnStep(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subject := natsutil.JournalSubject(runID, "work")
	if _, found, err := notify.RelayView(ctx, subject, nonce, 0); err != nil || found {
		t.Fatalf("empty journal view = %v, %v", found, err)
	}
	ack, err := notify.JSPublishMsg(ctx, journalRecord(runID, nonce, 0))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	record, found, err := notify.RelayView(ctx, subject, nonce, 0)
	gotType := record.Header.Get(journal.HeaderType)
	if err != nil || !found || record.Seq != ack.Sequence ||
		string(record.Data) != `{"ok":true}` || gotType != "tool.result" {
		t.Fatalf("own tail view = %+v, %v, %v", record, found, err)
	}
	if from, found, err := notify.RelayView(ctx, subject, nonce, 1); err != nil || !found ||
		from.Seq != ack.Sequence {
		t.Fatalf("view from 1 = %+v, %v, %v", from, found, err)
	}
	_, _, err = relayClient(t, nc, "artifact").RelayView(ctx, subject, nonce, 0)
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("another program's view: %v, want not_yours", err)
	}
	_, _, err = notify.RelayView(ctx, subject, "forged", 0)
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("view with a forged nonce: %v, want not_yours", err)
	}
}

// TestRelayStaleNonceAfterRedispatch: once the engine dispatches the
// step again (a retry), the old dispatch's nonce no longer relays.
func TestRelayStaleNonceAfterRedispatch(t *testing.T) {
	nc := startScopedAPI(t)
	wb := dag.NewWorkflow("notify.retry")
	wb.Task("work", "notify.work")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	def.Steps[0].Retries = 1
	scopedRequest(t, nc, "api.p.notify.workflows.register", mustMarshal(t, def))
	runID := replyField(t, startScoped(t, nc, "notify", "notify.retry"), "run_id")
	oldNonce := waitNonce(t, nc, runID, "")
	notify := relayClient(t, nc, "notify")
	if err := publishEvent(t, notify, startedEvent(runID, oldNonce)); err != nil {
		t.Fatalf("started: %v", err)
	}
	failed := protocol.NewStepEvent(
		protocol.EventStepFailed, runID, "work", []byte(`{"error":"x"}`))
	iteration := 0
	failed.Iteration, failed.AttemptNumber, failed.DispatchNonce = &iteration, 1, oldNonce
	waitRunning(t, nc, runID)
	if err := publishEvent(t, notify, &failed); err != nil {
		t.Fatalf("failed: %v", err)
	}
	newNonce := waitNonce(t, nc, runID, oldNonce)
	err = publishEvent(t, notify, startedEvent(runID, oldNonce))
	if refusalCode(err) != natsutil.RefusalNotYours {
		t.Fatalf("stale nonce after redispatch: %v, want not_yours", err)
	}
	if err := publishEvent(t, notify, startedEvent(runID, newNonce)); err != nil {
		t.Fatalf("the new dispatch's nonce was refused: %v", err)
	}
}

func stepState(t *testing.T, nc *nats.Conn, runID string) dag.StepState {
	t.Helper()
	data := scopedRequest(t, nc, "api.runs.get", []byte(runID))
	var run dag.WorkflowRun
	if err := json.Unmarshal(data, &run); err != nil {
		return dag.StepState{}
	}
	return run.Steps["work"]
}

// waitNonce waits for the step to carry a dispatch nonce other than not.
func waitNonce(t *testing.T, nc *nats.Conn, runID, not string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if nonce := stepState(t, nc, runID).DispatchNonce; nonce != "" && nonce != not {
			return nonce
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s never got a new dispatch nonce", runID)
	return ""
}

func waitRunning(t *testing.T, nc *nats.Conn, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if stepState(t, nc, runID).Status == dag.StepStatusRunning {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s step never ran", runID)
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

// TestRelayedJournalReplaysThroughView: a program's journal (relayed)
// claims, appends, and on a redelivery claims again and reads back its
// earlier records through journal.view, the path Open takes when the
// tail is not empty.
func TestRelayedJournalReplaysThroughView(t *testing.T) {
	nc := startScopedAPI(t)
	runID, nonce := dispatchedStep(t, nc)
	notify := relayClient(t, nc, "notify")
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task := journal.Task{
		RunID: runID, StepID: "work", Iteration: 0, Attempt: 1,
		DispatchNonce: nonce, Delivered: 1, StreamSeq: 1,
	}
	first, err := journal.Open(ctx, js, notify, task)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := first.Append(ctx, "tool.result", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	task.Delivered = 2
	second, err := journal.Open(ctx, js, notify, task)
	if err != nil {
		t.Fatalf("redelivered claim: %v", err)
	}
	records := second.Records()
	if len(records) == 0 || string(records[len(records)-1].Data) != `{"n":1}` {
		t.Fatalf("redelivery read back %+v, want the earlier tool.result", records)
	}
	if second.Seq() <= first.Seq() {
		t.Fatalf("second claim seq %d not after %d", second.Seq(), first.Seq())
	}
}
