// internal/engine/orchestrator_history_dlq_test.go
// Integration tests for #508: bounding WORKFLOW_HISTORY redelivery and
// dead-lettering exhausted poison events. Methodology: red-green TDD
// against a real embedded NATS server. Every test injects a ms-scale
// WithHistoryRedeliverBackoff schedule so a poison event exhausts in well
// under a second instead of the ~5.6min production window (7 retries
// before dead-lettering) (TigerStyle: bounded test waits). Each test
// asserts both a positive property (the engine stays alive / a run
// reaches the expected state) and a negative property (dead-letter
// count is exact, not "at least").
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

// historyDLQTestSchedule is a 3-entry ms-scale schedule shared by the
// exhaustion tests below: MaxDeliver=3, worst-case exhaustion well under
// 100ms instead of the ~5.6min production window (7 retries before
// dead-lettering, per historyRedeliverSchedule).
var historyDLQTestSchedule = []time.Duration{
	10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond,
}

// countDeadLetters returns the current DEAD_LETTERS message count
// matching the given subject filter. Bounded helper for polling.
func countDeadLetters(
	t *testing.T, js jetstream.JetStream, subjectFilter string,
) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		t.Fatalf("DEAD_LETTERS stream: %v", err)
	}
	info, err := stream.Info(
		ctx, jetstream.WithSubjectFilter(subjectFilter),
	)
	if err != nil {
		t.Fatalf("DEAD_LETTERS info: %v", err)
	}
	return info.State.Msgs
}

// waitForDeadLetterCount polls countDeadLetters until it reaches target
// or the bounded deadline expires.
func waitForDeadLetterCount(
	t *testing.T, js jetstream.JetStream,
	subjectFilter string, target uint64, timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last uint64
	for time.Now().Before(deadline) {
		last = countDeadLetters(t, js, subjectFilter)
		if last == target {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf(
		"waitForDeadLetterCount: %q count = %d, want %d after %s",
		subjectFilter, last, target, timeout,
	)
}

// TestOrchestrator_UnmarshalFailurePoisonEventExhaustsToDeadLetter drives
// a raw non-JSON message onto the history stream (the unmarshal-failure
// call site in handleEventJS). It must redeliver until MaxDeliver, land
// exactly ONE DEAD_LETTERS entry, and the orchestrator must still process
// a subsequent valid workflow.started for a different run afterward
// (liveness — mirrors orchestrator_resilience_test.go).
func TestOrchestrator_UnmarshalFailurePoisonEventExhaustsToDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	orch := NewOrchestrator(
		nc, WithHistoryRedeliverBackoff(historyDLQTestSchedule),
	)
	orch.Start()
	defer orch.Stop()

	mustPublish(t, js, "history.poison-unmarshal-run", []byte("not json"))

	// Positive: exactly one dead-letter lands within the bounded window
	// ((3 deliveries) * 10ms schedule + processing slack).
	waitForDeadLetterCount(
		t, jsNew, "dead.orchestrator.>", 1, 3*time.Second,
	)

	// Negative: it stays at exactly one — no further redelivery after
	// exhaustion (Ack stopped it).
	time.Sleep(150 * time.Millisecond)
	got := countDeadLetters(t, jsNew, "dead.orchestrator.>")
	if got != 1 {
		t.Fatalf(
			"dead.orchestrator.> count = %d after settle, want exactly 1",
			got,
		)
	}

	// Liveness: the engine still processes a subsequent valid event for
	// a different run.
	validDef := dag.WorkflowDef{
		Name: "post-poison-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, validDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, validDef.Name, defData)
	validEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "post-poison-run", defData,
	)
	goodData, mErr := validEvt.Marshal()
	if mErr != nil {
		t.Fatalf("marshal good evt: %v", mErr)
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
		t.Fatalf("engine wedged after poison-event exhaustion: %v", fErr)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task after post-poison run, got %d", len(msgs))
	}
}

// TestOrchestrator_DispatchErrorPoisonEventExhaustsToDeadLetter drives a
// workflow.started event whose WorkflowDef payload deterministically
// fails to unmarshal inside resolveStartPayload (dispatchEvent error
// path, not the top-level protocol.UnmarshalEvent). Nothing is ever
// persisted for this run, so every redelivery re-fails identically.
// Asserts exactly one entry, correct headers, and no redelivery beyond
// exhaustion.
func TestOrchestrator_DispatchErrorPoisonEventExhaustsToDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	orch := NewOrchestrator(
		nc, WithHistoryRedeliverBackoff(historyDLQTestSchedule),
	)
	orch.Start()
	defer orch.Stop()

	const runID = "dispatch-fail-run"
	// workflow_def is present (non-nil) but malformed: a JSON string
	// where dag.WorkflowDef is expected. resolveStartPayload's
	// structured-shape branch fires and json.Unmarshal into wfDef
	// fails deterministically every delivery — no snapshot is ever
	// saved, so the idempotency guard never short-circuits a retry.
	payload := []byte(`{"workflow_def": "not-an-object"}`)
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
	data, mErr := evt.Marshal()
	if mErr != nil {
		t.Fatalf("marshal evt: %v", mErr)
	}
	mustPublish(t, js, evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))

	subjectFilter := "dead.orchestrator.>"
	waitForDeadLetterCount(t, jsNew, subjectFilter, 1, 3*time.Second)

	// Negative: exactly one, not more, after settling.
	time.Sleep(150 * time.Millisecond)
	got := countDeadLetters(t, jsNew, subjectFilter)
	if got != 1 {
		t.Fatalf(
			"%s count = %d after settle, want exactly 1", subjectFilter, got,
		)
	}

	// Header assertions on the entry.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, sErr := jsNew.Stream(ctx, "DEAD_LETTERS")
	if sErr != nil {
		t.Fatalf("DEAD_LETTERS stream: %v", sErr)
	}
	raw, gErr := stream.GetLastMsgForSubject(
		ctx, "dead.orchestrator.workflow-started."+runID,
	)
	if gErr != nil {
		t.Fatalf("GetLastMsgForSubject: %v", gErr)
	}
	if got := raw.Header.Get(HeaderDLQConsumer); got != DLQConsumerWorkflowHistory {
		t.Fatalf(
			"HeaderDLQConsumer = %q, want %q", got, DLQConsumerWorkflowHistory,
		)
	}
	if got := raw.Header.Get(HeaderDLQRunID); got != runID {
		t.Fatalf("HeaderDLQRunID = %q, want %q", got, runID)
	}
	if got := raw.Header.Get(HeaderDLQEventType); got !=
		string(protocol.EventWorkflowStarted) {
		t.Fatalf(
			"HeaderDLQEventType = %q, want %q",
			got, protocol.EventWorkflowStarted,
		)
	}
}

// TestOrchestrator_TransientFailureThenSuccessDoesNotDeadLetter publishes
// a step.queued event for a run that does not exist YET (handleStepQueued
// fails to load the run — a transient, ordering-driven failure), then
// publishes the workflow.started that creates the run before the first
// NAK's redelivery fires. The retry must then succeed (run exists), well
// under MaxDeliver, and DEAD_LETTERS must stay untouched.
func TestOrchestrator_TransientFailureThenSuccessDoesNotDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	// A wider schedule than the exhaustion tests: the workflow.started
	// publish + processing needs to complete before the first NAK's
	// redelivery fires, so schedule[0] must comfortably exceed that.
	schedule := []time.Duration{
		150 * time.Millisecond, 150 * time.Millisecond, 150 * time.Millisecond,
	}
	orch := NewOrchestrator(nc, WithHistoryRedeliverBackoff(schedule))
	orch.Start()
	defer orch.Stop()

	const runID = "transient-then-success-run"
	wfDef := dag.WorkflowDef{
		Name: "transient-then-success-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-transient", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, defData)

	before := countDeadLetters(t, jsNew, "dead.orchestrator.>")

	// Fails on delivery 1: the run does not exist yet.
	queuedEvt := protocol.NewStepEvent(
		protocol.EventStepQueued, runID, "s1", nil,
	)
	queuedData, qErr := queuedEvt.Marshal()
	if qErr != nil {
		t.Fatalf("marshal step.queued: %v", qErr)
	}
	mustPublish(t, js, queuedEvt.NATSSubject(), queuedData,
		nats.MsgId(queuedEvt.NATSMsgID()))

	// Well within schedule[0]=150ms: create the run before the retry.
	time.Sleep(30 * time.Millisecond)
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, defData,
	)
	startData, sErr := startEvt.Marshal()
	if sErr != nil {
		t.Fatalf("marshal workflow.started: %v", sErr)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Positive: the run reaches a non-failed state (Running, with step
	// s1 tracked) within a bounded window.
	deadline := time.Now().Add(5 * time.Second)
	var run dag.WorkflowRun
	var reached bool
	for time.Now().Before(deadline) {
		r, loadErr := orch.store.Load(context.Background(), runID)
		if loadErr == nil && r.Status == dag.RunStatusRunning {
			run = r
			reached = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !reached {
		t.Fatalf("run %q never reached RunStatusRunning within 5s", runID)
	}
	if run.Status == dag.RunStatusFailed {
		t.Fatalf("run %q reached Failed, want a non-failed state", runID)
	}

	// Negative: the transient failure never produced a dead-letter.
	time.Sleep(200 * time.Millisecond)
	after := countDeadLetters(t, jsNew, "dead.orchestrator.>")
	if after != before {
		t.Fatalf(
			"dead.orchestrator.> count changed %d -> %d; "+
				"transient-then-success must not dead-letter",
			before, after,
		)
	}
}

// TestOrchestrator_HistoryConsumerHasMaxDeliverAndBackOff asserts the
// consumer-level contract directly: MaxDeliver equals the configured
// schedule length (not the pre-fix -1), BackOff stays empty — the #508
// design decision to drive escalation entirely through
// nakOrDeadLetterHistory's explicit NAKs rather than ConsumerConfig.BackOff
// — and AckWait stays at NATS's 30s default regardless of the schedule
// configured, per the SETTLED #508 contract's Non-goal #6 (AckWait is
// explicitly out of scope; changing it is a separate, unreviewed
// decision). The schedule here is intentionally out of order so the
// MaxDeliver assertion catches a "last entry" implementation instead of
// a true max.
func TestOrchestrator_HistoryConsumerHasMaxDeliverAndBackOff(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	schedule := []time.Duration{
		1 * time.Second, 3 * time.Second, 2 * time.Second,
	}
	orch := NewOrchestrator(nc, WithHistoryRedeliverBackoff(schedule))
	orch.Start()
	defer orch.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, sErr := jsNew.Stream(ctx, "WORKFLOW_HISTORY")
	if sErr != nil {
		t.Fatalf("WORKFLOW_HISTORY stream: %v", sErr)
	}
	consumer, cErr := stream.Consumer(ctx, "orchestrator")
	if cErr != nil {
		t.Fatalf("orchestrator consumer: %v", cErr)
	}
	info, iErr := consumer.Info(ctx)
	if iErr != nil {
		t.Fatalf("consumer info: %v", iErr)
	}

	// Positive: MaxDeliver equals the configured schedule length.
	if info.Config.MaxDeliver != len(schedule) {
		t.Fatalf(
			"Config.MaxDeliver = %d, want %d",
			info.Config.MaxDeliver, len(schedule),
		)
	}

	// Negative: the pre-fix unbounded value must not survive.
	if info.Config.MaxDeliver == -1 {
		t.Fatal("Config.MaxDeliver = -1 (unbounded) — regression of #508")
	}

	// BackOff must stay empty — see the doc comment above.
	if len(info.Config.BackOff) != 0 {
		t.Fatalf(
			"Config.BackOff = %v, want empty (escalation lives in "+
				"nakOrDeadLetterHistory, not ConsumerConfig.BackOff)",
			info.Config.BackOff,
		)
	}

	// Positive: AckWait stays at NATS's 30s default — #508's contract
	// (Non-goal #6) explicitly does not touch AckWait.
	if info.Config.AckWait != 30*time.Second {
		t.Fatalf(
			"Config.AckWait = %s, want %s (NATS default; #508 must not "+
				"change AckWait)",
			info.Config.AckWait, 30*time.Second,
		)
	}

	// Negative: AckWait must not be derived from the schedule's longest
	// entry (3s here) — that would be the reintroduced-scope-creep this
	// test guards against.
	if info.Config.AckWait == 3*time.Second {
		t.Fatal(
			"Config.AckWait = 3s (schedule's longest entry) — AckWait " +
				"must stay at NATS's 30s default, not be derived from " +
				"historyRedeliverSchedule",
		)
	}
}

// maxDeliveriesAdvisorySubject is the NATS-server-emitted advisory
// published when a message's delivery count reaches a consumer's
// MaxDeliver while the message is still pending (unacknowledged). Acking a
// message removes it from the pending set immediately — that code path
// never checks MaxDeliver and never sends this advisory — so its presence
// or absence is a real, server-observed signal for whether the terminal
// delivery was NAK'd or Ack'd. The orchestrator's WORKFLOW_HISTORY
// consumer is always named "orchestrator" (natsutil.SetupAll).
const maxDeliveriesAdvisorySubject = "$JS.EVENT.ADVISORY.CONSUMER." +
	"MAX_DELIVERIES.WORKFLOW_HISTORY.orchestrator"

// TestOrchestrator_HistoryDeadLetterPublishFailureNaksNotAck is the red
// test for the #508 review finding: PublishHistoryDeadLetter used to be
// fire-and-forget (log the JSPublishMsg error and return), and
// nakOrDeadLetterHistory Ack'd the terminal delivery unconditionally —
// silently dropping the poison event with no durable DLQ record and no
// operator signal when DEAD_LETTERS was transiently unavailable. This
// test deletes DEAD_LETTERS so every dead-letter publish in it fails, then
// asserts the terminal delivery is NAK'd (preserved), not Ack'd (dropped).
func TestOrchestrator_HistoryDeadLetterPublishFailureNaksNotAck(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	// Force every PublishHistoryDeadLetter call in this test to fail:
	// once DEAD_LETTERS is gone, no stream captures "dead.>" and
	// JSPublishMsg returns a real "no responders" publish error.
	deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if dErr := jsNew.DeleteStream(deleteCtx, "DEAD_LETTERS"); dErr != nil {
		t.Fatalf("DeleteStream(DEAD_LETTERS): %v", dErr)
	}

	sub, subErr := nc.SubscribeSync(maxDeliveriesAdvisorySubject)
	if subErr != nil {
		t.Fatalf("SubscribeSync advisory: %v", subErr)
	}
	defer sub.Unsubscribe()

	orch := NewOrchestrator(
		nc, WithHistoryRedeliverBackoff(historyDLQTestSchedule),
	)
	orch.Start()
	defer orch.Stop()

	mustPublish(t, js, "history.poison-dlqfail-run", []byte("not json"))

	// Positive: the terminal delivery is NAK'd, not Ack'd. Bounded well
	// beyond (3 deliveries * 10ms schedule) plus the final NAK delay.
	if _, advErr := sub.NextMsg(3 * time.Second); advErr != nil {
		t.Fatalf(
			"MAX_DELIVERIES advisory not received: %v — the poison event "+
				"was silently Ack'd despite the DLQ publish failing "+
				"(#508: a dead-letter publish failure must NAK, not Ack)",
			advErr,
		)
	}

	// Negative: exactly one advisory — NAK-on-failure must not
	// reintroduce redelivery beyond MaxDeliver.
	if _, advErr := sub.NextMsg(200 * time.Millisecond); advErr == nil {
		t.Fatal(
			"received a second MAX_DELIVERIES advisory — NAK-on-failure " +
				"must not cause repeated redelivery past MaxDeliver",
		)
	}
}

// TestOrchestrator_HistoryDeadLetterPublishSuccessAcksNoAdvisory is the
// positive companion: when the DLQ publish SUCCEEDS at terminal delivery,
// the message must be Ack'd (no MAX_DELIVERIES advisory — Ack removes the
// message from pending before the advisory path ever runs) and exactly
// one DLQ record must land.
func TestOrchestrator_HistoryDeadLetterPublishSuccessAcksNoAdvisory(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	sub, subErr := nc.SubscribeSync(maxDeliveriesAdvisorySubject)
	if subErr != nil {
		t.Fatalf("SubscribeSync advisory: %v", subErr)
	}
	defer sub.Unsubscribe()

	orch := NewOrchestrator(
		nc, WithHistoryRedeliverBackoff(historyDLQTestSchedule),
	)
	orch.Start()
	defer orch.Stop()

	mustPublish(t, js, "history.poison-dlqsuccess-run", []byte("not json"))

	// Positive: exactly one DLQ record lands — the publish succeeded.
	waitForDeadLetterCount(t, jsNew, "dead.orchestrator.>", 1, 3*time.Second)

	// Negative: no MAX_DELIVERIES advisory ever fires — the message was
	// Ack'd, not left pending, so JetStream never runs the advisory path.
	if _, advErr := sub.NextMsg(200 * time.Millisecond); advErr == nil {
		t.Fatal(
			"received a MAX_DELIVERIES advisory for a successful DLQ " +
				"publish — the terminal delivery must be Ack'd, not NAK'd",
		)
	}
}
