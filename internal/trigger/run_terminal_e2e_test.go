// internal/trigger/run_terminal_e2e_test.go
// Methodology: full engine+trigger round trip over a real embedded
// NATS server (repo convention — see internal/engine/admission_test.go
// and registrar_test.go). Each test starts its own server, registers
// workflow defs directly into workflow_defs KV, activates a
// run_terminal trigger via its registrar (bypassing the triggers-KV
// watcher, matching registrar_test.go's TestRegistrarsAreIdempotent
// pattern — the watcher's own KV plumbing is exercised elsewhere),
// completes the source run by publishing a raw StepCompleted event
// (no real worker needed for a one-step echo workflow, matching
// admission_skip_visibility_test.go), and polls workflow_runs KV with
// a bounded retry loop for the chained run.
//
// This test lives in package trigger (not engine) because
// internal/trigger already imports internal/engine (debounce.go) —
// the reverse import from engine into trigger would cycle.
package trigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// runTerminalPollMax bounds every polling loop below — CI must never
// hang on a bug that fails to produce the expected run.
const runTerminalPollMax = 50 // 50 * 100ms = 5s
const runTerminalPollInterval = 100 * time.Millisecond

// taskCountPollMax bounds countTaskMessages' Fetch loop -- a handful
// of iterations is ample since TASK_QUEUES never holds more than a
// couple of messages per subject in these tests.
const taskCountPollMax = 5

// oneStepEchoWorkflow returns a single-step workflow definition so
// completing it is one raw StepCompleted publish away, matching
// admission_skip_visibility_test.go's pattern.
func oneStepEchoWorkflow(name string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    name,
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
	}
}

// runTerminalTestHarness bundles the pieces every test in this file
// needs: an orchestrator, a trigger service, and the raw js/nc
// handles for publishing/reading fixtures.
type runTerminalTestHarness struct {
	t    *testing.T
	nc   *nats.Conn
	js   nats.JetStreamContext
	jsv2 jetstream.JetStream
	orch *engine.Orchestrator
	svc  *TriggerService
}

func newRunTerminalTestHarness(t *testing.T) *runTerminalTestHarness {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsv2, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	svc, err := NewTriggerService(nc, "test")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	t.Cleanup(svc.Stop)

	return &runTerminalTestHarness{
		t: t, nc: nc, js: js, jsv2: jsv2, orch: orch, svc: svc,
	}
}

// putWorkflowDef writes def to workflow_defs KV.
func (h *runTerminalTestHarness) putWorkflowDef(def dag.WorkflowDef) {
	h.t.Helper()
	kv, err := h.js.KeyValue("workflow_defs")
	if err != nil {
		h.t.Fatalf("workflow_defs KV: %v", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		h.t.Fatalf("marshal def: %v", err)
	}
	if _, err := kv.Put(def.Name, data); err != nil {
		h.t.Fatalf("put def: %v", err)
	}
}

// activateRunTerminal registers def with the run_terminal registrar
// directly — the production path (triggers KV + watcher) is exercised
// by service_test.go / registrar_test.go; this test isolates the
// engine+registrar interaction.
func (h *runTerminalTestHarness) activateRunTerminal(def TriggerDef) {
	h.t.Helper()
	if err := Validate(def); err != nil {
		h.t.Fatalf("Validate: %v", err)
	}
	reg := h.svc.registrars[kindRunTerminal]
	if err := reg.Activate(context.Background(), def); err != nil {
		h.t.Fatalf("Activate: %v", err)
	}
}

// startRun publishes a bare workflow.started event for wfDef under
// runID — the manual/direct-caller shape (resolveStartPayload's
// case 4), sufficient because this file only needs the SOURCE run to
// exist and complete; the run_terminal trigger under test is what
// starts the TARGET run.
func (h *runTerminalTestHarness) startRun(wfDef dag.WorkflowDef, runID string) {
	h.t.Helper()
	data, err := json.Marshal(wfDef)
	if err != nil {
		h.t.Fatalf("marshal wfDef: %v", err)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, data,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		h.t.Fatalf("marshal event: %v", err)
	}
	if _, err := h.js.Publish(
		evt.NATSSubject(), evtData, nats.MsgId(evt.NATSMsgID()),
	); err != nil {
		h.t.Fatalf("publish workflow.started: %v", err)
	}
}

// completeStep publishes a raw step.completed event for run/step —
// stands in for a worker finishing the run's only step, so the run
// reaches RunStatusCompleted without a real task executor.
func (h *runTerminalTestHarness) completeStep(runID, stepID string) {
	h.t.Helper()
	evt := protocol.NewStepEvent(
		protocol.EventStepCompleted, runID, stepID,
		[]byte(`{"status":"ok"}`),
	)
	data, err := evt.Marshal()
	if err != nil {
		h.t.Fatalf("marshal step event: %v", err)
	}
	if _, err := h.js.Publish(
		evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()),
	); err != nil {
		h.t.Fatalf("publish step event: %v", err)
	}
}

// waitRunStatus polls workflow_runs KV for runID to reach status,
// bounded by runTerminalPollMax attempts. Fails the test on timeout.
func (h *runTerminalTestHarness) waitRunStatus(
	runID string, status dag.RunStatus,
) dag.WorkflowRun {
	h.t.Helper()
	run, ok := h.pollRun(runID, func(r dag.WorkflowRun) bool {
		return r.Status == status
	})
	if !ok {
		h.t.Fatalf("run %q did not reach status %s within timeout",
			runID, status)
	}
	return run
}

// pollRun retries loadRun up to runTerminalPollMax times until match
// returns true, or gives up. Shared by waitRunStatus and the "confirm
// this run never starts" negative-space assertions below (called with
// a match func that always returns false, and the CALLER inspecting
// the ok==false / found-nothing outcome after the bounded wait).
func (h *runTerminalTestHarness) pollRun(
	runID string, match func(dag.WorkflowRun) bool,
) (dag.WorkflowRun, bool) {
	h.t.Helper()
	kv, err := h.jsv2.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		h.t.Fatalf("workflow_runs KV: %v", err)
	}
	for i := 0; i < runTerminalPollMax; i++ {
		entry, err := kv.Get(context.Background(), "run."+runID)
		if err == nil {
			var run dag.WorkflowRun
			if json.Unmarshal(entry.Value(), &run) == nil &&
				match(run) {
				return run, true
			}
		}
		time.Sleep(runTerminalPollInterval)
	}
	return dag.WorkflowRun{}, false
}

func TestRunTerminalTrigger_StartsTargetOnMatchingStatus(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-e2e-a")
	wfB := oneStepEchoWorkflow("rt-e2e-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	h.activateRunTerminal(TriggerDef{
		ID:         "rt-a-to-b",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfA.Name,
		},
	})

	h.startRun(wfA, "run-a-1")
	h.waitRunStatus("run-a-1", dag.RunStatusRunning)
	h.completeStep("run-a-1", "a")
	h.waitRunStatus("run-a-1", dag.RunStatusCompleted)

	// Positive: exactly one B run starts. Its run ID is freshly
	// minted (runid.New()), not predictable — the Nats-Msg-Id used
	// for dedup ("trig-rt-a-to-b-run-a-1") keys the PUBLISH, not the
	// run itself — so find it by scanning for the run whose Input
	// names run-a-1 as its source.
	bRun := h.waitForChainedRun(wfB.Name, "run-a-1")

	// Positive: input carries A's run_id/status/labels.
	var input runTerminalChainInput
	if err := json.Unmarshal(bRun.Input, &input); err != nil {
		t.Fatalf("unmarshal B input: %v", err)
	}
	if input.RunID != "run-a-1" {
		t.Fatalf("input.RunID = %q, want run-a-1", input.RunID)
	}
	if input.Status != "completed" {
		t.Fatalf("input.Status = %q, want completed", input.Status)
	}
	if input.WorkflowID != wfA.Name {
		t.Fatalf("input.WorkflowID = %q, want %q",
			input.WorkflowID, wfA.Name)
	}

	// Positive: TriggerDepth == 1 (source run's depth 0 + 1).
	if bRun.TriggerDepth != 1 {
		t.Fatalf("bRun.TriggerDepth = %d, want 1", bRun.TriggerDepth)
	}
}

// waitForChainedRun polls workflow_runs for the first run whose
// WorkflowID matches target and whose Input names sourceRunID.
// The chained run's ID IS deterministic (runTerminalChainRunID,
// #634 review round 2 — a SHA-256 of triggerID+sourceRunID, not a
// freshly minted one), but scanning by content is still simpler for
// callers than duplicating that hash formula here, and every test in
// this file already needs a scan-based lookup for the negative-space
// "confirm nothing fired" assertions anyway. Bounded by
// runTerminalPollMax * a small per-tick scan cost; workflow_runs
// stays tiny in these tests (single digits of runs), so a full-bucket
// scan per tick is acceptable here even though production code
// (reconciler.go) bounds/paginates this same style of scan for a much
// larger population.
func (h *runTerminalTestHarness) waitForChainedRun(
	targetWorkflow, sourceRunID string,
) dag.WorkflowRun {
	h.t.Helper()
	kv, err := h.jsv2.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		h.t.Fatalf("workflow_runs KV: %v", err)
	}
	for i := 0; i < runTerminalPollMax; i++ {
		keys, _ := kv.Keys(context.Background())
		for _, key := range keys {
			entry, err := kv.Get(context.Background(), key)
			if err != nil {
				continue
			}
			var run dag.WorkflowRun
			if json.Unmarshal(entry.Value(), &run) != nil {
				continue
			}
			if run.WorkflowID != targetWorkflow {
				continue
			}
			var input runTerminalChainInput
			if json.Unmarshal(run.Input, &input) != nil {
				continue
			}
			if input.RunID == sourceRunID {
				return run
			}
		}
		time.Sleep(runTerminalPollInterval)
	}
	h.t.Fatalf("no chained run of %q found for source %q within timeout",
		targetWorkflow, sourceRunID)
	return dag.WorkflowRun{}
}

// countChainedRuns counts runs of targetWorkflow whose Input names
// sourceRunID — used by the dedup and no-fire assertions below, which
// need a stable count rather than a single match.
func (h *runTerminalTestHarness) countChainedRuns(
	targetWorkflow, sourceRunID string,
) int {
	h.t.Helper()
	kv, err := h.jsv2.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		h.t.Fatalf("workflow_runs KV: %v", err)
	}
	keys, _ := kv.Keys(context.Background())
	count := 0
	for _, key := range keys {
		entry, err := kv.Get(context.Background(), key)
		if err != nil {
			continue
		}
		var run dag.WorkflowRun
		if json.Unmarshal(entry.Value(), &run) != nil {
			continue
		}
		if run.WorkflowID != targetWorkflow {
			continue
		}
		var input runTerminalChainInput
		if json.Unmarshal(run.Input, &input) != nil {
			continue
		}
		if input.RunID == sourceRunID {
			count++
		}
	}
	return count
}

// countTaskMessages counts every message ever published to subject on
// TASK_QUEUES (#634 review round 3: dedup-by-run-count is not enough
// to catch a healRun that double-DISPATCHES a single step's task --
// this counts at the task-message level instead). Bounded: stops
// after an empty Fetch, capped by taskCountPollMax iterations so a
// genuine bug (an unbounded stream of duplicates) cannot hang the
// test. Does not Ack -- these are throwaway ephemeral pulls in a test
// server with no real worker, so leaving messages unacked has no
// side effect on anything else in the test.
func (h *runTerminalTestHarness) countTaskMessages(subject string) int {
	h.t.Helper()
	sub, err := h.js.PullSubscribe(
		subject, "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		h.t.Fatalf("PullSubscribe %q: %v", subject, err)
	}
	// Unsubscribe (not just let it go out of scope) deletes the
	// underlying JetStream pull consumer immediately: a work-queue
	// stream allows only ONE filtered consumer per exact subject at a
	// time, and this helper is called more than once per test against
	// the SAME subject (a before/after count around one redelivery).
	// Messages fetched here are never Acked, so they remain
	// redeliverable to the NEXT ephemeral consumer this creates --
	// nothing is lost between calls.
	defer func() { _ = sub.Unsubscribe() }()
	count := 0
	for i := 0; i < taskCountPollMax; i++ {
		msgs, fetchErr := sub.Fetch(10, nats.MaxWait(300*time.Millisecond))
		if fetchErr != nil {
			break // nats.ErrTimeout (or similar): no more messages
		}
		count += len(msgs)
		if len(msgs) == 0 {
			break
		}
	}
	return count
}

// waitStepStatus polls workflow_runs for runID until step stepID
// reaches status, bounded by runTerminalPollMax attempts. Fails the
// test on timeout -- used to synchronize a test with "the step has
// been dispatched (Queued) but not yet completed" before exercising
// a redelivery against that exact window.
func (h *runTerminalTestHarness) waitStepStatus(
	runID, stepID string, status dag.StepStatus,
) dag.WorkflowRun {
	h.t.Helper()
	run, ok := h.pollRun(runID, func(r dag.WorkflowRun) bool {
		state, exists := r.Steps[stepID]
		return exists && state.Status == status
	})
	if !ok {
		h.t.Fatalf("run %q step %q did not reach status %s within timeout",
			runID, stepID, status)
	}
	return run
}

func TestRunTerminalTrigger_RedeliveryDedups(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-dedup-a")
	wfB := oneStepEchoWorkflow("rt-dedup-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	def := TriggerDef{
		ID:         "rt-dedup",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfA.Name,
		},
	}
	h.activateRunTerminal(def)

	h.startRun(wfA, "run-dedup-1")
	h.waitRunStatus("run-dedup-1", dag.RunStatusRunning)
	h.completeStep("run-dedup-1", "a")
	h.waitRunStatus("run-dedup-1", dag.RunStatusCompleted)
	h.waitForChainedRun(wfB.Name, "run-dedup-1")

	// Redeliver the same RunEvent the trigger already consumed by
	// republishing event.run.* directly with the SAME subject/body
	// finalizeRun produced — simulating the durable consumer
	// redelivering after a crash, the exact scenario Nats-Msg-Id
	// dedup on the workflow.started publish must absorb.
	evt := protocol.RunEvent{
		Type:       protocol.RunEventCompleted,
		RunID:      "run-dedup-1",
		WorkflowID: wfA.Name,
		Status:     "completed",
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal RunEvent: %v", err)
	}
	subject := "event.run." + wfA.Name + ".run-dedup-1.completed"
	if _, err := h.js.Publish(subject, data); err != nil {
		t.Fatalf("republish RunEvent: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Negative: still exactly one B run for this source.
	if count := h.countChainedRuns(wfB.Name, "run-dedup-1"); count != 1 {
		t.Fatalf("chained B run count = %d, want 1 (dedup)", count)
	}
}

func TestRunTerminalTrigger_StatusFilterOnlyFailed(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-filter-a")
	wfB := oneStepEchoWorkflow("rt-filter-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	h.activateRunTerminal(TriggerDef{
		ID:         "rt-failed-only",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfA.Name,
			Statuses: []string{"failed"},
		},
	})

	// A completes (not failed) — must NOT fire.
	h.startRun(wfA, "run-filter-1")
	h.waitRunStatus("run-filter-1", dag.RunStatusRunning)
	h.completeStep("run-filter-1", "a")
	h.waitRunStatus("run-filter-1", dag.RunStatusCompleted)
	time.Sleep(500 * time.Millisecond)
	if count := h.countChainedRuns(wfB.Name, "run-filter-1"); count != 0 {
		t.Fatalf("completed-only run fired a filtered trigger: count=%d",
			count)
	}

	// A2 fails — must fire.
	h.startRun(wfA, "run-filter-2")
	h.waitRunStatus("run-filter-2", dag.RunStatusRunning)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "run-filter-2", "a",
		[]byte(`{"error":"boom"}`),
	)
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal fail event: %v", err)
	}
	if _, err := h.js.Publish(
		failEvt.NATSSubject(), failData, nats.MsgId(failEvt.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish fail event: %v", err)
	}
	h.waitRunStatus("run-filter-2", dag.RunStatusFailed)
	h.waitForChainedRun(wfB.Name, "run-filter-2")
}

func TestRunTerminalTrigger_DepthCapStopsChain(t *testing.T) {
	// Lower the cap for this test only so a 4-workflow chain (A→B→
	// C→D) is enough to exercise it without an 8-hop fixture.
	orig := TriggerDepthMax
	TriggerDepthMax = 2
	t.Cleanup(func() { TriggerDepthMax = orig })

	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-depth-a")
	wfB := oneStepEchoWorkflow("rt-depth-b")
	wfC := oneStepEchoWorkflow("rt-depth-c")
	wfD := oneStepEchoWorkflow("rt-depth-d")
	for _, wf := range []dag.WorkflowDef{wfA, wfB, wfC, wfD} {
		h.putWorkflowDef(wf)
	}

	h.activateRunTerminal(TriggerDef{
		ID: "rt-depth-a-b", WorkflowID: wfB.Name, Enabled: true,
		RunTerminal: &RunTerminalConfig{Workflow: wfA.Name},
	})
	h.activateRunTerminal(TriggerDef{
		ID: "rt-depth-b-c", WorkflowID: wfC.Name, Enabled: true,
		RunTerminal: &RunTerminalConfig{Workflow: wfB.Name},
	})
	h.activateRunTerminal(TriggerDef{
		ID: "rt-depth-c-d", WorkflowID: wfD.Name, Enabled: true,
		RunTerminal: &RunTerminalConfig{Workflow: wfC.Name},
	})

	h.startRun(wfA, "run-depth-a")
	h.waitRunStatus("run-depth-a", dag.RunStatusRunning)
	h.completeStep("run-depth-a", "a")
	h.waitRunStatus("run-depth-a", dag.RunStatusCompleted)

	// B starts (depth 1) and must be completed to let the chain
	// continue toward C.
	bRun := h.waitForChainedRun(wfB.Name, "run-depth-a")
	if bRun.TriggerDepth != 1 {
		t.Fatalf("bRun.TriggerDepth = %d, want 1", bRun.TriggerDepth)
	}
	h.completeStep(bRun.RunID, "a")
	h.waitRunStatus(bRun.RunID, dag.RunStatusCompleted)

	// C starts (depth 2 == TriggerDepthMax, still allowed).
	cRun := h.waitForChainedRun(wfC.Name, bRun.RunID)
	if cRun.TriggerDepth != 2 {
		t.Fatalf("cRun.TriggerDepth = %d, want 2", cRun.TriggerDepth)
	}
	h.completeStep(cRun.RunID, "a")
	h.waitRunStatus(cRun.RunID, dag.RunStatusCompleted)

	// D must NEVER start: depth would be 3 > TriggerDepthMax(2).
	time.Sleep(500 * time.Millisecond)
	if count := h.countChainedRuns(wfD.Name, cRun.RunID); count != 0 {
		t.Fatalf("D started despite depth cap: count=%d", count)
	}
}

// TestRunTerminalTrigger_DeliverNewPolicyIgnoresHistoricalEvents
// covers #634 review Blocker 1: a NEW durable consumer must not
// replay EVENTS' retention window and start one target run per
// historic terminal event that happened before the trigger existed.
func TestRunTerminalTrigger_DeliverNewPolicyIgnoresHistoricalEvents(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-new-a")
	wfB := oneStepEchoWorkflow("rt-new-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	// Three source runs complete BEFORE the trigger is ever
	// registered.
	historical := []string{"run-new-1", "run-new-2", "run-new-3"}
	for _, runID := range historical {
		h.startRun(wfA, runID)
		h.waitRunStatus(runID, dag.RunStatusRunning)
		h.completeStep(runID, "a")
		h.waitRunStatus(runID, dag.RunStatusCompleted)
	}

	h.activateRunTerminal(TriggerDef{
		ID:         "rt-new-trigger",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfA.Name,
		},
	})
	time.Sleep(500 * time.Millisecond)

	// Negative: none of the historical completions fired the trigger.
	for _, runID := range historical {
		if count := h.countChainedRuns(wfB.Name, runID); count != 0 {
			t.Fatalf(
				"historical run %s incorrectly fired the trigger: count=%d",
				runID, count,
			)
		}
	}

	// Positive: a run completing AFTER registration does fire.
	h.startRun(wfA, "run-new-4")
	h.waitRunStatus("run-new-4", dag.RunStatusRunning)
	h.completeStep("run-new-4", "a")
	h.waitRunStatus("run-new-4", dag.RunStatusCompleted)
	h.waitForChainedRun(wfB.Name, "run-new-4")
}

// TestRunTerminalTrigger_SurvivesLateRedeliveryPastDedupWindow covers
// #634 review Blocker 2: dedup must not depend on WORKFLOW_HISTORY's
// short Nats-Msg-Id Duplicates window (natsutil/conn.go, a few
// seconds) — a crash/restart gap is minutes. This test republishes
// the identical source RunEvent after sleeping PAST that window and
// asserts the fix (deterministic run ID + SnapshotStore.CreateSnapshot)
// still collapses to exactly one target run, not two.
//
// #634 review round 3 strengthened this: it now ALSO counts task
// MESSAGES on TASK_QUEUES for B's step, not just B's run count. Round
// 2's healRun additionally redispatched a step already Queued,
// reasoning the task publish's Nats-Msg-Id would dedup a re-send —
// but TASK_QUEUES sets no Duplicates window (natsutil/conn.go), so
// past the JetStream server default (2 minutes) that redispatch was
// NOT deduped: it landed as a genuine second task message for the
// same step, which two workers could both pick up and execute. A
// run-count-only assertion could not see that bug (the run count
// stays 1 regardless — the duplication was at the task level). B's
// step is never completed in this test (there is no worker here), so
// it stays Queued for the whole test, exactly the state a redelivery
// arriving after the original dispatch would find it in.
func TestRunTerminalTrigger_SurvivesLateRedeliveryPastDedupWindow(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-late-a")
	wfB := oneStepEchoWorkflow("rt-late-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	h.activateRunTerminal(TriggerDef{
		ID:         "rt-late-trigger",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfA.Name,
		},
	})

	h.startRun(wfA, "run-late-1")
	h.waitRunStatus("run-late-1", dag.RunStatusRunning)
	h.completeStep("run-late-1", "a")
	h.waitRunStatus("run-late-1", dag.RunStatusCompleted)
	bRun := h.waitForChainedRun(wfB.Name, "run-late-1")
	h.waitStepStatus(bRun.RunID, "a", dag.StepStatusQueued)

	// Positive (sanity): exactly one task message before redelivery.
	taskSubject := "task.echo." + bRun.RunID
	if count := h.countTaskMessages(taskSubject); count != 1 {
		t.Fatalf("task message count before redelivery = %d, want 1", count)
	}

	// Sleep past WORKFLOW_HISTORY's Duplicates window (a few seconds)
	// before republishing — proves the fix does not rely on that
	// window at all.
	time.Sleep(6 * time.Second)

	evt := protocol.RunEvent{
		Type:       protocol.RunEventCompleted,
		RunID:      "run-late-1",
		WorkflowID: wfA.Name,
		Status:     "completed",
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal RunEvent: %v", err)
	}
	subject := "event.run." + wfA.Name + ".run-late-1.completed"
	if _, err := h.js.Publish(subject, data); err != nil {
		t.Fatalf("republish RunEvent: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Negative: still exactly one B run for this source, even past
	// the dedup window.
	if count := h.countChainedRuns(wfB.Name, "run-late-1"); count != 1 {
		t.Fatalf(
			"chained B run count = %d, want 1 (dedup past window)",
			count,
		)
	}
	// Negative: still exactly ONE task message for B's step across
	// the heal, not two — this is the assertion that actually catches
	// the round-2 redispatch bug.
	if count := h.countTaskMessages(taskSubject); count != 1 {
		t.Fatalf(
			"task message count after heal = %d, want 1 (no duplicate dispatch)",
			count,
		)
	}
}

// TestRunTerminalTrigger_HealDoesNotRedispatchAlreadyQueuedStep covers
// #634 review round 3's second required case: a redelivery of the
// source RunEvent that arrives while B's step is ALREADY Queued
// (dispatched once, not yet completed — no crash, no restart, just
// an ordinary redelivery racing an in-flight step) must not produce a
// second task message either. This is deliberately a SHORT-window
// redelivery (no 6s sleep) — the round-2 bug was not specific to
// crash/restart timing; the exact same over-eager redispatch would
// have double-dispatched a step sitting in an ordinary backlog too.
func TestRunTerminalTrigger_HealDoesNotRedispatchAlreadyQueuedStep(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-queued-a")
	wfB := oneStepEchoWorkflow("rt-queued-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	h.activateRunTerminal(TriggerDef{
		ID:         "rt-queued-trigger",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfA.Name,
		},
	})

	h.startRun(wfA, "run-queued-1")
	h.waitRunStatus("run-queued-1", dag.RunStatusRunning)
	h.completeStep("run-queued-1", "a")
	h.waitRunStatus("run-queued-1", dag.RunStatusCompleted)

	bRun := h.waitForChainedRun(wfB.Name, "run-queued-1")
	// Synchronize on "dispatched but not completed" — B's step "a" is
	// never completed in this test, so once Queued it stays Queued.
	h.waitStepStatus(bRun.RunID, "a", dag.StepStatusQueued)

	taskSubject := "task.echo." + bRun.RunID
	if count := h.countTaskMessages(taskSubject); count != 1 {
		t.Fatalf("task message count before redelivery = %d, want 1", count)
	}

	// Redeliver the SAME source RunEvent immediately (same
	// triggerID+sourceRunID -> same deterministic B run ID). healRun
	// finds B already Running with step "a" Queued and must do
	// nothing to it — resolveReadySteps only re-selects Pending steps.
	evt := protocol.RunEvent{
		Type:       protocol.RunEventCompleted,
		RunID:      "run-queued-1",
		WorkflowID: wfA.Name,
		Status:     "completed",
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal RunEvent: %v", err)
	}
	subject := "event.run." + wfA.Name + ".run-queued-1.completed"
	if _, err := h.js.Publish(subject, data); err != nil {
		t.Fatalf("republish RunEvent: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Negative: still exactly one B run.
	if count := h.countChainedRuns(wfB.Name, "run-queued-1"); count != 1 {
		t.Fatalf("chained B run count = %d, want 1", count)
	}
	// Negative: still exactly one task message for B's step — the
	// core assertion this test exists for.
	if count := h.countTaskMessages(taskSubject); count != 1 {
		t.Fatalf(
			"task message count after redelivery = %d, want 1 "+
				"(heal must not redispatch an already-Queued step)",
			count,
		)
	}
}

// TestRunTerminalTrigger_ExactWorkflowMatchNotSanitizedSubject covers
// #634 review Major 4: the FilterSubject is built from a SANITIZED
// workflow token, so two distinct workflow names that sanitize to the
// same token share a subject. A trigger watching one of them must NOT
// fire on the other reaching the same terminal status.
func TestRunTerminalTrigger_ExactWorkflowMatchNotSanitizedSubject(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	// Both sanitize to "rt_collide_a".
	wfDot := oneStepEchoWorkflow("rt.collide.a")
	wfUnd := oneStepEchoWorkflow("rt_collide_a")
	wfB := oneStepEchoWorkflow("rt-collide-b")
	h.putWorkflowDef(wfDot)
	h.putWorkflowDef(wfUnd)
	h.putWorkflowDef(wfB)

	h.activateRunTerminal(TriggerDef{
		ID:         "rt-collide-trigger",
		WorkflowID: wfB.Name,
		Enabled:    true,
		RunTerminal: &RunTerminalConfig{
			Workflow: wfDot.Name, // watches "rt.collide.a" exactly
		},
	})

	// The subject-colliding workflow completes — must NOT fire.
	h.startRun(wfUnd, "run-collide-und")
	h.waitRunStatus("run-collide-und", dag.RunStatusRunning)
	h.completeStep("run-collide-und", "a")
	h.waitRunStatus("run-collide-und", dag.RunStatusCompleted)
	time.Sleep(500 * time.Millisecond)
	if count := h.countChainedRuns(wfB.Name, "run-collide-und"); count != 0 {
		t.Fatalf(
			"subject-collision workflow incorrectly fired the trigger: count=%d",
			count,
		)
	}

	// The ACTUAL watched workflow completes — must fire.
	h.startRun(wfDot, "run-collide-dot")
	h.waitRunStatus("run-collide-dot", dag.RunStatusRunning)
	h.completeStep("run-collide-dot", "a")
	h.waitRunStatus("run-collide-dot", dag.RunStatusCompleted)
	h.waitForChainedRun(wfB.Name, "run-collide-dot")
}

// TestRunTerminalTrigger_DurableNamesDontCollideForSimilarIDs covers
// #634 review Major 5: two trigger IDs that sanitize to the same
// subject token ("rt.durable.x" and "rt_durable_x") must get DISTINCT
// durable consumer names, or the second Activate's
// CreateOrUpdateConsumer silently rewrites the first trigger's
// consumer and only one of the two ever actually fires.
func TestRunTerminalTrigger_DurableNamesDontCollideForSimilarIDs(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-durable-src")
	wfB1 := oneStepEchoWorkflow("rt-durable-b1")
	wfB2 := oneStepEchoWorkflow("rt-durable-b2")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB1)
	h.putWorkflowDef(wfB2)

	h.activateRunTerminal(TriggerDef{
		ID: "rt.durable.x", WorkflowID: wfB1.Name, Enabled: true,
		RunTerminal: &RunTerminalConfig{Workflow: wfA.Name},
	})
	h.activateRunTerminal(TriggerDef{
		ID: "rt_durable_x", WorkflowID: wfB2.Name, Enabled: true,
		RunTerminal: &RunTerminalConfig{Workflow: wfA.Name},
	})

	h.startRun(wfA, "run-durable-src")
	h.waitRunStatus("run-durable-src", dag.RunStatusRunning)
	h.completeStep("run-durable-src", "a")
	h.waitRunStatus("run-durable-src", dag.RunStatusCompleted)

	// Positive: BOTH triggers fire from the same source completion —
	// if the durable names collided, only one would.
	h.waitForChainedRun(wfB1.Name, "run-durable-src")
	h.waitForChainedRun(wfB2.Name, "run-durable-src")
}

// TestRunTerminalTrigger_DeleteRemovesDurableConsumer covers #634
// review Major 6: a genuine KV delete of the trigger must remove its
// durable EVENTS consumer, not just stop the in-process
// ConsumeContext (Deactivate alone), or the consumer accumulates on
// the server forever.
func TestRunTerminalTrigger_DeleteRemovesDurableConsumer(t *testing.T) {
	h := newRunTerminalTestHarness(t)

	wfA := oneStepEchoWorkflow("rt-delete-a")
	wfB := oneStepEchoWorkflow("rt-delete-b")
	h.putWorkflowDef(wfA)
	h.putWorkflowDef(wfB)

	id := "rt-delete-trigger"
	h.activateRunTerminal(TriggerDef{
		ID: id, WorkflowID: wfB.Name, Enabled: true,
		RunTerminal: &RunTerminalConfig{Workflow: wfA.Name},
	})

	stream, err := h.jsv2.Stream(context.Background(), "EVENTS")
	if err != nil {
		t.Fatalf("stream EVENTS: %v", err)
	}
	durable := runTerminalDurableName(id)

	// Positive (sanity): the consumer exists right after Activate.
	if _, err := stream.Consumer(context.Background(), durable); err != nil {
		t.Fatalf("consumer %s should exist after Activate: %v", durable, err)
	}

	// Simulate the real KV-delete path: permanent=true.
	if err := h.svc.removeTrigger(id, true); err != nil {
		t.Fatalf("removeTrigger: %v", err)
	}

	// Negative: the durable consumer is gone, not just deactivated.
	if _, err := stream.Consumer(context.Background(), durable); err == nil {
		t.Fatalf("consumer %s should be deleted after a permanent removeTrigger", durable)
	}
}
