// engine/orchestrator_helpers_test.go
// Unit tests for orchestrator helper functions and narrow collaborator
// seams: traceparent split/parse, handled-event classification, error
// stringification, completed/queued set tracking, loop-bound checks,
// load-run-and-def error paths, task-message building, step subject
// routing, step-def lookup, and parallel ready-task publication.
// Methodology: most cases are pure and assert directly against helper
// outputs (some iterate over a slice of inputs); publish-oriented cases
// use a real embedded NATS server. Each test gets its own server.

package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSplitTraceparent(t *testing.T) {
	// Methodology: unit test for the traceparent parsing utility.

	// Positive: valid W3C traceparent header.
	traceID, spanID, ok := splitTraceparent(
		"00-abc123-def456-01")
	if !ok {
		t.Fatal("expected ok=true for valid traceparent")
	}
	if traceID != "abc123" || spanID != "def456" {
		t.Fatalf("traceID=%q spanID=%q, want abc123/def456",
			traceID, spanID)
	}

	// Negative: invalid format (wrong version prefix).
	_, _, ok2 := splitTraceparent("01-abc-def-01")
	if ok2 {
		t.Fatal("expected ok=false for version != 00")
	}

	// Negative: too few segments.
	_, _, ok3 := splitTraceparent("00-abc-def")
	if ok3 {
		t.Fatal("expected ok=false for 3-segment string")
	}
}

func TestIsHandledEventType(t *testing.T) {
	// Methodology: unit test for event type filtering.

	// Positive: known types are handled.
	handled := []protocol.EventType{
		protocol.EventWorkflowStarted,
		protocol.EventStepCompleted,
		protocol.EventStepContinue,
		protocol.EventStepFailed,
		protocol.EventWorkflowSpawn,
		protocol.EventWorkflowCancelled,
	}
	for _, et := range handled {
		if !isHandledEventType(et) {
			t.Fatalf("%s should be handled", et)
		}
	}

	// Negative: unknown type is not handled.
	if isHandledEventType("foo.bar") {
		t.Fatal("foo.bar should not be handled")
	}
}

func TestErrString(t *testing.T) {
	// Positive: nil returns empty string.
	if errString(nil) != "" {
		t.Fatal("errString(nil) should be empty")
	}
	// Positive: non-nil returns error message.
	if errString(fmt.Errorf("boom")) != "boom" {
		t.Fatal("errString should return error text")
	}
}

func TestParseTraceparentFromHeader(t *testing.T) {
	// Methodology: unit test for traceparent parsing from NATS
	// message headers vs event field fallback.

	// Positive: header takes priority.
	msg := &nats.Msg{
		Header: nats.Header{
			"traceparent": {"00-tid1-sid1-01"},
		},
	}
	evt := &protocol.Event{TraceParent: "00-tid2-sid2-01"}
	traceID, spanID, ok := parseTraceparent(msg, evt)
	if !ok {
		t.Fatal("expected ok=true with header traceparent")
	}
	if traceID != "tid1" || spanID != "sid1" {
		t.Fatalf("header should take priority: got %s/%s",
			traceID, spanID)
	}

	// Positive: falls back to event field when no header.
	msg2 := &nats.Msg{}
	traceID2, spanID2, ok2 := parseTraceparent(msg2, evt)
	if !ok2 {
		t.Fatal("expected ok=true with event traceparent")
	}
	if traceID2 != "tid2" || spanID2 != "sid2" {
		t.Fatalf("should fall back to event: got %s/%s",
			traceID2, spanID2)
	}

	// Negative: neither header nor event has traceparent.
	msg3 := &nats.Msg{}
	evt3 := &protocol.Event{}
	_, _, ok3 := parseTraceparent(msg3, evt3)
	if ok3 {
		t.Fatal("expected ok=false when no traceparent")
	}
}

func TestCompletedSetAndQueuedSet(t *testing.T) {
	// Methodology: unit test for the set-building helpers.
	run := dag.WorkflowRun{
		RunID:      "test-sets",
		WorkflowID: "wf",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
			"b": {Status: dag.StepStatusSkipped},
			"c": {Status: dag.StepStatusQueued},
			"d": {Status: dag.StepStatusPending},
			"e": {Status: dag.StepStatusFailed},
			"f": {Status: dag.StepStatusRunning},
		},
	}

	completed := completedSet(run)
	// Positive: a and b are in completed set.
	if !completed["a"] || !completed["b"] {
		t.Fatal("a and b should be in completed set")
	}
	// Negative: c, d, e, f are NOT in completed set.
	if completed["c"] || completed["d"] ||
		completed["e"] || completed["f"] {
		t.Fatal("c/d/e/f should not be in completed set")
	}

	queued := queuedSet(run)
	// Positive: a, b, c, e, f are in queued set.
	for _, id := range []string{"a", "b", "c", "e", "f"} {
		if !queued[id] {
			t.Fatalf("%s should be in queued set", id)
		}
	}
	// Negative: d (pending) is NOT in queued set.
	if queued["d"] {
		t.Fatal("d (pending) should not be in queued set")
	}
}

func TestCheckLoopBoundsNoLoopConfig(t *testing.T) {
	// Methodology: unit test for checkLoopBounds edge cases.

	// Positive: nil Loop config returns false (no bounds).
	step := dag.StepDef{ID: "s", Task: "t"}
	exceeded, reason := checkLoopBounds(step, dag.StepState{})
	if exceeded {
		t.Fatal("nil Loop should not exceed bounds")
	}
	if reason != "" {
		t.Fatalf("reason should be empty, got %q", reason)
	}
}

func TestLoadRunAndDefMissingRun(t *testing.T) {
	// Methodology: verify loadRunAndDef returns error for
	// a run that doesn't exist in the snapshot store.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := NewOrchestrator(nc)

	// Positive: error returned for missing run.
	_, _, err := orch.loadRunAndDef(context.Background(), "nonexistent-run")
	if err == nil {
		t.Fatal("expected error for missing run")
	}

	// Positive: error message mentions the run ID.
	if !strings.Contains(err.Error(), "nonexistent-run") {
		t.Fatalf("error should mention run ID: %v", err)
	}
}

func TestLoadRunAndDefMissingWorkflowDef(t *testing.T) {
	// Methodology: snapshot exists but the workflow definition
	// is not registered. loadRunAndDef should return error.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	store := NewSnapshotStore(jsNew)
	run := dag.WorkflowRun{
		RunID:      "orphan-run",
		WorkflowID: "missing-def",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"s1": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)

	// Positive: error returned for missing workflow def.
	_, _, err = orch.loadRunAndDef(context.Background(), "orphan-run")
	if err == nil {
		t.Fatal("expected error for missing workflow def")
	}

	// Positive: error references the workflow ID.
	if !strings.Contains(err.Error(), "missing-def") {
		t.Fatalf("error should mention workflow ID: %v", err)
	}
}

func TestBuildTaskMsg(t *testing.T) {
	// Methodology: unit test for buildTaskMsg construction.
	msg := buildTaskMsg("task.foo.run-1", []byte("data"),
		"run-1.foo.queued")

	// Positive: subject is set.
	if msg.Subject != "task.foo.run-1" {
		t.Fatalf("Subject = %q, want task.foo.run-1",
			msg.Subject)
	}
	// Positive: dedup ID is set.
	if msg.Header.Get("Nats-Msg-Id") != "run-1.foo.queued" {
		t.Fatalf("Nats-Msg-Id = %q, want run-1.foo.queued",
			msg.Header.Get("Nats-Msg-Id"))
	}
}

func TestStepSubjectRouting(t *testing.T) {
	// Methodology: unit test for subject resolution.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	routes := map[dag.StepType]string{
		dag.StepTypeAgent: "agent.task",
	}
	orch := NewOrchestrator(nc,
		WithStepRoutes(routes))

	// Normal step -> default prefix.
	step := dag.StepDef{
		ID: "s1", Task: "my-task",
		Type: dag.StepTypeNormal,
	}
	subj := orch.publisher.stepSubject(step, "run-1")
	if subj != "task.my-task.run-1" {
		t.Fatalf("subject = %q, want task.my-task.run-1", subj)
	}

	// Agent step -> custom prefix.
	agentStep := dag.StepDef{
		ID: "s2", Task: "llm",
		Type: dag.StepTypeAgent,
	}
	agentSubj := orch.publisher.stepSubject(agentStep, "run-1")
	if agentSubj != "agent.task.llm.run-1" {
		t.Fatalf("subject = %q, want agent.task.llm.run-1",
			agentSubj)
	}
}

func TestFindStepDef(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name: "find-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "ta", Type: dag.StepTypeNormal},
			{ID: "b", Task: "tb", Type: dag.StepTypeNormal},
		},
	}

	// Positive: found step.
	step, found := findStepDef(wfDef, "b")
	if !found {
		t.Fatal("expected to find step b")
	}
	if step.Task != "tb" {
		t.Fatalf("step.Task = %q, want tb", step.Task)
	}

	// Negative: missing step.
	_, found2 := findStepDef(wfDef, "z")
	if found2 {
		t.Fatal("expected not to find step z")
	}
}

func TestPublishReadyTasksParallel(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	// Create a workflow with 5 independent entry steps (no deps)
	steps := make([]dag.StepDef, 5)
	for i := range steps {
		steps[i] = dag.StepDef{
			ID:   fmt.Sprintf("s%d", i),
			Task: fmt.Sprintf("task-%d", i),
			Type: dag.StepTypeNormal,
		}
	}
	wfDef := dag.WorkflowDef{
		Name: "parallel-wf", Version: "1", Steps: steps,
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-parallel", defData,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, evt.NATSSubject(), evtData, nats.MsgId(evt.NATSMsgID()))

	// All 5 tasks should appear
	for i := 0; i < 5; i++ {
		subject := fmt.Sprintf("task.task-%d.*", i)
		sub, err := js.PullSubscribe(subject, "",
			nats.BindStream("TASK_QUEUES"))
		if err != nil {
			t.Fatalf("PullSubscribe %s: %v", subject, err)
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
		if err != nil {
			t.Fatalf("Fetch task-%d failed: %v", i, err)
		}
		// Positive: each task published
		if len(msgs) != 1 {
			t.Fatalf("task-%d: expected 1 msg, got %d", i, len(msgs))
		}
	}
}
