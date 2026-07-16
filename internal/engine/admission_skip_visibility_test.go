// internal/engine/admission_skip_visibility_test.go
// Methodology: exercises the singleton-skip path end-to-end over a
// real embedded NATS server (repo convention -- see admission_test.go).
// Each test starts its own server (no sharing), settles state with a
// bounded sleep after publish, and reads results with bounded
// nats.MaxWait fetches rather than unbounded waits. Covers #502:
// a skipped run must remain visible (terminal, loadable, reason
// naming the holder) instead of vanishing with no persisted record.
package engine

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

// skipVisibilityWorkflowDef returns a singleton-skip workflow with one
// step so admitted runs stay Running (no worker to complete "a").
func skipVisibilityWorkflowDef() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    "singleton-skip-visibility-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
		},
		Singleton: &dag.SingletonConfig{
			Mode: dag.SingletonModeSkip,
		},
	}
}

func TestSingletonSkipPersistsVisibleTerminalRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := skipVisibilityWorkflowDef()
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startAdmissionRun(t, js, wfDef, "run-a", nil)
	time.Sleep(300 * time.Millisecond)
	startAdmissionRun(t, js, wfDef, "run-b", nil)
	time.Sleep(300 * time.Millisecond)

	// Positive: run-b is loadable -- no more ErrRunNotFound.
	runB, err := orch.store.Load(context.Background(), "run-b")
	if err != nil {
		t.Fatalf("load run-b: %v", err)
	}
	if runB.Status != dag.RunStatusCancelled {
		t.Fatalf("run-b status = %s, want cancelled", runB.Status)
	}

	skipStep, ok := runB.Steps["<admission-skip>"]
	if !ok {
		t.Fatal("run-b missing <admission-skip> step")
	}
	if !strings.Contains(skipStep.Error, "run-a") {
		t.Fatalf("skip reason %q does not name run-a", skipStep.Error)
	}
	// Negative: status must be Failed (renders `error:` in the CLI),
	// not Skipped -- see pinned decision #3 in the spec: FormatRunStatus
	// only prints the reason for StepStatusFailed steps.
	if skipStep.Status != dag.StepStatusFailed {
		t.Fatalf("skip step status = %v, want StepStatusFailed",
			skipStep.Status)
	}
	// Positive: fresh single-entry map, not the real "a" step still
	// lingering as stale Pending alongside the synthetic entry.
	if len(runB.Steps) != 1 {
		t.Fatalf("run-b Steps = %d entries, want 1", len(runB.Steps))
	}
}

func TestSingletonSkipDoesNotEnqueueTask(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := skipVisibilityWorkflowDef()
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startAdmissionRun(t, js, wfDef, "run-a", nil)
	time.Sleep(300 * time.Millisecond)

	// TASK_QUEUES is a work-queue stream: only one filtered consumer per
	// exact subject may exist at a time, so both fetches below reuse
	// this single subscription rather than creating a second one.
	sub, err := js.PullSubscribe(
		"task.echo.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}

	// Positive (sanity): run-a, the non-skipped run, enqueued a task --
	// proves the subscription is wired to the right subject and isn't
	// just timing out for an unrelated reason.
	sanityMsgs, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch sanity task: %v", err)
	}
	if len(sanityMsgs) != 1 {
		t.Fatalf("sanity task count = %d, want 1", len(sanityMsgs))
	}
	sanityMsgs[0].Ack()

	startAdmissionRun(t, js, wfDef, "run-b", nil)
	time.Sleep(300 * time.Millisecond)

	// Negative (primary): no further task message shows up for run-b.
	_, fetchErr := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if fetchErr != nats.ErrTimeout {
		t.Fatalf("Fetch after skip: err = %v, want nats.ErrTimeout",
			fetchErr)
	}
}

func TestSingletonLockRecoversAfterHolderTerminates(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := skipVisibilityWorkflowDef()
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startAdmissionRun(t, js, wfDef, "run-a", nil)
	time.Sleep(300 * time.Millisecond)

	// Complete run-a's only step so it reaches a terminal status and
	// the singleton lock goes stale.
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "run-a", "a",
		[]byte(`{"status":"ok"}`),
	)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal complete event: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))
	time.Sleep(300 * time.Millisecond)

	startAdmissionRun(t, js, wfDef, "run-c", nil)
	time.Sleep(300 * time.Millisecond)

	// Positive: run-c proceeds normally -- the lock reclaim path
	// (admission.go's staleness branch) is untouched by this fix.
	runC, err := orch.store.Load(context.Background(), "run-c")
	if err != nil {
		t.Fatalf("load run-c: %v", err)
	}
	if runC.Status != dag.RunStatusRunning {
		t.Fatalf("run-c status = %s, want running", runC.Status)
	}

	// Positive: a task was enqueued for run-c.
	sub, err := js.PullSubscribe(
		"task.echo.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch task for run-c: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("task count for run-c = %d, want 1", len(msgs))
	}
}

// skipVisibilityStartEvent builds (without publishing) the same
// workflow.started payload shape startAdmissionRun publishes, so a
// test can drive handleWorkflowStarted directly and inspect its
// return value instead of round-tripping through NakWithDelay.
func skipVisibilityStartEvent(
	t *testing.T, wfDef dag.WorkflowDef, runID string,
) protocol.Event {
	t.Helper()
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal wfDef: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"workflow_def": json.RawMessage(defData),
		"input":        json.RawMessage(nil),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
}

// failingPutKV wraps a real workflow_runs KV handle and forces Put to
// fail for exactly one key, simulating the transient snapshot-store
// write failure #506 guards against (a real KV outage would fail every
// key; failing one is the narrowest fault that still proves the
// propagation without destabilizing run-a's earlier successful save).
type failingPutKV struct {
	jetstream.KeyValue
	failKey string
}

func (f failingPutKV) Put(
	ctx context.Context, key string, value []byte,
) (uint64, error) {
	if key == f.failKey {
		return 0, errors.New("simulated snapshot write failure")
	}
	return f.KeyValue.Put(ctx, key, value)
}

// TestSkipSnapshotWriteFailureNaksForRedelivery covers #506: a skip
// snapshot write failure must propagate out of handleWorkflowStarted
// (so the caller NAKs and NATS redelivers) instead of being logged and
// swallowed, which would silently reproduce the #502 invisible-run bug
// on a transient store error.
func TestSkipSnapshotWriteFailureNaksForRedelivery(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := skipVisibilityWorkflowDef()
	defKV, _ := js.KeyValue("workflow_defs")
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))

	orch := NewOrchestrator(nc)

	// run-a is admitted directly (no consumer running -- see
	// advance_exec_test.go / def_reaper_test.go for the same
	// call-orch-methods-without-Start() convention) so it holds the
	// singleton lock run-b will be skipped against.
	evtA := skipVisibilityStartEvent(t, wfDef, "run-a")
	if err := orch.handleWorkflowStarted(
		context.Background(), evtA,
	); err != nil {
		t.Fatalf("handleWorkflowStarted run-a: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	runsKV, err := jsNew.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs): %v", err)
	}
	orch.store = &SnapshotStore{
		kv: failingPutKV{KeyValue: runsKV, failKey: "run.run-b"},
	}

	evtB := skipVisibilityStartEvent(t, wfDef, "run-b")

	// Negative: the skip-snapshot write failure must come back out of
	// handleWorkflowStarted as a non-nil error so handleEventJS NAKs
	// the message instead of ACKing a run that was never persisted.
	if err := orch.handleWorkflowStarted(
		context.Background(), evtB,
	); err == nil {
		t.Fatal("handleWorkflowStarted run-b (failing store): " +
			"want non-nil error, got nil")
	}

	if _, loadErr := orch.store.Load(
		context.Background(), "run-b",
	); !errors.Is(loadErr, ErrRunNotFound) {
		t.Fatalf(
			"load run-b after failed save: err = %v, want ErrRunNotFound",
			loadErr,
		)
	}

	// Redelivery: the transient failure has cleared, so a second
	// delivery of the same event must succeed and persist the run.
	orch.store = &SnapshotStore{kv: runsKV}
	if err := orch.handleWorkflowStarted(
		context.Background(), evtB,
	); err != nil {
		t.Fatalf("handleWorkflowStarted run-b (redelivery): %v", err)
	}

	// Positive: the skip snapshot now persists and the run is findable.
	runB, err := orch.store.Load(context.Background(), "run-b")
	if err != nil {
		t.Fatalf("load run-b after redelivery: %v", err)
	}
	if runB.Status != dag.RunStatusCancelled {
		t.Fatalf("run-b status = %s, want cancelled", runB.Status)
	}
	skipStep, ok := runB.Steps[admissionSkipStepID]
	if !ok || !strings.Contains(skipStep.Error, "run-a") {
		t.Fatalf("run-b skip step = %+v, want reason naming run-a", skipStep)
	}
}
