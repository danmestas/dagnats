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
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
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

	// Positive (sanity): run-a, the non-skipped run, enqueued a task --
	// proves the subscription below is wired to the right subject and
	// isn't just timing out for an unrelated reason.
	subSanity, err := js.PullSubscribe(
		"task.echo.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe sanity: %v", err)
	}
	sanityMsgs, err := subSanity.Fetch(1, nats.MaxWait(500*time.Millisecond))
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
	subSkip, err := js.PullSubscribe(
		"task.echo.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe skip: %v", err)
	}
	_, fetchErr := subSkip.Fetch(1, nats.MaxWait(500*time.Millisecond))
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
