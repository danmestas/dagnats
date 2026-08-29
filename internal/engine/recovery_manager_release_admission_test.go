// internal/engine/recovery_manager_release_admission_test.go
// PR #661 review round-2, fix 3 for #648: the compensation/CompensateFailed
// terminal sites in map_handlers.go (failMapStep, Failed) and
// recovery_manager.go (failAuxStep, CompensateFailed; HandleCompensateCompleted,
// Compensated) passed a nil (or admission-less) afterPersist to
// finalizeRun, so a run ending at one of those statuses never released
// its singleton lock / concurrency slot at all -- and since afterPersist
// was nil, finalizeRun's ReleasePending debt mechanism never even
// engaged, so the reconciler had nothing to recover either. The lock
// leaked permanently.
//
// Methodology: real embedded NATS/JetStream, white-box package engine,
// mirrors run_event_compensated_test.go's low-level event-driving
// style (compensation has no dagnatstest.Harness helper). Drives a
// SINGLETON-mode workflow through step failure -> compensation ->
// Compensated, then asserts the singleton_locks KV entry is gone
// immediately after -- checked directly against the KV rather than by
// starting a competing run, because singletonCheck's own stale-lock
// reclaim (admission.go) would silently paper over a leaked lock the
// next time a new run for the same key is admitted, which would make
// this assertion pass even on the unfixed code (see the file header
// in reconciler_release_pending_test.go for the same caveat).
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestOrchestrator_CompensatedSingletonRun_ReleasesLock(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "comp-singleton-release-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "comp-rel-task-a", Type: dag.StepTypeNormal,
				Compensate: "undo-a"},
			{ID: "b", Task: "comp-rel-task-b", DependsOn: []string{"a"},
				Type:  dag.StepTypeNormal,
				Retry: &dag.RetryPolicy{MaxAttempts: 1}},
			{ID: "undo-a", Task: "comp-rel-task-undo-a",
				Type: dag.StepTypeNormal},
		},
		AuxSteps:  map[string]bool{"undo-a": true},
		Singleton: &dag.SingletonConfig{Mode: dag.SingletonModeSkip},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	runID := "comp-rel-run-1"

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal start event: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForRunStatus(t, orch.store, runID, dag.RunStatusRunning, 5*time.Second)
	startRun, loadErr := orch.store.Load(t.Context(), runID)
	if loadErr != nil {
		t.Fatalf("load running run: %v", loadErr)
	}
	if startRun.SingletonKey == "" {
		t.Fatal("running run must have SingletonKey set once admitted")
	}
	singletonKey := startRun.SingletonKey

	singletonKV, err := js.KeyValue("singleton_locks")
	if err != nil {
		t.Fatalf("singleton_locks KV: %v", err)
	}
	if _, getErr := singletonKV.Get(singletonKey); getErr != nil {
		t.Fatalf("lock %q missing right after admission: %v", singletonKey, getErr)
	}

	subA, _ := js.PullSubscribe(
		"task.comp-rel-task-a.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsA) != 1 {
		t.Fatalf("expected task-a: err=%v len=%d", err, len(msgsA))
	}
	msgsA[0].Ack()
	completeA := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, runID, []byte(`{"result":"ok"}`),
	)
	completeA.StepID = "a"
	completeAData, err := completeA.Marshal()
	if err != nil {
		t.Fatalf("marshal step a completed: %v", err)
	}
	mustPublish(t, js, completeA.NATSSubject(), completeAData,
		nats.MsgId(completeA.NATSMsgID()))

	subB, _ := js.PullSubscribe(
		"task.comp-rel-task-b.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsB) != 1 {
		t.Fatalf("expected task-b: err=%v len=%d", err, len(msgsB))
	}
	msgsB[0].Ack()
	failB := protocol.NewWorkflowEvent(
		protocol.EventStepFailed, runID,
		[]byte(`{"error":"boom","failure_type":"non_retriable"}`),
	)
	failB.StepID = "b"
	failBData, err := failB.Marshal()
	if err != nil {
		t.Fatalf("marshal step b failed: %v", err)
	}
	mustPublish(t, js, failB.NATSSubject(), failBData,
		nats.MsgId(failB.NATSMsgID()))

	subUndo, _ := js.PullSubscribe(
		"task.comp-rel-task-undo-a.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgsUndo, err := subUndo.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsUndo) != 1 {
		t.Fatalf("expected undo-a: err=%v len=%d", err, len(msgsUndo))
	}
	msgsUndo[0].Ack()
	completeUndo := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, runID, []byte(`{"result":"undone"}`),
	)
	completeUndo.StepID = "undo-a"
	completeUndoData, err := completeUndo.Marshal()
	if err != nil {
		t.Fatalf("marshal undo-a completed: %v", err)
	}
	mustPublish(t, js, completeUndo.NATSSubject(), completeUndoData,
		nats.MsgId(completeUndo.NATSMsgID()))

	waitForRunStatus(t, orch.store, runID, dag.RunStatusCompensated, 5*time.Second)

	// Positive: the lock is gone -- releaseAdmission actually ran as
	// part of finalizing the Compensated status, not merely eligible
	// for a later reconciler recovery that a nil afterPersist would
	// never have triggered in the first place.
	deadline := time.Now().Add(5 * time.Second)
	var lockGone bool
	for time.Now().Before(deadline) {
		if _, getErr := singletonKV.Get(singletonKey); getErr != nil {
			lockGone = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !lockGone {
		t.Fatalf("lock %q still present after run reached Compensated", singletonKey)
	}

	// Positive (end to end): a second run for the same singleton is
	// now admitted (Running), not skipped.
	runID2 := "comp-rel-run-2"
	startEvt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID2, defData,
	)
	startData2, err := startEvt2.Marshal()
	if err != nil {
		t.Fatalf("marshal second start event: %v", err)
	}
	mustPublish(t, js, startEvt2.NATSSubject(), startData2,
		nats.MsgId(startEvt2.NATSMsgID()))
	waitForRunStatus(t, orch.store, runID2, dag.RunStatusRunning, 5*time.Second)
}
