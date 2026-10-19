// engine/cancel_missing_def_test.go
// Methodology: cancelling a run whose pinned def version is gone (#784
// item 15), end to end against a real embedded NATS server and a running
// orchestrator. A def is registered (pointer + immutable version key), a
// run is started that parks on a sleep step while a sibling task
// step holds a task concurrency slot, then every def key is deleted --
// the state `workflow delete --force` leaves behind. workflow.cancelled
// must still land: the run and its unfinished steps end cancelled, the
// task slot and the runactive marker are released, and
// nothing reaches DEAD_LETTERS. Before the fix the engine NAKed the event
// on every delivery ("pinned to workflow def version ... which no longer
// exists") and the run stayed running.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const cancelNoDefRunID = "cancel-nodef-run-1"

// cancelNoDefWorkflow parks on the nap sleep while task-a, limited to one
// concurrent task, sits queued holding its slot.
func cancelNoDefWorkflow() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name: "cancel-nodef-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "nodef-task",
				Type: dag.StepTypeNormal, MaxTaskConcurrency: 1,
			},
			{
				ID:   "nap",
				Type: dag.StepTypeSleep,
				Config: dag.MarshalConfig(&dag.SleepConfig{
					Duration: 10 * time.Minute,
				}),
			},
		},
	}
}

// startRunThenDeleteDef registers the workflow, starts the run, waits
// until it is parked (nap sleeping, task-a holding its slot), then
// deletes the pointer and the pinned version.
func startRunThenDeleteDef(
	t *testing.T, js nats.JetStreamContext, jsNew jetstream.JetStream,
	orch *Orchestrator,
) {
	t.Helper()
	wfDef := cancelNoDefWorkflow()
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue workflow_defs: %v", err)
	}
	defData := mustMarshal(t, wfDef)
	versionKey := dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef))
	mustPut(t, defKV, wfDef.Name, defData)
	mustPut(t, defKV, versionKey, defData)

	publishEvent(t, js, protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, cancelNoDefRunID, defData,
	))
	waitForStepStatus(t, orch.store, cancelNoDefRunID, "nap",
		dag.StepStatusRunning, 5*time.Second)
	waitForStepStatus(t, orch.store, cancelNoDefRunID, "task-a",
		dag.StepStatusQueued, 5*time.Second)
	assertTaskSlotHeld(t, jsNew, "nodef-task", true)

	if err := defKV.Delete(versionKey); err != nil {
		t.Fatalf("delete version key: %v", err)
	}
	if err := defKV.Delete(wfDef.Name); err != nil {
		t.Fatalf("delete pointer: %v", err)
	}
}

func TestCancelRunWithMissingPinnedDef(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	orch := NewOrchestrator(nc)
	if err := orch.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()
	startRunThenDeleteDef(t, js, jsNew, orch)

	publishEvent(t, js, protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, cancelNoDefRunID, nil,
	))
	waitForRunStatus(t, orch.store, cancelNoDefRunID,
		dag.RunStatusCancelled, 10*time.Second)

	run, err := orch.store.Load(context.Background(), cancelNoDefRunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, stepID := range []string{"task-a", "nap"} {
		if got := run.Steps[stepID].Status; got != dag.StepStatusCancelled {
			t.Errorf("step %s status = %v, want cancelled", stepID, got)
		}
	}
	if run.CompletedAt == nil {
		t.Error("cancelled run has no CompletedAt")
	}
	assertTaskSlotHeld(t, jsNew, "nodef-task", false)
	assertRunNotActive(t, jsNew, cancelNoDefRunID)
	assertNoDeadLetters(t, jsNew)

	// The nap's timer still fires later. With the def gone its event
	// must be classified as droppable, not retried into DEAD_LETTERS.
	late := protocol.NewStepEvent(
		protocol.EventStepSleepCompleted, cancelNoDefRunID, "nap", nil,
	)
	err = orch.dispatchEvent(context.Background(), late)
	if !errors.Is(err, errTerminalRunDefGone) {
		t.Fatalf("late timer on a finished def-less run: err = %v, "+
			"want errTerminalRunDefGone", err)
	}
}

// assertTaskSlotHeld waits (bounded) until the run's task-a holding a
// slot of taskType in concurrency_tasks matches want: the slot is taken
// just after the step is saved as queued, so the first read can race it.
func assertTaskSlotHeld(
	t *testing.T, js jetstream.JetStream, taskType string, want bool,
) {
	t.Helper()
	cm, err := NewConcurrencyManagerSafe(js)
	if err != nil {
		t.Fatalf("NewConcurrencyManagerSafe: %v", err)
	}
	holder := taskSlotHolder(cancelNoDefRunID, "task-a")
	deadline := time.Now().Add(5 * time.Second)
	var members []string
	for time.Now().Before(deadline) {
		members, _, err = cm.readMembers(
			context.Background(), cm.taskKV, "task."+taskType,
		)
		if err != nil {
			t.Fatalf("readMembers: %v", err)
		}
		if containsMember(members, holder) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("task slot held != %v within 5s (members %v)", want, members)
}

// assertRunNotActive fails if the run still has its runactive marker:
// the reconciler would keep treating a cancelled run as live.
func assertRunNotActive(t *testing.T, js jetstream.JetStream, runID string) {
	t.Helper()
	kv, err := js.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue workflow_runs: %v", err)
	}
	// The marker is removed just after the terminal save, so poll for it
	// rather than reading once the run reports cancelled.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = kv.Get(context.Background(), runActivePrefix+runID)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runactive marker still present (err=%v)", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// assertNoDeadLetters fails if anything was dead-lettered.
func assertNoDeadLetters(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	stream, err := js.Stream(context.Background(), "DEAD_LETTERS")
	if err != nil {
		t.Fatalf("Stream DEAD_LETTERS: %v", err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatalf("DEAD_LETTERS info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("DEAD_LETTERS holds %d messages, want 0", info.State.Msgs)
	}
}
