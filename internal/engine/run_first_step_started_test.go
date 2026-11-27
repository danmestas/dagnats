// engine/run_first_step_started_test.go
// Pins #1032's record half: a run records when its first step started,
// next to when it was created, so a client can show queue time and run
// time apart.
//
// Methodology: one grouped step through the real orchestrator, with no
// worker. While the step waits in the queue the run has no first-step
// time; the first step.started sets it, and a later step.started (a
// redelivery) leaves it as it was.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRunRecordsWhenItsFirstStepStarted(t *testing.T) {
	const runID = "first-step-started-1"
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	orch := startGroupedRetryWorkflow(t, nc, js, runID, nil)
	t.Cleanup(orch.Stop)
	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, runID, "train", dag.StepStatusQueued, 5*time.Second)
	queued, err := store.Load(t.Context(), runID)
	if err != nil {
		t.Fatalf("load queued run: %v", err)
	}
	if queued.FirstStepStartedAt != nil {
		t.Fatalf("a run whose step is still queued has first_step_started_at %v", queued.FirstStepStartedAt)
	}

	publishStepStarted(t, js, runID, "first")
	waitForStepStatus(t, store, runID, "train", dag.StepStatusRunning, 5*time.Second)
	started, err := store.Load(t.Context(), runID)
	if err != nil {
		t.Fatalf("load started run: %v", err)
	}
	if started.FirstStepStartedAt == nil || started.FirstStepStartedAt.Before(started.CreatedAt) {
		t.Fatalf("first_step_started_at = %v, want set and not before created_at %v",
			started.FirstStepStartedAt, started.CreatedAt)
	}

	first := *started.FirstStepStartedAt
	publishStepStarted(t, js, runID, "redelivery")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(t.Context(), runID)
		if err != nil {
			t.Fatalf("load run: %v", err)
		}
		if run.FirstStepStartedAt == nil || !run.FirstStepStartedAt.Equal(first) {
			t.Fatalf("a later step.started moved first_step_started_at from %v to %v",
				first, run.FirstStepStartedAt)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// publishStepStarted sends a step.started for the train step with a
// message ID of its own, so a second one is not deduplicated.
func publishStepStarted(t *testing.T, js nats.JetStreamContext, runID, tag string) {
	t.Helper()
	evt := protocol.NewStepEvent(protocol.EventStepStarted, runID, "train", nil)
	evt.AttemptNumber = 1
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal step.started: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: evt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {runID + ".started." + tag}},
	})
}
