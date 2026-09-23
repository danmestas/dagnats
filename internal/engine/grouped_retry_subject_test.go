// engine/grouped_retry_subject_test.go
// Regression tests for #721: a grouped step's timer-driven retry was
// re-published on the UNGROUPED subject, where no grouped consumer
// filters, so the retry was never claimed and the run never ended.
//
// Methodology: the end-to-end test behaves like a real grouped worker.
// It takes each attempt ONLY from the grouped subject and fails it. That
// is deliberate: publishing step.failed directly would drive the run to
// exhaustion even with the bug present, because run state does not care
// which subject a retry went to. Pulling from the grouped subject is what
// makes the test fail pre-fix exactly the way production did (attempt 2
// never arrives). Each test gets its own embedded server; waits are
// bounded.
package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestTimerMessageDispatchSubject pins the single resolution point every
// dispatch timer action shares: a carried Subject wins, and a legacy
// timer with none falls back to the pre-#721 ungrouped derivation.
func TestTimerMessageDispatchSubject(t *testing.T) {
	grouped := TimerMessage{
		RunID:    "r1",
		TaskType: "ml-training",
		Subject:  "task.ml-training.=gpu.r1",
	}
	if got := grouped.dispatchSubject(); got != "task.ml-training.=gpu.r1" {
		t.Fatalf("carried subject: got %q, want the grouped subject", got)
	}
	legacy := TimerMessage{RunID: "r1", TaskType: "ml-training"}
	if got := legacy.dispatchSubject(); got != "task.ml-training.r1" {
		t.Fatalf("legacy fallback: got %q, want task.ml-training.r1", got)
	}
	mustPanic(t, "empty RunID", func() {
		TimerMessage{TaskType: "x", Subject: "task.x.r"}.dispatchSubject()
	})
	mustPanic(t, "legacy timer without TaskType", func() {
		TimerMessage{RunID: "r1"}.dispatchSubject()
	})
}

// TestGroupedStepRetriesOnGroupedSubjectUntilRunFails is the #721
// regression: every attempt of a grouped step must arrive on the grouped
// subject, and once attempts are exhausted the run must end failed
// rather than staying running forever.
func TestGroupedStepRetriesOnGroupedSubjectUntilRunFails(t *testing.T) {
	const runID = "grp-retry-1"
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	orch := startGroupedRetryWorkflow(t, nc, js, runID)
	defer orch.Stop()

	grouped, err := js.PullSubscribe(
		"task.ml-training.=gpu.*", "", nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe grouped subject: %v", err)
	}
	received := failEveryGroupedAttempt(t, js, grouped, runID)

	// MaxAttempts 2 permits two retries after the first dispatch, so a
	// correctly routed step is dispatched three times. Pre-fix only the
	// first dispatch reached the grouped subject.
	if received != 3 {
		t.Fatalf("grouped subject received %d dispatches, want 3", received)
	}
	waitForRunStatus(t, NewSnapshotStore(jsNew), runID,
		dag.RunStatusFailed, 5*time.Second)

	// Negative space: nothing may land on the ungrouped subject, which no
	// grouped consumer filters. That is where the bug sent retries.
	ungrouped, err := js.PullSubscribe(
		"task.ml-training."+runID, "", nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe ungrouped subject: %v", err)
	}
	stray, _ := ungrouped.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if len(stray) != 0 {
		t.Fatalf("%d retry message(s) stranded on the ungrouped subject",
			len(stray))
	}
}

// startGroupedRetryWorkflow registers a single grouped step with a fast
// fixed retry policy, starts the orchestrator, and starts one run.
func startGroupedRetryWorkflow(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext, runID string,
) *Orchestrator {
	t.Helper()
	if nc == nil {
		t.Fatal("startGroupedRetryWorkflow: nc must not be nil")
	}
	if runID == "" {
		t.Fatal("startGroupedRetryWorkflow: runID must not be empty")
	}
	wfDef := dag.WorkflowDef{
		Name: "grouped-retry", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  2,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
		},
		Steps: []dag.StepDef{{
			ID: "train", Task: "ml-training",
			Type: dag.StepTypeNormal, WorkerGroup: "gpu",
		}},
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)
	mustPut(t, defKV, dag.DefVersionKey(wfDef.Name, dag.DefHash(wfDef)), defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	return orch
}

// failEveryGroupedAttempt plays a grouped worker: it claims each attempt
// from the grouped subject and reports it failed (retriable), mirroring
// production's step.started then step.failed. It returns how many
// dispatches arrived before the subject went quiet. Bounded so a
// runaway retry loop cannot hang the test.
func failEveryGroupedAttempt(
	t *testing.T, js nats.JetStreamContext,
	grouped *nats.Subscription, runID string,
) int {
	t.Helper()
	const dispatchesMax = 10
	received := 0
	for received < dispatchesMax {
		msgs, err := grouped.Fetch(1, nats.MaxWait(3*time.Second))
		if err != nil || len(msgs) == 0 {
			break
		}
		if err := msgs[0].Ack(); err != nil {
			t.Fatalf("ack attempt %d: %v", received+1, err)
		}
		received++
		publishAttemptFailure(t, js, runID, received)
	}
	if received >= dispatchesMax {
		t.Fatalf("still dispatching after %d attempts", dispatchesMax)
	}
	return received
}

// publishAttemptFailure emits step.started then a retriable step.failed
// for one attempt, each with a per-attempt message ID so JetStream
// dedup does not swallow a later attempt's events.
func publishAttemptFailure(
	t *testing.T, js nats.JetStreamContext, runID string, attempt int,
) {
	t.Helper()
	if attempt < 1 {
		t.Fatalf("publishAttemptFailure: attempt %d must be >= 1", attempt)
	}
	started := protocol.NewStepEvent(
		protocol.EventStepStarted, runID, "train", nil)
	started.AttemptNumber = attempt
	startedData, err := started.Marshal()
	if err != nil {
		t.Fatalf("marshal started: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: started.NATSSubject(), Data: startedData,
		Header: nats.Header{"Nats-Msg-Id": {started.NATSMsgID()}},
	})
	failed := protocol.NewStepEvent(
		protocol.EventStepFailed, runID, "train",
		[]byte(`"transient error"`))
	failedData, err := failed.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: failed.NATSSubject(), Data: failedData,
		Header: nats.Header{"Nats-Msg-Id": {
			fmt.Sprintf("%s.train.fail.%d", runID, attempt),
		}},
	})
}

// mustPanic fails the test unless fn panics. what names the precondition
// being checked, so a missing panic reads as the invariant that broke.
func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	if fn == nil {
		t.Fatal("mustPanic: fn must not be nil")
	}
	defer func() {
		if recover() == nil {
			t.Fatalf("expected a panic for %s, got none", what)
		}
	}()
	fn()
}
