// internal/engine/run_event_e2e_test.go
// Integration tests for the event.run.* consumer contract (#625).
// Methodology: real embedded NATS via dagnatstest.Harness. Subscribe to
// event.run.> BEFORE starting each run so no message can race the
// subscription, drive the run through the real worker/engine path to a
// terminal status, then assert exactly one RunEvent lands with the right
// run_id/workflow_id/status. Bounded waits throughout (no unbounded
// blocking receive).
package engine_test

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// subscribeRunEvents subscribes to event.run.> and returns a channel
// that receives decoded RunEvents. The subscription is established
// before the caller starts any run, so no publish can race it.
func subscribeRunEvents(
	t *testing.T, nc *nats.Conn,
) <-chan protocol.RunEvent {
	t.Helper()
	if nc == nil {
		panic("subscribeRunEvents: nc must not be nil")
	}
	out := make(chan protocol.RunEvent, 16)
	sub, err := nc.Subscribe("event.run.>", func(msg *nats.Msg) {
		var evt protocol.RunEvent
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Logf("subscribeRunEvents: unmarshal failed: %v", err)
			return
		}
		out <- evt
	})
	if err != nil {
		t.Fatalf("subscribeRunEvents: Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return out
}

// waitForRunEvent drains ch for a RunEvent matching runID within
// timeout, ignoring events for other runs (the subscription is
// process-wide across every test's runs on this NATS server).
func waitForRunEvent(
	t *testing.T, ch <-chan protocol.RunEvent,
	runID string, timeout time.Duration,
) protocol.RunEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case evt := <-ch:
			if evt.RunID == runID {
				return evt
			}
		case <-deadline:
			t.Fatalf(
				"waitForRunEvent: no event.run.* for run %q within %s",
				runID, timeout,
			)
		}
	}
}

func TestEngine_CompletedRun_PublishesRunCompletedEvent(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-task-ok", dagnatstest.PassHandler())
	h.Start(t)

	// Subscribe BEFORE the run starts — the whole point of the test is
	// that no terminal event can be missed by a late subscriber.
	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-completed-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-ok")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := h.RegisterAndRun(t, def, nil, 10*time.Second)
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("run status = %v, want Completed", got.Status)
	}

	evt := waitForRunEvent(t, events, got.RunID, 5*time.Second)
	if evt.Type != protocol.RunEventCompleted {
		t.Fatalf("Type = %q, want %q", evt.Type, protocol.RunEventCompleted)
	}
	if evt.WorkflowID != wfName {
		t.Fatalf("WorkflowID = %q, want %q", evt.WorkflowID, wfName)
	}
	if evt.Status != "completed" {
		t.Fatalf("Status = %q, want %q", evt.Status, "completed")
	}
}

// TestEngine_CompletedRunWithLabels_EventCarriesLabels covers #629's
// run-labels field now that dag.WorkflowRun.Labels exists on main:
// labels supplied at start time must ride the run.completed RunEvent,
// and the copy must be independent (not aliasing run.Labels).
func TestEngine_CompletedRunWithLabels_EventCarriesLabels(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-task-labeled", dagnatstest.PassHandler())
	h.Start(t)

	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-labeled-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-labeled")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx := t.Context()
	if err := h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	labels := map[string]string{"team": "platform", "env": "staging"}
	runID, err := h.Svc.StartRunWithLabels(ctx, wfName, nil, labels)
	if err != nil {
		t.Fatalf("StartRunWithLabels: %v", err)
	}
	got := dagnatstest.WaitForStatus(
		t, h.Svc, runID, 10*time.Second, dag.RunStatusCompleted,
	)
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("run status = %v, want Completed", got.Status)
	}

	evt := waitForRunEvent(t, events, runID, 5*time.Second)
	// Positive: both labels ride the event, uncorrupted.
	if len(evt.Labels) != 2 {
		t.Fatalf("Labels = %v, want 2 entries", evt.Labels)
	}
	if evt.Labels["team"] != "platform" || evt.Labels["env"] != "staging" {
		t.Fatalf("Labels = %v, want team=platform env=staging", evt.Labels)
	}
}

// TestEngine_CompletedRunWithoutLabels_EventOmitsLabelsKey is the
// negative-space counterpart: a run started with no labels must
// produce a RunEvent whose `labels` json key is absent (omitempty),
// not present-and-empty.
func TestEngine_CompletedRunWithoutLabels_EventOmitsLabelsKey(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-task-nolabels", dagnatstest.PassHandler())
	h.Start(t)

	rawCh := make(chan []byte, 4)
	sub, err := h.NC.Subscribe("event.run.>", func(msg *nats.Msg) {
		rawCh <- msg.Data
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	wfName := fmt.Sprintf("run-evt-nolabels-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-nolabels")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := h.RegisterAndRun(t, def, nil, 10*time.Second)
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("run status = %v, want Completed", got.Status)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case data := <-rawCh:
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(data, &raw); err != nil {
				t.Fatalf("unmarshal into map: %v", err)
			}
			var evt protocol.RunEvent
			if err := json.Unmarshal(data, &evt); err != nil {
				t.Fatalf("unmarshal RunEvent: %v", err)
			}
			if evt.RunID != got.RunID {
				continue
			}
			// Negative: no "labels" key at all when there are no labels.
			if _, ok := raw["labels"]; ok {
				t.Fatalf("labels key present with no labels set: %s", data)
			}
			return
		case <-deadline:
			t.Fatalf("no event.run.* for run %q within deadline", got.RunID)
		}
	}
}

func TestEngine_FailedRun_PublishesRunFailedEvent(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-task-fail",
		dagnatstest.FailHandler("permanent failure"))
	h.Start(t)

	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-failed-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-fail")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := h.RegisterAndRun(t, def, nil, 10*time.Second)
	if got.Status != dag.RunStatusFailed {
		t.Fatalf("run status = %v, want Failed", got.Status)
	}

	evt := waitForRunEvent(t, events, got.RunID, 5*time.Second)
	if evt.Type != protocol.RunEventFailed {
		t.Fatalf("Type = %q, want %q", evt.Type, protocol.RunEventFailed)
	}
	if evt.Status != "failed" {
		t.Fatalf("Status = %q, want %q", evt.Status, "failed")
	}
}

func TestEngine_CancelledRun_PublishesRunCancelledEvent(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	// A handler that blocks past the test's cancel call so the run is
	// still Running (cancellable) when CancelRun is issued.
	block := make(chan struct{})
	h.Handle(t, "run-evt-task-block", func(tc worker.TaskContext) error {
		<-block
		return tc.Complete(tc.Input())
	})
	h.Start(t)

	events := subscribeRunEvents(t, h.NC)

	wfName := fmt.Sprintf("run-evt-cancelled-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-task-block")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx := t.Context()
	if err := h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	runID, err := h.Svc.StartRun(ctx, wfName, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	dagnatstest.WaitForStatus(
		t, h.Svc, runID, 10*time.Second, dag.RunStatusRunning,
	)
	if err := h.Svc.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	got := dagnatstest.WaitForStatus(
		t, h.Svc, runID, 10*time.Second, dag.RunStatusCancelled,
	)
	if got.Status != dag.RunStatusCancelled {
		t.Fatalf("run status = %v, want Cancelled", got.Status)
	}

	evt := waitForRunEvent(t, events, runID, 5*time.Second)
	if evt.Type != protocol.RunEventCancelled {
		t.Fatalf("Type = %q, want %q", evt.Type, protocol.RunEventCancelled)
	}
	if evt.Status != "cancelled" {
		t.Fatalf("Status = %q, want %q", evt.Status, "cancelled")
	}

	// Unblock the still-running handler goroutine now, BEFORE the
	// harness's t.Cleanup-registered Worker.Stop() runs — Stop() drains
	// in-flight handlers with a 30s timeout, and t.Cleanup runs LIFO, so
	// leaving this to a Cleanup registered here would run after Stop()
	// and the test would eat the full drain timeout.
	close(block)
}

// TestEngine_SubscriberReactingToRunCompleted_SecondSingletonRunAdmitted
// is the acceptance test for the #625 review round-2 ordering fix: a
// subscriber that starts a second run of a singleton workflow the
// INSTANT it observes run.completed must see that run admitted, not
// skipped — finalizeRun's afterPersist (lock/slot release) now runs
// before either publish, so admission state is already consistent by
// the time any subscriber can react to the event.
//
// Note on strength: admission's own singleton staleness-reclaim check
// (singletonCheck loading the lock-holder's run and finding it already
// terminal) is a second, independent safety net that would likely mask
// this specific bug for singleton mode even without the afterPersist
// fix — the deterministic unit-level regression guard for the ordering
// itself is TestFinalizeRun_AfterPersistRunsBeforeEitherPublish. This
// test's value is verifying the documented end-to-end contract ("a
// subscriber may safely start the next run on run.completed") actually
// holds against the real admission pipeline, not just as a targeted
// bug catch.
func TestEngine_SubscriberReactingToRunCompleted_SecondSingletonRunAdmitted(
	t *testing.T,
) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "run-evt-singleton-task", dagnatstest.PassHandler())
	h.Start(t)

	wfName := fmt.Sprintf("run-evt-singleton-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	wb.Task("a", "run-evt-singleton-task")
	wb.WithSingleton(dag.SingletonModeSkip)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := t.Context()
	if err := h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	// React to run.completed for THIS workflow by starting a second run
	// synchronously inside the NATS message callback — the sharpest
	// reproduction of "a subscriber reacting the instant the event
	// arrives" the test can produce.
	secondRunID := make(chan string, 1)
	var reacted atomic.Bool
	sub, err := h.NC.Subscribe(
		"event.run."+wfName+".>",
		func(msg *nats.Msg) {
			// React exactly once — the second run's OWN completion also
			// matches this wildcard subscription, and without this guard
			// each subsequent run.completed would chain-start yet another
			// run for as long as the test process lives.
			if !reacted.CompareAndSwap(false, true) {
				return
			}
			runID, startErr := h.Svc.StartRun(ctx, wfName, nil)
			if startErr != nil {
				t.Logf("second StartRun failed: %v", startErr)
				return
			}
			secondRunID <- runID
		},
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	runID1, err := h.Svc.StartRun(ctx, wfName, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	dagnatstest.WaitForStatus(
		t, h.Svc, runID1, 10*time.Second, dag.RunStatusCompleted,
	)

	var runID2 string
	select {
	case runID2 = <-secondRunID:
	case <-time.After(5 * time.Second):
		t.Fatal("second StartRun was never issued by the subscriber")
	}

	got2 := dagnatstest.WaitForStatus(
		t, h.Svc, runID2, 10*time.Second, dag.RunStatusCompleted,
	)
	// Positive: the second run completed normally.
	if got2.Status != dag.RunStatusCompleted {
		t.Fatalf("second run status = %v, want Completed", got2.Status)
	}
	// Negative: it was never skip-admitted. persistSkippedRun (package
	// engine, unexported) writes a synthetic "<admission-skip>" step
	// when it handles a run — hardcoded here since this is an external
	// (engine_test) test package and cannot reference the unexported
	// admissionSkipStepID constant directly.
	if _, skipped := got2.Steps["<admission-skip>"]; skipped {
		t.Fatalf(
			"second run was admission-skipped (raced the singleton "+
				"lock release): steps=%v", got2.Steps,
		)
	}
}
