// worker/step_timeout_e2e_test.go
// End-to-end tests for issue #140: StepDef.Timeout was a no-op for
// normal steps — a worker that hangs on a normal step held its task
// forever, and no step.failed / retry / run failure ever fired.
//
// After the fix the engine schedules a TimerActionStepTimeout when
// the step transitions to Running, fires a synthetic step.failed
// (retriable) on expiry, and runs the result through the same retry
// + permanent-failure machinery as a worker-published step.failed.
// Staleness guards drop the timer fire if the step already moved on
// to Completed/Failed/Cancelled or to a later attempt.
//
// Methodology: pair a real orchestrator with a real worker. Block
// the handler on a channel so the timeout fires while the worker is
// genuinely wedged (not after a quick failure). Assert run and step
// terminal status, observed handler call counts, and (for staleness
// tests) the absence of a late synthetic failure once the run is
// already terminal.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// publishStartAndWaitTerminal publishes the workflow.started event
// and polls the snapshot store until run.Status reaches a terminal
// state or the deadline passes. Returns the loaded run.
func publishStartAndWaitTerminal(
	t *testing.T,
	js nats.JetStreamContext,
	jsNew jetstream.JetStream,
	runID string,
	defData []byte,
	want dag.RunStatus,
	timeout time.Duration,
) dag.WorkflowRun {
	t.Helper()
	if runID == "" {
		t.Fatal("publishStartAndWaitTerminal: runID empty")
	}
	if timeout <= 0 {
		t.Fatal("publishStartAndWaitTerminal: timeout <= 0")
	}
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, defData,
	)
	startData, _ := startEvt.Marshal()
	if _, err := js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish start: %v", err)
	}
	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(timeout)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, err := store.Load(context.Background(), runID)
		if err == nil && r.Status == want {
			return r
		}
		run = r
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(
		"run %q did not reach %v within %v (last=%v)",
		runID, want, timeout, run.Status,
	)
	return run
}

// TestStepTimeout_HandlerHangAndFailNoPolicy reproduces issue #140
// directly: a wedged worker without a retry policy must take the
// run to Failed via the timeout watchdog. Negative space: the
// handler is invoked exactly once (no retry without policy) and is
// still blocked when the assertion fires — proving the engine drove
// the failure rather than the worker exiting first.
func TestStepTimeout_HandlerHangAndFailNoPolicy(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "to-hang", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "s", Task: "to-hang-task",
				Type:    dag.StepTypeNormal,
				Timeout: 200 * time.Millisecond,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	release := make(chan struct{})
	defer close(release) // unblock handler on test exit
	w := NewWorker(nc)
	w.Handle("to-hang-task", func(tc TaskContext) error {
		calls.Add(1)
		select {
		case <-release:
			return tc.Complete([]byte(`"unblocked"`))
		case <-time.After(5 * time.Second):
			return tc.Fail(errors.New("handler timed out itself"))
		}
	})
	w.Start()
	defer w.Stop()

	run := publishStartAndWaitTerminal(
		t, js, jsNew, "run-to-1", defData,
		dag.RunStatusFailed, 3*time.Second,
	)

	step := run.Steps["s"]
	if step.Status != dag.StepStatusFailed {
		t.Fatalf(
			"step.Status = %v, want Failed",
			step.Status,
		)
	}
	if !strings.Contains(strings.ToLower(step.Error), "timeout") {
		t.Fatalf(
			"step.Error = %q, want to contain 'timeout'",
			step.Error,
		)
	}
	// Handler must have been invoked exactly once (no retry
	// without policy) and must still be wedged — which is the
	// proof that the engine fired the timer rather than the
	// worker exiting on its own.
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

// TestStepTimeout_TimeoutPlusRetryRetriesAttempts verifies the
// timeout watchdog flows through the existing retry policy. Each
// blocked attempt expires its own watchdog, the engine increments
// Attempts and re-dispatches via the SLEEP_TIMERS retry-backoff
// path, until MaxAttempts is exhausted and the run lands Failed.
func TestStepTimeout_TimeoutPlusRetryRetriesAttempts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// MaxAttempts: 1 with the engine's "<= MaxAttempts" gate
	// allows 1 retry — total 2 attempts before permanent failure.
	wfDef := dag.WorkflowDef{
		Name: "to-retry", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  1,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{
				ID: "s", Task: "to-retry-task",
				Type:    dag.StepTypeNormal,
				Timeout: 200 * time.Millisecond,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	release := make(chan struct{})
	defer close(release)
	w := NewWorker(nc)
	w.Handle("to-retry-task", func(tc TaskContext) error {
		calls.Add(1)
		select {
		case <-release:
			return tc.Complete([]byte(`"unblocked"`))
		case <-time.After(5 * time.Second):
			return tc.Fail(errors.New("handler self-timeout"))
		}
	})
	w.Start()
	defer w.Stop()

	run := publishStartAndWaitTerminal(
		t, js, jsNew, "run-to-2", defData,
		dag.RunStatusFailed, 8*time.Second,
	)

	step := run.Steps["s"]
	if step.Status != dag.StepStatusFailed {
		t.Fatalf(
			"step.Status = %v, want Failed",
			step.Status,
		)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf(
			"handler calls = %d, want 2 (initial + 1 retry)",
			got,
		)
	}
}

// TestStepTimeout_NoFireOnSuccess pins the staleness guard for the
// happy path. A long Timeout vs a fast handler — the watchdog
// must observe the step is no longer Running and drop the fire.
// Negative space: no spurious step.failed in the history stream
// and the run remains Completed past the timeout window.
func TestStepTimeout_NoFireOnSuccess(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	stepTimeout := 1500 * time.Millisecond
	wfDef := dag.WorkflowDef{
		Name: "to-success", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "s", Task: "to-fast-task",
				Type:    dag.StepTypeNormal,
				Timeout: stepTimeout,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	w := NewWorker(nc)
	w.Handle("to-fast-task", func(tc TaskContext) error {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	run := publishStartAndWaitTerminal(
		t, js, jsNew, "run-to-3", defData,
		dag.RunStatusCompleted, 5*time.Second,
	)

	step := run.Steps["s"]
	if step.Status != dag.StepStatusCompleted {
		t.Fatalf(
			"step.Status = %v, want Completed",
			step.Status,
		)
	}

	// Wait past Timeout so the staleness check has run. If the
	// guard were missing, fireStepTimeout would have published a
	// synthetic step.failed by now and the engine would have
	// flipped the run / step to Failed.
	time.Sleep(stepTimeout + 500*time.Millisecond)

	store := enginepkg.NewSnapshotStore(jsNew)
	after, err := store.Load(context.Background(), "run-to-3")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if after.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"post-timeout run.Status = %v, want still Completed",
			after.Status,
		)
	}
	if after.Steps["s"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"post-timeout step.Status = %v, want still Completed",
			after.Steps["s"].Status,
		)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

// TestStepTimeout_NoFireAfterRetry pins the staleness guard against
// stale fires from a prior attempt. Attempt 1 fails with a plain
// error (no timeout), the engine retries, attempt 2 succeeds well
// inside its own Timeout window. The watchdog scheduled at the
// start of attempt 1 must drop its fire when it eventually runs —
// the step is no longer Running on attempt 1; it has either moved
// to attempt 2 or is already Completed.
func TestStepTimeout_NoFireAfterRetry(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Timeout >> retry latency so the watchdog from attempt 1
	// is still pending when attempt 2 succeeds — exposing the
	// staleness guard.
	stepTimeout := 2 * time.Second
	wfDef := dag.WorkflowDef{
		Name: "to-stale", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  2,
			Strategy:     dag.RetryFixed,
			InitialDelay: 50 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{
				ID: "s", Task: "to-flaky-task",
				Type:    dag.StepTypeNormal,
				Timeout: stepTimeout,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	w := NewWorker(nc)
	w.Handle("to-flaky-task", func(tc TaskContext) error {
		n := calls.Add(1)
		if n == 1 {
			return tc.Fail(errors.New("flaky once"))
		}
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	run := publishStartAndWaitTerminal(
		t, js, jsNew, "run-to-4", defData,
		dag.RunStatusCompleted, 5*time.Second,
	)
	if got := calls.Load(); got != 2 {
		t.Fatalf(
			"handler calls = %d, want 2 (1 fail + 1 success)",
			got,
		)
	}

	// Wait past Timeout so the attempt-1 watchdog will have
	// fired by now. If the staleness guard is missing, the
	// fire would publish step.failed and flip the run to
	// Failed (or step Attempts beyond 2).
	time.Sleep(stepTimeout + 500*time.Millisecond)

	store := enginepkg.NewSnapshotStore(jsNew)
	after, err := store.Load(context.Background(), "run-to-4")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if after.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"post-timeout run.Status = %v, want still Completed",
			after.Status,
		)
	}
	if after.Steps["s"].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"post-timeout step.Status = %v, want still Completed",
			after.Steps["s"].Status,
		)
	}
	// Handler must not have been invoked a third time.
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls after wait = %d, want 2", got)
	}
	if run.RunID != "run-to-4" {
		t.Fatalf("test scaffolding: unexpected run %q", run.RunID)
	}
}
