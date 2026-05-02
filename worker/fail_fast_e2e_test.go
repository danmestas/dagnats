// worker/fail_fast_e2e_test.go
// End-to-end tests for issue #141: a worker handler returning a plain
// Go error (not RateLimitError, not NonRetryableError) must publish
// step.failed (retriable) and Ack the original message — the engine
// is the sole retry authority. The OLD behaviour was a hardcoded 5s
// NakWithDelay with no step.failed event; the engine never observed
// the failure and the run wedged in 'running' indefinitely.
// Methodology: pair a real orchestrator with a real worker. Run a
// failing handler under various retry policies and confirm that the
// engine drives the retry loop (or fast-fails when no policy exists).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestFailFast_GenericErrorRetriesUntilExhausted(t *testing.T) {
	// Direct repro of #141: a generic error used to NAK forever with no
	// step.failed emitted, leaving Attempts=0/N. After the fix the
	// engine sees step.failed (retriable), increments Attempts,
	// schedules retries via dag.CalculateDelay, and finally lands on
	// Failed once MaxAttempts is hit.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// MaxAttempts:3 + the engine's "<= MaxAttempts" gate allows 3
	// retries — total 4 handler invocations before permanent failure.
	wfDef := dag.WorkflowDef{
		Name: "ff-exhaust", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryExponential,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   2.0,
		},
		Steps: []dag.StepDef{
			{ID: "s", Task: "ff-bad", Type: dag.StepTypeNormal},
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
	w.Handle("ff-bad", func(tc TaskContext) error {
		calls.Add(1)
		return fmt.Errorf("fail-fast precondition")
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-ff-1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(20 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, loadErr := store.Load(context.Background(), "run-ff-1")
		if loadErr == nil && r.Status == dag.RunStatusFailed {
			run = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("run.Status = %v, want Failed (calls=%d)",
			run.Status, calls.Load())
	}
	step := run.Steps["s"]
	if step.Status != dag.StepStatusFailed {
		t.Fatalf("step.Status = %v, want Failed", step.Status)
	}
	// CRITICAL: under the OLD behaviour calls would have been bounded
	// only by JetStream's NAK redelivery loop and the run would never
	// have reached Failed. Under the fix, the engine drove >1 attempt.
	if got := calls.Load(); got < 2 {
		t.Fatalf(
			"handler calls = %d, want >= 2 — engine never retried (#141 repro)",
			got,
		)
	}
	if got := calls.Load(); got > 5 {
		t.Fatalf(
			"handler calls = %d, want <= 5 — runaway retry loop", got,
		)
	}
}

func TestFailFast_GenericErrorThenSuccess(t *testing.T) {
	// Mixed-success path: the generic-error path must produce the same
	// final correctness as a typed Fail call. The engine schedules each
	// retry via the backoff machinery; the third attempt completes.
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
		Name: "ff-mixed", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryExponential,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   2.0,
		},
		Steps: []dag.StepDef{
			{ID: "s", Task: "ff-mix", Type: dag.StepTypeNormal},
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
	w.Handle("ff-mix", func(tc TaskContext) error {
		n := calls.Add(1)
		if n <= 2 {
			return fmt.Errorf("transient on attempt %d", n)
		}
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-ff-2", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(20 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, loadErr := store.Load(context.Background(), "run-ff-2")
		if loadErr == nil && r.Status == dag.RunStatusCompleted {
			run = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("run.Status = %v, want Completed (calls=%d)",
			run.Status, calls.Load())
	}
	step := run.Steps["s"]
	if step.Status != dag.StepStatusCompleted {
		t.Fatalf("step.Status = %v, want Completed", step.Status)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
}

func TestFailFast_NoPolicyFailsOnce(t *testing.T) {
	// No retry policy: the generic error path must transition straight
	// to Failed with exactly one handler invocation. The negative space
	// is "no infinite NAK loop" — under the bug, calls would have grown
	// unbounded as JetStream redelivered the NAK'd message.
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
		Name: "ff-nopolicy", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s", Task: "ff-once", Type: dag.StepTypeNormal},
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
	w.Handle("ff-once", func(tc TaskContext) error {
		calls.Add(1)
		return errors.New("just fail")
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-ff-3", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(10 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, loadErr := store.Load(context.Background(), "run-ff-3")
		if loadErr == nil && r.Status == dag.RunStatusFailed {
			run = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("run.Status = %v, want Failed", run.Status)
	}
	if step := run.Steps["s"]; step.Status != dag.StepStatusFailed {
		t.Fatalf("step.Status = %v, want Failed", step.Status)
	}
	// Wait briefly to confirm no extra dispatch happens.
	time.Sleep(300 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf(
			"handler calls = %d, want 1 (no NAK redelivery loop)", got,
		)
	}
}
