// worker/retry_backoff_e2e_test.go
// End-to-end tests for issue #147: retriable step.failed must schedule
// the next attempt via dag.CalculateDelay-driven backoff rather than
// silently saving and returning. Pairs a real orchestrator with a real
// worker so the round trip exercises the SLEEP_TIMERS re-publish path.
// Methodology: configure a retry policy, register a worker handler whose
// failure pattern is controlled by call count, start the run, wait for
// terminal state under a bounded deadline, assert step + run status and
// observed attempt count.
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

func TestRetryBackoff_ExponentialRetriesUntilSuccess(t *testing.T) {
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
		Name: "rb-success", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryExponential,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   2.0,
		},
		Steps: []dag.StepDef{
			{ID: "s", Task: "rb-task", Type: dag.StepTypeNormal},
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
	// Fail on calls 1 and 2 (FailureType=retriable), succeed on call 3.
	w.Handle("rb-task", func(tc TaskContext) error {
		n := calls.Add(1)
		if n <= 2 {
			return tc.Fail(fmt.Errorf("transient on attempt %d", n))
		}
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rb-1", defData,
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
		r, loadErr := store.Load(context.Background(), "run-rb-1")
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
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
	step := run.Steps["s"]
	if step.Status != dag.StepStatusCompleted {
		t.Fatalf("step.Status = %v, want Completed", step.Status)
	}
	if step.Attempts != 3 {
		t.Fatalf("step.Attempts = %d, want 3", step.Attempts)
	}
}

func TestRetryBackoff_ExhaustsAndFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// MaxAttempts: 1 with the engine's "<= MaxAttempts" gate allows 1
	// retry — total 2 attempts before permanent failure. The negative
	// space here is "no THIRD attempt": after the 2nd failure, no
	// timer-driven retry should fire.
	wfDef := dag.WorkflowDef{
		Name: "rb-exhaust", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  1,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{ID: "s", Task: "rb-bad", Type: dag.StepTypeNormal},
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
	// Always fail.
	w.Handle("rb-bad", func(tc TaskContext) error {
		calls.Add(1)
		return tc.Fail(errors.New("perma-broken"))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rb-2", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(15 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, loadErr := store.Load(context.Background(), "run-rb-2")
		if loadErr == nil && r.Status == dag.RunStatusFailed {
			run = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("run.Status = %v, want Failed", run.Status)
	}
	step := run.Steps["s"]
	if step.Status != dag.StepStatusFailed {
		t.Fatalf("step.Status = %v, want Failed", step.Status)
	}

	// After terminal Failed, give the system a 2x backoff window to
	// confirm no extra dispatch happens — calls must stay at 2 (1
	// initial + 1 retry, no third attempt past MaxAttempts).
	time.Sleep(300 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2 (no extra retry)", got)
	}
}

func TestRetryBackoff_NoPolicyFailsImmediately(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// No DefaultRetry, no step.Retry, no step.Retries — policy resolves
	// to nil. A retriable failure must transition straight to Failed.
	wfDef := dag.WorkflowDef{
		Name: "rb-nopolicy", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s", Task: "rb-once", Type: dag.StepTypeNormal},
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
	w.Handle("rb-once", func(tc TaskContext) error {
		calls.Add(1)
		return tc.Fail(errors.New("just fail"))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rb-3", defData,
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
		r, loadErr := store.Load(context.Background(), "run-rb-3")
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
	// No retry scheduled — calls stayed at 1.
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}
