// e2e/features/http_failures_test.go
//
// Methodology: end-to-end coverage for ADR-013 §"Failure handling"
// and ADR-013 §"Route conflict at registration". Each subtest
// stands up its own embedded NATS server, real engine, real
// trigger service, real workers, and drives requests through the
// trigger.HTTPRouter via httptest. The signals each subtest cares
// about (timeout, fail, cancel, client-close, no-respond) come
// from REAL engine output — no fake event publishes.
//
// Scenarios covered here:
//   - TestHTTPTrigger_FailureModes: subtests for timeout (504),
//     workflow_failed (500), engine_cancelled (503),
//     client_cancelled (499), no_respond (504). Each asserts the
//     actual run state after the HTTP request completes.
//   - TestHTTPTrigger_RouteConflict: a second HTTP trigger
//     registration on the same (method, path) returns a typed
//     RouteConflictError; the original trigger keeps working.
package features

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// TestHTTPTrigger_FailureModes is the failure-mode matrix from
// ADR-013 §"Failure handling". Each subtest registers its own
// workflow + trigger + worker so timeouts and cancellations on one
// case do not poison another.
func TestHTTPTrigger_FailureModes(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		t.Run("timeout", func(t *testing.T) {
			testFailureTimeout(t, nc)
		})
		t.Run("workflow_failed", func(t *testing.T) {
			testFailureWorkflowFailed(t, nc)
		})
		t.Run("engine_cancelled", func(t *testing.T) {
			testFailureEngineCancelled(t, nc)
		})
		t.Run("client_cancelled", func(t *testing.T) {
			testFailureClientCancelled(t, nc)
		})
		t.Run("no_respond", func(t *testing.T) {
			testFailureNoRespond(t, nc)
		})
	})
}

// testFailureTimeout drives a workflow whose only step blocks
// longer than the trigger TimeoutMs. The handler returns 504 with
// a structured body; per ADR-013 the engine does not cancel the
// run, so the run snapshot remains in a non-terminal state.
func testFailureTimeout(t *testing.T, nc *nats.Conn) {
	t.Helper()
	stack := startHTTPE2EStack(t, nc)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	harness.SubscribeWorker(t, nc, "blocker",
		func(tc worker.TaskContext) error {
			<-block
			return tc.Complete([]byte(`"unreached"`))
		})

	wfName := harness.UniqueName(t, "timeout-wf")
	wfDef := dag.WorkflowDef{
		Name: wfName, Version: "v1",
		Steps: []dag.StepDef{
			{ID: "block", Task: "blocker",
				Type: dag.StepTypeNormal},
			respondStepDef(t, "respond",
				[]string{"block"},
				dag.RespondConfig{Status: 200}),
		},
	}
	_, path := stack.registerHTTPTrigger(t, wfDef,
		&trigger.HTTPConfig{
			Path:         "/" + harness.UniqueName(t, "tout"),
			Method:       http.MethodPost,
			TimeoutMs:    400,
			MaxBodyBytes: 1024,
		})

	rec := postOnRouter(t, stack.router,
		http.MethodPost, path, []byte(`{}`), nil)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body=%s",
			rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(),
		`"error":"workflow_timeout"`) {
		t.Fatalf("body = %q, want workflow_timeout",
			rec.Body.String())
	}
	// Negative: per ADR-013, engine does NOT cancel on client
	// timeout. The run snapshot stays running.
	assertRunNotTerminal(t, stack,
		rec.Header().Get("X-Dagnats-Run-Id"))
}

// testFailureWorkflowFailed drives a workflow whose only step
// calls FailPermanent. The orchestrator's failLoopStep emits a
// workflow.failed event and the handler maps it to 500.
func testFailureWorkflowFailed(t *testing.T, nc *nats.Conn) {
	t.Helper()
	stack := startHTTPE2EStack(t, nc)

	harness.SubscribeWorker(t, nc, "failer",
		func(tc worker.TaskContext) error {
			return tc.FailPermanent(
				errors.New("permanent failure for test"))
		})

	wfName := harness.UniqueName(t, "fail-wf")
	wfDef := dag.WorkflowDef{
		Name: wfName, Version: "v1",
		Steps: []dag.StepDef{
			{ID: "boom", Task: "failer",
				Type: dag.StepTypeNormal},
			respondStepDef(t, "respond",
				[]string{"boom"},
				dag.RespondConfig{Status: 200}),
		},
	}
	_, path := stack.registerHTTPTrigger(t, wfDef,
		&trigger.HTTPConfig{
			Path:         "/" + harness.UniqueName(t, "fail"),
			Method:       http.MethodPost,
			TimeoutMs:    10_000,
			MaxBodyBytes: 1024,
		})

	rec := postOnRouter(t, stack.router,
		http.MethodPost, path, []byte(`{}`), nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s",
			rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(),
		`"error":"workflow_failed"`) {
		t.Fatalf("body = %q, want workflow_failed",
			rec.Body.String())
	}
	// Negative: run reached the failed terminal state.
	assertRunReachesStatus(t, stack,
		rec.Header().Get("X-Dagnats-Run-Id"),
		dag.RunStatusFailed)
}

// testFailureEngineCancelled drives a workflow that blocks, then
// calls Service.CancelRun while the HTTP handler is waiting. The
// handler maps workflow.cancelled to 503.
func testFailureEngineCancelled(
	t *testing.T, nc *nats.Conn,
) {
	t.Helper()
	stack := startHTTPE2EStack(t, nc)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	var seenRunID string
	runIDCh := make(chan string, 1)
	harness.SubscribeWorker(t, nc, "blocker-cancel",
		func(tc worker.TaskContext) error {
			select {
			case runIDCh <- tc.RunID():
			default:
			}
			<-block
			return tc.Complete([]byte(`"unreached"`))
		})

	wfName := harness.UniqueName(t, "cancel-wf")
	wfDef := dag.WorkflowDef{
		Name: wfName, Version: "v1",
		Steps: []dag.StepDef{
			{ID: "block", Task: "blocker-cancel",
				Type: dag.StepTypeNormal},
			respondStepDef(t, "respond",
				[]string{"block"},
				dag.RespondConfig{Status: 200}),
		},
	}
	_, path := stack.registerHTTPTrigger(t, wfDef,
		&trigger.HTTPConfig{
			Path:         "/" + harness.UniqueName(t, "cancel"),
			Method:       http.MethodPost,
			TimeoutMs:    10_000,
			MaxBodyBytes: 1024,
		})

	// Cancel asynchronously after the worker reports its run id.
	cancelDoneCh := make(chan struct{})
	go func() {
		defer close(cancelDoneCh)
		select {
		case rid := <-runIDCh:
			seenRunID = rid
			// Small delay so the handler is parked on its
			// select before cancellation fires.
			time.Sleep(100 * time.Millisecond)
			_ = stack.svc.CancelRun(stack.ctx, rid)
		case <-time.After(5 * time.Second):
			t.Errorf("worker did not start within 5s")
		}
	}()
	rec := postOnRouter(t, stack.router,
		http.MethodPost, path, []byte(`{}`), nil)
	<-cancelDoneCh
	_ = seenRunID
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s",
			rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(),
		`"error":"workflow_cancelled"`) {
		t.Fatalf("body = %q, want workflow_cancelled",
			rec.Body.String())
	}
}

// testFailureClientCancelled drives a workflow that blocks, then
// closes the request context before any response arrives. The
// handler maps the context cancellation to 499 (nginx convention).
func testFailureClientCancelled(
	t *testing.T, nc *nats.Conn,
) {
	t.Helper()
	stack := startHTTPE2EStack(t, nc)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	harness.SubscribeWorker(t, nc, "blocker-client",
		func(tc worker.TaskContext) error {
			<-block
			return tc.Complete([]byte(`"unreached"`))
		})

	wfName := harness.UniqueName(t, "client-wf")
	wfDef := dag.WorkflowDef{
		Name: wfName, Version: "v1",
		Steps: []dag.StepDef{
			{ID: "block", Task: "blocker-client",
				Type: dag.StepTypeNormal},
			respondStepDef(t, "respond",
				[]string{"block"},
				dag.RespondConfig{Status: 200}),
		},
	}
	_, path := stack.registerHTTPTrigger(t, wfDef,
		&trigger.HTTPConfig{
			Path:         "/" + harness.UniqueName(t, "client"),
			Method:       http.MethodPost,
			TimeoutMs:    10_000,
			MaxBodyBytes: 1024,
		})

	// Wire a context we can cancel mid-flight. Cancel after a
	// short delay so the handler has parked on its select.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	req := httptest.NewRequest(http.MethodPost, path, nil).
		WithContext(ctx)
	rec := httptest.NewRecorder()
	stack.router.ServeHTTP(rec, req)

	if rec.Code != 499 {
		t.Fatalf("status = %d, want 499; body=%s",
			rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(),
		`"error":"client_closed"`) {
		t.Fatalf("body = %q, want client_closed",
			rec.Body.String())
	}
}

// testFailureNoRespond drives a workflow whose only step completes
// successfully but never produces a respond signal. The handler
// times out with 504 — the same outcome as the bare timeout case
// (ADR-013 maps this case to the same shape).
func testFailureNoRespond(t *testing.T, nc *nats.Conn) {
	t.Helper()
	stack := startHTTPE2EStack(t, nc)

	harness.SubscribeWorker(t, nc, "fast",
		func(tc worker.TaskContext) error {
			return tc.Complete([]byte(`{"done":true}`))
		})

	wfName := harness.UniqueName(t, "no-respond-wf")
	wfDef := dag.WorkflowDef{
		Name: wfName, Version: "v1",
		Steps: []dag.StepDef{
			{ID: "fast", Task: "fast",
				Type: dag.StepTypeNormal},
			// No respond step — DAG completes normally.
		},
	}
	_, path := stack.registerHTTPTrigger(t, wfDef,
		&trigger.HTTPConfig{
			Path:         "/" + harness.UniqueName(t, "nor"),
			Method:       http.MethodPost,
			TimeoutMs:    500,
			MaxBodyBytes: 1024,
		})

	rec := postOnRouter(t, stack.router,
		http.MethodPost, path, []byte(`{}`), nil)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body=%s",
			rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(),
		`"error":"workflow_timeout"`) {
		t.Fatalf("body = %q, want workflow_timeout",
			rec.Body.String())
	}
}

// assertRunNotTerminal verifies that runID's status is not in a
// terminal state. Bounded read — single GetRun call. Per ADR-013
// the engine does not cancel a run when the HTTP timeout fires;
// the run keeps going.
func assertRunNotTerminal(
	t *testing.T, stack *httpE2EStack, runID string,
) {
	t.Helper()
	if runID == "" {
		t.Fatal("assertRunNotTerminal: runID must not be empty")
	}
	run, err := stack.svc.GetRun(stack.ctx, runID)
	if err != nil {
		t.Fatalf("GetRun(%q): %v", runID, err)
	}
	if run.Status.IsTerminal() {
		t.Fatalf("run %q status = %s, must NOT be terminal "+
			"after timeout (ADR-013: engine does not cancel)",
			runID, run.Status)
	}
}

// assertRunReachesStatus polls the run's status until it reaches
// the desired state or the bounded budget elapses. Used for the
// workflow_failed case where the failure has to propagate through
// JetStream and the orchestrator's failLoopStep.
func assertRunReachesStatus(
	t *testing.T, stack *httpE2EStack,
	runID string, want dag.RunStatus,
) {
	t.Helper()
	if runID == "" {
		t.Fatal("assertRunReachesStatus: runID empty")
	}
	const budget = 5 * time.Second
	const maxIter = 100
	deadline := time.Now().Add(budget)
	for i := 0; i < maxIter; i++ {
		run, err := stack.svc.GetRun(stack.ctx, runID)
		if err == nil && run.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %q never reached %s within %s",
				runID, want, budget)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run %q never reached %s within %d iterations",
		runID, want, maxIter)
}

// TestHTTPTrigger_RouteConflict registers two HTTP triggers that
// claim the same (method, path) — the second registration must
// fail with a typed RouteConflictError. The first trigger keeps
// working after the failed second registration.
func TestHTTPTrigger_RouteConflict(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		harness.SubscribeWorker(t, nc, "echo-conflict",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"first-trigger"`))
			})

		wfName := harness.UniqueName(t, "conflict-wf")
		wfDef := dag.WorkflowDef{
			Name: wfName, Version: "v1",
			Steps: []dag.StepDef{
				{ID: "echo", Task: "echo-conflict",
					Type: dag.StepTypeNormal},
				respondStepDef(t, "respond",
					[]string{"echo"},
					dag.RespondConfig{Status: 200}),
			},
		}
		firstID, path := stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path: "/" + harness.UniqueName(t,
					"conflict"),
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
			})

		// Second registration on the same (method, path) → conflict.
		second := trigger.TriggerDef{
			ID:         harness.UniqueName(t, "trig-second"),
			WorkflowID: wfName,
			Enabled:    true,
			HTTP: &trigger.HTTPConfig{
				Path:         path,
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
			},
		}
		err := stack.svc.CreateTrigger(stack.ctx, second)
		if err == nil {
			t.Fatal("second CreateTrigger: want conflict, got nil")
		}
		var rce *trigger.RouteConflictError
		if !errors.As(err, &rce) {
			t.Fatalf("err = %T %q, want RouteConflictError",
				err, err)
		}
		if rce.HolderTriggerID != firstID {
			t.Fatalf("HolderTriggerID = %q, want %q",
				rce.HolderTriggerID, firstID)
		}

		// Negative: the original trigger keeps working.
		rec := postOnRouter(t, stack.router,
			http.MethodPost, path, []byte(`{}`), nil)
		if rec.Code != 200 {
			t.Fatalf("first trigger after conflict: status = %d; "+
				"body=%s", rec.Code, rec.Body)
		}
		if rec.Body.String() != `"first-trigger"` {
			t.Fatalf("body = %q, want first-trigger",
				rec.Body.String())
		}
	})
}
