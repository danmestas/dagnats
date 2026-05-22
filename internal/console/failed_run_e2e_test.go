// failed_run_e2e_test.go
//
// Methodology: end-to-end live verification of the T05 failed-run
// banner. Boots a real embedded NATS server + orchestrator + API
// service via dagnatstest.NewHarness, registers a single-step
// workflow and a worker handler that calls FailPermanent, then
// drives a real run through the engine until it reaches
// dag.RunStatusFailed. Mounts the live console.Mount handler with a
// real NewAPIDataSource (no fake) and GETs /console/runs/<id>, then
// asserts the run-error-banner DOM is present and that its
// jump-to-step anchor targets the failed step.
//
// Why a real engine (not a pre-baked fake): per issue #303's
// Ousterhout audit, the existing T05 fixture tests in pages_test.go
// already prove the template renders given pre-baked FailedStepID
// state. The gap they leave is whether the *production* path from
// "engine emits step.failed" to "console pulls run.Status==Failed
// and finds the failed step in the def" actually closes. Driving a
// real failure through a real worker is the only way to exercise
// that wire. Reuses the noop-worker shape from #282 (cli/noop_worker
// .go) but inlines it because runDemoSeed and demoWorkflowName are
// unexported in package cli.
//
// Bounds:
//   - failed-run wait: 5s (matches the ≤5s constraint in #303).
//   - HTTP read: 2s.
//   - Overall test budget: well under 10s.
//
// Assertions:
//  1. The run-error-banner DOM element is present in the rendered
//     /console/runs/<id> body.
//  2. The banner's jump-to-step anchor targets #step-row-<failedStepID>
//     where failedStepID matches the step in the workflow definition
//     that actually failed.
package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/worker"
)

// TestFailedRunBannerEndToEnd drives a real failure through the
// engine + worker and asserts the run-detail page renders the
// failed-run banner pointing at the step that actually failed.
func TestFailedRunBannerEndToEnd(t *testing.T) {
	const (
		workflowName  = "e2e-failed-run"
		taskType      = "e2e-failed-run-task"
		failedStepID  = "noop"
		runWaitBudget = 5 * time.Second
	)

	h := dagnatstest.NewHarness(t)

	// Register a handler that drives the step to failed via
	// FailPermanent. This is the production path the engine takes
	// for a real worker failure — same code path #282's noop worker
	// uses for its planned-failed outcomes.
	h.Handle(t, taskType, func(tc worker.TaskContext) error {
		return tc.FailPermanent(
			errors.New("e2e: planned failure for banner verification"),
		)
	})
	h.Start(t)

	// Build + register the single-step workflow. Step ID "noop" is
	// what the banner anchor must target (#step-row-noop).
	wb := dag.NewWorkflow(workflowName)
	wb.Task(failedStepID, taskType)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build workflow: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer regCancel()
	if err := h.Svc.RegisterWorkflow(regCtx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer startCancel()
	runID, err := h.Svc.StartRun(startCtx, workflowName, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if runID == "" {
		t.Fatal("StartRun returned empty runID")
	}

	// Bounded wait for the run to reach the terminal Failed state.
	// Polling cadence matches the noop-worker waitForTerminal
	// (cli/noop_worker.go) — 25ms is fast enough that the engine
	// rarely makes us wait more than a few iterations under no load.
	waitForFailedRun(t, h, runID, runWaitBudget)

	// Mount the production console handler with a real
	// NewAPIDataSource — no fake. This is the wire #303 wants
	// live-verified.
	cfg := Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data: NewAPIDataSource(
			h.Svc, h.NC, nil,
			slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		),
	}
	handler := Mount(cfg)

	body := getRunDetailBody(t, handler, runID)

	// Assertion 1 (positive): the banner element is in the DOM.
	// Matches the production template marker in
	// templates/components/run_error_banner.html — class
	// "run-error-banner" lives on the alert root and is the
	// load-bearing selector both the CSS and the existing T05
	// fixture test pin on.
	const bannerMarker = `class="alert alert-destructive run-error-banner"`
	if !strings.Contains(body, bannerMarker) {
		t.Fatalf(
			"run-error-banner missing from /console/runs/%s body."+
				" Expected substring: %q.\nBody:\n%s",
			runID, bannerMarker, truncateBody(body, 4000),
		)
	}

	// Assertion 2 (anchor target): the jump-to-step link points at
	// the step that actually failed in the run. The template emits
	// `href="#step-row-{{.FailedStepID}}"`. We reverse the wire by
	// asserting the href substring matches our known failedStepID.
	wantAnchor := fmt.Sprintf(
		`href="#step-row-%s"`, failedStepID,
	)
	if !strings.Contains(body, wantAnchor) {
		t.Fatalf(
			"jump-to-step anchor missing or wrong in /console/runs/%s."+
				" Expected substring: %q.\nBody:\n%s",
			runID, wantAnchor, truncateBody(body, 4000),
		)
	}
}

// waitForFailedRun polls h.Svc.GetRun until the run reaches the
// dag.RunStatusFailed terminal state or the budget elapses.
// Failing the test on timeout (rather than returning a status) keeps
// the assertion site at the call point; downstream rendering checks
// can assume the run is Failed when they execute.
func waitForFailedRun(
	t *testing.T, h *dagnatstest.Harness, runID string,
	budget time.Duration,
) {
	t.Helper()
	if h == nil {
		t.Fatal("waitForFailedRun: harness is nil")
	}
	if runID == "" {
		t.Fatal("waitForFailedRun: runID is empty")
	}
	if budget <= 0 {
		t.Fatal("waitForFailedRun: budget must be positive")
	}
	deadline := time.Now().Add(budget)
	// Bounded outer loop: cap iteration count even if the clock
	// misbehaves under heavy load. budget/25ms gives ~200 iters at
	// 5s; the +50 slack absorbs scheduling jitter.
	const pollSleep = 25 * time.Millisecond
	const maxIters = 250
	for i := 0; i < maxIters; i++ {
		if time.Now().After(deadline) {
			break
		}
		ctx, cancel := context.WithTimeout(
			context.Background(), 1*time.Second,
		)
		run, err := h.Svc.GetRun(ctx, runID)
		cancel()
		if err == nil && run.Status == dag.RunStatusFailed {
			return
		}
		time.Sleep(pollSleep)
	}
	// Surface the actual final state to aid debugging on flake.
	ctx, cancel := context.WithTimeout(
		context.Background(), 1*time.Second,
	)
	defer cancel()
	run, err := h.Svc.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf(
			"run %s did not reach Failed within %v; final GetRun"+
				" error: %v", runID, budget, err,
		)
	}
	t.Fatalf(
		"run %s did not reach Failed within %v; final status=%s"+
			" steps=%+v", runID, budget, run.Status, run.Steps,
	)
}

// getRunDetailBody GETs /console/runs/<id> against the in-test
// handler and returns the body. Asserts 200; fails the test
// otherwise. Bounded by a short read deadline so a hung response
// can't wedge the test.
func getRunDetailBody(
	t *testing.T, handler http.Handler, runID string,
) string {
	t.Helper()
	if handler == nil {
		t.Fatal("getRunDetailBody: handler is nil")
	}
	if runID == "" {
		t.Fatal("getRunDetailBody: runID is empty")
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet, "/console/runs/"+runID, nil,
	)
	// Apply a context deadline; httptest's recorder doesn't enforce
	// the deadline itself but the underlying handler honors it for
	// any blocking ds calls.
	ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf(
			"GET /console/runs/%s status = %d, want 200; body=%s",
			runID, rr.Code, rr.Body.String(),
		)
	}
	b, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(b)
}

// truncateBody bounds failure output. Kept local to the file because
// the equivalent helper in server/console_wiring_test.go lives in a
// different package.
func truncateBody(s string, n int) string {
	if n <= 0 {
		panic("truncateBody: n must be positive")
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(truncated)"
}
