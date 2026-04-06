// e2e_test.go
// End-to-end test: an HTTP-only worker completes a DagNats workflow
// through the bridge. No native NATS worker is used — all task
// execution goes through HTTP poll + resolve.
// Methodology: real NATS, real orchestrator, real bridge, httptest.
package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestBridgeE2EWorkflowCompletion(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	ctx := context.Background()

	// Start orchestrator
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Create bridge and HTTP server
	b := NewBridge(nc, nil)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Register a simple one-step workflow
	svc := api.NewService(nc)
	wb := dag.NewWorkflow("bridge-e2e")
	wb.Task("echo-step", "echo")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if err := svc.RegisterWorkflow(ctx, wfDef); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}

	// Start the workflow run
	runID, err := svc.StartRun(ctx, "bridge-e2e", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	// HTTP worker: poll for the task
	task := pollForTask(t, ts.URL, 10*time.Second)

	// Verify the polled task belongs to our run
	if task.RunID != runID {
		t.Fatalf(
			"expected run_id %s, got %s", runID, task.RunID,
		)
	}

	// HTTP worker: resolve as complete
	resolveTask(t, ts.URL, task.TaskID, `{
		"action":"complete",
		"output":{"message":"done via bridge"}
	}`)

	// Wait for workflow completion
	waitForCompletion(t, svc, ctx, runID, 10*time.Second)
}

// pollForTask polls the bridge until a task is available.
// Bounded by deadline.
func pollForTask(
	t *testing.T, baseURL string, deadline time.Duration,
) pollResponse {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()

	for {
		body := `{
			"task_types":["echo"],
			"max_tasks":1,
			"timeout_ms":2000
		}`
		resp, err := http.Post(
			baseURL+"/v1/tasks/poll",
			"application/json",
			strings.NewReader(body),
		)
		if err != nil {
			t.Fatalf("poll failed: %v", err)
		}

		var tasks []pollResponse
		if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
			resp.Body.Close()
			t.Fatalf("decode failed: %v", err)
		}
		resp.Body.Close()

		if len(tasks) > 0 {
			return tasks[0]
		}

		select {
		case <-timer.C:
			t.Fatal("timed out waiting for task")
		default:
		}
	}
}

// resolveTask sends a resolve request for the given task.
func resolveTask(
	t *testing.T, baseURL, taskID, body string,
) {
	t.Helper()
	resp, err := http.Post(
		baseURL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resolve returned %d", resp.StatusCode)
	}
}

// waitForCompletion polls run status until completed or deadline.
func waitForCompletion(
	t *testing.T,
	svc *api.Service,
	ctx context.Context,
	runID string,
	deadline time.Duration,
) {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()

	for {
		run, err := svc.GetRun(ctx, runID)
		if err == engine.ErrRunNotFound {
			select {
			case <-timer.C:
				t.Fatal("timed out waiting for run to appear")
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			t.Fatalf("GetRun failed: %v", err)
		}
		if run.Status == dag.RunStatusCompleted {
			// Paired assertion: verify step is completed too
			if run.Steps["echo-step"].Status !=
				dag.StepStatusCompleted {
				t.Fatalf(
					"step status = %v, want Completed",
					run.Steps["echo-step"].Status,
				)
			}
			return
		}
		if run.Status == dag.RunStatusFailed {
			t.Fatal("workflow failed unexpectedly")
		}
		select {
		case <-timer.C:
			t.Fatalf(
				"workflow did not complete, status: %v",
				run.Status,
			)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
