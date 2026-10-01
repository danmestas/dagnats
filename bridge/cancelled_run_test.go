// cancelled_run_test.go
// Pins #737: cancelling a run withdraws its tasks from HTTP workers. A
// queued task of a cancelled run was handed out by poll about four
// minutes after the cancel, ran to the end, and its step showed as
// completed under the cancelled run.
//
// Methodology: real NATS, real orchestrator, real API service, real
// bridge over HTTP, the shape of e2e_test.go. Each test starts a
// one-step run, cancels it through the API, and waits until the run
// reads cancelled before acting as the worker. Negative space on every
// path:
//   - the withdrawn task never comes back on a later poll;
//   - the step stays cancelled rather than reading running or completed;
//   - a run that was not cancelled still hands its task out.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/sdk/httpclient"
	"github.com/nats-io/nats.go/jetstream"
)

// cancelFixture is one registered one-step workflow with an orchestrator
// and a bridge in front of it.
type cancelFixture struct {
	svc *api.Service
	ts  *httptest.Server
	js  jetstream.JetStream
}

func newCancelFixture(t *testing.T, workflow string) cancelFixture {
	t.Helper()
	if workflow == "" {
		t.Fatal("newCancelFixture: workflow must not be empty")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	ts := httptest.NewServer(newTestBridge(t, nc).Handler())
	t.Cleanup(ts.Close)
	svc := api.NewService(nc)
	wb := dag.NewWorkflow(workflow)
	wb.Task("job", "echo")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return cancelFixture{svc: svc, ts: ts, js: js}
}

// startQueuedRun starts a run and waits until its step is queued, so the
// task message is on the queue before the test acts.
func (f cancelFixture) startQueuedRun(t *testing.T, workflow string) string {
	t.Helper()
	runID, err := f.svc.StartRun(context.Background(), workflow, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	f.waitForStep(t, runID, dag.StepStatusQueued)
	return runID
}

// cancelAndWait cancels runID and waits until the run reads cancelled.
func (f cancelFixture) cancelAndWait(t *testing.T, runID string) {
	t.Helper()
	if err := f.svc.CancelRun(context.Background(), runID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := f.svc.GetRun(context.Background(), runID)
		if err == nil && run.Status == dag.RunStatusCancelled {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not read cancelled within 5s", runID)
}

// waitForStep waits until runID's job step reads want.
func (f cancelFixture) waitForStep(t *testing.T, runID string, want dag.StepStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := f.svc.GetRun(context.Background(), runID)
		if err == nil && run.Steps["job"].Status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("step job of run %s did not read %v within 5s", runID, want)
}

// requireStepStatus asserts runID's job step reads want right now.
func (f cancelFixture) requireStepStatus(t *testing.T, runID string, want dag.StepStatus) {
	t.Helper()
	run, err := f.svc.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got := run.Steps["job"].Status; got != want {
		t.Fatalf("step job reads %v, want %v", got, want)
	}
}

// pollOnce long-polls echo once and returns the raw JSON array.
func (f cancelFixture) pollOnce(t *testing.T) string {
	t.Helper()
	status, raw := postPollRaw(t, f.ts,
		`{"task_types":["echo"],"max_tasks":1,"timeout_ms":1000}`)
	if status != http.StatusOK {
		t.Fatalf("poll status %d: %s", status, raw)
	}
	return strings.TrimSpace(raw)
}

// resolveRaw posts a resolve body for taskID and returns status and body.
func (f cancelFixture) resolveRaw(t *testing.T, taskID, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(f.ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read resolve body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// TestPollWithholdsCancelledRunsQueuedTask is #737's first test: start a
// run whose step has not been polled, cancel it, poll: no task, then or
// on a later poll.
func TestPollWithholdsCancelledRunsQueuedTask(t *testing.T) {
	f := newCancelFixture(t, "cancel-queued")
	runID := f.startQueuedRun(t, "cancel-queued")
	f.cancelAndWait(t, runID)

	for i := 1; i <= 2; i++ {
		if got := f.pollOnce(t); got != "[]" {
			t.Fatalf("poll %d handed out a cancelled run's task: %s", i, got)
		}
	}
	f.requireStepStatus(t, runID, dag.StepStatusCancelled)
	// Withheld means acked: TASK_QUEUES is a work queue, so the message
	// is gone rather than waiting out AckWait to be withheld again.
	stream, err := f.js.Stream(context.Background(), "TASK_QUEUES")
	if err != nil {
		t.Fatalf("TASK_QUEUES: %v", err)
	}
	_, err = stream.GetLastMsgForSubject(context.Background(), "task.echo."+runID)
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("the withheld task is still queued (err %v)", err)
	}
}

// TestPollStillHandsOutLiveRunsTask is the negative space: the check
// withholds cancelled runs only.
func TestPollStillHandsOutLiveRunsTask(t *testing.T) {
	f := newCancelFixture(t, "cancel-live")
	runID := f.startQueuedRun(t, "cancel-live")
	got := f.pollOnce(t)
	if !strings.Contains(got, `"run_id":"`+runID+`"`) {
		t.Fatalf("a live run's task was not handed out: %s", got)
	}
}

// TestClaimedTaskOfCancelledRunAnswersConflict is #737's second test:
// cancel a run whose task is claimed, heartbeat it: the answer says the
// run is cancelled, and the task is withdrawn so nothing can complete it.
func TestClaimedTaskOfCancelledRunAnswersConflict(t *testing.T) {
	f := newCancelFixture(t, "cancel-claimed")
	runID := f.startQueuedRun(t, "cancel-claimed")
	if got := f.pollOnce(t); !strings.Contains(got, runID) {
		t.Fatalf("the live task was not handed out: %s", got)
	}
	taskID := runID + ".job"
	f.cancelAndWait(t, runID)

	status, body := f.resolveRaw(t, taskID, `{"action":"heartbeat"}`)
	if status != http.StatusConflict || !strings.Contains(body, "run cancelled") {
		t.Fatalf("heartbeat = %d %q, want 409 run cancelled", status, body)
	}
	status, _ = f.resolveRaw(t, taskID, `{"action":"complete","output":{}}`)
	if status != http.StatusNotFound {
		t.Fatalf("complete after the conflict = %d, want 404: the task is withdrawn", status)
	}
	f.requireStepStatus(t, runID, dag.StepStatusCancelled)
}

// TestCompleteOfCancelledRunIsRefused: a worker that skips heartbeats
// and resolves complete is refused the same way, and the step is never
// recorded as completed.
func TestCompleteOfCancelledRunIsRefused(t *testing.T) {
	f := newCancelFixture(t, "cancel-complete")
	runID := f.startQueuedRun(t, "cancel-complete")
	if got := f.pollOnce(t); !strings.Contains(got, runID) {
		t.Fatalf("the live task was not handed out: %s", got)
	}
	f.cancelAndWait(t, runID)

	status, body := f.resolveRaw(t, runID+".job", `{"action":"complete","output":{}}`)
	if status != http.StatusConflict || !strings.Contains(body, "run cancelled") {
		t.Fatalf("complete = %d %q, want 409 run cancelled", status, body)
	}
	f.requireStepStatus(t, runID, dag.StepStatusCancelled)
}

// TestSDKResolveOfCancelledRunReturnsErrRunCancelled: a Go worker on
// sdk/httpclient sees the 409 as ErrRunCancelled, which it can test for
// with errors.Is to stop the work. Negative space: before the cancel the
// same client polls the task normally.
func TestSDKResolveOfCancelledRunReturnsErrRunCancelled(t *testing.T) {
	f := newCancelFixture(t, "cancel-sdk")
	runID := f.startQueuedRun(t, "cancel-sdk")
	client := httpclient.New(f.ts.URL)
	tasks, err := client.Poll(context.Background(), []string{"echo"}, 1, sdkPollTimeout)
	if err != nil || len(tasks) != 1 || tasks[0].RunID != runID {
		t.Fatalf("poll = %v, %v; want the live run's task", tasks, err)
	}
	f.cancelAndWait(t, runID)

	err = client.Complete(context.Background(), runID+".job", json.RawMessage(`{}`))
	if !errors.Is(err, httpclient.ErrRunCancelled) {
		t.Fatalf("Complete error = %v, want ErrRunCancelled", err)
	}
}
