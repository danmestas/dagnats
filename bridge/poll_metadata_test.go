// poll_metadata_test.go
// Pins #732: a task polled over HTTP carries its step's Metadata, as
// TaskPayload does for NATS workers, so a jobs: workflow's "ci.job"
// (#728) reaches an HTTP worker.
//
// Methodology: real NATS server and bridge. Publish a task message whose
// payload carries Metadata, then poll it twice over: as raw JSON (the
// wire contract quarry's runner reads) and through sdk/httpclient (the
// Go SDK decode path). Negative space: a task with no metadata has no
// "metadata" key at all, so responses for ordinary steps are unchanged.
// The end-to-end test runs the whole path: a jobs: spec compiled with a
// namespace, started through the real orchestrator, and polled from its
// worker group over HTTP.
package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/ci"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/sdk/httpclient"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const pollMetadataJob = `{"name":"test","steps":[{"run":"go test ./..."}]}`

// publishTaskWithMetadata publishes an echo task for runID whose
// payload carries metadata (nil for none).
func publishTaskWithMetadata(
	t *testing.T, nc *nats.Conn, runID string, metadata map[string]string,
) {
	t.Helper()
	if nc == nil || runID == "" {
		t.Fatal("publishTaskWithMetadata: nc and runID are required")
	}
	data, err := json.Marshal(protocol.TaskPayload{
		RunID: runID, StepID: "job", Input: json.RawMessage(`{}`),
		Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.PublishMsg(context.Background(), &nats.Msg{
		Subject: "task.echo." + runID, Data: data,
	}); err != nil {
		t.Fatalf("publish task msg: %v", err)
	}
}

// pollRawTask polls one echo task over HTTP and returns it as a generic
// JSON object, so the test sees the wire keys exactly.
func pollRawTask(t *testing.T, baseURL string) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"task_types": []string{"echo"}, "max_tasks": 1, "timeout_ms": 3000,
	})
	if err != nil {
		t.Fatalf("marshal poll request: %v", err)
	}
	resp, err := http.Post(baseURL+"/v1/tasks/poll", "application/json",
		bytes.NewReader(body))
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("poll status %d err %v: %s", resp.StatusCode, err, raw)
	}
	var tasks []map[string]any
	if err := json.Unmarshal(raw, &tasks); err != nil {
		t.Fatalf("decode poll response: %v (%s)", err, raw)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1: %s", len(tasks), raw)
	}
	return tasks[0]
}

func newMetadataTestServer(t *testing.T) (*nats.Conn, *httptest.Server) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes)); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ts := httptest.NewServer(newTestBridge(t, nc).Handler())
	t.Cleanup(ts.Close)
	return nc, ts
}

func TestPollCarriesStepMetadata(t *testing.T) {
	nc, ts := newMetadataTestServer(t)
	publishTaskWithMetadata(t, nc, "run-meta", map[string]string{"ci.job": pollMetadataJob})

	task := pollRawTask(t, ts.URL)
	metadata, ok := task["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("polled task has no metadata object: %v", task)
	}
	if metadata["ci.job"] != pollMetadataJob {
		t.Fatalf(`metadata["ci.job"] = %v, want %s`, metadata["ci.job"], pollMetadataJob)
	}
}

func TestPollOmitsMetadataWhenStepHasNone(t *testing.T) {
	nc, ts := newMetadataTestServer(t)
	publishTaskWithMetadata(t, nc, "run-nometa", nil)

	task := pollRawTask(t, ts.URL)
	if _, present := task["metadata"]; present {
		t.Fatalf("a step without metadata grew a metadata key: %v", task)
	}
	if task["step_id"] != "job" {
		t.Fatalf("step_id = %v, want job", task["step_id"])
	}
}

func TestSDKPollDecodesStepMetadata(t *testing.T) {
	nc, ts := newMetadataTestServer(t)
	publishTaskWithMetadata(t, nc, "run-sdkmeta", map[string]string{"ci.job": pollMetadataJob})

	tasks, err := httpclient.New(ts.URL).Poll(
		context.Background(), []string{"echo"}, 1, sdkPollTimeout,
	)
	if err != nil {
		t.Fatalf("client.Poll: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
	if got := tasks[0].Metadata["ci.job"]; got != pollMetadataJob {
		t.Fatalf(`TaskPayload.Metadata["ci.job"] = %q, want %s`, got, pollMetadataJob)
	}
}

// TestJobsWorkflowJobReachesHTTPWorker is #732's end-to-end case: the
// job a jobs: spec compiles into Metadata["ci.job"] arrives, verbatim, in
// the task an HTTP worker polls from the job's worker group.
func TestJobsWorkflowJobReachesHTTPWorker(t *testing.T) {
	nc, ts := newMetadataTestServer(t)
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	spec := "jobs:\n  test:\n    steps:\n      - run: go test ./...\n"
	compiled, diags := ci.CompileYAMLWith("jobs-e2e", []byte(spec),
		ci.CompileOptions{TaskNamespace: "repo"})
	if len(diags) != 0 {
		t.Fatalf("compile: %+v", diags)
	}
	wantJob := compiled.Workflow.Steps[0].Metadata["ci.job"]
	if wantJob == "" {
		t.Fatal("the compiled step carries no ci.job")
	}
	svc := api.NewService(nc)
	ctx := context.Background()
	if err := svc.RegisterWorkflow(ctx, compiled.Workflow); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	if _, err := svc.StartRun(ctx, "jobs-e2e", nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	task := pollGroupedTaskWithin(t, ts, "repo.job", "repo", 10*time.Second)
	metadata, ok := task["metadata"].(map[string]any)
	if !ok || metadata["ci.job"] != wantJob {
		t.Fatalf("polled task metadata = %v, want ci.job %s", task["metadata"], wantJob)
	}
	if task["step_id"] != "test" {
		t.Fatalf("step_id = %v, want the job id test", task["step_id"])
	}
}

// pollGroupedTaskWithin polls one task of taskType from group until it
// arrives or the bound passes; dispatch is asynchronous after StartRun.
func pollGroupedTaskWithin(
	t *testing.T, ts *httptest.Server, taskType, group string, bound time.Duration,
) map[string]any {
	t.Helper()
	if bound <= 0 {
		t.Fatal("pollGroupedTaskWithin: bound must be positive")
	}
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		status, raw := pollGroupedRaw(t, ts, taskType, group)
		if status != http.StatusOK {
			t.Fatalf("poll status %d: %s", status, raw)
		}
		var tasks []map[string]any
		if err := json.Unmarshal([]byte(raw), &tasks); err != nil {
			t.Fatalf("decode poll response: %v (%s)", err, raw)
		}
		if len(tasks) == 1 {
			return tasks[0]
		}
	}
	t.Fatalf("no %s task in group %s within %v", taskType, group, bound)
	return nil
}
