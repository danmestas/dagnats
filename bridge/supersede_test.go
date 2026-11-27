// supersede_test.go
// Pins #1031's scenario with what dagnats already has: a workflow whose
// singleton is {mode: cancel, key: <input path>} keeps only the newest run
// per key, and the bridge withholds a cancelled run's queued task (#737),
// so a worker with one slot is handed the newest task for a key, never a
// superseded one.
//
// Methodology: real NATS, real orchestrator, real API service, real bridge
// over HTTP. Two runs are started for the same branch, both before any
// worker polls; then one worker polls for one task. Negative space: the
// superseded run reads cancelled, its task is gone from the queue, and a
// run for another branch is not cancelled by either.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSingletonCancelHandsOutOnlyTheNewestTaskPerKey(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	ts := httptest.NewServer(newTestBridge(t, nc).Handler())
	t.Cleanup(ts.Close)
	svc := api.NewService(nc)
	wb := dag.NewWorkflow("push-ci").WithSingletonKey(dag.SingletonModeCancel, "branch")
	wb.Task("job", "echo")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := context.Background()
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	start := func(branch string) string {
		t.Helper()
		id, err := svc.StartRun(ctx, "push-ci", json.RawMessage(`{"branch":"`+branch+`"}`))
		if err != nil {
			t.Fatalf("StartRun(%s): %v", branch, err)
		}
		return id
	}
	old := start("main")
	waitRunStatus(t, svc, old, dag.RunStatusRunning)
	other := start("feature")
	tip := start("main")
	waitRunStatus(t, svc, old, dag.RunStatusCancelled)

	status, raw := postPollRaw(t, ts, `{"task_types":["echo"],"max_tasks":1,"timeout_ms":3000}`)
	if status != http.StatusOK {
		t.Fatalf("poll status %d: %s", status, raw)
	}
	if strings.Contains(raw, old) {
		t.Fatalf("poll handed out the superseded run %s: %s", old, raw)
	}
	if !strings.Contains(raw, tip) && !strings.Contains(raw, other) {
		t.Fatalf("poll handed out neither live run: %s", raw)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("TASK_QUEUES: %v", err)
	}
	if _, err := stream.GetLastMsgForSubject(ctx, "task.echo."+old); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("the superseded run's task is still queued (err %v)", err)
	}
	for _, live := range []string{tip, other} {
		run, err := svc.GetRun(ctx, live)
		if err != nil || run.Status == dag.RunStatusCancelled {
			t.Fatalf("run %s was cancelled or unreadable (%v): only the same key supersedes", live, err)
		}
	}
}
