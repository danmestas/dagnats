// natsapi_programs_test.go
// Tests for the program-scoped control plane (#885): api.p.<name>.
// workflows.register, runs.start, runs.get and runs.cancel. A program
// may only register and start workflows under "<name>.", may not point
// a sub-workflow step outside it, and sees another program's run
// exactly as it sees a run that does not exist.
// Methodology: real embedded NATS, a real orchestrator, and requests on
// the scoped subjects (which a program's NATS permissions confine it
// to); the name always comes from the subject.
package api

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

func startScopedAPI(t *testing.T) *nats.Conn {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)
	natsAPI := NewNATSAPI(NewService(nc), nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	return nc
}

func scopedRequest(t *testing.T, nc *nats.Conn, subject string, body []byte) []byte {
	t.Helper()
	reply, err := nc.Request(subject, body, 5*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}
	return reply.Data
}

func replyField(t *testing.T, data []byte, field string) string {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	value, _ := out[field].(string)
	return value
}

func registerScoped(t *testing.T, nc *nats.Conn, program string, wb *dag.WorkflowBuilder) []byte {
	t.Helper()
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return scopedRequest(t, nc, "api.p."+program+".workflows.register", mustMarshal(t, def))
}

func startScoped(t *testing.T, nc *nats.Conn, program, workflow string) []byte {
	t.Helper()
	body := mustMarshal(t, startRunRequest{Workflow: workflow})
	return scopedRequest(t, nc, "api.p."+program+".runs.start", body)
}

func TestScopedRegisterStaysUnderPrefix(t *testing.T) {
	nc := startScopedAPI(t)
	own := dag.NewWorkflow("notify.send")
	own.Task("a", "notify.work")
	if got := replyField(t, registerScoped(t, nc, "notify", own), "status"); got != "registered" {
		t.Fatal("a workflow under the prefix was not registered")
	}
	outside := []string{"artifact.fetch", "notifyx.send", "notify"}
	for _, name := range outside {
		wb := dag.NewWorkflow(name)
		wb.Task("a", "notify.work")
		if replyField(t, registerScoped(t, nc, "notify", wb), "error") == "" {
			t.Errorf("notify registered %q", name)
		}
	}
	child := dag.NewWorkflow("notify.wrap")
	child.SubWorkflow("inner", "artifact.fetch")
	if replyField(t, registerScoped(t, nc, "notify", child), "error") == "" {
		t.Fatal("notify registered a sub-workflow step into artifact.")
	}
}

func TestScopedStartGetCancel(t *testing.T) {
	nc := startScopedAPI(t)
	for _, program := range []string{"notify", "artifact"} {
		wb := dag.NewWorkflow(program + ".slow")
		wb.Task("a", program+".never") // no worker: the run stays running
		registerScoped(t, nc, program, wb)
	}
	if replyField(t, startScoped(t, nc, "notify", "artifact.slow"), "error") == "" {
		t.Fatal("notify started artifact's workflow")
	}
	notifyRun := replyField(t, startScoped(t, nc, "notify", "notify.slow"), "run_id")
	artifactRun := replyField(t, startScoped(t, nc, "artifact", "artifact.slow"), "run_id")
	if notifyRun == "" || artifactRun == "" {
		t.Fatal("scoped start returned no run id")
	}
	waitScopedRun(t, nc, "notify", notifyRun)

	unknown := scopedRequest(t, nc, "api.p.notify.runs.get", []byte("nosuchrun"))
	foreign := scopedRequest(t, nc, "api.p.notify.runs.get", []byte(artifactRun))
	if !bytes.Equal(unknown, foreign) || replyField(t, foreign, "error") == "" {
		t.Fatalf("foreign run %s differs from unknown run %s", foreign, unknown)
	}
	foreignCancel := scopedRequest(t, nc, "api.p.notify.runs.cancel", []byte(artifactRun))
	unknownCancel := scopedRequest(t, nc, "api.p.notify.runs.cancel", []byte("nosuchrun"))
	if !bytes.Equal(foreignCancel, unknownCancel) {
		t.Fatalf("foreign cancel %s differs from unknown cancel %s", foreignCancel, unknownCancel)
	}

	cancelled := scopedRequest(t, nc, "api.p.notify.runs.cancel", []byte(notifyRun))
	if replyField(t, cancelled, "status") != "cancelled" {
		t.Fatalf("own cancel = %s", cancelled)
	}
	waitScopedStatus(t, nc, "notify", notifyRun, dag.RunStatusCancelled)
	status, found := scopedRunStatus(t, nc, "artifact", artifactRun)
	if !found || status == dag.RunStatusCancelled {
		t.Fatal("notify's foreign cancel reached artifact's run")
	}
}

func TestScopedRejectsInvalidProgramName(t *testing.T) {
	nc := startScopedAPI(t)
	for _, subject := range []string{"api.p.Notify.runs.get", "api.p.n.runs.get"} {
		if replyField(t, scopedRequest(t, nc, subject, []byte("x")), "error") == "" {
			t.Errorf("%s answered without an error", subject)
		}
	}
	if replyField(t, scopedRequest(t, nc, "api.p.notify.runs.get", nil), "error") == "" {
		t.Fatal("an empty run id was answered without an error")
	}
}

func scopedRunStatus(t *testing.T, nc *nats.Conn, program, runID string) (dag.RunStatus, bool) {
	t.Helper()
	data := scopedRequest(t, nc, "api.p."+program+".runs.get", []byte(runID))
	var run dag.WorkflowRun
	if err := json.Unmarshal(data, &run); err != nil || run.RunID != runID {
		return 0, false
	}
	return run.Status, true
}

func waitScopedRun(t *testing.T, nc *nats.Conn, program, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, found := scopedRunStatus(t, nc, program, runID); found {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s never appeared", runID)
}

func waitScopedStatus(t *testing.T, nc *nats.Conn, program, runID string, want dag.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if status, _ := scopedRunStatus(t, nc, program, runID); status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %s", runID, want)
}
