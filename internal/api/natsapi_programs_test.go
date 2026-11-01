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
	"errors"
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
	reply, err := connFor(t, nc, subject).Request(subject, body, 5*time.Second)
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
	// No program can own an invalid name's inbox, so its request is dropped.
	for _, subject := range []string{"api.p.Notify.runs.get", "api.p.n.runs.get"} {
		_, err := nc.Request(subject, []byte("x"), 500*time.Millisecond)
		if !errors.Is(err, nats.ErrTimeout) {
			t.Errorf("%s: err = %v, want no answer", subject, err)
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

// TestScopedRegisterConfinesEveryStepType: a program may not dispatch to,
// wait on, or be cancelled by anything outside its prefix, through any
// step type or workflow field, and may not use planner steps at all.
func TestScopedRegisterConfinesEveryStepType(t *testing.T) {
	nc := startScopedAPI(t)
	match := dag.Match{Left: "task_id", Op: dag.MatchOpEq, Right: "input.task_id"}
	type builder = *dag.WorkflowBuilder
	cases := map[string]func(builder){
		"normal task": func(b builder) { b.Task("a", "artifact.agent") },
		"agent":       func(b builder) { b.Agent("a", "artifact.agent", nil) },
		"agent loop": func(b builder) {
			b.AgentLoop("a", "artifact.agent").WithMaxIterations(3)
		},
		"map": func(b builder) {
			b.Map("a", "artifact.agent").After(b.Task("src", "notify.src"))
		},
		"planner task": func(b builder) {
			b.Planner("a", "artifact.plan", plannerCfg("notify.x"))
		},
		"planner open": func(b builder) { b.Planner("a", "notify.plan", plannerCfg()) },
		"planner allows": func(b builder) {
			b.Planner("a", "notify.plan", plannerCfg("notify.x", "artifact.x"))
		},
		// Refused even fully confined: its fragments are not (yet).
		"planner confined": func(b builder) {
			b.Planner("a", "notify.plan", plannerCfg("notify.x"))
		},
		"wait event": func(b builder) { b.WaitForEvent("a", waitOpts("artifact.done", match)) },
		"cancel on": func(b builder) {
			b.Task("a", "notify.work")
			b.CancelOn("artifact.abort", match)
		},
	}
	for name, add := range cases {
		wb := dag.NewWorkflow("notify.case")
		add(wb)
		if replyField(t, registerScoped(t, nc, "notify", wb), "error") == "" {
			t.Errorf("%s: a step outside notify. was registered", name)
		}
	}
	allowed := dag.NewWorkflow("notify.ok")
	first := allowed.Agent("first", "notify.think", nil)
	wait := allowed.WaitForEvent("wait", waitOpts("notify.done", match))
	wait.After(first)
	allowed.Map("fan", "notify.item").After(wait)
	allowed.CancelOn("notify.abort", match)
	reply := registerScoped(t, nc, "notify", allowed)
	if replyField(t, reply, "status") != "registered" {
		t.Fatalf("a fully confined workflow was refused: %s", reply)
	}
}

func waitOpts(event string, match dag.Match) dag.WaitForEventOpts {
	return dag.WaitForEventOpts{Event: event, Match: match, Timeout: time.Minute}
}

func plannerCfg(allowed ...string) dag.PlannerConfig {
	return dag.PlannerConfig{MaxSteps: 5, AllowedTasks: allowed}
}

// TestScopedRegisterRefusesGrantedWorkflows: a workflow the operator's
// policy grants (or lets promote) is registered by the operator, so a
// program cannot swap new steps under an existing grant.
func TestScopedRegisterRefusesGrantedWorkflows(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	holder := &engine.GrantPolicyHolder{}
	holder.Store(engine.NewGrantPolicy(
		[]string{"notify.deploy"}, []string{"notify.promote"}))
	svc := NewServiceWithLimits(nc, RuntimeLimits{}, WithGrantPolicyHolder(holder))
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	for _, name := range []string{"notify.deploy", "notify.promote"} {
		wb := dag.NewWorkflow(name)
		wb.Task("a", "notify.work")
		if replyField(t, registerScoped(t, nc, "notify", wb), "error") == "" {
			t.Errorf("program registered granted workflow %s", name)
		}
	}
	plain := dag.NewWorkflow("notify.plain")
	plain.Task("a", "notify.work")
	if replyField(t, registerScoped(t, nc, "notify", plain), "status") != "registered" {
		t.Fatal("an ungranted workflow was refused")
	}
}

// TestConfineDefSubWorkflowBarePrefix: a sub-workflow target equal to the
// bare prefix is not under it. The builder cannot produce one (its task
// would be invalid), so the def is edited after Build.
func TestConfineDefSubWorkflowBarePrefix(t *testing.T) {
	wb := dag.NewWorkflow("notify.wrap")
	wb.SubWorkflow("inner", "notify.child")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := confineDef(def, "notify."); err != nil {
		t.Fatalf("a confined sub-workflow was refused: %v", err)
	}
	def.Steps[0].Config = dag.MarshalConfig(&dag.SubWorkflowConfig{Workflow: "notify."})
	if err := confineDef(def, "notify."); err == nil {
		t.Fatal("a sub-workflow targeting the bare prefix was accepted")
	}
}
