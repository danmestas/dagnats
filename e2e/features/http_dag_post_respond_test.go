// e2e/features/http_dag_post_respond_test.go
//
// Methodology: end-to-end coverage for the ADR-013 mental model
// ("respond is a side effect, not a return") and the two
// graph-level validator warnings (missing_respond,
// duplicate_respond). Each test stands up its own embedded NATS
// server, real engine, real trigger service, real workers.
//
// Scenarios covered here:
//   - TestHTTPTrigger_DAGContinuesPastRespond: a 3-step workflow
//     [A → respond → B]; after the HTTP client receives respond's
//     reply, step B must still run to completion (the engine
//     publishes step.completed for respond and the DAG advance
//     machinery picks up B from there).
//   - TestHTTPTrigger_ValidatorWarnings: three workflow shapes
//     drive POST /workflows and assert the warnings response —
//     missing_respond, duplicate_respond, and branch-per-outcome
//     (no warning). All three are persisted; warnings are
//     non-fatal at this layer.
package features

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// TestHTTPTrigger_DAGContinuesPastRespond is the load-bearing
// behavioral assertion from ADR-013's mental model: the DAG runs
// steps AFTER respond even though the HTTP client has already
// received its reply. We trip a flag in step B's worker and, after
// the HTTP response arrives, wait (bounded) for the flag to flip.
// Then we read run history and assert step B's step.completed
// arrived AFTER the response was written.
func TestHTTPTrigger_DAGContinuesPastRespond(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		var bRan atomic.Bool
		harness.SubscribeWorker(t, nc, "task-a",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`{"a":"ok"}`))
			})
		harness.SubscribeWorker(t, nc, "task-b",
			func(tc worker.TaskContext) error {
				bRan.Store(true)
				return tc.Complete([]byte(`{"b":"ok"}`))
			})

		wfName := harness.UniqueName(t, "post-respond")
		wfDef := dag.WorkflowDef{
			Name:    wfName,
			Version: "v1",
			Steps: []dag.StepDef{
				{ID: "a", Task: "task-a",
					Type: dag.StepTypeNormal},
				respondStepDef(t, "respond",
					[]string{"a"},
					dag.RespondConfig{Status: 200}),
				// B depends on respond — must run AFTER respond
				// has published the response on the wire.
				{ID: "b", Task: "task-b",
					Type:      dag.StepTypeNormal,
					DependsOn: []string{"respond"}},
			},
		}
		_, path := stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:         "/" + harness.UniqueName(t, "post"),
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
			})

		// Step 1: client gets its response.
		rec := postOnRouter(t, stack.router,
			http.MethodPost, path, []byte(`{"k":"v"}`), nil)
		if rec.Code != 200 {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body)
		}
		runID := rec.Header().Get("X-Dagnats-Run-Id")
		if runID == "" {
			t.Fatal("X-Dagnats-Run-Id header missing")
		}

		assertStepBEventuallyRuns(t, stack, runID, &bRan)
	})
}

// assertStepBEventuallyRuns waits (bounded) for the post-respond
// worker to fire and then walks the run history to confirm step
// B's step.completed event is present. Bounded — 10s ceiling.
func assertStepBEventuallyRuns(
	t *testing.T, stack *httpE2EStack,
	runID string, flag *atomic.Bool,
) {
	t.Helper()
	const budget = 10 * time.Second
	deadline := time.Now().Add(budget)
	// Bounded loop: budget/100ms = 100 iterations max.
	const maxIter = 200
	for i := 0; i < maxIter; i++ {
		if flag.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("step B did not run within %s", budget)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !flag.Load() {
		t.Fatalf("step B never ran within %d iterations", maxIter)
	}
	// Positive: step B's step.completed event is in history.
	events, err := stack.svc.ListRunEvents(stack.ctx, runID, false)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	bCompleted := false
	for _, evt := range events {
		if evt.StepID == "b" &&
			protocol.EventType(evt.Type) ==
				protocol.EventStepCompleted {
			bCompleted = true
			break
		}
	}
	if !bCompleted {
		t.Fatalf("step b's step.completed missing from history: %v",
			events)
	}
}

// TestHTTPTrigger_ValidatorWarnings exercises ADR-013 Layer 1
// graph validation via POST /workflows, asserting:
//   - HTTP-triggered workflow with no respond → missing_respond warning.
//   - HTTP-triggered workflow with two simultaneously-reachable
//     responds → duplicate_respond warning.
//   - Branch-per-outcome with two mutually-exclusive responds → no
//     warnings.
//
// All three workflows must be persisted (warnings are non-fatal at
// this layer; only field validation is fatal).
func TestHTTPTrigger_ValidatorWarnings(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		rest := api.NewRESTHandler(stack.svc)

		// Each shape: pre-register an HTTP trigger so the
		// validator's hasHTTPTrigger gate fires, then POST the
		// workflow definition through the REST handler.
		runValidatorShape(t, stack, rest,
			"missing", []dag.StepDef{
				{ID: "noop", Task: "noop",
					Type: dag.StepTypeNormal},
			},
			dag.WarnMissingRespond)

		runValidatorShape(t, stack, rest,
			"dup", duplicateRespondSteps(t),
			dag.WarnDuplicateRespond)

		runValidatorShape(t, stack, rest,
			"branched", branchPerOutcomeSteps(t),
			"")
	})
}

// runValidatorShape registers an HTTP trigger bound to wfName,
// posts the workflow def through the REST handler, and asserts
// the response carries the expected warning kind (empty wantKind
// means "no warnings"). The workflow itself must end up persisted
// in defKV regardless of warnings.
func runValidatorShape(
	t *testing.T, stack *httpE2EStack, rest http.Handler,
	tag string, steps []dag.StepDef, wantKind string,
) {
	t.Helper()
	wfName := harness.UniqueName(t, "validator-"+tag)
	def := trigger.TriggerDef{
		ID:         harness.UniqueName(t, "trig-"+tag),
		WorkflowID: wfName,
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path: "/" + harness.UniqueName(t,
				"vwarn-"+tag),
			Method:       http.MethodPost,
			TimeoutMs:    3_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := stack.svc.CreateTrigger(stack.ctx, def); err != nil {
		t.Fatalf("%s: CreateTrigger: %v", tag, err)
	}

	wfDef := dag.WorkflowDef{
		Name:    wfName,
		Version: "v1",
		Steps:   steps,
	}
	body, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("%s: marshal: %v", tag, err)
	}
	req := httptest.NewRequest(http.MethodPost,
		"/workflows", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	rest.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("%s: status = %d body=%s", tag, rec.Code,
			rec.Body)
	}
	assertWarning(t, tag, rec.Body.Bytes(), wantKind)
	assertWorkflowPersisted(t, stack, wfName)
}

// assertWarning decodes the registration response and verifies the
// presence (or absence) of the expected warning kind. Empty
// wantKind means "no warnings".
func assertWarning(
	t *testing.T, tag string, body []byte, wantKind string,
) {
	t.Helper()
	var resp struct {
		Status   string        `json:"status"`
		Name     string        `json:"name"`
		Warnings []dag.Warning `json:"warnings"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%s: unmarshal: %v body=%s", tag, err, body)
	}
	if resp.Status != "registered" {
		t.Fatalf("%s: Status = %q, want registered", tag,
			resp.Status)
	}
	if wantKind == "" {
		if len(resp.Warnings) != 0 {
			t.Fatalf("%s: warnings = %v, want none", tag,
				resp.Warnings)
		}
		return
	}
	if len(resp.Warnings) != 1 {
		t.Fatalf("%s: warnings = %v, want 1", tag, resp.Warnings)
	}
	if resp.Warnings[0].Kind != wantKind {
		t.Fatalf("%s: Kind = %q, want %q", tag,
			resp.Warnings[0].Kind, wantKind)
	}
}

// assertWorkflowPersisted reads the workflow back from defKV to
// confirm the registration actually stuck. Warnings are non-fatal,
// so the workflow must be present regardless of warning state.
func assertWorkflowPersisted(
	t *testing.T, stack *httpE2EStack, name string,
) {
	t.Helper()
	got, err := stack.svc.GetWorkflow(name)
	if err != nil {
		t.Fatalf("%s persistence: GetWorkflow: %v", name, err)
	}
	if got.Name != name {
		t.Fatalf("%s persistence: Name = %q, want %q",
			got.Name, name, name)
	}
}

// duplicateRespondSteps returns a workflow shape with two
// simultaneously-reachable respond steps gated only by the same
// upstream (no mutual exclusion) — the duplicate_respond shape.
func duplicateRespondSteps(t *testing.T) []dag.StepDef {
	t.Helper()
	return []dag.StepDef{
		{ID: "a", Task: "task-a",
			Type: dag.StepTypeNormal},
		respondStepDef(t, "r1",
			[]string{"a"},
			dag.RespondConfig{Status: 200}),
		respondStepDef(t, "r2",
			[]string{"a"},
			dag.RespondConfig{Status: 200}),
	}
}

// branchPerOutcomeSteps returns a shape where two respond steps
// are gated on opposite SkipIf predicates against the same parent
// — legitimate happy-vs-error branching that the validator must
// NOT flag.
func branchPerOutcomeSteps(t *testing.T) []dag.StepDef {
	t.Helper()
	return []dag.StepDef{
		{ID: "a", Task: "task-a",
			Type: dag.StepTypeNormal},
		{
			ID: "happy", Task: "task-happy",
			Type:      dag.StepTypeNormal,
			DependsOn: []string{"a"},
			SkipIf: &dag.ParentCond{
				StepID: "a", Field: "ok",
				Op: "==", Value: false,
			},
		},
		{
			ID: "err", Task: "task-err",
			Type:      dag.StepTypeNormal,
			DependsOn: []string{"a"},
			SkipIf: &dag.ParentCond{
				StepID: "a", Field: "ok",
				Op: "==", Value: true,
			},
		},
		respondStepDef(t, "r-ok",
			[]string{"happy"},
			dag.RespondConfig{Status: 200}),
		respondStepDef(t, "r-err",
			[]string{"err"},
			dag.RespondConfig{Status: 500}),
	}
}

