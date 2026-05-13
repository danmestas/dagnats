// e2e/features/http_multistep_test.go
//
// Methodology: end-to-end coverage for ADR-013 scenarios that
// exercise the trigger + DAG + respond execution path together.
// Each test stands up its own embedded NATS server, real engine,
// real trigger service, real workers, and drives requests through
// the trigger.HTTPRouter via httptest. No fakes, no engine
// stand-ins, no fake-event publishes — those tests already live
// at internal/trigger/*_test.go and internal/engine/*_test.go.
//
// Scenarios covered here:
//   - TestHTTPTrigger_MultiStep_BodyFrom: A → B → respond with
//     BodyFrom dotpath; response must come from B's output (the
//     immediate upstream), not A's.
//   - TestHTTPTrigger_MethodMatrix: five workflows on the same
//     path, one per allowed method; matching method returns the
//     workflow's response, mismatching method returns 405.
//   - TestHTTPTrigger_BodySize_413: MaxBodyBytes enforced — under
//     the cap returns 200, over the cap returns 413.
package features

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// TestHTTPTrigger_MultiStep_BodyFrom drives a 3-step workflow where
// the respond step depends on B (not A) and uses a BodyFrom dotpath.
// The dotpath resolves against the IMMEDIATE upstream's output (B's),
// so the response body must reflect B's data, not A's. This pins the
// engine respond_step.go resolveRespondBody contract end-to-end.
func TestHTTPTrigger_MultiStep_BodyFrom(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)

		harness.SubscribeWorker(t, nc, "task-a",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`{"result":"from-a"}`))
			})
		harness.SubscribeWorker(t, nc, "task-b",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`{"result":"from-b"}`))
			})

		wfName := harness.UniqueName(t, "multi-bodyfrom")
		wfDef := dag.WorkflowDef{
			Name:    wfName,
			Version: "v1",
			Steps: []dag.StepDef{
				{ID: "a", Task: "task-a",
					Type: dag.StepTypeNormal},
				{ID: "b", Task: "task-b",
					Type:      dag.StepTypeNormal,
					DependsOn: []string{"a"}},
				respondStepDef(t, "respond",
					[]string{"b"},
					dag.RespondConfig{
						Status:   200,
						BodyFrom: "result",
					}),
			},
		}
		_, path := stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:         "/" + harness.UniqueName(t, "multi"),
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
			})

		rec := postOnRouter(t, stack.router,
			http.MethodPost, path, []byte(`{"k":"v"}`), nil)

		if rec.Code != 200 {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
		}
		got := rec.Body.String()
		// Positive: body is B's dotpath value, JSON-quoted.
		if got != `"from-b"` {
			t.Fatalf("body = %q, want %q (B's result)", got,
				`"from-b"`)
		}
		// Negative: A's result must not leak through.
		if strings.Contains(got, "from-a") {
			t.Fatalf("body = %q, must not include from-a", got)
		}
	})
}

// TestHTTPTrigger_MethodMatrix registers one workflow per allowed
// method on the same path, asserts the matching method returns 200
// with the workflow's response, and the mismatching method returns
// 405 (the actual behavior — known path / wrong method → 405, per
// trigger.serveHTTPRoute). Discrepancy from the brief: brief mused
// 405 OR 404; the implementation returns 405 when path is registered
// but method is not. We assert the actual 405 behavior.
func TestHTTPTrigger_MethodMatrix(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)
		path := "/" + harness.UniqueName(t, "matrix")

		methods := []string{
			http.MethodGet, http.MethodPost, http.MethodPut,
			http.MethodPatch, http.MethodDelete,
		}
		for _, m := range methods {
			registerMethodMatrixWorkflow(t, stack, path, m)
		}

		// Positive: each method on the shared path returns 200 + the
		// echoed method name.
		for _, m := range methods {
			rec := postOnRouter(t, stack.router,
				m, path, []byte(`{}`), nil)
			if rec.Code != 200 {
				t.Fatalf("%s %s: status = %d body=%s",
					m, path, rec.Code, rec.Body)
			}
			want := `"method-` + m + `"`
			if rec.Body.String() != want {
				t.Fatalf("%s body = %q, want %q",
					m, rec.Body.String(), want)
			}
		}

		// Negative: an unregistered method (HEAD is not in the v1
		// closed set) is rejected with 405 — the path IS registered.
		rec405 := postOnRouter(t, stack.router,
			http.MethodHead, path, nil, nil)
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Fatalf("HEAD %s: status = %d, want 405 (actual impl)",
				path, rec405.Code)
		}
	})
}

// registerMethodMatrixWorkflow registers one method-matrix workflow:
// a tiny produce step (constant body) followed by a respond step.
// Splits work out of TestHTTPTrigger_MethodMatrix so each function
// stays under 70 lines.
func registerMethodMatrixWorkflow(
	t *testing.T, stack *httpE2EStack,
	path string, method string,
) {
	t.Helper()
	wfName := harness.UniqueName(t,
		"matrix-"+strings.ToLower(method))
	taskName := "method-task-" +
		strings.ToLower(method) + "-" + wfName
	harness.SubscribeWorker(t, stack.nc, taskName,
		func(tc worker.TaskContext) error {
			return tc.Complete(
				[]byte(`"method-` + method + `"`))
		})
	wfDef := dag.WorkflowDef{
		Name:    wfName,
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "produce", Task: taskName,
				Type: dag.StepTypeNormal},
			respondStepDef(t, "respond",
				[]string{"produce"},
				dag.RespondConfig{Status: 200}),
		},
	}
	_, _ = stack.registerHTTPTrigger(t, wfDef,
		&trigger.HTTPConfig{
			Path:         path,
			Method:       method,
			TimeoutMs:    10_000,
			MaxBodyBytes: 1024,
		})
}

// TestHTTPTrigger_BodySize_413 verifies that a request body over the
// configured MaxBodyBytes returns 413, and a body under the cap
// returns 200. The 413 is mapped from httpenvelope.ErrBodyTooLarge
// by trigger.HTTPHandler.readAndValidate.
func TestHTTPTrigger_BodySize_413(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)
		path := "/" + harness.UniqueName(t, "bodysize")
		wfName := harness.UniqueName(t, "bodysize-wf")
		wfDef := dag.WorkflowDef{
			Name:    wfName,
			Version: "v1",
			Steps: []dag.StepDef{
				respondStepDef(t, "respond", nil,
					dag.RespondConfig{Status: 200}),
			},
		}
		_, _ = stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:         path,
				Method:       http.MethodPost,
				TimeoutMs:    10_000,
				MaxBodyBytes: 1024,
			})

		// Positive: 512 bytes (under cap) → 200.
		small := bytes.Repeat([]byte("a"), 512)
		recSmall := postOnRouter(t, stack.router,
			http.MethodPost, path, small, nil)
		if recSmall.Code != 200 {
			t.Fatalf("small body: status = %d, want 200; body=%s",
				recSmall.Code, recSmall.Body)
		}

		// Negative: 2048 bytes (over cap) → 413.
		big := bytes.Repeat([]byte("b"), 2048)
		recBig := postOnRouter(t, stack.router,
			http.MethodPost, path, big, nil)
		if recBig.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("big body: status = %d, want 413; body=%s",
				recBig.Code, recBig.Body)
		}
	})
}
