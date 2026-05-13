// api/workflow_register_test.go
//
// Methodology: integration tests with embedded NATS. Per ADR-013 PR 3,
// POST /workflows runs dag.ValidateRespondReachability against the
// definition and any HTTP triggers already bound to it, returning
// warnings (NOT errors) in the response body. Field validation
// remains fatal (400). Each test starts its own NATS server.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
)

// makeRespondConfig returns the JSON for a default RespondConfig.
func makeRespondConfig(t *testing.T) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(dag.RespondConfig{Status: 200})
	if err != nil {
		t.Fatalf("marshal RespondConfig: %v", err)
	}
	return b
}

func registerHTTPTrigger(
	t *testing.T, svc *Service, workflowName string,
) {
	t.Helper()
	def := trigger.TriggerDef{
		ID:         "http-" + workflowName,
		WorkflowID: workflowName,
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:         "/api/" + workflowName,
			Method:       http.MethodPost,
			TimeoutMs:    3_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := svc.CreateTrigger(context.Background(), def); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
}

func TestRegisterWorkflowHTTPTriggerWithRespondNoWarnings(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wfDef := dag.WorkflowDef{
		Name:    "wf-clean",
		Version: "v1",
		Steps: []dag.StepDef{
			{
				ID:     "respond",
				Type:   dag.StepTypeRespond,
				Config: makeRespondConfig(t),
			},
		},
	}
	registerHTTPTrigger(t, svc, wfDef.Name)

	warnings, err := svc.RegisterWorkflowWithWarnings(
		context.Background(), wfDef,
	)
	if err != nil {
		t.Fatalf("RegisterWorkflowWithWarnings: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
}

func TestRegisterWorkflowHTTPTriggerWithoutRespondWarns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wfDef := dag.WorkflowDef{
		Name:    "wf-missing-respond",
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "noop", Type: dag.StepTypeNormal, Task: "noop"},
		},
	}
	registerHTTPTrigger(t, svc, wfDef.Name)

	warnings, err := svc.RegisterWorkflowWithWarnings(
		context.Background(), wfDef,
	)
	if err != nil {
		t.Fatalf("RegisterWorkflowWithWarnings: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("len(warnings) = %d, want 1: %v",
			len(warnings), warnings)
	}
	if warnings[0].Kind != dag.WarnMissingRespond {
		t.Fatalf("Kind = %q, want %q",
			warnings[0].Kind, dag.WarnMissingRespond)
	}
}

func TestRegisterWorkflowNoHTTPTriggerNoWarning(t *testing.T) {
	// A workflow without an HTTP trigger may legitimately omit
	// respond. Validator must NOT warn in that case.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wfDef := dag.WorkflowDef{
		Name:    "wf-no-http",
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "noop", Type: dag.StepTypeNormal, Task: "noop"},
		},
	}

	warnings, err := svc.RegisterWorkflowWithWarnings(
		context.Background(), wfDef,
	)
	if err != nil {
		t.Fatalf("RegisterWorkflowWithWarnings: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none (no HTTP trigger bound)",
			warnings)
	}
}

func TestRESTHandleRegisterWorkflowSurfacesWarnings(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	registerHTTPTrigger(t, svc, "wf-rest-warn")

	wfDef := dag.WorkflowDef{
		Name:    "wf-rest-warn",
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "noop", Type: dag.StepTypeNormal, Task: "noop"},
		},
	}
	body, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(
		http.MethodPost, "/workflows", bytes.NewReader(body),
	)
	rec := httptest.NewRecorder()
	NewRESTHandler(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	var resp struct {
		Status   string        `json:"status"`
		Name     string        `json:"name"`
		Warnings []dag.Warning `json:"warnings"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v body=%s",
			err, rec.Body.String())
	}
	if resp.Status != "registered" {
		t.Fatalf("Status = %q, want registered", resp.Status)
	}
	if len(resp.Warnings) != 1 {
		t.Fatalf("len(Warnings) = %d, want 1: %v",
			len(resp.Warnings), resp.Warnings)
	}
	if resp.Warnings[0].Kind != dag.WarnMissingRespond {
		t.Fatalf("Kind = %q, want %q",
			resp.Warnings[0].Kind, dag.WarnMissingRespond)
	}
}

func TestRESTHandleRegisterWorkflowFieldValidationFails(t *testing.T) {
	// Field validation (dag.Validate) returns an error, so the REST
	// handler must return 400 and NOT persist the workflow.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	// Definition with no steps — fails dag.Validate.
	wfDef := dag.WorkflowDef{
		Name:    "wf-bad",
		Version: "v1",
		Steps:   nil,
	}
	body, _ := json.Marshal(wfDef)
	req := httptest.NewRequest(
		http.MethodPost, "/workflows", bytes.NewReader(body),
	)
	rec := httptest.NewRecorder()
	NewRESTHandler(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(strings.ToLower(rec.Body.String()), "step") {
		t.Fatalf("error body = %q, want mention of step",
			rec.Body.String())
	}
}
