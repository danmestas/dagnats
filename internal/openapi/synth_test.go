// openapi/synth_test.go
//
// Methodology: pure unit tests. The synth package owns no I/O — every
// test builds a synthetic trigger/workflow input, runs Build, and
// asserts shape via the encoded JSON. Two assertions minimum per test:
// a positive presence check + a negative absence check, so the
// validator's bounds (e.g. duplicate keys, leaked fields) stay
// covered. Golden fixtures live under testdata/.
package openapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// minimalHTTPTrigger returns a triggerDef + matching workflow with no
// schemas, no auth, no idempotency header — the absolute floor a
// caller can ship.
func minimalHTTPTrigger(
	t *testing.T,
) (trigger.TriggerDef, dag.WorkflowDef) {
	t.Helper()
	return trigger.TriggerDef{
			ID:         "echo-trigger",
			WorkflowID: "echo",
			Enabled:    true,
			HTTP: &trigger.HTTPConfig{
				Path:         "/api/echo",
				Method:       "POST",
				TimeoutMs:    5000,
				MaxBodyBytes: 1 << 20,
			},
		}, dag.WorkflowDef{
			Name:    "echo",
			Version: "1.0",
			Steps: []dag.StepDef{
				{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
				{
					ID:        "r",
					Type:      dag.StepTypeRespond,
					DependsOn: []string{"a"},
					Config: json.RawMessage(
						`{"status":200,"content_type":"application/json"}`,
					),
				},
			},
		}
}

func TestBuildMinimalHTTPTrigger(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	spec := Build("test", "1", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	if got := spec.OpenAPI; got != "3.1.0" {
		t.Fatalf("openapi version = %q, want 3.1.0", got)
	}
	if _, ok := spec.Paths["/api/echo"]; !ok {
		t.Fatalf("path /api/echo missing: %#v", spec.Paths)
	}
}

func TestBuildMinimalEmitsFreeFormSchemas(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	spec := Build("test", "1", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	post := spec.Paths["/api/echo"].Post
	if post == nil {
		t.Fatalf("POST operation missing")
	}
	body := post.RequestBody.Content["application/json"].Schema
	if !bytes.Contains(body, []byte(`"additionalProperties":true`)) {
		t.Fatalf("free-form fallback missing in requestBody schema: %s",
			string(body))
	}
	resp := post.Responses["200"].Content["application/json"].Schema
	if !bytes.Contains(resp, []byte(`"additionalProperties":true`)) {
		t.Fatalf("free-form fallback missing in 200 schema: %s",
			string(resp))
	}
}

func TestBuildFullFeaturedTriggerAuthHMACIdempotency(t *testing.T) {
	td := trigger.TriggerDef{
		ID:         "full-trigger",
		WorkflowID: "full",
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:              "/api/orders",
			Method:            "POST",
			TimeoutMs:         30000,
			MaxBodyBytes:      4 << 20,
			Secret:            strings.Repeat("k", 32),
			IdempotencyHeader: "Idempotency-Key",
			Authentication: &trigger.HTTPAuthentication{
				Name:         "bearerAuth",
				Type:         "http",
				Scheme:       "bearer",
				BearerFormat: "JWT",
				Description:  "JWT bearer token",
			},
		},
	}
	def := dag.WorkflowDef{
		Name:         "full",
		Version:      "1",
		InputSchema:  json.RawMessage(`{"type":"object","properties":{"sku":{"type":"string"}}}`),
		OutputSchema: json.RawMessage(`{"type":"object","properties":{"order_id":{"type":"string"}}}`),
		Steps: []dag.StepDef{
			{ID: "a", Task: "create", Type: dag.StepTypeNormal},
			{
				ID:        "r",
				Type:      dag.StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    json.RawMessage(`{"status":200}`),
			},
		},
	}
	spec := Build("test", "1", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	post := spec.Paths["/api/orders"].Post
	if post == nil {
		t.Fatalf("POST missing on /api/orders")
	}
	// Security: bearer + HMAC.
	if post.Security == nil {
		t.Fatalf("expected security on full-featured operation")
	}
	if len(post.Security) != 2 {
		t.Fatalf("want 2 security requirements, got %d", len(post.Security))
	}
	if spec.Components == nil {
		t.Fatalf("expected components on full-featured spec")
	}
	if _, ok := spec.Components.SecuritySchemes["bearerAuth"]; !ok {
		t.Fatalf("bearerAuth security scheme missing")
	}
	if _, ok := spec.Components.SecuritySchemes["hmacSignature"]; !ok {
		t.Fatalf("hmacSignature security scheme missing")
	}
	// Idempotency header parameter present.
	var sawIdemp bool
	for _, p := range post.Parameters {
		if p.Name == "Idempotency-Key" && p.In == "header" {
			sawIdemp = true
		}
	}
	if !sawIdemp {
		t.Fatalf("Idempotency-Key header parameter missing: %#v",
			post.Parameters)
	}
	// Input schema flows through verbatim.
	gotIn := post.RequestBody.Content["application/json"].Schema
	if !bytes.Contains(gotIn, []byte(`"sku"`)) {
		t.Fatalf("input schema not propagated: %s", string(gotIn))
	}
}

func TestBuildMultipleTriggersStablePaths(t *testing.T) {
	td1 := trigger.TriggerDef{
		ID: "t-list", WorkflowID: "list", Enabled: true,
		HTTP: &trigger.HTTPConfig{
			Path: "/api/list", Method: "GET",
			TimeoutMs: 5000, MaxBodyBytes: 1024,
		},
	}
	td2 := trigger.TriggerDef{
		ID: "t-create", WorkflowID: "create", Enabled: true,
		HTTP: &trigger.HTTPConfig{
			Path: "/api/create", Method: "POST",
			TimeoutMs: 5000, MaxBodyBytes: 1024,
		},
	}
	defs := map[string]dag.WorkflowDef{
		"list":   {Name: "list", Version: "1"},
		"create": {Name: "create", Version: "1"},
	}
	spec := Build("test", "1",
		[]trigger.TriggerDef{td1, td2}, defs)
	if len(spec.Paths) != 2 {
		t.Fatalf("want 2 paths, got %d: %#v", len(spec.Paths), spec.Paths)
	}
	// Negative: the disabled trigger does not slip in.
	td3 := trigger.TriggerDef{
		ID: "t-disabled", WorkflowID: "disabled", Enabled: false,
		HTTP: &trigger.HTTPConfig{
			Path: "/api/disabled", Method: "POST",
			TimeoutMs: 5000, MaxBodyBytes: 1024,
		},
	}
	spec2 := Build("test", "1",
		[]trigger.TriggerDef{td1, td2, td3}, defs)
	if _, ok := spec2.Paths["/api/disabled"]; ok {
		t.Fatalf("disabled trigger leaked into spec")
	}
}

func TestBuildNoHTTPTriggersEmptyPaths(t *testing.T) {
	spec := Build("test", "1", nil, map[string]dag.WorkflowDef{})
	if len(spec.Paths) != 0 {
		t.Fatalf("want 0 paths, got %d: %#v", len(spec.Paths), spec.Paths)
	}
	if spec.Components != nil &&
		spec.Components.SecuritySchemes != nil &&
		len(spec.Components.SecuritySchemes) > 0 {
		t.Fatalf("no triggers but security schemes leaked: %#v",
			spec.Components.SecuritySchemes)
	}
}

func TestBuildFailureModeResponses(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	spec := Build("test", "1", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	post := spec.Paths["/api/echo"].Post
	for _, code := range []string{"200", "499", "500", "503", "504"} {
		if _, ok := post.Responses[code]; !ok {
			t.Fatalf("response %s missing: %#v", code, post.Responses)
		}
	}
	// X-Dagnats-Run-Id on every response.
	for code, resp := range post.Responses {
		if _, ok := resp.Headers["X-Dagnats-Run-Id"]; !ok {
			t.Fatalf("X-Dagnats-Run-Id missing on response %s", code)
		}
	}
}

func TestBuildJSONIsValidAndDeterministic(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	spec := Build("test", "1", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	first, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Round-trip — must reparse cleanly.
	var rt map[string]interface{}
	if err := json.Unmarshal(first, &rt); err != nil {
		t.Fatalf("round-trip unmarshal: %v", err)
	}
	// Second build identical to first (determinism).
	second, _ := json.Marshal(Build("test", "1",
		[]trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def}))
	if !bytes.Equal(first, second) {
		t.Fatalf("non-deterministic output:\nA=%s\nB=%s",
			string(first), string(second))
	}
}

func TestBuildExtensionsOnOperation(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	spec := Build("test", "1", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	out, err := json.Marshal(spec.Paths["/api/echo"].Post)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{
		"x-dagnats-max-body-bytes",
		"x-dagnats-timeout-ms",
	} {
		if !bytes.Contains(out, []byte(key)) {
			t.Fatalf("expected extension %q in operation: %s",
				key, string(out))
		}
	}
}

func TestHandlerServesSpec(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	h := Handler("test", "1", func(_ context.Context) (
		[]trigger.TriggerDef, map[string]dag.WorkflowDef, error,
	) {
		return []trigger.TriggerDef{td},
			map[string]dag.WorkflowDef{def.Name: def}, nil
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/openapi.json", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("/api/echo")) {
		t.Fatalf("body missing /api/echo: %s", rec.Body.String())
	}
}

func TestHandlerServesScalarHTML(t *testing.T) {
	h := Handler("test", "1", func(_ context.Context) (
		[]trigger.TriggerDef, map[string]dag.WorkflowDef, error,
	) {
		return nil, map[string]dag.WorkflowDef{}, nil
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/docs", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`data-url="/openapi.json"`,
		`<script src="/docs/scalar.js">`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("html missing %q:\n%s", want, body)
		}
	}
}

func TestHandlerServesScalarBundleGzip(t *testing.T) {
	h := Handler("test", "1", func(_ context.Context) (
		[]trigger.TriggerDef, map[string]dag.WorkflowDef, error,
	) {
		return nil, map[string]dag.WorkflowDef{}, nil
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/docs/scalar.js", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip",
			rec.Header().Get("Content-Encoding"))
	}
	if rec.Body.Len() < 1024 {
		t.Fatalf("scalar bundle too small (%d bytes)", rec.Body.Len())
	}
}

func TestBuildGoldenMinimal(t *testing.T) {
	td, def := minimalHTTPTrigger(t)
	spec := Build("test", "1.0.0", []trigger.TriggerDef{td},
		map[string]dag.WorkflowDef{def.Name: def})
	got, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	goldenPath := filepath.Join("testdata", "minimal.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(want)) {
		t.Fatalf(
			"minimal spec drift\nGOT:\n%s\nWANT:\n%s",
			string(got), string(want),
		)
	}
}

// TestRequestBodyByVerb pins the semantics: GET and DELETE produce no
// requestBody (most SDK generators break on GET/DELETE-with-body, even
// though OpenAPI 3.1 technically allows it). POST/PUT/PATCH produce
// the requestBody with required=true.
func TestRequestBodyByVerb(t *testing.T) {
	cases := []struct {
		method   string
		wantBody bool
	}{
		{"GET", false},
		{"DELETE", false},
		{"POST", true},
		{"PUT", true},
		{"PATCH", true},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			td := trigger.TriggerDef{
				ID:         "trig-" + tc.method,
				WorkflowID: "wf-" + tc.method,
				Enabled:    true,
				HTTP: &trigger.HTTPConfig{
					Path:         "/api/x",
					Method:       tc.method,
					TimeoutMs:    1000,
					MaxBodyBytes: 1024,
				},
			}
			def := dag.WorkflowDef{
				Name:    "wf-" + tc.method,
				Version: "1.0",
				Steps: []dag.StepDef{
					{ID: "a", Task: "echo", Type: dag.StepTypeNormal},
					{
						ID:        "r",
						Type:      dag.StepTypeRespond,
						DependsOn: []string{"a"},
						Config:    json.RawMessage(`{"status":200}`),
					},
				},
			}
			spec := Build("t", "1", []trigger.TriggerDef{td},
				map[string]dag.WorkflowDef{def.Name: def})
			item, ok := spec.Paths["/api/x"]
			if !ok {
				t.Fatalf("path missing for %s", tc.method)
			}
			var op *Operation
			switch tc.method {
			case "GET":
				op = item.Get
			case "POST":
				op = item.Post
			case "PUT":
				op = item.Put
			case "PATCH":
				op = item.Patch
			case "DELETE":
				op = item.Delete
			}
			if op == nil {
				t.Fatalf("operation missing for %s", tc.method)
			}
			// Positive AND negative space.
			gotBody := op.RequestBody != nil
			if gotBody != tc.wantBody {
				t.Fatalf("%s requestBody present = %v, want %v",
					tc.method, gotBody, tc.wantBody)
			}
		})
	}
}
