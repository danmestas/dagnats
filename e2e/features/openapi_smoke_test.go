// e2e/features/openapi_smoke_test.go
//
// Methodology: end-to-end smoke test for the OpenAPI 3.1 spec
// generator and the Scalar-rendered explorer. Uses the real
// embedded NATS server, the real api.Service, and the real
// trigger.TriggerService — only the outer HTTP listener is
// httptest, because binding a kernel socket adds nothing for a
// JSON-shape assertion and CI sandboxes block raw port binds.
//
// What this exercises:
//   - GET /openapi.json reflects the live workflow_defs / triggers
//     KVs (registering a fresh workflow + trigger appears in the
//     next request).
//   - The spec is valid OpenAPI 3.1 (basic structural round-trip).
//   - GET /docs serves the Scalar shell pointing at /openapi.json.
//   - GET /docs/scalar.js serves a gzipped JS payload.
package features

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/openapi"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
)

// TestOpenAPISmoke registers an HTTP-trigger workflow against a live
// engine + trigger service, mounts the openapi.Handler against an
// httptest server, and curls /openapi.json + /docs + /docs/scalar.js.
// Verifies the spec carries the expected path and that the UI shell
// references the bundle.
func TestOpenAPISmoke(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		stack := startHTTPE2EStack(t, nc)
		wfDef := dag.WorkflowDef{
			Name:         harness.UniqueName(t, "openapi-smoke"),
			Version:      "v1",
			InputSchema:  json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}}}`),
			OutputSchema: json.RawMessage(`{"type":"object","properties":{"echoed":{"type":"string"}}}`),
			Steps: []dag.StepDef{
				{ID: "echo", Task: "task-echo",
					Type: dag.StepTypeNormal},
				respondStepDef(t, "respond",
					[]string{"echo"},
					dag.RespondConfig{Status: 200}),
			},
		}
		path := "/api/" + harness.UniqueName(t, "smoke")
		_, _ = stack.registerHTTPTrigger(t, wfDef,
			&trigger.HTTPConfig{
				Path:         path,
				Method:       http.MethodPost,
				TimeoutMs:    3_000,
				MaxBodyBytes: 1024,
			})

		srv := startSmokeServer(t, stack.svc)
		t.Cleanup(srv.Close)

		spec := fetchSpec(t, srv.URL)
		if _, ok := spec.Paths[path]; !ok {
			t.Fatalf("path %q missing from spec; got paths: %v",
				path, spec.Paths)
		}
		body := fetchScalarHTML(t, srv.URL)
		if !strings.Contains(body, `data-url="/openapi.json"`) {
			t.Fatalf("docs html missing scalar data-url:\n%s", body)
		}
		if !strings.Contains(body, `<script src="/docs/scalar.js">`) {
			t.Fatalf("docs html missing scalar bundle reference:\n%s", body)
		}
		gz := fetchScalarBundle(t, srv.URL)
		if !strings.EqualFold(gz.encoding, "gzip") {
			t.Fatalf("bundle encoding = %q, want gzip", gz.encoding)
		}
		if len(gz.body) < 1024 {
			t.Fatalf("bundle too small (%d bytes)", len(gz.body))
		}
	})
}

// startSmokeServer wires an openapi.Handler against the real api
// service and serves it from an httptest server so the test can use
// real HTTP semantics (Content-Encoding, status codes, redirects)
// rather than the in-process httptest.ResponseRecorder shortcut.
func startSmokeServer(
	t *testing.T, svc *api.Service,
) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	handler := openapi.Handler(
		"dagnats HTTP API", "smoke",
		func(ctx context.Context) (
			[]trigger.TriggerDef,
			map[string]dag.WorkflowDef, error,
		) {
			triggers, err := svc.ListTriggers(ctx)
			if err != nil {
				return nil, nil, err
			}
			defs, err := svc.ListWorkflows(ctx)
			if err != nil {
				return nil, nil, err
			}
			idx := make(map[string]dag.WorkflowDef, len(defs))
			for _, d := range defs {
				idx[d.Name] = d
			}
			return triggers, idx, nil
		},
	)
	mux.Handle("/openapi.json", handler)
	mux.Handle("/docs", handler)
	mux.Handle("/docs/", handler)
	return httptest.NewServer(mux)
}

// fetchSpec issues GET /openapi.json against the test server, parses
// the body as JSON into the openapi.Spec struct, and returns it.
// A bounded HTTP timeout keeps the test from hanging on a missing
// route.
func fetchSpec(t *testing.T, base string) openapi.Spec {
	t.Helper()
	cli := &http.Client{Timeout: 5 * time.Second}
	resp, err := cli.Get(base + "/openapi.json")
	if err != nil {
		t.Fatalf("GET /openapi.json: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var spec openapi.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal spec: %v body=%s", err, raw)
	}
	if spec.OpenAPI != "3.1.0" {
		t.Fatalf("openapi version = %q, want 3.1.0", spec.OpenAPI)
	}
	return spec
}

// fetchScalarHTML issues GET /docs and returns the body as a string.
// Asserts only on status + content type — body validation is the
// caller's job.
func fetchScalarHTML(t *testing.T, base string) string {
	t.Helper()
	cli := &http.Client{Timeout: 5 * time.Second}
	resp, err := cli.Get(base + "/docs")
	if err != nil {
		t.Fatalf("GET /docs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(raw)
}

// bundleResp captures the Scalar bundle response shape the smoke
// test cares about: the Content-Encoding header and the raw body
// bytes. Stored as a struct so the caller can assert on both fields.
type bundleResp struct {
	encoding string
	body     []byte
}

// fetchScalarBundle disables transparent gzip decoding so the test
// can observe Content-Encoding: gzip directly. Without
// Transport.DisableCompression, Go's HTTP client silently inflates
// the response and strips the Content-Encoding header.
func fetchScalarBundle(t *testing.T, base string) bundleResp {
	t.Helper()
	tr := &http.Transport{DisableCompression: true}
	cli := &http.Client{Timeout: 5 * time.Second, Transport: tr}
	resp, err := cli.Get(base + "/docs/scalar.js")
	if err != nil {
		t.Fatalf("GET /docs/scalar.js: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return bundleResp{
		encoding: resp.Header.Get("Content-Encoding"),
		body:     raw,
	}
}
