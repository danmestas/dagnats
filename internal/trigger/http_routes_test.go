// trigger/http_routes_test.go
//
// Methodology: integration tests with embedded NATS verifying that
// the TriggerService surfaces HTTP triggers as a routed http.Handler,
// loaded from the triggers KV. Each test stands up its own NATS
// server (no shared state).
package trigger

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestHTTPRouterLoadsHTTPTriggerFromKV(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := TriggerDef{
		ID:         "http-loaded",
		WorkflowID: "wf-http-loaded",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/loaded",
			Method:       http.MethodPost,
			TimeoutMs:    500,
			MaxBodyBytes: 1024,
		},
	}
	defData, _ := json.Marshal(def)
	if _, err := trigKV.Put("http-loaded", defData); err != nil {
		t.Fatalf("KV.Put: %v", err)
	}

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	handler := svc.HTTPRouter()
	if handler == nil {
		t.Fatal("HTTPRouter returned nil")
	}

	// Unknown path → 404.
	rec404 := httptest.NewRecorder()
	req404 := httptest.NewRequest(
		http.MethodPost, "/api/nope", nil,
	)
	handler.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("unknown path: status = %d, want 404", rec404.Code)
	}

	// Known path with wrong method → 405.
	rec405 := httptest.NewRecorder()
	req405 := httptest.NewRequest(
		http.MethodGet, "/api/loaded", nil,
	)
	handler.ServeHTTP(rec405, req405)
	if rec405.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method: status = %d, want 405", rec405.Code)
	}

	// Known path + method dispatches to HTTPHandler (which will then
	// time out because no engine is responding). Verifies routing
	// reached the handler.
	recHit := httptest.NewRecorder()
	reqHit := httptest.NewRequest(
		http.MethodPost, "/api/loaded",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	handler.ServeHTTP(recHit, reqHit)
	if recHit.Code != http.StatusGatewayTimeout {
		t.Fatalf("known route: status = %d, want 504 timeout",
			recHit.Code)
	}
	if recHit.Header().Get("X-Dagnats-Run-Id") == "" {
		t.Fatal("X-Dagnats-Run-Id missing from routed response")
	}
}

func TestHTTPRouterReregisterReplacesPriorRoute(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Register, then re-register under the same ID with a different
	// path. The KV watcher should remove the old route + install the
	// new one. The old path must return 404 after the update.
	first := TriggerDef{
		ID:         "http-rereg",
		WorkflowID: "wf-rereg",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/old",
			Method:       http.MethodPost,
			TimeoutMs:    500,
			MaxBodyBytes: 256,
		},
	}
	d1, _ := json.Marshal(first)
	if _, err := trigKV.Put("http-rereg", d1); err != nil {
		t.Fatalf("KV.Put first: %v", err)
	}

	// Allow the watcher to react.
	time.Sleep(200 * time.Millisecond)

	second := first
	second.HTTP = &HTTPConfig{
		Path:         "/api/new",
		Method:       http.MethodPost,
		TimeoutMs:    500,
		MaxBodyBytes: 256,
	}
	d2, _ := json.Marshal(second)
	if _, err := trigKV.Put("http-rereg", d2); err != nil {
		t.Fatalf("KV.Put second: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	handler := svc.HTTPRouter()
	if handler == nil {
		t.Fatal("HTTPRouter returned nil")
	}

	// Old path is gone.
	recOld := httptest.NewRecorder()
	reqOld := httptest.NewRequest(
		http.MethodPost, "/api/old", nil,
	)
	handler.ServeHTTP(recOld, reqOld)
	if recOld.Code != http.StatusNotFound {
		t.Fatalf("old path: status = %d, want 404", recOld.Code)
	}

	// New path is reachable (504 because no engine).
	recNew := httptest.NewRecorder()
	reqNew := httptest.NewRequest(
		http.MethodPost, "/api/new",
		bytes.NewReader([]byte(`{}`)),
	)
	handler.ServeHTTP(recNew, reqNew)
	if recNew.Code != http.StatusGatewayTimeout {
		t.Fatalf("new path: status = %d, want 504", recNew.Code)
	}
}
