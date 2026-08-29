// api/rest_v1_test.go
// Tests for the control-plane's top-level /v1 routes (issue #628).
// Methodology: create a test service with real embedded NATS, seed the
// workers KV directly through worker.Directory, then drive MountV1's
// handler via httptest to verify status codes and JSON bodies. A separate
// mux-precedence test proves that a more-specific GET /v1/workers pattern
// registered by MountV1 wins over a bridge-owned "/v1/" catch-all pattern,
// while unrelated /v1/* paths still fall through to the catch-all.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// openTestTokenStore opens a workertoken.Store against the given
// jetstream handle for tests that only need MountV1's third argument
// satisfied, not token-management behavior itself (see
// rest_v1_tokens_test.go for those).
func openTestTokenStore(t *testing.T, js jetstream.JetStream) *workertoken.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := workertoken.Open(ctx, js)
	if err != nil {
		t.Fatalf("workertoken.Open: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

// listWorkersResponse mirrors the JSON body MountV1's handler writes for
// GET /v1/workers.
type listWorkersResponse struct {
	Workers []worker.WorkerRegistration `json:"workers"`
	Count   int                         `json:"count"`
}

func TestMountV1PanicsOnNilArgs(t *testing.T) {
	// Positive: nil mux panics.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil mux")
			}
		}()
		MountV1(nil, &Service{}, nil)
	}()

	// Negative: nil svc panics too (all three args are required).
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil svc")
			}
		}()
		MountV1(http.NewServeMux(), nil, nil)
	}()

	// Negative: nil tokenStore panics too.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil tokenStore")
			}
		}()
		MountV1(http.NewServeMux(), &Service{}, nil)
	}()
}

func TestRESTV1ListWorkers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	dir := worker.NewDirectory(js)
	reg1 := worker.WorkerRegistration{
		WorkerID: "worker-1", TaskTypes: []string{"task-a"},
		Language: "go", Transport: "nats", MaxTasks: 10,
	}
	reg2 := worker.WorkerRegistration{
		WorkerID: "worker-2", TaskTypes: []string{"task-b"},
		Language: "python", Transport: "nats", MaxTasks: 5,
	}
	if err := dir.Register(reg1); err != nil {
		t.Fatalf("Register worker-1: %v", err)
	}
	if err := dir.Register(reg2); err != nil {
		t.Fatalf("Register worker-2: %v", err)
	}

	mux := http.NewServeMux()
	MountV1(mux, svc, openTestTokenStore(t, js))
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/workers")
	if err != nil {
		t.Fatalf("GET /v1/workers: %v", err)
	}
	defer resp.Body.Close()

	// Positive: seeded workers come back with a 200 and count 2.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var out listWorkersResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("count = %d, want 2", out.Count)
	}
	if len(out.Workers) != 2 {
		t.Fatalf("len(workers) = %d, want 2", len(out.Workers))
	}
	foundWorker1, foundWorker2 := false, false
	for _, w := range out.Workers {
		if w.WorkerID == "worker-1" {
			foundWorker1 = true
		}
		if w.WorkerID == "worker-2" {
			foundWorker2 = true
		}
	}
	// Negative: both seeded IDs must be present, not just any two.
	if !foundWorker1 || !foundWorker2 {
		t.Fatalf("missing worker IDs in response: %+v", out.Workers)
	}
}

func TestRESTV1ListWorkersEmpty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	mux := http.NewServeMux()
	MountV1(mux, svc, openTestTokenStore(t, js))
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/workers")
	if err != nil {
		t.Fatalf("GET /v1/workers: %v", err)
	}
	defer resp.Body.Close()

	// Positive: no workers bucket still returns 200.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	rawBody := make([]byte, 4096)
	n, _ := resp.Body.Read(rawBody)
	body := string(rawBody[:n])

	// Negative: workers must serialize as [] not null when empty.
	if !jsonContains(body, `"workers":[]`) {
		t.Fatalf("body = %q, want workers:[] present", body)
	}
	if !jsonContains(body, `"count":0`) {
		t.Fatalf("body = %q, want count:0 present", body)
	}
}

// jsonContains is a tiny substring helper so the empty-response test can
// assert on exact JSON shape ("[]" not "null") without a second decode
// path that would normalize away the distinction.
func jsonContains(haystack, needle string) bool {
	return len(haystack) >= len(needle) &&
		indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestRESTV1MethodNotAllowed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	mux := http.NewServeMux()
	MountV1(mux, svc, openTestTokenStore(t, js))
	server := httptest.NewServer(mux)
	defer server.Close()

	// Positive: POST /v1/workers is rejected.
	resp, err := http.Post(server.URL+"/v1/workers", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /v1/workers: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.StatusCode,
			http.StatusMethodNotAllowed)
	}
}

// TestRESTV1MuxPrecedence proves the routing fact issue #628 depends on:
// a bridge-style "/v1/" catch-all registered on the SAME top-level mux as
// MountV1's more-specific "GET /v1/workers" pattern loses to it for that
// exact path (Go 1.22+ ServeMux picks the most specific pattern), while
// unrelated /v1/* paths (e.g. workers/connect) still reach the catch-all.
func TestRESTV1MuxPrecedence(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	catchAllHit := false
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		catchAllHit = true
		w.WriteHeader(http.StatusOK)
	})
	MountV1(mux, svc, openTestTokenStore(t, js))
	server := httptest.NewServer(mux)
	defer server.Close()

	// Positive: GET /v1/workers must NOT reach the catch-all.
	resp, err := http.Get(server.URL + "/v1/workers")
	if err != nil {
		t.Fatalf("GET /v1/workers: %v", err)
	}
	resp.Body.Close()
	if catchAllHit {
		t.Fatal("GET /v1/workers hit the bridge catch-all, want MountV1 route")
	}

	// Negative: POST /v1/workers/connect (a bridge route) must still
	// reach the catch-all -- MountV1 must not have shadowed it.
	catchAllHit = false
	resp, err = http.Post(
		server.URL+"/v1/workers/connect", "application/json", nil,
	)
	if err != nil {
		t.Fatalf("POST /v1/workers/connect: %v", err)
	}
	resp.Body.Close()
	if !catchAllHit {
		t.Fatal("POST /v1/workers/connect did not reach the bridge catch-all")
	}
}
