// token_resolve_test.go
// Tests for per-task resolve authorization (#627 security follow-up):
// resolve must be scoped to the token that claimed the task via poll,
// not just to the task types that token is allowed to poll. Before this
// fix, any minted token (scoped to any task type) could complete, fail,
// or pause ANY in-flight task by ID, because handleResolve never
// consulted Claims -- only the ackMap lookup by task ID.
// Methodology: real NATS, real orchestrator, real bridge, httptest --
// a genuine dispatched task is required so the ackMap entry actually
// carries a TokenID recorded by processPolledMsg.
package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// resolveWithBearer POSTs a resolve action for taskID, authenticating
// with bearer (empty means no Authorization header at all).
func resolveWithBearer(
	t *testing.T, baseURL, taskID, bearer, body string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequest(
		"POST", baseURL+"/v1/tasks/"+taskID+"/resolve",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("resolve request: %v", err)
	}
	return resp
}

// tokenResolveFixture wires an orchestrator + bridge + Store with the
// admin token set, registers a one-step "echo" workflow, and returns
// the base URL, the control-plane Service (for starting fresh runs),
// and two minted, echo-scoped bearer tokens plus the admin bearer.
func tokenResolveFixture(
	t *testing.T,
) (baseURL string, svc *api.Service, bearerA, bearerB, adminBearer string) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	adminBearer = "admin-secret"
	b := newTestBridge(t, nc)
	b.token = adminBearer
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)

	svc = api.NewService(nc)
	wb := dag.NewWorkflow("bridge-resolve-scope")
	wb.Task("echo-step", "echo")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), wfDef); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearerA, err = store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}
	_, bearerB, err = store.Mint(mintCtx, "worker-b", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint B: %v", err)
	}
	return ts.URL, svc, bearerA, bearerB, adminBearer
}

// startRunAndPollTask starts a fresh run of bridge-resolve-scope and
// polls for its task using bearer. Fails the test if no task appears
// within the bounded deadline.
func startRunAndPollTask(
	t *testing.T, baseURL string, svc *api.Service, bearer string,
) pollResponse {
	t.Helper()
	if _, err := svc.StartRun(
		context.Background(), "bridge-resolve-scope", nil,
	); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return pollForTaskWithBearer(t, baseURL, bearer, 10*time.Second)
}

// pollForTaskWithBearer polls the bridge until a task is available,
// authenticating with bearer. Bounded by deadline.
func pollForTaskWithBearer(
	t *testing.T, baseURL, bearer string, deadline time.Duration,
) pollResponse {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()

	for {
		resp := pollWithBearer(t, baseURL, bearer, `["echo"]`)
		var tasks []pollResponse
		if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
			resp.Body.Close()
			t.Fatalf("decode failed: %v", err)
		}
		resp.Body.Close()
		if len(tasks) > 0 {
			return tasks[0]
		}
		select {
		case <-timer.C:
			t.Fatal("timed out waiting for task")
		default:
		}
	}
}

func TestResolveRejectsDifferentMintedToken(t *testing.T) {
	baseURL, svc, bearerA, bearerB, _ := tokenResolveFixture(t)
	task := startRunAndPollTask(t, baseURL, svc, bearerA)

	// Positive: a different minted token (also scoped to "echo") is
	// rejected -- resolve authorization is per-CLAIMED-task, not just
	// per-task-type.
	resp := resolveWithBearer(t, baseURL, task.TaskID, bearerB, `{
		"action":"complete",
		"output":{"message":"hijacked"}
	}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}

	// Negative: the task must still be resolvable by its rightful
	// claimant -- B's rejected attempt must not have consumed it.
	okResp := resolveWithBearer(t, baseURL, task.TaskID, bearerA, `{
		"action":"complete",
		"output":{"message":"done by A"}
	}`)
	defer okResp.Body.Close()
	if okResp.StatusCode != http.StatusOK {
		t.Fatalf("A's resolve after B's rejected attempt: status = %d, want %d",
			okResp.StatusCode, http.StatusOK)
	}
}

func TestResolveAdminBypassesTaskOwnership(t *testing.T) {
	baseURL, svc, bearerA, _, adminBearer := tokenResolveFixture(t)
	task := startRunAndPollTask(t, baseURL, svc, bearerA)

	// Positive: the admin bearer resolves a task claimed by a minted
	// worker token.
	resp := resolveWithBearer(t, baseURL, task.TaskID, adminBearer, `{
		"action":"complete",
		"output":{"message":"done by admin"}
	}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestResolveOwnTokenSucceeds(t *testing.T) {
	baseURL, svc, bearerA, _, _ := tokenResolveFixture(t)
	task := startRunAndPollTask(t, baseURL, svc, bearerA)

	// Positive: the claiming token resolves its own task.
	resp := resolveWithBearer(t, baseURL, task.TaskID, bearerA, `{
		"action":"complete",
		"output":{"message":"done by A"}
	}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}
