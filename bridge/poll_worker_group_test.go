// poll_worker_group_test.go
// Tests for bridge worker-group polling (#695): bridge workers can now
// poll a worker group via pollRequest.WorkerGroup, and worker tokens
// can be scoped to specific groups via workertoken.Claims.WorkerGroups.
//
// Methodology: real embedded NATS server, real bridge, real HTTP
// roundtrip -- same conventions as pollconsumer_test.go and
// token_auth_test.go. The isolation tests (grouped vs ungrouped, group
// vs group) are the core proof: consumername.FilterFor derives
// non-overlapping filter subjects per group, so each gets its own
// JetStream consumer and never sees the other's messages.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// publishGroupedTaskFixture puts one task message on
// task.<taskType>.<group>.<runID> -- the subject StepSubject derives
// for a grouped step (internal/engine/task_publisher.go).
func publishGroupedTaskFixture(
	t *testing.T, js jetstream.JetStream, taskType, group, runID string,
) {
	t.Helper()
	if taskType == "" || group == "" || runID == "" {
		t.Fatalf(
			"publishGroupedTaskFixture: taskType, group, runID must not be empty",
		)
	}
	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-" + runID,
		Input:  json.RawMessage(`{}`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	subject := "task." + taskType + "." + group + "." + runID
	if _, err := js.Publish(context.Background(), subject, data); err != nil {
		t.Fatalf("publish grouped task: %v", err)
	}
}

// pollGroupedRaw issues a poll naming both task_types and worker_group,
// returning the decoded status/body via postPollRaw's convention (the
// caller decides how to interpret non-200).
func pollGroupedRaw(
	t *testing.T, ts *httptest.Server, taskType, group string,
) (int, string) {
	t.Helper()
	body := fmt.Sprintf(
		`{"task_types":["%s"],"worker_group":"%s","max_tasks":1,"timeout_ms":%d}`,
		taskType, group, pollConsumerFetchTimeoutMs,
	)
	return postPollRaw(t, ts, body)
}

// TestUngroupedPollIsByteIdenticalToPreWorkerGroup is the regression
// guard: an absent worker_group must behave exactly as it did before
// #695 -- same durable name, same filter subject.
func TestUngroupedPollIsByteIdenticalToPreWorkerGroup(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)
	publishTaskFixture(t, js, "ungrouped-regress", "run-1")

	tasks := postPoll(t, ts, "ungrouped-regress", 1, pollConsumerFetchTimeoutMs)
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
	got := consumersOnFilter(t, js, "task.ungrouped-regress.*")
	if len(got) != 1 {
		t.Fatalf("got %d consumers on task.ungrouped-regress.*, want 1",
			len(got))
	}
	if got[0].Durable != "workers-ungrouped-regress" {
		t.Fatalf("Durable = %q, want %q",
			got[0].Durable, "workers-ungrouped-regress")
	}
}

// TestGroupedPollIsolatesFromUngroupedAndOtherGroups is the core
// isolation proof (#695): a group's consumer filter never overlaps an
// ungrouped consumer's filter or another group's filter, so tasks
// published to one never reach a poller of another.
func TestGroupedPollIsolatesFromUngroupedAndOtherGroups(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)

	publishGroupedTaskFixture(t, js, "iso", "alpha", "alpha-1")
	publishGroupedTaskFixture(t, js, "iso", "beta", "beta-1")
	publishTaskFixture(t, js, "iso", "ungrouped-1")

	// Positive: the "alpha" group poller gets exactly the alpha task.
	status, body := pollGroupedRaw(t, ts, "iso", "alpha")
	if status != 200 {
		t.Fatalf("alpha poll status = %d, body = %q", status, body)
	}
	var alphaTasks []pollResponse
	if err := json.Unmarshal([]byte(body), &alphaTasks); err != nil {
		t.Fatalf("decode alpha poll: %v", err)
	}
	if len(alphaTasks) != 1 || alphaTasks[0].RunID != "alpha-1" {
		t.Fatalf("alpha poll = %+v, want exactly [alpha-1]", alphaTasks)
	}

	// Positive: the "beta" group poller gets exactly the beta task, not
	// alpha's (already claimed) or the ungrouped one.
	status, body = pollGroupedRaw(t, ts, "iso", "beta")
	if status != 200 {
		t.Fatalf("beta poll status = %d, body = %q", status, body)
	}
	var betaTasks []pollResponse
	if err := json.Unmarshal([]byte(body), &betaTasks); err != nil {
		t.Fatalf("decode beta poll: %v", err)
	}
	if len(betaTasks) != 1 || betaTasks[0].RunID != "beta-1" {
		t.Fatalf("beta poll = %+v, want exactly [beta-1]", betaTasks)
	}

	// Negative: an ungrouped poller for the same task type does NOT see
	// either group's task -- only the genuinely ungrouped one.
	ungrouped := postPoll(t, ts, "iso", 5, pollConsumerFetchTimeoutMs)
	if len(ungrouped) != 1 || ungrouped[0].RunID != "ungrouped-1" {
		t.Fatalf("ungrouped poll = %+v, want exactly [ungrouped-1]",
			ungrouped)
	}
}

// TestGroupedPollReachesGroupedConsumer proves a grouped poll actually
// creates/uses the group's own durable, distinct from the ungrouped
// one, naming the canonical consumer topology.
func TestGroupedPollReachesGroupedConsumer(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)
	publishGroupedTaskFixture(t, js, "topo", "fast", "topo-1")

	status, body := pollGroupedRaw(t, ts, "topo", "fast")
	if status != 200 {
		t.Fatalf("poll status = %d, body = %q", status, body)
	}
	var tasks []pollResponse
	if err := json.Unmarshal([]byte(body), &tasks); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}

	got := consumersOnFilter(t, js, "task.topo.fast.*")
	if len(got) != 1 {
		t.Fatalf("got %d consumers on task.topo.fast.*, want 1", len(got))
	}
	if got[0].Durable != "workers-topo-fast" {
		t.Fatalf("Durable = %q, want %q", got[0].Durable, "workers-topo-fast")
	}
}

// TestPollDottedTaskWithWorkerGroupRejected mirrors dag's
// validateStepDispatch combination rule (issue #695): a dotted task
// type combined with a non-empty worker_group can never map onto a
// real dispatch (FilterFor("a.b", "") and FilterFor("a", "b") collide),
// so the bridge must reject it at the request boundary with 400.
func TestPollDottedTaskWithWorkerGroupRejected(t *testing.T) {
	_, _, ts := newPollConsumerBridge(t)
	status, body := pollGroupedRaw(t, ts, "dagger.call", "fast")
	if status != 400 {
		t.Fatalf("status = %d, want 400 (body %q)", status, body)
	}

	// Negative: the same dotted type with NO worker_group stays legal.
	status, body = postPollRaw(t, ts, fmt.Sprintf(
		`{"task_types":["dagger.call"],"max_tasks":1,"timeout_ms":%d}`,
		pollConsumerFetchTimeoutMs,
	))
	if status != 200 {
		t.Fatalf("dotted task with no group: status = %d, want 200 (body %q)",
			status, body)
	}
}

// TestPollInvalidWorkerGroupRejected proves worker_group is validated
// with dag.ValidWorkerGroup, same as a StepDef.WorkerGroup.
func TestPollInvalidWorkerGroupRejected(t *testing.T) {
	_, _, ts := newPollConsumerBridge(t)
	status, body := pollGroupedRaw(t, ts, "echo", "has space")
	if status != 400 {
		t.Fatalf("status = %d, want 400 (body %q)", status, body)
	}
}

// TestTokenWorkerGroupScoping is the token-side isolation proof: a
// token scoped to worker group "alpha" may poll "alpha" but is
// forbidden from polling "beta" or the ungrouped queue; an unscoped
// (no WorkerGroups) token keeps today's behavior and may poll
// anything.
func TestTokenWorkerGroupScoping(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)

	b := newTestBridge(t, nc)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, scopedBearer, err := store.Mint(
		mintCtx, "worker-alpha", []string{"echo"}, []string{"alpha"}, "tester",
	)
	if err != nil {
		t.Fatalf("Mint scoped: %v", err)
	}
	_, unscopedBearer, err := store.Mint(
		mintCtx, "worker-any", []string{"echo"}, nil, "tester",
	)
	if err != nil {
		t.Fatalf("Mint unscoped: %v", err)
	}

	pollScoped := func(bearer, group string) int {
		t.Helper()
		body := fmt.Sprintf(
			`{"task_types":["echo"],"worker_group":"%s","max_tasks":1,"timeout_ms":100}`,
			group,
		)
		req, err := http.NewRequest(
			"POST", ts.URL+"/v1/tasks/poll", strings.NewReader(body),
		)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// Positive: the alpha-scoped token polling its own group is allowed.
	if status := pollScoped(scopedBearer, "alpha"); status != 200 {
		t.Fatalf("scoped token polling own group: status = %d, want 200", status)
	}
	// Negative: the alpha-scoped token polling a different group is
	// forbidden.
	if status := pollScoped(scopedBearer, "beta"); status != 403 {
		t.Fatalf("scoped token polling other group: status = %d, want 403", status)
	}
	// Negative: the alpha-scoped token can never fall back to the
	// ungrouped queue -- the whole point of the isolation.
	if status := pollScoped(scopedBearer, ""); status != 403 {
		t.Fatalf("scoped token polling ungrouped: status = %d, want 403", status)
	}
	// Positive: an unscoped token keeps today's behavior -- any group,
	// including ungrouped, is allowed.
	if status := pollScoped(unscopedBearer, "alpha"); status != 200 {
		t.Fatalf("unscoped token polling alpha: status = %d, want 200", status)
	}
	if status := pollScoped(unscopedBearer, ""); status != 200 {
		t.Fatalf("unscoped token polling ungrouped: status = %d, want 200", status)
	}
}
