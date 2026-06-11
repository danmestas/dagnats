// bridge/retry_backoff_e2e_test.go
// End-to-end test for issue #381: a step executed by an HTTP-bridge
// worker under a retry policy must walk the full retry ladder —
// distinct backoff timers per attempt — and dead-letter on
// exhaustion, exactly like the native worker path
// (worker/retry_backoff_e2e_test.go). Before the fix the bridge
// never published step.started with AttemptNumber, so
// run.Steps[id].Attempts pinned at 1, retry #2's SLEEP_TIMERS
// msg-id (runID.stepID.retry_backoff.<Attempts>) collided with
// retry #1's, JetStream deduped the timer, and the run hung in
// Running forever — never reaching failWorkflow/PublishDeadLetter.
// Methodology: real NATS, real orchestrator, real bridge over
// httptest. An HTTP worker loop polls and always resolves
// fail-retriable. Bounded deadline on every wait; assert run
// Failed, step Failed, MaxAttempts+1 total attempts with
// strictly-increasing attempt numbers (the dedup seam: one distinct
// timer msg-id per attempt), and exactly one DLQ entry. Negative
// space: after terminal Failed, no further attempt is dispatched
// and the DLQ count stays at one.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestBridgeRetryBackoff_ExhaustsAndDeadLetters(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// MaxAttempts: 3 with the engine's "<= MaxAttempts" gate allows 3
	// retries — total 4 attempts before permanent failure (mirrors the
	// MaxAttempts semantics pinned by worker/retry_backoff_e2e_test.go).
	wfDef := dag.WorkflowDef{
		Name: "bridge-rb-exhaust", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{ID: "s", Task: "bridge-rb-task", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// HTTP worker: poll continuously, always resolve fail-retriable.
	// Records each polled task's attempt number — the observable seam
	// for "one distinct backoff timer per attempt": a deduped timer
	// never re-publishes the task, so its attempt is never polled.
	var mu sync.Mutex
	var polledAttempts []int
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		pollDeadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(pollDeadline) {
			select {
			case <-stop:
				return
			default:
			}
			tasks, pollErr := bridgePollOnce(ts.URL, "bridge-rb-task")
			if pollErr != nil {
				return // server closing — test is done
			}
			if len(tasks) == 0 {
				// The bridge's ephemeral consumer can return early
				// (e.g. consumer-create contention) — pace the loop
				// so an empty poll doesn't busy-spin.
				time.Sleep(50 * time.Millisecond)
				continue
			}
			for _, task := range tasks {
				mu.Lock()
				polledAttempts = append(polledAttempts, task.Attempt)
				mu.Unlock()
				if failErr := bridgeResolveFail(ts.URL, task.TaskID); failErr != nil {
					return
				}
			}
		}
	}()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-bridge-rb-1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(25 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		r, loadErr := store.Load(context.Background(), "run-bridge-rb-1")
		if loadErr == nil && r.Status == dag.RunStatusFailed {
			run = r
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	attempts := append([]int(nil), polledAttempts...)
	mu.Unlock()
	dlqCount := countDeadLetters(t, jsNew)
	if run.Status != dag.RunStatusFailed {
		t.Fatalf(
			"run.Status = %v, want Failed — bridge retry stall (#381): "+
				"polled attempts %v, dlq entries %d; without AttemptNumber "+
				"propagation retry #2's timer msg-id dedups and the run "+
				"hangs in Running",
			run.Status, attempts, dlqCount,
		)
	}
	step := run.Steps["s"]
	if step.Status != dag.StepStatusFailed {
		t.Fatalf("step.Status = %v, want Failed", step.Status)
	}
	if step.Attempts != 4 {
		t.Fatalf("step.Attempts = %d, want 4", step.Attempts)
	}
	// One distinct timer msg-id per attempt: every attempt 1..4 was
	// dispatched exactly once, in order. A dedup collision would
	// truncate this sequence (the pre-fix bug stalls it at [1 2]).
	want := []int{1, 2, 3, 4}
	if len(attempts) != len(want) {
		t.Fatalf("polled attempts = %v, want %v", attempts, want)
	}
	for i := range want {
		if attempts[i] != want[i] {
			t.Fatalf("polled attempts = %v, want %v", attempts, want)
		}
	}
	if dlqCount != 1 {
		t.Fatalf("DLQ entries = %d, want 1", dlqCount)
	}

	// Negative space: after terminal Failed, a 3x backoff window must
	// produce no fifth attempt and no second DLQ entry.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	finalAttempts := len(polledAttempts)
	mu.Unlock()
	if finalAttempts != 4 {
		t.Fatalf(
			"polled attempt count after terminal = %d, want 4 "+
				"(no retry past exhaustion)", finalAttempts,
		)
	}
	if got := countDeadLetters(t, jsNew); got != 1 {
		t.Fatalf("DLQ entries after terminal = %d, want 1", got)
	}
}

// bridgePollOnce issues a single bounded long-poll for taskType.
func bridgePollOnce(
	baseURL, taskType string,
) ([]pollResponse, error) {
	body := fmt.Sprintf(
		`{"task_types":[%q],"max_tasks":1,"timeout_ms":1000}`,
		taskType,
	)
	resp, err := http.Post(
		baseURL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var tasks []pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// bridgeResolveFail resolves a task as a retriable failure.
func bridgeResolveFail(baseURL, taskID string) error {
	body := `{
		"action":"fail",
		"error":"perma-broken via bridge",
		"failure_type":"retriable"
	}`
	resp, err := http.Post(
		baseURL+"/v1/tasks/"+taskID+"/resolve",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resolve returned %d", resp.StatusCode)
	}
	return nil
}

// countDeadLetters returns the DEAD_LETTERS stream message count.
func countDeadLetters(
	t *testing.T, js jetstream.JetStream,
) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		t.Fatalf("DEAD_LETTERS stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("DEAD_LETTERS stream info: %v", err)
	}
	return info.State.Msgs
}
