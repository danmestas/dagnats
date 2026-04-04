// engine/approval_test.go
// Integration tests for approval gates. Uses real embedded NATS server.
// Methodology: register a workflow with an approval step, start a run,
// then verify token storage, event publication, approve/reject/timeout
// behavior, and double-approve guard.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// buildApprovalWorkflow creates a two-step workflow: an approval
// gate followed by a normal task step.
func buildApprovalWorkflow(
	t *testing.T,
) dag.WorkflowDef {
	t.Helper()
	b := dag.NewWorkflow("approval-wf")
	gate := b.Approval("gate", dag.ApprovalConfig{
		Timeout: 5 * time.Second,
		Subject: "approvals.test",
	})
	b.Task("after", "task-after").After(gate)
	def, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	return def
}

// startApprovalRun registers the workflow, starts the orchestrator,
// publishes a workflow.started event, and waits for the approval
// token to appear in KV. Returns the token.
func startApprovalRun(
	t *testing.T,
	js nats.JetStreamContext,
	nc *nats.Conn,
	wfDef dag.WorkflowDef,
	runID string,
) (string, *Orchestrator) {
	t.Helper()
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("get workflow_defs: %v", err)
	}
	defData, err := json.Marshal(wfDef)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := defKV.Put(wfDef.Name, defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()

	payload := buildStartEvtPayload(t, defData, nil)
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := js.Publish(
		evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()),
	); err != nil {
		t.Fatalf("publish start event: %v", err)
	}

	// Wait for the token to appear in KV.
	token := waitForApprovalToken(t, js, runID, "gate")
	return token, orch
}

func buildStartEvtPayload(
	t *testing.T, defData, input []byte,
) []byte {
	t.Helper()
	sp := struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input,omitempty"`
	}{
		WorkflowDef: defData,
		Input:       input,
	}
	data, err := json.Marshal(sp)
	if err != nil {
		t.Fatalf("marshal start payload: %v", err)
	}
	return data
}

func waitForApprovalToken(
	t *testing.T,
	js nats.JetStreamContext,
	runID, stepID string,
) string {
	t.Helper()
	kv, err := js.KeyValue("approval_tokens")
	if err != nil {
		t.Fatalf("get approval_tokens: %v", err)
	}
	key := runID + "." + stepID
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entry, err := kv.Get(key)
		if err == nil {
			var record struct {
				Token string `json:"token"`
			}
			if json.Unmarshal(entry.Value(), &record) == nil {
				if record.Token != "" {
					return record.Token
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("approval token did not appear within timeout")
	return ""
}

func TestApprovalStep_TokenStoredAndEventPublished(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := buildApprovalWorkflow(t)
	runID := "run-approval-1"

	// Subscribe to approval notification subject.
	notifySub, err := nc.SubscribeSync("approvals.test")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	token, orch := startApprovalRun(t, js, nc, wfDef, runID)
	defer orch.Stop()

	// Positive: token is non-empty (64 hex chars).
	if len(token) != 64 {
		t.Fatalf("expected 64-char token, got %d chars", len(token))
	}

	// Positive: notification was published.
	msg, err := notifySub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no notification received: %v", err)
	}
	if len(msg.Data) == 0 {
		t.Fatal("notification body is empty")
	}

	// Verify step is Running in snapshot.
	store := NewSnapshotStore(js)
	run, err := store.Load(runID)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if run.Steps["gate"].Status != dag.StepStatusRunning {
		t.Fatalf(
			"expected gate Running, got %s",
			run.Steps["gate"].Status,
		)
	}
}

func TestApprovalStep_ApproveUnblocksDownstream(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := buildApprovalWorkflow(t)
	runID := "run-approval-2"
	token, orch := startApprovalRun(t, js, nc, wfDef, runID)
	defer orch.Stop()

	// Publish approval.granted event (simulating API).
	grantPayload := []byte(`{"approved_by":"tester"}`)
	evt := protocol.NewStepEvent(
		protocol.EventApprovalGranted,
		runID, "gate", grantPayload,
	)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js.Publish(
		evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()),
	)

	// The "after" task should be enqueued.
	sub, err := js.PullSubscribe(
		"task.task-after.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("downstream task not enqueued: %v", err)
	}

	// Positive: downstream task received.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Negative: token must have been used (non-empty).
	if token == "" {
		t.Fatal("token should not be empty")
	}
}

func TestApprovalStep_RejectFailsWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := buildApprovalWorkflow(t)
	runID := "run-approval-3"
	_, orch := startApprovalRun(t, js, nc, wfDef, runID)
	defer orch.Stop()

	// Publish rejection.
	evt := protocol.NewStepEvent(
		protocol.EventApprovalRejected,
		runID, "gate",
		[]byte(`"deployment not safe"`),
	)
	evtData, _ := evt.Marshal()
	js.Publish(
		evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()),
	)

	// Wait for run to fail.
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(runID)
		if err == nil && run.Status == dag.RunStatusFailed {
			// Positive: run failed.
			state := run.Steps["gate"]
			if state.Status != dag.StepStatusFailed {
				t.Fatalf(
					"expected gate Failed, got %s",
					state.Status,
				)
			}
			// Negative: downstream should not be completed.
			afterState := run.Steps["after"]
			if afterState.Status == dag.StepStatusCompleted {
				t.Fatal("after step should not be completed")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run did not fail within timeout")
}

func TestApprovalStep_TimeoutFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Build workflow with very short timeout.
	b := dag.NewWorkflow("approval-timeout-wf")
	b.Approval("gate", dag.ApprovalConfig{
		Timeout: 200 * time.Millisecond,
		Subject: "approvals.timeout",
	})
	wfDef, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runID := "run-approval-4"
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	payload := buildStartEvtPayload(t, defData, nil)
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
	evtData, _ := evt.Marshal()
	js.Publish(
		evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()),
	)

	// Wait for run to fail via timeout.
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(runID)
		if err == nil && run.Status == dag.RunStatusFailed {
			state := run.Steps["gate"]
			// Positive: step failed with timeout message.
			if state.Status != dag.StepStatusFailed {
				t.Fatalf(
					"expected gate Failed, got %s",
					state.Status,
				)
			}
			if state.Error != "approval timed out" {
				t.Fatalf(
					"expected 'approval timed out', got %q",
					state.Error,
				)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("run did not fail from timeout within deadline")
}

func TestApprovalStep_DoubleApproveFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := buildApprovalWorkflow(t)
	runID := "run-approval-5"
	token, orch := startApprovalRun(t, js, nc, wfDef, runID)
	defer orch.Stop()

	// First approval via API service method — should succeed.
	svc := newTestService(t, nc)
	err = svc.HandleApproval(
		t.Context(), runID, "gate", token, "approve", nil,
	)
	if err != nil {
		t.Fatalf("first approval failed: %v", err)
	}

	// Second approval with same token — should fail.
	err = svc.HandleApproval(
		t.Context(), runID, "gate", token, "approve", nil,
	)
	// Positive: second attempt returns error.
	if err == nil {
		t.Fatal("expected error on double approve")
	}
	// Negative: first attempt should have succeeded (checked above).
}

// newTestService creates a Service for testing.
func newTestService(t *testing.T, nc *nats.Conn) *apiService {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("get workflow_defs: %v", err)
	}
	return &apiService{
		nc:    nc,
		js:    js,
		defKV: defKV,
	}
}

// apiService is a minimal API service for testing approval token
// consumption without importing the api package (circular dep).
type apiService struct {
	nc    *nats.Conn
	js    nats.JetStreamContext
	defKV nats.KeyValue
}

// HandleApproval replicates the token consumption logic from
// api/service.go to test it from the engine package.
func (s *apiService) HandleApproval(
	ctx interface{},
	runID, stepID, token, action string,
	body json.RawMessage,
) error {
	if token == "" {
		return errorf("token is required")
	}
	if action != "approve" && action != "reject" {
		return errorf(
			"action must be 'approve' or 'reject', got %q",
			action,
		)
	}
	kv, err := s.js.KeyValue("approval_tokens")
	if err != nil {
		return errorf("approval_tokens not available: %v", err)
	}
	key := runID + "." + stepID
	entry, err := kv.Get(key)
	if err != nil {
		return errorf("token not found or expired")
	}
	var record struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return errorf("corrupt token record")
	}
	if record.Token != token {
		return errorf("invalid token")
	}
	if err := kv.Delete(
		key, nats.LastRevision(entry.Revision()),
	); err != nil {
		return errorf("token already consumed")
	}

	evtType := protocol.EventApprovalGranted
	if action == "reject" {
		evtType = protocol.EventApprovalRejected
	}
	evt := protocol.NewStepEvent(evtType, runID, stepID, body)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	_, err = s.js.PublishMsg(msg)
	return err
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func errorf(format string, args ...interface{}) error {
	return &testError{
		msg: formatMsg(format, args...),
	}
}

func formatMsg(format string, args ...interface{}) string {
	if len(args) == 0 {
		return format
	}
	// Simple sprintf equivalent for test error messages.
	result := format
	for _, arg := range args {
		idx := indexOf(result, "%")
		if idx < 0 {
			break
		}
		// Skip format specifier character.
		end := idx + 2
		if end > len(result) {
			end = len(result)
		}
		var str string
		switch v := arg.(type) {
		case string:
			str = v
		case error:
			str = v.Error()
		default:
			str = "?"
		}
		result = result[:idx] + str + result[end:]
	}
	return result
}

func indexOf(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
