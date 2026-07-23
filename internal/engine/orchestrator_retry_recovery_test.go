// engine/orchestrator_retry_recovery_test.go
// Retry, failure-recovery, and compensation tests for the orchestrator:
// retry policy, retry exhaustion, dead-letter publication, on-failure steps,
// compensation chains, non-retriable failures, exact retry-after delay, and
// legacy payload handling. Uses real embedded NATS server.
// Methodology: publish step-failed events to the history stream, let the
// orchestrator process them, then verify retry scheduling, dead-letter
// output, and terminal state. Each test gets its own embedded server.

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrchestratorRetriesWithPolicy(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "retry-test", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
		},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "flaky-task", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "retry-test", defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "retry-run-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	waitForStepStatus(t, orch.store, "retry-run-1", "s1",
		dag.StepStatusQueued, 5*time.Second)

	// First failure — should not be permanently failed
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "retry-run-1", "s1",
		[]byte(`"transient error"`))
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})
	waitForStepAttempts(t, orch.store, "retry-run-1", "s1",
		1, 5*time.Second)

	store := NewSnapshotStore(jsNew)
	run, _ := store.Load(context.Background(), "retry-run-1")

	// Positive: run is still running (not failed yet)
	if run.Status != dag.RunStatusRunning {
		t.Fatalf("status = %v after 1 failure, want Running",
			run.Status)
	}

	// Positive: step has 1 attempt recorded
	if run.Steps["s1"].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1",
			run.Steps["s1"].Attempts)
	}
}

func TestOrchestratorExhaustsRetries(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "exhaust-test", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  2,
			Strategy:     dag.RetryFixed,
			InitialDelay: 50 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "bad-task", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "exhaust-test", defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "exhaust-run-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	waitForStepStatus(t, orch.store, "exhaust-run-1", "s1",
		dag.StepStatusQueued, 5*time.Second)

	// Fail 3 times (> MaxAttempts of 2). Mirror production: worker
	// emits step.started before step.failed. Attempts is owned by
	// step.queued/step.started lifecycle events (max() rule);
	// step.failed only updates state.
	for i := 0; i < 3; i++ {
		startedEvt := protocol.NewStepEvent(
			protocol.EventStepStarted, "exhaust-run-1", "s1", nil,
		)
		startedEvt.AttemptNumber = i + 1
		startedData, err := startedEvt.Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		mustPublishMsg(t, js, &nats.Msg{
			Subject: startedEvt.NATSSubject(), Data: startedData,
			Header: nats.Header{
				"Nats-Msg-Id": {startedEvt.NATSMsgID()},
			},
		})
		time.Sleep(50 * time.Millisecond)

		failEvt := protocol.NewStepEvent(
			protocol.EventStepFailed, "exhaust-run-1", "s1",
			[]byte(`"permanent error"`))
		// Unique msg ID per attempt
		msgID := fmt.Sprintf("exhaust-run-1.s1.fail.%d", i)
		failData, err := failEvt.Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		mustPublishMsg(t, js, &nats.Msg{
			Subject: failEvt.NATSSubject(), Data: failData,
			Header: nats.Header{"Nats-Msg-Id": {msgID}},
		})
		time.Sleep(100 * time.Millisecond)
	}

	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "exhaust-run-1")
		if err == nil && run.Status == dag.RunStatusFailed {
			// Positive: permanently failed
			if run.Steps["s1"].Status != dag.StepStatusFailed {
				t.Fatalf("step = %v, want Failed",
					run.Steps["s1"].Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be failed after exhausting retries")
}

func TestOrchestratorPublishesDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "dlq-test",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "bad-task", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "dlq-test", defData)

	// Subscribe to DLQ
	dlqSub, err := js.SubscribeSync("dead.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "dlq-run-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	waitForStepStatus(t, orch.store, "dlq-run-1", "s1",
		dag.StepStatusQueued, 5*time.Second)

	// Fail the step permanently (no retries configured)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "dlq-run-1", "s1",
		[]byte(`"permanent error"`))
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Positive: DLQ message appears
	dlqMsg, err := dlqSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected DLQ message: %v", err)
	}
	dlqMsg.Ack()

	// Positive: subject contains task name
	if !strings.HasPrefix(dlqMsg.Subject, "dead.bad-task.") {
		t.Fatalf("DLQ subject = %q, want prefix dead.bad-task.",
			dlqMsg.Subject)
	}
}

func TestOrchestratorOnFailureStep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	// Workflow: deploy fails → notify runs
	wfDef := dag.WorkflowDef{
		Name:    "onfail-test",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:        "deploy",
				Task:      "deploy-task",
				Type:      dag.StepTypeNormal,
				OnFailure: "notify",
			},
			{
				ID:   "notify",
				Task: "notify-task",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "onfail-test", defData)

	// Subscribe to task queue for notify
	taskSub, _ := js.SubscribeSync("task.notify-task.>",
		nats.AckExplicit(), nats.DeliverAll())

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "onfail-run-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	waitForStepStatus(t, orch.store, "onfail-run-1", "deploy",
		dag.StepStatusQueued, 5*time.Second)

	// Fail deploy step permanently
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "onfail-run-1", "deploy",
		[]byte(`"deploy crashed"`))
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Positive: notify task should be enqueued
	msg, err := taskSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected notify task to be enqueued: %v", err)
	}
	msg.Ack()

	// Positive: workflow should NOT be failed yet (on-failure is running)
	waitForStepStatus(t, orch.store, "onfail-run-1", "deploy",
		dag.StepStatusFailed, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, _ := store.Load(context.Background(), "onfail-run-1")
	if run.Status == dag.RunStatusFailed {
		t.Fatalf("workflow should not be failed while on-failure step pending")
	}
}

func TestOrchestratorCompensationChain(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "comp-test",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal,
				Compensate: "undo-a"},
			{ID: "b", Task: "task-b", DependsOn: []string{"a"},
				Type:  dag.StepTypeNormal,
				Retry: &dag.RetryPolicy{MaxAttempts: 1}},
			{ID: "undo-a", Task: "task-undo-a",
				Type: dag.StepTypeNormal},
		},
		AuxSteps: map[string]bool{"undo-a": true},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	runID := "comp-run-1"
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, defData)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, evt.NATSSubject(), evtData,
		nats.MsgId(evt.NATSMsgID()))

	// Complete step a
	sub, _ := js.PullSubscribe("task.task-a.*",
		"", nats.BindStream("TASK_QUEUES"))
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected task-a, got err=%v len=%d",
			err, len(msgs))
	}
	msgs[0].Ack()

	completeEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, runID,
		[]byte(`{"result":"ok"}`))
	completeEvt.StepID = "a"
	completeData, err := completeEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, completeEvt.NATSSubject(), completeData,
		nats.MsgId(completeEvt.NATSMsgID()))

	// Fail step b permanently (non-retriable)
	subB, _ := js.PullSubscribe("task.task-b.*",
		"", nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsB) != 1 {
		t.Fatalf("expected task-b, got err=%v len=%d",
			err, len(msgsB))
	}
	msgsB[0].Ack()

	failEvt := protocol.NewWorkflowEvent(
		protocol.EventStepFailed, runID,
		[]byte(`{"error":"boom","failure_type":"non_retriable"}`))
	failEvt.StepID = "b"
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()))

	// Positive: undo-a compensation task should be dispatched
	subUndo, _ := js.PullSubscribe("task.task-undo-a.*",
		"", nats.BindStream("TASK_QUEUES"))
	msgsUndo, err := subUndo.Fetch(
		1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("expected undo-a task: %v", err)
	}
	if len(msgsUndo) != 1 {
		t.Fatalf("expected 1 undo task, got %d",
			len(msgsUndo))
	}

	// Positive: undo task payload has compensation context
	var payload protocol.TaskPayload
	if err := json.Unmarshal(
		msgsUndo[0].Data, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload.StepID != "undo-a" {
		t.Errorf("step = %s, want undo-a", payload.StepID)
	}
}

func TestNonRetriableFailureSkipsRetries(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "test-nr", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  5,
			Strategy:     dag.RetryFixed,
			InitialDelay: time.Second,
		},
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Wait for task-a to be enqueued (proves start was processed).
	taskSub, subErr := js.SubscribeSync(
		"task.task-a.>",
		nats.AckExplicit(),
		nats.DeliverAll(),
	)
	if subErr != nil {
		t.Fatalf("subscribe task-a: %v", subErr)
	}

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"run-nr-1",
		defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    startData,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
		},
	})

	// Wait for the task to appear — proves workflow was created.
	taskMsg, taskErr := taskSub.NextMsg(3 * time.Second)
	if taskErr != nil {
		t.Fatalf("task-a not enqueued: %v", taskErr)
	}
	taskMsg.Ack()

	// Mirror production: worker emits step.started before step.failed.
	// Attempts is owned by step.queued/step.started lifecycle events
	// (max() rule); step.failed only updates state.
	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-nr-1", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	startedData, err := startedEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startedEvt.NATSSubject(),
		Data:    startedData,
		Header: nats.Header{
			"Nats-Msg-Id": {startedEvt.NATSMsgID()},
		},
	})

	failPayload := mustMarshal(t, protocol.StepFailedPayload{
		Error:       "permanent error",
		FailureType: protocol.FailureTypeNonRetriable,
	})
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed,
		"run-nr-1", "a",
		failPayload,
	)
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: failEvt.NATSSubject(),
		Data:    failData,
		Header: nats.Header{
			"Nats-Msg-Id": {failEvt.NATSMsgID()},
		},
	})

	waitForRunStatus(t, orch.store, "run-nr-1",
		dag.RunStatusFailed, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, loadErr := store.Load(context.Background(), "run-nr-1")
	if loadErr != nil {
		t.Fatalf("load run after fail: %v", loadErr)
	}

	// Positive: non-retriable should fail the workflow
	// immediately despite 5 max retries configured.
	if run.Status != dag.RunStatusFailed {
		t.Fatalf(
			"run status = %s, want failed", run.Status,
		)
	}

	// Negative: should have only 1 attempt (no retries).
	stepState := run.Steps["a"]
	if stepState.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", stepState.Attempts)
	}
}

func TestRetryAfterSchedulesExactDelay(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "test-ra", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: time.Minute,
		},
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-ra", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startPayload := mustMarshal(t, map[string]any{
		"workflow_def": wfDef,
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-ra-1", startPayload,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch.store, "run-ra-1", "a",
		dag.StepStatusQueued, 5*time.Second)

	failPayload := mustMarshal(t, protocol.StepFailedPayload{
		Error:        "rate limited",
		FailureType:  protocol.FailureTypeRetryAfter,
		RetryAfterMs: 200,
	})
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "run-ra-1", "a", failPayload,
	)
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()))

	// The task should be re-published after ~200ms via SLEEP_TIMERS.
	sub, _ := js.PullSubscribe(
		"task.task-ra.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	// Skip initial enqueue
	msgs, fetchErr := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if fetchErr != nil {
		t.Fatalf("initial task not received: %v", fetchErr)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 initial task, got %d", len(msgs))
	}

	// Second message = retry after timer fired
	retryMsgs, retryErr := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if retryErr != nil {
		t.Fatalf("retry task not received within 5s: %v", retryErr)
	}
	if len(retryMsgs) != 1 {
		t.Fatalf("expected 1 retry task, got %d", len(retryMsgs))
	}

	// Verify run is NOT failed (retries remain)
	time.Sleep(100 * time.Millisecond)
	store := NewSnapshotStore(jsNew)
	run, loadErr := store.Load(context.Background(), "run-ra-1")
	if loadErr != nil {
		t.Fatalf("load run: %v", loadErr)
	}
	if run.Status == dag.RunStatusFailed {
		t.Fatal("run should not be failed — retries remain")
	}
}

func TestOldStringPayloadTreatedAsRetriable(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "test-compat", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: time.Second,
		},
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"run-compat",
		defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js,
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	waitForStepStatus(t, orch.store, "run-compat", "a",
		dag.StepStatusQueued, 5*time.Second)

	oldPayload := []byte(`"transient error"`)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed,
		"run-compat", "a",
		oldPayload,
	)
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js,
		failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)
	store := NewSnapshotStore(jsNew)
	run, loadErr := store.Load(context.Background(), "run-compat")
	if loadErr != nil {
		t.Fatalf("load run: %v", loadErr)
	}

	// Positive: old format should be treated as retriable,
	// not cause immediate permanent failure.
	if run.Status == dag.RunStatusFailed {
		t.Fatal(
			"old format payload should be retriable, " +
				"not permanent",
		)
	}

	// Negative: should have recorded exactly 1 attempt.
	stepState := run.Steps["a"]
	if stepState.Attempts != 1 {
		t.Fatalf(
			"attempts = %d, want 1", stepState.Attempts,
		)
	}
}
