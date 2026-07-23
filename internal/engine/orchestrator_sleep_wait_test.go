// engine/orchestrator_sleep_wait_test.go
// Sleep, rate-limit deferral, and wait-for-event tests for the
// orchestrator: sleep-step scheduling, rate-limited task deferral to the
// sleep-timers stream, and wait-for-event match/timeout paths. Uses real
// embedded NATS server.
// Methodology: publish events to the history stream, let the orchestrator
// process them, then verify deferred tasks on SLEEP_TIMERS versus
// TASK_QUEUES and wait-event resolution. Each test gets its own server.

package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrchestratorSleepStep(t *testing.T) {
	// Methodology: workflow has task-a -> sleep(100ms) -> task-b.
	// Start orchestrator, complete task-a manually, verify the sleep
	// step completes via durable timer, then task-b gets enqueued.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "sleep-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "echo-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "nap", Type: dag.StepTypeSleep,
				DependsOn: []string{"task-a"},
				Config:    dag.MarshalConfig(&dag.SleepConfig{Duration: 100 * time.Millisecond}),
			},
			{
				ID: "task-b", Task: "echo-b",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"nap"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "sleep-run-1", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain and complete task-a.
	subA, _ := js.PullSubscribe(
		"task.echo-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "sleep-run-1",
		"task-a", []byte(`"done"`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// task-b should appear after the sleep timer fires (~100ms).
	subB, _ := js.PullSubscribe(
		"task.echo-b.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(10*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-b failed (sleep didn't fire?): %v", err)
	}

	// Positive: task-b was enqueued.
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}
	msgsB[0].Ack()

	// Complete task-b so workflow finishes.
	compB := protocol.NewStepEvent(
		protocol.EventStepCompleted, "sleep-run-1",
		"task-b", []byte(`"final"`))
	compBData, err := compB.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compB.NATSSubject(), compBData,
		nats.MsgId(compB.NATSMsgID()))

	// Wait for workflow to complete.
	waitForRunStatus(t, orch.store, "sleep-run-1",
		dag.RunStatusCompleted, 5*time.Second)

	run, err := orch.store.Load(context.Background(), "sleep-run-1")
	if err != nil {
		t.Fatalf("load run failed: %v", err)
	}

	// Positive: workflow completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected run status Completed, got %s",
			run.Status,
		)
	}

	// Positive: sleep step completed.
	sleepState := run.Steps["nap"]
	if sleepState.Status != dag.StepStatusCompleted {
		t.Fatalf(
			"expected sleep step Completed, got %s",
			sleepState.Status,
		)
	}

	// Negative: task-a should not be in pending state.
	if run.Steps["task-a"].Status == dag.StepStatusPending {
		t.Fatal("task-a should not still be pending")
	}
}

func TestOrchestratorRateLimitDelaysTask(t *testing.T) {
	// Methodology: workflow with a single step that has a global rate
	// limit of 1 per 10 seconds. Start two runs quickly. The first
	// should get its task published immediately. The second should be
	// deferred to the SLEEP_TIMERS stream instead of TASK_QUEUES.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "rl-wf",
		Version: "1",
		Steps: []dag.StepDef{{
			ID:   "rl-step",
			Task: "rl-task",
			Type: dag.StepTypeNormal,
			RateLimit: &dag.RateLimit{
				Limit:  1,
				Period: 10 * time.Second,
			},
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start first run — should consume the one token.
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "rl-run-1", defData,
	)
	data1, err := evt1.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js,
		evt1.NATSSubject(), data1,
		nats.MsgId(evt1.NATSMsgID()),
	)

	// First task should appear on TASK_QUEUES.
	taskSub, err := js.PullSubscribe(
		"task.rl-task.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch first task: %v", err)
	}
	// Positive: first task published normally.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}
	msgs[0].Ack()

	// Start second run — rate limit should be exhausted.
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "rl-run-2", defData,
	)
	data2, err := evt2.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js,
		evt2.NATSSubject(), data2,
		nats.MsgId(evt2.NATSMsgID()),
	)

	// Second task should NOT appear on TASK_QUEUES.
	time.Sleep(500 * time.Millisecond)
	msgs2, _ := taskSub.Fetch(
		1, nats.MaxWait(500*time.Millisecond),
	)
	// Negative: second task was deferred, not published.
	if len(msgs2) > 0 {
		t.Fatal("second task should be deferred by rate limit")
	}

	// The deferred task should be on the SLEEP_TIMERS stream.
	sleepSub, err := js.PullSubscribe(
		"sleep.>", "",
		nats.BindStream("SLEEP_TIMERS"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe sleep: %v", err)
	}
	sleepMsgs, err := sleepSub.Fetch(
		1, nats.MaxWait(3*time.Second),
	)
	if err != nil {
		t.Fatalf("Fetch sleep timer: %v", err)
	}
	// Positive: a rate_retry timer was scheduled.
	if len(sleepMsgs) != 1 {
		t.Fatalf(
			"expected 1 sleep timer msg, got %d",
			len(sleepMsgs),
		)
	}

	var tm TimerMessage
	if err := json.Unmarshal(
		sleepMsgs[0].Data, &tm,
	); err != nil {
		t.Fatalf("unmarshal timer: %v", err)
	}
	// Positive: action is rate_retry.
	if tm.Action != TimerActionRateRetry {
		t.Fatalf("action = %q, want rate_retry", tm.Action)
	}
	// Negative: TaskType is set correctly.
	if tm.TaskType != "rl-task" {
		t.Fatalf("TaskType = %q, want rl-task", tm.TaskType)
	}
}

func TestOrchestratorWaitForEventMatches(t *testing.T) {
	// Methodology: workflow has task-a -> wait-for-event -> task-b.
	// Complete task-a with output containing order_id. Publish a
	// matching event to the EVENTS stream. Verify task-b runs and
	// workflow completes.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "wait-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "echo-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "wait-step",
				Type:      dag.StepTypeWaitForEvent,
				DependsOn: []string{"task-a"},
				Config: dag.MarshalConfig(&dag.WaitForEventOpts{
					Event: "payment.completed",
					Match: dag.Match{
						Left:  "order_id",
						Op:    dag.MatchOpEq,
						Right: "step.task-a.output.order_id",
					},
					Timeout: 5 * time.Second,
				}),
			},
			{
				ID: "task-b", Task: "echo-b",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"wait-step"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "wait-run-1", defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain and complete task-a with order_id output.
	subA, _ := js.PullSubscribe(
		"task.echo-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "wait-run-1",
		"task-a", []byte(`{"order_id":"ord-abc"}`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for the wait step to register with the correlator.
	waitForStepStatus(t, orch.store, "wait-run-1", "wait-step",
		dag.StepStatusRunning, 5*time.Second)

	// Publish a matching event on the EVENTS stream.
	eventPayload := []byte(
		`{"order_id":"ord-abc","status":"paid"}`,
	)
	mustPublish(t, js, "event.payment.completed", eventPayload)

	// task-b should be enqueued after the wait step matches.
	subB, _ := js.PullSubscribe(
		"task.echo-b.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-b failed (timeout?): %v", err)
	}

	// Positive: task-b was dispatched.
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}

	// Complete task-b to finish the workflow.
	msgsB[0].Ack()
	compB := protocol.NewStepEvent(
		protocol.EventStepCompleted, "wait-run-1",
		"task-b", []byte(`"final"`))
	compBData, err := compB.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compB.NATSSubject(), compBData,
		nats.MsgId(compB.NATSMsgID()))

	waitForRunStatus(t, orch.store, "wait-run-1",
		dag.RunStatusCompleted, 5*time.Second)

	// Positive: run should be completed.
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "wait-run-1")
	if err != nil {
		t.Fatalf("Load run failed: %v", err)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("run.Status = %v, want Completed", run.Status)
	}

	// Negative: wait step output should be the event payload.
	waitState := run.Steps["wait-step"]
	if string(waitState.Output) !=
		`{"order_id":"ord-abc","status":"paid"}` {
		t.Fatalf("wait step output = %s, want event payload",
			string(waitState.Output))
	}
}

func TestOrchestratorWaitForEventTimeout(t *testing.T) {
	// Methodology: workflow has task-a -> wait-for-event(200ms) ->
	// task-b. Complete task-a, do NOT publish matching event. Verify
	// the wait step completes with timeout output and task-b still
	// runs (timeout is not a failure).
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "wait-timeout-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "echo-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "wait-step",
				Type:      dag.StepTypeWaitForEvent,
				DependsOn: []string{"task-a"},
				Config: dag.MarshalConfig(&dag.WaitForEventOpts{
					Event: "payment.completed",
					Match: dag.Match{
						Left:  "order_id",
						Op:    dag.MatchOpEq,
						Right: "step.task-a.output.order_id",
					},
					Timeout: 200 * time.Millisecond,
				}),
			},
			{
				ID: "task-b", Task: "echo-b",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"wait-step"},
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
		protocol.EventWorkflowStarted, "wait-run-2", defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe(
		"task.echo-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "wait-run-2",
		"task-a", []byte(`{"order_id":"ord-xyz"}`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Do NOT publish a matching event. Wait for timeout.
	// task-b should still be enqueued after the timeout.
	subB, _ := js.PullSubscribe(
		"task.echo-b.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(10*time.Second))
	if err != nil {
		t.Fatalf(
			"Fetch task-b after timeout failed: %v", err,
		)
	}

	// Positive: task-b was dispatched after timeout.
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}

	// Check the wait step has timeout output.
	store := NewSnapshotStore(jsNew)
	run, loadErr := store.Load(context.Background(), "wait-run-2")
	if loadErr != nil {
		t.Fatalf("Load run failed: %v", loadErr)
	}
	waitState := run.Steps["wait-step"]

	// Positive: wait step is completed (not failed).
	if waitState.Status != dag.StepStatusCompleted {
		t.Fatalf("wait step status = %v, want Completed",
			waitState.Status)
	}

	// Negative: output indicates timeout, not a match.
	if string(waitState.Output) != `{"timeout":true}` {
		t.Fatalf("wait step output = %s, want timeout indicator",
			string(waitState.Output))
	}
}
