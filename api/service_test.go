// api/service_test.go
// Tests for the control plane service: register workflows, start runs,
// get status.
// Methodology: real embedded NATS. Verify KV state after each operation.
package api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/trigger"
	"github.com/nats-io/nats.go"
)

func TestServiceRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("test-wf")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	err = svc.RegisterWorkflow(context.Background(), wfDef)
	if err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}
	got, err := svc.GetWorkflow("test-wf")
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	if got.Name != "test-wf" {
		t.Fatalf("Name = %q, want %q", got.Name, "test-wf")
	}
}

func TestServiceStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("test-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	runID, err := svc.StartRun(
		context.Background(), "test-wf", []byte(`"input"`),
	)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}
	if runID == "" {
		t.Fatal("runID must not be empty")
	}
}

func TestServiceGetRunStatus(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("test-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	runID, _ := svc.StartRun(context.Background(), "test-wf", nil)

	// Poll for snapshot (orchestrator processes async, bounded 5s).
	var run dag.WorkflowRun
	deadline := time.After(5 * time.Second)
	for {
		run, err = svc.GetRun(context.Background(), runID)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf(
				"run snapshot did not appear within 5s: %v", err,
			)
		case <-time.After(10 * time.Millisecond):
		}
	}

	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}
	if run.Status != dag.RunStatusPending &&
		run.Status != dag.RunStatusRunning {
		t.Fatalf(
			"Status = %v, want Pending or Running", run.Status,
		)
	}
}

func TestServiceGetRunNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	_, err = svc.GetRun(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
}

func TestServiceListWorkflows(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	wb1 := dag.NewWorkflow("wf-a")
	wb1.Task("a", "task-a")
	def1, _ := wb1.Build()
	wb2 := dag.NewWorkflow("wf-b")
	wb2.Task("b", "task-b")
	def2, _ := wb2.Build()
	svc.RegisterWorkflow(context.Background(), def1)
	svc.RegisterWorkflow(context.Background(), def2)
	defs, err := svc.ListWorkflows(context.Background())
	if err != nil {
		t.Fatalf("ListWorkflows failed: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("len(defs) = %d, want 2", len(defs))
	}
}

func TestServiceCancelRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("test-wf")
	wb.Task("a", "task-a")
	def, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), def)
	runID, _ := svc.StartRun(context.Background(), "test-wf", nil)
	err = svc.CancelRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("CancelRun failed: %v", err)
	}
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history." + runID)
	defer sub.Unsubscribe()
	found := false
	deadline := time.After(2 * time.Second)
	for {
		msg, err := sub.NextMsg(10 * time.Millisecond)
		if err != nil {
			select {
			case <-deadline:
				if !found {
					t.Fatal("workflow.cancelled event not found")
				}
				return
			case <-time.After(5 * time.Millisecond):
				continue
			}
		}
		var evt protocol.Event
		json.Unmarshal(msg.Data, &evt)
		if evt.Type == protocol.EventWorkflowCancelled {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("workflow.cancelled event not published")
	}
}

func TestServiceSendSignal(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "signals"}),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	runID := "test-run-123"
	data := []byte(`{"value":42}`)
	err = svc.SendSignal(context.Background(), runID, "sig1", data)
	if err != nil {
		t.Fatalf("SendSignal failed: %v", err)
	}
	entry, err := svc.signalKV.Get(runID + ".sig1")
	if err != nil {
		t.Fatalf("signal not written to KV: %v", err)
	}
	if string(entry.Value()) != string(data) {
		t.Fatalf("data = %q, want %q", entry.Value(), data)
	}
}

func TestServiceCreateTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	def := trigger.TriggerDef{
		ID:         "trig-1",
		WorkflowID: "wf-a",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	err = svc.CreateTrigger(context.Background(), def)
	if err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}
	entry, err := svc.triggerKV.Get("trig-1")
	if err != nil {
		t.Fatalf("trigger not written to KV: %v", err)
	}
	var stored trigger.TriggerDef
	json.Unmarshal(entry.Value(), &stored)
	if stored.ID != "trig-1" {
		t.Fatalf("ID = %q, want %q", stored.ID, "trig-1")
	}
}

func TestServiceCreateTriggerValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	def := trigger.TriggerDef{
		ID:         "bad-trig",
		WorkflowID: "wf-a",
		Enabled:    true,
	}
	err = svc.CreateTrigger(context.Background(), def)
	if err == nil {
		t.Fatal("expected validation error for trigger with no type")
	}
}

func TestServiceListTriggers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	def1 := trigger.TriggerDef{
		ID:         "trig-1",
		WorkflowID: "wf-a",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	def2 := trigger.TriggerDef{
		ID:         "trig-2",
		WorkflowID: "wf-b",
		Enabled:    true,
		Subject:    &trigger.SubjectConfig{Subject: "test.>"},
	}
	svc.CreateTrigger(context.Background(), def1)
	svc.CreateTrigger(context.Background(), def2)
	defs, err := svc.ListTriggers(context.Background())
	if err != nil {
		t.Fatalf("ListTriggers failed: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("len(defs) = %d, want 2", len(defs))
	}
}

func TestServiceDeleteTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	def := trigger.TriggerDef{
		ID:         "trig-del",
		WorkflowID: "wf-a",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	svc.CreateTrigger(context.Background(), def)
	err = svc.DeleteTrigger(context.Background(), "trig-del")
	if err != nil {
		t.Fatalf("DeleteTrigger failed: %v", err)
	}
	_, err = svc.triggerKV.Get("trig-del")
	if err == nil {
		t.Fatal("trigger should be deleted")
	}
}

func TestServiceListDeadLetters(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	js, _ := nc.JetStream()
	payload := protocol.TaskPayload{
		RunID:  "run-123",
		StepID: "step-1",
	}
	data, _ := json.Marshal(payload)
	msg := &nats.Msg{
		Subject: "dead.task-a",
		Data:    data,
		Header:  nats.Header{"Error": {"test error"}},
	}
	_, err = js.PublishMsg(msg)
	if err != nil {
		t.Fatalf("PublishMsg failed: %v", err)
	}
	letters, err := svc.ListDeadLetters(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListDeadLetters failed: %v", err)
	}
	if len(letters) == 0 {
		t.Fatal("expected at least one dead letter")
	}
	if letters[0].RunID != "run-123" {
		t.Fatalf("RunID = %q, want %q", letters[0].RunID, "run-123")
	}
}

func TestServiceReplayDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	js, _ := nc.JetStream()
	payload := protocol.TaskPayload{
		RunID:  "run-456",
		StepID: "step-2",
	}
	data, _ := json.Marshal(payload)
	msg := &nats.Msg{
		Subject: "dead.task-replay",
		Data:    data,
		Header:  nats.Header{"Error": {"replay test"}},
	}
	ack, err := js.PublishMsg(msg)
	if err != nil {
		t.Fatalf("PublishMsg failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	sub, subErr := js.SubscribeSync("task.task-replay")
	if subErr != nil {
		t.Fatalf("SubscribeSync failed: %v", subErr)
	}
	defer sub.Unsubscribe()
	err = svc.ReplayDeadLetter(context.Background(), ack.Sequence)
	if err != nil {
		t.Fatalf("ReplayDeadLetter failed: %v", err)
	}
	replayed, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("replayed message not received: %v", err)
	}
	var replayPayload protocol.TaskPayload
	json.Unmarshal(replayed.Data, &replayPayload)
	if replayPayload.RunID != "run-456" {
		t.Fatalf("RunID = %q, want %q", replayPayload.RunID, "run-456")
	}
}

func TestServiceListRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("list-test-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)

	runID1, _ := svc.StartRun(context.Background(), "list-test-wf", nil)
	runID2, _ := svc.StartRun(context.Background(), "list-test-wf", nil)

	deadline := time.After(5 * time.Second)
	runsFound := 0
	for {
		runs, err := svc.ListRuns(context.Background(), "")
		if err != nil {
			t.Fatalf("ListRuns failed: %v", err)
		}
		foundRun1 := false
		foundRun2 := false
		for _, run := range runs {
			if run.RunID == runID1 {
				foundRun1 = true
			}
			if run.RunID == runID2 {
				foundRun2 = true
			}
		}
		if foundRun1 && foundRun2 {
			runsFound = len(runs)
			break
		}
		select {
		case <-deadline:
			t.Fatal("runs did not appear in list within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if runsFound < 2 {
		t.Fatalf("expected at least 2 runs, got %d", runsFound)
	}
}

func TestServiceListRunsFilterByWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	wb1 := dag.NewWorkflow("filter-wf-a")
	wb1.Task("a", "task-a")
	def1, _ := wb1.Build()
	wb2 := dag.NewWorkflow("filter-wf-b")
	wb2.Task("b", "task-b")
	def2, _ := wb2.Build()
	svc.RegisterWorkflow(context.Background(), def1)
	svc.RegisterWorkflow(context.Background(), def2)

	runIDA, _ := svc.StartRun(context.Background(), "filter-wf-a", nil)
	svc.StartRun(context.Background(), "filter-wf-b", nil)

	deadline := time.After(5 * time.Second)
	for {
		runs, err := svc.ListRuns(context.Background(), "filter-wf-a")
		if err != nil {
			t.Fatalf("ListRuns failed: %v", err)
		}
		foundA := false
		for _, run := range runs {
			if run.RunID == runIDA {
				foundA = true
			}
			if run.WorkflowID != "filter-wf-a" {
				t.Fatalf(
					"filtered list contains wrong workflow: %s",
					run.WorkflowID,
				)
			}
		}
		if foundA {
			break
		}
		select {
		case <-deadline:
			t.Fatal("run did not appear in filtered list within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestServiceListRunEvents(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopTelemetry())
	wb := dag.NewWorkflow("events-test-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)
	runID, _ := svc.StartRun(context.Background(), "events-test-wf", nil)

	time.Sleep(200 * time.Millisecond)

	events, err := svc.ListRunEvents(context.Background(), runID, false)
	if err != nil {
		t.Fatalf("ListRunEvents failed: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}
	foundStarted := false
	for _, evt := range events {
		if evt.RunID != runID {
			t.Fatalf("event RunID = %q, want %q", evt.RunID, runID)
		}
		if evt.Type == string(protocol.EventWorkflowStarted) {
			foundStarted = true
		}
	}
	if !foundStarted {
		t.Fatal("workflow.started event not found in history")
	}
}
