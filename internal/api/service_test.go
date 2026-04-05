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
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

func TestServiceSetTriggerEnabled(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Store a trigger directly in KV with Enabled: true
	js, jsErr := nc.JetStream()
	if jsErr != nil {
		t.Fatalf("JetStream failed: %v", jsErr)
	}
	trigKV, kvErr := js.KeyValue("triggers")
	if kvErr != nil {
		t.Fatalf("KeyValue failed: %v", kvErr)
	}
	def := trigger.TriggerDef{
		ID:         "trig-1",
		WorkflowID: "wf-1",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	}
	data, marshalErr := json.Marshal(def)
	if marshalErr != nil {
		t.Fatalf("Marshal failed: %v", marshalErr)
	}
	_, putErr := trigKV.Put("trig-1", data)
	if putErr != nil {
		t.Fatalf("Put failed: %v", putErr)
	}

	// Positive: disable the trigger
	ctx := context.Background()
	err = svc.SetTriggerEnabled(ctx, "trig-1", false)
	if err != nil {
		t.Fatalf("SetTriggerEnabled(false) failed: %v", err)
	}
	entry, err := trigKV.Get("trig-1")
	if err != nil {
		t.Fatalf("Get after disable failed: %v", err)
	}
	var disabled trigger.TriggerDef
	json.Unmarshal(entry.Value(), &disabled)
	if disabled.Enabled {
		t.Fatal("expected Enabled=false after disable")
	}

	// Positive: re-enable the trigger
	err = svc.SetTriggerEnabled(ctx, "trig-1", true)
	if err != nil {
		t.Fatalf("SetTriggerEnabled(true) failed: %v", err)
	}
	entry, err = trigKV.Get("trig-1")
	if err != nil {
		t.Fatalf("Get after enable failed: %v", err)
	}
	var enabled trigger.TriggerDef
	json.Unmarshal(entry.Value(), &enabled)
	if !enabled.Enabled {
		t.Fatal("expected Enabled=true after enable")
	}

	// Negative: non-existent trigger returns error
	err = svc.SetTriggerEnabled(ctx, "no-such-trigger", false)
	if err == nil {
		t.Fatal("expected error for non-existent trigger")
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

func TestRegisterWorkflowInvalidDef(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: workflow with no steps returns validation error.
	badDef := dag.WorkflowDef{Name: "bad-wf"}
	err := svc.RegisterWorkflow(context.Background(), badDef)
	if err == nil {
		t.Fatal("expected error for invalid workflow def")
	}

	// Negative: valid workflow does not return error.
	wb := dag.NewWorkflow("valid-wf")
	wb.Task("a", "task-a")
	goodDef, _ := wb.Build()
	err = svc.RegisterWorkflow(context.Background(), goodDef)
	if err != nil {
		t.Fatalf("valid workflow should succeed: %v", err)
	}
}

func TestStartRunUnknownWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: unknown workflow returns error.
	_, err := svc.StartRun(
		context.Background(), "nonexistent", nil,
	)
	if err == nil {
		t.Fatal("expected error for unknown workflow")
	}

	// Negative: error message mentions the workflow name.
	if !contains(err.Error(), "nonexistent") {
		t.Fatalf("error should mention workflow: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchString(s, sub)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestGetWorkflowReturnsRegistered(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: register and retrieve a workflow.
	wb := dag.NewWorkflow("get-wf")
	wb.Task("a", "task-a")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	err = svc.RegisterWorkflow(context.Background(), def)
	if err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}
	got, err := svc.GetWorkflow("get-wf")
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	if got.Name != "get-wf" {
		t.Fatalf("Name = %q, want %q", got.Name, "get-wf")
	}

	// Negative: unknown workflow returns error.
	_, err = svc.GetWorkflow("no-such-workflow")
	if err == nil {
		t.Fatal("expected error for unknown workflow")
	}
}

func TestDeleteTriggerInnerRemovesKey(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Seed a trigger directly in KV.
	def := trigger.TriggerDef{
		ID:         "del-inner",
		WorkflowID: "wf-x",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	data, _ := json.Marshal(def)
	_, putErr := svc.triggerKV.Put("del-inner", data)
	if putErr != nil {
		t.Fatalf("Put failed: %v", putErr)
	}

	// Positive: deleteTriggerInner removes the key.
	err = svc.deleteTriggerInner("del-inner")
	if err != nil {
		t.Fatalf("deleteTriggerInner failed: %v", err)
	}
	_, err = svc.triggerKV.Get("del-inner")
	if err == nil {
		t.Fatal("key should be deleted after deleteTriggerInner")
	}
}

func TestDeleteTriggerInnerNilKV(t *testing.T) {
	// Verify deleteTriggerInner returns error when KV is nil.
	svc := &Service{triggerKV: nil}
	err := svc.deleteTriggerInner("any-id")
	if err == nil {
		t.Fatal("expected error when triggerKV is nil")
	}
}

func TestListRunEventsInnerReadsHistory(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Publish a history event directly.
	js, _ := nc.JetStream()
	runID := "events-inner-run"
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, []byte("{}"),
	)
	data, _ := evt.Marshal()
	_, pubErr := js.Publish(evt.NATSSubject(), data)
	if pubErr != nil {
		t.Fatalf("Publish failed: %v", pubErr)
	}
	time.Sleep(100 * time.Millisecond)

	// Positive: events are returned for the run.
	events, err := svc.listRunEventsInner(runID, false)
	if err != nil {
		t.Fatalf("listRunEventsInner failed: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}

	// Negative: data should be truncated when fullData is false.
	// (The default payload is short, so just confirm no error.)
	if events[0].RunID != runID {
		t.Fatalf("RunID = %q, want %q", events[0].RunID, runID)
	}
}

func TestIsTaskSubject(t *testing.T) {
	// Positive: subjects starting with "task." are task subjects.
	if !isTaskSubject("task.x.y") {
		t.Fatal("expected task.x.y to be a task subject")
	}
	if !isTaskSubject("task.a") {
		t.Fatal("expected task.a to be a task subject")
	}

	// Negative: other prefixes are not task subjects.
	if isTaskSubject("history.x") {
		t.Fatal("history.x should not be a task subject")
	}
	if isTaskSubject("dead") {
		t.Fatal("dead should not be a task subject")
	}
}

// testSpanWithIDs is a test double that implements both Span and
// SpanContext with configurable trace/span IDs.
type testSpanWithIDs struct {
	observe.Span
	traceID string
	spanID  string
}

func (s *testSpanWithIDs) TraceID() string { return s.traceID }
func (s *testSpanWithIDs) SpanID() string  { return s.spanID }
func (s *testSpanWithIDs) End()            {}
func (s *testSpanWithIDs) SetStatus(
	code observe.StatusCode, desc string,
) {
}
func (s *testSpanWithIDs) SetAttributes(
	attrs ...observe.Attribute,
) {
}
func (s *testSpanWithIDs) RecordError(err error)                     {}
func (s *testSpanWithIDs) AddEvent(n string, a ...observe.Attribute) {}

func TestInjectAPIMsgTraceCtx(t *testing.T) {
	span := &testSpanWithIDs{
		traceID: "aaaa1111bbbb2222cccc3333dddd4444",
		spanID:  "eeee5555ffff6666",
	}
	msg := &nats.Msg{Header: nats.Header{}}

	// Positive: traceparent header is set.
	injectAPIMsgTraceCtx(span, msg)
	tp := msg.Header.Get("traceparent")
	expected := "00-aaaa1111bbbb2222cccc3333dddd4444" +
		"-eeee5555ffff6666-01"
	if tp != expected {
		t.Fatalf("traceparent = %q, want %q", tp, expected)
	}

	// Negative: noop span (empty IDs) does not set header.
	msg2 := &nats.Msg{Header: nats.Header{}}
	noopSpan := observe.NewNoopTracer()
	_, ns := noopSpan.Start(context.Background(), "test")
	injectAPIMsgTraceCtx(ns, msg2)
	if msg2.Header.Get("traceparent") != "" {
		t.Fatal("noop span should not set traceparent")
	}
}

func TestExtractTaskFromSubject(t *testing.T) {
	// Positive: "dead." prefix is stripped.
	got := extractTaskFromSubject("dead.task-a")
	if got != "task-a" {
		t.Fatalf("got = %q, want %q", got, "task-a")
	}

	// Negative: non-"dead." subject returned as-is.
	got = extractTaskFromSubject("task.a")
	if got != "task.a" {
		t.Fatalf("got = %q, want %q", got, "task.a")
	}
}

func TestInjectAPITraceCtxWithIDs(t *testing.T) {
	span := &testSpanWithIDs{
		traceID: "aaaa1111bbbb2222cccc3333dddd4444",
		spanID:  "eeee5555ffff6666",
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-1", nil,
	)

	// Positive: trace context is injected.
	injectAPITraceCtx(span, &evt)
	expected := "00-aaaa1111bbbb2222cccc3333dddd4444" +
		"-eeee5555ffff6666-01"
	if evt.TraceParent != expected {
		t.Fatalf(
			"TraceParent = %q, want %q",
			evt.TraceParent, expected,
		)
	}

	// Negative: noop span leaves TraceParent empty.
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-2", nil,
	)
	noopTracer := observe.NewNoopTracer()
	_, ns := noopTracer.Start(context.Background(), "test")
	injectAPITraceCtx(ns, &evt2)
	if evt2.TraceParent != "" {
		t.Fatal("noop span should not set TraceParent")
	}
}

func TestSendSignalNilKVError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	// Create service without signals KV bucket.
	svc := NewService(nc, observe.NewNoopTelemetry())
	svc.signalKV = nil

	// Positive: SendSignal returns error when KV is nil.
	err := svc.SendSignal(
		context.Background(), "run-1", "sig", []byte("{}"),
	)
	if err == nil {
		t.Fatal("expected error when signalKV is nil")
	}

	// Negative: error message mentions KV.
	if !contains(err.Error(), "not available") {
		t.Fatalf("error should mention KV: %v", err)
	}
}

func TestDeleteTriggerNilKVErrorPath(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	svc.triggerKV = nil

	// Positive: DeleteTrigger returns error when KV is nil.
	err := svc.DeleteTrigger(
		context.Background(), "any-trig",
	)
	if err == nil {
		t.Fatal("expected error when triggerKV is nil")
	}

	// Negative: error mentions KV unavailability.
	if !contains(err.Error(), "not available") {
		t.Fatalf("error should mention KV: %v", err)
	}
}

func TestReplayDeadLetterNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: replaying nonexistent sequence returns error.
	err := svc.ReplayDeadLetter(context.Background(), 99999)
	if err == nil {
		t.Fatal("expected error for nonexistent sequence")
	}

	// Negative: error mentions the sequence number.
	if !contains(err.Error(), "99999") {
		t.Fatalf("error should mention sequence: %v", err)
	}
}

func TestListRunEventsErrorPath(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: listing events for nonexistent run returns
	// empty (not error), because SubscribeSync succeeds but
	// NextMsg times out.
	events, err := svc.ListRunEvents(
		context.Background(), "no-run", false,
	)
	if err != nil {
		t.Fatalf("ListRunEvents failed: %v", err)
	}

	// Negative: no events returned for nonexistent run.
	if len(events) != 0 {
		t.Fatalf(
			"expected 0 events, got %d", len(events),
		)
	}
}

func TestSetTriggerEnabledNilKVError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	svc.triggerKV = nil

	// Positive: returns error when KV is nil.
	err := svc.SetTriggerEnabled(
		context.Background(), "t1", false,
	)
	if err == nil {
		t.Fatal("expected error when triggerKV is nil")
	}

	// Negative: error mentions unavailability.
	if !contains(err.Error(), "not available") {
		t.Fatalf("error should mention KV: %v", err)
	}
}

func TestListTriggersNilKV(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	svc.triggerKV = nil

	// Positive: nil KV returns empty list, not error.
	defs, err := svc.ListTriggers(context.Background())
	if err != nil {
		t.Fatalf("ListTriggers failed: %v", err)
	}

	// Negative: result is empty.
	if len(defs) != 0 {
		t.Fatalf("expected 0 triggers, got %d", len(defs))
	}
}

func TestCreateTriggerNilKVError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())
	svc.triggerKV = nil

	def := trigger.TriggerDef{
		ID:         "t1",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}

	// Positive: CreateTrigger returns error when KV is nil.
	err := svc.CreateTrigger(context.Background(), def)
	if err == nil {
		t.Fatal("expected error when triggerKV is nil")
	}

	// Negative: error mentions unavailability.
	if !contains(err.Error(), "not available") {
		t.Fatalf("error should mention KV: %v", err)
	}
}

func TestNewServicePanicsNilNC(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil nc")
		}
	}()
	NewService(nil, observe.NewNoopTelemetry())
}

func TestNewServicePanicsNilTel(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil tel")
		}
	}()
	NewService(nc, nil)
}

func TestInjectAPIMsgTraceCtxNilHeader(t *testing.T) {
	span := &testSpanWithIDs{
		traceID: "aaaa1111bbbb2222cccc3333dddd4444",
		spanID:  "eeee5555ffff6666",
	}
	// msg.Header is nil -- function should create it.
	msg := &nats.Msg{}

	injectAPIMsgTraceCtx(span, msg)
	tp := msg.Header.Get("traceparent")
	if tp == "" {
		t.Fatal("expected traceparent to be set on nil header")
	}

	// Verify the full format.
	expected := "00-aaaa1111bbbb2222cccc3333dddd4444" +
		"-eeee5555ffff6666-01"
	if tp != expected {
		t.Fatalf("traceparent = %q, want %q", tp, expected)
	}
}

func TestStartRunInputValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Register a workflow with InputSchema
	wb := dag.NewWorkflow("schema-validation-test")
	wb.Task("process", "process-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	def = dag.WithSchemas[struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}, any](def)

	if err := svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	// Positive: valid input passes
	validInput := []byte(`{"name":"alice","age":30}`)
	_, err = svc.StartRun(
		context.Background(), def.Name, validInput,
	)
	if err != nil {
		t.Fatalf("valid input should pass: %v", err)
	}

	// Negative: wrong type fails
	badInput := []byte(`{"name":123,"age":30}`)
	_, err = svc.StartRun(
		context.Background(), def.Name, badInput,
	)
	if err == nil {
		t.Fatal("expected error for wrong input type")
	}
	if !contains(err.Error(), "input validation") {
		t.Fatalf("error should mention validation: %v", err)
	}

	// Positive: nil input skips validation
	_, err = svc.StartRun(
		context.Background(), def.Name, nil,
	)
	if err != nil {
		t.Fatalf("nil input should skip validation: %v", err)
	}
}

func TestStartTyped(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	type input struct {
		Name string `json:"name"`
	}

	wb := dag.NewWorkflow("typed-start-test")
	wb.Task("greet", "greet-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	// Positive: typed start works
	runID, err := StartTyped(
		context.Background(), svc, def.Name, input{Name: "alice"},
	)
	if err != nil {
		t.Fatalf("StartTyped: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty runID")
	}
}

func TestServiceListWorkers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: no workers returns empty list
	workers, err := svc.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("len(workers) = %d, want 0", len(workers))
	}

	// Register two workers via Directory
	jsNew, _ := jetstream.New(nc)
	dir := worker.NewDirectory(jsNew)
	reg1 := worker.WorkerRegistration{
		WorkerID:  "worker-1",
		TaskTypes: []string{"task-a", "task-b"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  10,
	}
	reg2 := worker.WorkerRegistration{
		WorkerID:  "worker-2",
		TaskTypes: []string{"task-c"},
		Language:  "python",
		Transport: "nats",
		MaxTasks:  5,
	}
	if err := dir.Register(reg1); err != nil {
		t.Fatalf("Register worker-1: %v", err)
	}
	if err := dir.Register(reg2); err != nil {
		t.Fatalf("Register worker-2: %v", err)
	}

	// Positive: ListWorkers returns both workers
	workers, err = svc.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if len(workers) != 2 {
		t.Fatalf("len(workers) = %d, want 2", len(workers))
	}

	// Negative: verify worker fields are correct
	foundWorker1 := false
	foundWorker2 := false
	for _, w := range workers {
		if w.WorkerID == "worker-1" {
			foundWorker1 = true
			if w.Language != "go" {
				t.Fatalf("worker-1 Language = %q, want %q",
					w.Language, "go")
			}
		}
		if w.WorkerID == "worker-2" {
			foundWorker2 = true
			if w.Language != "python" {
				t.Fatalf("worker-2 Language = %q, want %q",
					w.Language, "python")
			}
		}
	}
	if !foundWorker1 {
		t.Fatal("worker-1 not found in list")
	}
	if !foundWorker2 {
		t.Fatal("worker-2 not found in list")
	}
}

func TestServiceListWorkersNoBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	// Positive: ListWorkers returns empty when bucket doesn't exist
	workers, err := svc.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}

	// Negative: result is empty
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers, got %d", len(workers))
	}
}
