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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestServiceRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
	runID := "test-run-123"
	data := []byte(`{"value":42}`)
	err = svc.SendSignal(context.Background(), runID, "sig1", data)
	if err != nil {
		t.Fatalf("SendSignal failed: %v", err)
	}
	entry, err := svc.signalKV.Get(
		context.Background(), runID+".sig1",
	)
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
	svc := NewService(nc)
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
	entry, err := svc.triggerKV.Get(
		context.Background(), "trig-1",
	)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	_, err = svc.triggerKV.Get(context.Background(), "trig-del")
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
	svc := NewService(nc)

	// Store a trigger directly in KV with Enabled: true
	ctx := context.Background()
	trigKV := svc.triggerKV
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
	_, putErr := trigKV.Put(ctx, "trig-1", data)
	if putErr != nil {
		t.Fatalf("Put failed: %v", putErr)
	}

	// Positive: disable the trigger
	err = svc.SetTriggerEnabled(ctx, "trig-1", false)
	if err != nil {
		t.Fatalf("SetTriggerEnabled(false) failed: %v", err)
	}
	entry, err := trigKV.Get(ctx, "trig-1")
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
	entry, err = trigKV.Get(ctx, "trig-1")
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
	svc := NewService(nc)
	js, _ := nc.JetStream()
	payload := protocol.TaskPayload{
		RunID:  "run-123",
		StepID: "step-1",
	}
	data := mustMarshal(t, payload)
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
	svc := NewService(nc)
	js, _ := nc.JetStream()
	// Post-#200 shape: body is the TaskPayload, metadata in headers,
	// original task subject preserved verbatim.
	payload := protocol.TaskPayload{
		TaskID: "run-456.step-2",
		RunID:  "run-456",
		StepID: "step-2",
		Input:  []byte(`{"replay":"ok"}`),
	}
	data := mustMarshal(t, payload)
	taskSubject := "task.task-replay.run-456"
	msg := &nats.Msg{
		Subject: "dead.task-replay.run-456.step-2",
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id":                 {"dlq:run-456:step-2:1"},
			engine.HeaderDLQRunID:         {"run-456"},
			engine.HeaderDLQStepID:        {"step-2"},
			engine.HeaderDLQTask:          {"task-replay"},
			engine.HeaderDLQError:         {"replay test"},
			engine.HeaderDLQAttempts:      {"1"},
			engine.HeaderDLQDeliveryCount: {"1"},
			engine.HeaderDLQConsumer:      {engine.DLQConsumerTaskQueues},
			engine.HeaderDLQTaskSubject:   {taskSubject},
		},
	}
	ack, err := js.PublishMsg(msg)
	if err != nil {
		t.Fatalf("PublishMsg failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	sub, subErr := js.SubscribeSync(taskSubject)
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
	if err := json.Unmarshal(replayed.Data, &replayPayload); err != nil {
		t.Fatalf("unmarshal replay: %v", err)
	}
	if replayPayload.RunID != "run-456" {
		t.Fatalf("RunID = %q, want %q",
			replayPayload.RunID, "run-456")
	}
	if string(replayPayload.Input) != `{"replay":"ok"}` {
		t.Fatalf("Input = %q, want %q",
			replayPayload.Input, `{"replay":"ok"}`)
	}
}

func TestServiceListRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
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
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
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

// TestServiceListRunsRespectsLimit asserts ListRunsWithLimit caps the
// returned slice at the caller-supplied limit. We submit more runs
// than the limit, wait for them all to surface, then request fewer.
func TestServiceListRunsRespectsLimit(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("limit-test-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	if err := svc.RegisterWorkflow(
		context.Background(), wfDef,
	); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	const submitted = 5
	for i := 0; i < submitted; i++ {
		if _, err := svc.StartRun(
			context.Background(), "limit-test-wf", nil,
		); err != nil {
			t.Fatalf("StartRun %d: %v", i, err)
		}
	}

	// Wait until all submitted runs are visible in the store before
	// asserting the cap — otherwise the cap could be coincidentally
	// satisfied by the store still loading.
	deadline := time.After(10 * time.Second)
	for {
		all, err := svc.ListRunsWithLimit(
			context.Background(), "", 100,
		)
		if err != nil {
			t.Fatalf("ListRunsWithLimit baseline: %v", err)
		}
		if len(all) >= submitted {
			break
		}
		select {
		case <-deadline:
			t.Fatalf(
				"only %d/%d runs visible before deadline",
				len(all), submitted,
			)
		case <-time.After(20 * time.Millisecond):
		}
	}

	const want = 3
	runs, err := svc.ListRunsWithLimit(
		context.Background(), "", want,
	)
	if err != nil {
		t.Fatalf("ListRunsWithLimit: %v", err)
	}
	if len(runs) != want {
		t.Fatalf("len(runs) = %d, want %d", len(runs), want)
	}
	// Negative: results must still be newest-first (CreatedAt desc).
	for i := 1; i < len(runs); i++ {
		if runs[i].CreatedAt.After(runs[i-1].CreatedAt) {
			t.Fatalf(
				"runs not sorted desc: [%d]=%v before [%d]=%v",
				i-1, runs[i-1].CreatedAt,
				i, runs[i].CreatedAt,
			)
		}
	}
}

// TestServiceListRunsCeiling asserts that an out-of-range limit is
// clamped at MaxRunsLimitCeiling rather than rejected. Friendlier
// for operators who typo a big number.
func TestServiceListRunsCeiling(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	// Positive: clampRunsLimit applied — call should succeed with a
	// far-too-large limit (the store will be called with the
	// ceiling, which is well below any panic threshold).
	runs, err := svc.ListRunsWithLimit(
		context.Background(), "", 20000,
	)
	if err != nil {
		t.Fatalf(
			"ListRunsWithLimit(20000) returned error: %v "+
				"(want clamp, not failure)", err,
		)
	}
	if len(runs) > MaxRunsLimitCeiling {
		t.Fatalf(
			"len(runs) = %d exceeds ceiling %d",
			len(runs), MaxRunsLimitCeiling,
		)
	}

	// Negative: a zero limit should fall back to DefaultRunsLimit,
	// not panic. We don't assert the length (it depends on store
	// contents) — only that the call returns cleanly.
	if _, err := svc.ListRunsWithLimit(
		context.Background(), "", 0,
	); err != nil {
		t.Fatalf("ListRunsWithLimit(0) returned error: %v", err)
	}
}

// TestClampRunsLimit pins the clamp policy. Pure unit, no NATS.
func TestClampRunsLimit(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{in: 0, want: DefaultRunsLimit},
		{in: -1, want: DefaultRunsLimit},
		{in: 1, want: 1},
		{in: 500, want: 500},
		{in: MaxRunsLimitCeiling, want: MaxRunsLimitCeiling},
		{in: MaxRunsLimitCeiling + 1, want: MaxRunsLimitCeiling},
		{in: 20000, want: MaxRunsLimitCeiling},
	}
	for _, c := range cases {
		got := clampRunsLimit(c.in)
		if got != c.want {
			t.Errorf(
				"clampRunsLimit(%d) = %d, want %d",
				c.in, got, c.want,
			)
		}
	}
	// Negative: the policy must keep DefaultRunsLimit strictly
	// below the ceiling — if those collapse, the "raise the limit"
	// guidance is meaningless.
	if DefaultRunsLimit >= MaxRunsLimitCeiling {
		t.Fatalf(
			"DefaultRunsLimit (%d) must be < MaxRunsLimitCeiling (%d)",
			DefaultRunsLimit, MaxRunsLimitCeiling,
		)
	}
}

func TestServiceListRunEvents(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
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
	svc := NewService(nc)

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
	svc := NewService(nc)

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
	svc := NewService(nc)

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
	svc := NewService(nc)

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
	data := mustMarshal(t, def)
	_, putErr := svc.triggerKV.Put(
		context.Background(), "del-inner", data,
	)
	if putErr != nil {
		t.Fatalf("Put failed: %v", putErr)
	}

	// Positive: deleteTriggerInner removes the key.
	err = svc.deleteTriggerInner(context.Background(), "del-inner")
	if err != nil {
		t.Fatalf("deleteTriggerInner failed: %v", err)
	}
	_, err = svc.triggerKV.Get(context.Background(), "del-inner")
	if err == nil {
		t.Fatal("key should be deleted after deleteTriggerInner")
	}
}

func TestDeleteTriggerInnerNilKV(t *testing.T) {
	// Verify deleteTriggerInner returns error when KV is nil.
	svc := &Service{triggerKV: nil}
	err := svc.deleteTriggerInner(context.Background(), "any-id")
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
	svc := NewService(nc)

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

// testSpanWithIDs is a test double that wraps a noop OTel span
// with configurable trace/span IDs via a custom SpanContext.
type testSpanWithIDs struct {
	trace.Span
	sc trace.SpanContext
}

func newTestSpanWithIDs(
	traceID, spanID string,
) *testSpanWithIDs {
	tid, _ := trace.TraceIDFromHex(traceID)
	sid, _ := trace.SpanIDFromHex(spanID)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	noopTracer := tracenoop.NewTracerProvider().Tracer("")
	_, noopSpan := noopTracer.Start(context.Background(), "noop")
	return &testSpanWithIDs{Span: noopSpan, sc: sc}
}

func (s *testSpanWithIDs) SpanContext() trace.SpanContext {
	return s.sc
}

func TestInjectTraceContextOnMsg(t *testing.T) {
	// Set up W3C propagator for this test.
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	span := newTestSpanWithIDs(
		"aaaa1111bbbb2222cccc3333dddd4444",
		"eeee5555ffff6666",
	)
	ctx := trace.ContextWithRemoteSpanContext(
		context.Background(), span.SpanContext(),
	)
	msg := &nats.Msg{Header: nats.Header{}}

	// Positive: traceparent header is set via observe helper.
	observe.InjectTraceContext(ctx, msg, nil)
	tp := msg.Header.Get("traceparent")
	expected := "00-aaaa1111bbbb2222cccc3333dddd4444" +
		"-eeee5555ffff6666-01"
	if tp != expected {
		t.Fatalf("traceparent = %q, want %q", tp, expected)
	}

	// Negative: noop context does not set traceparent.
	msg2 := &nats.Msg{Header: nats.Header{}}
	observe.InjectTraceContext(
		context.Background(), msg2, nil,
	)
	if msg2.Header.Get("traceparent") != "" {
		t.Fatal("background ctx should not set traceparent")
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

func TestInjectTraceContextOnEvent(t *testing.T) {
	// Set up W3C propagator for this test.
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	span := newTestSpanWithIDs(
		"aaaa1111bbbb2222cccc3333dddd4444",
		"eeee5555ffff6666",
	)
	ctx := trace.ContextWithRemoteSpanContext(
		context.Background(), span.SpanContext(),
	)
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-1", nil,
	)
	msg := &nats.Msg{}

	// Positive: trace context is injected into event.
	observe.InjectTraceContext(ctx, msg, &evt)
	expected := "00-aaaa1111bbbb2222cccc3333dddd4444" +
		"-eeee5555ffff6666-01"
	if evt.TraceParent != expected {
		t.Fatalf(
			"TraceParent = %q, want %q",
			evt.TraceParent, expected,
		)
	}

	// Negative: background ctx leaves TraceParent empty.
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-2", nil,
	)
	msg2 := &nats.Msg{}
	observe.InjectTraceContext(
		context.Background(), msg2, &evt2,
	)
	if evt2.TraceParent != "" {
		t.Fatal("background ctx should not set TraceParent")
	}
}

func TestSendSignalNilKVError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	// Create service without signals KV bucket.
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)

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
	svc := NewService(nc)

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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	svc := NewService(nc)
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
	NewService(nil)
}

func TestInjectTraceContextNilHeader(t *testing.T) {
	// Set up W3C propagator for this test.
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	span := newTestSpanWithIDs(
		"aaaa1111bbbb2222cccc3333dddd4444",
		"eeee5555ffff6666",
	)
	ctx := trace.ContextWithRemoteSpanContext(
		context.Background(), span.SpanContext(),
	)
	// msg.Header is nil -- function should create it.
	msg := &nats.Msg{}

	observe.InjectTraceContext(ctx, msg, nil)
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
	svc := NewService(nc)

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
	svc := NewService(nc)

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
	svc := NewService(nc)

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

// TestServiceListWorkersFiltersStale proves that ListWorkers excludes
// workers whose last heartbeat is older than worker.MaxWorkerStaleness.
// Otherwise a SIGKILL'd worker keeps appearing in `dagnats workers list`
// until NATS gets around to purging the KV entry — which can be tens of
// seconds past the nominal bucket TTL. Regression for #233.
func TestServiceListWorkersFiltersStale(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	// Short window so the test runs in <1s instead of 60s.
	prev := worker.MaxWorkerStaleness
	worker.MaxWorkerStaleness = 50 * time.Millisecond
	t.Cleanup(func() { worker.MaxWorkerStaleness = prev })

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	dir := worker.NewDirectory(jsNew)
	dead := worker.WorkerRegistration{
		WorkerID:  "worker-dead",
		TaskTypes: []string{"task-a"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  1,
	}
	if err := dir.Register(dead); err != nil {
		t.Fatalf("Register dead: %v", err)
	}

	// Wait past the staleness window so the dead worker's entry is
	// older than the cutoff at the moment ListWorkers reads.
	time.Sleep(150 * time.Millisecond)

	live := worker.WorkerRegistration{
		WorkerID:  "worker-live",
		TaskTypes: []string{"task-b"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  1,
	}
	if err := dir.Register(live); err != nil {
		t.Fatalf("Register live: %v", err)
	}

	workers, err := svc.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}

	// Positive: the fresh worker survives the filter.
	// Negative: the stale worker is absent.
	if len(workers) != 1 {
		t.Fatalf("len(workers) = %d, want 1; got %+v",
			len(workers), workers)
	}
	if workers[0].WorkerID != "worker-live" {
		t.Fatalf("WorkerID = %q, want %q",
			workers[0].WorkerID, "worker-live")
	}
}

func TestServiceListWorkersNoBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc)

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

func TestStartRunIdempotency(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("idemp-test")
	wb.Task("process", "process-task")
	wb.WithIdempotencyKey("request_id")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	input := []byte(`{"request_id":"req-123","data":"hello"}`)

	// First call — new run
	runID1, err := svc.StartRun(
		context.Background(), def.Name, input,
	)
	if err != nil {
		t.Fatalf("StartRun 1: %v", err)
	}
	if runID1 == "" {
		t.Fatal("expected non-empty runID")
	}

	// Second call — same input, same idempotency key
	runID2, err := svc.StartRun(
		context.Background(), def.Name, input,
	)
	if err != nil {
		t.Fatalf("StartRun 2: %v", err)
	}

	// Positive: same run ID returned
	if runID2 != runID1 {
		t.Fatalf("expected same runID, got %q and %q",
			runID1, runID2)
	}

	// Different input — new run
	input2 := []byte(`{"request_id":"req-456","data":"world"}`)
	runID3, err := svc.StartRun(
		context.Background(), def.Name, input2,
	)
	if err != nil {
		t.Fatalf("StartRun 3: %v", err)
	}

	// Negative: different key produces different run
	if runID3 == runID1 {
		t.Fatal("different key should produce different run")
	}
}

func TestStartRunIdempotencyMissingKey(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	wb := dag.NewWorkflow("idemp-missing")
	wb.Task("process", "process-task")
	wb.WithIdempotencyKey("nonexistent_field")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), def)

	// Positive: missing key doesn't prevent run creation
	runID, err := svc.StartRun(
		context.Background(), def.Name,
		[]byte(`{"other":"value"}`),
	)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty runID despite missing key")
	}
}

func TestGetRunResponseTraceID(t *testing.T) {
	// Set up W3C propagator so StartRun injects traceparent.
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
		),
	)
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("trace-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Create a span context with a known trace ID.
	wantTraceID := "aaaa1111bbbb2222cccc3333dddd4444"
	span := newTestSpanWithIDs(
		wantTraceID, "eeee5555ffff6666",
	)
	ctx := trace.ContextWithRemoteSpanContext(
		context.Background(), span.SpanContext(),
	)
	runID, err := svc.StartRun(ctx, "trace-wf", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	// Wait for the orchestrator to create the snapshot.
	deadline := time.After(5 * time.Second)
	var resp RunResponse
	for {
		resp, err = svc.GetRunResponse(
			context.Background(), runID,
		)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("snapshot not ready in 5s: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Positive: trace_id matches the injected trace ID.
	if resp.TraceID != wantTraceID {
		t.Fatalf(
			"TraceID = %q, want %q",
			resp.TraceID, wantTraceID,
		)
	}

	// Negative: run fields are still correct.
	if resp.RunID != runID {
		t.Fatalf(
			"RunID = %q, want %q", resp.RunID, runID,
		)
	}
}

func TestGetRunResponseNoTrace(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc)
	wb := dag.NewWorkflow("notrace-wf")
	wb.Task("a", "task-a")
	wfDef, _ := wb.Build()
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Start run without any trace context.
	runID, err := svc.StartRun(
		context.Background(), "notrace-wf", nil,
	)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	deadline := time.After(5 * time.Second)
	var resp RunResponse
	for {
		resp, err = svc.GetRunResponse(
			context.Background(), runID,
		)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("snapshot not ready in 5s: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Positive: trace_id is empty when no context injected.
	if resp.TraceID != "" {
		t.Fatalf(
			"TraceID = %q, want empty", resp.TraceID,
		)
	}

	// Negative: run fields are still populated.
	if resp.RunID != runID {
		t.Fatalf(
			"RunID = %q, want %q", resp.RunID, runID,
		)
	}
}

func TestParseTraceID(t *testing.T) {
	// Positive: valid traceparent extracts trace ID.
	got := parseTraceID(
		"00-aaaa1111bbbb2222cccc3333dddd4444" +
			"-eeee5555ffff6666-01",
	)
	if got != "aaaa1111bbbb2222cccc3333dddd4444" {
		t.Fatalf("got = %q, want trace ID", got)
	}

	// Negative: empty input returns empty string.
	if parseTraceID("") != "" {
		t.Fatal("empty input should return empty")
	}

	// Negative: malformed input returns empty string.
	if parseTraceID("not-a-traceparent") != "" {
		t.Fatal("malformed input should return empty")
	}
}
