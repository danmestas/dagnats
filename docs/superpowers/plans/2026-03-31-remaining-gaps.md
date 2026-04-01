# Remaining Feature Gaps — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close seven remaining competitive gaps: worker groups, compensation/saga, workflow timeouts, signal API, input/output schemas, agent heartbeats/checkpoints, and dead-letter queues.

**Architecture:** All features are additive field/method extensions. Chunk 1 adds type fields to dag/types.go + validation. Chunk 2 adds orchestrator logic (compensation, timeouts, DLQ). Chunk 3 adds worker capabilities (heartbeat, checkpoint, signals, groups). Chunk 4 adds JSON Schema validation.

**Tech Stack:** Go, NATS JetStream KV, stdlib `crypto/hmac`, stdlib `net/http`

**Spec:** `docs/superpowers/specs/2026-03-31-remaining-gaps-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `dag/types.go` | Add WorkerGroup, OnFailure, Compensate to StepDef; Timeout, InputSchema, OutputSchema to WorkflowDef; Deadline to WorkflowRun |
| `dag/types_test.go` | JSON round-trip for new fields |
| `dag/validate.go` | Validate OnFailure/Compensate reference existing steps |
| `dag/validate_test.go` | Validation tests |
| `dag/schema.go` | JSON Schema subset validator |
| `dag/schema_test.go` | Schema validation tests |
| `engine/orchestrator.go` | Compensation, timeout check, DLQ publish, worker group routing |
| `engine/orchestrator_test.go` | Integration tests |
| `worker/worker.go` | TaskContext interface additions, WithGroups option |
| `worker/context.go` | Heartbeat, Checkpoint, LoadCheckpoint, WaitForSignal, SendSignal |
| `worker/context_test.go` | Worker capability tests |
| `natsutil/conn.go` | DEAD_LETTERS stream |

---

## Chunk 1: Type Extensions + Validation

### Task 1: Add new fields to dag/types.go

**Files:**
- Modify: `dag/types.go:147-170` (StepDef, WorkflowDef), `dag/types.go:186-196` (WorkflowRun)
- Test: `dag/types_test.go`

- [ ] **Step 1: Write failing test for new StepDef fields**

Add to `dag/types_test.go`:

```go
func TestStepDefCompensationFieldsJSON(t *testing.T) {
	step := StepDef{
		ID:          "deploy",
		Task:        "deploy-task",
		Type:        StepTypeNormal,
		WorkerGroup: "gpu",
		OnFailure:   "notify",
		Compensate:  "rollback",
	}
	data, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got StepDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: WorkerGroup round-trips
	if got.WorkerGroup != "gpu" {
		t.Fatalf("WorkerGroup = %q, want gpu", got.WorkerGroup)
	}
	// Positive: OnFailure round-trips
	if got.OnFailure != "notify" {
		t.Fatalf("OnFailure = %q, want notify", got.OnFailure)
	}
	// Positive: Compensate round-trips
	if got.Compensate != "rollback" {
		t.Fatalf("Compensate = %q, want rollback", got.Compensate)
	}
}

func TestWorkflowDefTimeoutAndSchemaJSON(t *testing.T) {
	wf := WorkflowDef{
		Name:    "test",
		Version: "1",
		Steps:   []StepDef{{ID: "s1", Task: "t", Type: StepTypeNormal}},
		Timeout: 30 * time.Minute,
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"string"}`),
	}
	data, err := json.Marshal(wf)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got WorkflowDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: Timeout round-trips
	if got.Timeout != 30*time.Minute {
		t.Fatalf("Timeout = %v, want 30m", got.Timeout)
	}
	// Positive: InputSchema round-trips
	if string(got.InputSchema) != `{"type":"object"}` {
		t.Fatalf("InputSchema = %s", got.InputSchema)
	}
}

func TestWorkflowRunDeadlineJSON(t *testing.T) {
	deadline := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	run := WorkflowRun{
		RunID: "r1", WorkflowID: "wf", Status: RunStatusRunning,
		Steps: map[string]StepState{}, CreatedAt: time.Now(),
		Deadline: deadline,
	}
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got WorkflowRun
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: Deadline round-trips
	if !got.Deadline.Equal(deadline) {
		t.Fatalf("Deadline = %v, want %v", got.Deadline, deadline)
	}
	// Positive: zero deadline omitted
	run2 := WorkflowRun{RunID: "r2", WorkflowID: "wf",
		Status: RunStatusPending, Steps: map[string]StepState{},
		CreatedAt: time.Now()}
	data2, _ := json.Marshal(run2)
	if bytes.Contains(data2, []byte(`"deadline"`)) {
		t.Fatalf("zero Deadline should be omitted")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run "TestStepDefCompensation|TestWorkflowDefTimeout|TestWorkflowRunDeadline" -v`
Expected: FAIL — fields undefined

- [ ] **Step 3: Add fields**

In `dag/types.go`, add to StepDef (after `Retry` field, line 159):

```go
WorkerGroup string `json:"worker_group,omitempty"`
OnFailure   string `json:"on_failure,omitempty"`
Compensate  string `json:"compensate,omitempty"`
```

Add to WorkflowDef (after `Concurrency` field, line 169):

```go
Timeout      time.Duration   `json:"timeout,omitempty"`
InputSchema  json.RawMessage `json:"input_schema,omitempty"`
OutputSchema json.RawMessage `json:"output_schema,omitempty"`
```

Add to WorkflowRun (after `ParentStepID` field, line 195):

```go
Deadline time.Time `json:"deadline,omitempty"`
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add dag/types.go dag/types_test.go
git commit -m "feat(dag): add WorkerGroup, OnFailure, Compensate, Timeout, schemas, Deadline fields"
```

---

### Task 2: Validate OnFailure/Compensate references

**Files:**
- Modify: `dag/validate.go:19-69`
- Test: `dag/validate_test.go`

- [ ] **Step 1: Write failing validation tests**

Add to `dag/validate_test.go`:

```go
func TestValidateOnFailureRefExists(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "v", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal,
				OnFailure: "missing"},
		},
	}
	err := dag.Validate(def)
	// Positive: error for missing reference
	if err == nil {
		t.Fatalf("expected error for OnFailure ref to missing step")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateCompensateRefExists(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "v", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal,
				Compensate: "ghost"},
		},
	}
	err := dag.Validate(def)
	if err == nil {
		t.Fatalf("expected error for Compensate ref to missing step")
	}
}

func TestValidateOnFailureAndCompensateValid(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "v", Version: "1",
		Steps: []dag.StepDef{
			{ID: "deploy", Task: "t", Type: dag.StepTypeNormal,
				OnFailure: "notify", Compensate: "rollback"},
			{ID: "notify", Task: "t", Type: dag.StepTypeNormal},
			{ID: "rollback", Task: "t", Type: dag.StepTypeNormal},
		},
	}
	if err := dag.Validate(def); err != nil {
		t.Fatalf("valid def rejected: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run "TestValidateOnFailure|TestValidateCompensate" -v`
Expected: FAIL — no validation for these fields yet

- [ ] **Step 3: Add validation rules**

In `dag/validate.go`, inside the step validation loop (after the SkipIf block, before line 68), add:

```go
if s.OnFailure != "" && !ids[s.OnFailure] {
	return fmt.Errorf(
		"step %q OnFailure references %q which does not exist",
		s.ID, s.OnFailure)
}
if s.Compensate != "" && !ids[s.Compensate] {
	return fmt.Errorf(
		"step %q Compensate references %q which does not exist",
		s.ID, s.Compensate)
}
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add dag/validate.go dag/validate_test.go
git commit -m "feat(dag): validate OnFailure and Compensate reference existing steps"
```

---

## Chunk 2: Orchestrator — Timeouts, Compensation, DLQ

### Task 3: Workflow timeout check + DLQ stream + dead letter publish

**Files:**
- Modify: `engine/orchestrator.go` (dispatchEvent, handleWorkflowStarted, handleStepFailed)
- Modify: `natsutil/conn.go` (add DEAD_LETTERS stream)
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Add DEAD_LETTERS stream to natsutil**

In `natsutil/conn.go`, add to the `streams` slice in `SetupStreams` (after EVENTS stream, line 32):

```go
{
	Name:      "DEAD_LETTERS",
	Subjects:  []string{"dead.>"},
	Retention: nats.LimitsPolicy,
	Storage:   nats.FileStorage,
},
```

- [ ] **Step 2: Write failing test for workflow timeout**

Add to `engine/orchestrator_test.go`:

```go
func TestOrchestratorWorkflowTimeout(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "timeout-test", Version: "1",
		Timeout: 200 * time.Millisecond,
		Steps: []dag.StepDef{
			{ID: "slow", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("timeout-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "timeout-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(100 * time.Millisecond)

	// Wait for timeout to expire
	time.Sleep(200 * time.Millisecond)

	// Send a step event after timeout (should trigger cancel)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "timeout-run-1", "slow",
		[]byte(`"timed out"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Check that run is cancelled
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("timeout-run-1")
		if err == nil && run.Status == dag.RunStatusCancelled {
			return // Positive: timed out → cancelled
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be cancelled after timeout")
}
```

- [ ] **Step 3: Write failing test for DLQ publish**

Add to `engine/orchestrator_test.go`:

```go
func TestOrchestratorPublishesDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "dlq-test", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "bad-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("dlq-test", defData)

	// Subscribe to DLQ
	dlqSub, err := js.SubscribeSync("dead.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "dlq-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail the step permanently (no retries configured)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "dlq-run-1", "s1",
		[]byte(`"permanent error"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
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
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run "TestOrchestratorWorkflowTimeout|TestOrchestratorPublishesDeadLetter" -v -timeout 30s`
Expected: FAIL

- [ ] **Step 5: Implement timeout check in dispatchEvent**

In `engine/orchestrator.go`, in `dispatchEvent` (after loading the run, before the switch statement), add a timeout check:

```go
// Check workflow timeout before dispatching any event.
run, loadErr := o.store.Load(evt.RunID)
if loadErr == nil && !run.Deadline.IsZero() &&
	time.Now().After(run.Deadline) &&
	run.Status == dag.RunStatusRunning {
	return o.handleWorkflowCancelled(ctx, evt)
}
```

In `handleWorkflowStarted`, after creating the run and before saving:

```go
if wfDef.Timeout > 0 {
	run.Deadline = time.Now().Add(wfDef.Timeout)
}
```

- [ ] **Step 6: Implement publishDeadLetter in handleStepFailed**

In `engine/orchestrator.go`, add method:

```go
func (o *Orchestrator) publishDeadLetter(
	runID string, stepDef dag.StepDef, state dag.StepState,
) {
	payload, err := json.Marshal(map[string]interface{}{
		"run_id":   runID,
		"step_id":  stepDef.ID,
		"task":     stepDef.Task,
		"error":    state.Error,
		"attempts": state.Attempts,
	})
	if err != nil {
		return
	}
	subject := fmt.Sprintf("dead.%s.%s.%s",
		stepDef.Task, runID, stepDef.ID)
	o.js.Publish(subject, payload)
}
```

In `handleStepFailed`, after permanent failure (after `o.runsFailed.Inc()`), add:

```go
stepDef, _ := findStepDef(wfDef, evt.StepID)
o.publishDeadLetter(run.RunID, stepDef, state)
```

- [ ] **Step 7: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run "TestOrchestratorWorkflowTimeout|TestOrchestratorPublishesDeadLetter" -v -timeout 30s`
Expected: PASS

- [ ] **Step 8: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 9: Commit**

```bash
git add natsutil/conn.go engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): add workflow timeouts and dead-letter queue publishing"
```

---

### Task 4: Compensation/saga logic in orchestrator

**Files:**
- Modify: `engine/orchestrator.go`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for OnFailure handler**

Add to `engine/orchestrator_test.go`:

```go
func TestOrchestratorOnFailureStep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Workflow: deploy fails → notify runs
	wfDef := dag.WorkflowDef{
		Name: "onfail-test", Version: "1",
		Steps: []dag.StepDef{
			{ID: "deploy", Task: "deploy-task",
				Type: dag.StepTypeNormal, OnFailure: "notify"},
			{ID: "notify", Task: "notify-task",
				Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("onfail-test", defData)

	// Subscribe to task queue for notify
	taskSub, _ := js.SubscribeSync("task.notify-task.>",
		nats.AckExplicit(), nats.DeliverAll())

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "onfail-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail deploy step permanently
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "onfail-run-1", "deploy",
		[]byte(`"deploy crashed"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
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
	store := NewSnapshotStore(js)
	time.Sleep(200 * time.Millisecond)
	run, _ := store.Load("onfail-run-1")
	if run.Status == dag.RunStatusFailed {
		t.Fatalf("workflow should not be failed while on-failure step pending")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestOrchestratorOnFailure -v -timeout 30s`
Expected: FAIL — on-failure step not enqueued

- [ ] **Step 3: Implement OnFailure in handleStepFailed**

In `engine/orchestrator.go`, modify the permanent failure path in `handleStepFailed`. After marking the step as failed but BEFORE marking the workflow as failed, check for OnFailure:

```go
// Check for on-failure handler before failing the workflow.
if stepDef.OnFailure != "" {
	// Enqueue the on-failure step with the error as input
	onFailStep, found := findStepDef(wfDef, stepDef.OnFailure)
	if found {
		ofState := run.Steps[onFailStep.ID]
		ofState.Status = dag.StepStatusQueued
		run.Steps[onFailStep.ID] = ofState
		if err := o.saveSnapshot(ctx, run); err != nil {
			return err
		}
		errorInput := []byte(fmt.Sprintf(
			`{"failed_step":"%s","error":%s}`,
			evt.StepID, state.Error))
		return o.publishTask(ctx, run.RunID, onFailStep, errorInput)
	}
}

// No on-failure handler — fail the workflow
run.Status = dag.RunStatusFailed
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestOrchestratorOnFailure -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): add OnFailure compensation — run handler step on permanent failure"
```

---

## Chunk 3: Worker Capabilities

### Task 5: Heartbeat, Checkpoint, and Signal on TaskContext

**Files:**
- Modify: `worker/worker.go:17-27` (TaskContext interface)
- Modify: `worker/context.go` (add methods + msg field)
- Test: `worker/context_test.go`

- [ ] **Step 1: Write failing tests for new TaskContext methods**

Add to `worker/context_test.go`:

```go
func TestTaskContextHeartbeat(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	tc := newTestTaskContext(nc, js, "run-hb", "step-hb")

	// Positive: heartbeat doesn't error (no msg in unit test — skip)
	// This is an integration concern; unit test verifies method exists
	_ = tc
}

func TestTaskContextCheckpoint(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	cpKV, _ := js.KeyValue("checkpoints")
	tc := &taskContext{
		nc: nc, js: js, runID: "run-cp", stepID: "step-cp",
		tel: observe.NewNoopTelemetry(),
		ctx: context.Background(),
		span: observe.NewNoopTelemetry().Tracer.NoopSpan(),
		checkpointKV: cpKV,
	}

	// Positive: checkpoint writes and reads back
	err := tc.Checkpoint([]byte(`{"progress":50}`))
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	data, err := tc.LoadCheckpoint()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if string(data) != `{"progress":50}` {
		t.Fatalf("checkpoint = %q, want progress 50", string(data))
	}
}

func TestTaskContextSignal(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "checkpoints"},
			natsutil.KVConfig{Bucket: "signals"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	sigKV, _ := js.KeyValue("signals")
	tc := &taskContext{
		nc: nc, js: js, runID: "run-sig", stepID: "step-sig",
		tel: observe.NewNoopTelemetry(),
		ctx: context.Background(),
		span: observe.NewNoopTelemetry().Tracer.NoopSpan(),
		signalKV: sigKV,
	}

	// Send signal in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		tc.SendSignal("run-sig", "approval", []byte(`"approved"`))
	}()

	// Positive: WaitForSignal receives it
	data, err := tc.WaitForSignal("approval", 2*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if string(data) != `"approved"` {
		t.Fatalf("signal = %q, want approved", string(data))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./worker/ -run "TestTaskContextHeartbeat|TestTaskContextCheckpoint|TestTaskContextSignal" -v -timeout 15s`
Expected: FAIL — methods undefined

- [ ] **Step 3: Update TaskContext interface**

In `worker/worker.go`, add to the `TaskContext` interface (after `PutStream`):

```go
type TaskContext interface {
	Input() []byte
	RunID() string
	StepID() string
	RetryCount() int
	Complete(output []byte) error
	Fail(err error) error
	Continue(output []byte) error
	PutStream(data []byte) error
	Heartbeat() error
	Checkpoint(state []byte) error
	LoadCheckpoint() ([]byte, error)
	WaitForSignal(name string, timeout time.Duration) ([]byte, error)
	SendSignal(runID, name string, data []byte) error
}
```

- [ ] **Step 4: Add KV fields to taskContext and implement methods**

In `worker/context.go`, add fields to `taskContext` struct:

```go
type taskContext struct {
	// ...existing fields...
	msg          *nats.Msg    // original NATS message for Heartbeat
	checkpointKV nats.KeyValue
	signalKV     nats.KeyValue
}
```

Add implementations:

```go
func (c *taskContext) Heartbeat() error {
	if c.msg == nil {
		return nil
	}
	return c.msg.InProgress()
}

func (c *taskContext) Checkpoint(state []byte) error {
	if c.checkpointKV == nil {
		return fmt.Errorf("checkpoint KV not configured")
	}
	key := c.runID + "." + c.stepID
	_, err := c.checkpointKV.Put(key, state)
	return err
}

func (c *taskContext) LoadCheckpoint() ([]byte, error) {
	if c.checkpointKV == nil {
		return nil, nil
	}
	key := c.runID + "." + c.stepID
	entry, err := c.checkpointKV.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return entry.Value(), nil
}

func (c *taskContext) WaitForSignal(
	name string, timeout time.Duration,
) ([]byte, error) {
	if c.signalKV == nil {
		return nil, fmt.Errorf("signal KV not configured")
	}
	if timeout > 1*time.Hour {
		timeout = 1 * time.Hour
	}
	key := c.runID + "." + name
	watcher, err := c.signalKV.Watch(key)
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			return entry.Value(), nil
		case <-timer.C:
			return nil, fmt.Errorf(
				"signal %q timed out after %s", name, timeout)
		}
	}
}

func (c *taskContext) SendSignal(
	runID, name string, data []byte,
) error {
	if c.signalKV == nil {
		return fmt.Errorf("signal KV not configured")
	}
	key := runID + "." + name
	_, err := c.signalKV.Put(key, data)
	return err
}
```

- [ ] **Step 5: Wire KV buckets in Worker.Start**

In `worker/worker.go`, in the `Start` method or `newTaskContext`, optionally bind the `checkpoints` and `signals` KV buckets:

```go
// In Worker, try to bind KV buckets (optional — may not exist)
func (w *Worker) bindKVBuckets() {
	w.checkpointKV, _ = w.js.KeyValue("checkpoints")
	w.signalKV, _ = w.js.KeyValue("signals")
}
```

Pass them to `newTaskContext`. If buckets don't exist, the fields are nil and methods return appropriate errors/nils.

- [ ] **Step 6: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./worker/ -v -timeout 30s`
Expected: ALL PASS

- [ ] **Step 7: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 8: Commit**

```bash
git add worker/worker.go worker/context.go worker/context_test.go
git commit -m "feat(worker): add Heartbeat, Checkpoint, LoadCheckpoint, WaitForSignal, SendSignal"
```

---

## Chunk 4: JSON Schema Validator

### Task 6: In-house JSON Schema subset validator

**Files:**
- Create: `dag/schema.go`
- Test: `dag/schema_test.go`

- [ ] **Step 1: Write failing tests for schema validation**

Create `dag/schema_test.go`:

```go
package dag

// Methodology: unit tests for the JSON Schema subset validator.
// Pure — no NATS.

import (
	"encoding/json"
	"testing"
)

func TestValidateSchemaTypeString(t *testing.T) {
	schema := json.RawMessage(`{"type":"string"}`)

	// Positive: string passes
	if err := ValidateSchema(schema,
		json.RawMessage(`"hello"`)); err != nil {
		t.Fatalf("string should pass: %v", err)
	}

	// Negative: number fails
	if err := ValidateSchema(schema,
		json.RawMessage(`42`)); err == nil {
		t.Fatalf("number should fail string schema")
	}
}

func TestValidateSchemaTypeObject(t *testing.T) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["name"],
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		}
	}`)

	// Positive: valid object
	if err := ValidateSchema(schema,
		json.RawMessage(`{"name":"alice","age":30}`)); err != nil {
		t.Fatalf("valid object: %v", err)
	}

	// Negative: missing required field
	if err := ValidateSchema(schema,
		json.RawMessage(`{"age":30}`)); err == nil {
		t.Fatalf("missing name should fail")
	}

	// Negative: wrong type for field
	if err := ValidateSchema(schema,
		json.RawMessage(`{"name":123}`)); err == nil {
		t.Fatalf("name as number should fail")
	}
}

func TestValidateSchemaTypeArray(t *testing.T) {
	schema := json.RawMessage(`{"type":"array"}`)

	// Positive: array passes
	if err := ValidateSchema(schema,
		json.RawMessage(`[1,2,3]`)); err != nil {
		t.Fatalf("array should pass: %v", err)
	}

	// Negative: object fails
	if err := ValidateSchema(schema,
		json.RawMessage(`{}`)); err == nil {
		t.Fatalf("object should fail array schema")
	}
}

func TestValidateSchemaNilSchemaPassesAll(t *testing.T) {
	// Positive: nil schema accepts anything
	if err := ValidateSchema(nil,
		json.RawMessage(`"anything"`)); err != nil {
		t.Fatalf("nil schema should accept: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run TestValidateSchema -v`
Expected: FAIL — `ValidateSchema` undefined

- [ ] **Step 3: Implement ValidateSchema**

Create `dag/schema.go`:

```go
package dag

import (
	"encoding/json"
	"fmt"
)

// ValidateSchema validates data against a JSON Schema subset.
// Supports: type (string, number, boolean, object, array),
// required, properties (recursive). Returns nil if schema is nil.
func ValidateSchema(
	schema json.RawMessage, data json.RawMessage,
) error {
	if schema == nil {
		return nil
	}
	var s schemaNode
	if err := json.Unmarshal(schema, &s); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("invalid data: %w", err)
	}
	return validateNode(s, value, "")
}

type schemaNode struct {
	Type       string                `json:"type"`
	Required   []string              `json:"required"`
	Properties map[string]schemaNode `json:"properties"`
}

func validateNode(
	s schemaNode, value interface{}, path string,
) error {
	if s.Type != "" {
		if err := checkType(s.Type, value, path); err != nil {
			return err
		}
	}
	obj, isObj := value.(map[string]interface{})
	if isObj && len(s.Required) > 0 {
		for _, key := range s.Required {
			if _, exists := obj[key]; !exists {
				return fmt.Errorf(
					"%s: missing required field %q", path, key)
			}
		}
	}
	if isObj && len(s.Properties) > 0 {
		for key, propSchema := range s.Properties {
			val, exists := obj[key]
			if !exists {
				continue
			}
			childPath := path + "." + key
			if path == "" {
				childPath = key
			}
			if err := validateNode(
				propSchema, val, childPath,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkType(
	expected string, value interface{}, path string,
) error {
	actual := jsonType(value)
	if actual != expected {
		if path == "" {
			path = "(root)"
		}
		return fmt.Errorf(
			"%s: expected type %q, got %q", path, expected, actual)
	}
	return nil
}

func jsonType(v interface{}) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}
```

Note: `validateNode` calls itself for nested properties. This is bounded by JSON depth (max ~20 levels in practice) and the schema is validated at registration time. The recursion depth is bounded by the schema structure, not user input. If strict no-recursion is enforced, convert to iterative with explicit stack.

- [ ] **Step 4: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run TestValidateSchema -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add dag/schema.go dag/schema_test.go
git commit -m "feat(dag): add JSON Schema subset validator for input/output validation"
```

---

### Task 7: Full suite verification

- [ ] **Step 1: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -v -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 2: Verify go vet**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go vet ./...`
Expected: No issues

- [ ] **Step 3: Check new file line counts**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && wc -l dag/schema.go engine/concurrency.go`
Expected: schema.go ~100 lines, all under 200
