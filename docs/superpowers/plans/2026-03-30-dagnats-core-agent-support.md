# DagNats Core: Agent & Child Workflow Support — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add generic extension points to DagNats core — configurable step routing, child workflow lifecycle, metadata on steps, and extensible NATS setup — so downstream packages can implement agent workloads without forking.

**Architecture:** Functional options on `NewOrchestrator` and `SetupAll` for backwards-compatible extension. Three new protocol event types for child workflow lifecycle. `Metadata map[string]string` on `StepDef` as a generic extension point. All changes are additive — existing tests must continue passing unchanged.

**Tech Stack:** Go, NATS JetStream, existing DagNats packages (`dag`, `engine`, `protocol`, `natsutil`)

**Spec:** `docs/superpowers/specs/2026-03-30-agent-sdk-integration-design.md`

---

## Chunk 1: dag package changes

### Task 1: Add StepTypeAgent and Metadata to dag/types.go

**Files:**
- Modify: `dag/types.go:11-26` (StepType enum), `dag/types.go:139-148` (StepDef), `dag/types.go:174-180` (WorkflowRun)
- Test: `dag/types_test.go`

- [ ] **Step 1: Write failing test for StepTypeAgent**

In `dag/types_test.go`, add:

```go
func TestStepTypeAgentStringAndJSON(t *testing.T) {
	// Positive: string representation
	if got := StepTypeAgent.String(); got != "agent" {
		t.Fatalf("StepTypeAgent.String() = %q, want %q", got, "agent")
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(StepTypeAgent)
	if err != nil {
		t.Fatalf("Marshal StepTypeAgent: %v", err)
	}
	if string(data) != `"agent"` {
		t.Fatalf("Marshal StepTypeAgent = %s, want %q", data, "agent")
	}

	var got StepType
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal StepTypeAgent: %v", err)
	}
	if got != StepTypeAgent {
		t.Fatalf("Unmarshal StepTypeAgent = %v, want %v", got, StepTypeAgent)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestStepTypeAgentStringAndJSON -v`
Expected: FAIL — `StepTypeAgent` undefined

- [ ] **Step 3: Add StepTypeAgent constant**

In `dag/types.go`, add `StepTypeAgent` to the iota block after `StepTypeSubWorkflow` (line 16):

```go
StepTypeSubWorkflow
StepTypeAgent
```

Extend the `stepTypeStrings` array (line 19) to include the new value:

```go
var stepTypeStrings = [...]string{"normal", "agent_loop", "sub_workflow", "agent"}
```

The existing `String()`, `MarshalJSON()`, and `UnmarshalJSON()` methods use array index lookup, so no changes are needed to those methods — `StepTypeAgent` will automatically map to `"agent"` via iota value 3.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestStepTypeAgentStringAndJSON -v`
Expected: PASS

- [ ] **Step 5: Write failing test for Metadata field on StepDef**

In `dag/types_test.go`, add:

```go
func TestStepDefMetadataJSON(t *testing.T) {
	step := StepDef{
		ID:       "code",
		Task:     "llm-coder",
		Type:     StepTypeAgent,
		Metadata: map[string]string{"role": "coder"},
	}

	data, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("Marshal StepDef with metadata: %v", err)
	}

	var got StepDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal StepDef with metadata: %v", err)
	}
	if got.Metadata["role"] != "coder" {
		t.Fatalf("Metadata[role] = %q, want %q", got.Metadata["role"], "coder")
	}

	// Negative: nil metadata omitted from JSON
	step2 := StepDef{ID: "plain", Task: "task", Type: StepTypeNormal}
	data2, _ := json.Marshal(step2)
	if bytes.Contains(data2, []byte("metadata")) {
		t.Fatalf("nil Metadata should be omitted from JSON, got %s", data2)
	}
}
```

Add `"bytes"` to imports if not present.

- [ ] **Step 6: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestStepDefMetadataJSON -v`
Expected: FAIL — `Metadata` field undefined

- [ ] **Step 7: Add Metadata field to StepDef**

In `dag/types.go`, add to `StepDef` struct after `SkipIf` (line 147):

```go
Metadata map[string]string `json:"metadata,omitempty"`
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestStepDefMetadataJSON -v`
Expected: PASS

- [ ] **Step 9: Write failing test for ParentRunID/ParentStepID on WorkflowRun**

In `dag/types_test.go`, add:

```go
func TestWorkflowRunParentFieldsJSON(t *testing.T) {
	run := WorkflowRun{
		RunID:        "child-1",
		WorkflowID:   "wf-1",
		Status:       RunStatusRunning,
		Steps:        map[string]StepState{},
		CreatedAt:    time.Now(),
		ParentRunID:  "parent-1",
		ParentStepID: "step-a",
	}

	data, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("Marshal WorkflowRun with parent: %v", err)
	}

	var got WorkflowRun
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal WorkflowRun with parent: %v", err)
	}
	if got.ParentRunID != "parent-1" {
		t.Fatalf("ParentRunID = %q, want %q", got.ParentRunID, "parent-1")
	}
	if got.ParentStepID != "step-a" {
		t.Fatalf("ParentStepID = %q, want %q", got.ParentStepID, "step-a")
	}

	// Negative: empty parent fields omitted
	run2 := WorkflowRun{RunID: "top", WorkflowID: "wf", Status: RunStatusPending,
		Steps: map[string]StepState{}, CreatedAt: time.Now()}
	data2, _ := json.Marshal(run2)
	if bytes.Contains(data2, []byte("parent_run_id")) {
		t.Fatalf("empty ParentRunID should be omitted, got %s", data2)
	}
}
```

- [ ] **Step 10: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestWorkflowRunParentFieldsJSON -v`
Expected: FAIL — `ParentRunID` undefined

- [ ] **Step 11: Add parent fields to WorkflowRun**

In `dag/types.go`, add to `WorkflowRun` struct after `CreatedAt` (line 180):

```go
ParentRunID  string `json:"parent_run_id,omitempty"`
ParentStepID string `json:"parent_step_id,omitempty"`
```

- [ ] **Step 12: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestWorkflowRunParentFieldsJSON -v`
Expected: PASS

- [ ] **Step 13: Run all dag tests to verify nothing is broken**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -v`
Expected: ALL PASS

- [ ] **Step 14: Commit**

```bash
git add dag/types.go dag/types_test.go
git commit -m "feat(dag): add StepTypeAgent, Metadata on StepDef, parent fields on WorkflowRun"
```

---

### Task 2: Add StepTypeAgent validation to dag/validate.go

**Files:**
- Modify: `dag/validate.go`
- Test: `dag/validate_test.go`

- [ ] **Step 1: Write failing test — agent step must not have AgentLoopConfig**

In `dag/validate_test.go`, add:

```go
func TestValidateAgentStepRejectsLoopConfig(t *testing.T) {
	// Methodology: validate that StepTypeAgent steps with AgentLoopConfig
	// are rejected. The Agent SDK manages its own loop.

	def := dag.WorkflowDef{
		Name:    "bad-agent",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "step1",
				Task: "llm-task",
				Type: dag.StepTypeAgent,
				Loop: &dag.AgentLoopConfig{MaxIterations: 5},
			},
		},
	}
	err := dag.Validate(def)
	if err == nil {
		t.Fatalf("expected error for agent step with loop config, got nil")
	}
	if !strings.Contains(err.Error(), "agent") {
		t.Fatalf("error should mention agent, got: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestValidateAgentStepRejectsLoopConfig -v`
Expected: FAIL — validation passes when it should reject

- [ ] **Step 3: Add validation rule**

In `dag/validate.go`, inside the step validation loop (after the AgentLoop config checks, around line 36), add:

```go
if step.Type == StepTypeAgent && step.Loop != nil {
	return fmt.Errorf("step %q: agent steps must not have AgentLoopConfig (Agent SDK manages its own loop)", step.ID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestValidateAgentStepRejectsLoopConfig -v`
Expected: PASS

- [ ] **Step 5: Write test — agent step without loop config passes validation**

In `dag/validate_test.go`, add:

```go
func TestValidateAgentStepValid(t *testing.T) {
	def := dag.WorkflowDef{
		Name:    "good-agent",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:       "step1",
				Task:     "llm-task",
				Type:     dag.StepTypeAgent,
				Metadata: map[string]string{"role": "coder"},
			},
		},
	}
	if err := dag.Validate(def); err != nil {
		t.Fatalf("valid agent step should pass, got: %v", err)
	}
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestValidateAgentStepValid -v`
Expected: PASS

- [ ] **Step 7: Run all dag tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -v`
Expected: ALL PASS

- [ ] **Step 8: Commit**

```bash
git add dag/validate.go dag/validate_test.go
git commit -m "feat(dag): validate agent steps reject AgentLoopConfig"
```

---

### Task 3: Add Agent() method to builder

**Files:**
- Modify: `dag/builder.go`
- Modify: `dag/stepref.go`
- Test: `dag/builder_test.go`

- [ ] **Step 1: Write failing test for builder Agent method**

In `dag/builder_test.go`, add:

```go
func TestBuilderAgentStep(t *testing.T) {
	wf := dag.NewWorkflow("agent-wf")
	plan := wf.Agent("plan", "llm-planner", map[string]string{"role": "planner"})
	_ = wf.Agent("code", "llm-coder", map[string]string{"role": "coder"}).After(plan)

	def, err := wf.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if len(def.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def.Steps))
	}

	// Positive: step type and metadata
	if def.Steps[0].Type != dag.StepTypeAgent {
		t.Fatalf("step 0 type = %v, want Agent", def.Steps[0].Type)
	}
	if def.Steps[0].Metadata["role"] != "planner" {
		t.Fatalf("step 0 metadata role = %q, want planner", def.Steps[0].Metadata["role"])
	}

	// Positive: dependency wiring
	if len(def.Steps[1].DependsOn) != 1 {
		t.Fatalf("step 1 deps = %d, want 1", len(def.Steps[1].DependsOn))
	}
	if def.Steps[1].DependsOn[0] != "plan" {
		t.Fatalf("step 1 dep = %q, want plan", def.Steps[1].DependsOn[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestBuilderAgentStep -v`
Expected: FAIL — `Agent` method undefined

- [ ] **Step 3: Add Agent method to WorkflowBuilder**

In `dag/builder.go`, after the `AgentLoop` method, add:

```go
// Agent adds an agent step to the workflow. The task parameter identifies the
// NATS subject prefix. Metadata carries role and other agent-specific config.
func (b *WorkflowBuilder) Agent(id, task string, metadata map[string]string) StepRef {
	if id == "" {
		panic("dag: step id must not be empty")
	}
	if task == "" {
		panic("dag: step task must not be empty")
	}
	b.steps = append(b.steps, StepDef{
		ID:       id,
		Task:     task,
		Type:     StepTypeAgent,
		Metadata: metadata,
	})
	b.current = len(b.steps) - 1
	return StepRef{id: id, index: b.current, builder: b}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -run TestBuilderAgentStep -v`
Expected: PASS

- [ ] **Step 5: Run all dag tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./dag/ -v`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add dag/builder.go dag/builder_test.go
git commit -m "feat(dag): add Agent() builder method for agent step type"
```

---

## Chunk 2: protocol and natsutil changes

### Task 4: Add child workflow event types to protocol

**Files:**
- Modify: `protocol/protocol.go:22-34`
- Test: `protocol/protocol_test.go`

- [ ] **Step 1: Write failing test for new event types**

In `protocol/protocol_test.go`, add:

```go
func TestChildWorkflowEventTypes(t *testing.T) {
	// Positive: constants exist and have correct string values
	if EventWorkflowSpawn != "workflow.spawn" {
		t.Fatalf("EventWorkflowSpawn = %q, want %q", EventWorkflowSpawn, "workflow.spawn")
	}
	if EventWorkflowChildCompleted != "workflow.child.completed" {
		t.Fatalf("EventWorkflowChildCompleted = %q, want workflow.child.completed", EventWorkflowChildCompleted)
	}
	if EventWorkflowChildFailed != "workflow.child.failed" {
		t.Fatalf("EventWorkflowChildFailed = %q, want workflow.child.failed", EventWorkflowChildFailed)
	}

	// Positive: can create events with new types
	evt := NewWorkflowEvent(EventWorkflowSpawn, "run-1", []byte(`{"child":"c-1"}`))
	if evt.Type != EventWorkflowSpawn {
		t.Fatalf("event type = %q, want %q", evt.Type, EventWorkflowSpawn)
	}
	if evt.RunID != "run-1" {
		t.Fatalf("run id = %q, want run-1", evt.RunID)
	}

	// Positive: NATSMsgID works for new event types
	if msgID := evt.NATSMsgID(); msgID == "" {
		t.Fatalf("NATSMsgID should not be empty")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./protocol/ -run TestChildWorkflowEventTypes -v`
Expected: FAIL — `EventWorkflowSpawn` undefined

- [ ] **Step 3: Add event type constants**

In `protocol/protocol.go`, after `EventWorkflowFailed` (line 34), add:

```go
EventWorkflowSpawn          EventType = "workflow.spawn"
EventWorkflowChildCompleted EventType = "workflow.child.completed"
EventWorkflowChildFailed    EventType = "workflow.child.failed"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./protocol/ -run TestChildWorkflowEventTypes -v`
Expected: PASS

- [ ] **Step 5: Run all protocol tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./protocol/ -v`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add protocol/protocol.go protocol/protocol_test.go
git commit -m "feat(protocol): add workflow.spawn and child completed/failed event types"
```

---

### Task 5: Make natsutil.SetupAll extensible with functional options

**Files:**
- Modify: `natsutil/conn.go:79-91`
- Test: `natsutil/conn_test.go`

- [ ] **Step 1: Write failing test for extensible SetupAll**

In `natsutil/conn_test.go`, add:

```go
func TestSetupAllWithExtras(t *testing.T) {
	// Methodology: verify SetupAll provisions extra streams and KV buckets
	// passed via functional options alongside the default resources.

	_, nc := StartTestServer(t)

	extraStream := StreamConfig{
		Name:     "AGENT_TASKS",
		Subjects: []string{"agent.task.>"},
	}
	extraKV := KVConfig{
		Bucket: "roles",
	}

	if err := SetupAll(nc, WithStreams(extraStream), WithKVBuckets(extraKV)); err != nil {
		t.Fatalf("SetupAll with extras: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// Positive: default streams exist
	if _, err := js.StreamInfo("WORKFLOW_HISTORY"); err != nil {
		t.Fatalf("WORKFLOW_HISTORY should exist: %v", err)
	}
	if _, err := js.StreamInfo("TASK_QUEUES"); err != nil {
		t.Fatalf("TASK_QUEUES should exist: %v", err)
	}

	// Positive: extra stream exists
	if _, err := js.StreamInfo("AGENT_TASKS"); err != nil {
		t.Fatalf("AGENT_TASKS should exist: %v", err)
	}

	// Positive: extra KV bucket exists
	if _, err := js.KeyValue("roles"); err != nil {
		t.Fatalf("roles KV should exist: %v", err)
	}

	// Positive: default KV buckets still exist
	if _, err := js.KeyValue("workflow_defs"); err != nil {
		t.Fatalf("workflow_defs should exist: %v", err)
	}
}

func TestSetupAllNoExtras(t *testing.T) {
	// Verify backwards compatibility — no options means same behavior.
	_, nc := StartTestServer(t)

	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll no extras: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	if _, err := js.StreamInfo("WORKFLOW_HISTORY"); err != nil {
		t.Fatalf("WORKFLOW_HISTORY should exist: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./natsutil/ -run TestSetupAll -v`
Expected: FAIL — `StreamConfig`, `KVConfig`, `WithStreams`, `WithKVBuckets` undefined, or `SetupAll` signature mismatch

- [ ] **Step 3: Implement functional options**

In `natsutil/conn.go`, before `SetupAll`, add the types and option functions:

```go
// StreamConfig defines an additional JetStream stream to provision.
type StreamConfig struct {
	Name     string
	Subjects []string
}

// KVConfig defines an additional KV bucket to provision.
type KVConfig struct {
	Bucket string
}

// SetupOption configures additional NATS resources for SetupAll.
type SetupOption func(*setupOptions)

type setupOptions struct {
	streams []StreamConfig
	kvs     []KVConfig
}

// WithStreams adds extra JetStream streams to provision.
func WithStreams(configs ...StreamConfig) SetupOption {
	return func(o *setupOptions) {
		o.streams = append(o.streams, configs...)
	}
}

// WithKVBuckets adds extra KV buckets to provision.
func WithKVBuckets(configs ...KVConfig) SetupOption {
	return func(o *setupOptions) {
		o.kvs = append(o.kvs, configs...)
	}
}
```

Then change the `SetupAll` signature and body:

```go
func SetupAll(nc *nats.Conn, opts ...SetupOption) error {
	if nc == nil {
		panic("natsutil: connection must not be nil")
	}

	var options setupOptions
	for _, opt := range opts {
		opt(&options)
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("natsutil: jetstream: %w", err)
	}

	if err := SetupStreams(js); err != nil {
		return err
	}
	if err := SetupKVBuckets(js); err != nil {
		return err
	}
	if err := SetupTelemetryStream(js); err != nil {
		return err
	}

	for _, sc := range options.streams {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      sc.Name,
			Subjects:  sc.Subjects,
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
		})
		if err != nil {
			return fmt.Errorf("natsutil: add stream %q: %w", sc.Name, err)
		}
	}

	for _, kc := range options.kvs {
		_, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: kc.Bucket,
		})
		if err != nil {
			return fmt.Errorf("natsutil: create kv %q: %w", kc.Bucket, err)
		}
	}

	return nil
}
```

- [ ] **Step 4: Fix existing callers of SetupAll**

The existing callers pass only `nc` — the variadic opts means they compile without changes. Verify by searching:

Run: `cd /Users/dmestas/projects/dagnats && grep -rn 'SetupAll(' --include='*.go' | grep -v '_test.go' | grep -v conn.go`

Update any callers that pass a second argument (there should be none).

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./natsutil/ -run TestSetupAll -v`
Expected: PASS

- [ ] **Step 6: Run ALL tests to verify nothing broke**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -count=1`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add natsutil/conn.go natsutil/conn_test.go
git commit -m "feat(natsutil): extensible SetupAll with WithStreams/WithKVBuckets options"
```

---

## Chunk 3: engine — configurable step routing

### Task 6: Add functional options to NewOrchestrator and configurable step routing

**Files:**
- Modify: `engine/orchestrator.go:25-87` (struct + constructor), `engine/orchestrator.go:553-654` (publishTask + buildTaskMsg)
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for step routing**

In `engine/orchestrator_test.go`, add:

```go
func TestOrchestratorRoutesAgentStepsToCustomStream(t *testing.T) {
	// Methodology: verify that StepTypeAgent steps are published to a custom
	// stream when WithStepRoutes is configured. Normal steps still go to TASK_QUEUES.

	_, nc := natsutil.StartTestServer(t)

	// Setup with an extra AGENT_TASKS stream
	err := natsutil.SetupAll(nc,
		natsutil.WithStreams(natsutil.StreamConfig{
			Name:     "AGENT_TASKS",
			Subjects: []string{"agent.task.>"},
		}),
	)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	routes := map[dag.StepType]string{
		dag.StepTypeAgent: "agent.task",
	}
	orch := NewOrchestrator(nc, observe.NewNoopTelemetry(),
		WithStepRoutes(routes))
	if orch == nil {
		t.Fatalf("orchestrator should not be nil")
	}

	// Register a workflow with one agent step
	wf := dag.WorkflowDef{
		Name:    "routed-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "agent-step", Task: "llm-task", Type: dag.StepTypeAgent,
				Metadata: map[string]string{"role": "coder"}},
		},
	}
	defData, _ := json.Marshal(wf)
	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")
	if _, err := defKV.Put("routed-wf", defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	// Subscribe to AGENT_TASKS stream to catch the routed message
	sub, err := js.SubscribeSync("agent.task.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe agent tasks: %v", err)
	}

	orch.Start()
	defer orch.Stop()

	// Start a workflow run
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-route-1",
		defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", startEvt.NATSMsgID())
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish start: %v", err)
	}

	// Wait for routed message on AGENT_TASKS
	agentMsg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected message on AGENT_TASKS, got: %v", err)
	}
	if !strings.HasPrefix(agentMsg.Subject, "agent.task.") {
		t.Fatalf("subject = %q, want prefix agent.task.", agentMsg.Subject)
	}
	agentMsg.Ack()
}
```

Note: add `"strings"` to the test file imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorRoutesAgentStepsToCustomStream -v -timeout 30s`
Expected: FAIL — `WithStepRoutes` undefined

- [ ] **Step 3: Add functional options to Orchestrator**

In `engine/orchestrator.go`, add the option types before `NewOrchestrator`:

```go
// OrchestratorOption configures optional orchestrator behavior.
type OrchestratorOption func(*Orchestrator)

// WithStepRoutes configures custom stream routing for step types.
// Steps with types not in the map are routed to TASK_QUEUES (default).
func WithStepRoutes(routes map[dag.StepType]string) OrchestratorOption {
	return func(o *Orchestrator) {
		o.stepRoutes = routes
	}
}
```

Add `stepRoutes` field to the `Orchestrator` struct (after `runLocks`):

```go
stepRoutes map[dag.StepType]string
```

Change `NewOrchestrator` signature to accept options:

```go
func NewOrchestrator(nc *nats.Conn, tel *observe.Telemetry, opts ...OrchestratorOption) *Orchestrator {
```

At the end of `NewOrchestrator`, before the return, apply options:

```go
for _, opt := range opts {
	opt(o)
}
```

- [ ] **Step 4: Add stepSubject helper and update publishTask + publishIterationTask**

Add a helper method that resolves the subject prefix based on step type:

```go
func (o *Orchestrator) stepSubject(step dag.StepDef, runID string) string {
	prefix := "task"
	if o.stepRoutes != nil {
		if p, ok := o.stepRoutes[step.Type]; ok {
			prefix = p
		}
	}
	return prefix + "." + step.Task + "." + runID
}
```

In `publishTask` (line 587), change:

```go
// Before:
msg := buildTaskMsg(step.Task, runID, data, msgID)
// After:
subject := o.stepSubject(step, runID)
msg := &nats.Msg{
	Subject: subject,
	Data:    data,
	Header:  nats.Header{"Nats-Msg-Id": {msgID}},
}
```

In `publishIterationTask` (line 632), make the same change:

```go
// Before:
msg := buildTaskMsg(step.Task, runID, data, msgID)
// After:
subject := o.stepSubject(step, runID)
msg := &nats.Msg{
	Subject: subject,
	Data:    data,
	Header:  nats.Header{"Nats-Msg-Id": {msgID}},
}
```

Remove the old `buildTaskMsg` function (lines 639-654) since it is no longer used.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorRoutesAgentStepsToCustomStream -v -timeout 30s`
Expected: PASS

- [ ] **Step 6: Run ALL tests to verify existing routing is unchanged**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -count=1 -timeout 120s`
Expected: ALL PASS — existing tests still route to `TASK_QUEUES`

- [ ] **Step 7: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): configurable step type routing via WithStepRoutes option"
```

---

## Chunk 4: engine — child workflow lifecycle

### Task 7: Handle workflow.spawn events

**Files:**
- Modify: `engine/orchestrator.go`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for workflow.spawn handling**

In `engine/orchestrator_test.go`, add:

```go
func TestOrchestratorHandlesWorkflowSpawn(t *testing.T) {
	// Methodology: publish a workflow.spawn event. Verify the engine creates
	// a child WorkflowRun with ParentRunID/ParentStepID and starts it.

	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register a child workflow definition
	childDef := dag.WorkflowDef{
		Name:    "child-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "child-step", Task: "child-task", Type: dag.StepTypeNormal},
		},
	}
	childDefData, _ := json.Marshal(childDef)
	if _, err := defKV.Put("child-wf", childDefData); err != nil {
		t.Fatalf("put child def: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish spawn event
	spawnPayload, _ := json.Marshal(map[string]string{
		"child_run_id":    "child-run-1",
		"child_workflow":  "child-wf",
		"parent_step_id":  "parent-step-a",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-run-1", spawnPayload)
	data, _ := spawnEvt.Marshal()
	msg := &nats.Msg{
		Subject: spawnEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", spawnEvt.NATSMsgID())
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish spawn: %v", err)
	}

	// Wait for child run to appear in snapshot store
	store := NewSnapshotStore(js)
	var childRun dag.WorkflowRun
	var loadErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		childRun, loadErr = store.Load("child-run-1")
		if loadErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if loadErr != nil {
		t.Fatalf("child run should exist: %v", loadErr)
	}
	if childRun.ParentRunID != "parent-run-1" {
		t.Fatalf("ParentRunID = %q, want parent-run-1", childRun.ParentRunID)
	}
	if childRun.ParentStepID != "parent-step-a" {
		t.Fatalf("ParentStepID = %q, want parent-step-a", childRun.ParentStepID)
	}
	if childRun.WorkflowID != "child-wf" {
		t.Fatalf("WorkflowID = %q, want child-wf", childRun.WorkflowID)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorHandlesWorkflowSpawn -v -timeout 30s`
Expected: FAIL — spawn event not handled

- [ ] **Step 3: Implement spawn event handler**

First, update `isHandledEventType` (line 168-178) to include the new event types:

```go
case protocol.EventWorkflowStarted,
	protocol.EventStepCompleted,
	protocol.EventStepContinue,
	protocol.EventStepFailed,
	protocol.EventWorkflowSpawn,
	protocol.EventWorkflowChildCompleted,
	protocol.EventWorkflowChildFailed:
	return true
```

Then add `handleWorkflowSpawn` method:

```go
func (o *Orchestrator) handleWorkflowSpawn(
	ctx context.Context, evt protocol.Event,
) error {
	var payload struct {
		ChildRunID   string `json:"child_run_id"`
		ChildWorkflow string `json:"child_workflow"`
		ParentStepID string `json:"parent_step_id"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal spawn payload: %w", err)
	}

	// Load child workflow definition
	entry, err := o.defKV.Get(payload.ChildWorkflow)
	if err != nil {
		return fmt.Errorf("load child workflow def %q: %w",
			payload.ChildWorkflow, err)
	}
	var childDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &childDef); err != nil {
		return fmt.Errorf("unmarshal child def: %w", err)
	}

	// TODO: enforce max nesting depth (check parent chain length)

	// Create child run
	childRun := dag.NewWorkflowRun(childDef, payload.ChildRunID)
	childRun.ParentRunID = evt.RunID
	childRun.ParentStepID = payload.ParentStepID
	childRun.Status = dag.RunStatusRunning

	if err := o.saveSnapshot(ctx, childRun); err != nil {
		return err
	}

	return o.enqueueReady(ctx, childDef, childRun)
}
```

Add the case to `dispatchEvent`:

```go
case protocol.EventWorkflowSpawn:
	return o.handleWorkflowSpawn(ctx, evt)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorHandlesWorkflowSpawn -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): handle workflow.spawn events to create child workflows"
```

---

### Task 8: Handle child workflow completion — notify parent

**Files:**
- Modify: `engine/orchestrator.go`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for child completion notification**

In `engine/orchestrator_test.go`, add:

```go
func TestOrchestratorChildCompletionNotifiesParent(t *testing.T) {
	// Methodology: when a child workflow completes, the engine should publish
	// a workflow.child.completed event on the parent's history subject.

	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()

	// Subscribe to parent's history subject
	sub, err := js.SubscribeSync("history.parent-run-1",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())

	// Create a child run snapshot that's linked to parent
	store := NewSnapshotStore(js)
	childRun := dag.WorkflowRun{
		RunID:        "child-run-2",
		WorkflowID:   "child-wf",
		Status:       dag.RunStatusRunning,
		Steps:        map[string]dag.StepState{"s1": {Status: dag.StepStatusCompleted, Output: []byte(`"done"`)}},
		CreatedAt:    time.Now(),
		ParentRunID:  "parent-run-1",
		ParentStepID: "parent-step-a",
	}
	if err := store.Save(childRun); err != nil {
		t.Fatalf("save child run: %v", err)
	}

	orch.Start()
	defer orch.Stop()

	// Simulate child workflow completing by publishing workflow.completed for child
	completeEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCompleted, "child-run-2", []byte(`"done"`))
	data, _ := completeEvt.Marshal()
	msg := &nats.Msg{
		Subject: completeEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", completeEvt.NATSMsgID())
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish child complete: %v", err)
	}

	// Look for workflow.child.completed event on parent's history
	deadline := time.Now().Add(5 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		m, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		m.Ack()
		evt, _ := protocol.UnmarshalEvent(m.Data)
		if evt.Type == protocol.EventWorkflowChildCompleted {
			found = true
			var payload struct {
				ChildRunID string `json:"child_run_id"`
			}
			json.Unmarshal(evt.Payload, &payload)
			if payload.ChildRunID != "child-run-2" {
				t.Fatalf("child run id = %q, want child-run-2", payload.ChildRunID)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected workflow.child.completed event on parent history")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorChildCompletionNotifiesParent -v -timeout 30s`
Expected: FAIL — no child completion notification

- [ ] **Step 3: Modify completeWorkflow to check for parent**

The `completeWorkflow` method is called when all steps are done. We need to check if the completed run has a parent and notify it. In `engine/orchestrator.go`, in the `completeWorkflow` method (around line 259), after `o.runsCompleted.Inc()` and before the return, add:

```go
// If this is a child workflow, notify the parent
if run.ParentRunID != "" {
	childPayload, _ := json.Marshal(map[string]interface{}{
		"child_run_id":   run.RunID,
		"parent_step_id": run.ParentStepID,
		"output":         json.RawMessage(o.collectChildOutput(run)),
	})
	parentEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowChildCompleted, run.ParentRunID,
		childPayload)
	data, err := parentEvt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal child completed event: %w", err)
	}
	msg := &nats.Msg{
		Subject: parentEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", parentEvt.NATSMsgID())
	if _, err := o.js.PublishMsg(msg); err != nil {
		return fmt.Errorf("publish child completed: %w", err)
	}
}
```

Add a helper to collect child output (last step's output or aggregated):

```go
func (o *Orchestrator) collectChildOutput(run dag.WorkflowRun) []byte {
	// Return the output of the last completed step, or aggregate
	for _, state := range run.Steps {
		if state.Status == dag.StepStatusCompleted && state.Output != nil {
			return state.Output
		}
	}
	return nil
}
```

Similarly, modify the `failWorkflow` path to publish `EventWorkflowChildFailed` when `ParentRunID` is set.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorChildCompletionNotifiesParent -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run ALL tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): child workflow completion notifies parent via workflow.child.completed"
```

---

### Task 9: Enforce max nesting depth

**Files:**
- Modify: `engine/orchestrator.go`
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for nesting depth enforcement**

In `engine/orchestrator_test.go`, add:

```go
func TestOrchestratorRejectsExcessiveNesting(t *testing.T) {
	// Methodology: create a chain of parent runs at max depth, then attempt
	// to spawn another child. The spawn should fail.

	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")
	store := NewSnapshotStore(js)

	// Register child def
	childDef := dag.WorkflowDef{
		Name: "deep-child", Version: "1",
		Steps: []dag.StepDef{{ID: "s1", Task: "t", Type: dag.StepTypeNormal}},
	}
	childDefData, _ := json.Marshal(childDef)
	defKV.Put("deep-child", childDefData)

	// Create a chain: run-0 -> run-1 -> run-2 (depth 3)
	for i := 0; i < 3; i++ {
		run := dag.WorkflowRun{
			RunID: fmt.Sprintf("run-%d", i), WorkflowID: "deep-child",
			Status: dag.RunStatusRunning,
			Steps:  map[string]dag.StepState{"s1": {Status: dag.StepStatusRunning}},
			CreatedAt: time.Now(),
		}
		if i > 0 {
			run.ParentRunID = fmt.Sprintf("run-%d", i-1)
			run.ParentStepID = "s1"
		}
		store.Save(run)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Try to spawn from run-2 (would be depth 4 — exceeds default 3)
	spawnPayload, _ := json.Marshal(map[string]string{
		"child_run_id":   "run-3",
		"child_workflow": "deep-child",
		"parent_step_id": "s1",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "run-2", spawnPayload)
	data, _ := spawnEvt.Marshal()
	msg := &nats.Msg{
		Subject: spawnEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", spawnEvt.NATSMsgID())
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish spawn: %v", err)
	}

	// The child run should NOT be created. Poll briefly instead of sleeping.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := store.Load("run-3")
		if err == nil {
			t.Fatalf("run-3 should not exist — nesting too deep")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Confirmed: run-3 was never created
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorRejectsExcessiveNesting -v -timeout 30s`
Expected: FAIL — child run is created despite nesting limit

- [ ] **Step 3: Implement nesting depth check**

In `engine/orchestrator.go`, add a depth-checking helper and a max depth constant:

```go
const maxNestingDepth = 3

func (o *Orchestrator) nestingDepth(runID string) int {
	depth := 0
	currentID := runID
	for depth < maxNestingDepth+1 {
		run, err := o.store.Load(currentID)
		if err != nil || run.ParentRunID == "" {
			break
		}
		depth++
		currentID = run.ParentRunID
	}
	return depth
}
```

In `handleWorkflowSpawn`, before creating the child run, add:

```go
depth := o.nestingDepth(evt.RunID)
if depth >= maxNestingDepth {
	o.tel.Logger.Error("spawn rejected: max nesting depth exceeded",
		fmt.Errorf("depth %d >= max %d", depth, maxNestingDepth),
		observe.String("parent_run_id", evt.RunID))
	return fmt.Errorf("max nesting depth %d exceeded", maxNestingDepth)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats && go test ./engine/ -run TestOrchestratorRejectsExcessiveNesting -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run ALL tests**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): enforce max nesting depth on child workflow spawning"
```

---

### Task 10: Final integration — E2E test with agent routing and child workflow

**Files:**
- Create: `e2e_agent_test.go`

- [ ] **Step 1: Write E2E test**

Create `e2e_agent_test.go` in the project root:

```go
package dagnats_test

// Methodology: end-to-end test verifying that agent steps are routed to
// a custom stream, and child workflow spawning + completion propagation
// works through the full orchestrator lifecycle.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestE2EAgentStepRouting(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	err := natsutil.SetupAll(nc,
		natsutil.WithStreams(natsutil.StreamConfig{
			Name:     "AGENT_TASKS",
			Subjects: []string{"agent.task.>"},
		}),
	)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Workflow: normal step -> agent step
	wfDef := dag.WorkflowDef{
		Name:    "mixed-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "prepare", Task: "prep-task", Type: dag.StepTypeNormal},
			{ID: "agent", Task: "llm-task", Type: dag.StepTypeAgent,
				DependsOn: []string{"prepare"},
				Metadata:  map[string]string{"role": "coder"}},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("mixed-wf", defData)

	// Start orchestrator with agent routing
	routes := map[dag.StepType]string{dag.StepTypeAgent: "agent.task"}
	orch := engine.NewOrchestrator(nc, observe.NewNoopTelemetry(),
		engine.WithStepRoutes(routes))
	orch.Start()
	defer orch.Stop()

	// Normal worker handles "prepare"
	w := worker.NewWorker(nc, observe.NewNoopTelemetry())
	w.Handle("prep-task", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"prepared"`))
	})
	w.Start()
	defer w.Stop()

	// Subscribe to AGENT_TASKS to verify routing
	agentSub, _ := js.SubscribeSync("agent.task.>",
		nats.AckExplicit(), nats.DeliverAll())

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "e2e-mixed-1", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{Subject: startEvt.NATSSubject(), Data: data, Header: nats.Header{}}
	msg.Header.Set("Nats-Msg-Id", startEvt.NATSMsgID())
	js.PublishMsg(msg)

	// Wait for agent task to arrive on AGENT_TASKS (not TASK_QUEUES)
	agentMsg, err := agentSub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("agent task should arrive on AGENT_TASKS: %v", err)
	}
	if agentMsg == nil {
		t.Fatalf("agent message should not be nil")
	}

	// Verify it's the agent step's task
	var payload protocol.TaskPayload
	json.Unmarshal(agentMsg.Data, &payload)
	if payload.StepID != "agent" {
		t.Fatalf("step id = %q, want agent", payload.StepID)
	}
	agentMsg.Ack()
}
```

- [ ] **Step 2: Run the E2E test**

Run: `cd /Users/dmestas/projects/dagnats && go test -run TestE2EAgentStepRouting -v -timeout 30s`
Expected: PASS

- [ ] **Step 3: Run ALL tests one final time**

Run: `cd /Users/dmestas/projects/dagnats && go test ./... -v -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add e2e_agent_test.go
git commit -m "test: E2E test for agent step routing to custom stream"
```
