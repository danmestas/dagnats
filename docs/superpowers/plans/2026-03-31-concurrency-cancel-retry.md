# Concurrency Limits, Workflow Cancel, and Retry Policies — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add configurable retry policies, workflow cancellation, and concurrency limits to DagNats — closing the three most critical feature gaps vs Kestra/Hatchet/Temporal.

**Architecture:** Three independent features sharing `dag/types.go` extensions. Retry policies are pure `dag/` logic (no NATS). Cancel adds a new event type and orchestrator handler + worker Done() channel via KV watch. Concurrency adds KV-based counters with optimistic locking. Built incrementally — each chunk is independently testable and committable.

**Tech Stack:** Go, NATS JetStream KV, existing `dag/`, `engine/`, `worker/`, `protocol/` packages

**Spec:** `docs/superpowers/specs/2026-03-31-concurrency-cancel-retry-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `dag/retry.go` | RetryStrategy enum, RetryPolicy struct, ResolveRetryPolicy, CalculateDelay |
| `dag/retry_test.go` | Strategy resolution + delay calculation tests |
| `dag/types.go` | Add StepStatusCancelled, ConcurrencyLimit, Retry field on StepDef, DefaultRetry on WorkflowDef |
| `dag/types_test.go` | JSON round-trip for new types |
| `protocol/protocol.go` | Add EventWorkflowCancelled, EventStepCancelled |
| `protocol/protocol_test.go` | Event type tests |
| `engine/orchestrator.go` | Modified handleStepFailed (use retry policy), new handleWorkflowCancelled |
| `engine/orchestrator_test.go` | Retry + cancel integration tests |
| `engine/concurrency.go` | ConcurrencyManager — KV acquire/release/queue |
| `engine/concurrency_test.go` | Concurrency integration tests |
| `worker/context.go` | Add Done() channel, Cancel() method, KV cancel watcher |
| `worker/worker.go` | Wire cancel watcher into task execution |
| `worker/context_test.go` | Done() channel tests |

---

## Chunk 1: Retry Policies (pure dag/ logic)

### Task 1: RetryStrategy enum and RetryPolicy type

**Files:**
- Create: `dag/retry.go`
- Test: `dag/retry_test.go`

- [ ] **Step 1: Write failing tests for RetryStrategy and RetryPolicy**

Create `dag/retry_test.go`:

```go
package dag

// Methodology: unit tests for retry policy types and logic.
// Pure — no NATS dependency.

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestRetryStrategyStringAndJSON(t *testing.T) {
	// Positive: string representation
	if RetryFixed.String() != "fixed" {
		t.Fatalf("RetryFixed.String() = %q", RetryFixed.String())
	}
	if RetryLinear.String() != "linear" {
		t.Fatalf("RetryLinear.String() = %q", RetryLinear.String())
	}
	if RetryExponential.String() != "exponential" {
		t.Fatalf("RetryExponential.String() = %q",
			RetryExponential.String())
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(RetryExponential)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RetryStrategy
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != RetryExponential {
		t.Fatalf("round-trip = %v, want Exponential", got)
	}
}

func TestRetryPolicyJSON(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts:  3,
		Strategy:     RetryExponential,
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
	}

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got RetryPolicy
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: fields round-trip
	if got.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", got.MaxAttempts)
	}
	if got.Strategy != RetryExponential {
		t.Fatalf("Strategy = %v, want Exponential", got.Strategy)
	}
	if got.Multiplier != 2.0 {
		t.Fatalf("Multiplier = %f, want 2.0", got.Multiplier)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run "TestRetryStrategy|TestRetryPolicy" -v`
Expected: FAIL — `RetryStrategy` undefined

- [ ] **Step 3: Implement RetryStrategy and RetryPolicy**

Create `dag/retry.go`:

```go
package dag

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// RetryStrategy selects the backoff algorithm for step retries.
type RetryStrategy int

const (
	RetryFixed       RetryStrategy = iota // Same delay every attempt
	RetryLinear                           // delay * attempt
	RetryExponential                      // delay * multiplier^(attempt-1)
)

var retryStrategyStrings = [...]string{
	"fixed", "linear", "exponential",
}

func (s RetryStrategy) String() string {
	if int(s) < len(retryStrategyStrings) {
		return retryStrategyStrings[s]
	}
	panic(fmt.Sprintf("unknown RetryStrategy %d", s))
}

func (s RetryStrategy) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *RetryStrategy) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	for i, v := range retryStrategyStrings {
		if v == str {
			*s = RetryStrategy(i)
			return nil
		}
	}
	return fmt.Errorf("unknown RetryStrategy: %q", str)
}

// RetryPolicy configures retry behavior for a step or as a workflow
// default. MaxAttempts=0 means no retries.
type RetryPolicy struct {
	MaxAttempts  int           `json:"max_attempts"`
	Strategy     RetryStrategy `json:"strategy"`
	InitialDelay time.Duration `json:"initial_delay"`
	MaxDelay     time.Duration `json:"max_delay"`
	Multiplier   float64       `json:"multiplier,omitempty"`
}

// ResolveRetryPolicy returns the effective retry policy for a step.
// Resolution order: step Retry → workflow DefaultRetry → legacy
// Retries field → nil (no retries).
func ResolveRetryPolicy(
	wfDef WorkflowDef, stepDef StepDef,
) *RetryPolicy {
	if stepDef.Retry != nil {
		return stepDef.Retry
	}
	if wfDef.DefaultRetry != nil {
		return wfDef.DefaultRetry
	}
	if stepDef.Retries > 0 {
		return &RetryPolicy{
			MaxAttempts:  stepDef.Retries,
			Strategy:     RetryFixed,
			InitialDelay: 5 * time.Second,
			MaxDelay:     5 * time.Second,
		}
	}
	return nil
}

// CalculateDelay returns the delay before the next retry attempt.
// Attempt is 1-based (first retry = attempt 1).
func CalculateDelay(
	policy RetryPolicy, attempt int,
) time.Duration {
	if attempt < 1 {
		panic("CalculateDelay: attempt must be >= 1")
	}
	var delay time.Duration
	switch policy.Strategy {
	case RetryFixed:
		delay = policy.InitialDelay
	case RetryLinear:
		delay = policy.InitialDelay * time.Duration(attempt)
	case RetryExponential:
		d := float64(policy.InitialDelay) *
			math.Pow(policy.Multiplier, float64(attempt-1))
		delay = time.Duration(d)
	default:
		delay = policy.InitialDelay
	}
	if policy.MaxDelay > 0 && delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	return delay
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run "TestRetryStrategy|TestRetryPolicy" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add dag/retry.go dag/retry_test.go
git commit -m "feat(dag): add RetryStrategy, RetryPolicy, ResolveRetryPolicy, CalculateDelay"
```

---

### Task 2: ResolveRetryPolicy and CalculateDelay tests

**Files:**
- Modify: `dag/retry_test.go`

- [ ] **Step 1: Write tests for resolution and delay calculation**

Add to `dag/retry_test.go`:

```go
func TestResolveRetryPolicyStepOverridesWorkflow(t *testing.T) {
	stepPolicy := &RetryPolicy{
		MaxAttempts: 5, Strategy: RetryExponential,
		InitialDelay: 2 * time.Second, Multiplier: 3.0,
	}
	wfDefault := &RetryPolicy{
		MaxAttempts: 2, Strategy: RetryFixed,
		InitialDelay: 1 * time.Second,
	}
	wfDef := WorkflowDef{DefaultRetry: wfDefault}
	stepDef := StepDef{Retry: stepPolicy}

	// Positive: step policy wins
	got := ResolveRetryPolicy(wfDef, stepDef)
	if got.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", got.MaxAttempts)
	}
	if got.Strategy != RetryExponential {
		t.Fatalf("Strategy = %v, want Exponential", got.Strategy)
	}
}

func TestResolveRetryPolicyFallsToWorkflow(t *testing.T) {
	wfDefault := &RetryPolicy{
		MaxAttempts: 2, Strategy: RetryFixed,
		InitialDelay: 1 * time.Second,
	}
	wfDef := WorkflowDef{DefaultRetry: wfDefault}
	stepDef := StepDef{}

	// Positive: workflow default used
	got := ResolveRetryPolicy(wfDef, stepDef)
	if got.MaxAttempts != 2 {
		t.Fatalf("MaxAttempts = %d, want 2", got.MaxAttempts)
	}
}

func TestResolveRetryPolicyLegacyRetries(t *testing.T) {
	wfDef := WorkflowDef{}
	stepDef := StepDef{Retries: 3}

	// Positive: synthesized from legacy field
	got := ResolveRetryPolicy(wfDef, stepDef)
	if got == nil {
		t.Fatalf("expected non-nil policy from Retries=3")
	}
	if got.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", got.MaxAttempts)
	}
	if got.Strategy != RetryFixed {
		t.Fatalf("Strategy = %v, want Fixed", got.Strategy)
	}
}

func TestResolveRetryPolicyNilWhenNone(t *testing.T) {
	got := ResolveRetryPolicy(WorkflowDef{}, StepDef{})
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestCalculateDelayFixed(t *testing.T) {
	p := RetryPolicy{
		Strategy: RetryFixed, InitialDelay: 5 * time.Second,
	}
	// Positive: same delay every attempt
	if d := CalculateDelay(p, 1); d != 5*time.Second {
		t.Fatalf("attempt 1 = %v, want 5s", d)
	}
	if d := CalculateDelay(p, 3); d != 5*time.Second {
		t.Fatalf("attempt 3 = %v, want 5s", d)
	}
}

func TestCalculateDelayLinear(t *testing.T) {
	p := RetryPolicy{
		Strategy: RetryLinear, InitialDelay: 2 * time.Second,
	}
	// Positive: delay * attempt
	if d := CalculateDelay(p, 1); d != 2*time.Second {
		t.Fatalf("attempt 1 = %v, want 2s", d)
	}
	if d := CalculateDelay(p, 3); d != 6*time.Second {
		t.Fatalf("attempt 3 = %v, want 6s", d)
	}
}

func TestCalculateDelayExponential(t *testing.T) {
	p := RetryPolicy{
		Strategy: RetryExponential, InitialDelay: 1 * time.Second,
		Multiplier: 2.0, MaxDelay: 30 * time.Second,
	}
	// Positive: 1s, 2s, 4s, 8s...
	if d := CalculateDelay(p, 1); d != 1*time.Second {
		t.Fatalf("attempt 1 = %v, want 1s", d)
	}
	if d := CalculateDelay(p, 3); d != 4*time.Second {
		t.Fatalf("attempt 3 = %v, want 4s", d)
	}

	// Positive: capped at MaxDelay
	if d := CalculateDelay(p, 10); d != 30*time.Second {
		t.Fatalf("attempt 10 = %v, want 30s (capped)", d)
	}
}

func TestCalculateDelayPanicsOnZeroAttempt(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for attempt 0")
		}
	}()
	CalculateDelay(RetryPolicy{Strategy: RetryFixed}, 0)
}
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -run "TestResolve|TestCalculate" -v`
Expected: PASS

- [ ] **Step 3: Add Retry field to StepDef and DefaultRetry to WorkflowDef**

In `dag/types.go`, add to `StepDef` (after `Metadata` field, line 151):

```go
Retry *RetryPolicy `json:"retry,omitempty"`
```

Add to `WorkflowDef` (after `Steps` field, line 160):

```go
DefaultRetry *RetryPolicy      `json:"default_retry,omitempty"`
Concurrency  *ConcurrencyLimit  `json:"concurrency,omitempty"`
```

Add `ConcurrencyLimit` type (after `AgentLoopConfig`, around line 138):

```go
// ConcurrencyLimit controls parallel execution at workflow and step level.
type ConcurrencyLimit struct {
	MaxRuns  int `json:"max_runs,omitempty"`
	MaxSteps int `json:"max_steps,omitempty"`
}
```

Add `StepStatusCancelled` to the `StepStatus` enum (after `StepStatusSkipped`, line 100):

```go
StepStatusCancelled
```

Update `stepStatusStrings` (line 103):

```go
var stepStatusStrings = [...]string{
	"pending", "queued", "running", "completed",
	"failed", "skipped", "cancelled",
}
```

- [ ] **Step 4: Run all dag tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./dag/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add dag/retry.go dag/retry_test.go dag/types.go
git commit -m "feat(dag): add RetryPolicy resolution, delay calculation, StepStatusCancelled, ConcurrencyLimit"
```

---

## Chunk 2: Workflow Cancel

### Task 3: Cancel event types and orchestrator handler

**Files:**
- Modify: `protocol/protocol.go:24-37`
- Modify: `engine/orchestrator.go` (isHandledEventType, new handler)
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Add cancel event types to protocol**

In `protocol/protocol.go`, after `EventWorkflowChildFailed` (line 36), add:

```go
EventWorkflowCancelled EventType = "workflow.cancelled"
EventStepCancelled     EventType = "step.cancelled"
```

- [ ] **Step 2: Write failing test for cancel handler**

Add to `engine/orchestrator_test.go`:

```go
func TestOrchestratorCancelsRunningWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "cancel-test", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("cancel-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cancel-run-1", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg)
	time.Sleep(200 * time.Millisecond)

	// Cancel the workflow
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cancel-run-1", nil)
	cancelData, _ := cancelEvt.Marshal()
	cancelMsg := &nats.Msg{
		Subject: cancelEvt.NATSSubject(),
		Data:    cancelData,
		Header:  nats.Header{"Nats-Msg-Id": {cancelEvt.NATSMsgID()}},
	}
	js.PublishMsg(cancelMsg)

	// Wait for processing
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("cancel-run-1")
		if err == nil && run.Status == dag.RunStatusCancelled {
			// Positive: run is cancelled
			// Positive: step is cancelled
			s1 := run.Steps["s1"]
			if s1.Status != dag.StepStatusCancelled {
				t.Fatalf("step status = %v, want Cancelled",
					s1.Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be cancelled within 3s")
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestOrchestratorCancels -v -timeout 30s`
Expected: FAIL — cancel event not handled

- [ ] **Step 4: Implement handleWorkflowCancelled**

In `engine/orchestrator.go`:

Update `isHandledEventType` to include `protocol.EventWorkflowCancelled`:

```go
case protocol.EventWorkflowStarted,
	protocol.EventStepCompleted,
	protocol.EventStepContinue,
	protocol.EventStepFailed,
	protocol.EventWorkflowSpawn,
	protocol.EventWorkflowCancelled:
	return true
```

Add case to `dispatchEvent`:

```go
case protocol.EventWorkflowCancelled:
	return o.handleWorkflowCancelled(ctx, evt)
```

Add the handler method:

```go
// handleWorkflowCancelled marks the run and all in-flight steps as
// cancelled, saves state, and adjusts metrics.
func (o *Orchestrator) handleWorkflowCancelled(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowCancelled: RunID must not be empty")
	}
	_, run, err := o.loadRunAndDef(evt.RunID)
	if err != nil {
		return err
	}
	if run.Status != dag.RunStatusRunning {
		return nil // Already terminal — no-op
	}

	run.Status = dag.RunStatusCancelled
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusQueued ||
			state.Status == dag.StepStatusRunning ||
			state.Status == dag.StepStatusPending {
			state.Status = dag.StepStatusCancelled
			run.Steps[id] = state
		}
	}

	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	o.runsActive.Dec()
	return o.notifyParentIfChild(run, fmt.Errorf("cancelled"))
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestOrchestratorCancels -v -timeout 30s`
Expected: PASS

- [ ] **Step 6: Run all engine + project tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add protocol/protocol.go engine/orchestrator.go engine/orchestrator_test.go
git commit -m "feat(engine): add workflow cancellation with step-level cancel propagation"
```

---

### Task 4: Integrate retry policies into orchestrator handleStepFailed

**Files:**
- Modify: `engine/orchestrator.go:434-483` (handleStepFailed)
- Test: `engine/orchestrator_test.go`

- [ ] **Step 1: Write failing test for retry policy integration**

Add to `engine/orchestrator_test.go`:

```go
func TestOrchestratorRetriesWithPolicy(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
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
	defData, _ := json.Marshal(wfDef)
	defKV.Put("retry-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "retry-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// First failure — should not be permanently failed
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "retry-run-1", "s1",
		[]byte(`"transient error"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	store := NewSnapshotStore(js)
	run, _ := store.Load("retry-run-1")

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
	defData, _ := json.Marshal(wfDef)
	defKV.Put("exhaust-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "exhaust-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail 3 times (> MaxAttempts of 2)
	for i := 0; i < 3; i++ {
		failEvt := protocol.NewStepEvent(
			protocol.EventStepFailed, "exhaust-run-1", "s1",
			[]byte(`"permanent error"`))
		// Unique msg ID per attempt
		msgID := fmt.Sprintf("exhaust-run-1.s1.fail.%d", i)
		failData, _ := failEvt.Marshal()
		js.PublishMsg(&nats.Msg{
			Subject: failEvt.NATSSubject(), Data: failData,
			Header:  nats.Header{"Nats-Msg-Id": {msgID}},
		})
		time.Sleep(100 * time.Millisecond)
	}

	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("exhaust-run-1")
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
```

- [ ] **Step 2: Run test to verify it fails (or doesn't behave correctly)**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run "TestOrchestratorRetries|TestOrchestratorExhausts" -v -timeout 30s`
Expected: May pass partially (existing retry logic) but won't use new RetryPolicy

- [ ] **Step 3: Update handleStepFailed to use ResolveRetryPolicy**

Replace the retry logic in `engine/orchestrator.go` `handleStepFailed` (lines 453-466) with:

```go
	stepDef, _ := findStepDef(wfDef, evt.StepID)
	policy := dag.ResolveRetryPolicy(wfDef, stepDef)

	if policy != nil && state.Attempts <= policy.MaxAttempts {
		// Retries remaining — save state and let NATS redeliver.
		run.Steps[evt.StepID] = state
		return o.saveSnapshot(ctx, run)
	}
```

Remove the old `maxRetries` lookup code.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run "TestOrchestratorRetries|TestOrchestratorExhausts" -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add engine/orchestrator.go engine/orchestrator_test.go dag/types.go
git commit -m "feat(engine): integrate configurable retry policies into handleStepFailed"
```

---

## Chunk 3: Concurrency Limits

### Task 5: ConcurrencyManager — KV-based acquire/release

**Files:**
- Create: `engine/concurrency.go`
- Test: `engine/concurrency_test.go`

- [ ] **Step 1: Write failing test for concurrency acquire/release**

Create `engine/concurrency_test.go`:

```go
package engine

// Methodology: integration tests for KV-based concurrency limits.
// Each test uses its own embedded NATS server.

import (
	"testing"

	"github.com/danmestas/dagnats/natsutil"
)

func TestConcurrencyAcquireAndRelease(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: first acquire succeeds
	ok, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if !ok {
		t.Fatalf("acquire 1 should succeed")
	}

	// Positive: second acquire succeeds (limit 2)
	ok2, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if !ok2 {
		t.Fatalf("acquire 2 should succeed")
	}

	// Negative: third acquire fails (at limit)
	ok3, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if ok3 {
		t.Fatalf("acquire 3 should fail (limit 2)")
	}

	// Release one
	if err := cm.ReleaseRun("wf-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Positive: acquire succeeds again after release
	ok4, err := cm.AcquireRun("wf-1", 2)
	if err != nil {
		t.Fatalf("acquire 4: %v", err)
	}
	if !ok4 {
		t.Fatalf("acquire 4 should succeed after release")
	}
}

func TestConcurrencyUnlimitedWhenZero(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	cm := NewConcurrencyManager(js)

	// Positive: limit 0 means unlimited
	ok, err := cm.AcquireRun("wf-2", 0)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !ok {
		t.Fatalf("limit 0 should always succeed")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestConcurrency -v -timeout 30s`
Expected: FAIL — `NewConcurrencyManager` undefined

- [ ] **Step 3: Implement ConcurrencyManager**

Create `engine/concurrency.go`:

```go
package engine

import (
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

// ConcurrencyManager enforces run and step concurrency limits using
// NATS KV counters with optimistic locking. Thread-safe.
type ConcurrencyManager struct {
	runKV nats.KeyValue
}

// NewConcurrencyManager creates a manager using the concurrency_runs
// KV bucket. Panics if the bucket doesn't exist.
func NewConcurrencyManager(
	js nats.JetStreamContext,
) *ConcurrencyManager {
	if js == nil {
		panic("NewConcurrencyManager: js must not be nil")
	}
	kv, err := js.KeyValue("concurrency_runs")
	if err != nil {
		panic("NewConcurrencyManager: concurrency_runs: " +
			err.Error())
	}
	return &ConcurrencyManager{runKV: kv}
}

// AcquireRun increments the counter for the workflow. Returns false
// if the limit is reached. Limit 0 means unlimited.
func (cm *ConcurrencyManager) AcquireRun(
	workflowID string, limit int,
) (bool, error) {
	if workflowID == "" {
		panic("AcquireRun: workflowID must not be empty")
	}
	if limit <= 0 {
		return true, nil // Unlimited
	}

	key := "workflow." + workflowID

	// Retry loop for optimistic locking (bounded)
	for attempt := 0; attempt < 10; attempt++ {
		current, rev, err := cm.readCounter(key)
		if err != nil {
			return false, err
		}
		if current >= limit {
			return false, nil
		}
		if cm.casIncrement(key, current, rev) {
			return true, nil
		}
		// CAS failed — retry
	}
	return false, fmt.Errorf("acquire: too many CAS retries")
}

// ReleaseRun decrements the counter for the workflow.
func (cm *ConcurrencyManager) ReleaseRun(
	workflowID string,
) error {
	if workflowID == "" {
		panic("ReleaseRun: workflowID must not be empty")
	}
	key := "workflow." + workflowID

	for attempt := 0; attempt < 10; attempt++ {
		current, rev, err := cm.readCounter(key)
		if err != nil {
			return err
		}
		if current <= 0 {
			return nil // Already at zero
		}
		newVal := current - 1
		data := []byte(strconv.Itoa(newVal))
		if rev == 0 {
			_, err = cm.runKV.Create(key, data)
		} else {
			_, err = cm.runKV.Update(key, data, rev)
		}
		if err == nil {
			return nil
		}
		// CAS failed — retry
	}
	return fmt.Errorf("release: too many CAS retries")
}

func (cm *ConcurrencyManager) readCounter(
	key string,
) (int, uint64, error) {
	entry, err := cm.runKV.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	val, err := strconv.Atoi(string(entry.Value()))
	if err != nil {
		return 0, entry.Revision(), nil
	}
	return val, entry.Revision(), nil
}

func (cm *ConcurrencyManager) casIncrement(
	key string, current int, rev uint64,
) bool {
	newVal := current + 1
	data := []byte(strconv.Itoa(newVal))
	var err error
	if rev == 0 {
		_, err = cm.runKV.Create(key, data)
	} else {
		_, err = cm.runKV.Update(key, data, rev)
	}
	return err == nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./engine/ -run TestConcurrency -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add engine/concurrency.go engine/concurrency_test.go
git commit -m "feat(engine): add ConcurrencyManager with KV-based acquire/release"
```

---

### Task 6: Full test suite verification

- [ ] **Step 1: Run all tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -v -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 2: Verify go vet**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go vet ./...`
Expected: No issues

- [ ] **Step 3: Check line counts**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && wc -l dag/retry.go engine/concurrency.go`
Expected: Both under 150 lines
