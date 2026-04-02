# Coverage Improvements Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise code coverage from 66.1% to 80%+ across 6 packages.

**Architecture:** 6 independent work units (one per package), each adding tests for zero-coverage functions. Pure unit tests for dag/ and actor/; NATS integration tests for worker/, engine/, trigger/, api/. All use existing `natsutil.StartTestServer(t)` infrastructure.

**Tech Stack:** Go testing, `natsutil.StartTestServer`, `text/tabwriter`, `net/http/httptest`

---

## Task 1: dag/ — Pure Unit Tests (79% → 90%+)

**Files:**
- Create: `dag/resolve_test.go`
- Create: `dag/stepref_test.go`
- Extend: `dag/builder_test.go`
- Extend: `dag/retry_test.go`
- Extend: `dag/schema_test.go`
- Extend: `dag/condition_test.go`

### Subtask 1A: Resolve Functions

- [ ] **Create `dag/resolve_test.go` with methodology comment**

```go
// dag/resolve_test.go
// Methodology: unit tests for DAG resolution logic. Each test builds
// a WorkflowDef with known step states and verifies resolution output
// against expected ready/skipped/complete results.
package dag
```

- [ ] **Test ResolveReady — returns entry points when nothing completed**

Build a 3-step linear workflow (a → b → c). Call `ResolveReady(def, empty, empty)`. Assert returns only step "a". Assert step "b" is NOT returned.

- [ ] **Test ResolveReady — returns next step after dependency completes**

Mark "a" as completed. Call `ResolveReady`. Assert returns "b". Assert "c" is NOT returned.

- [ ] **Test ResolveReady — skips already-queued steps**

Mark "a" completed, "b" queued. Assert ResolveReady returns empty (b already queued, c not ready).

- [ ] **Test ResolveReady — fan-out returns multiple steps**

Build fan-out: a → {b, c} → d. Complete "a". Assert returns both "b" and "c".

- [ ] **Test IsComplete — false when steps pending**

Assert `IsComplete(def, empty)` is false. Assert `IsComplete(def, {"a": true})` is false for 3-step workflow.

- [ ] **Test IsComplete — true when all completed**

Mark all steps completed. Assert IsComplete returns true.

- [ ] **Test ResolveInput — assembles input from dependency outputs**

Build 2-step workflow (a → b). Set step "a" output to `{"result": 1}`. Call `ResolveInput(stepB, steps)`. Assert output matches "a"'s output.

- [ ] **Test ResolveInput — no dependencies returns nil**

Call ResolveInput on entry step with no DependsOn. Assert returns nil, no error.

- [ ] **Test allDepsCompleted — helper function**

Assert true when all deps in completed map. Assert false when one dep missing.

- [ ] **Run tests:** `go test ./dag/ -run TestResolve -v -count=1`

### Subtask 1B: StepRef Fluent API

- [ ] **Create `dag/stepref_test.go` with methodology comment**

- [ ] **Test StepRef.ID — returns step identifier**

```go
ref := NewWorkflow("w").Task("step1", "task1")
if ref.ID() != "step1" { t.Fatal(...) }
```

- [ ] **Test StepRef.After — wires dependency**

Build a → b with `b := builder.Task("b", "t").After(a)`. Build and verify b.DependsOn contains "a".

- [ ] **Test StepRef.WithTimeout — sets step timeout**

- [ ] **Test StepRef.WithRetries — sets legacy retries field**

- [ ] **Test StepRef.WithMaxIterations — sets loop config**

- [ ] **Test StepRef.SkipIf — attaches condition**

- [ ] **Run tests:** `go test ./dag/ -run TestStepRef -v -count=1`

### Subtask 1C: Retry Policy & Schema

- [ ] **Test RetryStrategy.MarshalJSON — serializes backoff type names**

Test Fixed → "fixed", Linear → "linear", Exponential → "exponential".

- [ ] **Test ResolveRetryPolicy — step overrides workflow default**

Build workflow with DefaultRetry and a step with its own Retry. Assert step policy wins.

- [ ] **Test ResolveRetryPolicy — falls back to workflow default**

Step with no Retry. Assert workflow DefaultRetry is returned.

- [ ] **Test ResolveRetryPolicy — falls back to legacy Retries field**

Step with Retries=3, no Retry, no DefaultRetry. Assert returns Fixed policy with MaxAttempts=3.

- [ ] **Test checkType — validates JSON types**

Test string, number, boolean, object, array, null against matching and mismatching values.

- [ ] **Run tests:** `go test ./dag/ -run "TestRetry|TestSchema" -v -count=1`

### Subtask 1D: Conditions

- [ ] **Test compareValues — numeric comparisons**

Table-driven: (1, "==", 1)→true, (1, "<", 2)→true, (2, "<=", 1)→false, etc.

- [ ] **Test compareValues — string comparisons**

Table-driven: ("a", "==", "a")→true, ("a", "!=", "b")→true, ("a", "<", "b")→true.

- [ ] **Test toFloat64 — type conversions**

int→float64, float64→float64, json.Number→float64, string→false.

- [ ] **Test SkipIfOutput — constructs ParentCond**

Verify StepID, Field, Op, Value fields are set correctly.

- [ ] **Run tests:** `go test ./dag/ -run "TestCompare|TestToFloat|TestSkipIfOutput" -v -count=1`

- [ ] **Verify coverage:** `go test ./dag/ -cover` (target: 90%+)

- [ ] **Commit:** `git commit -m "test(dag): add resolve, stepref, retry, schema, and condition tests"`

---

## Task 2: worker/ — Unit + Integration Tests (60% → 85%+)

**Files:**
- Extend: `worker/context_test.go`
- Extend: `worker/worker_test.go`
- Extend: `worker/errors_test.go`
- Extend: `worker/typed_test.go`

### Subtask 2A: TaskContext Accessors and Methods

- [ ] **Test Input/RunID/StepID/RetryCount — accessor coverage**

Create a taskContext with known values. Assert each accessor returns expected value.

- [ ] **Test Fail — publishes EventStepFailed to history stream**

Integration test with embedded NATS. Create taskContext, call Fail(errors.New("boom")). Subscribe to history stream and verify event type and error payload.

- [ ] **Test PutStream — publishes to object store subject**

Integration test. Call PutStream([]byte("data")). Verify message appears on expected subject.

- [ ] **Test Heartbeat — sends InProgress on message**

Integration test. Create context with real NATS msg. Call Heartbeat(). Verify no error returned.

- [ ] **Run tests:** `go test ./worker/ -run "TestContext" -v -count=1`

### Subtask 2B: Worker Registration and Trace Propagation

- [ ] **Test Handle — populates handler map**

Create worker, call Handle("task1", fn). Assert internal handlers map contains "task1".

- [ ] **Test splitWorkerTraceparent — parses W3C format**

Input: `"00-traceid32hex-spanid16hex-01"`. Assert traceID, spanID, ok=true.

- [ ] **Test splitWorkerTraceparent — rejects malformed input**

Input: `"invalid"`. Assert ok=false.

- [ ] **Test injectWorkerTraceCtx — sets traceparent header on event**

Create a span with known trace/span IDs. Call injectWorkerTraceCtx. Verify event and message headers contain traceparent.

- [ ] **Run tests:** `go test ./worker/ -run "TestHandle|TestSplit|TestInject" -v -count=1`

### Subtask 2C: Error Types and Typed Handler

- [ ] **Test NonRetryableError — implements error and Unwrap**

```go
inner := errors.New("fail")
nre := NewNonRetryableError(inner)
if nre.Error() != "fail" { t.Fatal(...) }
if !errors.Is(nre, inner) { t.Fatal(...) }
```

- [ ] **Test Typed — JSON unmarshal and typed invocation**

Define `Typed[MyInput, MyOutput](fn)`. Publish a task with JSON input. Assert fn receives deserialized MyInput and output is serialized MyOutput.

- [ ] **Run tests:** `go test ./worker/ -run "TestNonRetryable|TestTyped" -v -count=1`

- [ ] **Verify coverage:** `go test ./worker/ -cover` (target: 85%+)

- [ ] **Commit:** `git commit -m "test(worker): add context accessor, trace propagation, and typed handler tests"`

---

## Task 3: engine/ — Integration Tests (71% → 85%+)

**Files:**
- Extend: `engine/orchestrator_test.go`
- Extend: `engine/workflow_actor_test.go`

### Subtask 3A: Agent Loop Continue Path

- [ ] **Test handleStepContinue — re-enqueues with incremented iteration**

Define a workflow with an AgentLoop step (MaxIterations=3). Start the run. Publish StepContinue event with iteration=1. Verify a new task appears on TASK_QUEUES with iteration=2.

- [ ] **Test handleStepContinue — enforces MaxIterations limit**

Publish StepContinue with iteration=MaxIterations. Verify step is marked failed with "max iterations exceeded" error.

- [ ] **Run tests:** `go test ./engine/ -run TestOrchestratorContinue -v -count=1`

### Subtask 3B: SkipIf Conditions

- [ ] **Test orchestrator skips steps with met SkipIf condition**

Define workflow: a → b (with SkipIf: a.output.skip == true) → c. Complete step "a" with output `{"skip": true}`. Verify "b" is skipped (StepStatusSkipped). Verify "c" is enqueued (its dep "b" is satisfied by skip).

- [ ] **Run tests:** `go test ./engine/ -run TestOrchestratorSkipIf -v -count=1`

### Subtask 3C: Snapshot Save/Restore

- [ ] **Test saveSnapshot persists run state to KV**

Start orchestrator, trigger a workflow, let one step complete. Read KV directly and verify snapshot matches expected run state.

- [ ] **Test loadRunAndDef restores from KV**

Write a run snapshot to KV manually. Call loadRunAndDef. Assert returned WorkflowRun matches what was stored.

- [ ] **Run tests:** `go test ./engine/ -run TestSnapshot -v -count=1`

### Subtask 3D: Workflow Actor

- [ ] **Test WorkflowActor receives and dispatches events**

Create a WorkflowActor. Send it a WorkflowStarted message via actor.Send. Verify RunStatus changes to Running.

- [ ] **Test WorkflowActor.handleStepCompleted advances DAG**

Send StepCompleted for first step. Verify next step transitions to queued.

- [ ] **Run tests:** `go test ./engine/ -run TestWorkflowActor -v -count=1`

- [ ] **Verify coverage:** `go test ./engine/ -cover` (target: 85%+)

- [ ] **Commit:** `git commit -m "test(engine): add continue, skipif, snapshot, and workflow actor tests"`

---

## Task 4: actor/ — Pure Unit Tests (79% → 90%+)

**Files:**
- Create: `actor/runtime_test.go`
- Create: `actor/supervision_test.go`

### Subtask 4A: Runtime Lifecycle

- [ ] **Create `actor/runtime_test.go` with methodology comment and test actor**

```go
// actor/runtime_test.go
// Methodology: unit tests for actor runtime lifecycle. Uses a simple
// echo actor that records received messages for assertion.
package actor

type echoActor struct {
    received []Message
    mu       sync.Mutex
}

func (a *echoActor) Receive(ctx *Context, msg Message) error {
    a.mu.Lock()
    a.received = append(a.received, msg)
    a.mu.Unlock()
    return nil
}
```

- [ ] **Test Spawn — actor appears in runtime**

```go
rt := NewRuntime()
addr := Address{Type: "test", ID: "1"}
err := rt.Spawn(addr, &echoActor{})
// Assert no error. Assert Send to addr succeeds (actor exists).
```

- [ ] **Test Spawn — duplicate address returns ErrAlreadyExists**

- [ ] **Test Send — delivers message to actor mailbox**

Spawn echo actor, send a message, wait briefly, assert actor.received contains the message.

- [ ] **Test Send — unknown address returns ErrActorNotFound**

- [ ] **Test Send — full mailbox returns ErrMailboxFull**

Spawn with `WithMailboxSize(1)`. Send 2 messages without consuming. Assert second returns ErrMailboxFull.

- [ ] **Test Stop — removes actor from runtime**

Spawn, stop, then Send → ErrActorNotFound.

- [ ] **Test StopAll — removes all actors**

Spawn 3 actors, StopAll, assert all addresses return ErrActorNotFound.

- [ ] **Run tests:** `go test ./actor/ -run TestRuntime -v -count=1`

### Subtask 4B: Lifecycle Hooks

- [ ] **Create lifecycleActor that records PreStart/PostStop calls**

```go
type lifecycleActor struct {
    echoActor
    preStartCalled  bool
    postStopCalled  bool
}
func (a *lifecycleActor) PreStart(ctx *Context) error {
    a.preStartCalled = true; return nil
}
func (a *lifecycleActor) PostStop(ctx *Context) {
    a.postStopCalled = true
}
```

- [ ] **Test PreStart called on Spawn**

- [ ] **Test PostStop called on Stop**

- [ ] **Run tests:** `go test ./actor/ -run TestLifecycle -v -count=1`

### Subtask 4C: Supervision and Restarts

- [ ] **Create `actor/supervision_test.go` with methodology comment**

- [ ] **Test OneForOne.Decide — returns configured directive**

```go
s := &OneForOne{Decider: func(err error) Directive { return Restart }}
if s.Decide(errors.New("boom")) != Restart { t.Fatal(...) }
```

- [ ] **Test OneForOne.RestartScope — returns RestartOne**

- [ ] **Test AllForOne.Decide — returns configured directive**

- [ ] **Test AllForOne.RestartScope — returns RestartAll**

- [ ] **Test RestartTracker.Allow — permits within budget**

Create tracker with limit=3, window=1min. Assert Allow() returns true 3 times, false on 4th.

- [ ] **Test RestartTracker.Allow — resets after window expires**

Create tracker with window=10ms. Exhaust budget, sleep 20ms, assert Allow() returns true again.

- [ ] **Run tests:** `go test ./actor/ -run "TestSupervision|TestRestart" -v -count=1`

- [ ] **Verify coverage:** `go test ./actor/ -cover` (target: 90%+)

- [ ] **Commit:** `git commit -m "test(actor): add runtime lifecycle, supervision, and restart tracker tests"`

---

## Task 5: trigger/ — Unit + Integration Tests (71% → 85%+)

**Files:**
- Extend: `trigger/cron_test.go`
- Extend: `trigger/scheduler_test.go`
- Extend: `trigger/service_test.go`
- Extend: `trigger/webhook_test.go`
- Extend: `trigger/validate_test.go`

### Subtask 5A: Cron Matching Edge Cases

- [ ] **Test Matches — exact minute match**

Parse "30 9 * * *". Assert Matches(9:30) true. Assert Matches(9:31) false.

- [ ] **Test Matches — wildcard fields**

Parse "* * * * *". Assert matches any time.

- [ ] **Test Matches — day-of-week filtering**

Parse "0 0 * * 1" (Monday midnight). Assert matches Monday, rejects Tuesday.

- [ ] **Run tests:** `go test ./trigger/ -run TestMatches -v -count=1`

### Subtask 5B: Scheduler Lifecycle

- [ ] **Test Start — fires ticks at interval**

Create scheduler, add a trigger that matches now. Call Start with 50ms interval and a stopChan. After 200ms, close stopChan. Assert trigger fired at least once.

- [ ] **Test shouldFire — checks last-run timestamp**

Set last_run_at to 2 minutes ago. Assert shouldFire returns true for matching cron. Set last_run_at to current minute. Assert returns false (already fired).

- [ ] **Test backfillTrigger — replays missed cron matches**

Set last_run_at to 5 minutes ago. Assert exactly 5 workflow events published.

- [ ] **Run tests:** `go test ./trigger/ -run "TestSchedulerStart|TestShouldFire|TestBackfillTrigger" -v -count=1`

### Subtask 5C: TriggerService Lifecycle

- [ ] **Test TriggerService.Start — loads triggers and starts scheduler**

Create service, add a cron trigger to KV, call Start. Assert TriggerCount() returns 1.

- [ ] **Test TickNow — forces immediate evaluation**

Add a matching cron trigger. Call TickNow. Assert workflow event published.

- [ ] **Test WebhookHandler — returns non-nil HTTP handler**

Call WebhookHandler(). Assert result is not nil. Issue a GET request. Assert 405 Method Not Allowed (webhooks are POST only).

- [ ] **Run tests:** `go test ./trigger/ -run "TestTriggerService" -v -count=1`

### Subtask 5D: Validation

- [ ] **Test validateCronConfig — rejects empty expression**

- [ ] **Test validateCronConfig — accepts valid expression**

- [ ] **Run tests:** `go test ./trigger/ -run TestValidateCron -v -count=1`

- [ ] **Verify coverage:** `go test ./trigger/ -cover` (target: 85%+)

- [ ] **Commit:** `git commit -m "test(trigger): add cron matching, scheduler lifecycle, and service tests"`

---

## Task 6: api/ — Integration Tests (70% → 85%+)

**Files:**
- Extend: `api/service_test.go`
- Extend: `api/rest_test.go`
- Extend: `api/natsapi_test.go`

### Subtask 6A: Service Inner Functions

- [ ] **Test GetWorkflow — retrieves from KV**

Register a workflow, then call GetWorkflow by name. Assert returned def matches.

- [ ] **Test GetWorkflow — returns error for unknown name**

Call GetWorkflow("nonexistent"). Assert error is non-nil.

- [ ] **Test deleteTriggerInner — removes from KV**

Create a trigger in KV, call deleteTriggerInner, verify key is gone.

- [ ] **Test listRunEventsInner — reads history stream**

Publish 3 events to history stream for a run. Call listRunEventsInner. Assert returns 3 events in order.

- [ ] **Test isTaskSubject — pattern matching**

Assert `isTaskSubject("task.mytask.run123")` is true. Assert `isTaskSubject("history.run123")` is false.

- [ ] **Run tests:** `go test ./api/ -run "TestGetWorkflow|TestDeleteTrigger|TestListRunEvents|TestIsTask" -v -count=1`

### Subtask 6B: REST Handler Routing

- [ ] **Test routeWorkflows — rejects GET (POST only)**

Use httptest.NewRecorder. Send GET to /workflows. Assert 405.

- [ ] **Test routeRuns — rejects GET (POST only)**

- [ ] **Test routeRunByID — rejects POST (GET only)**

- [ ] **Test routeHealth — rejects POST (GET only)**

- [ ] **Run tests:** `go test ./api/ -run "TestRoute" -v -count=1`

### Subtask 6C: NATS API Handlers

- [ ] **Test NATSAPI.Start — creates subscriptions**

Create NATSAPI, call Start. Send a request to "api.workflows.register". Assert reply received (subscription active).

- [ ] **Test handleRegister — registers workflow via NATS request/reply**

Send a WorkflowDef JSON to "api.workflows.register". Assert reply contains success. Verify workflow exists in KV.

- [ ] **Run tests:** `go test ./api/ -run "TestNATSAPI" -v -count=1`

- [ ] **Verify coverage:** `go test ./api/ -cover` (target: 85%+)

- [ ] **Commit:** `git commit -m "test(api): add inner function, REST routing, and NATS handler tests"`

---

## Verification

After all 6 tasks merge:

```bash
go test ./... -coverprofile=coverage.out -timeout 120s
go tool cover -func=coverage.out | grep "^total:"
# Expected: total: (statements) ~80-82%

# Per-package:
go test ./dag/ -cover           # Target: 90%+
go test ./worker/ -cover        # Target: 85%+
go test ./engine/ -cover        # Target: 85%+
go test ./actor/ -cover         # Target: 90%+
go test ./trigger/ -cover       # Target: 85%+
go test ./api/ -cover           # Target: 85%+
```

## Execution Strategy

All 6 tasks are independent (different packages). Execute as parallel worktree agents, then merge sequentially. Each agent runs `go test ./<pkg>/ -cover` before committing to verify target met.
