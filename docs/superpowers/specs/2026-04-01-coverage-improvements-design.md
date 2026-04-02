# Code Coverage Improvements: 66% to 80%+

## Context

TigerStyle audit and coverage profiling revealed 66.1% statement coverage with 223 functions at 0%. The core workflow engine (engine/, dag/) is well-tested through integration and e2e tests, but many functions are only exercised indirectly. Direct unit tests are missing for builder APIs, resolve functions, TaskContext methods, actor runtime lifecycle, and several handler branches.

**Goal:** Raise overall coverage from 66.1% to 80%+ by adding targeted tests to 6 packages. Skip cli/ (dispatchers call os.Exit, low ROI), observe/noop (trivial no-ops), and cmd/ (entry points).

## Packages and Targets

| Package | Current | Target | Zero-Coverage Funcs | Test Type |
|---------|---------|--------|--------------------:|-----------|
| dag/ | 79.0% | 90%+ | 28 | Pure unit tests |
| worker/ | 60.4% | 85%+ | 15 | Unit + NATS integration |
| engine/ | 70.9% | 85%+ | 20 | NATS integration |
| actor/ | 78.8% | 90%+ | 15 | Pure unit tests |
| trigger/ | 71.1% | 85%+ | 8 | Unit + NATS integration |
| api/ | 70.0% | 85%+ | 12 | NATS integration |

## Work Unit 1: dag/ (pure unit tests)

**File:** `dag/builder_test.go` (extend), `dag/resolve_test.go` (create), `dag/retry_test.go` (extend), `dag/schema_test.go` (extend), `dag/stepref_test.go` (create)

### Functions to cover (28 at 0%):

**Builder API** (dag/builder.go):
- `NewWorkflow` — verify returns StepRef with name set
- `Name`, `Version` — verify accessors return correct values
- `Task` — verify step creation with correct type and dependency wiring
- `AgentLoop` — verify StepTypeAgentLoop set, MaxIterations default
- `Build` — verify produces valid WorkflowDef with correct step count

**Resolve functions** (dag/resolve.go):
- `ResolveReady` — given step states, return which steps are ready to run
- `ResolveSkipped` — given SkipIf conditions and outputs, return skipped steps
- `ResolveInput` — given step dependencies and their outputs, build input
- `IsComplete` — all steps completed/skipped/failed
- `allDepsCompleted` — helper for dependency checking

**Retry policy** (dag/retry.go):
- `MarshalJSON` — verify backoff type serialization
- `ResolveRetryPolicy` — step-level overrides workflow default

**Schema validation** (dag/schema.go):
- `checkType` — type validation against JSON schema types

**StepRef fluent API** (dag/stepref.go):
- `ID`, `After` — builder accessor/modifier methods

**Condition helpers** (dag/condition.go):
- `compareValues`, `toFloat64`, `SkipIfOutput` — comparison operators and output extraction

### Test approach:
All pure functions. No NATS. Table-driven tests with positive and negative cases. 2+ assertions per test (per CLAUDE.md).

## Work Unit 2: worker/ (unit + integration)

**Files:** `worker/context_test.go` (extend), `worker/worker_test.go` (extend), `worker/errors_test.go` (extend), `worker/typed_test.go` (extend)

### Functions to cover (15 at 0%):

**TaskContext methods** (worker/context.go):
- `newTaskContext` — verify field initialization
- `Input`, `RunID`, `StepID`, `RetryCount` — accessor coverage
- `Fail` — verify failure event published to correct subject
- `PutStream` — verify stream data published
- `Heartbeat` — verify InProgress sent on message

**Worker registration** (worker/worker.go):
- `Handle` — verify handler map population
- `splitWorkerTraceparent` — parse traceparent header
- `injectWorkerTraceCtx` — attach trace context to NATS message

**Error types** (worker/errors.go):
- `Error`, `Unwrap`, `NewNonRetryableError` — interface compliance

**Typed handler** (worker/typed.go):
- `Typed` — verify JSON unmarshal + typed handler invocation

### Test approach:
Context methods need embedded NATS (they publish events). Trace propagation tests are pure string parsing. Error/typed tests are pure unit tests.

## Work Unit 3: engine/ (integration tests)

**Files:** `engine/orchestrator_test.go` (extend), `engine/workflow_actor_test.go` (extend)

### Functions to cover (20 at 0%):

**Orchestrator handler branches** (engine/orchestrator.go):
- `handleStepContinue` — agent loop continue path (needs workflow with AgentLoop step that calls Continue)
- Step compensation/OnFailure paths
- Sub-workflow routing
- Snapshot save/restore edge cases

**Workflow actor** (engine/workflow_actor.go):
- `handleStepCompleted` — verify DAG advancement after step completion
- Actor initialization and message routing

### Test approach:
Integration tests with embedded NATS. Define test workflows that exercise:
1. A workflow with an AgentLoop step that calls `Continue` then `Complete`
2. A workflow with SkipIf conditions
3. Snapshot save after step completion, verify restore produces same state

## Work Unit 4: actor/ (pure unit tests)

**Files:** `actor/runtime_test.go` (create), `actor/supervision_test.go` (create)

### Functions to cover (15 at 0%):

**Runtime lifecycle** (actor/runtime.go):
- `NewRuntime` — verify empty actors map
- `Spawn` — verify actor appears in runtime, mailbox created
- `Send` — verify message delivered to mailbox
- `runActor` — verify Receive called on messages, PreStart/PostStop lifecycle
- `restartActor` — verify PostStop called, new goroutine started
- `stopActor` — verify done channel closed, actor removed from runtime

**Actor primitives** (actor/actor.go):
- `Address.String` — format verification
- `Context.Self`, `Context.Send`, `Context.Spawn` — context operations
- `spawnDefaults` — default option values

**Supervision** (actor/supervision.go):
- `OneForOne.Decide`, `OneForOne.RestartScope` — restart single actor
- `AllForOne.Decide`, `AllForOne.RestartScope` — restart all siblings

**Restart tracking** (actor/restarts.go):
- `NewRestartTracker` — initial state
- `Allow` — budget tracking within time window

### Test approach:
Pure unit tests. Create test actors (implement Actor interface with simple Receive). Verify lifecycle hooks, message delivery, supervision decisions, restart budgets.

## Work Unit 5: trigger/ (unit + integration)

**Files:** `trigger/scheduler_test.go` (extend), `trigger/service_test.go` (extend), `trigger/cron_test.go` (extend), `trigger/webhook_test.go` (extend)

### Functions to cover (8 at 0%):

**Scheduler** (trigger/scheduler.go):
- `Start` — verify ticker goroutine starts and fires
- `backfillTrigger` — verify missed cron slots are fired
- `shouldFire` — verify cron matching logic with last-run tracking

**Cron** (trigger/cron.go):
- `Matches` — verify time matching against parsed cron schedule

**Service lifecycle** (trigger/service.go):
- `Start` — verify scheduler starts and KV watcher launches
- `TickNow` — verify immediate tick fires pending triggers
- `WebhookHandler` — verify HTTP handler returns correct handler func

**Validation** (trigger/validate.go):
- `validateCronConfig` — verify cron expression validation

### Test approach:
Cron matching is pure. Service/scheduler lifecycle needs embedded NATS. Webhook handler needs httptest.

## Work Unit 6: api/ (integration tests)

**Files:** `api/service_test.go` (extend), `api/rest_test.go` (extend), `api/natsapi_test.go` (extend)

### Functions to cover (12 at 0%):

**Service inner functions** (api/service.go):
- `GetWorkflow` — verify KV retrieval
- `deleteTriggerInner` — verify KV delete
- `listRunEventsInner` — verify history stream consumption
- `isTaskSubject` — subject pattern matching
- `injectAPIMsgTraceCtx` — trace context on NATS messages

**REST handlers** (api/rest.go):
- `NewRESTHandler` — verify route setup
- `routeWorkflows`, `routeRuns`, `routeRunByID`, `routeHealth` — method routing

**NATS handlers** (api/natsapi.go):
- `Start` — verify subscriptions created
- `handleRegister` — verify workflow registration via NATS request/reply

### Test approach:
All integration with embedded NATS. REST handlers use httptest.NewRecorder. NATS handlers use direct nc.Request.

## Execution Strategy

All 6 work units are independent (different packages, no shared files). Execute as parallel worktree agents:

```
Phase 1 (6 parallel agents):
  Agent 1: dag/       — pure unit tests
  Agent 2: worker/    — unit + NATS integration
  Agent 3: engine/    — NATS integration
  Agent 4: actor/     — pure unit tests
  Agent 5: trigger/   — unit + NATS integration
  Agent 6: api/       — NATS integration

Phase 2 (sequential):
  Merge all worktrees
  Run go test ./... -coverprofile=coverage.out
  Verify 80%+ overall
```

## Verification

```bash
# After all merges:
go test ./... -coverprofile=coverage.out -timeout 120s
go tool cover -func=coverage.out | grep "^total:"
# Expected: total: (statements) ~80-82%

# Per-package verification:
go test ./dag/ -cover           # Target: 90%+
go test ./worker/ -cover        # Target: 85%+
go test ./engine/ -cover        # Target: 85%+
go test ./actor/ -cover         # Target: 90%+
go test ./trigger/ -cover       # Target: 85%+
go test ./api/ -cover           # Target: 85%+
```

## Constraints

- Red-green TDD: write failing test first, then verify it passes with existing code
- 2+ assertions per test (positive + negative space)
- Bounded timeouts on all test waits
- No shared NATS servers between tests
- Each test file opens with a methodology comment
- gofmt formatting, go vet clean, 100-column line limit
