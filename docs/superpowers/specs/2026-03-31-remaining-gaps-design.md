# Remaining Feature Gaps Design

## Context

After implementing concurrency limits, workflow cancel, retry policies, triggers, and
the actor model, DagNats still lacks seven features that competitors provide. These are
all small, well-scoped additions that mostly extend existing types and orchestrator logic.

## Design Decisions

1. **Compensation over error/finally** — Saga-style OnFailure + Compensate per step
   is more powerful than static error/finally blocks.
2. **Signal via KV (pull)** — Workers watch KV keys for signals. No orchestrator
   changes. Matches existing KV watch pattern for child workflows.
3. **Agent heartbeats via NATS InProgress** — No custom heartbeat service. Reuses
   NATS AckWait extension mechanism.
4. **DLQ via JetStream stream** — No architecture change. One stream, one publish
   call in handleStepFailed.
5. **JSON Schema subset** — In-house ~100 LOC validator. No external dependency.
   Supports type, required, properties only.

---

## 1. Worker Groups

**Changes:**

`dag/types.go` — Add field to StepDef:
```go
WorkerGroup string `json:"worker_group,omitempty"`
```

`engine/orchestrator.go` — Modify `stepSubject` helper to include worker group:
```go
func (o *Orchestrator) stepSubject(step dag.StepDef, runID string) string {
    prefix := "task"
    if o.stepRoutes != nil {
        if p, ok := o.stepRoutes[step.Type]; ok {
            prefix = p
        }
    }
    subject := prefix + "." + step.Task
    if step.WorkerGroup != "" {
        subject += "." + step.WorkerGroup
    }
    return subject + "." + runID
}
```

`worker/worker.go` — Add group subscription option:
```go
type WorkerOption func(*Worker)

func WithGroups(groups ...string) WorkerOption
```

Workers subscribe to `task.{taskType}.{group}.>` for each group. If no groups
specified, subscribe to `task.{taskType}.>` (backwards compatible).

---

## 2. Compensation/Saga

**Changes:**

`dag/types.go` — Add fields to StepDef:
```go
OnFailure  string `json:"on_failure,omitempty"`
Compensate string `json:"compensate,omitempty"`
```

`dag/validate.go` — Validate that OnFailure and Compensate reference existing step
IDs in the workflow definition.

`engine/orchestrator.go` — Modify `handleStepFailed`:
- After retries exhausted, check `stepDef.OnFailure`
- If set: enqueue the on-failure step with the error as input instead of failing
  the workflow immediately
- The on-failure step's completion/failure then determines workflow outcome

`engine/orchestrator.go` — Add `runCompensations` method:
- Called when workflow fails after some steps completed
- Walk completed steps in reverse order
- For each with `Compensate` set: enqueue the compensation step
- Compensation steps run sequentially (each waits for previous to finish)
- Compensation failure is logged but doesn't change workflow status (best-effort)

**Compensation flow:**
```
Step A completes → Step B completes → Step C fails (retries exhausted)
  → OnFailure handler for C runs (if configured)
  → If workflow still fails: run B.Compensate, then A.Compensate (reverse order)
  → Workflow status: Failed (with compensation attempted)
```

---

## 3. Workflow Timeouts

**Changes:**

`dag/types.go` — Add field to WorkflowDef:
```go
Timeout time.Duration `json:"timeout,omitempty"`
```

`dag/types.go` — Add field to WorkflowRun:
```go
Deadline time.Time `json:"deadline,omitempty"`
```

`engine/orchestrator.go` — In `handleWorkflowStarted`:
```go
if wfDef.Timeout > 0 {
    run.Deadline = time.Now().Add(wfDef.Timeout)
}
```

`engine/orchestrator.go` — In `dispatchEvent`, before handler dispatch:
```go
if !run.Deadline.IsZero() && time.Now().After(run.Deadline) {
    return o.handleWorkflowCancelled(ctx, evt) // Reuse cancel logic
}
```

No background timer. Timeout check piggybacks on event processing.

---

## 4. Signal API

**NATS Resources:**
- KV Bucket: `signals`

**Changes:**

`worker/context.go` — Add methods to TaskContext interface and implementation:
```go
type TaskContext interface {
    // ...existing...
    WaitForSignal(name string, timeout time.Duration) ([]byte, error)
    SendSignal(runID, name string, data []byte) error
}
```

`WaitForSignal` implementation:
- Creates KV watcher on key `{runID}.{signalName}`
- Blocks until value appears or timeout expires
- Returns the signal data
- Bounded timeout (max 1 hour)

`SendSignal` implementation:
- Writes to `signals` KV bucket at key `{runID}.{name}`
- Any caller can send (worker, API, external system)

`api/service.go` — Add REST endpoint:
```
POST /runs/{id}/signal/{name}  — body is signal payload
```

---

## 5. Input/Output Schemas

**Changes:**

`dag/types.go` — Add fields to WorkflowDef:
```go
InputSchema  json.RawMessage `json:"input_schema,omitempty"`
OutputSchema json.RawMessage `json:"output_schema,omitempty"`
```

`dag/schema.go` — New file, ~100 LOC in-house JSON Schema validator:
```go
func ValidateSchema(schema json.RawMessage, data json.RawMessage) error
```

Supports subset: `type` (string, number, boolean, object, array), `required`,
`properties` (recursive). No `$ref`, `allOf`, `anyOf`, `oneOf`, `patternProperties`.

`engine/orchestrator.go` — In `handleWorkflowStarted`:
```go
if wfDef.InputSchema != nil {
    if err := dag.ValidateSchema(wfDef.InputSchema, evt.Payload); err != nil {
        return o.failWorkflow(ctx, run, "input validation: "+err.Error())
    }
}
```

Output validation: log warning on mismatch, don't fail (output already produced).

---

## 6. Agent Heartbeats + KV Checkpointing

**NATS Resources:**
- KV Bucket: `checkpoints`

**Changes:**

`worker/worker.go` — Add TaskContext methods:
```go
type TaskContext interface {
    // ...existing...
    Heartbeat() error
    Checkpoint(state []byte) error
    LoadCheckpoint() ([]byte, error)
}
```

`worker/context.go` — Implementation:

`Heartbeat`: Calls `msg.InProgress()` on the underlying NATS message. Extends the
AckWait deadline without acking. Long-running agents call every 30s.

`Checkpoint`: Writes to `checkpoints` KV bucket at key `{runID}.{stepID}`. Overwrites
previous checkpoint.

`LoadCheckpoint`: Reads from `checkpoints` KV. Returns `nil, nil` if no checkpoint
exists (first run). On worker restart after crash, agent loads checkpoint to resume.

`worker/worker.go` — Store the NATS message reference on taskContext so Heartbeat
can call `msg.InProgress()`.

---

## 7. Dead-Letter Queue

**NATS Resources:**
- Stream: `DEAD_LETTERS`, subjects: `dead.>`

**Changes:**

`natsutil/conn.go` — Add DLQ stream to `SetupStreams`:
```go
{
    Name:      "DEAD_LETTERS",
    Subjects:  []string{"dead.>"},
    Retention: nats.LimitsPolicy,
    Storage:   nats.FileStorage,
    MaxAge:    30 * 24 * time.Hour, // 30-day retention
}
```

`engine/orchestrator.go` — In `handleStepFailed`, after permanent failure:
```go
o.publishDeadLetter(run.RunID, stepDef, state)
```

`engine/orchestrator.go` — Add method:
```go
func (o *Orchestrator) publishDeadLetter(
    runID string, stepDef dag.StepDef, state dag.StepState,
) {
    payload, _ := json.Marshal(map[string]interface{}{
        "run_id":   runID,
        "step_id":  stepDef.ID,
        "task":     stepDef.Task,
        "error":    state.Error,
        "attempts": state.Attempts,
    })
    subject := "dead." + stepDef.Task + "." + runID + "." + stepDef.ID
    o.js.Publish(subject, payload)
}
```

CLI: `dagnats dlq list` reads from DEAD_LETTERS stream. `dagnats dlq replay <id>`
republishes to the original task subject.

---

## Testing Strategy

**Unit tests (dag/):**
- Worker group in StepDef JSON round-trip
- OnFailure/Compensate validation (must reference existing steps)
- Workflow timeout + deadline field
- InputSchema/OutputSchema validation (valid + invalid)
- Schema validator: type checks, required fields, nested properties

**Integration tests (engine/, real NATS):**
- Worker group routing: step with group routes to correct subject
- Compensation: step fails → on-failure runs → compensation runs in reverse
- Workflow timeout: start run with 500ms timeout, don't complete, verify cancelled
- DLQ: fail a step permanently, verify message on DEAD_LETTERS stream

**Worker tests (worker/):**
- Heartbeat extends AckWait
- Checkpoint write/read round-trip
- WaitForSignal blocks until signal arrives
- WaitForSignal times out

No shared NATS servers between tests.

---

## Files Summary

| File | Changes |
|------|---------|
| `dag/types.go` | WorkerGroup, OnFailure, Compensate on StepDef; Timeout on WorkflowDef; Deadline on WorkflowRun; InputSchema/OutputSchema on WorkflowDef |
| `dag/validate.go` | Validate OnFailure/Compensate references |
| `dag/schema.go` | New — JSON Schema subset validator (~100 LOC) |
| `engine/orchestrator.go` | Worker group routing, compensation logic, timeout check, DLQ publish |
| `worker/context.go` | Heartbeat, Checkpoint, LoadCheckpoint, WaitForSignal, SendSignal |
| `worker/worker.go` | Store msg ref, WithGroups option |
| `natsutil/conn.go` | DEAD_LETTERS stream |
| `api/service.go` | POST /runs/{id}/signal/{name} |
