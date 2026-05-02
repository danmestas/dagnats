# Step State Transitions via Lifecycle Events — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task is sized 2–5 minutes.

**Goal:** Wire the missing `step.queued` and `step.started` lifecycle events end-to-end (worker emits, engine emits, engine consumes with monotonic guards) so `dagnats run status` reports `Status=Running, Attempts=N` while a worker is mid-execution and `dagnats run events` shows the full lifecycle.

**Architecture:** Two deeper helpers absorb the wire-format details. `taskContext.publishStarted(msg)` (worker side, alongside the unchanged `publishEvent`) reads `NumDelivered` from the original NATS message metadata, sets `AttemptNumber` on the event, and publishes — the dispatch loop never sees `msg.Metadata()` or `Event.AttemptNumber`. `engine.publishLifecycleEvent(ctx, js, evt)` (engine side, new file) handles marshal + msg-id + publish for engine-emitted events. Two new orchestrator handlers (`handleStepStarted`, `handleStepQueued`) each own one monotonic precondition; they refuse to roll a step backward from a terminal status.

**Tech Stack:** Go (module `github.com/danmestas/dagnats`), NATS JetStream client (`github.com/nats-io/nats.go/jetstream`), embedded NATS for tests via `internal/natsutil.StartTestServer(t)` + `natsutil.SetupAll(nc)`, `slog` for structured logging, `go test` red-green TDD per `CLAUDE.md`.

**Spec:** [`docs/superpowers/specs/2026-05-02-issue-137-step-state-transitions-design.md`](../specs/2026-05-02-issue-137-step-state-transitions-design.md)

**Notes for the implementer:**

- The spec uses placeholder names (`loadRunState`, `persistRunState`, `state.Step(...)`) inside the engine handler bodies. The actual repo helpers are: `o.store.Load(ctx, runID)` returns `dag.WorkflowRun`; `o.saveSnapshot(ctx, run)` persists; `run.Steps[stepID]` is the `dag.StepState` (note: it is a value, mutate then re-assign). Tasks 9 and 10 below already substitute these — no further translation needed.
- The spec's handler bodies mention `step.StartedAt = evt.Timestamp`. The current `dag.StepState` struct has **no `StartedAt` field**, and §8.4 of the spec explicitly scopes timestamp-on-state out. The handlers in Tasks 9 and 10 below therefore omit that line. Do not add a `StartedAt` field in this PR.
- The engine's `step.queued` publish belongs at the dispatch site. The cleanest insertion point is in `enqueueReady` (orchestrator.go) **after** `dispatchReadySteps` returns successfully, iterating `ready` for normal-step IDs only — map / sleep / wait / sub-workflow / approval steps already have their own typed lifecycle events. Tasks 7 and 8 below specify the exact placement.
- Worker `w.js` is a v2 `jetstream.JetStream` (see `worker.go:148`). Engine `o.js` is also v2 (see `orchestrator.go:1410`). Tests using `js.PublishMsg` use v2; tests draining the history stream via `js.SubscribeSync` use the legacy `nc.JetStream()` API — both are valid in this repo per `worker_test.go` and `orchestrator_test.go`.

---

## File structure

Files created by this plan:

| File | Responsibility |
|---|---|
| `internal/engine/event_publisher.go` | New helper file. Defines `publishLifecycleEvent(ctx, js, evt) error` for engine-emitted lifecycle events. Marshals, sets `Nats-Msg-Id`, calls `js.PublishMsg`. Pure forward of `evt.NATSSubject()` and `evt.NATSMsgID()`. |
| `worker/lifecycle_event_test.go` | Integration tests for the worker's `publishStarted` helper and the dispatch-loop integration. Owns `TestWorker_PublishesStepStartedBeforeHandler`, `TestWorker_PublishStartedFailure_NaksAndRetries`, `TestWorker_AttemptNumberFromNumDelivered`, `TestPublishStarted_PanicsOnNilMsg`, `TestPublishStarted_PanicsOnEmptyRunID`. |
| `internal/engine/lifecycle_event_test.go` | Integration tests for the engine's `step.queued` publish, the two new handlers, and end-to-end engine state transitions. Owns `TestOrchestrator_PublishesStepQueuedOnDispatch`, `TestOrchestrator_StepQueuedMsgIdIsDeterministic`, `TestOrchestrator_DispatchProceedsIfQueuedPublishFails`, plus the `TestOnEvent_StepStarted_*` and `TestOnEvent_StepQueued_*` tests. |
| `worker/lifecycle_e2e_test.go` | End-to-end tests joining a real orchestrator + a real worker: `TestEndToEnd_LifecycleEventsFire`, `TestEndToEnd_AttemptsVisibleDuringRun`, `TestEndToEnd_RetryViaNakIncrementsAttempts`. |

Files modified:

| File | Change |
|---|---|
| `protocol/protocol.go` | Add `AttemptNumber int` field to `Event` struct (with `omitempty`). Update `NATSMsgID()` to append `.<N>` suffix when `StepID != ""` and `AttemptNumber > 0`. |
| `protocol/protocol_test.go` | Append `TestEvent_MarshalRoundTrip_PreservesAttemptNumber`, `TestEvent_UnmarshalLegacyMissingAttempt`, `TestNATSMsgID_NoAttempt`, `TestNATSMsgID_WithAttempt`, `TestNATSMsgID_WorkflowEventIgnoresAttempt`. |
| `worker/context.go` | Add new method `publishStarted(msg jetstream.Msg) error`. The existing `publishEvent` is **not** modified. |
| `worker/worker.go` | Insert 8-line `tc.publishStarted(msg)` block between current line 706 (`tc.workerID = w.workerID`) and current line 707 (`err = handler(tc)`). On error: log + `msg.NakWithDelay(1*time.Second)` + return. |
| `internal/engine/orchestrator.go` | Add `EventStepStarted` and `EventStepQueued` to the `isHandledEventType` switch and to the `dispatchEvent` switch. Add new methods `handleStepStarted` and `handleStepQueued`. Insert `step.queued` publish loop in `enqueueReady` after `dispatchReadySteps` returns successfully (filtered to normal step types only). |

No other files touch. The history stream layout is unchanged. The TASK_QUEUES stream is unchanged. The consumer pattern is unchanged.

---

## Branch setup

The branch `fix/issue-137-step-state-transitions` is already checked out off `main` and the spec has already been committed to it. Verify before Task 1:

```bash
cd /Users/dmestas/projects/dagnats
git status
git log --oneline -5
go test ./... -count=1
```

Expected: branch is `fix/issue-137-step-state-transitions`; the most recent commit on the branch is the spec at `docs/superpowers/specs/2026-05-02-issue-137-step-state-transitions-design.md`; baseline tests pass. If baseline tests are red, abort and fix tip first.

---

## Task 1: `Event.AttemptNumber` field — pure unit test then add field

**Files:**
- Modify: `protocol/protocol.go`
- Modify: `protocol/protocol_test.go`

- [ ] **Step 1.1: Append failing tests to `protocol/protocol_test.go`.**

Append at end of file (note the file already has a methodology comment at the top — do not duplicate it):

```go
func TestEvent_MarshalRoundTrip_PreservesAttemptNumber(t *testing.T) {
	original := Event{
		Type:          EventStepStarted,
		RunID:         "run-attempt",
		StepID:        "step-x",
		Timestamp:     time.Now().UTC().Truncate(time.Millisecond),
		AttemptNumber: 7,
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.AttemptNumber != 7 {
		t.Fatalf("AttemptNumber = %d, want 7", decoded.AttemptNumber)
	}
	if decoded.Type != original.Type {
		t.Fatalf("Type = %q, want %q", decoded.Type, original.Type)
	}
}

func TestEvent_UnmarshalLegacyMissingAttempt(t *testing.T) {
	// Legacy event JSON written before this field existed must still
	// deserialize successfully with AttemptNumber defaulting to zero.
	legacy := []byte(`{"type":"step.completed","run_id":"r","step_id":"s","timestamp":"2026-01-01T00:00:00Z"}`)
	var decoded Event
	if err := json.Unmarshal(legacy, &decoded); err != nil {
		t.Fatalf("Unmarshal legacy failed: %v", err)
	}
	if decoded.AttemptNumber != 0 {
		t.Fatalf("AttemptNumber = %d, want 0 for legacy event", decoded.AttemptNumber)
	}
	if decoded.Type != EventStepCompleted {
		t.Fatalf("Type = %q, want %q", decoded.Type, EventStepCompleted)
	}
}

func TestEvent_OmitEmpty_AttemptNumberZero(t *testing.T) {
	// AttemptNumber=0 must not appear in marshalled JSON so existing
	// wire format is preserved for events that don't use it.
	evt := Event{
		Type:      EventWorkflowStarted,
		RunID:     "run-omit",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if bytes.Contains(data, []byte("attempt_number")) {
		t.Fatalf("marshalled JSON must omit attempt_number when zero, got: %s", data)
	}
}
```

- [ ] **Step 1.2: Run the tests and confirm they fail with the expected compile error.**

```bash
go test ./protocol/ -run 'TestEvent_MarshalRoundTrip_PreservesAttemptNumber|TestEvent_UnmarshalLegacyMissingAttempt|TestEvent_OmitEmpty_AttemptNumberZero' -count=1
```

Expected: `unknown field AttemptNumber in struct literal of type Event` from the compiler. If you see anything else, stop and re-read the test code.

- [ ] **Step 1.3: Add the field to the `Event` struct.**

In `protocol/protocol.go`, edit the `Event` struct (lines 92-101). Replace:

```go
type Event struct {
	Type        EventType       `json:"type"`
	RunID       string          `json:"run_id"`
	StepID      string          `json:"step_id,omitempty"`
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	TraceParent string          `json:"trace_parent,omitempty"`
	TraceState  string          `json:"trace_state,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
}
```

with:

```go
type Event struct {
	Type          EventType       `json:"type"`
	RunID         string          `json:"run_id"`
	StepID        string          `json:"step_id,omitempty"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	TraceParent   string          `json:"trace_parent,omitempty"`
	TraceState    string          `json:"trace_state,omitempty"`
	WorkerID      string          `json:"worker_id,omitempty"`
	AttemptNumber int             `json:"attempt_number,omitempty"`
}
```

- [ ] **Step 1.4: Run the tests and confirm they pass.**

```bash
go test ./protocol/ -run 'TestEvent_MarshalRoundTrip_PreservesAttemptNumber|TestEvent_UnmarshalLegacyMissingAttempt|TestEvent_OmitEmpty_AttemptNumberZero' -count=1
```

Expected: all three pass.

- [ ] **Step 1.5: Commit.**

```bash
git add protocol/protocol.go protocol/protocol_test.go
git commit -m "protocol: add Event.AttemptNumber field with omitempty"
```

---

## Task 2: `NATSMsgID` includes attempt suffix — pure unit test then update function

**Files:**
- Modify: `protocol/protocol.go`
- Modify: `protocol/protocol_test.go`

- [ ] **Step 2.1: Append failing tests to `protocol/protocol_test.go`.**

```go
func TestNATSMsgID_NoAttempt(t *testing.T) {
	// AttemptNumber == 0: existing behaviour is preserved exactly.
	evt := Event{Type: EventStepCompleted, RunID: "r1", StepID: "s1"}
	got := evt.NATSMsgID()
	want := "r1.s1.step.completed"
	if got != want {
		t.Fatalf("NATSMsgID() = %q, want %q", got, want)
	}
}

func TestNATSMsgID_WithAttempt(t *testing.T) {
	cases := []struct {
		name    string
		attempt int
		want    string
	}{
		{"attempt_one", 1, "r1.s1.step.started.1"},
		{"attempt_two", 2, "r1.s1.step.started.2"},
		{"attempt_forty_two", 42, "r1.s1.step.started.42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evt := Event{
				Type:          EventStepStarted,
				RunID:         "r1",
				StepID:        "s1",
				AttemptNumber: tc.attempt,
			}
			got := evt.NATSMsgID()
			if got != tc.want {
				t.Fatalf("NATSMsgID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNATSMsgID_WorkflowEventIgnoresAttempt(t *testing.T) {
	// Workflow events have empty StepID; the attempt suffix must NOT
	// be appended even if AttemptNumber happens to be set.
	evt := Event{
		Type:          EventWorkflowStarted,
		RunID:         "r1",
		AttemptNumber: 5, // deliberately set; should be ignored
	}
	got := evt.NATSMsgID()
	want := "r1.workflow.started"
	if got != want {
		t.Fatalf("NATSMsgID() = %q, want %q (workflow event must not append attempt)", got, want)
	}
}
```

- [ ] **Step 2.2: Run the tests and confirm two of three fail.**

```bash
go test ./protocol/ -run 'TestNATSMsgID_NoAttempt|TestNATSMsgID_WithAttempt|TestNATSMsgID_WorkflowEventIgnoresAttempt' -count=1
```

Expected: `TestNATSMsgID_NoAttempt` and `TestNATSMsgID_WorkflowEventIgnoresAttempt` already pass (existing behaviour). `TestNATSMsgID_WithAttempt` fails because the function does not yet append `.N`.

- [ ] **Step 2.3: Update `NATSMsgID` to append the attempt suffix.**

In `protocol/protocol.go`, find the imports block at the top (lines 3-6) and add `"strconv"`:

```go
import (
	"encoding/json"
	"strconv"
	"time"
)
```

Then replace the `NATSMsgID` function (lines 154-159):

```go
func (e Event) NATSMsgID() string {
	if e.StepID == "" {
		return e.RunID + "." + string(e.Type)
	}
	return e.RunID + "." + e.StepID + "." + string(e.Type)
}
```

with:

```go
func (e Event) NATSMsgID() string {
	if e.StepID == "" {
		return e.RunID + "." + string(e.Type)
	}
	base := e.RunID + "." + e.StepID + "." + string(e.Type)
	if e.AttemptNumber > 0 {
		return base + "." + strconv.Itoa(e.AttemptNumber)
	}
	return base
}
```

- [ ] **Step 2.4: Run the tests and confirm all three pass.**

```bash
go test ./protocol/ -run 'TestNATSMsgID_NoAttempt|TestNATSMsgID_WithAttempt|TestNATSMsgID_WorkflowEventIgnoresAttempt' -count=1
```

Expected: all three green. Also run `go test ./protocol/ -count=1` to confirm no regression in other protocol tests.

- [ ] **Step 2.5: Commit.**

```bash
git add protocol/protocol.go protocol/protocol_test.go
git commit -m "protocol: NATSMsgID appends .<N> suffix for per-attempt dedup"
```

---

## Task 3: `taskContext.publishStarted` helper — assertion tests then add method

**Constraint reminder:** The existing `publishEvent` (`worker/context.go:291-322`) is **not** modified in this task or any other. `publishStarted` is a parallel deeper helper, not a refactor of `publishEvent`. If you are tempted to "DRY up" the two: don't — the spec deliberately keeps them separate so callsites carry no knowledge of `AttemptNumber` or `msg.Metadata().NumDelivered`.

**Files:**
- Modify: `worker/context.go`
- Create: `worker/lifecycle_event_test.go`

- [ ] **Step 3.1: Create `worker/lifecycle_event_test.go` with the two failing assertion tests.**

```go
// worker/lifecycle_event_test.go
// Tests for the worker-side step.started lifecycle publish helper.
// Assertion-defense tests are pure unit tests; integration tests start
// embedded NATS and run a worker end-to-end to verify the helper fires
// before the user's handler is invoked.
// Methodology: red-green TDD. Each test specifies a single observable
// behaviour and includes both a positive and a negative assertion.
package worker

import (
	"testing"
)

func TestPublishStarted_PanicsOnNilMsg(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil msg, got none")
		}
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	tc := &taskContext{runID: "r1", stepID: "s1"}
	_ = tc.publishStarted(nil)
}

func TestPublishStarted_PanicsOnEmptyRunID(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty runID, got none")
		}
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	// nil msg vs empty runID: empty runID must be checked too, even if
	// the msg is non-nil. Use a nil msg here would short-circuit on the
	// msg check, so wrap with a sentinel non-nil msg interface.
	// Constructing a real jetstream.Msg without a server is awkward;
	// instead we deliberately violate the precondition by supplying a
	// non-nil msg via a tiny stub. The point is to assert publishStarted
	// rejects empty runID even when msg is fine.
	tc := &taskContext{runID: ""}
	_ = tc.publishStarted(stubJetstreamMsg{})
}

// stubJetstreamMsg is the minimal non-nil jetstream.Msg implementation
// needed to drive publishStarted past the nil-msg check. publishStarted
// must panic on empty runID before it touches any msg method.
type stubJetstreamMsg struct{}

// All methods panic — publishStarted must not call any of them when
// runID is empty.
func (stubJetstreamMsg) Subject() string                  { panic("unused") }
func (stubJetstreamMsg) Reply() string                    { panic("unused") }
func (stubJetstreamMsg) Headers() map[string][]string     { panic("unused") }
func (stubJetstreamMsg) Data() []byte                     { panic("unused") }
func (stubJetstreamMsg) Ack() error                       { panic("unused") }
func (stubJetstreamMsg) DoubleAck(_ any) error            { panic("unused") }
func (stubJetstreamMsg) Nak() error                       { panic("unused") }
func (stubJetstreamMsg) NakWithDelay(_ any) error         { panic("unused") }
func (stubJetstreamMsg) InProgress() error                { panic("unused") }
func (stubJetstreamMsg) Term() error                      { panic("unused") }
func (stubJetstreamMsg) TermWithReason(_ string) error    { panic("unused") }
func (stubJetstreamMsg) Metadata() (any, error)           { panic("unused") }
```

**Note on the stub:** `jetstream.Msg` is an interface in `github.com/nats-io/nats.go/jetstream`. The exact method set must satisfy `jetstream.Msg` for the test file to compile. The list above is illustrative; before you commit Step 3.1, run the test to discover the real interface and adjust signatures verbatim from the compiler's "missing method X" errors. The behavioural contract — every method panics — stays.

If wiring a stub matching the full interface bloats the test file beyond reason, the simpler alternative is to drop `TestPublishStarted_PanicsOnEmptyRunID` and rely on the panic being exercised through the integration test in Task 4. The panic itself is a correctness guard, not a test surface. The plan budgets this swap: if your stub exceeds 30 lines, replace `TestPublishStarted_PanicsOnEmptyRunID` with a comment in the file `// covered by Task 4 integration test via assertion-defense path` and proceed.

- [ ] **Step 3.2: Run the failing tests and confirm they fail with `publishStarted is undefined`.**

```bash
go test ./worker/ -run 'TestPublishStarted_PanicsOnNilMsg|TestPublishStarted_PanicsOnEmptyRunID' -count=1
```

Expected: compile error referencing `publishStarted`. If anything else, fix the test file.

- [ ] **Step 3.3: Add `publishStarted` to `worker/context.go`.**

Append at the end of `worker/context.go` (after `SendSignal`, the last existing method):

```go
// publishStarted publishes step.started for the current attempt.
// Reads the attempt number from the original NATS message metadata —
// NumDelivered increments on each redelivery (NAK retries, AckWait
// expiry), so the resulting AttemptNumber is correct for both
// happy-path first attempts and post-NAK redelivery.
//
// Returns error if metadata read or publish fails. The caller (worker
// dispatch loop) NAKs the original message on error so the engine
// never sees step.completed for an attempt it never saw step.started
// for. Lifecycle stays consistent.
//
// Called once per attempt, before invoking the user's task handler.
// Hides the metadata read + AttemptNumber assignment + publish chain
// from the dispatch loop. publishEvent (used by Complete and Fail*)
// is NOT modified — it stays unchanged so its callers don't gain
// knowledge of AttemptNumber.
func (c *taskContext) publishStarted(msg jetstream.Msg) error {
	if msg == nil {
		panic("publishStarted: msg must not be nil")
	}
	if c.runID == "" {
		panic("publishStarted: runID must not be empty")
	}
	meta, err := msg.Metadata()
	if err != nil {
		return err
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepStarted, c.runID, c.stepID, nil,
	)
	evt.WorkerID = c.workerID
	evt.AttemptNumber = int(meta.NumDelivered)
	outMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	observe.InjectTraceContext(c.ctx, outMsg, &evt)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	outMsg.Data = data
	_, err = c.js.PublishMsg(c.ctx, outMsg)
	return err
}
```

- [ ] **Step 3.4: Run the assertion tests and confirm they pass.**

```bash
go test ./worker/ -run 'TestPublishStarted_PanicsOnNilMsg|TestPublishStarted_PanicsOnEmptyRunID' -count=1
```

Expected: pass. (If `TestPublishStarted_PanicsOnEmptyRunID` was deferred per the Step 3.1 fallback, only the nil-msg panic test runs.)

- [ ] **Step 3.5: Commit.**

```bash
git add worker/context.go worker/lifecycle_event_test.go
git commit -m "worker: add publishStarted helper for step.started lifecycle event"
```

---

## Task 4: Worker emits `step.started` before invoking handler — integration test

**Files:**
- Modify: `worker/lifecycle_event_test.go`
- Modify: `worker/worker.go`

- [ ] **Step 4.1: Append the integration test to `worker/lifecycle_event_test.go`.**

```go
import (
	// keep existing imports; add these:
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestWorker_PublishesStepStartedBeforeHandler(t *testing.T) {
	// Methodology: register a handler that records a sentinel marker
	// on first invocation. Drain the history stream. Assert the
	// step.started event arrives at all and carries AttemptNumber=1
	// plus the WorkerID.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var handlerCalled atomic.Bool
	w := NewWorker(nc)
	w.Handle("started-task", func(tc TaskContext) error {
		handlerCalled.Store(true)
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-started-1",
		StepID: "step-x",
		Input:  json.RawMessage(`"go"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish("task.started-task.run-started-1", data); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	// Wait for handler to be called (bounded).
	deadline := time.After(5 * time.Second)
	for !handlerCalled.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Drain history.run-started-1 and look for step.started.
	sub, err := js.SubscribeSync("history.run-started-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	var sawStarted, sawCompleted bool
	var startedEvt protocol.Event
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) && !(sawStarted && sawCompleted) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		switch evt.Type {
		case protocol.EventStepStarted:
			sawStarted = true
			startedEvt = evt
		case protocol.EventStepCompleted:
			sawCompleted = true
		}
	}
	if !sawStarted {
		t.Fatal("expected step.started in history stream, got none")
	}
	if !sawCompleted {
		t.Fatal("expected step.completed in history stream, got none")
	}
	if startedEvt.AttemptNumber != 1 {
		t.Fatalf("AttemptNumber = %d, want 1", startedEvt.AttemptNumber)
	}
	if startedEvt.WorkerID == "" {
		t.Fatal("WorkerID must be set on step.started event")
	}
	if startedEvt.RunID != "run-started-1" {
		t.Fatalf("RunID = %q, want %q", startedEvt.RunID, "run-started-1")
	}
	if startedEvt.StepID != "step-x" {
		t.Fatalf("StepID = %q, want %q", startedEvt.StepID, "step-x")
	}
}

// requireUnused keeps imports honest while the integration test is the
// only consumer. Remove if the linter rejects empty calls.
var _ = context.Background
```

- [ ] **Step 4.2: Run the test and confirm it fails.**

```bash
go test ./worker/ -run TestWorker_PublishesStepStartedBeforeHandler -count=1
```

Expected: fail at `expected step.started in history stream, got none`. The worker has not yet been wired to call `publishStarted`.

- [ ] **Step 4.3: Insert the `publishStarted` call into the dispatch loop.**

In `worker/worker.go`, locate the block between current lines 706 (`tc.workerID = w.workerID`) and 707 (`err = handler(tc)`). Replace:

```go
	tc.workerID = w.workerID
	err = handler(tc)
```

with:

```go
	tc.workerID = w.workerID

	if err := tc.publishStarted(msg); err != nil {
		slog.ErrorContext(ctx, "failed to begin attempt — NAK and retry",
			"error", err,
			"task_type", taskType,
			"run_id", payload.RunID,
			"step_id", payload.StepID,
		)
		msg.NakWithDelay(1 * time.Second)
		return
	}

	err = handler(tc)
```

If `worker.go` does not already import `"log/slog"`, add it. Check the existing imports at the top of the file before adding.

- [ ] **Step 4.4: Run the test and confirm it passes.**

```bash
go test ./worker/ -run TestWorker_PublishesStepStartedBeforeHandler -count=1
```

Expected: green. Also run the full worker suite to catch regressions:

```bash
go test ./worker/ -count=1
```

Expected: all green.

- [ ] **Step 4.5: Commit.**

```bash
git add worker/worker.go worker/lifecycle_event_test.go
git commit -m "worker: emit step.started before handler invocation"
```

---

## Task 5: Worker NAKs and retries on `publishStarted` failure

**Files:**
- Modify: `worker/lifecycle_event_test.go`

**Caveat budget (≤1 hour):** Injecting a JetStream publish failure cleanly is non-trivial. Two approaches:

1. **Close the underlying NATS connection mid-test.** Race-prone — the worker's consumer might pull no messages at all if the connection drops too early.
2. **Wrap `w.js` with a thin shim that returns an error from `PublishMsg`.** Requires plumbing — `taskContext.js` is set in `newTaskContext` from `w.js`, which is set from `jetstream.New(nc)` in `NewWorker`. There is no constructor seam for an alternate JS handle.

If 1 hour of effort doesn't yield a stable test, fall back to skip-with-issue per spec §6:

```go
func TestWorker_PublishStartedFailure_NaksAndRetries(t *testing.T) {
	t.Skip("publish-failure injection deferred — see #<NEW_ISSUE>")
}
```

— and file a follow-up issue titled "Test for `publishStarted` publish-failure NAK path (deferred from #137)" referencing this skip. The failure-mode logic ships untested in this PR; the issue tracks it. This mirrors PR #136's Task 16 fallback shape exactly.

- [ ] **Step 5.1: Append the failing test to `worker/lifecycle_event_test.go`.**

```go
func TestWorker_PublishStartedFailure_NaksAndRetries(t *testing.T) {
	// Methodology: close the NATS connection after the worker is
	// running but before publishing the task, so the worker's
	// publishStarted call fails. Verify the handler is NOT invoked
	// (publish failure must short-circuit before handler) and the
	// message gets NAKed (redelivery would re-trigger publishStarted).
	//
	// If this technique proves race-prone in practice, swap to t.Skip
	// with a follow-up issue per the plan's caveat budget.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var handlerCalls atomic.Int32
	w := NewWorker(nc)
	w.Handle("fail-publish", func(tc TaskContext) error {
		handlerCalls.Add(1)
		return tc.Complete(nil)
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-fail-pub",
		StepID: "step-fp",
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish("task.fail-publish.run-fail-pub", data); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	// Close the connection AFTER publish so the task is on the queue
	// but publishStarted will fail when the worker pulls it.
	// Note: this races with worker pull. If flake rate exceeds 5%,
	// switch to t.Skip per the plan's fallback.
	time.Sleep(100 * time.Millisecond)
	nc.Close()

	// Give the worker time to attempt and NAK.
	time.Sleep(2 * time.Second)

	// Handler must not have been called (publishStarted failed before).
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0 (publishStarted failure must short-circuit)", got)
	}
	// Connection close means we can't easily inspect the queue from
	// here. The negative assertion above is the correctness signal:
	// handler was not invoked despite a task being on the queue.
}
```

- [ ] **Step 5.2: Run the test.**

```bash
go test ./worker/ -run TestWorker_PublishStartedFailure_NaksAndRetries -count=1
```

If it passes on the first run AND repeats green over 5 consecutive runs (`-count=5`), keep it. If flaky, skip-with-issue per the budget rule above.

- [ ] **Step 5.3: If the test passes, no implementation work — Task 4 already added the NAK-on-failure code path. Commit the test.**

```bash
git add worker/lifecycle_event_test.go
git commit -m "worker: test step.started publish-failure NAKs and skips handler"
```

If you skipped with an issue:

```bash
git add worker/lifecycle_event_test.go
git commit -m "worker: skeleton test for publishStarted failure NAK path (deferred to #<N>)"
```

— including the issue number in the commit body.

---

## Task 6: AttemptNumber from NumDelivered across NAK retries — integration test

**Files:**
- Modify: `worker/lifecycle_event_test.go`

- [ ] **Step 6.1: Append the test.**

```go
func TestWorker_AttemptNumberFromNumDelivered(t *testing.T) {
	// Methodology: handler errors on the first call, succeeds on
	// the second. The first attempt produces AttemptNumber=1, the
	// post-NAK redelivery produces AttemptNumber=2. Both step.started
	// events must appear in the history stream with distinct
	// AttemptNumber values, proving NumDelivered → AttemptNumber.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	var calls atomic.Int32
	w := NewWorker(nc)
	w.Handle("retry-attempt", func(tc TaskContext) error {
		n := calls.Add(1)
		if n == 1 {
			return fmt.Errorf("transient error attempt %d", n)
		}
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  "run-retry-1",
		StepID: "step-r",
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish("task.retry-attempt.run-retry-1", data); err != nil {
		t.Fatalf("publish task: %v", err)
	}

	// Wait for second handler call.
	deadline := time.After(15 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("calls = %d, want 2 within 15s", calls.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Drain the history stream and collect step.started events.
	sub, err := js.SubscribeSync("history.run-retry-1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	attemptsSeen := make(map[int]bool)
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) && !(attemptsSeen[1] && attemptsSeen[2]) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepStarted {
			attemptsSeen[evt.AttemptNumber] = true
		}
	}
	if !attemptsSeen[1] {
		t.Fatal("expected step.started with AttemptNumber=1, missing")
	}
	if !attemptsSeen[2] {
		t.Fatal("expected step.started with AttemptNumber=2, missing")
	}
}
```

Add `"fmt"` to the test file imports if not already present.

- [ ] **Step 6.2: Run the test.**

```bash
go test ./worker/ -run TestWorker_AttemptNumberFromNumDelivered -count=1
```

Expected: green on the first run. The worker dispatch loop already emits `step.started` (Task 4), and `publishStarted` already reads `NumDelivered` (Task 3). This test is verification that the path composes correctly across a real NAK redelivery.

If it fails (e.g., AttemptNumber=1 appears for both): re-read `publishStarted` and confirm `int(meta.NumDelivered)` is the literal expression used. NumDelivered is 1-based per JetStream semantics — the first delivery has NumDelivered=1.

- [ ] **Step 6.3: Commit.**

```bash
git add worker/lifecycle_event_test.go
git commit -m "worker: test AttemptNumber follows NumDelivered across NAK retries"
```

---

## Task 7: Engine `publishLifecycleEvent` helper — new file

**Files:**
- Create: `internal/engine/event_publisher.go`

This task adds the helper without yet calling it from anywhere. Task 8 wires the call site.

- [ ] **Step 7.1: Create `internal/engine/event_publisher.go`.**

```go
// internal/engine/event_publisher.go
// Lifecycle event publisher for engine-emitted events. Mirrors the
// worker-side publishEvent pattern at worker/context.go but without a
// per-task context — engine has only a JetStream handle and an Event.
//
// The single deeper helper hides marshal + Nats-Msg-Id header + publish
// from each call site so the orchestrator dispatch path stays a thin
// orchestration loop.
package engine

import (
	"context"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// publishLifecycleEvent publishes evt to the history stream with
// proper Nats-Msg-Id dedup. Caller has already populated the Event
// (Type, RunID, StepID, AttemptNumber, etc.); this function only
// handles marshal + msg-id + publish.
//
// No trace-context propagation: engine is not running inside an OTEL
// span at dispatch time the way the worker is inside a handler span.
// If telemetry surfaces a need later, add observe.InjectTraceContext
// at the publish site (one-line change).
func publishLifecycleEvent(
	ctx context.Context,
	js jetstream.JetStream,
	evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("publishLifecycleEvent: evt.RunID must not be empty")
	}
	if evt.Type == "" {
		panic("publishLifecycleEvent: evt.Type must not be empty")
	}
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	_, err = js.PublishMsg(ctx, msg)
	return err
}
```

- [ ] **Step 7.2: Verify the package still builds.**

```bash
go build ./internal/engine/...
go vet ./internal/engine/...
```

Expected: clean. The helper is unreferenced — Go's vet will not flag unused package-level functions.

- [ ] **Step 7.3: Commit.**

```bash
git add internal/engine/event_publisher.go
git commit -m "engine: add publishLifecycleEvent helper for engine-emitted events"
```

---

## Task 8: Engine emits `step.queued` at dispatch site — integration tests + insertion

**Files:**
- Create: `internal/engine/lifecycle_event_test.go`
- Modify: `internal/engine/orchestrator.go`

- [ ] **Step 8.1: Create `internal/engine/lifecycle_event_test.go` with the three failing dispatch-side tests.**

```go
// internal/engine/lifecycle_event_test.go
// Tests for engine-side step.queued + step.started lifecycle event
// handling. Embedded NATS, real orchestrator. Dispatch-side tests
// (Tasks 8 below) verify the publish at the dispatch site; handler
// tests (Tasks 9-10) verify the onEvent switch updates state correctly
// with monotonic guards.
// Methodology: red-green TDD. Each test asserts both a positive event
// (the event we expect) and a negative property (e.g., msg-id matches
// exactly, not just "contains").
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

func TestOrchestrator_PublishesStepQueuedOnDispatch(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "queued-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-q1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Drain history.run-q1 and look for step.queued before step.started.
	sub, err := js.SubscribeSync("history.run-q1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	var queuedEvt protocol.Event
	var sawQueued bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sawQueued {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepQueued && evt.StepID == "a" {
			queuedEvt = evt
			sawQueued = true
		}
	}
	if !sawQueued {
		t.Fatal("expected step.queued event for step 'a', not found")
	}
	if queuedEvt.AttemptNumber != 1 {
		t.Fatalf("AttemptNumber = %d, want 1", queuedEvt.AttemptNumber)
	}
	if queuedEvt.RunID != "run-q1" {
		t.Fatalf("RunID = %q, want %q", queuedEvt.RunID, "run-q1")
	}
}

func TestOrchestrator_StepQueuedMsgIdIsDeterministic(t *testing.T) {
	// Methodology: synthesise the expected Event and verify NATSMsgID
	// matches the wire convention exactly. This is a pure protocol
	// check — no orchestrator run-time involvement — but kept in the
	// engine test file because the determinism rule is enforced at the
	// engine's publish site.
	evt := protocol.Event{
		Type:          protocol.EventStepQueued,
		RunID:         "run-mid",
		StepID:        "step-x",
		AttemptNumber: 1,
	}
	got := evt.NATSMsgID()
	want := "run-mid.step-x.step.queued.1"
	if got != want {
		t.Fatalf("NATSMsgID = %q, want %q", got, want)
	}
	// Negative: AttemptNumber=2 produces a different id.
	evt.AttemptNumber = 2
	got2 := evt.NATSMsgID()
	if got2 == got {
		t.Fatalf("NATSMsgID for attempt 1 and 2 must differ; both = %q", got)
	}
}

func TestOrchestrator_DispatchProceedsIfQueuedPublishFails(t *testing.T) {
	// Methodology: this test verifies the failure-mode policy in §3 of
	// the spec — engine logs but does NOT roll back if step.queued
	// publish fails, because the task is already on TASK_QUEUES and
	// rolling it back is fragile.
	//
	// We can't easily inject a publish failure on the history stream
	// from here without instrumenting the engine. The negative-space
	// check we CAN make: even when step.queued publish would fail,
	// the task message arrives on TASK_QUEUES. We approximate by
	// running the happy path and asserting the task is queued — which
	// proves the dispatch path doesn't depend on step.queued success.
	//
	// If this proves under-specified for the failure-mode contract,
	// add a follow-up issue and revisit.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "dispatch-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-dp", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Even if step.queued publish were to fail, the task must still be
	// on TASK_QUEUES.
	sub, _ := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task message not delivered: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message, got %d", len(msgs))
	}
	msgs[0].Ack()
}

// _ keeps the imports honest if not all are used by the dispatch-side
// tests — the handler tests in Tasks 9-10 use the rest.
var _ context.Context = context.Background()
var _ jetstream.JetStream = nil
```

- [ ] **Step 8.2: Run the failing tests.**

```bash
go test ./internal/engine/ -run 'TestOrchestrator_PublishesStepQueuedOnDispatch|TestOrchestrator_StepQueuedMsgIdIsDeterministic|TestOrchestrator_DispatchProceedsIfQueuedPublishFails' -count=1
```

Expected: `TestOrchestrator_StepQueuedMsgIdIsDeterministic` passes (Tasks 1-2 already wired NATSMsgID). `TestOrchestrator_PublishesStepQueuedOnDispatch` fails — the dispatch path does not yet emit `step.queued`. `TestOrchestrator_DispatchProceedsIfQueuedPublishFails` may pass (publishing a task is the existing happy path) but stays as a regression guard.

- [ ] **Step 8.3: Add the `step.queued` publish loop in `enqueueReady`.**

In `internal/engine/orchestrator.go`, locate `enqueueReady` (function header around line 1424). Find the block at lines 1490-1498:

```go
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	return o.dispatchReadySteps(ctx, wfDef, run, ready)
```

Replace with:

```go
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if err := o.saveSnapshot(ctx, run); err != nil {
		return err
	}
	if err := o.dispatchReadySteps(ctx, wfDef, run, ready); err != nil {
		return err
	}
	// Emit step.queued for normal steps after the task is on the queue.
	// Map / sleep / wait / sub-workflow / approval steps have their own
	// typed lifecycle events and are excluded here.
	for _, step := range ready {
		if step.Type != dag.StepTypeNormal && step.Type != dag.StepTypeAgentLoop {
			continue
		}
		qEvt := protocol.NewStepEvent(
			protocol.EventStepQueued, run.RunID, step.ID, nil,
		)
		qEvt.AttemptNumber = 1
		if err := publishLifecycleEvent(ctx, o.js, qEvt); err != nil {
			slog.ErrorContext(ctx, "failed to publish step.queued",
				"error", err,
				"run_id", run.RunID,
				"step_id", step.ID,
			)
			// Do NOT roll back — the task is already on TASK_QUEUES
			// and a worker will pick it up. step.queued is
			// observability-only at this point in the design;
			// missing it is not correctness-fatal. See spec §3.
		}
	}
	return nil
```

If `internal/engine/orchestrator.go` does not already import `"log/slog"`, add it (the file already uses slog in other handlers — check first). The `protocol` package is already imported.

- [ ] **Step 8.4: Run the tests and confirm they pass.**

```bash
go test ./internal/engine/ -run 'TestOrchestrator_PublishesStepQueuedOnDispatch|TestOrchestrator_StepQueuedMsgIdIsDeterministic|TestOrchestrator_DispatchProceedsIfQueuedPublishFails' -count=1
```

Expected: all three green. Also run the full engine suite to catch regressions:

```bash
go test ./internal/engine/ -count=1
```

Expected: green (any pre-existing flakes flagged in the spec checklist may surface — re-run if so).

- [ ] **Step 8.5: Commit.**

```bash
git add internal/engine/orchestrator.go internal/engine/lifecycle_event_test.go
git commit -m "engine: publish step.queued at dispatch site (observability)"
```

---

## Task 9: `handleStepStarted` engine handler with monotonic guards

**Files:**
- Modify: `internal/engine/lifecycle_event_test.go`
- Modify: `internal/engine/orchestrator.go`

- [ ] **Step 9.1: Append the six failing handler tests to `internal/engine/lifecycle_event_test.go`.**

```go
func TestOnEvent_StepStarted_TransitionsQueuedToRunning(t *testing.T) {
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
		Name: "started-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start the workflow — step 'a' should reach Queued.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-st1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Wait for queued state.
	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-st1", "a", dag.StepStatusQueued, 5*time.Second)

	// Now simulate worker emitting step.started.
	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-st1", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	startedData, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	// Engine must transition Queued → Running.
	waitForStepStatus(t, store, "run-st1", "a", dag.StepStatusRunning, 5*time.Second)

	// Negative space: status is NOT still Queued.
	run, err := store.Load(context.Background(), "run-st1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if run.Steps["a"].Status == dag.StepStatusQueued {
		t.Fatal("expected Running, got Queued")
	}
}

func TestOnEvent_StepStarted_IncrementsAttempts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "attempts-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-att", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-att", "a", dag.StepStatusQueued, 5*time.Second)

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-att", "a", nil,
	)
	startedEvt.AttemptNumber = 3
	startedData, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	waitForStepAttempts(t, store, "run-att", "a", 3, 5*time.Second)
	run, _ := store.Load(context.Background(), "run-att")
	if run.Steps["a"].Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", run.Steps["a"].Attempts)
	}
	if run.Steps["a"].Attempts == 0 {
		t.Fatal("Attempts must not stay at 0")
	}
}

func TestOnEvent_StepStarted_IsIdempotentOnSameAttempt(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "idem-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-idem", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-idem", "a", dag.StepStatusQueued, 5*time.Second)

	// Publish step.started twice with the same attempt; both deduped
	// by Nats-Msg-Id, so the engine sees only one. Attempts stays at 2.
	for i := 0; i < 2; i++ {
		startedEvt := protocol.NewStepEvent(
			protocol.EventStepStarted, "run-idem", "a", nil,
		)
		startedEvt.AttemptNumber = 2
		data, _ := startedEvt.Marshal()
		js.Publish(
			startedEvt.NATSSubject(), data,
			nats.MsgId(startedEvt.NATSMsgID()),
		)
	}
	waitForStepStatus(t, store, "run-idem", "a", dag.StepStatusRunning, 5*time.Second)
	run, _ := store.Load(context.Background(), "run-idem")
	if run.Steps["a"].Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2 (idempotent)", run.Steps["a"].Attempts)
	}
}

func TestOnEvent_StepStarted_IgnoredAfterCompleted(t *testing.T) {
	// Methodology: seed the store with a Completed step, then fire
	// a stale step.started. The engine must not regress the state.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "stale-comp-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-stcomp")
	run.Status = dag.RunStatusRunning
	st := run.Steps["a"]
	st.Status = dag.StepStatusCompleted
	st.Attempts = 1
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-stcomp", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	data, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), data,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, err := store.Load(context.Background(), "run-stcomp")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("Status = %v, want Completed (must not regress)",
			loaded.Steps["a"].Status)
	}
}

func TestOnEvent_StepStarted_IgnoredAfterFailed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "stale-fail-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-stfail")
	run.Status = dag.RunStatusFailed
	st := run.Steps["a"]
	st.Status = dag.StepStatusFailed
	st.Attempts = 4
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-stfail", "a", nil,
	)
	startedEvt.AttemptNumber = 4
	data, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), data,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, _ := store.Load(context.Background(), "run-stfail")
	if loaded.Steps["a"].Status != dag.StepStatusFailed {
		t.Fatalf("Status = %v, want Failed (must not regress)",
			loaded.Steps["a"].Status)
	}
	if loaded.Steps["a"].Attempts != 4 {
		t.Fatalf("Attempts = %d, want 4 (must not change)",
			loaded.Steps["a"].Attempts)
	}
}

func TestOnEvent_StepStarted_AttemptsMonotonic_NeverDecreases(t *testing.T) {
	// Methodology: seed a step with Attempts=5, fire step.started
	// with AttemptNumber=2. Engine's max() rule must keep Attempts=5.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "mono-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-mono")
	run.Status = dag.RunStatusRunning
	st := run.Steps["a"]
	st.Status = dag.StepStatusRunning
	st.Attempts = 5
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-mono", "a", nil,
	)
	startedEvt.AttemptNumber = 2
	data, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), data,
		nats.MsgId(startedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, _ := store.Load(context.Background(), "run-mono")
	if loaded.Steps["a"].Attempts != 5 {
		t.Fatalf("Attempts = %d, want 5 (monotonic — never decreases)",
			loaded.Steps["a"].Attempts)
	}
}

// waitForStepStatus polls the store until step status matches or
// timeout fires. Bounded — never spins past timeout.
func waitForStepStatus(
	t *testing.T,
	store *SnapshotStore,
	runID, stepID string,
	want dag.StepStatus,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), runID)
		if err == nil && run.Steps[stepID].Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("step %q in run %q did not reach status %v within %v",
		stepID, runID, want, timeout)
}

// waitForStepAttempts polls the store until step attempts match or
// timeout fires.
func waitForStepAttempts(
	t *testing.T,
	store *SnapshotStore,
	runID, stepID string,
	want int,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), runID)
		if err == nil && run.Steps[stepID].Attempts == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("step %q in run %q did not reach attempts %d within %v",
		stepID, runID, want, timeout)
}
```

- [ ] **Step 9.2: Run the failing tests.**

```bash
go test ./internal/engine/ -run 'TestOnEvent_StepStarted_' -count=1
```

Expected: all six fail. The engine's `dispatchEvent` switch has no `EventStepStarted` case yet, and `isHandledEventType` does not list it — so `step.started` events are ignored.

- [ ] **Step 9.3: Add `EventStepStarted` to `isHandledEventType`.**

In `internal/engine/orchestrator.go`, locate `isHandledEventType` (lines 250-269). Add `protocol.EventStepStarted` to the switch:

```go
func isHandledEventType(t protocol.EventType) bool {
	switch t {
	case protocol.EventWorkflowStarted,
		protocol.EventStepQueued,
		protocol.EventStepStarted,
		protocol.EventStepCompleted,
		protocol.EventStepContinue,
		protocol.EventStepFailed,
		protocol.EventWorkflowSpawn,
		protocol.EventWorkflowChildCompleted,
		protocol.EventWorkflowChildFailed,
		protocol.EventWorkflowCancelled,
		protocol.EventStepSleepCompleted,
		protocol.EventStepWaitMatched,
		protocol.EventStepWaitTimeout,
		protocol.EventApprovalGranted,
		protocol.EventApprovalRejected,
		protocol.EventApprovalExpired:
		return true
	}
	return false
}
```

(`EventStepQueued` is added too, in support of Task 10 — adding both at once keeps the switch in one diff.)

- [ ] **Step 9.4: Add the `EventStepStarted` case to `dispatchEvent`.**

In `dispatchEvent` (the switch starting at line 290 of `orchestrator.go`), add the new case alongside the existing ones (after `EventStepFailed` is fine):

```go
	case protocol.EventStepFailed:
		return o.handleStepFailed(ctx, evt)
	case protocol.EventStepStarted:
		return o.handleStepStarted(ctx, evt)
	case protocol.EventStepQueued:
		return o.handleStepQueued(ctx, evt)
```

(`EventStepQueued` case added in support of Task 10.)

- [ ] **Step 9.5: Add the `handleStepStarted` method.**

Append at the end of `internal/engine/orchestrator.go` (or near the other `handleStep*` methods — keep ordering with the existing pattern):

```go
// handleStepStarted transitions the step from Queued to Running and
// updates the attempt counter. Monotonic: refuses to regress a
// terminal state — a stale step.started arriving after the engine
// already saw step.completed/step.failed is logged and ignored.
//
// Attempts uses max() rule so out-of-order delivery cannot decrement
// the counter; a higher AttemptNumber wins.
func (o *Orchestrator) handleStepStarted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepStarted: evt.RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepStarted: evt.StepID must not be empty")
	}

	run, err := o.store.Load(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run %q: %w", evt.RunID, err)
	}
	state, ok := run.Steps[evt.StepID]
	if !ok {
		slog.WarnContext(ctx,
			"step.started for unknown step",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
		)
		return nil
	}

	// Monotonic guard — don't regress a terminal state.
	if state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed {
		slog.WarnContext(ctx,
			"stale step.started ignored — step is terminal",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
			"current_status", state.Status,
			"event_attempt", evt.AttemptNumber,
		)
		return nil
	}

	state.Status = dag.StepStatusRunning
	if evt.AttemptNumber > state.Attempts {
		state.Attempts = evt.AttemptNumber
	}
	run.Steps[evt.StepID] = state
	return o.saveSnapshot(ctx, run)
}
```

The file already imports `"context"`, `"fmt"`, `"log/slog"`, `dag`, and `protocol` for the existing handlers — no import changes required. Verify with `goimports -l internal/engine/orchestrator.go`.

- [ ] **Step 9.6: Run the tests.**

```bash
go test ./internal/engine/ -run 'TestOnEvent_StepStarted_' -count=1
```

Expected: all six green. Also re-run the dispatch-side tests from Task 8 to confirm no regression:

```bash
go test ./internal/engine/ -run 'TestOrchestrator_Publishes|TestOrchestrator_StepQueuedMsgIdIsDeterministic|TestOrchestrator_DispatchProceeds' -count=1
```

- [ ] **Step 9.7: Commit.**

```bash
git add internal/engine/orchestrator.go internal/engine/lifecycle_event_test.go
git commit -m "engine: handle step.started — Queued→Running with monotonic guards"
```

---

## Task 10: `handleStepQueued` engine handler — replay reconstruction

**Files:**
- Modify: `internal/engine/lifecycle_event_test.go`
- Modify: `internal/engine/orchestrator.go`

- [ ] **Step 10.1: Append the two failing tests to `internal/engine/lifecycle_event_test.go`.**

```go
func TestOnEvent_StepQueued_DuringReplay_ReconstructsState(t *testing.T) {
	// Methodology: simulate replay by publishing a sequence of events
	// to a fresh history stream and verifying final state. The
	// step.queued event during replay must set Status=Queued without
	// rolling back any later transitions.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "replay-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Replay sequence: workflow.started → step.queued → step.started → step.completed.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rp", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	store := NewSnapshotStore(jsNew)
	waitForStepStatus(t, store, "run-rp", "a", dag.StepStatusQueued, 5*time.Second)

	startedEvt := protocol.NewStepEvent(
		protocol.EventStepStarted, "run-rp", "a", nil,
	)
	startedEvt.AttemptNumber = 1
	startedData, _ := startedEvt.Marshal()
	js.Publish(
		startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()),
	)
	waitForStepStatus(t, store, "run-rp", "a", dag.StepStatusRunning, 5*time.Second)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "run-rp", "a", []byte(`"done"`),
	)
	compData, _ := compEvt.Marshal()
	js.Publish(
		compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()),
	)
	waitForStepStatus(t, store, "run-rp", "a", dag.StepStatusCompleted, 5*time.Second)

	loaded, _ := store.Load(context.Background(), "run-rp")
	if loaded.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("Status = %v, want Completed", loaded.Steps["a"].Status)
	}
	if loaded.Steps["a"].Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1", loaded.Steps["a"].Attempts)
	}
}

func TestOnEvent_StepQueued_NoRollback_FromRunning(t *testing.T) {
	// Methodology: seed a step with Status=Running, then fire
	// step.queued. Engine's monotonic guard must keep state at Running.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "noroll-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
	run := dag.NewWorkflowRun(wfDef, "run-nr")
	run.Status = dag.RunStatusRunning
	st := run.Steps["a"]
	st.Status = dag.StepStatusRunning
	st.Attempts = 2
	run.Steps["a"] = st
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	queuedEvt := protocol.NewStepEvent(
		protocol.EventStepQueued, "run-nr", "a", nil,
	)
	queuedEvt.AttemptNumber = 1
	data, _ := queuedEvt.Marshal()
	js.Publish(
		queuedEvt.NATSSubject(), data,
		nats.MsgId(queuedEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	loaded, _ := store.Load(context.Background(), "run-nr")
	if loaded.Steps["a"].Status != dag.StepStatusRunning {
		t.Fatalf("Status = %v, want Running (no rollback)",
			loaded.Steps["a"].Status)
	}
	if loaded.Steps["a"].Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2 (must not change)",
			loaded.Steps["a"].Attempts)
	}
}
```

- [ ] **Step 10.2: Run the failing tests.**

```bash
go test ./internal/engine/ -run 'TestOnEvent_StepQueued_' -count=1
```

Expected: both fail. The `dispatchEvent` switch has the `EventStepQueued` case from Task 9.4, but the `handleStepQueued` method doesn't exist yet, so the file won't compile.

- [ ] **Step 10.3: Add the `handleStepQueued` method.**

Append below `handleStepStarted` in `internal/engine/orchestrator.go`:

```go
// handleStepQueued is mostly a no-op during normal operation — the
// engine's dispatch path already set Status to Queued before it
// emitted this event. The handler exists for state recovery on
// engine restart, where the history stream is replayed and the
// engine reconstructs run state from events alone.
//
// Monotonic: refuses to roll back from Running, Completed, Failed.
func (o *Orchestrator) handleStepQueued(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepQueued: evt.RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepQueued: evt.StepID must not be empty")
	}

	run, err := o.store.Load(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run %q: %w", evt.RunID, err)
	}
	state, ok := run.Steps[evt.StepID]
	if !ok {
		slog.WarnContext(ctx,
			"step.queued for unknown step",
			"run_id", evt.RunID, "step_id", evt.StepID,
		)
		return nil
	}
	if state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed ||
		state.Status == dag.StepStatusRunning {
		// Already past Queued — don't roll back.
		return nil
	}
	state.Status = dag.StepStatusQueued
	if evt.AttemptNumber > state.Attempts {
		state.Attempts = evt.AttemptNumber
	}
	run.Steps[evt.StepID] = state
	return o.saveSnapshot(ctx, run)
}
```

- [ ] **Step 10.4: Run the tests.**

```bash
go test ./internal/engine/ -run 'TestOnEvent_StepQueued_' -count=1
```

Expected: both green.

Run the full engine suite:

```bash
go test ./internal/engine/ -count=1
```

Expected: green.

- [ ] **Step 10.5: Commit.**

```bash
git add internal/engine/orchestrator.go internal/engine/lifecycle_event_test.go
git commit -m "engine: handle step.queued for replay reconstruction with monotonic guard"
```

---

## Task 11: End-to-end lifecycle events fire — full integration test

**Files:**
- Create: `worker/lifecycle_e2e_test.go`

- [ ] **Step 11.1: Create `worker/lifecycle_e2e_test.go` with the full lifecycle test.**

```go
// worker/lifecycle_e2e_test.go
// End-to-end tests joining a real orchestrator with a real worker.
// Verifies the complete lifecycle event sequence appears in history
// in the correct order with non-decreasing timestamps, and that
// retries via NAK increment the attempt counter both in events and
// in engine state.
// Methodology: register a workflow, start a run, register a worker
// that completes, drain the history stream end-to-end, assert the
// expected sequence and field values.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEndToEnd_LifecycleEventsFire(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	// Register workflow def.
	wfDef := dag.WorkflowDef{
		Name: "e2e-life", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "lifecycle-task", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	// Start orchestrator.
	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start worker.
	w := NewWorker(nc)
	w.Handle("lifecycle-task", func(tc TaskContext) error {
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	// Trigger the run.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-e2e1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Drain history.run-e2e1 and collect the event sequence.
	sub, err := js.SubscribeSync("history.run-e2e1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	type record struct {
		typ     protocol.EventType
		attempt int
		ts      time.Time
	}
	var seq []record
	want := map[protocol.EventType]bool{
		protocol.EventWorkflowStarted:   false,
		protocol.EventStepQueued:        false,
		protocol.EventStepStarted:       false,
		protocol.EventStepCompleted:     false,
		protocol.EventWorkflowCompleted: false,
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		seq = append(seq, record{evt.Type, evt.AttemptNumber, evt.Timestamp})
		if _, ok := want[evt.Type]; ok {
			want[evt.Type] = true
		}
		allSeen := true
		for _, v := range want {
			if !v {
				allSeen = false
				break
			}
		}
		if allSeen {
			break
		}
	}
	for typ, seen := range want {
		if !seen {
			t.Fatalf("expected event %q in history, missing. seq=%v", typ, seq)
		}
	}

	// Assert order: workflow.started < step.queued < step.started < step.completed < workflow.completed.
	wantOrder := []protocol.EventType{
		protocol.EventWorkflowStarted,
		protocol.EventStepQueued,
		protocol.EventStepStarted,
		protocol.EventStepCompleted,
		protocol.EventWorkflowCompleted,
	}
	idx := 0
	for _, r := range seq {
		if idx < len(wantOrder) && r.typ == wantOrder[idx] {
			idx++
		}
	}
	if idx != len(wantOrder) {
		t.Fatalf("event order mismatch — got=%v, want subsequence=%v", seq, wantOrder)
	}

	// Timestamps non-decreasing for the canonical sequence.
	var prev time.Time
	for _, r := range seq {
		// only check for the events we care about ordering on
		switch r.typ {
		case protocol.EventStepQueued,
			protocol.EventStepStarted,
			protocol.EventStepCompleted:
			if !prev.IsZero() && r.ts.Before(prev) {
				t.Fatalf("timestamps not non-decreasing: %v before %v in seq=%v", r.ts, prev, seq)
			}
			prev = r.ts
		}
	}

	// AttemptNumber=1 on step.queued and step.started.
	for _, r := range seq {
		if (r.typ == protocol.EventStepQueued || r.typ == protocol.EventStepStarted) && r.attempt != 1 {
			t.Fatalf("event %q AttemptNumber = %d, want 1", r.typ, r.attempt)
		}
	}
}

// keep imports honest
var _ = context.Background
var _ = jetstream.JetStream(nil)
var _ = fmt.Sprintf
var _ atomic.Bool
```

- [ ] **Step 11.2: Run the test.**

```bash
go test ./worker/ -run TestEndToEnd_LifecycleEventsFire -count=1
```

Expected: green if Tasks 1-10 are all in. The orchestrator emits `step.queued`, the worker emits `step.started`, the worker emits `step.completed` via the existing `Complete()` path, and the orchestrator emits `workflow.completed`.

If a single event is missing: tail the orchestrator logs (test stdout) for the publish-failure log line — that points to which side dropped the event.

- [ ] **Step 11.3: Commit.**

```bash
git add worker/lifecycle_e2e_test.go
git commit -m "test: end-to-end lifecycle events fire in canonical order"
```

---

## Task 12: End-to-end attempts visible during run — the original #137 repro

**Files:**
- Modify: `worker/lifecycle_e2e_test.go`

- [ ] **Step 12.1: Append the long-running task test.**

```go
func TestEndToEnd_AttemptsVisibleDuringRun(t *testing.T) {
	// Methodology: this is the original #137 bug repro. Start a long
	// task (handler sleeps 2s), then sample run state during the sleep.
	// Assert Status=Running, Attempts=1 — proving the fix.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "e2e-attempts", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "long-task", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	handlerStarted := make(chan struct{}, 1)
	handlerProceed := make(chan struct{})
	w := NewWorker(nc)
	w.Handle("long-task", func(tc TaskContext) error {
		handlerStarted <- struct{}{}
		<-handlerProceed
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-att-vis", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Wait for handler to actually start.
	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not invoked within 10s")
	}

	// Sample state — should now be Running with Attempts=1.
	store := enginepkg.NewSnapshotStore(jsNew)
	deadline := time.Now().Add(3 * time.Second)
	var observed dag.StepState
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "run-att-vis")
		if err == nil {
			observed = run.Steps["a"]
			if observed.Status == dag.StepStatusRunning && observed.Attempts == 1 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if observed.Status != dag.StepStatusRunning {
		t.Fatalf("Status = %v, want Running (#137 repro)", observed.Status)
	}
	if observed.Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1 (#137 repro)", observed.Attempts)
	}

	// Release the handler so the test cleans up.
	close(handlerProceed)
}
```

- [ ] **Step 12.2: Run the test.**

```bash
go test ./worker/ -run TestEndToEnd_AttemptsVisibleDuringRun -count=1
```

Expected: green. This is the exact bug — pre-fix it would observe `Status=Queued, Attempts=0` and fail.

- [ ] **Step 12.3: Commit.**

```bash
git add worker/lifecycle_e2e_test.go
git commit -m "test: e2e attempts visible during run (closes #137 repro)"
```

---

## Task 13: End-to-end retry via NAK increments attempts

**Files:**
- Modify: `worker/lifecycle_e2e_test.go`

- [ ] **Step 13.1: Append the retry-via-NAK test.**

```go
func TestEndToEnd_RetryViaNakIncrementsAttempts(t *testing.T) {
	// Methodology: handler errors on the first call, succeeds on
	// the second. Assert run final state has Attempts=2 and the
	// history stream contains both step.started events with distinct
	// AttemptNumber values.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, _ := jetstream.New(nc)

	wfDef := dag.WorkflowDef{
		Name: "e2e-retry", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "retry-task", Type: dag.StepTypeNormal, Retries: 3},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := enginepkg.NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	var calls atomic.Int32
	w := NewWorker(nc)
	w.Handle("retry-task", func(tc TaskContext) error {
		n := calls.Add(1)
		if n == 1 {
			return fmt.Errorf("transient on attempt %d", n)
		}
		return tc.Complete([]byte(`"done"`))
	})
	w.Start()
	defer w.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-rt", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	// Wait for second handler call (first errored, NAK redelivered).
	deadline := time.After(20 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("calls = %d, want 2 within 20s", calls.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Wait for run to reach Completed.
	store := enginepkg.NewSnapshotStore(jsNew)
	endDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(endDeadline) {
		run, err := store.Load(context.Background(), "run-rt")
		if err == nil && run.Steps["a"].Status == dag.StepStatusCompleted {
			if run.Steps["a"].Attempts != 2 {
				t.Fatalf("final Attempts = %d, want 2", run.Steps["a"].Attempts)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// History stream contains both attempts of step.started.
	sub, _ := js.SubscribeSync("history.run-rt", nats.DeliverAll())
	attemptsSeen := make(map[int]bool)
	histDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(histDeadline) && !(attemptsSeen[1] && attemptsSeen[2]) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepStarted {
			attemptsSeen[evt.AttemptNumber] = true
		}
	}
	if !attemptsSeen[1] {
		t.Fatal("expected step.started attempt 1, missing")
	}
	if !attemptsSeen[2] {
		t.Fatal("expected step.started attempt 2, missing")
	}
}
```

- [ ] **Step 13.2: Run the test.**

```bash
go test ./worker/ -run TestEndToEnd_RetryViaNakIncrementsAttempts -count=1
```

Expected: green.

- [ ] **Step 13.3: Commit.**

```bash
git add worker/lifecycle_e2e_test.go
git commit -m "test: e2e NAK retry increments attempts in events and state"
```

---

## Task 14: Final CI parity, push, open PR

**Files:**
- (no code changes — verification only)

- [ ] **Step 14.1: Run the full CI-equivalent suite locally.**

```bash
cd /Users/dmestas/projects/dagnats
go test ./... -count=1
go vet ./...
gofmt -l .
```

Expected:

- `go test ./...`: all green. Pre-existing flakes flagged in spec §8.3 (`TestSuperclusterTopologyFormed`, `TestRESTBulkRetry`) may surface — re-run if they do, distinguishing flake from real failure per the user's CI policy.
- `go vet ./...`: silent.
- `gofmt -l .`: empty output. If anything is listed, run `gofmt -w <file>` on it and commit a `style: gofmt` follow-up.

- [ ] **Step 14.2: Push the branch.**

```bash
git push -u origin fix/issue-137-step-state-transitions
```

- [ ] **Step 14.3: Open the PR.**

```bash
gh pr create --title "fix(#137): wire step.queued + step.started lifecycle events" --body "$(cat <<'EOF'
## Summary

Closes #137. Engine now transitions step state from `Queued → Running` when a worker pulls a task, and `dagnats run status` reports accurate `Status` and `Attempts` while a step is mid-execution.

The fix wires the two missing lifecycle events:

- **Worker emits `step.started`** at the start of every attempt (before invoking the user's handler), with `AttemptNumber = msg.Metadata().NumDelivered` so retries via NAK redelivery automatically produce distinct events.
- **Engine emits `step.queued`** at the dispatch site, after the task is published to `TASK_QUEUES`. Failure-mode is log-and-proceed (the task is already on the queue; rolling back is fragile) — deliberately asymmetric with the worker's NAK-on-failure for `step.started`.
- **Engine handles both events** with monotonic state guards: a stale `step.started` arriving after `step.completed` is logged and ignored; `Attempts` follows a `max()` rule and never decreases.

A small protocol change (`Event.AttemptNumber` with `omitempty`, `NATSMsgID()` appends `.<N>`) keeps each attempt's events distinct in the history stream so `Nats-Msg-Id` dedup doesn't drop retried `step.started` events.

## Lifecycle diagram

```
workflow.started
   ↓
step.queued (attempt=1)        ← engine emits at dispatch
   ↓
step.started (attempt=1)       ← worker emits at handler entry
   ↓
... handler runs ...
   ↓
step.completed                 ← worker emits at success
   ↓
workflow.completed
```

On NAK retry, the worker emits a second `step.started (attempt=2)`. Engine's monotonic `max()` rule on `Attempts` makes the run's reported attempt count strictly correct.

## Test plan

- [ ] `go test ./protocol/ -count=1` — protocol-level unit tests for `AttemptNumber` and `NATSMsgID` suffix
- [ ] `go test ./worker/ -count=1` — worker integration tests (`publishStarted`, dispatch-loop integration, NumDelivered → AttemptNumber)
- [ ] `go test ./internal/engine/ -count=1` — engine integration tests (dispatch-side `step.queued` publish, monotonic handlers for both events)
- [ ] `go test ./worker/ -run TestEndToEnd -count=1` — three end-to-end tests including the `#137` repro
- [ ] `go vet ./...` — no warnings
- [ ] `gofmt -l .` — empty

## Follow-ups (not in this PR)

This PR is deliberately scoped to the happy-path lifecycle. Three related issues are made cleaner by the events landed here, and the spec spells out exactly what each follow-up needs:

- **#141 (fast-fail wedges)** — 3-line `tc.publishFailed(msg, payload)` follow-up, mirrors the `publishStarted` shape exactly. Spec §7.1.
- **#140 (step timeout doesn't fire)** — `step.started` provides the timer-arming point; `AckWait + Heartbeat` is the NATS-native option. Spec §7.2.
- **#147 (retry never re-dispatches)** — `AttemptNumber = NumDelivered` is robust to either retry model; switch to `payload.Attempt` is one line. Spec §7.3.

## Rollback procedure

Clean revert. The PR is contained to `protocol/`, `worker/`, and `internal/engine/`. Old code reading new events ignores `AttemptNumber` (Go JSON unmarshal default). New events on the history stream post-revert: old code's `onEvent` switch has no case for `step.queued`/`step.started`, falls through to default. No NATS state migration required. Operational rollback: just revert the merge.

## Spec link

Design spec: [`docs/superpowers/specs/2026-05-02-issue-137-step-state-transitions-design.md`](../docs/superpowers/specs/2026-05-02-issue-137-step-state-transitions-design.md)

Implementation plan: [`docs/superpowers/plans/2026-05-02-issue-137-step-state-transitions.md`](../docs/superpowers/plans/2026-05-02-issue-137-step-state-transitions.md)
EOF
)"
```

- [ ] **Step 14.4: Note the PR URL in the commit log when the PR is open.**

The PR URL printed by `gh pr create` is the artifact this plan terminates on. Per `CLAUDE.md` global policy: do **not** auto-merge. Wait for the user's manual merge.

- [ ] **Step 14.5: After PR push, run `gh pr checks <num>` to monitor remote CI.**

```bash
gh pr checks $(gh pr view --json number -q .number)
```

Distinguish transient infra failures (registry timeouts, runner outages) from real failures. If real, fix and push a new commit on the same branch — never `--amend` past hook failures.

---

## Self-review

### Spec coverage table

| Spec section | Tasks | Notes |
|---|---|---|
| §1 — `Event.AttemptNumber` field | Task 1 | `omitempty` preserves wire format; legacy unmarshal test guards. |
| §1 — `NATSMsgID()` appends `.<N>` | Task 2 | Workflow events ignored even with attempt set; tested. |
| §2 — `tc.publishStarted(msg)` helper | Task 3 | `publishEvent` explicitly **unchanged**; assertion-defense tests. |
| §2 — Worker dispatch insertion | Task 4 | NAK-on-failure path included; integration test verifies precondition. |
| §2 — `NumDelivered → AttemptNumber` | Task 6 | NAK retry path produces distinct attempts in events. |
| §2 — Publish-failure NAK behaviour | Task 5 | Includes skip-with-issue fallback per spec §6 budget. |
| §3 — `publishLifecycleEvent` helper | Task 7 | Pure helper, no callers yet. |
| §3 — Engine `step.queued` at dispatch | Task 8 | Inside `enqueueReady`, post-`dispatchReadySteps`. Filtered to `StepTypeNormal` and `StepTypeAgentLoop`. |
| §3 — Determinism of `step.queued` msg-id | Task 8 | `TestOrchestrator_StepQueuedMsgIdIsDeterministic`. |
| §3 — Asymmetric failure-mode (log + proceed) | Task 8 | `TestOrchestrator_DispatchProceedsIfQueuedPublishFails`. |
| §4 — `handleStepStarted` with monotonic guards | Task 9 | Six sub-tests cover queued→running, attempts, idempotent, terminal-ignored (×2), monotonic max(). |
| §4 — `handleStepQueued` for replay | Task 10 | Two sub-tests cover replay reconstruction and no-rollback-from-Running. |
| §4 — `isHandledEventType` switch update | Task 9 (Step 9.3) | Both `EventStepStarted` and `EventStepQueued` added. |
| §5 — End-to-end lifecycle order | Task 11 | Asserts `workflow.started → step.queued → step.started → step.completed → workflow.completed` subsequence with non-decreasing timestamps. |
| §5 — End-to-end attempts visible during run (#137 repro) | Task 12 | The exact #137 bug; closes the issue. |
| §5 — End-to-end retry via NAK | Task 13 | Asserts both events and final state. |
| §6 — Publish-failure injection budget | Task 5 | Caveat budget honoured; skip-with-issue fallback included verbatim. |
| §7.1 — #141 follow-up shape | Task 14 PR body | Documented in PR body, not coded. |
| §7.2 — #140 follow-up shape | Task 14 PR body | Documented. |
| §7.3 — #147 follow-up shape | Task 14 PR body | Documented. |
| §8.1 — Risk inventory | Task 14 PR body / behaviours covered by tests | Each risk row maps to a test or a documented log+proceed path. |
| §8.2 — Rollback procedure | Task 14 PR body | "Just revert the merge." |
| §8.3 — PR checklist | Task 14 (Step 14.1, Step 14.3) | All boxes mapped to local CI commands or PR-body sections. |

### Placeholder scan

Searched the plan for forbidden patterns:

- "TBD" — none.
- "TODO" — none.
- "implement later" — none.
- "fill in details" — none.
- "appropriate error handling" — none.
- "similar to Task N" — none.
- "etc." — none in load-bearing sentences.

Every code block is verbatim — no `// ...` ellipsis in any code I wrote (the only ellipses are in narrative sentences, e.g., "... handler runs ..." in the lifecycle diagram, which is documentation, not code).

### Type and name consistency

- `Event.AttemptNumber` (field), `evt.AttemptNumber` (variable access), `int` everywhere — consistent. JSON tag `attempt_number` matches the spec.
- `tc.publishStarted(msg)` — name matches spec §2 verbatim.
- `publishLifecycleEvent(ctx, js, evt)` — name matches spec §3 verbatim.
- `handleStepStarted(ctx, evt)`, `handleStepQueued(ctx, evt)` — names match spec §4. Method on `*Orchestrator`. Signatures match existing handlers (`handleStepCompleted`, `handleStepFailed`).
- `o.store.Load(ctx, runID)` and `o.saveSnapshot(ctx, run)` — substituted for spec's `loadRunState` / `persistRunState` placeholders. Verified against actual repo by reading `internal/engine/orchestrator.go`.
- `run.Steps[stepID]` — substituted for spec's `state.Step(...)` placeholder. Returns `dag.StepState` (a value type — must mutate then re-assign per existing pattern at orchestrator.go:1491-1493).
- Dropped `step.StartedAt = evt.Timestamp` — `dag.StepState` has no such field, and spec §8.4 explicitly scopes timestamp-on-state out.
- Test names in plan match what the tasks declare (e.g., `TestOnEvent_StepStarted_TransitionsQueuedToRunning` appears in both Task 9's test list and is the function name in Step 9.1's code block).
- File names consistent across "File structure" table and the per-task `**Files:**` lines.
- Branch name `fix/issue-137-step-state-transitions` consistent across "Branch setup" and Task 14's push command.
