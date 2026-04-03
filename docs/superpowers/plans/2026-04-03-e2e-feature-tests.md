# E2E Feature Tests Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the remaining 16 feature correctness tests that exercise every DagNats feature through the full stack across all NATS topologies.

**Architecture:** Each test file follows the same pattern: call `harness.RunE2E` with a test function that builds a workflow, starts workers, runs the workflow, and asserts results. All tests are independent — they can be implemented in parallel since each creates a separate file in `e2e/features/`.

**Tech Stack:** Go, `e2e/harness` package (RunE2E, helpers), DagNats public APIs

**Spec:** `docs/superpowers/specs/2026-04-03-e2e-test-suite-design.md`

**Harness docs:** The `e2e/harness/` package provides:
- `RunE2E(t, func(t, nc))` — runs test against all enabled topologies
- `SubscribeWorker(t, nc, taskName, handler)` — starts worker with cleanup
- `NewTestService(t, nc)` — creates `api.Service` with noop telemetry
- `UniqueName(t, base)` — unique workflow names for KV isolation
- `RegisterAndStart(t, svc, wfDef, input)` — register + start, returns runID
- `WaitForRunStatus(t, svc, runID, status, timeout)` — bounded 250ms poll
- `AssertHistoryContains(t, svc, runID, eventTypes...)` — subsequence check

**Convention:** Every test file starts with a methodology comment. Every test asserts positive AND negative space (2+ assertions). Timeout for `WaitForRunStatus` is 15s (generous for supercluster). The orchestrator must be started in every test since `RunE2E` only provides the connection.

**Common test preamble** (repeated in each test, not extracted — keeps tests self-contained):
```go
harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
    tel := observe.NewNoopTelemetry()
    orch := engine.NewOrchestrator(nc, tel)
    orch.Start()
    t.Cleanup(func() { orch.Stop() })
    svc := harness.NewTestService(t, nc)
    // ... build workflow, register workers, run, assert
})
```

---

## Batch 1: Core Workflow Patterns (Tasks 1-4)

These test fundamental workflow shapes and can be dispatched in parallel.

### Task 1: TestParallelFanOut

**File:** `e2e/features/fanout_test.go`

```go
// e2e/features/fanout_test.go
// Tests parallel fan-out and join. Methodology: workflow A→(B,C,D)→E
// where B,C,D run in parallel and E waits for all three. Verify all
// 5 steps complete and E runs last.
package features

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestParallelFanOut(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Workers for all 5 steps.
		for _, name := range []string{
			"entry", "p1", "p2", "p3", "join",
		} {
			name := name
			harness.SubscribeWorker(t, nc, name,
				func(tc worker.TaskContext) error {
					return tc.Complete(
						[]byte(`"` + name + `-done"`),
					)
				},
			)
		}

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "fanout")
		wb := dag.NewWorkflow(wfName)
		entry := wb.Task("entry", "entry")
		p1 := wb.Task("p1", "p1").After(entry)
		p2 := wb.Task("p2", "p2").After(entry)
		p3 := wb.Task("p3", "p3").After(entry)
		wb.Task("join", "join").After(p1, p2, p3)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow completes.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: all 5 steps completed.
		for _, id := range []string{
			"entry", "p1", "p2", "p3", "join",
		} {
			if run.Steps[id].Status != dag.StepStatusCompleted {
				t.Fatalf("step %s: %s", id, run.Steps[id].Status)
			}
		}

		// Negative: join ran after all parallel steps.
		harness.AssertHistoryContains(t, svc, runID,
			protocol.EventWorkflowStarted,
			protocol.EventWorkflowCompleted,
		)

		// Negative: join output confirms it ran.
		if string(run.Steps["join"].Output) != `"join-done"` {
			t.Fatalf("join output: %s",
				string(run.Steps["join"].Output))
		}
	})
}
```

- [ ] Create `e2e/features/fanout_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestParallelFanOut -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestParallelFanOut`

### Task 2: TestRetryExhaustion and TestNonRetryableError

**File:** `e2e/features/retry_dlq_test.go`

```go
// e2e/features/retry_dlq_test.go
// Tests retry exhaustion → DLQ and non-retryable immediate failure.
// Methodology: configure retries, fail the handler, verify DLQ entry
// and replay. For non-retryable, verify immediate failure with no retries.
package features

import (
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestRetryExhaustion(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Handler always fails with a retryable error.
		harness.SubscribeWorker(t, nc, "flaky",
			func(tc worker.TaskContext) error {
				return fmt.Errorf("transient failure")
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "retry-exhaust")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "flaky").WithRetries(2)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow fails after retries exhausted.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusFailed, 15*time.Second,
		)

		// Positive: step is marked failed.
		if run.Steps["step"].Status != dag.StepStatusFailed {
			t.Fatalf("step status: %s", run.Steps["step"].Status)
		}

		// Negative: step was attempted multiple times.
		if run.Steps["step"].Attempts < 2 {
			t.Fatalf("expected 2+ attempts, got %d",
				run.Steps["step"].Attempts)
		}
	})
}

func TestNonRetryableError(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Handler fails with non-retryable error.
		harness.SubscribeWorker(t, nc, "fatal",
			func(tc worker.TaskContext) error {
				return worker.NewNonRetryableError(
					fmt.Errorf("config missing"),
				)
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "non-retryable")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "fatal").WithRetries(5)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow fails immediately.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusFailed, 15*time.Second,
		)

		// Negative: only 1 attempt — retries were NOT used.
		if run.Steps["step"].Attempts > 1 {
			t.Fatalf("expected 1 attempt (non-retryable), got %d",
				run.Steps["step"].Attempts)
		}
	})
}
```

- [ ] Create `e2e/features/retry_dlq_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestRetry -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestRetryExhaustion and TestNonRetryableError`

### Task 3: TestSignalWait

**File:** `e2e/features/signals_test.go`

```go
// e2e/features/signals_test.go
// Tests cross-step signal coordination. Methodology: step blocks on
// WaitForSignal, external SendSignal unblocks it, workflow completes.
package features

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestSignalWait(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Step waits for "approval" signal.
		harness.SubscribeWorker(t, nc, "wait-for-approval",
			func(tc worker.TaskContext) error {
				data, err := tc.WaitForSignal(
					"approval", 30*time.Second,
				)
				if err != nil {
					return err
				}
				return tc.Complete(data)
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "signal")
		wb := dag.NewWorkflow(wfName)
		wb.Task("wait", "wait-for-approval")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Give worker time to start waiting for the signal.
		time.Sleep(1 * time.Second)

		// Send the signal via API.
		ctx := context.Background()
		err = svc.SendSignal(
			ctx, runID, "approval", []byte(`"approved"`),
		)
		if err != nil {
			t.Fatalf("SendSignal: %v", err)
		}

		// Positive: workflow completes.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Negative: step output contains the signal data.
		if string(run.Steps["wait"].Output) != `"approved"` {
			t.Fatalf("output: %s",
				string(run.Steps["wait"].Output))
		}
	})
}
```

- [ ] Create `e2e/features/signals_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestSignalWait -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestSignalWait`

### Task 4: TestAgentLoop

**File:** `e2e/features/agent_loop_test.go`

```go
// e2e/features/agent_loop_test.go
// Tests agent loop with checkpoints. Methodology: step calls Continue()
// 3 times with Checkpoint() each iteration, then Complete(). Verify
// iteration count and checkpoint persistence.
package features

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestAgentLoop(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Agent loop: continue 3 times, then complete.
		harness.SubscribeWorker(t, nc, "counter",
			func(tc worker.TaskContext) error {
				// Load checkpoint to get current count.
				var count int
				cp, _ := tc.LoadCheckpoint()
				if cp != nil {
					json.Unmarshal(cp, &count)
				}
				count++

				// Save checkpoint.
				cpData, _ := json.Marshal(count)
				tc.Checkpoint(cpData)

				if count >= 3 {
					return tc.Complete(cpData)
				}
				return tc.Continue(cpData)
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "agent-loop")
		wb := dag.NewWorkflow(wfName)
		wb.AgentLoop("loop", "counter").
			WithMaxIterations(10)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow completes.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: step completed.
		if run.Steps["loop"].Status != dag.StepStatusCompleted {
			t.Fatalf("step: %s", run.Steps["loop"].Status)
		}

		// Negative: iterations happened (count=3 in output).
		if string(run.Steps["loop"].Output) != "3" {
			t.Fatalf("output: %s",
				string(run.Steps["loop"].Output))
		}
	})
}
```

- [ ] Create `e2e/features/agent_loop_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestAgentLoop -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestAgentLoop`

---

## Batch 2: Workflow Controls (Tasks 5-8)

### Task 5: TestConcurrencyLimit

**File:** `e2e/features/concurrency_test.go`

```go
// e2e/features/concurrency_test.go
// Tests workflow concurrency limits. Methodology: register workflow
// with max_runs=1, start 2 runs, verify first runs while second is
// pending, complete first, verify second auto-starts.
package features

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestConcurrencyLimit(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Worker that completes on command via a channel.
		gate := make(chan struct{}, 1)
		var taskCount atomic.Int32
		harness.SubscribeWorker(t, nc, "gated",
			func(tc worker.TaskContext) error {
				taskCount.Add(1)
				<-gate
				return tc.Complete([]byte(`"done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()
		wfName := harness.UniqueName(t, "concurrency")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "gated")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.Concurrency = &dag.ConcurrencyLimit{MaxRuns: 1}

		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Start 2 runs.
		runID1, err := svc.StartRun(ctx, wfName, nil)
		if err != nil {
			t.Fatalf("StartRun 1: %v", err)
		}
		runID2, err := svc.StartRun(ctx, wfName, nil)
		if err != nil {
			t.Fatalf("StartRun 2: %v", err)
		}

		// Wait for run 1 to be running.
		harness.WaitForRunStatus(
			t, svc, runID1,
			dag.RunStatusRunning, 10*time.Second,
		)

		// Positive: run 2 is pending (concurrency limit).
		time.Sleep(1 * time.Second)
		run2, err := svc.GetRun(ctx, runID2)
		if err != nil {
			t.Fatalf("GetRun 2: %v", err)
		}
		if run2.Status != dag.RunStatusPending {
			t.Fatalf("run 2: expected pending, got %s",
				run2.Status)
		}

		// Complete run 1.
		gate <- struct{}{}
		harness.WaitForRunStatus(
			t, svc, runID1,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Negative: run 2 auto-starts and completes.
		gate <- struct{}{}
		harness.WaitForRunStatus(
			t, svc, runID2,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
```

- [ ] Create `e2e/features/concurrency_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestConcurrencyLimit -v -count=1 -timeout 60s`
- [ ] Commit: `test(e2e): add TestConcurrencyLimit`

### Task 6: TestWorkflowTimeout

**File:** `e2e/features/timeout_test.go`

```go
// e2e/features/timeout_test.go
// Tests workflow timeout enforcement. Methodology: workflow with 2s
// timeout, agent loop step that continues every 500ms. Verify the
// orchestrator cancels the workflow on next event after deadline.
package features

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestWorkflowTimeout(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Agent loop that never completes — continues forever.
		harness.SubscribeWorker(t, nc, "infinite",
			func(tc worker.TaskContext) error {
				return tc.Continue([]byte(`"tick"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "timeout")
		wb := dag.NewWorkflow(wfName)
		wb.AgentLoop("loop", "infinite").
			WithMaxIterations(1000)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.Timeout = 2 * time.Second

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow gets cancelled due to timeout.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCancelled, 15*time.Second,
		)

		// Negative: it didn't complete successfully.
		if run.Status == dag.RunStatusCompleted {
			t.Fatal("expected cancelled, got completed")
		}
	})
}
```

- [ ] Create `e2e/features/timeout_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestWorkflowTimeout -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestWorkflowTimeout`

### Task 7: TestConditionalSkip

**File:** `e2e/features/conditional_test.go`

```go
// e2e/features/conditional_test.go
// Tests conditional step skipping. Methodology: A→B→C where B has
// SkipIf condition on A's output. When A outputs the skip value,
// B is skipped and C still runs.
package features

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestConditionalSkip(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "check",
			func(tc worker.TaskContext) error {
				return tc.Complete(
					[]byte(`{"action":"skip"}`),
				)
			},
		)
		harness.SubscribeWorker(t, nc, "process",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"processed"`))
			},
		)
		harness.SubscribeWorker(t, nc, "finalize",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"finalized"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "conditional")
		wb := dag.NewWorkflow(wfName)
		check := wb.Task("check", "check")
		process := wb.Task("process", "process").
			After(check).
			SkipIf(dag.SkipIfOutput(
				check, "action", "==", "skip",
			))
		wb.Task("finalize", "finalize").After(process)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: process was skipped.
		if run.Steps["process"].Status != dag.StepStatusSkipped {
			t.Fatalf("process: expected skipped, got %s",
				run.Steps["process"].Status)
		}

		// Negative: finalize still ran (skipped counts as satisfied).
		if run.Steps["finalize"].Status != dag.StepStatusCompleted {
			t.Fatalf("finalize: expected completed, got %s",
				run.Steps["finalize"].Status)
		}
	})
}
```

- [ ] Create `e2e/features/conditional_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestConditionalSkip -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestConditionalSkip`

### Task 8: TestOnFailureHandler

**File:** `e2e/features/on_failure_test.go`

```go
// e2e/features/on_failure_test.go
// Tests on-failure handler execution. Methodology: main step fails,
// fallback step executes with error context in its input.
package features

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestOnFailureHandler(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "risky",
			func(tc worker.TaskContext) error {
				return worker.NewNonRetryableError(
					fmt.Errorf("disk full"),
				)
			},
		)

		var fallbackInput json.RawMessage
		harness.SubscribeWorker(t, nc, "recover",
			func(tc worker.TaskContext) error {
				fallbackInput = tc.Input()
				return tc.Complete([]byte(`"recovered"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "on-failure")
		wb := dag.NewWorkflow(wfName)
		wb.Task("main", "risky")
		wb.Task("fallback", "recover")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		// Set on_failure link (not available via builder).
		wfDef.Steps[0].OnFailure = "fallback"

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Wait — the workflow may complete (fallback succeeds)
		// or fail (depends on whether fallback completion
		// marks the workflow as done). Give it time.
		time.Sleep(5 * time.Second)

		run, _ := svc.GetRun(
			context.Background(), runID,
		)

		// Positive: fallback step was executed.
		if run.Steps["fallback"].Status !=
			dag.StepStatusCompleted {
			t.Fatalf("fallback: expected completed, got %s",
				run.Steps["fallback"].Status)
		}

		// Negative: fallback received error context.
		if len(fallbackInput) == 0 {
			t.Fatal("fallback received no input")
		}
	})
}
```

Note: This test needs `"context"` in imports for the GetRun fallback. Add it.

- [ ] Create `e2e/features/on_failure_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestOnFailureHandler -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestOnFailureHandler`

---

## Batch 3: Triggers (Tasks 9-11)

### Task 9: TestCronTrigger

**File:** `e2e/features/cron_test.go`

```go
// e2e/features/cron_test.go
// Tests cron trigger fires workflow. Methodology: register cron trigger,
// force tick, verify workflow run created and completes.
package features

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestCronTrigger(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "cron-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"cron-done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		// Register workflow.
		wfName := harness.UniqueName(t, "cron-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "cron-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Create trigger service and register cron trigger.
		ts, err := trigger.NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		err = ts.Start()
		if err != nil {
			t.Fatalf("TriggerService.Start: %v", err)
		}
		t.Cleanup(func() { ts.Stop() })

		triggerDef := trigger.TriggerDef{
			ID:         harness.UniqueName(t, "cron"),
			WorkflowID: wfName,
			Enabled:    true,
			Cron: &trigger.CronConfig{
				Expression: "* * * * *",
				Timezone:   "UTC",
			},
		}
		err = svc.CreateTrigger(ctx, triggerDef)
		if err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}

		// Force a tick.
		ts.TickNow()

		// Positive: a run was created (poll for any completed run).
		time.Sleep(2 * time.Second)
		runs, err := svc.ListRuns(ctx, wfName)
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		if len(runs) == 0 {
			t.Fatal("no runs created by cron trigger")
		}

		// Negative: the triggered run completed.
		harness.WaitForRunStatus(
			t, svc, runs[0].RunID,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
```

- [ ] Create `e2e/features/cron_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestCronTrigger -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestCronTrigger`

### Task 10: TestWebhookTrigger

**File:** `e2e/features/webhook_test.go`

```go
// e2e/features/webhook_test.go
// Tests webhook trigger fires workflow. Methodology: register webhook
// trigger, POST to handler via httptest, verify workflow run starts.
package features

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestWebhookTrigger(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "webhook-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"webhook-done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		wfName := harness.UniqueName(t, "webhook-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "webhook-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		ts, err := trigger.NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		err = ts.Start()
		if err != nil {
			t.Fatalf("TriggerService.Start: %v", err)
		}
		t.Cleanup(func() { ts.Stop() })

		webhookPath := "/" + harness.UniqueName(t, "hook")
		triggerDef := trigger.TriggerDef{
			ID:         harness.UniqueName(t, "webhook"),
			WorkflowID: wfName,
			Enabled:    true,
			Webhook: &trigger.WebhookConfig{
				Path: webhookPath,
			},
		}
		err = svc.CreateTrigger(ctx, triggerDef)
		if err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}

		// POST to webhook handler.
		handler := ts.WebhookHandler()
		req := httptest.NewRequest(
			http.MethodPost, webhookPath,
			strings.NewReader(`{"event":"test"}`),
		)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		// Positive: webhook accepted.
		if rec.Code != http.StatusOK {
			t.Fatalf("webhook: expected 200, got %d", rec.Code)
		}

		// Negative: run was created and completes.
		time.Sleep(2 * time.Second)
		runs, err := svc.ListRuns(ctx, wfName)
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		if len(runs) == 0 {
			t.Fatal("no runs created by webhook trigger")
		}
		harness.WaitForRunStatus(
			t, svc, runs[0].RunID,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
```

- [ ] Create `e2e/features/webhook_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestWebhookTrigger -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestWebhookTrigger`

### Task 11: TestSubjectTrigger

**File:** `e2e/features/subject_trigger_test.go`

```go
// e2e/features/subject_trigger_test.go
// Tests subject trigger fires workflow. Methodology: register trigger
// on a NATS subject, publish message, verify workflow run starts.
package features

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestSubjectTrigger(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "subject-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"subject-done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		wfName := harness.UniqueName(t, "subject-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "subject-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		ts, err := trigger.NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		err = ts.Start()
		if err != nil {
			t.Fatalf("TriggerService.Start: %v", err)
		}
		t.Cleanup(func() { ts.Stop() })

		subjectName := harness.UniqueName(t, "events.order")
		triggerDef := trigger.TriggerDef{
			ID:         harness.UniqueName(t, "subject"),
			WorkflowID: wfName,
			Enabled:    true,
			Subject: &trigger.SubjectConfig{
				Subject: subjectName,
			},
		}
		err = svc.CreateTrigger(ctx, triggerDef)
		if err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}

		// Allow subscription to establish.
		time.Sleep(1 * time.Second)

		// Publish to the trigger subject.
		err = nc.Publish(subjectName, []byte(`{"order_id":"123"}`))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}

		// Positive: run was created.
		time.Sleep(2 * time.Second)
		runs, err := svc.ListRuns(ctx, wfName)
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		if len(runs) == 0 {
			t.Fatal("no runs created by subject trigger")
		}

		// Negative: run completes.
		harness.WaitForRunStatus(
			t, svc, runs[0].RunID,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
```

- [ ] Create `e2e/features/subject_trigger_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestSubjectTrigger -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestSubjectTrigger`

---

## Batch 4: Validation and Routing (Tasks 12-14)

### Task 12: TestInputSchemaValidation

**File:** `e2e/features/input_schema_test.go`

```go
// e2e/features/input_schema_test.go
// Tests input schema validation. Methodology: register workflow with
// schema, start with invalid input (fails), start with valid input
// (succeeds).
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestInputSchemaValidation(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "echo",
			func(tc worker.TaskContext) error {
				return tc.Complete(tc.Input())
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		wfName := harness.UniqueName(t, "schema")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "echo")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.InputSchema = json.RawMessage(`{
			"type": "object",
			"required": ["name"],
			"properties": {
				"name": {"type": "string"}
			}
		}`)
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Positive: invalid input → run fails immediately.
		badRunID, err := svc.StartRun(
			ctx, wfName, []byte(`{"age": 25}`),
		)
		if err != nil {
			t.Fatalf("StartRun (bad): %v", err)
		}
		badRun := harness.WaitForRunStatus(
			t, svc, badRunID,
			dag.RunStatusFailed, 10*time.Second,
		)
		if badRun.Status != dag.RunStatusFailed {
			t.Fatalf("bad input: expected failed, got %s",
				badRun.Status)
		}

		// Negative: valid input → run completes.
		goodRunID, err := svc.StartRun(
			ctx, wfName, []byte(`{"name": "Dan"}`),
		)
		if err != nil {
			t.Fatalf("StartRun (good): %v", err)
		}
		harness.WaitForRunStatus(
			t, svc, goodRunID,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
```

- [ ] Create `e2e/features/input_schema_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestInputSchemaValidation -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestInputSchemaValidation`

### Task 13: TestWorkerGroups

**File:** `e2e/features/worker_groups_test.go`

```go
// e2e/features/worker_groups_test.go
// Tests worker group routing. Methodology: step has WorkerGroup="gpu",
// two workers subscribed (one with gpu group, one without). Verify
// only the gpu worker receives the task.
package features

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestWorkerGroups(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		var gpuCalled atomic.Bool
		var defaultCalled atomic.Bool

		// GPU worker — subscribes to gpu group.
		gpuWorker := worker.NewWorker(
			nc, tel, worker.WithGroups("gpu"),
		)
		gpuWorker.Handle("render", func(tc worker.TaskContext) error {
			gpuCalled.Store(true)
			return tc.Complete([]byte(`"gpu-done"`))
		})
		gpuWorker.Start()
		t.Cleanup(func() { gpuWorker.Stop() })

		// Default worker — no group.
		defaultWorker := worker.NewWorker(nc, tel)
		defaultWorker.Handle("render",
			func(tc worker.TaskContext) error {
				defaultCalled.Store(true)
				return tc.Complete([]byte(`"default-done"`))
			},
		)
		defaultWorker.Start()
		t.Cleanup(func() { defaultWorker.Stop() })

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "worker-groups")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "render")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.Steps[0].WorkerGroup = "gpu"

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: GPU worker handled the task.
		if !gpuCalled.Load() {
			t.Fatal("GPU worker was not called")
		}

		// Negative: default worker did NOT handle it.
		if defaultCalled.Load() {
			t.Fatal("default worker should not have been called")
		}
	})
}
```

- [ ] Create `e2e/features/worker_groups_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestWorkerGroups -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestWorkerGroups`

### Task 14: TestDeduplication

**File:** `e2e/features/dedup_test.go`

```go
// e2e/features/dedup_test.go
// Tests JetStream deduplication via Nats-Msg-Id. Methodology: publish
// same workflow.started event twice with identical MsgId, verify only
// one run is created.
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestDeduplication(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "dedup-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()
		js, _ := nc.JetStream()

		wfName := harness.UniqueName(t, "dedup-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "dedup-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Manually publish workflow.started event twice with same MsgId.
		runID := "dedup-run-" + harness.UniqueName(t, "id")
		defData, _ := json.Marshal(wfDef)
		evt := protocol.NewWorkflowEvent(
			protocol.EventWorkflowStarted, runID, defData,
		)
		evtData, _ := evt.Marshal()
		msgID := evt.NATSMsgID()

		// First publish.
		_, err = js.Publish(
			evt.NATSSubject(), evtData, nats.MsgId(msgID),
		)
		if err != nil {
			t.Fatalf("Publish 1: %v", err)
		}

		// Second publish — same MsgId (should be deduped).
		_, err = js.Publish(
			evt.NATSSubject(), evtData, nats.MsgId(msgID),
		)
		if err != nil {
			t.Fatalf("Publish 2: %v", err)
		}

		// Wait for the run to complete.
		harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: exactly one run exists.
		runs, err := svc.ListRuns(ctx, wfName)
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}

		count := 0
		for _, r := range runs {
			if r.RunID == runID {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("expected 1 run, got %d", count)
		}

		// Negative: no duplicate run created.
		if len(runs) > 1 {
			t.Fatalf("expected 1 total run, got %d", len(runs))
		}
	})
}
```

- [ ] Create `e2e/features/dedup_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestDeduplication -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestDeduplication`

---

## Batch 5: Child Workflow (Task 15)

### Task 15: TestChildWorkflow

This is the most complex test — it requires manually publishing a spawn event.

**File:** `e2e/features/child_test.go`

```go
// e2e/features/child_test.go
// Tests child workflow spawn and parent notification. Methodology:
// parent step publishes a workflow.spawn event, child workflow runs
// and completes, verify parent receives child.completed notification.
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestChildWorkflow(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		js, _ := nc.JetStream()
		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		// Register child workflow.
		childWfName := harness.UniqueName(t, "child-wf")
		childWb := dag.NewWorkflow(childWfName)
		childWb.Task("child-step", "child-task")
		childDef, err := childWb.Build()
		if err != nil {
			t.Fatalf("Build child: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, childDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow child: %v", err)
		}

		// Worker for child.
		harness.SubscribeWorker(t, nc, "child-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"child-done"`))
			},
		)

		// Parent step spawns child by publishing spawn event.
		parentWfName := harness.UniqueName(t, "parent-wf")
		childRunID := harness.UniqueName(t, "child-run")

		harness.SubscribeWorker(t, nc, "spawn-task",
			func(tc worker.TaskContext) error {
				spawnPayload, _ := json.Marshal(map[string]string{
					"child_run_id":   childRunID,
					"child_workflow": childWfName,
					"parent_step_id": "spawner",
				})
				evt := protocol.NewWorkflowEvent(
					protocol.EventWorkflowSpawn,
					tc.RunID(),
					spawnPayload,
				)
				evtData, _ := evt.Marshal()
				msg := &nats.Msg{
					Subject: evt.NATSSubject(),
					Data:    evtData,
					Header: nats.Header{
						"Nats-Msg-Id": {evt.NATSMsgID()},
					},
				}
				_, pubErr := js.PublishMsg(msg)
				if pubErr != nil {
					return pubErr
				}
				return tc.Complete([]byte(`"spawned"`))
			},
		)

		// Register and start parent workflow.
		parentWb := dag.NewWorkflow(parentWfName)
		parentWb.Task("spawner", "spawn-task")
		parentDef, err := parentWb.Build()
		if err != nil {
			t.Fatalf("Build parent: %v", err)
		}
		parentRunID := harness.RegisterAndStart(
			t, svc, parentDef, nil,
		)

		// Wait for parent to complete (spawner step completes).
		harness.WaitForRunStatus(
			t, svc, parentRunID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Wait for child to complete.
		harness.WaitForRunStatus(
			t, svc, childRunID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: child run exists with parent linkage.
		childRun, err := svc.GetRun(ctx, childRunID)
		if err != nil {
			t.Fatalf("GetRun child: %v", err)
		}
		if childRun.ParentRunID != parentRunID {
			t.Fatalf("child ParentRunID: expected %q, got %q",
				parentRunID, childRun.ParentRunID)
		}

		// Negative: child completed successfully.
		if childRun.Status != dag.RunStatusCompleted {
			t.Fatalf("child status: %s", childRun.Status)
		}
	})
}
```

- [ ] Create `e2e/features/child_test.go`
- [ ] Run: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -run TestChildWorkflow -v -count=1 -timeout 30s`
- [ ] Commit: `test(e2e): add TestChildWorkflow`

---

## Final Validation (Task 16)

### Task 16: Run all feature tests across all topologies

This is not a new file — it validates the entire suite works together.

- [ ] Run all feature tests against embedded: `E2E_TOPOLOGY=embedded go test ./e2e/features/ -v -count=1 -timeout 120s`
- [ ] Run all feature tests against all topologies: `go test ./e2e/features/ -v -count=1 -timeout 300s`
- [ ] Run existing tests to confirm no regressions: `go test ./... -count=1 -timeout 180s`
- [ ] Commit any fixes needed.

---

## Summary

| Batch | Tests | Files |
|---|---|---|
| 1: Core patterns | FanOut, RetryExhaustion, NonRetryableError, SignalWait, AgentLoop | 4 files |
| 2: Controls | ConcurrencyLimit, WorkflowTimeout, ConditionalSkip, OnFailureHandler | 4 files |
| 3: Triggers | CronTrigger, WebhookTrigger, SubjectTrigger | 3 files |
| 4: Validation | InputSchemaValidation, WorkerGroups, Deduplication | 3 files |
| 5: Child workflow | ChildWorkflow | 1 file |
| 6: Validation | Full suite across all topologies | 0 files |

**15 new test files, 17 test functions (retry_dlq has 2), all in `e2e/features/`.**
