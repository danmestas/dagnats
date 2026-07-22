// e2e/features/concurrency_test.go
// Tests workflow concurrency limits. Methodology: register workflow
// with max_runs=1, start 2 runs, verify first runs while second is
// pending, complete first, verify second auto-starts.
package features

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestConcurrencyLimit(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		// Create concurrency_runs KV bucket (not in default setup).
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		_, err = js.CreateKeyValue(
			&nats.KeyValueConfig{Bucket: "concurrency_runs"},
		)
		if err != nil {
			t.Fatalf("CreateKeyValue: %v", err)
		}

		orch := engine.NewOrchestrator(nc)
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

		// Establish the baseline before the negative check below: run
		// 2 must be observably Pending first. StartRun returns as
		// soon as the start request is accepted, before admission for
		// run 2 has necessarily been processed off the async
		// workflow-started NATS event, so GetRun can legitimately
		// race ahead of run 2's first snapshot. This is a positive
		// precondition wait (matching WaitForPrecondition's idiom
		// elsewhere in this harness, #558), not the negative
		// assertion itself.
		harness.WaitForRunStatus(
			t, svc, runID2, dag.RunStatusPending, 5*time.Second,
		)

		// Positive: run 2 stays pending because the concurrency limit
		// blocks admission. This is a negative assertion (run 2 must
		// NOT start), so it can't be proven by a fixed sleep followed
		// by one check — that only proves time elapsed, and passes
		// vacuously if the scheduler simply hasn't gotten to run 2
		// yet (#562). It also can't be proven by polling until
		// Pending is observed — that succeeds on the very first poll
		// and would prove even less than the sleep.
		//
		// Instead, give the scheduler a real window to dispatch run 2
		// and watch two things on every tick for the whole window:
		// taskCount, which the gated worker bumps the instant it
		// receives a task — before it ever blocks on the gate — and
		// run 2's status. Admission for run 2 is decided
		// asynchronously off the workflow-started NATS event
		// (Orchestrator.handleWorkflowStarted -> AdmissionController
		// .Admit), so if the concurrency limit were broken, taskCount
		// would jump to 2 almost immediately — well inside this
		// window — rather than only becoming visible at the end of a
		// fixed sleep. The window is a small multiple of the 250ms
		// poll interval harness.WaitForRunStatus uses for the same
		// local JetStream setup elsewhere in this suite, giving ample
		// margin over real dispatch latency without slowing the test.
		const admissionWindow = 2 * time.Second
		const admissionPollInterval = 50 * time.Millisecond
		harness.AssertHoldsForWindow(
			t, "run 2 stays pending under the concurrency limit",
			admissionWindow, admissionPollInterval,
			func() (bool, string) {
				if got := taskCount.Load(); got >= 2 {
					return false, fmt.Sprintf(
						"worker received a second task "+
							"(taskCount=%d) — concurrency "+
							"limit did not block admission",
						got,
					)
				}
				run2, err := svc.GetRun(ctx, runID2)
				if err != nil {
					return false, fmt.Sprintf("GetRun 2: %v", err)
				}
				if run2.Status != dag.RunStatusPending {
					return false, fmt.Sprintf(
						"run 2: expected pending, got %s",
						run2.Status,
					)
				}
				return true, ""
			},
		)

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
