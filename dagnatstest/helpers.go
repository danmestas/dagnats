// dagnatstest/helpers.go
// Convenience helpers for workflow integration tests. RunAndWait and
// WaitForStatus eliminate the poll-loop boilerplate that every test
// otherwise duplicates.
package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// RunAndWait starts a workflow run and blocks until it reaches any
// terminal status (Completed, Failed, Cancelled, Compensated,
// CompensateFailed). Returns the final WorkflowRun snapshot.
// Fatals the test if the run does not finish within timeout.
func RunAndWait(
	t *testing.T,
	svc *api.Service,
	workflow string,
	input []byte,
	timeout time.Duration,
) dag.WorkflowRun {
	t.Helper()
	if svc == nil {
		panic("RunAndWait: svc must not be nil")
	}
	if workflow == "" {
		panic("RunAndWait: workflow must not be empty")
	}

	ctx := context.Background()
	runID, err := svc.StartRun(ctx, workflow, input)
	if err != nil {
		t.Fatalf("RunAndWait: StartRun %q: %v", workflow, err)
	}

	return WaitForStatus(
		t, svc, runID, timeout,
		dag.RunStatusCompleted,
		dag.RunStatusFailed,
		dag.RunStatusCancelled,
		dag.RunStatusCompensated,
		dag.RunStatusCompensateFailed,
	)
}

// WaitForStatus polls svc.GetRun every 25ms until the run reaches
// one of the given target statuses. Returns the matching snapshot.
// Fatals the test with a descriptive message on timeout.
func WaitForStatus(
	t *testing.T,
	svc *api.Service,
	runID string,
	timeout time.Duration,
	statuses ...dag.RunStatus,
) dag.WorkflowRun {
	t.Helper()
	if svc == nil {
		panic("WaitForStatus: svc must not be nil")
	}
	if runID == "" {
		panic("WaitForStatus: runID must not be empty")
	}
	if len(statuses) == 0 {
		panic("WaitForStatus: statuses must not be empty")
	}

	ctx := t.Context()
	deadline := time.After(timeout)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		run, err := svc.GetRun(ctx, runID)
		if err == nil {
			for _, target := range statuses {
				if run.Status == target {
					return run
				}
			}
		}

		select {
		case <-deadline:
			lastStatus := "unknown"
			run, err := svc.GetRun(ctx, runID)
			if err == nil {
				lastStatus = run.Status.String()
			}
			t.Fatalf(
				"WaitForStatus: run %q did not reach "+
					"target status within %s (last: %s)",
				runID, timeout, lastStatus,
			)
		case <-ctx.Done():
			t.Fatalf(
				"WaitForStatus: context cancelled for run %q",
				runID,
			)
		case <-ticker.C:
		}
	}
}
