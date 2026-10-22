// worker/tool_replay_test.go
// Tool replay through the Worker (ADR-026, #805): a worker stalled
// inside an unsafe tool past its AckWait is replaced by the
// redelivery, which finds the intent without a result, does not rerun
// the tool, records an interrupted result for the model and completes
// the step; the stalled worker, once it resumes, is fenced before it
// can record anything; the unsafe tool's side effect, counted outside
// the journal, happens exactly once; the journal audit passes.
//
// Methodology: one real orchestrator and two real Workers on an
// embedded NATS server, with a short AckWait. Dispatch is as the
// engine does it: iteration 1 carries the stamped attempt, so the
// stalled delivery and its redelivery differ only in delivery count.
// Every wait is bounded.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
)

func TestToolReplay_StalledUnsafeToolIsInterruptedNotRerun(t *testing.T) {
	const taskType = "tool-replay-task"
	nc, js, jsNew, defData := startLoopEngine(t, "wf-tool-replay", taskType)
	release := make(chan struct{})
	stalledDone := make(chan error, 1)
	claimed := make(chan struct{}, 2)
	var effects atomic.Int32
	handler := func(tc TaskContext) error {
		j, err := tc.Journal()
		if err != nil {
			return err
		}
		if j.Task().Iteration == 0 {
			return tc.Continue(nil)
		}
		claimed <- struct{}{}
		stalled := j.Task().Delivered == 1
		result, err := j.Tool(tc.Context(), journal.ToolCall{Name: "charge"}, journal.Unsafe,
			func(context.Context, journal.ToolCall) ([]byte, error) {
				effects.Add(1)
				if stalled {
					<-release
				}
				return []byte(`"charged"`), nil
			})
		if stalled {
			stalledDone <- err
		}
		if err != nil {
			return err
		}
		output, err := json.Marshal(result)
		if err != nil {
			return err
		}
		return tc.Complete(output)
	}
	startFencingWorker(t, nc, taskType, handler)
	startFencingWorker(t, nc, taskType, handler)
	startStepRun(t, js, "run-tool-replay", defData)
	waitClosed(t, claimed, "first claim")
	waitClosed(t, claimed, "second claim")
	run := waitStepSettled(t, jsNew, "run-tool-replay", journalTestTimeout)
	step := run.Steps["s"]
	if step.Status != dag.StepStatusCompleted {
		t.Fatalf("step = %+v, want completed by the redelivery", step)
	}
	var result journal.ToolResult
	if err := json.Unmarshal(step.Output, &result); err != nil {
		t.Fatalf("decode output %s: %v", step.Output, err)
	}
	if !result.Interrupted || result.Error != journal.ErrInterrupted || len(result.Output) != 0 {
		t.Errorf("successor saw %+v, want an interrupted result with no output", result)
	}
	close(release)
	select {
	case err := <-stalledDone:
		if !errors.Is(err, journal.ErrFenced) {
			t.Errorf("stalled Tool err = %v, want ErrFenced", err)
		}
	case <-time.After(journalTestTimeout):
		t.Fatal("stalled worker never resumed")
	}
	if n := effects.Load(); n != 1 {
		t.Errorf("unsafe tool ran %d times, want exactly once", n)
	}
	assertJournalClean(t, nc, "run-tool-replay")
}

// assertJournalClean runs the journal audit on runID and fails the
// test on a violation.
func assertJournalClean(t *testing.T, nc *nats.Conn, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	report, err := journal.Check(ctx, nc, runID)
	if err != nil || !report.OK() {
		t.Errorf("Check = %+v, %v; want no violations", report, err)
	}
}
