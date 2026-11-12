// engine/cancel_queued_test.go
// Methodology: cancels of a run queued behind a full concurrency limit
// (#1008), over a real embedded NATS server with the concurrency_runs
// bucket at limit 1, so the run-slot member set is the real KV record.
// The history consumer is never started; events go to dispatchEvent in
// the order under test. The dequeue/cancel race is forced, not hoped
// for: afterDequeueLoad runs the cancel between the dequeue's load of
// the queued run and its slot acquisition. Each test asserts the slot
// set, the run's status and TASK_QUEUES, so a slot held by a terminal
// run or a dispatch of a cancelled run shows.
package engine

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// startLimited publishes and handles a start, then checks the status
// admission gave the run.
func (f unstartedFixture) startLimited(t *testing.T, runID string, want dag.RunStatus) {
	t.Helper()
	start := f.startEvent(t, runID)
	publishEvent(t, f.js, start)
	f.dispatch(t, start)
	if got := f.runStatus(t, runID); got != want {
		t.Fatalf("run %s after start = %s, want %s", runID, got, want)
	}
}

// finishStep completes step a of runID, which completes the run.
func (f unstartedFixture) finishStep(t *testing.T, runID string) {
	t.Helper()
	done := protocol.NewStepEvent(protocol.EventStepCompleted, runID, "a", []byte(`"done"`))
	publishEvent(t, f.js, done)
	f.dispatch(t, done)
	if got := f.runStatus(t, runID); got != dag.RunStatusCompleted {
		t.Fatalf("run %s after its step = %s, want completed", runID, got)
	}
}

func (f unstartedFixture) assertSlots(t *testing.T, want ...string) {
	t.Helper()
	got := f.slotHolders(t)
	if !slices.Equal(got, want) {
		t.Fatalf("slot holders = %v, want %v", got, want)
	}
}

func (f unstartedFixture) cancel(t *testing.T, runID string) {
	t.Helper()
	publishEvent(t, f.js, cancelEvent(runID))
	f.dispatch(t, cancelEvent(runID))
}

// A holds the only slot, B and C queue. Cancelling B finalizes it; when
// A finishes, B is skipped and C is admitted.
func TestCancelQueuedRunNeverStarts(t *testing.T) {
	f := newLimitedFixture(t)
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "queued-b", dag.RunStatusPending)
	f.startLimited(t, "queued-c", dag.RunStatusPending)

	f.cancel(t, "queued-b")
	if got := f.runStatus(t, "queued-b"); got != dag.RunStatusCancelled {
		t.Fatalf("queued run after cancel = %s, want cancelled", got)
	}
	f.assertSlots(t, "holder")

	f.finishStep(t, "holder")
	if got := f.runStatus(t, "queued-b"); got != dag.RunStatusCancelled {
		t.Fatalf("cancelled run after the slot freed = %s, want cancelled", got)
	}
	if got := f.runStatus(t, "queued-c"); got != dag.RunStatusRunning {
		t.Fatalf("next queued run = %s, want running", got)
	}
	f.assertSlots(t, "queued-c")
	if got := f.taskCount(t); got != 2 {
		t.Fatalf("TASK_QUEUES holds %d tasks, want 2 (holder and queued-c)", got)
	}
}

// The cancel lands between the dequeue's load and its slot acquisition.
// The dequeue loses the run, so the cancelled run holds no slot and is
// never dispatched, and the freed slot admits the next start.
func TestCancelInsideDequeueWindowHoldsNoSlot(t *testing.T) {
	f := newLimitedFixture(t)
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "raced", dag.RunStatusPending)
	fired := 0
	f.orch.afterDequeueLoad = func(ctx context.Context, runID string) {
		if runID != "raced" || fired > 0 {
			return
		}
		fired++
		f.cancel(t, runID)
	}

	f.finishStep(t, "holder")
	if fired != 1 {
		t.Fatalf("dequeue hook fired %d times for the queued run, want 1", fired)
	}
	if got := f.runStatus(t, "raced"); got != dag.RunStatusCancelled {
		t.Fatalf("raced run = %s, want cancelled", got)
	}
	f.assertSlots(t)
	if got := f.taskCount(t); got != 1 {
		t.Fatalf("TASK_QUEUES holds %d tasks, want 1: the cancelled run dispatched", got)
	}
	f.startLimited(t, "after", dag.RunStatusRunning)
	f.assertSlots(t, "after")
}

// The reverse order: the dequeue wins, the run starts, and the cancel
// then cancels a running run as it always has, giving its slot back.
func TestCancelAfterDequeueCancelsRunningRun(t *testing.T) {
	f := newLimitedFixture(t)
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "dequeued", dag.RunStatusPending)

	f.finishStep(t, "holder")
	if got := f.runStatus(t, "dequeued"); got != dag.RunStatusRunning {
		t.Fatalf("dequeued run = %s, want running", got)
	}
	f.assertSlots(t, "dequeued")
	if got := f.taskCount(t); got != 2 {
		t.Fatalf("TASK_QUEUES holds %d tasks, want 2", got)
	}
	f.cancel(t, "dequeued")
	if got := f.runStatus(t, "dequeued"); got != dag.RunStatusCancelled {
		t.Fatalf("running run after cancel = %s, want cancelled", got)
	}
	f.assertSlots(t)
}

// A dequeue whose scan saw the run pending but whose load finds it
// already cancelled leaves it alone: no slot, no dispatch.
func TestDequeueSkipsRunCancelledBeforeLoad(t *testing.T) {
	f := newLimitedFixture(t)
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "stale", dag.RunStatusPending)
	f.cancel(t, "stale")
	f.finishStep(t, "holder")

	lost, err := f.orch.transitionPendingToRunning(context.Background(), "stale")
	if err != nil {
		t.Fatalf("transitionPendingToRunning: %v", err)
	}
	if lost {
		t.Fatal("a run that was not pending at load reported as lost after acquiring")
	}
	if got := f.runStatus(t, "stale"); got != dag.RunStatusCancelled {
		t.Fatalf("stale run = %s, want cancelled", got)
	}
	f.assertSlots(t)
	if got := f.taskCount(t); got != 1 {
		t.Fatalf("TASK_QUEUES holds %d tasks, want 1", got)
	}
}

// Two dequeues race for one queued run: the second runs inside the
// first's window and wins. The loser must not release the slot, which
// is the winner's (slots are keyed by run, not by dequeue).
func TestLosingDequeueKeepsTheWinnersSlot(t *testing.T) {
	f := newLimitedFixture(t)
	ctx := context.Background()
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "contested", dag.RunStatusPending)
	// Frees the slot with no dequeue of its own, so both dequeues
	// below are this test's.
	err := f.orch.admission.concurrency.ReleaseRun(ctx, f.def.Name, "holder")
	if err != nil {
		t.Fatalf("ReleaseRun holder: %v", err)
	}
	f.orch.afterDequeueLoad = func(ctx context.Context, runID string) {
		f.orch.afterDequeueLoad = nil
		lost, err := f.orch.transitionPendingToRunning(ctx, runID)
		if err != nil || lost {
			t.Errorf("winning dequeue: lost=%v err=%v, want it to win", lost, err)
		}
	}

	lost, err := f.orch.transitionPendingToRunning(ctx, "contested")
	if err != nil || lost {
		t.Fatalf("losing dequeue: lost=%v err=%v, want neither", lost, err)
	}
	if got := f.runStatus(t, "contested"); got != dag.RunStatusRunning {
		t.Fatalf("contested run = %s, want running", got)
	}
	f.assertSlots(t, "contested")
	if got := f.taskCount(t); got != 2 {
		t.Fatalf("TASK_QUEUES holds %d tasks, want 2: one dispatch per run", got)
	}
}

// A start queues while a losing dequeue holds the slot for a cancelled
// run. When that dequeue gives the slot back it admits the new run;
// otherwise nothing would, with no run left running to finish.
func TestLostDequeueAdmitsTheNextQueuedRun(t *testing.T) {
	f := newLimitedFixture(t)
	cm := f.orch.admission.concurrency
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "raced", dag.RunStatusPending)
	f.orch.afterDequeueLoad = func(ctx context.Context, runID string) {
		f.orch.afterDequeueLoad = nil
		f.cancel(t, runID)
		// Stands in for the slot the lost dequeue is about to take,
		// so the start below queues.
		ok, err := cm.AcquireRun(ctx, f.def.Name, "stand-in", 1)
		if err != nil || !ok {
			t.Fatalf("stand-in AcquireRun: ok=%v err=%v", ok, err)
		}
		f.startLimited(t, "late", dag.RunStatusPending)
		if err := cm.ReleaseRun(ctx, f.def.Name, "stand-in"); err != nil {
			t.Fatalf("stand-in ReleaseRun: %v", err)
		}
	}

	f.finishStep(t, "holder")
	if got := f.runStatus(t, "raced"); got != dag.RunStatusCancelled {
		t.Fatalf("raced run = %s, want cancelled", got)
	}
	if got := f.runStatus(t, "late"); got != dag.RunStatusRunning {
		t.Fatalf("late run = %s, want running: the given-back slot went unused", got)
	}
	f.assertSlots(t, "late")
}

// Cancelling a queued run leaves workflow.runs.active alone: the run
// was never counted active, so decrementing would drive the gauge low.
func TestCancelQueuedRunKeepsActiveGauge(t *testing.T) {
	f := newLimitedFixture(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Errorf("meter provider shutdown: %v", err)
		}
	})
	f.orch.metrics = newOrchMetrics(mp.Meter("cancel-queued-test"))
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.startLimited(t, "queued", dag.RunStatusPending)
	f.cancel(t, "queued")
	if got := activeRuns(t, reader); got != 1 {
		t.Fatalf("workflow.runs.active = %d after cancelling a queued run, want 1", got)
	}
	f.cancel(t, "holder")
	if got := activeRuns(t, reader); got != 0 {
		t.Fatalf("workflow.runs.active = %d after cancelling the running run, want 0", got)
	}
}

func activeRuns(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "workflow.runs.active" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("workflow.runs.active has type %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

// A cancel for a run nobody started is still dropped: no snapshot is
// written and the slot set is untouched.
func TestCancelUnknownRunUnchanged(t *testing.T) {
	f := newLimitedFixture(t)
	f.startLimited(t, "holder", dag.RunStatusRunning)
	f.dispatch(t, cancelEvent("never-started"))
	_, err := f.orch.store.Load(context.Background(), "never-started")
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Load of an unknown cancelled run: err=%v, want ErrRunNotFound", err)
	}
	f.assertSlots(t, "holder")
}
