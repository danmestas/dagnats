// worker/cancel_handler_test.go
// Pins #802: a cancel reaches a handler that is already running.
//
// Methodology: real embedded NATS, real Worker. A handler blocks on
// tc.Context().Done() and reports how it was released. Each test
// dispatches a task, waits for the handler to start, then cancels
// through one of the two paths (the core-NATS signal, the run state
// seen on Heartbeat). Negative space: a handler of another run is
// never released, and a handler left alone is not released by timing.
// Every wait is bounded.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const cancelWaitBound = 3 * time.Second

type cancelRig struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	started chan string   // run ids whose handler began
	ctxDone chan string   // run ids whose handler saw ctx.Done()
	beatErr chan error    // result of Heartbeat, when beat is set
	release chan struct{} // closed at cleanup so idle handlers return
}

// newCancelRig starts a worker whose "block" handler waits for its
// context to end. When beat is true the handler calls Heartbeat first.
func newCancelRig(t *testing.T, beat bool) *cancelRig {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	rig := &cancelRig{
		nc: nc, js: js,
		started: make(chan string, 4),
		ctxDone: make(chan string, 4),
		beatErr: make(chan error, 4),
		release: make(chan struct{}),
	}
	w := NewWorker(nc)
	blocker := func(tc TaskContext) error {
		rig.started <- tc.RunID()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if beat {
				if err := tc.Heartbeat(); err != nil {
					rig.beatErr <- err
				}
			}
			select {
			case <-tc.Context().Done():
				rig.ctxDone <- tc.RunID()
				return nil
			case <-rig.release:
				return nil
			case <-time.After(50 * time.Millisecond):
			}
		}
		return nil
	}
	// One task type per concurrent handler: a type runs one at a time.
	w.Handle("block", blocker)
	w.Handle("block-b", blocker)
	w.Start()
	t.Cleanup(w.Stop)
	t.Cleanup(func() { close(rig.release) })
	return rig
}

func (r *cancelRig) dispatch(t *testing.T, taskType, runID string) {
	t.Helper()
	data, err := json.Marshal(protocol.TaskPayload{RunID: runID, StepID: "s"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := r.js.Publish(context.Background(), "task."+taskType+"."+runID, data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func (r *cancelRig) awaitStarted(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-r.started:
		if got != want {
			t.Fatalf("started run %q, want %q", got, want)
		}
	case <-time.After(cancelWaitBound):
		t.Fatalf("handler of %s did not start", want)
	}
}

func TestHandlerContext_EndsOnCancelSignal(t *testing.T) {
	rig := newCancelRig(t, false)
	rig.dispatch(t, "block", "run-sig-a")
	rig.awaitStarted(t, "run-sig-a")
	rig.dispatch(t, "block-b", "run-sig-b")
	rig.awaitStarted(t, "run-sig-b")

	if err := rig.nc.Publish(protocol.CancelSubject("run-sig-a"), nil); err != nil {
		t.Fatalf("Publish cancel: %v", err)
	}
	select {
	case got := <-rig.ctxDone:
		if got != "run-sig-a" {
			t.Fatalf("ctx ended for %q, want run-sig-a", got)
		}
	case <-time.After(cancelWaitBound):
		t.Fatal("handler ctx not cancelled within bound")
	}
	// Negative: the other run's handler is untouched.
	select {
	case got := <-rig.ctxDone:
		t.Fatalf("ctx of %q ended without a cancel", got)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestHeartbeat_ReportsCancelledRun(t *testing.T) {
	rig := newCancelRig(t, true)
	putRunStatus(t, rig.js, "run-hb", dag.RunStatusRunning)
	rig.dispatch(t, "block", "run-hb")
	rig.awaitStarted(t, "run-hb")
	// Negative: while the run is live, beats succeed and ctx stays open.
	select {
	case err := <-rig.beatErr:
		t.Fatalf("Heartbeat on a live run failed: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	// No signal is sent: the beat is the only way to learn of the cancel.
	putRunStatus(t, rig.js, "run-hb", dag.RunStatusCancelled)
	select {
	case err := <-rig.beatErr:
		if !errors.Is(err, ErrRunCancelled) {
			t.Fatalf("Heartbeat error = %v, want ErrRunCancelled", err)
		}
	case <-time.After(cancelWaitBound):
		t.Fatal("Heartbeat never reported the cancelled run")
	}
	select {
	case <-rig.ctxDone:
	case <-time.After(cancelWaitBound):
		t.Fatal("handler ctx not cancelled after Heartbeat saw the cancel")
	}
}
