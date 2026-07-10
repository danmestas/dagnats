// worker/stop_drain_test.go
// Tests that Worker.Stop() drains in-flight handleMessage calls before
// returning (#498). Prior to the fix, Stop() stopped new message delivery
// but never waited for a handler that was already executing — so a
// caller's deferred cleanup (e.g. closing a DB pool) could run out from
// under a handler still mid-execution.
package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// TestStop_DrainsInFlightHandler registers a handler that signals it has
// started, then blocks until released. It publishes one task, waits for
// the handler to be in-flight, and calls Stop() from a goroutine.
// Methodology: assert Stop() does NOT return while the handler is still
// blocked (a short select on a "stop returned" channel must time out),
// then release the handler and assert Stop() returns only after the
// handler has run to completion. This is the exact shape a real caller
// relies on: Stop() returning is the caller's signal that it's now safe
// to close shared resources (DB pools, etc.) the handler was using.
func TestStop_DrainsInFlightHandler(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})

	w := NewWorker(nc)
	w.Handle("drain", func(ctx TaskContext) error {
		close(started)
		<-release
		close(completed)
		return ctx.Complete([]byte(`"ok"`))
	})
	w.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload := protocol.TaskPayload{
		RunID:  "drain-run",
		StepID: "s",
		Input:  json.RawMessage(`"x"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := js.Publish(ctx, "task.drain.drain-run", data); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start within 5s")
	}

	stopReturned := make(chan struct{})
	go func() {
		w.Stop()
		close(stopReturned)
	}()

	// Stop() must block while the handler is still in flight. This is
	// the assertion that fails pre-fix: without handlerWG, Stop() races
	// ahead and closes stopReturned almost immediately.
	select {
	case <-stopReturned:
		t.Fatal("Stop() returned before the in-flight handler finished")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return after the handler was released")
	}

	select {
	case <-completed:
	default:
		t.Fatal("handler did not complete before Stop() returned")
	}
}
