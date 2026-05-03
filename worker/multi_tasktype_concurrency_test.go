// worker/multi_tasktype_concurrency_test.go
// Regression guard for #138: two distinct task types on the same worker must
// run concurrently. If type-a's handler is blocked, type-b's handler must
// still execute — proves the per-type pull consumers are independent and
// nothing serializes dispatch across task types within one Worker.
package worker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestMultiTaskType_RunsConcurrently(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	aStarted := make(chan struct{})
	aBlock := make(chan struct{})
	bDone := make(chan struct{})

	w := NewWorker(nc)
	w.Handle("type-a", func(tc TaskContext) error {
		close(aStarted)
		<-aBlock
		return tc.Complete([]byte(`"a-done"`))
	})
	w.Handle("type-b", func(tc TaskContext) error {
		close(bDone)
		return tc.Complete([]byte(`"b-done"`))
	})
	w.Start()
	defer w.Stop()

	js, _ := nc.JetStream()
	payloadA := protocol.TaskPayload{RunID: "run-a", StepID: "s"}
	dataA, _ := json.Marshal(payloadA)
	if _, err := js.Publish("task.type-a.run-a", dataA); err != nil {
		t.Fatalf("publish A: %v", err)
	}

	// Wait for type-a's handler to be invoked AND blocked before
	// publishing type-b. A bare time.Sleep here would race: under CI
	// load type-b could land before type-a entered its block,
	// masking the very serialization bug this test guards against.
	select {
	case <-aStarted:
	case <-time.After(5 * time.Second):
		close(aBlock)
		t.Fatal("type-a handler did not start within 5s")
	}

	payloadB := protocol.TaskPayload{RunID: "run-b", StepID: "s"}
	dataB, _ := json.Marshal(payloadB)
	if _, err := js.Publish("task.type-b.run-b", dataB); err != nil {
		t.Fatalf("publish B: %v", err)
	}

	select {
	case <-bDone:
	case <-time.After(3 * time.Second):
		close(aBlock)
		t.Fatal("type-b NOT processed within 3s while type-a blocked — cross-type concurrency broken")
	}
	close(aBlock)
}
