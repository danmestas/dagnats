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

	aBlock := make(chan struct{})
	bDone := make(chan struct{})

	w := NewWorker(nc)
	w.Handle("type-a", func(tc TaskContext) error {
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

	time.Sleep(300 * time.Millisecond)

	payloadB := protocol.TaskPayload{RunID: "run-b", StepID: "s"}
	dataB, _ := json.Marshal(payloadB)
	if _, err := js.Publish("task.type-b.run-b", dataB); err != nil {
		t.Fatalf("publish B: %v", err)
	}

	select {
	case <-bDone:
		t.Log("type-b processed concurrently with blocked type-a — works")
	case <-time.After(3 * time.Second):
		close(aBlock)
		t.Fatal("type-b NOT processed within 3s while type-a blocked — cross-type concurrency broken")
	}
	close(aBlock)
}
