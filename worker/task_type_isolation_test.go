// worker/task_type_isolation_test.go
// Regression guard for #674: a worker's per-task-type durable pull
// consumer must receive only its own task type's dispatches. Before the
// fix, consumername.FilterFor anchored on a trailing ">" wildcard, so a
// worker polling "build" also received "build.linux" dispatches because
// StepSubject minted "task.build.linux.<runID>" for the latter — two
// unrelated task types that merely share a dotted prefix.
// Methodology: real embedded NATS/JetStream. Two Workers, each Handle()-ing
// a distinct task type ("build" and "build.linux"). Publish one task
// message per type and assert each handler fires exactly once for its own
// message and never for the other's.
package worker

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestWorker_TaskTypeIsolation_DottedSibling(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	var buildCalls, buildLinuxCalls atomic.Int32

	buildWorker := NewWorker(nc)
	buildWorker.Handle("build", func(tc TaskContext) error {
		buildCalls.Add(1)
		return tc.Complete([]byte(`"build-done"`))
	})
	buildWorker.Start()
	defer buildWorker.Stop()

	linuxWorker := NewWorker(nc)
	linuxWorker.Handle("build.linux", func(tc TaskContext) error {
		buildLinuxCalls.Add(1)
		return tc.Complete([]byte(`"build-linux-done"`))
	})
	linuxWorker.Start()
	defer linuxWorker.Stop()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	linuxPayload := protocol.TaskPayload{RunID: "run-linux", StepID: "s"}
	linuxData, _ := json.Marshal(linuxPayload)
	if _, err := js.Publish(
		"task.build.linux.run-linux", linuxData,
	); err != nil {
		t.Fatalf("publish build.linux: %v", err)
	}

	// Bounded wait: give the "build.linux" worker time to process its own
	// message, and give the "build" worker the SAME window to (wrongly)
	// pick it up too, before asserting isolation held.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && buildLinuxCalls.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if buildLinuxCalls.Load() != 1 {
		t.Fatalf(
			"build.linux handler calls = %d, want 1",
			buildLinuxCalls.Load(),
		)
	}
	if buildCalls.Load() != 0 {
		t.Fatalf(
			"build handler calls = %d after only a build.linux "+
				"dispatch, want 0 (wildcard leak)",
			buildCalls.Load(),
		)
	}

	// Positive: the "build" worker DOES receive its own task type.
	buildPayload := protocol.TaskPayload{RunID: "run-build", StepID: "s"}
	buildData, _ := json.Marshal(buildPayload)
	if _, err := js.Publish("task.build.run-build", buildData); err != nil {
		t.Fatalf("publish build: %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && buildCalls.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if buildCalls.Load() != 1 {
		t.Fatalf("build handler calls = %d, want 1", buildCalls.Load())
	}
	if buildLinuxCalls.Load() != 1 {
		t.Fatalf(
			"build.linux handler calls = %d after a build dispatch, "+
				"want unchanged 1",
			buildLinuxCalls.Load(),
		)
	}
}
