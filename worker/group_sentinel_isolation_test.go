// worker/group_sentinel_isolation_test.go
// Methodology: real embedded NATS server, two real Workers driven
// through the public API (WithGroups + Handle, exactly how a production
// deployment wires a grouped worker), publishing directly onto the two
// subjects StepSubject derives (internal/engine/task_publisher.go).
// This is the headline proof for #704: a dotted task type carrying a
// worker group ("dagger.call", group "fast") and the CONCATENATED
// ungrouped task type ("dagger.call.fast") used to derive the
// byte-identical filter subject and durable name — the entire
// motivation for the "=" sentinel. Both consumers now live
// simultaneously and each receives only its own tasks. Bounded 10s
// poll loop.
package worker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestGroupSentinelIsolatesDottedTaskFromConcatenatedUngrouped(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	var groupedCount, ungroupedCount atomic.Int32
	var groupedSeen, ungroupedSeen sync.Map

	// Grouped: task type "dagger.call" (dotted, a production shape --
	// call: always compiles to it), worker group "fast". StepSubject
	// derives "task.dagger.call.=fast.{runID}"; consumerFilterFor derives
	// the matching "task.dagger.call.=fast.*" filter.
	groupedWorker := NewWorker(nc, WithGroups("fast"))
	groupedWorker.Handle("dagger.call", func(tc TaskContext) error {
		groupedCount.Add(1)
		groupedSeen.Store(tc.RunID(), true)
		return tc.Complete([]byte(`"ok"`))
	})
	groupedWorker.Start()
	t.Cleanup(groupedWorker.Stop)

	// Ungrouped: the CONCATENATED dotted task type "dagger.call.fast" --
	// before #704's sentinel this derived the SAME filter subject and
	// durable name as the grouped pair above.
	ungroupedWorker := NewWorker(nc)
	ungroupedWorker.Handle("dagger.call.fast", func(tc TaskContext) error {
		ungroupedCount.Add(1)
		ungroupedSeen.Store(tc.RunID(), true)
		return tc.Complete([]byte(`"ok"`))
	})
	ungroupedWorker.Start()
	t.Cleanup(ungroupedWorker.Stop)

	publish := func(subject, runID string) {
		t.Helper()
		payload := protocol.TaskPayload{
			RunID: runID, StepID: "s", Input: json.RawMessage(`"x"`),
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		if _, err := js.Publish(context.Background(), subject, data); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}

	publish("task.dagger.call.=fast.grouped-run", "grouped-run")
	publish("task.dagger.call.fast.ungrouped-run", "ungrouped-run")

	deadline := time.After(10 * time.Second)
	for groupedCount.Load() < 1 || ungroupedCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatalf(
				"timed out: grouped=%d ungrouped=%d",
				groupedCount.Load(), ungroupedCount.Load(),
			)
		case <-time.After(50 * time.Millisecond):
		}
	}

	// A settle window: if either consumer's filter over-matched (the
	// exact bug #704 removes), the misdelivered message would also
	// arrive in this window, not just the intended one.
	time.Sleep(300 * time.Millisecond)

	if groupedCount.Load() != 1 {
		t.Fatalf("groupedCount = %d, want exactly 1 (no cross-delivery)",
			groupedCount.Load())
	}
	if ungroupedCount.Load() != 1 {
		t.Fatalf("ungroupedCount = %d, want exactly 1 (no cross-delivery)",
			ungroupedCount.Load())
	}
	if _, ok := groupedSeen.Load("grouped-run"); !ok {
		t.Fatal("grouped worker never received grouped-run")
	}
	if _, ok := groupedSeen.Load("ungrouped-run"); ok {
		t.Fatal("grouped worker received ungrouped-run -- filter collision")
	}
	if _, ok := ungroupedSeen.Load("ungrouped-run"); !ok {
		t.Fatal("ungrouped worker never received ungrouped-run")
	}
	if _, ok := ungroupedSeen.Load("grouped-run"); ok {
		t.Fatal("ungrouped worker received grouped-run -- filter collision")
	}

	// Consumer topology: distinct durables for the two pairs, matching
	// consumername.NameFor's "="-in-name distinction.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := stream.Consumer(ctx, "workers-dagger-call-=fast"); err != nil {
		t.Fatalf("expected durable workers-dagger-call-=fast: %v", err)
	}
	if _, err := stream.Consumer(ctx, "workers-dagger-call-fast"); err != nil {
		t.Fatalf("expected durable workers-dagger-call-fast: %v", err)
	}
}
