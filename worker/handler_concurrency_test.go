// worker/handler_concurrency_test.go
// Methodology: regression guard for #842. Each test runs a real embedded
// NATS server and one Worker, publishes several tasks of a single task
// type, and holds every handler on a release channel so the number of
// handlers in flight at once can be counted. The counter is an atomic
// gauge plus a high-water mark. Every wait is bounded. Positive space: n
// handlers overlap, results and checkpoints stay with their own task, and
// Stop drains them all. Negative space: the default never overlaps, and
// the high-water mark never passes n.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

const concurrencyWaitBound = 5 * time.Second

// inflightGauge counts handlers running now and the most ever running.
type inflightGauge struct {
	now  atomic.Int64
	peak atomic.Int64
}

func (g *inflightGauge) enter() {
	current := g.now.Add(1)
	for {
		peak := g.peak.Load()
		if current <= peak || g.peak.CompareAndSwap(peak, current) {
			return
		}
	}
}

func (g *inflightGauge) exit() { g.now.Add(-1) }

// blockingHandler records entry on started, checkpoints its own run ID,
// waits for release, and completes with an output naming its run. A
// checkpoint read back as another run's state lands on mixed.
func blockingHandler(
	gauge *inflightGauge, started chan<- string, release <-chan struct{},
	mixed *atomic.Int64,
) HandlerFunc {
	return func(tc TaskContext) error {
		gauge.enter()
		defer gauge.exit()
		if err := tc.Checkpoint([]byte(tc.RunID())); err != nil {
			return err
		}
		started <- tc.RunID()
		select {
		case <-release:
		case <-time.After(2 * concurrencyWaitBound):
			return fmt.Errorf("handler for %s never released", tc.RunID())
		}
		state, err := tc.LoadCheckpoint()
		if err != nil {
			return err
		}
		if string(state) != tc.RunID() {
			mixed.Add(1)
		}
		return tc.Complete([]byte(`"out-` + tc.RunID() + `"`))
	}
}

func startConcurrencyServer(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

func publishConcurrencyTask(
	t *testing.T, js jetstream.JetStream, taskType, runID string,
) {
	t.Helper()
	payload := protocol.TaskPayload{
		TaskID: runID + ".s", RunID: runID, StepID: "s",
		Input: json.RawMessage(`"go"`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	subject := "task." + taskType + "." + runID
	if _, err := js.Publish(context.Background(), subject, data); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}

// awaitStarts waits for count handler entries, failing after the bound.
func awaitStarts(t *testing.T, started <-chan string, count int) []string {
	t.Helper()
	runs := make([]string, 0, count)
	deadline := time.After(concurrencyWaitBound)
	for len(runs) < count {
		select {
		case run := <-started:
			runs = append(runs, run)
		case <-deadline:
			t.Fatalf("only %d of %d handlers started within %v",
				len(runs), count, concurrencyWaitBound)
		}
	}
	return runs
}

// awaitCompletedOutput returns the output of runID's step.completed event.
func awaitCompletedOutput(t *testing.T, nc *nats.Conn, runID string) string {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unsubscribe: %v", err)
		}
	}()
	deadline := time.Now().Add(concurrencyWaitBound)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if evt.Type == protocol.EventStepCompleted {
			return string(evt.Payload)
		}
	}
	t.Fatalf("no step.completed for %s within %v", runID, concurrencyWaitBound)
	return ""
}

// awaitConsumerSettled waits until the task type's durable has nothing
// pending and nothing awaiting an ack: every message was acked once.
func awaitConsumerSettled(t *testing.T, js jetstream.JetStream, taskType string) {
	t.Helper()
	ctx := context.Background()
	durable := consumerNameFor(taskType, "")
	deadline := time.Now().Add(concurrencyWaitBound)
	for time.Now().Before(deadline) {
		cons, err := js.Consumer(ctx, "TASK_QUEUES", durable)
		if err != nil {
			t.Fatalf("Consumer %s: %v", durable, err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			t.Fatalf("Info %s: %v", durable, err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("consumer %s still has unacked work after %v",
		durable, concurrencyWaitBound)
}

func TestWithConcurrency_RunsNHandlersAtOnce(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var mixed atomic.Int64
	started := make(chan string, 3)
	release := make(chan struct{})

	w := NewWorker(nc)
	w.Handle("probe.block",
		blockingHandler(&gauge, started, release, &mixed),
		WithConcurrency(3))
	w.Start()
	defer w.Stop()

	runs := []string{"run-c1", "run-c2", "run-c3"}
	for _, run := range runs {
		publishConcurrencyTask(t, js, "probe.block", run)
	}
	awaitStarts(t, started, len(runs))
	if peak := gauge.peak.Load(); peak != 3 {
		close(release)
		t.Fatalf("peak in-flight handlers = %d, want 3", peak)
	}
	close(release)

	for _, run := range runs {
		got := awaitCompletedOutput(t, nc, run)
		if want := `"out-` + run + `"`; got != want {
			t.Fatalf("run %s completed with %s, want %s", run, got, want)
		}
	}
	if n := mixed.Load(); n != 0 {
		t.Fatalf("%d handlers read another task's checkpoint", n)
	}
	awaitConsumerSettled(t, js, "probe.block")
}

func TestHandle_DefaultRunsOneAtATime(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var mixed atomic.Int64
	started := make(chan string, 2)
	release := make(chan struct{})

	w := NewWorker(nc)
	w.Handle("probe.block", blockingHandler(&gauge, started, release, &mixed))
	w.Start()
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.block", "run-d1")
	publishConcurrencyTask(t, js, "probe.block", "run-d2")
	awaitStarts(t, started, 1)
	select {
	case run := <-started:
		close(release)
		t.Fatalf("second handler (%s) started while the first was blocked", run)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	awaitStarts(t, started, 1)
	if peak := gauge.peak.Load(); peak != 1 {
		t.Fatalf("peak in-flight handlers = %d, want 1", peak)
	}
	awaitConsumerSettled(t, js, "probe.block")
}

func TestWithConcurrency_NeverExceedsN(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var done atomic.Int64
	const taskCount = 8

	w := NewWorker(nc)
	w.Handle("probe.burst", func(tc TaskContext) error {
		gauge.enter()
		time.Sleep(100 * time.Millisecond)
		gauge.exit()
		defer done.Add(1)
		return tc.Complete([]byte(`"ok"`))
	}, WithConcurrency(2))
	w.Start()
	defer w.Stop()

	for i := range taskCount {
		publishConcurrencyTask(t, js, "probe.burst", fmt.Sprintf("run-b%d", i))
	}
	deadline := time.Now().Add(2 * concurrencyWaitBound)
	for done.Load() < taskCount && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := done.Load(); got != taskCount {
		t.Fatalf("%d of %d tasks finished", got, taskCount)
	}
	if peak := gauge.peak.Load(); peak != 2 {
		t.Fatalf("peak in-flight handlers = %d, want exactly 2", peak)
	}
	awaitConsumerSettled(t, js, "probe.burst")
}

func TestWithConcurrency_StopWaitsForEveryHandler(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var mixed atomic.Int64
	var returned atomic.Int64
	started := make(chan string, 3)
	release := make(chan struct{})

	inner := blockingHandler(&gauge, started, release, &mixed)
	w := NewWorker(nc)
	w.Handle("probe.block", func(tc TaskContext) error {
		defer returned.Add(1)
		return inner(tc)
	}, WithConcurrency(3))
	w.Start()

	for _, run := range []string{"run-s1", "run-s2", "run-s3"} {
		publishConcurrencyTask(t, js, "probe.block", run)
	}
	awaitStarts(t, started, 3)

	stopped := make(chan int64, 1)
	go func() {
		w.Stop()
		stopped <- returned.Load()
	}()
	select {
	case <-stopped:
		close(release)
		t.Fatal("Stop returned while handlers were still blocked")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case count := <-stopped:
		if count != 3 {
			t.Fatalf("Stop returned with %d of 3 handlers finished", count)
		}
	case <-time.After(concurrencyWaitBound):
		t.Fatal("Stop did not return after handlers were released")
	}
}

func TestWithConcurrency_PartitionedMember(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var mixed atomic.Int64
	started := make(chan string, 3)
	release := make(chan struct{})

	w := NewWorker(nc, WithPartitions(2))
	w.Handle("probe.part",
		blockingHandler(&gauge, started, release, &mixed),
		WithConcurrency(3))
	w.Start()
	defer w.Stop()
	defer close(release)

	for _, run := range []string{"run-p1", "run-p2", "run-p3"} {
		publishConcurrencyTask(t, js, "probe.part", run)
	}
	awaitStarts(t, started, 3)
	if peak := gauge.peak.Load(); peak != 3 {
		t.Fatalf("peak in-flight handlers = %d, want 3", peak)
	}
}

func TestWithConcurrency_Bounds(t *testing.T) {
	for _, n := range []int{-1, 0, ConcurrencyMax + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("WithConcurrency(%d) did not panic", n)
				}
			}()
			WithConcurrency(n)
		}()
	}
	if WithConcurrency(1) == nil || WithConcurrency(ConcurrencyMax) == nil {
		t.Fatal("WithConcurrency must accept 1 and ConcurrencyMax")
	}
}

func TestWithConcurrency_RejectsSingleton(t *testing.T) {
	nc, _ := startConcurrencyServer(t)
	w := NewWorker(nc)
	noop := func(tc TaskContext) error { return tc.Complete(nil) }
	w.Handle("probe.one", noop, WithConcurrency(2))
	w.HandleSingleton("probe.one", noop)
	defer func() {
		if recover() == nil {
			t.Fatal("Start with a concurrent singleton did not panic")
		}
	}()
	w.Start()
}
