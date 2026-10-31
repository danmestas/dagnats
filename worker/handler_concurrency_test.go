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
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/danmestas/dagnats/internal/natstest"
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
	_, nc := natstest.StartServer(t)
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
	publishConcurrencySubject(t, js, "task."+taskType+"."+runID, runID)
}

func publishConcurrencySubject(
	t *testing.T, js jetstream.JetStream, subject, runID string,
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

// expectPanic runs fn and fails unless it panics with a message holding want.
func expectPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		got := fmt.Sprint(recover())
		if !strings.Contains(got, want) {
			t.Fatalf("panic = %q, want one containing %q", got, want)
		}
	}()
	fn()
}

func TestWithConcurrency_RejectsSingletonAtRegistration(t *testing.T) {
	nc, _ := startConcurrencyServer(t)
	noop := func(tc TaskContext) error { return tc.Complete(nil) }
	const want = "singleton probe.one cannot take WithConcurrency above 1"

	first := NewWorker(nc)
	first.Handle("probe.one", noop, WithConcurrency(2))
	expectPanic(t, want, func() { first.HandleSingleton("probe.one", noop) })

	second := NewWorker(nc)
	second.HandleSingleton("probe.one", noop)
	expectPanic(t, want, func() {
		second.Handle("probe.one", noop, WithConcurrency(2))
	})

	// Negative space: n = 1 is the default lane and stays allowed.
	third := NewWorker(nc)
	third.HandleSingleton("probe.one", noop)
	third.Handle("probe.one", noop, WithConcurrency(1))
}

// TestWithConcurrency_PerLane pins n as a per-consumer limit: three group
// lanes at n = 2 run six tasks at once, and no lane passes two.
func TestWithConcurrency_PerLane(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	groups := []string{"ga", "gb", "gc"}
	lanes := map[string]*inflightGauge{}
	for _, group := range groups {
		lanes[group] = &inflightGauge{}
	}
	var total inflightGauge
	started := make(chan string, 9)
	release := make(chan struct{})

	w := NewWorker(nc, WithGroups(groups...))
	w.Handle("probe.lane", func(tc TaskContext) error {
		lane := lanes[strings.Split(tc.RunID(), "-")[1]]
		lane.enter()
		total.enter()
		defer lane.exit()
		defer total.exit()
		started <- tc.RunID()
		<-release
		return tc.Complete([]byte(`"ok"`))
	}, WithConcurrency(2))
	w.Start()
	defer w.Stop()
	defer close(release)

	for _, group := range groups {
		for i := range 3 {
			run := fmt.Sprintf("run-%s-%d", group, i)
			subject := "task.probe.lane." + consumername.GroupSentinel +
				group + "." + run
			publishConcurrencySubject(t, js, subject, run)
		}
	}
	awaitStarts(t, started, 6)
	select {
	case run := <-started:
		t.Fatalf("seventh handler (%s) started; want at most 2 per lane", run)
	case <-time.After(500 * time.Millisecond):
	}
	if peak := total.peak.Load(); peak != 6 {
		t.Fatalf("peak in flight across lanes = %d, want 6", peak)
	}
	for group, lane := range lanes {
		if peak := lane.peak.Load(); peak != 2 {
			t.Fatalf("lane %s peaked at %d, want 2", group, peak)
		}
	}
}

// TestWithConcurrency_StopReleasesSlotWaiter: with both slots held, the
// third delivery waits in the consume callback. Stop must make that
// waiter NAK at once, while the two handlers are still running, so a
// live worker picks the task up instead of it sitting parked until a
// slot frees or AckWait expires.
func TestWithConcurrency_StopReleasesSlotWaiter(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var mixed atomic.Int64
	started := make(chan string, 3)
	release := make(chan struct{})

	stopping := NewWorker(nc)
	stopping.Handle("probe.wait",
		blockingHandler(&gauge, started, release, &mixed),
		WithConcurrency(2))
	stopping.Start()
	for _, run := range []string{"run-w1", "run-w2", "run-w3"} {
		publishConcurrencyTask(t, js, "probe.wait", run)
	}
	held := awaitStarts(t, started, 2)

	stopped := make(chan struct{})
	go func() {
		stopping.Stop()
		close(stopped)
	}()
	took := make(chan string, 3)
	live := NewWorker(nc)
	live.Handle("probe.wait", func(tc TaskContext) error {
		took <- tc.RunID()
		return tc.Complete([]byte(`"ok"`))
	})
	live.Start()
	defer live.Stop()

	select {
	case run := <-took:
		close(release)
		for _, h := range held {
			if h == run {
				t.Fatalf("live worker took %s, which was still running", run)
			}
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("slot waiter did not NAK while Stop drained the running handlers")
	}
	select {
	case <-stopped:
	case <-time.After(concurrencyWaitBound):
		t.Fatal("Stop did not return after handlers were released")
	}
	if peak := gauge.peak.Load(); peak != 2 {
		t.Fatalf("peak in-flight handlers = %d, want 2", peak)
	}
}

// drainSignalLane stands in for a jetstream ConsumeContext: Closed fires
// only when the test says the lane's subscription has drained.
type drainSignalLane struct {
	jetstream.ConsumeContext
	closed chan struct{}
}

func (l *drainSignalLane) Stop()                   {}
func (l *drainSignalLane) Drain()                  {}
func (l *drainSignalLane) Closed() <-chan struct{} { return l.closed }

// nakRecordingMsg records each Nak on naks instead of sending it.
type nakRecordingMsg struct {
	jetstream.Msg
	naks chan struct{}
}

func (m *nakRecordingMsg) Nak() error {
	m.naks <- struct{}{}
	return nil
}

// TestStopNaksParkedOnlyAfterLaneDrains pins the ordering behind
// StopReleasesSlotWaiter: a delivery a lane turned away during Stop is
// NAK'd only once that lane's subscription has drained. A NAK sent while
// the pull is still open hands the redelivery straight back to the
// stopping Worker. A delivery turned away after the release is NAK'd at
// once, since nothing would release it later.
func TestStopNaksParkedOnlyAfterLaneDrains(t *testing.T) {
	nc, _ := startConcurrencyServer(t)
	w := NewWorker(nc)
	consume := &drainSignalLane{closed: make(chan struct{})}
	park := &lanePark{}
	w.trackLane(consume, park)
	parked := &nakRecordingMsg{naks: make(chan struct{}, 1)}
	park.parkOrNak(parked)

	stopped := make(chan struct{})
	go func() {
		w.Stop()
		close(stopped)
	}()
	select {
	case <-parked.naks:
		t.Fatal("parked delivery NAK'd while its lane's pull was still open")
	case <-time.After(200 * time.Millisecond):
	}
	close(consume.closed)
	select {
	case <-parked.naks:
	case <-time.After(concurrencyWaitBound):
		t.Fatal("parked delivery not NAK'd after its lane drained")
	}
	select {
	case <-stopped:
	case <-time.After(concurrencyWaitBound):
		t.Fatal("Stop did not return")
	}

	late := &nakRecordingMsg{naks: make(chan struct{}, 1)}
	park.parkOrNak(late)
	select {
	case <-late.naks:
	default:
		t.Fatal("delivery turned away after release was parked, not NAK'd")
	}
}

// TestStop_RegistersEveryJetstreamLane pins that the lanes Stop drains
// are the consumers Start really creates: the pull consumer plus the
// sticky consumer, or one pull consumer per group (groups take no
// sticky consumer).
func TestStop_RegistersEveryJetstreamLane(t *testing.T) {
	nc, _ := startConcurrencyServer(t)
	plain := NewWorker(nc)
	plain.Handle("probe.count", func(tc TaskContext) error { return nil },
		WithConcurrency(2))
	plain.Start()
	defer plain.Stop()
	if got := len(plain.lanes); got != 2 {
		t.Fatalf("plain worker lanes = %d, want 2 (pull + sticky)", got)
	}
	grouped := NewWorker(nc, WithGroups("a", "b", "c"))
	grouped.Handle("probe.count", func(tc TaskContext) error { return nil })
	grouped.Start()
	defer grouped.Stop()
	if got := len(grouped.lanes); got != 3 {
		t.Fatalf("grouped worker lanes = %d, want 3 (one per group)", got)
	}
	if len(plain.stoppers) != 0 || len(grouped.stoppers) != 0 {
		t.Fatal("a jetstream lane was registered as a plain stopper")
	}
}

// TestStop_BacklogRedeliversPromptly: six tasks reach the stopping
// Worker's client buffer while only n can run. Stop must hand every one
// of the 6-n waiting in the buffer to a live worker well inside AckWait
// (5 minutes), each redelivered exactly once, so a draining NAK never
// loops back through this Worker burning delivery attempts. Above n = 1
// that happens while the held handlers still run; at n = 1 the buffer
// sits behind the inline handler, so it happens once that returns.
func TestStop_BacklogRedeliversPromptly(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			runStopBacklog(t, n)
		})
	}
}

func runStopBacklog(t *testing.T, n int) {
	const total = 6
	nc, js := startConcurrencyServer(t)
	var gauge inflightGauge
	var mixed atomic.Int64
	started := make(chan string, total)
	release := make(chan struct{})
	var opts []HandlerOption
	if n > 1 {
		opts = append(opts, WithConcurrency(n))
	}
	stopping := NewWorker(nc)
	stopping.Handle("probe.backlog",
		blockingHandler(&gauge, started, release, &mixed), opts...)
	stopping.Start()
	for i := range total {
		publishConcurrencyTask(t, js, "probe.backlog", fmt.Sprintf("run-b%d", i))
	}
	held := awaitStarts(t, started, n)
	awaitAckPending(t, js, consumerNameFor("probe.backlog", ""), total)

	stopped := make(chan struct{})
	go func() {
		stopping.Stop()
		close(stopped)
	}()
	took := make(chan string, total)
	live := NewWorker(nc)
	live.Handle("probe.backlog", func(tc TaskContext) error {
		took <- tc.RunID()
		return tc.Complete([]byte(`"ok"`))
	}, WithConcurrency(total))
	live.Start()
	defer live.Stop()
	if n == 1 {
		close(release)
	}
	got := awaitStarts(t, took, total-n)
	if n > 1 {
		close(release)
	}
	for _, run := range got {
		if slices.Contains(held, run) {
			t.Fatalf("live worker took %s, which was still running", run)
		}
	}
	select {
	case <-stopped:
	case <-time.After(concurrencyWaitBound):
		t.Fatal("Stop did not return after handlers were released")
	}
	assertDeliveredOnce(t, js, consumerNameFor("probe.backlog", ""), total, total-n)
}

// TestStop_DrainingNakDoesNotLoop: Stop sets draining before it drains
// the consumers, with a directory round trip between, so a delivery can
// reach an idle callback while the pull is still live. NAK'd then, it
// redelivers to the same callback, which NAKs it again, burning delivery
// attempts until the pull ends. The test holds that window open by
// setting draining itself: the delivery must be delivered once, parked,
// and redelivered once to a live worker after Stop drains the lane.
func TestStop_DrainingNakDoesNotLoop(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			runDrainingNak(t, n)
		})
	}
}

func runDrainingNak(t *testing.T, n int) {
	nc, js := startConcurrencyServer(t)
	var opts []HandlerOption
	if n > 1 {
		opts = append(opts, WithConcurrency(n))
	}
	stopping := NewWorker(nc)
	stopping.Handle("probe.loop", func(tc TaskContext) error {
		return tc.Complete([]byte(`"stopping"`))
	}, opts...)
	stopping.Start()
	stopping.drainMu.Lock()
	stopping.draining = true
	stopping.drainMu.Unlock()
	publishConcurrencyTask(t, js, "probe.loop", "run-loop")
	name := consumerNameFor("probe.loop", "")
	awaitAckPending(t, js, name, 1)
	time.Sleep(300 * time.Millisecond)
	if got := taskConsumerInfo(t, js, name).Delivered.Consumer; got != 1 {
		t.Fatalf("deliveries while draining = %d, want 1: the NAK looped", got)
	}

	took := make(chan string, 1)
	live := NewWorker(nc)
	live.Handle("probe.loop", func(tc TaskContext) error {
		took <- tc.RunID()
		return tc.Complete([]byte(`"ok"`))
	})
	live.Start()
	defer live.Stop()
	stopping.Stop()
	awaitStarts(t, took, 1)
	assertDeliveredOnce(t, js, name, 1, 1)
}

// TestStop_LaneReleaseSharesDrainBound: an unset drainTimeout falls
// back to defaultDrainTimeout for the handler drain, and the lane
// releases must wait on that same bound, not on zero plus the grace.
func TestStop_LaneReleaseSharesDrainBound(t *testing.T) {
	nc, _ := startConcurrencyServer(t)
	w := NewWorker(nc)
	w.drainTimeout = 0
	if got := w.effectiveDrainTimeout(); got != defaultDrainTimeout {
		t.Fatalf("drain bound = %v, want %v", got, defaultDrainTimeout)
	}
	if got, want := w.laneReleaseWait(), defaultDrainTimeout+laneCloseGrace; got != want {
		t.Fatalf("lane release wait = %v, want %v", got, want)
	}
	w.drainTimeout = 3 * time.Second
	if got, want := w.laneReleaseWait(), 3*time.Second+laneCloseGrace; got != want {
		t.Fatalf("lane release wait = %v, want %v", got, want)
	}
}

// taskConsumerInfo reads a TASK_QUEUES consumer's state.
func taskConsumerInfo(
	t *testing.T, js jetstream.JetStream, name string,
) *jetstream.ConsumerInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), concurrencyWaitBound)
	defer cancel()
	cons, err := js.Consumer(ctx, "TASK_QUEUES", name)
	if err != nil {
		t.Fatalf("consumer %s: %v", name, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer %s info: %v", name, err)
	}
	return info
}

// awaitAckPending waits until the consumer has handed out want messages,
// so every task sits in the Worker's client buffer before Stop.
func awaitAckPending(t *testing.T, js jetstream.JetStream, name string, want int) {
	t.Helper()
	const polls_max = 250
	for range polls_max {
		if taskConsumerInfo(t, js, name).NumAckPending == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("consumer %s never had %d messages out", name, want)
}

// assertDeliveredOnce checks the consumer delivered each of total tasks
// once, plus exactly one redelivery for each of redelivered, and that
// none is left unacked.
func assertDeliveredOnce(
	t *testing.T, js jetstream.JetStream, name string, total, redelivered int,
) {
	t.Helper()
	const polls_max = 250
	var info *jetstream.ConsumerInfo
	for range polls_max {
		info = taskConsumerInfo(t, js, name)
		if info.NumAckPending == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if info.NumAckPending != 0 {
		t.Fatalf("%d tasks still unacked after Stop", info.NumAckPending)
	}
	if got, want := info.Delivered.Consumer, uint64(total+redelivered); got != want {
		t.Fatalf("deliveries = %d, want %d: a NAK looped back", got, want)
	}
}
