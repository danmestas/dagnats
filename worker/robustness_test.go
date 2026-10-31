// worker/robustness_test.go
// Methodology: each test runs a real embedded NATS server. AckWait is
// shortened to one second so a handler that outlives it is cheap to
// stage. Start's failure paths are driven by a missing stream and by a
// durable whose immutable config refuses the update; the leak check is
// the client's live subscription count. The heartbeat and the guard are
// judged by counted handler executions over several AckWaits, and the
// ticker by the order of InProgress and Ack calls on a recording
// message. Positive space: one execution, an error from Start, a final
// ack after the last InProgress. Negative space: no second execution, no
// subscription left behind, no InProgress after the ack.
package worker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const robustAckWait = 1 * time.Second

func TestStart_ReturnsErrorWhenStreamMissing(t *testing.T) {
	_, nc := natstest.StartServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	baseline := settledSubscriptions(t, js)
	w := NewWorker(nc)
	w.Handle("render", func(TaskContext) error { return nil })

	err = w.Start()
	if err == nil {
		w.Stop()
		t.Fatal("Start returned nil with no TASK_QUEUES stream")
	}
	if !strings.Contains(err.Error(), "TASK_QUEUES") {
		t.Fatalf("Start error does not name the stream: %v", err)
	}
	awaitSubscriptions(t, nc, baseline)
}

// settledSubscriptions makes one JetStream request, so the connection's
// shared reply subscription exists, and returns the count with it.
func settledSubscriptions(t *testing.T, js jetstream.JetStream) int {
	t.Helper()
	if _, err := js.AccountInfo(context.Background()); err != nil {
		t.Fatalf("AccountInfo: %v", err)
	}
	return js.Conn().NumSubscriptions()
}

// awaitSubscriptions waits for the count to fall to want: nats.go ends a
// stopped consumer's subscription on its own goroutine.
func awaitSubscriptions(t *testing.T, nc *nats.Conn, want int) {
	t.Helper()
	deadline := time.Now().Add(concurrencyWaitBound)
	for nc.NumSubscriptions() != want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := nc.NumSubscriptions(); n != want {
		t.Fatalf("failed Start left %d subscriptions open, want %d", n, want)
	}
}

// TestStart_StopsStartedConsumersOnFailure starts "a-ok" before the
// failing "b-bad" (Start subscribes in task-type order), so the rollback
// has a live consumer to stop.
func TestStart_StopsStartedConsumersOnFailure(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	ctx := context.Background()
	// MaxWaiting cannot be updated, so the worker's CreateOrUpdate of
	// this durable fails with a server error, not a panic.
	_, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES",
		jetstream.ConsumerConfig{
			Durable:       consumerNameFor("b-bad", ""),
			FilterSubject: consumerFilterFor("b-bad", ""),
			AckPolicy:     jetstream.AckExplicitPolicy,
			MaxWaiting:    7,
		})
	if err != nil {
		t.Fatalf("seed b-bad durable: %v", err)
	}
	baseline := settledSubscriptions(t, js)
	w := NewWorker(nc)
	w.Handle("a-ok", func(TaskContext) error { return nil })
	w.Handle("b-bad", func(TaskContext) error { return nil })

	err = w.Start()
	if err == nil {
		w.Stop()
		t.Fatal("Start returned nil although b-bad cannot subscribe")
	}
	if !strings.Contains(err.Error(), "b-bad") {
		t.Fatalf("Start error does not name the failing consumer: %v", err)
	}
	awaitSubscriptions(t, nc, baseline)
}

// countingHandler counts executions, holds for hold, then completes.
func countingHandler(runs *atomic.Int64, hold time.Duration) HandlerFunc {
	return func(tc TaskContext) error {
		runs.Add(1)
		time.Sleep(hold)
		return tc.Complete([]byte(`"done"`))
	}
}

// TestHeartbeat_LongHandlerRunsOnce runs one lane (no WithConcurrency),
// so a redelivery could only queue behind the handler, never meet the
// guard: one execution here is the heartbeat's doing alone.
func TestHeartbeat_LongHandlerRunsOnce(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	w := NewWorker(nc)
	w.Handle("probe.slow", countingHandler(&runs, 3*robustAckWait),
		WithAckWait(robustAckWait))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.slow", "run-hb")
	if got := awaitCompletedOutput(t, nc, "run-hb"); got != `"done"` {
		t.Fatalf("completed output = %s, want \"done\"", got)
	}
	// A redelivery made during the run would land within an AckWait of
	// the ack; wait past that before counting.
	time.Sleep(2 * robustAckWait)
	if n := runs.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1", n)
	}
	awaitConsumerSettled(t, js, "probe.slow")
}

// TestGuard_SkipsRedeliveryWhileRunning forces an immediate redelivery
// by publishing a raw NAK to the running message's reply subject, which
// sidesteps the client's already-acked flag on the original. With two
// slots the redelivery arrives while the first run holds one.
func TestGuard_SkipsRedeliveryWhileRunning(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	w := NewWorker(nc)
	w.Handle("probe.dup", func(tc TaskContext) error {
		if runs.Add(1) == 1 {
			reply := tc.(*taskContext).msg.Reply()
			if err := nc.Publish(reply, []byte("-NAK")); err != nil {
				return err
			}
		}
		time.Sleep(robustAckWait + robustAckWait/2)
		return tc.Complete([]byte(`"done"`))
	}, WithAckWait(robustAckWait), WithConcurrency(2))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.dup", "run-dup")
	if got := awaitCompletedOutput(t, nc, "run-dup"); got != `"done"` {
		t.Fatalf("completed output = %s, want \"done\"", got)
	}
	time.Sleep(2 * robustAckWait)
	if n := runs.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1", n)
	}
	awaitConsumerSettled(t, js, "probe.dup")
}

// recordingMsg logs InProgress and Ack calls, in order, then forwards
// them to the real message. ackHold keeps Ack open after it records, so
// a ticker still running at the moment of the ack records after it.
type recordingMsg struct {
	jetstream.Msg
	mu      sync.Mutex
	events  []string
	ackHold time.Duration
}

func (m *recordingMsg) record(event string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

func (m *recordingMsg) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.events...)
}

func (m *recordingMsg) InProgress() error {
	m.record("progress")
	return m.Msg.InProgress()
}

func (m *recordingMsg) Ack() error {
	m.record("ack")
	time.Sleep(m.ackHold)
	return m.Msg.Ack()
}

func (m *recordingMsg) NakWithDelay(delay time.Duration) error {
	m.record("nak")
	return m.Msg.NakWithDelay(delay)
}

func fetchOneTask(
	t *testing.T, js jetstream.JetStream, taskType string,
) jetstream.Msg {
	t.Helper()
	ctx := context.Background()
	cons, err := js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES",
		jetstream.ConsumerConfig{
			Durable:       consumerNameFor(taskType, ""),
			FilterSubject: consumerFilterFor(taskType, ""),
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       robustAckWait,
		})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(concurrencyWaitBound))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	for msg := range batch.Messages() {
		return msg
	}
	t.Fatalf("fetch returned no message: %v", batch.Error())
	return nil
}

func TestHeartbeat_StopsBeforeAck(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	publishConcurrencyTask(t, js, "probe.tick", "run-tick")
	msg := &recordingMsg{
		Msg: fetchOneTask(t, js, "probe.tick"), ackHold: robustAckWait,
	}
	var runs atomic.Int64
	w := NewWorker(nc)
	handler := countingHandler(&runs, 2*robustAckWait)
	w.Handle("probe.tick", handler)

	w.runMessage(lane{
		taskType: "probe.tick", handler: handler, ackWait: robustAckWait,
	}, msg, claim{})
	settled := msg.snapshot()
	time.Sleep(robustAckWait)
	later := msg.snapshot()

	if len(settled) < 3 || settled[len(settled)-1] != "ack" {
		t.Fatalf("events = %v, want several progress then a final ack",
			settled)
	}
	if len(later) != len(settled) {
		t.Fatalf("events after the ack: %v", later[len(settled):])
	}
}

// TestHeartbeat_StopsBeforePauseNak: an InProgress after Pause's delayed
// NAK would reset the server's redelivery timer to AckWait, cutting the
// pause short.
func TestHeartbeat_StopsBeforePauseNak(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	publishConcurrencyTask(t, js, "probe.pause", "run-pause")
	msg := &recordingMsg{Msg: fetchOneTask(t, js, "probe.pause")}
	w := NewWorker(nc)
	handler := func(tc TaskContext) error {
		time.Sleep(2 * robustAckWait)
		err := tc.Pause("nap", time.Minute)
		time.Sleep(robustAckWait)
		return err
	}
	w.Handle("probe.pause", handler)
	w.bindOptionalKV()

	w.runMessage(lane{
		taskType: "probe.pause", handler: handler, ackWait: robustAckWait,
	}, msg, claim{})
	events := msg.snapshot()

	if len(events) < 2 || events[len(events)-1] != "nak" {
		t.Fatalf("events = %v, want progress then a final nak", events)
	}
	for _, event := range events[:len(events)-1] {
		if event != "progress" {
			t.Fatalf("events = %v, want only progress before the nak", events)
		}
	}
}

// TestHeartbeat_ManualHeartbeatStillWorks keeps ctx.Heartbeat working
// alongside the automatic ticker.
func TestHeartbeat_ManualHeartbeatStillWorks(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	heartbeatErrs := make(chan error, 1)
	w := NewWorker(nc)
	w.Handle("probe.manual", func(tc TaskContext) error {
		heartbeatErrs <- tc.Heartbeat()
		return tc.Complete([]byte(`"done"`))
	}, WithAckWait(robustAckWait))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.manual", "run-manual")
	if got := awaitCompletedOutput(t, nc, "run-manual"); got != `"done"` {
		t.Fatalf("completed output = %s, want \"done\"", got)
	}
	select {
	case err := <-heartbeatErrs:
		if err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}
	case <-time.After(concurrencyWaitBound):
		t.Fatal("handler never heartbeat")
	}
}

// awaitRuns waits for runs to reach want, failing after bound.
func awaitRuns(t *testing.T, runs *atomic.Int64, want int64, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for runs.Load() < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runs.Load(); n != want {
		t.Fatalf("handler ran %d times within %s, want %d", n, bound, want)
	}
}

// hangFirst returns a handler whose first run hangs until release and
// whose later runs complete at once.
func hangFirst(runs *atomic.Int64, release <-chan struct{}) HandlerFunc {
	return func(tc TaskContext) error {
		if runs.Add(1) == 1 {
			select {
			case <-release:
			case <-time.After(4 * concurrencyWaitBound):
			}
			return nil
		}
		return tc.Complete([]byte(`"done"`))
	}
}

// TestHeartbeat_CapLetsAHungHandlerRedeliver: past WithHeartbeatMax the
// worker stops heartbeating and stops treating the hung run as running,
// so the AckWait redelivery runs in the second slot.
func TestHeartbeat_CapLetsAHungHandlerRedeliver(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	w := NewWorker(nc, WithHeartbeatMax(robustAckWait+robustAckWait/2))
	w.Handle("probe.hung", hangFirst(&runs, release),
		WithAckWait(robustAckWait), WithConcurrency(2))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.hung", "run-hung")
	awaitRuns(t, &runs, 1, concurrencyWaitBound)
	awaitRuns(t, &runs, 2, concurrencyWaitBound)
}

// TestHeartbeat_StopsWhenTheRunIsCancelled: a handler that ignores its
// cancelled context must not keep its message alive.
func TestHeartbeat_StopsWhenTheRunIsCancelled(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	publishConcurrencyTask(t, js, "probe.cancel", "run-cancel")
	msg := &recordingMsg{Msg: fetchOneTask(t, js, "probe.cancel")}
	w := NewWorker(nc)
	handler := func(tc TaskContext) error {
		time.Sleep(3 * robustAckWait)
		return nil
	}
	w.Handle("probe.cancel", handler)
	go func() {
		time.Sleep(robustAckWait + robustAckWait/10)
		msg.record("cancel")
		w.cancels.cancelRun("run-cancel")
	}()

	w.runMessage(lane{
		taskType: "probe.cancel", handler: handler, ackWait: robustAckWait,
	}, msg, claim{})
	events := msg.snapshot()

	before, after := 0, 0
	seenCancel := false
	for _, event := range events {
		switch {
		case event == "cancel":
			seenCancel = true
		case event == "progress" && seenCancel:
			after++
		case event == "progress":
			before++
		}
	}
	if !seenCancel || before < 1 {
		t.Fatalf("events = %v, want progress before a cancel", events)
	}
	// One InProgress may already be in flight as the cancel lands.
	if after > 1 {
		t.Fatalf("events = %v, want the heartbeat to stop at the cancel", events)
	}
}

// TestGuard_SkipsADeliveryQueuedBehindTheRun: in a lane of one a
// redelivery made during the run waits in the client until the run has
// acked; it must not run again then.
func TestGuard_SkipsADeliveryQueuedBehindTheRun(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	w := NewWorker(nc)
	w.Handle("probe.queued", func(tc TaskContext) error {
		if runs.Add(1) == 1 {
			reply := tc.(*taskContext).msg.Reply()
			if err := nc.Publish(reply, []byte("-NAK")); err != nil {
				return err
			}
		}
		time.Sleep(robustAckWait + robustAckWait/2)
		return tc.Complete([]byte(`"done"`))
	}, WithAckWait(robustAckWait))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.queued", "run-queued")
	if got := awaitCompletedOutput(t, nc, "run-queued"); got != `"done"` {
		t.Fatalf("completed output = %s, want \"done\"", got)
	}
	time.Sleep(2 * robustAckWait)
	if n := runs.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1", n)
	}
	awaitConsumerSettled(t, js, "probe.queued")
}

// TestHeartbeat_StopAbandoningAHandlerLetsItRedeliver: once Stop's drain
// times out, the stopped worker's heartbeat must not keep the task from
// the next worker.
func TestHeartbeat_StopAbandoningAHandlerLetsItRedeliver(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	handler := hangFirst(&runs, release)
	first := NewWorker(nc)
	first.drainTimeout = 100 * time.Millisecond
	first.Handle("probe.abandon", handler, WithAckWait(robustAckWait))
	if err := first.Start(); err != nil {
		t.Fatalf("Start first: %v", err)
	}
	publishConcurrencyTask(t, js, "probe.abandon", "run-abandon")
	awaitRuns(t, &runs, 1, concurrencyWaitBound)
	first.Stop()

	second := NewWorker(nc)
	second.Handle("probe.abandon", handler, WithAckWait(robustAckWait))
	if err := second.Start(); err != nil {
		t.Fatalf("Start second: %v", err)
	}
	defer second.Stop()
	awaitRuns(t, &runs, 2, concurrencyWaitBound)
}

// TestHeartbeat_MaxRunTimeOverridesTheWorkerDefault: a handler with
// WithMaxRunTime keeps heartbeating past a lower worker default, then
// is freed at its own cap.
func TestHeartbeat_MaxRunTimeOverridesTheWorkerDefault(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	w := NewWorker(nc, WithHeartbeatMax(robustAckWait))
	w.Handle("probe.long", hangFirst(&runs, release),
		WithAckWait(robustAckWait), WithConcurrency(2),
		WithMaxRunTime(4*robustAckWait))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.long", "run-long")
	awaitRuns(t, &runs, 1, concurrencyWaitBound)
	// The worker default would free it at 1 s and redeliver by 2 s.
	time.Sleep(3 * robustAckWait)
	if n := runs.Load(); n != 1 {
		t.Fatalf("handler ran %d times before its own max run time, want 1", n)
	}
	awaitRuns(t, &runs, 2, concurrencyWaitBound)
}

func TestWithMaxRunTime_Bounds(t *testing.T) {
	expectPanic(t, "WithMaxRunTime: d must be positive", func() {
		WithMaxRunTime(0)
	})
	expectPanic(t, "WithMaxRunTime: d must be <= MaxRunTimeMax", func() {
		WithMaxRunTime(MaxRunTimeMax + time.Second)
	})
}

// TestHeartbeat_CancelFreesAHungHandlersTask: a cancelled run's handler
// that hangs must not have its redeliveries skipped as duplicates.
func TestHeartbeat_CancelFreesAHungHandlersTask(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	var runs atomic.Int64
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	w := NewWorker(nc)
	w.Handle("probe.cancelhang", hangFirst(&runs, release),
		WithAckWait(robustAckWait), WithConcurrency(2))
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	publishConcurrencyTask(t, js, "probe.cancelhang", "run-cancelhang")
	awaitRuns(t, &runs, 1, concurrencyWaitBound)
	w.cancels.cancelRun("run-cancelhang")
	awaitRuns(t, &runs, 2, concurrencyWaitBound)
}

// metaMsg is a message that only answers Metadata.
type metaMsg struct {
	jetstream.Msg
	meta jetstream.MsgMetadata
}

func (m *metaMsg) Metadata() (*jetstream.MsgMetadata, error) { return &m.meta, nil }

// TestRunningSet_RecreatedStreamReusesSequences: a recreated stream's
// message with an acked one's sequence is a different message.
func TestRunningSet_RecreatedStreamReusesSequences(t *testing.T) {
	var s runningSet
	s.addLane(1)
	now := time.Now()
	published := func(at time.Time) *metaMsg {
		meta := jetstream.MsgMetadata{Stream: "STICKY_TASKS", Timestamp: at}
		meta.Sequence.Stream = 7
		return &metaMsg{meta: meta}
	}
	c, v := s.claim(published(now), now)
	if v != verdictRun {
		t.Fatalf("first claim verdict = %v, want run", v)
	}
	s.release(c, true, time.Minute, now)

	if _, v := s.claim(published(now), now); v != verdictSettled {
		t.Errorf("same message again: verdict = %v, want settled", v)
	}
	if _, v := s.claim(published(now.Add(time.Second)), now); v != verdictRun {
		t.Errorf("reused sequence, new message: verdict = %v, want run", v)
	}
}
