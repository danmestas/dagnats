// worker/sticky_recover_test.go
// Methodology: a real embedded server and a started Worker. Each test
// deletes the Worker's sticky consumer out from under it, as the server
// does once the consumer's InactiveThreshold lapses, then checks the
// Worker recreates it under the same name and that a sticky-bound task
// reaches the handler within a bounded wait, while a task it already
// handled is not replayed. The deletion lands two ways: with the pull
// outstanding (the server answers 409 Consumer Deleted, which ends
// nats.go's Consume) and while the Worker is disconnected (the next
// pull gets no responders, which Consume only reports). A last test
// stops the Worker while a recovery waits out its backoff.
package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// stickyRecoverWait bounds every wait for a recovered delivery.
const stickyRecoverWait = 15 * time.Second

// startStickyWorker starts a Worker handling taskType that reports each
// run ID it handles on the returned channel.
func startStickyWorker(
	t *testing.T, nc *nats.Conn, taskType string, tune func(*Worker),
) (*Worker, <-chan string) {
	t.Helper()
	handled := make(chan string, 16)
	w := NewWorker(nc)
	w.Handle(taskType, func(tc TaskContext) error {
		handled <- tc.RunID()
		return nil
	})
	if tune != nil {
		tune(w)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(w.Stop)
	return w, handled
}

// stickyConsumerName returns the one consumer on STICKY_TASKS.
func stickyConsumerName(t *testing.T, js jetstream.JetStream) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "STICKY_TASKS")
	if err != nil {
		t.Fatalf("STICKY_TASKS: %v", err)
	}
	names := stream.ListConsumers(ctx)
	var found []string
	for info := range names.Info() {
		found = append(found, info.Name)
	}
	if err := names.Err(); err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("STICKY_TASKS has consumers %v, want exactly 1", found)
	}
	return found[0]
}

// awaitHandled waits for runID, failing on any other run or the bound.
func awaitHandled(t *testing.T, handled <-chan string, runID string) {
	t.Helper()
	select {
	case got := <-handled:
		if got != runID {
			t.Fatalf("handled run %q, want %q (a replay?)", got, runID)
		}
	case <-time.After(stickyRecoverWait):
		t.Fatalf("run %q not handled within %v", runID, stickyRecoverWait)
	}
}

func publishSticky(t *testing.T, js jetstream.JetStream, w *Worker, taskType, runID string) {
	t.Helper()
	subject := "sticky." + taskType + "." + w.workerID + "." + runID
	publishConcurrencySubject(t, js, subject, runID)
}

func deleteConsumer(t *testing.T, js jetstream.JetStream, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := js.DeleteConsumer(ctx, "STICKY_TASKS", name); err != nil {
		t.Fatalf("delete sticky consumer %s: %v", name, err)
	}
}

func TestStickyConsumer_RecreatedAfterDeletion(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	const taskType = "probe.recreate"
	w, handled := startStickyWorker(t, nc, taskType, nil)
	name := stickyConsumerName(t, js)
	publishSticky(t, js, w, taskType, "run-before")
	awaitHandled(t, handled, "run-before")

	deleteConsumer(t, js, name)
	publishSticky(t, js, w, taskType, "run-after")

	// The task published after the deletion arrives, and the one
	// handled before it is not replayed ahead of it.
	awaitHandled(t, handled, "run-after")
	if got := stickyConsumerName(t, js); got != name {
		t.Fatalf("recreated consumer %q, want the same name %q", got, name)
	}
	// The Worker skips a recent redelivery itself, so count the server's
	// deliveries: a replay of run-before would make two.
	if delivered := stickyDeliveredCount(t, js, name); delivered != 1 {
		t.Fatalf("recreated consumer delivered %d messages, want 1", delivered)
	}
	select {
	case extra := <-handled:
		t.Fatalf("unexpected extra delivery of %q", extra)
	case <-time.After(300 * time.Millisecond):
	}
}

// A task published in the gap, after the reap and before the recreate,
// is delivered whether or not the consumer had delivered anything: with
// nothing delivered the recreate keeps DeliverAll (the filter names this
// Worker's random ID, so nothing older matches), and after a delivery
// it starts just past that delivery, which is still before the gap.
func TestStickyConsumer_RecreatedAfterDeletionWhileDisconnected(t *testing.T) {
	t.Run("never delivered", func(t *testing.T) {
		checkDisconnectedRecreate(t, false)
	})
	t.Run("delivered before", func(t *testing.T) {
		checkDisconnectedRecreate(t, true)
	})
}

// reconnectGate holds a client out of its reconnect until released, so
// a test controls exactly how long the disconnect gap lasts instead of
// racing a reconnect timer against a loaded runner.
type reconnectGate struct {
	disconnected chan struct{}
	released     chan struct{}
	once         sync.Once
}

func (g *reconnectGate) release() { g.once.Do(func() { close(g.released) }) }

// awaitDisconnected waits until the client has noticed the server
// dropped it; before that, IsConnected can still read true.
func (g *reconnectGate) awaitDisconnected(t *testing.T) {
	t.Helper()
	select {
	case <-g.disconnected:
	case <-time.After(stickyRecoverWait):
		t.Fatalf("worker did not see its disconnect within %v", stickyRecoverWait)
	}
}

// connectGatedWorker connects a client whose reconnect attempts block
// in the delay callback until the gate is released. The callback runs
// before every attempt and holds no client lock, so blocking is safe;
// cleanup releases it first so Close never waits on the gate.
func connectGatedWorker(t *testing.T, url string) (*nats.Conn, *reconnectGate) {
	t.Helper()
	gate := &reconnectGate{
		disconnected: make(chan struct{}, 1),
		released:     make(chan struct{}),
	}
	nc, err := nats.Connect(url,
		nats.CustomReconnectDelay(func(int) time.Duration {
			<-gate.released
			return 0
		}),
		nats.DisconnectErrHandler(func(*nats.Conn, error) {
			select {
			case gate.disconnected <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("connect worker: %v", err)
	}
	t.Cleanup(nc.Close)
	t.Cleanup(gate.release)
	return nc, gate
}

func checkDisconnectedRecreate(t *testing.T, deliveredBefore bool) {
	t.Helper()
	srv, admin := natstest.StartServer(t)
	if err := natsutil.SetupAll(admin); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	nc, gate := connectGatedWorker(t, srv.ClientURL())
	const taskType = "probe.noresponders"
	w, handled := startStickyWorker(t, nc, taskType, nil)
	name := stickyConsumerName(t, js)
	if deliveredBefore {
		publishSticky(t, js, w, taskType, "run-before")
		awaitHandled(t, handled, "run-before")
	}

	clientID, err := nc.GetClientID()
	if err != nil {
		t.Fatalf("client id: %v", err)
	}
	if err := srv.DisconnectClientByID(clientID); err != nil {
		t.Fatalf("disconnect worker: %v", err)
	}
	gate.awaitDisconnected(t)
	deleteConsumer(t, js, name)
	publishSticky(t, js, w, taskType, "run-gap")
	// The gate, not a timer, holds the Worker out, so this cannot lose
	// a race on a slow runner.
	if nc.IsConnected() {
		t.Fatal("worker reconnected before the gap publish; the test proves nothing")
	}
	gate.release()

	awaitHandled(t, handled, "run-gap")
	if got := stickyConsumerName(t, js); got != name {
		t.Fatalf("recreated consumer %q, want the same name %q", got, name)
	}
	// Exactly the gap task: nothing lost, and run-before not replayed.
	if delivered := stickyDeliveredCount(t, js, name); delivered != 1 {
		t.Fatalf("recreated consumer delivered %d messages, want 1", delivered)
	}
}

func TestStickyConsumer_StopDuringRecoveryBackoffIsPrompt(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	const taskType = "probe.stopbackoff"
	w, _ := startStickyWorker(t, nc, taskType, func(w *Worker) {
		w.stickyRetryMin = time.Minute
	})
	sticky := stickyLaneOf(t, w)
	awaitStickyPull(t, js, stickyConsumerName(t, js))

	// Without the stream every recovery attempt fails, so the lane
	// waits out its minute-long backoff.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := js.DeleteStream(ctx, "STICKY_TASKS"); err != nil {
		t.Fatalf("delete STICKY_TASKS: %v", err)
	}
	deadline := time.Now().Add(stickyRecoverWait)
	for sticky.attempts.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sticky.attempts.Load() < 1 {
		t.Fatal("deleting the stream started no recovery attempt")
	}

	begun := time.Now()
	w.Stop()
	if took := time.Since(begun); took > 3*time.Second {
		t.Fatalf("Stop took %v during a recovery backoff", took)
	}
	select {
	case <-sticky.exited:
	case <-time.After(time.Second):
		t.Fatal("sticky recovery goroutine outlived Stop")
	}
}

// stickyLaneOf returns the Worker's one sticky lane.
func stickyLaneOf(t *testing.T, w *Worker) *stickyConsume {
	t.Helper()
	var found []*stickyConsume
	for _, l := range w.lanes {
		if s, ok := l.consume.(*stickyConsume); ok {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("worker has %d sticky lanes, want 1", len(found))
	}
	return found[0]
}

// stickyConsumerInfo fetches the named STICKY_TASKS consumer's info.
func stickyConsumerInfo(
	t *testing.T, js jetstream.JetStream, name string,
) *jetstream.ConsumerInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := js.Consumer(ctx, "STICKY_TASKS", name)
	if err != nil {
		t.Fatalf("sticky consumer %s: %v", name, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("sticky consumer info %s: %v", name, err)
	}
	return info
}

func stickyDeliveredCount(t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()
	return stickyConsumerInfo(t, js, name).Delivered.Consumer
}

// awaitStickyPull waits until the Worker's pull is outstanding, so a
// deletion reaches it as a 409 rather than racing the first pull.
func awaitStickyPull(t *testing.T, js jetstream.JetStream, name string) {
	t.Helper()
	deadline := time.Now().Add(stickyRecoverWait)
	for stickyConsumerInfo(t, js, name).NumWaiting < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("no pull outstanding on %s within %v", name, stickyRecoverWait)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStickyConsumer_RecoveredAfterAFailedPass: a recovery pass can run
// out of attempts while the consumer cannot be recreated, here because
// its stream is gone. By then Consume has ended, so no further consume
// error arrives to start another pass. Recovery must try again on its
// own, or the lane stays dead until the Worker restarts.
func TestStickyConsumer_RecoveredAfterAFailedPass(t *testing.T) {
	nc, js := startConcurrencyServer(t)
	const taskType = "probe.failedpass"
	const passAttempts = 3
	w, handled := startStickyWorker(t, nc, taskType, func(w *Worker) {
		w.stickyRetryMin = 20 * time.Millisecond
		w.stickyRecoverAttempts = passAttempts
	})
	sticky := stickyLaneOf(t, w)
	stickyConsumerName(t, js)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := js.DeleteStream(ctx, "STICKY_TASKS"); err != nil {
		t.Fatalf("delete sticky stream: %v", err)
	}
	// Wait until at least one whole pass has failed.
	deadline := time.Now().Add(stickyRecoverWait)
	for sticky.attempts.Load() < int64(passAttempts) {
		if time.Now().After(deadline) {
			t.Fatalf("recovery made %d attempts, want a full pass of %d",
				sticky.attempts.Load(), passAttempts)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if err := natsutil.SetupStickyStream(js, 1); err != nil {
		t.Fatalf("SetupStickyStream: %v", err)
	}

	// Positive: once the stream is back, a later pass recreates the
	// consumer and a sticky task reaches the handler.
	publishSticky(t, js, w, taskType, "run-after-pass")
	awaitHandled(t, handled, "run-after-pass")
	// Negative: recovery did not stop at the first failed pass.
	if got := sticky.attempts.Load(); got <= int64(passAttempts) {
		t.Fatalf("recovery made %d attempts, want more than one pass", got)
	}
}
