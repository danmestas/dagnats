package worker

import (
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ConcurrencyMax bounds WithConcurrency. It stays below both the server's
// default MaxAckPending (1000) and nats.go's default pull batch (500), so
// every consumer this Worker creates already keeps n messages in flight
// without per-consumer tuning.
const ConcurrencyMax = 256

// WithConcurrency lets each consumer lane of the handler's type run up to
// n tasks at once (#842). A lane is one consumer: the pull consumer, the
// sticky consumer, each WithGroups group, each WithPartitions member. The
// default is 1 per lane, which is how every lane has always run. With
// n > 1 a lane's tasks no longer finish in delivery order, including
// within a partition. Panics unless 1 <= n <= ConcurrencyMax, and on a
// HandleSingleton type when n > 1.
func WithConcurrency(n int) HandlerOption {
	if n < 1 {
		panic("WithConcurrency: n must be >= 1")
	}
	if n > ConcurrencyMax {
		panic("WithConcurrency: n must be <= ConcurrencyMax")
	}
	return func(w *Worker, taskType string) {
		if w == nil {
			panic("WithConcurrency: w must not be nil")
		}
		if taskType == "" {
			panic("WithConcurrency: taskType must not be empty")
		}
		if w.singletons[taskType] && n > 1 {
			panic(singletonConcurrencyPanic(taskType))
		}
		if w.handlerConcurrency == nil {
			w.handlerConcurrency = make(map[string]int)
		}
		w.handlerConcurrency[taskType] = n
	}
}

// singletonConcurrencyPanic is the one message for a singleton given
// WithConcurrency above 1, whichever of the two registrations came last.
func singletonConcurrencyPanic(taskType string) string {
	if taskType == "" {
		panic("singletonConcurrencyPanic: taskType must not be empty")
	}
	return "singleton " + taskType + " cannot take WithConcurrency above 1"
}

// messageHandler returns the consume callback for one consumer lane. At
// the default of 1 the callback runs the task inline, exactly as before
// #842. Above 1 the lane gets its own n-slot semaphore: each call makes a
// fresh one, and each consumer calls this once.
func (w *Worker) messageHandler(l lane) func(jetstream.Msg) {
	if l.taskType == "" {
		panic("messageHandler: taskType must not be empty")
	}
	if l.handler == nil {
		panic("messageHandler: handler must not be nil")
	}
	n := max(w.handlerConcurrency[l.taskType], 1)
	w.running.addLane(n)
	if n == 1 {
		return func(msg jetstream.Msg) {
			w.handleMessage(l, msg)
		}
	}
	slots := make(chan struct{}, n)
	return func(msg jetstream.Msg) {
		w.dispatchConcurrent(l, msg, slots)
	}
}

// dispatchConcurrent takes a slot, then runs the task on its own
// goroutine. Blocking while every slot is busy is the back-pressure: the
// consumer takes no further message, as at n = 1, and nats.go pauses its
// heartbeat monitor while a callback runs. The handlerWG Add happens here,
// before the goroutine exists, so Stop's drain covers it. The running
// claim comes before the slot wait, so a redelivery that arrives while
// every slot is busy is skipped at once instead of running after the
// first delivery's ack.
func (w *Worker) dispatchConcurrent(
	l lane, msg jetstream.Msg, slots chan struct{},
) {
	if msg == nil {
		panic("dispatchConcurrent: msg must not be nil")
	}
	if cap(slots) < 2 {
		panic("dispatchConcurrent: slots must hold at least 2")
	}
	if w.stopping == nil {
		panic("dispatchConcurrent: worker not built by NewWorker")
	}
	c, v := w.running.claim(msg, time.Now())
	if v != verdictRun {
		skipDelivery(l.taskType, msg, c, v)
		return
	}
	tracked := &ackTracker{Msg: msg}
	select {
	case slots <- struct{}{}:
	case <-w.stopping:
		w.releaseClaim(l, tracked, c)
		w.parkOrNak(msg)
		return
	}
	if !w.beginHandler() {
		w.releaseClaim(l, tracked, c)
		<-slots
		w.parkOrNak(msg)
		return
	}
	go func() {
		defer func() { <-slots }()
		defer w.handlerWG.Done()
		defer w.releaseClaim(l, tracked, c)
		w.runMessage(l, tracked, c)
	}()
}

// stopParkedMax bounds the deliveries Stop holds back. It matches the
// server's default MaxAckPending, the most one consumer can have out
// unacked; past it a delivery is NAK'd at once, as before parking.
const stopParkedMax = 1000

// slotLaneCloseWait bounds how long Stop waits for the concurrent lanes'
// subscriptions to end before it NAKs what they parked anyway.
const slotLaneCloseWait = 5 * time.Second

// stopPark holds the deliveries concurrent lanes turned away during Stop.
type stopPark struct {
	mu       sync.Mutex
	msgs     []jetstream.Msg
	released bool
}

// parkOrNak holds msg for releaseParked instead of NAKing it now. A NAK
// while this Worker's pull is still open on the server can redeliver
// the message straight back to it, and nats.go drops what reaches a
// stopped consumer, stranding the task until AckWait. Once the park is
// released, or full, msg is NAK'd at once.
func (w *Worker) parkOrNak(msg jetstream.Msg) {
	if msg == nil {
		panic("parkOrNak: msg must not be nil")
	}
	p := &w.stopPark
	p.mu.Lock()
	if !p.released && len(p.msgs) < stopParkedMax {
		p.msgs = append(p.msgs, msg)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	_ = msg.Nak()
}

// releaseParked NAKs the parked deliveries once the server no longer
// holds a pull from any concurrent lane, so each redelivery goes to a
// live Worker. A concurrent lane's callback never blocks once stopping
// is closed, so its subscription ends and Closed fires without waiting
// on handlers. The flush makes the server process the unsubscribes
// before the NAKs that follow.
func (w *Worker) releaseParked() {
	if w.nc == nil {
		panic("releaseParked: nc must not be nil")
	}
	if w.stopping == nil {
		panic("releaseParked: worker not built by NewWorker")
	}
	if !w.awaitSlotLanesClosed() {
		slog.Warn("worker.Stop: concurrent lane did not close in time; " +
			"NAKing parked deliveries anyway")
	}
	if err := w.nc.FlushTimeout(slotLaneCloseWait); err != nil {
		slog.Warn("worker.Stop: flush before NAKing parked deliveries failed",
			"error", err)
	}
	p := &w.stopPark
	p.mu.Lock()
	msgs := p.msgs
	p.msgs = nil
	p.released = true
	p.mu.Unlock()
	for _, msg := range msgs {
		_ = msg.Nak()
	}
}

// awaitSlotLanesClosed reports whether every concurrent lane's
// subscription ended within slotLaneCloseWait.
func (w *Worker) awaitSlotLanesClosed() bool {
	if !w.draining {
		panic("awaitSlotLanesClosed: draining must be set first")
	}
	if len(w.slotLanes) > len(w.stoppers) {
		panic("awaitSlotLanesClosed: every slot lane must be a stopper")
	}
	deadline := time.NewTimer(slotLaneCloseWait)
	defer deadline.Stop()
	for _, l := range w.slotLanes {
		select {
		case <-l.Closed():
		case <-deadline.C:
			return false
		}
	}
	return true
}
