package worker

import (
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
		l.park.parkOrNak(msg)
		return
	}
	if !w.beginHandler() {
		w.releaseClaim(l, tracked, c)
		<-slots
		l.park.parkOrNak(msg)
		return
	}
	go func() {
		defer func() { <-slots }()
		defer w.handlerWG.Done()
		defer w.releaseClaim(l, tracked, c)
		w.runMessage(l, tracked, c)
	}()
}
