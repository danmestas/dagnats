package worker

import (
	"github.com/nats-io/nats.go/jetstream"
)

// ConcurrencyMax bounds WithConcurrency. It stays below both the server's
// default MaxAckPending (1000) and nats.go's default pull batch (500), so
// every consumer this Worker creates already keeps n messages in flight
// without per-consumer tuning.
const ConcurrencyMax = 256

// WithConcurrency lets up to n tasks of the handler's type run at once in
// this Worker (#842). The default is 1: one task of a type at a time.
// With n > 1 tasks of that type no longer finish in delivery order,
// including within a partition under WithPartitions. Panics unless
// 1 <= n <= ConcurrencyMax.
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
		if w.handlerConcurrency == nil {
			w.handlerConcurrency = make(map[string]int)
		}
		w.handlerConcurrency[taskType] = n
	}
}

// messageHandler returns the consume callback for taskType. At the
// default of 1 the callback runs the task inline, exactly as before #842.
// Above 1 every consumer of the type (pull, sticky, per-group, elastic)
// shares one semaphore, so n bounds the type across the whole Worker.
// Called only from Start, which runs single-threaded.
func (w *Worker) messageHandler(
	taskType string, handler HandlerFunc,
) func(jetstream.Msg) {
	if taskType == "" {
		panic("messageHandler: taskType must not be empty")
	}
	if handler == nil {
		panic("messageHandler: handler must not be nil")
	}
	n := w.handlerConcurrency[taskType]
	if w.singletons[taskType] && n > 1 {
		panic("messageHandler: singleton " + taskType +
			" cannot take WithConcurrency above 1")
	}
	if n <= 1 {
		return func(msg jetstream.Msg) {
			w.handleMessage(taskType, handler, msg)
		}
	}
	if w.handlerSlots == nil {
		w.handlerSlots = make(map[string]chan struct{})
	}
	slots, ok := w.handlerSlots[taskType]
	if !ok {
		slots = make(chan struct{}, n)
		w.handlerSlots[taskType] = slots
	}
	return func(msg jetstream.Msg) {
		w.dispatchConcurrent(taskType, handler, msg, slots)
	}
}

// dispatchConcurrent takes a slot, then runs the task on its own
// goroutine. Blocking while every slot is busy is the back-pressure: the
// consumer takes no further message, as at n = 1, and nats.go pauses its
// heartbeat monitor while a callback runs. The handlerWG Add happens here,
// before the goroutine exists, so Stop's drain covers it.
func (w *Worker) dispatchConcurrent(
	taskType string, handler HandlerFunc, msg jetstream.Msg,
	slots chan struct{},
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
	select {
	case slots <- struct{}{}:
	case <-w.stopping:
		_ = msg.Nak()
		return
	}
	if !w.beginHandler() {
		<-slots
		_ = msg.Nak()
		return
	}
	go func() {
		defer func() { <-slots }()
		defer w.handlerWG.Done()
		w.runMessage(taskType, handler, msg)
	}()
}
