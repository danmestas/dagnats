package worker

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// lane is what one consumer dispatches to: the task type, its handler,
// and the AckWait that consumer was created with, which the automatic
// heartbeat must beat.
type lane struct {
	taskType string
	handler  HandlerFunc
	ackWait  time.Duration
}

// progressTicker sends InProgress on a message every half AckWait while
// its handler runs, so a handler that outlives AckWait is not redelivered
// and run a second time (MaxDeliver is unbounded). It holds the message
// for as long as the handler runs: the engine's step timeout (a watchdog
// in SLEEP_TIMERS), not AckWait, is what bounds a hung handler, and a
// handler Stop abandons after its drain timeout stops heartbeating.
type progressTicker struct {
	// frozen counts the chaos hooks holding the task; ticks are skipped
	// while it is above zero (see Freeze).
	frozen        atomic.Int32
	stopRequested chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
}

// startProgress starts msg's automatic heartbeat, or, under the
// withoutAutoHeartbeat seam, returns a ticker that is already stopped.
func (w *Worker) startProgress(
	msg jetstream.Msg, ackWait time.Duration,
) *progressTicker {
	if msg == nil {
		panic("startProgress: msg must not be nil")
	}
	if ackWait <= 0 {
		panic("startProgress: ackWait must be positive")
	}
	p := &progressTicker{
		stopRequested: make(chan struct{}),
		done:          make(chan struct{}),
	}
	if w.autoHeartbeatOff {
		close(p.done)
		return p
	}
	go p.run(msg, ackWait/2, w.abandoned)
	return p
}

// withoutAutoHeartbeat turns the automatic heartbeat off, so a test can
// stage a stalled worker: a blocked handler in a frozen or partitioned
// process sends nothing, and its message redelivers after AckWait.
func withoutAutoHeartbeat() WorkerOption {
	return func(w *Worker) {
		if w == nil {
			panic("withoutAutoHeartbeat: w must not be nil")
		}
		w.autoHeartbeatOff = true
	}
}

// run's loop is bounded by the handler's lifetime: stop ends it, and so
// does Stop abandoning the handler.
func (p *progressTicker) run(
	msg jetstream.Msg, interval time.Duration, abandoned <-chan struct{},
) {
	defer close(p.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopRequested:
			return
		case <-abandoned:
			return
		case <-ticker.C:
			if p.frozen.Load() > 0 {
				continue
			}
			// A failed InProgress costs at most this tick; the next one
			// (or the handler's own Heartbeat) tries again.
			if err := msg.InProgress(); err != nil {
				slog.Debug("automatic heartbeat failed", "error", err)
			}
		}
	}
}

// stop ends the ticker and waits for its goroutine to exit, so no
// InProgress can follow the Ack, NAK or Term the caller sends next. An
// InProgress after Pause's delayed NAK would reset the redelivery timer
// to AckWait. Idempotent.
func (p *progressTicker) stop() {
	if p == nil {
		panic("progressTicker.stop: p must not be nil")
	}
	if p.done == nil {
		panic("progressTicker.stop: not built by startProgressTicker")
	}
	p.stopOnce.Do(func() { close(p.stopRequested) })
	<-p.done
}

// Freeze suspends the heartbeat until the matching Thaw; ticks in
// between are skipped, not queued. It implements chaos.Freezer, so a
// chaos stall freezes the task as a frozen process would be: one that
// sends no heartbeat, whose delivery lapses at AckWait.
func (p *progressTicker) Freeze() {
	if p == nil {
		panic("progressTicker.Freeze: p must not be nil")
	}
	if p.frozen.Add(1) < 1 {
		panic("progressTicker.Freeze: count went below one")
	}
}

func (p *progressTicker) Thaw() {
	if p == nil {
		panic("progressTicker.Thaw: p must not be nil")
	}
	if p.frozen.Add(-1) < 0 {
		panic("progressTicker.Thaw: not frozen")
	}
}

// deliveryKey names one stream message. Sequences are per stream, and a
// Worker consumes both TASK_QUEUES and STICKY_TASKS.
type deliveryKey struct {
	stream   string
	sequence uint64
}

// runningSet holds the messages this Worker is running, so a redelivery
// of one of them is not started a second time. Every entry belongs to a
// consume callback that is running the handler or waiting for a slot;
// a lane has at most its concurrency limit plus one, so capacity, the
// sum over lanes, bounds the set.
type runningSet struct {
	mu       sync.Mutex
	keys     map[deliveryKey]struct{}
	capacity int
}

// addLane grows capacity for one consumer lane of concurrency n. Called
// only while Start subscribes, before any delivery.
func (s *runningSet) addLane(n int) {
	if n < 1 {
		panic("runningSet.addLane: n must be >= 1")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[deliveryKey]struct{})
	}
	s.capacity += n + 1
}

// claim records msg as running. It returns false when msg is already
// running here. A message without JetStream metadata is not tracked: it
// gets the zero key, which release ignores.
func (s *runningSet) claim(msg jetstream.Msg) (deliveryKey, bool) {
	if msg == nil {
		panic("runningSet.claim: msg must not be nil")
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return deliveryKey{}, true
	}
	key := deliveryKey{stream: meta.Stream, sequence: meta.Sequence.Stream}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, running := s.keys[key]; running {
		return key, false
	}
	if len(s.keys) >= s.capacity {
		panic("runningSet.claim: more messages running than lanes allow")
	}
	s.keys[key] = struct{}{}
	return key, true
}

func (s *runningSet) release(key deliveryKey) {
	if key == (deliveryKey{}) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, running := s.keys[key]; !running {
		panic("runningSet.release: key was not claimed")
	}
	delete(s.keys, key)
}

// skipRunning drops a redelivery of a message this Worker is still
// running. No Ack: the running handler's outcome must be the one that
// settles the message. No NAK or Term either: the running handler's
// heartbeat keeps extending the deadline on the same stream sequence,
// so the server will not hand the message out again while it runs, and
// the running handler's own Ack, Fail or Pause settles it.
func skipRunning(taskType string, key deliveryKey) {
	if taskType == "" {
		panic("skipRunning: taskType must not be empty")
	}
	if key == (deliveryKey{}) {
		panic("skipRunning: key must name a message")
	}
	slog.Info("skipping redelivery of a task still running here",
		"task_type", taskType,
		"stream", key.stream,
		"stream_sequence", key.sequence,
	)
}
