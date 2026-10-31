package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultHeartbeatMax is how long the automatic heartbeat keeps one
// task's message alive unless WithHeartbeatMax says otherwise.
const DefaultHeartbeatMax = time.Hour

// WithHeartbeatMax caps how long the automatic heartbeat keeps one
// task's message alive. A step's timeout is optional, so without a cap a
// hung handler in a live process would hold its task forever; past the
// cap the worker stops heartbeating, logs a warning, and the task
// redelivers after AckWait. Set it above the longest handler you run.
// Panics unless d is positive.
func WithHeartbeatMax(d time.Duration) WorkerOption {
	if d <= 0 {
		panic("WithHeartbeatMax: d must be positive")
	}
	return func(w *Worker) {
		if w == nil {
			panic("WithHeartbeatMax: w must not be nil")
		}
		w.heartbeatMax = d
	}
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

// progressTicker sends InProgress on a message every half AckWait while
// its handler runs, so a handler that outlives AckWait is not redelivered
// and run a second time (MaxDeliver is unbounded). It ends when the
// handler returns, when the task's context ends (a cancelled run), when
// Stop abandons the handler after its drain timeout, or at the
// heartbeat cap (WithHeartbeatMax). After any but the first, AckWait
// redelivery resumes; the engine's step timeout, when the step has one,
// still fails a hung attempt on its own.
type progressTicker struct {
	// frozen counts the chaos hooks holding the task; ticks are skipped
	// while it is above zero (see Freeze).
	frozen        atomic.Int32
	stopRequested chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
}

// progressEnds is what, besides stop, ends a ticker.
type progressEnds struct {
	ctx       context.Context // the task's; Done on a cancelled run
	abandoned <-chan struct{} // closed when Stop gives up on handlers
	max       time.Duration   // the heartbeat cap
	atMax     func()          // runs once if the cap is reached
}

// startProgress starts the automatic heartbeat of the task tc runs from
// msg, or, under the withoutAutoHeartbeat seam, returns a ticker that is
// already stopped. At the cap it releases c, so this Worker runs the
// redelivery instead of skipping it as a duplicate.
func (w *Worker) startProgress(
	msg jetstream.Msg, l lane, tc *taskContext, c claim,
) *progressTicker {
	if msg == nil {
		panic("startProgress: msg must not be nil")
	}
	if l.ackWait <= 0 {
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
	ends := progressEnds{
		ctx: tc.ctx, abandoned: w.abandoned, max: w.heartbeatMax,
		atMax: func() {
			slog.Warn("task outlived the heartbeat cap; it will redeliver",
				"task_type", l.taskType, "run_id", tc.runID,
				"step_id", tc.stepID, "heartbeat_max", w.heartbeatMax)
			w.running.forget(c)
		},
	}
	go p.run(msg, l.ackWait/2, ends)
	return p
}

// run's loop is bounded: the cap ends it if nothing else does first.
func (p *progressTicker) run(
	msg jetstream.Msg, interval time.Duration, ends progressEnds,
) {
	if interval <= 0 || ends.max <= 0 {
		panic("progressTicker.run: interval and max must be positive")
	}
	defer close(p.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	capped := time.NewTimer(ends.max)
	defer capped.Stop()
	for {
		select {
		case <-p.stopRequested:
			return
		case <-ends.abandoned:
			return
		case <-ends.ctx.Done():
			return
		case <-capped.C:
			ends.atMax()
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
		panic("progressTicker.stop: not built by startProgress")
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
