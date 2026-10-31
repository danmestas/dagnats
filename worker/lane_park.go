package worker

import (
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// laneParkedMax bounds the deliveries one lane holds back during Stop.
// It matches the server's default MaxAckPending: a consumer cannot have
// more unacked deliveries out than that, so only a consumer configured
// above it can overflow, and an overflowing delivery is NAK'd at once.
const laneParkedMax = 1000

// laneCloseGrace is how long past the handler drain Stop waits for a
// lane's subscription to finish draining before NAKing what it parked.
const laneCloseGrace = 5 * time.Second

// lanePark holds the deliveries one consumer lane turns away while its
// Worker stops. A NAK sent while the lane's pull is still open on the
// server can redeliver the message straight back to this lane, which
// turns it away again, burning a delivery attempt each round, until the
// pull ends and the last redelivery strands until AckWait. Parked
// deliveries are NAK'd only once the lane's subscription has drained, so
// each redelivery goes to a live worker.
//
// A nil *lanePark NAKs at once: a lane with no drain signal to wait on.
type lanePark struct {
	mu       sync.Mutex
	msgs     []jetstream.Msg
	released bool
}

// parkOrNak holds msg until release, or NAKs it now when the park is
// nil, already released, or full.
func (p *lanePark) parkOrNak(msg jetstream.Msg) {
	if msg == nil {
		panic("parkOrNak: msg must not be nil")
	}
	if p != nil {
		p.mu.Lock()
		if !p.released && len(p.msgs) < laneParkedMax {
			p.msgs = append(p.msgs, msg)
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
	}
	_ = msg.Nak()
}

// release NAKs every parked delivery and makes later ones NAK at once.
func (p *lanePark) release() {
	if p == nil {
		panic("release: park must not be nil")
	}
	p.mu.Lock()
	msgs := p.msgs
	p.msgs = nil
	p.released = true
	p.mu.Unlock()
	if len(msgs) > laneParkedMax {
		panic("release: park holds more than laneParkedMax")
	}
	for _, msg := range msgs {
		_ = msg.Nak()
	}
}

// drainingLane is a consumer lane Stop drains rather than stops, so the
// messages nats.go already buffered still reach the callback, where they
// are parked instead of discarded.
type drainingLane struct {
	consume jetstream.ConsumeContext
	park    *lanePark
}

// trackLane records a jetstream lane for Stop as soon as it consumes.
func (w *Worker) trackLane(consume jetstream.ConsumeContext, park *lanePark) {
	if consume == nil {
		panic("trackLane: consume must not be nil")
	}
	if park == nil {
		panic("trackLane: park must not be nil")
	}
	w.lanes = append(w.lanes, drainingLane{consume: consume, park: park})
}

// releaseLanes starts one goroutine per lane that NAKs the lane's parked
// deliveries once its subscription has drained. Closed fires after core
// nats has flushed the unsubscribe, so the server holds no pull from the
// lane by then. Per lane, so a lane still running an inline handler does
// not hold back the others. A lane whose drain outlives an abandoned
// handler, or the grace past the drain timeout, is released anyway.
func (w *Worker) releaseLanes() {
	if !w.draining {
		panic("releaseLanes: draining must be set first")
	}
	if w.abandoned == nil {
		panic("releaseLanes: worker not built by NewWorker")
	}
	wait := w.laneReleaseWait()
	for _, l := range w.lanes {
		w.laneReleases.Add(1)
		go func() {
			defer w.laneReleases.Done()
			timer := time.NewTimer(wait)
			defer timer.Stop()
			select {
			case <-l.consume.Closed():
			case <-w.abandoned:
			case <-timer.C:
				slog.Warn("worker.Stop: lane did not drain in time; "+
					"NAKing its parked deliveries anyway",
					"worker_id", w.workerID)
			}
			l.park.release()
		}()
	}
}

// laneReleaseWait bounds each lane release: the handler drain bound plus
// laneCloseGrace, since an inline handler can delay its lane's drain.
func (w *Worker) laneReleaseWait() time.Duration {
	wait := w.effectiveDrainTimeout() + laneCloseGrace
	if wait <= laneCloseGrace {
		panic("laneReleaseWait: drain bound must be positive")
	}
	return wait
}
