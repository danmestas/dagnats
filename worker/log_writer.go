// worker/log_writer.go
// logLane buffers and publishes one task's captured stdout/stderr to the
// BUILD_LOGS hot lane (#624, subject logs.{runID}.{stepID}). One lane
// per task, created lazily on first LogOut()/LogErr() Write so a handler
// that never logs never spins up a ticker goroutine. Complete, Continue,
// Fail, and FailPermanent all drain the lane (flush pending bytes,
// stop the ticker) before publishing their resolution event — the
// drain-before-resolve invariant — so a consumer that observes the
// terminal event can never race ahead of the log bytes that produced it.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/metric"
)

// logStepBytesMax is the effective per-step total-bytes budget across
// both streams. A package var (not a bare reference to
// protocol.LogStepBytesMax) so tests can shrink it and exercise the
// truncation path without writing 64 MiB per test; production code
// never overrides it.
var logStepBytesMax int64 = protocol.LogStepBytesMax

// logFlushAfter is how long a buffer's oldest unflushed byte may sit
// before the ticker flushes it. logFlushCheckInterval is the ticker's
// own period — well below logFlushAfter so polling adds only small
// jitter to the 250ms deadline, while still being a SINGLE ticker per
// task rather than a timer per write.
const (
	logFlushAfter         = 250 * time.Millisecond
	logFlushCheckInterval = 25 * time.Millisecond
)

// logPublishTimeout bounds each individual chunk publish so a wedged
// NATS round trip cannot hang the ticker goroutine or a Write() call
// indefinitely.
const logPublishTimeout = 5 * time.Second

// logStreamBuf accumulates bytes for one stream (out or err) between
// flushes. pendingSince is the zero time when nothing is buffered.
type logStreamBuf struct {
	data         []byte
	pendingSince time.Time
}

// logLane owns the buffered publish pipeline for one task's logs.
type logLane struct {
	mu  sync.Mutex
	tp  *natsutil.TracingPublisher
	ctx context.Context

	subject string
	runID   string
	stepID  string

	seq uint64
	out logStreamBuf
	err logStreamBuf

	totalBytes int64
	truncated  bool
	terminal   bool

	ticker *time.Ticker
	stopCh chan struct{}
	doneCh chan struct{}

	failures metric.Int64Counter
}

// newLogLane starts the lane's background flush ticker and returns it.
// Callers must eventually call drain() or failDrain() exactly once to
// stop the ticker; leaving a lane un-drained leaks its goroutine.
func newLogLane(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	runID, stepID string,
	failures metric.Int64Counter,
) *logLane {
	if tp == nil {
		panic("newLogLane: tp must not be nil")
	}
	if runID == "" {
		panic("newLogLane: runID must not be empty")
	}
	lane := &logLane{
		tp:       tp,
		ctx:      ctx,
		subject:  "logs." + runID + "." + natsutil.SubjectToken(stepID),
		runID:    runID,
		stepID:   stepID,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		failures: failures,
		ticker:   time.NewTicker(logFlushCheckInterval),
	}
	go lane.run()
	return lane
}

// run is the lane's single background goroutine: it wakes on the
// shared ticker (never per write) and flushes any buffer whose oldest
// unflushed byte has aged past logFlushAfter.
func (l *logLane) run() {
	defer close(l.doneCh)
	for {
		select {
		case <-l.ticker.C:
			l.mu.Lock()
			l.flushAgedLocked()
			l.mu.Unlock()
		case <-l.stopCh:
			l.ticker.Stop()
			return
		}
	}
}

func (l *logLane) flushAgedLocked() {
	now := time.Now()
	if !l.out.pendingSince.IsZero() &&
		now.Sub(l.out.pendingSince) >= logFlushAfter {
		l.flushLocked(protocol.LogStreamOut, &l.out)
	}
	if !l.err.pendingSince.IsZero() &&
		now.Sub(l.err.pendingSince) >= logFlushAfter {
		l.flushLocked(protocol.LogStreamErr, &l.err)
	}
}

// writeOut and writeErr are the two io.Writer entry points LogOut()
// and LogErr() return. They never return a non-nil error: a Write()
// on an io.Writer that fails would break ordinary handler usage
// (fmt.Fprintln, log.Logger) over a best-effort side channel. A
// publish failure is logged and counted instead (see publish).
func (l *logLane) writeOut(p []byte) (int, error) {
	return l.write(protocol.LogStreamOut, &l.out, p)
}

func (l *logLane) writeErr(p []byte) (int, error) {
	return l.write(protocol.LogStreamErr, &l.err, p)
}

// write appends p to buf, honoring the step-wide byte budget and the
// per-chunk size cap, flushing as either boundary is crossed. The loop
// bound is len(p)/LogChunkBytesMax+1 — finite because p is finite; it
// is not an unbounded-iteration loop.
func (l *logLane) write(
	streamName string, buf *logStreamBuf, p []byte,
) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	written := len(p)
	if l.terminal {
		slog.Debug("log write after task terminal; dropped",
			"run_id", l.runID, "step_id", l.stepID, "stream", streamName)
		return written, nil
	}
	if l.truncated {
		return written, nil
	}
	remaining := p
	for len(remaining) > 0 {
		budget := logStepBytesMax - l.totalBytes
		if budget <= 0 {
			l.flushLocked(streamName, buf)
			l.emitMarkerLocked(protocol.LogMarkerTruncated)
			l.truncated = true
			return written, nil
		}
		space := int64(protocol.LogChunkBytesMax - len(buf.data))
		if space <= 0 {
			l.flushLocked(streamName, buf)
			continue
		}
		take := int64(len(remaining))
		if take > budget {
			take = budget
		}
		if take > space {
			take = space
		}
		buf.data = append(buf.data, remaining[:take]...)
		l.totalBytes += take
		if buf.pendingSince.IsZero() {
			buf.pendingSince = time.Now()
		}
		remaining = remaining[take:]
		if int64(len(buf.data)) >= protocol.LogChunkBytesMax {
			l.flushLocked(streamName, buf)
		}
		if l.totalBytes >= logStepBytesMax {
			l.flushLocked(streamName, buf)
			l.emitMarkerLocked(protocol.LogMarkerTruncated)
			l.truncated = true
			return written, nil
		}
	}
	return written, nil
}

// flushLocked publishes buf's contents as one LogChunk and clears it.
// No-op on an empty buffer. Caller must hold l.mu.
func (l *logLane) flushLocked(streamName string, buf *logStreamBuf) {
	if len(buf.data) == 0 {
		return
	}
	data := buf.data
	buf.data = nil
	buf.pendingSince = time.Time{}
	l.publishLocked(streamName, data)
}

// emitMarkerLocked publishes a marker chunk (Stream ==
// protocol.LogStreamMarker, Data == marker). Caller must hold l.mu.
func (l *logLane) emitMarkerLocked(marker string) {
	l.publishLocked(protocol.LogStreamMarker, []byte(marker))
}

// publishLocked assigns the next seq (shared across out/err/marker so
// ordering by Seq reconstructs write order) and publishes. A publish
// error is logged and counted via l.failures — the chunk is dropped,
// never retried, so a NATS outage cannot back up unbounded log data in
// memory or block the handler. Caller must hold l.mu.
func (l *logLane) publishLocked(streamName string, data []byte) {
	seq := l.seq
	l.seq++
	chunk := protocol.LogChunk{
		Seq:    seq,
		TS:     time.Now(),
		Stream: streamName,
		Data:   data,
	}
	payload, err := json.Marshal(chunk)
	if err != nil {
		slog.Error("marshal log chunk failed",
			"error", err, "run_id", l.runID, "step_id", l.stepID)
		return
	}
	msgID := fmt.Sprintf("log-%s-%s-%d", l.runID, l.stepID, seq)
	msg := &nats.Msg{
		Subject: l.subject,
		Data:    payload,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	ctx, cancel := context.WithTimeout(l.ctx, logPublishTimeout)
	defer cancel()
	if _, err := l.tp.JSPublishMsg(ctx, msg); err != nil {
		if l.failures != nil {
			l.failures.Add(context.Background(), 1)
		}
		slog.Error("publish log chunk failed",
			"error", err, "run_id", l.runID, "step_id", l.stepID,
			"stream", streamName)
	}
}

// drain flushes any buffered content and stops the ticker, WITHOUT
// emitting a marker. Used by Complete and Continue.
func (l *logLane) drain() {
	l.mu.Lock()
	l.flushLocked(protocol.LogStreamOut, &l.out)
	l.flushLocked(protocol.LogStreamErr, &l.err)
	alreadyTerminal := l.terminal
	l.terminal = true
	l.mu.Unlock()
	if !alreadyTerminal {
		l.stop()
	}
}

// failDrain flushes any buffered content, THEN emits the
// LogMarkerFailed marker, THEN stops the ticker — so the marker is
// guaranteed to land after every log byte the handler wrote and before
// the caller's step.failed resolution publish that follows. Used by
// Fail and FailPermanent.
func (l *logLane) failDrain() {
	l.mu.Lock()
	l.flushLocked(protocol.LogStreamOut, &l.out)
	l.flushLocked(protocol.LogStreamErr, &l.err)
	l.emitMarkerLocked(protocol.LogMarkerFailed)
	alreadyTerminal := l.terminal
	l.terminal = true
	l.mu.Unlock()
	if !alreadyTerminal {
		l.stop()
	}
}

// stop signals the background goroutine and waits for it to exit.
// Called with l.mu NOT held — the goroutine itself needs l.mu to
// process a tick, so holding it here while waiting on doneCh would
// deadlock against an in-flight tick.
func (l *logLane) stop() {
	close(l.stopCh)
	<-l.doneCh
}
