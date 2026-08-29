// worker/log_writer.go
// logLane buffers and publishes one task ATTEMPT/ITERATION's captured
// stdout/stderr to the BUILD_LOGS hot lane (#624, subject
// logs.{runID}.{stepID}.{attempt}.{iteration}). One lane per
// attempt/iteration, created lazily on first LogOut()/LogErr() Write so
// a handler that never logs never spins up a ticker goroutine.
//
// Attempt is part of the subject (#624 review round 2), not just the
// payload: a retry re-dispatches the same runID/stepID, and
// BUILD_LOGS's dedup window (2 min) is comfortably longer than the gap
// between two attempts, so a bare logs.{runID}.{stepID} subject would
// let attempt 2's seq-0 chunk collide with attempt 1's Nats-Msg-Id and
// silently vanish as a duplicate.
//
// Iteration is a SECOND, independent dimension (#624 review round 3):
// an agent-loop step's Continue re-dispatches the SAME attempt with
// iteration incremented — never bumping attempt, which would consume
// retry budget the step never spent — so without iteration in the
// subject/Msg-Id too, iteration 2+'s chunks would collide with
// iteration 0's the exact same way un-scoped attempts collided before
// round 2. Scoping the subject by both makes every
// attempt+iteration's chunk stream independent and gives from=failure
// an unambiguous target.
//
// Complete, Fail, FailPermanent, FailRetryAfter, Continue, and Pause
// all drain the lane (flush pending bytes, emit the attempt-ending
// marker, stop the ticker) before publishing their resolution — the
// drain-before-resolve invariant — so a consumer that observes the
// terminal event can never race ahead of the log bytes that produced
// it, and the marker is always the true last message on the subject.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
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

	subject   string
	runID     string
	stepID    string
	attempt   int
	iteration int

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
// attempt scopes the subject/Msg-Id to this specific task attempt (see
// the package doc comment) — it is the resolved 1-based AttemptNumber
// (worker/context.go's resolveAttemptNumber), the SAME numbering
// step.started/step.failed's AttemptNumber field uses and
// dag.StepState.Attempts is derived from, so GET .../logs's default
// ?attempt= (the step's current Attempts) lands on the right subject
// without a caller ever needing to know this package's internals.
// Always >= 1; 0 or negative is a programmer error.
//
// iteration scopes the subject a SECOND dimension (#624 review round
// 3): an agent-loop step's Continue re-dispatches the SAME attempt
// with iteration incremented, never bumping attempt (that would
// consume retry budget the step never spent) — without iteration in
// the subject/Msg-Id, iteration 2+'s seq-0 chunk collides with
// iteration 0's the exact way un-scoped attempts collided before round
// 2. iteration is 0 for a non-loop step (protocol.TaskPayload.Iteration's
// own zero value). Always >= 0.
//
// Callers must eventually call drainWithMarker exactly once to stop
// the ticker; leaving a lane un-drained leaks its goroutine.
//
// runID must not contain NATS subject metacharacters — every runID in
// this codebase is engine- or nuid-generated (never raw user input),
// so this is a programmer-error guard, not a defense against untrusted
// input (mirrors internal/engine's runEventSubject).
func newLogLane(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	runID, stepID string,
	attempt, iteration int,
	failures metric.Int64Counter,
) *logLane {
	if tp == nil {
		panic("newLogLane: tp must not be nil")
	}
	if runID == "" {
		panic("newLogLane: runID must not be empty")
	}
	if strings.ContainsAny(runID, ". \t*>") {
		panic("newLogLane: runID must not contain NATS subject metacharacters")
	}
	if attempt < 1 {
		panic("newLogLane: attempt must be >= 1")
	}
	if iteration < 0 {
		panic("newLogLane: iteration must be >= 0")
	}
	lane := &logLane{
		tp:        tp,
		ctx:       ctx,
		subject:   natsutil.LogSubject(runID, stepID, attempt, iteration),
		runID:     runID,
		stepID:    stepID,
		attempt:   attempt,
		iteration: iteration,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		failures:  failures,
		ticker:    time.NewTicker(logFlushCheckInterval),
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
		Seq:       seq,
		Attempt:   l.attempt,
		Iteration: l.iteration,
		TS:        time.Now(),
		Stream:    streamName,
		Data:      data,
	}
	payload, err := json.Marshal(chunk)
	if err != nil {
		slog.Error("marshal log chunk failed",
			"error", err, "run_id", l.runID, "step_id", l.stepID)
		return
	}
	msgID := natsutil.LogMsgID(l.runID, l.stepID, l.attempt, l.iteration, seq)
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

// drainWithMarker flushes any buffered content, THEN emits marker (one
// of the protocol.LogMarker* constants), THEN stops the ticker — so the
// marker is guaranteed to be the true LAST message on this attempt's
// subject, landing after every log byte the handler wrote and before
// the caller's resolution-event publish that follows (#624 review:
// every attempt-ending TaskContext method — Complete, Fail,
// FailPermanent, FailRetryAfter, Continue, Pause — calls this with its
// own marker, so GET .../logs's follow mode and from=failure can both
// treat "marker chunk arrived" as a reliable end-of-attempt signal).
// Idempotent: a second call on an already-terminal lane is a no-op
// (only the first marker for an attempt is meaningful).
func (l *logLane) drainWithMarker(marker string) {
	if marker == "" {
		panic("drainWithMarker: marker must not be empty")
	}
	l.mu.Lock()
	alreadyTerminal := l.terminal
	if !alreadyTerminal {
		l.flushLocked(protocol.LogStreamOut, &l.out)
		l.flushLocked(protocol.LogStreamErr, &l.err)
		l.emitMarkerLocked(marker)
		l.terminal = true
	}
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
