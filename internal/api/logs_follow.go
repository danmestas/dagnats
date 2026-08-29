// internal/api/logs_follow.go
// SSE half of GET /runs/{id}/logs?follow=1 (#624). Split from logs.go
// to keep each file under a manageable size — the follow path has its
// own concerns (one long-lived consumer, keepalive cadence, marker-
// driven eof) distinct from the paged-read path.
//
// #624 review fix: the original implementation opened a FRESH ordered
// consumer every 500ms and rescanned the whole attempt's subject from
// the beginning — O(n^2) over a connection's lifetime and, at the
// 256-follower cap, roughly 512 ephemeral consumers/sec against
// BUILD_LOGS. This version opens exactly ONE ordered consumer per
// connection, anchored at the caller's cursor, and blocks on it with a
// bounded wait that doubles as the keepalive tick. Eof no longer
// depends on polling BUILD_LOGS at all: every attempt-ending
// TaskContext method (worker/context.go) now emits a terminal marker
// as the guaranteed last chunk on its subject, so eof fires the moment
// that marker is READ off the one open consumer. The run-snapshot poll
// only exists as a crash fallback (a marker that, for whatever reason,
// never got published) and runs far less often, and never opens a
// second BUILD_LOGS consumer to do it.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// logsKeepaliveInterval is both how often serveLogsFollow writes an
// SSE comment while idle (so an intermediary doesn't treat the
// connection as dead) AND the bounded wait passed to the single open
// consumer's Next() call — the same number serves both purposes so
// there is exactly one blocking wait per loop iteration, not a
// separate timer racing a separate fetch.
const logsKeepaliveInterval = 15 * time.Second

// logsCrashFallbackPollInterval is how often serveLogsFollow re-checks
// the run snapshot as a fallback when no terminal marker has arrived
// off the log consumer — deliberately coarse (10s, not the old 500ms)
// because this path exists only to cover the case where the marker
// itself never got published (a worker crash between draining logs and
// the marker actually landing), not as the normal termination signal.
// It reads svc.GetRun (a KV get), never opens a BUILD_LOGS consumer.
const logsCrashFallbackPollInterval = 10 * time.Second

// logsFollowStreamCheckTimeout bounds the one cheap round trip
// serveLogsFollow spends per idle keepalive tick confirming BUILD_LOGS
// still exists. It is a JS API get, not a consumer open.
const logsFollowStreamCheckTimeout = 3 * time.Second

// logsEOFEvent is the "event: eof" payload. Reason is set only for the
// two non-final outcomes (continued/paused) where the attempt is over
// but the STEP isn't necessarily done — a UI is expected to re-attach
// with a fresh attempt param. It's empty for completed/failed, where
// there is nothing more to attach to.
type logsEOFEvent struct {
	Reason string `json:"reason,omitempty"`
}

// isAttemptEndingMarker reports whether data (a LogStreamMarker
// chunk's Data) is one of the markers every TaskContext attempt-ending
// method emits — as opposed to LogMarkerTruncated, which can appear
// mid-attempt and must NOT end a follow.
func isAttemptEndingMarker(marker string) bool {
	switch marker {
	case protocol.LogMarkerCompleted, protocol.LogMarkerFailed,
		protocol.LogMarkerContinued, protocol.LogMarkerPaused:
		return true
	}
	return false
}

// serveLogsFollow upgrades to Server-Sent Events over ONE long-lived
// BUILD_LOGS consumer anchored at cursor (0 = from the start of the
// attempt's subject). See the file doc comment for why this replaced
// a poll-and-rescan design. Bounded by protocol.LogFollowDurationMax
// and gated by logFollowConcurrentMax.
func serveLogsFollow(
	svc *Service, w http.ResponseWriter, r *http.Request,
	runID, stepID string, attempt, iteration int,
) {
	if atomic.AddInt64(&logFollowActive, 1) > logFollowConcurrentMax {
		atomic.AddInt64(&logFollowActive, -1)
		http.Error(w, "too many concurrent log follows", http.StatusServiceUnavailable)
		return
	}
	defer atomic.AddInt64(&logFollowActive, -1)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), protocol.LogFollowDurationMax)
	defer cancel()

	cons, err := openLogsFollowConsumer(
		ctx, svc.js, runID, stepID, attempt, iteration, cursorParam(r),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	runLogsFollowLoop(ctx, svc, w, flusher, cons, runID, stepID, attempt, iteration)
}

// openLogsFollowConsumer opens the single ordered consumer a follow
// connection uses for its entire lifetime, anchored at cursor exactly
// like fetchLogsPage's non-follow counterpart.
func openLogsFollowConsumer(
	ctx context.Context, js jetstream.JetStream,
	runID, stepID string, attempt, iteration int, cursor uint64,
) (jetstream.Consumer, error) {
	if js == nil {
		panic("openLogsFollowConsumer: js must not be nil")
	}
	cons, err := js.OrderedConsumer(
		ctx, "BUILD_LOGS",
		logsOrderedConsumerConfig(runID, stepID, attempt, iteration, cursor),
	)
	if err != nil {
		return nil, fmt.Errorf("open BUILD_LOGS consumer: %w", err)
	}
	return cons, nil
}

// runLogsFollowLoop drives one already-open consumer for the life of
// the connection: fetch (bounded wait = the keepalive tick) -> emit ->
// repeat, with a coarse crash-fallback snapshot poll running in
// parallel. Ends on ctx.Done(), a write failure, or an attempt-ending
// marker/snapshot terminal signal.
func runLogsFollowLoop(
	ctx context.Context, svc *Service, w http.ResponseWriter, flusher http.Flusher,
	cons jetstream.Consumer, runID, stepID string, attempt, iteration int,
) {
	fallback := time.NewTicker(logsCrashFallbackPollInterval)
	defer fallback.Stop()
	lastActivity := time.Now()

	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(logsKeepaliveInterval))
		if err != nil {
			if !errors.Is(err, nats.ErrTimeout) {
				// #624 review round 2: a real error (deleted consumer,
				// connection problem, etc.) — NOT the ordinary "nothing
				// arrived within this wait" timeout every idle
				// iteration hits — must end the stream with an
				// event: error, not be treated as idle and looped on
				// via a now-broken consumer until the 1h duration cap.
				writeLogsErrorEvent(w, flusher, err)
				return
			}
			// Idle for a full keepalive window — write the comment and
			// check the crash-fallback ticker before looping back into
			// another bounded wait.
			if _, werr := fmt.Fprint(w, ": keepalive\n\n"); werr != nil {
				return
			}
			flusher.Flush()
			// Belt to the MaxResetAttempts brace: an idle wait that
			// ends in ErrTimeout tells us nothing about whether
			// BUILD_LOGS still exists (a pull request in flight when
			// the stream is deleted simply expires). Confirm it
			// ourselves rather than waiting for the consumer to
			// report a loss it may never report.
			if logsStreamMissing(ctx, svc.js) {
				writeLogsErrorEvent(w, flusher, errLogStreamUnavailable)
				return
			}
			if shouldCrashFallbackEnd(
				ctx, svc, fallback, runID, stepID, attempt, iteration, &lastActivity,
			) {
				writeLogsEOFEvent(w, flusher, "")
				return
			}
			continue
		}
		lastActivity = time.Now()
		chunk, _, ok := decodeLogsMsg(msg, runID, stepID)
		if ackErr := msg.Ack(); ackErr != nil {
			slog.Warn("ack SSE BUILD_LOGS message failed",
				"error", ackErr, "run_id", runID, "step_id", stepID)
		}
		if !ok {
			continue
		}
		if !writeLogsChunkEvent(w, flusher, chunk) {
			return
		}
		if chunk.Stream == protocol.LogStreamMarker && isAttemptEndingMarker(string(chunk.Data)) {
			writeLogsEOFEvent(w, flusher, string(chunk.Data))
			return
		}
	}
}

// errLogStreamUnavailable is the cause reported when the follow's own
// per-tick check finds BUILD_LOGS gone. It is a fixed sentinel so the
// SSE error payload names the condition rather than leaking whichever
// transport-level error the JS API happened to return.
var errLogStreamUnavailable = errors.New("log stream unavailable")

// logsStreamMissing reports whether BUILD_LOGS has gone away. Only a
// definite ErrStreamNotFound counts: a timed-out or otherwise failed
// check must not end a healthy follow, so every other outcome
// (including an error) reports "still there" and the next tick
// retries.
func logsStreamMissing(ctx context.Context, js jetstream.JetStream) bool {
	if js == nil {
		panic("logsStreamMissing: js must not be nil")
	}
	if ctx == nil {
		panic("logsStreamMissing: ctx must not be nil")
	}
	checkCtx, cancel := context.WithTimeout(ctx, logsFollowStreamCheckTimeout)
	defer cancel()
	_, err := js.Stream(checkCtx, "BUILD_LOGS")
	return errors.Is(err, jetstream.ErrStreamNotFound)
}

// shouldCrashFallbackEnd checks the run snapshot for a terminal status
// no more often than logsCrashFallbackPollInterval, WITHOUT opening a
// BUILD_LOGS consumer — it exists only to end a follow whose marker
// never arrived (a crashed worker), not as the normal path.
// lastActivity is reset by the caller on every successfully read
// chunk so a fallback check doesn't fire seconds after real traffic.
func shouldCrashFallbackEnd(
	ctx context.Context, svc *Service, ticker *time.Ticker,
	runID, stepID string, attempt, iteration int, lastActivity *time.Time,
) bool {
	select {
	case <-ticker.C:
	default:
		return false
	}
	if time.Since(*lastActivity) < logsCrashFallbackPollInterval {
		return false
	}
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		return false
	}
	state, known := run.Steps[stepID]
	if !known {
		return false
	}
	return dispatchIsDone(attempt, iteration, state)
}

func writeLogsChunkEvent(w http.ResponseWriter, flusher http.Flusher, c protocol.LogChunk) bool {
	data, err := json.Marshal(c)
	if err != nil {
		slog.Error("marshal SSE log chunk", "error", err)
		return true
	}
	if _, err := fmt.Fprintf(w, "event: chunk\ndata: %s\n\n", data); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// writeLogsEOFEvent writes the terminal SSE event. marker is the
// attempt-ending marker value that triggered it ("" for the crash
// fallback, which has none) — only continued/paused surface as
// "reason" in the payload; completed/failed and the crash fallback
// carry an empty object.
func writeLogsEOFEvent(w http.ResponseWriter, flusher http.Flusher, marker string) {
	var payload logsEOFEvent
	if marker == protocol.LogMarkerContinued || marker == protocol.LogMarkerPaused {
		payload.Reason = marker
	}
	data, err := json.Marshal(payload)
	if err != nil {
		data = []byte("{}")
	}
	// The connection is ending either way (caller returns right after
	// this call) — a write failure here has nothing left to affect,
	// but it's still logged rather than silently dropped per #624
	// review's discarded-error nit.
	if _, err := fmt.Fprintf(w, "event: eof\ndata: %s\n\n", data); err != nil {
		slog.Warn("write SSE eof event failed", "error", err)
		return
	}
	flusher.Flush()
}

// logsErrorEvent is the "event: error" payload — a genuine failure of
// the follow's consumer (deleted consumer, connection problem), as
// opposed to event: eof's normal end-of-attempt signal.
type logsErrorEvent struct {
	Reason string `json:"reason"`
}

// writeLogsErrorEvent ends a follow connection on a real consumer
// error (#624 review round 2) — distinct from the ordinary per-wait
// nats.ErrTimeout every idle loop iteration produces, which must NOT
// reach here (that's the event: eof / keepalive path).
func writeLogsErrorEvent(w http.ResponseWriter, flusher http.Flusher, cause error) {
	if cause == nil {
		panic("writeLogsErrorEvent: cause must not be nil")
	}
	slog.Warn("SSE log follow consumer error; ending stream", "error", cause)
	data, err := json.Marshal(logsErrorEvent{Reason: cause.Error()})
	if err != nil {
		data = []byte(`{"reason":"internal error"}`)
	}
	if _, err := fmt.Fprintf(w, "event: error\ndata: %s\n\n", data); err != nil {
		slog.Warn("write SSE error event failed", "error", err)
		return
	}
	flusher.Flush()
}
