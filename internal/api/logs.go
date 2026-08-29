// internal/api/logs.go
// GET /runs/{id}/logs (#624): the tail-read counterpart to the
// BUILD_LOGS hot lane worker/log_writer.go and bridge/logs.go publish
// into. Non-follow returns a bounded page of stored chunks; follow=1
// upgrades to Server-Sent Events. dagnats owns the JetStream hot lane
// only — this handler never reaches past BUILD_LOGS's own TTL.
//
// Cursor shape (#624 review): the cursor is the JetStream STREAM
// sequence of the last delivered message + 1 — opaque to the client,
// who only ever copies next_cursor from a prior response into the next
// request's cursor= param. This replaced an after_seq design that
// fetched a fixed ~1032-message window from the start of the subject
// on every request and filtered client-side: paging past that window
// silently stalled (every page after it returned empty with eof never
// becoming true). Anchoring directly on the stream's own sequence
// numbers via DeliverByStartSequencePolicy makes each page an O(page
// size) JetStream fetch, not an O(everything before the cursor) scan.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// logsFetchIdleWait bounds how long fetchLogsPage waits for the NEXT
// message before concluding nothing more is buffered right now (not:
// the subject will never receive another message).
const logsFetchIdleWait = 200 * time.Millisecond

// logFollowConcurrentMax is the effective SSE-follow concurrency cap.
// A package var (not a bare reference to protocol.LogFollowConcurrentMax)
// so tests can lower it and exercise the 503 path without opening 257
// real connections.
var logFollowConcurrentMax int64 = protocol.LogFollowConcurrentMax

// logFollowActive counts in-flight SSE follow connections across this
// server process.
var logFollowActive int64

// logsResponse is the non-follow GET /runs/{id}/logs body.
type logsResponse struct {
	Chunks     []protocol.LogChunk `json:"chunks"`
	NextCursor uint64              `json:"next_cursor"`
	EOF        bool                `json:"eof"`
}

// logsErrorBody is the JSON body writeJSONError sends.
type logsErrorBody struct {
	Error string `json:"error"`
}

// writeJSONError writes a JSON error body ({"error": msg}) with the
// given status. Used where a plain http.Error text body isn't
// specific enough (from=failure's 404, #624 review round 2) — callers
// that just need a status code with any error string still use
// http.Error elsewhere in this file.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(logsErrorBody{Error: msg}); err != nil {
		slog.Error("encode logs error body", "error", err)
	}
}

// handleGetRunLogs serves GET /runs/{id}/logs?step=&attempt=&cursor=&follow=&from=.
func handleGetRunLogs(svc *Service, w http.ResponseWriter, r *http.Request) {
	if svc == nil {
		panic("handleGetRunLogs: svc must not be nil")
	}
	if r == nil {
		panic("handleGetRunLogs: r must not be nil")
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/runs/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing run ID", http.StatusBadRequest)
		return
	}
	runID := parts[0]
	stepID := r.URL.Query().Get("step")
	if stepID == "" {
		http.Error(w, "step query parameter is required", http.StatusBadRequest)
		return
	}

	run, err := svc.GetRun(r.Context(), runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stepState, known := run.Steps[stepID]
	if !known {
		http.Error(w, "step not found", http.StatusNotFound)
		return
	}
	attempt := attemptParam(r, stepState)

	if r.URL.Query().Get("follow") == "1" {
		serveLogsFollow(svc, w, r, runID, stepID, attempt)
		return
	}
	serveLogsPage(svc, w, r, runID, stepID, attempt, stepState)
}

// attemptParam parses "attempt"; absent/invalid resolves to the step's
// current attempt count (dag.StepState.Attempts) — reading logs for a
// step with no explicit ?attempt= gets its live/most-recent attempt.
func attemptParam(r *http.Request, stepState dag.StepState) int {
	val := r.URL.Query().Get("attempt")
	if val == "" {
		return stepState.Attempts
	}
	n, err := strconv.Atoi(val)
	if err != nil || n < 0 {
		return stepState.Attempts
	}
	return n
}

// cursorParam parses "cursor"; absent/invalid resolves to 0, meaning
// "start of the subject" (DeliverAllPolicy).
func cursorParam(r *http.Request) uint64 {
	val := r.URL.Query().Get("cursor")
	if val == "" {
		return 0
	}
	n, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// attemptSubject builds the attempt-scoped BUILD_LOGS subject, matching
// worker/log_writer.go and bridge/logs.go exactly.
func attemptSubject(runID, stepID string, attempt int) string {
	if strings.ContainsAny(runID, ". \t*>") {
		panic("attemptSubject: runID must not contain NATS subject metacharacters")
	}
	return fmt.Sprintf("logs.%s.%s.%d", runID, natsutil.SubjectToken(stepID), attempt)
}

// serveLogsPage handles the non-follow read: resolve the start cursor
// (from=failure short-circuits to the attempt's terminal marker via
// GetLastMsgForSubject; otherwise the caller's cursor param), fetch one
// bounded page, and report next_cursor/eof.
func serveLogsPage(
	svc *Service, w http.ResponseWriter, r *http.Request,
	runID, stepID string, attempt int, stepState dag.StepState,
) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	startCursor := cursorParam(r)
	if r.URL.Query().Get("from") == "failure" {
		found, seq, err := lastFailureMarkerSeq(ctx, svc.js, runID, stepID, attempt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !found {
			// #624 review round 2: from=failure must be strict —
			// either this attempt's subject has no messages at all, or
			// its last message exists but is NOT a "failed" marker
			// (the attempt completed, is still running, or ended some
			// other way). Either way there is no recorded failure
			// position to start from, so this is a 404, not an empty
			// 200 page — a caller asking for the failure position on
			// an attempt that never failed made a request error, not
			// a "nothing here yet" query.
			writeJSONError(w, http.StatusNotFound, "attempt has no failure marker")
			return
		}
		startCursor = seq
	}

	page, lastStreamSeq, err := fetchLogsPage(
		ctx, svc.js, runID, stepID, attempt, startCursor, protocol.LogReadChunksMax,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Advance off lastStreamSeq whenever anything was consumed — even
	// a page of nothing but malformed chunks (page empty, lastStreamSeq
	// still > 0) must move the cursor forward (#624 review round 2 nit).
	nextCursor := startCursor
	if lastStreamSeq > 0 {
		nextCursor = lastStreamSeq + 1
	}
	gotFullPage := len(page) >= protocol.LogReadChunksMax
	eof := !gotFullPage &&
		(attemptIsPast(attempt, stepState) || stepTerminal(stepState.Status))

	resp := logsResponse{Chunks: page, NextCursor: nextCursor, EOF: eof}
	if resp.Chunks == nil {
		resp.Chunks = []protocol.LogChunk{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode logs response", "error", err)
	}
}

// attemptIsPast reports whether attempt is strictly behind the step's
// current attempt count — a past attempt's subject can never receive
// another message no matter what the CURRENT attempt's status is, so
// reads of it are always eligible for eof once a page comes up short.
func attemptIsPast(attempt int, stepState dag.StepState) bool {
	return attempt < stepState.Attempts
}

// stepTerminal reports whether status is a terminal StepStatus — half
// of the eof condition for GET .../logs on the step's CURRENT attempt
// (the other half, for a past attempt, is attemptIsPast).
func stepTerminal(status dag.StepStatus) bool {
	switch status {
	case dag.StepStatusCompleted, dag.StepStatusFailed,
		dag.StepStatusSkipped, dag.StepStatusCancelled,
		dag.StepStatusRecovered:
		return true
	}
	return false
}

// lastFailureMarkerSeq resolves from=failure's start cursor in O(1):
// the drain-before-resolve invariant guarantees the attempt-ending
// marker (completed/failed/continued/paused) is the TRUE LAST message
// on this attempt's subject, so IF this attempt failed, "the failure
// position" is just "the last message" — fetched directly via
// GetLastMsgForSubject rather than scanning from the beginning. found
// is false both when the subject has no messages yet AND when the
// last message exists but is not a LogMarkerFailed marker (#624
// review round 2: from=failure must be strict, not "whatever happens
// to be last") — the caller does not need to distinguish the two; both
// mean "there is no recorded failure position to start from".
func lastFailureMarkerSeq(
	ctx context.Context, js jetstream.JetStream, runID, stepID string, attempt int,
) (found bool, seq uint64, err error) {
	if js == nil {
		panic("lastFailureMarkerSeq: js must not be nil")
	}
	stream, err := js.Stream(ctx, "BUILD_LOGS")
	if err != nil {
		return false, 0, fmt.Errorf("open BUILD_LOGS stream: %w", err)
	}
	subject := attemptSubject(runID, stepID, attempt)
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("get last message for %s: %w", subject, err)
	}
	var chunk protocol.LogChunk
	if unmarshalErr := json.Unmarshal(msg.Data, &chunk); unmarshalErr != nil {
		return false, 0, fmt.Errorf("unmarshal last message for %s: %w", subject, unmarshalErr)
	}
	if chunk.Stream != protocol.LogStreamMarker || string(chunk.Data) != protocol.LogMarkerFailed {
		return false, 0, nil
	}
	return true, msg.Sequence, nil
}

// fetchLogsPage fetches up to max BUILD_LOGS messages for
// logs.{runID}.{stepID}.{attempt}, starting at stream sequence cursor
// (0 means "from the start of the subject"), in stream order. Returns
// the fetched chunks and the JetStream stream sequence of the last one
// delivered (0 if none). Stops early once no further message arrives
// within logsFetchIdleWait — interpreted as "nothing more buffered
// right now", not "the subject is done forever".
func fetchLogsPage(
	ctx context.Context, js jetstream.JetStream,
	runID, stepID string, attempt int, cursor uint64, max int,
) ([]protocol.LogChunk, uint64, error) {
	if js == nil {
		panic("fetchLogsPage: js must not be nil")
	}
	if runID == "" {
		panic("fetchLogsPage: runID must not be empty")
	}
	if max <= 0 {
		panic("fetchLogsPage: max must be positive")
	}
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{attemptSubject(runID, stepID, attempt)},
	}
	if cursor > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = cursor
	} else {
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}
	cons, err := js.OrderedConsumer(ctx, "BUILD_LOGS", cfg)
	if err != nil {
		return nil, 0, fmt.Errorf("open BUILD_LOGS consumer: %w", err)
	}
	var chunks []protocol.LogChunk
	var lastStreamSeq uint64
	for len(chunks) < max {
		if ctx.Err() != nil {
			break
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(logsFetchIdleWait))
		if err != nil {
			break
		}
		chunk, seq, ok := decodeLogsMsg(msg, runID, stepID)
		if ackErr := msg.Ack(); ackErr != nil {
			slog.Warn("ack BUILD_LOGS message failed",
				"error", ackErr, "run_id", runID, "step_id", stepID)
		}
		// Advance the cursor off seq regardless of ok — a decode
		// failure must not pin next_cursor at a stale position (#624
		// review round 2 nit), or a page of nothing but malformed
		// messages would make every subsequent request re-fetch the
		// same messages forever.
		if seq > 0 {
			lastStreamSeq = seq
		}
		if !ok {
			continue
		}
		chunks = append(chunks, chunk)
	}
	return chunks, lastStreamSeq, nil
}

// decodeLogsMsg unmarshals msg into a LogChunk and reads its JetStream
// stream sequence. ok is false (chunk zero) on a malformed payload,
// logged and skipped rather than failing the whole page — but
// streamSeq is still returned whenever metadata was readable, EVEN ON
// an unmarshal failure (#624 review round 2 nit): the caller advances
// its paging cursor off streamSeq regardless of ok, so a run of
// undecodable messages can never stall pagination by pinning the
// cursor at the same stale position forever.
func decodeLogsMsg(
	msg jetstream.Msg, runID, stepID string,
) (chunk protocol.LogChunk, streamSeq uint64, ok bool) {
	meta, metaErr := msg.Metadata()
	if metaErr != nil {
		slog.Warn("BUILD_LOGS message metadata unavailable",
			"error", metaErr, "run_id", runID, "step_id", stepID)
		return protocol.LogChunk{}, 0, false
	}
	if unmarshalErr := json.Unmarshal(msg.Data(), &chunk); unmarshalErr != nil {
		slog.Warn("skipping malformed BUILD_LOGS chunk",
			"error", unmarshalErr, "run_id", runID, "step_id", stepID,
			"stream_seq", meta.Sequence.Stream)
		return protocol.LogChunk{}, meta.Sequence.Stream, false
	}
	return chunk, meta.Sequence.Stream, true
}
