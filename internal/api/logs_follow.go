// internal/api/logs_follow.go
// SSE half of GET /runs/{id}/logs?follow=1 (#624). Split from logs.go
// to keep each file under a manageable size — the follow path has its
// own concerns (flush cadence, keepalive, terminal detection) distinct
// from the paged-read path.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/protocol"
)

// logsKeepaliveInterval is how often serveLogsFollow writes an SSE
// comment while idle, so an intermediary (proxy, load balancer) does
// not treat the connection as dead and close it.
const logsKeepaliveInterval = 15 * time.Second

// logsTerminalPollInterval is how often serveLogsFollow re-checks the
// run snapshot for the step's terminal status once the BUILD_LOGS lane
// itself has gone quiet (no new chunk arrived).
const logsTerminalPollInterval = 500 * time.Millisecond

// serveLogsFollow upgrades to Server-Sent Events: an "event: chunk"
// per LogChunk, a ": keepalive" comment every logsKeepaliveInterval
// while idle, and "event: eof" once the step is terminal AND the
// BUILD_LOGS lane for it is drained (no chunk arrived on the last
// poll). Bounded by protocol.LogFollowDurationMax and gated by
// logFollowConcurrentMax.
func serveLogsFollow(
	svc *Service, w http.ResponseWriter, r *http.Request, runID, stepID string,
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
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx, cancel := context.WithTimeout(r.Context(), protocol.LogFollowDurationMax)
	defer cancel()

	afterSeq := afterSeqParam(r)
	fromFailure := r.URL.Query().Get("from") == "failure"
	nextSeq, ok := logsFollowCatchUp(ctx, svc, w, flusher, runID, stepID, afterSeq, fromFailure)
	if !ok {
		return
	}

	keepalive := time.NewTicker(logsKeepaliveInterval)
	defer keepalive.Stop()
	poll := time.NewTicker(logsTerminalPollInterval)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-poll.C:
			var wrote bool
			nextSeq, wrote = logsFollowDrain(ctx, svc, w, flusher, runID, stepID, nextSeq)
			if wrote {
				keepalive.Reset(logsKeepaliveInterval)
			}
			run, err := svc.GetRun(ctx, runID)
			if err != nil {
				continue
			}
			state, known := run.Steps[stepID]
			if known && stepTerminal(state.Status) {
				// One more drain in case a chunk landed between the
				// drain above and this snapshot read.
				_, _ = logsFollowDrain(ctx, svc, w, flusher, runID, stepID, nextSeq)
				writeLogsEOFEvent(w, flusher)
				return
			}
		}
	}
}

// logsFollowCatchUp fetches and emits everything currently stored
// after afterSeq (or from the failure marker) before the poll loop
// starts, so a late attach still sees history. Returns the seq to
// resume from and false if the response write already failed.
func logsFollowCatchUp(
	ctx context.Context, svc *Service, w http.ResponseWriter, flusher http.Flusher,
	runID, stepID string, afterSeq int64, fromFailure bool,
) (uint64, bool) {
	all, err := fetchStepLogChunks(ctx, svc.js, runID, stepID)
	if err != nil {
		return uint64(afterSeq + 1), true
	}
	startIdx := 0
	if fromFailure {
		startIdx = len(all)
		for i, c := range all {
			if c.Stream == protocol.LogStreamMarker &&
				string(c.Data) == protocol.LogMarkerFailed {
				startIdx = i
				break
			}
		}
	} else {
		for i, c := range all {
			if int64(c.Seq) > afterSeq {
				startIdx = i
				break
			}
			startIdx = i + 1
		}
	}
	next := uint64(afterSeq + 1)
	for _, c := range all[startIdx:] {
		if !writeLogsChunkEvent(w, flusher, c) {
			return next, false
		}
		next = c.Seq + 1
	}
	return next, true
}

// logsFollowDrain fetches and emits any chunk with Seq >= fromSeq
// published since the last drain. Returns the next resume seq and
// whether anything was written.
func logsFollowDrain(
	ctx context.Context, svc *Service, w http.ResponseWriter, flusher http.Flusher,
	runID, stepID string, fromSeq uint64,
) (uint64, bool) {
	all, err := fetchStepLogChunks(ctx, svc.js, runID, stepID)
	if err != nil {
		return fromSeq, false
	}
	wrote := false
	next := fromSeq
	for _, c := range all {
		if c.Seq < fromSeq {
			continue
		}
		if !writeLogsChunkEvent(w, flusher, c) {
			return next, wrote
		}
		next = c.Seq + 1
		wrote = true
	}
	return next, wrote
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

func writeLogsEOFEvent(w http.ResponseWriter, flusher http.Flusher) {
	fmt.Fprint(w, "event: eof\ndata: {}\n\n")
	flusher.Flush()
}
