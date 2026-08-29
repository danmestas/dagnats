// internal/api/logs.go
// GET /runs/{id}/logs (#624): the tail-read counterpart to the
// BUILD_LOGS hot lane worker/log_writer.go and bridge/logs.go publish
// into. Non-follow returns a bounded page of stored chunks; follow=1
// upgrades to Server-Sent Events. dagnats owns the JetStream hot lane
// only — this handler never reaches past BUILD_LOGS's own TTL.
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

// logsFetchScanMax bounds how many BUILD_LOGS messages a single
// non-follow request (or a follow's initial catch-up) will read for
// one step. A step can hold at most LogStepBytesMax/LogChunkBytesMax
// data chunks plus a couple of markers, so this comfortably covers a
// full step's history in one bounded scan.
const logsFetchScanMax = protocol.LogStepBytesMax/protocol.LogChunkBytesMax + 8

// logsFetchIdleWait bounds how long fetchStepLogChunks waits for the
// NEXT message before concluding the stream is exhausted (not: the
// step has no more logs ever — just none buffered right now).
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
	Chunks  []protocol.LogChunk `json:"chunks"`
	NextSeq uint64              `json:"next_seq"`
	EOF     bool                `json:"eof"`
}

// handleGetRunLogs serves GET /runs/{id}/logs?step=&after_seq=&follow=&from=.
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

	if r.URL.Query().Get("follow") == "1" {
		serveLogsFollow(svc, w, r, runID, stepID)
		return
	}
	serveLogsPage(svc, w, r, runID, stepID, stepState)
}

// afterSeqParam parses "after_seq"; absent/invalid resolves to -1
// (include from the very first chunk, seq 0).
func afterSeqParam(r *http.Request) int64 {
	val := r.URL.Query().Get("after_seq")
	if val == "" {
		return -1
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// serveLogsPage handles the non-follow read: fetch, filter (after_seq
// or from=failure), page to LogReadChunksMax, and report next_seq/eof.
func serveLogsPage(
	svc *Service, w http.ResponseWriter, r *http.Request,
	runID, stepID string, stepState dag.StepState,
) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	all, err := fetchStepLogChunks(ctx, svc.js, runID, stepID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	startIdx := 0
	afterSeq := afterSeqParam(r)
	if r.URL.Query().Get("from") == "failure" {
		startIdx = len(all) // not found → empty
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
	filtered := all[startIdx:]

	page := filtered
	truncatedPage := false
	if len(page) > protocol.LogReadChunksMax {
		page = page[:protocol.LogReadChunksMax]
		truncatedPage = true
	}

	nextSeq := uint64(afterSeq + 1)
	if len(page) > 0 {
		nextSeq = page[len(page)-1].Seq + 1
	}
	resp := logsResponse{
		Chunks:  page,
		NextSeq: nextSeq,
		EOF:     !truncatedPage && stepTerminal(stepState.Status),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode logs response", "error", err)
	}
}

// stepTerminal reports whether status is a terminal StepStatus — the
// eof condition for GET .../logs (no non-follow status implies "may
// still receive more chunks").
func stepTerminal(status dag.StepStatus) bool {
	switch status {
	case dag.StepStatusCompleted, dag.StepStatusFailed,
		dag.StepStatusSkipped, dag.StepStatusCancelled,
		dag.StepStatusRecovered:
		return true
	}
	return false
}

// fetchStepLogChunks drains up to logsFetchScanMax BUILD_LOGS messages
// for logs.{runID}.{stepID}, in stream order, stopping once no further
// message arrives within logsFetchIdleWait (interpreted as "nothing
// more buffered right now", not "never will be").
func fetchStepLogChunks(
	ctx context.Context, js jetstream.JetStream, runID, stepID string,
) ([]protocol.LogChunk, error) {
	if js == nil {
		panic("fetchStepLogChunks: js must not be nil")
	}
	if runID == "" {
		panic("fetchStepLogChunks: runID must not be empty")
	}
	subject := "logs." + runID + "." + natsutil.SubjectToken(stepID)
	cons, err := js.OrderedConsumer(ctx, "BUILD_LOGS",
		jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}})
	if err != nil {
		return nil, fmt.Errorf("open BUILD_LOGS consumer: %w", err)
	}
	var chunks []protocol.LogChunk
	for len(chunks) < logsFetchScanMax {
		if ctx.Err() != nil {
			break
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(logsFetchIdleWait))
		if err != nil {
			break
		}
		var c protocol.LogChunk
		if unmarshalErr := json.Unmarshal(msg.Data(), &c); unmarshalErr != nil {
			slog.Warn("skipping malformed BUILD_LOGS chunk",
				"error", unmarshalErr, "run_id", runID, "step_id", stepID)
			msg.Ack()
			continue
		}
		msg.Ack()
		chunks = append(chunks, c)
	}
	return chunks, nil
}
