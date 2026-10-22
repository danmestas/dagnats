// internal/api/journal_stream.go
// GET /runs/{run}/steps/{step}/journal (#808): the client read of an
// agent step's journal (ADR-026). The cursor is the JetStream sequence
// of a journal record, so a client resumes from any record it has seen.
// Non-follow returns one bounded page; follow=1 streams records as
// Server-Sent Events, each framed with id: {sequence}, so a dropped
// connection resumes through the Last-Event-ID header with no gap or
// duplicate. The follow shares its admission cap, duration bound and
// SSE framing with GET /runs/{id}/logs (logs_follow.go).
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

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// journalPageMax bounds one non-follow response, in records.
const journalPageMax = 1024

// journalPollInterval is the follow's bounded wait on its consumer. An
// idle wait is also when the follow checks whether the step ended, so
// it is the worst-case delay of the eof event.
var journalPollInterval = 2 * time.Second

// journalResponse is the non-follow body. NextFrom is the sequence to
// pass as from= for the following page; EOF means the step is over and
// the page reached the end of its journal.
type journalResponse struct {
	Records  []journal.Record `json:"records"`
	NextFrom uint64           `json:"next_from"`
	EOF      bool             `json:"eof"`
}

// handleGetStepJournal serves GET /runs/{run}/steps/{step}/journal. It
// admits exactly the callers run logs admit: the run must exist and
// the step must belong to it.
func handleGetStepJournal(svc *Service, w http.ResponseWriter, r *http.Request) {
	if svc == nil {
		panic("handleGetStepJournal: svc must not be nil")
	}
	if r == nil {
		panic("handleGetStepJournal: r must not be nil")
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/runs/"), "/")
	runID, stepID := parts[0], parts[2]
	// Bounded so a stalled KV read (a cluster mid-election) answers 500
	// for the client to retry instead of holding the connection open.
	lookupCtx, cancelLookup := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancelLookup()
	run, err := svc.GetRun(lookupCtx, runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	state, known := run.Steps[stepID]
	if !known {
		http.Error(w, "step not found", http.StatusNotFound)
		return
	}
	from := journalFrom(r)
	if r.URL.Query().Get("follow") == "1" {
		serveJournalFollow(svc, w, r, runID, stepID, from)
		return
	}
	serveJournalPage(svc, w, r, runID, stepID, from, stepTerminal(state.Status))
}

// journalFrom is the first sequence to deliver (0 = the start). A
// Last-Event-ID header names the last record the client received, so
// it resumes one past it, and it wins over from=: an EventSource that
// reconnects resends its original URL together with the header.
func journalFrom(r *http.Request) uint64 {
	if r == nil {
		panic("journalFrom: r must not be nil")
	}
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		if n, err := strconv.ParseUint(last, 10, 64); err == nil {
			return n + 1
		}
	}
	n, err := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// journalConsumerSpec is the one ordered-consumer spec both reads use.
func journalConsumerSpec(runID, stepID string, from uint64) natsutil.OrderedConsumerSpec {
	if runID == "" || stepID == "" {
		panic("journalConsumerSpec: run and step must not be empty")
	}
	spec := natsutil.OrderedConsumerSpec{
		FilterSubjects: []string{natsutil.JournalSubject(runID, stepID)},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	}
	if from > 0 {
		spec.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		spec.OptStartSeq = from
	}
	return spec
}

// serveJournalPage answers one bounded page starting at from.
func serveJournalPage(
	svc *Service, w http.ResponseWriter, r *http.Request,
	runID, stepID string, from uint64, stepDone bool,
) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	cons, err := natsutil.OpenConsumer(
		ctx, svc.js, natsutil.AgentJournalStream, journalConsumerSpec(runID, stepID, from),
	)
	if err != nil {
		writeJournalReadError(w, err)
		return
	}
	records := make([]journal.Record, 0, 16)
	for len(records) < journalPageMax && ctx.Err() == nil {
		msg, err := cons.Next(jetstream.FetchMaxWait(logsFetchIdleWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				break
			}
			writeJournalReadError(w, err)
			return
		}
		record, ok := decodeJournalMsg(msg, runID, stepID)
		if ok {
			records = append(records, record)
		}
	}
	resp := journalResponse{Records: records, NextFrom: from}
	if n := len(records); n > 0 {
		resp.NextFrom = records[n-1].Seq + 1
	}
	resp.EOF = stepDone && len(records) < journalPageMax
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode journal response", "error", err)
	}
}

// decodeJournalMsg acks msg and decodes it; ok is false for a message
// whose metadata is unreadable, which is logged and skipped.
func decodeJournalMsg(msg jetstream.Msg, runID, stepID string) (journal.Record, bool) {
	record, err := journal.DecodeMsg(msg)
	if ackErr := msg.Ack(); ackErr != nil {
		slog.Warn("ack AGENT_JOURNAL message failed",
			"error", ackErr, "run_id", runID, "step_id", stepID)
	}
	if err != nil {
		slog.Warn("skipping undecodable journal message",
			"error", err, "run_id", runID, "step_id", stepID)
		return journal.Record{}, false
	}
	return record, true
}

// writeJournalReadError maps a journal read failure onto a status; a
// missing stream is a 503, like the log lane.
func writeJournalReadError(w http.ResponseWriter, cause error) {
	if cause == nil {
		panic("writeJournalReadError: cause must not be nil")
	}
	if errors.Is(cause, jetstream.ErrStreamNotFound) {
		slog.Warn("AGENT_JOURNAL stream unavailable for journal read", "error", cause)
		writeJSONError(w, http.StatusServiceUnavailable, "journal stream unavailable")
		return
	}
	http.Error(w, cause.Error(), http.StatusInternalServerError)
}

// serveJournalFollow streams the step's journal as SSE from `from`
// over one ordered consumer, until the step ends, the client leaves or
// followDurationMax passes.
func serveJournalFollow(
	svc *Service, w http.ResponseWriter, r *http.Request,
	runID, stepID string, from uint64,
) {
	release, flusher, ok := beginFollow(w)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), followDurationMax)
	defer cancel()
	cons, err := natsutil.OpenConsumer(
		ctx, svc.js, natsutil.AgentJournalStream, journalConsumerSpec(runID, stepID, from),
	)
	if err != nil {
		writeJournalReadError(w, err)
		return
	}
	startSSE(w, flusher)
	runJournalFollowLoop(ctx, svc, w, flusher, cons, runID, stepID)
}

// runJournalFollowLoop is fetch (bounded wait) -> emit, repeated. A
// wait that times out means the follow is caught up: that is when it
// writes a keepalive, confirms the stream still exists and ends with
// eof if the step is over.
func runJournalFollowLoop(
	ctx context.Context, svc *Service, w http.ResponseWriter, flusher http.Flusher,
	cons jetstream.Consumer, runID, stepID string,
) {
	lastWrite := time.Now()
	for ctx.Err() == nil {
		msg, err := cons.Next(jetstream.FetchMaxWait(journalPollInterval))
		if err == nil {
			record, ok := decodeJournalMsg(msg, runID, stepID)
			if ok && !writeJournalRecordEvent(w, flusher, record) {
				return
			}
			lastWrite = time.Now()
			continue
		}
		if !errors.Is(err, nats.ErrTimeout) {
			writeLogsErrorEvent(w, flusher, err)
			return
		}
		if time.Since(lastWrite) >= logsKeepaliveInterval {
			if _, werr := fmt.Fprint(w, ": keepalive\n\n"); werr != nil {
				return
			}
			flusher.Flush()
			lastWrite = time.Now()
			if streamMissing(ctx, svc.js, natsutil.AgentJournalStream) {
				writeLogsErrorEvent(w, flusher, errJournalStreamUnavailable)
				return
			}
		}
		if journalStepEnded(ctx, svc, runID, stepID) {
			writeLogsEOFEvent(w, flusher, "")
			return
		}
	}
}

// errJournalStreamUnavailable is reported when the follow finds the
// journal stream gone.
var errJournalStreamUnavailable = errors.New("journal stream unavailable")

// journalStepEnded reports whether the step reached a terminal status.
// A failed lookup reports false so the next idle tick retries.
func journalStepEnded(ctx context.Context, svc *Service, runID, stepID string) bool {
	run, err := svc.GetRun(ctx, runID)
	if err != nil {
		return false
	}
	state, known := run.Steps[stepID]
	return known && stepTerminal(state.Status)
}

// writeJournalRecordEvent frames one record; id is its sequence so
// Last-Event-ID resumes at the next one. False means the client is gone.
func writeJournalRecordEvent(
	w http.ResponseWriter, flusher http.Flusher, record journal.Record,
) bool {
	data, err := json.Marshal(record)
	if err != nil {
		slog.Error("marshal SSE journal record", "error", err, "seq", record.Seq)
		return true
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: record\ndata: %s\n\n", record.Seq, data); err != nil {
		return false
	}
	flusher.Flush()
	return true
}
