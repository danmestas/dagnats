// bridge/logs.go
// POST /v1/tasks/{id}/logs (#624): the HTTP-bridge counterpart to the
// worker SDK's LogOut()/LogErr() — lets a non-Go worker publish
// stdout/stderr-tagged chunks to the BUILD_LOGS hot lane. Ownership is
// enforced exactly like resolve (authorizeTaskOwner against the
// claiming TokenID), and the bridge — not the caller — assigns Seq and
// owns the per-step LogStepBytesMax budget, mirroring worker/log_writer.go
// so a consumer reading BUILD_LOGS sees the same chunk shape regardless
// of which lane produced it.
//
// Subject/attempt/iteration scoping (#624 review rounds 2-3): the
// subject is logs.{runID}.{stepID}.{attempt}.{iteration}, matching
// worker/log_writer.go exactly — both are read from the claimed task's
// own message (the same one authorizeTaskOwner already validated
// ownership against), NOT from the caller's request body, so an HTTP
// worker can never spoof which attempt/iteration its chunks land on.
package bridge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	// logsBodyBytesMax bounds the POST body; a body reading over this
	// via the LimitReader below is rejected 413.
	logsBodyBytesMax = 1 << 20 // 1 MiB
	// logsChunksPerRequestMax bounds how many chunks one request may
	// batch, independent of the body-size cap.
	logsChunksPerRequestMax = 256
)

// bridgeLogStepBytesMax is the effective per-step total-bytes budget
// the bridge enforces. A package var (not a bare reference to
// protocol.LogStepBytesMax) so tests can shrink it and exercise the
// truncation path without POSTing 64 MiB; production code never
// overrides it.
var bridgeLogStepBytesMax int64 = protocol.LogStepBytesMax

// logsRequest is the JSON body for POST /v1/tasks/{id}/logs.
type logsRequest struct {
	Chunks []logsRequestChunk `json:"chunks"`
}

// logsRequestChunk carries one caller-supplied chunk. Data is
// base64-encoded (the same encoding encoding/json uses for []byte),
// so callers that already marshal protocol.LogChunk-shaped values can
// reuse the same encoding step.
type logsRequestChunk struct {
	Stream string `json:"stream"`
	Data   string `json:"data"`
}

// logPlanStep is one LogChunk the ingest endpoint will publish,
// computed by planLogIngest before any NATS I/O.
type logPlanStep struct {
	stream string
	data   []byte
}

// handleLogs ingests a batch of log chunks for a claimed task. Auth
// mirrors handleResolve: the task must be in the AckMap (claimed via
// poll) and the caller must be its claiming token or an admin. Split
// from the actual ingest (ingestLogChunks) to stay under the house
// function-length budget.
func (b *Bridge) handleLogs(w http.ResponseWriter, r *http.Request) {
	if b.ackMap == nil {
		panic("handleLogs: ackMap must not be nil")
	}
	if b.pub == nil {
		panic("handleLogs: pub must not be nil")
	}
	incoming := otel.GetTextMapPropagator().Extract(
		r.Context(), propagation.HeaderCarrier(r.Header),
	)
	ctx, span := b.tracer.Start(incoming, "bridge.logs")
	defer span.End()

	taskID := r.PathValue("id")
	if taskID == "" {
		http.Error(w, "task id is required", http.StatusBadRequest)
		return
	}

	claims := claimsFromContext(r.Context())
	claimedMsg, claimingTokenID, ok := b.ackMap.LoadWithTokenID(taskID)
	if !ok {
		// #624 review round 3: authorize BEFORE deciding 409 vs 404 —
		// rejectUnclaimedLogPost/WasResolvedBy only reveal "already
		// resolved" to the SAME caller that resolved it (or admin), so
		// a token can't probe 409-vs-404 for a task it never claimed.
		b.rejectUnclaimedLogPost(w, taskID, claims)
		return
	}
	if !b.authorizeTaskOwner(claims, claimingTokenID) {
		http.Error(w, "task not claimed by this token", http.StatusForbidden)
		return
	}
	attempt, iteration, err := logsTaskAttemptIteration(claimedMsg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req, err := parseLogsRequest(r)
	if err != nil {
		if err == errLogsBodyTooLarge {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	decoded, err := decodeLogsChunks(req.Chunks)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b.ingestLogChunks(ctx, w, taskID, attempt, iteration, claims, decoded)
}

// rejectUnclaimedLogPost distinguishes 409 (already resolved BY claims
// — see AckMap.MarkResolved/WasResolvedBy, #624 review rounds 2-3)
// from 404 (never claimed by claims at all) for a taskID no longer in
// the AckMap. WasResolvedBy itself is the authorization check: it only
// returns true when claims match the TokenID that actually resolved
// the task (or claims.Admin), so a caller can never learn "this task
// was resolved" for a task it never owned.
func (b *Bridge) rejectUnclaimedLogPost(
	w http.ResponseWriter, taskID string, claims workertoken.Claims,
) {
	if b.ackMap.WasResolvedBy(taskID, claims) {
		http.Error(w, "task already resolved; log ingest closed",
			http.StatusConflict)
		return
	}
	http.Error(w, "task not found", http.StatusNotFound)
}

// ingestLogChunks assigns seq/truncation state and publishes the
// decoded chunks for an already-authorized request.
func (b *Bridge) ingestLogChunks(
	ctx context.Context, w http.ResponseWriter,
	taskID string, attempt, iteration int, claims workertoken.Claims,
	decoded []decodedChunk,
) {
	runID, stepID := splitTaskID(taskID)
	var steps []logPlanStep
	var startSeq uint64
	found := b.ackMap.WithLogState(taskID, func(
		seq uint64, totalBytes int64, truncated bool,
	) (uint64, int64, bool) {
		startSeq = seq
		steps, seq, totalBytes, truncated = planLogIngest(
			seq, totalBytes, truncated, decoded,
		)
		return seq, totalBytes, truncated
	})
	if !found {
		// Raced the reaper/resolve between the ownership check in
		// handleLogs and here.
		b.rejectUnclaimedLogPost(w, taskID, claims)
		return
	}
	b.publishLogSteps(ctx, runID, stepID, attempt, iteration, startSeq, steps)
	w.WriteHeader(http.StatusOK)
}

// logsTaskAttemptIteration unmarshals the claimed task's original
// message once and returns both dimensions of BUILD_LOGS subject
// identity (logs.{runID}.{stepID}.{attempt}.{iteration}, #624 review
// round 3): attempt is resolved to the 1-based AttemptNumber via
// taskAttemptNumber (bridge/poll.go) — the SAME numbering
// step.started/step.failed's AttemptNumber field and
// worker/log_writer.go's resolveAttemptNumber use, so a consumer
// correlating BUILD_LOGS with the lifecycle history stream sees one
// consistent identity regardless of which lane (native worker or HTTP
// bridge) produced it, and GET .../logs's default ?attempt= resolves
// to the right subject. iteration needs no such resolution — the
// engine always sets protocol.TaskPayload.Iteration explicitly (0 for
// a non-loop step; N for an agent-loop step's Nth Continue
// re-dispatch, internal/engine/task_publisher.go's PublishIteration)
// — so the raw payload value IS the answer.
func logsTaskAttemptIteration(msg jetstream.Msg) (attempt, iteration int, err error) {
	if msg == nil {
		panic("logsTaskAttemptIteration: msg must not be nil")
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		return 0, 0, fmt.Errorf("unmarshal task payload: %w", err)
	}
	attempt, err = taskAttemptNumber(msg, payload.Attempt)
	if err != nil {
		return 0, 0, err
	}
	return attempt, payload.Iteration, nil
}

var errLogsBodyTooLarge = fmt.Errorf("request body exceeds %d bytes", logsBodyBytesMax)

// parseLogsRequest reads and validates the POST body, bounded to
// logsBodyBytesMax and logsChunksPerRequestMax.
func parseLogsRequest(r *http.Request) (logsRequest, error) {
	if r == nil {
		panic("parseLogsRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("parseLogsRequest: r.Body must not be nil")
	}
	limited := io.LimitReader(r.Body, logsBodyBytesMax+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return logsRequest{}, fmt.Errorf("read body: %w", err)
	}
	if len(body) > logsBodyBytesMax {
		return logsRequest{}, errLogsBodyTooLarge
	}
	var req logsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return logsRequest{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if len(req.Chunks) > logsChunksPerRequestMax {
		return logsRequest{}, fmt.Errorf(
			"too many chunks: %d exceeds max %d",
			len(req.Chunks), logsChunksPerRequestMax,
		)
	}
	return req, nil
}

// decodedChunk is a request chunk after stream validation and base64
// decoding, ready for planLogIngest.
type decodedChunk struct {
	stream string
	data   []byte
}

// decodeLogsChunks validates each chunk's stream tag and decodes its
// base64 data. Bounded by the caller-enforced logsChunksPerRequestMax.
func decodeLogsChunks(chunks []logsRequestChunk) ([]decodedChunk, error) {
	out := make([]decodedChunk, 0, len(chunks))
	for i, c := range chunks {
		if c.Stream != protocol.LogStreamOut && c.Stream != protocol.LogStreamErr {
			return nil, fmt.Errorf(
				"chunk %d: invalid stream %q (want %q or %q)",
				i, c.Stream, protocol.LogStreamOut, protocol.LogStreamErr,
			)
		}
		data, err := base64.StdEncoding.DecodeString(c.Data)
		if err != nil {
			return nil, fmt.Errorf("chunk %d: invalid base64 data: %w", i, err)
		}
		out = append(out, decodedChunk{stream: c.Stream, data: data})
	}
	return out, nil
}

// planLogIngest decides which LogChunks to publish next from the
// current per-task counters and the incoming chunks — splitting any
// chunk over protocol.LogChunkBytesMax and stopping, with one trailing
// truncated marker, once bridgeLogStepBytesMax is reached. Pure: no
// I/O, no locking, so it is safe to call from inside AckMap.WithLogState.
func planLogIngest(
	seq uint64, totalBytes int64, truncated bool,
	chunks []decodedChunk,
) (steps []logPlanStep, newSeq uint64, newTotalBytes int64, newTruncated bool) {
	newSeq, newTotalBytes, newTruncated = seq, totalBytes, truncated
	if newTruncated {
		return nil, newSeq, newTotalBytes, newTruncated
	}
	appendStep := func(stream string, data []byte) {
		steps = append(steps, logPlanStep{stream: stream, data: data})
		newSeq++
	}
chunkLoop:
	for _, c := range chunks {
		remaining := c.data
		if len(remaining) == 0 {
			continue
		}
		for len(remaining) > 0 {
			budget := bridgeLogStepBytesMax - newTotalBytes
			if budget <= 0 {
				appendStep(protocol.LogStreamMarker, []byte(protocol.LogMarkerTruncated))
				newTruncated = true
				break chunkLoop
			}
			take := int64(len(remaining))
			if take > protocol.LogChunkBytesMax {
				take = protocol.LogChunkBytesMax
			}
			if take > budget {
				take = budget
			}
			appendStep(c.stream, append([]byte(nil), remaining[:take]...))
			newTotalBytes += take
			remaining = remaining[take:]
			if newTotalBytes >= bridgeLogStepBytesMax {
				appendStep(protocol.LogStreamMarker, []byte(protocol.LogMarkerTruncated))
				newTruncated = true
				break chunkLoop
			}
		}
	}
	return steps, newSeq, newTotalBytes, newTruncated
}

// emitLogMarker publishes marker (one of the protocol.LogMarker*
// constants) as the LAST message on taskID's current attempt subject,
// via the same seq counter POST /v1/tasks/{id}/logs uses — the
// HTTP-bridge counterpart to worker/log_writer.go's drainWithMarker
// (#624 review round 2: bridge never emitted a terminal marker at all,
// so from=failure/follow's eof for an HTTP worker had nothing reliable
// to key off). Call this BEFORE the resolution's own publish/ack/NAK
// and BEFORE removing the task from the AckMap — WithLogState needs
// the live entry to assign seq. Best-effort like every other log
// publish in this package: a failure here is logged, never propagated,
// so a BUILD_LOGS hiccup can never block a task from resolving.
func (b *Bridge) emitLogMarker(
	ctx context.Context, taskID string, msg jetstream.Msg, marker string,
) {
	if taskID == "" {
		panic("emitLogMarker: taskID must not be empty")
	}
	if marker == "" {
		panic("emitLogMarker: marker must not be empty")
	}
	attempt, iteration, err := logsTaskAttemptIteration(msg)
	if err != nil {
		slog.Warn("emitLogMarker: derive attempt/iteration failed",
			"error", err, "task_id", taskID)
		return
	}
	runID, stepID := splitTaskID(taskID)
	steps := []logPlanStep{{stream: protocol.LogStreamMarker, data: []byte(marker)}}
	var startSeq uint64
	found := b.ackMap.WithLogState(taskID, func(
		seq uint64, totalBytes int64, truncated bool,
	) (uint64, int64, bool) {
		startSeq = seq
		return seq + 1, totalBytes, truncated
	})
	if !found {
		slog.Warn("emitLogMarker: no AckMap entry (already resolved or reaped)",
			"task_id", taskID, "marker", marker)
		return
	}
	b.publishLogSteps(ctx, runID, stepID, attempt, iteration, startSeq, steps)
}

// publishLogSteps publishes each planned step as its own LogChunk,
// assigning seq in order starting from startSeq — the counter value
// WithLogState's callback observed BEFORE planLogIngest advanced it
// for this batch, so seq assignment matches the state transition that
// already committed under the AckMap lock even if a concurrent
// request's batch is mid-publish. A publish failure is logged and the
// chunk dropped, never retried or blocking the caller — same policy
// as worker/log_writer.go.
func (b *Bridge) publishLogSteps(
	ctx context.Context, runID, stepID string, attempt, iteration int,
	startSeq uint64, steps []logPlanStep,
) {
	if len(steps) == 0 {
		return
	}
	if strings.ContainsAny(runID, ". \t*>") {
		panic("publishLogSteps: runID must not contain NATS subject metacharacters")
	}
	// SubjectToken(stepID) feeds both the subject and the Msg-Id below
	// so they reflect the same sanitized identity — matching
	// worker/log_writer.go's publishLocked (#624 review).
	stepToken := natsutil.SubjectToken(stepID)
	subject := fmt.Sprintf("logs.%s.%s.%d.%d", runID, stepToken, attempt, iteration)
	for i, step := range steps {
		seq := startSeq + uint64(i)
		chunk := protocol.LogChunk{
			Seq:       seq,
			Attempt:   attempt,
			Iteration: iteration,
			TS:        time.Now(),
			Stream:    step.stream,
			Data:      step.data,
		}
		payload, err := json.Marshal(chunk)
		if err != nil {
			slog.Error("marshal log chunk failed",
				"error", err, "run_id", runID, "step_id", stepID)
			continue
		}
		msgID := fmt.Sprintf("log-%s-%s-%d-%d-%d",
			runID, stepToken, attempt, iteration, seq)
		msg := &nats.Msg{
			Subject: subject,
			Data:    payload,
			Header:  nats.Header{"Nats-Msg-Id": {msgID}},
		}
		if _, err := b.pub.JSPublishMsg(ctx, msg); err != nil {
			if b.logChunkFailures != nil {
				b.logChunkFailures.Add(context.Background(), 1)
			}
			slog.Error("publish log chunk failed",
				"error", err, "run_id", runID, "step_id", stepID,
				"stream", step.stream)
		}
	}
}
