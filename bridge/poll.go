package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// pollRequest is the JSON body for POST /v1/tasks/poll.
type pollRequest struct {
	TaskTypes []string `json:"task_types"`
	MaxTasks  int      `json:"max_tasks"`
	TimeoutMs int64    `json:"timeout_ms"`
}

// pollResponse is a single task returned from a poll.
type pollResponse struct {
	TaskID    string          `json:"task_id"`
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id"`
	Iteration int             `json:"iteration,omitempty"`
	Attempt   int             `json:"attempt,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
}

// pollTimeoutMaxMs caps the maximum long-poll timeout at 60 seconds.
const pollTimeoutMaxMs = 60_000

// handlePoll long-polls for tasks from the TASK_QUEUES stream.
// Returns a JSON array of task payloads, or an empty array on
// timeout.
func (b *Bridge) handlePoll(
	w http.ResponseWriter, r *http.Request,
) {
	if b.js == nil {
		panic("handlePoll: js must not be nil")
	}
	if b.ackMap == nil {
		panic("handlePoll: ackMap must not be nil")
	}
	ctx, span := b.tracer.Start(r.Context(), "bridge.poll")
	defer span.End()

	start := time.Now()
	req, err := parsePollRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tasks := b.fetchTasks(ctx, req)

	elapsed := time.Since(start).Milliseconds()
	b.requestCount.Add(ctx, 1)
	b.requestDuration.Record(ctx, float64(elapsed))
	slog.InfoContext(ctx, "poll completed",
		"task_count", len(tasks),
		"elapsed_ms", elapsed,
	)

	writePollResponse(w, tasks)
}

// parsePollRequest validates the poll JSON body.
func parsePollRequest(r *http.Request) (pollRequest, error) {
	if r == nil {
		panic("parsePollRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("parsePollRequest: r.Body must not be nil")
	}
	var req pollRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON: %w", err)
	}
	if len(req.TaskTypes) == 0 {
		return req, fmt.Errorf("task_types is required")
	}
	if req.MaxTasks <= 0 {
		req.MaxTasks = 1
	}
	if req.TimeoutMs <= 0 {
		req.TimeoutMs = 5000
	}
	if req.TimeoutMs > pollTimeoutMaxMs {
		req.TimeoutMs = pollTimeoutMaxMs
	}
	return req, nil
}

// fetchTasks pulls messages from NATS for each task type.
// Stores each fetched message in the ackMap so resolve can
// ack/nak it later.
func (b *Bridge) fetchTasks(
	ctx context.Context, req pollRequest,
) []pollResponse {
	if ctx == nil {
		panic("fetchTasks: ctx must not be nil")
	}
	if len(req.TaskTypes) == 0 {
		panic("fetchTasks: task_types must not be empty")
	}
	if req.MaxTasks <= 0 {
		panic("fetchTasks: max_tasks must be positive")
	}
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	tasks := make([]pollResponse, 0, req.MaxTasks)
	remaining := req.MaxTasks

	for _, taskType := range req.TaskTypes {
		if remaining <= 0 {
			break
		}
		fetched := b.fetchForType(ctx, taskType, remaining, timeout)
		tasks = append(tasks, fetched...)
		remaining -= len(fetched)
	}
	return tasks
}

// fetchForType creates an ephemeral consumer for one task type
// and fetches up to count messages. Each message is stored in
// the ackMap.
func (b *Bridge) fetchForType(
	ctx context.Context,
	taskType string, count int, timeout time.Duration,
) []pollResponse {
	if ctx == nil {
		panic("fetchForType: ctx must not be nil")
	}
	if taskType == "" {
		panic("fetchForType: taskType must not be empty")
	}
	if count <= 0 {
		panic("fetchForType: count must be positive")
	}
	subject := "task." + taskType + ".>"
	stream, err := b.js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		return nil
	}
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject:     subject,
			AckPolicy:         jetstream.AckExplicitPolicy,
			InactiveThreshold: timeout,
		},
	)
	if err != nil {
		return nil
	}
	fetchResult, err := cons.Fetch(
		count, jetstream.FetchMaxWait(timeout),
	)
	if err != nil {
		return nil
	}
	tasks := make([]pollResponse, 0, count)
	for msg := range fetchResult.Messages() {
		resp, ok := b.processPolledMsg(ctx, msg)
		if ok {
			tasks = append(tasks, resp)
		}
	}
	return tasks
}

// taskAttemptCountMax bounds the attempt number accepted from a
// polled task message. The engine caps retries via RetryPolicy
// MaxAttempts and NATS redeliveries via consumer limits; an attempt
// beyond this bound means a corrupted payload, and rejecting it
// beats minting unbounded per-attempt msg-ids.
const taskAttemptCountMax = 100_000

// processPolledMsg unmarshals a NATS message into a poll response,
// publishes step.started for the attempt, and stores the message in
// the ackMap for later resolution.
//
// step.started must land before the task is handed to the HTTP
// worker: the engine's handleStepStarted max()es AttemptNumber into
// run.Steps[id].Attempts, which scheduleRetryBackoff uses to build
// per-attempt SLEEP_TIMERS msg-ids. Without it, bridge-executed
// steps pin Attempts at 1 and retry #2's timer is JetStream-deduped
// — the run hangs and never dead-letters (issue #381). On publish
// failure the message is NAKed, not handed out, so the engine never
// sees a resolve for an attempt it never saw start.
func (b *Bridge) processPolledMsg(
	ctx context.Context, msg jetstream.Msg,
) (pollResponse, bool) {
	if msg == nil {
		panic("processPolledMsg: msg must not be nil")
	}
	if b.ackMap == nil {
		panic("processPolledMsg: ackMap must not be nil")
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		msg.Ack()
		return pollResponse{}, false
	}
	if payload.RunID == "" || payload.StepID == "" {
		msg.Ack() // Malformed task — same disposition as bad JSON.
		return pollResponse{}, false
	}
	attemptNumber, err := taskAttemptNumber(msg, payload.Attempt)
	if err != nil {
		nakPolledMsg(ctx, msg, "derive attempt number", err)
		return pollResponse{}, false
	}
	err = b.publishStepStarted(
		ctx, payload.RunID, payload.StepID, attemptNumber,
	)
	if err != nil {
		nakPolledMsg(ctx, msg, "publish step.started", err)
		return pollResponse{}, false
	}
	taskID := payload.RunID + "." + payload.StepID
	b.ackMap.Store(taskID, msg)
	resp := pollResponse{
		TaskID:    taskID,
		RunID:     payload.RunID,
		StepID:    payload.StepID,
		Iteration: payload.Iteration,
		Attempt:   attemptNumber,
		Input:     payload.Input,
	}
	return resp, true
}

// taskAttemptNumber derives the 1-based attempt number for a polled
// task message. Preference order mirrors the native worker's
// publishStarted (worker/context.go): payload.Attempt when set — the
// engine's SLEEP_TIMERS re-publish carries it, and the fresh message's
// NumDelivered=1 would lose the count — else NATS NumDelivered, which
// is correct for in-place redelivery of the original dispatch.
func taskAttemptNumber(
	msg jetstream.Msg, payloadAttempt int,
) (int, error) {
	if msg == nil {
		panic("taskAttemptNumber: msg must not be nil")
	}
	if payloadAttempt < 0 {
		// The engine never writes a negative attempt — corrupt data.
		panic("taskAttemptNumber: payloadAttempt must not be negative")
	}
	attemptNumber := payloadAttempt
	if attemptNumber == 0 {
		meta, err := msg.Metadata()
		if err != nil {
			return 0, fmt.Errorf("read msg metadata: %w", err)
		}
		attemptNumber = int(meta.NumDelivered)
	}
	if attemptNumber < 1 || attemptNumber > taskAttemptCountMax {
		return 0, fmt.Errorf(
			"attempt %d outside [1, %d]",
			attemptNumber, taskAttemptCountMax,
		)
	}
	return attemptNumber, nil
}

// publishStepStarted publishes step.started carrying AttemptNumber —
// the bridge-side mirror of the native worker's publishStarted. The
// AttemptNumber rides into Event.NATSMsgID so each attempt's
// lifecycle events stay distinct under JetStream dedup.
func (b *Bridge) publishStepStarted(
	ctx context.Context,
	runID, stepID string,
	attemptNumber int,
) error {
	if ctx == nil {
		panic("publishStepStarted: ctx must not be nil")
	}
	if attemptNumber < 1 {
		panic("publishStepStarted: attemptNumber must be >= 1")
	}
	if attemptNumber > taskAttemptCountMax {
		panic(
			"publishStepStarted: attemptNumber exceeds taskAttemptCountMax",
		)
	}
	// NewStepEvent asserts non-empty runID / stepID.
	evt := protocol.NewStepEvent(
		protocol.EventStepStarted, runID, stepID, nil,
	)
	evt.AttemptNumber = attemptNumber
	return b.publishEvent(ctx, evt)
}

// nakPolledMsg returns a message to the queue when the bridge could
// not safely hand it to a poller. NAK (not ack): the attempt must
// redeliver rather than be silently consumed.
func nakPolledMsg(
	ctx context.Context,
	msg jetstream.Msg,
	cause string,
	causeErr error,
) {
	if msg == nil {
		panic("nakPolledMsg: msg must not be nil")
	}
	if cause == "" {
		panic("nakPolledMsg: cause must not be empty")
	}
	slog.WarnContext(ctx, "polled task returned to queue",
		"cause", cause,
		"error", causeErr,
	)
	if err := msg.Nak(); err != nil {
		slog.WarnContext(ctx, "nak polled task failed",
			"cause", cause,
			"error", err,
		)
	}
}

// writePollResponse encodes the tasks as a JSON array.
func writePollResponse(
	w http.ResponseWriter, tasks []pollResponse,
) {
	if w == nil {
		panic("writePollResponse: w must not be nil")
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tasks); err != nil {
		http.Error(
			w, "encode failed", http.StatusInternalServerError,
		)
	}
}
