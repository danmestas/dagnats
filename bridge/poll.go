package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/observe"
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
	ctx, span := b.tel.Tracer.Start(r.Context(), "bridge.poll")
	defer span.End()

	start := time.Now()
	req, err := parsePollRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tasks := b.fetchTasks(ctx, req)

	elapsed := time.Since(start).Milliseconds()
	b.requestCount.Inc()
	b.requestDuration.Observe(float64(elapsed))
	b.tel.Logger.Info("poll completed",
		observe.Int("task_count", len(tasks)),
		observe.Int("elapsed_ms", int(elapsed)),
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
		resp, ok := b.processPolledMsg(msg)
		if ok {
			tasks = append(tasks, resp)
		}
	}
	return tasks
}

// processPolledMsg unmarshals a NATS message into a poll response
// and stores it in the ackMap for later resolution.
func (b *Bridge) processPolledMsg(
	msg jetstream.Msg,
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
	taskID := payload.RunID + "." + payload.StepID
	b.ackMap.Store(taskID, msg)
	resp := pollResponse{
		TaskID:    taskID,
		RunID:     payload.RunID,
		StepID:    payload.StepID,
		Iteration: payload.Iteration,
		Attempt:   payload.Attempt,
		Input:     payload.Input,
	}
	return resp, true
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
