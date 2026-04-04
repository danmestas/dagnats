package bridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// resolveRequest is the JSON body for POST /v1/tasks/{id}/resolve.
type resolveRequest struct {
	Action     string          `json:"action"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	Name       string          `json:"name,omitempty"`
	DurationMs int64           `json:"duration_ms,omitempty"`
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// pauseDurationMaxMs caps the maximum pause at 1 hour.
const pauseDurationMaxMs = 3_600_000

// handleResolve resolves a polled task by completing, failing,
// pausing, or checkpointing it.
func (b *Bridge) handleResolve(
	w http.ResponseWriter, r *http.Request,
) {
	if b.ackMap == nil {
		panic("handleResolve: ackMap must not be nil")
	}
	if b.js == nil {
		panic("handleResolve: js must not be nil")
	}
	taskID := r.PathValue("id")
	if taskID == "" {
		http.Error(
			w, "task id is required", http.StatusBadRequest,
		)
		return
	}

	msg, ok := b.ackMap.Load(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	req, err := parseResolveRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = b.dispatchAction(taskID, msg, req)
	if err != nil {
		http.Error(
			w, err.Error(), http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// parseResolveRequest validates the resolve JSON body.
func parseResolveRequest(
	r *http.Request,
) (resolveRequest, error) {
	if r == nil {
		panic("parseResolveRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("parseResolveRequest: r.Body must not be nil")
	}
	var req resolveRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON: %w", err)
	}
	if req.Action == "" {
		return req, fmt.Errorf("action is required")
	}
	validActions := map[string]bool{
		"complete":   true,
		"fail":       true,
		"pause":      true,
		"checkpoint": true,
	}
	if !validActions[req.Action] {
		return req, fmt.Errorf("invalid action: %s", req.Action)
	}
	return req, nil
}

// dispatchAction routes the resolve request to the correct handler.
func (b *Bridge) dispatchAction(
	taskID string, msg *nats.Msg, req resolveRequest,
) error {
	if taskID == "" {
		panic("dispatchAction: taskID must not be empty")
	}
	if msg == nil {
		panic("dispatchAction: msg must not be nil")
	}
	switch req.Action {
	case "complete":
		return b.resolveComplete(taskID, msg, req)
	case "fail":
		return b.resolveFail(taskID, msg, req)
	case "pause":
		return b.resolvePause(taskID, msg, req)
	case "checkpoint":
		return b.resolveCheckpoint(taskID, msg, req)
	default:
		return fmt.Errorf("unhandled action: %s", req.Action)
	}
}

// resolveComplete publishes step.completed, acks the NATS message,
// and removes the task from the ackMap.
func (b *Bridge) resolveComplete(
	taskID string, msg *nats.Msg, req resolveRequest,
) error {
	if taskID == "" {
		panic("resolveComplete: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveComplete: msg must not be nil")
	}
	runID, stepID := splitTaskID(taskID)
	evt := protocol.NewStepEvent(
		protocol.EventStepCompleted, runID, stepID, req.Output,
	)
	if err := b.publishEvent(evt); err != nil {
		return fmt.Errorf("publish complete event: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	b.ackMap.Delete(taskID)
	return nil
}

// resolveFail publishes step.failed, acks the NATS message, and
// removes the task from the ackMap.
func (b *Bridge) resolveFail(
	taskID string, msg *nats.Msg, req resolveRequest,
) error {
	if taskID == "" {
		panic("resolveFail: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveFail: msg must not be nil")
	}
	runID, stepID := splitTaskID(taskID)
	errPayload := []byte(fmt.Sprintf("%q", req.Error))
	evt := protocol.NewStepEvent(
		protocol.EventStepFailed, runID, stepID, errPayload,
	)
	if err := b.publishEvent(evt); err != nil {
		return fmt.Errorf("publish fail event: %w", err)
	}
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}
	b.ackMap.Delete(taskID)
	return nil
}

// resolvePause writes checkpoint to KV and NAKs with delay.
func (b *Bridge) resolvePause(
	taskID string, msg *nats.Msg, req resolveRequest,
) error {
	if taskID == "" {
		panic("resolvePause: taskID must not be empty")
	}
	if msg == nil {
		panic("resolvePause: msg must not be nil")
	}
	if req.DurationMs <= 0 || req.DurationMs > pauseDurationMaxMs {
		return fmt.Errorf(
			"duration_ms must be in (0, %d]", pauseDurationMaxMs,
		)
	}
	if err := b.writeCheckpoint(taskID, req.Checkpoint); err != nil {
		return err
	}
	duration := time.Duration(req.DurationMs) * time.Millisecond
	if err := msg.NakWithDelay(duration); err != nil {
		return fmt.Errorf("nak with delay: %w", err)
	}
	b.ackMap.Delete(taskID)
	return nil
}

// resolveCheckpoint writes checkpoint data to KV and extends the
// ack deadline so the task stays in-flight.
func (b *Bridge) resolveCheckpoint(
	taskID string, msg *nats.Msg, req resolveRequest,
) error {
	if taskID == "" {
		panic("resolveCheckpoint: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveCheckpoint: msg must not be nil")
	}
	if err := b.writeCheckpoint(taskID, req.Data); err != nil {
		return err
	}
	if err := msg.InProgress(); err != nil {
		return fmt.Errorf("in-progress: %w", err)
	}
	return nil
}

// writeCheckpoint stores data in the checkpoints KV bucket.
func (b *Bridge) writeCheckpoint(
	taskID string, data []byte,
) error {
	if taskID == "" {
		panic("writeCheckpoint: taskID must not be empty")
	}
	if b.checkpointKV == nil {
		return fmt.Errorf("checkpoint KV not configured")
	}
	_, err := b.checkpointKV.Put(taskID, data)
	if err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

// publishEvent marshals and publishes an event to the history
// stream with deduplication.
func (b *Bridge) publishEvent(evt protocol.Event) error {
	if evt.RunID == "" {
		panic("publishEvent: RunID must not be empty")
	}
	if evt.Type == "" {
		panic("publishEvent: Type must not be empty")
	}
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	_, err = b.js.PublishMsg(msg)
	return err
}

// splitTaskID splits a task ID into runID and stepID.
// Task IDs are formatted as {runID}.{stepID}.
func splitTaskID(taskID string) (runID, stepID string) {
	if taskID == "" {
		panic("splitTaskID: taskID must not be empty")
	}
	// Find the first dot — runID is everything before it,
	// stepID is everything after.
	for i := 0; i < len(taskID); i++ {
		if taskID[i] == '.' {
			runID = taskID[:i]
			stepID = taskID[i+1:]
			if runID == "" || stepID == "" {
				panic("splitTaskID: runID and stepID must not be empty")
			}
			return runID, stepID
		}
	}
	// No dot found — programmer error with malformed task ID.
	panic("splitTaskID: taskID must contain a dot separator")
}
