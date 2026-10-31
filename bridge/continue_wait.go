package bridge

import (
	"context"
	"errors"
	"net/http"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/protocol"
)

// invalidWaitError is a `wait` the engine could not honor.
type invalidWaitError struct{ msg string }

func (e *invalidWaitError) Error() string { return e.msg }

// waitBody is the typed error body for a refused wait.
type waitBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// continueWait validates the wait of a continue and returns it as the
// protocol shape the Go SDK commits, nil for none. A wait for input is
// refused while submissions are queued, by the same check the Go SDK
// makes (inputs.Store.RefuseIfQueued): a wake for them would never come.
func (b *Bridge) continueWait(
	ctx context.Context, taskID string, in *protocol.ContinueWait,
) (*protocol.ContinueWait, error) {
	if ctx == nil {
		panic("continueWait: ctx must not be nil")
	}
	if taskID == "" {
		panic("continueWait: taskID must not be empty")
	}
	if in == nil {
		return nil, nil
	}
	if in.DelayMs < 0 {
		return nil, &invalidWaitError{msg: "wait.delay_ms must not be negative"}
	}
	wait := &protocol.ContinueWait{DelayMs: in.DelayMs, Input: in.Input}
	if in.Until != nil {
		if in.Until.IsZero() {
			return nil, &invalidWaitError{msg: "wait.until must be a real time"}
		}
		utc := in.Until.UTC()
		wait.Until = &utc
	}
	if wait.IsZero() {
		return nil, nil
	}
	if !wait.Input {
		return wait, nil
	}
	runID, stepID := splitTaskID(taskID)
	store, err := inputs.Open(ctx, b.js)
	if err != nil {
		return nil, err
	}
	if err := store.RefuseIfQueued(ctx, runID, stepID); err != nil {
		return nil, err
	}
	return wait, nil
}

// writeWaitRefusal answers the two typed refusals of a wait: 400
// invalid_wait and 409 inputs_queued. False when err is neither.
func writeWaitRefusal(w http.ResponseWriter, err error) bool {
	var invalid *invalidWaitError
	if errors.As(err, &invalid) {
		writeJSON(w, http.StatusBadRequest, waitBody{Error: err.Error(), Code: "invalid_wait"})
		return true
	}
	if errors.Is(err, inputs.ErrQueued) {
		writeJSON(w, http.StatusConflict, waitBody{
			Error: err.Error(), Code: "inputs_queued",
		})
		return true
	}
	return false
}
