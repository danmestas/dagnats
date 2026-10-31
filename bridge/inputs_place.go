package bridge

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/journal"
)

// inputsPlaceResponse is {"inputs":[{request_id, mode, content}]};
// content is the submission's raw JSON and absent when it had none.
type inputsPlaceResponse struct {
	Inputs []journal.InputPlaced `json:"inputs"`
}

// resolveInputsPlace moves the queued inputs of req.Mode into the
// step's conversation through journal.Placer, the code the Go SDK's
// PlaceInputs runs, claiming the journal on first use. The placer lives
// on the ack-map entry so a delivery returns each placement once; the
// placed inputs settle to done when the iteration commits.
func (b *Bridge) resolveInputsPlace(
	ctx context.Context, taskID, tokenID string, msg jetstream.Msg,
	req resolveRequest, w http.ResponseWriter,
) error {
	if taskID == "" {
		panic("resolveInputsPlace: taskID must not be empty")
	}
	if msg == nil {
		panic("resolveInputsPlace: msg must not be nil")
	}
	mode := inputs.Mode(req.Mode)
	if mode != inputs.ModeSteer && mode != inputs.ModeFollowUp {
		writeJSON(w, http.StatusBadRequest, waitBody{
			Error: fmt.Sprintf("mode must be %q or %q", inputs.ModeSteer, inputs.ModeFollowUp),
			Code:  "invalid_mode",
		})
		return errResponseAlreadyWritten
	}
	return b.withJournal(ctx, taskID, tokenID, msg, w,
		func(j *journal.Journal) (any, error) {
			// Resync surfaces a fence a place with nothing to write would miss.
			if err := j.Resync(ctx); err != nil {
				return nil, err
			}
			placer, err := b.placerFor(ctx, taskID, j)
			if err != nil {
				return nil, err
			}
			placed, err := placer.Place(ctx, mode)
			if err != nil {
				return nil, err
			}
			out := inputsPlaceResponse{Inputs: placed}
			if out.Inputs == nil {
				out.Inputs = []journal.InputPlaced{}
			}
			return out, nil
		})
}

// placerFor returns the delivery's placer, creating it on first use.
// The caller holds the task's journal lock.
func (b *Bridge) placerFor(
	ctx context.Context, taskID string, j *journal.Journal,
) (*journal.Placer, error) {
	if placer := b.ackMap.Placer(taskID); placer != nil {
		return placer, nil
	}
	store, err := inputs.Open(ctx, b.js)
	if err != nil {
		return nil, err
	}
	task := j.Task()
	placer := &journal.Placer{
		Journal: j, Store: store, RunID: task.RunID, StepID: task.StepID,
		Iteration: task.Iteration,
	}
	if !b.ackMap.SetPlacer(taskID, placer) {
		return nil, errors.New("task is no longer claimed")
	}
	return placer, nil
}
