package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// historyStreamName is the stream every run's events land on, under
// "history.<runID>" (protocol.Event.NATSSubject).
const historyStreamName = "WORKFLOW_HISTORY"

// startEventScanMax bounds the history messages read looking for a
// run's workflow.started. Before the orchestrator handles a start the
// run's subject holds only that event, a redelivered copy or two, and
// cancels, so the start is among the first few.
const startEventScanMax = 16

// LoadAccepted is Load for readers outside the orchestrator (#1006). A
// run the API acknowledged has its workflow.started in the history
// stream before the orchestrator handles it and writes the snapshot;
// LoadAccepted answers such a run as pending, built from that event,
// with no Revision (it is not a snapshot and must never be saved).
// Everything else answers exactly as Load does, including
// ErrRunNotFound for a run whose snapshot was deleted (pruned): its
// start event may outlive it, and must not bring it back.
func (s *SnapshotStore) LoadAccepted(ctx context.Context, runID string) (dag.WorkflowRun, error) {
	if ctx == nil {
		panic("LoadAccepted: ctx must not be nil")
	}
	if runID == "" {
		panic("LoadAccepted: runID must not be empty")
	}
	run, deleted, err := s.loadOrMarker(ctx, runID)
	if !errors.Is(err, ErrRunNotFound) || deleted {
		return run, err
	}
	evt, found, err := findStartEvent(ctx, s.nc, runID)
	if err != nil {
		return dag.WorkflowRun{}, err
	}
	if !found {
		return dag.WorkflowRun{}, ErrRunNotFound
	}
	run, ok := pendingRunFromStart(evt)
	if !ok {
		return dag.WorkflowRun{}, ErrRunNotFound
	}
	return run, nil
}

// historySubject returns runID's history subject, or false when runID
// could not name a single run: a wildcard or empty token would make the
// subject match other runs' events. The rule is the KV key rule, which
// every stored run ID already meets ("run.<runID>" is its snapshot key).
func historySubject(runID string) (string, bool) {
	if !natsutil.ValidKVKey(runID) {
		return "", false
	}
	return "history." + runID, true
}

// findStartEvent reads runID's workflow.started from the history
// stream through the stream leader, so a start the API just published
// is visible to a reader routed to any server.
func findStartEvent(
	ctx context.Context, nc *nats.Conn, runID string,
) (protocol.Event, bool, error) {
	if nc == nil {
		panic("findStartEvent: nc must not be nil")
	}
	if runID == "" {
		panic("findStartEvent: runID must not be empty")
	}
	subject, ok := historySubject(runID)
	if !ok {
		return protocol.Event{}, false, nil
	}
	seq := uint64(1)
	for range startEventScanMax {
		msg, err := natsutil.StreamMsgLeader(ctx, nc, historyStreamName,
			map[string]any{"seq": seq, "next_by_subj": subject})
		if err != nil || msg == nil {
			return protocol.Event{}, false, err
		}
		evt, err := protocol.UnmarshalEvent(msg.Data)
		// The run_id check keeps a hand-published event whose body
		// disagrees with its subject from naming another run.
		if err == nil && evt.Type == protocol.EventWorkflowStarted && evt.RunID == runID {
			return evt, true, nil
		}
		seq = msg.Sequence + 1
	}
	return protocol.Event{}, false, nil
}

// cancelInHistory reports whether runID's latest history event is a
// workflow.cancelled. Called by a start that found no snapshot, when
// the run's subject holds only its start and any cancels sent since.
func cancelInHistory(ctx context.Context, nc *nats.Conn, runID string) (bool, error) {
	if nc == nil {
		panic("cancelInHistory: nc must not be nil")
	}
	if runID == "" {
		panic("cancelInHistory: runID must not be empty")
	}
	subject, ok := historySubject(runID)
	if !ok {
		return false, nil
	}
	msg, err := natsutil.StreamMsgLeader(ctx, nc, historyStreamName,
		map[string]any{"last_by_subj": subject})
	if err != nil {
		return false, fmt.Errorf("read latest event of run %q: %w", runID, err)
	}
	if msg == nil {
		return false, nil
	}
	evt, err := protocol.UnmarshalEvent(msg.Data)
	return err == nil && evt.Type == protocol.EventWorkflowCancelled, nil
}

// pendingRunFromStart builds the pending run a workflow.started
// describes, in any payload shape resolveStartPayload accepts: the API's
// {workflow_def, input, labels}, a trigger envelope naming workflow_id,
// or a bare def. A trigger envelope carries no def, so its run has no
// steps yet. ok is false when the payload names no workflow.
func pendingRunFromStart(evt protocol.Event) (dag.WorkflowRun, bool) {
	if evt.RunID == "" {
		panic("pendingRunFromStart: RunID must not be empty")
	}
	if evt.Type != protocol.EventWorkflowStarted {
		panic("pendingRunFromStart: event must be workflow.started")
	}
	var shape struct {
		WorkflowDef json.RawMessage   `json:"workflow_def"`
		WorkflowID  string            `json:"workflow_id"`
		Input       json.RawMessage   `json:"input"`
		Labels      map[string]string `json:"labels"`
	}
	if json.Unmarshal(evt.Payload, &shape) != nil {
		return dag.WorkflowRun{}, false
	}
	defJSON := shape.WorkflowDef
	if defJSON == nil && shape.WorkflowID == "" {
		defJSON = evt.Payload
	}
	var def dag.WorkflowDef
	if defJSON != nil && json.Unmarshal(defJSON, &def) != nil {
		return dag.WorkflowRun{}, false
	}
	workflowID := shape.WorkflowID
	if workflowID == "" {
		workflowID = def.Name
	}
	run := dag.WorkflowRun{RunID: evt.RunID, WorkflowID: workflowID,
		Status: dag.RunStatusPending, Steps: map[string]dag.StepState{}}
	if len(def.Steps) > 0 {
		run = dag.NewWorkflowRun(def, evt.RunID)
	}
	if shape.WorkflowDef != nil {
		run.Input, run.Labels = shape.Input, shape.Labels
	}
	if !evt.Timestamp.IsZero() {
		run.CreatedAt = evt.Timestamp
	}
	run.TraceParent, run.RootRunID = evt.TraceParent, evt.RunID
	return run, run.WorkflowID != ""
}
