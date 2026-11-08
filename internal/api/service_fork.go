// internal/api/service_fork.go
// Forks of agent runs (#810). POST /runs/{run}/fork and api.runs.fork
// start a new run of the parent's workflow, at the def version the
// parent is pinned to and with the parent's input and labels. The
// forked step's journal starts with a fork record pointing at the
// parent's journal and sequence (journal.WriteFork), so it replays the
// parent up to there and runs on; the parent steps it does not depend
// on and has finished are reused, not executed (dag.ForkReuse).
//
// Admin-only: the endpoints sit on the global control plane, which a
// program's api.p.<name>.* permissions do not reach.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// ForkRequest names the parent step and the journal sequence of the
// last parent record the fork replays.
type ForkRequest struct {
	Step string `json:"step"`
	Seq  uint64 `json:"seq"`
}

// ForkResponse is the new run and where it was forked from.
type ForkResponse struct {
	RunID       string `json:"run_id"`
	ParentRunID string `json:"parent_run_id"`
	Step        string `json:"step"`
	Seq         uint64 `json:"seq"`
}

// ForkError is a fork the request itself got wrong: NotFound for an
// unknown step, otherwise an invalid request. Nothing was written.
type ForkError struct {
	NotFound bool
	Msg      string
}

func (e *ForkError) Error() string { return "fork: " + e.Msg }

// ForkRun forks parentRunID at req. An unknown parent run is
// engine.ErrRunNotFound; a refused request is a *ForkError.
func (s *Service) ForkRun(
	ctx context.Context, parentRunID string, req ForkRequest,
) (ForkResponse, error) {
	if ctx == nil {
		panic("ForkRun: ctx must not be nil")
	}
	if s.js == nil || s.tp == nil {
		panic("ForkRun: js and tp must be set")
	}
	if parentRunID == "" || req.Step == "" || req.Seq == 0 {
		return ForkResponse{}, &ForkError{Msg: "run, step and seq are required"}
	}
	parent, err := s.GetRun(ctx, parentRunID)
	if err != nil {
		return ForkResponse{}, err
	}
	defJSON, def, err := s.forkDef(ctx, parent, req.Step)
	if err != nil {
		return ForkResponse{}, err
	}
	runID := runid.New()
	_, err = journal.WriteFork(ctx, s.js, s.tp, runID, journal.Fork{
		ParentRunID: parentRunID, ParentStepID: req.Step, ParentSeq: req.Seq,
	})
	if errors.Is(err, journal.ErrForkNoJournal) || errors.Is(err, journal.ErrForkPoint) {
		return ForkResponse{}, &ForkError{Msg: err.Error()}
	}
	if err != nil {
		return ForkResponse{}, err
	}
	start := dag.ForkStart{
		Origin: dag.ForkOrigin{RunID: parentRunID, StepID: req.Step, JournalSeq: req.Seq},
		Reused: dag.ForkReuse(def, parent, req.Step),
	}
	if err := s.publishForkStarted(ctx, runID, defJSON, parent, start); err != nil {
		return ForkResponse{}, err
	}
	slog.InfoContext(ctx, "forked run", "run_id", runID, "parent_run_id", parentRunID,
		"step", req.Step, "seq", req.Seq)
	return ForkResponse{RunID: runID, ParentRunID: parentRunID, Step: req.Step, Seq: req.Seq}, nil
}

// forkDef loads the def the parent run is pinned to and checks that
// step is one of its declared steps.
func (s *Service) forkDef(
	ctx context.Context, parent dag.WorkflowRun, step string,
) ([]byte, dag.WorkflowDef, error) {
	if s.defKV == nil {
		panic("forkDef: defKV must not be nil")
	}
	if parent.WorkflowID == "" {
		panic("forkDef: parent must name its workflow")
	}
	if _, ok := parent.Steps[step]; !ok {
		return nil, dag.WorkflowDef{}, &ForkError{NotFound: true, Msg: "step " + step + " not found"}
	}
	key := parent.WorkflowID
	if parent.DefHash != "" {
		key = dag.DefVersionKey(parent.WorkflowID, parent.DefHash)
	}
	entry, err := s.defKV.Get(ctx, key)
	if err != nil {
		return nil, dag.WorkflowDef{}, fmt.Errorf("load the parent's workflow def %q: %w", key, err)
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return nil, dag.WorkflowDef{}, fmt.Errorf("decode workflow def %q: %w", key, err)
	}
	for _, declared := range def.Steps {
		if declared.ID == step {
			return entry.Value(), def, nil
		}
	}
	return nil, dag.WorkflowDef{}, &ForkError{Msg: "step " + step + " is not declared by the workflow"}
}

// publishForkStarted publishes the forked run's workflow.started: the
// parent's def, input and labels plus the fork section.
func (s *Service) publishForkStarted(
	ctx context.Context, runID string, defJSON []byte, parent dag.WorkflowRun,
	start dag.ForkStart,
) error {
	if runID == "" || len(defJSON) == 0 {
		panic("publishForkStarted: run and def must be set")
	}
	if start.Origin.RunID != parent.RunID {
		panic("publishForkStarted: origin must be the parent")
	}
	payload, err := json.Marshal(struct {
		WorkflowDef json.RawMessage   `json:"workflow_def"`
		Input       json.RawMessage   `json:"input,omitempty"`
		Labels      map[string]string `json:"labels,omitempty"`
		Fork        dag.ForkStart     `json:"fork"`
	}{defJSON, parent.Input, parent.Labels, start})
	if err != nil {
		return fmt.Errorf("marshal fork start: %w", err)
	}
	evt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID, payload)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	_, err = s.tp.JSPublishMsgEvent(ctx, msg, &evt)
	return err
}
