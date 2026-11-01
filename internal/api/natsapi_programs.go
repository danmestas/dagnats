package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// programQueueGroup makes one instance answer each scoped request, so a
// cluster starts a program's run once, not once per node.
const programQueueGroup = "dagnats-program-api"

// programEndpoints is the program-scoped control plane (#885). A
// program's NATS permissions let it publish only under
// api.p.<its name>.>, so the name in the subject is the caller's
// identity: every handler confines the request to workflows named
// "<name>.*".
func (n *NATSAPI) programEndpoints() []endpointSpec {
	return []endpointSpec{
		{"program-workflows-register", "api.p.*.workflows.register",
			n.handleProgramRegister, programQueueGroup},
		{"program-runs-start", "api.p.*.runs.start", n.handleProgramStart, programQueueGroup},
		{"program-runs-get", "api.p.*.runs.get", n.handleProgramGet, programQueueGroup},
		{"program-runs-cancel", "api.p.*.runs.cancel", n.handleProgramCancel, programQueueGroup},
	}
}

// programPrefix returns "<name>." for a request on api.p.<name>.*, or an
// error when the name is not a valid program name.
func programPrefix(subject string) (string, error) {
	tokens := strings.Split(subject, ".")
	if len(tokens) < 4 || tokens[0] != "api" || tokens[1] != "p" {
		panic("programPrefix: endpoint subject is api.p.<name>.<op>: " + subject)
	}
	if err := program.ValidateName(tokens[2]); err != nil {
		return "", err
	}
	return tokens[2] + ".", nil
}

func (n *NATSAPI) replyErr(req micro.Request, err error) {
	if err == nil {
		panic("replyErr: err must not be nil")
	}
	n.reply(req, map[string]string{"error": err.Error()})
}

func (n *NATSAPI) handleProgramRegister(req micro.Request) {
	if req == nil || n.svc == nil {
		panic("handleProgramRegister: req and svc must not be nil")
	}
	prefix, err := programPrefix(req.Subject())
	if err != nil {
		n.replyErr(req, err)
		return
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(req.Data(), &def); err != nil {
		n.replyErr(req, err)
		return
	}
	if err := workflowUnderPrefix(def, prefix); err != nil {
		n.replyErr(req, err)
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	if err := n.svc.RegisterWorkflow(ctx, def); err != nil {
		n.replyErr(req, err)
		return
	}
	n.reply(req, map[string]string{"status": "registered", "name": def.Name})
}

// workflowUnderPrefix refuses a def named outside prefix, or with a
// sub-workflow step that would start a workflow outside it: that would
// run another program's workflow on this program's behalf.
func workflowUnderPrefix(def dag.WorkflowDef, prefix string) error {
	if prefix == "" {
		panic("workflowUnderPrefix: prefix must not be empty")
	}
	if !strings.HasPrefix(def.Name, prefix) || len(def.Name) == len(prefix) {
		return fmt.Errorf("workflow %q is not under %q", def.Name, prefix)
	}
	for _, step := range def.Steps {
		if step.Type != dag.StepTypeSubWorkflow {
			continue
		}
		cfg, err := dag.ParseSubWorkflowConfig(step)
		if err != nil {
			return fmt.Errorf("step %q: %w", step.ID, err)
		}
		if !strings.HasPrefix(cfg.Workflow, prefix) {
			return fmt.Errorf("step %q starts workflow %q, which is not under %q",
				step.ID, cfg.Workflow, prefix)
		}
	}
	return nil
}

func (n *NATSAPI) handleProgramStart(req micro.Request) {
	if req == nil || n.svc == nil {
		panic("handleProgramStart: req and svc must not be nil")
	}
	prefix, err := programPrefix(req.Subject())
	if err != nil {
		n.replyErr(req, err)
		return
	}
	var r startRunRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		n.replyErr(req, err)
		return
	}
	if !strings.HasPrefix(r.Workflow, prefix) {
		n.replyErr(req, fmt.Errorf("workflow %q is not under %q", r.Workflow, prefix))
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	runID, err := n.svc.StartRun(ctx, r.Workflow, r.Input)
	if err != nil {
		n.replyErr(req, err)
		return
	}
	n.reply(req, map[string]string{"run_id": runID})
}

// ownedRun loads the run a scoped request names. A run of another
// program's workflow is reported as engine.ErrRunNotFound, the same as
// a run that does not exist, so one program cannot probe another's run
// IDs.
func (n *NATSAPI) ownedRun(req micro.Request) (context.Context, string, error) {
	if req == nil || n.svc == nil {
		panic("ownedRun: req and svc must not be nil")
	}
	prefix, err := programPrefix(req.Subject())
	if err != nil {
		return nil, "", err
	}
	runID := string(req.Data())
	if runID == "" {
		return nil, "", errors.New("run id is required")
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	run, err := n.svc.GetRun(ctx, runID)
	foreign := err == nil && !strings.HasPrefix(run.WorkflowID, prefix)
	if foreign || errors.Is(err, engine.ErrRunNotFound) {
		return nil, "", engine.ErrRunNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return ctx, runID, nil
}

func (n *NATSAPI) handleProgramGet(req micro.Request) {
	ctx, runID, err := n.ownedRun(req)
	if err != nil {
		n.replyErr(req, err)
		return
	}
	resp, err := n.svc.GetRunResponse(ctx, runID)
	if err != nil {
		n.replyErr(req, err)
		return
	}
	n.reply(req, resp)
}

func (n *NATSAPI) handleProgramCancel(req micro.Request) {
	ctx, runID, err := n.ownedRun(req)
	if err != nil {
		n.replyErr(req, err)
		return
	}
	if err := n.svc.CancelRun(ctx, runID); err != nil {
		n.replyErr(req, err)
		return
	}
	n.reply(req, map[string]string{"status": "cancelled", "run_id": runID})
}
