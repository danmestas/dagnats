// engine/respond_step.go
//
// Executor for dag.StepTypeRespond steps per ADR-013. The respond
// step is engine-resolved (no worker invocation): it builds the HTTP
// response envelope, publishes it on the engine-private response
// subject, marks the step Completed in the run snapshot, and emits
// step.completed so DAG advance keeps working for any downstream
// steps.
//
// Why engine-resolved: a respond step is a pure side effect of run
// state — body comes from upstream output or a BodyFrom dotpath into
// existing data. Routing it through a worker would buy nothing and
// add a round-trip plus an extra failure mode.
//
// Why publish step.completed: subsequent DAG steps may legitimately
// run after respond (cleanup, audit logging — ADR-013's "respond is
// a side effect, not a return"). The engine treats respond like any
// other step terminating; the response publish is a side effect on
// top of the normal completion flow.
package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/httpenvelope"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// respondWirePayload is the on-the-wire shape published to
// httpenvelope.ResponseSubject(runID). The API handler unmarshals
// the same struct shape; renaming a field here is a coordinated
// change across packages.
type respondWirePayload struct {
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers,omitempty"`
	ContentType string            `json:"content_type"`
	Body        []byte            `json:"body,omitempty"`
}

// enqueueRespondStep executes a respond step end-to-end: resolves
// the body, publishes the response envelope on the per-run subject,
// marks the step Completed, persists the snapshot, and emits a
// step.completed event so the normal DAG advance pipeline runs.
//
// Errors at any I/O stage propagate up; the orchestrator NAKs the
// triggering event so a transient NATS hiccup is retried.
func (o *Orchestrator) enqueueRespondStep(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeRespond {
		panic("enqueueRespondStep: step is not a Respond step")
	}
	if run.RunID == "" {
		panic("enqueueRespondStep: RunID must not be empty")
	}

	cfg, err := dag.ParseRespondConfig(step)
	if err != nil {
		return fmt.Errorf("enqueueRespondStep: %w", err)
	}
	cfg = cfg.Defaulted()

	body, err := resolveRespondBody(cfg, step, run)
	if err != nil {
		return fmt.Errorf("resolveRespondBody: %w", err)
	}

	if err := publishRespondPayload(
		ctx, o.nc, run.RunID, cfg, body,
	); err != nil {
		return fmt.Errorf("publishRespondPayload: %w", err)
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusCompleted
	state.Output = body
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run); err != nil {
		return err
	}

	return publishRespondStepCompleted(ctx, o.js, run.RunID, step.ID, body)
}

// resolveRespondBody picks the body bytes per RespondConfig.BodyFrom:
//   - empty       — upstream step's output if any, else run.Input.
//   - "data.X.Y"  — dotpath into the run's resolved input (the
//     trigger envelope's Data for HTTP-trigger runs).
//   - "X.Y"       — dotpath into the upstream's output.
//
// The simple split (data.* vs other) avoids a config knob for
// "which source"; the dotpath prefix is the source signal. Authors
// who want the immediate upstream output verbatim leave BodyFrom
// empty.
func resolveRespondBody(
	cfg dag.RespondConfig, step dag.StepDef, run *dag.WorkflowRun,
) ([]byte, error) {
	if run == nil {
		panic("resolveRespondBody: run must not be nil")
	}
	if run.Steps == nil {
		panic("resolveRespondBody: run.Steps must not be nil")
	}

	upstream := pickUpstreamOutput(step, run)

	if cfg.BodyFrom == "" {
		if len(upstream) > 0 {
			return upstream, nil
		}
		return run.Input, nil
	}

	source := upstream
	if len(source) == 0 {
		source = run.Input
	}
	if len(source) == 0 {
		return nil, fmt.Errorf(
			"BodyFrom %q requires source data (upstream output or run input)",
			cfg.BodyFrom,
		)
	}

	value, err := dag.ExtractDotPath(cfg.BodyFrom, source)
	if err != nil {
		return nil, fmt.Errorf("dotpath %q: %w", cfg.BodyFrom, err)
	}
	return json.Marshal(value)
}

// pickUpstreamOutput returns the first upstream step's output bytes
// when a respond step has exactly one dependency, the conventional
// shape. Fan-in into a respond step is rare and out of scope for v1
// — authors who need it can put an aggregating normal step before
// respond.
func pickUpstreamOutput(
	step dag.StepDef, run *dag.WorkflowRun,
) []byte {
	if len(step.DependsOn) == 0 {
		return nil
	}
	if len(step.DependsOn) != 1 {
		return nil
	}
	return run.Steps[step.DependsOn[0]].Output
}

// publishRespondPayload emits the response envelope to the per-run
// response subject via plain NATS (not JetStream — the response is
// ephemeral; the originating API handler is the only subscriber).
func publishRespondPayload(
	ctx context.Context, nc *nats.Conn,
	runID string, cfg dag.RespondConfig, body []byte,
) error {
	if nc == nil {
		panic("publishRespondPayload: nc must not be nil")
	}
	if runID == "" {
		panic("publishRespondPayload: runID must not be empty")
	}
	if cfg.Status == 0 {
		panic("publishRespondPayload: Status must be defaulted before publish")
	}

	payload := respondWirePayload{
		Status:      cfg.Status,
		Headers:     cfg.Headers,
		ContentType: cfg.ContentType,
		Body:        body,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal respond payload: %w", err)
	}
	_ = ctx
	return nc.Publish(httpenvelope.ResponseSubject(runID), data)
}

// publishRespondStepCompleted emits a step.completed event on the
// run's history subject so the orchestrator's normal advance path
// picks it up. The respond step is "completed" the instant the
// response goes on the wire — any subsequent steps in the DAG run
// afterward; the HTTP client has already received its reply.
func publishRespondStepCompleted(
	ctx context.Context, js jetstream.JetStream,
	runID string, stepID string, output []byte,
) error {
	if js == nil {
		panic("publishRespondStepCompleted: js must not be nil")
	}
	if runID == "" {
		panic("publishRespondStepCompleted: runID must not be empty")
	}

	evt := protocol.NewStepEvent(
		protocol.EventStepCompleted, runID, stepID, output,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal step.completed: %w", err)
	}
	_, err = js.Publish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	if err != nil {
		return fmt.Errorf("publish step.completed: %w", err)
	}
	return nil
}
