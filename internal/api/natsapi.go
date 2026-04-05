// api/natsapi.go
// NATS request/reply transport for the DagNats control plane.
// Subscribes to well-known subjects and delegates to Service -- all transport
// concerns (subject routing, JSON framing) are isolated here so Service
// remains transport-agnostic.
package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// NATSAPI wires Service methods to NATS request/reply subjects. It
// owns no business logic -- it only translates between wire bytes and
// typed calls.
type NATSAPI struct {
	svc    *Service
	nc     *nats.Conn
	logger observe.Logger
	subs   []*nats.Subscription
}

// NewNATSAPI constructs a NATSAPI bound to svc, nc, and logger.
// Panics if any argument is nil.
func NewNATSAPI(
	svc *Service, nc *nats.Conn, logger observe.Logger,
) *NATSAPI {
	if svc == nil {
		panic("NewNATSAPI: svc must not be nil")
	}
	if nc == nil {
		panic("NewNATSAPI: nc must not be nil")
	}
	if logger == nil {
		panic("NewNATSAPI: logger must not be nil")
	}
	return &NATSAPI{svc: svc, nc: nc, logger: logger}
}

// Start registers subscriptions for all control-plane subjects.
// Panics on Subscribe failure -- unrecoverable programmer error.
func (n *NATSAPI) Start() {
	if n.nc == nil {
		panic("NATSAPI.Start: nc must not be nil")
	}
	if n.svc == nil {
		panic("NATSAPI.Start: svc must not be nil")
	}
	handlers := map[string]nats.MsgHandler{
		"api.workflows.register": n.handleRegister,
		"api.runs.start":         n.handleStartRun,
		"api.runs.get":           n.handleGetRun,
	}
	for subject, handler := range handlers {
		sub, err := n.nc.Subscribe(subject, handler)
		if err != nil {
			panic(
				"NATSAPI.Start: Subscribe failed for " +
					subject + ": " + err.Error(),
			)
		}
		n.subs = append(n.subs, sub)
	}
}

// Stop drains all active subscriptions. Errors are intentionally
// ignored -- the connection is typically being torn down.
func (n *NATSAPI) Stop() {
	if n.nc == nil {
		panic("NATSAPI.Stop: nc must not be nil")
	}
	if n.subs == nil {
		panic("NATSAPI.Stop: subs must not be nil")
	}
	for _, sub := range n.subs {
		sub.Unsubscribe() //nolint:errcheck -- best-effort teardown
	}
}

// handleRegister unmarshals a WorkflowDef and calls RegisterWorkflow.
func (n *NATSAPI) handleRegister(msg *nats.Msg) {
	if msg == nil {
		panic("handleRegister: msg must not be nil")
	}
	if n.svc == nil {
		panic("handleRegister: svc must not be nil")
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(msg.Data, &def); err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	if err := n.svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	n.reply(msg, map[string]string{
		"status": "registered", "name": def.Name,
	})
}

// handleStartRun unmarshals a startRunRequest and calls StartRun.
func (n *NATSAPI) handleStartRun(msg *nats.Msg) {
	if msg == nil {
		panic("handleStartRun: msg must not be nil")
	}
	if n.svc == nil {
		panic("handleStartRun: svc must not be nil")
	}
	var req startRunRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	runID, err := n.svc.StartRun(
		context.Background(), req.Workflow, req.Input,
	)
	if err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	n.reply(msg, map[string]string{"run_id": runID})
}

// handleGetRun reads the run ID from the raw message body and returns
// the current snapshot. The body is plain text (not JSON).
func (n *NATSAPI) handleGetRun(msg *nats.Msg) {
	if msg == nil {
		panic("handleGetRun: msg must not be nil")
	}
	if n.svc == nil {
		panic("handleGetRun: svc must not be nil")
	}
	runID := string(msg.Data)
	run, err := n.svc.GetRun(context.Background(), runID)
	if err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	data, err := json.Marshal(run)
	if err != nil {
		n.reply(msg, map[string]string{"error": err.Error()})
		return
	}
	msg.Respond(data) //nolint:errcheck -- reply failure is non-fatal
}

// reply marshals payload to JSON and sends it as a reply. A marshal
// error is logged -- panicking would kill the subscription goroutine.
func (n *NATSAPI) reply(msg *nats.Msg, payload any) {
	if msg == nil {
		panic("reply: msg must not be nil")
	}
	if n.logger == nil {
		panic("reply: logger must not be nil")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		n.logger.Error("reply: marshal failed",
			fmt.Errorf("marshal reply payload: %w", err))
		return
	}
	msg.Respond(data) //nolint:errcheck -- best-effort reply
}
