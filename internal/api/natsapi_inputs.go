// internal/api/natsapi_inputs.go
// NATS micro transport of the input endpoints (ADR-027), the
// equivalents of the REST paths in rest_inputs.go:
//
//	api.inputs.submit   {run_id, step_id, request_id, mode, content}
//	api.inputs.get      {run_id, step_id, request_id, wait_ms?, after?}
//	api.inputs.withdraw {run_id, step_id, request_id}
//
// Replies are the submission JSON (submit adds "created"), the
// withdrawal {status: withdrawn}, or an {error, kind} envelope.
package api

import (
	"encoding/json"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// inputMicroRequest is the body of every api.inputs.* request.
type inputMicroRequest struct {
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id"`
	RequestID string          `json:"request_id"`
	Mode      inputs.Mode     `json:"mode,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	WaitMs    int64           `json:"wait_ms,omitempty"`
	After     inputs.Status   `json:"after,omitempty"`
}

// inputEndpoints lists the micro endpoints, served by every instance
// like the run endpoints.
func (n *NATSAPI) inputEndpoints() []endpointSpec {
	if n.svc == nil {
		panic("inputEndpoints: svc must not be nil")
	}
	specs := []endpointSpec{
		{"inputs-submit", "api.inputs.submit", n.handleInputSubmit, ""},
		{"inputs-get", "api.inputs.get", n.handleInputGet, ""},
		{"inputs-withdraw", "api.inputs.withdraw", n.handleInputWithdraw, ""},
	}
	if len(specs) != 3 {
		panic("inputEndpoints: three endpoints")
	}
	return specs
}

// decodeInputRequest unmarshals and checks the addressing fields.
func (n *NATSAPI) decodeInputRequest(req micro.Request) (inputMicroRequest, bool) {
	if req == nil {
		panic("decodeInputRequest: req must not be nil")
	}
	if n.svc == nil {
		panic("decodeInputRequest: svc must not be nil")
	}
	var r inputMicroRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		n.replyInputError(req, err.Error(), "transport")
		return r, false
	}
	if r.RunID == "" || r.StepID == "" {
		n.replyInputError(req, "run_id and step_id are required", "invalid")
		return r, false
	}
	return r, true
}

// handleInputSubmit creates the submission; "created" tells a repeat
// from a first submission.
func (n *NATSAPI) handleInputSubmit(req micro.Request) {
	r, ok := n.decodeInputRequest(req)
	if !ok {
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	sub, created, err := n.svc.SubmitInput(ctx, r.RunID, r.StepID, InputRequest{
		RequestID: r.RequestID, Mode: r.Mode, Content: r.Content,
	})
	if err != nil {
		n.replyInputError(req, err.Error(), inputErrorKind(err))
		return
	}
	n.reply(req, struct {
		inputs.Submission
		Created bool `json:"created"`
	}{sub, created})
}

// handleInputGet reads the status, long-polling for wait_ms.
func (n *NATSAPI) handleInputGet(req micro.Request) {
	r, ok := n.decodeInputRequest(req)
	if !ok {
		return
	}
	after := r.After
	if after == "" {
		after = inputs.StatusQueued
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	sub, err := n.svc.GetInput(ctx, r.RunID, r.StepID, r.RequestID, after,
		time.Duration(max(r.WaitMs, 0))*time.Millisecond)
	if err != nil {
		n.replyInputError(req, err.Error(), inputErrorKind(err))
		return
	}
	n.reply(req, sub)
}

// handleInputWithdraw deletes a queued submission.
func (n *NATSAPI) handleInputWithdraw(req micro.Request) {
	r, ok := n.decodeInputRequest(req)
	if !ok {
		return
	}
	ctx := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
	if err := n.svc.WithdrawInput(ctx, r.RunID, r.StepID, r.RequestID); err != nil {
		n.replyInputError(req, err.Error(), inputErrorKind(err))
		return
	}
	n.reply(req, map[string]string{
		"status": "withdrawn", "run_id": r.RunID, "step_id": r.StepID, "request_id": r.RequestID,
	})
}

// replyInputError sends the {error, kind} envelope.
func (n *NATSAPI) replyInputError(req micro.Request, msg, kind string) {
	if kind == "" {
		panic("replyInputError: kind must not be empty")
	}
	if req == nil {
		panic("replyInputError: req must not be nil")
	}
	n.reply(req, map[string]string{"error": msg, "kind": kind})
}
