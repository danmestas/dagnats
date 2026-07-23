// api/service_approval.go
// Split out of service.go (#566): approval handling domain of the control
// plane Service. Shares the private Service NATS/KV bundle; no new
// connection layer. Behavior identical to the pre-split file.
package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// HandleApproval validates a token and publishes an approval
// granted or rejected event. Uses atomic CAS delete on the KV
// entry to guarantee exactly-once consumption.
func (s *Service) HandleApproval(
	ctx context.Context,
	runID, stepID, token, action string,
	body json.RawMessage,
) error {
	if ctx == nil {
		panic("HandleApproval: ctx must not be nil")
	}
	if runID == "" {
		panic("HandleApproval: runID must not be empty")
	}
	return s.observed(ctx, "handleApproval",
		[]attribute.KeyValue{
			attribute.String("run_id", runID),
			attribute.String("step_id", stepID),
		},
		func(ctx context.Context) error {
			return s.handleApprovalInner(
				ctx, runID, stepID, token, action, body,
			)
		},
	)
}

// handleApprovalInner loads the token, verifies it, atomically
// deletes it, and publishes the corresponding event.
func (s *Service) handleApprovalInner(
	ctx context.Context,
	runID, stepID, token, action string,
	body json.RawMessage,
) error {
	if runID == "" {
		panic(
			"handleApprovalInner: runID must not be empty",
		)
	}
	if stepID == "" {
		panic(
			"handleApprovalInner: stepID must not be empty",
		)
	}
	return s.consumeTokenAndPublish(
		ctx, runID, stepID, token, action, body,
	)
}

// consumeTokenAndPublish performs atomic token verification and
// event publishing. Separated to keep functions under 70 lines.
func (s *Service) consumeTokenAndPublish(
	ctx context.Context,
	runID, stepID, token, action string,
	body json.RawMessage,
) error {
	if token == "" {
		return fmt.Errorf("token is required")
	}
	if action != "approve" && action != "reject" {
		return fmt.Errorf(
			"action must be 'approve' or 'reject', got %q",
			action,
		)
	}
	kv, err := s.js.KeyValue(ctx, "approval_tokens")
	if err != nil {
		return fmt.Errorf(
			"approval_tokens bucket not available: %w", err,
		)
	}
	key := runID + "." + stepID
	entry, err := kv.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("token not found or expired")
	}

	return s.verifyAndPublish(
		ctx, kv, entry, key, token, action, runID, stepID, body,
	)
}

// verifyAndPublish checks the token matches, atomically deletes
// it, and publishes the approval event.
func (s *Service) verifyAndPublish(
	ctx context.Context,
	kv jetstream.KeyValue,
	entry jetstream.KeyValueEntry,
	key, token, action, runID, stepID string,
	body json.RawMessage,
) error {
	if kv == nil {
		panic("verifyAndPublish: kv must not be nil")
	}
	if entry == nil {
		panic("verifyAndPublish: entry must not be nil")
	}
	var record struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(
		entry.Value(), &record,
	); err != nil {
		return fmt.Errorf("corrupt token record: %w", err)
	}
	if record.Token != token {
		return fmt.Errorf("invalid token")
	}

	// Atomic CAS delete -- if revision changed, token was
	// already consumed by a concurrent request.
	if err := kv.Delete(
		ctx, key,
		jetstream.LastRevision(entry.Revision()),
	); err != nil {
		return fmt.Errorf("token already consumed")
	}

	evtType := protocol.EventApprovalGranted
	if action == "reject" {
		evtType = protocol.EventApprovalRejected
	}
	evt := protocol.NewStepEvent(
		evtType, runID, stepID, body,
	)
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
	_, err = s.tp.JSPublishMsg(ctx, msg)
	return err
}
