// api/bulk_run.go
// Bulk run starts multiple workflow runs in a single API call.
// Workflow def loaded once, inputs validated atomically.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const maxBulkRunLimit = 1000

type BulkRunRequest struct {
	WorkflowID string            `json:"workflow_id"`
	Inputs     []json.RawMessage `json:"inputs"`
}

type BulkRunResponse struct {
	RunIDs []string `json:"run_ids"`
	Total  int      `json:"total"`
}

func (s *Service) BulkStartRuns(
	ctx context.Context, req BulkRunRequest,
) (BulkRunResponse, error) {
	if ctx == nil {
		panic("BulkStartRuns: ctx must not be nil")
	}
	if req.WorkflowID == "" {
		return BulkRunResponse{},
			fmt.Errorf("workflow_id is required")
	}
	_, span := s.tracer.Start(ctx,
		"dagnats.api bulkStartRuns",
		trace.WithAttributes(
			attribute.String("workflow_id", req.WorkflowID),
			attribute.Int64("count", int64(len(req.Inputs))),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Add(ctx, 1)
	resp, err := s.bulkRunInner(ctx, span, req)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Record(ctx, elapsed)
	if err != nil {
		s.errorCount.Add(ctx, 1)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		slog.InfoContext(ctx, "bulk run completed",
			"workflow_id", req.WorkflowID,
			"started", resp.Total,
		)
	}
	return resp, err
}

func (s *Service) bulkRunInner(
	ctx context.Context, span trace.Span,
	req BulkRunRequest,
) (BulkRunResponse, error) {
	if req.WorkflowID == "" {
		panic("bulkRunInner: WorkflowID must not be empty")
	}
	if s.defKV == nil {
		panic("bulkRunInner: defKV must not be nil")
	}
	if err := validateBulkRunRequest(req); err != nil {
		return BulkRunResponse{}, err
	}
	entry, err := s.defKV.Get(
		ctx, req.WorkflowID,
	)
	if err != nil {
		return BulkRunResponse{}, fmt.Errorf(
			"workflow %q not found: %w",
			req.WorkflowID, err,
		)
	}
	defBytes := entry.Value()
	var schema json.RawMessage
	var def dag.WorkflowDef
	if err := json.Unmarshal(defBytes, &def); err == nil {
		schema = def.InputSchema
	}
	if schema != nil {
		for i, input := range req.Inputs {
			if err := dag.ValidateSchema(
				schema, input,
			); err != nil {
				return BulkRunResponse{},
					fmt.Errorf("input[%d]: %w", i, err)
			}
		}
	}
	return s.publishBulkRuns(
		ctx, span, req.WorkflowID, defBytes, req.Inputs,
	)
}

func (s *Service) publishBulkRuns(
	ctx context.Context,
	span trace.Span,
	workflowID string,
	defBytes []byte,
	inputs []json.RawMessage,
) (BulkRunResponse, error) {
	if workflowID == "" {
		panic("publishBulkRuns: workflowID must not be empty")
	}
	if defBytes == nil {
		panic("publishBulkRuns: defBytes must not be nil")
	}
	runIDs := make([]string, 0, len(inputs))
	for _, input := range inputs {
		runID := generateRunID()
		payload, err := buildStartPayload(defBytes, input)
		if err != nil {
			return BulkRunResponse{
				RunIDs: runIDs, Total: len(runIDs),
			}, err
		}
		evt := protocol.NewWorkflowEvent(
			protocol.EventWorkflowStarted, runID, payload,
		)
		msg := &nats.Msg{
			Subject: evt.NATSSubject(),
			Header: nats.Header{
				"Nats-Msg-Id": {evt.NATSMsgID()},
			},
		}
		observe.InjectTraceContext(ctx, msg, &evt)
		data, err := evt.Marshal()
		if err != nil {
			return BulkRunResponse{
				RunIDs: runIDs, Total: len(runIDs),
			}, err
		}
		msg.Data = data
		if _, err := s.js.PublishMsg(
			ctx, msg,
		); err != nil {
			return BulkRunResponse{
				RunIDs: runIDs, Total: len(runIDs),
			}, err
		}
		runIDs = append(runIDs, runID)
	}
	return BulkRunResponse{
		RunIDs: runIDs, Total: len(runIDs),
	}, nil
}

func validateBulkRunRequest(req BulkRunRequest) error {
	if len(req.Inputs) == 0 {
		return fmt.Errorf("inputs must not be empty")
	}
	if len(req.Inputs) > maxBulkRunLimit {
		return fmt.Errorf(
			"too many inputs (%d > %d)",
			len(req.Inputs), maxBulkRunLimit,
		)
	}
	return nil
}
