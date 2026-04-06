// api/bulk_cancel.go
// Bulk cancellation of workflow runs filtered by workflow ID, status,
// and time range. Cancels sequentially to avoid thundering herd.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/danmestas/dagnats/dag"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const maxBulkCancelLimit = 1000

// BulkCancelRequest specifies which runs to cancel.
type BulkCancelRequest struct {
	WorkflowID string    `json:"workflow_id"`
	Status     string    `json:"status,omitempty"`
	After      time.Time `json:"after,omitempty"`
	Before     time.Time `json:"before,omitempty"`
	DryRun     bool      `json:"dry_run,omitempty"`
}

// BulkCancelResponse reports the outcome.
type BulkCancelResponse struct {
	Cancelled []string `json:"cancelled"`
	Skipped   []string `json:"skipped,omitempty"`
	Total     int      `json:"total"`
	DryRun    bool     `json:"dry_run"`
}

// BulkCancelRuns cancels runs matching the filter criteria.
func (s *Service) BulkCancelRuns(
	ctx context.Context, req BulkCancelRequest,
) (BulkCancelResponse, error) {
	if ctx == nil {
		panic("BulkCancelRuns: ctx must not be nil")
	}
	if req.WorkflowID == "" {
		return BulkCancelResponse{},
			fmt.Errorf("workflow_id is required")
	}
	_, span := s.tracer.Start(ctx,
		"dagnats.api bulkCancelRuns",
		trace.WithAttributes(
			attribute.String("workflow_id", req.WorkflowID),
			attribute.String("status_filter", req.Status),
			attribute.Bool("dry_run", req.DryRun),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Add(ctx, 1)

	resp, err := s.bulkCancelInner(ctx, req)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Record(ctx, elapsed)
	if err != nil {
		s.errorCount.Add(ctx, 1)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		slog.InfoContext(ctx, "bulk cancel completed",
			"workflow_id", req.WorkflowID,
			"cancelled", len(resp.Cancelled),
			"skipped", len(resp.Skipped),
		)
	}
	return resp, err
}

// bulkCancelInner lists, filters, and cancels matching runs.
func (s *Service) bulkCancelInner(
	ctx context.Context, req BulkCancelRequest,
) (BulkCancelResponse, error) {
	if req.WorkflowID == "" {
		panic("bulkCancelInner: WorkflowID must not be empty")
	}
	if s.store == nil {
		panic("bulkCancelInner: store must not be nil")
	}

	status, err := validateBulkCancelRequest(req)
	if err != nil {
		return BulkCancelResponse{}, err
	}

	runs, err := s.store.ListAll(ctx, maxBulkCancelLimit+1)
	if err != nil {
		return BulkCancelResponse{},
			fmt.Errorf("list runs: %w", err)
	}
	matched := filterRuns(
		runs, req.WorkflowID, status,
		req.After, req.Before,
	)

	if len(matched) > maxBulkCancelLimit {
		return BulkCancelResponse{}, fmt.Errorf(
			"too many matching runs (%d > %d);"+
				" narrow with after/before or status",
			len(matched), maxBulkCancelLimit,
		)
	}

	if req.DryRun {
		ids := make([]string, len(matched))
		for i, r := range matched {
			ids[i] = r.RunID
		}
		return BulkCancelResponse{
			Cancelled: ids, Total: len(ids), DryRun: true,
		}, nil
	}

	return s.executeBulkCancel(ctx, matched), nil
}

// validateBulkCancelRequest checks request validity.
func validateBulkCancelRequest(
	req BulkCancelRequest,
) (string, error) {
	status := req.Status
	if status == "" {
		status = "all"
	}
	validStatuses := map[string]bool{
		"running": true, "pending": true, "all": true,
	}
	if !validStatuses[status] {
		return "", fmt.Errorf(
			"invalid status filter: %q"+
				" (must be running, pending, or all)",
			status,
		)
	}
	if !req.After.IsZero() && !req.Before.IsZero() &&
		!req.Before.After(req.After) {
		return "", fmt.Errorf("before must be after after")
	}
	return status, nil
}

// executeBulkCancel cancels matched runs sequentially.
func (s *Service) executeBulkCancel(
	ctx context.Context, matched []dag.WorkflowRun,
) BulkCancelResponse {
	if s == nil {
		panic("executeBulkCancel: service must not be nil")
	}
	if s.js == nil {
		panic("executeBulkCancel: js must not be nil")
	}
	var resp BulkCancelResponse
	for _, run := range matched {
		if run.Status.IsTerminal() {
			resp.Skipped = append(
				resp.Skipped, run.RunID,
			)
			continue
		}
		if err := s.cancelRunInner(ctx, run.RunID); err != nil {
			resp.Skipped = append(
				resp.Skipped, run.RunID,
			)
			continue
		}
		resp.Cancelled = append(
			resp.Cancelled, run.RunID,
		)
	}
	resp.Total = len(resp.Cancelled) + len(resp.Skipped)
	if resp.Cancelled == nil {
		resp.Cancelled = []string{}
	}
	return resp
}

// filterRuns selects runs matching workflow, status, and time range.
func filterRuns(
	runs []dag.WorkflowRun,
	workflowID, status string,
	after, before time.Time,
) []dag.WorkflowRun {
	if workflowID == "" {
		panic("filterRuns: workflowID must not be empty")
	}
	if status == "" {
		panic("filterRuns: status must not be empty")
	}
	var matched []dag.WorkflowRun
	for _, run := range runs {
		if run.WorkflowID != workflowID {
			continue
		}
		if !matchesStatusFilter(run.Status, status) {
			continue
		}
		if !after.IsZero() &&
			run.CreatedAt.Before(after) {
			continue
		}
		if !before.IsZero() &&
			run.CreatedAt.After(before) {
			continue
		}
		matched = append(matched, run)
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.Before(
			matched[j].CreatedAt,
		)
	})
	return matched
}

// matchesStatusFilter checks if a run status matches the filter.
func matchesStatusFilter(
	runStatus dag.RunStatus, filter string,
) bool {
	switch filter {
	case "all":
		return runStatus == dag.RunStatusRunning ||
			runStatus == dag.RunStatusPending
	case "running":
		return runStatus == dag.RunStatusRunning
	case "pending":
		return runStatus == dag.RunStatusPending
	}
	return false
}
