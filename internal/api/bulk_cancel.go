// api/bulk_cancel.go
// Bulk cancellation of workflow runs filtered by workflow ID, status,
// and time range. Cancels sequentially to avoid thundering herd.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"go.opentelemetry.io/otel/attribute"
)

const maxBulkCancelLimit = 1000

// BulkCancelRequest specifies which runs to cancel. Labels (#629), when
// set, requires every key/value pair to match with AND semantics,
// composing with WorkflowID/Status/After/Before rather than replacing
// them. Labels is itself bounded by dag.ValidateLabels — a filter with
// more than dag.LabelsCountMax entries is rejected the same way an
// invalid label on a started run would be.
type BulkCancelRequest struct {
	WorkflowID string            `json:"workflow_id"`
	Status     string            `json:"status,omitempty"`
	After      time.Time         `json:"after,omitempty"`
	Before     time.Time         `json:"before,omitempty"`
	DryRun     bool              `json:"dry_run,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
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
	var resp BulkCancelResponse
	err := s.observed(ctx, "bulkCancelRuns",
		[]attribute.KeyValue{
			attribute.String("workflow_id", req.WorkflowID),
			attribute.String("status_filter", req.Status),
			attribute.Bool("dry_run", req.DryRun),
		},
		func(ctx context.Context) error {
			var innerErr error
			resp, innerErr = s.bulkCancelInner(ctx, req)
			if innerErr == nil {
				slog.InfoContext(ctx,
					"bulk cancel completed",
					"workflow_id", req.WorkflowID,
					"cancelled", len(resp.Cancelled),
					"skipped", len(resp.Skipped),
				)
			}
			return innerErr
		},
	)
	return resp, err
}

// bulkCancelInner scans, filters, and cancels matching runs. Uses the
// creation-ordered newest-first scan (#659) instead of the old
// order-agnostic ListAll+filter sample, so a run labeled/matched after
// the population grew past maxBulkCancelLimit is still found instead
// of silently no-op'd.
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

	pred := func(run dag.WorkflowRun) bool {
		return runMatchesBulkCancel(
			run, req.WorkflowID, status,
			req.After, req.Before, req.Labels,
		)
	}
	matched, _, err := s.store.ScanNewestFirst(
		ctx, pred, maxBulkCancelLimit+1, engine.ScanFetchMax,
	)
	if err != nil {
		return BulkCancelResponse{},
			fmt.Errorf("list runs: %w", err)
	}

	if len(matched) > maxBulkCancelLimit {
		return BulkCancelResponse{}, fmt.Errorf(
			"too many matching runs (%d > %d);"+
				" narrow with after/before or status",
			len(matched), maxBulkCancelLimit,
		)
	}
	// ScanNewestFirst returns newest-first; cancel oldest-first
	// (matches the pre-#659 filterRuns ordering) to bias survivors
	// toward the runs a consumer most recently started.
	reverseRuns(matched)

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
	if err := dag.ValidateLabels(req.Labels); err != nil {
		return "", err
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

// runMatchesBulkCancel reports whether run matches workflow, status,
// time range, and labels (#629, AND semantics — every entry in labels
// must be present on the run with an equal value). This is the
// ScanNewestFirst predicate for bulkCancelInner.
func runMatchesBulkCancel(
	run dag.WorkflowRun,
	workflowID, status string,
	after, before time.Time,
	labels map[string]string,
) bool {
	if workflowID == "" {
		panic("runMatchesBulkCancel: workflowID must not be empty")
	}
	if status == "" {
		panic("runMatchesBulkCancel: status must not be empty")
	}
	if run.WorkflowID != workflowID {
		return false
	}
	if !matchesStatusFilter(run.Status, status) {
		return false
	}
	if !after.IsZero() && run.CreatedAt.Before(after) {
		return false
	}
	if !before.IsZero() && run.CreatedAt.After(before) {
		return false
	}
	return dag.LabelsMatch(labels, run.Labels)
}

// reverseRuns reverses runs in place.
func reverseRuns(runs []dag.WorkflowRun) {
	for i, j := 0, len(runs)-1; i < j; i, j = i+1, j-1 {
		runs[i], runs[j] = runs[j], runs[i]
	}
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
