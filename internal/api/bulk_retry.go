// internal/api/bulk_retry.go
// Bulk retry of failed workflow runs. Supports two modes:
// rerun (fresh start with original input) and replay
// (re-publish DLQ task messages to resume at failed step).
package api

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"go.opentelemetry.io/otel/attribute"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const maxBulkRetryLimit = 1000

// BulkRetryRequest specifies which failed runs to retry.
type BulkRetryRequest struct {
	WorkflowID string    `json:"workflow_id"`
	Mode       string    `json:"mode"`
	After      time.Time `json:"after,omitempty"`
	Before     time.Time `json:"before,omitempty"`
	DryRun     bool      `json:"dry_run,omitempty"`
}

// BulkRetryResponse reports the outcome.
// BulkRetryResponse reports the outcome. Truncated (#659 review round
// 2) is true when the underlying newest-first scan hit its fetch cap
// before it could prove there were no more matches beyond the scanned
// window -- a caller must not read Retried/Skipped as "definitely
// everything that matches" when Truncated is set, even if the result
// happens to be empty.
type BulkRetryResponse struct {
	Retried   []BulkRetryItem `json:"retried"`
	Skipped   []string        `json:"skipped,omitempty"`
	Total     int             `json:"total"`
	DryRun    bool            `json:"dry_run"`
	Truncated bool            `json:"truncated,omitempty"`
}

// BulkRetryItem links an original run to its retry outcome.
type BulkRetryItem struct {
	OriginalRunID string `json:"original_run_id"`
	NewRunID      string `json:"new_run_id,omitempty"`
}

// BulkRetryRuns retries failed runs matching the filter.
func (s *Service) BulkRetryRuns(
	ctx context.Context, req BulkRetryRequest,
) (BulkRetryResponse, error) {
	if ctx == nil {
		panic("BulkRetryRuns: ctx must not be nil")
	}
	if req.WorkflowID == "" {
		return BulkRetryResponse{},
			fmt.Errorf("workflow_id is required")
	}
	var resp BulkRetryResponse
	err := s.observed(ctx, "bulkRetryRuns",
		[]attribute.KeyValue{
			attribute.String("workflow_id", req.WorkflowID),
			attribute.String("mode", req.Mode),
			attribute.Bool("dry_run", req.DryRun),
		},
		func(ctx context.Context) error {
			var innerErr error
			resp, innerErr = s.bulkRetryInner(ctx, req)
			if innerErr == nil {
				slog.InfoContext(ctx,
					"bulk retry completed",
					"workflow_id", req.WorkflowID,
					"mode", req.Mode,
					"retried", len(resp.Retried),
					"skipped", len(resp.Skipped),
				)
			}
			return innerErr
		},
	)
	return resp, err
}

// bulkRetryInner scans, filters, and retries failed runs. Uses the
// creation-ordered newest-first scan (#659, same shape as
// bulkCancelInner) instead of the old order-agnostic ListAll+filter
// sample, so a run that just failed after the population grew past
// maxBulkRetryLimit is still found instead of silently no-op'd.
func (s *Service) bulkRetryInner(
	ctx context.Context,
	req BulkRetryRequest,
) (BulkRetryResponse, error) {
	if req.WorkflowID == "" {
		panic("bulkRetryInner: WorkflowID must not be empty")
	}
	if s.store == nil {
		panic("bulkRetryInner: store must not be nil")
	}
	if err := validateBulkRetryRequest(req); err != nil {
		return BulkRetryResponse{}, err
	}

	pred := func(run dag.WorkflowRun) bool {
		return runMatchesBulkRetry(
			run, req.WorkflowID, req.After, req.Before,
		)
	}
	matched, stats, err := s.store.ScanNewestFirst(
		ctx, pred, maxBulkRetryLimit+1, scaledFetchMax(maxBulkRetryLimit+1),
	)
	if err != nil {
		return BulkRetryResponse{},
			fmt.Errorf("list runs: %w", err)
	}

	if len(matched) > maxBulkRetryLimit {
		return BulkRetryResponse{}, fmt.Errorf(
			"too many matching runs (%d > %d);"+
				" narrow with after/before",
			len(matched), maxBulkRetryLimit,
		)
	}

	// ScanNewestFirst returns newest-first; retry oldest-first (matches
	// the pre-#659 filterFailedRuns ordering).
	reverseRuns(matched)

	if req.DryRun {
		items := make([]BulkRetryItem, len(matched))
		for i, r := range matched {
			items[i] = BulkRetryItem{
				OriginalRunID: r.RunID,
			}
		}
		return BulkRetryResponse{
			Retried: items, Total: len(items),
			DryRun: true, Truncated: stats.Truncated,
		}, nil
	}

	return s.bulkRetryExecute(ctx, req.Mode, matched, stats.Truncated)
}

// bulkRetryExecute dispatches to the rerun/replay executor and stamps
// the resulting response with Truncated -- both executors build a
// fresh BulkRetryResponse with no knowledge of the scan that produced
// their input, so Truncated is attached here, once, rather than
// threaded through both.
func (s *Service) bulkRetryExecute(
	ctx context.Context, mode string,
	matched []dag.WorkflowRun, truncated bool,
) (BulkRetryResponse, error) {
	var resp BulkRetryResponse
	var err error
	switch mode {
	case "rerun":
		resp, err = s.bulkRerun(ctx, matched)
	case "replay":
		resp, err = s.bulkReplay(ctx, matched)
	default:
		panic("bulkRetryExecute: invalid mode passed validation")
	}
	if err != nil {
		return BulkRetryResponse{}, err
	}
	resp.Truncated = truncated
	return resp, nil
}

// bulkRerun starts fresh runs with original inputs.
// Uses a noop span for per-run trace injection — the parent
// bulkRetryRuns span already captures the bulk operation.
func (s *Service) bulkRerun(
	ctx context.Context,
	matched []dag.WorkflowRun,
) (BulkRetryResponse, error) {
	if ctx == nil {
		panic("bulkRerun: ctx must not be nil")
	}
	if s.js == nil {
		panic("bulkRerun: js must not be nil")
	}
	noopTracer := tracenoop.NewTracerProvider().Tracer("")
	_, noopSpan := noopTracer.Start(ctx, "noop")
	var resp BulkRetryResponse
	for _, run := range matched {
		newID, err := s.startRunInner(
			ctx, noopSpan,
			run.WorkflowID, run.Input, run.Labels,
		)
		if err != nil {
			resp.Skipped = append(
				resp.Skipped, run.RunID,
			)
			continue
		}
		resp.Retried = append(resp.Retried,
			BulkRetryItem{
				OriginalRunID: run.RunID,
				NewRunID:      newID,
			},
		)
	}
	resp.Total = len(resp.Retried) + len(resp.Skipped)
	if resp.Retried == nil {
		resp.Retried = []BulkRetryItem{}
	}
	return resp, nil
}

// bulkReplay re-publishes DLQ task messages for failed steps.
func (s *Service) bulkReplay(
	ctx context.Context, matched []dag.WorkflowRun,
) (BulkRetryResponse, error) {
	if matched == nil {
		panic("bulkReplay: matched must not be nil")
	}
	if s.js == nil {
		panic("bulkReplay: js must not be nil")
	}
	// Scan limit matches retry limit so all matched runs
	// can find their DLQ entries.
	dlqEntries, err := s.listDeadLettersInner(
		maxBulkRetryLimit,
	)
	if err != nil {
		return BulkRetryResponse{},
			fmt.Errorf("list DLQ: %w", err)
	}

	dlqByRun := make(
		map[string][]DeadLetterView, len(dlqEntries),
	)
	for _, entry := range dlqEntries {
		dlqByRun[entry.RunID] = append(
			dlqByRun[entry.RunID], entry,
		)
	}

	var resp BulkRetryResponse
	for _, run := range matched {
		entries, found := dlqByRun[run.RunID]
		if !found || len(entries) == 0 {
			resp.Skipped = append(
				resp.Skipped, run.RunID,
			)
			continue
		}
		for _, entry := range entries {
			err := s.replayDeadLetterInner(ctx, entry.Sequence)
			if err != nil {
				continue
			}
		}
		resp.Retried = append(resp.Retried,
			BulkRetryItem{OriginalRunID: run.RunID},
		)
	}
	resp.Total = len(resp.Retried) + len(resp.Skipped)
	if resp.Retried == nil {
		resp.Retried = []BulkRetryItem{}
	}
	return resp, nil
}

// runMatchesBulkRetry reports whether run is a failed run for
// workflowID within [after, before). This is the ScanNewestFirst
// predicate for bulkRetryInner.
func runMatchesBulkRetry(
	run dag.WorkflowRun, workflowID string, after, before time.Time,
) bool {
	if workflowID == "" {
		panic("runMatchesBulkRetry: workflowID must not be empty")
	}
	if run.WorkflowID != workflowID {
		return false
	}
	if run.Status != dag.RunStatusFailed &&
		run.Status != dag.RunStatusCompensateFailed {
		return false
	}
	if !after.IsZero() && run.CreatedAt.Before(after) {
		return false
	}
	if !before.IsZero() && run.CreatedAt.After(before) {
		return false
	}
	return true
}

// validateBulkRetryRequest checks request constraints.
func validateBulkRetryRequest(
	req BulkRetryRequest,
) error {
	if req.WorkflowID == "" {
		panic(
			"validateBulkRetryRequest: WorkflowID must not be empty",
		)
	}
	if req.Mode != "rerun" && req.Mode != "replay" {
		return fmt.Errorf(
			`mode must be "rerun" or "replay"`,
		)
	}
	if !req.After.IsZero() && !req.Before.IsZero() &&
		!req.Before.After(req.After) {
		return fmt.Errorf(
			"before must be after after",
		)
	}
	return nil
}
