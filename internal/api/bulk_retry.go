// internal/api/bulk_retry.go
// Bulk retry of failed workflow runs. Supports two modes:
// rerun (fresh start with original input) and replay
// (re-publish DLQ task messages to resume at failed step).
package api

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/observe"
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
type BulkRetryResponse struct {
	Retried []BulkRetryItem `json:"retried"`
	Skipped []string        `json:"skipped,omitempty"`
	Total   int             `json:"total"`
	DryRun  bool            `json:"dry_run"`
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
	if s.tel == nil {
		panic("BulkRetryRuns: tel must not be nil")
	}
	if req.WorkflowID == "" {
		return BulkRetryResponse{},
			fmt.Errorf("workflow_id is required")
	}
	ctx, span := s.tel.Tracer.Start(ctx,
		"api.bulkRetryRuns",
		observe.WithAttributes(
			observe.StringAttr("workflow_id", req.WorkflowID),
			observe.StringAttr("mode", req.Mode),
			observe.BoolAttr("dry_run", req.DryRun),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	resp, err := s.bulkRetryInner(ctx, req)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	} else {
		s.tel.Logger.Info("bulk retry completed",
			observe.String("workflow_id", req.WorkflowID),
			observe.String("mode", req.Mode),
			observe.Int("retried", len(resp.Retried)),
			observe.Int("skipped", len(resp.Skipped)),
		)
	}
	return resp, err
}

// bulkRetryInner lists failed runs and retries them.
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

	runs, err := s.store.ListAll(maxBulkRetryLimit + 1)
	if err != nil {
		return BulkRetryResponse{},
			fmt.Errorf("list runs: %w", err)
	}
	matched := filterFailedRuns(
		runs, req.WorkflowID,
		req.After, req.Before,
	)

	if len(matched) > maxBulkRetryLimit {
		return BulkRetryResponse{}, fmt.Errorf(
			"too many matching runs (%d > %d);"+
				" narrow with after/before",
			len(matched), maxBulkRetryLimit,
		)
	}

	if req.DryRun {
		items := make([]BulkRetryItem, len(matched))
		for i, r := range matched {
			items[i] = BulkRetryItem{
				OriginalRunID: r.RunID,
			}
		}
		return BulkRetryResponse{
			Retried: items, Total: len(items),
			DryRun: true,
		}, nil
	}

	switch req.Mode {
	case "rerun":
		return s.bulkRerun(ctx, matched)
	case "replay":
		return s.bulkReplay(matched)
	default:
		panic("bulkRetryInner: invalid mode passed validation")
	}
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
	noopTracer := observe.NewNoopTracer()
	_, noopSpan := noopTracer.Start(ctx, "noop")
	var resp BulkRetryResponse
	for _, run := range matched {
		newID, err := s.startRunInner(
			ctx, noopSpan,
			run.WorkflowID, run.Input,
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
	matched []dag.WorkflowRun,
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
		map[string][]DeadLetter, len(dlqEntries),
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
			err := s.replayDeadLetterInner(entry.Sequence)
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

// filterFailedRuns selects failed runs for the given workflow.
func filterFailedRuns(
	runs []dag.WorkflowRun,
	workflowID string,
	after, before time.Time,
) []dag.WorkflowRun {
	if runs == nil {
		panic("filterFailedRuns: runs must not be nil")
	}
	if workflowID == "" {
		panic(
			"filterFailedRuns: workflowID must not be empty",
		)
	}
	var matched []dag.WorkflowRun
	for _, run := range runs {
		if run.WorkflowID != workflowID {
			continue
		}
		if run.Status != dag.RunStatusFailed &&
			run.Status != dag.RunStatusCompensateFailed {
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
