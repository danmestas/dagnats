// api/scheduled.go
// One-shot scheduled workflow execution at a future timestamp.
// Stores pending runs in the scheduled_runs KV bucket. Timer
// infrastructure (SLEEP_TIMERS) fires the workflow at run_at.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// maxScheduleAhead is the maximum duration a run can be scheduled
// into the future. 365 days prevents overflow without limiting
// legitimate business use cases.
const maxScheduleAhead = 365 * 24 * time.Hour

// maxScheduledRuns bounds the total number of pending scheduled runs.
const maxScheduledRuns = 100_000

// ScheduledRun represents a workflow run that will start at a future
// time. Stored in the scheduled_runs KV bucket, keyed by RunID.
type ScheduledRun struct {
	RunID      string          `json:"run_id"`
	WorkflowID string          `json:"workflow_id"`
	Input      json.RawMessage `json:"input,omitempty"`
	RunAt      time.Time       `json:"run_at"`
	CreatedAt  time.Time       `json:"created_at"`
	Status     string          `json:"status"`
}

// ScheduleRun validates the workflow exists, generates a run ID,
// and stores a ScheduledRun in KV. Returns the run ID.
// Panics on nil ctx or empty workflowName (programmer errors).
func (s *Service) ScheduleRun(
	ctx context.Context,
	workflowName string,
	input []byte,
	runAt time.Time,
) (string, error) {
	if ctx == nil {
		panic("ScheduleRun: ctx must not be nil")
	}
	if workflowName == "" {
		panic("ScheduleRun: workflowName must not be empty")
	}
	_, span := s.tel.Tracer.Start(ctx,
		"api.scheduleRun",
		observe.WithAttributes(
			observe.StringAttr("workflow_name", workflowName),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	runID, err := s.scheduleRunInner(
		workflowName, input, runAt,
	)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
		return "", err
	}
	span.SetAttributes(observe.StringAttr("run_id", runID))
	return runID, nil
}

// scheduleRunInner holds the core logic for ScheduleRun.
func (s *Service) scheduleRunInner(
	workflowName string,
	input []byte,
	runAt time.Time,
) (string, error) {
	if workflowName == "" {
		panic("scheduleRunInner: workflowName must not be empty")
	}
	if s.scheduledKV == nil {
		return "", fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}

	// Validate workflow exists.
	_, err := s.defKV.Get(context.Background(), workflowName)
	if err != nil {
		return "", fmt.Errorf(
			"workflow %q not found: %w", workflowName, err,
		)
	}

	// Validate run_at bounds.
	delay := time.Until(runAt)
	if delay <= 0 {
		return "", fmt.Errorf(
			"run_at must be in the future",
		)
	}
	if delay > maxScheduleAhead {
		return "", fmt.Errorf(
			"run_at exceeds maximum schedule-ahead of %v",
			maxScheduleAhead,
		)
	}

	// Enforce max scheduled runs bound.
	keys, err := s.scheduledKV.Keys(context.Background())
	if err == nil && len(keys) >= maxScheduledRuns {
		return "", fmt.Errorf(
			"maximum scheduled runs (%d) reached",
			maxScheduledRuns,
		)
	}

	runID := generateRunID()
	sr := ScheduledRun{
		RunID:      runID,
		WorkflowID: workflowName,
		Input:      input,
		RunAt:      runAt,
		CreatedAt:  time.Now().UTC(),
		Status:     "scheduled",
	}
	data, err := json.Marshal(sr)
	if err != nil {
		return "", fmt.Errorf("marshal scheduled run: %w", err)
	}
	_, err = s.scheduledKV.Put(
		context.Background(), runID, data,
	)
	if err != nil {
		return "", fmt.Errorf("store scheduled run: %w", err)
	}

	// Publish timer message to SLEEP_TIMERS.
	timerData, err := json.Marshal(sr)
	if err != nil {
		return "", fmt.Errorf("marshal timer: %w", err)
	}
	timerMsg := &nats.Msg{
		Subject: "scheduled." + runID,
		Data:    timerData,
		Header: nats.Header{
			"Nats-Msg-Id": {"scheduled." + runID},
		},
	}
	_, err = s.js.PublishMsg(
		context.Background(), timerMsg,
	)
	if err != nil {
		return "", fmt.Errorf("publish timer: %w", err)
	}

	return runID, nil
}

// GetScheduledRun retrieves a pending scheduled run by ID.
// Returns nats.ErrKeyNotFound when the run doesn't exist.
func (s *Service) GetScheduledRun(
	runID string,
) (ScheduledRun, error) {
	if runID == "" {
		panic("GetScheduledRun: runID must not be empty")
	}
	if s.scheduledKV == nil {
		return ScheduledRun{}, fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}
	entry, err := s.scheduledKV.Get(
		context.Background(), runID,
	)
	if err != nil {
		return ScheduledRun{}, err
	}
	var sr ScheduledRun
	err = json.Unmarshal(entry.Value(), &sr)
	return sr, err
}

// CancelScheduledRun sets a pending scheduled run's status to
// cancelled. The timer will see "cancelled" and discard (no-op).
func (s *Service) CancelScheduledRun(
	runID string,
) error {
	if runID == "" {
		panic("CancelScheduledRun: runID must not be empty")
	}
	if s.scheduledKV == nil {
		return fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}
	entry, err := s.scheduledKV.Get(
		context.Background(), runID,
	)
	if err != nil {
		return fmt.Errorf(
			"scheduled run %q not found: %w", runID, err,
		)
	}
	var sr ScheduledRun
	if err := json.Unmarshal(entry.Value(), &sr); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if sr.Status != "scheduled" {
		return fmt.Errorf(
			"cannot cancel: status is %q", sr.Status,
		)
	}
	sr.Status = "cancelled"
	data, err := json.Marshal(sr)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, err = s.scheduledKV.Update(
		context.Background(), runID, data, entry.Revision(),
	)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	return nil
}

// ListScheduledRuns returns all pending scheduled runs sorted
// by run_at ascending.
func (s *Service) ListScheduledRuns() ([]ScheduledRun, error) {
	if s.scheduledKV == nil {
		return nil, fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}
	keys, err := s.scheduledKV.Keys(context.Background())
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	const maxKeys = 10_000
	if len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}

	runs := make([]ScheduledRun, 0, len(keys))
	for _, key := range keys {
		entry, err := s.scheduledKV.Get(
			context.Background(), key,
		)
		if err != nil {
			continue
		}
		var sr ScheduledRun
		if err := json.Unmarshal(
			entry.Value(), &sr,
		); err != nil {
			continue
		}
		if sr.Status == "scheduled" {
			runs = append(runs, sr)
		}
	}
	// Sort by run_at ascending.
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].RunAt.Before(runs[j].RunAt)
	})
	return runs, nil
}
