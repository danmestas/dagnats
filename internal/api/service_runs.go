// api/service_runs.go
// Split out of service.go (#566): run lifecycle + list/count/events + signals domain of the control
// plane Service. Shares the private Service NATS/KV bundle; no new
// connection layer. Behavior identical to the pre-split file.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// RunEvent is a history event for display. Seq is the JetStream
// stream sequence — used by the console SSE so live-stream resume
// skips the prefix the initial server-render already showed.
type RunEvent struct {
	Type        string    `json:"type"`
	RunID       string    `json:"run_id"`
	StepID      string    `json:"step_id"`
	Timestamp   time.Time `json:"timestamp"`
	Data        string    `json:"data"`
	TraceParent string    `json:"trace_parent,omitempty"`
	Seq         uint64    `json:"seq,omitempty"`
}

// StartRun fetches the named workflow definition, generates a run ID,
// and publishes a WorkflowStarted event with injected trace context.
// The orchestrator is the sole owner of run state -- it creates the
// initial snapshot when it processes the event.
func (s *Service) StartRun(
	ctx context.Context, workflowName string, input []byte,
) (string, error) {
	if ctx == nil {
		panic("StartRun: ctx must not be nil")
	}
	if workflowName == "" {
		panic("StartRun: workflowName must not be empty")
	}
	var runID string
	err := s.observed(ctx, "startRun",
		[]attribute.KeyValue{
			attribute.String("workflow_name", workflowName),
		},
		func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			var innerErr error
			runID, innerErr = s.startRunInner(
				ctx, span, workflowName, input,
			)
			if innerErr == nil {
				span.SetAttributes(
					attribute.String("run_id", runID),
				)
			}
			return innerErr
		},
	)
	return runID, err
}

// startRunInner holds the core publish logic for StartRun,
// including trace context injection on the outgoing NATS message.
func (s *Service) startRunInner(
	ctx context.Context,
	span trace.Span,
	workflowName string,
	input []byte,
) (string, error) {
	if workflowName == "" {
		panic(
			"startRunInner: workflowName must not be empty",
		)
	}
	if span == nil {
		panic("startRunInner: span must not be nil")
	}
	entry, err := s.defKV.Get(
		ctx, workflowName,
	)
	if err != nil {
		return "", fmt.Errorf(
			"workflow %q not found: %w", workflowName, err,
		)
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return "", fmt.Errorf("unmarshal workflow def: %w", err)
	}

	// Validate input against InputSchema if present.
	if input != nil && def.InputSchema != nil {
		if err := dag.ValidateSchema(
			def.InputSchema, input,
		); err != nil {
			return "", fmt.Errorf(
				"input validation: %w", err,
			)
		}
	}

	// Idempotency check: if configured, extract key from input
	// and check for existing run.
	if def.IdempotencyKey != "" && input != nil &&
		s.idempotencyKV != nil {
		existingID, err := s.checkIdempotency(
			ctx, workflowName, def.IdempotencyKey, input,
		)
		if err != nil {
			slog.ErrorContext(ctx,
				"idempotency check failed",
				"error", err,
				"workflow", workflowName,
			)
			// Fall through — run without idempotency
		} else if existingID != "" {
			return existingID, nil
		}
	}

	runID := runid.New()
	payload, err := buildStartPayload(entry.Value(), input)
	if err != nil {
		return "", err
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	// JSPublishMsgEvent injects trace context, then re-marshals evt
	// so the persisted body carries TraceParent. msg.Data must stay
	// unset on entry.
	if _, err := s.tp.JSPublishMsgEvent(ctx, msg, &evt); err != nil {
		return "", err
	}
	// Store idempotency key mapping after successful publish.
	if def.IdempotencyKey != "" && input != nil &&
		s.idempotencyKV != nil {
		s.storeIdempotencyKey(
			ctx, workflowName, def.IdempotencyKey, input, runID,
		)
	}

	slog.InfoContext(ctx, "started run",
		"run_id", runID,
		"workflow", workflowName,
	)
	return runID, nil
}

// RunResponse wraps a workflow run snapshot with the trace ID
// extracted from the run's first history event. Always includes
// trace_id (empty string when no trace context is available).
type RunResponse struct {
	dag.WorkflowRun
	TraceID string `json:"trace_id"`
}

// GetRun retrieves the current snapshot for the given run ID.
// Returns engine.ErrRunNotFound when no snapshot exists.
func (s *Service) GetRun(
	ctx context.Context, runID string,
) (dag.WorkflowRun, error) {
	if ctx == nil {
		panic("GetRun: ctx must not be nil")
	}
	if runID == "" {
		panic("GetRun: runID must not be empty")
	}
	var run dag.WorkflowRun
	err := s.observed(ctx, "getRun",
		[]attribute.KeyValue{
			attribute.String("run_id", runID),
		},
		func(ctx context.Context) error {
			var innerErr error
			run, innerErr = s.store.Load(ctx, runID)
			return innerErr
		},
	)
	return run, err
}

// GetRunResponse retrieves the run snapshot enriched with the
// trace ID from the first history event. Returns empty trace_id
// when no trace context exists on the start event.
func (s *Service) GetRunResponse(
	ctx context.Context, runID string,
) (RunResponse, error) {
	if ctx == nil {
		panic("GetRunResponse: ctx must not be nil")
	}
	if runID == "" {
		panic("GetRunResponse: runID must not be empty")
	}
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return RunResponse{}, err
	}
	traceID := s.fetchRunTraceID(runID)
	return RunResponse{
		WorkflowRun: run,
		TraceID:     traceID,
	}, nil
}

// fetchRunTraceID reads the first history event for a run and
// extracts the trace ID from its TraceParent field. Returns ""
// on any failure (best-effort, non-blocking).
func (s *Service) fetchRunTraceID(runID string) string {
	if runID == "" {
		panic("fetchRunTraceID: runID must not be empty")
	}
	if s.nc == nil {
		panic("fetchRunTraceID: nc must not be nil")
	}
	jsLegacy, err := s.nc.JetStream()
	if err != nil {
		return ""
	}
	sub, err := jsLegacy.SubscribeSync(
		"history."+runID,
		nats.DeliverAll(),
		nats.AckNone(),
	)
	if err != nil {
		return ""
	}
	defer sub.Unsubscribe() //nolint:errcheck

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		return ""
	}
	var evt protocol.Event
	if json.Unmarshal(msg.Data, &evt) != nil {
		return ""
	}
	return parseTraceID(evt.TraceParent)
}

// parseTraceID extracts the 32-char hex trace ID from a W3C
// traceparent string. Format: "00-{traceID}-{spanID}-{flags}".
// Returns "" when the input is empty or malformed.
func parseTraceID(traceparent string) string {
	if traceparent == "" {
		return ""
	}
	if len(traceparent) > 256 {
		panic("parseTraceID: traceparent exceeds max length")
	}
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return ""
	}
	return parts[1]
}

// buildStartPayload wraps the workflow definition and optional user
// input into a structured JSON payload for the workflow.started event.
// The engine parses this to store Input on the WorkflowRun snapshot.
func buildStartPayload(
	defBytes []byte, input []byte,
) ([]byte, error) {
	if defBytes == nil {
		panic("buildStartPayload: defBytes must not be nil")
	}
	if len(defBytes) == 0 {
		panic("buildStartPayload: defBytes must not be empty")
	}
	sp := struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input,omitempty"`
	}{
		WorkflowDef: defBytes,
		Input:       input,
	}
	return json.Marshal(sp)
}

// GetRunInput retrieves the stored input for the given run ID.
// Returns nil when the run had no input. Returns an error when
// the run snapshot cannot be loaded.
func (s *Service) GetRunInput(
	ctx context.Context, runID string,
) ([]byte, error) {
	if ctx == nil {
		panic("GetRunInput: ctx must not be nil")
	}
	if runID == "" {
		panic("GetRunInput: runID must not be empty")
	}
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return run.Input, nil
}

// CancelRun publishes a workflow.cancelled event to the history stream.
func (s *Service) CancelRun(
	ctx context.Context, runID string,
) error {
	if ctx == nil {
		panic("CancelRun: ctx must not be nil")
	}
	if runID == "" {
		panic("CancelRun: runID must not be empty")
	}
	return s.observed(ctx, "cancelRun",
		[]attribute.KeyValue{
			attribute.String("run_id", runID),
		},
		func(ctx context.Context) error {
			return s.cancelRunInner(ctx, runID)
		},
	)
}

// cancelRunInner publishes the workflow.cancelled event.
func (s *Service) cancelRunInner(
	ctx context.Context, runID string,
) error {
	if runID == "" {
		panic("cancelRunInner: runID must not be empty")
	}
	if s.js == nil {
		panic("cancelRunInner: js must not be nil")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	_, err = s.tp.JSPublishMsg(ctx, msg)
	return err
}

// SendSignal writes a signal to the signals KV bucket at {runID}.{name}.
func (s *Service) SendSignal(
	ctx context.Context, runID string, name string, data []byte,
) error {
	if ctx == nil {
		panic("SendSignal: ctx must not be nil")
	}
	if runID == "" {
		panic("SendSignal: runID must not be empty")
	}
	if name == "" {
		panic("SendSignal: name must not be empty")
	}
	return s.observed(ctx, "sendSignal",
		[]attribute.KeyValue{
			attribute.String("run_id", runID),
			attribute.String("signal_name", name),
		},
		func(ctx context.Context) error {
			return s.sendSignalInner(ctx, runID, name, data)
		},
	)
}

// sendSignalInner writes to the signals KV bucket.
func (s *Service) sendSignalInner(
	ctx context.Context, runID string, name string, data []byte,
) error {
	if runID == "" {
		panic("sendSignalInner: runID must not be empty")
	}
	if name == "" {
		panic("sendSignalInner: name must not be empty")
	}
	if s.signalKV == nil {
		return fmt.Errorf("signals KV bucket not available")
	}
	key := runID + "." + name
	_, err := s.signalKV.Put(
		ctx, key, data,
	)
	return err
}

// DefaultRunsLimit is the row cap applied when callers pass a
// non-positive limit to ScanRuns or ListRuns. It preserves pre-#257
// behavior (the cap used to be hard-coded inside the run scan).
const DefaultRunsLimit = 1000

// MaxRunsLimitCeiling is the hard server-side ceiling. ScanRuns and
// ListRuns clamp any caller-supplied limit at or below this value.
// Defense against accidental OOM from "fetch everything" callers.
const MaxRunsLimitCeiling = 10000

// ScanRuns returns up to limit runs matching filter, newest-first. It is
// the CHEAP bounded read: the store caps the key scan at limit (ListAll)
// instead of fetching the whole run population, so high-frequency callers
// (console pages, CLI status, agent-runtime scans) stay O(limit), not
// O(population). It trades a true most-recent-N for a bounded, low-cost
// sample — callers that need an honest Total/Truncated must use ListRuns
// (the envelope). limit <= 0 is treated as DefaultRunsLimit; limit >
// MaxRunsLimitCeiling is clamped to the ceiling (friendlier than a 400
// error from a typo).
func (s *Service) ScanRuns(
	ctx context.Context, filter RunsFilter, limit int,
) ([]dag.WorkflowRun, error) {
	if ctx == nil {
		panic("ScanRuns: ctx must not be nil")
	}
	if s.store == nil {
		panic("ScanRuns: store must not be nil")
	}
	effective := clampRunsLimit(limit)
	var runs []dag.WorkflowRun
	err := s.observed(ctx, "scanRuns", nil,
		func(ctx context.Context) error {
			var innerErr error
			runs, innerErr = s.scanRunsInner(ctx, filter, effective)
			return innerErr
		},
	)
	return runs, err
}

// clampRunsLimit normalises a caller-supplied limit. Non-positive
// inputs collapse to DefaultRunsLimit; over-ceiling inputs clamp to
// MaxRunsLimitCeiling.
func clampRunsLimit(limit int) int {
	if limit <= 0 {
		return DefaultRunsLimit
	}
	if limit > MaxRunsLimitCeiling {
		return MaxRunsLimitCeiling
	}
	return limit
}

// scanRunsInner fetches up to limit runs from the cheap order-agnostic
// store scan (ListAll), applies the filter, and sorts newest-first. The
// filter is applied WITHIN the bounded sample — a filtered ScanRuns may
// miss matches older than the sampled window (same caveat the envelope
// documents for filtered totals); callers needing exactness use ListRuns.
func (s *Service) scanRunsInner(
	ctx context.Context, filter RunsFilter, limit int,
) ([]dag.WorkflowRun, error) {
	if s.store == nil {
		panic("scanRunsInner: store must not be nil")
	}
	if limit <= 0 {
		panic("scanRunsInner: limit must be positive")
	}
	runs, err := s.store.ListAll(ctx, limit)
	if err != nil {
		return nil, err
	}
	if !filter.isEmpty() {
		filtered := make([]dag.WorkflowRun, 0, len(runs))
		for _, run := range runs {
			if filter.matches(run) {
				filtered = append(filtered, run)
			}
		}
		runs = filtered
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].CreatedAt.After(runs[j].CreatedAt)
	})
	return runs, nil
}

// RunsFilter narrows a run-list or count query. The zero value matches
// every run. Workflow filters by WorkflowID; State (nil = any) filters
// by run status; Since (zero = any age) keeps runs with CreatedAt at
// or after the cutoff.
type RunsFilter struct {
	Workflow string
	State    *dag.RunStatus
	Since    time.Time
}

// isEmpty reports whether the filter matches every run (the zero
// value). We avoid `f == (RunsFilter{})` because time.Time `==` is a
// stdlib footgun — it compares the wall-clock + monotonic + location
// fields, so two "zero" times can compare unequal. IsZero is correct.
func (f RunsFilter) isEmpty() bool {
	return f.Workflow == "" && f.State == nil && f.Since.IsZero()
}

// matches reports whether a single run satisfies the filter.
func (f RunsFilter) matches(run dag.WorkflowRun) bool {
	if f.Workflow != "" && run.WorkflowID != f.Workflow {
		return false
	}
	if f.State != nil && run.Status != *f.State {
		return false
	}
	if !f.Since.IsZero() && run.CreatedAt.Before(f.Since) {
		return false
	}
	return true
}

// RunsEnvelope is the honest shape returned by ListRuns: the
// (already-truncated) Runs window plus the true Total matching the
// filter, the Returned count, and whether Total exceeded the window.
type RunsEnvelope struct {
	Runs      []dag.WorkflowRun
	Total     int
	Returned  int
	Truncated bool
}

// runReader is the read surface ListRuns and CountRuns need from the
// store: the globally-sorted latest-N window (ListRecent) and an exact
// keys-only population count (CountAll). *engine.SnapshotStore satisfies
// it; the seam keeps the envelope/count logic unit-testable.
//
// NOTE: this deliberately uses ListRecent, NOT the cheap order-agnostic
// ListAll — the #452 surface needs the genuine most-recent N. The cheap
// ScanRuns path and other callers (reconciler, bulk retry/cancel) keep
// using ListAll directly and are unaffected by this seam.
type runReader interface {
	ListRecent(ctx context.Context, limit int) ([]dag.WorkflowRun, error)
	CountAll(ctx context.Context) (int, error)
}

// ListRuns returns the newest matching runs capped at limit, alongside
// the total so callers can report "showing N of M" and truncation
// honestly (#452). This is the HONEST list surface: it fetches the
// genuine most-recent window (ListRecent) plus an exact total, so it
// costs O(population). High-frequency callers that only need a bounded
// sample should use ScanRuns instead.
//
// For an UNFILTERED query the total is the exact population from the
// cheap keys-only CountAll path — so "showing 1000 of 146046" is the
// real figure, not the ceiling-capped window size.
//
// For a FILTERED query (state/since/workflow) the total is the count
// of matches WITHIN the most-recent MaxRunsLimitCeiling window: a true
// full-population filtered count is not cheaply available without a
// secondary index, so a filtered total may undercount matches older
// than that window. The exact filtered count lands with the #453
// time-ordered index work.
func (s *Service) ListRuns(
	ctx context.Context, filter RunsFilter, limit int,
) (RunsEnvelope, error) {
	if ctx == nil {
		panic("ListRuns: ctx must not be nil")
	}
	if s.store == nil {
		panic("ListRuns: store must not be nil")
	}
	effective := clampRunsLimit(limit)
	var env RunsEnvelope
	err := s.observed(ctx, "listRunsEnvelope", nil,
		func(ctx context.Context) error {
			var innerErr error
			env, innerErr = listRunsEnvelopeFrom(ctx, s.store, filter, effective)
			return innerErr
		},
	)
	return env, err
}

// listRunsEnvelopeFrom fetches the globally-sorted latest-N window,
// truncates it to limit for the returned rows, and derives the total
// per the ListRuns envelope contract (exact CountAll when unfiltered,
// in-window match count when filtered).
func listRunsEnvelopeFrom(
	ctx context.Context, store runReader, filter RunsFilter, limit int,
) (RunsEnvelope, error) {
	if store == nil {
		panic("listRunsEnvelopeFrom: store must not be nil")
	}
	if limit <= 0 {
		panic("listRunsEnvelopeFrom: limit must be positive")
	}
	all, err := store.ListRecent(ctx, MaxRunsLimitCeiling)
	if err != nil {
		return RunsEnvelope{}, err
	}
	matched := make([]dag.WorkflowRun, 0, len(all))
	for _, run := range all {
		if filter.matches(run) {
			matched = append(matched, run)
		}
	}
	total := len(matched)
	if filter.isEmpty() {
		// Exact population — not the ceiling-capped window length.
		total, err = store.CountAll(ctx)
		if err != nil {
			return RunsEnvelope{}, err
		}
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return RunsEnvelope{
		Runs:      matched,
		Total:     total,
		Returned:  len(matched),
		Truncated: total > len(matched),
	}, nil
}

// CountRuns returns the number of runs matching the filter without
// materializing rows (#452).
//
// An UNFILTERED count uses the cheap keys-only CountAll path and is
// exact for the whole population. A FILTERED count fetches the most-
// recent MaxRunsLimitCeiling window and applies the filter in memory,
// so it may undercount matches older than that window — the exact
// filtered count lands with the #453 time-ordered index work.
func (s *Service) CountRuns(
	ctx context.Context, filter RunsFilter,
) (int, error) {
	if ctx == nil {
		panic("CountRuns: ctx must not be nil")
	}
	if s.store == nil {
		panic("CountRuns: store must not be nil")
	}
	var count int
	err := s.observed(ctx, "countRuns", nil,
		func(ctx context.Context) error {
			var innerErr error
			count, innerErr = countRunsFrom(ctx, s.store, filter)
			return innerErr
		},
	)
	return count, err
}

// countRunsFrom is the unobserved body of CountRuns, parameterised on
// runReader so the filter math is unit-testable without a live store.
func countRunsFrom(
	ctx context.Context, store runReader, filter RunsFilter,
) (int, error) {
	if store == nil {
		panic("countRunsFrom: store must not be nil")
	}
	if filter.isEmpty() {
		return store.CountAll(ctx)
	}
	all, err := store.ListRecent(ctx, MaxRunsLimitCeiling)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, run := range all {
		if filter.matches(run) {
			count++
		}
	}
	return count, nil
}

// ListRunEvents retrieves history events for a given run.
// Data field truncated to 200 chars unless fullData is true.
func (s *Service) ListRunEvents(
	ctx context.Context, runID string, fullData bool,
) ([]RunEvent, error) {
	if ctx == nil {
		panic("ListRunEvents: ctx must not be nil")
	}
	if runID == "" {
		panic("ListRunEvents: runID must not be empty")
	}
	var events []RunEvent
	err := s.observed(ctx, "listRunEvents",
		[]attribute.KeyValue{
			attribute.String("run_id", runID),
		},
		func(_ context.Context) error {
			var innerErr error
			events, innerErr = s.listRunEventsInner(
				runID, fullData,
			)
			return innerErr
		},
	)
	return events, err
}

// listRunEventsInner subscribes to history stream and reads events.
func (s *Service) listRunEventsInner(
	runID string, fullData bool,
) ([]RunEvent, error) {
	if runID == "" {
		panic("listRunEventsInner: runID must not be empty")
	}
	if s.js == nil {
		panic("listRunEventsInner: js must not be nil")
	}
	const maxEvents = 500
	const dataTruncateLen = 200

	jsLegacy, err := s.nc.JetStream()
	if err != nil {
		return nil, err
	}
	sub, err := jsLegacy.SubscribeSync(
		"history."+runID,
		nats.DeliverAll(),
		nats.AckNone(),
	)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe() //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	msgs := fetchMessages(sub, maxEvents, deadline)
	events := make([]RunEvent, 0, len(msgs))
	for _, msg := range msgs {
		var evt protocol.Event
		if json.Unmarshal(msg.Data, &evt) != nil {
			continue
		}
		dataStr := string(evt.Payload)
		if !fullData && len(dataStr) > dataTruncateLen {
			dataStr = dataStr[:dataTruncateLen]
		}
		var seq uint64
		if md, err := msg.Metadata(); err == nil && md != nil {
			seq = md.Sequence.Stream
		}
		events = append(events, RunEvent{
			Type:        string(evt.Type),
			RunID:       evt.RunID,
			StepID:      evt.StepID,
			Timestamp:   evt.Timestamp,
			Data:        dataStr,
			TraceParent: evt.TraceParent,
			Seq:         seq,
		})
	}
	return events, nil
}

// StartTyped marshals a typed input and starts a workflow run.
// Convenience wrapper around StartRun for typed workflows.
func StartTyped[I any](
	ctx context.Context, svc *Service, workflowName string, input I,
) (string, error) {
	if svc == nil {
		panic("StartTyped: svc must not be nil")
	}
	data, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("marshal input: %w", err)
	}
	return svc.StartRun(ctx, workflowName, data)
}
