// api/service.go
// Control plane service: register workflow definitions, start runs, query state.
// This layer is shared by REST and NATS request/reply handlers -- it owns no
// transport concerns, only business logic backed by NATS KV and JetStream.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/trigger"
	"github.com/nats-io/nats.go"
)

// Service is the control plane for DagNats. It writes workflow definitions to
// KV and publishes WorkflowStarted events to the history stream. Run state is
// owned exclusively by the orchestrator -- the service only reads snapshots.
type Service struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	defKV     nats.KeyValue
	store     *engine.SnapshotStore
	tel       *observe.Telemetry
	triggerKV nats.KeyValue
	signalKV  nats.KeyValue

	// Pre-allocated metric instruments -- created once in constructor.
	requestCount    observe.Counter
	requestDuration observe.Histogram
	errorCount      observe.Counter
}

// DeadLetter represents a message that failed processing.
type DeadLetter struct {
	Sequence  uint64    `json:"sequence"`
	Subject   string    `json:"subject"`
	RunID     string    `json:"run_id"`
	StepID    string    `json:"step_id"`
	Task      string    `json:"task"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

// RunEvent is a history event for display.
type RunEvent struct {
	Type      string    `json:"type"`
	RunID     string    `json:"run_id"`
	StepID    string    `json:"step_id"`
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"data"`
}

// NewService binds the control plane to an active NATS connection.
// Panics if JetStream init fails or the workflow_defs bucket does not
// exist -- callers must call natsutil.SetupAll before constructing.
func NewService(nc *nats.Conn, tel *observe.Telemetry) *Service {
	if nc == nil {
		panic("NewService: nc must not be nil")
	}
	if tel == nil {
		panic("NewService: tel must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewService: JetStream init failed: " + err.Error())
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		panic(
			"NewService: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	triggerKV, _ := js.KeyValue("triggers")
	signalKV, _ := js.KeyValue("signals")
	return &Service{
		nc:        nc,
		js:        js,
		defKV:     defKV,
		store:     engine.NewSnapshotStore(js),
		tel:       tel,
		triggerKV: triggerKV,
		signalKV:  signalKV,
		requestCount: tel.Metrics.Counter(
			"api.requests", nil,
		),
		requestDuration: tel.Metrics.Histogram(
			"api.request.duration_ms", nil,
		),
		errorCount: tel.Metrics.Counter(
			"api.errors", nil,
		),
	}
}

// RegisterWorkflow validates and persists a workflow definition under
// its name. Subsequent calls with the same name overwrite the previous
// version -- the engine reads the definition at run-start time.
func (s *Service) RegisterWorkflow(
	ctx context.Context, def dag.WorkflowDef,
) error {
	if ctx == nil {
		panic("RegisterWorkflow: ctx must not be nil")
	}
	ctx, span := s.tel.Tracer.Start(ctx,
		"api.registerWorkflow",
		observe.WithAttributes(
			observe.StringAttr("workflow_name", def.Name),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	err := s.registerWorkflowInner(def)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return err
}

// registerWorkflowInner holds the core logic, keeping the
// instrumented wrapper under the 70-line limit.
func (s *Service) registerWorkflowInner(
	def dag.WorkflowDef,
) error {
	if err := dag.Validate(def); err != nil {
		return fmt.Errorf("invalid workflow: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.defKV.Put(def.Name, data)
	return err
}

// GetWorkflow retrieves the registered definition for the named
// workflow. Returns a NATS key-not-found error when not registered.
func (s *Service) GetWorkflow(name string) (dag.WorkflowDef, error) {
	entry, err := s.defKV.Get(name)
	if err != nil {
		return dag.WorkflowDef{}, err
	}
	var def dag.WorkflowDef
	err = json.Unmarshal(entry.Value(), &def)
	return def, err
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
	ctx, span := s.tel.Tracer.Start(ctx,
		"api.startRun",
		observe.WithAttributes(
			observe.StringAttr("workflow_name", workflowName),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	runID, err := s.startRunInner(ctx, span, workflowName, input)
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

// startRunInner holds the core publish logic for StartRun,
// including trace context injection on the outgoing NATS message.
func (s *Service) startRunInner(
	ctx context.Context,
	span observe.Span,
	workflowName string,
	input []byte,
) (string, error) {
	entry, err := s.defKV.Get(workflowName)
	if err != nil {
		return "", fmt.Errorf(
			"workflow %q not found: %w", workflowName, err,
		)
	}
	runID := generateRunID()
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, entry.Value(),
	)
	injectAPITraceCtx(span, &evt)
	data, err := evt.Marshal()
	if err != nil {
		return "", err
	}
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	injectAPIMsgTraceCtx(span, msg)
	_, err = s.js.PublishMsg(msg)
	if err != nil {
		return "", err
	}
	s.tel.Logger.Info("started run",
		observe.String("run_id", runID),
		observe.String("workflow", workflowName),
	)
	return runID, nil
}

// GetRun retrieves the current snapshot for the given run ID.
// Returns engine.ErrRunNotFound when no snapshot exists.
func (s *Service) GetRun(
	ctx context.Context, runID string,
) (dag.WorkflowRun, error) {
	if ctx == nil {
		panic("GetRun: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx,
		"api.getRun",
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	run, err := s.store.Load(runID)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return run, err
}

// generateRunID returns a 32-character lowercase hex string from
// 16 crypto-random bytes. Panics only if the OS entropy source is
// unavailable -- a fatal system condition.
func generateRunID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("generateRunID: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// injectAPITraceCtx sets TraceParent on the event from the active
// span's SpanContext, enabling the engine to link its spans as
// children of the API span. No-op when span lacks SpanContext.
func injectAPITraceCtx(span observe.Span, evt *protocol.Event) {
	if span == nil {
		panic("injectAPITraceCtx: span must not be nil")
	}
	if evt == nil {
		panic("injectAPITraceCtx: evt must not be nil")
	}
	tp := formatAPITraceparent(span)
	if tp == "" {
		return
	}
	evt.TraceParent = tp
}

// injectAPIMsgTraceCtx sets traceparent header on the outgoing NATS
// message from the active span's SpanContext. No-op when span lacks
// SpanContext or IDs are empty.
func injectAPIMsgTraceCtx(span observe.Span, msg *nats.Msg) {
	if span == nil {
		panic("injectAPIMsgTraceCtx: span must not be nil")
	}
	if msg == nil {
		panic("injectAPIMsgTraceCtx: msg must not be nil")
	}
	tp := formatAPITraceparent(span)
	if tp == "" {
		return
	}
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set("traceparent", tp)
}

// formatAPITraceparent extracts trace/span IDs from the span and
// returns a W3C traceparent string. Returns "" when the span does
// not implement SpanContext or has empty IDs.
func formatAPITraceparent(span observe.Span) string {
	sc, ok := span.(observe.SpanContext)
	if !ok {
		return ""
	}
	traceID := sc.TraceID()
	spanID := sc.SpanID()
	if traceID == "" || spanID == "" {
		return ""
	}
	return "00-" + traceID + "-" + spanID + "-01"
}

// ListWorkflows retrieves all registered workflow definitions from KV.
func (s *Service) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if ctx == nil {
		panic("ListWorkflows: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.listWorkflows")
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	defs, err := s.listWorkflowsInner()
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return defs, err
}

// listWorkflowsInner holds the KV iteration logic.
func (s *Service) listWorkflowsInner() ([]dag.WorkflowDef, error) {
	keys, err := s.defKV.Keys()
	if err != nil {
		return nil, err
	}
	defs := make([]dag.WorkflowDef, 0, len(keys))
	for _, key := range keys {
		entry, err := s.defKV.Get(key)
		if err != nil {
			return nil, err
		}
		var def dag.WorkflowDef
		err = json.Unmarshal(entry.Value(), &def)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, nil
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
	_, span := s.tel.Tracer.Start(ctx,
		"api.cancelRun",
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	err := s.cancelRunInner(runID)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return err
}

// cancelRunInner publishes the workflow.cancelled event.
func (s *Service) cancelRunInner(runID string) error {
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
	_, err = s.js.PublishMsg(msg)
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
	_, span := s.tel.Tracer.Start(ctx,
		"api.sendSignal",
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
			observe.StringAttr("signal_name", name),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	err := s.sendSignalInner(runID, name, data)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return err
}

// sendSignalInner writes to the signals KV bucket.
func (s *Service) sendSignalInner(
	runID string, name string, data []byte,
) error {
	if s.signalKV == nil {
		return fmt.Errorf("signals KV bucket not available")
	}
	key := runID + "." + name
	_, err := s.signalKV.Put(key, data)
	return err
}

// CreateTrigger validates and stores a trigger definition.
func (s *Service) CreateTrigger(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if ctx == nil {
		panic("CreateTrigger: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx,
		"api.createTrigger",
		observe.WithAttributes(
			observe.StringAttr("trigger_id", def.ID),
			observe.StringAttr("workflow_id", def.WorkflowID),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	err := s.createTriggerInner(def)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return err
}

// createTriggerInner validates and writes the trigger to KV.
func (s *Service) createTriggerInner(def trigger.TriggerDef) error {
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	if err := trigger.Validate(def); err != nil {
		return fmt.Errorf("invalid trigger: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.triggerKV.Put(def.ID, data)
	return err
}

// ListTriggers retrieves all trigger definitions from KV.
func (s *Service) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if ctx == nil {
		panic("ListTriggers: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.listTriggers")
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	defs, err := s.listTriggersInner()
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return defs, err
}

// listTriggersInner holds the KV iteration logic.
func (s *Service) listTriggersInner() ([]trigger.TriggerDef, error) {
	if s.triggerKV == nil {
		return []trigger.TriggerDef{}, nil
	}
	keys, err := s.triggerKV.Keys()
	if err != nil {
		return nil, err
	}
	defs := make([]trigger.TriggerDef, 0, len(keys))
	for _, key := range keys {
		entry, err := s.triggerKV.Get(key)
		if err != nil {
			return nil, err
		}
		var def trigger.TriggerDef
		err = json.Unmarshal(entry.Value(), &def)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, nil
}

// DeleteTrigger removes a trigger definition from KV.
func (s *Service) DeleteTrigger(
	ctx context.Context, triggerID string,
) error {
	if ctx == nil {
		panic("DeleteTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("DeleteTrigger: triggerID must not be empty")
	}
	_, span := s.tel.Tracer.Start(ctx,
		"api.deleteTrigger",
		observe.WithAttributes(
			observe.StringAttr("trigger_id", triggerID),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	err := s.deleteTriggerInner(triggerID)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return err
}

// deleteTriggerInner deletes the trigger from KV.
func (s *Service) deleteTriggerInner(triggerID string) error {
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	return s.triggerKV.Delete(triggerID)
}

// ListDeadLetters retrieves up to limit dead letter messages.
func (s *Service) ListDeadLetters(
	ctx context.Context, limit int,
) ([]DeadLetter, error) {
	if ctx == nil {
		panic("ListDeadLetters: ctx must not be nil")
	}
	if limit <= 0 {
		panic("ListDeadLetters: limit must be positive")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.listDeadLetters")
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	letters, err := s.listDeadLettersInner(limit)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return letters, err
}

// listDeadLettersInner fetches messages from the DEAD_LETTERS stream.
func (s *Service) listDeadLettersInner(
	limit int,
) ([]DeadLetter, error) {
	sub, err := s.js.SubscribeSync("dead.>")
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe() //nolint:errcheck
	letters := make([]DeadLetter, 0, limit)
	for i := 0; i < limit; i++ {
		msg, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			break
		}
		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			continue
		}
		var payload protocol.TaskPayload
		unmarshalErr := json.Unmarshal(msg.Data, &payload)
		if unmarshalErr != nil {
			continue
		}
		letters = append(letters, DeadLetter{
			Sequence:  meta.Sequence.Stream,
			Subject:   msg.Subject,
			RunID:     payload.RunID,
			StepID:    payload.StepID,
			Task:      extractTaskFromSubject(msg.Subject),
			Error:     msg.Header.Get("Error"),
			Timestamp: meta.Timestamp,
		})
	}
	return letters, nil
}

// extractTaskFromSubject extracts the task name from a subject.
func extractTaskFromSubject(subject string) string {
	if len(subject) > 5 && subject[:5] == "dead." {
		return subject[5:]
	}
	return subject
}

// ReplayDeadLetter fetches a dead letter by sequence and republishes it.
func (s *Service) ReplayDeadLetter(
	ctx context.Context, seq uint64,
) error {
	if ctx == nil {
		panic("ReplayDeadLetter: ctx must not be nil")
	}
	if seq == 0 {
		panic("ReplayDeadLetter: seq must be positive")
	}
	_, span := s.tel.Tracer.Start(ctx,
		"api.replayDeadLetter",
		observe.WithAttributes(
			observe.Int64Attr("sequence", int64(seq)),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	err := s.replayDeadLetterInner(seq)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return err
}

// replayDeadLetterInner fetches by sequence and republishes.
func (s *Service) replayDeadLetterInner(seq uint64) error {
	letters, err := s.listDeadLettersInner(100)
	if err != nil {
		return err
	}
	var target *DeadLetter
	for i := range letters {
		if letters[i].Sequence == seq {
			target = &letters[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("dead letter with sequence %d not found", seq)
	}
	payload := protocol.TaskPayload{
		RunID:  target.RunID,
		StepID: target.StepID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	origSubject := target.Task
	if !isTaskSubject(origSubject) {
		origSubject = "task." + origSubject
	}
	_, err = s.js.Publish(origSubject, data)
	return err
}

// isTaskSubject checks if a subject starts with "task.".
func isTaskSubject(subject string) bool {
	return len(subject) >= 5 && subject[:5] == "task."
}

// ListRuns retrieves all workflow runs, optionally filtered by workflow ID.
// Returns runs sorted by CreatedAt descending (newest first).
func (s *Service) ListRuns(
	ctx context.Context, workflowFilter string,
) ([]dag.WorkflowRun, error) {
	if ctx == nil {
		panic("ListRuns: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.listRuns")
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	runs, err := s.listRunsInner(workflowFilter)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return runs, err
}

// listRunsInner retrieves all runs from the store, filters, and sorts.
func (s *Service) listRunsInner(
	workflowFilter string,
) ([]dag.WorkflowRun, error) {
	const maxRunsLimit = 1000
	runs, err := s.store.ListAll(maxRunsLimit)
	if err != nil {
		return nil, err
	}
	if workflowFilter != "" {
		filtered := make([]dag.WorkflowRun, 0, len(runs))
		for _, run := range runs {
			if run.WorkflowID == workflowFilter {
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
	_, span := s.tel.Tracer.Start(ctx,
		"api.listRunEvents",
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	events, err := s.listRunEventsInner(runID, fullData)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
	}
	return events, err
}

// listRunEventsInner subscribes to history stream and reads events.
func (s *Service) listRunEventsInner(
	runID string, fullData bool,
) ([]RunEvent, error) {
	const maxEvents = 500
	const fetchTimeoutMs = 2000
	const dataTruncateLen = 200

	sub, err := s.js.SubscribeSync(
		"history."+runID,
		nats.DeliverAll(),
		nats.AckNone(),
	)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe() //nolint:errcheck

	events := make([]RunEvent, 0, maxEvents)
	for i := 0; i < maxEvents; i++ {
		msg, err := sub.NextMsg(fetchTimeoutMs * time.Millisecond)
		if err != nil {
			break
		}
		var evt protocol.Event
		err = json.Unmarshal(msg.Data, &evt)
		if err != nil {
			continue
		}
		dataStr := string(evt.Payload)
		if !fullData && len(dataStr) > dataTruncateLen {
			dataStr = dataStr[:dataTruncateLen]
		}
		events = append(events, RunEvent{
			Type:      string(evt.Type),
			RunID:     evt.RunID,
			StepID:    evt.StepID,
			Timestamp: evt.Timestamp,
			Data:      dataStr,
		})
	}
	return events, nil
}
