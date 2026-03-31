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
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"

	"github.com/nats-io/nats.go"
)

// Service is the control plane for DagNats. It writes workflow definitions to
// KV and publishes WorkflowStarted events to the history stream. Run state is
// owned exclusively by the orchestrator -- the service only reads snapshots.
type Service struct {
	nc    *nats.Conn
	js    nats.JetStreamContext
	defKV nats.KeyValue
	store *engine.SnapshotStore
	tel   *observe.Telemetry

	// Pre-allocated metric instruments -- created once in constructor.
	requestCount    observe.Counter
	requestDuration observe.Histogram
	errorCount      observe.Counter
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
	return &Service{
		nc:    nc,
		js:    js,
		defKV: defKV,
		store: engine.NewSnapshotStore(js),
		tel:   tel,
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

// ListWorkflows returns all registered workflow definitions by
// iterating the workflow_defs KV bucket keys. Returns at most
// 1000 workflows to stay bounded.
func (s *Service) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if ctx == nil {
		panic("ListWorkflows: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.listWorkflows")
	defer span.End()
	s.requestCount.Inc()
	start := time.Now()

	keys, err := s.defKV.Keys()
	if err != nil {
		// Keys returns nats.ErrNoKeysFound when bucket is empty.
		if err.Error() == "nats: no keys found" {
			s.requestDuration.Observe(
				float64(time.Since(start).Milliseconds()),
			)
			return []dag.WorkflowDef{}, nil
		}
		s.errorCount.Inc()
		return nil, err
	}
	const maxWorkflows = 1000
	defs := make([]dag.WorkflowDef, 0, len(keys))
	for i, key := range keys {
		if i >= maxWorkflows {
			break
		}
		entry, err := s.defKV.Get(key)
		if err != nil {
			continue
		}
		var def dag.WorkflowDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}
		defs = append(defs, def)
	}
	s.requestDuration.Observe(
		float64(time.Since(start).Milliseconds()),
	)
	return defs, nil
}

// ListRuns returns workflow run snapshots, optionally filtered by
// workflow name and status. Returns at most 1000 runs.
func (s *Service) ListRuns(
	ctx context.Context, workflow string, status string,
) ([]dag.WorkflowRun, error) {
	if ctx == nil {
		panic("ListRuns: ctx must not be nil")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.listRuns")
	defer span.End()
	s.requestCount.Inc()
	start := time.Now()

	runs, err := s.listRunsInner(workflow, status)
	s.requestDuration.Observe(
		float64(time.Since(start).Milliseconds()),
	)
	if err != nil {
		s.errorCount.Inc()
	}
	return runs, err
}

// listRunsInner holds the core iteration logic.
func (s *Service) listRunsInner(
	workflow string, status string,
) ([]dag.WorkflowRun, error) {
	keys, err := s.store.Keys()
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return []dag.WorkflowRun{}, nil
		}
		return nil, err
	}
	const maxRuns = 1000
	runs := make([]dag.WorkflowRun, 0, len(keys))
	for i, key := range keys {
		if i >= maxRuns {
			break
		}
		// Keys are stored as "run.{runID}"
		runID := key
		if len(runID) > 4 && runID[:4] == "run." {
			runID = runID[4:]
		}
		run, err := s.store.Load(runID)
		if err != nil {
			continue
		}
		if workflow != "" && run.WorkflowID != workflow {
			continue
		}
		if status != "" && run.Status.String() != status {
			continue
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// GetRunEvents reads the event history for a run from the
// WORKFLOW_HISTORY stream. Returns at most 10000 events.
func (s *Service) GetRunEvents(
	ctx context.Context, runID string,
) ([]protocol.Event, error) {
	if ctx == nil {
		panic("GetRunEvents: ctx must not be nil")
	}
	if runID == "" {
		panic("GetRunEvents: runID must not be empty")
	}
	_, span := s.tel.Tracer.Start(ctx, "api.getRunEvents",
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
		),
	)
	defer span.End()
	s.requestCount.Inc()
	start := time.Now()

	events, err := s.getRunEventsInner(runID)
	s.requestDuration.Observe(
		float64(time.Since(start).Milliseconds()),
	)
	if err != nil {
		s.errorCount.Inc()
	}
	return events, err
}

// getRunEventsInner reads events from the stream by creating
// an ephemeral consumer filtered to history.{runID}.
func (s *Service) getRunEventsInner(
	runID string,
) ([]protocol.Event, error) {
	subject := "history." + runID
	sub, err := s.js.SubscribeSync(
		subject,
		nats.DeliverAll(),
		nats.AckNone(),
		nats.BindStream("WORKFLOW_HISTORY"),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"subscribe to %s: %w", subject, err,
		)
	}
	defer sub.Unsubscribe()

	const maxEvents = 10000
	events := make([]protocol.Event, 0, 64)
	timeout := 200 * time.Millisecond
	for i := 0; i < maxEvents; i++ {
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		evt, err := protocol.UnmarshalEvent(msg.Data)
		if err != nil {
			continue
		}
		events = append(events, evt)
	}
	return events, nil
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
