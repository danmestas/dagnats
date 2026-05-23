// api/service.go
// Control plane service: register workflow definitions, start runs, query state.
// This layer is shared by REST and NATS request/reply handlers -- it owns no
// transport concerns, only business logic backed by NATS KV and JetStream.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Service is the control plane for DagNats. It writes workflow
// definitions to KV and publishes WorkflowStarted events to the
// history stream. Run state is owned exclusively by the
// orchestrator -- the service only reads snapshots.
type Service struct {
	nc *nats.Conn
	js jetstream.JetStream
	// tp wraps publish operations so every workflow.started /
	// task re-enqueue / scheduled run dispatch carries W3C trace
	// context. Constructed once in NewService and shared with
	// helpers (timer.go, scheduled.go, bulk_run.go). #334.
	tp            *natsutil.TracingPublisher
	defKV         jetstream.KeyValue
	store         *engine.SnapshotStore
	tracer        trace.Tracer
	meter         metric.Meter
	triggerKV     jetstream.KeyValue
	signalKV      jetstream.KeyValue
	scheduledKV   jetstream.KeyValue
	idempotencyKV jetstream.KeyValue

	// Pre-allocated metric instruments -- created once.
	requestCount    metric.Int64Counter
	requestDuration metric.Float64Histogram
	errorCount      metric.Int64Counter
}

// DeadLetter represents a message that failed processing. The
// final schema (#200) extends the legacy fields with Body, Headers,
// DeliveryCount, and Consumer so replay can re-publish the original
// task verbatim and operators can see delivery metadata.
type DeadLetter struct {
	Sequence  uint64    `json:"sequence"`
	Subject   string    `json:"subject"`
	RunID     string    `json:"run_id"`
	StepID    string    `json:"step_id"`
	Task      string    `json:"task"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`

	// Body is the original task message payload at the moment of
	// DLQ entry — the marshalled protocol.TaskPayload bytes that
	// would have been on the task subject. Empty for legacy entries
	// written before this schema landed; replay against a legacy
	// entry returns ErrDLQBodyMissing.
	Body []byte `json:"body,omitempty"`

	// Headers carries the original NATS headers verbatim so replay
	// reproduces the same dispatch context.
	Headers nats.Header `json:"headers,omitempty"`

	// DeliveryCount is the JetStream redelivery count at the moment
	// of DLQ publish — i.e. the value that triggered exhaustion.
	DeliveryCount int `json:"delivery_count,omitempty"`

	// Consumer is the JetStream consumer name that delivered the
	// original message. Surfaces in the CLI so operators can tell
	// which path the task came through.
	Consumer string `json:"consumer,omitempty"`
}

// DeadLetterView is the operator-facing rendering of a DLQ entry:
// the raw DeadLetter plus derived fields the CLI surfaces directly.
// CLI code does no derivation of its own — all derivation lives here.
type DeadLetterView struct {
	DeadLetter
	BodyPreserved bool `json:"body_preserved"`
}

// newDeadLetterView returns the operator-facing rendering of a
// DeadLetter. BodyPreserved is true when the stored Body is
// non-empty — only such entries are replayable.
func newDeadLetterView(dl DeadLetter) DeadLetterView {
	return DeadLetterView{
		DeadLetter:    dl,
		BodyPreserved: len(dl.Body) > 0,
	}
}

// ErrDLQBodyMissing is returned by ReplayDeadLetter when the DLQ
// entry's Body is empty — typically a legacy entry written before
// the body-preservation schema landed. Operators recover such
// entries via upstream reconstruction; the CLI must not silently
// re-publish a stub.
var ErrDLQBodyMissing = errors.New(
	"dlq entry body not preserved; replay unsupported",
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

// NewService binds the control plane to an active NATS connection.
// Panics if JetStream init fails or the workflow_defs bucket does
// not exist -- callers must call natsutil.SetupAll first.
func NewService(nc *nats.Conn) *Service {
	if nc == nil {
		panic("NewService: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(
			"NewService: jetstream.New: " + err.Error(),
		)
	}
	ctx := context.Background()
	defKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		panic(
			"NewService: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	triggerKV, _ := js.KeyValue(ctx, "triggers")
	signalKV, _ := js.KeyValue(ctx, "signals")
	scheduledKV, _ := js.KeyValue(ctx, "scheduled_runs")
	idempotencyKV, _ := js.KeyValue(
		ctx, "idempotency_keys",
	)
	m := otel.Meter("dagnats/api")
	reqCount, _ := m.Int64Counter("api.requests")
	reqDur, _ := m.Float64Histogram(
		"api.request.duration_ms",
	)
	errCount, _ := m.Int64Counter("api.errors")
	return &Service{
		nc:              nc,
		js:              js,
		tp:              natsutil.NewTracingPublisher(nc, js),
		defKV:           defKV,
		store:           engine.NewSnapshotStore(js),
		tracer:          otel.Tracer("dagnats/api"),
		meter:           m,
		triggerKV:       triggerKV,
		signalKV:        signalKV,
		scheduledKV:     scheduledKV,
		idempotencyKV:   idempotencyKV,
		requestCount:    reqCount,
		requestDuration: reqDur,
		errorCount:      errCount,
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
	if def.Name == "" {
		panic("RegisterWorkflow: def.Name must not be empty")
	}
	return s.observed(ctx, "registerWorkflow",
		[]attribute.KeyValue{
			attribute.String("workflow_name", def.Name),
		},
		func(ctx context.Context) error {
			return s.registerWorkflowInner(ctx, def)
		},
	)
}

// registerWorkflowInner holds the core logic, keeping the
// instrumented wrapper under the 70-line limit.
func (s *Service) registerWorkflowInner(
	ctx context.Context, def dag.WorkflowDef,
) error {
	if s.defKV == nil {
		panic("registerWorkflowInner: defKV must not be nil")
	}
	if def.Name == "" {
		panic("registerWorkflowInner: def.Name must not be empty")
	}
	if err := dag.Validate(def); err != nil {
		return fmt.Errorf("invalid workflow: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.defKV.Put(ctx, def.Name, data)
	return err
}

// RegisterWorkflowWithWarnings is the variant that returns the
// graph-level warnings produced by dag.ValidateRespondReachability
// alongside the persistence outcome. Per ADR-013 PR 3, the REST
// handler surfaces these warnings in the response body so the
// workflow author sees them at registration time, not first
// production hang. Fatal field-level errors (dag.Validate) still
// short-circuit the persist; warnings do NOT.
//
// hasHTTPTrigger is computed by walking the triggers KV for any
// trigger whose WorkflowID matches def.Name and whose HTTP variant
// is non-nil. A registration error during the trigger lookup is
// logged and treated as "no HTTP trigger" — failing the registration
// over a transient list error would be worse than skipping the
// reachability warning.
func (s *Service) RegisterWorkflowWithWarnings(
	ctx context.Context, def dag.WorkflowDef,
) ([]dag.Warning, error) {
	if ctx == nil {
		panic("RegisterWorkflowWithWarnings: ctx must not be nil")
	}
	if def.Name == "" {
		panic("RegisterWorkflowWithWarnings: def.Name must not be empty")
	}
	if err := s.RegisterWorkflow(ctx, def); err != nil {
		return nil, err
	}
	hasHTTP := s.hasHTTPTriggerFor(ctx, def.Name)
	return dag.ValidateRespondReachability(def, hasHTTP), nil
}

// hasHTTPTriggerFor returns true when at least one trigger in the
// triggers KV binds an HTTP variant to workflowName. Errors are
// logged and the function falls through to false so a transient KV
// hiccup never escalates into a failed registration.
func (s *Service) hasHTTPTriggerFor(
	ctx context.Context, workflowName string,
) bool {
	if workflowName == "" {
		panic("hasHTTPTriggerFor: workflowName must not be empty")
	}
	if s.triggerKV == nil {
		return false
	}
	defs, err := s.listTriggersInner(ctx)
	if err != nil {
		if !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn("list triggers for HTTP-trigger check",
				"error", err, "workflow", workflowName)
		}
		return false
	}
	for _, d := range defs {
		if d.WorkflowID != workflowName {
			continue
		}
		if d.HTTP != nil {
			return true
		}
	}
	return false
}

// GetWorkflow retrieves the registered definition for the named
// workflow. Returns a key-not-found error when not registered.
func (s *Service) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if name == "" {
		panic("GetWorkflow: name must not be empty")
	}
	if s.defKV == nil {
		panic("GetWorkflow: defKV must not be nil")
	}
	entry, err := s.defKV.Get(context.Background(), name)
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

// ListWorkflows retrieves all registered workflow definitions from KV.
func (s *Service) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if ctx == nil {
		panic("ListWorkflows: ctx must not be nil")
	}
	if s.defKV == nil {
		panic("ListWorkflows: defKV must not be nil")
	}
	var defs []dag.WorkflowDef
	err := s.observed(ctx, "listWorkflows", nil,
		func(ctx context.Context) error {
			var innerErr error
			defs, innerErr = s.listWorkflowsInner(ctx)
			return innerErr
		},
	)
	return defs, err
}

// listWorkflowsInner holds the KV iteration logic.
func (s *Service) listWorkflowsInner(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if s.defKV == nil {
		panic("listWorkflowsInner: defKV must not be nil")
	}
	if s.js == nil {
		panic("listWorkflowsInner: js must not be nil")
	}
	keys, err := s.defKV.Keys(ctx)
	if err != nil {
		// Empty bucket -- treat as the documented "no workflows
		// registered" case so consumers (console, REST, NATS) get
		// nil slice + nil error and can render empty-state. Mirrors
		// the pattern used by ListTriggers, scheduled.go, and the
		// engine snapshot store.
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []dag.WorkflowDef{}, nil
		}
		return nil, err
	}

	entries, err := natsutil.ParallelGetJS(
		s.defKV, keys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return nil, err
	}

	defs := make([]dag.WorkflowDef, 0, len(entries))
	for _, entry := range entries {
		var def dag.WorkflowDef
		if err := json.Unmarshal(
			entry.Value(), &def,
		); err != nil {
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

// CreateTrigger validates and stores a trigger definition.
func (s *Service) CreateTrigger(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if ctx == nil {
		panic("CreateTrigger: ctx must not be nil")
	}
	if def.ID == "" {
		panic("CreateTrigger: def.ID must not be empty")
	}
	return s.observed(ctx, "createTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", def.ID),
			attribute.String("workflow_id", def.WorkflowID),
		},
		func(ctx context.Context) error {
			return s.createTriggerInner(ctx, def)
		},
	)
}

// createTriggerInner validates and writes the trigger to KV. For HTTP
// triggers it additionally checks for an existing trigger that already
// claims the same (method, path) and refuses with a typed
// RouteConflictError. Self-replace (same trigger ID) is allowed so
// operators can update a route's config without temporary unregister.
func (s *Service) createTriggerInner(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if def.ID == "" {
		panic("createTriggerInner: def.ID must not be empty")
	}
	if def.WorkflowID == "" {
		panic(
			"createTriggerInner: def.WorkflowID must not be empty",
		)
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	if err := trigger.Validate(def); err != nil {
		return fmt.Errorf("invalid trigger: %w", err)
	}
	if err := s.checkHTTPRouteConflict(ctx, def); err != nil {
		return err
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.triggerKV.Put(
		ctx, def.ID, data,
	)
	return err
}

// checkHTTPRouteConflict returns a *trigger.RouteConflictError when
// def is an HTTP trigger whose (method, path) is already claimed by
// a different trigger ID. Non-HTTP triggers are pass-through. Same-ID
// re-registration (idempotent update) is allowed.
func (s *Service) checkHTTPRouteConflict(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if def.HTTP == nil {
		return nil
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	existing, err := s.listTriggersInner(ctx)
	if err != nil {
		// No keys yet is a benign "first trigger" case.
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return fmt.Errorf("list triggers for conflict check: %w", err)
	}
	for _, other := range existing {
		if other.ID == def.ID {
			continue
		}
		if other.HTTP == nil {
			continue
		}
		if other.HTTP.Method != def.HTTP.Method {
			continue
		}
		if other.HTTP.Path != def.HTTP.Path {
			continue
		}
		return &trigger.RouteConflictError{
			Method:          def.HTTP.Method,
			Path:            def.HTTP.Path,
			HolderTriggerID: other.ID,
		}
	}
	return nil
}

// ListTriggers retrieves all trigger definitions from KV.
func (s *Service) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if ctx == nil {
		panic("ListTriggers: ctx must not be nil")
	}
	if s.js == nil {
		panic("ListTriggers: js must not be nil")
	}
	var defs []trigger.TriggerDef
	err := s.observed(ctx, "listTriggers", nil,
		func(ctx context.Context) error {
			var innerErr error
			defs, innerErr = s.listTriggersInner(ctx)
			return innerErr
		},
	)
	return defs, err
}

// listTriggersInner holds the KV iteration logic.
func (s *Service) listTriggersInner(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if s.js == nil {
		panic("listTriggersInner: js must not be nil")
	}
	if s.triggerKV == nil {
		return []trigger.TriggerDef{}, nil
	}
	keys, err := s.triggerKV.Keys(ctx)
	if err != nil {
		return nil, err
	}

	entries, err := natsutil.ParallelGetJS(
		s.triggerKV, keys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return nil, err
	}

	defs := make([]trigger.TriggerDef, 0, len(entries))
	for _, entry := range entries {
		var def trigger.TriggerDef
		if err := json.Unmarshal(
			entry.Value(), &def,
		); err != nil {
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
	return s.observed(ctx, "deleteTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			return s.deleteTriggerInner(ctx, triggerID)
		},
	)
}

// deleteTriggerInner deletes the trigger from KV.
func (s *Service) deleteTriggerInner(
	ctx context.Context, triggerID string,
) error {
	if triggerID == "" {
		panic(
			"deleteTriggerInner: triggerID must not be empty",
		)
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	return s.triggerKV.Delete(
		ctx, triggerID,
	)
}

// SetTriggerEnabled updates the enabled state of a trigger.
func (s *Service) SetTriggerEnabled(
	ctx context.Context, triggerID string, enabled bool,
) error {
	if ctx == nil {
		panic("SetTriggerEnabled: ctx must not be nil")
	}
	if triggerID == "" {
		panic("SetTriggerEnabled: triggerID must not be empty")
	}
	return s.observed(ctx, "setTriggerEnabled",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			return s.setTriggerEnabledInner(
				ctx, triggerID, enabled,
			)
		},
	)
}

// setTriggerEnabledInner reads, updates, and writes the trigger.
func (s *Service) setTriggerEnabledInner(
	ctx context.Context, triggerID string, enabled bool,
) error {
	if triggerID == "" {
		panic(
			"setTriggerEnabledInner: triggerID must not be empty",
		)
	}
	if s.js == nil {
		panic("setTriggerEnabledInner: js must not be nil")
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	entry, err := s.triggerKV.Get(ctx, triggerID)
	if err != nil {
		return fmt.Errorf(
			"trigger %q not found: %w", triggerID, err,
		)
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return fmt.Errorf("unmarshal trigger: %w", err)
	}
	def.Enabled = enabled
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	_, err = s.triggerKV.Put(ctx, triggerID, data)
	return err
}

// ErrTriggerKindNotFireable is returned by FireTrigger when the
// targeted trigger isn't a kind the manual fire-now path supports.
// #352 scopes manual fires to cron + webhook triggers — subject and
// HTTP triggers carry caller-bound input the console has no way to
// synthesize, so a manual fire of them would produce a malformed run.
var ErrTriggerKindNotFireable = errors.New(
	"trigger kind not fireable from manual fire-now path",
)

// ErrTriggerDisabled is returned by FireTrigger when the operator
// targets a trigger whose Enabled bit is false. The operator must
// re-enable it first; firing a disabled trigger would write a fire
// row history that contradicts the trigger's configured state.
var ErrTriggerDisabled = errors.New(
	"trigger is disabled; enable it before firing",
)

// FireTrigger publishes one workflow.started + TriggerFire history
// record for the given trigger. Returns the run ID the workflow
// orchestrator will observe so the operator can deep-link to the
// run in the console (or the CLI can echo it to stdout). #352.
//
// Allowed kinds: cron + webhook. Other kinds return
// ErrTriggerKindNotFireable so the handler can short-circuit to 400
// rather than fire a partial run. Disabled triggers return
// ErrTriggerDisabled.
//
// All transport / dedup logic lives in trigger.Fire — this method
// just resolves the def from KV, validates kind / enabled, and
// delegates. The dedup-msg-id strategy for SourceManual is the
// nanosecond-unique form so consecutive operator clicks each produce
// a distinct run.
func (s *Service) FireTrigger(
	ctx context.Context, triggerID string,
) (string, error) {
	if ctx == nil {
		panic("FireTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("FireTrigger: triggerID must not be empty")
	}
	var runID string
	err := s.observed(ctx, "fireTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			var innerErr error
			runID, innerErr = s.fireTriggerInner(ctx, triggerID)
			return innerErr
		},
	)
	return runID, err
}

// fireTriggerInner is the un-observed core. Split out so the
// observed() wrapper above stays at ≤70 lines under the project rule.
func (s *Service) fireTriggerInner(
	ctx context.Context, triggerID string,
) (string, error) {
	if s.triggerKV == nil {
		return "", fmt.Errorf("triggers KV bucket not available")
	}
	entry, err := s.triggerKV.Get(ctx, triggerID)
	if err != nil {
		return "", fmt.Errorf(
			"trigger %q not found: %w", triggerID, err,
		)
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return "", fmt.Errorf("unmarshal trigger: %w", err)
	}
	if def.Cron == nil && def.Webhook == nil {
		return "", ErrTriggerKindNotFireable
	}
	if !def.Enabled {
		return "", ErrTriggerDisabled
	}
	return trigger.Fire(
		ctx, s.tp, def, trigger.SourceManual, time.Now(),
	)
}

// TriggerUpdates holds optional field overrides for UpdateTrigger.
// Pointer fields distinguish "not provided" from "set to zero value".
type TriggerUpdates struct {
	CronExpr *string
	Timezone *string
	Backfill *bool
	Subject  *string
	Webhook  *string
	Secret   *string
}

// UpdateTrigger reads an existing trigger, applies overrides, validates,
// and writes back. Only non-nil fields in updates are applied.
func (s *Service) UpdateTrigger(
	ctx context.Context, triggerID string, updates TriggerUpdates,
) error {
	if ctx == nil {
		panic("UpdateTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("UpdateTrigger: triggerID must not be empty")
	}
	return s.observed(ctx, "updateTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			return s.updateTriggerInner(
				ctx, triggerID, updates,
			)
		},
	)
}

// updateTriggerInner reads, patches, validates, and writes the trigger.
func (s *Service) updateTriggerInner(
	ctx context.Context, triggerID string, updates TriggerUpdates,
) error {
	if triggerID == "" {
		panic("updateTriggerInner: triggerID must not be empty")
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	entry, err := s.triggerKV.Get(ctx, triggerID)
	if err != nil {
		return fmt.Errorf(
			"trigger %q not found: %w", triggerID, err,
		)
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return fmt.Errorf("unmarshal trigger: %w", err)
	}
	applyTriggerUpdates(&def, updates)
	if err := trigger.Validate(def); err != nil {
		return fmt.Errorf(
			"invalid trigger after update: %w", err,
		)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	_, err = s.triggerKV.Put(ctx, triggerID, data)
	return err
}

// applyTriggerUpdates patches non-nil fields from updates onto def.
func applyTriggerUpdates(
	def *trigger.TriggerDef, updates TriggerUpdates,
) {
	if def == nil {
		panic("applyTriggerUpdates: def must not be nil")
	}
	if def.ID == "" {
		panic("applyTriggerUpdates: def.ID must not be empty")
	}
	if updates.CronExpr != nil && def.Cron != nil {
		def.Cron.Expression = *updates.CronExpr
	}
	if updates.Timezone != nil && def.Cron != nil {
		def.Cron.Timezone = *updates.Timezone
	}
	if updates.Backfill != nil && def.Cron != nil {
		def.Cron.Backfill = *updates.Backfill
	}
	if updates.Subject != nil && def.Subject != nil {
		def.Subject.Subject = *updates.Subject
	}
	if updates.Webhook != nil && def.Webhook != nil {
		def.Webhook.Path = *updates.Webhook
	}
	if updates.Secret != nil && def.Webhook != nil {
		def.Webhook.Secret = *updates.Secret
	}
}

// CountDeadLetters returns the current number of entries on the
// DEAD_LETTERS stream. CLI uses this to surface truncation footers
// without rolling its own NATS plumbing.
func (s *Service) CountDeadLetters(ctx context.Context) (int, error) {
	if ctx == nil {
		panic("CountDeadLetters: ctx must not be nil")
	}
	if s.js == nil {
		panic("CountDeadLetters: js must not be nil")
	}
	stream, err := s.js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return int(info.State.Msgs), nil
}

// ListDeadLetters retrieves up to limit dead letter messages.
// Returns operator-facing views with derived fields (e.g.
// BodyPreserved) so CLI rendering is pure transport.
func (s *Service) ListDeadLetters(
	ctx context.Context, limit int,
) ([]DeadLetterView, error) {
	if ctx == nil {
		panic("ListDeadLetters: ctx must not be nil")
	}
	if limit <= 0 {
		panic("ListDeadLetters: limit must be positive")
	}
	var views []DeadLetterView
	err := s.observed(ctx, "listDeadLetters", nil,
		func(_ context.Context) error {
			var innerErr error
			views, innerErr = s.listDeadLettersInner(limit)
			return innerErr
		},
	)
	return views, err
}

// listDeadLettersInner fetches messages from the DEAD_LETTERS
// stream using a legacy SubscribeSync via the raw connection.
// Returns DeadLetterView so derived fields (BodyPreserved) are
// computed exactly once at the engine boundary.
func (s *Service) listDeadLettersInner(
	limit int,
) ([]DeadLetterView, error) {
	if limit <= 0 {
		panic("listDeadLettersInner: limit must be positive")
	}
	if s.nc == nil {
		panic("listDeadLettersInner: nc must not be nil")
	}
	jsLegacy, err := s.nc.JetStream()
	if err != nil {
		return nil, err
	}
	sub, err := jsLegacy.SubscribeSync("dead.>")
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe() //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	msgs := fetchMessages(sub, limit, deadline)
	views := make([]DeadLetterView, 0, len(msgs))
	for _, msg := range msgs {
		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			continue
		}
		views = append(views,
			newDeadLetterView(parseDLQMessage(msg, meta)))
	}
	return views, nil
}

// parseDLQMessage decodes a DLQ stream message into a DeadLetter,
// supporting both the post-#200 shape (body in msg.Data, metadata
// in structured headers) and the pre-#200 legacy shape (metadata
// JSON in msg.Data, no body preserved). Detection key:
// HeaderDLQRunID is set only for post-#200 entries.
func parseDLQMessage(
	msg *nats.Msg, meta *nats.MsgMetadata,
) DeadLetter {
	if msg == nil {
		panic("parseDLQMessage: msg must not be nil")
	}
	if meta == nil {
		panic("parseDLQMessage: meta must not be nil")
	}
	if msg.Header.Get(engine.HeaderDLQRunID) != "" {
		return parseModernDLQ(msg, meta)
	}
	return parseLegacyDLQ(msg, meta)
}

// parseModernDLQ decodes a post-#200 DLQ entry: body in msg.Data,
// metadata in structured Dagnats-Dlq-* headers, original task
// subject preserved via HeaderDLQTaskSubject.
func parseModernDLQ(
	msg *nats.Msg, meta *nats.MsgMetadata,
) DeadLetter {
	attempts, _ := strconv.Atoi(
		msg.Header.Get(engine.HeaderDLQAttempts),
	)
	deliveryCount, _ := strconv.Atoi(
		msg.Header.Get(engine.HeaderDLQDeliveryCount),
	)
	taskSubject := msg.Header.Get(engine.HeaderDLQTaskSubject)
	stored := DeadLetter{
		Sequence:      meta.Sequence.Stream,
		Subject:       msg.Subject,
		RunID:         msg.Header.Get(engine.HeaderDLQRunID),
		StepID:        msg.Header.Get(engine.HeaderDLQStepID),
		Task:          msg.Header.Get(engine.HeaderDLQTask),
		Error:         msg.Header.Get(engine.HeaderDLQError),
		Timestamp:     meta.Timestamp,
		Body:          msg.Data,
		DeliveryCount: deliveryCount,
		Consumer:      msg.Header.Get(engine.HeaderDLQConsumer),
	}
	if stored.Error == "" {
		stored.Error = msg.Header.Get("Error")
	}
	// Stash attempts (legacy) into headers map for downstream use;
	// also preserve the original task subject so replay knows where
	// to re-publish without re-deriving from (task, runID).
	stored.Headers = nats.Header{}
	if taskSubject != "" {
		stored.Headers[engine.HeaderDLQTaskSubject] = []string{taskSubject}
	}
	if attempts > 0 {
		stored.Headers[engine.HeaderDLQAttempts] = []string{
			strconv.Itoa(attempts),
		}
	}
	return stored
}

// parseLegacyDLQ decodes a pre-#200 DLQ entry: metadata JSON in
// msg.Data, no body preserved. The returned DeadLetter has empty
// Body so newDeadLetterView reports BodyPreserved=false and replay
// returns ErrDLQBodyMissing.
func parseLegacyDLQ(
	msg *nats.Msg, meta *nats.MsgMetadata,
) DeadLetter {
	var raw struct {
		RunID    string `json:"run_id"`
		StepID   string `json:"step_id"`
		Task     string `json:"task"`
		Error    string `json:"error"`
		Attempts int    `json:"attempts"`
	}
	_ = json.Unmarshal(msg.Data, &raw) //nolint:errcheck
	errStr := raw.Error
	if errStr == "" {
		errStr = msg.Header.Get("Error")
	}
	taskName := raw.Task
	if taskName == "" {
		taskName = extractTaskFromSubject(msg.Subject)
	}
	return DeadLetter{
		Sequence:      meta.Sequence.Stream,
		Subject:       msg.Subject,
		RunID:         raw.RunID,
		StepID:        raw.StepID,
		Task:          taskName,
		Error:         errStr,
		Timestamp:     meta.Timestamp,
		DeliveryCount: raw.Attempts,
	}
}

// fetchMessages drains up to limit messages from sub within the
// given total deadline. Returns on first NextMsg error (timeout or
// stream exhaustion). Owns the timeout algebra so callers don't.
//
// The per-message timeout is two-tier:
//
//   - 100ms "warm" window for the first message — covers the
//     consumer-creation roundtrip plus any backlog delivery. On
//     loopback / LAN the first message lands in <10ms; 100ms is a
//     generous ceiling that still cuts page-load TTFB by ~5x vs the
//     original 500ms.
//   - 5ms "tail" window for subsequent messages — once one message
//     arrived the NATS client's local pending queue already holds
//     the rest of the prefix (the server streams the full set on
//     the consumer pull), so 5ms is plenty to drain the buffer
//     and detect end-of-stream.
//
// Previously every fetchMessages call paid 500ms on both the first
// and the tail, which taxed every page that walked a NATS
// subscription synchronously (DLQ list, DLQ detail, run-detail
// event timeline) by ~505ms TTFB even when there were no messages
// to read.
func fetchMessages(
	sub *nats.Subscription, limit int, deadline time.Time,
) []*nats.Msg {
	if sub == nil {
		panic("fetchMessages: sub must not be nil")
	}
	if limit <= 0 {
		panic("fetchMessages: limit must be positive")
	}
	const firstWait = 100 * time.Millisecond
	const tailWait = 5 * time.Millisecond
	msgs := make([]*nats.Msg, 0, limit)
	for i := 0; i < limit; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timeout := firstWait
		if len(msgs) > 0 {
			timeout = tailWait
		}
		if remaining < timeout {
			timeout = remaining
		}
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		msgs = append(msgs, msg)
	}
	return msgs
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
	return s.observed(ctx, "replayDeadLetter",
		[]attribute.KeyValue{
			attribute.Int64("sequence", int64(seq)),
		},
		func(ctx context.Context) error {
			return s.replayDeadLetterInner(ctx, seq)
		},
	)
}

// replayDeadLetterInner fetches the DLQ entry by sequence and
// re-publishes its stored body verbatim onto the original task
// subject. Returns ErrDLQBodyMissing when the entry pre-dates the
// body-preservation schema (no Body field) — operators must recover
// such entries upstream rather than replay.
func (s *Service) replayDeadLetterInner(
	ctx context.Context, seq uint64,
) error {
	if seq == 0 {
		panic("replayDeadLetterInner: seq must be positive")
	}
	if s.js == nil {
		panic("replayDeadLetterInner: js must not be nil")
	}
	views, err := s.listDeadLettersInner(100)
	if err != nil {
		return err
	}
	var target *DeadLetterView
	for i := range views {
		if views[i].Sequence == seq {
			target = &views[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf(
			"dead letter with sequence %d not found", seq,
		)
	}
	if len(target.Body) == 0 {
		return fmt.Errorf("dlq sequence %d: %w",
			seq, ErrDLQBodyMissing)
	}
	taskSubject := ""
	if target.Headers != nil {
		taskSubject = target.Headers.Get(
			engine.HeaderDLQTaskSubject,
		)
	}
	if taskSubject == "" {
		taskSubject = deriveTaskSubject(target.Task)
	}
	msg := &nats.Msg{
		Subject: taskSubject,
		Data:    target.Body,
		Header:  target.Headers,
	}
	if _, err := s.tp.JSPublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("dlq replay: publish: %w", err)
	}
	return nil
}

// DiscardDeadLetter removes the entry at the given stream sequence
// from the DEAD_LETTERS stream permanently. Returns an error when the
// sequence is missing or JetStream rejects the delete. Operators
// trigger this via /console/dlq/<seq>/discard after a typed
// confirmation; CLI may expose it later.
func (s *Service) DiscardDeadLetter(
	ctx context.Context, seq uint64,
) error {
	if ctx == nil {
		panic("DiscardDeadLetter: ctx must not be nil")
	}
	if seq == 0 {
		panic("DiscardDeadLetter: seq must be positive")
	}
	return s.observed(ctx, "discardDeadLetter",
		[]attribute.KeyValue{
			attribute.Int64("sequence", int64(seq)),
		},
		func(ctx context.Context) error {
			stream, err := s.js.Stream(ctx, "DEAD_LETTERS")
			if err != nil {
				return fmt.Errorf("dlq stream: %w", err)
			}
			if err := stream.DeleteMsg(ctx, seq); err != nil {
				return fmt.Errorf("delete dlq seq %d: %w", seq, err)
			}
			return nil
		},
	)
}

// deriveTaskSubject is the legacy-shape fallback when the DLQ
// entry's stored task subject is missing — best-effort recovery for
// entries written before HeaderDLQTaskSubject existed.
func deriveTaskSubject(task string) string {
	if isTaskSubject(task) {
		return task
	}
	return "task." + task
}

// isTaskSubject checks if a subject starts with "task.".
func isTaskSubject(subject string) bool {
	return len(subject) >= 5 && subject[:5] == "task."
}

// DefaultRunsLimit is the row cap applied when callers invoke
// ListRuns without an explicit limit. It preserves pre-#257 behavior
// (the cap used to be hard-coded inside listRunsInner).
const DefaultRunsLimit = 1000

// MaxRunsLimitCeiling is the hard server-side ceiling. ListRunsWithLimit
// clamps any caller-supplied limit at or below this value. Defense
// against accidental OOM from "fetch everything" callers.
const MaxRunsLimitCeiling = 10000

// ListRuns retrieves all workflow runs, optionally filtered by workflow ID.
// Returns runs sorted by CreatedAt descending (newest first).
// Applies DefaultRunsLimit as the row cap; callers that need to raise
// the cap should use ListRunsWithLimit.
func (s *Service) ListRuns(
	ctx context.Context, workflowFilter string,
) ([]dag.WorkflowRun, error) {
	return s.ListRunsWithLimit(ctx, workflowFilter, DefaultRunsLimit)
}

// ListRunsWithLimit is the same as ListRuns but lets callers raise the
// row cap up to MaxRunsLimitCeiling. limit <= 0 is treated as
// DefaultRunsLimit; limit > MaxRunsLimitCeiling is clamped to the
// ceiling (friendlier than a 400 error from a typo).
func (s *Service) ListRunsWithLimit(
	ctx context.Context, workflowFilter string, limit int,
) ([]dag.WorkflowRun, error) {
	if ctx == nil {
		panic("ListRunsWithLimit: ctx must not be nil")
	}
	if s.store == nil {
		panic("ListRunsWithLimit: store must not be nil")
	}
	effective := clampRunsLimit(limit)
	var runs []dag.WorkflowRun
	err := s.observed(ctx, "listRuns", nil,
		func(ctx context.Context) error {
			var innerErr error
			runs, innerErr = s.listRunsInner(
				ctx, workflowFilter, effective,
			)
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

// listRunsInner retrieves up to limit runs from the store, filters, and sorts.
func (s *Service) listRunsInner(
	ctx context.Context, workflowFilter string, limit int,
) ([]dag.WorkflowRun, error) {
	if s.store == nil {
		panic("listRunsInner: store must not be nil")
	}
	if s.js == nil {
		panic("listRunsInner: js must not be nil")
	}
	if limit <= 0 {
		panic("listRunsInner: limit must be positive")
	}
	runs, err := s.store.ListAll(ctx, limit)
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
// checkIdempotency extracts the idempotency key from input, hashes it,
// and checks the KV for an existing run. Returns the existing run ID
// if found, empty string if not, or error on extraction/KV failure.
func (s *Service) checkIdempotency(
	ctx context.Context,
	workflowName string, keyPath string, input []byte,
) (string, error) {
	if workflowName == "" {
		panic("checkIdempotency: workflowName must not be empty")
	}
	if keyPath == "" {
		panic("checkIdempotency: keyPath must not be empty")
	}
	val, err := dag.ExtractDotPath(keyPath, input)
	if err != nil {
		return "", fmt.Errorf("extract key %q: %w", keyPath, err)
	}
	kvKey := idempotencyHash(workflowName, fmt.Sprintf("%v", val))

	entry, err := s.idempotencyKV.Get(
		ctx, kvKey,
	)
	if err == nil {
		return string(entry.Value()), nil
	}
	return "", nil
}

// storeIdempotencyKey stores the idempotency key -> run ID mapping.
// Uses Create for atomicity — if another request raced and won, this
// is a no-op (the winner's mapping stands).
func (s *Service) storeIdempotencyKey(
	ctx context.Context,
	workflowName string, keyPath string,
	input []byte, runID string,
) {
	val, err := dag.ExtractDotPath(keyPath, input)
	if err != nil {
		return // extraction failed — skip silently
	}
	kvKey := idempotencyHash(workflowName, fmt.Sprintf("%v", val))
	// Create fails if key exists (race loser) — that's fine.
	_, _ = s.idempotencyKV.Create(
		ctx, kvKey, []byte(runID),
	)
}

// idempotencyHash produces a deterministic KV key from workflow name
// and extracted key value using SHA-256.
func idempotencyHash(workflowName string, keyValue string) string {
	h := sha256.Sum256(
		[]byte(workflowName + "." + keyValue),
	)
	return hex.EncodeToString(h[:])
}

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

// ListWorkers returns all currently registered workers from the
// directory. Returns an empty slice when no workers are registered
// or when the workers KV bucket does not exist.
func (s *Service) ListWorkers(
	ctx context.Context,
) ([]worker.WorkerRegistration, error) {
	if ctx == nil {
		panic("ListWorkers: ctx must not be nil")
	}
	if s.js == nil {
		panic("ListWorkers: js must not be nil")
	}
	var workers []worker.WorkerRegistration
	err := s.observed(ctx, "listWorkers", nil,
		func(ctx context.Context) error {
			var innerErr error
			workers, innerErr = s.listWorkersInner(ctx)
			return innerErr
		},
	)
	return workers, err
}

// listWorkersInner attempts to list workers from the directory.
// Returns empty slice when the workers bucket does not exist --
// normal condition when no workers have registered yet.
func (s *Service) listWorkersInner(
	ctx context.Context,
) ([]worker.WorkerRegistration, error) {
	if s.js == nil {
		panic("listWorkersInner: js must not be nil")
	}
	kv, err := s.js.KeyValue(ctx, "workers")
	if err != nil {
		return []worker.WorkerRegistration{}, nil
	}
	if kv == nil {
		panic(
			"listWorkersInner: kv must not be nil when err is nil",
		)
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return []worker.WorkerRegistration{}, nil
	}
	if keys == nil {
		panic(
			"listWorkersInner: keys must not be nil when err is nil",
		)
	}
	workers := make(
		[]worker.WorkerRegistration, 0, len(keys),
	)
	cutoff := time.Now().Add(-worker.MaxWorkerStaleness)
	for _, key := range keys {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		if worker.MaxWorkerStaleness > 0 &&
			entry.Created().Before(cutoff) {
			continue
		}
		var reg worker.WorkerRegistration
		if err := json.Unmarshal(
			entry.Value(), &reg,
		); err != nil {
			continue
		}
		workers = append(workers, reg)
	}
	return workers, nil
}

// TriggerFireEntry is a trigger fire record enriched with
// run status information for CLI display.
type TriggerFireEntry struct {
	trigger.TriggerFire
	Status   string        `json:"status,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
}

// ListTriggerFires retrieves fire history for the given
// trigger. Creates an ephemeral consumer on TRIGGER_HISTORY
// and fetches up to limit messages.
func (s *Service) ListTriggerFires(
	ctx context.Context, triggerID string, limit int,
) ([]TriggerFireEntry, error) {
	if ctx == nil {
		panic("ListTriggerFires: ctx must not be nil")
	}
	if triggerID == "" {
		panic(
			"ListTriggerFires: triggerID must not be empty",
		)
	}
	var fires []TriggerFireEntry
	err := s.observed(ctx, "listTriggerFires",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(_ context.Context) error {
			var innerErr error
			fires, innerErr = s.listTriggerFiresInner(
				triggerID, limit,
			)
			return innerErr
		},
	)
	return fires, err
}

// listTriggerFiresInner fetches trigger fire records from the
// TRIGGER_HISTORY stream via an ephemeral consumer.
func (s *Service) listTriggerFiresInner(
	triggerID string, limit int,
) ([]TriggerFireEntry, error) {
	if triggerID == "" {
		panic(
			"listTriggerFiresInner: triggerID must not be empty",
		)
	}
	if s.js == nil {
		panic("listTriggerFiresInner: js must not be nil")
	}
	ctx := context.Background()
	subject := "trigger.fire." + triggerID
	cons, err := s.js.CreateOrUpdateConsumer(
		ctx, "TRIGGER_HISTORY",
		jetstream.ConsumerConfig{
			FilterSubject:     subject,
			DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
			AckPolicy:         jetstream.AckNonePolicy,
			InactiveThreshold: 10 * time.Second,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create consumer: %w", err,
		)
	}
	return s.fetchFireEntries(cons, limit)
}

// fetchFireEntries reads messages from the consumer and
// unmarshals them into TriggerFireEntry records. Enriches
// each record with run status when a RunID is present.
func (s *Service) fetchFireEntries(
	cons jetstream.Consumer, limit int,
) ([]TriggerFireEntry, error) {
	if cons == nil {
		panic("fetchFireEntries: cons must not be nil")
	}
	if limit <= 0 {
		panic("fetchFireEntries: limit must be positive")
	}
	ctx := context.Background()
	batch, err := cons.Fetch(limit,
		jetstream.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	entries := make([]TriggerFireEntry, 0, limit)
	for msg := range batch.Messages() {
		var fire trigger.TriggerFire
		if json.Unmarshal(msg.Data(), &fire) != nil {
			continue
		}
		entry := TriggerFireEntry{TriggerFire: fire}
		if fire.RunID != "" {
			entry.Status, entry.Duration =
				s.enrichFireStatus(ctx, fire.RunID)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// enrichFireStatus loads run status and duration for a fire
// record. Returns empty values on error (best-effort).
func (s *Service) enrichFireStatus(
	ctx context.Context, runID string,
) (string, time.Duration) {
	if runID == "" {
		panic("enrichFireStatus: runID must not be empty")
	}
	if ctx == nil {
		panic("enrichFireStatus: ctx must not be nil")
	}
	run, err := s.store.Load(ctx, runID)
	if err != nil {
		return "", 0
	}
	var dur time.Duration
	if run.Status != dag.RunStatusPending &&
		run.Status != dag.RunStatusRunning {
		dur = time.Since(run.CreatedAt)
	}
	return run.Status.String(), dur
}
