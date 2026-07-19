package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// pollRequest is the JSON body for POST /v1/tasks/poll.
type pollRequest struct {
	TaskTypes []string `json:"task_types"`
	MaxTasks  int      `json:"max_tasks"`
	TimeoutMs int64    `json:"timeout_ms"`
}

// pollResponse is a single task returned from a poll.
type pollResponse struct {
	TaskID    string          `json:"task_id"`
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id"`
	Iteration int             `json:"iteration,omitempty"`
	Attempt   int             `json:"attempt,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	// W3C names, deliberately un-underscored: these are the wire keys
	// from the trace-context spec, unlike protocol.Event's persisted
	// trace_parent/trace_state. Workers feed them straight into a
	// standard propagator.
	TraceParent string `json:"traceparent,omitempty"`
	TraceState  string `json:"tracestate,omitempty"`
}

// pollTimeoutMaxMs caps the maximum long-poll timeout at 60 seconds.
const pollTimeoutMaxMs = 60_000

// handlePoll long-polls for tasks from the TASK_QUEUES stream.
// Returns a JSON array of task payloads, or an empty array on
// timeout.
func (b *Bridge) handlePoll(
	w http.ResponseWriter, r *http.Request,
) {
	if b.js == nil {
		panic("handlePoll: js must not be nil")
	}
	if b.ackMap == nil {
		panic("handlePoll: ackMap must not be nil")
	}
	// No request-level span: a poll that finds nothing is waiting, not
	// work, and one span per poll per worker swamped span volume at
	// idle (#531). The per-task bridge.dispatch span covers the work;
	// these instruments cover the wait.
	ctx := r.Context()

	start := time.Now()
	req, err := parsePollRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tasks, err := b.fetchTasks(ctx, req)
	if err != nil {
		// Loud by construction: a consumer the bridge cannot obtain is
		// a topology fault, not "no work". Reporting it as an empty
		// array is what hid #532 for the whole life of this endpoint.
		slog.WarnContext(ctx, "poll failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	elapsed := time.Since(start).Milliseconds()
	b.requestCount.Add(ctx, 1, routePoll)
	b.requestDuration.Record(ctx, float64(elapsed), routePoll)
	slog.InfoContext(ctx, "poll completed",
		"task_count", len(tasks),
		"elapsed_ms", elapsed,
	)

	writePollResponse(w, tasks)
}

// parsePollRequest validates the poll JSON body.
func parsePollRequest(r *http.Request) (pollRequest, error) {
	if r == nil {
		panic("parsePollRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("parsePollRequest: r.Body must not be nil")
	}
	var req pollRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON: %w", err)
	}
	if len(req.TaskTypes) == 0 {
		return req, fmt.Errorf("task_types is required")
	}
	for _, taskType := range req.TaskTypes {
		if err := validateTaskType(taskType); err != nil {
			return req, err
		}
	}
	if req.MaxTasks <= 0 {
		req.MaxTasks = 1
	}
	if req.TimeoutMs <= 0 {
		req.TimeoutMs = 5000
	}
	if req.TimeoutMs > pollTimeoutMaxMs {
		req.TimeoutMs = pollTimeoutMaxMs
	}
	return req, nil
}

// fetchTasks pulls messages from NATS for each task type.
// Stores each fetched message in the ackMap so resolve can
// ack/nak it later.
func (b *Bridge) fetchTasks(
	ctx context.Context, req pollRequest,
) ([]pollResponse, error) {
	if ctx == nil {
		panic("fetchTasks: ctx must not be nil")
	}
	if len(req.TaskTypes) == 0 {
		panic("fetchTasks: task_types must not be empty")
	}
	if req.MaxTasks <= 0 {
		panic("fetchTasks: max_tasks must be positive")
	}
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	tasks := make([]pollResponse, 0, req.MaxTasks)
	remaining := req.MaxTasks
	var firstErr error

	for _, taskType := range req.TaskTypes {
		if remaining <= 0 {
			break
		}
		fetched, err := b.fetchForType(ctx, taskType, remaining, timeout)
		// Keep what was fetched even when the type errored: by this
		// point each message has had step.started published and been
		// parked in the ackMap, and the ackMap has no reaper. Dropping
		// them would strand them unacked until AckWait expires while the
		// run displays a started attempt nobody is running.
		tasks = append(tasks, fetched...)
		remaining -= len(fetched)
		if err != nil {
			// Degrade per type, not per poll: one unusable task type
			// must not starve its healthy siblings in the same request.
			slog.WarnContext(ctx, "task type unavailable this poll",
				"task_type", taskType, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	// Only a poll that produced nothing AND faulted is an error. A
	// partial set is real work and must reach the caller; reporting a
	// fault as an empty array is what hid #532.
	if len(tasks) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return tasks, nil
}

// fetchForType fetches up to count messages for one task type off the
// shared per-type durable consumer, storing each in the ackMap.
//
// TASK_QUEUES is a work-queue stream, so JetStream admits exactly one
// consumer per overlapping filter subject. The bridge therefore shares
// the native worker's durable rather than owning its own: look up
// first, create only when absent. Lookup-before-create (not
// CreateOrUpdateConsumer) is load-bearing — an update would overwrite
// a native worker's WithAckWait override with our default.
func (b *Bridge) fetchForType(
	ctx context.Context,
	taskType string, count int, timeout time.Duration,
) ([]pollResponse, error) {
	if ctx == nil {
		panic("fetchForType: ctx must not be nil")
	}
	if taskType == "" {
		panic("fetchForType: taskType must not be empty")
	}
	if count <= 0 {
		panic("fetchForType: count must be positive")
	}
	cons, err := b.taskConsumer(ctx, taskType)
	if err != nil {
		return nil, err
	}
	fetchResult, err := cons.Fetch(
		count, jetstream.FetchMaxWait(timeout),
	)
	if err != nil {
		slog.WarnContext(ctx, "fetch tasks failed",
			"task_type", taskType, "error", err)
		return nil, fmt.Errorf("fetch %q tasks: %w", taskType, err)
	}
	tasks := make([]pollResponse, 0, count)
	for msg := range fetchResult.Messages() {
		resp, ok := b.processPolledMsg(ctx, msg)
		if ok {
			tasks = append(tasks, resp)
		}
	}
	// jetstream surfaces mid-fetch faults (connection loss, missed
	// heartbeat) here rather than from Fetch, and the message channel
	// simply closes early. Without this check a truncated fetch is
	// indistinguishable from a short successful poll — the same silent
	// failure shape #532 removed. Tasks already dispatched above are
	// returned alongside the error; the caller keeps them.
	if err := fetchResult.Error(); err != nil {
		slog.WarnContext(ctx, "fetch tasks truncated",
			"task_type", taskType, "error", err)
		return tasks, fmt.Errorf("fetch %q tasks: %w", taskType, err)
	}
	return tasks, nil
}

// jsErrCodeWorkQueueConsumerNotUnique is the server's rejection of a
// second consumer whose filter overlaps an existing one on a
// work-queue stream ("filtered consumer not unique on workqueue
// stream"). nats.go exports no constant for it.
const jsErrCodeWorkQueueConsumerNotUnique jetstream.ErrorCode = 10100

// taskConsumer returns the durable consumer shared by every poller of
// taskType, creating it with the native worker's canonical config when
// it does not yet exist.
func (b *Bridge) taskConsumer(
	ctx context.Context, taskType string,
) (jetstream.Consumer, error) {
	if ctx == nil {
		panic("taskConsumer: ctx must not be nil")
	}
	if taskType == "" {
		panic("taskConsumer: taskType must not be empty")
	}
	name := consumername.NameFor(taskType, "")
	filter := consumername.FilterFor(taskType, "")
	cons, err := b.adoptConsumer(ctx, name, filter)
	if err == nil {
		return cons, nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, err
	}
	cons, err = b.js.CreateConsumer(
		ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
			Durable:       name,
			Name:          name,
			FilterSubject: filter,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			// Match the native worker exactly (worker/worker.go): a
			// bridge worker inheriting the server's 30s AckWait default
			// had its task redelivered mid-flight and executed twice.
			AckWait:    consumername.DefaultAckWait,
			MaxDeliver: -1,
		},
	)
	if err == nil {
		return cons, nil
	}
	// TOCTOU: a native worker can create the durable between our lookup
	// and our create. JetStream answers ErrConsumerExists only when the
	// existing config DIFFERS — e.g. a worker carrying a WithAckWait
	// override — so the common race lands here, not on the happy path.
	// Adopting is what the lookup above wanted; the filter check still
	// applies, so we never inherit a consumer serving other subjects.
	if errors.Is(err, jetstream.ErrConsumerExists) {
		cons, adoptErr := b.adoptConsumer(ctx, name, filter)
		if adoptErr != nil {
			return nil, fmt.Errorf(
				"adopt consumer %q after concurrent create: %w",
				name, adoptErr,
			)
		}
		return cons, nil
	}
	return nil, consumerCreateError(ctx, name, filter, err)
}

// adoptConsumer returns the existing durable named name, but only after
// confirming it actually serves filter.
//
// Adopting by name alone is unsafe: consumername.Sanitize collapses '.'
// to '-', so "send.email" and "send-email" both name
// "workers-send-email" while their filters stay distinct. A bridge that
// trusted the name would serve one type's tasks to the other type's
// poller. Reports ErrConsumerNotFound unwrapped so the caller can branch
// on it and create.
func (b *Bridge) adoptConsumer(
	ctx context.Context, name, filter string,
) (jetstream.Consumer, error) {
	if ctx == nil {
		panic("adoptConsumer: ctx must not be nil")
	}
	if name == "" {
		panic("adoptConsumer: name must not be empty")
	}
	if filter == "" {
		panic("adoptConsumer: filter must not be empty")
	}
	cons, err := b.js.Consumer(ctx, "TASK_QUEUES", name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return nil, err
		}
		slog.WarnContext(ctx, "task consumer lookup failed",
			"consumer", name, "error", err)
		return nil, fmt.Errorf("look up consumer %q: %w", name, err)
	}
	// CachedInfo: js.Consumer already round-tripped for this info.
	info := cons.CachedInfo()
	if info == nil {
		return nil, fmt.Errorf(
			"consumer %q returned no cached config", name,
		)
	}
	if info.Config.FilterSubject != filter {
		return nil, consumerFilterMismatchError(
			ctx, name, filter, info.Config.FilterSubject,
		)
	}
	return cons, nil
}

// consumerFilterMismatchError reports a durable whose name matches ours
// but whose filter does not. Loud by construction, same as the
// work-queue uniqueness rejection: silently serving the other type's
// tasks is the failure this replaces, and only an operator can resolve
// it by renaming one of the two task types.
func consumerFilterMismatchError(
	ctx context.Context, name, want, got string,
) error {
	if name == "" {
		panic("consumerFilterMismatchError: name must not be empty")
	}
	if want == got {
		panic("consumerFilterMismatchError: filters must differ")
	}
	slog.WarnContext(ctx, "task consumer filter mismatch",
		"consumer", name, "want_filter", want, "got_filter", got)
	return fmt.Errorf(
		"consumer %q already serves filter %s but this poll needs %s: "+
			"two task types sanitize to the same durable name "+
			"(dots collapse to hyphens) — rename one of them",
		name, got, want,
	)
}

// consumerCreateError turns a consumer-create failure into a message
// an operator can act on. The work-queue uniqueness rejection gets
// named explicitly: it means someone else already holds an
// overlapping filter — in practice a grouped worker durable, which
// the bridge's ungrouped poll cannot share.
func consumerCreateError(
	ctx context.Context, name, filter string, cause error,
) error {
	if name == "" {
		panic("consumerCreateError: name must not be empty")
	}
	if cause == nil {
		panic("consumerCreateError: cause must not be nil")
	}
	slog.WarnContext(ctx, "task consumer create failed",
		"consumer", name, "filter", filter, "error", cause)
	var apiErr *jetstream.APIError
	if errors.As(cause, &apiErr) &&
		apiErr.ErrorCode == jsErrCodeWorkQueueConsumerNotUnique {
		return fmt.Errorf(
			"cannot create consumer %q on filter %s: another consumer "+
				"already covers an overlapping filter on the "+
				"TASK_QUEUES work-queue stream. Grouped task types are "+
				"not pollable over the bridge; see "+
				"/docs/workers/http-bridge#limitations: %w",
			name, filter, cause,
		)
	}
	return fmt.Errorf(
		"create consumer %q on filter %s: %w", name, filter, cause,
	)
}

// taskTypeLenMax bounds a polled task type. Types name durable
// consumers and subject tokens; anything longer is a malformed client.
const taskTypeLenMax = 128

// validateTaskType rejects task types that cannot safely become a
// subject token or a durable consumer name. The durable created for a
// type never self-deletes, so a typo would otherwise leave a permanent
// consumer behind — validation at the door is the cheap fix.
func validateTaskType(taskType string) error {
	if taskType == "" {
		return fmt.Errorf("task_types entries must not be empty")
	}
	if len(taskType) > taskTypeLenMax {
		return fmt.Errorf(
			"task type exceeds %d bytes", taskTypeLenMax,
		)
	}
	if taskType[0] == '.' || taskType[len(taskType)-1] == '.' {
		return fmt.Errorf(
			"invalid task type %q: must not start or end with '.'",
			taskType,
		)
	}
	if strings.Contains(taskType, "..") {
		return fmt.Errorf(
			"invalid task type %q: empty subject token", taskType,
		)
	}
	for i := 0; i < len(taskType); i++ {
		if !isTaskTypeByte(taskType[i]) {
			return fmt.Errorf(
				"invalid task type %q: illegal character %q",
				taskType, string(taskType[i]),
			)
		}
	}
	return nil
}

// isTaskTypeByte reports whether c may appear in a task type.
// Deliberately narrower than NATS subject rules: no wildcards, no
// whitespace, so the type maps 1:1 onto one subject path.
func isTaskTypeByte(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z':
		return true
	case c >= '0' && c <= '9':
		return true
	case c == '-', c == '_', c == '.':
		return true
	}
	return false
}

// taskAttemptCountMax bounds the attempt number accepted from a
// polled task message. The engine caps retries via RetryPolicy
// MaxAttempts and NATS redeliveries via consumer limits; an attempt
// beyond this bound means a corrupted payload, and rejecting it
// beats minting unbounded per-attempt msg-ids.
const taskAttemptCountMax = 100_000

// processPolledMsg unmarshals a NATS message into a poll response,
// publishes step.started for the attempt, and stores the message in
// the ackMap for later resolution.
//
// step.started must land before the task is handed to the HTTP
// worker: the engine's handleStepStarted max()es AttemptNumber into
// run.Steps[id].Attempts, which scheduleRetryBackoff uses to build
// per-attempt SLEEP_TIMERS msg-ids. Without it, bridge-executed
// steps pin Attempts at 1 and retry #2's timer is JetStream-deduped
// — the run hangs and never dead-letters (issue #381). On publish
// failure the message is NAKed, not handed out, so the engine never
// sees a resolve for an attempt it never saw start.
func (b *Bridge) processPolledMsg(
	ctx context.Context, msg jetstream.Msg,
) (pollResponse, bool) {
	if msg == nil {
		panic("processPolledMsg: msg must not be nil")
	}
	if b.ackMap == nil {
		panic("processPolledMsg: ackMap must not be nil")
	}
	payload, ok := decodePolledTask(ctx, msg)
	if !ok {
		return pollResponse{}, false
	}
	// Extract, never root: the engine injects its enqueueTask trace
	// context onto the task message (internal/engine/task_publish.go),
	// so dispatch joins that trace. A fresh root here would orphan the
	// step lifecycle exactly as #527/#528 fixed.
	//
	// Graft the remote span context onto ctx rather than using the
	// extracted context directly: extraction roots on Background
	// (observe/propagation.go), which would discard the request's
	// deadline and cancellation — the only bound on the step.started
	// publish below.
	remote := trace.SpanContextFromContext(
		observe.ExtractTraceContext(msg, nil),
	)
	dispatchCtx, span := b.tracer.Start(
		trace.ContextWithRemoteSpanContext(ctx, remote),
		"bridge.dispatch",
	)
	defer span.End()
	span.SetAttributes(
		attribute.String("run_id", payload.RunID),
		attribute.String("step_id", payload.StepID),
	)
	attemptNumber, err := taskAttemptNumber(msg, payload.Attempt)
	if err != nil {
		nakPolledMsg(
			dispatchCtx, msg, "derive attempt number", err, span,
		)
		return pollResponse{}, false
	}
	span.SetAttributes(attribute.Int("attempt", attemptNumber))
	err = b.publishStepStarted(
		dispatchCtx, payload.RunID, payload.StepID, attemptNumber,
	)
	if err != nil {
		nakPolledMsg(
			dispatchCtx, msg, "publish step.started", err, span,
		)
		return pollResponse{}, false
	}
	taskID := payload.RunID + "." + payload.StepID
	b.ackMap.Store(taskID, msg)
	traceHdr := dispatchTraceHeader(dispatchCtx)
	resp := pollResponse{
		TaskID:      taskID,
		RunID:       payload.RunID,
		StepID:      payload.StepID,
		Iteration:   payload.Iteration,
		Attempt:     attemptNumber,
		Input:       payload.Input,
		TraceParent: traceHdr.Get("traceparent"),
		TraceState:  traceHdr.Get("tracestate"),
	}
	return resp, true
}

// dispatchTraceHeader renders ctx's trace context as W3C headers so the
// poll response can hand the worker a parent for its execution span
// (issue #534).
//
// The bridge.dispatch span ENDS when processPolledMsg returns, well
// before the worker starts that child. That is normal for a messaging
// producer span — the link is the trace/span ID pair on the wire, not an
// open span — so do not "fix" it by holding the span open across the
// worker's whole execution.
//
// Returns an empty header when ctx carries no valid span context; the
// omitempty tags then drop the fields entirely rather than emitting an
// empty traceparent.
func dispatchTraceHeader(ctx context.Context) nats.Header {
	if ctx == nil {
		panic("dispatchTraceHeader: ctx must not be nil")
	}
	hdr := nats.Header{}
	observe.InjectTraceContextHeader(ctx, hdr)
	// tracestate is meaningless without a traceparent, and a worker
	// extracting the pair would silently drop it. Not an assertion: the
	// global propagator is pluggable, so this is third-party runtime
	// output rather than a local invariant, and panicking here would
	// take down the poll handler after step.started was already
	// published. Drop the orphan instead.
	if hdr.Get("traceparent") == "" {
		hdr.Del("tracestate")
	}
	return hdr
}

// decodePolledTask unmarshals and validates a polled task message.
// A body that cannot be decoded or lacks identity is acked, not naked:
// redelivery cannot repair it. Reports false when the message was
// consumed and must not be dispatched.
func decodePolledTask(
	ctx context.Context, msg jetstream.Msg,
) (protocol.TaskPayload, bool) {
	if ctx == nil {
		panic("decodePolledTask: ctx must not be nil")
	}
	if msg == nil {
		panic("decodePolledTask: msg must not be nil")
	}
	var payload protocol.TaskPayload
	err := json.Unmarshal(msg.Data(), &payload)
	if err == nil && (payload.RunID == "" || payload.StepID == "") {
		err = fmt.Errorf("run_id and step_id are required")
	}
	if err != nil {
		slog.WarnContext(ctx, "malformed task consumed",
			"error", err)
		if ackErr := msg.Ack(); ackErr != nil {
			slog.WarnContext(ctx, "ack malformed task failed",
				"error", ackErr)
		}
		return protocol.TaskPayload{}, false
	}
	return payload, true
}

// taskAttemptNumber derives the 1-based attempt number for a polled
// task message. Preference order mirrors the native worker's
// publishStarted (worker/context.go): payload.Attempt when set — the
// engine's SLEEP_TIMERS re-publish carries it, and the fresh message's
// NumDelivered=1 would lose the count — else NATS NumDelivered, which
// is correct for in-place redelivery of the original dispatch.
func taskAttemptNumber(
	msg jetstream.Msg, payloadAttempt int,
) (int, error) {
	if msg == nil {
		panic("taskAttemptNumber: msg must not be nil")
	}
	if payloadAttempt < 0 {
		// The engine never writes a negative attempt — corrupt data.
		panic("taskAttemptNumber: payloadAttempt must not be negative")
	}
	attemptNumber := payloadAttempt
	if attemptNumber == 0 {
		meta, err := msg.Metadata()
		if err != nil {
			return 0, fmt.Errorf("read msg metadata: %w", err)
		}
		attemptNumber = int(meta.NumDelivered)
	}
	if attemptNumber < 1 || attemptNumber > taskAttemptCountMax {
		return 0, fmt.Errorf(
			"attempt %d outside [1, %d]",
			attemptNumber, taskAttemptCountMax,
		)
	}
	return attemptNumber, nil
}

// publishStepStarted publishes step.started carrying AttemptNumber —
// the bridge-side mirror of the native worker's publishStarted. The
// AttemptNumber rides into Event.NATSMsgID so each attempt's
// lifecycle events stay distinct under JetStream dedup.
func (b *Bridge) publishStepStarted(
	ctx context.Context,
	runID, stepID string,
	attemptNumber int,
) error {
	if ctx == nil {
		panic("publishStepStarted: ctx must not be nil")
	}
	if attemptNumber < 1 {
		panic("publishStepStarted: attemptNumber must be >= 1")
	}
	if attemptNumber > taskAttemptCountMax {
		panic(
			"publishStepStarted: attemptNumber exceeds taskAttemptCountMax",
		)
	}
	// NewStepEvent asserts non-empty runID / stepID.
	evt := protocol.NewStepEvent(
		protocol.EventStepStarted, runID, stepID, nil,
	)
	evt.AttemptNumber = attemptNumber
	return b.publishEvent(ctx, evt)
}

// nakPolledTaskRetryDelay paces redelivery of a task the bridge
// could not hand out. 5s matches the engine's consumer-error NAK
// precedent (orchestrator / api timer paths).
const nakPolledTaskRetryDelay = 5 * time.Second

// nakPolledMsg returns a message to the queue when the bridge could
// not safely hand it to a poller. NAK (not ack): the attempt must
// redeliver rather than be silently consumed. Delayed: a bare NAK
// under a persistent history-publish failure would tight-loop
// redeliveries and inflate NumDelivered-derived attempt counts.
//
// span is the caller's bridge.dispatch span: recording here keeps the
// failure on the span that owns the dispatch without pushing a second
// error path back up into processPolledMsg.
func nakPolledMsg(
	ctx context.Context,
	msg jetstream.Msg,
	cause string,
	causeErr error,
	span trace.Span,
) {
	if msg == nil {
		panic("nakPolledMsg: msg must not be nil")
	}
	if cause == "" {
		panic("nakPolledMsg: cause must not be empty")
	}
	if span == nil {
		panic("nakPolledMsg: span must not be nil")
	}
	span.RecordError(causeErr)
	span.SetStatus(codes.Error, cause)
	slog.WarnContext(ctx, "polled task returned to queue",
		"cause", cause,
		"error", causeErr,
	)
	if err := msg.NakWithDelay(nakPolledTaskRetryDelay); err != nil {
		slog.WarnContext(ctx, "nak polled task failed",
			"cause", cause,
			"error", err,
		)
	}
}

// writePollResponse encodes the tasks as a JSON array.
func writePollResponse(
	w http.ResponseWriter, tasks []pollResponse,
) {
	if w == nil {
		panic("writePollResponse: w must not be nil")
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tasks); err != nil {
		http.Error(
			w, "encode failed", http.StatusInternalServerError,
		)
	}
}
