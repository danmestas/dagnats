package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Sentinel errors for missing KV buckets. Exported so callers
// can check with errors.Is and get remediation guidance.
var errCheckpointKVNotConfigured = errors.New(
	"checkpoint KV not configured" +
		" — ensure natsutil.SetupAll() has been called" +
		" or start the server with 'dagnats serve'",
)

var errSignalKVNotConfigured = errors.New(
	"signal KV not configured" +
		" — ensure natsutil.SetupAll() has been called" +
		" or start the server with 'dagnats serve'",
)

type taskContext struct {
	nc *nats.Conn
	js jetstream.JetStream
	// tp wraps nc + js for trace-context-injecting publish (#334).
	// Built once in newTaskContext from the same nc/js pair so
	// every step lifecycle event (started, completed, failed,
	// continue) carries W3C trace context automatically.
	tp         *natsutil.TracingPublisher
	publishMsg publishMsgFunc // injected by Worker for publishStarted
	tracer     trace.Tracer
	runID      string
	stepID     string
	iteration  int
	attempt    int
	// attemptNumber is the resolved 1-based AttemptNumber — set by
	// publishStarted before the handler runs (see resolveAttemptNumber's
	// doc comment for why this must match step.started/step.failed's
	// AttemptNumber exactly). Zero until publishStarted runs;
	// effectiveAttempt() falls back to resolving it fresh for the
	// handful of tests that construct a taskContext directly.
	attemptNumber int
	input         []byte
	metadata      map[string]string
	ctx           context.Context
	span          trace.Span
	msg           jetstream.Msg
	checkpointKV  jetstream.KeyValue
	signalKV      jetstream.KeyValue
	paused        bool   // set by Pause() to prevent double-ack
	workerID      string // included in completion events for sticky routing
	// controlPlane is the per-step runtime control-plane handle, set by
	// startTaskSpan only for gated steps. nil by default — deny-by-default
	// is the zero value, so an ungated step can never reach it.
	controlPlane ControlPlane

	// logLane buffers and publishes LogOut()/LogErr() writes to the
	// BUILD_LOGS hot lane (#624). Lazily created by ensureLogLane on
	// first Write so a handler that never logs never pays for the
	// background ticker goroutine.
	logLane          *logLane
	logChunkFailures metric.Int64Counter

	// Cancellation (#802), set by Worker.bindCancel. runCancelled reads
	// the run state; releaseCancel deregisters and ends ctx. Both are
	// nil for a taskContext built directly in a test.
	runCancelled  func() bool
	releaseCancel func()

	// dispatchNonce is the engine's per-dispatch token from the task
	// payload; every lifecycle event echoes it with iteration and
	// attempt so the engine can fence off a stale worker (#801).
	dispatchNonce string
	// loopState is the state the engine committed with the step's last
	// accepted continue and delivered in the payload (#801).
	loopState []byte
	// loopStateMax is the engine's cap on the state a Continue may
	// carry, from the payload; the protocol default when absent.
	loopStateMax int
	// legacyEngine is set when the payload carried no state cap: the
	// engine predates #801 and ignores the state a continue carries, so
	// Checkpoint also writes the checkpoints bucket as it used to and
	// Continue leaves that entry alone, so a rollout that upgrades
	// workers before engines loses no state.
	legacyEngine bool
	// pendingState buffers Checkpoint until Continue publishes it in
	// the continue event (#801): checkpoint and continue are one write.
	// pendingStateSet distinguishes Checkpoint(nil) from no checkpoint.
	pendingState    []byte
	pendingStateSet bool
	// kvCheckpointSeen records that LoadCheckpoint returned a value from
	// the checkpoints bucket (a pause marker, or a checkpoint from a
	// worker older than #801), which Continue then deletes so it cannot
	// shadow the committed state on the next iteration.
	kvCheckpointSeen bool
}

// newTaskContext constructs a taskContext from a dispatched
// TaskPayload. The ctx and span carry trace context from the
// parent executeTask span so child spans link correctly.
func newTaskContext(
	nc *nats.Conn,
	tracer trace.Tracer,
	js jetstream.JetStream,
	payload protocol.TaskPayload,
	ctx context.Context,
	span trace.Span,
	msg jetstream.Msg,
	checkpointKV jetstream.KeyValue,
	signalKV jetstream.KeyValue,
) *taskContext {
	if tracer == nil {
		panic("newTaskContext: tracer must not be nil")
	}
	if ctx == nil {
		panic("newTaskContext: ctx must not be nil")
	}
	// Build tp from whichever of nc/js is non-nil. Tests that pass
	// only nc (PutStream-only) get a core-only wrapper; tests that
	// pass only js (JetStream-only) get a JS-only wrapper.
	// Production callers always pass both.
	var tp *natsutil.TracingPublisher
	switch {
	case nc != nil && js != nil:
		tp = natsutil.NewTracingPublisher(nc, js)
	case nc != nil:
		tp = natsutil.NewTracingPublisherCoreOnly(nc)
	case js != nil:
		tp = natsutil.NewTracingPublisherJSOnly(js)
	}
	loopStateMax := payload.StateMaxBytes
	if loopStateMax <= 0 {
		loopStateMax = protocol.LoopStateMaxBytesDefault
	}
	return &taskContext{
		nc:            nc,
		js:            js,
		tp:            tp,
		tracer:        tracer,
		runID:         payload.RunID,
		stepID:        payload.StepID,
		iteration:     payload.Iteration,
		attempt:       payload.Attempt,
		input:         payload.Input,
		metadata:      payload.Metadata,
		ctx:           ctx,
		span:          span,
		msg:           msg,
		checkpointKV:  checkpointKV,
		signalKV:      signalKV,
		dispatchNonce: payload.DispatchNonce,
		loopState:     payload.State,
		loopStateMax:  loopStateMax,
		legacyEngine:  payload.StateMaxBytes <= 0,
	}
}

func (c *taskContext) Input() []byte               { return c.input }
func (c *taskContext) RunID() string               { return c.runID }
func (c *taskContext) StepID() string              { return c.stepID }
func (c *taskContext) RetryCount() int             { return c.attempt }
func (c *taskContext) Metadata() map[string]string { return c.metadata }
func (c *taskContext) Context() context.Context    { return c.ctx }

// ControlPlane returns the per-step control-plane handle, or nil when
// the step was not gated. nil is the deny-by-default zero value.
func (c *taskContext) ControlPlane() ControlPlane { return c.controlPlane }

// logWriter is the io.Writer LogOut()/LogErr() hand back to callers.
// stream selects which of the lane's two buffers a Write() targets.
type logWriter struct {
	lane   *logLane
	stream string
}

func (w *logWriter) Write(p []byte) (int, error) {
	if w.stream == protocol.LogStreamOut {
		return w.lane.writeOut(p)
	}
	return w.lane.writeErr(p)
}

// resolveAttemptNumber derives the 1-based AttemptNumber for a
// dispatched task — the SAME resolution publishStarted's inline logic
// already used (payload.Attempt when the engine set one explicitly,
// else NATS NumDelivered), extracted here so BUILD_LOGS's attempt-
// scoped subject (#624 review) and step.started/step.failed's
// AttemptNumber field are computed by ONE function and can never drift
// apart. That agreement matters beyond consistency: GET .../logs
// defaults its ?attempt= param to dag.StepState.Attempts, which is
// itself derived from AttemptNumber — if the log subject used a
// different numbering, that default would silently point at the wrong
// (or a nonexistent) subject.
//
// msg == nil resolves via payloadAttempt alone (falling back to 1) —
// exercised only by tests that construct a taskContext directly
// without a NATS message; the real dispatch path (publishStarted)
// always has a non-nil msg and preserves its original contract: when
// msg is non-nil, Metadata() is read (and its error propagated)
// unconditionally, exactly as publishStarted did before this was
// extracted, even though a positive payloadAttempt makes the value
// unused — a Metadata() failure on the real message is still a real
// problem worth surfacing.
func resolveAttemptNumber(payloadAttempt int, msg jetstream.Msg) (int, error) {
	if msg == nil {
		if payloadAttempt > 0 {
			return payloadAttempt, nil
		}
		return 1, nil
	}
	meta, err := msg.Metadata()
	if err != nil {
		return 0, err
	}
	if payloadAttempt > 0 {
		return payloadAttempt, nil
	}
	if meta == nil {
		return 1, nil
	}
	n := int(meta.NumDelivered)
	if n < 1 {
		n = 1
	}
	return n, nil
}

// effectiveAttempt returns c.attemptNumber if publishStarted has
// already resolved it (the normal dispatch path — see handleMessage,
// which calls publishStarted before invoking the handler), or resolves
// it fresh as a fallback for tests that construct a taskContext
// directly. Errors from that fallback resolution are swallowed to 1
// (the "first/only attempt" default) rather than propagated — an
// io.Writer's LogOut()/LogErr() must never fail a handler over this.
func (c *taskContext) effectiveAttempt() int {
	if c.attemptNumber > 0 {
		return c.attemptNumber
	}
	n, err := resolveAttemptNumber(c.attempt, c.msg)
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// ensureLogLane lazily creates c.logLane on first use, scoped to this
// attempt and iteration (effectiveAttempt() — see resolveAttemptNumber's
// doc comment for why this must match step.started's AttemptNumber).
// The check-then-set on c.logLane here is NOT itself safe for
// concurrent LogOut()/LogErr() calls racing the FIRST creation — call
// LogOut()/LogErr() once, from the handler goroutine, before handing
// the returned io.Writer to concurrent users (e.g. os/exec's
// cmd.Stdout/Stderr). Once created, the lane itself IS safe for
// concurrent Write() calls (see the TaskContext interface's LogOut/
// LogErr doc comment) — this constructor race is the only unsafe part.
func (c *taskContext) ensureLogLane() *logLane {
	if c.logLane == nil {
		c.logLane = newLogLane(
			c.ctx, c.tp, c.runID, c.stepID,
			c.effectiveAttempt(), c.iteration, c.logChunkFailures,
		)
	}
	return c.logLane
}

// LogOut returns a writer that publishes stdout-tagged chunks to the
// BUILD_LOGS hot lane (logs.{runID}.{stepID}.{attempt}.{iteration}).
// Buffered: writes are batched and flushed at LogChunkBytesMax or
// after 250ms, never blocking on NATS I/O.
func (c *taskContext) LogOut() io.Writer {
	return &logWriter{lane: c.ensureLogLane(), stream: protocol.LogStreamOut}
}

// LogErr is LogOut's stderr-tagged counterpart.
func (c *taskContext) LogErr() io.Writer {
	return &logWriter{lane: c.ensureLogLane(), stream: protocol.LogStreamErr}
}

// drainLogs flushes any buffered log bytes and emits marker (one of
// the protocol.LogMarker* constants) as the last chunk on this
// attempt's subject, then stops the lane's ticker. No-op if the
// handler never called LogOut()/LogErr(). Called by every
// attempt-ending TaskContext method (Complete, Fail, FailPermanent,
// FailRetryAfter, Continue, Pause) with the marker matching its own
// outcome — see drainWithMarker's doc comment for why this must be
// exhaustive.
func (c *taskContext) drainLogs(marker string) {
	if c.logLane != nil {
		c.logLane.drainWithMarker(marker)
	}
}

// Complete publishes a step.completed event with trace context.
func (c *taskContext) Complete(output []byte) error {
	if c.msg == nil {
		panic("Complete: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Complete: runID must not be empty")
	}
	_, span := c.tracer.Start(c.ctx,
		"worker.complete",
		trace.WithAttributes(
			attribute.String("run_id", c.runID),
			attribute.String("step_id", c.stepID),
			attribute.Int64(
				"output_size_bytes", int64(len(output)),
			),
		),
	)
	defer span.End()
	// Drain-before-resolve invariant (#624): flush trailing log bytes
	// and emit the "completed" marker so a consumer observing
	// step.completed can never see it before the log lines (and the
	// marker) that led to it.
	c.drainLogs(protocol.LogMarkerCompleted)
	return c.publishEvent(
		protocol.EventStepCompleted, output,
	)
}

// Fail publishes a step.failed event with retriable failure type.
func (c *taskContext) Fail(err error) error {
	if c.msg == nil {
		panic("Fail: msg already consumed or nil")
	}
	if err == nil {
		panic("Fail: err must not be nil")
	}
	_, span := c.tracer.Start(c.ctx,
		"worker.fail",
		trace.WithAttributes(
			attribute.String("run_id", c.runID),
			attribute.String("step_id", c.stepID),
			attribute.String("error", err.Error()),
		),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	// Drain-before-resolve invariant (#624): the "failed" marker chunk
	// must precede the step.failed event this call publishes next.
	c.drainLogs(protocol.LogMarkerFailed)
	return c.publishFailedPayload(protocol.StepFailedPayload{
		Error:       err.Error(),
		FailureType: protocol.FailureTypeRetriable,
	})
}

// FailPermanent publishes a step.failed event with non-retriable
// failure type. The engine skips all retries for this step.
func (c *taskContext) FailPermanent(err error) error {
	if c.msg == nil {
		panic("FailPermanent: msg already consumed or nil")
	}
	if err == nil {
		panic("FailPermanent: err must not be nil")
	}
	_, span := c.tracer.Start(c.ctx,
		"worker.failPermanent",
		trace.WithAttributes(
			attribute.String("run_id", c.runID),
			attribute.String("step_id", c.stepID),
			attribute.String("error", err.Error()),
			attribute.String("failure_type", "non_retriable"),
		),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	// Drain-before-resolve invariant (#624): the "failed" marker chunk
	// must precede the step.failed event this call publishes next.
	c.drainLogs(protocol.LogMarkerFailed)
	return c.publishFailedPayload(protocol.StepFailedPayload{
		Error:       err.Error(),
		FailureType: protocol.FailureTypeNonRetriable,
	})
}

// FailRetryAfter publishes a step.failed event with an explicit
// retry delay. The engine retries after exactly the specified
// duration, ignoring the step's backoff policy for this attempt.
func (c *taskContext) FailRetryAfter(
	err error, after time.Duration,
) error {
	if c.msg == nil {
		panic("FailRetryAfter: msg already consumed or nil")
	}
	if err == nil {
		panic("FailRetryAfter: err must not be nil")
	}
	if after <= 0 {
		panic("FailRetryAfter: after must be positive")
	}
	_, span := c.tracer.Start(c.ctx,
		"worker.failRetryAfter",
		trace.WithAttributes(
			attribute.String("run_id", c.runID),
			attribute.String("step_id", c.stepID),
			attribute.String("error", err.Error()),
			attribute.String("failure_type", "retry_after"),
		),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	afterMs := after.Milliseconds()
	if afterMs > 3_600_000 {
		afterMs = 3_600_000
	}
	if afterMs < 100 {
		afterMs = 100
	}
	// This attempt failed too (the engine will retry after afterMs on
	// a fresh attempt-scoped subject) — "failed" is the correct marker
	// for this attempt's own subject, same as Fail/FailPermanent.
	c.drainLogs(protocol.LogMarkerFailed)
	return c.publishFailedPayload(protocol.StepFailedPayload{
		Error:        err.Error(),
		FailureType:  protocol.FailureTypeRetryAfter,
		RetryAfterMs: afterMs,
	})
}

// publishFailedPayload marshals a StepFailedPayload and publishes
// it as a step.failed event.
func (c *taskContext) publishFailedPayload(
	payload protocol.StepFailedPayload,
) error {
	if c.runID == "" {
		panic("publishFailedPayload: runID must not be empty")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal StepFailedPayload: %w", err)
	}
	return c.publishEvent(protocol.EventStepFailed, data)
}

// Continue publishes a step.continue event carrying the state the
// handler checkpointed this iteration (#801), or the state this
// iteration started from when it checkpointed nothing. The engine
// commits that state in the same save that advances the iteration, so
// checkpoint and continue are one write. The event names its dispatch
// (iteration, attempt, nonce) and its MsgId is the stable
// {run}.{step}.step.continue.{attempt}.{iteration}: a redelivered task
// that continues again is deduplicated by JetStream inside the dedup
// window and dropped by the engine outside it. Fails without
// publishing when the state is over the engine's cap.
func (c *taskContext) Continue(output []byte) error {
	if c.msg == nil {
		panic("Continue: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Continue: runID must not be empty")
	}
	_, span := c.tracer.Start(c.ctx,
		"worker.continue",
		trace.WithAttributes(
			attribute.String("run_id", c.runID),
			attribute.String("step_id", c.stepID),
			attribute.Int64(
				"iteration", int64(c.iteration),
			),
		),
	)
	defer span.End()
	state := c.loopState
	if c.pendingStateSet {
		state = c.pendingState
	}
	if len(state) > c.loopStateMax {
		return fmt.Errorf(
			"Continue: state is %d bytes, over the %d byte cap (loop_state_max_bytes)",
			len(state), c.loopStateMax,
		)
	}
	payload, err := json.Marshal(protocol.StepContinuePayload{
		Output: output, State: state, StateSet: true,
	})
	if err != nil {
		return fmt.Errorf("marshal StepContinuePayload: %w", err)
	}
	// Drain-before-resolve invariant (#624): flush trailing log bytes
	// and emit "continued" before the step.continue publish below —
	// this attempt's subject is done; the next iteration gets a fresh
	// attempt-scoped subject.
	c.drainLogs(protocol.LogMarkerContinued)
	if err := c.publishEvent(protocol.EventStepContinue, payload); err != nil {
		return err
	}
	c.clearShadowingCheckpoint()
	return nil
}

// clearShadowingCheckpoint deletes the checkpoints-bucket entry that
// LoadCheckpoint returned this delivery (a pause marker, or a
// checkpoint left by a worker older than #801), once the continue it
// fed has been published. Left in place it would shadow the committed
// state on every later iteration. Best effort: the continue is already
// committed, so a failed delete is logged, not returned.
func (c *taskContext) clearShadowingCheckpoint() {
	if c.runID == "" {
		panic("clearShadowingCheckpoint: runID must not be empty")
	}
	if c.stepID == "" {
		panic("clearShadowingCheckpoint: stepID must not be empty")
	}
	if !c.kvCheckpointSeen || c.checkpointKV == nil || c.legacyEngine {
		return
	}
	key := c.runID + "." + c.stepID
	if err := c.checkpointKV.Delete(c.ctx, key); err != nil &&
		!errors.Is(err, jetstream.ErrKeyNotFound) {
		slog.WarnContext(c.ctx, "delete checkpoint after continue",
			"run_id", c.runID, "step_id", c.stepID, "error", err)
	}
	c.kvCheckpointSeen = false
}

// PutStream publishes data to a streaming subject for real-time
// consumption. Uses core NATS pub/sub (not JetStream) -- tokens are
// ephemeral, fire-and-forget. Clients subscribe to
// stream.{run_id}.{step_id} for live delivery.
func (c *taskContext) PutStream(data []byte) error {
	if c.msg == nil {
		panic("PutStream: msg already consumed or nil")
	}
	if c.nc == nil {
		panic("PutStream: nc must not be nil")
	}
	subject := fmt.Sprintf(
		"stream.%s.%s", c.runID, c.stepID,
	)
	return c.tp.Publish(c.ctx, subject, data)
}

// publishEvent creates, traces, and publishes a step lifecycle
// event with trace context propagation. The event names the dispatch
// it answers (#801): the iteration and nonce from the task payload and
// the same 1-based attempt step.started reported, so the engine can
// match it against the step's saved state and drop it when a later
// dispatch has superseded this worker. That identity also makes each
// retry's and iteration's MsgId distinct.
func (c *taskContext) publishEvent(
	eventType protocol.EventType, payload []byte,
) error {
	if eventType == "" {
		panic("publishEvent: eventType must not be empty")
	}
	if c.runID == "" {
		panic("publishEvent: runID must not be empty")
	}
	evt := protocol.NewStepEvent(
		eventType, c.runID, c.stepID, payload,
	)
	evt.WorkerID = c.workerID
	evt.SetIdentity(c.iteration, c.effectiveAttempt(), c.dispatchNonce)
	outMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	// JSPublishMsgEvent injects trace context, then marshals evt
	// internally so the persisted body carries TraceParent.
	_, err := c.tp.JSPublishMsgEvent(c.ctx, outMsg, &evt)
	return err
}

// Heartbeat extends the AckWait timer on the original NATS message
// to prevent redelivery while long-running work is in progress.
func (c *taskContext) Heartbeat() error {
	if c.msg == nil {
		panic("Heartbeat: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Heartbeat: runID must not be empty")
	}
	// The cancel signal is at-most-once; the run state is the truth.
	if c.runCancelled != nil && c.runCancelled() {
		return ErrRunCancelled
	}
	return c.msg.InProgress()
}

// Checkpoint records the state the next Continue commits (#801). It
// writes nothing itself: the state rides the step.continue event and
// the engine saves it with the iteration, so a crash between
// Checkpoint and Continue leaves nothing behind and the redelivered
// iteration starts from the state this one started from. Within the
// same iteration LoadCheckpoint returns the checkpointed state. Under
// an engine older than #801 (no state cap on the task) the state is
// also written to the checkpoints bucket, as before, since that engine
// will not commit it.
func (c *taskContext) Checkpoint(state []byte) error {
	if c.msg == nil {
		panic("Checkpoint: msg already consumed or nil")
	}
	if c.runID == "" {
		panic("Checkpoint: runID must not be empty")
	}
	c.pendingState = state
	c.pendingStateSet = true
	if c.legacyEngine {
		return c.writeCheckpointKV(state)
	}
	return nil
}

// LoadCheckpoint returns the state this iteration should start from:
// the state checkpointed earlier in this same delivery, else the
// checkpoints-bucket entry for {runID}.{stepID} when one exists (a
// Pause marker, or a checkpoint written by a worker older than #801),
// else the state the engine committed with the step's last accepted
// continue and delivered in the task payload. (nil, nil) when none of
// these exist.
func (c *taskContext) LoadCheckpoint() ([]byte, error) {
	if c.runID == "" {
		panic("LoadCheckpoint: runID must not be empty")
	}
	if c.stepID == "" {
		panic("LoadCheckpoint: stepID must not be empty")
	}
	if c.pendingStateSet {
		return c.pendingState, nil
	}
	if c.checkpointKV != nil {
		key := c.runID + "." + c.stepID
		entry, err := c.checkpointKV.Get(c.ctx, key)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		if err == nil {
			c.kvCheckpointSeen = true
			return entry.Value(), nil
		}
	}
	return c.loopState, nil
}

// writeCheckpointKV writes state to the checkpoints bucket at
// {runID}.{stepID}: Pause's durable marker, the one checkpoint that
// must outlive this delivery without an engine round trip.
func (c *taskContext) writeCheckpointKV(state []byte) error {
	if c.runID == "" {
		panic("writeCheckpointKV: runID must not be empty")
	}
	if c.stepID == "" {
		panic("writeCheckpointKV: stepID must not be empty")
	}
	if c.checkpointKV == nil {
		return errCheckpointKVNotConfigured
	}
	key := c.runID + "." + c.stepID
	_, err := c.checkpointKV.Put(c.ctx, key, state)
	return err
}

// WaitForSignal watches KV at {runID}.{name} until a value appears
// or timeout expires. Timeout is capped at 1 hour for safety.
func (c *taskContext) WaitForSignal(
	name string, timeout time.Duration,
) ([]byte, error) {
	if name == "" {
		panic("WaitForSignal: name must not be empty")
	}
	if timeout <= 0 || timeout > 1*time.Hour {
		panic("WaitForSignal: timeout must be in (0, 1h]")
	}
	if c.signalKV == nil {
		return nil, errSignalKVNotConfigured
	}
	key := c.runID + "." + name
	watcher, err := c.signalKV.Watch(
		c.ctx, key,
	)
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			return entry.Value(), nil
		case <-timer.C:
			return nil, fmt.Errorf(
				"signal %q timed out after %s", name, timeout,
			)
		}
	}
}

// Pause checkpoints state with a pause marker, then NAKs with delay.
// On redeliver, the handler calls LoadCheckpoint to detect the resume.
// The engine is not involved — step stays StepStatusRunning.
func (c *taskContext) Pause(name string, duration time.Duration) error {
	if name == "" {
		panic("Pause: name must not be empty")
	}
	if duration <= 0 {
		panic("Pause: duration must be positive")
	}
	if c.msg == nil {
		panic("Pause: msg already consumed or nil")
	}
	checkpoint := map[string]any{"pause_resume": name}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("marshal pause checkpoint: %w", err)
	}
	if err := c.writeCheckpointKV(data); err != nil {
		return fmt.Errorf("save pause checkpoint: %w", err)
	}
	c.paused = true
	// This taskContext (and its logLane, if the handler ever wrote to
	// LogOut/LogErr before pausing) is discarded once this call
	// returns; the "paused" marker closes out this attempt's subject
	// and stops the ticker goroutine rather than leaking it.
	c.drainLogs(protocol.LogMarkerPaused)
	return c.msg.NakWithDelay(duration)
}

// SendSignal writes data to KV at {runID}.{name} to wake up a
// waiting WaitForSignal call (possibly in another step).
func (c *taskContext) SendSignal(
	runID, name string, data []byte,
) error {
	if runID == "" {
		panic("SendSignal: runID must not be empty")
	}
	if name == "" {
		panic("SendSignal: name must not be empty")
	}
	if c.signalKV == nil {
		return errSignalKVNotConfigured
	}
	key := runID + "." + name
	_, err := c.signalKV.Put(
		c.ctx, key, data,
	)
	return err
}

// publishStarted publishes step.started for the current attempt.
// AttemptNumber preference order:
//  1. payload.Attempt (c.attempt > 0) — set explicitly by the engine
//     when re-publishing via SLEEP_TIMERS (retry_after / retry_backoff).
//     The fresh NATS message has NumDelivered=1 so deriving from
//     metadata would lose the attempt count across timer-driven retries.
//  2. meta.NumDelivered — increments on NATS redelivery (NAK retries,
//     AckWait expiry) and is correct for in-place retries that share
//     the original message.
//
// Returns error if metadata read or publish fails. The caller (worker
// dispatch loop) NAKs the original message on error so the engine
// never sees step.completed for an attempt it never saw step.started
// for. Lifecycle stays consistent.
//
// Called once per attempt, before invoking the user's task handler.
// Hides the AttemptNumber resolution + publish chain from the dispatch
// loop. publishEvent (used by Complete and Fail*) is NOT modified — it
// stays unchanged so its callers don't gain knowledge of AttemptNumber.
func (c *taskContext) publishStarted(msg jetstream.Msg) error {
	if msg == nil {
		panic("publishStarted: msg must not be nil")
	}
	if c.runID == "" {
		panic("publishStarted: runID must not be empty")
	}
	attemptNumber, err := resolveAttemptNumber(c.attempt, msg)
	if err != nil {
		return err
	}
	c.attemptNumber = attemptNumber
	evt := protocol.NewStepEvent(
		protocol.EventStepStarted, c.runID, c.stepID, nil,
	)
	evt.WorkerID = c.workerID
	evt.AttemptNumber = attemptNumber
	outMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	// Dual-write trace context onto the Event payload so post-restart
	// replay still has the trace ID. Header injection happens inside
	// the publishMsg seam (TracingPublisher.JSPublishMsg) — duplicated
	// here only for the evt.TraceParent persistence side.
	evt.TraceParent = ""
	evt.TraceState = ""
	syntheticHdr := nats.Header{}
	syntheticMsg := &nats.Msg{Header: syntheticHdr}
	observe.InjectTraceContext(c.ctx, syntheticMsg, &evt)
	data, err := evt.Marshal()
	if err != nil {
		return err
	}
	outMsg.Data = data
	if c.publishMsg == nil {
		panic("publishStarted: publishMsg must not be nil")
	}
	_, err = c.publishMsg(c.ctx, outMsg)
	return err
}
