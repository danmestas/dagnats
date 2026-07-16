// engine/orchestrator.go
// The orchestrator is the thin I/O shell of DagNats. It subscribes to the
// history stream, resolves DAG dependencies via dag.ResolveReady, and publishes
// task messages. All delivery guarantees, retries, and timeouts are handled by
// NATS — this file contains no timers, no retry logic, no in-memory queues.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// historyRedeliverSchedule bounds WORKFLOW_HISTORY redelivery (#508).
// Indexes the explicit NakWithDelay in handleEventJS (the dominant,
// explicit-NAK path). We deliberately do NOT set ConsumerConfig.BackOff:
// BackOff overrides the per-attempt AckWait, causing spurious ack-timeout
// redeliveries and #196-class duplicate processing, and BackOff is inert
// on NAK'd messages anyway. MaxDeliver is the only consumer-level knob;
// this schedule is the sole escalation source. len IS the MaxDeliver cap
// — keep them defined together. The final entry is never used as a NAK
// delay (the last delivery dead-letters rather than NAKing); it defines
// the total window.
//
// The consumer's AckWait is also derived from this schedule (the longest
// entry — see historyConsumerAckWait), NOT left at NATS's 30s default.
// MaxDeliver counts BOTH explicit-NAK redeliveries AND AckWait-expiry
// redeliveries against the same budget: an in-flight event that
// legitimately runs long (contended run-lock, slow KV write during a
// NATS/KV leader election) would otherwise burn delivery budget via
// silent ack-timeout redeliveries while still being processed, dead-
// lettering it before this schedule's nominal window elapses. Sizing
// AckWait to the longest schedule entry keeps that shared budget a
// ceiling on genuinely poison events, not a floor that a merely-slow
// handler can trip.
var historyRedeliverSchedule = []time.Duration{
	5 * time.Second, 10 * time.Second, 20 * time.Second,
	30 * time.Second, 60 * time.Second, 90 * time.Second,
	120 * time.Second, 180 * time.Second,
}

// historyConsumerAckWait returns the AckWait for the WORKFLOW_HISTORY
// consumer: the longest entry in schedule. See historyRedeliverSchedule's
// doc comment for why AckWait must not undercut the redelivery schedule
// it shares a MaxDeliver budget with.
func historyConsumerAckWait(schedule []time.Duration) time.Duration {
	if len(schedule) == 0 {
		panic("historyConsumerAckWait: schedule must not be empty")
	}
	longest := schedule[0]
	for _, delay := range schedule {
		if delay > longest {
			longest = delay
		}
	}
	if longest <= 0 {
		panic("historyConsumerAckWait: longest schedule entry must be positive")
	}
	return longest
}

// Orchestrator subscribes to the history stream and drives workflow execution.
// It is intentionally stateless between events — all run state lives in the
// snapshot store (NATS KV), so the orchestrator can crash and resume safely.
type Orchestrator struct {
	nc *nats.Conn
	js jetstream.JetStream
	// tp wraps nc + js so every publish auto-injects W3C trace context.
	// Constructed once in NewOrchestrator and shared with every subsystem
	// that publishes (TaskPublisher, RecoveryManager, ApprovalGate,
	// Correlator, AdmissionController, SleepTimer). Per #334, raw
	// JS or core NATS Publish/PublishMsg outside the wrapper are
	// CI-lint forbidden.
	tp         *natsutil.TracingPublisher
	defKV      jetstream.KeyValue
	store      *SnapshotStore
	tracer     trace.Tracer
	cc         jetstream.ConsumeContext
	runLocks   sync.Map             // map[string]*sync.Mutex — per-run serialization
	admission  *AdmissionController // singleton + concurrency
	approval   *ApprovalGate        // approval token lifecycle
	sleepTimer *SleepTimer          // durable sleep via NakWithDelay
	correlator *Correlator          // event wait-for-event matching
	sticky     *StickyRouter        // worker affinity bindings
	publisher  *TaskPublisher       // task dispatch pipeline
	recovery   *RecoveryManager     // failure recovery + compensation

	// Pre-allocated metric instruments — created once in constructor.
	metrics orchMetrics

	// reconcileCancel stops the periodic janitor goroutine. Set
	// in Start, called in Stop. nil before Start / after Stop.
	reconcileCancel context.CancelFunc

	// runsMaxAge is the opt-in run-retention window (#453). Zero
	// (the default) disables the sweeper entirely: the prune ticker
	// is not even started, so upgrading never silently deletes runs.
	// When > 0, terminal runs whose CompletedAt is older than this
	// are dropped (delete-only) by the background prune pass.
	runsMaxAge time.Duration

	// defReaperGrace is the opt-in ephemeral-def garbage-collection
	// window (#377). Zero (the default) disables the reaper entirely:
	// the reaper ticker is not even started. When > 0, a def under the
	// reaper-eligible prefix is dropped once its tree-root run has been
	// terminal for longer than this grace.
	defReaperGrace time.Duration

	// capHitPrev tracks whether the previous reconcile cycle hit
	// reconcileMaxRunsScan. Used to suppress the steady-state
	// scan-cap WARN (#260): emit only on the not-capped → capped
	// transition; drop to DEBUG while continuously capped; emit
	// INFO on the capped → not-capped recovery edge. Accessed
	// only from the single reconciler goroutine, no lock needed.
	capHitPrev bool

	// grantPolicy holds the hot-reloadable capability-grant policy (#380).
	// nil (the default) denies every control-plane grant — deny-by-default.
	// Shared with the TaskPublisher so the enqueue path strips the
	// control-plane capability from any step whose workflow is not granted.
	grantPolicy *GrantPolicyHolder

	// historyRedeliverSchedule bounds WORKFLOW_HISTORY redelivery
	// (#508). Defaults to the package-level historyRedeliverSchedule
	// var; overridable via WithHistoryRedeliverBackoff. len() becomes
	// the consumer's MaxDeliver cap in Start().
	historyRedeliverSchedule []time.Duration
}

// OrchestratorOption configures optional orchestrator behavior.
type OrchestratorOption func(*Orchestrator)

// WithStepRoutes configures step type → subject prefix routing.
// Steps with types not in the map route to "task" (default).
func WithStepRoutes(
	routes map[dag.StepType]string,
) OrchestratorOption {
	return func(o *Orchestrator) {
		o.publisher.stepRoutes = routes
	}
}

// WithGrantPolicyHolder wires the hot-reloadable capability-grant policy
// (#380). The orchestrator shares the holder with its TaskPublisher so the
// enqueue path strips the control-plane capability from a step whose
// workflow is not granted. A nil holder (the default, when this option is
// not supplied) means every grant is denied — deny-by-default.
func WithGrantPolicyHolder(holder *GrantPolicyHolder) OrchestratorOption {
	return func(o *Orchestrator) {
		o.grantPolicy = holder
		if o.publisher != nil {
			o.publisher.grantPolicy = holder
		}
	}
}

// WithRunsMaxAge enables the opt-in run-retention sweeper (#453) with the
// given window. A zero or negative window leaves the sweeper disabled (the
// default), so the prune ticker is never started and no runs are deleted.
// When positive, terminal runs whose CompletedAt is older than maxAge are
// dropped by the background prune pass.
func WithRunsMaxAge(maxAge time.Duration) OrchestratorOption {
	return func(o *Orchestrator) {
		if maxAge > 0 {
			o.runsMaxAge = maxAge
		}
	}
}

// WithDefReaperGrace enables the opt-in ephemeral-def reaper (#377) with the
// given grace window. A zero or negative grace leaves the reaper disabled
// (the default), so the reaper ticker is never started and no defs are
// deleted. When positive, ephemeral defs whose tree-root run has been
// terminal longer than grace are garbage-collected by the background pass.
func WithDefReaperGrace(grace time.Duration) OrchestratorOption {
	return func(o *Orchestrator) {
		if grace > 0 {
			o.defReaperGrace = grace
		}
	}
}

// WithHistoryRedeliverBackoff overrides the WORKFLOW_HISTORY redelivery
// schedule (#508). len(schedule) becomes the consumer MaxDeliver cap.
// Primary use: integration tests inject a ms-scale schedule so a poison
// event exhausts in <1s instead of the ~8.6min production window
// (TigerStyle: bounded test waits). A nil/empty schedule keeps the default.
func WithHistoryRedeliverBackoff(schedule []time.Duration) OrchestratorOption {
	return func(o *Orchestrator) {
		if len(schedule) > 0 {
			o.historyRedeliverSchedule = schedule
		}
	}
}

// NewOrchestrator creates an Orchestrator bound to the given NATS connection.
// Panics if nc is nil or JetStream cannot be obtained — both are programmer
// errors. KV buckets must already exist (call natsutil.SetupAll first).
func NewOrchestrator(
	nc *nats.Conn,
	opts ...OrchestratorOption,
) *Orchestrator {
	if nc == nil {
		panic("NewOrchestrator: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("NewOrchestrator: jetstream.New: " + err.Error())
	}
	defKV, err := js.KeyValue(
		context.Background(), "workflow_defs",
	)
	if err != nil {
		panic(
			"NewOrchestrator: workflow_defs bucket not found: " +
				err.Error(),
		)
	}
	tp := natsutil.NewTracingPublisher(nc, js)
	cm, _ := NewConcurrencyManagerSafe(js)
	store := NewSnapshotStore(js)
	singletonKV, _ := js.KeyValue(
		context.Background(), "singleton_locks",
	)
	ac := NewAdmissionController(
		nc, js, tp, store, cm, singletonKV,
	)
	rl := NewRateLimiter(js)
	m := otel.Meter("dagnats/engine")
	om := newOrchMetrics(m)
	pm := newPubMetrics(m)
	tracer := otel.Tracer("dagnats/engine")
	sleepTimer := NewSleepTimer(nc, js, tp)
	stickyKV, _ := js.KeyValue(
		context.Background(), "sticky_bindings",
	)
	sticky := NewStickyRouter(
		stickyKV, js, tp, sleepTimer, tracer,
		pm.stepEnqueue,
	)
	o := &Orchestrator{
		nc:                       nc,
		js:                       js,
		tp:                       tp,
		defKV:                    defKV,
		store:                    store,
		tracer:                   tracer,
		admission:                ac,
		sleepTimer:               sleepTimer,
		sticky:                   sticky,
		metrics:                  om,
		historyRedeliverSchedule: historyRedeliverSchedule,
	}
	o.wireDependentSubsystems(rl, ac, pm, om)
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// wireDependentSubsystems builds and binds the subsystems whose
// construction depends on the partially-constructed Orchestrator (their
// callbacks close over o or call o.loadRunAndDef). Extracted so
// NewOrchestrator stays under TigerStyle's 70-line limit and the
// callback wiring lives in one focused unit.
func (o *Orchestrator) wireDependentSubsystems(
	rl *RateLimiter,
	ac *AdmissionController,
	pm pubMetrics,
	om orchMetrics,
) {
	if o == nil {
		panic("wireDependentSubsystems: o must not be nil")
	}
	if o.js == nil {
		panic("wireDependentSubsystems: o.js must not be nil")
	}
	publisher := NewTaskPublisher(
		o.js, o.tp, rl, ac, o.sticky, o.sleepTimer, o.tracer,
		pm, o.loadRunAndDef,
	)
	o.publisher = publisher
	o.recovery = NewRecoveryManager(
		o.js, o.tp, publisher, o.tracer,
		om.runsActive, om.runsFailed,
		om.dlqEntries, om.dlqDepth,
	)
	o.approval = NewApprovalGate(
		o.nc, o.js, o.tp, o.sleepTimer, o.tracer,
	)
	o.correlator = NewCorrelator(o.nc, o.js, o.tp)
	// Wire the step timeout watchdog (issue #140). Hooking from the
	// orchestrator side keeps SleepTimer free of a SnapshotStore
	// dependency while still letting the fire path do staleness
	// checks against live run state before publishing a synthetic
	// step.failed.
	o.sleepTimer.OnStepTimeout(o.fireStepTimeout)
}

// Start subscribes to history.> on the WORKFLOW_HISTORY stream using
// a pull consumer. Messages are delivered asynchronously to handleEvent.
// SleepTimer and Correlator start lazily on first use via sync.Once.
// Panics if already started.
func (o *Orchestrator) Start() {
	if o.cc != nil {
		panic("Orchestrator.Start: already started")
	}
	if len(o.historyRedeliverSchedule) == 0 {
		panic(
			"Orchestrator.Start: historyRedeliverSchedule must not be empty",
		)
	}
	o.cc = o.startHistoryConsumer()

	// Wire the periodic reconciliation janitor (#185). The
	// goroutine exits when reconcileCancel is invoked from
	// Stop. Started after the consumer is wired so a healthy
	// orchestrator is always doing one or the other.
	reconcileCtx, cancel := context.WithCancel(
		context.Background(),
	)
	o.reconcileCancel = cancel
	o.startReconciler(reconcileCtx)

	// Opt-in run-retention sweeper (#453). Started ONLY when a
	// retention window is configured — when runsMaxAge is zero the
	// ticker never runs, the headline OFF-by-default safety property.
	if o.runsMaxAge > 0 {
		o.startRunPruner(reconcileCtx)
	}

	// Opt-in ephemeral-def reaper (#377). Started ONLY when a grace is
	// configured — when defReaperGrace is zero the ticker never runs.
	//
	// CRITICAL invariant (Ousterhout fix 1): the run snapshot MUST
	// outlive the def-grace window. The reaper treats a missing root run
	// as a true orphan and sweeps its def; if the run-pruner could delete
	// a root run BEFORE the def's grace elapsed, the reaper would observe
	// a false orphan and reap a def whose tree had not yet aged out. With
	// runsMaxAge >= defReaperGrace the run always survives at least as
	// long as the def-grace, so a missing root genuinely means both
	// windows elapsed. runsMaxAge == 0 (pruner off) trivially satisfies
	// this — the run never disappears under the reaper.
	if o.defReaperGrace > 0 {
		if o.runsMaxAge != 0 && o.runsMaxAge < o.defReaperGrace {
			panic("Start: runsMaxAge must be 0 or >= defReaperGrace " +
				"(orphan-sweep safety invariant, #377)")
		}
		o.startDefReaper(reconcileCtx)
	}
}

// startHistoryConsumer creates (or updates) the durable "orchestrator"
// consumer on WORKFLOW_HISTORY and begins consuming into handleEventJS.
// Extracted from Start so the consumer-setup logic and its rationale
// live in one focused unit, mirroring wireDependentSubsystems. Panics on
// any JetStream error — consumer setup failure at startup is a programmer
// or environment error, not a runtime condition callers can recover from.
func (o *Orchestrator) startHistoryConsumer() jetstream.ConsumeContext {
	if o == nil {
		panic("startHistoryConsumer: o must not be nil")
	}
	if o.js == nil {
		panic("startHistoryConsumer: o.js must not be nil")
	}
	stream, err := o.js.Stream(
		context.Background(), "WORKFLOW_HISTORY",
	)
	if err != nil {
		panic(
			"Orchestrator.Start: stream: " + err.Error(),
		)
	}
	// Durable consumer name persists ack offsets across dagnats
	// restarts. Without this (originally an ephemeral consumer),
	// every restart created a new consumer that replayed the entire
	// history stream from sequence 1, re-delivering workflow.started
	// and step.* events for runs that completed days ago. Combined
	// with non-idempotent handlers, that produced duplicate run
	// executions and the symptoms reported in #196 / #194 / #195.
	//
	// First deploy of this change still replays once because the
	// durable consumer is being created for the first time; the
	// idempotency guards added in #196 — terminal-run short-circuits
	// at the top of handleWorkflowStarted, handleStepCompleted, and
	// handleStepFailed, plus the pre-existing stale-event guard in
	// handleStepStarted — make that replay a no-op for runs that
	// have already reached a terminal state.
	//
	// MaxDeliver and AckWait: see historyRedeliverSchedule's doc
	// comment above for the full NAK-escalation / BackOff / shared-
	// budget rationale (#508).
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			Durable:       "orchestrator",
			FilterSubject: "history.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			MaxDeliver:    len(o.historyRedeliverSchedule),
			AckWait:       historyConsumerAckWait(o.historyRedeliverSchedule),
		},
	)
	if err != nil {
		panic(
			"Orchestrator.Start: consumer: " + err.Error(),
		)
	}
	cc, err := cons.Consume(o.handleEventJS)
	if err != nil {
		panic(
			"Orchestrator.Start: consume: " + err.Error(),
		)
	}
	return cc
}

// Stop drains and unsubscribes from the history stream.
// Safe to call multiple times.
func (o *Orchestrator) Stop() {
	if o.reconcileCancel != nil {
		o.reconcileCancel()
		o.reconcileCancel = nil
	}
	if o.correlator != nil {
		o.correlator.Stop()
	}
	if o.sleepTimer != nil {
		o.sleepTimer.Stop()
	}
	if o.cc == nil {
		return
	}
	o.cc.Stop()
	o.cc = nil
}

// getRunLock returns a per-run mutex, creating one on first access.
// Serializes all event handling for a given run to prevent concurrent
// KV load-modify-save races between parallel step completions.
func (o *Orchestrator) getRunLock(runID string) *sync.Mutex {
	val, _ := o.runLocks.LoadOrStore(runID, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// handleEventJS is the central dispatcher. It unmarshals the event,
// extracts trace context, and routes to the appropriate handler.
// Unknown event types are acked and logged — not errors.
func (o *Orchestrator) handleEventJS(msg jetstream.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data())
	if err != nil {
		slog.ErrorContext(
			context.Background(),
			"handleEvent: unmarshal failed", "error", err,
		)
		o.nakOrDeadLetterHistory(
			context.Background(), msg,
			"", "", "unmarshal-failed", err,
		)
		return
	}
	if !isHandledEventType(evt.Type) {
		msg.Ack()
		return
	}
	ctx := observe.ExtractTraceContext(msg, &evt)
	ctx, span := o.tracer.Start(ctx,
		"dagnats.engine handleEvent",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("run_id", evt.RunID),
			attribute.String("event_type", string(evt.Type)),
			attribute.String("step_id", evt.StepID),
		),
	)
	defer span.End()
	err = o.dispatchEvent(ctx, evt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		slog.ErrorContext(ctx, "handleEvent: handler error",
			"error", err,
			"event_type", string(evt.Type),
			"run_id", evt.RunID,
		)
		o.nakOrDeadLetterHistory(
			ctx, msg, evt.RunID, evt.StepID,
			string(evt.Type), err,
		)
		return
	}
	msg.Ack()
}

// historyRedeliverDelay returns the NAK delay for delivery numDelivered
// (1-based). Indexes schedule[numDelivered-1], clamped to the last entry.
// Panics on numDelivered==0 (NATS never delivers with 0 — programmer error).
func historyRedeliverDelay(
	schedule []time.Duration, numDelivered uint64,
) time.Duration {
	if numDelivered == 0 {
		panic("historyRedeliverDelay: numDelivered must be >= 1")
	}
	if len(schedule) == 0 {
		panic("historyRedeliverDelay: schedule must not be empty")
	}
	idx := numDelivered - 1
	if idx >= uint64(len(schedule)) {
		idx = uint64(len(schedule)) - 1
	}
	return schedule[idx]
}

// shouldDeadLetterHistory reports whether this delivery has hit the cap.
// True exactly when numDelivered >= uint64(maxDeliver).
func shouldDeadLetterHistory(maxDeliver int, numDelivered uint64) bool {
	if maxDeliver <= 0 {
		panic("shouldDeadLetterHistory: maxDeliver must be positive")
	}
	if numDelivered == 0 {
		panic("shouldDeadLetterHistory: numDelivered must be >= 1")
	}
	return numDelivered >= uint64(maxDeliver)
}

// nakOrDeadLetterHistory NAKs with the schedule-derived delay while
// below the MaxDeliver cap; at/above the cap it dead-letters the raw
// event via RecoveryManager and Acks so the poison message stops
// redelivering (#508). On a Metadata() error (should not happen post-
// Consume) it fails safe: logs and NAKs with schedule[0] rather than
// risk wrongly dead-lettering.
func (o *Orchestrator) nakOrDeadLetterHistory(
	ctx context.Context, msg jetstream.Msg,
	runID, stepID, eventType string, cause error,
) {
	if msg == nil {
		panic("nakOrDeadLetterHistory: msg must not be nil")
	}
	if len(o.historyRedeliverSchedule) == 0 {
		panic(
			"nakOrDeadLetterHistory: historyRedeliverSchedule must not be empty",
		)
	}
	md, err := msg.Metadata()
	if err != nil {
		slog.ErrorContext(ctx,
			"nakOrDeadLetterHistory: Metadata failed, "+
				"failing safe with NAK",
			"error", err,
		)
		msg.NakWithDelay(o.historyRedeliverSchedule[0])
		return
	}
	numDelivered := md.NumDelivered
	if shouldDeadLetterHistory(
		len(o.historyRedeliverSchedule), numDelivered,
	) {
		o.recovery.PublishHistoryDeadLetter(
			ctx, msg.Data(), runID, stepID, eventType,
			numDelivered, md.Sequence.Stream, cause,
		)
		msg.Ack()
		return
	}
	msg.NakWithDelay(
		historyRedeliverDelay(o.historyRedeliverSchedule, numDelivered),
	)
}

// isHandledEventType returns true for event types the orchestrator processes.
func isHandledEventType(t protocol.EventType) bool {
	switch t {
	case protocol.EventWorkflowStarted,
		protocol.EventStepQueued,
		protocol.EventStepStarted,
		protocol.EventStepCompleted,
		protocol.EventStepContinue,
		protocol.EventStepFailed,
		protocol.EventWorkflowSpawn,
		protocol.EventWorkflowChildCompleted,
		protocol.EventWorkflowChildFailed,
		protocol.EventWorkflowCancelled,
		protocol.EventStepSleepCompleted,
		protocol.EventStepWaitMatched,
		protocol.EventStepWaitTimeout,
		protocol.EventApprovalGranted,
		protocol.EventApprovalRejected,
		protocol.EventApprovalExpired:
		return true
	}
	return false
}

// dispatchEvent routes an event to its handler under a per-run lock.
// A defer recover converts any handler panic into an error so a single
// poisoned event cannot kill the consumer goroutine and crash the
// engine. The recovered error is logged with full event context and
// returned upstream where handleEventJS NAKs the message.
func (o *Orchestrator) dispatchEvent(
	ctx context.Context, evt protocol.Event,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx,
				"dispatchEvent: handler panic recovered",
				"panic", fmt.Sprintf("%v", r),
				"event_type", string(evt.Type),
				"run_id", evt.RunID,
				"step_id", evt.StepID,
			)
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	if evt.RunID == "" {
		panic("dispatchEvent: RunID must not be empty")
	}
	lock := o.getRunLock(evt.RunID)
	lock.Lock()
	defer lock.Unlock()

	// Check workflow timeout before dispatching any event.
	run, loadErr := o.store.Load(ctx, evt.RunID)
	if loadErr == nil && run.Deadline != nil &&
		time.Now().After(*run.Deadline) &&
		run.Status == dag.RunStatusRunning {
		return o.handleWorkflowCancelled(ctx, evt)
	}

	switch evt.Type {
	case protocol.EventWorkflowStarted:
		return o.handleWorkflowStarted(ctx, evt)
	case protocol.EventStepCompleted:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepSleepCompleted:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepWaitMatched:
		return o.handleStepCompleted(ctx, evt)
	case protocol.EventStepWaitTimeout:
		return o.handleWaitTimeout(ctx, evt)
	case protocol.EventStepContinue:
		return o.handleStepContinue(ctx, evt)
	case protocol.EventStepFailed:
		return o.handleStepFailed(ctx, evt)
	case protocol.EventStepStarted:
		return o.handleStepStarted(ctx, evt)
	case protocol.EventStepQueued:
		return o.handleStepQueued(ctx, evt)
	case protocol.EventWorkflowSpawn:
		return o.handleWorkflowSpawn(ctx, evt)
	case protocol.EventWorkflowChildCompleted:
		return o.handleChildCompleted(ctx, evt)
	case protocol.EventWorkflowChildFailed:
		return o.handleChildFailed(ctx, evt)
	case protocol.EventWorkflowCancelled:
		return o.handleWorkflowCancelled(ctx, evt)
	case protocol.EventApprovalGranted:
		return o.approval.HandleGranted(
			ctx, evt, o.loadRunAndDef,
			o.completeWorkflow, o.saveSnapshot,
			o.enqueueReady,
		)
	case protocol.EventApprovalRejected:
		return o.approval.HandleRejected(
			ctx, evt, o.loadRunAndDef, o.failWorkflow,
		)
	case protocol.EventApprovalExpired:
		return o.approval.HandleExpired(
			ctx, evt, o.loadRunAndDef, o.failWorkflow,
		)
	default:
		return nil
	}
}

// handleWorkflowStarted creates the initial WorkflowRun from the event
// payload, saves it, then enqueues all entry-point steps. If concurrency
// limit is configured and reached, the run stays Pending.
func (o *Orchestrator) handleWorkflowStarted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowStarted: RunID must not be empty")
	}
	if evt.Payload == nil {
		panic("handleWorkflowStarted: Payload must not be nil")
	}

	// Idempotency guard (#196). Bug shape: dagnats restart causes
	// the WORKFLOW_HISTORY consumer to replay historical events,
	// including workflow.started for runs that have long since
	// completed. Without this guard, NewWorkflowRun + saveSnapshot
	// below overwrite the existing terminal-state KV entry with a
	// fresh Pending run and re-dispatch the first step, producing
	// duplicate workflow.completed events and worker storms. Any
	// existing record means a prior workflow.started for this RunID
	// has been processed — treat the redelivery as a no-op.
	if existing, loadErr := o.store.Load(
		ctx, evt.RunID,
	); loadErr == nil {
		slog.InfoContext(ctx,
			"skipping redelivered workflow.started — "+
				"run already exists in workflow_runs KV",
			"run_id", evt.RunID,
			"existing_status", existing.Status.String(),
		)
		return nil
	} else if !errors.Is(loadErr, ErrRunNotFound) {
		return fmt.Errorf(
			"load existing run %q: %w", evt.RunID, loadErr,
		)
	}

	wfDef, input, err := o.resolveStartPayload(ctx, evt)
	if errors.Is(err, errStartPayloadHandled) {
		return nil
	}
	if err != nil {
		return err
	}

	// Validate the WorkflowDef itself before constructing a run.
	// dag.NewWorkflowRun panics on invariant violations (e.g. empty
	// Steps); validating here turns that panic into a recorded
	// failure. A trigger publishing a malformed payload (see #167)
	// must not crash the engine.
	if validateErr := dag.Validate(wfDef); validateErr != nil {
		o.persistFailedStartRun(ctx, evt, wfDef.Name, validateErr)
		return nil
	}

	// Validate input against schema if configured.
	if wfDef.InputSchema != nil {
		if err := dag.ValidateSchema(wfDef.InputSchema, input); err != nil {
			// Create a failed run for visibility
			run := dag.NewWorkflowRun(wfDef, evt.RunID)
			run.TraceParent = evt.TraceParent
			run.RootRunID = run.RunID // top-level run is its own tree-root (#377)
			run = markTerminal(run, dag.RunStatusFailed)
			o.saveSnapshot(ctx, run, "")
			return fmt.Errorf("input validation: %w", err)
		}
	}

	run := dag.NewWorkflowRun(wfDef, evt.RunID)
	run.TraceParent = evt.TraceParent
	run.RootRunID = run.RunID // top-level run is its own tree-root (#377)
	run.Input = input

	admission, admitErr := o.admission.Admit(ctx, wfDef, run, input)
	if admitErr != nil {
		return admitErr
	}
	if admission.cancelID != "" {
		o.admission.publishWorkflowCancelledEvent(admission.cancelID)
	}
	run.PriorityOffset = admission.offset
	run.SingletonKey = admission.singletonKey
	switch admission.action {
	case admissionSkip:
		if err := o.persistSkippedRun(
			ctx, run, admission.skippedBy,
		); err != nil {
			return fmt.Errorf("save skipped run: %w", err)
		}
		return nil
	case admissionQueue:
		run.Status = dag.RunStatusPending
		if err := o.saveSnapshot(ctx, run, ""); err != nil {
			return fmt.Errorf("save pending run: %w", err)
		}
		return nil
	}

	run.Status = dag.RunStatusRunning
	if wfDef.Timeout > 0 {
		deadline := time.Now().Add(wfDef.Timeout)
		run.Deadline = &deadline
	}
	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		return fmt.Errorf("save initial run: %w", err)
	}
	o.metrics.runsActive.Add(ctx, 1)
	if err := o.enqueueReady(ctx, wfDef, run); err != nil {
		return err
	}
	o.registerCancelWaiters(ctx, wfDef, run)
	return nil
}

// errStartPayloadHandled signals that resolveStartPayload has already
// persisted a permanent failure for the event and the caller should
// ACK without further processing. Detect with errors.Is.
var errStartPayloadHandled = errors.New("start payload already handled")

// resolveStartPayload decodes evt.Payload into a WorkflowDef and Input.
// Three shapes are accepted, in priority order:
//
//  1. Structured {workflow_def, input} — produced by the API service
//     when a user invokes a workflow manually.
//  2. TriggerEnvelope {trigger, source, workflow_id, ...} — produced
//     by every trigger type (#167). The def is resolved from
//     workflow_defs KV by WorkflowID; the envelope itself becomes the
//     run's Input so workflows can observe how they were fired.
//  3. Bare WorkflowDef — backward compat for direct callers (tests
//     and any embedded users that pre-date the structured shape).
//
// For trigger envelopes referencing a workflow that has no registered
// def, the helper persists a RunStatusFailed snapshot and returns
// errStartPayloadHandled so the caller ACKs the message — redelivery
// would re-fail identically.
func (o *Orchestrator) resolveStartPayload(
	ctx context.Context, evt protocol.Event,
) (dag.WorkflowDef, json.RawMessage, error) {
	var startPayload struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(evt.Payload, &startPayload); err == nil &&
		startPayload.WorkflowDef != nil {
		var wfDef dag.WorkflowDef
		if err := json.Unmarshal(startPayload.WorkflowDef, &wfDef); err != nil {
			return dag.WorkflowDef{}, nil,
				fmt.Errorf("unmarshal WorkflowDef: %w", err)
		}
		return wfDef, startPayload.Input, nil
	}

	if workflowID, ok := decodeTriggerEnvelope(evt.Payload); ok {
		entry, err := o.defKV.Get(ctx, workflowID)
		if err != nil {
			o.persistFailedStartRun(ctx, evt, workflowID,
				fmt.Errorf("resolve trigger workflow def: %w", err))
			return dag.WorkflowDef{}, nil, errStartPayloadHandled
		}
		var wfDef dag.WorkflowDef
		if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
			return dag.WorkflowDef{}, nil,
				fmt.Errorf("unmarshal trigger workflow def: %w", err)
		}
		return wfDef, evt.Payload, nil
	}

	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(evt.Payload, &wfDef); err != nil {
		return dag.WorkflowDef{}, nil,
			fmt.Errorf("unmarshal WorkflowDef: %w", err)
	}
	return wfDef, nil, nil
}

// decodeTriggerEnvelope returns the workflow ID from a TriggerEnvelope
// payload (#167). ok is false for any payload that does not look like
// a trigger envelope so the caller can fall through to the next shape.
func decodeTriggerEnvelope(payload []byte) (string, bool) {
	var env struct {
		Trigger    string `json:"trigger"`
		WorkflowID string `json:"workflow_id"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return "", false
	}
	if env.Trigger == "" || env.WorkflowID == "" {
		return "", false
	}
	return env.WorkflowID, true
}

// persistFailedStartRun records a permanent failure for a
// workflow.started event whose payload could not be turned into a
// runnable WorkflowDef. ACKing the message (the caller returns nil) is
// correct because redelivery would just re-write the same failure.
func (o *Orchestrator) persistFailedStartRun(
	ctx context.Context, evt protocol.Event,
	workflowID string, reason error,
) {
	if evt.RunID == "" {
		panic("persistFailedStartRun: RunID must not be empty")
	}
	if reason == nil {
		panic("persistFailedStartRun: reason must not be nil")
	}
	slog.ErrorContext(ctx,
		"workflow.started: failing run permanently",
		"error", reason,
		"run_id", evt.RunID,
		"workflow_id", workflowID,
	)
	failed := markTerminal(dag.WorkflowRun{
		RunID:       evt.RunID,
		WorkflowID:  workflowID,
		Steps:       map[string]dag.StepState{},
		CreatedAt:   time.Now().UTC(),
		TraceParent: evt.TraceParent,
	}, dag.RunStatusFailed)
	if saveErr := o.saveSnapshot(ctx, failed, ""); saveErr != nil {
		slog.ErrorContext(ctx,
			"workflow.started: save failed-run snapshot",
			"error", saveErr,
			"run_id", evt.RunID,
		)
	}
}

// admissionSkipStepID keys the synthetic step persistSkippedRun uses
// to carry the skip reason (#502). Mirrors reconcileWedged's
// "<reconciler>" synthetic-step pattern -- reusing a fake step ID is
// the only existing precedent for attaching a reason without a
// WorkflowRun schema change.
const admissionSkipStepID = "<admission-skip>"

// persistSkippedRun records a terminal snapshot for a run that was
// never dispatched because a singleton lock (mode: skip) was already
// held by another run. Without this, the run vanishes with no
// snapshot ever written -- `dagnats run status <run-id>` reports
// "not found" for a run start that was acked and silently dropped
// (#502). run must be the already-constructed dag.WorkflowRun for
// this admission (RunID/WorkflowID/TraceParent/CreatedAt populated by
// the caller); its Steps map is replaced here, not mutated in place.
//
// The save error is returned, not merely logged (#506): the caller
// (handleWorkflowStarted) ACKs on nil, so swallowing a transient KV
// write failure here would silently reproduce #502 -- the skip is
// recorded nowhere and the run vanishes. Returning the error lets it
// propagate the same way the admissionQueue and normal-running
// branches already do, so handleEventJS NAKs and NATS redelivers.
// This does not add a durable DLQ backstop for a save that keeps
// failing forever -- that gap is dispatcher-wide and tracked in #508.
func (o *Orchestrator) persistSkippedRun(
	ctx context.Context, run dag.WorkflowRun, skippedBy string,
) error {
	if run.RunID == "" {
		panic("persistSkippedRun: RunID must not be empty")
	}
	if skippedBy == "" {
		panic("persistSkippedRun: skippedBy must not be empty")
	}
	reason := "singleton skip: run already active: " + skippedBy
	slog.WarnContext(ctx,
		"workflow.started: singleton skip -- run already active",
		"run_id", run.RunID,
		"skipped_by", skippedBy,
		"workflow", run.WorkflowID,
	)
	// Fresh map, not an in-place append: NewWorkflowRun pre-populates
	// run.Steps with one Pending entry per real workflow step, and
	// FormatRunStatusWithDef (cli/run.go) iterates run.Steps -- leaving
	// those in would render every real step as stale "pending" forever
	// instead of surfacing this reason. Do not "simplify" this back to
	// mutating the passed-in map.
	run.Steps = map[string]dag.StepState{
		admissionSkipStepID: {
			// Status must be Failed, not Skipped: formatStepLine
			// (cli/run.go) only prints `error: %s` for
			// StepStatusFailed steps. Run-level status below is
			// Cancelled (non-paging); this step-level status is
			// Failed (carries the message) -- a deliberate
			// mismatch. Do not "fix" it into consistency, or the
			// reason text silently stops rendering.
			Status: dag.StepStatusFailed,
			Error:  reason,
		},
	}
	// The lock key on `run` names the OTHER run's lock, not one this
	// run owns -- clear it so a stray ReleaseSingletonLock call can't
	// be misread as this run's lock (harmless today given its RunID
	// guard, but misleading to persist).
	run.SingletonKey = ""
	run = markTerminal(run, dag.RunStatusCancelled)
	if saveErr := o.saveSnapshot(ctx, run, ""); saveErr != nil {
		return fmt.Errorf("save skipped-run snapshot: %w", saveErr)
	}
	return nil
}

// registerCancelWaiters registers one correlator waiter per
// CancelOn entry so a matching external event cancels the run.
func (o *Orchestrator) registerCancelWaiters(
	ctx context.Context, wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
) {
	if o.correlator == nil {
		return
	}
	if run.RunID == "" {
		panic("registerCancelWaiters: RunID must not be empty")
	}
	if run.Input == nil && len(wfDef.CancelOn) > 0 {
		// Input may be nil — Resolve handles it gracefully.
	}
	for i, cancel := range wfDef.CancelOn {
		resolved, err := cancel.Match.Resolve(
			nil, run.Input,
		)
		if err != nil {
			slog.ErrorContext(ctx,
				"cancel match resolve failed",
				"error", err,
			)
			continue
		}
		waiter := EventWaiter{
			RunID:     run.RunID,
			StepID:    fmt.Sprintf("__cancel_%d", i),
			EventType: cancel.Event,
			Match:     resolved,
			Action:    WaiterActionCancel,
		}
		if err := o.correlator.AddWaiter(ctx, waiter); err != nil {
			slog.ErrorContext(ctx,
				"add cancel waiter failed", "error", err,
			)
		}
	}
}

// handleStepCompleted marks the step output in the snapshot, then checks
// whether the workflow is fully complete or new steps have become unblocked.
func (o *Orchestrator) handleStepCompleted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepCompleted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepCompleted: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	// Idempotency guard (#196). A step.completed event for a run
	// already in a terminal state is a redelivery from a JetStream
	// history replay. Without this guard, Advance would re-mark the
	// step Completed and call completeWorkflow, double-decrementing
	// runsActive and republishing workflow.completed.
	if run.Status.IsTerminal() {
		slog.InfoContext(ctx,
			"skipping step.completed for terminal run",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
			"run_status", run.Status.String(),
		)
		return nil
	}

	// Map instances have their own completion logic.
	if isMapInstanceID(evt.StepID) {
		return o.handleMapInstanceCompleted(
			ctx, wfDef, run, evt,
		)
	}

	// Planner steps must materialize output before DAG
	// resolution — short-circuit before Advance.
	stepDef, foundStep := findStepDef(wfDef, evt.StepID)
	if foundStep && stepDef.Type == dag.StepTypePlanner {
		state := run.Steps[evt.StepID]
		state.Status = dag.StepStatusCompleted
		state.Output = evt.Payload
		run.Steps[evt.StepID] = state
		o.releaseTaskSlot(ctx, wfDef, evt.StepID)
		return o.materializePlannerOutput(
			ctx, wfDef, run, stepDef, evt.Payload,
		)
	}

	// Pure core: compute state transition and side effects.
	advEvt := Event{
		Type:    EventStepCompleted,
		StepID:  evt.StepID,
		Payload: evt.Payload,
	}
	run, _ = Advance(wfDef, run, advEvt)

	// Orchestrator-only I/O that Advance cannot handle.
	o.releaseTaskSlot(ctx, wfDef, evt.StepID)
	o.sticky.CreateBinding(ctx, wfDef, run, evt)
	o.recovery.RecoverIfOnFailure(wfDef, &run, evt.StepID)

	if o.recovery.HandleCompensateCompleted(
		ctx, wfDef, &run, evt.StepID, o.saveSnapshot,
	) {
		return nil
	}

	// Recovery may have changed run state (e.g. marking a step
	// Recovered), so use orchestrator's enqueueReady which
	// respects post-recovery state.
	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// markTerminal sets a run's terminal status and stamps CompletedAt in
// one place so no terminal transition can record a finished run while
// leaving CompletedAt nil (which would render the Traces "Duration" as
// an em-dash for a run that has actually finished). Every terminal
// path — complete, fail, loop-step fail, map-step fail, schema-
// validation fail, failed-start — funnels its status change through
// here. Returns the mutated copy because runs are passed by value.
func markTerminal(
	run dag.WorkflowRun, status dag.RunStatus,
) dag.WorkflowRun {
	if run.RunID == "" {
		panic("markTerminal: RunID must not be empty")
	}
	if !status.IsTerminal() {
		panic("markTerminal: status must be terminal")
	}
	run.Status = status
	now := time.Now().UTC()
	run.CompletedAt = &now
	return run
}

// RootRunIDOf is the SINGLE definition of a run's tree-root (#377):
// run.RootRunID when set, else run.RunID (the run self-roots). Legacy
// snapshots predating the RootRunID field deserialize to "" and so
// self-root, which is correct for any top-level run. Pure and total
// modulo the RunID invariant. Exported so the control-plane register
// path (internal/api) derives the root by the identical rule.
func RootRunIDOf(run dag.WorkflowRun) string {
	// Exactly one programmer-error precondition: a run with no RunID is
	// malformed and could not have a meaningful root. There is no second
	// invariant to assert — RootRunID is a free-form optional field, so
	// any value (including "") is valid input handled by the fallback.
	if run.RunID == "" {
		panic("RootRunIDOf: RunID must not be empty")
	}
	if run.RootRunID != "" {
		return run.RootRunID
	}
	return run.RunID
}

// completeWorkflow marks the run complete, saves, publishes the event,
// adjusts metrics, and releases concurrency slot.
func (o *Orchestrator) completeWorkflow(
	ctx context.Context, run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("completeWorkflow: RunID must not be empty")
	}
	run = markTerminal(run, dag.RunStatusCompleted)
	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		return err
	}
	o.admission.ReleaseSingletonLock(ctx, run)
	o.sticky.DeleteBinding(ctx, run.RunID)
	wfAttr := metric.WithAttributes(
		attribute.String("workflow", run.WorkflowID),
	)
	o.metrics.runsActive.Add(ctx, -1, wfAttr)
	o.metrics.runsCompleted.Add(ctx, 1, wfAttr)
	if err := o.admission.ReleaseRunIfConcurrency(
		ctx, run.WorkflowID,
	); err != nil {
		return err
	}
	if o.admission.HasConcurrency() {
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			slog.ErrorContext(ctx,
				"failed to start next pending run",
				"error", err,
				"workflow_id", run.WorkflowID,
			)
		}
	}
	if err := o.publishWorkflowCompleted(ctx, run.RunID); err != nil {
		return err
	}
	return o.notifyParentIfChild(ctx, run, nil)
}

// startNextPendingRun finds the oldest pending run for a workflow and
// transitions it to Running. Called after ReleaseRun to enable queue
// progression. No-op if no pending runs exist.
func (o *Orchestrator) startNextPendingRun(
	ctx context.Context, workflowID string,
) error {
	if workflowID == "" {
		panic("startNextPendingRun: workflowID must not be empty")
	}
	if o.store == nil {
		panic("startNextPendingRun: store must not be nil")
	}

	runID, found, err := o.findOldestPendingRun(ctx, workflowID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return o.transitionPendingToRunning(ctx, runID)
}

// findOldestPendingRun scans workflow_runs KV for the oldest pending
// run for the given workflow. Returns (runID, true, nil) when found.
func (o *Orchestrator) findOldestPendingRun(
	ctx context.Context, workflowID string,
) (string, bool, error) {
	if workflowID == "" {
		panic("findOldestPendingRun: workflowID must not be empty")
	}
	if o.store == nil {
		panic("findOldestPendingRun: store must not be nil")
	}
	keys, err := o.store.kv.Keys(ctx)
	if err != nil {
		return "", false, fmt.Errorf("list run keys: %w", err)
	}

	entries, err := natsutil.ParallelGetJS(
		o.store.kv, keys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return "", false, fmt.Errorf(
			"parallel get runs: %w", err,
		)
	}

	var oldestRun dag.WorkflowRun
	var foundPending bool

	for _, entry := range entries {
		var run dag.WorkflowRun
		if err := json.Unmarshal(entry.Value(), &run); err != nil {
			continue
		}
		if run.WorkflowID != workflowID {
			continue
		}
		if run.Status != dag.RunStatusPending {
			continue
		}
		if !foundPending ||
			run.EffectiveTime().Before(oldestRun.EffectiveTime()) {
			oldestRun = run
			foundPending = true
		}
	}

	if !foundPending {
		return "", false, nil
	}
	return oldestRun.RunID, true, nil
}

// transitionPendingToRunning loads a pending run, acquires concurrency,
// transitions to Running, and enqueues entry steps.
func (o *Orchestrator) transitionPendingToRunning(
	ctx context.Context, runID string,
) error {
	if runID == "" {
		panic("transitionPendingToRunning: runID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, runID)
	if err != nil {
		return fmt.Errorf("load pending run %q: %w", runID, err)
	}

	if wfDef.Concurrency != nil {
		acquired, err := o.admission.AcquireRun(
			ctx, wfDef.Name, wfDef.Concurrency.MaxRuns,
		)
		if err != nil {
			return fmt.Errorf("acquire for pending run: %w", err)
		}
		if !acquired {
			return nil // Slot not available (shouldn't happen)
		}
	}

	run.Status = dag.RunStatusRunning
	if wfDef.Timeout > 0 {
		deadline := time.Now().Add(wfDef.Timeout)
		run.Deadline = &deadline
	}
	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		return fmt.Errorf("save running run: %w", err)
	}
	o.metrics.runsActive.Add(ctx, 1)
	return o.enqueueReady(ctx, wfDef, run)
}

// handleStepContinue re-enqueues an agent-loop step for another iteration.
// Uses Advance for iteration increment and MaxIterations check, then
// applies LoopStartedAt tracking, MaxDuration enforcement, and
// LoopDelay scheduling that only the orchestrator can do.
func (o *Orchestrator) handleStepContinue(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepContinue: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepContinue: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !found {
		return fmt.Errorf(
			"step %q not found in workflow def", evt.StepID,
		)
	}

	// Pure core: increment iterations and check MaxIterations.
	advEvt := Event{
		Type:   EventStepContinue,
		StepID: evt.StepID,
	}
	run, effects := Advance(wfDef, run, advEvt)

	// If Advance produced a FailWorkflow effect, MaxIterations
	// was exceeded — fail via orchestrator's full failure path.
	if hasEffect[FailWorkflow](effects) {
		state := run.Steps[evt.StepID]
		return o.failLoopStep(
			ctx, run, evt.StepID, state, state.Error,
		)
	}

	// Orchestrator-only: track loop start time and enforce
	// MaxDuration, which the pure core does not handle.
	state := run.Steps[evt.StepID]
	if state.Iterations == 1 {
		state.LoopStartedAt = time.Now().UTC()
	}
	if exceeded, reason := checkLoopBounds(
		stepDef, state,
	); exceeded {
		return o.failLoopStep(
			ctx, run, evt.StepID, state, reason,
		)
	}
	// Re-stamp a fresh per-dispatch nonce for this iteration (#380): it rides
	// this snapshot save (no extra write) and is threaded onto the
	// PublishIteration payload so the iteration's control-plane calls bind to
	// this dispatch.
	state.DispatchNonce = runid.New()
	run.Steps[evt.StepID] = state

	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.publishContinueTask(
		ctx, run, stepDef, state,
	)
}

// publishContinueTask resolves input and publishes the next
// iteration task, with optional LoopDelay scheduling.
func (o *Orchestrator) publishContinueTask(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error {
	if stepDef.ID == "" {
		panic("publishContinueTask: stepDef.ID must not be empty")
	}
	if run.RunID == "" {
		panic("publishContinueTask: RunID must not be empty")
	}
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for step %q: %w", stepDef.ID, err,
		)
	}
	loopCfg, _ := dag.ParseAgentLoopConfig(stepDef)
	// state.DispatchNonce was stamped fresh by handleContinue before the
	// snapshot save, so it is already persisted; thread it (with the run's
	// workflow name) through both the delayed and immediate re-enqueue (#380).
	if loopCfg.LoopDelay > 0 {
		return o.scheduleDelayedIteration(
			ctx, run.RunID, run.WorkflowID, stepDef, input,
			state.Iterations, loopCfg.LoopDelay, state.DispatchNonce,
		)
	}
	return o.publisher.PublishIteration(
		ctx, run.RunID, stepDef, input, state.Iterations,
		run.WorkflowID, state.DispatchNonce,
	)
}

// scheduleDelayedIteration defers re-enqueue via a context-aware
// timer goroutine. Cancels cleanly if context expires.
func (o *Orchestrator) scheduleDelayedIteration(
	ctx context.Context,
	runID string,
	workflowName string,
	stepDef dag.StepDef,
	input []byte,
	iteration int,
	delay time.Duration,
	dispatchNonce string,
) error {
	if runID == "" {
		panic(
			"scheduleDelayedIteration: runID must not be empty",
		)
	}
	if delay <= 0 {
		panic(
			"scheduleDelayedIteration: delay must be positive",
		)
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pubErr := o.publisher.PublishIteration(
				ctx, runID, stepDef, input, iteration,
				workflowName, dispatchNonce,
			)
			if pubErr != nil {
				slog.ErrorContext(ctx,
					"delayed iteration publish failed",
					"error", pubErr,
					"run_id", runID,
					"step_id", stepDef.ID,
				)
			}
		}
	}()
	return nil
}

// findStepDef locates a step definition by ID within a workflow def.
func findStepDef(
	wfDef dag.WorkflowDef, stepID string,
) (dag.StepDef, bool) {
	for _, s := range wfDef.Steps {
		if s.ID == stepID {
			return s, true
		}
	}
	return dag.StepDef{}, false
}

// checkLoopBounds returns (true, reason) when the step has hit its
// MaxIterations or MaxDuration ceiling. Both limits are checked.
func checkLoopBounds(
	stepDef dag.StepDef, state dag.StepState,
) (bool, string) {
	cfg, err := dag.ParseAgentLoopConfig(stepDef)
	if err != nil {
		return false, ""
	}
	if cfg.MaxIterations > 0 &&
		state.Iterations >= cfg.MaxIterations {
		return true, fmt.Sprintf(
			"agent loop exceeded max iterations (%d)",
			cfg.MaxIterations,
		)
	}
	if cfg.MaxDuration > 0 &&
		!state.LoopStartedAt.IsZero() &&
		time.Since(state.LoopStartedAt) >= cfg.MaxDuration {
		return true, fmt.Sprintf(
			"agent loop exceeded max duration (%s)",
			cfg.MaxDuration,
		)
	}
	return false, ""
}

// failLoopStep marks the step and run as failed, saves state, publishes
// a workflow.failed event, and adjusts metrics.
func (o *Orchestrator) failLoopStep(
	ctx context.Context,
	run dag.WorkflowRun,
	stepID string,
	state dag.StepState,
	reason string,
) error {
	if stepID == "" {
		panic("failLoopStep: stepID must not be empty")
	}
	if reason == "" {
		panic("failLoopStep: reason must not be empty")
	}
	state.Status = dag.StepStatusFailed
	state.Error = reason
	run.Steps[stepID] = state
	run = markTerminal(run, dag.RunStatusFailed)
	if err := o.saveSnapshot(ctx, run, stepID); err != nil {
		return err
	}
	wfAttr := metric.WithAttributes(
		attribute.String("workflow", run.WorkflowID),
	)
	o.metrics.runsActive.Add(ctx, -1, wfAttr)
	o.metrics.runsFailed.Add(ctx, 1, wfAttr)
	if err := o.admission.ReleaseRunIfConcurrency(
		ctx, run.WorkflowID,
	); err != nil {
		return err
	}
	if o.admission.HasConcurrency() {
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			slog.ErrorContext(ctx,
				"failed to start next pending run",
				"error", err,
				"workflow_id", run.WorkflowID,
			)
		}
	}
	if err := o.publishWorkflowFailed(ctx, run.RunID); err != nil {
		return err
	}
	return o.notifyParentIfChild(ctx, run, fmt.Errorf("%s", reason))
}

// parseFailPayload parses a StepFailedPayload from event payload.
// Falls back to treating raw strings as retriable errors for
// backward compatibility with old workers that send plain strings.
func parseFailPayload(
	data json.RawMessage,
) protocol.StepFailedPayload {
	if len(data) == 0 {
		return protocol.StepFailedPayload{
			FailureType: protocol.FailureTypeRetriable,
		}
	}
	var payload protocol.StepFailedPayload
	if err := json.Unmarshal(data, &payload); err == nil &&
		payload.Error != "" {
		if payload.FailureType == "" {
			payload.FailureType = protocol.FailureTypeRetriable
		}
		return payload
	}
	// Backward compat: raw quoted string
	var rawErr string
	if err := json.Unmarshal(data, &rawErr); err == nil {
		return protocol.StepFailedPayload{
			Error:       rawErr,
			FailureType: protocol.FailureTypeRetriable,
		}
	}
	return protocol.StepFailedPayload{
		Error:       string(data),
		FailureType: protocol.FailureTypeRetriable,
	}
}

// handleStepFailed processes a step failure event. Parses the
// structured StepFailedPayload and branches on FailureType:
// non-retriable skips retries, retry-after schedules exact delay,
// retriable uses existing backoff behavior.
func (o *Orchestrator) handleStepFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepFailed: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	// Idempotency guard (#196). Same shape as the guard in
	// handleStepCompleted — a step.failed for a terminal run is a
	// redelivery and re-running the failure path would double-fire
	// failWorkflow + DLQ publish + runsFailed metric.
	if run.Status.IsTerminal() {
		slog.InfoContext(ctx,
			"skipping step.failed for terminal run",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
			"run_status", run.Status.String(),
		)
		return nil
	}

	// Check if this is a map instance failure.
	if isMapInstanceID(evt.StepID) {
		return o.handleMapInstanceFailed(
			ctx, wfDef, run, evt,
		)
	}

	// Attempts is owned by step.queued / step.started lifecycle events
	// (max() rule in handleStepQueued/handleStepStarted). step.failed
	// fires within an attempt and must not touch the counter.
	state := run.Steps[evt.StepID]

	failPayload := parseFailPayload(evt.Payload)
	state.Error = failPayload.Error

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	policy := dag.ResolveRetryPolicy(wfDef, stepDef)

	if failPayload.FailureType ==
		protocol.FailureTypeNonRetriable {
		return o.dispatchNonRetriableFailure(
			ctx, wfDef, run, stepDef, evt, failPayload,
		)
	}

	// Retry-after: schedule exact delay if retries remain.
	if failPayload.FailureType ==
		protocol.FailureTypeRetryAfter {
		o.metrics.failRetryAfter.Add(ctx, 1)
		return o.handleRetryAfter(
			ctx, wfDef, &run, stepDef, &state,
			evt.StepID, failPayload.RetryAfterMs, policy,
		)
	}

	return o.dispatchRetriableFailure(
		ctx, wfDef, run, state, stepDef, evt, policy,
	)
}

// dispatchNonRetriableFailure is the inner branch of handleStepFailed for
// failure_type=non_retriable: increments metrics, runs the pure-core
// Advance to record the terminal step state, preserves run.Status so
// on-failure recovery handlers can intercept, and delegates the rest to
// the recovery manager.
func (o *Orchestrator) dispatchNonRetriableFailure(
	ctx context.Context, wfDef dag.WorkflowDef, run dag.WorkflowRun,
	stepDef dag.StepDef, evt protocol.Event,
	failPayload protocol.StepFailedPayload,
) error {
	if evt.RunID == "" {
		panic("dispatchNonRetriableFailure: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("dispatchNonRetriableFailure: StepID must not be empty")
	}

	// Non-retriable: use pure core for step state transition,
	// then delegate to recovery manager for failure handling.
	// Advance sets run.Status=Failed, but recovery may intercept
	// with an on-failure handler, so preserve the original status.
	o.metrics.failNonRetriable.Add(ctx, 1)
	slog.InfoContext(ctx,
		"step failed permanently (non-retriable)",
		"run_id", evt.RunID,
		"step_id", evt.StepID,
	)
	advEvt := Event{
		Type:   EventStepFailed,
		StepID: evt.StepID,
		FailPayload: FailPayload{
			Error:       failPayload.Error,
			FailureType: FailureTypeNonRetriable,
		},
	}
	prevStatus := run.Status
	run, _ = Advance(wfDef, run, advEvt)
	// Recovery may handle the failure with an on-failure
	// handler — don't prematurely mark the run Failed.
	run.Status = prevStatus
	state := run.Steps[evt.StepID]
	return o.recovery.HandlePermanentFailure(
		ctx, wfDef, run, stepDef, state, evt.StepID,
		o.saveSnapshot, o.failWorkflow,
		o.notifyParentIfChild, o.releaseTaskSlot,
	)
}

// dispatchRetriableFailure is the inner branch of handleStepFailed for
// failure_type=retriable: if attempts remain, save snapshot and schedule
// retry backoff; if exhausted, transition step to Failed and call
// HandlePermanentFailure. Pre-#147 this branch silently saved without
// scheduling — the explicit name pins the post-#147 contract.
func (o *Orchestrator) dispatchRetriableFailure(
	ctx context.Context, wfDef dag.WorkflowDef, run dag.WorkflowRun,
	state dag.StepState, stepDef dag.StepDef, evt protocol.Event,
	policy *dag.RetryPolicy,
) error {
	if evt.RunID == "" {
		panic("dispatchRetriableFailure: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("dispatchRetriableFailure: StepID must not be empty")
	}

	// Retriable (default): schedule the next attempt via the durable
	// SLEEP_TIMERS path. dag.CalculateDelay drives the wait; the
	// timer re-publishes the task so step.queued / step.started will
	// fire fresh for the new attempt. Without this, attempts were
	// recorded but never re-dispatched (issue #147).
	if policy != nil && state.Attempts <= policy.MaxAttempts {
		run.Steps[evt.StepID] = state
		if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
			return err
		}
		return o.scheduleRetryBackoff(
			ctx, evt.RunID, evt.StepID, stepDef, policy, run, wfDef.Name,
		)
	}

	state.Status = dag.StepStatusFailed
	run.Steps[evt.StepID] = state
	return o.recovery.HandlePermanentFailure(
		ctx, wfDef, run, stepDef, state, evt.StepID,
		o.saveSnapshot, o.failWorkflow,
		o.notifyParentIfChild, o.releaseTaskSlot,
	)
}

// handleRetryAfter handles a retry-after failure: schedules an
// exact delay if retries remain, otherwise permanent failure.
func (o *Orchestrator) handleRetryAfter(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepDef dag.StepDef,
	state *dag.StepState,
	stepID string,
	retryAfterMs int64,
	policy *dag.RetryPolicy,
) error {
	if stepID == "" {
		panic("handleRetryAfter: stepID must not be empty")
	}
	if run.RunID == "" {
		panic("handleRetryAfter: RunID must not be empty")
	}
	if policy != nil && state.Attempts <= policy.MaxAttempts {
		run.Steps[stepID] = *state
		if err := o.saveSnapshot(ctx, *run, stepID); err != nil {
			return err
		}
		return o.scheduleRetryAfter(
			ctx, run.RunID, stepID, stepDef,
			retryAfterMs, *run, wfDef.Name,
		)
	}
	state.Status = dag.StepStatusFailed
	run.Steps[stepID] = *state
	return o.recovery.HandlePermanentFailure(
		ctx, wfDef, *run, stepDef, *state, stepID,
		o.saveSnapshot, o.failWorkflow,
		o.notifyParentIfChild, o.releaseTaskSlot,
	)
}

// scheduleRetryAfter schedules a timer to re-publish the task
// after the worker-requested delay via SLEEP_TIMERS.
func (o *Orchestrator) scheduleRetryAfter(
	ctx context.Context,
	runID string, stepID string,
	stepDef dag.StepDef,
	retryAfterMs int64,
	run dag.WorkflowRun,
	workflowName string,
) error {
	if runID == "" {
		panic("scheduleRetryAfter: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleRetryAfter: stepID must not be empty")
	}
	if retryAfterMs <= 0 {
		retryAfterMs = 100
	}
	if retryAfterMs > 3_600_000 {
		retryAfterMs = 3_600_000
	}
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for retry-after step %q: %w",
			stepID, err,
		)
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:       TimerActionRetryAfter,
		RunID:        runID,
		StepID:       stepID,
		DurationMs:   retryAfterMs,
		TaskType:     stepDef.Task,
		Input:        input,
		Attempt:      run.Steps[stepID].Attempts,
		WorkflowName: workflowName,
	})
}

// scheduleRetryBackoff schedules a timer that re-publishes the task
// after the policy-derived delay. Mirrors scheduleRetryAfter; the
// only difference is the delay source (dag.CalculateDelay vs the
// worker-supplied retryAfterMs) and the timer Action. Both ride the
// same SLEEP_TIMERS plumbing, which keeps the retry path durable
// across orchestrator restarts.
func (o *Orchestrator) scheduleRetryBackoff(
	ctx context.Context,
	runID string, stepID string,
	stepDef dag.StepDef,
	policy *dag.RetryPolicy,
	run dag.WorkflowRun,
	workflowName string,
) error {
	if runID == "" {
		panic("scheduleRetryBackoff: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleRetryBackoff: stepID must not be empty")
	}
	if policy == nil {
		panic("scheduleRetryBackoff: policy must not be nil")
	}
	// state.Attempts is 1-indexed and counts attempts that have
	// already started (see handleStepStarted's max() rule). The
	// upcoming attempt is Attempts+1, so the delay before the next
	// attempt is CalculateDelay(policy, Attempts) — e.g. for
	// Attempts=1 with exponential the delay is InitialDelay (the
	// 1st retry), and for Attempts=2 it is InitialDelay*Multiplier.
	attempts := run.Steps[stepID].Attempts
	if attempts < 1 {
		panic("scheduleRetryBackoff: attempts must be >= 1")
	}
	// Unreachable for any def that passed dag.Validate (it bounds
	// every policy's MaxAttempts); tripping it means corrupted run
	// state, and failing loudly beats minting unbounded timer
	// msg-ids. The bridge's taskAttemptCountMax mirrors this bound.
	if attempts > dag.RetryAttemptCountMax {
		panic("scheduleRetryBackoff: attempts exceeds RetryAttemptCountMax")
	}
	delay := dag.CalculateDelay(*policy, attempts)
	delayMs := delay.Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	if delayMs > 3_600_000 {
		delayMs = 3_600_000
	}
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for retry-backoff step %q: %w",
			stepID, err,
		)
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:       TimerActionRetryBackoff,
		RunID:        runID,
		StepID:       stepID,
		DurationMs:   delayMs,
		TaskType:     stepDef.Task,
		Input:        input,
		Attempt:      attempts,
		WorkflowName: workflowName,
	})
}

// scheduleStepTimeout schedules a watchdog timer that fires a
// synthetic step.failed (retriable) if the step is still on the
// same attempt when stepDef.Timeout elapses (issue #140). Caller
// gates on stepDef.Timeout > 0 — entering with zero is a bug.
//
// The Attempt field carries the attempt number that was current
// when the timer was scheduled. fireStepTimeout drops the fire if
// the step has since moved to a later attempt or terminal status.
// MsgId encodes Attempt so a step that runs N attempts gets N
// independent timers, none deduped against the others.
func (o *Orchestrator) scheduleStepTimeout(
	ctx context.Context,
	runID, stepID string,
	stepDef dag.StepDef,
	attempt int,
) error {
	if runID == "" {
		panic("scheduleStepTimeout: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleStepTimeout: stepID must not be empty")
	}
	if stepDef.Timeout <= 0 {
		panic("scheduleStepTimeout: Timeout must be > 0")
	}
	if attempt < 1 {
		panic("scheduleStepTimeout: attempt must be >= 1")
	}
	delayMs := stepDef.Timeout.Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	if delayMs > 24*60*60*1000 {
		delayMs = 24 * 60 * 60 * 1000
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionStepTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: delayMs,
		TaskType:   stepDef.Task,
		Attempt:    attempt,
	})
}

// fireStepTimeout publishes a synthetic step.failed (retriable)
// for a step whose stepDef.Timeout elapsed while it was still
// running on the same attempt that scheduled the timer.
//
// Staleness is the load-bearing invariant: by the time the timer
// fires, the step may have completed, failed via worker, been
// cancelled, or progressed to a later attempt. Any of those means
// the timer is observing a prior life of the step — drop it.
//
// AttemptNumber on the synthetic event piggybacks on Event.NATSMsgID,
// scoping JetStream dedup to (run, step, attempt) so the timer fire
// can coexist with a worker step.failed that landed concurrently —
// engine treats both arrivals as one logical failure for that attempt.
func (o *Orchestrator) fireStepTimeout(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireStepTimeout: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic("fireStepTimeout: StepID must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	run, err := o.store.Load(ctx, tm.RunID)
	if err != nil {
		return // No state to act on — nothing to fail.
	}
	state, ok := run.Steps[tm.StepID]
	if !ok {
		return // Unknown step — drop.
	}
	// Staleness: only fire if the step is still Running on the
	// exact attempt the timer was scheduled for.
	if state.Status != dag.StepStatusRunning {
		return
	}
	if state.Attempts != tm.Attempt {
		return
	}
	dur := time.Duration(tm.DurationMs) * time.Millisecond
	payload := protocol.StepFailedPayload{
		Error: fmt.Sprintf(
			"step timeout exceeded (%s)", dur,
		),
		FailureType: protocol.FailureTypeRetriable,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepFailed,
		tm.RunID, tm.StepID, data,
	)
	evt.AttemptNumber = tm.Attempt
	if err := publishLifecycleEvent(ctx, o.tp, evt); err != nil {
		slog.WarnContext(ctx,
			"step timeout: publish step.failed",
			"run_id", tm.RunID,
			"step_id", tm.StepID,
			"error", err,
		)
	}
}

// failWorkflow marks the workflow as permanently failed and releases
// resources. Extracted to avoid duplication between failure paths.
func (o *Orchestrator) failWorkflow(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error {
	run = markTerminal(run, dag.RunStatusFailed)
	if err := o.saveSnapshot(ctx, run, stepDef.ID); err != nil {
		return err
	}
	o.admission.ReleaseSingletonLock(ctx, run)
	o.sticky.DeleteBinding(ctx, run.RunID)
	wfAttr := metric.WithAttributes(
		attribute.String("workflow", run.WorkflowID),
	)
	o.metrics.runsActive.Add(ctx, -1, wfAttr)
	o.metrics.runsFailed.Add(ctx, 1, wfAttr)
	if err := o.admission.ReleaseRunIfConcurrency(
		ctx, run.WorkflowID,
	); err != nil {
		return err
	}
	if o.admission.HasConcurrency() {
		if err := o.startNextPendingRun(ctx, run.WorkflowID); err != nil {
			slog.ErrorContext(ctx,
				"failed to start next pending run",
				"error", err,
				"workflow_id", run.WorkflowID,
			)
		}
	}
	if err := o.publishWorkflowFailed(ctx, run.RunID); err != nil {
		return err
	}
	// Best-effort definition reload so PublishDeadLetter can resolve
	// the step's input via dag.ResolveInput. A missing def degrades
	// to using run.Input directly — replay still works for single-step
	// workflows, which is the firestorm-dataworks shape.
	wfDef, _ := o.loadDef(ctx, run.WorkflowID)
	// Reconciler-driven failure paths use a synthetic stepDef with
	// no Task name; for those, leave taskSubject empty and let
	// PublishDeadLetter derive a best-effort default.
	taskSubject := ""
	if stepDef.Task != "" {
		taskSubject = o.publisher.StepSubject(stepDef, run.RunID)
	}
	o.recovery.PublishDeadLetter(ctx, run, wfDef, stepDef, state,
		taskSubject)
	return o.notifyParentIfChild(
		ctx, run, fmt.Errorf("%s", state.Error),
	)
}

// handleWorkflowCancelled marks the run and all in-flight steps as
// cancelled, saves state, and adjusts metrics.
func (o *Orchestrator) handleWorkflowCancelled(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowCancelled: RunID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}
	if run.Status != dag.RunStatusRunning {
		return nil
	}

	run = markTerminal(run, dag.RunStatusCancelled)
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusQueued ||
			state.Status == dag.StepStatusRunning ||
			state.Status == dag.StepStatusPending {
			state.Status = dag.StepStatusCancelled
			run.Steps[id] = state
		}
	}

	// Release task concurrency slots for cancelled steps that
	// were queued or running (they held a slot).
	o.releaseCancelledTaskSlots(ctx, wfDef, run)

	// Clean up approval tokens for cancelled approval steps.
	o.approval.CleanupTokens(ctx, wfDef, run)

	if o.correlator != nil {
		o.correlator.RemoveWaitersForRun(ctx, run.RunID)
	}

	o.cascadeCancelChildren(ctx, wfDef, run)
	o.admission.ReleaseSingletonLock(ctx, run)
	o.sticky.DeleteBinding(ctx, run.RunID)

	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		return err
	}
	o.metrics.runsActive.Add(ctx, -1)
	if err := o.admission.ReleaseRunIfConcurrency(
		ctx, run.WorkflowID,
	); err != nil {
		return err
	}
	if o.admission.HasConcurrency() {
		if err := o.startNextPendingRun(
			ctx, run.WorkflowID,
		); err != nil {
			slog.ErrorContext(ctx,
				"failed to start next pending run",
				"error", err,
				"workflow_id", run.WorkflowID,
			)
		}
	}
	return o.notifyParentIfChild(ctx, run, fmt.Errorf("cancelled"))
}

// cascadeCancelChildren publishes cancellation events for all
// non-detached child workflows that are still running. Detached
// children have no ParentRunID so they are not cancelled.
func (o *Orchestrator) cascadeCancelChildren(
	ctx context.Context,
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) {
	if run.RunID == "" {
		panic("cascadeCancelChildren: RunID must not be empty")
	}
	if run.Steps == nil {
		panic("cascadeCancelChildren: Steps must not be nil")
	}

	for _, stepDef := range wfDef.Steps {
		if stepDef.Type != dag.StepTypeSubWorkflow {
			continue
		}
		state := run.Steps[stepDef.ID]
		if state.ChildRunID == "" {
			continue
		}
		childRun, err := o.store.Load(ctx, state.ChildRunID)
		if err != nil {
			continue
		}
		// Detached children have no ParentRunID — skip them.
		if childRun.ParentRunID == "" {
			continue
		}
		if childRun.Status != dag.RunStatusRunning {
			continue
		}
		o.publishCancelEvent(ctx, state.ChildRunID)
	}
}

// publishCancelEvent publishes EventWorkflowCancelled for a run.
func (o *Orchestrator) publishCancelEvent(
	ctx context.Context, runID string,
) {
	if runID == "" {
		panic("publishCancelEvent: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// MaxNestingDepth caps how deeply runs may spawn children. Exported so
// the api control-plane spawn endpoint can enforce the SAME cap
// synchronously before publishing a spawn event — there is exactly one
// depth-checked spawn path, and this is its single source of truth.
const MaxNestingDepth = 3

// maxNestingDepth is the package-internal alias retained so the existing
// orchestrator call sites read unchanged.
const maxNestingDepth = MaxNestingDepth

// nestingDepth walks the parent chain to compute current depth.
// Returns 0 for top-level runs, 1 for first child, etc.
func (o *Orchestrator) nestingDepth(
	ctx context.Context, runID string,
) int {
	depth := 0
	currentID := runID
	for i := 0; i < maxNestingDepth+1; i++ {
		run, err := o.store.Load(ctx, currentID)
		if err != nil || run.ParentRunID == "" {
			break
		}
		depth++
		currentID = run.ParentRunID
	}
	return depth
}

// handleWorkflowSpawn creates a child WorkflowRun from a spawn event.
// The child is linked to the parent via ParentRunID and ParentStepID.
func (o *Orchestrator) handleWorkflowSpawn(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWorkflowSpawn: RunID must not be empty")
	}
	var payload struct {
		ChildRunID    string          `json:"child_run_id"`
		ChildWorkflow string          `json:"child_workflow"`
		ParentStepID  string          `json:"parent_step_id"`
		Input         json.RawMessage `json:"input"`
		Detach        bool            `json:"detach"`
	}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal spawn payload: %w", err)
	}
	if payload.ChildRunID == "" {
		panic("handleWorkflowSpawn: child_run_id must not be empty")
	}

	// Enforce max nesting depth by walking the parent chain.
	// The child would be at depth+1, so reject when depth+1 > max.
	depth := o.nestingDepth(ctx, evt.RunID)
	if depth+1 >= maxNestingDepth {
		slog.ErrorContext(ctx,
			"spawn rejected: max nesting depth exceeded",
			"error", fmt.Errorf(
				"depth %d >= max %d", depth, maxNestingDepth,
			),
		)
		return fmt.Errorf(
			"max nesting depth %d exceeded", maxNestingDepth,
		)
	}

	return o.createChildRun(ctx, evt.RunID, payload.ChildRunID,
		payload.ChildWorkflow, payload.ParentStepID,
		payload.Input, payload.Detach)
}

// createChildRun loads the child workflow def, creates the child run,
// and enqueues its entry-point steps. For detached children the parent
// link is omitted so they run independently.
func (o *Orchestrator) createChildRun(
	ctx context.Context,
	parentRunID string,
	childRunID string,
	childWorkflow string,
	parentStepID string,
	input json.RawMessage,
	detach bool,
) error {
	if childRunID == "" {
		panic("createChildRun: childRunID must not be empty")
	}
	if childWorkflow == "" {
		panic("createChildRun: childWorkflow must not be empty")
	}

	entry, err := o.defKV.Get(ctx, childWorkflow)
	if err != nil {
		return fmt.Errorf(
			"load child workflow def %q: %w",
			childWorkflow, err,
		)
	}
	var childDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &childDef); err != nil {
		return fmt.Errorf("unmarshal child def: %w", err)
	}

	childRun := dag.NewWorkflowRun(childDef, childRunID)
	childRun.Input = input
	childRun.Status = dag.RunStatusRunning
	if !detach {
		childRun.ParentRunID = parentRunID
		childRun.ParentStepID = parentStepID
		// Inherit the tree-root from the parent so every run in a spawn
		// tree carries the same RootRunID (#377). O(1) parent load — the
		// root rule is transitive, so the parent already holds the root.
		// A genuinely-missing parent (ErrRunNotFound) means this child
		// heads a new tree, so it self-roots — mirroring nestingDepth,
		// which treats a missing parent as the chain root. Only a real
		// store fault (not a miss) propagates as a wrapped error.
		parent, err := o.store.Load(ctx, parentRunID)
		switch {
		case err == nil:
			childRun.RootRunID = RootRunIDOf(parent)
		case errors.Is(err, ErrRunNotFound):
			childRun.RootRunID = childRunID
		default:
			return fmt.Errorf(
				"load parent run %q for root derivation: %w",
				parentRunID, err,
			)
		}
	} else {
		childRun.RootRunID = childRunID // detached child self-roots (#377)
	}

	if err := o.saveSnapshot(ctx, childRun, ""); err != nil {
		return err
	}

	o.metrics.runsActive.Add(ctx, 1)
	return o.enqueueReady(ctx, childDef, childRun)
}

// notifyParentIfChild publishes a child completion or failure event on the
// parent's history subject when this run has a parent. No-op for top-level.
func (o *Orchestrator) notifyParentIfChild(
	ctx context.Context, run dag.WorkflowRun, childErr error,
) error {
	if run.ParentRunID == "" {
		return nil
	}

	eventType := protocol.EventWorkflowChildCompleted
	if childErr != nil {
		eventType = protocol.EventWorkflowChildFailed
	}

	payload, err := json.Marshal(map[string]any{
		"child_run_id":   run.RunID,
		"parent_step_id": run.ParentStepID,
		"error":          errString(childErr),
	})
	if err != nil {
		return fmt.Errorf("marshal child event payload: %w", err)
	}

	// Use NewStepEvent keyed by ParentStepID so that multiple child
	// completions from different sub-workflow steps produce distinct
	// dedup IDs instead of colliding on a single workflow-level MsgID.
	evt := protocol.NewStepEvent(
		eventType, run.ParentRunID, run.ParentStepID, payload,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	// JSPublishMsgEvent marshals evt after injecting trace context;
	// leave msg.Data empty so the persisted body carries TraceParent.
	_ = payload // payload is folded into evt above
	if _, err := o.tp.JSPublishMsgEvent(ctx, msg, &evt); err != nil {
		return fmt.Errorf("publish child event: %w", err)
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// enqueueReady resolves all currently-ready steps and publishes one task
// message per step. Steps with satisfied SkipIf conditions are marked Skipped
// instead of enqueued, potentially unblocking further downstream steps.
func (o *Orchestrator) enqueueReady(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("enqueueReady: RunID must not be empty")
	}
	ctx, span := o.tracer.Start(ctx,
		"dagnats.engine enqueueReady",
		trace.WithAttributes(
			attribute.String("run_id", run.RunID),
			attribute.String("workflow_name", wfDef.Name),
		),
	)
	defer span.End()

	ready, skipped, finished, err := o.resolveReadySteps(
		ctx, wfDef, &run,
	)
	if err != nil {
		return err
	}
	if finished {
		return nil
	}
	span.SetAttributes(
		attribute.Int64("ready_steps_count", int64(len(ready))),
	)
	if len(ready) == 0 && len(skipped) == 0 {
		return nil
	}
	if len(ready) == 0 {
		return nil
	}
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		// Stamp a fresh per-dispatch nonce (#380): it rides this snapshot
		// write (no extra KV write) and is mirrored onto the TaskPayload in
		// PublishBatch so the worker can prove it received this dispatch.
		state.DispatchNonce = runid.New()
		run.Steps[step.ID] = state
	}
	// Multi-step batch — no single owning step, so pass "".
	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		return err
	}
	o.publishStepQueuedEvents(ctx, run, ready)
	return o.dispatchReadySteps(ctx, wfDef, run, ready)
}

// resolveReadySteps determines which steps should be dispatched in this
// pass: marks newly-skipped steps, returns early if the run completes
// purely via skips, then resolves the ready set (excluding skips) and
// applies the per-run concurrency cap. Mutates run.Steps in place for
// skipped steps. The `finished` flag tells the caller the workflow has
// already been completed (or is over the cap with nothing to do) and no
// further dispatch is required.
func (o *Orchestrator) resolveReadySteps(
	ctx context.Context, wfDef dag.WorkflowDef, run *dag.WorkflowRun,
) (ready []dag.StepDef, skipped []dag.StepDef, finished bool, err error) {
	if run == nil {
		panic("resolveReadySteps: run must not be nil")
	}
	if run.RunID == "" {
		panic("resolveReadySteps: RunID must not be empty")
	}
	completed := completedSet(*run)
	queued := queuedSet(*run)

	// Process skipped steps first — they may unblock downstream steps
	// that would otherwise not appear in ResolveReady.
	skipped = dag.ResolveSkipped(wfDef, completed, queued, run.Steps)
	for _, step := range skipped {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusSkipped
		run.Steps[step.ID] = state
	}
	if len(skipped) > 0 {
		// Recompute completed set after marking skips.
		completed = completedSet(*run)
		if dag.IsComplete(wfDef, completed) {
			if err := o.completeWorkflow(ctx, *run); err != nil {
				return nil, skipped, true, err
			}
			return nil, skipped, true, nil
		}
	}

	ready = dag.ResolveReady(wfDef, completed, queued)
	// Exclude steps that were just marked as skipped.
	filtered := make([]dag.StepDef, 0, len(ready))
	for _, step := range ready {
		if run.Steps[step.ID].Status != dag.StepStatusSkipped {
			filtered = append(filtered, step)
		}
	}
	ready = filtered

	// Per-run step concurrency: cap how many steps dispatch.
	if wfDef.Concurrency != nil &&
		wfDef.Concurrency.MaxSteps > 0 {
		activeCount := countActiveSteps(*run)
		available := wfDef.Concurrency.MaxSteps - activeCount
		if available <= 0 {
			return nil, skipped, true, nil
		}
		if len(ready) > available {
			ready = ready[:available]
		}
	}
	return ready, skipped, false, nil
}

// publishStepQueuedEvents emits step.queued BEFORE the task dispatch —
// otherwise on a fast transport the worker can pick up the task and emit
// step.started before the engine's step.queued lands in the history
// stream, producing out-of-order timestamps. The publish-before-dispatch
// ordering matches the semantic ordering. Failure to publish is logged
// but doesn't roll back the dispatch (the task is the load-bearing
// artifact; step.queued is observability). Map / sleep / wait /
// sub-workflow / approval steps have their own typed lifecycle events
// and are excluded here.
func (o *Orchestrator) publishStepQueuedEvents(
	ctx context.Context, run dag.WorkflowRun, ready []dag.StepDef,
) {
	if run.RunID == "" {
		panic("publishStepQueuedEvents: RunID must not be empty")
	}
	if o.js == nil {
		panic("publishStepQueuedEvents: js must not be nil")
	}
	for _, step := range ready {
		if step.Type != dag.StepTypeNormal && step.Type != dag.StepTypeAgentLoop {
			continue
		}
		qEvt := protocol.NewStepEvent(
			protocol.EventStepQueued, run.RunID, step.ID, nil,
		)
		qEvt.AttemptNumber = 1
		if err := publishLifecycleEvent(ctx, o.tp, qEvt); err != nil {
			slog.ErrorContext(ctx, "failed to publish step.queued",
				"error", err,
				"run_id", run.RunID,
				"step_id", step.ID,
			)
			// Do NOT roll back the dispatch on publish failure —
			// step.queued is observability-only; missing it is not
			// correctness-fatal. See spec §3.
		}
	}
}

// dispatchReadySteps separates map steps from normal steps and
// dispatches each appropriately.
func (o *Orchestrator) dispatchReadySteps(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	ready []dag.StepDef,
) error {
	var normalSteps []dag.StepDef
	for _, step := range ready {
		switch step.Type {
		case dag.StepTypeSubWorkflow:
			if err := o.enqueueSubWorkflow(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeMap:
			if err := o.enqueueMapStep(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeSleep:
			if err := o.enqueueSleepStep(
				ctx, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeWaitForEvent:
			if err := o.enqueueWaitForEventStep(
				ctx, wfDef, &run, step,
			); err != nil {
				return err
			}
		case dag.StepTypeApproval:
			if err := o.approval.Enqueue(
				ctx, wfDef, &run, step,
				o.saveSnapshot,
			); err != nil {
				return err
			}
		case dag.StepTypeRespond:
			if err := o.enqueueRespondStep(
				ctx, &run, step,
			); err != nil {
				return err
			}
		default:
			normalSteps = append(normalSteps, step)
		}
	}
	if len(normalSteps) > 0 {
		return o.publisher.PublishBatch(
			ctx, run.RunID, wfDef, run, normalSteps,
		)
	}
	return nil
}

// saveSnapshot saves the run state to KV and records the duration.
// Records the duration with workflow + step labels so the
// drilldown surface can split latency per (workflow, step) — the
// step granularity is what lets operators isolate a single hot
// step's KV-write pressure from the workflow's global average.
// stepID may be empty when the save is not associated with a
// specific step (workflow init, completion, failure, child run
// spawn). RunID is intentionally not attached — unbounded
// cardinality would blow up the metrics store. See orchMetrics
// docs and metricLabelAllowlist for the label policy.
func (o *Orchestrator) saveSnapshot(
	ctx context.Context, run dag.WorkflowRun, stepID string,
) error {
	if run.RunID == "" {
		panic("saveSnapshot: RunID must not be empty")
	}
	if ctx == nil {
		panic("saveSnapshot: ctx must not be nil")
	}
	start := time.Now()
	err := o.store.Save(ctx, run)
	elapsed := float64(time.Since(start).Milliseconds())
	o.metrics.snapshotDuration.Record(ctx, elapsed,
		metric.WithAttributes(
			attribute.String("workflow", run.WorkflowID),
			attribute.String("step", stepID),
		),
	)
	return err
}

// loadRunAndDef loads the workflow definition and current run snapshot.
func (o *Orchestrator) loadRunAndDef(
	ctx context.Context, runID string,
) (dag.WorkflowDef, dag.WorkflowRun, error) {
	if runID == "" {
		panic("loadRunAndDef: runID must not be empty")
	}
	run, err := o.store.Load(ctx, runID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("load run %q: %w", runID, err)
	}
	entry, err := o.defKV.Get(ctx, run.WorkflowID)
	if err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("load workflow def %q: %w",
				run.WorkflowID, err)
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
		return dag.WorkflowDef{}, dag.WorkflowRun{},
			fmt.Errorf("unmarshal workflow def %q: %w",
				run.WorkflowID, err)
	}
	wfDef = dag.EffectiveDef(wfDef, run)
	return wfDef, run, nil
}

// publishWorkflowCompleted publishes a workflow.completed event.
func (o *Orchestrator) publishWorkflowCompleted(
	ctx context.Context, runID string,
) error {
	if runID == "" {
		panic("publishWorkflowCompleted: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCompleted, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf(
			"marshal workflow.completed event: %w", err,
		)
	}
	_, err = o.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	return err
}

// publishWorkflowFailed publishes a workflow.failed event.
func (o *Orchestrator) publishWorkflowFailed(
	ctx context.Context, runID string,
) error {
	if runID == "" {
		panic("publishWorkflowFailed: runID must not be empty")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowFailed, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf(
			"marshal workflow.failed event: %w", err,
		)
	}
	_, err = o.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	return err
}

// parseTraceparent reads traceparent from *nats.Msg header first,
// falling back to the event field. Used by tests.
func parseTraceparent(
	msg *nats.Msg, evt *protocol.Event,
) (traceID, spanID string, ok bool) {
	if msg.Header != nil {
		if h := msg.Header.Get("traceparent"); h != "" {
			return splitTraceparent(h)
		}
	}
	if evt.TraceParent != "" {
		return splitTraceparent(evt.TraceParent)
	}
	return "", "", false
}

// splitTraceparent parses "00-{traceID}-{spanID}-{flags}" into parts.
func splitTraceparent(
	tp string,
) (traceID, spanID string, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// enqueueSleepStep marks the step as Running, publishes a
// SleepStarted event, and schedules a durable timer. No worker
// is involved — the timer fires the completion event directly.
func (o *Orchestrator) enqueueSleepStep(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeSleep {
		panic("enqueueSleepStep: step is not a Sleep step")
	}
	if run.RunID == "" {
		panic("enqueueSleepStep: RunID must not be empty")
	}

	sleepCfg, err := dag.ParseSleepConfig(step)
	if err != nil {
		return fmt.Errorf("enqueueSleepStep: %w", err)
	}

	// Mark step as Running and record wake time.
	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	wakeAt := time.Now().Add(sleepCfg.Duration)
	state.WakeAt = &wakeAt
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run, step.ID); err != nil {
		return err
	}

	// Publish sleep started event for observability.
	o.publishSleepStarted(ctx, run.RunID, step.ID)

	// Schedule durable timer via NakWithDelay.
	durationMs := sleepCfg.Duration.Milliseconds()
	if durationMs <= 0 {
		durationMs = 1
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionSleepComplete,
		RunID:      run.RunID,
		StepID:     step.ID,
		DurationMs: durationMs,
	})
}

// publishSleepStarted publishes an EventStepSleepStarted event.
func (o *Orchestrator) publishSleepStarted(
	ctx context.Context, runID string, stepID string,
) {
	if runID == "" {
		panic("publishSleepStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("publishSleepStarted: stepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepSleepStarted,
		runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// isMapInstanceID returns true if the step ID is a map instance
// (format: "{stepID}.map.{index}").
func isMapInstanceID(stepID string) bool {
	return strings.Contains(stepID, ".map.")
}

// parseMapInstanceID splits a compound map instance ID into the
// base step ID and instance index. Panics if the format is invalid.
func parseMapInstanceID(stepID string) (string, int) {
	parts := strings.Split(stepID, ".map.")
	if len(parts) != 2 {
		panic("parseMapInstanceID: invalid format: " + stepID)
	}
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("parseMapInstanceID: invalid index: " + parts[1])
	}
	return parts[0], idx
}

// mapInstanceID constructs a compound step ID for a map instance.
func mapInstanceID(stepID string, index int) string {
	return stepID + ".map." + strconv.Itoa(index)
}

// enqueueMapStep reads the upstream output as a JSON array and
// publishes one task per element. MapInstances track each item's
// state on the Map step's StepState.
func (o *Orchestrator) enqueueMapStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeMap {
		panic("enqueueMapStep: step is not a Map step")
	}
	if len(step.DependsOn) != 1 {
		panic("enqueueMapStep: Map step must have exactly one dep")
	}

	// Read upstream output as JSON array.
	upstream := run.Steps[step.DependsOn[0]]
	var items []json.RawMessage
	if err := json.Unmarshal(upstream.Output, &items); err != nil {
		return fmt.Errorf(
			"map step %q: upstream output is not a JSON array: %w",
			step.ID, err,
		)
	}

	if err := o.validateAndInitMapInstances(
		ctx, run, step, items,
	); err != nil {
		return err
	}

	return o.publishMapTasks(ctx, run.RunID, wfDef.Name, step, items)
}

// validateAndInitMapInstances checks MaxItems and initializes
// the MapInstances slice on the step state.
func (o *Orchestrator) validateAndInitMapInstances(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	items []json.RawMessage,
) error {
	mapCfg, err := dag.ParseMapConfig(step)
	if err != nil {
		panic("validateAndInitMapInstances: " + err.Error())
	}
	maxItems := mapCfg.MaxItems
	if len(items) > maxItems {
		return fmt.Errorf(
			"map step %q: %d items exceeds MaxItems %d",
			step.ID, len(items), maxItems,
		)
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	state.MapInstances = make(
		[]dag.MapInstanceState, len(items),
	)
	for i := range items {
		state.MapInstances[i] = dag.MapInstanceState{
			Status: dag.StepStatusQueued,
		}
	}
	run.Steps[step.ID] = state
	return o.saveSnapshot(ctx, *run, step.ID)
}

// publishMapTasks publishes one task per map item concurrently.
// workflowName is the parent run's workflow definition name (wfDef.Name),
// used ONLY for telemetry -- see the strip comment below for why passing
// the real name here is safe.
func (o *Orchestrator) publishMapTasks(
	ctx context.Context,
	runID string,
	workflowName string,
	step dag.StepDef,
	items []json.RawMessage,
) error {
	var g errgroup.Group
	for i, item := range items {
		i, item := i, item
		instanceStep := step
		instanceStep.ID = mapInstanceID(step.ID, i)
		// #513: map instances are data-parallel work items that must
		// categorically never hold a control-plane handle (#380). The STRIP
		// below -- not the workflowName value passed to Publish -- is what
		// enforces that deny-by-default: stripControlPlaneCapability removes
		// "control-plane" from this instance's RequiredCapabilities
		// unconditionally, regardless of grant policy. Because
		// effectiveCapabilities (grant_policy.go) is strip-only and
		// short-circuits when control-plane is already absent from caps, the
		// workflowName argument becomes structurally irrelevant to this
		// instance's grant decision from here on -- which is exactly what
		// makes it safe to pass the REAL workflow name through for
		// telemetry instead of forging "". Do not revert to an empty name
		// here, and do not delete the strip below: either change would
		// silently reconnect telemetry to the grant key or regrant
		// control-plane to map instances.
		instanceStep.RequiredCapabilities = stripControlPlaneCapability(
			step.RequiredCapabilities,
		)
		// A fresh nonce keeps the run-binding field populated though
		// instances do not call the control plane (#380).
		nonce := runid.New()
		g.Go(func() error {
			return o.publisher.Publish(
				ctx, runID, instanceStep, item, 0, workflowName, nonce,
			)
		})
	}
	return g.Wait()
}

// handleMapInstanceCompleted updates a single map instance's state.
// When all instances are done, collects outputs and completes the
// Map step.
func (o *Orchestrator) handleMapInstanceCompleted(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) error {
	baseID, idx := parseMapInstanceID(evt.StepID)
	state := run.Steps[baseID]

	if idx < 0 || idx >= len(state.MapInstances) {
		return fmt.Errorf(
			"map instance index %d out of range for %q",
			idx, baseID,
		)
	}

	state.MapInstances[idx].Status = dag.StepStatusCompleted
	state.MapInstances[idx].Output = evt.Payload
	run.Steps[baseID] = state

	if !allMapInstancesDone(state.MapInstances) {
		return o.saveSnapshot(ctx, run, baseID)
	}

	return o.collectMapOutputs(ctx, wfDef, run, baseID, state)
}

// allMapInstancesDone returns true when every instance is completed.
func allMapInstancesDone(instances []dag.MapInstanceState) bool {
	for _, inst := range instances {
		if inst.Status != dag.StepStatusCompleted {
			return false
		}
	}
	return true
}

// collectMapOutputs gathers outputs from all instances into an
// ordered JSON array and completes the Map step.
func (o *Orchestrator) collectMapOutputs(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
) error {
	outputs := make(
		[]json.RawMessage, len(state.MapInstances),
	)
	for i, inst := range state.MapInstances {
		outputs[i] = inst.Output
	}
	collected, err := json.Marshal(outputs)
	if err != nil {
		return fmt.Errorf("marshal map outputs: %w", err)
	}

	state.Status = dag.StepStatusCompleted
	state.Output = collected
	run.Steps[baseID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, baseID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// handleMapInstanceFailed marks the Map step as failed immediately
// (fail-fast). Remaining instances will expire via AckWait.
func (o *Orchestrator) handleMapInstanceFailed(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) error {
	baseID, idx := parseMapInstanceID(evt.StepID)
	state := run.Steps[baseID]

	if idx < 0 || idx >= len(state.MapInstances) {
		return fmt.Errorf(
			"map instance index %d out of range for %q",
			idx, baseID,
		)
	}

	state.MapInstances[idx].Status = dag.StepStatusFailed
	if evt.Payload != nil {
		state.MapInstances[idx].Error = string(evt.Payload)
	}

	// Fail-fast: mark the Map step as failed.
	state.Status = dag.StepStatusFailed
	state.Error = fmt.Sprintf(
		"map instance %d failed: %s", idx,
		state.MapInstances[idx].Error,
	)
	run.Steps[baseID] = state

	return o.failMapStep(ctx, wfDef, run, baseID, state)
}

// failMapStep handles the on-failure handler or fails the workflow.
func (o *Orchestrator) failMapStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
) error {
	stepDef, _ := findStepDef(wfDef, baseID)

	// Check for on-failure handler.
	if stepDef.OnFailure != "" {
		return o.runMapOnFailure(
			ctx, wfDef, run, baseID, state, stepDef,
		)
	}

	// No on-failure — fail the workflow.
	run = markTerminal(run, dag.RunStatusFailed)
	if err := o.saveSnapshot(ctx, run, baseID); err != nil {
		return err
	}
	wfAttr := metric.WithAttributes(
		attribute.String("workflow", run.WorkflowID),
	)
	o.metrics.runsActive.Add(ctx, -1, wfAttr)
	o.metrics.runsFailed.Add(ctx, 1, wfAttr)
	if err := o.publishWorkflowFailed(ctx, run.RunID); err != nil {
		return err
	}
	taskSubject := ""
	if stepDef.Task != "" {
		taskSubject = o.publisher.StepSubject(stepDef, run.RunID)
	}
	o.recovery.PublishDeadLetter(ctx, run, wfDef, stepDef, state,
		taskSubject)
	return o.notifyParentIfChild(
		ctx, run, fmt.Errorf("%s", state.Error),
	)
}

// runMapOnFailure enqueues the on-failure step for a failed map.
func (o *Orchestrator) runMapOnFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	baseID string,
	state dag.StepState,
	stepDef dag.StepDef,
) error {
	onFailStep, found := findStepDef(
		wfDef, stepDef.OnFailure,
	)
	if !found {
		return nil
	}
	ofState := run.Steps[onFailStep.ID]
	ofState.Status = dag.StepStatusQueued
	ofState.DispatchNonce = runid.New()
	run.Steps[onFailStep.ID] = ofState
	if err := o.saveSnapshot(ctx, run, onFailStep.ID); err != nil {
		return err
	}
	errorInput := []byte(fmt.Sprintf(
		`{"failed_step":"%s","error":%q}`,
		baseID, state.Error,
	))
	return o.publisher.Publish(
		ctx, run.RunID, onFailStep, errorInput, 0,
		run.WorkflowID, ofState.DispatchNonce,
	)
}

// enqueueWaitForEventStep marks the step as Running, resolves the
// match condition, publishes a WaitStarted event, registers the
// waiter with the correlator, and schedules a timeout timer.
func (o *Orchestrator) enqueueWaitForEventStep(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeWaitForEvent {
		panic("enqueueWaitForEventStep: wrong step type")
	}
	if run.RunID == "" {
		panic("enqueueWaitForEventStep: RunID must not be empty")
	}

	opts, err := dag.ParseWaitForEventConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: WaitForEvent config is nil", step.ID,
		)
	}

	resolvedMatch, err := o.resolveWaitMatch(
		opts.Match, run,
	)
	if err != nil {
		return fmt.Errorf(
			"resolve match for step %q: %w", step.ID, err,
		)
	}

	return o.startWaitForEvent(
		ctx, run, step, &opts, resolvedMatch,
	)
}

// resolveWaitMatch resolves a builder-time Match to a runtime
// ResolvedMatch using step outputs and workflow input.
func (o *Orchestrator) resolveWaitMatch(
	match dag.Match,
	run *dag.WorkflowRun,
) (dag.ResolvedMatch, error) {
	if run == nil {
		panic("resolveWaitMatch: run must not be nil")
	}
	if run.Steps == nil {
		panic("resolveWaitMatch: run.Steps must not be nil")
	}
	stepOutputs := make(map[string][]byte, len(run.Steps))
	for id, state := range run.Steps {
		if state.Output != nil {
			stepOutputs[id] = state.Output
		}
	}
	return match.Resolve(stepOutputs, run.Input)
}

// startWaitForEvent marks the step Running, publishes
// WaitStarted, registers the correlator waiter, and schedules
// the timeout timer. Extracted to keep parent under 70 lines.
func (o *Orchestrator) startWaitForEvent(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	opts *dag.WaitForEventOpts,
	resolvedMatch dag.ResolvedMatch,
) error {
	if run.RunID == "" {
		panic("startWaitForEvent: RunID must not be empty")
	}
	if step.ID == "" {
		panic("startWaitForEvent: step.ID must not be empty")
	}

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run, step.ID); err != nil {
		return err
	}

	o.publishWaitStarted(ctx, run.RunID, step.ID)

	waiter := EventWaiter{
		RunID:     run.RunID,
		StepID:    step.ID,
		EventType: opts.Event,
		Match:     resolvedMatch,
	}
	if err := o.correlator.AddWaiter(ctx, waiter); err != nil {
		return fmt.Errorf("add waiter: %w", err)
	}

	return o.scheduleWaitTimeout(ctx, run.RunID, step.ID, opts.Timeout)
}

// scheduleWaitTimeout schedules a timer for the wait-for-event
// timeout. Uses the same SleepTimer infrastructure as sleep steps.
func (o *Orchestrator) scheduleWaitTimeout(
	ctx context.Context,
	runID string, stepID string, timeout time.Duration,
) error {
	if runID == "" {
		panic("scheduleWaitTimeout: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleWaitTimeout: stepID must not be empty")
	}
	durationMs := timeout.Milliseconds()
	if durationMs <= 0 {
		durationMs = 1
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionWaitTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: durationMs,
	})
}

// publishWaitStarted publishes an EventStepWaitStarted event.
func (o *Orchestrator) publishWaitStarted(
	ctx context.Context, runID string, stepID string,
) {
	if runID == "" {
		panic("publishWaitStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("publishWaitStarted: stepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepWaitStarted,
		runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// handleWaitTimeout marks the wait step as completed with a timeout
// output so downstream steps can branch on it. Timeout is not a
// failure — it completes the step with {"timeout": true}.
func (o *Orchestrator) handleWaitTimeout(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWaitTimeout: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleWaitTimeout: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	// Only process if the step is still Running (not already matched).
	if state.Status != dag.StepStatusRunning {
		return nil
	}

	state.Status = dag.StepStatusCompleted
	state.Output = []byte(`{"timeout":true}`)
	run.Steps[evt.StepID] = state

	// Remove the waiter since the step timed out.
	if o.correlator != nil {
		o.correlator.RemoveWaitersForRun(ctx, evt.RunID)
	}

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// enqueueSubWorkflow resolves input, generates a child run ID, and
// publishes a spawn event. For detached sub-workflows the parent step
// completes immediately; otherwise it stays Running until the child
// finishes.
func (o *Orchestrator) enqueueSubWorkflow(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
) error {
	if step.Type != dag.StepTypeSubWorkflow {
		panic("enqueueSubWorkflow: wrong step type")
	}
	if run.RunID == "" {
		panic("enqueueSubWorkflow: RunID must not be empty")
	}

	cfg, err := dag.ParseSubWorkflowConfig(step)
	if err != nil {
		return fmt.Errorf("parse sub-workflow config: %w", err)
	}

	input, err := dag.ResolveInput(step, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for step %q: %w", step.ID, err,
		)
	}
	childRunID := nuid.Next()

	if err := o.spawnChild(
		ctx, wfDef, run, step, cfg, input, childRunID,
	); err != nil {
		return err
	}

	// Detached sub-workflows complete the parent step immediately,
	// which may unblock downstream steps or complete the workflow.
	if cfg.Detach {
		completed := completedSet(*run)
		if dag.IsComplete(wfDef, completed) {
			return o.completeWorkflow(ctx, *run)
		}
		return o.enqueueReady(ctx, wfDef, *run)
	}
	return nil
}

// spawnChild marks the parent step state, saves the snapshot, and
// publishes the spawn event. Extracted to keep enqueueSubWorkflow
// within the 70-line limit.
func (o *Orchestrator) spawnChild(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	step dag.StepDef,
	cfg dag.SubWorkflowConfig,
	input []byte,
	childRunID string,
) error {
	if childRunID == "" {
		panic("spawnChild: childRunID must not be empty")
	}
	if step.ID == "" {
		panic("spawnChild: step.ID must not be empty")
	}

	state := run.Steps[step.ID]
	if cfg.Detach {
		state.Status = dag.StepStatusCompleted
		state.ChildRunID = childRunID
		state.Output = []byte(fmt.Sprintf(
			`{"child_run_id":%q}`, childRunID,
		))
	} else {
		state.Status = dag.StepStatusRunning
		state.ChildRunID = childRunID
	}
	run.Steps[step.ID] = state
	if err := o.saveSnapshot(ctx, *run, step.ID); err != nil {
		return err
	}

	return o.publishSpawnEvent(
		ctx, run.RunID, step.ID, cfg, input, childRunID,
	)
}

// publishSpawnEvent publishes EventWorkflowSpawn to the history
// stream with the child run metadata in the payload.
func (o *Orchestrator) publishSpawnEvent(
	ctx context.Context,
	parentRunID string,
	parentStepID string,
	cfg dag.SubWorkflowConfig,
	input []byte,
	childRunID string,
) error {
	if parentRunID == "" {
		panic("publishSpawnEvent: parentRunID must not be empty")
	}
	if parentStepID == "" {
		panic("publishSpawnEvent: parentStepID must not be empty")
	}

	payload, err := json.Marshal(map[string]interface{}{
		"child_run_id":   childRunID,
		"child_workflow": cfg.Workflow,
		"parent_step_id": parentStepID,
		"input":          json.RawMessage(input),
		"detach":         cfg.Detach,
	})
	if err != nil {
		return fmt.Errorf("marshal spawn payload: %w", err)
	}

	evt := protocol.NewStepEvent(
		protocol.EventWorkflowSpawn,
		parentRunID, parentStepID, payload,
	)
	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	if _, err := o.tp.JSPublishMsgEvent(ctx, msg, &evt); err != nil {
		return fmt.Errorf("publish spawn event: %w", err)
	}
	return nil
}

// handleChildCompleted processes EventWorkflowChildCompleted: loads
// the child run's terminal output, marks the parent step Completed,
// and enqueues the next ready steps.
func (o *Orchestrator) handleChildCompleted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleChildCompleted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleChildCompleted: StepID must not be empty")
	}

	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already handled or cancelled.
	}

	output, err := o.loadChildTerminalOutputs(ctx, state.ChildRunID)
	if err != nil {
		return fmt.Errorf("load child outputs: %w", err)
	}

	state.Status = dag.StepStatusCompleted
	state.Output = output
	run.Steps[evt.StepID] = state

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// loadChildTerminalOutputs loads the child run and its workflow def,
// finds terminal steps (steps no other step depends on), and returns
// their outputs. One terminal step returns raw output; multiple
// returns a JSON map keyed by step ID.
func (o *Orchestrator) loadChildTerminalOutputs(
	ctx context.Context, childRunID string,
) ([]byte, error) {
	if childRunID == "" {
		panic("loadChildTerminalOutputs: childRunID empty")
	}
	childDef, childRun, err := o.loadRunAndDef(ctx, childRunID)
	if err != nil {
		return nil, err
	}
	return collectTerminalOutputs(childDef, childRun)
}

// collectTerminalOutputs finds steps that no other step depends on
// and returns their outputs. Single terminal returns raw output;
// multiple terminals return a JSON map keyed by step ID.
func collectTerminalOutputs(
	def dag.WorkflowDef, run dag.WorkflowRun,
) ([]byte, error) {
	if len(def.Steps) == 0 {
		panic("collectTerminalOutputs: def has no steps")
	}
	if run.Steps == nil {
		panic("collectTerminalOutputs: run.Steps is nil")
	}
	depTargets := make(map[string]bool, len(def.Steps))
	for _, step := range def.Steps {
		for _, dep := range step.DependsOn {
			depTargets[dep] = true
		}
	}
	var terminals []dag.StepDef
	const maxTerminals = 1000
	for _, step := range def.Steps {
		if !depTargets[step.ID] {
			terminals = append(terminals, step)
		}
		if len(terminals) > maxTerminals {
			break
		}
	}
	if len(terminals) == 1 {
		return run.Steps[terminals[0].ID].Output, nil
	}
	collected := make(
		map[string]json.RawMessage, len(terminals),
	)
	for _, step := range terminals {
		collected[step.ID] = run.Steps[step.ID].Output
	}
	return json.Marshal(collected)
}

// handleChildFailed processes EventWorkflowChildFailed: marks the
// parent step Failed and delegates to failWorkflow.
func (o *Orchestrator) handleChildFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleChildFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleChildFailed: StepID must not be empty")
	}

	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	if state.Status != dag.StepStatusRunning {
		return nil // Already handled or cancelled.
	}

	var payload struct {
		Error string `json:"error"`
	}
	if evt.Payload != nil {
		if err := json.Unmarshal(
			evt.Payload, &payload,
		); err != nil {
			return fmt.Errorf(
				"unmarshal child failed payload: %w", err,
			)
		}
	}

	state.Status = dag.StepStatusFailed
	state.Error = "child workflow failed: " + payload.Error
	run.Steps[evt.StepID] = state

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	return o.failWorkflow(ctx, run, stepDef, state)
}

// completedSet returns a set of step IDs whose status is Completed,
// Skipped, or Recovered. All three count as "resolved" for downstream
// dependency resolution and workflow completion checks.
func completedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("completedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		if state.Status == dag.StepStatusCompleted ||
			state.Status == dag.StepStatusSkipped ||
			state.Status == dag.StepStatusRecovered {
			result[id] = true
		}
	}
	return result
}

// queuedSet returns a set of step IDs whose status is Queued or beyond.
func queuedSet(run dag.WorkflowRun) map[string]bool {
	if run.Steps == nil {
		panic("queuedSet: run.Steps must not be nil")
	}
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		switch state.Status {
		case dag.StepStatusQueued, dag.StepStatusRunning,
			dag.StepStatusCompleted, dag.StepStatusFailed,
			dag.StepStatusSkipped:
			result[id] = true
		}
	}
	return result
}

// releaseTaskSlot releases a task concurrency slot for the given
// step if MaxTaskConcurrency is configured.
func (o *Orchestrator) releaseTaskSlot(
	ctx context.Context, wfDef dag.WorkflowDef, stepID string,
) {
	if !o.admission.HasConcurrency() {
		return
	}
	stepDef, found := findStepDef(wfDef, stepID)
	if !found || stepDef.MaxTaskConcurrency <= 0 {
		return
	}
	if err := o.admission.ReleaseTask(
		ctx, stepDef.Task,
	); err != nil {
		slog.ErrorContext(ctx,
			"release task slot failed",
			"error", err,
			"step_id", stepID,
		)
	}
}

// releaseCancelledTaskSlots releases task concurrency slots for
// all steps that were cancelled while queued or running.
func (o *Orchestrator) releaseCancelledTaskSlots(
	ctx context.Context,
	wfDef dag.WorkflowDef, run dag.WorkflowRun,
) {
	if !o.admission.HasConcurrency() {
		return
	}
	for id, state := range run.Steps {
		if state.Status != dag.StepStatusCancelled {
			continue
		}
		stepDef, found := findStepDef(wfDef, id)
		if !found || stepDef.MaxTaskConcurrency <= 0 {
			continue
		}
		if err := o.admission.ReleaseTask(
			ctx, stepDef.Task,
		); err != nil {
			slog.ErrorContext(ctx,
				"release cancelled task slot failed",
				"error", err,
				"step_id", id,
			)
		}
	}
}

// countActiveSteps counts steps that are currently queued or running.
func countActiveSteps(run dag.WorkflowRun) int {
	if run.Steps == nil {
		panic("countActiveSteps: run.Steps must not be nil")
	}
	count := 0
	for _, state := range run.Steps {
		if state.Status == dag.StepStatusQueued ||
			state.Status == dag.StepStatusRunning {
			count++
		}
	}
	return count
}

// handleStepStarted transitions the step from Queued to Running and
// updates the attempt counter. Monotonic: refuses to regress a
// terminal state — a stale step.started arriving after the engine
// already saw step.completed/step.failed is logged and ignored.
//
// Attempts uses max() rule so out-of-order delivery cannot decrement
// the counter; a higher AttemptNumber wins.
func (o *Orchestrator) handleStepStarted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepStarted: evt.RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepStarted: evt.StepID must not be empty")
	}

	run, err := o.store.Load(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run %q: %w", evt.RunID, err)
	}
	state, ok := run.Steps[evt.StepID]
	if !ok {
		slog.WarnContext(ctx,
			"step.started for unknown step",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
		)
		return nil
	}

	// Monotonic guard — don't regress a terminal state.
	if state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed {
		slog.WarnContext(ctx,
			"stale step.started ignored — step is terminal",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
			"current_status", state.Status,
			"event_attempt", evt.AttemptNumber,
		)
		return nil
	}

	attemptCountBefore := state.Attempts
	state.Status = dag.StepStatusRunning
	if evt.AttemptNumber > state.Attempts {
		state.Attempts = evt.AttemptNumber
	}
	// Postcondition: the max() rule above is what keeps per-attempt
	// retry timer msg-ids distinct (#381) — a regression to "assign"
	// would let out-of-order step.started decrement the counter.
	if state.Attempts < attemptCountBefore {
		panic("handleStepStarted: Attempts must be non-decreasing")
	}
	run.Steps[evt.StepID] = state
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	// Schedule the per-step watchdog (issue #140). Every
	// step.started arms a fresh timer; stale fires from prior
	// attempts are dropped by fireStepTimeout's staleness guard.
	// Skipping the watchdog on def-load failure is acceptable: the
	// run is already saved as Running, and the next step.started
	// (e.g. on retry) will re-arm.
	wfDef, err := o.loadDef(ctx, run.WorkflowID)
	if err != nil {
		return nil
	}
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !found || stepDef.Timeout <= 0 {
		return nil
	}
	return o.scheduleStepTimeout(
		ctx, evt.RunID, evt.StepID, stepDef, state.Attempts,
	)
}

// loadDef fetches and unmarshals a WorkflowDef from defKV. Split
// out from loadRunAndDef so callers that already have the run can
// skip the redundant snapshot load.
func (o *Orchestrator) loadDef(
	ctx context.Context, workflowID string,
) (dag.WorkflowDef, error) {
	if workflowID == "" {
		panic("loadDef: workflowID must not be empty")
	}
	if o.defKV == nil {
		panic("loadDef: defKV must not be nil")
	}
	entry, err := o.defKV.Get(ctx, workflowID)
	if err != nil {
		return dag.WorkflowDef{},
			fmt.Errorf("load workflow def %q: %w",
				workflowID, err)
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
		return dag.WorkflowDef{},
			fmt.Errorf("unmarshal workflow def %q: %w",
				workflowID, err)
	}
	return wfDef, nil
}

// handleStepQueued is mostly a no-op during normal operation — the
// engine's dispatch path already set Status to Queued before it
// emitted this event. The handler exists for state recovery on
// engine restart, where the history stream is replayed and the
// engine reconstructs run state from events alone.
//
// Monotonic: refuses to roll back from Running, Completed, Failed.
func (o *Orchestrator) handleStepQueued(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepQueued: evt.RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepQueued: evt.StepID must not be empty")
	}

	run, err := o.store.Load(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run %q: %w", evt.RunID, err)
	}
	state, ok := run.Steps[evt.StepID]
	if !ok {
		slog.WarnContext(ctx,
			"step.queued for unknown step",
			"run_id", evt.RunID, "step_id", evt.StepID,
		)
		return nil
	}
	if state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed ||
		state.Status == dag.StepStatusRunning {
		// Already past Queued — don't roll back.
		return nil
	}
	attemptCountBefore := state.Attempts
	state.Status = dag.StepStatusQueued
	if evt.AttemptNumber > state.Attempts {
		state.Attempts = evt.AttemptNumber
	}
	// Postcondition: same max()-rule guard as handleStepStarted —
	// Attempts is the input to per-attempt retry timer msg-ids (#381).
	if state.Attempts < attemptCountBefore {
		panic("handleStepQueued: Attempts must be non-decreasing")
	}
	run.Steps[evt.StepID] = state
	return o.saveSnapshot(ctx, run, evt.StepID)
}
