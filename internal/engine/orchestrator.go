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
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// historyRedeliverSchedule bounds WORKFLOW_HISTORY redelivery (#508).
// Indexes the explicit NakWithDelay in handleEventJS (the dominant,
// explicit-NAK path). We deliberately do NOT set ConsumerConfig.BackOff:
// BackOff overrides the per-attempt AckWait, causing spurious ack-timeout
// redeliveries and #196-class duplicate processing, and BackOff is inert
// on NAK'd messages anyway. MaxDeliver is the only consumer-level knob;
// this schedule is the sole escalation source. len IS the MaxDeliver cap
// — keep them defined together. Normal retries index entries 0-6 (the
// first 7): sum(entries[0:7]) = 335s, the ~5.6min production window
// across 7 retries before the 8th delivery hits the MaxDeliver cap and
// dead-letters instead of NAKing. The final entry (180s) is never used
// as a normal per-delivery retry delay — it is reused only as the NAK
// delay in the DLQ-publish-failure fallback (nakOrDeadLetterHistory).
//
// AckWait on this consumer is deliberately left unset (NATS's 30s
// default) and is out of scope for #508 — see the SETTLED contract's
// Non-goal #6. Changing it changes redelivery/duplicate-processing
// behavior for the whole consumer and needs its own Ousterhout review.
var historyRedeliverSchedule = []time.Duration{
	5 * time.Second, 10 * time.Second, 20 * time.Second,
	30 * time.Second, 60 * time.Second, 90 * time.Second,
	120 * time.Second, 180 * time.Second,
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
	tp           *natsutil.TracingPublisher
	defKV        jetstream.KeyValue
	store        *SnapshotStore
	tracer       trace.Tracer
	cc           jetstream.ConsumeContext
	runLocks     sync.Map             // map[string]*sync.Mutex — per-run serialization
	admission    *AdmissionController // singleton + concurrency
	approval     *ApprovalGate        // approval token lifecycle
	sleepTimer   *SleepTimer          // durable sleep via NakWithDelay
	sleepHandler *sleepHandler        // sleep step-kind lifecycle
	correlator   *Correlator          // event wait-for-event matching
	sticky       *StickyRouter        // worker affinity bindings
	publisher    *TaskPublisher       // task dispatch pipeline
	recovery     *RecoveryManager     // failure recovery + compensation

	// Pre-allocated metric instruments — created once in constructor.
	metrics orchMetrics

	// reconcileCancel stops the periodic janitor goroutine. Set
	// in Start, called in Stop. nil before Start / after Stop.
	reconcileCancel context.CancelFunc

	// runsMaxAge is the run-retention window (#453, #521). Zero disables the
	// sweeper entirely: the prune ticker is not even started. The server
	// defaults this to 30d (server.DefaultRunsMaxAge); an operator can still
	// disable it with an explicit 0/off. When > 0, terminal runs whose
	// CompletedAt is older than this are dropped (delete-only) by the
	// background prune pass. In-flight runs are never touched.
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

// WithRunsMaxAge sets the run-retention sweeper window (#453, #521). A zero
// or negative window leaves the sweeper disabled, so the prune ticker is
// never started and no runs are deleted; the server passes 30d by default
// (server.DefaultRunsMaxAge) but honors an explicit 0/off to disable it.
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
// event exhausts in <1s instead of the ~5.6min production window (7
// retries; see historyRedeliverSchedule's doc comment above for the
// dead-letter trigger and the last entry's separate role) (TigerStyle:
// bounded test waits). A nil/empty schedule keeps the default.
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
	// Inject the run-lifecycle port (the Orchestrator itself) so the
	// gate's Handle* methods no longer take four callbacks per call.
	o.approval.mutator = o
	o.sleepHandler = newSleepHandler(o, o.sleepTimer, o.tp)
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
	// MaxDeliver: see historyRedeliverSchedule's doc comment above for
	// the full NAK-escalation / BackOff rationale (#508). AckWait is
	// deliberately left unset (NATS's 30s default) per the SETTLED
	// #508 contract's Non-goal #6.
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			Durable:       "orchestrator",
			FilterSubject: "history.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			MaxDeliver:    len(o.historyRedeliverSchedule),
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
	waitConsumeClosed(o.cc, "orchestrator")
	o.cc = nil
}

// consumeStopDrainTimeout bounds how long waitConsumeClosed blocks. A
// jetstream.ConsumeContext's pull-loop goroutine notices Stop() and
// exits asynchronously — this should resolve in microseconds, so the
// timeout exists only to keep a wedged goroutine from hanging shutdown
// forever, not because the drain is expected to be slow.
const consumeStopDrainTimeout = 5 * time.Second

// waitConsumeClosed blocks until cc reports fully stopped via Closed(),
// or consumeStopDrainTimeout elapses. jetstream.ConsumeContext.Stop()
// only signals its pull-loop goroutine to exit — Closed() is the only
// synchronous point at which that goroutine is guaranteed to have
// stopped touching JetStream. Without this wait, Stop() can return
// while the goroutine is still mid-fetch, which races embedded-server
// shutdown and store-dir removal in tests built on
// natsutil.StartTestServer. component names the caller in the timeout
// log so a hang is traceable to orchestrator vs correlator vs
// sleep-timer.
func waitConsumeClosed(cc jetstream.ConsumeContext, component string) {
	if cc == nil {
		panic("waitConsumeClosed: cc must not be nil")
	}
	if component == "" {
		panic("waitConsumeClosed: component must not be empty")
	}
	select {
	case <-cc.Closed():
	case <-time.After(consumeStopDrainTimeout):
		slog.Warn(
			"waitConsumeClosed: drain timeout — Stop() returning "+
				"without full quiesce",
			"component", component,
			"timeout", consumeStopDrainTimeout,
		)
	}
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
// event via RecoveryManager. It Acks only when that dead-letter publish
// succeeds, so the poison message stops redelivering with a durable DLQ
// record behind it (#508). If the dead-letter publish itself fails
// (DEAD_LETTERS transiently unavailable, at limit, connection blip), it
// NAKs with the schedule's last entry instead of Acking: MaxDeliver was
// already reached, so JetStream will not attempt further redelivery — the
// NAK simply leaves the message un-acked (pending) rather than silently
// consumed, so it survives in WORKFLOW_HISTORY and NATS emits a
// MAX_DELIVERIES advisory an operator can alert on. MaxDeliver caps total
// deliveries at the consumer level regardless of Ack/NAK, so this cannot
// reintroduce an infinite redelivery loop. On a Metadata() error (should
// not happen post-Consume) it fails safe: logs and NAKs with schedule[0]
// rather than risk wrongly dead-lettering.
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
		dlqErr := o.recovery.PublishHistoryDeadLetter(
			ctx, msg.Data(), runID, stepID, eventType,
			numDelivered, md.Sequence.Stream, cause,
		)
		if dlqErr != nil {
			slog.ErrorContext(ctx,
				"nakOrDeadLetterHistory: dead-letter publish failed, "+
					"NAKing instead of Acking to preserve the poison "+
					"event",
				"error", dlqErr,
				"run_id", runID,
				"step_id", stepID,
			)
			lastIdx := len(o.historyRedeliverSchedule) - 1
			msg.NakWithDelay(o.historyRedeliverSchedule[lastIdx])
			return
		}
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
		return o.approval.HandleGranted(ctx, evt)
	case protocol.EventApprovalRejected:
		return o.approval.HandleRejected(ctx, evt)
	case protocol.EventApprovalExpired:
		return o.approval.HandleExpired(ctx, evt)
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

	wfDef, input, labels, triggerDepth, err := o.resolveStartPayload(ctx, evt)
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

	// Re-validate labels at the trust boundary (#629). The API service
	// already rejects invalid labels with a 400, but any other producer
	// of workflow.started (a direct engine caller, a future trigger
	// type) gets the same guarantee here rather than a corrupt snapshot.
	if labelErr := dag.ValidateLabels(labels); labelErr != nil {
		o.persistFailedStartRun(ctx, evt, wfDef.Name, labelErr)
		return nil
	}

	// Validate input against schema if configured.
	if wfDef.InputSchema != nil {
		if err := dag.ValidateSchema(wfDef.InputSchema, input); err != nil {
			// Create a failed run for visibility
			run := dag.NewWorkflowRun(wfDef, evt.RunID)
			run.TraceParent = evt.TraceParent
			run.RootRunID = run.RunID // top-level run is its own tree-root (#377)
			if _, finalizeErr := finalizeRun(
				ctx, o.tp, o.saveSnapshot, run,
				dag.RunStatusFailed, "", nil,
			); finalizeErr != nil {
				slog.ErrorContext(ctx,
					"input validation: save failed-run snapshot",
					"error", finalizeErr,
					"run_id", evt.RunID,
				)
			}
			return fmt.Errorf("input validation: %w", err)
		}
	}

	run := dag.NewWorkflowRun(wfDef, evt.RunID)
	run.TraceParent = evt.TraceParent
	run.RootRunID = run.RunID // top-level run is its own tree-root (#377)
	run.Input = input
	run.Labels = labels
	run.TriggerDepth = triggerDepth

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

// resolveStartPayload decodes evt.Payload into a WorkflowDef, Input,
// Labels, and TriggerDepth. Four shapes are accepted, in priority
// order:
//
//  1. Structured {workflow_def, input, labels} — produced by the API
//     service when a user invokes a workflow manually (#629 adds
//     labels). TriggerDepth is always 0 here — manual starts root a
//     new trigger-chain lineage.
//  2. Run-terminal chain payload {trigger:"run_terminal", source,
//     workflow_id, input, trigger_depth} — produced by the
//     run_terminal trigger (#634) when a source run reaches a
//     terminal status. Unlike the generic TriggerEnvelope below, the
//     run's Input is exactly the nested `input` object (source run_id
//     /workflow_id/status/labels), not the whole wrapper — see
//     internal/trigger's fireRunTerminal. TriggerDepth carries the
//     already-capped depth the trigger computed (source depth + 1);
//     the engine trusts it (internal producer) rather than
//     recomputing, matching the trust boundary #629 already applies
//     to Labels from the API service.
//  3. TriggerEnvelope {trigger, source, workflow_id, ...} — produced
//     by every OTHER trigger type (#167). The def is resolved from
//     workflow_defs KV by WorkflowID; the envelope itself becomes the
//     run's Input so workflows can observe how they were fired. No
//     labels shape exists for trigger envelopes yet. TriggerDepth 0.
//  4. Bare WorkflowDef — backward compat for direct callers (tests
//     and any embedded users that pre-date the structured shape).
//
// For trigger envelopes referencing a workflow that has no registered
// def, the helper persists a RunStatusFailed snapshot and returns
// errStartPayloadHandled so the caller ACKs the message — redelivery
// would re-fail identically.
func (o *Orchestrator) resolveStartPayload(
	ctx context.Context, evt protocol.Event,
) (dag.WorkflowDef, json.RawMessage, map[string]string, int, error) {
	var startPayload struct {
		WorkflowDef json.RawMessage   `json:"workflow_def"`
		Input       json.RawMessage   `json:"input"`
		Labels      map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(evt.Payload, &startPayload); err == nil &&
		startPayload.WorkflowDef != nil {
		var wfDef dag.WorkflowDef
		if err := json.Unmarshal(startPayload.WorkflowDef, &wfDef); err != nil {
			return dag.WorkflowDef{}, nil, nil, 0,
				fmt.Errorf("unmarshal WorkflowDef: %w", err)
		}
		return wfDef, startPayload.Input, startPayload.Labels, 0, nil
	}

	if workflowID, input, depth, ok := decodeRunTerminalChainPayload(
		evt.Payload,
	); ok {
		wfDef, err := o.loadDefOrFail(ctx, evt, workflowID)
		if err != nil {
			return dag.WorkflowDef{}, nil, nil, 0, err
		}
		return wfDef, input, nil, depth, nil
	}

	if workflowID, ok := decodeTriggerEnvelope(evt.Payload); ok {
		wfDef, err := o.loadDefOrFail(ctx, evt, workflowID)
		if err != nil {
			return dag.WorkflowDef{}, nil, nil, 0, err
		}
		return wfDef, evt.Payload, nil, 0, nil
	}

	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(evt.Payload, &wfDef); err != nil {
		return dag.WorkflowDef{}, nil, nil, 0,
			fmt.Errorf("unmarshal WorkflowDef: %w", err)
	}
	return wfDef, nil, nil, 0, nil
}

// loadDefOrFail resolves workflowID from workflow_defs KV, sharing the
// "persist a visible failed run, ACK, don't propagate" handling
// between the run-terminal chain path and the generic TriggerEnvelope
// path — both fail identically when the target workflow was never
// registered (or was removed after the trigger was created).
func (o *Orchestrator) loadDefOrFail(
	ctx context.Context, evt protocol.Event, workflowID string,
) (dag.WorkflowDef, error) {
	if workflowID == "" {
		panic("loadDefOrFail: workflowID must not be empty")
	}
	entry, err := o.defKV.Get(ctx, workflowID)
	if err != nil {
		o.persistFailedStartRun(ctx, evt, workflowID,
			fmt.Errorf("resolve trigger workflow def: %w", err))
		return dag.WorkflowDef{}, errStartPayloadHandled
	}
	var wfDef dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &wfDef); err != nil {
		return dag.WorkflowDef{},
			fmt.Errorf("unmarshal trigger workflow def: %w", err)
	}
	return wfDef, nil
}

// decodeRunTerminalChainPayload recognizes the run_terminal trigger's
// chain-start payload (#634) and extracts the flat Input object plus
// TriggerDepth. Distinguished from the generic TriggerEnvelope by
// Trigger=="run_terminal": that trigger is engine-internal (never a
// user-facing envelope shape), so pinning the literal string here is
// safe and keeps the decode a pure structural check like its sibling
// decodeTriggerEnvelope.
func decodeRunTerminalChainPayload(
	payload []byte,
) (workflowID string, input json.RawMessage, depth int, ok bool) {
	var env struct {
		Trigger      string          `json:"trigger"`
		WorkflowID   string          `json:"workflow_id"`
		Input        json.RawMessage `json:"input"`
		TriggerDepth int             `json:"trigger_depth"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return "", nil, 0, false
	}
	if env.Trigger != "run_terminal" || env.WorkflowID == "" ||
		len(env.Input) == 0 {
		return "", nil, 0, false
	}
	return env.WorkflowID, env.Input, env.TriggerDepth, true
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
	failed := dag.WorkflowRun{
		RunID:       evt.RunID,
		WorkflowID:  workflowID,
		Steps:       map[string]dag.StepState{},
		CreatedAt:   time.Now().UTC(),
		TraceParent: evt.TraceParent,
	}
	if _, err := finalizeRun(
		ctx, o.tp, o.saveSnapshot, failed, dag.RunStatusFailed, "", nil,
	); err != nil {
		slog.ErrorContext(ctx,
			"workflow.started: save failed-run snapshot",
			"error", err,
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
	if _, err := finalizeRun(
		ctx, o.tp, o.saveSnapshot, run, dag.RunStatusCancelled, "", nil,
	); err != nil {
		return fmt.Errorf("save skipped-run snapshot: %w", err)
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

// completeWorkflow marks the run complete, saves, publishes the terminal
// events (finalizeRun), adjusts metrics, and releases concurrency slot.
func (o *Orchestrator) completeWorkflow(
	ctx context.Context, run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("completeWorkflow: RunID must not be empty")
	}
	run, err := finalizeRun(
		ctx, o.tp, o.saveSnapshot, run, dag.RunStatusCompleted, "",
		func(ctx context.Context) error {
			// Runs BEFORE either publish (#625 review round 2): a
			// subscriber reacting to run.completed by starting the next
			// run must see the singleton lock and concurrency slot
			// already released, not race their release.
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
			return nil
		},
	)
	if err != nil {
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

// findOldestPendingRun finds the oldest pending run for the given workflow
// among what it can read this tick (#523). It is bounded and best-effort:
// it filters to run.* keys BEFORE any GET, caps the fetched keys at
// pendingRunScanMax, and uses ParallelGetJSBestEffort so a slow key is
// skipped rather than discarding the whole batch. When the population
// exceeds the cap or any key was skipped it degrades LOUDLY (WARN + metric)
// so operators get a prune/tune-retention signal.
//
// Honest bounds on what that resilience buys:
//   - A FIFO *reordering* (we start a newer pending run because the oldest's
//     key was momentarily slow/skipped) self-heals — the oldest is still in
//     the population and the next completion re-fires this scan.
//   - A pending run *outside the sampled window* (population > cap, and it
//     sorted past the first pendingRunScanMax keys) is NOT recovered by a
//     completion or the reconciler: in the starvation case this fix targets
//     — all remaining runs Pending, none Running — there is no next
//     completion, and reconcileRunningRuns only acts on Running runs. That
//     run is freed only when retention pruning (#453) shrinks the population
//     back under the cap. A1+A2 therefore give resilience and a loud signal,
//     NOT a guarantee that an over-cap bucket surfaces every pending run —
//     which is exactly why the WARN and workflow.runs.scan_degraded metric
//     exist as the operator's prune-now signal.
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
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("list run keys: %w", err)
	}
	// Filter to run.* keys FIRST — key names are cheap, so we never GET a
	// non-run key just to discard it. Then cap the expensive GETs.
	runKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if isRunKey(key) {
			runKeys = append(runKeys, key)
		}
	}
	total := len(runKeys)
	capped := total > pendingRunScanMax
	if capped {
		runKeys = runKeys[:pendingRunScanMax]
	}
	entries, skipped, err := natsutil.ParallelGetJSBestEffort(
		ctx, o.store.kv, runKeys,
		natsutil.DefaultParallelism, bestEffortGetTimeout,
	)
	if err != nil {
		return "", false, fmt.Errorf("parallel get runs: %w", err)
	}
	o.logPendingScanDegraded(ctx, workflowID, total, capped, skipped)
	return oldestPendingFromEntries(entries, workflowID)
}

// oldestPendingFromEntries selects the oldest pending run for workflowID
// from the fetched run snapshots. Values that fail to unmarshal are skipped
// (a corrupt snapshot never wedges queue progression).
func oldestPendingFromEntries(
	entries map[string][]byte, workflowID string,
) (string, bool, error) {
	if workflowID == "" {
		panic("oldestPendingFromEntries: workflowID must not be empty")
	}
	if entries == nil {
		panic("oldestPendingFromEntries: entries must not be nil")
	}
	var oldestRun dag.WorkflowRun
	var foundPending bool
	for _, value := range entries {
		var run dag.WorkflowRun
		if err := json.Unmarshal(value, &run); err != nil {
			continue
		}
		if run.WorkflowID != workflowID ||
			run.Status != dag.RunStatusPending {
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

// logPendingScanDegraded emits the LOUD prune/tune-retention signal (#523)
// when a pending scan had to cap the population or skip slow keys. Fires
// per completion by design: findOldestPendingRun runs concurrently across
// workflows, so a mutable transition-state (like the reconciler's
// capHitPrev) is unsafe here; a WARN + a monotonic counter are the
// concurrency-safe operator surface. A clean scan logs nothing.
func (o *Orchestrator) logPendingScanDegraded(
	ctx context.Context, workflowID string, total int,
	capped bool, skipped int,
) {
	if ctx == nil {
		panic("logPendingScanDegraded: ctx must not be nil")
	}
	if workflowID == "" {
		panic("logPendingScanDegraded: workflowID must not be empty")
	}
	if !capped && skipped == 0 {
		return
	}
	o.metrics.runsScanDegraded.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", workflowID),
	))
	slog.WarnContext(ctx,
		"pending scan degraded; prune or tune run retention",
		"workflow_id", workflowID,
		"run_keys", total,
		"cap", pendingRunScanMax,
		"capped", capped,
		"skipped", skipped,
	)
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

// failWorkflow marks the workflow as permanently failed and releases
// resources. Extracted to avoid duplication between failure paths.
func (o *Orchestrator) failWorkflow(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error {
	run, err := finalizeRun(
		ctx, o.tp, o.saveSnapshot, run, dag.RunStatusFailed, stepDef.ID,
		func(ctx context.Context) error {
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
			return nil
		},
	)
	if err != nil {
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

	// finalizeRun stamps Status/CompletedAt (markTerminal), saves, and
	// publishes the new event.run.* notification — cancellation has no
	// history-stream terminal counterpart (see publishHistoryTerminalEvent),
	// so this is the only reliable "run finished cancelling" signal.
	run, err = finalizeRun(
		ctx, o.tp, o.saveSnapshot, run, dag.RunStatusCancelled, "",
		func(ctx context.Context) error {
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
			return nil
		},
	)
	if err != nil {
		return err
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
//
// recursion:allow this is the hub of the dispatch cycle -- enqueueReady
// -> dispatchReadySteps -> enqueueSubWorkflow -> completeWorkflow ->
// startNextPendingRun -> enqueueReady, plus the failure variant through
// failWorkflow. Re-entry happens only when finishing one run admits
// another, so depth follows sub-workflow nesting, which the DAG
// validator bounds at registration. Restructuring this into an explicit
// work queue is a real change to live dispatch, tracked separately;
// annotating it here records the decision rather than hiding it.
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
		// Stamp a fresh per-dispatch nonce and StartedAt (#380, #626): both
		// ride this snapshot write (no extra KV write); the nonce is
		// mirrored onto the TaskPayload in PublishBatch so the worker can
		// prove it received this dispatch.
		stampDispatch(&state, time.Now().UTC())
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
			if err := o.sleepHandler.enqueue(
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
	// Stamp CompletedAt (#626) for any step that just reached a terminal
	// status on this snapshot — one place covers every completion/failure
	// branch instead of each one remembering to call stampCompleted.
	// Mutates run.Steps in place (map is a reference type) so it rides
	// this write; no extra KV write.
	stampTerminalSteps(run.Steps, time.Now().UTC())
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
