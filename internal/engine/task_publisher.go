// engine/task_publisher.go
// TaskPublisher owns rate limiting, task concurrency gating, sticky
// routing, subject resolution, and NATS publish with dedup headers.
// Extracted from Orchestrator to reduce its surface area.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// buildTaskMsg constructs a *nats.Msg with dedup header.
func buildTaskMsg(
	subject string, data []byte, msgID string,
) *nats.Msg {
	if subject == "" {
		panic("buildTaskMsg: subject must not be empty")
	}
	if msgID == "" {
		panic("buildTaskMsg: msgID must not be empty")
	}
	return &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
}

// TaskPublisher handles all task dispatch: rate limiting,
// concurrency acquisition, sticky routing, and NATS publish.
// The pub field is the TracingPublisher wrapper (#334) — it
// auto-injects W3C trace context on every outgoing message.
// js is retained for non-publish JetStream operations
// (atomic-batch publish via jetstreamext, KV access).
type TaskPublisher struct {
	js          jetstream.JetStream
	pub         *natsutil.TracingPublisher
	rateLimiter *RateLimiter
	admission   *AdmissionController
	sticky      *StickyRouter
	sleepTimer  *SleepTimer
	stepRoutes  map[dag.StepType]string
	tracer      trace.Tracer

	metrics pubMetrics

	// loadRunAndDef is injected by Orchestrator so that Publish
	// can check sticky workflow definitions without importing
	// snapshot/def loading logic.
	loadRunAndDef func(
		ctx context.Context, runID string,
	) (dag.WorkflowDef, dag.WorkflowRun, error)

	// grantPolicy is the hot-reloadable capability-grant policy (#380),
	// shared with the Orchestrator via WithGrantPolicyHolder. doPublish
	// strips the control-plane capability from a step whose workflow is not
	// granted, so an ungranted step's task message never carries it and the
	// worker withholds the handle. nil holder Loads nil → deny-by-default.
	grantPolicy *GrantPolicyHolder

	// loopStateMaxBytes is stamped on every TaskPayload (#801) so the
	// worker refuses an oversized Continue with the engine's own cap.
	loopStateMaxBytes int
}

// NewTaskPublisher creates a TaskPublisher with the given deps.
// pub is required for trace-context-injecting publish (#334).
func NewTaskPublisher(
	js jetstream.JetStream,
	pub *natsutil.TracingPublisher,
	rateLimiter *RateLimiter,
	admission *AdmissionController,
	sticky *StickyRouter,
	sleepTimer *SleepTimer,
	tracer trace.Tracer,
	metrics pubMetrics,
	loadRunAndDef func(
		ctx context.Context, runID string,
	) (dag.WorkflowDef, dag.WorkflowRun, error),
) *TaskPublisher {
	if js == nil {
		panic("NewTaskPublisher: js must not be nil")
	}
	if pub == nil {
		panic("NewTaskPublisher: pub must not be nil")
	}
	if tracer == nil {
		panic("NewTaskPublisher: tracer must not be nil")
	}
	if loadRunAndDef == nil {
		panic(
			"NewTaskPublisher: loadRunAndDef must not be nil",
		)
	}
	return &TaskPublisher{
		js:                js,
		pub:               pub,
		rateLimiter:       rateLimiter,
		admission:         admission,
		sticky:            sticky,
		sleepTimer:        sleepTimer,
		tracer:            tracer,
		metrics:           metrics,
		loadRunAndDef:     loadRunAndDef,
		loopStateMaxBytes: protocol.LoopStateMaxBytesDefault,
	}
}

// Publish dispatches a single task after checking rate limits,
// task concurrency, and sticky bindings. If rate-limited or
// concurrency-blocked, schedules a timer for delayed re-attempt.
//
// run is the CURRENT run snapshot (#624 review round 4): doPublish and
// sticky.PublishTask derive BOTH Attempt and Iteration from it via
// dispatchIdentity, so no call site computes either by hand. Passing
// run (not a pre-computed attempt int) is what makes the class of bug
// this round fixes impossible to reintroduce — the only way to get an
// Attempt/Iteration onto the wire is through dispatchIdentity.
// Publish is a new attempt (dispatchNewAttempt); PublishBatch is the
// one caller with another mode.
func (tp *TaskPublisher) Publish(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	run dag.WorkflowRun,
	workflowName string,
	dispatchNonce string,
) error {
	return tp.publish(
		ctx, runID, step, input, run, workflowName, dispatchNonce, dispatchNewAttempt,
	)
}

// publish is Publish with the dispatch's identity mode.
func (tp *TaskPublisher) publish(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	run dag.WorkflowRun,
	workflowName string,
	dispatchNonce string,
	mode dispatchIdentityMode,
) error {
	if runID == "" {
		panic("TaskPublisher.Publish: runID must not be empty")
	}
	if step.ID == "" {
		panic("TaskPublisher.Publish: step.ID must not be empty")
	}
	if run.RunID != "" && run.RunID != runID {
		panic("TaskPublisher.Publish: run.RunID must match runID")
	}

	// A deferral is not an attempt: the timer carries the identity this
	// dispatch would have sent, and the fire path re-sends it unchanged.
	attempt, _ := dispatchIdentity(run, step.ID, mode)
	meta := dispatchMeta{workflowName: workflowName, nonce: dispatchNonce, attempt: attempt}

	// Check rate limit before concurrency acquisition so we
	// don't hold a concurrency slot while waiting for tokens.
	if delayed, err := tp.checkRateLimit(ctx, step, runID, input, meta); err != nil {
		return err
	} else if delayed {
		return nil
	}

	// Check per-task-type concurrency before publishing.
	if acquired, err := tp.acquireTaskSlot(ctx, step, runID); err != nil {
		return err
	} else if !acquired {
		return tp.scheduleConcurrencyRetry(ctx, step, runID, input, meta)
	}

	// Check sticky binding — if a binding exists, route to the
	// bound worker instead of the normal subject.
	workerID := tp.sticky.GetWorker(ctx, runID)
	if workerID != "" && tp.loadRunAndDef != nil {
		wfDef, _, loadErr := tp.loadRunAndDef(ctx, runID)
		if loadErr == nil && wfDef.Sticky != dag.StickyNone {
			return tp.sticky.PublishTask(
				ctx, runID, step, input, run,
				workerID, wfDef.Sticky, dispatchNonce, workflowName,
				tp.StepSubject(step, runID), mode,
			)
		}
	}

	return tp.doPublish(
		ctx, runID, step, input, run, workflowName, dispatchNonce, mode,
	)
}

// dispatchMeta carries the per-dispatch grant decision (#380) through the
// rate-limit / concurrency retry-scheduling chain so the timer fire can
// re-publish a TaskPayload that still honors the grant policy. nonce is the
// run-binding token; caps are the ALREADY-STRIPPED effective capabilities
// (effectiveCapabilities applied once at the dispatch call site). attempt
// is the attempt the deferred dispatch carries, so its re-send carries it
// too: a deferral never reached a handler and spends no retry budget.
type dispatchMeta struct {
	workflowName string
	nonce        string
	attempt      int
}

// nonceOrMint returns the given nonce, or a fresh one when it is empty. It
// is the last-line guard at the TaskPayload build choke points (#380): every
// dispatch carries a non-empty run-binding nonce, so VerifyDispatch never
// over-denies a legitimate granted handler because a path forgot to stamp.
func nonceOrMint(nonce string) string {
	if nonce == "" {
		return runid.New()
	}
	return nonce
}

// checkRateLimit evaluates rate limits for the step. Returns
// delayed=true if the task was deferred via SleepTimer.
func (tp *TaskPublisher) checkRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte, meta dispatchMeta,
) (bool, error) {
	if runID == "" {
		panic("TaskPublisher.checkRateLimit: runID must not be empty")
	}
	allowed, retryAfter, err := tp.rateAllows(ctx, step, input)
	if err != nil || allowed {
		return false, err
	}
	return true, tp.scheduleRateRetry(ctx, step, runID, input, retryAfter, meta)
}

// rateAllows takes the step's rate-limit tokens for one dispatch, or
// reports how long until they refill. A step without a rate limit, or
// a publisher without a limiter, is always allowed.
func (tp *TaskPublisher) rateAllows(
	ctx context.Context, step dag.StepDef, input []byte,
) (allowed bool, retryAfter time.Duration, err error) {
	if tp.rateLimiter == nil {
		return true, 0, nil
	}
	if step.Task == "" {
		panic("TaskPublisher.rateAllows: step.Task must not be empty")
	}
	if step.RateLimit != nil {
		rl := step.RateLimit
		allowed, retryAfter, err = tp.rateLimiter.Allow(
			ctx, step.Task, "_global", rl.Limit, rl.Period, 1,
		)
		if err != nil {
			return false, 0, fmt.Errorf("rate limit check: %w", err)
		}
		return allowed, retryAfter, nil
	}
	if step.KeyedRateLimit != nil {
		return tp.keyedRateAllows(ctx, step, input)
	}
	return true, 0, nil
}

// keyedRateAllows is rateAllows for a per-key rate limit.
func (tp *TaskPublisher) keyedRateAllows(
	ctx context.Context, step dag.StepDef, input []byte,
) (bool, time.Duration, error) {
	if step.KeyedRateLimit == nil {
		panic("keyedRateAllows: KeyedRateLimit must not be nil")
	}
	if tp.rateLimiter == nil {
		panic("keyedRateAllows: rateLimiter must not be nil")
	}
	krl := step.KeyedRateLimit
	keyVal, err := dag.ExtractDotPath(krl.Key, input)
	if err != nil {
		return false, 0, fmt.Errorf(
			"extract rate limit key %q: %w", krl.Key, err,
		)
	}
	key := fmt.Sprintf("%v", keyVal)
	allowed, retryAfter, err := tp.rateLimiter.Allow(
		ctx, step.Task, key,
		krl.Limit, krl.Period, krl.Units,
	)
	if err != nil {
		return false, 0, fmt.Errorf("keyed rate limit: %w", err)
	}
	return allowed, retryAfter, nil
}

// acquireTaskSlot takes the step's task concurrency slot for runID.
// Taking a slot the holder already has succeeds without a second one
// (AcquireTask), so a deferred dispatch's every fire may ask again. A
// step without a limit, or an engine without concurrency, always gets
// one.
func (tp *TaskPublisher) acquireTaskSlot(
	ctx context.Context, step dag.StepDef, runID string,
) (bool, error) {
	if runID == "" {
		panic("acquireTaskSlot: runID must not be empty")
	}
	if step.ID == "" {
		panic("acquireTaskSlot: step.ID must not be empty")
	}
	if step.MaxTaskConcurrency <= 0 || !tp.admission.HasConcurrency() {
		return true, nil
	}
	acquired, err := tp.admission.AcquireTask(
		ctx, step.Task, step.MaxTaskConcurrency, taskSlotHolder(runID, step.ID),
	)
	if err != nil {
		return false, err
	}
	if !acquired {
		tp.metrics.taskConcRejected.Add(ctx, 1)
		return false, nil
	}
	tp.metrics.taskConcAcquired.Add(ctx, 1)
	return true, nil
}

// concurrencyRetryDelay is how often a dispatch waiting for a task
// concurrency slot asks for one again.
const concurrencyRetryDelay = time.Second

// readmitDeferred is the DeferralAdmitter: the admission a deferral
// timer's dispatch must pass again before it is re-sent, from the stage
// that deferred it. A rate deferral asks the limiter again and, once
// allowed, goes on to the concurrency slot as a fresh dispatch does; a
// concurrency deferral asks for the slot only, since it already spent
// its rate tokens. A rate_retry timer that is not RateDeferred is a
// soft-sticky fallback, which passed admission before its sticky send
// (or a rate timer from an older binary): it is not charged tokens
// again, but its slot is still asked for, which costs a fallback
// nothing as its holder already has it.
func (tp *TaskPublisher) readmitDeferred(
	ctx context.Context, tm TimerMessage,
) (send bool, refireAfter time.Duration, err error) {
	if !deferralAction(tm.Action) {
		panic("readmitDeferred: timer must be a deferral")
	}
	if tm.RunID == "" {
		panic("readmitDeferred: RunID must not be empty")
	}
	step, found, err := tp.deferredStep(ctx, tm)
	if err != nil {
		return false, 0, err
	}
	if !found {
		return true, 0, nil
	}
	if tm.Action == TimerActionRateRetry && tm.RateDeferred {
		allowed, retryAfter, err := tp.rateAllows(ctx, step, tm.Input)
		if err != nil || !allowed {
			return false, max(retryAfter, 100*time.Millisecond), err
		}
	}
	acquired, err := tp.acquireTaskSlot(ctx, step, tm.RunID)
	if err != nil || acquired {
		return acquired, 0, err
	}
	if tm.Action == TimerActionTaskConcurRetry {
		return false, concurrencyRetryDelay, nil
	}
	// Past the limiter but not the slot: the wait from here is a
	// concurrency deferral's, so a timer of that kind takes it over and
	// the limiter is not asked again.
	meta := dispatchMeta{
		workflowName: tm.WorkflowName, nonce: tm.DispatchNonce,
		attempt: max(tm.Attempt, firstDispatchAttempt),
	}
	return false, 0, tp.scheduleConcurrencyRetry(ctx, step, tm.RunID, tm.Input, meta)
}

// deferredStep is the step a deferral timer re-sends, as the run's
// workflow def has it; a map instance is its map step under the
// instance's ID, the shape publishMapTasks dispatched. found is false
// when the def no longer names the step: there is no limit left to
// honour, so the dispatch is re-sent.
func (tp *TaskPublisher) deferredStep(
	ctx context.Context, tm TimerMessage,
) (dag.StepDef, bool, error) {
	if tm.StepID == "" {
		panic("deferredStep: StepID must not be empty")
	}
	if tp.loadRunAndDef == nil {
		panic("deferredStep: loadRunAndDef must not be nil")
	}
	wfDef, _, err := tp.loadRunAndDef(ctx, tm.RunID)
	if err != nil {
		return dag.StepDef{}, false, fmt.Errorf("load run %q: %w", tm.RunID, err)
	}
	instance := isMapInstanceID(tm.StepID)
	defID := tm.StepID
	if instance {
		defID, _ = parseMapInstanceID(tm.StepID)
	}
	step, found := findStepDef(wfDef, defID)
	if !found {
		return dag.StepDef{}, false, nil
	}
	step.ID = tm.StepID
	if instance {
		// As publishMapTasks does (#513): a timer scheduled for an
		// instance must never carry the control-plane capability.
		step.RequiredCapabilities = stripControlPlaneCapability(step.RequiredCapabilities)
	}
	return step, true, nil
}

// scheduleRateRetry schedules a timer to re-attempt task dispatch
// after the rate limit window allows more tokens. The grant decision
// (stripped caps + run-binding nonce) rides the TimerMessage so the
// timer fire re-publishes a policy-honoring, run-bound TaskPayload (#380).
func (tp *TaskPublisher) scheduleRateRetry(
	ctx context.Context, step dag.StepDef, runID string,
	input []byte, retryAfter time.Duration, meta dispatchMeta,
) error {
	if runID == "" {
		panic("scheduleRateRetry: runID must not be empty")
	}
	if step.ID == "" {
		panic("scheduleRateRetry: step.ID must not be empty")
	}
	if meta.attempt < firstDispatchAttempt {
		panic("scheduleRateRetry: meta.attempt must be a dispatch's attempt")
	}
	durationMs := retryAfter.Milliseconds()
	if durationMs <= 0 {
		durationMs = 100
	}
	return tp.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionRateRetry,
		RunID:         runID,
		StepID:        step.ID,
		DurationMs:    durationMs,
		RateDeferred:  true,
		TaskType:      step.Task,
		Subject:       tp.StepSubject(step, runID),
		Metadata:      step.Metadata,
		Input:         input,
		WorkflowName:  meta.workflowName,
		DispatchNonce: meta.nonce,
		Attempt:       meta.attempt,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, meta.workflowName,
			tp.grantPolicy.Load(),
		),
	})
}

// scheduleConcurrencyRetry schedules a timer to re-attempt
// task dispatch after the task concurrency slot frees up. The grant
// decision (stripped caps + run-binding nonce) rides the TimerMessage so
// the timer fire re-publishes a policy-honoring, run-bound TaskPayload (#380).
func (tp *TaskPublisher) scheduleConcurrencyRetry(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte, meta dispatchMeta,
) error {
	if runID == "" {
		panic(
			"scheduleConcurrencyRetry: " +
				"runID must not be empty",
		)
	}
	if step.ID == "" {
		panic(
			"scheduleConcurrencyRetry: " +
				"step.ID must not be empty",
		)
	}
	if meta.attempt < firstDispatchAttempt {
		panic("scheduleConcurrencyRetry: meta.attempt must be a dispatch's attempt")
	}
	return tp.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionTaskConcurRetry,
		RunID:         runID,
		StepID:        step.ID,
		DurationMs:    concurrencyRetryDelay.Milliseconds(),
		TaskType:      step.Task,
		Subject:       tp.StepSubject(step, runID),
		Metadata:      step.Metadata,
		Input:         input,
		WorkflowName:  meta.workflowName,
		DispatchNonce: meta.nonce,
		Attempt:       meta.attempt,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, meta.workflowName, tp.grantPolicy.Load(),
		),
	})
}

// spanWorkflowNameAttrs builds the base enqueueTask span attributes,
// including workflow_name ONLY when non-empty. Mirrors
// worker/worker.go's startTaskSpan: an empty workflowName means the map
// fan-out path (or an older payload) has no telemetry name to report, and
// emitting an empty-string attribute would be worse than omitting it
// entirely (#503, #513). Shared by doPublish and PublishIteration so the
// omit-when-empty guard lives in exactly one place.
func spanWorkflowNameAttrs(
	runID, stepID, taskName, workflowName string,
) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("run_id", runID),
		attribute.String("step_id", stepID),
		attribute.String("task_name", taskName),
	}
	if workflowName != "" {
		attrs = append(attrs, attribute.String("workflow_name", workflowName))
	}
	return attrs
}

// doPublish performs the actual NATS publish for a task message. It is the
// deny-by-default grant gate at the payload source (#380): the step's
// RequiredCapabilities are passed through effectiveCapabilities, which
// strips the control-plane capability when workflowName is not granted, so
// the message a worker receives never carries a capability the policy
// withholds. dispatchNonce rides the payload for server-side run-binding;
// it was stamped on the step's StepState in the same snapshot write.
//
// Attempt and Iteration come from dispatchIdentity(run, step.ID, mode)
// — this is the ONLY site in the publish path that builds a
// TaskPayload, so it is the ONLY site that needs to call it (#624
// review round 4).
func (tp *TaskPublisher) doPublish(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	run dag.WorkflowRun,
	workflowName string,
	dispatchNonce string,
	mode dispatchIdentityMode,
) error {
	if runID == "" {
		panic("doPublish: runID must not be empty")
	}
	if step.ID == "" {
		panic("doPublish: step.ID must not be empty")
	}
	// Span name carries the task name only (bounded cardinality: task
	// names are registered handler types, a small fixed set) — never
	// run_id or workflow_name, which are unbounded per-run values that
	// would blow up the tracing backend's span-name index (#503).
	ctx, span := tp.tracer.Start(ctx,
		"enqueueTask "+step.Task,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			spanWorkflowNameAttrs(runID, step.ID, step.Task, workflowName)...,
		),
	)
	defer span.End()
	attempt, iteration := dispatchIdentity(run, step.ID, mode)
	payload := protocol.TaskPayload{
		TaskID:       runID + "." + step.ID,
		RunID:        runID,
		StepID:       step.ID,
		Attempt:      attempt,
		Iteration:    iteration,
		Input:        input,
		WorkflowName: workflowName,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, workflowName, tp.grantPolicy.Load(),
		),
		// Defensive: mint a nonce if the caller passed none, so a future
		// new dispatch path that forgets to thread one cannot silently
		// produce an unverifiable (always-denied) dispatch (#380).
		DispatchNonce: nonceOrMint(dispatchNonce),
		Metadata:      step.Metadata,
		State:         run.Steps[step.ID].LoopState,
		StateMaxBytes: tp.loopStateMaxBytes,
		AgentLoop:     step.Type == dag.StepTypeAgentLoop,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	msgID := runID + "." + step.ID + ".queued"
	subject := tp.stepSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	_, err = tp.pub.JSPublishMsg(ctx, msg)
	if err != nil {
		return err
	}
	tp.metrics.stepEnqueue.Add(ctx, 1)
	return nil
}

// PublishIteration publishes a TaskPayload for an agent-loop
// re-enqueue. Each iteration's MsgId is distinct for dedup. loopState
// is the state the step committed with the continue that produced this
// iteration (#801), read from the snapshot by every caller.
func (tp *TaskPublisher) PublishIteration(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
	iteration int,
	workflowName string,
	dispatchNonce string,
	loopState []byte,
) error {
	if runID == "" {
		panic(
			"PublishIteration: runID must not be empty",
		)
	}
	if step.ID == "" {
		panic(
			"PublishIteration: step.ID must not be empty",
		)
	}
	// Span name carries the task name only — see doPublish for the
	// bounded-cardinality rationale (#503).
	ctx, span := tp.tracer.Start(ctx,
		"enqueueTask "+step.Task,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			spanWorkflowNameAttrs(runID, step.ID, step.Task, workflowName)...,
		),
	)
	defer span.End()
	// The agent-loop Continue re-enqueue (#380): re-apply the grant strip so a
	// granted loop keeps its control-plane capability across iterations, and
	// stamp the run-binding nonce so the iteration's control-plane calls pass
	// VerifyDispatch. A nil holder Loads nil → deny-by-default.
	//
	// attempt is the caller's currentAttempt(run, step.ID) (#624 review
	// round 3) — the SAME attempt this Continue call is a part of, not
	// a new one via nextDispatchAttempt: a Continue iteration must not
	// consume retry budget the step never spent. iteration is the new
	// value; the two together are what scope this dispatch's BUILD_LOGS
	// subject (logs.{runID}.{stepID}.{attempt}.{iteration}) so iteration
	// 2+'s chunks never collide with iteration 0's.
	payload := protocol.TaskPayload{
		TaskID:       runID + "." + step.ID,
		RunID:        runID,
		StepID:       step.ID,
		Attempt:      attempt,
		Iteration:    iteration,
		Input:        input,
		WorkflowName: workflowName,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, workflowName, tp.grantPolicy.Load(),
		),
		DispatchNonce: nonceOrMint(dispatchNonce),
		Metadata:      step.Metadata,
		State:         loopState,
		StateMaxBytes: tp.loopStateMaxBytes,
		AgentLoop:     step.Type == dag.StepTypeAgentLoop,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	msgID := fmt.Sprintf(
		"%s.%s.continue.%d", runID, step.ID, iteration,
	)
	subject := tp.stepSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	_, err = tp.pub.JSPublishMsg(ctx, msg)
	if err != nil {
		return err
	}
	tp.metrics.stepEnqueue.Add(ctx, 1)
	return nil
}

// stepSubject resolves the NATS subject for a step based on
// routing config. Defaults to "task.{task}.{runID}" if no
// custom route is configured.
func (tp *TaskPublisher) stepSubject(
	step dag.StepDef, runID string,
) string {
	return tp.StepSubject(step, runID)
}

// StepSubject is the exported variant of stepSubject so other
// subsystems (notably DLQ publish, which needs to preserve the
// original dispatch subject for verbatim replay) can resolve it
// without duplicating the routing logic.
func (tp *TaskPublisher) StepSubject(
	step dag.StepDef, runID string,
) string {
	if runID == "" {
		panic("StepSubject: runID must not be empty")
	}
	if step.Task == "" {
		panic("StepSubject: step.Task must not be empty")
	}
	prefix := "task"
	if tp.stepRoutes != nil {
		if p, ok := tp.stepRoutes[step.Type]; ok {
			prefix = p
		}
	}
	subject := prefix + "." + step.Task
	if step.WorkerGroup != "" {
		// Group sentinel (#704): disambiguates a dotted Task from an
		// undotted Task plus WorkerGroup, which used to derive the
		// byte-identical subject — see internal/consumername.FilterFor
		// and consumername.GroupSentinel for the full constraint set
		// that pins its value.
		subject += "." + consumername.GroupSentinel + step.WorkerGroup
	}
	return subject + "." + runID
}

// PublishBatch publishes a task message for each ready step.
// Steps are published concurrently since they are independent. It
// sends enqueueReady's dispatches and their resends (resendUnsent), so
// both copies carry one identity: dispatchFirstAttempt (#953).
func (tp *TaskPublisher) PublishBatch(
	ctx context.Context,
	runID string,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	ready []dag.StepDef,
) error {
	if runID == "" {
		panic("PublishBatch: runID must not be empty")
	}
	if len(ready) == 0 {
		panic("PublishBatch: ready must not be empty")
	}
	var g errgroup.Group
	for _, step := range ready {
		step := step
		input, err := dag.ResolveInput(step, run.Steps, run.Input)
		if err != nil {
			return fmt.Errorf(
				"resolve input for step %q: %w",
				step.ID, err,
			)
		}
		// The grant strip keys on the workflow name; the run-binding nonce
		// was stamped onto the step state by enqueueReady and rides the
		// in-memory snapshot here (no re-load per task). run itself
		// (not a pre-computed attempt) flows through to Publish so
		// Attempt/Iteration are both derived by dispatchIdentity
		// (#624 review round 4) — this call site used to read
		// Steps[step.ID].Attempts bare with no +1, which under-counted
		// the attempt for any step being re-queued after a prior one.
		nonce := run.Steps[step.ID].DispatchNonce
		g.Go(func() error {
			return tp.publish(
				ctx, runID, step, input, run,
				run.WorkflowID, nonce, dispatchFirstAttempt,
			)
		})
	}
	return g.Wait()
}
