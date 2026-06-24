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
		js:            js,
		pub:           pub,
		rateLimiter:   rateLimiter,
		admission:     admission,
		sticky:        sticky,
		sleepTimer:    sleepTimer,
		tracer:        tracer,
		metrics:       metrics,
		loadRunAndDef: loadRunAndDef,
	}
}

// Publish dispatches a single task after checking rate limits,
// task concurrency, and sticky bindings. If rate-limited or
// concurrency-blocked, schedules a timer for delayed re-attempt.
func (tp *TaskPublisher) Publish(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
	workflowName string,
	dispatchNonce string,
) error {
	if runID == "" {
		panic("TaskPublisher.Publish: runID must not be empty")
	}
	if step.ID == "" {
		panic("TaskPublisher.Publish: step.ID must not be empty")
	}

	// Check rate limit before concurrency acquisition so we
	// don't hold a concurrency slot while waiting for tokens.
	if delayed, err := tp.checkRateLimit(
		ctx, step, runID, input, workflowName, dispatchNonce,
	); err != nil {
		return err
	} else if delayed {
		return nil
	}

	// Check per-task-type concurrency before publishing.
	if step.MaxTaskConcurrency > 0 &&
		tp.admission.HasConcurrency() {
		acquired, err := tp.admission.AcquireTask(
			ctx, step.Task, step.MaxTaskConcurrency,
		)
		if err != nil {
			return err
		}
		if !acquired {
			tp.metrics.taskConcRejected.Add(ctx, 1)
			return tp.scheduleConcurrencyRetry(
				ctx, step, runID, input, workflowName, dispatchNonce,
			)
		}
		tp.metrics.taskConcAcquired.Add(ctx, 1)
	}

	// Check sticky binding — if a binding exists, route to the
	// bound worker instead of the normal subject.
	workerID := tp.sticky.GetWorker(ctx, runID)
	if workerID != "" && tp.loadRunAndDef != nil {
		wfDef, _, loadErr := tp.loadRunAndDef(ctx, runID)
		if loadErr == nil && wfDef.Sticky != dag.StickyNone {
			return tp.sticky.PublishTask(
				ctx, runID, step, input, attempt,
				workerID, wfDef.Sticky, dispatchNonce,
			)
		}
	}

	return tp.doPublish(
		ctx, runID, step, input, attempt, workflowName, dispatchNonce,
	)
}

// dispatchMeta carries the per-dispatch grant decision (#380) through the
// rate-limit / concurrency retry-scheduling chain so the timer fire can
// re-publish a TaskPayload that still honors the grant policy. nonce is the
// run-binding token; caps are the ALREADY-STRIPPED effective capabilities
// (effectiveCapabilities applied once at the dispatch call site).
type dispatchMeta struct {
	workflowName string
	nonce        string
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
	step dag.StepDef, runID string, input []byte,
	workflowName, nonce string,
) (bool, error) {
	if tp.rateLimiter == nil {
		return false, nil
	}
	if step.Task == "" {
		panic(
			"TaskPublisher.checkRateLimit: " +
				"step.Task must not be empty",
		)
	}
	meta := dispatchMeta{workflowName: workflowName, nonce: nonce}
	if step.RateLimit != nil {
		return tp.applyGlobalRateLimit(ctx, step, runID, input, meta)
	}
	if step.KeyedRateLimit != nil {
		return tp.applyKeyedRateLimit(ctx, step, runID, input, meta)
	}
	return false, nil
}

// applyGlobalRateLimit checks the global rate limit for this
// task type and schedules a retry if tokens are exhausted.
func (tp *TaskPublisher) applyGlobalRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte, meta dispatchMeta,
) (bool, error) {
	if step.RateLimit == nil {
		panic(
			"applyGlobalRateLimit: RateLimit must not be nil",
		)
	}
	if runID == "" {
		panic(
			"applyGlobalRateLimit: runID must not be empty",
		)
	}
	rl := step.RateLimit
	allowed, retryAfter, err := tp.rateLimiter.Allow(
		ctx, step.Task, "_global", rl.Limit, rl.Period, 1,
	)
	if err != nil {
		return false, fmt.Errorf("rate limit check: %w", err)
	}
	if allowed {
		return false, nil
	}
	return true, tp.scheduleRateRetry(
		ctx, step, runID, input, retryAfter, meta,
	)
}

// applyKeyedRateLimit checks the per-key rate limit for this
// task and schedules a retry if tokens are exhausted.
func (tp *TaskPublisher) applyKeyedRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte, meta dispatchMeta,
) (bool, error) {
	if step.KeyedRateLimit == nil {
		panic(
			"applyKeyedRateLimit: " +
				"KeyedRateLimit must not be nil",
		)
	}
	if runID == "" {
		panic(
			"applyKeyedRateLimit: runID must not be empty",
		)
	}
	krl := step.KeyedRateLimit
	keyVal, err := dag.ExtractDotPath(krl.Key, input)
	if err != nil {
		return false, fmt.Errorf(
			"extract rate limit key %q: %w", krl.Key, err,
		)
	}
	key := fmt.Sprintf("%v", keyVal)
	allowed, retryAfter, err := tp.rateLimiter.Allow(
		ctx, step.Task, key,
		krl.Limit, krl.Period, krl.Units,
	)
	if err != nil {
		return false, fmt.Errorf("keyed rate limit: %w", err)
	}
	if allowed {
		return false, nil
	}
	return true, tp.scheduleRateRetry(
		ctx, step, runID, input, retryAfter, meta,
	)
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
	durationMs := retryAfter.Milliseconds()
	if durationMs <= 0 {
		durationMs = 100
	}
	return tp.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionRateRetry,
		RunID:         runID,
		StepID:        step.ID,
		DurationMs:    durationMs,
		TaskType:      step.Task,
		Input:         input,
		DispatchNonce: meta.nonce,
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
	step dag.StepDef, runID string, input []byte,
	workflowName, nonce string,
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
	return tp.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionTaskConcurRetry,
		RunID:         runID,
		StepID:        step.ID,
		DurationMs:    1000,
		TaskType:      step.Task,
		Input:         input,
		DispatchNonce: nonce,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, workflowName, tp.grantPolicy.Load(),
		),
	})
}

// doPublish performs the actual NATS publish for a task message. It is the
// deny-by-default grant gate at the payload source (#380): the step's
// RequiredCapabilities are passed through effectiveCapabilities, which
// strips the control-plane capability when workflowName is not granted, so
// the message a worker receives never carries a capability the policy
// withholds. dispatchNonce rides the payload for server-side run-binding;
// it was stamped on the step's StepState in the same snapshot write.
func (tp *TaskPublisher) doPublish(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
	workflowName string,
	dispatchNonce string,
) error {
	if runID == "" {
		panic("doPublish: runID must not be empty")
	}
	if step.ID == "" {
		panic("doPublish: step.ID must not be empty")
	}
	ctx, span := tp.tracer.Start(ctx,
		"dagnats.engine enqueueTask",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("run_id", runID),
			attribute.String("step_id", step.ID),
			attribute.String("task_name", step.Task),
		),
	)
	defer span.End()
	payload := protocol.TaskPayload{
		TaskID:  runID + "." + step.ID,
		RunID:   runID,
		StepID:  step.ID,
		Attempt: attempt,
		Input:   input,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, workflowName, tp.grantPolicy.Load(),
		),
		// Defensive: mint a nonce if the caller passed none, so a future
		// new dispatch path that forgets to thread one cannot silently
		// produce an unverifiable (always-denied) dispatch (#380).
		DispatchNonce: nonceOrMint(dispatchNonce),
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
// re-enqueue. Each iteration's MsgId is distinct for dedup.
func (tp *TaskPublisher) PublishIteration(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	iteration int,
	workflowName string,
	dispatchNonce string,
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
	ctx, span := tp.tracer.Start(ctx,
		"dagnats.engine enqueueTask",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("run_id", runID),
			attribute.String("step_id", step.ID),
			attribute.String("task_name", step.Task),
		),
	)
	defer span.End()
	// The agent-loop Continue re-enqueue (#380): re-apply the grant strip so a
	// granted loop keeps its control-plane capability across iterations, and
	// stamp the run-binding nonce so the iteration's control-plane calls pass
	// VerifyDispatch. A nil holder Loads nil → deny-by-default.
	payload := protocol.TaskPayload{
		TaskID:    runID + "." + step.ID,
		RunID:     runID,
		StepID:    step.ID,
		Iteration: iteration,
		Input:     input,
		RequiredCapabilities: effectiveCapabilities(
			step.RequiredCapabilities, workflowName, tp.grantPolicy.Load(),
		),
		DispatchNonce: nonceOrMint(dispatchNonce),
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
		subject += "." + step.WorkerGroup
	}
	return subject + "." + runID
}

// PublishBatch publishes a task message for each ready step.
// Steps are published concurrently since they are independent.
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
		attempt := run.Steps[step.ID].Attempts
		// The grant strip keys on the workflow name; the run-binding nonce
		// was stamped onto the step state by enqueueReady and rides the
		// in-memory snapshot here (no re-load per task).
		nonce := run.Steps[step.ID].DispatchNonce
		g.Go(func() error {
			return tp.Publish(
				ctx, runID, step, input, attempt,
				run.WorkflowID, nonce,
			)
		})
	}
	return g.Wait()
}
