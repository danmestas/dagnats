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
	"github.com/danmestas/dagnats/observe"
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
type TaskPublisher struct {
	js          jetstream.JetStream
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
}

// NewTaskPublisher creates a TaskPublisher with the given deps.
func NewTaskPublisher(
	js jetstream.JetStream,
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
		ctx, step, runID, input,
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
				ctx, step, runID, input,
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
				workerID, wfDef.Sticky,
			)
		}
	}

	return tp.doPublish(ctx, runID, step, input, attempt)
}

// checkRateLimit evaluates rate limits for the step. Returns
// delayed=true if the task was deferred via SleepTimer.
func (tp *TaskPublisher) checkRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
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

	if step.RateLimit != nil {
		return tp.applyGlobalRateLimit(
			ctx, step, runID, input,
		)
	}
	if step.KeyedRateLimit != nil {
		return tp.applyKeyedRateLimit(
			ctx, step, runID, input,
		)
	}
	return false, nil
}

// applyGlobalRateLimit checks the global rate limit for this
// task type and schedules a retry if tokens are exhausted.
func (tp *TaskPublisher) applyGlobalRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
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
		ctx, step, runID, input, retryAfter,
	)
}

// applyKeyedRateLimit checks the per-key rate limit for this
// task and schedules a retry if tokens are exhausted.
func (tp *TaskPublisher) applyKeyedRateLimit(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
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
		ctx, step, runID, input, retryAfter,
	)
}

// scheduleRateRetry schedules a timer to re-attempt task dispatch
// after the rate limit window allows more tokens.
func (tp *TaskPublisher) scheduleRateRetry(
	ctx context.Context, step dag.StepDef, runID string,
	input []byte, retryAfter time.Duration,
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
		Action:     TimerActionRateRetry,
		RunID:      runID,
		StepID:     step.ID,
		DurationMs: durationMs,
		TaskType:   step.Task,
		Input:      input,
	})
}

// scheduleConcurrencyRetry schedules a timer to re-attempt
// task dispatch after the task concurrency slot frees up.
func (tp *TaskPublisher) scheduleConcurrencyRetry(
	ctx context.Context,
	step dag.StepDef, runID string, input []byte,
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
		Action:     TimerActionTaskConcurRetry,
		RunID:      runID,
		StepID:     step.ID,
		DurationMs: 1000,
		TaskType:   step.Task,
		Input:      input,
	})
}

// doPublish performs the actual NATS publish for a task message.
func (tp *TaskPublisher) doPublish(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
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
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}
	msgID := runID + "." + step.ID + ".queued"
	subject := tp.stepSubject(step, runID)
	msg := buildTaskMsg(subject, data, msgID)
	observe.InjectTraceContext(ctx, msg, nil)
	_, err = tp.js.PublishMsg(ctx, msg)
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
	payload := protocol.TaskPayload{
		TaskID:    runID + "." + step.ID,
		RunID:     runID,
		StepID:    step.ID,
		Iteration: iteration,
		Input:     input,
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
	observe.InjectTraceContext(ctx, msg, nil)
	_, err = tp.js.PublishMsg(ctx, msg)
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
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			return fmt.Errorf(
				"resolve input for step %q: %w",
				step.ID, err,
			)
		}
		attempt := run.Steps[step.ID].Attempts
		g.Go(func() error {
			return tp.Publish(
				ctx, runID, step, input, attempt,
			)
		})
	}
	return g.Wait()
}
