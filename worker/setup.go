package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// setupInFlightMax bounds how many task types Start sets up at once.
// Each type's setup is a few JetStream API round trips to the stream
// leader; on a slow link (160 ms and more between mesh nodes) a program
// with 16 task types spent about 30 s setting them up one at a time.
const setupInFlightMax = 8

// taskQueueView is one listing of TASK_QUEUES' consumers, shared by
// every task type's orphan cleanup and cross-process collision check
// instead of listing the stream twice per type.
type taskQueueView struct {
	stream    jetstream.Stream
	consumers []*jetstream.ConsumerInfo
}

// loadTaskQueueView opens TASK_QUEUES and lists its consumers once.
// Iterator form, not a single page: past page 1 a stale orphan would
// re-trigger #136 in deployments with enough state.
func (w *Worker) loadTaskQueueView(ctx context.Context) (taskQueueView, error) {
	if ctx == nil || w.js == nil {
		panic("loadTaskQueueView: ctx and js must not be nil")
	}
	stream, err := w.js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		return taskQueueView{}, fmt.Errorf("orphan cleanup: open TASK_QUEUES: %w", err)
	}
	view := taskQueueView{stream: stream}
	iter := stream.ListConsumers(ctx)
	for info := range iter.Info() {
		view.consumers = append(view.consumers, info)
	}
	if err := iter.Err(); err != nil {
		return taskQueueView{}, fmt.Errorf(
			"orphan cleanup: list TASK_QUEUES consumers: %w", err)
	}
	return view, nil
}

// precheckTaskQueues runs every pull consumer's orphan cleanup and
// cross-process collision check, in task-type order, against one
// listing. Serial and on the caller's goroutine: a collision panics,
// and cleanup must finish before the type's durable is created. An
// elastic worker's consumer groups run neither check.
func (w *Worker) precheckTaskQueues(types []string) error {
	if len(types) == 0 {
		panic("precheckTaskQueues: no task types")
	}
	if w.partitions > 0 {
		return nil
	}
	ctx := context.Background()
	view, err := w.loadTaskQueueView(ctx)
	if err != nil {
		return err
	}
	groups := w.groups
	if len(groups) == 0 {
		groups = []string{""}
	}
	for _, taskType := range types {
		for _, group := range groups {
			if err := w.precheckPullConsumer(ctx, view, taskType, group); err != nil {
				return err
			}
		}
	}
	return nil
}

// precheckPullConsumer is one (taskType, group)'s orphan cleanup, then
// its collision check. The two operate on disjoint consumer sets --
// cleanup on ephemerals with our filter, the collision check on
// durables with our name -- so order is interchangeable for
// correctness. Cleanup-first preserves the timing of cleanup's
// concurrent-dedup race (ADR-006 §3). The collision check (ADR-010) is
// best-effort: it catches the steady-state collision after one worker
// creates the durable, though a TOCTOU window at the create remains.
func (w *Worker) precheckPullConsumer(
	ctx context.Context, view taskQueueView, taskType, group string,
) error {
	if taskType == "" || view.stream == nil {
		panic("precheckPullConsumer: taskType and view must be set")
	}
	durable := consumerNameFor(taskType, group)
	filter := consumerFilterFor(taskType, group)
	if err := cleanupOrphanEphemerals(ctx, view, filter, durable); err != nil {
		return err
	}
	return assertNoCrossProcessCollision(ctx, view, filter, durable)
}

// subscribeTasks sets up every task type's consumers, up to
// w.setupInFlight at a time, after precheckTaskQueues. Errors are
// joined in task-type order, so the first type to fail leads.
func (w *Worker) subscribeTasks(types []string) error {
	if len(types) == 0 || w.setupInFlight < 1 {
		panic("subscribeTasks: types and a positive in-flight bound are required")
	}
	// Missing stream is fine: sticky routing is optional.
	sticky, err := w.js.Stream(context.Background(), "STICKY_TASKS")
	if err != nil {
		sticky = nil
	}
	errs := make([]error, len(types))
	slots := make(chan struct{}, w.setupInFlight)
	var wg sync.WaitGroup
	for i, taskType := range types {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			errs[i] = w.setupTask(taskType, w.handlers[taskType], sticky)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// setupTask creates one task type's consumers and starts consuming:
// its elastic group, or one pull consumer per worker group, or its pull
// consumer plus its sticky consumer for worker-affinity routing. Each
// consumer joins w.lanes (elastic ones w.stoppers) as soon as it exists,
// so a later failure's Stop ends it.
func (w *Worker) setupTask(
	taskType string, handler HandlerFunc, sticky jetstream.Stream,
) error {
	if taskType == "" {
		panic("setupTask: taskType must not be empty")
	}
	if handler == nil {
		panic("setupTask: handler must not be nil")
	}
	if w.partitions > 0 {
		return w.subscribeElastic(taskType, handler)
	}
	if len(w.groups) > 0 {
		for _, group := range w.groups {
			checkStrandedGroupSubject(context.Background(), w.js, taskType, group)
			if _, err := w.createPullConsumer(taskType, group, handler); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err := w.createPullConsumer(taskType, "", handler); err != nil {
		return err
	}
	if sticky != nil {
		w.createStickyConsumer(taskType, handler, sticky)
	}
	return nil
}
