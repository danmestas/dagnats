// internal/engine/admission.go
// Run admission pipeline: singleton check, priority resolution,
// concurrency check. Called once from handleWorkflowStarted.
// Each gate is independent. Adding future gates happens here,
// not in the event handler.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

type admissionAction int

const (
	admissionProceed admissionAction = iota
	admissionSkip
	admissionQueue
)

type admissionResult struct {
	action       admissionAction
	cancelID     string // singleton cancel mode
	offset       int    // priority offset
	singletonKey string // KV key for lock release
}

// admitRun evaluates all flow control gates in order.
func (o *Orchestrator) admitRun(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	input json.RawMessage,
) (admissionResult, error) {
	if run.RunID == "" {
		panic("admitRun: RunID must not be empty")
	}
	var result admissionResult

	// 1. Singleton
	if wfDef.Singleton != nil && o.singletonKV != nil {
		sResult, kvKey, err := o.singletonCheck(
			ctx, wfDef.Name, wfDef.Singleton,
			run.RunID, input,
		)
		if err != nil {
			return result, err
		}
		result.singletonKey = kvKey
		if sResult.action == admissionSkip {
			slog.InfoContext(ctx, "singleton skip",
				"run_id", run.RunID,
			)
			result.action = admissionSkip
			return result, nil
		}
		result.cancelID = sResult.cancelID
	}

	// 2. Priority
	result.offset = dag.ResolvePriority(
		wfDef.Priority, input,
	)

	// 3. Concurrency
	if wfDef.Concurrency != nil && o.concurrency != nil {
		acquired, err := o.concurrency.AcquireRun(
			ctx, wfDef.Name, wfDef.Concurrency.MaxRuns,
		)
		if err != nil {
			return result, fmt.Errorf(
				"acquire run: %w", err,
			)
		}
		if !acquired {
			result.action = admissionQueue
		}
	}

	return result, nil
}

// singletonCheck verifies the singleton lock. Returns an
// admissionResult directly (not a tuple) for interface clarity.
func (o *Orchestrator) singletonCheck(
	ctx context.Context,
	workflowName string,
	cfg *dag.SingletonConfig,
	newRunID string,
	input json.RawMessage,
) (admissionResult, string, error) {
	if workflowName == "" {
		panic(
			"singletonCheck: workflowName must not be empty",
		)
	}
	if cfg == nil {
		panic("singletonCheck: cfg must not be nil")
	}
	kvKey := workflowName
	if cfg.Key != "" {
		keyVal, err := dag.ExtractDotPath(
			cfg.Key, input,
		)
		if err == nil {
			kvKey = workflowName + "." +
				fmt.Sprintf("%v", keyVal)
		}
	}

	lockData, _ := json.Marshal(map[string]string{
		"run_id": newRunID,
	})

	// Try to claim
	_, err := o.singletonKV.Create(
		ctx, kvKey, lockData,
	)
	if err == nil {
		return admissionResult{}, kvKey, nil
	}

	// Key exists -- check if stale
	entry, err := o.singletonKV.Get(ctx, kvKey)
	if err != nil {
		return admissionResult{}, kvKey, nil
	}
	var lock struct {
		RunID string `json:"run_id"`
	}
	if unmarshalErr := json.Unmarshal(
		entry.Value(), &lock,
	); unmarshalErr != nil {
		return admissionResult{}, kvKey, nil
	}

	// Verify existing run is active
	existingRun, loadErr := o.store.Load(ctx, lock.RunID)
	if loadErr != nil ||
		existingRun.Status.IsTerminal() {
		// Stale lock -- reclaim
		_, updateErr := o.singletonKV.Update(
			ctx, kvKey, lockData, entry.Revision(),
		)
		if updateErr != nil {
			return admissionResult{}, kvKey, nil
		}
		return admissionResult{}, kvKey, nil
	}

	// Active run exists
	return o.applySingletonMode(
		ctx, cfg.Mode, kvKey, lock.RunID,
		lockData, entry.Revision(),
	)
}

// applySingletonMode handles the mode-based action for an
// active singleton lock. Extracted to keep singletonCheck
// within the 70-line limit.
func (o *Orchestrator) applySingletonMode(
	ctx context.Context,
	mode dag.SingletonMode,
	kvKey string,
	existingRunID string,
	lockData []byte,
	revision uint64,
) (admissionResult, string, error) {
	if kvKey == "" {
		panic(
			"applySingletonMode: kvKey must not be empty",
		)
	}
	if existingRunID == "" {
		panic(
			"applySingletonMode: existingRunID not empty",
		)
	}
	switch mode {
	case dag.SingletonModeSkip:
		return admissionResult{action: admissionSkip},
			kvKey, nil
	case dag.SingletonModeCancel:
		_, updateErr := o.singletonKV.Update(
			ctx, kvKey, lockData,
			revision,
		)
		if updateErr != nil {
			slog.ErrorContext(ctx,
				"singleton cancel: update lock failed",
				"error", updateErr,
			)
		}
		return admissionResult{cancelID: existingRunID},
			kvKey, nil
	default:
		panic("applySingletonMode: unknown mode")
	}
}

// releaseSingletonLock deletes the lock if it belongs to
// this run. Uses SingletonKey stored on the run -- no need
// to reload the workflow def or recompute the key path.
func (o *Orchestrator) releaseSingletonLock(
	ctx context.Context, run dag.WorkflowRun,
) {
	if o.singletonKV == nil {
		return
	}
	if run.SingletonKey == "" {
		return
	}
	entry, err := o.singletonKV.Get(
		ctx, run.SingletonKey,
	)
	if err != nil {
		return
	}
	var lock struct {
		RunID string `json:"run_id"`
	}
	if unmarshalErr := json.Unmarshal(
		entry.Value(), &lock,
	); unmarshalErr != nil {
		return
	}
	if lock.RunID == run.RunID {
		if deleteErr := o.singletonKV.Delete(
			ctx, run.SingletonKey,
		); deleteErr != nil {
			slog.ErrorContext(ctx,
				"release singleton lock failed",
				"error", deleteErr,
				"key", run.SingletonKey,
			)
		}
	}
}

// publishWorkflowCancelledEvent publishes a cancel event
// onto the history stream so handleWorkflowCancelled picks
// it up through the normal event loop.
func (o *Orchestrator) publishWorkflowCancelledEvent(
	runID string,
) {
	if runID == "" {
		panic(
			"publishWorkflowCancelledEvent: empty runID",
		)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	_, pubErr := o.js.Publish(
		context.Background(), evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	if pubErr != nil {
		slog.ErrorContext(context.Background(),
			"publish cancel event failed",
			"error", pubErr,
			"run_id", runID,
		)
	}
}
