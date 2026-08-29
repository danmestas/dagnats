// internal/engine/admission.go
// Run admission pipeline: singleton check, priority resolution,
// concurrency check. Called once from handleWorkflowStarted.
// Each gate is independent. Adding future gates happens here,
// not in the event handler.
package engine

import (
	"context"
	"encoding/json"
	"errors"
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
	skippedBy    string // singleton skip mode: run ID holding the lock
	offset       int    // priority offset
	singletonKey string // KV key for lock release
}

// Admit evaluates all flow control gates in order.
func (ac *AdmissionController) Admit(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	input json.RawMessage,
) (admissionResult, error) {
	if run.RunID == "" {
		panic("Admit: RunID must not be empty")
	}
	var result admissionResult

	// 1. Singleton
	if wfDef.Singleton != nil && ac.singletonKV != nil {
		sResult, kvKey, err := ac.singletonCheck(
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
				"skipped_by", sResult.skippedBy,
			)
			result.action = admissionSkip
			result.skippedBy = sResult.skippedBy
			return result, nil
		}
		result.cancelID = sResult.cancelID
	}

	// 2. Priority
	result.offset = dag.ResolvePriority(
		wfDef.Priority, input,
	)

	// 3. Concurrency
	if wfDef.Concurrency != nil && ac.concurrency != nil {
		acquired, err := ac.concurrency.AcquireRun(
			ctx, wfDef.Name, run.RunID, wfDef.Concurrency.MaxRuns,
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
func (ac *AdmissionController) singletonCheck(
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
	_, err := ac.singletonKV.Create(
		ctx, kvKey, lockData,
	)
	if err == nil {
		return admissionResult{}, kvKey, nil
	}

	// Key exists -- check if stale
	entry, err := ac.singletonKV.Get(ctx, kvKey)
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
	existingRun, loadErr := ac.store.Load(ctx, lock.RunID)
	if loadErr != nil ||
		existingRun.Status.IsTerminal() {
		// Stale lock -- reclaim
		_, updateErr := ac.singletonKV.Update(
			ctx, kvKey, lockData, entry.Revision(),
		)
		if updateErr != nil {
			return admissionResult{}, kvKey, nil
		}
		return admissionResult{}, kvKey, nil
	}

	// Active run exists
	return ac.applySingletonMode(
		ctx, cfg.Mode, kvKey, lock.RunID,
		lockData, entry.Revision(),
	)
}

// applySingletonMode handles the mode-based action for an
// active singleton lock. Extracted to keep singletonCheck
// within the 70-line limit.
func (ac *AdmissionController) applySingletonMode(
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
		return admissionResult{
			action:    admissionSkip,
			skippedBy: existingRunID,
		}, kvKey, nil
	case dag.SingletonModeCancel:
		_, updateErr := ac.singletonKV.Update(
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

// releaseSingletonLockRaceHook is a test-only seam (default no-op)
// called between ReleaseSingletonLock's ownership Get and its
// revision-guarded Delete. Production never overrides it. It exists
// because the TOCTOU window it brackets -- a NEW run reclaiming the
// same key between our Get and our Delete -- is only reachable under
// genuine concurrent execution; a test needs to force that
// interleaving deterministically rather than race a goroutine against
// it. See admission_release_race_test.go.
var releaseSingletonLockRaceHook = func() {}

// ReleaseSingletonLock deletes the lock if it belongs to this run.
// Uses SingletonKey stored on the run -- no need to reload the
// workflow def or recompute the key path.
//
// The Delete is revision-guarded (#648 PR review round 2): a run's
// release can be replayed arbitrarily late by the reconciler's
// ReleasePending sweep, long after the ownership Get below was
// accurate. Without a revision check, a NEW run reclaiming this SAME
// key in the gap between that Get and the Delete would have its
// fresh lock wiped out by a delete-by-key that no longer reflects who
// actually holds it.
//
// The Delete error is classified, not uniformly swallowed (#648 PR
// review round 3): a revision mismatch (jetstream.ErrKeyExists --
// nats.go documents that CAS conflicts on Update/Delete both surface
// through this sentinel, not a distinct one) means exactly what the
// paragraph above describes -- reclaimed by a new run, the safe
// outcome, logged at DEBUG and NOT an error. Any OTHER Delete error
// (a genuine transient KV/network failure) is a real release failure
// and is returned so the caller (releaseAdmission) reports it as an
// afterPersist error, which finalizeRun's ReleasePending debt
// mechanism records durably and the reconciler later retries.
// Returning void here (the pre-round-3 behavior) would have logged
// and silently dropped that failure with no recovery path at all.
func (ac *AdmissionController) ReleaseSingletonLock(
	ctx context.Context, run dag.WorkflowRun,
) error {
	if ac.singletonKV == nil {
		return nil
	}
	if run.SingletonKey == "" {
		return nil
	}
	entry, err := ac.singletonKV.Get(
		ctx, run.SingletonKey,
	)
	if err != nil {
		return nil
	}
	var lock struct {
		RunID string `json:"run_id"`
	}
	if unmarshalErr := json.Unmarshal(
		entry.Value(), &lock,
	); unmarshalErr != nil {
		return nil
	}
	if lock.RunID != run.RunID {
		return nil
	}
	releaseSingletonLockRaceHook()
	deleteErr := ac.singletonKV.Delete(
		ctx, run.SingletonKey,
		jetstream.LastRevision(entry.Revision()),
	)
	if deleteErr == nil {
		return nil
	}
	if errors.Is(deleteErr, jetstream.ErrKeyExists) {
		slog.DebugContext(ctx,
			"release singleton lock: revision changed since Get -- "+
				"lock was reclaimed by a new run, treating as "+
				"already released",
			"error", deleteErr,
			"key", run.SingletonKey,
		)
		return nil
	}
	slog.ErrorContext(ctx,
		"release singleton lock failed",
		"error", deleteErr,
		"key", run.SingletonKey,
	)
	return fmt.Errorf("release singleton lock: %w", deleteErr)
}

// publishWorkflowCancelledEvent publishes a cancel event
// onto the history stream so handleWorkflowCancelled picks
// it up through the normal event loop.
func (ac *AdmissionController) publishWorkflowCancelledEvent(
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
	_, pubErr := ac.tp.JSPublish(
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
