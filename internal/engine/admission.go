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
	"time"

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

// singletonClaimAttemptsMax bounds the claim loop. Each extra pass
// follows a rival changing the lock between this admission's read and
// its write; past the bound the start is retried by redelivery.
const singletonClaimAttemptsMax = 4

// singletonUnsavedGrace is how long a lock whose holder has no run
// snapshot still counts as held. Admission claims the lock before the
// run is first saved, so a concurrent start can find a lock whose
// holder is not stored yet; only a lock older than this (a holder that
// crashed before its first save) is stale.
const singletonUnsavedGrace = 2 * time.Minute

// singletonCheck verifies the singleton lock. It fails closed: an
// error reading or writing the lock refuses the admission (the start
// is retried by redelivery) and never admits a second run.
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
	lockData, err := json.Marshal(map[string]string{
		"run_id": newRunID,
	})
	if err != nil {
		return admissionResult{}, kvKey, fmt.Errorf("marshal singleton lock: %w", err)
	}
	for attempt := 0; attempt < singletonClaimAttemptsMax; attempt++ {
		result, done, err := ac.trySingletonClaim(
			ctx, cfg.Mode, kvKey, newRunID, lockData,
		)
		if err != nil {
			return admissionResult{}, kvKey, err
		}
		if done {
			return result, kvKey, nil
		}
	}
	return admissionResult{}, kvKey, fmt.Errorf(
		"singleton lock %s changed %d times during admission",
		kvKey, singletonClaimAttemptsMax,
	)
}

// trySingletonClaim is one pass of the claim loop. done is false when
// a rival changed the lock between the read and the write, and the
// caller should look again.
func (ac *AdmissionController) trySingletonClaim(
	ctx context.Context,
	mode dag.SingletonMode,
	kvKey, newRunID string,
	lockData []byte,
) (admissionResult, bool, error) {
	if kvKey == "" || newRunID == "" {
		panic("trySingletonClaim: kvKey and newRunID must not be empty")
	}
	if ac.singletonKV == nil {
		panic("trySingletonClaim: singletonKV must not be nil")
	}
	_, err := ac.singletonKV.Create(ctx, kvKey, lockData)
	if err == nil {
		return admissionResult{}, true, nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return admissionResult{}, false, fmt.Errorf("claim singleton lock %s: %w", kvKey, err)
	}
	entry, err := ac.singletonKV.Get(ctx, kvKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return admissionResult{}, false, nil // released in between
	}
	if err != nil {
		return admissionResult{}, false, fmt.Errorf("read singleton lock %s: %w", kvKey, err)
	}
	holder, held, err := ac.singletonHolder(ctx, entry)
	if err != nil {
		return admissionResult{}, false, err
	}
	if holder == newRunID {
		// Our own claim, from a delivery of this start that stopped
		// before saving the run.
		return admissionResult{}, true, nil
	}
	if !held {
		return ac.updateSingletonLock(ctx, kvKey, lockData, entry.Revision(), admissionResult{})
	}
	return ac.applySingletonMode(
		ctx, mode, kvKey, holder,
		lockData, entry.Revision(),
	)
}

// singletonHolder names the run holding entry's lock and whether it
// still holds it. An unreadable lock, a finished holder, and a holder
// with no snapshot past singletonUnsavedGrace are stale. Any other
// failure to load the holder is an error: the holder may be live.
func (ac *AdmissionController) singletonHolder(
	ctx context.Context, entry jetstream.KeyValueEntry,
) (string, bool, error) {
	if entry == nil {
		panic("singletonHolder: entry must not be nil")
	}
	if ac.store == nil {
		panic("singletonHolder: store must not be nil")
	}
	var lock struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(entry.Value(), &lock); err != nil || lock.RunID == "" {
		return "", false, nil
	}
	holderRun, err := ac.store.Load(ctx, lock.RunID)
	if errors.Is(err, ErrRunNotFound) {
		unsaved := time.Since(entry.Created()) < singletonUnsavedGrace
		return lock.RunID, unsaved, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load singleton holder %s: %w", lock.RunID, err)
	}
	return lock.RunID, !holderRun.Status.IsTerminal(), nil
}

// updateSingletonLock moves the lock to this admission at revision and
// returns result once it has. A revision mismatch means a rival moved
// it first: done is false and the caller looks again.
func (ac *AdmissionController) updateSingletonLock(
	ctx context.Context, kvKey string, lockData []byte, revision uint64,
	result admissionResult,
) (admissionResult, bool, error) {
	if kvKey == "" {
		panic("updateSingletonLock: kvKey must not be empty")
	}
	if revision == 0 {
		panic("updateSingletonLock: revision must be positive")
	}
	_, err := ac.singletonKV.Update(ctx, kvKey, lockData, revision)
	if err == nil {
		return result, true, nil
	}
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) {
		return admissionResult{}, false, nil
	}
	return admissionResult{}, false, fmt.Errorf("move singleton lock %s: %w", kvKey, err)
}

// applySingletonMode handles the mode-based action for an
// active singleton lock. Skip needs no write; cancel moves the lock to
// the new run first and cancels the holder only once it has.
func (ac *AdmissionController) applySingletonMode(
	ctx context.Context,
	mode dag.SingletonMode,
	kvKey string,
	existingRunID string,
	lockData []byte,
	revision uint64,
) (admissionResult, bool, error) {
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
		}, true, nil
	case dag.SingletonModeCancel:
		return ac.updateSingletonLock(
			ctx, kvKey, lockData, revision,
			admissionResult{cancelID: existingRunID},
		)
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
