// internal/engine/admission.go
// Run admission pipeline: singleton check, priority resolution,
// concurrency check. Called once from handleWorkflowStarted.
// Each gate is independent. Adding future gates happens here,
// not in the event handler.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
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

// errAdmissionPermanent marks an admission error that no redelivery
// can cure. handleWorkflowStarted fails the run with the reason
// instead of retrying the start into the dead-letter stream.
var errAdmissionPermanent = errors.New("admission cannot succeed")

// singletonLock is a lock record. Displaced is the run a cancel-mode
// admission took the lock from: the cancel is published after the
// lock moves, so a start that stops in between and is redelivered
// finds its own claim and publishes that cancel again (the event's
// msg-id makes a repeat a no-op).
type singletonLock struct {
	RunID     string `json:"run_id"`
	Displaced string `json:"displaced,omitempty"`
}

// singletonDigestPrefix starts every encoded lock key. The namespace
// is exclusive: no raw key is used as is when it starts with it.
const singletonDigestPrefix = "h."

// singletonLockKey is the lock's KV key: the workflow name, plus the
// configured key's value when there is one. A key that is already a
// valid KV key is used as is, byte for byte, so a lock taken before
// this encoding existed still matches. Anything else (an e-mail
// address, a value with a space) would be refused by every Create, so
// it is replaced by a digest of itself, which is stable across
// deliveries and engines; so is a raw key that starts with the digest
// prefix, which keeps that namespace for digests alone.
func singletonLockKey(
	workflowName string, cfg *dag.SingletonConfig, input json.RawMessage,
) string {
	if workflowName == "" {
		panic("singletonLockKey: workflowName must not be empty")
	}
	if cfg == nil {
		panic("singletonLockKey: cfg must not be nil")
	}
	kvKey := workflowName
	if cfg.Key != "" {
		keyVal, err := dag.ExtractDotPath(cfg.Key, input)
		if err == nil {
			kvKey = workflowName + "." + fmt.Sprintf("%v", keyVal)
		}
	}
	// A raw key in the digest namespace is hashed too, so a workflow
	// named "h" can never hold another key's lock.
	if natsutil.ValidKVKey(kvKey) && !strings.HasPrefix(kvKey, singletonDigestPrefix) {
		return kvKey
	}
	sum := sha256.Sum256([]byte(kvKey))
	return singletonDigestPrefix + hex.EncodeToString(sum[:])
}

// singletonCheck verifies the singleton lock. It fails closed: an
// error reading or writing the lock refuses the admission and never
// admits a second run. The start is then redelivered on the history
// consumer's schedule (about 5.6 minutes over 7 retries). A lock
// bucket that stays unavailable longer than that dead-letters the
// start: the run ID the API returned never gets a snapshot, so `run
// status` reports it not found, and the event is in DEAD_LETTERS for
// replay. An error no retry can cure is errAdmissionPermanent.
func (ac *AdmissionController) singletonCheck(
	ctx context.Context,
	workflowName string,
	cfg *dag.SingletonConfig,
	newRunID string,
	input json.RawMessage,
) (admissionResult, string, error) {
	if newRunID == "" {
		panic("singletonCheck: newRunID must not be empty")
	}
	if cfg == nil {
		panic("singletonCheck: cfg must not be nil")
	}
	kvKey := singletonLockKey(workflowName, cfg, input)
	for attempt := 0; attempt < singletonClaimAttemptsMax; attempt++ {
		result, done, err := ac.trySingletonClaim(ctx, cfg.Mode, kvKey, newRunID)
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
	ctx context.Context, mode dag.SingletonMode, kvKey, newRunID string,
) (admissionResult, bool, error) {
	if kvKey == "" || newRunID == "" {
		panic("trySingletonClaim: kvKey and newRunID must not be empty")
	}
	if ac.singletonKV == nil {
		panic("trySingletonClaim: singletonKV must not be nil")
	}
	_, err := ac.singletonKV.Create(ctx, kvKey, marshalSingletonLock(newRunID, ""))
	if err == nil {
		return admissionResult{}, true, nil
	}
	if errors.Is(err, jetstream.ErrInvalidKey) {
		return admissionResult{}, false, fmt.Errorf(
			"%w: singleton lock key %q: %w", errAdmissionPermanent, kvKey, err)
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
	lock, held, err := ac.singletonHolder(ctx, entry)
	if err != nil {
		return admissionResult{}, false, err
	}
	if lock.RunID == newRunID {
		// Our own claim, from a delivery of this start that stopped
		// before saving the run; finish the cancel it may not have sent.
		return admissionResult{cancelID: lock.Displaced}, true, nil
	}
	if !held {
		return ac.updateSingletonLock(ctx, kvKey,
			marshalSingletonLock(newRunID, ""), entry.Revision(), admissionResult{})
	}
	return ac.applySingletonMode(ctx, mode, kvKey, newRunID, lock.RunID, entry.Revision())
}

// marshalSingletonLock encodes a lock held by runID, displacing
// displaced (empty when it displaces nobody).
func marshalSingletonLock(runID, displaced string) []byte {
	if runID == "" {
		panic("marshalSingletonLock: runID must not be empty")
	}
	if runID == displaced {
		panic("marshalSingletonLock: a run cannot displace itself")
	}
	data, err := json.Marshal(singletonLock{RunID: runID, Displaced: displaced})
	if err != nil {
		panic("marshalSingletonLock: " + err.Error())
	}
	return data
}

// singletonHolder decodes entry's lock and reports whether its holder
// still holds it. An unreadable lock, a finished holder, and a holder
// with no snapshot past singletonUnsavedGrace are stale (an unreadable
// lock decodes to an empty RunID). Any other failure to load the
// holder is an error: the holder may be live.
func (ac *AdmissionController) singletonHolder(
	ctx context.Context, entry jetstream.KeyValueEntry,
) (singletonLock, bool, error) {
	if entry == nil {
		panic("singletonHolder: entry must not be nil")
	}
	if ac.store == nil {
		panic("singletonHolder: store must not be nil")
	}
	var lock singletonLock
	if err := json.Unmarshal(entry.Value(), &lock); err != nil || lock.RunID == "" {
		return singletonLock{}, false, nil
	}
	holderRun, err := ac.store.Load(ctx, lock.RunID)
	if errors.Is(err, ErrRunNotFound) {
		// Created is the server's timestamp and time.Now this engine's
		// clock, so skew between them shifts the grace by the skew: an
		// engine clock running more than the grace ahead could reclaim
		// a lock whose holder is still between its claim and its first
		// save. Hosts are expected to run NTP.
		unsaved := time.Since(entry.Created()) < singletonUnsavedGrace
		return lock, unsaved, nil
	}
	if err != nil {
		return singletonLock{}, false, fmt.Errorf("load singleton holder %s: %w", lock.RunID, err)
	}
	return lock, !holderRun.Status.IsTerminal(), nil
}

// updateSingletonLock moves the lock to lockData at revision and
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

// applySingletonMode handles the mode-based action for a lock held by
// existingRunID. Skip needs no write; cancel moves the lock to
// newRunID, recording the holder it displaces, and cancels the holder
// only once the lock has moved.
func (ac *AdmissionController) applySingletonMode(
	ctx context.Context,
	mode dag.SingletonMode,
	kvKey, newRunID, existingRunID string,
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
			ctx, kvKey, marshalSingletonLock(newRunID, existingRunID), revision,
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

// publishCancelHook is a test-only seam (nil in production) run before
// the cancel publish; an error it returns stands in for a failed
// publish, which no real fault can be made to produce on demand.
var publishCancelHook func() error

// publishWorkflowCancelledEvent publishes a cancel event
// onto the history stream so handleWorkflowCancelled picks
// it up through the normal event loop.
func (ac *AdmissionController) publishWorkflowCancelledEvent(
	runID string,
) error {
	if runID == "" {
		panic(
			"publishWorkflowCancelledEvent: empty runID",
		)
	}
	if ac.tp == nil {
		panic("publishWorkflowCancelledEvent: tp must not be nil")
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal cancel of %s: %w", runID, err)
	}
	if publishCancelHook != nil {
		if err := publishCancelHook(); err != nil {
			return fmt.Errorf("publish cancel of %s: %w", runID, err)
		}
	}
	_, err = ac.tp.JSPublish(
		context.Background(), evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
	if err != nil {
		return fmt.Errorf("publish cancel of %s: %w", runID, err)
	}
	return nil
}
