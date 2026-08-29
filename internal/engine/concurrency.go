package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// concurrencyCASRetriesMax bounds the optimistic-concurrency retry
// loop shared by every KV-CAS operation in this file (TigerStyle:
// bounded loops).
const concurrencyCASRetriesMax = 10

// ConcurrencyManager enforces run and step concurrency limits using
// NATS KV with optimistic locking. Thread-safe.
//
// Run-level limits (AcquireRun/ReleaseRun) are backed by a bounded
// MEMBER SET keyed by run ID, not a bare integer counter (#648 PR
// review round 3). A bare counter has no notion of WHICH run holds a
// slot, so a release replayed for a run that already released (the
// reconciler's ReleasePending sweep can replay arbitrarily late, e.g.
// if the flag-clear save after a successful release itself fails)
// decrements whatever the counter currently reads -- which may by
// then represent a DIFFERENT run's legitimately held slot. A member
// set makes release-by-run-ID naturally idempotent: removing a run
// that isn't a member is a no-op, so a replay can never free a slot
// it doesn't own. Task-level limits (AcquireTask/ReleaseTask) stay
// counter-based: they gate a task TYPE's in-flight execution slots,
// not a specific run's admission, and are not subject to the
// reconciler's ReleasePending replay path.
type ConcurrencyManager struct {
	runKV  jetstream.KeyValue
	taskKV jetstream.KeyValue
}

// NewConcurrencyManager creates a manager using the concurrency_runs
// and concurrency_tasks KV buckets. Panics if buckets don't exist.
func NewConcurrencyManager(
	js jetstream.JetStream,
) *ConcurrencyManager {
	if js == nil {
		panic("NewConcurrencyManager: js must not be nil")
	}
	ctx := context.Background()
	runKV, err := js.KeyValue(ctx, "concurrency_runs")
	if err != nil {
		panic("NewConcurrencyManager: concurrency_runs: " +
			err.Error())
	}
	taskKV, err := js.KeyValue(ctx, "concurrency_tasks")
	if err != nil {
		panic("NewConcurrencyManager: concurrency_tasks: " +
			err.Error())
	}
	return &ConcurrencyManager{
		runKV: runKV, taskKV: taskKV,
	}
}

// NewConcurrencyManagerSafe creates a manager using the
// concurrency_runs and concurrency_tasks KV buckets. Returns nil
// if the runs bucket doesn't exist. Tasks bucket is optional.
func NewConcurrencyManagerSafe(
	js jetstream.JetStream,
) (*ConcurrencyManager, error) {
	if js == nil {
		panic("NewConcurrencyManagerSafe: js must not be nil")
	}
	ctx := context.Background()
	runKV, err := js.KeyValue(ctx, "concurrency_runs")
	if err != nil {
		return nil, err
	}
	taskKV, _ := js.KeyValue(ctx, "concurrency_tasks")
	return &ConcurrencyManager{
		runKV: runKV, taskKV: taskKV,
	}, nil
}

// runMembership is the JSON shape stored at "workflow.<id>" in
// concurrency_runs: the set of run IDs currently holding a slot for
// that workflow. len(Members) is the active count; it is always
// <= the caller-supplied limit by construction (AcquireRun refuses to
// grow past it).
type runMembership struct {
	Members []string `json:"members"`
}

// AcquireRun claims a run-concurrency slot for runID under workflowID.
// Returns false if the limit is already held by other runs. Limit 0
// means unlimited. Re-acquiring a runID that already holds a slot is
// a no-op success (idempotent), not a second slot.
func (cm *ConcurrencyManager) AcquireRun(
	ctx context.Context, workflowID, runID string, limit int,
) (bool, error) {
	if workflowID == "" {
		panic("AcquireRun: workflowID must not be empty")
	}
	if runID == "" {
		panic("AcquireRun: runID must not be empty")
	}
	if limit <= 0 {
		return true, nil // Unlimited
	}

	key := "workflow." + workflowID

	for attempt := 0; attempt < concurrencyCASRetriesMax; attempt++ {
		members, rev, err := cm.readMembers(ctx, key)
		if err != nil {
			return false, err
		}
		if containsMember(members, runID) {
			return true, nil // Already holds a slot -- idempotent.
		}
		if len(members) >= limit {
			return false, nil
		}
		newMembers := append(append([]string{}, members...), runID)
		if len(newMembers) > limit {
			panic("AcquireRun: member set grew beyond limit")
		}
		if cm.casWriteMembers(ctx, key, newMembers, rev) {
			return true, nil
		}
		// CAS failed — retry
	}
	return false, fmt.Errorf("acquire: too many CAS retries")
}

// ReleaseRun releases runID's slot under workflowID, if held.
// Releasing a runID that is not (or no longer) a member is a safe
// no-op — this is what makes a replayed release idempotent regardless
// of how late it lands (#648 PR review round 3).
func (cm *ConcurrencyManager) ReleaseRun(
	ctx context.Context, workflowID, runID string,
) error {
	if workflowID == "" {
		panic("ReleaseRun: workflowID must not be empty")
	}
	if runID == "" {
		panic("ReleaseRun: runID must not be empty")
	}
	key := "workflow." + workflowID

	for attempt := 0; attempt < concurrencyCASRetriesMax; attempt++ {
		members, rev, err := cm.readMembers(ctx, key)
		if err != nil {
			return err
		}
		if !containsMember(members, runID) {
			return nil // Not held (or already released) -- no-op.
		}
		newMembers := removeMember(members, runID)
		if cm.casWriteMembers(ctx, key, newMembers, rev) {
			return nil
		}
		// CAS failed — retry
	}
	return fmt.Errorf("release: too many CAS retries")
}

// containsMember reports whether runID is present in members.
func containsMember(members []string, runID string) bool {
	for _, m := range members {
		if m == runID {
			return true
		}
	}
	return false
}

// removeMember returns a copy of members with runID removed (a
// no-op copy if runID is absent -- callers check presence first so
// this path is only hit when it IS present).
func removeMember(members []string, runID string) []string {
	out := make([]string, 0, len(members))
	for _, m := range members {
		if m != runID {
			out = append(out, m)
		}
	}
	return out
}

// readMembers reads the run-membership set at key. A missing key
// reads as an empty set with revision 0 (so the caller's next CAS
// write is a Create). A value that fails to unmarshal as
// runMembership -- either corrupt JSON or the LEGACY plain-integer
// counter format this replaces -- also reads as an empty set, but
// keeps the entry's real revision so the caller's next CAS write
// lands as an Update, migrating the key to the new format in place on
// first touch.
//
// The legacy-format case is a deliberate, documented one-time
// under-count: a bare integer counter has no record of WHICH run IDs
// it was counting, so there is no way to reconstruct the correct
// initial member set from it. Any workflow mid-flight at the moment
// of this upgrade may briefly admit more concurrent runs than its
// limit, until the runs the old counter was tracking finish (their
// ReleaseRun calls will find them absent from the new empty set and
// no-op, same as any other release of a run that already isn't a
// member). This window is bounded to the runs in flight at upgrade
// time and self-heals as they complete.
func (cm *ConcurrencyManager) readMembers(
	ctx context.Context, key string,
) ([]string, uint64, error) {
	entry, err := cm.runKV.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	var m runMembership
	if unmarshalErr := json.Unmarshal(entry.Value(), &m); unmarshalErr != nil {
		return nil, entry.Revision(), nil
	}
	return m.Members, entry.Revision(), nil
}

// casWriteMembers CAS-writes the member set at key: Create if rev==0
// (key never existed), Update guarded by rev otherwise (covers both
// a fresh key at its real revision and a legacy-format value being
// migrated in place). Returns false on any CAS conflict so the
// caller's bounded retry loop re-reads and retries.
func (cm *ConcurrencyManager) casWriteMembers(
	ctx context.Context, key string, members []string, rev uint64,
) bool {
	data, err := json.Marshal(runMembership{Members: members})
	if err != nil {
		return false
	}
	if rev == 0 {
		_, err = cm.runKV.Create(ctx, key, data)
	} else {
		_, err = cm.runKV.Update(ctx, key, data, rev)
	}
	return err == nil
}

// AcquireTask increments the counter for a task type. Returns false
// if the limit is reached. Limit 0 means unlimited.
func (cm *ConcurrencyManager) AcquireTask(
	ctx context.Context, taskType string, limit int,
) (bool, error) {
	if taskType == "" {
		panic("AcquireTask: taskType must not be empty")
	}
	if limit <= 0 {
		return true, nil // Unlimited
	}
	if cm.taskKV == nil {
		return true, nil // No bucket — allow
	}

	key := "task." + taskType
	for attempt := 0; attempt < concurrencyCASRetriesMax; attempt++ {
		current, rev, err := cm.readKV(ctx, cm.taskKV, key)
		if err != nil {
			return false, err
		}
		if current >= limit {
			return false, nil
		}
		if cm.casIncrementKV(ctx, cm.taskKV, key, current, rev) {
			return true, nil
		}
	}
	return false, fmt.Errorf("acquire task: too many CAS retries")
}

// ReleaseTask decrements the counter for a task type.
func (cm *ConcurrencyManager) ReleaseTask(
	ctx context.Context, taskType string,
) error {
	if taskType == "" {
		panic("ReleaseTask: taskType must not be empty")
	}
	if cm.taskKV == nil {
		return nil // No bucket — no-op
	}

	key := "task." + taskType
	for attempt := 0; attempt < concurrencyCASRetriesMax; attempt++ {
		current, rev, err := cm.readKV(ctx, cm.taskKV, key)
		if err != nil {
			return err
		}
		if current <= 0 {
			return nil // Already at zero
		}
		newVal := current - 1
		data := []byte(strconv.Itoa(newVal))
		if rev == 0 {
			_, err = cm.taskKV.Create(ctx, key, data)
		} else {
			_, err = cm.taskKV.Update(ctx, key, data, rev)
		}
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("release task: too many CAS retries")
}

// readKV reads a counter from any KV bucket.
func (cm *ConcurrencyManager) readKV(
	ctx context.Context, kv jetstream.KeyValue, key string,
) (int, uint64, error) {
	if kv == nil {
		panic("readKV: kv must not be nil")
	}
	if key == "" {
		panic("readKV: key must not be empty")
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	val, err := strconv.Atoi(string(entry.Value()))
	if err != nil {
		return 0, entry.Revision(), nil
	}
	return val, entry.Revision(), nil
}

// casIncrementKV performs a CAS increment on any KV bucket.
func (cm *ConcurrencyManager) casIncrementKV(
	ctx context.Context, kv jetstream.KeyValue, key string,
	current int, rev uint64,
) bool {
	if kv == nil {
		panic("casIncrementKV: kv must not be nil")
	}
	if key == "" {
		panic("casIncrementKV: key must not be empty")
	}
	newVal := current + 1
	data := []byte(strconv.Itoa(newVal))
	var err error
	if rev == 0 {
		_, err = kv.Create(ctx, key, data)
	} else {
		_, err = kv.Update(ctx, key, data, rev)
	}
	return err == nil
}
