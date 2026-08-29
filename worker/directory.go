package worker

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrWorkerIDOwned is returned by RegisterOwned (on register/
// heartbeat) and DeregisterOwned (on disconnect) when workerID's
// current entry is owned by a different token and the caller is
// neither that token nor an admin -- including when the caller lost
// a race to another writer between the ownership check and the
// revision-guarded write.
var ErrWorkerIDOwned = errors.New(
	"worker_id is registered to another token",
)

// ErrWorkerIDContended is returned by RegisterOwned and
// DeregisterOwned when a revision-guarded write loses the race to a
// concurrent writer ownedWriteRetriesMax times in a row -- distinct
// from ErrWorkerIDOwned because contention is not an ownership
// decision: the fresh entry re-Get after each conflict may still
// belong to the caller, we simply never won the write. Callers
// should treat this as transient (e.g. the bridge maps it to 503
// with Retry-After), never as a takeover.
var ErrWorkerIDContended = errors.New(
	"worker_id write lost the revision race too many times",
)

// ownedWriteRetriesMax bounds RegisterOwned/DeregisterOwned's
// Get -> decide -> write retry loop (the fix for the CI-only race
// where a revision conflict caused by the SAME owner -- a heartbeat
// re-register racing a reconnect's write, or a heartbeat racing this
// connection's own disconnect-time deregister -- was wrongly treated
// as an ownership violation). A conflict only proves someone else
// wrote in the window, not who; re-Get and re-run the ownership rule
// against the fresh entry before concluding either way.
const ownedWriteRetriesMax = 5

// ownedWriteBackoffBaseMs is the base delay (in milliseconds) for
// ownedWriteBackoff's jittered exponential backoff between retries:
// 5, 10, 20, 40ms before jitter, doubling per attempt. Keeps
// contending writers (e.g. every worker's heartbeat re-registering
// on the same tick interval) from retrying in lockstep and re-
// colliding on the same revision every attempt.
const ownedWriteBackoffBaseMs = 5

// ownedWriteBackoff returns the sleep duration before retrying
// attempt (0-indexed: the attempt about to run). Attempt 0 backs off
// zero -- there is nothing to back off from yet. Later attempts
// double the base delay and apply +/-25% jitter so multiple
// contenders desynchronize instead of retrying in lockstep.
func ownedWriteBackoff(attempt int) time.Duration {
	if attempt < 0 {
		panic("ownedWriteBackoff: attempt must not be negative")
	}
	if attempt >= ownedWriteRetriesMax {
		panic("ownedWriteBackoff: attempt must be less than ownedWriteRetriesMax")
	}
	if attempt == 0 {
		return 0
	}
	baseMs := float64(ownedWriteBackoffBaseMs) * float64(uint(1)<<uint(attempt-1))
	jitterFrac := 0.75 + rand.Float64()*0.5 // 0.75..1.25
	return time.Duration(baseMs * jitterFrac * float64(time.Millisecond))
}

// sleepOwnedWriteBackoff sleeps for ownedWriteBackoff(attempt),
// returning early if ctx is done first -- a bounded backoff must
// never outlive the caller's own timeout.
func sleepOwnedWriteBackoff(ctx context.Context, attempt int) {
	if ctx == nil {
		panic("sleepOwnedWriteBackoff: ctx must not be nil")
	}
	if attempt < 0 {
		panic("sleepOwnedWriteBackoff: attempt must not be negative")
	}
	d := ownedWriteBackoff(attempt)
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// AdminTokenID is the reserved token_id value written to a
// registration created by the bridge's admin bearer or dev mode
// (#650 round 3). Using "" for these entries made an admin takeover
// indistinguishable from a genuinely unowned entry (a pre-#627
// record, or a native Go worker outside the bridge's scope -- see
// Register/Deregister) and therefore claimable by the next bridge
// token to connect. workertoken.Mint asserts a minted id can never
// equal this value (ids are nuids, so the collision is not reachable
// in practice, but the assertion makes the invariant explicit), so
// an entry carrying AdminTokenID is unambiguously admin-owned: only
// an admin caller (which includes every dev-mode caller -- dev mode
// has no identity to enforce) may re-register or delete it. Defined
// in internal/workertoken (the token-identity package) and re-
// exported here so the dependency runs public -> internal, not the
// reverse.
const AdminTokenID = workertoken.AdminTokenID

// ownershipAllows is the single ownership decision shared by
// RegisterOwned and DeregisterOwned (#650): an admin caller may
// always act; otherwise an entry with no token_id has no identity to
// enforce (pre-#627, or a native Go worker that never went through
// the bridge -- out of #650's bridge scope in both directions) and
// is open to any caller; anything else -- including AdminTokenID --
// requires the caller's token_id to match exactly.
func ownershipAllows(
	existingTokenID, callerTokenID string, callerIsAdmin bool,
) bool {
	if callerIsAdmin {
		return true
	}
	if existingTokenID == "" {
		return true
	}
	return existingTokenID == callerTokenID
}

// MaxWorkerStaleness is the read-time cutoff used by List(): entries
// whose last Put is older than this are treated as dead and filtered
// out. The workers KV bucket has a 60s TTL, but NATS may delay
// purging past the nominal TTL — this filter makes staleness
// deterministic for callers (e.g. `dagnats workers list`) so a
// SIGKILL'd worker stops appearing within MaxWorkerStaleness rather
// than waiting for the next NATS cleanup pass. Matches the bucket
// TTL so dead entries vanish promptly after the heartbeat would
// have refreshed them. Variable rather than const so tests can
// shrink the window.
var MaxWorkerStaleness = 60 * time.Second

// WorkerRegistration is the directory entry for a running worker.
// The directory is observability-only — the engine never reads it.
// Workers register on startup and maintain their entry via periodic
// heartbeat writes (the KV bucket has a 60s TTL).
//
// Identity & heartbeat fields (LastSeen, Pid, Hostname, Version) make
// the existing workers bucket double as a heartbeat surface — avoiding
// a parallel worker_heartbeats bucket (#289). LastSeen is stamped by
// Register on every write, so each periodic heartbeat tick advances
// it automatically. All four fields use omitempty so older payloads
// written before this struct grew (zero-valued) deserialise cleanly.
type WorkerRegistration struct {
	WorkerID  string            `json:"worker_id"`
	TaskTypes []string          `json:"task_types"`
	Language  string            `json:"language"`
	Transport string            `json:"transport"`
	MaxTasks  int               `json:"max_tasks"`
	Metadata  map[string]string `json:"metadata,omitempty"`

	// Identity — populated once at worker boot, stable for the life
	// of the process.
	Pid      int    `json:"pid,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Version  string `json:"version,omitempty"`

	// LastSeen is the wall-clock timestamp of the most recent write
	// to the KV bucket. Register stamps this on every call, so the
	// periodic heartbeat naturally refreshes it. Readers compare it
	// to time.Now() to gauge worker liveness without depending on
	// NATS KV's TTL-eviction latency.
	LastSeen time.Time `json:"last_seen,omitempty"`

	// TokenID identifies the workertoken.Token the worker presented to
	// the HTTP bridge's /v1/workers/connect, if any (#627). Empty for
	// the env-token admin path, dev mode, or non-bridge (native NATS)
	// workers -- it exists purely so an operator can see, from the
	// directory, which minted token a given worker is using.
	TokenID string `json:"token_id,omitempty"`
}

// Directory provides worker visibility via NATS KV.
// Each worker writes its registration to the "workers" bucket;
// the bucket's TTL ensures stale entries are purged automatically.
type Directory struct {
	kv jetstream.KeyValue
}

// NewDirectory creates a Directory backed by the "workers" KV
// bucket. Panics if js is nil or the bucket does not exist — both
// are programmer errors indicating missing setup.
func NewDirectory(js jetstream.JetStream) *Directory {
	if js == nil {
		panic("NewDirectory: js must not be nil")
	}
	kv, err := js.KeyValue(
		context.Background(), "workers",
	)
	if err != nil {
		panic(
			"NewDirectory: workers bucket not found: " +
				err.Error(),
		)
	}
	return &Directory{kv: kv}
}

// registerOwnedTestHook, when non-nil, is called by
// registerOwnedAttempt immediately after its Get and before the
// ownership decision or write -- a test-only injection point that
// lets a test perform a concurrent write inside RegisterOwned's
// Get-to-write window instead of relying on timing. Must never be
// set outside tests.
var registerOwnedTestHook func()

// RegisterOwned is the single, revision-guarded write path for
// worker_id registration used by the bridge for both the initial
// connect and the periodic heartbeat re-register (#650 round 3). A
// prior two-step "check ownership, then plain Put" (and the
// heartbeat's unconditional Put) each raced a concurrent writer
// between the check and the write: two tokens racing an unclaimed id
// could both pass the check and last-writer-wins, and a heartbeat
// replaying its connect-time TokenID could resurrect a worker_id
// after an admin had taken it over. Get -> ownershipAllows -> Create
// (key absent) or Update with the Get's revision (key present)
// closed both races, but a raw revision conflict on its own proves
// only that someone wrote in the window -- not who. A prior version
// mapped any conflict straight to ErrWorkerIDOwned, which wrongly
// rejected a reconnect racing its own heartbeat's re-register (CI-
// only flake: TestConnectWorkerIDOwnershipDevMode's second connect
// racing the first connection's disconnect-time deregister). On
// conflict, registerOwnedAttempt re-Gets and this loop retries the
// decision against the fresh entry, up to ownedWriteRetriesMax times.
func (d *Directory) RegisterOwned(
	reg WorkerRegistration, callerTokenID string, callerIsAdmin bool,
) error {
	if reg.WorkerID == "" {
		panic("Directory.RegisterOwned: WorkerID must not be empty")
	}
	if len(reg.TaskTypes) == 0 {
		panic("Directory.RegisterOwned: TaskTypes must not be empty")
	}
	if d.kv == nil {
		panic("Directory.RegisterOwned: kv must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	for attempt := 0; attempt < ownedWriteRetriesMax; attempt++ {
		sleepOwnedWriteBackoff(ctx, attempt)
		done, err := registerOwnedAttempt(
			ctx, d.kv, reg, callerTokenID, callerIsAdmin,
		)
		if done {
			return err
		}
	}
	return ErrWorkerIDContended
}

// registerOwnedAttempt performs one Get -> decide -> write attempt of
// RegisterOwned's retry loop, split out to keep RegisterOwned under
// the 70-line function limit. done=true means the caller must return
// err as-is (success, or a rejection/failure against fresh state);
// done=false means a create/revision conflict was hit and the caller
// should retry against fresh state.
func registerOwnedAttempt(
	ctx context.Context, kv jetstream.KeyValue,
	reg WorkerRegistration, callerTokenID string, callerIsAdmin bool,
) (done bool, err error) {
	if kv == nil {
		panic("registerOwnedAttempt: kv must not be nil")
	}
	if reg.WorkerID == "" {
		panic("registerOwnedAttempt: reg.WorkerID must not be empty")
	}
	entry, getErr := kv.Get(ctx, reg.WorkerID)
	if getErr != nil && getErr != jetstream.ErrKeyNotFound {
		return true, getErr
	}
	if registerOwnedTestHook != nil {
		registerOwnedTestHook()
	}
	reg.LastSeen = time.Now()
	data, err := json.Marshal(reg)
	if err != nil {
		return true, err
	}
	if getErr == jetstream.ErrKeyNotFound {
		if _, err := kv.Create(ctx, reg.WorkerID, data); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				// Someone else created the key between our Get (not
				// found) and this Create -- retry against fresh state
				// rather than assuming they beat us for ownership
				// reasons.
				return false, nil
			}
			return true, err
		}
		return true, nil
	}
	var existing WorkerRegistration
	if err := json.Unmarshal(entry.Value(), &existing); err != nil {
		// Corrupt/stale entry: it can't prove ownership either way,
		// so treat it as absent for the decision -- but the write
		// below is still guarded by the revision we just read, so a
		// concurrent writer in between still wins the race honestly.
		existing = WorkerRegistration{}
	}
	if !ownershipAllows(existing.TokenID, callerTokenID, callerIsAdmin) {
		return true, ErrWorkerIDOwned
	}
	if _, err := kv.Update(
		ctx, reg.WorkerID, data, entry.Revision(),
	); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Another writer's Create/Update landed between our Get
			// and this Update. That writer might be the very same
			// owner (a heartbeat tick) -- not a takeover -- so retry
			// against fresh state instead of concluding ownership
			// from a bare revision conflict.
			return false, nil
		}
		return true, err
	}
	return true, nil
}

// Register writes the worker's registration to the KV bucket with an
// unguarded Put -- no ownership check, no revision guard. Reserved
// for native Go workers, which never go through the bridge and so
// have no TokenID to enforce (#650's ownership scope is the bridge's
// HTTP connect/heartbeat path only; see RegisterOwned). The worker
// must call Register periodically (before the 60s TTL) to maintain
// its presence. Panics on empty WorkerID or TaskTypes.
func (d *Directory) Register(reg WorkerRegistration) error {
	if reg.WorkerID == "" {
		panic("Directory.Register: WorkerID must not be empty")
	}
	if len(reg.TaskTypes) == 0 {
		panic("Directory.Register: TaskTypes must not be empty")
	}
	if d.kv == nil {
		panic("Directory.Register: kv must not be nil")
	}
	// Stamp LastSeen on every write so each heartbeat tick advances
	// it automatically; callers don't need to refresh the field.
	reg.LastSeen = time.Now()
	data, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	_, err = d.kv.Put(ctx, reg.WorkerID, data)
	return err
}

// Deregister removes the worker's entry from the directory.
// Panics if workerID is empty. Returns nil if the key does not
// exist.
func (d *Directory) Deregister(workerID string) error {
	if workerID == "" {
		panic("Directory.Deregister: workerID must not be empty")
	}
	if d.kv == nil {
		panic("Directory.Deregister: kv must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	err := d.kv.Delete(ctx, workerID)
	if err == jetstream.ErrKeyNotFound {
		return nil
	}
	return err
}

// deregisterOwnedTestHook, when non-nil, is called by
// deregisterOwnedAttempt immediately after its Get and before the
// ownership decision or delete -- the delete-side counterpart to
// registerOwnedTestHook. Must never be set outside tests.
var deregisterOwnedTestHook func()

// DeregisterOwned removes workerID's entry, but only if the caller
// still owns it (#650, the delete-side counterpart to RegisterOwned):
// ownershipAllows must hold for the entry's current token_id against
// the caller. A disconnect from a token that has since been
// superseded (e.g. an admin took the worker_id over while the
// original owner's connection was still open) must not delete the
// current owner's entry out from under it -- it returns
// ErrWorkerIDOwned instead and leaves the entry untouched. Uses the
// Get's revision with jetstream.LastRevision on Delete so a
// concurrent write between the Get and the Delete aborts the delete
// instead of clobbering it, same TOCTOU window RegisterOwned closes
// on the write side -- but a bare revision conflict doesn't prove who
// wrote in the window: it could be this same connection's own
// heartbeat re-registering right as it disconnects, which must not
// skip a legitimate deregister. On conflict, deregisterOwnedAttempt
// re-Gets and this loop retries the decision against the fresh entry,
// up to ownedWriteRetriesMax times. Returns nil if the key does not
// exist.
func (d *Directory) DeregisterOwned(
	workerID, callerTokenID string, callerIsAdmin bool,
) error {
	if workerID == "" {
		panic("Directory.DeregisterOwned: workerID must not be empty")
	}
	if d.kv == nil {
		panic("Directory.DeregisterOwned: kv must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	for attempt := 0; attempt < ownedWriteRetriesMax; attempt++ {
		sleepOwnedWriteBackoff(ctx, attempt)
		done, err := deregisterOwnedAttempt(
			ctx, d.kv, workerID, callerTokenID, callerIsAdmin,
		)
		if done {
			return err
		}
	}
	return ErrWorkerIDContended
}

// deregisterOwnedAttempt performs one Get -> decide -> delete attempt
// of DeregisterOwned's retry loop, split out to keep DeregisterOwned
// under the 70-line function limit. done semantics match
// registerOwnedAttempt.
func deregisterOwnedAttempt(
	ctx context.Context, kv jetstream.KeyValue,
	workerID, callerTokenID string, callerIsAdmin bool,
) (done bool, err error) {
	if kv == nil {
		panic("deregisterOwnedAttempt: kv must not be nil")
	}
	if workerID == "" {
		panic("deregisterOwnedAttempt: workerID must not be empty")
	}
	entry, err := kv.Get(ctx, workerID)
	if err == jetstream.ErrKeyNotFound {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if deregisterOwnedTestHook != nil {
		deregisterOwnedTestHook()
	}
	if !callerIsAdmin {
		var existing WorkerRegistration
		if err := json.Unmarshal(entry.Value(), &existing); err != nil {
			// A corrupt/stale entry can't prove ownership either way
			// -- leave it alone rather than delete data this caller
			// can't be shown to own.
			return true, ErrWorkerIDOwned
		}
		if !ownershipAllows(existing.TokenID, callerTokenID, callerIsAdmin) {
			return true, ErrWorkerIDOwned
		}
	}
	err = kv.Delete(
		ctx, workerID, jetstream.LastRevision(entry.Revision()),
	)
	if err == jetstream.ErrKeyNotFound {
		return true, nil
	}
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Someone re-registered (or re-deregistered) between our
			// Get and this Delete. That writer might be this same
			// connection's own heartbeat -- not a takeover -- so
			// retry against fresh state instead of concluding
			// ownership from a bare revision conflict.
			return false, nil
		}
		return true, err
	}
	return true, nil
}

// List returns all currently registered workers.
// Returns an empty slice when no workers are registered.
// Skips entries that fail to unmarshal (TTL expiry race).
func (d *Directory) List() ([]WorkerRegistration, error) {
	if d.kv == nil {
		panic("Directory.List: kv must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	keys, err := d.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	workers := make([]WorkerRegistration, 0, 32)
	cutoff := time.Now().Add(-MaxWorkerStaleness)
	for key := range keys.Keys() {
		entry, err := d.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		if MaxWorkerStaleness > 0 &&
			entry.Created().Before(cutoff) {
			continue
		}
		var reg WorkerRegistration
		if err := json.Unmarshal(
			entry.Value(), &reg,
		); err != nil {
			continue
		}
		workers = append(workers, reg)
	}
	return workers, nil
}
