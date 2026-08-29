package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

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
// has no identity to enforce) may re-register or delete it.
const AdminTokenID = "admin"

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
// closes both races: a losing/late writer's Create/Update fails on a
// duplicate-key or revision conflict and gets ErrWorkerIDOwned, same
// as a synchronous ownership rejection. Bounded to one Get plus one
// Create/Update.
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
	entry, getErr := d.kv.Get(ctx, reg.WorkerID)
	if getErr != nil && getErr != jetstream.ErrKeyNotFound {
		return getErr
	}
	reg.LastSeen = time.Now()
	data, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	if getErr == jetstream.ErrKeyNotFound {
		return createOwned(ctx, d.kv, reg.WorkerID, data)
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
		return ErrWorkerIDOwned
	}
	if _, err := d.kv.Update(
		ctx, reg.WorkerID, data, entry.Revision(),
	); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Another writer's Create/Update landed between our Get
			// and this Update (a takeover, or a losing racer in a
			// concurrent claim) -- we lost the race and must not
			// clobber the value that won it.
			return ErrWorkerIDOwned
		}
		return err
	}
	return nil
}

// createOwned performs the Create half of RegisterOwned's Get ->
// Create-or-Update decision, split out to keep RegisterOwned under
// the 70-line function limit.
func createOwned(
	ctx context.Context, kv jetstream.KeyValue, workerID string, data []byte,
) error {
	if kv == nil {
		panic("createOwned: kv must not be nil")
	}
	if workerID == "" {
		panic("createOwned: workerID must not be empty")
	}
	if _, err := kv.Create(ctx, workerID, data); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Someone else created the key between our Get (not
			// found) and this Create -- they won the race for a
			// fresh id.
			return ErrWorkerIDOwned
		}
		return err
	}
	return nil
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

// DeregisterOwned removes workerID's entry, but only if the caller
// still owns it (#650, the delete-side counterpart to RegisterOwned):
// ownershipAllows must hold for the entry's current token_id against
// the caller. A disconnect from a token that has since been
// superseded (e.g. an admin took the worker_id over while the
// original owner's connection was still open) must not delete the
// current owner's entry out from under it -- it returns
// ErrWorkerIDOwned instead and leaves the entry untouched. Uses the
// Get's revision with jetstream.LastRevision on Delete so a
// concurrent re-register between the Get and the Delete aborts the
// delete instead of clobbering the new owner (closes the same TOCTOU
// window RegisterOwned closes on the write side). Returns nil if the
// key does not exist.
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
	entry, err := d.kv.Get(ctx, workerID)
	if err == jetstream.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if !callerIsAdmin {
		var existing WorkerRegistration
		if err := json.Unmarshal(entry.Value(), &existing); err != nil {
			// A corrupt/stale entry can't prove ownership either way
			// -- leave it alone rather than delete data this caller
			// can't be shown to own.
			return ErrWorkerIDOwned
		}
		if !ownershipAllows(existing.TokenID, callerTokenID, callerIsAdmin) {
			return ErrWorkerIDOwned
		}
	}
	err = d.kv.Delete(
		ctx, workerID, jetstream.LastRevision(entry.Revision()),
	)
	if err == jetstream.ErrKeyNotFound {
		return nil
	}
	return err
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
