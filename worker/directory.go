package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ErrWorkerIDOwned is returned by CheckOwnership when workerID is
// already registered under a different, non-empty token_id and the
// caller is neither that token nor an admin.
var ErrWorkerIDOwned = errors.New(
	"worker_id is registered to another token",
)

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

// CheckOwnership enforces per-token worker_id ownership (#650): once a
// worker_id has been registered under a non-empty token_id, only that
// same token -- or an admin caller -- may re-register it. Entries
// with an empty token_id (written before #627, or by dev-mode/native
// workers) have no owner and are claimable by anyone. Bounded to one
// KV Get; callers make this check before their own Register/Put.
func (d *Directory) CheckOwnership(
	workerID, callerTokenID string, callerIsAdmin bool,
) error {
	if workerID == "" {
		panic("Directory.CheckOwnership: workerID must not be empty")
	}
	if d.kv == nil {
		panic("Directory.CheckOwnership: kv must not be nil")
	}
	if callerIsAdmin {
		return nil
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
	var existing WorkerRegistration
	if err := json.Unmarshal(entry.Value(), &existing); err != nil {
		// Corrupt or stale entry -- treat as unowned rather than
		// block a legitimate registration on undecodable data.
		return nil
	}
	if existing.TokenID != "" && existing.TokenID != callerTokenID {
		return ErrWorkerIDOwned
	}
	return nil
}

// Register writes the worker's registration to the KV bucket.
// The worker must call Register periodically (before the 60s TTL)
// to maintain its presence. Panics on empty WorkerID or TaskTypes.
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
