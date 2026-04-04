package worker

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

// WorkerRegistration is the directory entry for a running worker.
// The directory is observability-only — the engine never reads it.
// Workers register on startup and maintain their entry via periodic
// heartbeat writes (the KV bucket has a 60s TTL).
type WorkerRegistration struct {
	WorkerID  string            `json:"worker_id"`
	TaskTypes []string          `json:"task_types"`
	Language  string            `json:"language"`
	Transport string            `json:"transport"`
	MaxTasks  int               `json:"max_tasks"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Directory provides worker visibility via NATS KV.
// Each worker writes its registration to the "workers" bucket;
// the bucket's TTL ensures stale entries are purged automatically.
type Directory struct {
	kv nats.KeyValue
}

// NewDirectory creates a Directory backed by the "workers" KV bucket.
// Panics if js is nil or the bucket does not exist — both are
// programmer errors indicating missing setup.
func NewDirectory(js nats.JetStreamContext) *Directory {
	if js == nil {
		panic("NewDirectory: js must not be nil")
	}
	kv, err := js.KeyValue("workers")
	if err != nil {
		panic("NewDirectory: workers bucket not found: " + err.Error())
	}
	if kv == nil {
		panic("NewDirectory: kv must not be nil")
	}
	return &Directory{kv: kv}
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
	data, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	_, err = d.kv.Put(reg.WorkerID, data)
	return err
}

// Deregister removes the worker's entry from the directory.
// Panics if workerID is empty. Returns nil if the key does not exist.
func (d *Directory) Deregister(workerID string) error {
	if workerID == "" {
		panic("Directory.Deregister: workerID must not be empty")
	}
	if d.kv == nil {
		panic("Directory.Deregister: kv must not be nil")
	}
	err := d.kv.Delete(workerID)
	if err == nats.ErrKeyNotFound {
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
	keys, err := d.kv.Keys()
	if err == nats.ErrNoKeysFound {
		return []WorkerRegistration{}, nil
	}
	if err != nil {
		return nil, err
	}
	if keys == nil {
		panic("Directory.List: keys must not be nil when err is nil")
	}
	workers := make([]WorkerRegistration, 0, len(keys))
	for _, key := range keys {
		entry, err := d.kv.Get(key)
		if err != nil {
			continue
		}
		var reg WorkerRegistration
		if err := json.Unmarshal(entry.Value(), &reg); err != nil {
			continue
		}
		workers = append(workers, reg)
	}
	return workers, nil
}
