// api/service_workers.go
// Split out of service.go (#566): worker discovery domain of the control
// plane Service. Shares the private Service NATS/KV bundle; no new
// connection layer. Behavior identical to the pre-split file.
package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// ListWorkers returns all currently registered workers from the
// directory. Returns an empty slice when no workers are registered
// or when the workers KV bucket does not exist.
func (s *Service) ListWorkers(
	ctx context.Context,
) ([]worker.WorkerRegistration, error) {
	if ctx == nil {
		panic("ListWorkers: ctx must not be nil")
	}
	if s.js == nil {
		panic("ListWorkers: js must not be nil")
	}
	var workers []worker.WorkerRegistration
	err := s.observed(ctx, "listWorkers", nil,
		func(ctx context.Context) error {
			var innerErr error
			workers, innerErr = s.listWorkersInner(ctx)
			return innerErr
		},
	)
	return workers, err
}

// listWorkersInner attempts to list workers from the directory.
// Returns empty slice when the workers bucket does not exist --
// normal condition when no workers have registered yet.
func (s *Service) listWorkersInner(
	ctx context.Context,
) ([]worker.WorkerRegistration, error) {
	if s.js == nil {
		panic("listWorkersInner: js must not be nil")
	}
	kv, err := s.js.KeyValue(ctx, "workers")
	if err != nil {
		return []worker.WorkerRegistration{}, nil
	}
	if kv == nil {
		panic(
			"listWorkersInner: kv must not be nil when err is nil",
		)
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return []worker.WorkerRegistration{}, nil
	}
	if keys == nil {
		panic(
			"listWorkersInner: keys must not be nil when err is nil",
		)
	}
	workers := make(
		[]worker.WorkerRegistration, 0, len(keys),
	)
	cutoff := time.Now().Add(-worker.MaxWorkerStaleness)
	for _, key := range keys {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		if worker.MaxWorkerStaleness > 0 &&
			entry.Created().Before(cutoff) {
			continue
		}
		var reg worker.WorkerRegistration
		if err := json.Unmarshal(
			entry.Value(), &reg,
		); err != nil {
			continue
		}
		workers = append(workers, reg)
	}
	return workers, nil
}
