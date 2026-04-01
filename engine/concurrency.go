package engine

import (
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

// ConcurrencyManager enforces run and step concurrency limits using
// NATS KV counters with optimistic locking. Thread-safe.
type ConcurrencyManager struct {
	runKV nats.KeyValue
}

// NewConcurrencyManager creates a manager using the concurrency_runs
// KV bucket. Panics if the bucket doesn't exist.
func NewConcurrencyManager(
	js nats.JetStreamContext,
) *ConcurrencyManager {
	if js == nil {
		panic("NewConcurrencyManager: js must not be nil")
	}
	kv, err := js.KeyValue("concurrency_runs")
	if err != nil {
		panic("NewConcurrencyManager: concurrency_runs: " +
			err.Error())
	}
	return &ConcurrencyManager{runKV: kv}
}

// NewConcurrencyManagerSafe creates a manager using the
// concurrency_runs KV bucket. Returns nil if bucket doesn't exist.
func NewConcurrencyManagerSafe(
	js nats.JetStreamContext,
) (*ConcurrencyManager, error) {
	if js == nil {
		panic("NewConcurrencyManagerSafe: js must not be nil")
	}
	kv, err := js.KeyValue("concurrency_runs")
	if err != nil {
		return nil, err
	}
	return &ConcurrencyManager{runKV: kv}, nil
}

// AcquireRun increments the counter for the workflow. Returns false
// if the limit is reached. Limit 0 means unlimited.
func (cm *ConcurrencyManager) AcquireRun(
	workflowID string, limit int,
) (bool, error) {
	if workflowID == "" {
		panic("AcquireRun: workflowID must not be empty")
	}
	if limit <= 0 {
		return true, nil // Unlimited
	}

	key := "workflow." + workflowID

	// Retry loop for optimistic locking (bounded)
	for attempt := 0; attempt < 10; attempt++ {
		current, rev, err := cm.readCounter(key)
		if err != nil {
			return false, err
		}
		if current >= limit {
			return false, nil
		}
		if cm.casIncrement(key, current, rev) {
			return true, nil
		}
		// CAS failed — retry
	}
	return false, fmt.Errorf("acquire: too many CAS retries")
}

// ReleaseRun decrements the counter for the workflow.
func (cm *ConcurrencyManager) ReleaseRun(
	workflowID string,
) error {
	if workflowID == "" {
		panic("ReleaseRun: workflowID must not be empty")
	}
	key := "workflow." + workflowID

	for attempt := 0; attempt < 10; attempt++ {
		current, rev, err := cm.readCounter(key)
		if err != nil {
			return err
		}
		if current <= 0 {
			return nil // Already at zero
		}
		newVal := current - 1
		data := []byte(strconv.Itoa(newVal))
		if rev == 0 {
			_, err = cm.runKV.Create(key, data)
		} else {
			_, err = cm.runKV.Update(key, data, rev)
		}
		if err == nil {
			return nil
		}
		// CAS failed — retry
	}
	return fmt.Errorf("release: too many CAS retries")
}

func (cm *ConcurrencyManager) readCounter(
	key string,
) (int, uint64, error) {
	entry, err := cm.runKV.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
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

func (cm *ConcurrencyManager) casIncrement(
	key string, current int, rev uint64,
) bool {
	newVal := current + 1
	data := []byte(strconv.Itoa(newVal))
	var err error
	if rev == 0 {
		_, err = cm.runKV.Create(key, data)
	} else {
		_, err = cm.runKV.Update(key, data, rev)
	}
	return err == nil
}
