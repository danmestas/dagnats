package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// ConcurrencyManager enforces run and step concurrency limits using
// NATS KV counters with optimistic locking. Thread-safe.
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

// AcquireRun increments the counter for the workflow. Returns false
// if the limit is reached. Limit 0 means unlimited.
func (cm *ConcurrencyManager) AcquireRun(
	ctx context.Context, workflowID string, limit int,
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
		current, rev, err := cm.readCounter(ctx, key)
		if err != nil {
			return false, err
		}
		if current >= limit {
			return false, nil
		}
		if cm.casIncrement(ctx, key, current, rev) {
			return true, nil
		}
		// CAS failed — retry
	}
	return false, fmt.Errorf("acquire: too many CAS retries")
}

// ReleaseRun decrements the counter for the workflow.
func (cm *ConcurrencyManager) ReleaseRun(
	ctx context.Context, workflowID string,
) error {
	if workflowID == "" {
		panic("ReleaseRun: workflowID must not be empty")
	}
	key := "workflow." + workflowID

	for attempt := 0; attempt < 10; attempt++ {
		current, rev, err := cm.readCounter(ctx, key)
		if err != nil {
			return err
		}
		if current <= 0 {
			return nil // Already at zero
		}
		newVal := current - 1
		data := []byte(strconv.Itoa(newVal))
		if rev == 0 {
			_, err = cm.runKV.Create(ctx, key, data)
		} else {
			_, err = cm.runKV.Update(ctx, key, data, rev)
		}
		if err == nil {
			return nil
		}
		// CAS failed — retry
	}
	return fmt.Errorf("release: too many CAS retries")
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
	for attempt := 0; attempt < 10; attempt++ {
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
	for attempt := 0; attempt < 10; attempt++ {
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

func (cm *ConcurrencyManager) readCounter(
	ctx context.Context, key string,
) (int, uint64, error) {
	entry, err := cm.runKV.Get(ctx, key)
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

func (cm *ConcurrencyManager) casIncrement(
	ctx context.Context, key string, current int, rev uint64,
) bool {
	newVal := current + 1
	data := []byte(strconv.Itoa(newVal))
	var err error
	if rev == 0 {
		_, err = cm.runKV.Create(ctx, key, data)
	} else {
		_, err = cm.runKV.Update(ctx, key, data, rev)
	}
	return err == nil
}
