// internal/engine/admission_controller.go
// AdmissionController consolidates singleton checks, concurrency
// gating, and priority resolution. Extracted from Orchestrator
// to reduce its surface area. No behavioral change.
package engine

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// AdmissionController owns the admission pipeline: singleton
// locks, concurrency limits, and priority resolution.
type AdmissionController struct {
	nc          *nats.Conn
	js          jetstream.JetStream
	store       *SnapshotStore
	concurrency *ConcurrencyManager
	singletonKV jetstream.KeyValue
}

// NewAdmissionController creates an AdmissionController with
// the given dependencies. nc and js are required for publishing
// cancel events. concurrency and singletonKV may be nil.
func NewAdmissionController(
	nc *nats.Conn,
	js jetstream.JetStream,
	store *SnapshotStore,
	concurrency *ConcurrencyManager,
	singletonKV jetstream.KeyValue,
) *AdmissionController {
	if nc == nil {
		panic(
			"NewAdmissionController: nc must not be nil",
		)
	}
	if js == nil {
		panic(
			"NewAdmissionController: js must not be nil",
		)
	}
	return &AdmissionController{
		nc:          nc,
		js:          js,
		store:       store,
		concurrency: concurrency,
		singletonKV: singletonKV,
	}
}

// AcquireRun delegates to ConcurrencyManager if present.
// Returns true (acquired) when concurrency is nil.
func (ac *AdmissionController) AcquireRun(
	ctx context.Context, workflowID string, limit int,
) (bool, error) {
	if ac.concurrency == nil {
		return true, nil
	}
	return ac.concurrency.AcquireRun(ctx, workflowID, limit)
}

// ReleaseRun delegates to ConcurrencyManager if present.
func (ac *AdmissionController) ReleaseRun(
	ctx context.Context, workflowID string,
) error {
	if ac.concurrency == nil {
		return nil
	}
	return ac.concurrency.ReleaseRun(ctx, workflowID)
}

// AcquireTask delegates to ConcurrencyManager if present.
// Returns true (acquired) when concurrency is nil.
func (ac *AdmissionController) AcquireTask(
	ctx context.Context, taskType string, limit int,
) (bool, error) {
	if ac.concurrency == nil {
		return true, nil
	}
	return ac.concurrency.AcquireTask(ctx, taskType, limit)
}

// ReleaseTask delegates to ConcurrencyManager if present.
func (ac *AdmissionController) ReleaseTask(
	ctx context.Context, taskType string,
) error {
	if ac.concurrency == nil {
		return nil
	}
	return ac.concurrency.ReleaseTask(ctx, taskType)
}

// HasConcurrency reports whether a ConcurrencyManager is
// configured. Callers use this to skip concurrency-related
// work entirely when no manager exists.
func (ac *AdmissionController) HasConcurrency() bool {
	return ac.concurrency != nil
}

// ReleaseRunIfConcurrency releases a run slot and returns
// an error if the release fails. No-op without concurrency.
func (ac *AdmissionController) ReleaseRunIfConcurrency(
	ctx context.Context, workflowID string,
) error {
	if ac.concurrency == nil {
		return nil
	}
	if err := ac.concurrency.ReleaseRun(
		ctx, workflowID,
	); err != nil {
		return fmt.Errorf("release run: %w", err)
	}
	return nil
}
