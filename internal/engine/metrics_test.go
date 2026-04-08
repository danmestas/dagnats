// internal/engine/metrics_test.go
// Methodology: unit tests for metric struct constructors.
// Verifies non-nil instruments are returned from a real OTel meter.
package engine

import (
	"testing"

	"go.opentelemetry.io/otel"
)

func TestNewOrchMetricsReturnsNonNil(t *testing.T) {
	m := otel.Meter("test")
	om := newOrchMetrics(m)

	if om.runsActive == nil {
		t.Fatal("runsActive must not be nil")
	}
	if om.runsCompleted == nil {
		t.Fatal("runsCompleted must not be nil")
	}
	if om.runsFailed == nil {
		t.Fatal("runsFailed must not be nil")
	}
	if om.snapshotDuration == nil {
		t.Fatal("snapshotDuration must not be nil")
	}
	if om.failNonRetriable == nil {
		t.Fatal("failNonRetriable must not be nil")
	}
	if om.failRetryAfter == nil {
		t.Fatal("failRetryAfter must not be nil")
	}
}

func TestNewPubMetricsReturnsNonNil(t *testing.T) {
	m := otel.Meter("test")
	pm := newPubMetrics(m)

	if pm.stepEnqueue == nil {
		t.Fatal("stepEnqueue must not be nil")
	}
	if pm.taskConcAcquired == nil {
		t.Fatal("taskConcAcquired must not be nil")
	}
	if pm.taskConcRejected == nil {
		t.Fatal("taskConcRejected must not be nil")
	}
}
