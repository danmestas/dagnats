// internal/engine/metrics.go
// Metric instrument bundles for Orchestrator and TaskPublisher.
// Centralizes OTel meter creation to reduce constructor noise.
package engine

import "go.opentelemetry.io/otel/metric"

// orchMetrics bundles the Orchestrator's pre-allocated metric
// instruments. Created once in NewOrchestrator.
type orchMetrics struct {
	runsActive       metric.Int64UpDownCounter
	runsCompleted    metric.Int64Counter
	runsFailed       metric.Int64Counter
	runsReconciled   metric.Int64Counter
	snapshotDuration metric.Float64Histogram
	failNonRetriable metric.Int64Counter
	failRetryAfter   metric.Int64Counter
}

// newOrchMetrics creates all orchestrator metric instruments.
// Panics if meter is nil.
func newOrchMetrics(m metric.Meter) orchMetrics {
	if m == nil {
		panic("newOrchMetrics: meter must not be nil")
	}
	runsActive, _ := m.Int64UpDownCounter(
		"workflow.runs.active",
	)
	runsCompleted, _ := m.Int64Counter(
		"workflow.runs.completed",
	)
	runsFailed, _ := m.Int64Counter(
		"workflow.runs.failed",
	)
	runsReconciled, _ := m.Int64Counter(
		"workflow.runs.reconciled",
	)
	snapshotDuration, _ := m.Float64Histogram(
		"snapshot.save.duration_ms",
	)
	failNonRetriable, _ := m.Int64Counter(
		"step.failure.non_retriable",
	)
	failRetryAfter, _ := m.Int64Counter(
		"step.failure.retry_after",
	)
	return orchMetrics{
		runsActive:       runsActive,
		runsCompleted:    runsCompleted,
		runsFailed:       runsFailed,
		runsReconciled:   runsReconciled,
		snapshotDuration: snapshotDuration,
		failNonRetriable: failNonRetriable,
		failRetryAfter:   failRetryAfter,
	}
}

// pubMetrics bundles the TaskPublisher's metric instruments.
type pubMetrics struct {
	stepEnqueue      metric.Int64Counter
	taskConcAcquired metric.Int64Counter
	taskConcRejected metric.Int64Counter
}

// newPubMetrics creates all publisher metric instruments.
func newPubMetrics(m metric.Meter) pubMetrics {
	if m == nil {
		panic("newPubMetrics: meter must not be nil")
	}
	stepEnqueue, _ := m.Int64Counter(
		"step.enqueue.count",
	)
	taskConcAcquired, _ := m.Int64Counter(
		"task.concurrency.acquired",
	)
	taskConcRejected, _ := m.Int64Counter(
		"task.concurrency.rejected",
	)
	return pubMetrics{
		stepEnqueue:      stepEnqueue,
		taskConcAcquired: taskConcAcquired,
		taskConcRejected: taskConcRejected,
	}
}
