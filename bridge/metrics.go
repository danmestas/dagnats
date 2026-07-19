package bridge

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

// ackMapSizeMetricName is the in-flight dispatch gauge. Named as a
// constant so the test asserts against the same symbol the callback
// registers: a literal on each side would let a rename pass the test
// while silently renaming the series operators alert on.
const ackMapSizeMetricName = "bridge.ackmap.size"

// RegisterBridgeMetrics wires the bridge's observable instruments to m
// and returns the registration so a caller can unregister it.
//
// The ackmap size is an observable gauge rather than an up/down counter
// deliberately. A counter requires every mutation site — store, resolve,
// reap, cap eviction — to Add the right delta forever, and drifts
// permanently the moment one is missed. This instrument was previously
// an Int64UpDownCounter with no Add call anywhere, so it reported a
// constant zero: indistinguishable from an idle bridge, and wrong the
// entire time it existed. A callback reading AckMap.Count() cannot
// drift, because it reports the real value at every collection.
//
// Mirrors RegisterSchedulerMetrics (internal/trigger/metrics.go): a
// standalone registration function rather than construction inside
// NewBridge, so the error is returned to a caller that can assert on it
// instead of being discarded at startup.
func RegisterBridgeMetrics(
	m metric.Meter, b *Bridge,
) (metric.Registration, error) {
	if m == nil {
		return nil, fmt.Errorf("RegisterBridgeMetrics: meter is nil")
	}
	if b == nil {
		return nil, fmt.Errorf("RegisterBridgeMetrics: bridge is nil")
	}
	if b.ackMap == nil {
		return nil, fmt.Errorf("RegisterBridgeMetrics: ackMap is nil")
	}
	ackMapSize, err := m.Int64ObservableGauge(
		ackMapSizeMetricName,
		metric.WithDescription(
			"Tasks dispatched to HTTP bridge workers and awaiting "+
				"resolve. Falls when a worker resolves, and when the "+
				"AckMap reaps an entry whose delivery outlived AckWait.",
		),
	)
	if err != nil {
		return nil, fmt.Errorf("ackmap size gauge: %w", err)
	}
	return m.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(ackMapSize, b.ackMap.Count())
			return nil
		},
		ackMapSize,
	)
}
