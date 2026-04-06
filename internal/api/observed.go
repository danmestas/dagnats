// api/observed.go
// Centralizes tracing and metrics instrumentation so every service
// method shares the same start-span / record-count / record-duration
// / record-error sequence without copy-pasting 10 lines each time.
package api

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// observed wraps a service operation with tracing and metrics.
// The fn closure receives a context carrying the active span so
// downstream calls participate in the same trace. Attributes
// are attached to the span; the method name tags every metric.
func (s *Service) observed(
	ctx context.Context,
	name string,
	attrs []attribute.KeyValue,
	fn func(context.Context) error,
) error {
	if ctx == nil {
		panic("observed: ctx must not be nil")
	}
	if name == "" {
		panic("observed: name must not be empty")
	}
	ctx, span := s.tracer.Start(
		ctx,
		"dagnats.api "+name,
		trace.WithAttributes(attrs...),
	)
	defer span.End()

	start := time.Now()
	methodAttr := metric.WithAttributes(
		attribute.String("method", name),
	)
	s.requestCount.Add(ctx, 1, methodAttr)

	err := fn(ctx)

	elapsed := float64(
		time.Since(start).Milliseconds(),
	)
	s.requestDuration.Record(ctx, elapsed, methodAttr)
	if err != nil {
		s.errorCount.Add(ctx, 1, methodAttr)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
