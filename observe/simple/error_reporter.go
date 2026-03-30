// observe/simple/error_reporter.go
// ErrorReporter that records errors on the active LiveSpan when present,
// falling back to the Logger when no span is in context.
package simple

import (
	"context"

	"github.com/danmestas/dagnats/observe"
)

type errorReporter struct {
	tracer observe.Tracer
	logger observe.Logger
}

// NewErrorReporter returns an observe.ErrorReporter that delegates to the
// active span when one exists in ctx, and falls back to logger otherwise.
func NewErrorReporter(
	tracer observe.Tracer,
	logger observe.Logger,
) observe.ErrorReporter {
	if tracer == nil {
		panic("NewErrorReporter: tracer must not be nil")
	}
	if logger == nil {
		panic("NewErrorReporter: logger must not be nil")
	}
	return &errorReporter{tracer: tracer, logger: logger}
}

// CaptureError records err on the active span when present; otherwise logs
// it together with any provided tags as structured fields.
func (r *errorReporter) CaptureError(
	ctx context.Context,
	err error,
	tags map[string]string,
) {
	if err == nil {
		return
	}
	span := SpanFromContext(ctx)
	if span != nil {
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
		return
	}
	// Fallback: log the error with tags as structured fields.
	fields := make([]observe.Field, 0, len(tags))
	for k, v := range tags {
		fields = append(fields, observe.String(k, v))
	}
	r.logger.Error(err.Error(), err, fields...)
}

// CaptureMessage attaches msg as a span event when a span is active;
// otherwise logs it at the appropriate level.
func (r *errorReporter) CaptureMessage(
	ctx context.Context,
	msg string,
	level observe.Level,
) {
	span := SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(msg)
		return
	}
	if level >= observe.LevelError {
		r.logger.Error(msg, nil)
	} else {
		r.logger.Info(msg)
	}
}
