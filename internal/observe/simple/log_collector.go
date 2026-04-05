// observe/simple/log_collector.go
// LogCollector implements observe.Logger backed by NATS JetStream.
// Each log call serializes a LogRecord to JSON and publishes it to
// "telemetry.logs.{service}.{level}" on the TELEMETRY stream so that any
// downstream consumer can aggregate or forward without coupling to a specific
// logging backend.
package simple

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go/jetstream"
)

// LogCollector publishes LogRecord events to the NATS TELEMETRY
// stream. Safe for concurrent use -- each publish is independent.
// With returns a new LogCollector that inherits all parent fields;
// the parent's field slice is never mutated after construction.
type LogCollector struct {
	js          jetstream.JetStream
	serviceName string
	fields      []observe.Field
}

// NewLogCollector constructs a LogCollector.
// Panics on nil js or empty serviceName -- programmer errors.
func NewLogCollector(
	js jetstream.JetStream, serviceName string,
) *LogCollector {
	if js == nil {
		panic("NewLogCollector: js must not be nil")
	}
	if serviceName == "" {
		panic("NewLogCollector: serviceName must not be empty")
	}
	return &LogCollector{js: js, serviceName: serviceName}
}

// Info publishes a LogRecord at level "info" to NATS.
func (lc *LogCollector) Info(msg string, fields ...observe.Field) {
	if msg == "" {
		panic("LogCollector.Info: msg must not be empty")
	}
	if lc.js == nil {
		panic("LogCollector.Info: js must not be nil")
	}
	lc.publish("info", msg, nil, fields)
}

// Error publishes a LogRecord at level "error" to NATS.
// If err is non-nil its message is captured in the Error field.
func (lc *LogCollector) Error(msg string, err error, fields ...observe.Field) {
	if msg == "" {
		panic("LogCollector.Error: msg must not be empty")
	}
	if lc.js == nil {
		panic("LogCollector.Error: js must not be nil")
	}
	lc.publish("error", msg, err, fields)
}

// With returns a new LogCollector with the given fields appended to the
// parent's fields. The parent slice is copied to prevent mutation.
func (lc *LogCollector) With(fields ...observe.Field) observe.Logger {
	if len(fields) == 0 {
		return lc
	}
	merged := make([]observe.Field, len(lc.fields)+len(fields))
	copy(merged, lc.fields)
	copy(merged[len(lc.fields):], fields)
	return &LogCollector{js: lc.js, serviceName: lc.serviceName, fields: merged}
}

// publish builds a LogRecord from the given parameters and publishes it to
// "telemetry.logs.{service}.{level}". Errors are logged but never propagated —
// telemetry is best-effort and must not crash the caller.
func (lc *LogCollector) publish(level, msg string, err error, callFields []observe.Field) {
	if level == "" {
		panic("LogCollector.publish: level must not be empty")
	}
	if msg == "" {
		panic("LogCollector.publish: msg must not be empty")
	}
	rec := LogRecord{
		Level:     level,
		Message:   msg,
		Service:   lc.serviceName,
		Timestamp: time.Now().UTC(),
		Fields:    mergeFields(lc.fields, callFields),
	}
	if err != nil {
		rec.Error = err.Error()
	}
	data, marshalErr := json.Marshal(rec)
	if marshalErr != nil {
		log.Printf("LogCollector.publish: marshal error level=%s: %v", level, marshalErr)
		return
	}
	subject := "telemetry.logs." + lc.serviceName + "." + level
	_, pubErr := lc.js.Publish(
		context.Background(), subject, data,
	)
	if pubErr != nil {
		log.Printf(
			"LogCollector.publish: publish error subject=%s: %v",
			subject, pubErr,
		)
	}
}

// mergeFields combines base fields with call-site fields into a single map.
// Returns nil when both slices are empty to keep the JSON compact.
func mergeFields(base, extra []observe.Field) map[string]any {
	total := len(base) + len(extra)
	if total == 0 {
		return nil
	}
	out := make(map[string]any, total)
	for _, f := range base {
		out[f.Key] = f.Value
	}
	for _, f := range extra {
		out[f.Key] = f.Value
	}
	return out
}
