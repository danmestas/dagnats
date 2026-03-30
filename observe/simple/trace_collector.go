// observe/simple/trace_collector.go
// TraceCollector implements observe.Tracer backed by NATS JetStream.
// Completed spans are sent to a buffered channel and published asynchronously
// to "telemetry.spans.{service}.{run_id}" with Nats-Msg-Id deduplication.
package simple

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

const recordsChanCapacity = 1024

// spanContextKey is the private context key for storing the active LiveSpan.
type spanContextKey struct{}

// SpanFromContext returns the active LiveSpan from the context, or nil
// if no span is present. Exported for ErrorReporter integration.
func SpanFromContext(ctx context.Context) *LiveSpan {
	if ctx == nil {
		panic("SpanFromContext: ctx must not be nil")
	}
	span, _ := ctx.Value(spanContextKey{}).(*LiveSpan)
	return span
}

// TraceCollector implements observe.Tracer, publishing completed spans to NATS.
type TraceCollector struct {
	js          nats.JetStreamContext
	metrics     observe.Metrics
	serviceName string
	records     chan SpanRecord
	done        chan struct{}
	once        sync.Once
}

// NewTraceCollector constructs a TraceCollector and starts the background
// publisher goroutine. Call Flush() to drain and stop the publisher.
func NewTraceCollector(
	js nats.JetStreamContext,
	serviceName string,
	metrics observe.Metrics,
) *TraceCollector {
	if js == nil {
		panic("NewTraceCollector: js must not be nil")
	}
	if serviceName == "" {
		panic("NewTraceCollector: serviceName must not be empty")
	}
	if metrics == nil {
		panic("NewTraceCollector: metrics must not be nil")
	}
	tc := &TraceCollector{
		js:          js,
		metrics:     metrics,
		serviceName: serviceName,
		records:     make(chan SpanRecord, recordsChanCapacity),
		done:        make(chan struct{}),
	}
	go tc.publishLoop()
	return tc
}

// publishLoop reads SpanRecords from the channel and publishes them to NATS.
// Exits when the records channel is closed, then signals done.
func (tc *TraceCollector) publishLoop() {
	defer close(tc.done)
	for rec := range tc.records {
		publishSpanRecord(tc.js, rec)
	}
}

// publishSpanRecord serializes a SpanRecord and publishes it to NATS with
// Nats-Msg-Id for deduplication. Errors are logged but never returned.
func publishSpanRecord(js nats.JetStreamContext, rec SpanRecord) {
	if js == nil {
		panic("publishSpanRecord: js must not be nil")
	}
	data, err := json.Marshal(rec)
	if err != nil {
		log.Printf("publishSpanRecord: marshal error name=%s: %v",
			rec.Name, err)
		return
	}
	runID := extractRunID(rec.Attributes)
	subject := "telemetry.spans." + rec.Service + "." + runID
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", rec.TraceID+"."+rec.SpanID)
	if _, err := js.PublishMsg(msg); err != nil {
		log.Printf("publishSpanRecord: publish error subject=%s: %v",
			subject, err)
	}
}

// extractRunID pulls "run_id" from the attributes map, defaulting to "no-run".
func extractRunID(attrs map[string]any) string {
	if attrs == nil {
		return "no-run"
	}
	if v, ok := attrs["run_id"]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return "no-run"
}

// Flush closes the records channel and waits for the background goroutine
// to drain. Times out after 5 seconds to avoid hanging indefinitely.
func (tc *TraceCollector) Flush() {
	tc.once.Do(func() { close(tc.records) })
	select {
	case <-tc.done:
	case <-time.After(5 * time.Second):
		log.Printf("TraceCollector.Flush: timed out after 5s")
	}
}

// Start creates a new LiveSpan, stores it in the context, and returns the
// enriched context and span. If a parent span exists in the context, the
// child inherits its traceID and sets parentID.
func (tc *TraceCollector) Start(
	ctx context.Context,
	name string,
	opts ...observe.SpanOption,
) (context.Context, observe.Span) {
	if ctx == nil {
		panic("TraceCollector.Start: ctx must not be nil")
	}
	if name == "" {
		panic("TraceCollector.Start: name must not be empty")
	}
	span := newLiveSpan(ctx, name, tc.serviceName,
		tc.records, tc.metrics, opts)
	childCtx := context.WithValue(ctx, spanContextKey{}, span)
	return childCtx, span
}

// spanKindString converts an observe.SpanKind to its wire-format string.
func spanKindString(k observe.SpanKind) string {
	switch k {
	case observe.SpanKindServer:
		return "server"
	case observe.SpanKindClient:
		return "client"
	default:
		return "internal"
	}
}

// statusString converts an observe.StatusCode to its wire-format string.
func statusString(code observe.StatusCode) string {
	if code == observe.StatusError {
		return "error"
	}
	return "ok"
}

// LiveSpan implements observe.Span. It accumulates data in memory and
// publishes a SpanRecord to the shared records channel on End().
// All mutable fields are guarded by mu for concurrent safety.
type LiveSpan struct {
	mu         sync.Mutex
	traceID    string
	spanID     string
	parentID   string
	name       string
	service    string
	kind       string
	startTime  time.Time
	attributes map[string]any
	events     []SpanEvent
	statusCode observe.StatusCode
	statusDesc string
	errorMsg   string
	records    chan SpanRecord
	metrics    observe.Metrics
	ended      bool
}

// TraceID returns the span's trace ID (needed for context propagation).
func (s *LiveSpan) TraceID() string { return s.traceID }

// SpanID returns the span's ID (needed for parent linking).
func (s *LiveSpan) SpanID() string { return s.spanID }

// newLiveSpan constructs a LiveSpan, inheriting trace context from the parent
// span in ctx if present.
func newLiveSpan(
	ctx context.Context,
	name string,
	service string,
	records chan SpanRecord,
	metrics observe.Metrics,
	opts []observe.SpanOption,
) *LiveSpan {
	if ctx == nil {
		panic("newLiveSpan: ctx must not be nil")
	}
	if records == nil {
		panic("newLiveSpan: records must not be nil")
	}
	traceID := generateTraceID()
	parentID := ""
	if parent := SpanFromContext(ctx); parent != nil {
		traceID = parent.traceID
		parentID = parent.spanID
	} else if info, ok := ParentInfoFromContext(ctx); ok {
		traceID = info.TraceID
		parentID = info.SpanID
	}
	kind := observe.SpanKindInternal
	attrs := map[string]any{}
	for _, opt := range opts {
		applySpanOption(opt, &kind, attrs)
	}
	return &LiveSpan{
		traceID:    traceID,
		spanID:     generateSpanID(),
		parentID:   parentID,
		name:       name,
		service:    service,
		kind:       spanKindString(kind),
		startTime:  time.Now().UTC(),
		attributes: attrs,
		records:    records,
		metrics:    metrics,
	}
}

// applySpanOption extracts kind and attributes from a SpanOption.
// Uses type assertion since SpanOption is a sealed interface.
func applySpanOption(
	opt observe.SpanOption,
	kind *observe.SpanKind,
	attrs map[string]any,
) {
	if opt == nil {
		panic("applySpanOption: opt must not be nil")
	}
	if kind == nil {
		panic("applySpanOption: kind must not be nil")
	}
	switch o := opt.(type) {
	case interface{ Kind() observe.SpanKind }:
		*kind = o.Kind()
	case interface{ Attrs() []observe.Attribute }:
		for _, a := range o.Attrs() {
			attrs[a.Key] = a.Value
		}
	}
}

// End finalizes the span and sends a SpanRecord to the records
// channel. Double-calls are silently ignored. If the channel is
// full, the span is dropped and a counter is incremented.
func (s *LiveSpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	rec := s.buildRecord()
	select {
	case s.records <- rec:
	default:
		if s.metrics != nil {
			s.metrics.Counter(
				"telemetry.spans.dropped", nil,
			).Inc()
		}
		log.Printf(
			"LiveSpan.End: channel full, dropping %s",
			s.name)
	}
}

// buildRecord constructs a SpanRecord from the current span state.
// Caller must hold s.mu.
func (s *LiveSpan) buildRecord() SpanRecord {
	endTime := time.Now().UTC()
	return SpanRecord{
		TraceID:    s.traceID,
		SpanID:     s.spanID,
		ParentID:   s.parentID,
		Name:       s.name,
		Service:    s.service,
		Kind:       s.kind,
		StartTime:  s.startTime,
		EndTime:    endTime,
		DurationMS: endTime.Sub(s.startTime).Milliseconds(),
		Status:     statusString(s.statusCode),
		Attributes: s.attributes,
		Events:     s.events,
		Error:      s.errorMsg,
	}
}

// SetStatus sets the status code and description of the span.
func (s *LiveSpan) SetStatus(
	code observe.StatusCode, description string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = code
	s.statusDesc = description
}

// SetAttributes merges key-value pairs into the span's attributes.
func (s *LiveSpan) SetAttributes(attrs ...observe.Attribute) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range attrs {
		s.attributes[a.Key] = a.Value
	}
}

// RecordError stores the error message and sets error status.
func (s *LiveSpan) RecordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errorMsg = err.Error()
	s.statusCode = observe.StatusError
}

// AddEvent appends a timestamped event to the span.
func (s *LiveSpan) AddEvent(
	name string, attrs ...observe.Attribute,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	eventAttrs := make(map[string]any, len(attrs))
	for _, a := range attrs {
		eventAttrs[a.Key] = a.Value
	}
	s.events = append(s.events, SpanEvent{
		Name:       name,
		Time:       time.Now().UTC(),
		Attributes: eventAttrs,
	})
}
