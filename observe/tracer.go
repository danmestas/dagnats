package observe

import "context"

// StatusCode classifies the outcome of a span. Only StatusOK and StatusError
// are valid; callers that pass unknown values will encounter panics in adapters.
type StatusCode int

const (
	StatusOK    StatusCode = iota
	StatusError StatusCode = iota
)

// SpanKind describes the role a span plays in a distributed trace.
// Internal is the safe default; Server/Client distinguish RPC boundaries.
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer   SpanKind = iota
	SpanKindClient   SpanKind = iota
)

// Attribute is a typed key-value pair attached to spans and events.
// Value must be one of: string, int64, float64, bool — adapters may panic
// on other types to surface programmer errors early.
type Attribute struct {
	Key   string
	Value any
}

// StringAttr constructs an Attribute with a string value.
func StringAttr(key, val string) Attribute { return Attribute{Key: key, Value: val} }

// Int64Attr constructs an Attribute with an int64 value.
func Int64Attr(key string, val int64) Attribute { return Attribute{Key: key, Value: val} }

// Float64Attr constructs an Attribute with a float64 value.
func Float64Attr(key string, val float64) Attribute { return Attribute{Key: key, Value: val} }

// BoolAttr constructs an Attribute with a bool value.
func BoolAttr(key string, val bool) Attribute { return Attribute{Key: key, Value: val} }

// SpanOption is a sealed option type for Tracer.Start. The private marker
// method prevents external packages from implementing the interface, keeping
// the option set under our control while remaining extensible within observe.
type SpanOption interface {
	spanOption()
}

// spanKindOption carries a SpanKind to Tracer.Start.
type spanKindOption struct{ kind SpanKind }

func (spanKindOption) spanOption() {}

// Kind returns the SpanKind carried by this option.
func (o spanKindOption) Kind() SpanKind { return o.kind }

// WithSpanKind sets the kind of the span being started.
func WithSpanKind(kind SpanKind) SpanOption { return spanKindOption{kind: kind} }

// attrsOption carries initial Attributes to Tracer.Start.
type attrsOption struct{ attrs []Attribute }

func (attrsOption) spanOption() {}

// Attrs returns the Attributes carried by this option.
func (o attrsOption) Attrs() []Attribute { return o.attrs }

// WithAttributes attaches Attributes to the span at creation time.
func WithAttributes(attrs ...Attribute) SpanOption { return attrsOption{attrs: attrs} }

// Span represents a single unit of work in a distributed trace. End must be
// called exactly once; all other methods are safe to call in any order before End.
type Span interface {
	End()
	SetStatus(code StatusCode, description string)
	SetAttributes(attrs ...Attribute)
	RecordError(err error)
	AddEvent(name string, attrs ...Attribute)
}

// Tracer creates Spans. Implementations must be safe for concurrent use.
// The returned context carries the new span so downstream calls can propagate it.
type Tracer interface {
	Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)
}
