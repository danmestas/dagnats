package otlp

import (
	"github.com/danmestas/dagnats/internal/observe/simple"
)

// Value type discriminator for AnyValue encoding.
const (
	valueString = iota
	valueBool
	valueInt
	valueDouble
)

// keyValue is the internal representation of an OTLP KeyValue pair.
type keyValue struct {
	key   string
	value anyValue
}

// anyValue is the internal representation of an OTLP AnyValue.
type anyValue struct {
	kind      int
	str       string
	boolVal   bool
	intVal    int64
	doubleVal float64
}

// spanEvent is the internal representation of an OTLP Span.Event.
type spanEvent struct {
	name         string
	timeUnixNano int64
	attributes   []keyValue
}

// hexToBytes converts a hex string to bytes. Returns nil for empty
// or odd-length strings. Invalid hex chars decode as zero.
func hexToBytes(hex string) []byte {
	if len(hex) > 64 {
		panic("hexToBytes: hex string too long")
	}
	if hex == "" {
		return nil
	}
	if len(hex)%2 != 0 {
		return nil
	}

	out := make([]byte, len(hex)/2)
	for i := 0; i < len(out); i++ {
		out[i] = hexNibble(hex[i*2])<<4 |
			hexNibble(hex[i*2+1])
	}
	return out
}

// hexNibble converts a single hex character to its 4-bit value.
// Returns 0 for invalid characters.
func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 0
	}
}

// convertKind maps span kind strings to OTLP SpanKind values.
// "internal"→1, "server"→2, "client"→3, "producer"→4,
// "consumer"→5, unknown→0
func convertKind(kind string) int {
	if len(kind) > 20 {
		panic("convertKind: kind string too long")
	}

	switch kind {
	case "internal":
		return 1
	case "server":
		return 2
	case "client":
		return 3
	case "producer":
		return 4
	case "consumer":
		return 5
	default:
		return 0
	}
}

// convertLevel maps log level strings to OTLP SeverityNumber.
// "trace"→1, "debug"→5, "info"→9, "warn"→13, "error"→17
func convertLevel(level string) int {
	if len(level) > 20 {
		panic("convertLevel: level string too long")
	}

	switch level {
	case "trace":
		return 1
	case "debug":
		return 5
	case "info":
		return 9
	case "warn":
		return 13
	case "error":
		return 17
	default:
		return 0
	}
}

// convertStatus maps status strings to OTLP StatusCode and message.
// "ok"→1, "error"→2, other→0 (unset)
func convertStatus(
	status, errMsg string,
) (code int, msg string) {
	if len(status) > 20 {
		panic("convertStatus: status string too long")
	}
	if len(errMsg) > 10000 {
		panic("convertStatus: errMsg too long")
	}

	switch status {
	case "ok":
		return 1, ""
	case "error":
		return 2, errMsg
	default:
		return 0, ""
	}
}

// convertAttributes maps a generic attribute map to KeyValue pairs.
// Supports string, bool, int, int64, float64 values.
func convertAttributes(
	attrs map[string]any,
) []keyValue {
	if attrs == nil {
		return nil
	}
	const maxAttrs = 10000
	if len(attrs) > maxAttrs {
		panic("convertAttributes: attrs exceeds bound")
	}

	kvs := make([]keyValue, 0, len(attrs))
	for k, v := range attrs {
		kvs = append(kvs, keyValue{
			key:   k,
			value: toAnyValue(v),
		})
	}
	return kvs
}

// toAnyValue converts a Go value to an OTLP AnyValue.
func toAnyValue(v any) anyValue {
	if v == nil {
		return anyValue{kind: valueString, str: ""}
	}

	switch val := v.(type) {
	case string:
		return anyValue{kind: valueString, str: val}
	case bool:
		return anyValue{kind: valueBool, boolVal: val}
	case int:
		return anyValue{kind: valueInt, intVal: int64(val)}
	case int64:
		return anyValue{kind: valueInt, intVal: val}
	case float64:
		return anyValue{
			kind: valueDouble, doubleVal: val,
		}
	default:
		// Fall back to string representation
		return anyValue{kind: valueString, str: ""}
	}
}

// convertTags maps metric string tags to KeyValue pairs.
func convertTags(
	tags map[string]string,
) []keyValue {
	if tags == nil {
		return nil
	}
	const maxTags = 10000
	if len(tags) > maxTags {
		panic("convertTags: tags exceeds bound")
	}

	kvs := make([]keyValue, 0, len(tags))
	for k, v := range tags {
		kvs = append(kvs, keyValue{
			key: k,
			value: anyValue{
				kind: valueString,
				str:  v,
			},
		})
	}
	return kvs
}

// convertEvents maps SpanEvent slices to internal spanEvent.
func convertEvents(
	events []simple.SpanEvent,
) []spanEvent {
	if events == nil {
		return nil
	}
	const maxEvents = 10000
	if len(events) > maxEvents {
		panic("convertEvents: events exceeds bound")
	}

	out := make([]spanEvent, 0, len(events))
	for _, e := range events {
		out = append(out, spanEvent{
			name:         e.Name,
			timeUnixNano: e.Time.UnixNano(),
			attributes:   convertAttributes(e.Attributes),
		})
	}
	return out
}
