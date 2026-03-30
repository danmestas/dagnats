package simple

// propagation.go implements W3C trace context propagation for NATS messages.
// Dual-writing to both NATS headers and protocol.Event fields ensures
// trace context survives both in-flight (header) and at-rest (event store) paths.

import (
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// generateTraceID returns a new random 32-char hex trace ID (16 bytes).
// Panics if crypto/rand fails — that is an unrecoverable system error.
func generateTraceID() string {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic("generateTraceID: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// generateSpanID returns a new random 16-char hex span ID (8 bytes).
// Panics if crypto/rand fails — that is an unrecoverable system error.
func generateSpanID() string {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		panic("generateSpanID: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// formatTraceparent builds a W3C traceparent header value from its components.
// Format: "00-{traceID}-{spanID}-01" (version 00, sampled flag 01).
func formatTraceparent(traceID, spanID string) string {
	return "00-" + traceID + "-" + spanID + "-01"
}

// parseTraceparent splits a W3C traceparent string into its traceID and spanID.
// Returns ok=false for any input that does not match the expected 4-part format
// or does not use version "00". Rejects malformed values rather than guessing.
func parseTraceparent(tp string) (traceID, spanID string, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return "", "", false
	}
	if parts[0] != "00" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// injectTraceparent writes the W3C traceparent to both the NATS message header
// and the protocol.Event field atomically. Both writes happen together so that
// neither store holds a trace context the other is missing.
func injectTraceparent(msg *nats.Msg, evt *protocol.Event, traceID, spanID string) {
	if msg == nil {
		panic("injectTraceparent: msg must not be nil")
	}
	if evt == nil {
		panic("injectTraceparent: evt must not be nil")
	}
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	value := formatTraceparent(traceID, spanID)
	msg.Header.Set("traceparent", value)
	evt.TraceParent = value
}

// extractTraceparent reads the W3C traceparent from the NATS message header first,
// falling back to the protocol.Event field when the header is absent.
// Returns ok=false when neither location contains a valid traceparent.
func extractTraceparent(msg *nats.Msg, evt *protocol.Event) (traceID, spanID string, ok bool) {
	if msg == nil {
		panic("extractTraceparent: msg must not be nil")
	}
	if evt == nil {
		panic("extractTraceparent: evt must not be nil")
	}
	if msg.Header != nil {
		if header := msg.Header.Get("traceparent"); header != "" {
			return parseTraceparent(header)
		}
	}
	if evt.TraceParent != "" {
		return parseTraceparent(evt.TraceParent)
	}
	return "", "", false
}
