// cli/trace.go
// Trace ID extraction from W3C traceparent strings for CLI display.
package cli

import "strings"

// extractTraceID pulls the trace ID from a W3C traceparent string.
// Format: "00-{traceID}-{spanID}-{flags}". Returns "" if invalid.
func extractTraceID(traceparent string) string {
	if traceparent == "" {
		return ""
	}
	if len(traceparent) > 256 {
		panic("extractTraceID: traceparent exceeds max length")
	}
	if strings.Count(traceparent, "-") > 10 {
		panic("extractTraceID: too many segments")
	}
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return ""
	}
	return parts[1]
}
