// cli/log_record.go
// LogRecord represents a structured log entry consumed from the
// NATS telemetry.logs stream. This is the wire format published
// by the OTel-based NATS log exporter (observe/natsexporter).
package cli

// LogRecord is the JSON shape of log entries on the NATS
// telemetry log stream. Used by the logs tail command for
// deserialization and display.
type LogRecord struct {
	Timestamp   string            `json:"timestamp"`
	Severity    string            `json:"severity"`
	Body        string            `json:"body"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	TraceID     string            `json:"traceId,omitempty"`
	SpanID      string            `json:"spanId,omitempty"`
	ServiceName string            `json:"serviceName"`
}
