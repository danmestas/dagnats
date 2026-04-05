package simple

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

const (
	jaegerBatchMax     = 100
	jaegerFlushTimeout = 5 * time.Second
	jaegerHTTPTimeout  = 10 * time.Second
	jaegerChanSize     = 256
)

// ExportToJaeger subscribes to span messages on the TELEMETRY
// stream and POSTs batches to a Jaeger OTLP/HTTP endpoint.
// Blocks until ctx is cancelled, then flushes remaining spans.
func ExportToJaeger(
	ctx context.Context,
	js nats.JetStreamContext,
	endpoint string,
	logger observe.Logger,
) {
	if js == nil {
		panic("ExportToJaeger: js must not be nil")
	}
	if endpoint == "" {
		panic("ExportToJaeger: endpoint must not be empty")
	}
	if logger == nil {
		panic("ExportToJaeger: logger must not be nil")
	}

	msgCh := make(chan *nats.Msg, jaegerChanSize)
	sub, err := js.ChanSubscribe(
		"telemetry.spans.>", msgCh, nats.DeliverNew(),
	)
	if err != nil {
		logger.Error("jaeger: subscribe failed", err)
		return
	}
	defer sub.Unsubscribe() //nolint:errcheck

	client := &http.Client{Timeout: jaegerHTTPTimeout}
	url := endpoint + "/v1/traces"
	batch := make([]SpanRecord, 0, jaegerBatchMax)
	ticker := time.NewTicker(jaegerFlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			drainChannel(msgCh, &batch, logger)
			if len(batch) > 0 {
				postBatch(client, url, batch, logger)
			}
			return
		case <-ticker.C:
			if len(batch) > 0 {
				postBatch(client, url, batch, logger)
				batch = batch[:0]
			}
		case msg := <-msgCh:
			span, ok := decodeSpan(msg.Data, logger)
			if !ok {
				continue
			}
			batch = append(batch, span)
			if len(batch) >= jaegerBatchMax {
				postBatch(client, url, batch, logger)
				batch = batch[:0]
			}
		}
	}
}

// drainChannel reads any remaining messages from the channel
// without blocking, appending decoded spans to the batch.
func drainChannel(
	ch <-chan *nats.Msg,
	batch *[]SpanRecord,
	logger observe.Logger,
) {
	for i := 0; i < jaegerBatchMax; i++ {
		select {
		case msg := <-ch:
			span, ok := decodeSpan(msg.Data, logger)
			if ok {
				*batch = append(*batch, span)
			}
		default:
			return
		}
	}
}

func decodeSpan(
	data []byte,
	logger observe.Logger,
) (SpanRecord, bool) {
	var rec SpanRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		logger.Error("jaeger: unmarshal span", err)
		return rec, false
	}
	return rec, true
}

// OTLP/HTTP JSON envelope types with camelCase field names.
type otlpPayload struct {
	ResourceSpans []otlpResourceSpan `json:"resourceSpans"`
}
type otlpResourceSpan struct {
	Resource   otlpResource    `json:"resource"`
	ScopeSpans []otlpScopeSpan `json:"scopeSpans"`
}
type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}
type otlpScopeSpan struct {
	Spans []otlpSpan `json:"spans"`
}
type otlpSpan struct {
	TraceID    string         `json:"traceId"`
	SpanID     string         `json:"spanId"`
	ParentID   string         `json:"parentSpanId,omitempty"`
	Name       string         `json:"name"`
	Kind       int            `json:"kind"`
	Start      string         `json:"startTimeUnixNano"`
	End        string         `json:"endTimeUnixNano"`
	Status     otlpStatus     `json:"status"`
	Attributes []otlpKeyValue `json:"attributes,omitempty"`
	Events     []otlpEvent    `json:"events,omitempty"`
}
type otlpStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}
type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}
type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}
type otlpEvent struct {
	Name       string         `json:"name"`
	Time       string         `json:"timeUnixNano"`
	Attributes []otlpKeyValue `json:"attributes,omitempty"`
}

// spanToOTLP converts a SpanRecord to the OTLP wire format.
func spanToOTLP(rec SpanRecord) otlpSpan {
	if rec.TraceID == "" {
		panic("spanToOTLP: TraceID must not be empty")
	}
	if rec.SpanID == "" {
		panic("spanToOTLP: SpanID must not be empty")
	}
	attrs := make([]otlpKeyValue, 0, len(rec.Attributes))
	for k, v := range rec.Attributes {
		attrs = append(attrs, otlpKeyValue{
			Key:   k,
			Value: otlpAnyValue{StringValue: fmt.Sprint(v)},
		})
	}
	events := make([]otlpEvent, 0, len(rec.Events))
	for _, e := range rec.Events {
		events = append(events, eventToOTLP(e))
	}
	return otlpSpan{
		TraceID:    rec.TraceID,
		SpanID:     rec.SpanID,
		ParentID:   rec.ParentID,
		Name:       rec.Name,
		Kind:       otlpSpanKind(rec.Kind),
		Start:      unixNano(rec.StartTime),
		End:        unixNano(rec.EndTime),
		Status:     otlpStatusFromString(rec.Status),
		Attributes: attrs,
		Events:     events,
	}
}

func eventToOTLP(e SpanEvent) otlpEvent {
	attrs := make([]otlpKeyValue, 0, len(e.Attributes))
	for k, v := range e.Attributes {
		attrs = append(attrs, otlpKeyValue{
			Key:   k,
			Value: otlpAnyValue{StringValue: fmt.Sprint(v)},
		})
	}
	return otlpEvent{
		Name: e.Name, Time: unixNano(e.Time), Attributes: attrs,
	}
}

func unixNano(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixNano())
}

func otlpSpanKind(kind string) int {
	switch kind {
	case "server":
		return 2
	case "client":
		return 3
	default:
		return 1 // internal
	}
}

func otlpStatusFromString(status string) otlpStatus {
	if status == "error" {
		return otlpStatus{Code: 2}
	}
	return otlpStatus{Code: 1}
}

func buildOTLPPayload(
	batch []SpanRecord,
) otlpPayload {
	if len(batch) == 0 {
		panic("buildOTLPPayload: batch must not be empty")
	}
	if batch[0].Service == "" {
		panic("buildOTLPPayload: first span Service must not be empty")
	}
	spans := make([]otlpSpan, 0, len(batch))
	svcName := ""
	for _, rec := range batch {
		spans = append(spans, spanToOTLP(rec))
		if svcName == "" {
			svcName = rec.Service
		}
	}
	return otlpPayload{
		ResourceSpans: []otlpResourceSpan{{
			Resource: otlpResource{
				Attributes: []otlpKeyValue{{
					Key:   "service.name",
					Value: otlpAnyValue{StringValue: svcName},
				}},
			},
			ScopeSpans: []otlpScopeSpan{{Spans: spans}},
		}},
	}
}

func postBatch(
	client *http.Client,
	url string,
	batch []SpanRecord,
	logger observe.Logger,
) {
	if client == nil {
		panic("postBatch: client must not be nil")
	}
	if url == "" {
		panic("postBatch: url must not be empty")
	}
	payload := buildOTLPPayload(batch)
	body, err := json.Marshal(payload)
	if err != nil {
		logger.Error("jaeger: marshal payload", err)
		return
	}
	resp, err := client.Post(
		url, "application/json", bytes.NewReader(body),
	)
	if err != nil {
		logger.Error("jaeger: POST failed", err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Error("jaeger: non-2xx response", fmt.Errorf(
			"status %d", resp.StatusCode))
	}
}
