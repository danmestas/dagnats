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

// otlpPayload is the simplified OTLP/HTTP JSON envelope.
type otlpPayload struct {
	ResourceSpans []resourceSpan `json:"resourceSpans"`
}

type resourceSpan struct {
	ScopeSpans []scopeSpan `json:"scopeSpans"`
}

type scopeSpan struct {
	Spans []SpanRecord `json:"spans"`
}

func postBatch(
	client *http.Client,
	url string,
	batch []SpanRecord,
	logger observe.Logger,
) {
	payload := otlpPayload{
		ResourceSpans: []resourceSpan{
			{ScopeSpans: []scopeSpan{
				{Spans: batch},
			}},
		},
	}

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
