package otlp

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/observe/simple"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// BridgeConfig controls the OTLP bridge behavior.
type BridgeConfig struct {
	Endpoint      string
	Insecure      bool
	Headers       map[string]string
	BatchSize     int
	FlushInterval time.Duration
	ServiceName   string
}

// Bridge consumes from the NATS TELEMETRY stream and exports
// to an OTLP/HTTP endpoint as protobuf.
type Bridge struct {
	nc     *nats.Conn
	cfg    BridgeConfig
	cancel context.CancelFunc
	wg     sync.WaitGroup
	client *http.Client
}

// NewBridge creates a bridge that will consume from TELEMETRY
// and POST to the configured OTLP endpoint.
func NewBridge(nc *nats.Conn, cfg BridgeConfig) *Bridge {
	if nc == nil {
		panic("NewBridge: nc must not be nil")
	}
	if cfg.Endpoint == "" {
		panic("NewBridge: Endpoint must not be empty")
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "dagnats"
	}

	return &Bridge{
		nc:  nc,
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Start begins consuming from the TELEMETRY stream.
func (b *Bridge) Start() {
	if b.cancel != nil {
		panic("Bridge.Start: already started")
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(1)
	go b.consumeLoop(ctx)
}

// Stop signals the bridge to stop and waits for shutdown.
func (b *Bridge) Stop() {
	if b.cancel == nil {
		panic("Bridge.Stop: not started")
	}

	b.cancel()
	b.wg.Wait()
}

// consumeLoop is the main consumer loop that fetches messages
// from the TELEMETRY stream and routes them to export.
func (b *Bridge) consumeLoop(ctx context.Context) {
	defer b.wg.Done()

	js, err := jetstream.New(b.nc)
	if err != nil {
		log.Printf("otlp-bridge: jetstream: %v", err)
		return
	}

	cons, err := b.createConsumer(ctx, js)
	if err != nil {
		log.Printf("otlp-bridge: consumer: %v", err)
		return
	}

	b.fetchLoop(ctx, cons)
}

// createConsumer creates the durable OTLP bridge consumer.
func (b *Bridge) createConsumer(
	ctx context.Context, js jetstream.JetStream,
) (jetstream.Consumer, error) {
	if js == nil {
		panic("createConsumer: js must not be nil")
	}

	return js.CreateOrUpdateConsumer(
		ctx, "TELEMETRY",
		jetstream.ConsumerConfig{
			Durable:       "otlp-bridge",
			FilterSubject: "telemetry.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			MaxDeliver:    4,
			AckWait:       30 * time.Second,
		},
	)
}

// fetchLoop fetches batches and processes them until cancelled.
func (b *Bridge) fetchLoop(
	ctx context.Context, cons jetstream.Consumer,
) {
	if cons == nil {
		panic("fetchLoop: cons must not be nil")
	}

	const maxIterations = 1<<63 - 1
	for i := 0; i < maxIterations; i++ {
		if ctx.Err() != nil {
			return
		}
		b.fetchBatch(ctx, cons)
	}
}

// fetchBatch fetches up to BatchSize messages and exports them.
func (b *Bridge) fetchBatch(
	ctx context.Context, cons jetstream.Consumer,
) {
	if cons == nil {
		panic("fetchBatch: cons must not be nil")
	}

	msgs, err := cons.Fetch(
		b.cfg.BatchSize,
		jetstream.FetchMaxWait(b.cfg.FlushInterval),
	)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("otlp-bridge: fetch: %v", err)
		return
	}

	b.processBatch(ctx, msgs)
}

// processBatch routes messages by subject and exports them.
func (b *Bridge) processBatch(
	ctx context.Context,
	msgs jetstream.MessageBatch,
) {
	if msgs == nil {
		panic("processBatch: msgs must not be nil")
	}

	var spans []simple.SpanRecord
	var logs []simple.LogRecord
	var metrics []simple.MetricPoint
	var allMsgs []jetstream.Msg

	const maxPerBatch = 10000
	count := 0
	for msg := range msgs.Messages() {
		if count >= maxPerBatch {
			break
		}
		count++
		allMsgs = append(allMsgs, msg)
		subj := msg.Subject()
		b.routeMessage(
			subj, msg.Data(),
			&spans, &logs, &metrics,
		)
	}

	if err := msgs.Error(); err != nil {
		if ctx.Err() == nil {
			log.Printf("otlp-bridge: batch error: %v", err)
		}
	}

	if len(allMsgs) == 0 {
		return
	}

	ok := b.exportAll(ctx, spans, logs, metrics)
	b.ackOrNak(ok, allMsgs)
}

// routeMessage decodes a message and appends to the right batch.
func (b *Bridge) routeMessage(
	subject string,
	data []byte,
	spans *[]simple.SpanRecord,
	logs *[]simple.LogRecord,
	metrics *[]simple.MetricPoint,
) {
	if subject == "" {
		panic("routeMessage: subject must not be empty")
	}
	if len(data) > 10*1024*1024 {
		panic("routeMessage: data exceeds 10MB")
	}

	switch {
	case strings.HasPrefix(subject, "telemetry.spans."):
		var s simple.SpanRecord
		if err := json.Unmarshal(data, &s); err != nil {
			log.Printf(
				"otlp-bridge: unmarshal span: %v", err,
			)
			return
		}
		*spans = append(*spans, s)

	case strings.HasPrefix(subject, "telemetry.logs."):
		var l simple.LogRecord
		if err := json.Unmarshal(data, &l); err != nil {
			log.Printf(
				"otlp-bridge: unmarshal log: %v", err,
			)
			return
		}
		*logs = append(*logs, l)

	case strings.HasPrefix(subject, "telemetry.metrics."):
		var m simple.MetricPoint
		if err := json.Unmarshal(data, &m); err != nil {
			log.Printf(
				"otlp-bridge: unmarshal metric: %v", err,
			)
			return
		}
		*metrics = append(*metrics, m)

	default:
		log.Printf(
			"otlp-bridge: unknown subject: %s", subject,
		)
	}
}

// exportAll sends spans, logs, and metrics to the OTLP endpoint.
// Returns true if all non-empty exports succeeded.
func (b *Bridge) exportAll(
	ctx context.Context,
	spans []simple.SpanRecord,
	logs []simple.LogRecord,
	metrics []simple.MetricPoint,
) bool {
	ok := true
	if len(spans) > 0 {
		if !b.exportSpans(ctx, spans) {
			ok = false
		}
	}
	if len(logs) > 0 {
		if !b.exportLogs(ctx, logs) {
			ok = false
		}
	}
	if len(metrics) > 0 {
		if !b.exportMetrics(ctx, metrics) {
			ok = false
		}
	}
	return ok
}

// exportSpans marshals and POSTs spans to /v1/traces.
func (b *Bridge) exportSpans(
	ctx context.Context, spans []simple.SpanRecord,
) bool {
	data, err := marshalTraceExport(
		b.cfg.ServiceName, spans,
	)
	if err != nil {
		log.Printf("otlp-bridge: marshal traces: %v", err)
		return false
	}
	return b.post(ctx, "/v1/traces", data)
}

// exportLogs marshals and POSTs logs to /v1/logs.
func (b *Bridge) exportLogs(
	ctx context.Context, logs []simple.LogRecord,
) bool {
	data, err := marshalLogExport(
		b.cfg.ServiceName, logs,
	)
	if err != nil {
		log.Printf("otlp-bridge: marshal logs: %v", err)
		return false
	}
	return b.post(ctx, "/v1/logs", data)
}

// exportMetrics marshals and POSTs metrics to /v1/metrics.
func (b *Bridge) exportMetrics(
	ctx context.Context, metrics []simple.MetricPoint,
) bool {
	data, err := marshalMetricExport(
		b.cfg.ServiceName, metrics,
	)
	if err != nil {
		log.Printf("otlp-bridge: marshal metrics: %v", err)
		return false
	}
	return b.post(ctx, "/v1/metrics", data)
}

// post sends protobuf data to the OTLP endpoint at the given path.
func (b *Bridge) post(
	ctx context.Context, path string, data []byte,
) bool {
	if path == "" {
		panic("post: path must not be empty")
	}
	if data == nil {
		panic("post: data must not be nil")
	}

	url := strings.TrimRight(b.cfg.Endpoint, "/") + path
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, url,
		bytes.NewReader(data),
	)
	if err != nil {
		log.Printf("otlp-bridge: create request: %v", err)
		return false
	}
	req.Header.Set(
		"Content-Type", "application/x-protobuf",
	)
	for k, v := range b.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		log.Printf("otlp-bridge: POST %s: %v", path, err)
		return false
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf(
				"otlp-bridge: close body: %v", err,
			)
		}
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true
	}
	log.Printf(
		"otlp-bridge: POST %s: status %d",
		path, resp.StatusCode,
	)
	return false
}

// ackOrNak acknowledges or naks all messages in the batch.
func (b *Bridge) ackOrNak(
	ok bool, msgs []jetstream.Msg,
) {
	const maxMsgs = 10000
	if len(msgs) > maxMsgs {
		panic("ackOrNak: msgs exceeds max bound")
	}

	for _, msg := range msgs {
		if ok {
			if err := msg.Ack(); err != nil {
				log.Printf(
					"otlp-bridge: ack: %v", err,
				)
			}
		} else {
			if err := msg.NakWithDelay(
				5 * time.Second,
			); err != nil {
				log.Printf(
					"otlp-bridge: nak: %v", err,
				)
			}
		}
	}
}
