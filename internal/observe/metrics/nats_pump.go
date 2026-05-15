package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// PumpSubject is the wildcard that captures every metric record the
// observe/natsexporter publishes. Pattern matches the documented shape
// in observe/natsexporter/metric_exporter.go.
const PumpSubject = "telemetry.metrics.>"

// PumpConsumerName is the durable consumer name the aggregator
// installs on the TELEMETRY stream. Durable so a console restart
// resumes from the last-acked offset rather than replaying 7 days of
// history.
const PumpConsumerName = "console_metrics_aggregator"

// PumpBatchMax is the upper bound on messages fetched per pull. Caps
// per-iteration memory; the pump loop iterates until ctx is done.
const PumpBatchMax = 500

// PumpReplayWindow is how far back the pump replays on first start.
// 1 hour balances "dashboard recovers after restart" against "we
// don't redownload 24h of history every boot".
const PumpReplayWindow = 1 * time.Hour

// metricRecord mirrors the JSON shape observe/natsexporter publishes.
// The aggregator deliberately decodes this lightweight copy rather
// than importing the OTel SDK types — keeps the package
// provider-agnostic and the dependency graph thin.
type metricRecord struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Unit        string          `json:"unit,omitempty"`
	ServiceName string          `json:"serviceName"`
	Data        json.RawMessage `json:"data"`
	Timestamp   string          `json:"timestamp"`
}

// StartPump installs the durable consumer on TELEMETRY and pumps
// messages into Ingest until ctx is cancelled. Returns a stop function
// that blocks until the pump goroutine exits. The pump never errors
// fatally — transport errors are slog.Warn'd and the loop retries
// after a fixed backoff.
//
// Returns the stop function and a synchronous setup error (stream
// missing, consumer creation failed). After setup completes the pump
// loop continues even through transient JetStream failures.
func (a *Aggregator) StartPump(
	ctx context.Context, js jetstream.JetStream,
) (func(), error) {
	if a == nil {
		panic("Aggregator.StartPump: a is nil")
	}
	if ctx == nil {
		panic("Aggregator.StartPump: ctx is nil")
	}
	if js == nil {
		panic("Aggregator.StartPump: js is nil")
	}
	cons, err := pumpInstallConsumer(ctx, js)
	if err != nil {
		return func() {}, err
	}
	pumpCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.pumpLoop(pumpCtx, cons)
	}()
	return func() { cancel(); <-done }, nil
}

// pumpInstallConsumer creates or updates the durable consumer. Returns
// the consumer handle ready for Fetch loops.
func pumpInstallConsumer(
	ctx context.Context, js jetstream.JetStream,
) (jetstream.Consumer, error) {
	if ctx == nil {
		panic("pumpInstallConsumer: ctx is nil")
	}
	if js == nil {
		panic("pumpInstallConsumer: js is nil")
	}
	startTime := time.Now().Add(-PumpReplayWindow)
	cfg := jetstream.ConsumerConfig{
		Durable:         PumpConsumerName,
		AckPolicy:       jetstream.AckNonePolicy,
		FilterSubject:   PumpSubject,
		DeliverPolicy:   jetstream.DeliverByStartTimePolicy,
		OptStartTime:    &startTime,
		MaxAckPending:   -1,
		MaxRequestBatch: PumpBatchMax,
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "TELEMETRY", cfg)
	if err != nil {
		return nil, fmt.Errorf("metrics pump: consumer: %w", err)
	}
	return cons, nil
}

// pumpLoop is the steady-state read loop. Bounded loop count keeps the
// goroutine from infinite-looping on a broken consumer; in practice
// the maxIterations branch is unreachable since ctx cancellation
// breaks first.
func (a *Aggregator) pumpLoop(
	ctx context.Context, cons jetstream.Consumer,
) {
	if a == nil {
		panic("pumpLoop: a is nil")
	}
	if cons == nil {
		panic("pumpLoop: cons is nil")
	}
	const maxIterations = 1_000_000_000
	for i := 0; i < maxIterations; i++ {
		if ctx.Err() != nil {
			return
		}
		batch, err := cons.Fetch(
			PumpBatchMax, jetstream.FetchMaxWait(time.Second),
		)
		if err != nil {
			a.pumpHandleFetchError(ctx, err)
			continue
		}
		a.pumpDrainBatch(ctx, batch)
	}
}

// pumpHandleFetchError logs the transport error and sleeps before the
// next iteration. Context-aware so shutdown still drains promptly.
func (a *Aggregator) pumpHandleFetchError(
	ctx context.Context, err error,
) {
	if errors.Is(err, context.Canceled) {
		return
	}
	a.logger.Warn("metrics pump: fetch failed",
		"err", err)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
}

// pumpDrainBatch decodes each message, calls Ingest, then moves on.
// JetStream MessageBatch yields a channel; we range it with a bounded
// counter so a malformed batch can't pin the pump goroutine.
func (a *Aggregator) pumpDrainBatch(
	ctx context.Context, batch jetstream.MessageBatch,
) {
	if batch == nil {
		return
	}
	const maxPerBatch = PumpBatchMax
	count := 0
	for msg := range batch.Messages() {
		count++
		if count > maxPerBatch {
			break
		}
		if ctx.Err() != nil {
			return
		}
		a.pumpHandleMessage(msg)
	}
}

// pumpHandleMessage parses one NATS message into a metricRecord and
// folds it into the aggregator. Parse failures slog.Warn — the OTel
// pipeline shouldn't be the bottleneck for dashboard rendering, so we
// keep going.
func (a *Aggregator) pumpHandleMessage(msg jetstream.Msg) {
	if msg == nil {
		return
	}
	var rec metricRecord
	if err := json.Unmarshal(msg.Data(), &rec); err != nil {
		a.logger.Warn("metrics pump: bad json",
			"err", err, "subject", msg.Subject())
		return
	}
	series, point, ok := decodeRecord(rec)
	if !ok {
		// decodeRecord already logged; nothing more to say.
		return
	}
	if err := a.Ingest(series, point); err != nil {
		a.logger.Warn("metrics pump: ingest dropped",
			"name", rec.Name, "err", err)
	}
}

// decodeRecord normalises one metricRecord into a Series + Point. The
// OTel SDK serialises Data with a kind-shaped payload (Sum, Gauge,
// Histogram) — we sniff the shape rather than maintaining a tagged
// union mirror.
func decodeRecord(rec metricRecord) (Series, Point, bool) {
	if rec.Name == "" {
		return Series{}, Point{}, false
	}
	ts := parseTimestamp(rec.Timestamp)
	series := Series{
		Name:        rec.Name,
		Description: rec.Description,
		Unit:        rec.Unit,
		Service:     rec.ServiceName,
	}
	var pt Point
	pt.Timestamp = ts
	if decoded, ok := decodeSum(rec.Data); ok {
		series.Kind = KindCounter
		return series, mergePoint(pt, decoded), true
	}
	if decoded, ok := decodeGauge(rec.Data); ok {
		series.Kind = KindGauge
		return series, mergePoint(pt, decoded), true
	}
	if decoded, ok := decodeHistogram(rec.Data); ok {
		series.Kind = KindHistogram
		return series, mergePoint(pt, decoded), true
	}
	return Series{}, Point{}, false
}

// mergePoint folds the decoded data-point fields into the base Point
// (which already carries the timestamp). Pulled out so the decoders
// stay close to the OTel JSON shape.
func mergePoint(base, decoded Point) Point {
	base.Value = decoded.Value
	base.Count = decoded.Count
	base.Sum = decoded.Sum
	base.Buckets = decoded.Buckets
	base.Labels = decoded.Labels
	return base
}

// parseTimestamp parses the RFC3339Nano timestamp the natsexporter
// writes, falling back to time.Now on parse failure so the point
// still lands.
func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Now().UTC()
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Now().UTC()
	}
	return t
}

// safeFloat coerces a float64 to a finite, NaN-free value. The
// Prometheus exporter rejects NaN / Inf in counter samples; we strip
// them at ingest so downstream consumers never have to defensive-code.
func safeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}
