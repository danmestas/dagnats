package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PumpSubject is the wildcard that captures every metric record the
// observe/natsexporter publishes. Pattern matches the documented shape
// in observe/natsexporter/metric_exporter.go.
const PumpSubject = "telemetry.metrics.>"

// PumpConsumerName is the legacy durable consumer name. The pump no
// longer creates a durable (see pumpInstallConsumer for why); this
// const survives only so startup can best-effort delete a pre-existing
// durable left by older builds, cleaning up the broken-on-restart
// state on upgrade.
const PumpConsumerName = "console_metrics_aggregator"

// PumpBatchMax is the upper bound on messages fetched per pull. Caps
// per-iteration memory; the pump loop iterates until ctx is done.
const PumpBatchMax = 500

// PumpReplayWindow is how far back the pump replays on each start.
// 1 hour balances "dashboard recovers after restart" against "we
// don't redownload 24h of history every boot".
const PumpReplayWindow = 1 * time.Hour

// PumpInactiveThreshold tells the server to reap the ephemeral pump
// consumer this long after the process stops fetching (i.e. after the
// engine exits). Bounded so a crashed/restarted engine never leaves
// orphan consumers accumulating on TELEMETRY across restarts.
const PumpInactiveThreshold = 5 * time.Minute

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
		a.pumpLoop(pumpCtx, js, cons)
	}()
	return func() { cancel(); <-done }, nil
}

// pumpInstallConsumer creates the ephemeral pull consumer the pump
// reads from. Returns the consumer handle ready for Fetch loops.
//
// The consumer is EPHEMERAL (no Durable) on purpose. A durable with
// DeliverByStartTimePolicy + OptStartTime breaks on restart: those
// fields are immutable, so CreateOrUpdateConsumer against an existing
// durable fails with "start time can not be updated" (nats 10012) and
// the aggregator silently disables itself. AckNonePolicy meant the
// durable never tracked offsets either, so its "resume from last-acked
// offset" rationale never actually applied. An ephemeral consumer
// replaying the recent window on each start gives the intended
// behavior with no durable state to conflict on.
func pumpInstallConsumer(
	ctx context.Context, js jetstream.JetStream,
) (jetstream.Consumer, error) {
	if ctx == nil {
		panic("pumpInstallConsumer: ctx is nil")
	}
	if js == nil {
		panic("pumpInstallConsumer: js is nil")
	}
	// Upgrade path: best-effort delete a durable left by older builds
	// so deployments carrying the broken-on-restart durable get
	// cleaned up. A missing consumer is the expected steady state.
	if err := js.DeleteConsumer(
		ctx, "TELEMETRY", PumpConsumerName,
	); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("metrics pump: delete legacy durable: %w", err)
	}
	return pumpCreateConsumer(ctx, js, time.Now().Add(-PumpReplayWindow))
}

// pumpCreateConsumer creates an ephemeral pull consumer on TELEMETRY
// delivering metric records stamped at or after start.
func pumpCreateConsumer(
	ctx context.Context, js jetstream.JetStream, start time.Time,
) (jetstream.Consumer, error) {
	if js == nil {
		panic("pumpCreateConsumer: js is nil")
	}
	if start.IsZero() {
		panic("pumpCreateConsumer: start is zero")
	}
	cfg := jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckNonePolicy,
		FilterSubject:     PumpSubject,
		DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
		OptStartTime:      &start,
		MaxAckPending:     -1,
		MaxRequestBatch:   PumpBatchMax,
		InactiveThreshold: PumpInactiveThreshold,
	}
	cons, err := js.CreateConsumer(ctx, "TELEMETRY", cfg)
	if err != nil {
		return nil, fmt.Errorf("metrics pump: consumer: %w", err)
	}
	return cons, nil
}

// pumpLoop is the steady-state read loop. Bounded loop count keeps the
// goroutine from infinite-looping on a broken consumer; in practice
// the maxIterations branch is unreachable since ctx cancellation
// breaks first.
//
// Every fetch fault backs off before the next fetch. Fetch reports most
// faults on the batch rather than its own return value, so a loop that
// reads only Fetch's error spins at full speed when the consumer is
// gone (#952).
func (a *Aggregator) pumpLoop(
	ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer,
) {
	if a == nil {
		panic("pumpLoop: a is nil")
	}
	if cons == nil {
		panic("pumpLoop: cons is nil")
	}
	const maxIterations = 1_000_000_000
	var last time.Time
	for i := 0; i < maxIterations; i++ {
		if ctx.Err() != nil {
			return
		}
		err := a.pumpFetchOnce(ctx, cons, &last)
		if err == nil {
			continue
		}
		if !a.pumpBackoff(ctx, err) {
			return
		}
		if pumpConsumerGone(err) {
			cons = a.pumpRecreate(ctx, js, cons, last)
		}
	}
}

// pumpFetchOnce fetches and ingests one batch, advancing last to the
// stream timestamp of the newest message ingested. Returns the fetch
// fault, from Fetch itself or surfaced on the batch, if any.
func (a *Aggregator) pumpFetchOnce(
	ctx context.Context, cons jetstream.Consumer, last *time.Time,
) error {
	if cons == nil {
		panic("pumpFetchOnce: cons is nil")
	}
	if last == nil {
		panic("pumpFetchOnce: last is nil")
	}
	batch, err := cons.Fetch(
		PumpBatchMax, jetstream.FetchMaxWait(time.Second),
	)
	if err != nil {
		return err
	}
	a.pumpDrainBatch(ctx, batch, last)
	return batch.Error()
}

// pumpConsumerGone reports whether err means the pump's consumer no
// longer exists. With no consumer listening, a fetch request gets the
// server's 503 no-responders status.
func pumpConsumerGone(err error) bool {
	return errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrConsumerNotFound)
}

// pumpRecreate replaces a vanished consumer with one resuming just
// after the last ingested message, or replaying the usual window when
// nothing was ingested yet. On failure it keeps the old handle; the
// next fetch fails, backs off, and lands here again.
func (a *Aggregator) pumpRecreate(
	ctx context.Context, js jetstream.JetStream,
	old jetstream.Consumer, last time.Time,
) jetstream.Consumer {
	if old == nil {
		panic("pumpRecreate: old is nil")
	}
	start := time.Now().Add(-PumpReplayWindow)
	if !last.IsZero() {
		start = last.Add(time.Nanosecond)
	}
	cons, err := pumpCreateConsumer(ctx, js, start)
	if err != nil {
		a.logger.Warn("metrics pump: recreate consumer failed", "err", err)
		return old
	}
	a.logger.Info("metrics pump: recreated vanished consumer",
		"resume_from", start)
	return cons
}

// pumpBackoff logs the fetch fault and waits before the next fetch.
// Returns false when ctx ended, so the loop exits promptly.
func (a *Aggregator) pumpBackoff(ctx context.Context, err error) bool {
	if err == nil {
		panic("pumpBackoff: err is nil")
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	a.logger.Warn("metrics pump: fetch failed", "err", err)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}

// pumpDrainBatch decodes each message, calls Ingest, then moves on.
// JetStream MessageBatch yields a channel; we range it with a bounded
// counter so a malformed batch can't pin the pump goroutine.
func (a *Aggregator) pumpDrainBatch(
	ctx context.Context, batch jetstream.MessageBatch, last *time.Time,
) {
	if batch == nil {
		return
	}
	if last == nil {
		panic("pumpDrainBatch: last is nil")
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
		if meta, err := msg.Metadata(); err == nil && meta.Timestamp.After(*last) {
			*last = meta.Timestamp
		}
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
		// decodeRecord is logging-free (keeps otel_decode.go NATS- and
		// log-free); surface the drop here so a silently-undecoded
		// metric kind is visible instead of vanishing.
		a.logger.Warn("metrics pump: undecoded record",
			"name", rec.Name, "subject", msg.Subject())
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
