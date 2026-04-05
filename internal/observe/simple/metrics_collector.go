// observe/simple/metrics_collector.go
// MetricsCollector implements observe.Metrics backed by NATS JetStream.
// Each metric operation serializes a MetricPoint to JSON and publishes it to
// "telemetry.metrics.{service}.{name}" on the TELEMETRY stream so that any
// downstream consumer can aggregate or forward without coupling to a specific
// metrics backend.
package simple

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go/jetstream"
)

// MetricsCollector publishes MetricPoint events to the NATS
// TELEMETRY stream. Safe for concurrent use -- each instrument
// publishes independently.
type MetricsCollector struct {
	js          jetstream.JetStream
	serviceName string
}

// NewMetricsCollector constructs a MetricsCollector.
// Panics on nil js or empty serviceName -- programmer errors.
func NewMetricsCollector(
	js jetstream.JetStream, serviceName string,
) *MetricsCollector {
	if js == nil {
		panic("NewMetricsCollector: js must not be nil")
	}
	if serviceName == "" {
		panic("NewMetricsCollector: serviceName must not be empty")
	}
	return &MetricsCollector{js: js, serviceName: serviceName}
}

// Counter returns a new simpleCounter for the given name and tags.
func (mc *MetricsCollector) Counter(name string, tags map[string]string) observe.Counter {
	if name == "" {
		panic("MetricsCollector.Counter: name must not be empty")
	}
	if mc.js == nil {
		panic("MetricsCollector.Counter: js must not be nil")
	}
	return &simpleCounter{
		js: mc.js, serviceName: mc.serviceName,
		name: name, tags: tags,
	}
}

// Histogram returns a new simpleHistogram for the given name and tags.
func (mc *MetricsCollector) Histogram(name string, tags map[string]string) observe.Histogram {
	if name == "" {
		panic("MetricsCollector.Histogram: name must not be empty")
	}
	if mc.js == nil {
		panic("MetricsCollector.Histogram: js must not be nil")
	}
	return &simpleHistogram{
		js: mc.js, serviceName: mc.serviceName,
		name: name, tags: tags,
	}
}

// Gauge returns a new simpleGauge for the given name and tags.
func (mc *MetricsCollector) Gauge(name string, tags map[string]string) observe.Gauge {
	if name == "" {
		panic("MetricsCollector.Gauge: name must not be empty")
	}
	if mc.js == nil {
		panic("MetricsCollector.Gauge: js must not be nil")
	}
	return &simpleGauge{
		js: mc.js, serviceName: mc.serviceName,
		name: name, tags: tags,
	}
}

// publishMetric serializes a MetricPoint to JSON and publishes
// it to NATS. Errors are logged but never returned -- best-effort.
func publishMetric(js jetstream.JetStream, pt MetricPoint) {
	if js == nil {
		panic("publishMetric: js must not be nil")
	}
	if pt.Name == "" {
		panic("publishMetric: metric name must not be empty")
	}
	data, err := json.Marshal(pt)
	if err != nil {
		log.Printf(
			"publishMetric: marshal error name=%s: %v",
			pt.Name, err)
		return
	}
	subject := "telemetry.metrics." + pt.Service + "." + pt.Name
	_, err = js.Publish(context.Background(), subject, data)
	if err != nil {
		log.Printf(
			"publishMetric: publish error subject=%s: %v",
			subject, err)
	}
}

// simpleCounter is a monotonically increasing metric instrument.
type simpleCounter struct {
	js          jetstream.JetStream
	serviceName string
	name        string
	tags        map[string]string
}

// Inc increments the counter by 1.
func (c *simpleCounter) Inc() { c.Add(1.0) }

// Add increments the counter by delta.
func (c *simpleCounter) Add(delta float64) {
	publishMetric(c.js, MetricPoint{
		Name:      c.name,
		Type:      "counter",
		Value:     delta,
		Tags:      c.tags,
		Service:   c.serviceName,
		Timestamp: time.Now().UTC(),
	})
}

// simpleHistogram records observations (e.g. latencies, sizes).
type simpleHistogram struct {
	js          jetstream.JetStream
	serviceName string
	name        string
	tags        map[string]string
}

// Observe records a single observation value.
func (h *simpleHistogram) Observe(value float64) {
	publishMetric(h.js, MetricPoint{
		Name:      h.name,
		Type:      "histogram",
		Value:     value,
		Tags:      h.tags,
		Service:   h.serviceName,
		Timestamp: time.Now().UTC(),
	})
}

// simpleGauge is a metric that can go up or down.
// The current value is maintained atomically so concurrent Set/Inc/Dec calls
// are race-free. math.Float64bits/Float64frombits is the standard idiom for
// storing float64 in a uint64 atomic.
type simpleGauge struct {
	js          jetstream.JetStream
	serviceName string
	name        string
	tags        map[string]string
	bits        atomic.Uint64 // stores float64 via math.Float64bits
}

// Set replaces the gauge value and publishes it.
func (g *simpleGauge) Set(value float64) {
	g.bits.Store(math.Float64bits(value))
	g.publish()
}

const gaugeCASRetryMax = 1000

// Inc increments the gauge by 1 and publishes.
func (g *simpleGauge) Inc() {
	for i := 0; i < gaugeCASRetryMax; i++ {
		old := g.bits.Load()
		next := math.Float64frombits(old) + 1.0
		if g.bits.CompareAndSwap(old, math.Float64bits(next)) {
			g.publish()
			return
		}
	}
	log.Printf("simpleGauge.Inc: CAS failed after %d retries",
		gaugeCASRetryMax)
}

// Dec decrements the gauge by 1 and publishes.
func (g *simpleGauge) Dec() {
	for i := 0; i < gaugeCASRetryMax; i++ {
		old := g.bits.Load()
		next := math.Float64frombits(old) - 1.0
		if g.bits.CompareAndSwap(old, math.Float64bits(next)) {
			g.publish()
			return
		}
	}
	log.Printf("simpleGauge.Dec: CAS failed after %d retries",
		gaugeCASRetryMax)
}

// publish sends the current gauge value to the TELEMETRY stream.
func (g *simpleGauge) publish() {
	value := math.Float64frombits(g.bits.Load())
	publishMetric(g.js, MetricPoint{
		Name:      g.name,
		Type:      "gauge",
		Value:     value,
		Tags:      g.tags,
		Service:   g.serviceName,
		Timestamp: time.Now().UTC(),
	})
}
