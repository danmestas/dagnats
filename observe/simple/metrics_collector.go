// observe/simple/metrics_collector.go
// MetricsCollector implements observe.Metrics backed by NATS JetStream.
// Each metric operation serializes a MetricPoint to JSON and publishes it to
// "telemetry.metrics.{service}.{name}" on the TELEMETRY stream so that any
// downstream consumer can aggregate or forward without coupling to a specific
// metrics backend.
package simple

import (
	"encoding/json"
	"log"
	"math"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// MetricsCollector publishes MetricPoint events to the NATS TELEMETRY stream.
// Safe for concurrent use — each instrument publishes independently.
type MetricsCollector struct {
	js          nats.JetStreamContext
	serviceName string
}

// NewMetricsCollector constructs a MetricsCollector.
// Panics on nil js or empty serviceName — both are programmer errors.
func NewMetricsCollector(js nats.JetStreamContext, serviceName string) *MetricsCollector {
	if js == nil {
		panic("NewMetricsCollector: js must not be nil")
	}
	if serviceName == "" {
		panic("NewMetricsCollector: serviceName must not be empty")
	}
	return &MetricsCollector{js: js, serviceName: serviceName}
}

// Counter returns a new simpleCounter for the given name and tags.
func (mc *MetricsCollector) Counter(name string, tags map[string]string) *simpleCounter {
	if name == "" {
		panic("MetricsCollector.Counter: name must not be empty")
	}
	return &simpleCounter{js: mc.js, serviceName: mc.serviceName, name: name, tags: tags}
}

// Histogram returns a new simpleHistogram for the given name and tags.
func (mc *MetricsCollector) Histogram(name string, tags map[string]string) *simpleHistogram {
	if name == "" {
		panic("MetricsCollector.Histogram: name must not be empty")
	}
	return &simpleHistogram{js: mc.js, serviceName: mc.serviceName, name: name, tags: tags}
}

// Gauge returns a new simpleGauge for the given name and tags.
func (mc *MetricsCollector) Gauge(name string, tags map[string]string) *simpleGauge {
	if name == "" {
		panic("MetricsCollector.Gauge: name must not be empty")
	}
	return &simpleGauge{js: mc.js, serviceName: mc.serviceName, name: name, tags: tags}
}

// publishMetric serializes a MetricPoint to JSON and publishes it to NATS.
// Errors are logged but never returned — metric publishing is best-effort.
func publishMetric(js nats.JetStreamContext, pt MetricPoint) {
	if js == nil {
		panic("publishMetric: js must not be nil")
	}
	data, err := json.Marshal(pt)
	if err != nil {
		log.Printf("publishMetric: marshal error name=%s: %v", pt.Name, err)
		return
	}
	subject := "telemetry.metrics." + pt.Service + "." + pt.Name
	if _, err := js.Publish(subject, data); err != nil {
		log.Printf("publishMetric: publish error subject=%s: %v", subject, err)
	}
}

// simpleCounter is a monotonically increasing metric instrument.
type simpleCounter struct {
	js          nats.JetStreamContext
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
	js          nats.JetStreamContext
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
	js          nats.JetStreamContext
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

// Inc increments the gauge by 1 and publishes.
func (g *simpleGauge) Inc() {
	for {
		old := g.bits.Load()
		newVal := math.Float64frombits(old) + 1.0
		if g.bits.CompareAndSwap(old, math.Float64bits(newVal)) {
			break
		}
	}
	g.publish()
}

// Dec decrements the gauge by 1 and publishes.
func (g *simpleGauge) Dec() {
	for {
		old := g.bits.Load()
		newVal := math.Float64frombits(old) - 1.0
		if g.bits.CompareAndSwap(old, math.Float64bits(newVal)) {
			break
		}
	}
	g.publish()
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
