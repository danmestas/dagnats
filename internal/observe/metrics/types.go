package metrics

import (
	"strings"
	"time"
)

// Kind identifies the metric instrument shape. The aggregator does not
// invent or coerce kinds — it stores whatever the upstream record says
// it is. Three kinds cover OTel's primary instruments mapped onto
// Prometheus conventions:
//
//   - KindCounter   — monotonic. Renders as Prometheus counter (suffixed _total).
//   - KindGauge     — settable. Renders as Prometheus gauge.
//   - KindHistogram — bucketed distribution. Renders Prometheus
//     _bucket / _sum / _count companions.
type Kind string

const (
	// KindUnknown is the zero value; a sample with KindUnknown is
	// dropped at ingestion with a slog.Warn so operators see the gap.
	KindUnknown   Kind = ""
	KindCounter   Kind = "counter"
	KindGauge     Kind = "gauge"
	KindHistogram Kind = "histogram"
)

// Point is one numeric observation at a moment in time. Labels are the
// fully-resolved (key=value) attributes of the data point — flattened
// from OTel's attribute.Set form. For histograms, Value is the running
// sum and Buckets carries the per-bucket cumulative counts.
type Point struct {
	Timestamp time.Time
	Value     float64
	Count     uint64
	Sum       float64
	Buckets   []HistogramBucket
	Labels    map[string]string
}

// HistogramBucket is one cumulative bucket in a histogram observation.
// UpperBound matches Prometheus's "le" semantics: a bucket with bound
// 0.5 counts every observation with value <= 0.5. The +Inf bucket has
// math.Inf(+1) here.
type HistogramBucket struct {
	UpperBound float64
	Count      uint64
}

// Series carries the history of one metric. Name is the canonical OTel
// metric name (dots, underscores preserved); the Prometheus exporter
// is responsible for translating to Prometheus naming rules.
type Series struct {
	Name        string
	Kind        Kind
	Description string
	Unit        string
	Service     string
	Points      []Point
}

// Latest returns the most recent point in the series, or the zero
// Point when the series is empty. Used by the dashboard to render
// "current value" tiles without re-scanning the buffer.
func (s Series) Latest() Point {
	if len(s.Points) == 0 {
		return Point{}
	}
	return s.Points[len(s.Points)-1]
}

// LabelKey is the canonical key the aggregator uses for label-aware
// metric storage. Multiple data points with different label sets all
// belong to the same Series; this helper renders the labels into a
// stable string so the renderer can group them.
func LabelKey(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sortStrings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

// sortStrings is a small alloc-free insertion sort for the LabelKey
// path. The label set in a single point is always tiny (≤ 8) so an
// O(n²) sort beats pulling in sort.Strings for ergonomics.
func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}
