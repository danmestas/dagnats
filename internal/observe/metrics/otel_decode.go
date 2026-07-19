package metrics

import (
	"encoding/json"
	"math"
)

// otelAttribute mirrors the OTel SDK's JSON shape for an attribute.
// Both key/value land as JSON strings/numbers; we coerce to string for
// the label map.
type otelAttribute struct {
	Key   string          `json:"Key"`
	Value json.RawMessage `json:"Value"`
}

// otelDataPoint mirrors the OTel SDK's data-point JSON shape for
// gauges + sums. Value / Int are the two numeric carriers; the SDK
// picks one based on whether the instrument is float or int.
type otelDataPoint struct {
	Attributes []otelAttribute `json:"Attributes"`
	StartTime  string          `json:"StartTime"`
	Time       string          `json:"Time"`
	Value      *float64        `json:"Value,omitempty"`
	Int        *int64          `json:"Int,omitempty"`
}

// otelGauge is the JSON shape of metricdata.Gauge[T].
type otelGauge struct {
	DataPoints []otelDataPoint `json:"DataPoints"`
}

// otelSum is the JSON shape of metricdata.Sum[T].
type otelSum struct {
	DataPoints  []otelDataPoint `json:"DataPoints"`
	Temporality json.RawMessage `json:"Temporality"`
	IsMonotonic bool            `json:"IsMonotonic"`
}

// otelHistogramDP mirrors metricdata.HistogramDataPoint[T].
type otelHistogramDP struct {
	Attributes   []otelAttribute `json:"Attributes"`
	StartTime    string          `json:"StartTime"`
	Time         string          `json:"Time"`
	Count        uint64          `json:"Count"`
	Bounds       []float64       `json:"Bounds"`
	BucketCounts []uint64        `json:"BucketCounts"`
	Sum          float64         `json:"Sum"`
}

// otelHistogram is the JSON shape of metricdata.Histogram[T].
type otelHistogram struct {
	DataPoints  []otelHistogramDP `json:"DataPoints"`
	Temporality json.RawMessage   `json:"Temporality"`
}

// decodeSum attempts to read a counter / sum data point from raw JSON.
// Returns false when the shape doesn't match — caller tries the next
// kind. The aggregator uses the first non-empty data point; multi-
// label series would need a follow-up rev (out of scope for v1).
func decodeSum(raw json.RawMessage) (Point, bool) {
	if len(raw) == 0 {
		return Point{}, false
	}
	var sum otelSum
	if err := json.Unmarshal(raw, &sum); err != nil {
		return Point{}, false
	}
	if len(sum.DataPoints) == 0 {
		return Point{}, false
	}
	dp := sum.DataPoints[0]
	if dp.Value == nil && dp.Int == nil {
		return Point{}, false
	}
	return Point{
		Value:  safeFloat(numericFromDP(dp)),
		Labels: attributesToMap(dp.Attributes),
	}, true
}

// decodeGauge handles metricdata.Gauge. Same shape sniff as sum, but
// without the IsMonotonic/Temporality fields. We attempt gauge after
// sum so a sum (with both fields populated) doesn't accidentally
// match the gauge decoder.
func decodeGauge(raw json.RawMessage) (Point, bool) {
	if len(raw) == 0 {
		return Point{}, false
	}
	var gauge otelGauge
	if err := json.Unmarshal(raw, &gauge); err != nil {
		return Point{}, false
	}
	if len(gauge.DataPoints) == 0 {
		return Point{}, false
	}
	dp := gauge.DataPoints[0]
	if dp.Value == nil && dp.Int == nil {
		return Point{}, false
	}
	return Point{
		Value:  safeFloat(numericFromDP(dp)),
		Labels: attributesToMap(dp.Attributes),
	}, true
}

// decodeHistogram handles metricdata.Histogram. Buckets are flattened
// into cumulative HistogramBucket entries with a final +Inf bucket
// matching Prometheus conventions.
func decodeHistogram(raw json.RawMessage) (Point, bool) {
	if len(raw) == 0 {
		return Point{}, false
	}
	var hist otelHistogram
	if err := json.Unmarshal(raw, &hist); err != nil {
		return Point{}, false
	}
	if len(hist.DataPoints) == 0 {
		return Point{}, false
	}
	dp := hist.DataPoints[0]
	if len(dp.BucketCounts) == 0 {
		return Point{}, false
	}
	buckets := flattenBuckets(dp.Bounds, dp.BucketCounts)
	return Point{
		Value:   safeFloat(dp.Sum),
		Count:   dp.Count,
		Sum:     safeFloat(dp.Sum),
		Buckets: buckets,
		Labels:  attributesToMap(dp.Attributes),
	}, true
}

// numericFromDP picks the populated numeric field from a data point.
// OTel guarantees exactly one of Value / Int is non-nil per instrument
// type.
func numericFromDP(dp otelDataPoint) float64 {
	if dp.Value != nil {
		return *dp.Value
	}
	if dp.Int != nil {
		return float64(*dp.Int)
	}
	return 0
}

// flattenBuckets converts OTel's parallel (bounds, bucketCounts) arrays
// into a single cumulative HistogramBucket slice. Prometheus expects
// cumulative counts; OTel publishes per-bucket counts, so we run a
// running total. Bounded loop on the bucket count.
func flattenBuckets(
	bounds []float64, counts []uint64,
) []HistogramBucket {
	if len(counts) == 0 {
		return nil
	}
	// Bounds defines the explicit upper bounds; counts has one more
	// entry than bounds (the final +Inf bucket).
	const maxBuckets = 256
	limit := len(counts)
	if limit > maxBuckets {
		limit = maxBuckets
	}
	out := make([]HistogramBucket, 0, limit)
	cumulative := uint64(0)
	for i := 0; i < limit; i++ {
		cumulative += counts[i]
		bound := math.Inf(+1)
		if i < len(bounds) {
			bound = bounds[i]
		}
		out = append(out, HistogramBucket{
			UpperBound: bound,
			Count:      cumulative,
		})
	}
	return out
}

// attributesToMap renders an OTel attribute slice into a stable
// map[string]string. Values are JSON-coerced (strings unquoted,
// numbers Stringified, booleans rendered "true"/"false").
func attributesToMap(
	attrs []otelAttribute,
) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]string, len(attrs))
	for _, a := range attrs {
		if a.Key == "" {
			continue
		}
		out[a.Key] = attributeValueString(a.Value)
	}
	return out
}

// attributeValueString reads an OTel attribute value (which the SDK
// serialises as either a primitive or a {"Type":..., "Value":...}
// shape). The native primitive path covers the cases the engine emits;
// the typed-shape fallback handles future label types without a
// schema migration.
//
// recursion:allow the typed-shape fallback unwraps one {"Type","Value"}
// layer per call. The SDK emits at most one such wrapper, so this is a
// single re-entry in practice rather than an open-ended descent.
func attributeValueString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	if s, ok := tryUnmarshalString(raw); ok {
		return s
	}
	if n, ok := tryUnmarshalNumber(raw); ok {
		return n
	}
	if b, ok := tryUnmarshalBool(raw); ok {
		return b
	}
	var typed struct {
		Type  string          `json:"Type"`
		Value json.RawMessage `json:"Value"`
	}
	if err := json.Unmarshal(raw, &typed); err == nil {
		return attributeValueString(typed.Value)
	}
	return ""
}

// tryUnmarshalString returns the string when raw is a JSON string,
// false otherwise. Cheap probe used at the top of the dispatcher.
func tryUnmarshalString(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, true
	}
	return "", false
}

// tryUnmarshalNumber returns the JSON number as its string form. We
// don't care about int-vs-float distinction at this layer; the label
// renderer just needs a stable string.
func tryUnmarshalNumber(raw json.RawMessage) (string, bool) {
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String(), true
	}
	return "", false
}

// tryUnmarshalBool returns "true"/"false" when raw is a JSON bool.
func tryUnmarshalBool(raw json.RawMessage) (string, bool) {
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		if b {
			return "true", true
		}
		return "false", true
	}
	return "", false
}
