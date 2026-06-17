// otel_decode_test.go covers the JSON decoders that translate
// the OTel SDK's metricdata wire shape into Point / Series form.
//
// Methodology:
//   - Pure unit tests; no NATS, no SDK runtime.
//   - Each test feeds a literal JSON payload mirroring the shape the
//     OTel SDK emits and asserts the extracted point matches.
//   - Minimum 2 assertions per test.
package metrics

import (
	"math"
	"testing"
)

func TestDecodeSum_ExtractsValueAndLabels(t *testing.T) {
	raw := []byte(`{
		"DataPoints": [{
			"Attributes": [
				{"Key": "outcome", "Value": "completed"}
			],
			"Int": 42
		}],
		"Temporality": "CumulativeTemporality",
		"IsMonotonic": true
	}`)
	pt, ok := decodeSum(raw)
	if !ok {
		t.Fatal("decodeSum returned false on a valid Sum payload")
	}
	if pt.Value != 42 {
		t.Fatalf("Value = %v, want 42", pt.Value)
	}
	if pt.Labels["outcome"] != "completed" {
		t.Fatalf("Labels[outcome] = %q, want completed", pt.Labels["outcome"])
	}
}

func TestDecodeSum_HandlesFloatValue(t *testing.T) {
	raw := []byte(`{"DataPoints": [{"Value": 3.14}]}`)
	pt, ok := decodeSum(raw)
	if !ok {
		t.Fatal("decodeSum must accept float Value")
	}
	if math.Abs(pt.Value-3.14) > 1e-9 {
		t.Fatalf("Value = %v, want ~3.14", pt.Value)
	}
}

func TestDecodeSum_RejectsEmptyDataPoints(t *testing.T) {
	raw := []byte(`{"DataPoints": []}`)
	if _, ok := decodeSum(raw); ok {
		t.Fatal("decodeSum must reject empty DataPoints")
	}
	if _, ok := decodeSum(nil); ok {
		t.Fatal("decodeSum must reject nil")
	}
}

func TestDecodeSum_AcceptsRealSDKTemporality(t *testing.T) {
	// The OTel SDK serialises Temporality via MarshalText, so the wire
	// form is a string ("CumulativeTemporality"), not the int our older
	// fixtures used. A Temporality typed as int fails json.Unmarshal on
	// the real payload, dropping every counter on the floor.
	raw := []byte(`{
		"DataPoints": [{"Int": 9}],
		"Temporality": "CumulativeTemporality",
		"IsMonotonic": true
	}`)
	pt, ok := decodeSum(raw)
	if !ok {
		t.Fatal("decodeSum rejected a real SDK Sum payload (string Temporality)")
	}
	if pt.Value != 9 {
		t.Fatalf("Value = %v, want 9", pt.Value)
	}
}

func TestDecodeHistogram_AcceptsRealSDKTemporality(t *testing.T) {
	// Same string-Temporality regression as Sum, but for histograms —
	// this is the path the snapshot p50 metric rides, so a decode
	// failure here is exactly why p50 never reached the console.
	raw := []byte(`{
		"DataPoints": [{
			"Count": 5,
			"Sum": 2.5,
			"Bounds": [0.1, 0.5],
			"BucketCounts": [1, 2, 2]
		}],
		"Temporality": "CumulativeTemporality"
	}`)
	pt, ok := decodeHistogram(raw)
	if !ok {
		t.Fatal("decodeHistogram rejected a real SDK Histogram payload (string Temporality)")
	}
	if pt.Count != 5 {
		t.Fatalf("Count = %d, want 5", pt.Count)
	}
	if len(pt.Buckets) != 3 {
		t.Fatalf("len Buckets = %d, want 3", len(pt.Buckets))
	}
}

func TestDecodeGauge_ExtractsValue(t *testing.T) {
	raw := []byte(`{"DataPoints": [{"Int": 7}]}`)
	pt, ok := decodeGauge(raw)
	if !ok {
		t.Fatal("decodeGauge returned false for valid payload")
	}
	if pt.Value != 7 {
		t.Fatalf("Value = %v, want 7", pt.Value)
	}
}

func TestDecodeHistogram_BuildsCumulativeBuckets(t *testing.T) {
	raw := []byte(`{
		"DataPoints": [{
			"Attributes": [
				{"Key": "workflow", "Value": "demo"}
			],
			"Count": 10,
			"Sum": 12.5,
			"Bounds": [0.1, 0.5, 1.0],
			"BucketCounts": [3, 4, 2, 1]
		}]
	}`)
	pt, ok := decodeHistogram(raw)
	if !ok {
		t.Fatal("decodeHistogram returned false")
	}
	if pt.Count != 10 {
		t.Fatalf("Count = %d, want 10", pt.Count)
	}
	if pt.Labels["workflow"] != "demo" {
		t.Fatalf("Labels[workflow] = %q", pt.Labels["workflow"])
	}
	if len(pt.Buckets) != 4 {
		t.Fatalf("len Buckets = %d, want 4", len(pt.Buckets))
	}
	want := []uint64{3, 7, 9, 10}
	for i, b := range pt.Buckets {
		if b.Count != want[i] {
			t.Fatalf("Buckets[%d].Count = %d, want %d", i, b.Count, want[i])
		}
	}
	// Last bucket must be +Inf per Prometheus convention.
	if !math.IsInf(pt.Buckets[3].UpperBound, +1) {
		t.Fatalf("Buckets[3].UpperBound = %v, want +Inf", pt.Buckets[3].UpperBound)
	}
}

func TestDecodeHistogram_RejectsEmptyBuckets(t *testing.T) {
	raw := []byte(`{"DataPoints": [{"BucketCounts": []}]}`)
	if _, ok := decodeHistogram(raw); ok {
		t.Fatal("decodeHistogram must reject empty BucketCounts")
	}
	if _, ok := decodeHistogram(nil); ok {
		t.Fatal("decodeHistogram must reject nil")
	}
}

func TestDecodeRecord_DispatchesByDataShape(t *testing.T) {
	rec := metricRecord{
		Name: "test_counter",
		Data: []byte(`{
			"DataPoints": [{"Int": 5}],
			"Temporality": "CumulativeTemporality",
			"IsMonotonic": true
		}`),
	}
	series, _, ok := decodeRecord(rec)
	if !ok {
		t.Fatal("decodeRecord returned false")
	}
	if series.Name != "test_counter" {
		t.Fatalf("Name = %q", series.Name)
	}
	// The Int data-point shape matches both sum and gauge. Sum now
	// decodes directly via json.RawMessage Temporality, and decodeRecord
	// still attempts sum before gauge, so KindCounter holds.
	if series.Kind != KindCounter {
		t.Fatalf("Kind = %q, want counter", series.Kind)
	}
}

func TestAttributeValueString_HandlesPrimitives(t *testing.T) {
	cases := map[string]string{
		`"hello"`: "hello",
		`42`:      "42",
		`true`:    "true",
		`false`:   "false",
	}
	for raw, want := range cases {
		got := attributeValueString([]byte(raw))
		if got != want {
			t.Fatalf("raw=%s got %q, want %q", raw, got, want)
		}
	}
	// Defensive: empty raw returns empty.
	if got := attributeValueString(nil); got != "" {
		t.Fatalf("nil raw = %q, want empty", got)
	}
}
