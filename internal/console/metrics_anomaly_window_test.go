// internal/console/metrics_anomaly_window_test.go
// Methodology: pure-function assertions that DetectAnomalies emits
// the window bracketing (since/until) the click-to-runs handler
// needs. Tests are table-free because each property has one shape;
// the existing metrics_anomaly_test.go covers the threshold matrix.
package console

import "testing"

func TestDetectAnomaliesEmitsWindowBracket(t *testing.T) {
	pts := []MetricPoint{buildHistogramPoint(10, 50)}
	got := DetectAnomalies(pts)
	if len(got) != 1 {
		t.Fatalf("expected 1 marker, got %d", len(got))
	}
	if got[0].TimestampSecs <= 0 {
		t.Fatalf("expected positive TimestampSecs, got %f",
			got[0].TimestampSecs)
	}
	wantSince := got[0].TimestampSecs - AnomalyWindowHalfSecs
	if got[0].WindowStartSecs != wantSince {
		t.Fatalf("WindowStartSecs = %f, want %f",
			got[0].WindowStartSecs, wantSince)
	}
	wantUntil := got[0].TimestampSecs + AnomalyWindowHalfSecs
	if got[0].WindowEndSecs != wantUntil {
		t.Fatalf("WindowEndSecs = %f, want %f",
			got[0].WindowEndSecs, wantUntil)
	}
}

func TestAnomalyWindowHalfSecsPositive(t *testing.T) {
	// Pin the constant — a regression to zero would silently break
	// the click-to-runs window. 90s = ±1.5 minutes, the smallest
	// usefully-wide window that catches adjacent runs.
	if AnomalyWindowHalfSecs <= 0 {
		t.Fatalf("AnomalyWindowHalfSecs must be positive, got %d",
			AnomalyWindowHalfSecs)
	}
	if AnomalyWindowHalfSecs > 600 {
		t.Fatalf("AnomalyWindowHalfSecs too wide, got %d",
			AnomalyWindowHalfSecs)
	}
}
